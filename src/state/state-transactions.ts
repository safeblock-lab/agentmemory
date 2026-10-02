import { Buffer } from 'node:buffer'
import { createHash } from 'node:crypto'
import { GRAPH_JOB_SCOPES, GRAPH_JOB_SCOPE_PREFIXES, GRAPH_MANAGED_SCOPES, isGuardedGraphRecord, KV } from './schema.js'

export const STATE_COMMIT_TARGET_BYTES = 4 * 1024 * 1024
export const MAX_STATE_COMMIT_OPERATIONS = 256
export const MAX_STATE_LEASE_TTL_MS = 300_000
const MAX_COUNTER = 18_446_744_073_709_551_615n

export type StateCounter = string
export type StateJsonValue = null | boolean | number | string | StateJsonValue[] | { [key: string]: StateJsonValue }
export interface StateGraphGuard { owner_id: string; generation: StateCounter; fence: StateCounter }
export interface StateGraphLease extends StateGraphGuard { expires_at_ms: number }
export type StateLeaseRequest =
  | { action: 'acquire'; owner_id: string; generation: StateCounter; ttl_ms: number }
  | ({ action: 'renew'; ttl_ms: number } & StateGraphGuard)
  | ({ action: 'release' } & StateGraphGuard)
export interface StateLeaseReleased { released: true }
export interface StateVersioned<T> { exists: boolean; value: T | null; version: StateCounter }
export interface StateVersionedRequest { scope: string; key: string; guard?: StateGraphGuard }
export interface StateCommitIdentity { generation: StateCounter; job_id: string; logical_delta_id: string; chunk_ordinal: number }
export type StateGraphVisibility = 'staging' | 'applying' | 'complete'
export interface StateGraphCheckpoint {
  generation: StateCounter; job_id: string; logical_delta_id: string
  delta_ordinal: number; next_chunk_ordinal: number; visibility: StateGraphVisibility
}
interface StateExpectedRow { scope: string; key: string; expected_version: StateCounter }
export type StateCommitOperation =
  | (StateExpectedRow & { type: 'set'; value: StateJsonValue })
  | (StateExpectedRow & { type: 'delete' | 'check' })
export interface StateCommitBatchInput {
  identity: StateCommitIdentity; expected_checkpoint_version: StateCounter
  checkpoint: StateGraphCheckpoint; operations: StateCommitOperation[]
  advance_generation?: true; allow_oversized_record?: true
}
export interface StatePreparedBatch { payload_json: string; payload_digest: string }
export interface StateCommitBatchRequest extends StatePreparedBatch { guard: StateGraphGuard }
export interface StateBatchReceipt {
  identity: StateCommitIdentity; payload_digest: string; generation: StateCounter
  checkpoint_version: StateCounter; row_versions: Array<{ scope: string; key: string; version: StateCounter }>
}
export type StateTransactionFunction = 'state::get_versioned' | 'state::lease' | 'state::commit_batch'
export type StateTransactionTrigger = (functionId: StateTransactionFunction, payload: unknown) => Promise<unknown>

const ERROR_MESSAGES = {
  STATE_TX_INVALID_REQUEST: 'The state transaction request is invalid',
  STATE_TX_INVALID_RESPONSE: 'The state transaction response is invalid',
  STATE_TX_UNSUPPORTED: 'The configured iii-engine does not support state transactions',
  STATE_TX_CONFLICT: 'A state row version changed',
  STATE_TX_CHECKPOINT_CONFLICT: 'The graph checkpoint changed',
  STATE_TX_REPLAY_CONFLICT: 'The committed identity has different prepared content',
  STATE_TX_LEASE_BUSY: 'Another graph writer owns the lease',
  STATE_TX_FENCED: 'The graph writer lease is expired or superseded',
  STATE_TX_GENERATION_STALE: 'The graph generation changed',
  STATE_GRAPH_RECOVERY_REQUIRED: 'An incomplete graph delta must be recovered',
  STATE_TX_LIMIT_EXCEEDED: 'The state transaction exceeds its supported budget',
  STATE_RECORD_TOO_LARGE: 'A state record exceeds the native transport envelope',
  STATE_TX_FAILED: 'The state transaction failed',
} as const
export type StateTransactionErrorCode = keyof typeof ERROR_MESSAGES
export class StateTransactionError extends Error {
  constructor(readonly code: StateTransactionErrorCode, cause?: unknown) {
    super(`${code}: ${ERROR_MESSAGES[code]}`, { cause }); this.name = 'StateTransactionError'
  }
}
function requireValid(valid: boolean, code: StateTransactionErrorCode = 'STATE_TX_INVALID_REQUEST'): asserts valid {
  if (!valid) throw new StateTransactionError(code)
}
function object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
function keys(value: unknown, required: string[], optional: string[] = []): value is Record<string, unknown> {
  return object(value) && required.every((key) => Object.hasOwn(value, key))
    && Object.keys(value).every((key) => required.includes(key) || optional.includes(key))
}
function text(value: unknown): value is string { return typeof value === 'string' && value.length > 0 }
function ordinal(value: unknown): value is number { return Number.isSafeInteger(value) && Number(value) >= 0 && Number(value) < Number.MAX_SAFE_INTEGER }
export function isStateCounter(value: unknown): value is StateCounter {
  return typeof value === 'string' && /^(0|[1-9][0-9]{0,19})$/.test(value) && BigInt(value) <= MAX_COUNTER
}
function guard(value: unknown): value is StateGraphGuard {
  return keys(value, ['owner_id', 'generation', 'fence']) && text(value.owner_id)
    && isStateCounter(value.generation) && value.generation !== '0' && isStateCounter(value.fence) && value.fence !== '0'
}
function normalizeGuard(value: StateGraphGuard): StateGraphGuard {
  requireValid(keys(value, ['owner_id', 'generation', 'fence'], ['expires_at_ms']))
  const result = { owner_id: value.owner_id, generation: value.generation, fence: value.fence }
  requireValid(guard(result)); return result
}
function counterAfter(value: StateCounter): StateCounter { return (BigInt(value) + 1n).toString() }
function responseJson(value: unknown): void {
  try { json(value) } catch (error) { throw new StateTransactionError('STATE_TX_INVALID_RESPONSE', error) }
}

function json(value: unknown, ancestors = new Set<object>()): string {
  if (value === null || typeof value === 'boolean') return JSON.stringify(value)
  if (typeof value === 'number') { requireValid(Number.isFinite(value)); return JSON.stringify(value) }
  if (typeof value === 'string') {
    requireValid(!/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/u.test(value))
    return JSON.stringify(value)
  }
  requireValid(object(value) || Array.isArray(value))
  requireValid(!ancestors.has(value) && [Object.prototype, Array.prototype, null].includes(Object.getPrototypeOf(value)))
  ancestors.add(value)
  const names = Object.keys(value)
  requireValid(Reflect.ownKeys(value).length === names.length + (Array.isArray(value) ? 1 : 0))
  requireValid(names.every((name) => Object.hasOwn(Object.getOwnPropertyDescriptor(value, name) ?? {}, 'value')))
  let encoded: string
  if (Array.isArray(value)) {
    requireValid(names.length === value.length && names.every((name, index) => name === String(index)))
    encoded = `[${value.map((item) => json(item, ancestors)).join(',')}]`
  } else {
    names.sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)))
    encoded = `{${names.map((name) => `${json(name)}:${json(value[name], ancestors)}`).join(',')}}`
  }
  ancestors.delete(value)
  return encoded
}
function validateBody(value: unknown): asserts value is StateCommitBatchInput {
  requireValid(keys(value, ['identity', 'expected_checkpoint_version', 'checkpoint', 'operations'], ['advance_generation', 'allow_oversized_record']))
  const id = value.identity, point = value.checkpoint
  requireValid(keys(id, ['generation', 'job_id', 'logical_delta_id', 'chunk_ordinal']) && isStateCounter(id.generation)
    && id.generation !== '0' && text(id.job_id) && text(id.logical_delta_id) && ordinal(id.chunk_ordinal))
  requireValid(isStateCounter(value.expected_checkpoint_version) && BigInt(value.expected_checkpoint_version) < MAX_COUNTER)
  requireValid(keys(point, ['generation', 'job_id', 'logical_delta_id', 'delta_ordinal', 'next_chunk_ordinal', 'visibility'])
    && point.generation === id.generation && point.job_id === id.job_id && point.logical_delta_id === id.logical_delta_id
    && ordinal(point.delta_ordinal) && ordinal(point.next_chunk_ordinal) && point.next_chunk_ordinal === Number(id.chunk_ordinal) + 1
    && ['staging', 'applying', 'complete'].includes(String(point.visibility)))
  requireValid(value.advance_generation === undefined || (value.advance_generation === true && point.visibility === 'complete' && BigInt(String(id.generation)) < MAX_COUNTER))
  requireValid(value.allow_oversized_record === undefined || value.allow_oversized_record === true)
  requireValid(Array.isArray(value.operations) && value.operations.length <= MAX_STATE_COMMIT_OPERATIONS)
  const seen = new Set<string>()
  for (const op of value.operations) {
    requireValid(keys(op, ['type', 'scope', 'key', 'expected_version'], op?.type === 'set' ? ['value'] : []))
    requireValid(['set', 'delete', 'check'].includes(String(op.type)) && (op.type !== 'set' || Object.hasOwn(op, 'value')))
    requireValid(text(op.scope) && text(op.key) && isGuardedGraphRecord(op.scope, op.key)
      && !(GRAPH_MANAGED_SCOPES as readonly string[]).includes(op.scope) && isStateCounter(op.expected_version))
    requireValid(op.type === 'check' || BigInt(op.expected_version) < MAX_COUNTER)
    const row = JSON.stringify([op.scope, op.key]); requireValid(!seen.has(row)); seen.add(row)
    if (point.visibility === 'staging') requireValid(((GRAPH_JOB_SCOPES as readonly string[]).includes(op.scope) && op.key === id.job_id)
      || GRAPH_JOB_SCOPE_PREFIXES.some((prefix) => op.scope === `${prefix}${id.job_id}`))
    if (point.visibility !== 'complete') requireValid(op.scope !== KV.graphSnapshot || op.type === 'check')
  }
  if (point.visibility === 'complete') requireValid(value.operations.some((op) => op.scope === KV.graphSnapshot && op.key === 'current' && ['set', 'check'].includes(op.type)))
  if (value.allow_oversized_record) requireValid(value.operations.filter((op) => op.type !== 'check').length === 1 && value.operations.some((op) => op.type === 'set'))
}
export function prepareStateCommitBatch(input: StateCommitBatchInput): StatePreparedBatch {
  try {
    const payload_json = json(input); const decoded: unknown = JSON.parse(payload_json); validateBody(decoded)
    return Object.freeze({ payload_json, payload_digest: createHash('sha256').update(payload_json).digest('hex') })
  } catch (error) {
    if (error instanceof StateTransactionError) throw error
    throw new StateTransactionError('STATE_TX_INVALID_REQUEST', error)
  }
}
async function invoke(trigger: StateTransactionTrigger, functionId: StateTransactionFunction, payload: unknown): Promise<unknown> {
  try { return await trigger(functionId, payload) }
  catch (error) {
    const record = object(error) ? error : {}; const message = typeof record.message === 'string' ? record.message : ''
    const code = typeof record.code === 'string' ? record.code : message.match(/^([A-Z][A-Z0-9_]+):/)?.[1]
    if (code && Object.hasOwn(ERROR_MESSAGES, code)) throw new StateTransactionError(code as StateTransactionErrorCode, error)
    const missing = ['FUNCTION_NOT_FOUND', 'NO_SUCH_FUNCTION', 'UNKNOWN_FUNCTION'].includes(code ?? '')
      || ((code === 'UNSUPPORTED' || /(?:no function|function not found|unknown function)/i.test(message)) && (record.function_id === functionId || message.includes(functionId)))
    throw new StateTransactionError(missing ? 'STATE_TX_UNSUPPORTED' : 'STATE_TX_FAILED', error)
  }
}
export async function getVersionedState<T>(trigger: StateTransactionTrigger, request: StateVersionedRequest): Promise<StateVersioned<T>> {
  requireValid(keys(request, ['scope', 'key'], ['guard']) && text(request.scope) && text(request.key))
  const payload = { scope: request.scope, key: request.key, ...(request.guard === undefined ? {} : { guard: normalizeGuard(request.guard) }) }
  const result = await invoke(trigger, 'state::get_versioned', payload)
  responseJson(result)
  requireValid(keys(result, ['exists', 'value', 'version']) && typeof result.exists === 'boolean' && isStateCounter(result.version)
    && (result.exists || result.value === null) && (!result.exists || result.version !== '0'), 'STATE_TX_INVALID_RESPONSE')
  try { json(result.value) } catch (error) { throw new StateTransactionError('STATE_TX_INVALID_RESPONSE', error) }
  return result as unknown as StateVersioned<T>
}
export async function requestStateLease(trigger: StateTransactionTrigger, request: StateLeaseRequest): Promise<StateGraphLease | StateLeaseReleased> {
  const action = request?.action; const names = action === 'acquire' ? ['action', 'owner_id', 'generation', 'ttl_ms'] : action === 'renew' ? ['action', 'owner_id', 'generation', 'fence', 'ttl_ms'] : ['action', 'owner_id', 'generation', 'fence']
  requireValid(keys(request as unknown, names) && ['acquire', 'renew', 'release'].includes(String(action)) && text(request.owner_id)
    && isStateCounter(request.generation) && request.generation !== '0')
  if (request.action !== 'acquire') requireValid(guard({ owner_id: request.owner_id, generation: request.generation, fence: request.fence }))
  if (request.action !== 'release') requireValid(Number.isSafeInteger(request.ttl_ms) && request.ttl_ms >= 1 && request.ttl_ms <= MAX_STATE_LEASE_TTL_MS)
  const result = await invoke(trigger, 'state::lease', request)
  responseJson(result)
  const valid = request.action === 'release' ? keys(result, ['released']) && result.released === true
    : keys(result, ['owner_id', 'generation', 'fence', 'expires_at_ms']) && guard({ owner_id: result.owner_id, generation: result.generation, fence: result.fence })
      && result.owner_id === request.owner_id && result.generation === request.generation && Number.isSafeInteger(result.expires_at_ms) && Number(result.expires_at_ms) > 0
      && (request.action === 'acquire' || result.fence === request.fence)
  requireValid(valid, 'STATE_TX_INVALID_RESPONSE'); return result as unknown as StateGraphLease | StateLeaseReleased
}
export async function commitStateBatch(trigger: StateTransactionTrigger, writer: StateGraphGuard, prepared: StatePreparedBatch): Promise<StateBatchReceipt> {
  const authorization = normalizeGuard(writer)
  requireValid(keys(prepared, ['payload_json', 'payload_digest']) && text(prepared.payload_json)
    && typeof prepared.payload_digest === 'string' && /^[a-f0-9]{64}$/.test(prepared.payload_digest))
  requireValid(createHash('sha256').update(prepared.payload_json).digest('hex') === prepared.payload_digest)
  let body: unknown
  try { body = JSON.parse(prepared.payload_json); requireValid(json(body) === prepared.payload_json) } catch (error) { throw new StateTransactionError('STATE_TX_INVALID_REQUEST', error) }
  validateBody(body)
  const request = { guard: authorization, ...prepared }
  requireValid(Buffer.byteLength(JSON.stringify(request)) <= STATE_COMMIT_TARGET_BYTES || body.allow_oversized_record === true, 'STATE_TX_LIMIT_EXCEEDED')
  const result = await invoke(trigger, 'state::commit_batch', request)
  responseJson(result)
  const response = 'STATE_TX_INVALID_RESPONSE'
  requireValid(keys(result, ['identity', 'payload_digest', 'generation', 'checkpoint_version', 'row_versions'])
    && json(result.identity) === json(body.identity) && result.payload_digest === prepared.payload_digest
    && result.generation === (body.advance_generation ? counterAfter(body.identity.generation) : body.identity.generation)
    && result.checkpoint_version === counterAfter(body.expected_checkpoint_version) && Array.isArray(result.row_versions)
    && result.row_versions.length === body.operations.length, response)
  requireValid(result.row_versions.every((row, index) => {
    const op = body.operations[index]
    return keys(row, ['scope', 'key', 'version']) && row.scope === op.scope && row.key === op.key
      && row.version === (op.type === 'check' ? op.expected_version : counterAfter(op.expected_version))
  }), response)
  return result as unknown as StateBatchReceipt
}
