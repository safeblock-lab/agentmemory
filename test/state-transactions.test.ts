import { createHash } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { StateKV } from '../src/state/kv.js'
import { isGuardedGraphRecord, KV } from '../src/state/schema.js'
import {
  MAX_STATE_COMMIT_OPERATIONS, prepareStateCommitBatch, STATE_COMMIT_TARGET_BYTES,
  type StateBatchReceipt, type StateCommitBatchInput, type StateCommitBatchRequest,
  type StateGraphGuard, type StateGraphLease, type StateLeaseRequest, type StatePreparedBatch,
  type StateVersioned,
} from '../src/state/state-transactions.js'

const writer: StateGraphGuard = { owner_id: 'attempt-1', generation: '1', fence: '1' }
const next = (value: string) => (BigInt(value) + 1n).toString()
const rowKey = (scope: string, key: string) => JSON.stringify([scope, key])
function input(overrides: Partial<StateCommitBatchInput> = {}): StateCommitBatchInput {
  return {
    identity: { generation: '1', job_id: 'job-1', logical_delta_id: 'heuristic-0', chunk_ordinal: 0 },
    expected_checkpoint_version: '0',
    checkpoint: { generation: '1', job_id: 'job-1', logical_delta_id: 'heuristic-0', delta_ordinal: 0, next_chunk_ordinal: 1, visibility: 'applying' },
    operations: [{ type: 'set', scope: KV.graphNodes, key: 'node-1', expected_version: '0', value: { weight: 1, capturedAt: 'frozen' } }],
    ...overrides,
  }
}
function receipt(body: StateCommitBatchInput, prepared: StatePreparedBatch): StateBatchReceipt {
  return {
    identity: body.identity, payload_digest: prepared.payload_digest,
    generation: body.advance_generation ? next(body.identity.generation) : body.identity.generation,
    checkpoint_version: next(body.expected_checkpoint_version),
    row_versions: body.operations.map((op) => ({ scope: op.scope, key: op.key, version: op.type === 'check' ? op.expected_version : next(op.expected_version) })),
  }
}
function createKV(handler: (request: { function_id: string; payload: unknown }) => unknown) {
  const trigger = vi.fn(async (request) => handler(request))
  return { trigger, kv: new StateKV({ trigger } as never) }
}
// A protocol oracle, not a substitute for native database atomicity tests.
function engine() {
  let generation = '1', fence = '0', now = 1_000, lease: StateGraphLease | null = null
  let recovery: StateCommitBatchInput['checkpoint'] | null = null, loseAcknowledgment = false
  const rows = new Map<string, StateVersioned<unknown>>()
  const checkpoints = new Map<string, string>(), receipts = new Map<string, StateBatchReceipt>()
  function fail(code: string): never { throw Object.assign(new Error('private native state'), { code }) }
  function authorize(guard: StateGraphGuard) {
    if (guard.generation !== generation) fail('STATE_TX_GENERATION_STALE')
    if (!lease || lease.expires_at_ms <= now || guard.owner_id !== lease.owner_id || guard.fence !== lease.fence) fail('STATE_TX_FENCED')
  }
  const instance = createKV(({ function_id, payload }) => {
    if (function_id === 'state::lease') {
      const request = payload as StateLeaseRequest
      if (request.generation !== generation) fail('STATE_TX_GENERATION_STALE')
      if (request.action === 'acquire') {
        if (lease && lease.expires_at_ms > now) {
          if (lease.owner_id !== request.owner_id) fail('STATE_TX_LEASE_BUSY')
          return { ...lease }
        }
        fence = next(fence); lease = { owner_id: request.owner_id, generation, fence, expires_at_ms: now + request.ttl_ms }
        return { ...lease }
      }
      authorize(request)
      if (request.action === 'release') { lease = null; return { released: true } }
      lease = { owner_id: request.owner_id, generation, fence: request.fence, expires_at_ms: now + request.ttl_ms }
      return { ...lease }
    }
    if (function_id === 'state::get_versioned') {
      const request = payload as { scope: string; key: string; guard?: StateGraphGuard }
      if (request.guard) authorize(request.guard)
      else if (recovery && isGuardedGraphRecord(request.scope, request.key) && !(request.scope === KV.graphSnapshot && request.key === 'current')) fail('STATE_GRAPH_RECOVERY_REQUIRED')
      return rows.get(rowKey(request.scope, request.key)) ?? { exists: false, value: null, version: '0' }
    }
    if (function_id === 'state::set') {
      const request = payload as { scope: string; key: string }
      if (isGuardedGraphRecord(request.scope, request.key)) fail('STATE_TX_FENCED')
      return null
    }
    const request = payload as StateCommitBatchRequest; authorize(request.guard)
    const body = JSON.parse(request.payload_json) as StateCommitBatchInput
    const key = JSON.stringify([body.identity.generation, body.identity.job_id, body.identity.logical_delta_id, body.identity.chunk_ordinal])
    const prior = receipts.get(key)
    if (prior) {
      if (prior.payload_digest !== request.payload_digest) fail('STATE_TX_REPLAY_CONFLICT')
      return structuredClone(prior)
    }
    if (body.identity.generation !== generation) fail('STATE_TX_GENERATION_STALE')
    if (recovery && (recovery.job_id !== body.identity.job_id || recovery.logical_delta_id !== body.identity.logical_delta_id)) fail('STATE_GRAPH_RECOVERY_REQUIRED')
    if ((checkpoints.get(body.identity.job_id) ?? '0') !== body.expected_checkpoint_version) fail('STATE_TX_CHECKPOINT_CONFLICT')
    for (const op of body.operations) if ((rows.get(rowKey(op.scope, op.key))?.version ?? '0') !== op.expected_version) fail('STATE_TX_CONFLICT')
    const result = receipt(body, request)
    for (const [index, op] of body.operations.entries()) if (op.type !== 'check') rows.set(rowKey(op.scope, op.key), {
      exists: op.type === 'set', value: op.type === 'set' ? structuredClone(op.value) : null, version: result.row_versions[index].version,
    })
    checkpoints.set(body.identity.job_id, result.checkpoint_version); receipts.set(key, result)
    if (body.checkpoint.visibility === 'applying') recovery = body.checkpoint
    if (body.checkpoint.visibility === 'complete') recovery = null
    if (body.advance_generation) { generation = result.generation; if (lease) lease.generation = generation }
    if (loseAcknowledgment) { loseAcknowledgment = false; throw new Error('private transport lost after commit') }
    return structuredClone(result)
  })
  return {
    ...instance, rows, receipts, expire: () => { now += 1_001 }, loseAck: () => { loseAcknowledgment = true },
    acquire: async (owner_id = writer.owner_id, currentGeneration = generation) => instance.kv.lease({ action: 'acquire', owner_id, generation: currentGeneration, ttl_ms: 1_000 }) as Promise<StateGraphLease>,
  }
}

describe('StateKV transaction contract', () => {
  it('preserves absent, null and tombstone versions without numeric precision loss', async () => {
    for (const value of [{ exists: false, value: null, version: '0' }, { exists: false, value: null, version: '9007199254740993' }, { exists: true, value: null, version: '18446744073709551615' }]) {
      const { kv, trigger } = createKV(() => value)
      await expect(kv.getVersioned('scope', 'key')).resolves.toEqual(value)
      expect(trigger.mock.calls[0][0]).toEqual({ function_id: 'state::get_versioned', payload: { scope: 'scope', key: 'key' } })
    }
  })
  it('freezes resulting JSON bytes and digest before effects', () => {
    const body = input(); const prepared = prepareStateCommitBatch(body)
    body.operations[0] = { type: 'delete', scope: KV.graphNodes, key: 'changed', expected_version: '0' }
    expect(Object.isFrozen(prepared)).toBe(true)
    expect(JSON.parse(prepared.payload_json).operations[0].value.weight).toBe(1)
    expect(prepared.payload_digest).toBe(createHash('sha256').update(prepared.payload_json).digest('hex'))
    expect(prepareStateCommitBatch(input()).payload_digest).toBe(prepared.payload_digest)
  })
  it('rejects unsafe JSON, counters, scopes and chunk/checkpoint identities', () => {
    const cycle: Record<string, unknown> = {}; cycle.self = cycle
    for (const invalid of [undefined, NaN, Infinity, 1n, new Date(), cycle, [, 1], { hidden: undefined }, '\ud800']) {
      expect(() => prepareStateCommitBatch(input({ operations: [{ type: 'set', scope: KV.graphNodes, key: 'a', expected_version: '0', value: invalid as never }] }))).toThrow('STATE_TX_INVALID_REQUEST')
    }
    for (const version of ['01', '-1', '1.0', '18446744073709551615', '18446744073709551616', 1]) {
      expect(() => prepareStateCommitBatch(input({ expected_checkpoint_version: version as string }))).toThrow('STATE_TX_INVALID_REQUEST')
    }
    for (const body of [input({ operations: [{ type: 'set', scope: KV.graphControl, key: 'current', expected_version: '0', value: {} }] }), input({ operations: [{ type: 'delete', scope: KV.memories, key: 'id', expected_version: '0' }] }), input({ checkpoint: { ...input().checkpoint, next_chunk_ordinal: 2 } }), input({ operations: [...input().operations, ...input().operations] })]) {
      expect(() => prepareStateCommitBatch(body)).toThrow('STATE_TX_INVALID_REQUEST')
    }
  })
  it('validates leases, renews without changing fence and fences plain graph writers', async () => {
    const store = engine(); const lease = await store.acquire()
    await expect(store.kv.lease({ action: 'renew', ...writer, ttl_ms: 300_000 })).resolves.toMatchObject(writer)
    await expect(store.kv.lease({ action: 'acquire', owner_id: 'other', generation: '1', ttl_ms: 1 })).rejects.toMatchObject({ code: 'STATE_TX_LEASE_BUSY' })
    await expect(store.kv.set(KV.graphNodes, 'bypass', {})).rejects.toMatchObject({ code: 'STATE_TX_FENCED' })
    await expect(store.kv.lease({ action: 'release', ...writer })).resolves.toEqual({ released: true })
    const later = await store.acquire(); expect(BigInt(later.fence)).toBe(BigInt(lease.fence) + 1n)
    for (const ttl_ms of [0, 300_001, 1.5]) await expect(store.kv.lease({ action: 'acquire', owner_id: 'bad', generation: '1', ttl_ms })).rejects.toMatchObject({ code: 'STATE_TX_INVALID_REQUEST' })
  })
  it('protects graph metadata/callback keys and retains tombstone versions against ABA', async () => {
    const store = engine(); await store.acquire()
    for (const [scope, key] of [[KV.graphControl, 'current'], [KV.graphJobs, 'job-1'], [KV.graphCheckpoints, 'job-1'], [KV.graphReceipts, 'receipt'], [KV.graphWorkingSnapshots, 'job-1'], [KV.graphPrepared('job-1'), 'chunk'], [KV.batchCallbacks, 'active:graph'], [KV.batchCallbacks, 'graph:effect']]) {
      await expect(store.kv.set(scope, key, {})).rejects.toMatchObject({ code: 'STATE_TX_FENCED' })
    }
    expect(isGuardedGraphRecord(KV.batchCallbacks, 'consolidation:effect')).toBe(false)
    for (const [chunk, operation] of [[0, { type: 'set', value: null }], [1, { type: 'delete' }], [2, { type: 'set', value: { reinserted: true } }]] as const) {
      const body = input({ identity: { ...input().identity, chunk_ordinal: chunk }, expected_checkpoint_version: String(chunk), checkpoint: { ...input().checkpoint, next_chunk_ordinal: chunk + 1 }, operations: [{ ...operation, scope: KV.graphNodes, key: 'node-1', expected_version: String(chunk) }] })
      await store.kv.commitBatch(writer, prepareStateCommitBatch(body))
      await expect(store.kv.getVersioned(KV.graphNodes, 'node-1', writer)).resolves.toMatchObject({ exists: operation.type === 'set', version: String(chunk + 1) })
    }
    const stale = input({ identity: { ...input().identity, chunk_ordinal: 3 }, expected_checkpoint_version: '3', checkpoint: { ...input().checkpoint, next_chunk_ordinal: 4 } })
    await expect(store.kv.commitBatch(writer, prepareStateCommitBatch(stale))).rejects.toMatchObject({ code: 'STATE_TX_CONFLICT' })
    expect(store.rows.get(rowKey(KV.graphNodes, 'node-1'))?.version).toBe('3')
  })
  it('replays one frozen effect under a fresh guard without duplicate versions or weights', async () => {
    const store = engine(); await store.acquire(); const prepared = prepareStateCommitBatch(input())
    const first = await store.kv.commitBatch(writer, prepared); store.expire(); const renewed = await store.acquire('attempt-2')
    await expect(store.kv.commitBatch(renewed, prepared)).resolves.toEqual(first)
    expect(store.rows.get(rowKey(KV.graphNodes, 'node-1'))).toEqual({ exists: true, value: { weight: 1, capturedAt: 'frozen' }, version: '1' })
    expect(store.receipts.size).toBe(1)
    await expect(store.kv.commitBatch(writer, prepared)).rejects.toMatchObject({ code: 'STATE_TX_FENCED' })
    const changed = input(); changed.operations[0] = { ...changed.operations[0], type: 'set', value: { weight: 2 } }
    await expect(store.kv.commitBatch(renewed, prepareStateCommitBatch(changed))).rejects.toMatchObject({ code: 'STATE_TX_REPLAY_CONFLICT' })
  })
  it('checks every version before any row, checkpoint or receipt effect', async () => {
    const store = engine(); await store.acquire()
    const body = input({ operations: [...input().operations, { type: 'delete', scope: KV.graphEdges, key: 'missing', expected_version: '1' }] })
    await expect(store.kv.commitBatch(writer, prepareStateCommitBatch(body))).rejects.toMatchObject({ code: 'STATE_TX_CONFLICT' })
    expect(store.rows.size).toBe(0); expect(store.receipts.size).toBe(0)
    await expect(store.kv.commitBatch(writer, prepareStateCommitBatch(input({ expected_checkpoint_version: '1' })))).rejects.toMatchObject({ code: 'STATE_TX_CHECKPOINT_CONFLICT' })
    await expect(store.kv.commitBatch(writer, prepareStateCommitBatch(input()))).resolves.toMatchObject({ checkpoint_version: '1' })
  })
  it('keeps recovery barrier after lease expiry and publishes each original completed delta', async () => {
    const store = engine(); await store.acquire(); store.rows.set(rowKey(KV.graphSnapshot, 'current'), { exists: true, value: { updatedAt: 'before' }, version: '1' })
    await store.kv.commitBatch(writer, prepareStateCommitBatch(input())); store.expire(); const recoveryOwner = await store.acquire('recovery')
    await expect(store.kv.getVersioned(KV.graphNodes, 'node-1')).rejects.toMatchObject({ code: 'STATE_GRAPH_RECOVERY_REQUIRED' })
    await expect(store.kv.getVersioned(KV.graphSnapshot, 'current')).resolves.toMatchObject({ value: { updatedAt: 'before' } })
    await expect(store.kv.getVersioned(KV.graphNodes, 'node-1', recoveryOwner)).resolves.toMatchObject({ version: '1' })
    const other = input({ identity: { ...input().identity, job_id: 'other' }, checkpoint: { ...input().checkpoint, job_id: 'other', visibility: 'staging' }, operations: [] })
    await expect(store.kv.commitBatch(recoveryOwner, prepareStateCommitBatch(other))).rejects.toMatchObject({ code: 'STATE_GRAPH_RECOVERY_REQUIRED' })
    for (const [chunk, delta, snapshotVersion] of [[1, 'heuristic-0', '1'], [0, 'provider-unit-1', '2']] as const) {
      const complete = input({ identity: { ...input().identity, logical_delta_id: delta, chunk_ordinal: chunk }, expected_checkpoint_version: snapshotVersion, checkpoint: { ...input().checkpoint, logical_delta_id: delta, delta_ordinal: delta === 'heuristic-0' ? 0 : 1, next_chunk_ordinal: chunk + 1, visibility: 'complete' }, operations: [{ type: 'set', scope: KV.graphSnapshot, key: 'current', expected_version: snapshotVersion, value: { updatedAt: delta } }] })
      await store.kv.commitBatch(recoveryOwner, prepareStateCommitBatch(complete))
      await expect(store.kv.getVersioned(KV.graphSnapshot, 'current')).resolves.toMatchObject({ value: { updatedAt: delta } })
    }
  })
  it('recovers a lost final reset acknowledgment after reacquiring the new generation', async () => {
    const store = engine(); await store.acquire()
    const reset = input({ advance_generation: true, checkpoint: { ...input().checkpoint, visibility: 'complete' }, operations: [{ type: 'set', scope: KV.graphSnapshot, key: 'current', expected_version: '0', value: { resetAt: 'frozen-reset' } }] })
    const prepared = prepareStateCommitBatch(reset); store.loseAck()
    await expect(store.kv.commitBatch(writer, prepared)).rejects.toMatchObject({ code: 'STATE_TX_FAILED' })
    store.expire(); const replacementOwner = await store.acquire('recovery-reset', '2')
    await expect(store.kv.commitBatch(replacementOwner, prepared)).resolves.toEqual(receipt(reset, prepared))
    expect(store.rows.get(rowKey(KV.graphSnapshot, 'current'))?.version).toBe('1')
    const obsolete = input({ identity: { ...input().identity, logical_delta_id: 'uncommitted' }, checkpoint: { ...input().checkpoint, logical_delta_id: 'uncommitted' } })
    await expect(store.kv.commitBatch(replacementOwner, prepareStateCommitBatch(obsolete))).rejects.toMatchObject({ code: 'STATE_TX_GENERATION_STALE' })
    reset.operations[0] = { type: 'set', scope: KV.graphSnapshot, key: 'current', expected_version: '0', value: { resetAt: 'changed' } }
    await expect(store.kv.commitBatch(replacementOwner, prepareStateCommitBatch(reset))).rejects.toMatchObject({ code: 'STATE_TX_REPLAY_CONFLICT' })
  })
  it('enforces transport bytes/operation bounds with one explicit indivisible record exception', async () => {
    const body = input({ operations: [{ type: 'set', scope: KV.graphNodes, key: 'big', expected_version: '0', value: 'x'.repeat(STATE_COMMIT_TARGET_BYTES) }] })
    const { kv, trigger } = createKV(({ payload }) => { const prepared = payload as StatePreparedBatch; return receipt(JSON.parse(prepared.payload_json), prepared) })
    await expect(kv.commitBatch(writer, prepareStateCommitBatch(body))).rejects.toMatchObject({ code: 'STATE_TX_LIMIT_EXCEEDED' }); expect(trigger).not.toHaveBeenCalled()
    await expect(kv.commitBatch(writer, prepareStateCommitBatch({ ...body, allow_oversized_record: true }))).resolves.toMatchObject({ checkpoint_version: '1' })
    const operations = Array.from({ length: MAX_STATE_COMMIT_OPERATIONS }, (_, index) => ({ type: 'check' as const, scope: KV.graphNodes, key: String(index), expected_version: '0' }))
    expect(() => prepareStateCommitBatch(input({ operations }))).not.toThrow()
    expect(() => prepareStateCommitBatch(input({ operations: [...operations, { ...operations[0], key: 'overflow' }] }))).toThrow('STATE_TX_INVALID_REQUEST')
    expect(() => prepareStateCommitBatch(input({ allow_oversized_record: true, operations: [...input().operations, { type: 'delete', scope: KV.graphNodes, key: 'other', expected_version: '0' }] }))).toThrow('STATE_TX_INVALID_REQUEST')
  })
  it('rejects malformed wire responses, prepared bytes and unsafe numeric counters', async () => {
    for (const value of [{ exists: true, value: null, version: 9007199254740993 }, { exists: false, value: 'wrong', version: '0' }, { exists: true, value: {}, version: '0' }]) await expect(createKV(() => value).kv.getVersioned('scope', 'key')).rejects.toMatchObject({ code: 'STATE_TX_INVALID_RESPONSE' })
    const prepared = prepareStateCommitBatch(input())
    for (const altered of [{ ...receipt(input(), prepared), checkpoint_version: '2' }, { ...receipt(input(), prepared), identity: null }, { ...receipt(input(), prepared), row_versions: [] }]) await expect(createKV(() => altered).kv.commitBatch(writer, prepared)).rejects.toMatchObject({ code: 'STATE_TX_INVALID_RESPONSE' })
    await expect(createKV(() => ({ ...writer, fence: '2', expires_at_ms: 1 })).kv.lease({ action: 'renew', ...writer, ttl_ms: 1 })).rejects.toMatchObject({ code: 'STATE_TX_INVALID_RESPONSE' })
    await expect(createKV(() => null).kv.commitBatch(writer, { ...prepared, payload_digest: '0'.repeat(64) })).rejects.toMatchObject({ code: 'STATE_TX_INVALID_REQUEST' })
  })
  it('reports unsupported native capability without plain/unpaged fallback and sanitizes causes', async () => {
    const cause = Object.assign(new Error('private credentials'), { code: 'FUNCTION_NOT_FOUND' })
    const { kv, trigger } = createKV(() => { throw cause })
    for (const run of [() => kv.getVersioned('scope', 'key'), () => kv.lease({ action: 'acquire', owner_id: 'a', generation: '1', ttl_ms: 1 }), () => kv.commitBatch(writer, prepareStateCommitBatch(input()))]) await expect(run()).rejects.toMatchObject({ code: 'STATE_TX_UNSUPPORTED', cause, message: expect.not.stringContaining('private') })
    expect(trigger.mock.calls.map(([request]) => request.function_id)).toEqual(['state::get_versioned', 'state::lease', 'state::commit_batch'])
  })
})
