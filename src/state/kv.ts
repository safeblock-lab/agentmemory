import type { IIIClient } from 'iii-sdk'
import { CanonicalMemoryIndex } from './canonical-memory.js'
import { createGraphAutomaticRecoveryTransport, type GraphRecoveryCommand, type GraphAutomaticRecoveryResponse,
  type GraphTerminalRecoveryCommand, type GraphTerminalRecoveryResponse } from './graph-automatic-recovery.js'
import {
  triggerStateStorage,
  triggerSnapshotStorage,
  type GraphStorageMetadata,
  type GraphStorageMetadataPrepareOptions,
  type GraphStorageMetadataPrepareResult,
} from './storage.js'
import {
  commitStateBatch, getVersionedState, requestStateLease, pruneStateWork, StateTransactionError,
  type StateBatchReceipt, type StateGraphGuard, type StateGraphLease,
  type StateLeaseReleased, type StateLeaseRequest, type StatePreparedBatch,
  type StateTransactionTrigger, type StateVersioned, type StateGraphPruneRequest, type StateAuditPruneRequest, type StatePruneResult,
} from './state-transactions.js'
import {
  iterateStatePages,
  MAX_STALE_LIST_RESTARTS,
  StatePageError,
  type StatePage,
  type StatePageOptions,
  type StatePageRequest,
  type StateScopeRevision,
} from './state-pages.js'

export type { StatePage, StatePageOptions } from './state-pages.js'
export type { StateGraphGuard, StateGraphLease, StateLeaseRequest, StatePreparedBatch, StateBatchReceipt, StateVersioned } from './state-transactions.js'

export type StateUpdateMutationOutcome = 'not_applied' | 'applied_with_errors' | 'unknown'
export interface StateUpdateOperationError { code: string; op_index: number }
export class StateUpdateError extends Error {
  readonly name = 'StateUpdateError'
  readonly retryable = false
  constructor(
    readonly code: 'STATE_UPDATE_REJECTED' | 'STATE_UPDATE_PARTIAL_FAILURE' | 'STATE_TX_INVALID_RESPONSE',
    readonly mutationOutcome: StateUpdateMutationOutcome,
    readonly operationErrors: readonly StateUpdateOperationError[] = [],
  ) { super(code) }
}

type StateWriteListener = (event: { scope: string; key: string; value: unknown }) => void

const stateWriteListeners = new Set<StateWriteListener>()

export function subscribeStateWrites(listener: StateWriteListener): () => void {
  stateWriteListeners.add(listener)
  return () => stateWriteListeners.delete(listener)
}

function notifyStateWrite(event: { scope: string; key: string; value: unknown }): void {
  for (const listener of stateWriteListeners) {
    try {
      listener(event)
    } catch {
      try {
        console.warn('StateKV write observer failed')
      } catch {}
    }
  }
}

export class StateKV {
  constructor(private sdk: IIIClient) {}

  graphAutomaticRecovery(command: GraphTerminalRecoveryCommand): Promise<GraphTerminalRecoveryResponse>
  graphAutomaticRecovery(command: GraphRecoveryCommand): Promise<GraphAutomaticRecoveryResponse>
  graphAutomaticRecovery(command: GraphRecoveryCommand | GraphTerminalRecoveryCommand): Promise<GraphAutomaticRecoveryResponse | GraphTerminalRecoveryResponse> {
    return createGraphAutomaticRecoveryTransport(this.sdk)(command)
  }

  snapshotTransport = (request: import("./snapshot-read-view.js").SnapshotReadRequest, timeoutMs: number): Promise<unknown> =>
    triggerSnapshotStorage(this.sdk, request, timeoutMs)

  readonly canonicalMemory = new CanonicalMemoryIndex(this)

  indexedRetrieval = process.env.AGENTMEMORY_RETRIEVAL_MODE !== 'legacy';

  retrieval<T>(payload: Record<string, unknown>, options?: { timeoutMs?: number }): Promise<T> {
    const timeoutMs = options?.timeoutMs;
    if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1)) {
      throw new RangeError('state_retrieval_invalid_timeout');
    }
    return this.sdk.trigger<Record<string, unknown>, T>({
      function_id: 'state::retrieval', payload,
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    });
  }

  private transactionTrigger: StateTransactionTrigger = (functionId, payload) =>
    this.sdk.trigger<unknown, unknown>({ function_id: functionId, payload })

  getVersioned<T = unknown>(scope: string, key: string, guard?: StateGraphGuard): Promise<StateVersioned<T>> {
    return getVersionedState<T>(this.transactionTrigger, { scope, key, ...(guard === undefined ? {} : { guard }) })
  }

  lease(request: StateLeaseRequest): Promise<StateGraphLease | StateLeaseReleased> {
    return requestStateLease(this.transactionTrigger, request)
  }

  commitBatch(guard: StateGraphGuard, prepared: StatePreparedBatch): Promise<StateBatchReceipt> {
    return commitStateBatch(this.transactionTrigger, guard, prepared)
  }

  pruneTerminalGraphJob(request: StateGraphPruneRequest): Promise<StatePruneResult> {
    return pruneStateWork(this.transactionTrigger, request)
  }

  pruneHistoricalAudit(request: StateAuditPruneRequest): Promise<StatePruneResult> {
    return pruneStateWork(this.transactionTrigger, request)
  }

  async get<T = unknown>(scope: string, key: string): Promise<T | null> {
    const value = await this.sdk.trigger<{ scope: string; key: string }, T | null | undefined>({
      function_id: 'state::get',
      payload: { scope, key },
    })
    return value === undefined ? null : value
  }

  async set<T = unknown>(scope: string, key: string, value: T): Promise<T> {
    const result = await this.sdk.trigger<{ scope: string; key: string; value: T }, unknown>({
      function_id: 'state::set',
      payload: { scope, key, value },
    })
    if (typeof result !== 'object' || result === null || Array.isArray(result)
      || Object.keys(result).length !== 2 || !Object.hasOwn(result, 'old_value') || !Object.hasOwn(result, 'new_value')) {
      throw new StateTransactionError('STATE_TX_INVALID_RESPONSE')
    }
    const committedValue = (result as { new_value: T }).new_value
    notifyStateWrite({ scope, key, value: committedValue })
    return committedValue
  }

  async update<T = unknown>(
    scope: string,
    key: string,
    ops: Array<{ type: string; path: string; value?: unknown; by?: number }>,
  ): Promise<T> {
    const result = await this.sdk.trigger<
      { scope: string; key: string; ops: Array<{ type: string; path: string; value?: unknown; by?: number }> },
      unknown
    >({
      function_id: 'state::update',
      payload: { scope, key, ops },
    })
    const invalid = (): never => { throw new StateUpdateError('STATE_TX_INVALID_RESPONSE', 'unknown') }
    if (typeof result !== 'object' || result === null || Array.isArray(result)
      || !Object.hasOwn(result, 'old_value') || !Object.hasOwn(result, 'new_value')
      || Object.keys(result).some(key => !['old_value', 'new_value', 'errors'].includes(key))) invalid()
    const envelope = result as { new_value: T; errors?: unknown }
    const failures: StateUpdateOperationError[] = []
    if (Object.hasOwn(envelope, 'errors')) {
      if (!Array.isArray(envelope.errors)) return invalid()
      for (const error of envelope.errors) {
        if (typeof error !== 'object' || error === null || Array.isArray(error)
          || !Object.hasOwn(error, 'op_index') || !Number.isSafeInteger(error.op_index) || error.op_index < 0
          || !Object.hasOwn(error, 'code') || typeof error.code !== 'string' || !error.code.length
          || !Object.hasOwn(error, 'message') || typeof error.message !== 'string'
          || Object.keys(error).some(key => !['op_index', 'code', 'message', 'doc_url'].includes(key))
          || Object.hasOwn(error, 'doc_url') && typeof error.doc_url !== 'string') return invalid()
        failures.push({ code: /^[A-Za-z][A-Za-z0-9_.-]{0,95}$/.test(error.code) ? error.code : 'UNKNOWN', op_index: error.op_index })
      }
    }
    if (failures.length && (envelope.errors as Array<{ code: string }>).some(error => error.code.startsWith('append_bounded.'))) {
      throw new StateUpdateError('STATE_UPDATE_REJECTED', 'not_applied', failures)
    }
    notifyStateWrite({ scope, key, value: envelope.new_value })
    if (failures.length) throw new StateUpdateError('STATE_UPDATE_PARTIAL_FAILURE', 'applied_with_errors', failures)
    return envelope.new_value
  }

  async delete(scope: string, key: string): Promise<void> {
    return this.sdk.trigger<{ scope: string; key: string }, void>({
      function_id: 'state::delete',
      payload: { scope, key },
    })
  }

  async list<T = unknown>(scope: string): Promise<T[]> {
    for (let staleRestarts = 0; ; staleRestarts++) {
      const values: T[] = []
      try {
        for await (const value of this.values<T>(scope)) values.push(value)
        return values
      } catch (error) {
        if (!(error instanceof StatePageError) || error.code !== 'STATE_PAGE_CURSOR_STALE') throw error
        if (staleRestarts >= MAX_STALE_LIST_RESTARTS) throw error
      }
    }
  }

  pages<T = unknown>(scope: string, options?: StatePageOptions): AsyncGenerator<StatePage<T>> {
    return iterateStatePages<T>(
      (payload: StatePageRequest) =>
        this.sdk.trigger<StatePageRequest, unknown>({
          function_id: 'state::list_page',
          payload,
        }),
      scope,
      options,
    )
  }

  scopeRevision(scope: string, prefix = false): Promise<StateScopeRevision> {
    return this.sdk.trigger<{ scope: string; prefix: boolean }, StateScopeRevision>({
      function_id: 'state::scope_revision_v1',
      payload: { scope, prefix },
    })
  }

  graphStorageMetadata(): Promise<GraphStorageMetadata> {
    return triggerStateStorage<GraphStorageMetadata>(this.sdk, { action: 'graph_metadata' })
  }

  prepareGraphStorageMetadata(options: GraphStorageMetadataPrepareOptions): Promise<GraphStorageMetadataPrepareResult> {
    return triggerStateStorage<GraphStorageMetadataPrepareResult>(this.sdk, {
      action: 'graph_metadata_prepare', ...options,
    })
  }

  async *values<T = unknown>(scope: string, options?: StatePageOptions): AsyncGenerator<T> {
    for await (const page of this.pages<T>(scope, options)) {
      for (const value of page.items) yield value
    }
  }
}
