import type { IIIClient } from 'iii-sdk'
import {
  commitStateBatch, getVersionedState, requestStateLease, pruneStateWork,
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

export class StateKV {
  constructor(private sdk: IIIClient) {}

  indexedRetrieval = process.env.AGENTMEMORY_RETRIEVAL_MODE !== 'legacy';

  retrieval<T>(payload: Record<string, unknown>): Promise<T> {
    return this.sdk.trigger<Record<string, unknown>, T>({ function_id: 'state::retrieval', payload });
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
    return this.sdk.trigger<{ scope: string; key: string }, T | null>({
      function_id: 'state::get',
      payload: { scope, key },
    })
  }

  async set<T = unknown>(scope: string, key: string, value: T): Promise<T> {
    return this.sdk.trigger<{ scope: string; key: string; value: T }, T>({
      function_id: 'state::set',
      payload: { scope, key, value },
    })
  }

  async update<T = unknown>(
    scope: string,
    key: string,
    ops: Array<{ type: string; path: string; value?: unknown }>,
  ): Promise<T> {
    return this.sdk.trigger<
      { scope: string; key: string; ops: Array<{ type: string; path: string; value?: unknown }> },
      T
    >({
      function_id: 'state::update',
      payload: { scope, key, ops },
    })
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

  async *values<T = unknown>(scope: string, options?: StatePageOptions): AsyncGenerator<T> {
    for await (const page of this.pages<T>(scope, options)) {
      for (const value of page.items) yield value
    }
  }
}
