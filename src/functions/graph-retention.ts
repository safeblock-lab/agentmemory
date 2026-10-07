import { randomUUID } from 'node:crypto'
import type { StateKV } from '../state/kv.js'
import { KV } from '../state/schema.js'
import { StatePageError } from '../state/state-pages.js'
import { StateTransactionError, type StateGraphCheckpoint, type StateGraphGuard } from '../state/state-transactions.js'
import type { GraphControlState, GraphExtractionJob } from '../types.js'
import { logger } from '../logger.js'

const CURSOR_KEY = 'graph-terminal-retention-page'
const running = new WeakSet<StateKV>()
const busy = ['STATE_TX_LEASE_BUSY', 'STATE_TX_FENCED', 'STATE_TX_GENERATION_STALE', 'STATE_GRAPH_RECOVERY_REQUIRED']

export async function pruneTerminalGraphWork(kv: StateKV): Promise<{ deleted: number; pending: boolean }> {
  if (running.has(kv)) return { deleted: 0, pending: true }
  running.add(kv)
  let guard: StateGraphGuard | undefined
  try {
    const control = (await kv.getVersioned<GraphControlState>(KV.graphControl, 'current')).value
    if (!control || control.recovery) return { deleted: 0, pending: true }
    const lease = await kv.lease({ action: 'acquire', owner_id: randomUUID(), generation: control.generation, ttl_ms: 120_000 })
    if ('released' in lease) throw new StateTransactionError('STATE_TX_INVALID_RESPONSE')
    guard = { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }
    const saved = await kv.get<{ cursor: string | null }>(KV.config, CURSOR_KEY)
    let page
    try {
      page = (await kv.pages<{ id: string }>(KV.graphJobs, { limit: 8, fields: ['id'], ...(saved?.cursor ? { cursor: saved.cursor } : {}) }).next()).value
    } catch (error) {
      if (!(error instanceof StatePageError) || !['STATE_PAGE_CURSOR_STALE', 'STATE_PAGE_CURSOR_INVALID'].includes(error.code)) throw error
      await kv.set(KV.config, CURSOR_KEY, { cursor: null })
      return { deleted: 0, pending: true }
    }
    if (!page) return { deleted: 0, pending: false }
    let deleted = 0
    let pending = false
    for (const { id } of page.items) {
      const job = (await kv.getVersioned<GraphExtractionJob & { recoveryStopped?: boolean }>(KV.graphJobs, id, guard)).value
      if (!job || (!['completed', 'invalidated'].includes(job.state) && !job.recoveryStopped)) continue
      const checkpoint = await kv.getVersioned<StateGraphCheckpoint>(KV.graphCheckpoints, id, guard)
      if (!checkpoint.value || checkpoint.value.visibility !== 'complete') continue
      try {
        const result = await kv.pruneTerminalGraphJob({ guard, job_id: id, expected_checkpoint_version: checkpoint.version, limit: 256, max_bytes: 4 * 1024 * 1024 })
        deleted += result.deleted_count
        pending ||= !result.done
      } catch (error) {
        if (!(error instanceof StateTransactionError) || !['STATE_TX_CHECKPOINT_CONFLICT', 'STATE_TX_INVALID_REQUEST', 'STATE_GRAPH_RECOVERY_REQUIRED'].includes(error.code)) throw error
        logger.warn('Terminal graph retention eligibility rejected', { jobId: id, code: error.code })
      }
    }
    if (!pending) await kv.set(KV.config, CURSOR_KEY, { cursor: page.next_cursor })
    return { deleted, pending: pending || page.next_cursor !== null }
  } catch (error) {
    if (error instanceof StateTransactionError && busy.includes(error.code)) return { deleted: 0, pending: true }
    throw error
  } finally {
    try {
      if (guard) await kv.lease({ action: 'release', ...guard })
    } catch (error) {
      if (!(error instanceof StateTransactionError) || !busy.includes(error.code)) throw error
    } finally { running.delete(kv) }
  }
}
