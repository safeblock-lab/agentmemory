import { randomUUID } from 'node:crypto'
import type { StateKV } from '../state/kv.js'
import { KV } from '../state/schema.js'
import { StatePageError } from '../state/state-pages.js'
import { StateTransactionError, type StateGraphCheckpoint, type StateGraphGuard, type StatePruneCursor } from '../state/state-transactions.js'
import type { GraphControlState, GraphExtractionJob } from '../types.js'
import { logger } from '../logger.js'
import { hasRetentionProtection, retentionEligibility, terminalRetentionOrigin } from './storage-retention.js'

const CURSOR_KEY = 'graph-terminal-retention-page'
const running = new WeakSet<StateKV>()
const busy = ['STATE_TX_LEASE_BUSY', 'STATE_TX_FENCED', 'STATE_TX_GENERATION_STALE', 'STATE_GRAPH_RECOVERY_REQUIRED']
const PROTECTED_GRAPH_JOBS = new Set(['graphjob_muzu6a4k_c7aa10a891eb', 'graphjob_mv0567t0_7c392765f08d'])
interface RetentionProgress {
  cursor: string | null
  pendingJob?: { id: string; checkpointVersion: string; cursor: StatePruneCursor | null }
}

export async function pruneTerminalGraphWork(kv: StateKV): Promise<{
  deleted: number; logicalBytes: number; storedBytes: number; pending: boolean
}> {
  if (running.has(kv)) return { deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true }
  running.add(kv)
  let guard: StateGraphGuard | undefined
  try {
    const maintenance = await kv.get<{ cleanupActivatedAt?: string }>(KV.storageMaintenance, 'current')
    if (!maintenance?.cleanupActivatedAt) return { deleted: 0, logicalBytes: 0, storedBytes: 0, pending: false }
    const control = (await kv.getVersioned<GraphControlState>(KV.graphControl, 'current')).value
    if (!control || control.recovery) return { deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true }
    const lease = await kv.lease({ action: 'acquire', owner_id: randomUUID(), generation: control.generation, ttl_ms: 120_000 })
    if ('released' in lease) throw new StateTransactionError('STATE_TX_INVALID_RESPONSE')
    guard = { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }
    const saved = await kv.get<RetentionProgress>(KV.config, CURSOR_KEY)
    let page
    try {
      page = saved?.pendingJob
        ? { items: [{ id: saved.pendingJob.id }], next_cursor: saved.cursor }
        : (await kv.pages<{ id: string }>(KV.graphJobs, { limit: 8, fields: ['id'], ...(saved?.cursor ? { cursor: saved.cursor } : {}) }).next()).value
    } catch (error) {
      if (!(error instanceof StatePageError) || !['STATE_PAGE_CURSOR_STALE', 'STATE_PAGE_CURSOR_INVALID'].includes(error.code)) throw error
      await kv.set(KV.config, CURSOR_KEY, { cursor: null })
      return { deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true }
    }
    if (!page) return { deleted: 0, logicalBytes: 0, storedBytes: 0, pending: false }
    let deleted = 0
    let logicalBytes = 0
    let storedBytes = 0
    let pending = false
    let pendingJob: RetentionProgress['pendingJob']
    for (const { id } of page.items) {
      if (PROTECTED_GRAPH_JOBS.has(id)) continue
      const job = (await kv.getVersioned<GraphExtractionJob & { recoveryStopped?: boolean }>(KV.graphJobs, id, guard)).value
      if (!job || !['completed', 'invalidated'].includes(job.state)) continue
      if (!retentionEligibility('completedBodies', {
        origin: terminalRetentionOrigin({ ...job }), protected: hasRetentionProtection({ ...job }), dependenciesClosed: true,
      }).eligible) continue
      const checkpoint = await kv.getVersioned<StateGraphCheckpoint>(KV.graphCheckpoints, id, guard)
      if (!checkpoint.value || checkpoint.value.visibility !== 'complete') continue
      if (saved?.pendingJob?.id === id && saved.pendingJob.checkpointVersion !== checkpoint.version) {
        pending = true
        continue
      }
      try {
        const result = await kv.pruneTerminalGraphJob({ guard, job_id: id, expected_checkpoint_version: checkpoint.version,
          limit: 256, max_bytes: 4 * 1024 * 1024,
          ...(saved?.pendingJob?.id === id && saved.pendingJob.cursor ? { cursor: saved.pendingJob.cursor } : {}) })
        deleted += result.deleted_count
        logicalBytes += result.deleted_logical_bytes ?? 0
        storedBytes += result.deleted_stored_bytes ?? result.deleted_bytes
        pending ||= !result.done
        if (!result.done && !pendingJob) pendingJob = { id, checkpointVersion: checkpoint.version, cursor: result.cursor }
      } catch (error) {
        if (!(error instanceof StateTransactionError) || !['STATE_TX_CHECKPOINT_CONFLICT', 'STATE_TX_INVALID_REQUEST', 'STATE_GRAPH_RECOVERY_REQUIRED'].includes(error.code)) throw error
        pending = true
        logger.warn('Terminal graph retention eligibility rejected', { jobId: id, code: error.code })
      }
    }
    // Terminal jobs remain the durable due-work index; incomplete cuts return on the next scan cycle.
    await kv.set(KV.config, CURSOR_KEY, { cursor: page.next_cursor, ...(pendingJob ? { pendingJob } : {}) })
    return { deleted, logicalBytes, storedBytes, pending: pending || page.next_cursor !== null }
  } catch (error) {
    if (error instanceof StateTransactionError && busy.includes(error.code)) return { deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true }
    throw error
  } finally {
    try {
      if (guard) await kv.lease({ action: 'release', ...guard })
    } catch (error) {
      if (!(error instanceof StateTransactionError) || !busy.includes(error.code)) throw error
    } finally { running.delete(kv) }
  }
}
