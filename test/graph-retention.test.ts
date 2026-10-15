import { describe, expect, it, vi } from 'vitest'
import { pruneTerminalGraphWork } from '../src/functions/graph-retention.js'
import type { StateKV } from '../src/state/kv.js'
import { KV } from '../src/state/schema.js'
import { StatePageError } from '../src/state/state-pages.js'
import { StateTransactionError } from '../src/state/state-transactions.js'

function harness() {
  let remaining = 600
  let saved: { cursor: string | null } | null = null
  const lease = { owner_id: 'fresh', generation: '2', fence: '8', expires_at_ms: Date.now() + 120_000 }
  const kv = {
    getVersioned: vi.fn(async (scope: string) => scope === KV.graphControl
      ? { value: { generation: '2', recovery: null } }
      : scope === KV.graphJobs ? { value: { id: 'job', generation: '1', state: 'completed' } }
      : { value: { visibility: 'complete' }, version: '7' }),
    get: vi.fn(async (scope: string) => scope === KV.storageMaintenance ? { cleanupActivatedAt: '2026-01-01' } : saved),
    set: vi.fn(async (_scope: string, _key: string, value: { cursor: string | null }) => { saved = value }),
    lease: vi.fn(async (request: { action: string }) => request.action === 'release' ? { released: true } : lease),
    pages: vi.fn(async function* () { yield { items: [{ id: 'job' }], next_cursor: 'next-page' } }),
    pruneTerminalGraphJob: vi.fn(async (_request: { cursor?: { scope_index: number; after_key: string } }) => {
      const count = Math.min(256, remaining)
      remaining -= count
      return { deleted_count: count, deleted_bytes: count * 10, done: remaining === 0, cursor: remaining ? { scope_index: 0, after_key: 'deleted' } : null }
    }),
  }
  return { kv, run: () => pruneTerminalGraphWork(kv as unknown as StateKV), getRemaining: () => remaining }
}

describe('terminal retention continuation', () => {
  it('drains more than one page across ticks and process-state loss with a fresh lease', async () => {
    const h = harness()
    expect(await h.run()).toEqual({ deleted: 256, logicalBytes: 0, storedBytes: 2560, pending: true })
    expect(h.kv.set).toHaveBeenCalledWith(KV.config, 'graph-terminal-retention-page', { cursor: 'next-page', pendingJob: { id: 'job', checkpointVersion: '7', cursor: { scope_index: 0, after_key: 'deleted' } } })
    expect(await h.run()).toEqual({ deleted: 256, logicalBytes: 0, storedBytes: 2560, pending: true })
    expect(await h.run()).toEqual({ deleted: 88, logicalBytes: 0, storedBytes: 880, pending: true })
    expect(h.getRemaining()).toBe(0)
    expect(h.kv.set).toHaveBeenCalledWith(KV.config, 'graph-terminal-retention-page', { cursor: 'next-page' })
    expect(h.kv.pruneTerminalGraphJob).toHaveBeenCalledWith(expect.objectContaining({ guard: { owner_id: 'fresh', generation: '2', fence: '8' }, expected_checkpoint_version: '7', limit: 256 }))
    expect(h.kv.pruneTerminalGraphJob.mock.calls[1][0]).toMatchObject({ cursor: { scope_index: 0, after_key: 'deleted' } })
    expect(h.kv.pages).toHaveBeenCalledTimes(1)
    expect(h.kv.lease.mock.calls.filter(([r]) => r.action === 'release')).toHaveLength(3)
    expect(h.kv.getVersioned.mock.calls.some(([scope]) => scope === KV.graphJobs)).toBe(true)
  })

  it('preserves resumable jobs and recovery barriers without dispatching maintenance', async () => {
    const h = harness()
    h.kv.getVersioned.mockImplementation(async (scope) => scope === KV.graphControl ? { value: { generation: '2', recovery: null } } : { value: { state: 'applying' } } as never)
    expect(await h.run()).toEqual({ deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true })
    expect(h.kv.pruneTerminalGraphJob).not.toHaveBeenCalled()
    h.kv.getVersioned.mockResolvedValue({ value: { generation: '2', recovery: { job_id: 'job' } } } as never)
    await h.run()
    expect(h.kv.lease).toHaveBeenCalledTimes(2)
  })

  it('resets only the maintenance scan cursor after engine restart invalidates pagination', async () => {
    const h = harness()
    h.kv.pages.mockImplementation(async function* () { throw new StatePageError('STATE_PAGE_CURSOR_STALE') })
    expect(await h.run()).toEqual({ deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true })
    expect(h.kv.set).toHaveBeenCalledWith(KV.config, 'graph-terminal-retention-page', { cursor: null })
    expect(h.kv.pruneTerminalGraphJob).not.toHaveBeenCalled()
  })

  it('releases the lease after a failed page and retries work on the next tick', async () => {
    const h = harness()
    h.kv.pruneTerminalGraphJob.mockRejectedValueOnce(new StateTransactionError('STATE_TX_FAILED'))
    await expect(h.run()).rejects.toMatchObject({ code: 'STATE_TX_FAILED' })
    expect(h.getRemaining()).toBe(600)
    expect(h.kv.set).not.toHaveBeenCalled()
    expect(await h.run()).toEqual({ deleted: 256, logicalBytes: 0, storedBytes: 2560, pending: true })
    expect(h.kv.lease.mock.calls.filter(([r]) => r.action === 'release')).toHaveLength(2)
  })

  it('leaves a busy lease with its current writer', async () => {
    const h = harness()
    h.kv.lease.mockRejectedValueOnce(new StateTransactionError('STATE_TX_LEASE_BUSY'))
    expect(await h.run()).toEqual({ deleted: 0, logicalBytes: 0, storedBytes: 0, pending: true })
    expect(h.kv.pruneTerminalGraphJob).not.toHaveBeenCalled()
    expect(h.kv.lease).toHaveBeenCalledTimes(1)
  })

  it.each(['graphjob_muzu6a4k_c7aa10a891eb', 'graphjob_mv0567t0_7c392765f08d'])('excludes protected family %s before loading or pruning it', async (id) => {
    const h = harness()
    h.kv.pages.mockImplementation(async function* () { yield { items: [{ id }], next_cursor: null } } as never)
    expect(await h.run()).toEqual({ deleted: 0, logicalBytes: 0, storedBytes: 0, pending: false })
    expect(h.kv.pruneTerminalGraphJob).not.toHaveBeenCalled()
    expect(h.kv.getVersioned.mock.calls.some(([scope]) => scope === KV.graphJobs)).toBe(false)
  })

  it.each(['failed', 'captured', 'unknown', 'applying'])('keeps %s jobs even with a complete-looking checkpoint', async (state) => {
    const h = harness()
    h.kv.getVersioned.mockImplementation(async (scope) => scope === KV.graphControl
      ? { value: { generation: '2', recovery: null } }
      : scope === KV.graphJobs ? { value: { id: 'job', state, recoveryStopped: true } }
      : { value: { visibility: 'complete' }, version: '7' } as never)
    await h.run()
    expect(h.kv.pruneTerminalGraphJob).not.toHaveBeenCalled()
  })

  it('drops only a stale continuation when checkpoint authority changes', async () => {
    const h = harness()
    await h.run()
    h.kv.pruneTerminalGraphJob.mockClear()
    h.kv.getVersioned.mockImplementation(async (scope) => scope === KV.graphControl
      ? { value: { generation: '2', recovery: null } }
      : scope === KV.graphJobs ? { value: { id: 'job', state: 'completed' } }
      : { value: { visibility: 'complete' }, version: '8' } as never)
    expect(await h.run()).toMatchObject({ deleted: 0, pending: true })
    expect(h.kv.pruneTerminalGraphJob).not.toHaveBeenCalled()
    expect(h.kv.set).toHaveBeenLastCalledWith(KV.config, 'graph-terminal-retention-page', { cursor: 'next-page' })
  })

  it('retains native recovery refusals without advancing a job cursor', async () => {
    const h = harness()
    h.kv.pruneTerminalGraphJob.mockRejectedValueOnce(new StateTransactionError('STATE_GRAPH_RECOVERY_REQUIRED'))
    expect(await h.run()).toMatchObject({ deleted: 0, pending: true })
    expect(h.getRemaining()).toBe(600)
    expect(h.kv.set).toHaveBeenLastCalledWith(KV.config, 'graph-terminal-retention-page', { cursor: 'next-page' })
  })
})
