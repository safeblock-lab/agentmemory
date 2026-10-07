import { describe, expect, it, vi } from 'vitest'
import { pruneStateWork, StateTransactionError } from '../src/state/state-transactions.js'

const guard = { owner_id: 'maintenance', generation: '2', fence: '3' }
const job = { guard, job_id: 'job', expected_checkpoint_version: '7', limit: 2, max_bytes: 4096 }
const page = { deleted_count: 2, deleted_bytes: 200, cursor: { scope_index: 0, after_key: 'key' }, done: false }

describe('bounded native retention transport', () => {
  it('passes a pinned checkpoint and continuation unchanged for restart', async () => {
    const trigger = vi.fn().mockResolvedValue(page)
    expect(await pruneStateWork(trigger, job)).toEqual(page)
    expect(trigger).toHaveBeenCalledWith('state::graph_prune_terminal', job)
    const resumed = { ...job, cursor: page.cursor }
    await pruneStateWork(trigger, resumed)
    expect(trigger).toHaveBeenLastCalledWith('state::graph_prune_terminal', resumed)
  })

  it('keeps audit pruning on its separate endpoint with explicit frozen key ceiling', async () => {
    const trigger = vi.fn().mockResolvedValue({ deleted_count: 0, deleted_bytes: 0, cursor: null, done: true })
    const request = { guard, through_key: 'last-historical-key', limit: 128, max_bytes: 4096 }
    await pruneStateWork(trigger, request)
    expect(trigger).toHaveBeenCalledWith('state::audit_prune_history', request)
  })

  it.each([{ limit: 0 }, { limit: 257 }, { max_bytes: 4 * 1024 * 1024 + 1 }, { expected_checkpoint_version: '0' }, { cursor: { scope_index: 7, after_key: '' } }])('rejects invalid budget or checkpoint before dispatch: %j', async (invalid) => {
    const trigger = vi.fn()
    await expect(pruneStateWork(trigger, { ...job, ...invalid })).rejects.toMatchObject({ code: 'STATE_TX_INVALID_REQUEST' })
    expect(trigger).not.toHaveBeenCalled()
  })

  it('rejects false completion and propagates native recovery refusal', async () => {
    await expect(pruneStateWork(vi.fn().mockResolvedValue({ ...page, done: true }), job)).rejects.toMatchObject({ code: 'STATE_TX_INVALID_RESPONSE' })
    await expect(pruneStateWork(vi.fn().mockRejectedValue(new StateTransactionError('STATE_GRAPH_RECOVERY_REQUIRED')), job)).rejects.toMatchObject({ code: 'STATE_GRAPH_RECOVERY_REQUIRED' })
  })
})
