import { describe, expect, it, vi } from 'vitest'
import { immutablePage } from '../src/functions/consolidation-manifest.js'
import { StateKV } from '../src/state/kv.js'

type StateRequest = {
  function_id: string
  payload: Record<string, unknown> & { scope?: string; key?: string; value?: unknown }
  timeoutMs?: number
}

function createKV(handler: (request: StateRequest) => unknown | Promise<unknown>) {
  const trigger = vi.fn((request: StateRequest) => Promise.resolve(handler(request)))
  return { kv: new StateKV({ trigger } as never), trigger }
}

describe('StateKV missing-value contract', () => {
  it('normalizes the iii state::get missing result to null with the requested scope and key', async () => {
    const { kv, trigger } = createKV(() => undefined)

    await expect(kv.get('mem:state', 'missing-key')).resolves.toBeNull()
    expect(trigger).toHaveBeenCalledExactlyOnceWith({
      function_id: 'state::get',
      payload: { scope: 'mem:state', key: 'missing-key' },
    })
  })

  it.each([
    ['false', false],
    ['zero', 0],
    ['empty string', ''],
    ['object', { present: true }],
    ['explicit null', null],
  ])('preserves a stored %s result', async (_name, stored) => {
    const { kv } = createKV(() => stored)

    await expect(kv.get('mem:state', 'stored-key')).resolves.toEqual(stored)
  })

  it('allows immutable page first-write and exact replay after a JSON state round trip, while rejecting collisions', async () => {
    const values = new Map<string, unknown>()
    const { kv, trigger } = createKV(({ function_id, payload }) => {
      const id = `${payload.scope}\0${payload.key}`
      if (function_id === 'state::get') {
        if (!values.has(id)) return undefined
        return JSON.parse(JSON.stringify(values.get(id)))
      }
      if (function_id === 'state::set') {
        const value = JSON.parse(JSON.stringify(payload.value))
        values.set(id, value)
        return value
      }
      throw new Error(`unexpected state function: ${function_id}`)
    })
    const page = { schema: 1, sources: ['session-a', 'summary-b'] }

    await expect(immutablePage(kv, 'page-key', page)).resolves.toBeUndefined()
    await expect(immutablePage(kv, 'page-key', { ...page })).resolves.toBeUndefined()
    await expect(immutablePage(kv, 'page-key', { ...page, sources: ['session-c'] }))
      .rejects.toThrow('consolidation_immutable_page_conflict')

    expect(trigger.mock.calls.filter(([request]) => request.function_id === 'state::set')).toHaveLength(1)
    expect(values.get('mem:state\0page-key')).toEqual(page)
  })

  it('forwards an operation-specific retrieval timeout without changing the retrieval payload', async () => {
    const { kv, trigger } = createKV(() => ({ status: 'ready' }))

    await expect(kv.retrieval({ action: 'viewer_recent_prepare' }, { timeoutMs: 70_000 }))
      .resolves.toEqual({ status: 'ready' })
    expect(trigger).toHaveBeenCalledExactlyOnceWith({
      function_id: 'state::retrieval',
      payload: { action: 'viewer_recent_prepare' },
      timeoutMs: 70_000,
    })
  })

  it('keeps ordinary retrievals on the SDK default timeout', async () => {
    const { kv, trigger } = createKV(() => ({ status: 'ready' }))

    await kv.retrieval({ action: 'preparation_status' })

    expect(trigger).toHaveBeenCalledExactlyOnceWith({
      function_id: 'state::retrieval',
      payload: { action: 'preparation_status' },
    })
  })

  it.each([0, -1, 1.5, Number.NaN])('rejects an invalid retrieval timeout %s', timeoutMs => {
    const { kv } = createKV(() => ({ status: 'ready' }))

    expect(() => kv.retrieval({ action: 'viewer_recent_prepare' }, { timeoutMs }))
      .toThrow('state_retrieval_invalid_timeout')
  })
})
