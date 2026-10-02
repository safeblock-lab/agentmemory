import { describe, expect, it, vi } from 'vitest'
import { StateKV } from '../src/state/kv.js'

type WireRequest = {
  function_id: string
  payload: { scope: string; cursor?: string; limit: number; max_bytes: number }
}

function page(items: unknown[], next_cursor: string | null) {
  return { items, next_cursor }
}

function createKV(handler: (request: WireRequest) => unknown | Promise<unknown>) {
  const trigger = vi.fn((request: WireRequest) => Promise.resolve(handler(request)))
  const kv = new StateKV({ trigger } as never)
  return { kv, trigger }
}

describe('StateKV paged listing', () => {
  it('yields a page before requesting the next one', async () => {
    const { kv, trigger } = createKV((request) =>
      request.payload.cursor ? page(['second'], null) : page(['first'], 'cursor-1'),
    )
    const pages = kv.pages<string>('scope')

    await expect(pages.next()).resolves.toMatchObject({ value: page(['first'], 'cursor-1'), done: false })
    expect(trigger).toHaveBeenCalledTimes(1)
    await expect(pages.next()).resolves.toMatchObject({ value: page(['second'], null), done: false })
    expect(trigger).toHaveBeenCalledTimes(2)
    expect(trigger.mock.calls[1][0].payload.cursor).toBe('cursor-1')
  })

  it('yields values before requesting the next page', async () => {
    const { kv, trigger } = createKV((request) =>
      request.payload.cursor ? page(['second'], null) : page(['first'], 'cursor-1'),
    )
    const values = kv.values<string>('scope')

    await expect(values.next()).resolves.toEqual({ value: 'first', done: false })
    expect(trigger).toHaveBeenCalledTimes(1)
    await expect(values.next()).resolves.toEqual({ value: 'second', done: false })
    expect(trigger).toHaveBeenCalledTimes(2)
  })

  it('preserves list array results while collecting pages', async () => {
    const { kv, trigger } = createKV((request) =>
      request.payload.cursor ? page(['three'], null) : page(['one', 'two'], 'cursor-1'),
    )

    await expect(kv.list<string>('scope')).resolves.toEqual(['one', 'two', 'three'])
    expect(trigger).toHaveBeenCalledTimes(2)
    expect(trigger.mock.calls[0][0]).toEqual({
      function_id: 'state::list_page',
      payload: { scope: 'scope', limit: 256, max_bytes: 1_048_576 },
    })
  })

  it('does not restart a values iterator after it has yielded', async () => {
    const { kv, trigger } = createKV(vi
      .fn()
      .mockResolvedValueOnce(page(['already-yielded'], 'cursor-1'))
      .mockRejectedValueOnce({ code: 'STATE_PAGE_CURSOR_STALE', message: 'stale' }))
    const values = kv.values<string>('scope')

    await expect(values.next()).resolves.toEqual({ value: 'already-yielded', done: false })
    await expect(values.next()).rejects.toMatchObject({ code: 'STATE_PAGE_CURSOR_STALE' })
    expect(trigger).toHaveBeenCalledTimes(2)
  })

  it('restarts list from the beginning and discards partial values on a stale cursor', async () => {
    const { kv, trigger } = createKV((request) => {
      if (!request.payload.cursor) {
        const initialCalls = trigger.mock.calls.filter(([call]) => !call.payload.cursor).length
        return initialCalls === 1 ? page(['discard-me'], 'stale-cursor') : page(['fresh-one'], 'fresh-cursor')
      }
      if (request.payload.cursor === 'stale-cursor') {
        throw Object.assign(new Error('stale'), { code: 'STATE_PAGE_CURSOR_STALE' })
      }
      return page(['fresh-two'], null)
    })

    await expect(kv.list<string>('scope')).resolves.toEqual(['fresh-one', 'fresh-two'])
    expect(trigger).toHaveBeenCalledTimes(4)
    expect(trigger.mock.calls[2][0].payload.cursor).toBeUndefined()
  })

  it('bounds stale-cursor restarts', async () => {
    const { kv, trigger } = createKV((request) => {
      if (!request.payload.cursor) return page(['partial'], 'stale-cursor')
      throw Object.assign(new Error('stale'), { code: 'STATE_PAGE_CURSOR_STALE' })
    })

    await expect(kv.list('scope')).rejects.toMatchObject({ code: 'STATE_PAGE_CURSOR_STALE' })
    expect(trigger).toHaveBeenCalledTimes(8)
  })

  it('rejects malformed pages, count overflow, and cursor cycles', async () => {
    const malformed = createKV(() => ({ items: [], unexpected: true }))
    await expect(malformed.kv.pages('scope').next()).rejects.toMatchObject({ code: 'STATE_PAGE_INVALID_RESPONSE' })

    const tooMany = createKV(() => page(['one', 'two'], null))
    await expect(tooMany.kv.pages('scope', { limit: 1 }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_RESPONSE',
    })

    const cycle = createKV((request) =>
      request.payload.cursor === 'cursor-a'
        ? page(['b'], 'cursor-b')
        : request.payload.cursor === 'cursor-b'
          ? page(['a'], 'cursor-a')
          : page(['start'], 'cursor-a'),
    )
    await expect(cycle.kv.list('scope')).rejects.toMatchObject({ code: 'STATE_PAGE_CURSOR_INVALID' })
    expect(cycle.trigger).toHaveBeenCalledTimes(3)
  })

  it('enforces the response byte budget', async () => {
    const { kv } = createKV(() => page(['x'.repeat(1_100)], null))
    await expect(kv.pages('scope', { maxBytes: 1_024 }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_RESPONSE',
    })
  })

  it('maps an oversized saved record without exposing error details', async () => {
    const { kv } = createKV(() => {
      throw Object.assign(new Error('record contents: private payload'), { code: 'STATE_RECORD_TOO_LARGE' })
    })

    await expect(kv.pages('scope').next()).rejects.toMatchObject({
      code: 'STATE_RECORD_TOO_LARGE',
      message: expect.not.stringContaining('private payload'),
    })
  })

  it('preserves trigger causes while exposing stable sanitized messages', async () => {
    const cases = [
      {
        cause: Object.assign(new Error('private vector payload'), { code: 'STATE_RECORD_TOO_LARGE' }),
        code: 'STATE_RECORD_TOO_LARGE',
        message: 'STATE_RECORD_TOO_LARGE: A state record exceeds the page byte budget',
      },
      {
        cause: Object.assign(new Error('private engine details'), { code: 'FUNCTION_NOT_FOUND' }),
        code: 'STATE_PAGE_UNSUPPORTED',
        message: 'STATE_PAGE_UNSUPPORTED: The configured iii-engine does not support state pagination',
      },
      {
        cause: new Error('private transport details'),
        code: 'STATE_PAGE_FAILED',
        message: 'STATE_PAGE_FAILED: The state page request failed',
      },
    ]

    for (const expected of cases) {
      const { kv } = createKV(() => {
        throw expected.cause
      })
      const rejection = await kv.pages('scope').next().then(
        () => undefined,
        (error: unknown) => error,
      )

      expect(rejection).toMatchObject({ code: expected.code, message: expected.message })
      expect((rejection as Error).cause).toBe(expected.cause)
      expect((rejection as Error).message).not.toContain('private')
    }
  })

  it('rejects invalid request bounds before calling the engine', async () => {
    const { kv, trigger } = createKV(() => page([], null))

    await expect(kv.pages('scope', { limit: 0 }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_REQUEST',
    })
    await expect(kv.pages('scope', { maxBytes: 1_023 }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_REQUEST',
    })
    await expect(kv.pages('scope', { cursor: 'x'.repeat(4_097) }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_REQUEST',
    })
    expect(trigger).not.toHaveBeenCalled()
  })

  it('accepts the documented upper request bounds', async () => {
    const { kv, trigger } = createKV(() => page([], null))

    await expect(kv.pages('scope', { limit: 1_024, maxBytes: 1_048_576 }).next()).resolves.toMatchObject({
      done: false,
    })
    expect(trigger.mock.calls[0][0].payload).toMatchObject({ limit: 1_024, max_bytes: 1_048_576 })
    await expect(kv.pages('scope', { limit: 1_025 }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_REQUEST',
    })
    await expect(kv.pages('scope', { maxBytes: 1_048_577 }).next()).rejects.toMatchObject({
      code: 'STATE_PAGE_INVALID_REQUEST',
    })
    expect(trigger).toHaveBeenCalledTimes(1)
  })

  it('reports unsupported pagination on older engines without falling back to state::list', async () => {
    const { kv, trigger } = createKV(() => {
      throw new Error('No function: state::list_page')
    })

    await expect(kv.list('scope')).rejects.toMatchObject({ code: 'STATE_PAGE_UNSUPPORTED' })
    expect(trigger).toHaveBeenCalledTimes(1)
    expect(trigger.mock.calls[0][0].function_id).toBe('state::list_page')
  })
})
