import { Buffer } from 'node:buffer'

export const DEFAULT_STATE_PAGE_LIMIT = 256
export const DEFAULT_STATE_PAGE_MAX_BYTES = 1_048_576
export const MAX_STATE_PAGE_LIMIT = 1_024
export const MAX_STATE_PAGE_BYTES = 1_048_576
export const MAX_STATE_PAGE_CURSOR_BYTES = 4_096
export const MAX_STALE_LIST_RESTARTS = 3

const MAX_TRACKED_CURSORS = 256

export type StatePageErrorCode =
  | 'STATE_RECORD_TOO_LARGE'
  | 'STATE_PAGE_CURSOR_INVALID'
  | 'STATE_PAGE_CURSOR_STALE'
  | 'STATE_PAGE_UNSUPPORTED'
  | 'STATE_PAGE_INVALID_REQUEST'
  | 'STATE_PAGE_INVALID_RESPONSE'
  | 'STATE_PAGE_FAILED'

const ERROR_MESSAGES: Record<StatePageErrorCode, string> = {
  STATE_RECORD_TOO_LARGE: 'A state record exceeds the page byte budget',
  STATE_PAGE_CURSOR_INVALID: 'The state page cursor is invalid or did not advance',
  STATE_PAGE_CURSOR_STALE: 'The state page cursor is stale; restart the scan',
  STATE_PAGE_UNSUPPORTED: 'The configured iii-engine does not support state pagination',
  STATE_PAGE_INVALID_REQUEST: 'The state page request is outside supported bounds',
  STATE_PAGE_INVALID_RESPONSE: 'The state page response does not match the pagination contract',
  STATE_PAGE_FAILED: 'The state page request failed',
}

export class StatePageError extends Error {
  constructor(readonly code: StatePageErrorCode, cause?: unknown) {
    super(`${code}: ${ERROR_MESSAGES[code]}`, { cause })
    this.name = 'StatePageError'
  }
}

export interface StatePage<T> {
  items: T[]
  next_cursor: string | null
}

export interface StatePageOptions {
  cursor?: string
  limit?: number
  maxBytes?: number
}

export interface StatePageRequest {
  scope: string
  cursor?: string
  limit: number
  max_bytes: number
}

type StatePageTrigger = (request: StatePageRequest) => Promise<unknown>

interface NormalizedOptions {
  cursor?: string
  limit: number
  maxBytes: number
}

function invalidRequest(): never {
  throw new StatePageError('STATE_PAGE_INVALID_REQUEST')
}

function normalizeOptions(options: StatePageOptions): NormalizedOptions {
  const limit = options.limit ?? DEFAULT_STATE_PAGE_LIMIT
  const maxBytes = options.maxBytes ?? DEFAULT_STATE_PAGE_MAX_BYTES
  const cursor = options.cursor

  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_STATE_PAGE_LIMIT) invalidRequest()
  if (!Number.isInteger(maxBytes) || maxBytes < 1_024 || maxBytes > MAX_STATE_PAGE_BYTES) invalidRequest()
  if (cursor !== undefined && (!cursor || Buffer.byteLength(cursor, 'utf8') > MAX_STATE_PAGE_CURSOR_BYTES)) {
    invalidRequest()
  }

  return { cursor, limit, maxBytes }
}

function getErrorCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null) return undefined
  const code = (error as { code?: unknown }).code
  if (typeof code === 'string') return code
  const message = (error as { message?: unknown }).message
  return typeof message === 'string' ? message.match(/^([A-Z][A-Z0-9_]+):/)?.[1] : undefined
}

function isUnsupportedEngine(error: unknown): boolean {
  const code = getErrorCode(error)
  if (code && ['FUNCTION_NOT_FOUND', 'NO_SUCH_FUNCTION', 'UNKNOWN_FUNCTION'].includes(code)) return true
  if (typeof error !== 'object' || error === null) return false
  const message = (error as { message?: unknown }).message
  const functionId = (error as { function_id?: unknown }).function_id
  if (code === 'UNSUPPORTED' && functionId === 'state::list_page') return true
  const missingFunction =
    typeof message === 'string' && /(?:no function|function not found|unknown function)/i.test(message)
  return missingFunction && (functionId === 'state::list_page' || message.includes('state::list_page'))
}

function normalizeTriggerError(error: unknown): StatePageError {
  if (isUnsupportedEngine(error)) return new StatePageError('STATE_PAGE_UNSUPPORTED', error)

  const code = getErrorCode(error)
  if (
    code === 'STATE_RECORD_TOO_LARGE' ||
    code === 'STATE_PAGE_CURSOR_INVALID' ||
    code === 'STATE_PAGE_CURSOR_STALE' ||
    code === 'STATE_PAGE_UNSUPPORTED' ||
    code === 'STATE_PAGE_INVALID_REQUEST'
  ) {
    return new StatePageError(code, error)
  }
  return new StatePageError('STATE_PAGE_FAILED', error)
}

function validatePage<T>(value: unknown, request: StatePageRequest): StatePage<T> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new StatePageError('STATE_PAGE_INVALID_RESPONSE')
  }

  const page = value as Record<string, unknown>
  const keys = Object.keys(page)
  if (
    keys.length !== 2 ||
    !Object.hasOwn(page, 'items') ||
    !Object.hasOwn(page, 'next_cursor') ||
    !Array.isArray(page.items) ||
    page.items.length > request.limit
  ) {
    throw new StatePageError('STATE_PAGE_INVALID_RESPONSE')
  }

  const nextCursor = page.next_cursor
  if (
    nextCursor !== null &&
    (typeof nextCursor !== 'string' ||
      nextCursor.length === 0 ||
      Buffer.byteLength(nextCursor, 'utf8') > MAX_STATE_PAGE_CURSOR_BYTES)
  ) {
    throw new StatePageError('STATE_PAGE_INVALID_RESPONSE')
  }
  if (nextCursor !== null && page.items.length === 0) {
    throw new StatePageError('STATE_PAGE_INVALID_RESPONSE')
  }

  let responseBytes: number
  try {
    const serialized = JSON.stringify({ items: page.items, next_cursor: nextCursor })
    if (serialized === undefined) throw new Error()
    responseBytes = Buffer.byteLength(serialized, 'utf8')
  } catch {
    throw new StatePageError('STATE_PAGE_INVALID_RESPONSE')
  }
  if (responseBytes > request.max_bytes) throw new StatePageError('STATE_PAGE_INVALID_RESPONSE')

  return { items: page.items as T[], next_cursor: nextCursor as string | null }
}

export async function* iterateStatePages<T>(
  trigger: StatePageTrigger,
  scope: string,
  options: StatePageOptions = {},
): AsyncGenerator<StatePage<T>> {
  if (typeof scope !== 'string' || typeof options !== 'object' || options === null) invalidRequest()
  const normalized = normalizeOptions(options)
  const seenCursors = new Set<string>()
  const cursorOrder: string[] = []
  let cursor = normalized.cursor
  if (cursor !== undefined) {
    seenCursors.add(cursor)
    cursorOrder.push(cursor)
  }

  for (;;) {
    const request: StatePageRequest = {
      scope,
      ...(cursor === undefined ? {} : { cursor }),
      limit: normalized.limit,
      max_bytes: normalized.maxBytes,
    }
    let rawPage: unknown
    try {
      rawPage = await trigger(request)
    } catch (error) {
      throw normalizeTriggerError(error)
    }

    const page = validatePage<T>(rawPage, request)
    const nextCursor = page.next_cursor
    if (nextCursor !== null) {
      if (nextCursor === cursor || seenCursors.has(nextCursor)) {
        throw new StatePageError('STATE_PAGE_CURSOR_INVALID')
      }
      seenCursors.add(nextCursor)
      cursorOrder.push(nextCursor)
      if (cursorOrder.length > MAX_TRACKED_CURSORS) {
        const expiredCursor = cursorOrder.shift()
        if (expiredCursor !== undefined) seenCursors.delete(expiredCursor)
      }
    }

    yield page
    if (nextCursor === null) return
    cursor = nextCursor
  }
}
