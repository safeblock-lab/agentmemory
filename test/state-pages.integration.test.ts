import { AddressInfo, connect, createServer } from 'node:net'
import { spawn, type ChildProcess } from 'node:child_process'
import { createWriteStream, mkdtempSync, mkdirSync, writeFileSync } from 'node:fs'
import { once } from 'node:events'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { afterAll, describe, expect, it } from 'vitest'
import type { WebSocket as WsWebSocket } from 'ws'

import { registerWorker, type IIIClient } from 'iii-sdk'
import { StateKV } from '../src/state/kv.js'
import { StatePageError } from '../src/state/state-pages.js'

const enabled = process.env['AGENTMEMORY_RUN_ENGINE_INTEGRATION'] === '1'
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const runRoot = join(repoRoot, '.native-pagination-build', 'integration')
const receiverLimit = 32 * 1024
const pageBudget = 4 * 1024

type RuntimeSocket = WsWebSocket & { _receiver?: { _maxPayload: number } }
type RuntimeClient = IIIClient & { isOpen(): boolean; ws?: RuntimeSocket }
type SocketObservation = { opened: boolean; closed: boolean; frames: number; maxFrameBytes: number; errors: string[] }
const socketObservations: SocketObservation[] = []
const trackedSockets = new WeakSet<WsWebSocket>()

describe.skipIf(!enabled)('patched iii state pagination transport', () => {
  let runDir: string
  let configPath: string
  let wsPort: number
  let engine: ChildProcess | undefined
  let engineLogs: ReturnType<typeof createWriteStream>
  let client: RuntimeClient
  let kv: StateKV
  const evidence: Record<string, unknown> = {}

  function saveEvidence(values: Record<string, unknown>): void {
    Object.assign(evidence, values)
    writeFileSync(join(runDir, 'result.json'), `${JSON.stringify(evidence, null, 2)}\n`)
  }

  function capAndTrackCurrentSocket(): void {
    const socket = client.ws
    if (!socket || !socket._receiver) throw new Error('iii-sdk WebSocket receiver is unavailable')
    if (trackedSockets.has(socket)) return
    socket._receiver._maxPayload = receiverLimit
    expect(socket._receiver._maxPayload).toBe(receiverLimit)
    const observed: SocketObservation = { opened: true, closed: false, frames: 0, maxFrameBytes: 0, errors: [] }
    socketObservations.push(observed)
    trackedSockets.add(socket)
    socket.on('close', () => { observed.closed = true })
    socket.on('error', (error) => { observed.errors.push(error.message) })
    socket.on('message', (data) => {
      const frameBytes = Array.isArray(data)
        ? data.reduce((total, part) => total + part.byteLength, 0)
        : data.byteLength
      observed.frames += 1
      observed.maxFrameBytes = Math.max(observed.maxFrameBytes, frameBytes)
    })
  }

  async function reserveLoopbackPort(): Promise<number> {
    const listener = createServer()
    await new Promise<void>((resolveListen, reject) => {
      listener.once('error', reject)
      listener.listen(0, '127.0.0.1', resolveListen)
    })
    const port = (listener.address() as AddressInfo).port
    await new Promise<void>((resolveClose, reject) => listener.close((error) => error ? reject(error) : resolveClose()))
    return port
  }

  async function waitFor(predicate: () => boolean, label: string, timeoutMs = 10_000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      if (predicate()) return
      if (engine?.exitCode !== null && engine?.exitCode !== undefined) {
        throw new Error(`iii-engine exited before ${label} (code ${engine.exitCode})`)
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, 40))
    }
    throw new Error(`Timed out waiting for ${label}`)
  }

  async function startEngine(): Promise<void> {
    const binary = join(repoRoot, '.iii-engine-build', 'artifacts', 'win32-x64', 'iii.exe')
    engine = spawn(binary, ['--config', configPath, '--no-update-check'], {
      cwd: runDir,
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        HOME: runDir,
        USERPROFILE: runDir,
        III_TELEMETRY_ENABLED: 'false',
        IIIWORKER_DISABLE_BUILTIN_DAEMONS: '1',
        III_DISABLE_TRACE_PAYLOADS: '1',
        RUST_LOG: 'error',
      },
    })
    engine.stdout?.pipe(engineLogs)
    engine.stderr?.pipe(engineLogs)
    const deadline = Date.now() + 15_000
    let listening = false
    while (Date.now() < deadline) {
      if (engine.exitCode !== null) throw new Error(`iii-engine startup failed; see ${join(runDir, 'engine.log')}`)
      const connected = await new Promise<boolean>((resolveConnect) => {
        const connection = connect(wsPort, '127.0.0.1')
        connection.once('connect', () => { connection.destroy(); resolveConnect(true) })
        connection.once('error', () => resolveConnect(false))
      })
      if (connected) {
        listening = true
        break
      }
      await new Promise((resolveWait) => setTimeout(resolveWait, 50))
    }
    if (!listening) throw new Error('Timed out waiting for isolated iii-engine listener')
  }

  async function stopEngine(): Promise<void> {
    if (!engine || engine.exitCode !== null) return
    const owned = engine
    owned.kill('SIGTERM')
    let stopTimer: ReturnType<typeof setTimeout> | undefined
    try {
      await Promise.race([
        once(owned, 'exit'),
        new Promise((_, reject) => {
          stopTimer = setTimeout(() => reject(new Error('Timed out stopping test-owned iii-engine')), 5_000)
        }),
      ])
    } finally {
      if (stopTimer) clearTimeout(stopTimer)
    }
    engine = undefined
  }

  async function waitForSdk(): Promise<void> {
    await waitFor(() => client.isOpen(), 'SDK WebSocket connection')
    capAndTrackCurrentSocket()
    await client.trigger({ function_id: 'state::get', payload: { scope: 'ready', key: 'probe' }, timeoutMs: 5_000 })
  }

  async function expectPageError<T>(operation: Promise<T>, code: string): Promise<void> {
    try {
      await operation
      throw new Error(`Expected ${code}`)
    } catch (error) {
      expect(error).toBeInstanceOf(StatePageError)
      expect((error as StatePageError).code).toBe(code)
    }
  }

  async function nextPage(scope: string, options: { cursor?: string; limit?: number } = {}) {
    const pages = kv.pages(scope, { ...options, maxBytes: pageBudget })
    return pages.next()
  }

  it('bounds real SDK frames, preserves order and keeps semantic errors on the socket', async () => {
    mkdirSync(runRoot, { recursive: true })
    runDir = mkdtempSync(join(runRoot, 'run-'))
    wsPort = await reserveLoopbackPort()
    configPath = join(runDir, 'config.yaml')
    writeFileSync(configPath, `modules:
  - name: iii-state
    config:
      adapter:
        name: kv
        config:
          store_method: in_memory
workers:
  - name: iii-worker-manager
    config:
      port: ${wsPort}
      host: 127.0.0.1
`, { flag: 'wx' })
    engineLogs = createWriteStream(join(runDir, 'engine.log'), { flags: 'wx' })
    await startEngine()
    client = registerWorker(`ws://127.0.0.1:${wsPort}`, {
      workerName: 'state-pagination-integration',
      enableMetricsReporting: false,
      invocationTimeoutMs: 3_000,
      otel: { enabled: false },
      reconnectionConfig: { initialDelayMs: 100, maxDelayMs: 100, backoffMultiplier: 1, jitterFactor: 0, maxRetries: -1 },
    }) as RuntimeClient
    kv = new StateKV(client)
    await waitForSdk()

    const scope = `pagination-${Date.now()}`
    const expected = Array.from({ length: 100 }, (_, sequence) => ({
      sequence,
      text: 'ñ🙂"\\\n'.repeat(38),
    }))
    for (const value of expected) await kv.set(scope, `key-${value.sequence}`, value)
    const fullScopeBytes = Buffer.byteLength(JSON.stringify(expected), 'utf8')
    expect(fullScopeBytes).toBeGreaterThan(receiverLimit)

    const socketBeforeUnboundedList = client.ws
    await expect(client.trigger({ function_id: 'state::list', payload: { scope }, timeoutMs: 1_000 })).rejects.toBeDefined()
    await waitFor(() => Boolean(socketObservations[0]?.closed), 'oversized WebSocket receiver close')
    expect(socketObservations[0]?.errors.join('\n')).toMatch(/Max payload size exceeded/i)
    await waitFor(() => client.isOpen() && client.ws !== socketBeforeUnboundedList, 'SDK reconnect after oversized full list')
    await waitForSdk()

    const observedValues: unknown[] = []
    for await (const page of kv.pages(scope, { limit: 256, maxBytes: pageBudget })) {
      const pageBytes = Buffer.byteLength(JSON.stringify({ items: page.items, next_cursor: page.next_cursor }), 'utf8')
      expect(pageBytes).toBeLessThanOrEqual(pageBudget)
      observedValues.push(...page.items)
    }
    expect(observedValues).toEqual(expected)
    const activeSocketCount = socketObservations.length
    expect(activeSocketCount).toBe(2)
    expect(socketObservations.at(-1)?.maxFrameBytes).toBeLessThan(receiverLimit)
    saveEvidence({
      receiverLimitBytes: receiverLimit,
      pageBudgetBytes: pageBudget,
      fullScopeJsonBytes: fullScopeBytes,
      fullListReceiverError: socketObservations[0]?.errors.join('; '),
      pagesReturned: observedValues.length,
      largestObservedFrameBytes: Math.max(...socketObservations.map((socket) => socket.maxFrameBytes)),
      socketCountAfterPagination: activeSocketCount,
      pagesStayedConnected: !socketObservations.at(-1)?.closed,
    })

    const oversizedScope = `oversized-${Date.now()}`
    await kv.set(oversizedScope, 'single', { text: 'x'.repeat(5_000) })
    const oversizedPages = kv.pages(oversizedScope, { maxBytes: pageBudget })
    await expectPageError(oversizedPages.next(), 'STATE_RECORD_TOO_LARGE')
    await expect(kv.get(scope, 'key-0')).resolves.toEqual(expected[0])
    expect(socketObservations.length).toBe(activeSocketCount)
    expect(socketObservations.at(-1)?.closed).toBe(false)
    saveEvidence({ oversizeRecordError: 'STATE_RECORD_TOO_LARGE', sameSocketAfterOversize: true })

    const first = await nextPage(scope, { limit: 2 })
    expect(first.done).toBe(false)
    const cursor = first.value.next_cursor
    expect(typeof cursor).toBe('string')
    await expectPageError(nextPage(scope, { cursor: 'not-a-valid-cursor' }), 'STATE_PAGE_CURSOR_INVALID')
    await expectPageError(nextPage(`${scope}-other`, { cursor: cursor! }), 'STATE_PAGE_CURSOR_INVALID')

    const stalePages = kv.pages(scope, { limit: 2, maxBytes: pageBudget })
    const staleFirst = await stalePages.next()
    expect(staleFirst.value?.next_cursor).toBeTruthy()
    await kv.set(scope, 'mutation', { sequence: -1 })
    await expectPageError(stalePages.next(), 'STATE_PAGE_CURSOR_STALE')
    await expect(kv.get(scope, 'key-0')).resolves.toEqual(expected[0])
    expect(socketObservations.length).toBe(activeSocketCount)
    expect(socketObservations.at(-1)?.closed).toBe(false)
    saveEvidence({ mutationCursorError: 'STATE_PAGE_CURSOR_STALE', sameSocketAfterStaleCursor: true })

    const restartPages = kv.pages(scope, { limit: 2, maxBytes: pageBudget })
    const beforeRestart = await restartPages.next()
    const restartCursor = beforeRestart.value?.next_cursor
    expect(restartCursor).toBeTruthy()
    await stopEngine()
    await startEngine()
    await waitForSdk()
    await expectPageError(nextPage(scope, { cursor: restartCursor! }), 'STATE_PAGE_CURSOR_STALE')
    await expect(client.trigger({ function_id: 'state::list_groups', payload: {} })).resolves.toBeDefined()
    expect(socketObservations.length).toBe(activeSocketCount + 1)
    expect(socketObservations.at(-1)?.closed).toBe(false)
    saveEvidence({ restartCursorError: 'STATE_PAGE_CURSOR_STALE', totalSockets: socketObservations.length, passed: true })
  }, 90_000)

  afterAll(async () => {
    await client?.shutdown().catch(() => undefined)
    await stopEngine().catch(() => undefined)
    await new Promise<void>((resolveClose) => engineLogs?.end(resolveClose))
  })
})
