import { afterEach, describe, expect, it, vi } from 'vitest';
vi.mock('iii-sdk', () => ({ InvocationError: class InvocationError extends Error {}, TriggerAction: { Void: () => ({}) } }));
import { createProjectIndexMaintenance } from '../src/state/indexed-retrieval.js';
import { registerApiTriggers } from '../src/triggers/api.js';
import { registerMcpEndpoints } from '../src/mcp/server.js';
import { getAllTools } from '../src/mcp/tools-registry.js';
import { KV } from '../src/state/schema.js';
import type { StateKV } from '../src/state/kv.js';
import { registerStorageMaintenance } from '../src/functions/storage-maintenance.js';
import type { StorageMaintenanceState, StorageCleanupSummary } from '../src/types.js';
import { pruneTerminalGraphWork } from '../src/functions/graph-retention.js';

afterEach(() => vi.useRealTimers());

function projectMaintenance() {
  let saved: unknown = null;
  const kv = {
    indexedRetrieval: true,
    get: vi.fn(async () => saved),
    set: vi.fn(async (_scope: string, _key: string, value: unknown) => { saved = structuredClone(value); }),
    retrieval: vi.fn(async () => ({ status: 'pending', processed: 16, cursor: '16', revision: '2', incomplete_rows: '0' })),
    list: vi.fn(() => { throw new Error('Full scan forbidden'); }),
    pages: vi.fn(() => { throw new Error('Page walk forbidden'); }),
  };
  let paused = false, busy = false;
  const log = vi.fn();
  const make = () => createProjectIndexMaintenance(kv as unknown as StateKV, () => paused, () => busy, log);
  return { kv, make, log, pause: () => { paused = true; }, busy: () => { busy = true; } };
}

describe('bounded project index progress', () => {
  it('continues sessions, summaries and vectors over process-state loss with one native cut per tick', async () => {
    vi.useFakeTimers();
    const h = projectMaintenance();
    await h.make()();
    vi.advanceTimersByTime(60_001);
    await h.make()();
    vi.advanceTimersByTime(60_001);
    await h.make()();
    expect(h.kv.retrieval.mock.calls.map(([p]) => p)).toEqual([
      { action: 'project_catalog_prepare', source: 'sessions', max_rows: 16, max_bytes: 65_536 },
      { action: 'project_catalog_prepare', source: 'summaries', max_rows: 16, max_bytes: 65_536 },
      { action: 'project_vectors_prepare', index_id: 'observations', max_rows: 16, max_bytes: 65_536 },
    ]);
    expect(h.kv.set).toHaveBeenLastCalledWith(KV.config, 'project-index-maintenance', expect.objectContaining({
      next: 0, sources: expect.objectContaining({ sessions: expect.objectContaining({ status: 'pending' }) }),
    }));
    expect(h.kv.list).not.toHaveBeenCalled(); expect(h.kv.pages).not.toHaveBeenCalled();
  });

  it('honors pause and foreground activity before any state call', async () => {
    const h = projectMaintenance(); h.pause(); await h.make()();
    expect(h.kv.get).not.toHaveBeenCalled();
    const active = projectMaintenance(); active.busy(); await active.make()();
    expect(active.kv.get).not.toHaveBeenCalled();
  });

  it('coalesces overlapping ticks and backs off a failed or malformed native response', async () => {
    vi.useFakeTimers();
    const h = projectMaintenance();
    let resolve!: (value: unknown) => void;
    h.kv.retrieval.mockImplementationOnce(() => new Promise(r => { resolve = r; }) as never);
    const tick = h.make(); const first = tick();
    await Promise.resolve(); await tick();
    expect(h.kv.retrieval).toHaveBeenCalledTimes(1);
    resolve({ status: 'ready', processed: 17, cursor: '', revision: '2' });
    await first; await h.make()();
    expect(h.kv.retrieval).toHaveBeenCalledTimes(1);
    expect(h.log).toHaveBeenCalledWith(expect.stringContaining('STATE_TX_INVALID_RESPONSE'));
    vi.advanceTimersByTime(15 * 60_000 + 1); await h.make()();
    expect(h.kv.retrieval).toHaveBeenCalledTimes(2);
  });
});

function boundary(kind: 'api' | 'mcp', secret = '') {
  const handlers = new Map<string, (req: unknown) => Promise<{ status_code: number; body: unknown }>>();
  const trigger = vi.fn(async () => ({ status: 'partial', nodes: [] }));
  const sdk = { registerFunction: (id: string, handler: never) => handlers.set(id, handler), registerTrigger: vi.fn(), trigger };
  const kv = { get: vi.fn(async () => null), list: vi.fn(async () => []) };
  if (kind === 'api') registerApiTriggers(sdk as never, kv as never, secret);
  else registerMcpEndpoints(sdk as never, kv as never, secret);
  return { handlers, trigger };
}

describe('project graph public boundaries', () => {
  it.each(['api::graph-query', 'api::graph-snapshot-rebuild', 'api::graph-stats'])('%s whitelists and forwards a known project', async id => {
    const h = boundary('api');
    const response = await h.handlers.get(id)!({ headers: {}, body: { project: ' project-a ', evil: true }, query_params: { project: ' project-a ' } });
    expect(response.status_code).toBe(200);
    expect(h.trigger).toHaveBeenCalledWith({ function_id: id.replace('api::', 'mem::'), payload: expect.objectContaining({ project: 'project-a' }) });
    expect(h.trigger.mock.calls[0]![0].payload).not.toHaveProperty('evil');
  });
  it.each([42, '', 'x'.repeat(257)])('rejects invalid graph project %j before triggering', async project => {
    for (const id of ['api::graph-query', 'api::graph-snapshot-rebuild', 'api::graph-stats']) {
      const h = boundary('api');
      expect((await h.handlers.get(id)!({ headers: {}, body: { project }, query_params: { project } })).status_code).toBe(400);
      expect(h.trigger).not.toHaveBeenCalled();
    }
  });
  it('authenticates before forwarding the graph scope', async () => {
    const h = boundary('api', 'secret');
    expect((await h.handlers.get('api::graph-query')!({ headers: {}, body: { project: 'p' } })).status_code).toBe(401);
    expect(h.trigger).not.toHaveBeenCalled();
  });
  it('exposes and validates the existing MCP project argument without adding a tool', async () => {
    const tool = getAllTools().find(t => t.name === 'memory_graph_query');
    expect(tool?.inputSchema.properties).toHaveProperty('project');
    const h = boundary('mcp');
    await h.handlers.get('mcp::tools::call')!({ headers: {}, body: { name: 'memory_graph_query', arguments: { project: ' p ', query: 'auth' } } });
    expect(h.trigger).toHaveBeenCalledWith({ function_id: 'mem::graph-query', payload: { project: 'p', query: 'auth' } });
    h.trigger.mockClear();
    const response = await h.handlers.get('mcp::tools::call')!({ headers: {}, body: { name: 'memory_graph_query', arguments: { project: 42 } } });
    expect(response.body).toMatchObject({ isError: true }); expect(h.trigger).not.toHaveBeenCalled();
  });
});

describe('temporal technical retention integration', () => {
  it('rotates the durable terminal-job scan after native temporal dependency deferral', async () => {
    const kv = {
      get: vi.fn(async (_scope: string, key: string) => key === 'current' ? { cleanupActivatedAt: '2026-01-01' } : null),
      getVersioned: vi.fn(async (scope: string) => ({ version: '2', value: scope === KV.graphControl
        ? { generation: '1' } : scope === KV.graphJobs ? { state: 'completed', kind: 'temporal' } : { visibility: 'complete' } })),
      lease: vi.fn(async ({ action }: { action: string }) => action === 'release' ? { released: true } : { owner_id: 'owner', generation: '1', fence: '2' }),
      pages: vi.fn(async function* () { yield { items: [{ id: 'unit' }], next_cursor: 'later-jobs' }; }),
      pruneTerminalGraphJob: vi.fn(async () => ({ deleted_count: 0, deleted_bytes: 0, done: false, deferred: true })),
      set: vi.fn(),
    };
    expect(await pruneTerminalGraphWork(kv as never)).toMatchObject({ pending: true, deleted: 0 });
    expect(kv.pruneTerminalGraphJob).toHaveBeenCalledTimes(1);
    expect(kv.set).toHaveBeenCalledWith(KV.config, 'graph-terminal-retention-page', { cursor: 'later-jobs' });
  });
  it.each([false, true])('sends versioned completed manifest candidates only without a pending reference (%s)', async pending => {
    const rows = new Map<string, unknown>();
    const id = 'temporaljob_1234567890abcdef';
    const key = `temporal-work:${id}:manifest`;
    const old = new Date(Date.now() - 6 * 86_400_000).toISOString();
    const manifest = { version: 1, id, state: 'completed', pageCount: 1, currentPage: 1,
      nodesAdded: 1, edgesAdded: 0, workUnits: 1, createdAt: old, updatedAt: old };
    let handle!: (request: unknown) => Promise<{ state?: StorageMaintenanceState; success?: boolean; cleanup?: StorageCleanupSummary }>;
    const kv = {
      get: vi.fn(async (scope: string, item: string) => scope === KV.temporalGraphPending ? (pending ? { jobId: id } : null) : rows.get(`${scope}/${item}`) ?? null),
      set: vi.fn(async (scope: string, item: string, value: unknown) => { rows.set(`${scope}/${item}`, structuredClone(value)); }),
      getVersioned: vi.fn(async (scope: string, item: string) => ({
        exists: scope === KV.temporalGraphWork && item === key || scope === KV.temporalGraphPending && pending,
        version: '9', value: scope === KV.temporalGraphWork && item === key ? manifest : scope === KV.temporalGraphPending && pending ? { jobId: id } : null,
      })),
      pages: vi.fn(async function* (scope: string) { yield { items: scope === KV.temporalGraphWork ? [manifest] : [], next_cursor: null }; }),
    };
    const physical = { page_size: 4096, page_count: 1, freelist_count: 0, reusable_bytes: 0, allocated_page_bytes: 4096,
      auto_vacuum: 2, journal_mode: 'wal', database_bytes: 4096, wal_bytes: 0 };
    const trigger = vi.fn(async ({ payload }: { payload: Record<string, unknown> }) => {
      if (payload.action === 'diagnostics') return { category_version: 1, sampled_at: new Date().toISOString(), consistency: 'keyset_live', complete: true, done: true,
        rows_scanned: 1, categories: [], cursor: null, physical, coverage: 'state_rows' };
      if (payload.action === 'prune_technical') return { deleted_count: 1, raw_stripped_count: 0, deleted_logical_bytes: 200, deleted_stored_bytes: 100,
        physical_bytes_reclaimed: null, skipped_count: 0, processed_count: 1, cutoff_at_ms: Date.now() - 5 * 86_400_000, done: true, input_logical_bytes: 200 };
      if (payload.action === 'reclaim') return { mode: 'incremental', requires_explicit_full_rebuild: false, checkpoint_busy: false,
        wal_frames: 0, checkpointed_frames: 0, checkpoint_performed: false, status: 'complete', reason: 'no_free_pages', elapsed_ms: 1,
        pages_reclaimed: 0, physical_bytes_reclaimed: 0, before: physical, after: physical };
      throw new Error('Unexpected storage action');
    });
    registerStorageMaintenance({ registerFunction: (_id: string, fn: typeof handle) => { handle = fn; }, trigger } as never, kv as never);
    const state = (await handle({ action: 'status' })).state!;
    state.observationIndex.complete = true; state.cleanupActivatedAt = new Date().toISOString();
    state.diagnostics.lastComplete = { sampledAt: old, categoryVersion: 1, categories: [], physical, coverage: 'state_rows', rowsScanned: 1 };
    state.compaction.nextEligibleAt = new Date(Date.now() + 60_000).toISOString();
    rows.set(`${KV.storageMaintenance}/current`, state);
    const result = await handle({ action: 'tick' });
    expect(result.success).toBe(true);
    const pruning = trigger.mock.calls.filter(([req]) => req.payload.action === 'prune_technical');
    if (pending) {
      expect(pruning[0]![0].payload).toMatchObject({ candidates: [{ scope: KV.temporalGraphPending, key: id, expected_version: '9',
        preserved: { scope: KV.temporalGraphWork, key, expected_version: '9' } }] });
    } else {
      expect(pruning[0]![0].payload).toMatchObject({ candidates: [{ scope: KV.temporalGraphWork, key, expected_version: '9' }] });
      expect(result.cleanup).toMatchObject({ technicalDeleted: 1, deletedLogicalBytes: 200, deletedStoredBytes: 100 });
    }
    expect(kv.pages).toHaveBeenCalledWith(KV.temporalGraphWork, expect.objectContaining({ limit: 1, maxBytes: 65_536 }));
  });
});
