import { describe, expect, it, vi } from 'vitest';
import type { EmbeddingProvider } from '../src/types.js';
import type { StateKV } from '../src/state/kv.js';
import { KV } from '../src/state/schema.js';
import { IndexedVector } from '../src/state/indexed-vector.js';

const identity = { index_id: 'observations', model: 'local-test', dimensions: 2, generation: 'local-v1' };
const provider: EmbeddingProvider = {
  name: 'local-test', dimensions: 2,
  embed: async () => new Float32Array([1, 0]),
  embedBatch: vi.fn(async texts => texts.map(() => new Float32Array([1, 0]))),
};
const ref = (id: string) => ({ source_scope: KV.observations('s'), id, source_version: '1' });

function harness(ids: string[], pages: Array<Record<string, unknown>>, overrides: Record<string, unknown> = {}, markerStatus = 'ready') {
  const sources = new Map(ids.map(id => [id, { title: id, narrative: 'body', sessionId: 's' }]));
  const upserted: string[] = [];
  let marker: Record<string, unknown> = { ...identity, status: markerStatus };
  const retrieval = vi.fn(async (payload: Record<string, unknown>): Promise<unknown> => {
    if (payload.action === 'index_status') return {
      version: 1, capabilities: ['state::semantic_lsh_v1', 'state::indexed_graph_v1'],
      graph: [KV.graphNodes, KV.graphEdges].map(scope => ({ scope, status: 'ready' })),
      semantic: [{ ...identity, status: 'ready', count: '10', lexical_count: '10', lexical_ready: true,
        source_kind: 'agentmemory', source_prepared: true, dirty_count: String(ids.length - upserted.length),
        coverage_ready: upserted.length === ids.length, ...overrides }],
    };
    if (payload.action === 'source_prepare') return pages.shift() ?? { processed: 0, items: [], coverage_ready: true };
    if (payload.action === 'semantic_upsert') {
      upserted.push(...(payload.items as Array<{ id: string }>).map(item => item.id));
      return {};
    }
    throw new Error(`Unexpected retrieval action: ${payload.action}`);
  });
  const kv = {
    retrieval,
    get: vi.fn(async () => marker),
    set: vi.fn(async (_scope: string, _key: string, value: Record<string, unknown>) => { marker = value; }),
    getVersioned: vi.fn(async (_scope: string, id: string) => ({ exists: sources.has(id), version: '1', value: sources.get(id) })),
    list: vi.fn(() => { throw new Error('Full corpus read forbidden'); }),
    pages: vi.fn(() => { throw new Error('Full corpus page walk forbidden'); }),
  } as unknown as StateKV;
  return { vector: new IndexedVector(kv, provider), retrieval, upserted, kv, getMarker: () => marker };
}

describe('bounded indexed startup recovery', () => {
  it('repairs four dirty sources through two native pages and coalesces callers', async () => {
    const ids = ['a', 'b', 'c', 'd'];
    const { vector, retrieval, upserted, kv } = harness(ids, [
      { processed: 0, items: ids.map(ref), coverage_ready: false },
      { processed: 0, items: [], coverage_ready: true },
    ]);
    const first = vector.recoverDirtySources(provider);
    const second = vector.recoverDirtySources(provider);
    expect(first).toBe(second);
    expect(await first).toBe(4);
    expect(upserted).toEqual(ids);
    await expect(vector.ready()).resolves.toBeUndefined();
    expect(retrieval.mock.calls.filter(([request]) => request.action === 'source_prepare')).toHaveLength(2);
    expect((kv as { list: ReturnType<typeof vi.fn> }).list).not.toHaveBeenCalled();
  });

  it('does no native source preparation when already healthy', async () => {
    const { vector, retrieval } = harness([], []);
    expect(await vector.recoverDirtySources(provider)).toBe(0);
    expect(retrieval).not.toHaveBeenCalledWith(expect.objectContaining({ action: 'source_prepare' }));
  });

  it('repairs a matching pending marker only after native source coverage is complete', async () => {
    const { vector, retrieval, getMarker, kv } = harness(['a'], [
      { processed: 0, items: [ref('a')], coverage_ready: false },
      { processed: 0, items: [], coverage_ready: true },
    ], {}, 'pending');
    await expect(vector.ready()).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(await vector.recoverDirtySources(provider)).toBe(1);
    expect(getMarker()).toMatchObject({ ...identity, status: 'ready', count: 10 });
    expect((kv as { set: ReturnType<typeof vi.fn> }).set).toHaveBeenCalledTimes(1);
    expect(retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: 'semantic_upsert' }));
    await expect(vector.ready()).resolves.toBeUndefined();
  });

  it('reconciles a pending marker when native coverage is already complete', async () => {
    const { vector, retrieval, getMarker } = harness([], [], {}, 'pending');
    expect(await vector.recoverDirtySources(provider)).toBe(0);
    expect(getMarker()).toMatchObject({ status: 'ready', count: 10 });
    expect(retrieval).not.toHaveBeenCalledWith(expect.objectContaining({ action: 'source_prepare' }));
  });

  it('handles a pending scope within the row and page budget', async () => {
    const { vector, retrieval } = harness(['a'], [
      { processed: 2, items: [ref('a')], coverage_ready: false },
      { processed: 0, items: [], coverage_ready: true },
    ], { source_prepared: false });
    // Native source preparation marks pending scopes ready before the final status probe.
    const native = retrieval.getMockImplementation()!;
    let prepared = false;
    retrieval.mockImplementation(async payload => {
      if (payload.action === 'source_prepare') prepared = true;
      const result = await native(payload) as { semantic?: Array<Record<string, unknown>> };
      if (payload.action === 'index_status' && prepared) result.semantic![0].source_prepared = true;
      return result;
    });
    expect(await vector.recoverDirtySources(provider)).toBe(1);
  });

  it('rejects oversized dirty gaps before preparation', async () => {
    const { vector, retrieval } = harness([], [], { dirty_count: '129', coverage_ready: false });
    await expect(vector.recoverDirtySources(provider)).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(retrieval).not.toHaveBeenCalledWith(expect.objectContaining({ action: 'source_prepare' }));
  });

  it('stops after sixteen nonconverging pages', async () => {
    const { vector, retrieval } = harness([], Array.from({ length: 16 }, () => ({ processed: 256, items: [], coverage_ready: false })), { source_prepared: false });
    await expect(vector.recoverDirtySources(provider)).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(retrieval.mock.calls.filter(([request]) => request.action === 'source_prepare')).toHaveLength(16);
  });

  it('rejects identity and graph corruption without mutating sources', async () => {
    for (const overrides of [{ model: 'wrong-model' }, { generation: 'other' }]) {
      const { vector, retrieval } = harness(['a'], [], overrides);
      await expect(vector.recoverDirtySources(provider)).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
      expect(retrieval).not.toHaveBeenCalledWith(expect.objectContaining({ action: 'source_prepare' }));
    }
    const { vector } = harness(['a'], []);
    await expect(vector.recoverDirtySources({ ...provider, name: 'wrong-model' })).rejects.toMatchObject({ code: 'STATE_SEMANTIC_IDENTITY_MISMATCH' });
    const graph = harness(['a'], []);
    const native = graph.retrieval.getMockImplementation()!;
    graph.retrieval.mockImplementation(async payload => {
      const result = await native(payload) as { graph?: Array<{ status: string }> };
      if (payload.action === 'index_status') result.graph![0].status = 'pending';
      return result;
    });
    await expect(graph.vector.recoverDirtySources(provider)).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(graph.retrieval).not.toHaveBeenCalledWith(expect.objectContaining({ action: 'source_prepare' }));
  });
});
