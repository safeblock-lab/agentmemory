import { beforeEach, afterEach, describe, it, expect, vi } from 'vitest';
import { resolve } from 'node:path';
import { GraphRetrieval } from '../src/functions/graph-retrieval.js';
import { withSerializedGraphRetrieval } from '../src/functions/graph-retrieval-read.js';
import { HybridSearch } from '../src/state/hybrid-search.js';
import { SearchIndex } from '../src/state/search-index.js';
import { IndexedVector } from '../src/state/indexed-vector.js';
import { indexedGraphItems, type IndexedSeed } from '../src/state/indexed-retrieval.js';
import { prepareIndexedCorpus } from '../src/state/indexed-preparation.js';
import { IndexedLocalEmbedding } from '../src/state/indexed-embedding.js';
import { setIndexedVector, setEmbeddingProvider, vectorIndexAddGuarded, vectorIndexRemove, flushIndexSave, getSearchIndex } from '../src/functions/search.js';
import { rerank } from '../src/state/reranker.js';
import type { StateKV } from '../src/state/kv.js';
import type { EmbeddingProvider, CompressedObservation, GraphNode, GraphEdge } from '../src/types.js';
import { KV } from '../src/state/schema.js';

vi.mock('../src/state/reranker.js', () => ({ rerank: vi.fn(async (_query, results) => results) }));
const { extraction, pipeline } = vi.hoisted(() => {
  const extraction = vi.fn();
  return { extraction, pipeline: vi.fn(async () => extraction) };
});
vi.mock('@huggingface/transformers', () => ({ pipeline }));
const provider: EmbeddingProvider = { name: 'test-local', dimensions: 2, embed: async () => new Float32Array([1, 0]), embedBatch: async texts => texts.map(() => new Float32Array([1, 0])) };
const identity = { index_id: 'observations', model: provider.name, dimensions: 2, generation: 'local-v1' };
const node: GraphNode = { id: 'n1', name: 'AgentMemory', type: 'concept', properties: {}, sourceObservationIds: ['a'], createdAt: '2026-01-01' };
const edge = (id: string, date: string): GraphEdge => ({ id, sourceNodeId: 'n1', targetNodeId: 'n1', type: 'related_to', weight: 1, sourceObservationIds: ['a'], createdAt: date, tcommit: date, isLatest: true });
const obs = (id: string): CompressedObservation => ({ id, sessionId: 's', timestamp: '2026-01-01', type: 'file_edit', title: id, subtitle: '', facts: [], narrative: id, concepts: [], files: [], importance: 5 });
function fixture() {
  const records = new Map<string, unknown>([['indexed-corpus', { ...identity, status: 'ready' }], ['n1', node], ['a', obs('a')], ['b', obs('b')]]);
  const state = {
    indexedRetrieval: true,
    get: vi.fn(async (_scope: string, key: string) => records.get(key) ?? null),
    getVersioned: vi.fn(async (scope: string, key: string) => scope === KV.graphControl ? { value: { generation: '1', fence: '1' } } : { exists: records.has(key), version: '1', value: records.get(key) }),
    set: vi.fn(async (_scope: string, key: string, value: unknown) => { records.set(key, value); return value; }),
    pages: vi.fn(() => { throw new Error('Corpus enumeration prohibited in query'); }),
    list: vi.fn(() => { throw new Error('Corpus materialization prohibited'); }),
    retrieval: vi.fn(async (payload: Record<string, unknown>): Promise<unknown> => {
      if (payload.action === 'index_status') return { version: 1, capabilities: ['state::semantic_lsh_v1', 'state::indexed_graph_v1'], graph: [KV.graphNodes, KV.graphEdges].map(scope => ({ scope, status: 'ready' })), semantic: [{ ...identity, status: 'ready', count: '2', lexical_count: '2', lexical_ready: true, source_kind: 'agentmemory', source_prepared: true, dirty_count: '0', coverage_ready: true }] };
      if (payload.action === 'source_prepare') return { source_prepared: true, dirty_count: '0', coverage_ready: true, items: [] };
      if (payload.action === 'graph_seeds') return { generation: '1', items: (payload.entity_names as string[]).length ? [{ key: 'n1', id: 'n1', position: '9007199254740993', entity: true, observation: false }] : [] };
      if (payload.action === 'graph_edges') return { generation: '1', items: [] };
      if (payload.action === 'keyword_search') return { ...identity, items: [{ obsId: 'a', sessionId: 's', score: 1 }] };
      if (payload.action === 'semantic_search') return { ...identity, approximate: true, budget_exhausted: false, candidates: '1', items: [{ obsId: 'b', sessionId: 's', score: 1 }] };
      return {};
    }),
  };
  return { state, kv: state as unknown as StateKV, records };
}
beforeEach(() => {
  vi.clearAllMocks();
  extraction.mockReset();
  pipeline.mockReset().mockImplementation(async () => extraction);
});
afterEach(() => { setIndexedVector(null); setEmbeddingProvider(null); });
describe('indexed runtime contracts', () => {
  it('expands semantic bucket coverage without expanding candidate or result bounds', async () => {
    const { kv, state } = fixture();
    await new IndexedVector(kv, provider).search(new Float32Array([1, 0]), 1000);
    expect(state.retrieval).toHaveBeenCalledWith(expect.objectContaining({
      action: 'semantic_search', probes: 128, max_candidates: 4096, limit: 100,
    }));
    expect(state.list).not.toHaveBeenCalled();
    expect(state.pages).not.toHaveBeenCalled();
  });
  it('selects 256 probes explicitly before vector construction and freezes that identity', async () => {
    vi.resetModules();
    const candidate = await import('../src/state/indexed-vector.js');
    expect(candidate.configuredSemanticProbes()).toBe(128);
    candidate.configureCandidateSemanticProbes(256);
    const { kv, state } = fixture();
    await new candidate.IndexedVector(kv, provider).search(new Float32Array([1, 0]), 1000);
    expect(state.retrieval).toHaveBeenCalledWith(expect.objectContaining({ probes: 256, max_candidates: 4096, limit: 100 }));
    expect(() => candidate.configureCandidateSemanticProbes(256)).toThrow('before creating');
  });
  it('bounds graph admission while a retrieval is blocked', async () => {
    const { kv } = fixture();
    let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    const pending = Array.from({ length: 8 }, () => withSerializedGraphRetrieval(kv, async () => { await barrier; return 1; }));
    await expect(withSerializedGraphRetrieval(kv, async () => 1)).rejects.toMatchObject({ code: 'GRAPH_RETRIEVAL_RESOURCE_LIMIT' });
    release(); await Promise.all(pending);
    expect(await withSerializedGraphRetrieval(kv, async () => 2)).toBe(2);
  });
  it('rejects excess embedding admission while earlier batches remain pending', async () => {
    let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    extraction.mockImplementation(async (texts: string[]) => { await barrier; return { tolist: () => texts.map(() => Array(384).fill(0)) }; });
    const embedding = new IndexedLocalEmbedding();
    const pending = Array.from({ length: 8 }, () => embedding.embed('document'));
    await expect(embedding.embed('excess')).rejects.toThrow('queue exceeds 8');
    release(); await Promise.all(pending);
    expect(await embedding.embed('next')).toHaveLength(384);
  });
  it('loads the embedding from its explicit local model directory', async () => {
    extraction.mockImplementation(async (texts: string[]) => ({ tolist: () => texts.map(() => Array(384).fill(0)) }));
    await new IndexedLocalEmbedding().embed('offline model readiness');
    const cache = resolve(process.cwd(), '.cache', 'agentmemory', 'embeddings');
    expect(pipeline).toHaveBeenCalledWith('feature-extraction', resolve(cache, 'Xenova', 'all-MiniLM-L6-v2'), expect.objectContaining({
      dtype: 'q8', local_files_only: true, cache_dir: cache,
    }));
  });
  it('retrieves exact graph seeds and temporal facts without full scope reads', async () => {
    const { kv, state } = fixture();
    const edges = [edge('later', '2026-02-01'), edge('earlier', '2026-01-01')];
    const original = state.retrieval.getMockImplementation()!;
    state.retrieval.mockImplementation(async payload => payload.action === 'graph_edges' ? { generation: '1', items: edges.map((value, i) => ({ key: value.id, position: String(2 - i), value })) } : original(payload));
    const retrieval = new GraphRetrieval(kv);
    expect((await retrieval.searchByEntities(['AgentMemory']))[0].obsId).toBe('a');
    const temporal = await retrieval.temporalQuery('AgentMemory', '2026-01-15');
    expect(temporal.entity?.id).toBe('n1');
    expect(temporal.currentState.map(item => item.id)).toEqual(['earlier']);
    expect(state.pages).not.toHaveBeenCalled();
    expect(state.list).not.toHaveBeenCalled();
  });
  it('merges chunked seed flags and orders positions larger than safe integers', async () => {
    const { kv, state } = fixture();
    state.retrieval.mockResolvedValueOnce({ generation: '1', items: [{ key: 'z', id: 'z', position: '9007199254740993', entity: true, observation: false }] })
      .mockResolvedValueOnce({ generation: '1', items: [{ key: 'a', id: 'a', position: '9007199254740992', entity: false, observation: true }, { key: 'z', id: 'z', position: '9007199254740993', entity: false, observation: true }] });
    const result = await indexedGraphItems<IndexedSeed>(kv, [{ action: 'graph_seeds' }, { action: 'graph_seeds' }]);
    expect(result.map(item => item.id)).toEqual(['a', 'z']);
    expect(result[1]).toMatchObject({ entity: true, observation: true });
  });
  it('rejects generation changes and native resource errors instead of truncating', async () => {
    const { kv, state } = fixture();
    state.retrieval.mockResolvedValueOnce({ generation: '1', items: [] }).mockResolvedValueOnce({ generation: '2', items: [] });
    await expect(indexedGraphItems(kv, [{}, {}])).rejects.toMatchObject({ code: 'STATE_GRAPH_RECOVERY_REQUIRED' });
    state.retrieval.mockResolvedValueOnce({ error: 'STATE_RETRIEVAL_RESOURCE_LIMIT' });
    await expect(indexedGraphItems(kv, [{}])).rejects.toMatchObject({ code: 'STATE_RETRIEVAL_RESOURCE_LIMIT' });
  });
  it('fuses lexical and semantic expanded candidates before one final local rerank', async () => {
    const { kv, state } = fixture();
    const bm25 = new SearchIndex();
    const residentSearch = vi.spyOn(bm25, 'search');
    const hybrid = new HybridSearch(bm25, null, provider, kv);
    const results = await hybrid.searchWithExpansion('auth', 20, { reformulations: ['jwt', 'tokens'], temporalConcretizations: [], entityExtractions: [] });
    expect(new Set(results.map(item => item.observation.id))).toEqual(new Set(['a', 'b']));
    expect(rerank).toHaveBeenCalledTimes(1);
    expect(residentSearch).not.toHaveBeenCalled();
    expect(state.pages).not.toHaveBeenCalled();
  });
  it('fails closed for incomplete lexical coverage with no resident fallback', async () => {
    const { kv, state } = fixture();
    const original = state.retrieval.getMockImplementation()!;
    state.retrieval.mockImplementation(async payload => {
      const response = await original(payload) as { semantic?: Array<Record<string, unknown>> };
      if (payload.action === 'index_status') Object.assign(response.semantic![0], { lexical_count: '1', lexical_ready: false });
      return response;
    });
    await expect(new HybridSearch(new SearchIndex(), null, provider, kv).search('auth')).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(state.pages).not.toHaveBeenCalled();
  });
  it('rejects a durable dirty source even when the corpus marker remains ready', async () => {
    const { kv, state } = fixture();
    const original = state.retrieval.getMockImplementation()!;
    state.retrieval.mockImplementation(async payload => {
      const response = await original(payload) as { semantic?: Array<Record<string, unknown>> };
      if (payload.action === 'index_status') Object.assign(response.semantic![0], { dirty_count: '1', coverage_ready: false });
      return response;
    });
    await expect(new IndexedVector(kv, provider).ready()).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(state.pages).not.toHaveBeenCalled();
  });
  it('awaits durable upserts/deletes and does not retain newly indexed documents', async () => {
    const { kv, state, records } = fixture();
    records.set('new', { ...obs('new'), title: 'identifier_name', narrative: 'body' });
    setIndexedVector(new IndexedVector(kv, provider)); setEmbeddingProvider(provider);
    getSearchIndex().add(obs('new'));
    expect(getSearchIndex().size).toBe(0);
    expect(await vectorIndexAddGuarded('new', 's', 'identifier_name body', { kind: 'observation', logId: 'new' })).toBe(true);
    expect(state.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: 'semantic_upsert', items: [expect.objectContaining({ text: 'identifier_name body', embedding: 'AACAPwAAAAA=', source_scope: 'mem:obs:s', source_version: '1' })] }));
    vectorIndexRemove('new'); await flushIndexSave();
    expect(state.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: 'semantic_delete', ids: ['new'] }));
  });
  it('keeps failed deletes closed until explicit preparation repairs the runtime', async () => {
    const { kv, state, records } = fixture();
    const vector = new IndexedVector(kv, provider);
    const original = state.retrieval.getMockImplementation()!;
    state.retrieval.mockImplementation(async payload => payload.action === 'semantic_delete' ? { error: 'STATE_INDEX_NOT_READY' } : payload.action === 'index_prepare' ? { status: 'ready' } : original(payload));
    vector.remove('missing');
    await expect(vector.ready()).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    state.pages.mockImplementation(() => (async function* () { yield { items: [], next_cursor: null }; })() as never);
    await prepareIndexedCorpus(kv, vector, provider);
    expect(records.get('indexed-corpus')).toMatchObject({ status: 'ready' });
    await expect(vector.ready()).resolves.toBeUndefined();
  });
  it('leaves failed preparation pending', async () => {
    const { kv, state, records } = fixture();
    const original = state.retrieval.getMockImplementation()!;
    state.retrieval.mockImplementation(async payload => payload.action === 'source_prepare' ? { coverage_ready: false, items: [{ source_scope: 'mem:obs:s', id: 'a', source_version: '1' }] } : original(payload));
    await expect(prepareIndexedCorpus(kv, new IndexedVector(kv, provider), { ...provider, embedBatch: async () => { throw new Error('missing model'); } })).rejects.toThrow('missing model');
    expect(records.get('indexed-corpus')).toMatchObject({ status: 'pending' });
  });
  it('marks failed embedding coverage pending instead of presenting partial readiness', async () => {
    const { kv, records } = fixture();
    setIndexedVector(new IndexedVector(kv, provider)); setEmbeddingProvider({ ...provider, embed: async () => { throw new Error('offline'); } });
    expect(await vectorIndexAddGuarded('new', 's', 'text', { kind: 'observation', logId: 'new' })).toBe(false);
    expect(records.get('indexed-corpus')).toMatchObject({ status: 'pending' });
  });
  it('rejects stale embedding input before a revision guarded upsert', async () => {
    const { kv, state } = fixture();
    await expect(new IndexedVector(kv, provider).add('a', 's', new Float32Array([1, 0]), 'old content')).rejects.toMatchObject({ code: 'STATE_INDEX_NOT_READY' });
    expect(state.retrieval).not.toHaveBeenCalledWith(expect.objectContaining({ action: 'semantic_upsert' }));
  });
  it('prepares native source pages including orphan scopes without enumerating sessions', async () => {
    const { kv, state, records } = fixture();
    const original = state.retrieval.getMockImplementation()!;
    let sourcePages = 0;
    state.retrieval.mockImplementation(async payload => payload.action === 'index_prepare' ? { status: 'ready' } : payload.action === 'source_prepare' && sourcePages++ === 0 ? { coverage_ready: false, items: [{ source_scope: 'mem:obs:orphan', id: 'a', source_version: '1' }] } : original(payload));
    expect(await prepareIndexedCorpus(kv, new IndexedVector(kv, provider), provider)).toBe(2);
    expect(records.get('indexed-corpus')).toMatchObject({ status: 'ready', count: 2 });
    expect(state.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: 'semantic_upsert', items: [expect.objectContaining({ source_scope: 'mem:obs:orphan' })] }));
    expect(state.pages).not.toHaveBeenCalled();
    expect(state.list).not.toHaveBeenCalled();
  });
});
