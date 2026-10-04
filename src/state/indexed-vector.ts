import type { StateKV } from './kv.js';
import type { EmbeddingProvider } from '../types.js';
import { IndexedRetrievalError, retrievalRequest, type IndexedStatus } from './indexed-retrieval.js';
import { KV } from './schema.js';
import { logger } from '../logger.js';

const sharedVectors = new WeakMap<StateKV, IndexedVector>();
const MAX_QUERY_READINESS_ATTEMPTS = 3;
let semanticProbes: 128 | 256 = 128;
let vectorCreated = false;
export function configureCandidateSemanticProbes(probes: 256): void {
  if (vectorCreated || probes !== 256) throw new Error('Configure the 256-probe candidate before creating indexed vectors.');
  semanticProbes = probes;
}
export function configuredSemanticProbes(): number { return semanticProbes; }
export function getIndexedVector(kv: StateKV, provider: EmbeddingProvider): IndexedVector {
  let selected = sharedVectors.get(kv);
  if (!selected) { selected = new IndexedVector(kv, provider); sharedVectors.set(kv, selected); }
  if (selected.identity.model !== provider.name || selected.identity.dimensions !== provider.dimensions) throw new IndexedRetrievalError('STATE_SEMANTIC_IDENTITY_MISMATCH', 'Indexed provider changed.');
  return selected;
}

export class IndexedVector {
  readonly identity: { index_id: string; model: string; dimensions: number; generation: string };
  private pending: Promise<void> = Promise.resolve();
  private failure: unknown;
  private admitted = 0;
  constructor(private kv: StateKV, provider: EmbeddingProvider) {
    vectorCreated = true;
    this.identity = { index_id: 'observations', model: provider.name, dimensions: provider.dimensions, generation: 'local-v1' };
  }
  async ready(): Promise<void> {
    await this.flush();
    const status = await retrievalRequest<IndexedStatus>(this.kv, { action: 'index_status' });
    const marker = await this.kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus');
    const selected = status.semantic?.find(item => item.index_id === this.identity.index_id);
    if (status.version !== 1 || !status.capabilities?.includes('state::semantic_lsh_v1') || !status.capabilities.includes('state::indexed_graph_v1') ||
      ![KV.graphNodes, KV.graphEdges].every(scope => status.graph?.some(item => item.scope === scope && item.status === 'ready')) ||
      !selected || selected.status !== 'ready' || selected.lexical_ready !== true || !/^\d+$/.test(selected.count) || selected.lexical_count !== selected.count ||
      selected.source_kind !== 'agentmemory' || selected.source_prepared !== true || selected.dirty_count !== '0' || selected.coverage_ready !== true ||
      selected.model !== this.identity.model || selected.dimensions !== this.identity.dimensions || selected.generation !== this.identity.generation ||
      marker?.status !== 'ready' || marker.model !== this.identity.model || marker.dimensions !== this.identity.dimensions || marker.generation !== this.identity.generation) {
      throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Prepare the complete semantic corpus with the selected embedding identity before searching.');
    }
  }
  private async queryWithReadinessRetry<T>(query: () => Promise<T>): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      await this.ready();
      try {
        return await query();
      } catch (error) {
        if (
          attempt >= MAX_QUERY_READINESS_ATTEMPTS ||
          !(error instanceof IndexedRetrievalError) ||
          error.code !== 'STATE_SEMANTIC_SOURCE_NOT_READY'
        ) {
          throw error;
        }
      }
    }
  }
  async configure(): Promise<void> {
    await this.pending;
    await retrievalRequest(this.kv, { action: 'semantic_configure', ...this.identity, source_kind: 'agentmemory' });
    this.failure = undefined;
  }
  async markIncomplete(): Promise<void> {
    await this.kv.set(KV.config, 'indexed-corpus', { ...this.identity, status: 'pending' });
  }
  async add(id: string, sessionId: string, embedding: Float32Array, text: string, sourceScope = KV.observations(sessionId)): Promise<void> {
    const source = await this.kv.getVersioned<{ title: string; narrative?: string; content?: string }>(sourceScope, id);
    const content = sourceScope === KV.memories ? source.value?.content : source.value?.narrative;
    if (!source.exists || typeof source.version !== 'string' || !/^\d+$/.test(source.version) ||
      `${source.value?.title} ${content ?? ''}`.slice(0, 16_000) !== text) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Searchable source changed before indexed upsert.');
    const bytes = Buffer.alloc(embedding.length * 4);
    for (let i = 0; i < embedding.length; i++) bytes.writeFloatLE(embedding[i], i * 4);
    await this.mutate({ action: 'semantic_upsert', ...this.identity, items: [{ id, session_id: sessionId, embedding: bytes.toString('base64'), text, source_scope: sourceScope, source_version: source.version }] });
  }
  async keywordSearch(query: string, limit: number): Promise<Array<{ obsId: string; sessionId: string; score: number }>> {
    return this.queryWithReadinessRetry(async () => {
      const response = await retrievalRequest<{ items: Array<{ obsId: string; sessionId: string; score: number }>; model: string; dimensions: number; generation: string }>(this.kv, { action: 'keyword_search', ...this.identity, query, limit: Math.min(limit, 100) });
      this.validateIdentity(response);
      if (!Array.isArray(response.items)) throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid lexical candidate response.');
      return response.items;
    });
  }
  remove(id: string): void {
    void this.mutate({ action: 'semantic_delete', ...this.identity, ids: [id] }).catch(error => { this.failure = error; });
  }
  private mutate(payload: Record<string, unknown>): Promise<void> {
    if (this.admitted >= 128) throw new IndexedRetrievalError('STATE_RETRIEVAL_RESOURCE_LIMIT', 'Indexed mutation queue exceeds 128 records.');
    this.admitted++;
    const operation = this.pending.then(async () => { await retrievalRequest(this.kv, payload); });
    this.pending = operation.catch(error => { this.failure = error; }).finally(() => { this.admitted--; });
    return operation;
  }
  async flush(): Promise<void> {
    await this.pending;
    if (this.failure) throw this.failure;
  }
  async search(embedding: Float32Array, limit: number): Promise<Array<{ obsId: string; sessionId: string; score: number }>> {
    const bytes = Buffer.alloc(embedding.length * 4);
    for (let i = 0; i < embedding.length; i++) bytes.writeFloatLE(embedding[i], i * 4);
    return this.queryWithReadinessRetry(async () => {
      const result = await retrievalRequest<{ items: Array<{ obsId: string; sessionId: string; score: number }>; approximate: boolean; budget_exhausted: boolean; candidates: string; model: string; dimensions: number; generation: string }>(this.kv, {
        action: 'semantic_search', ...this.identity, embedding: bytes.toString('base64'), limit: Math.min(limit, 100), max_candidates: 4096, probes: semanticProbes,
      });
      this.validateIdentity(result);
      if (!Array.isArray(result.items) || result.approximate !== true || typeof result.budget_exhausted !== 'boolean') throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid semantic candidate response.');
      logger.info('Indexed semantic candidates', { approximate: true, budgetExhausted: result.budget_exhausted, candidates: result.candidates });
      return result.items;
    });
  }
  private validateIdentity(value: { model: string; dimensions: number; generation: string }): void {
    if (value.model !== this.identity.model || value.dimensions !== this.identity.dimensions || value.generation !== this.identity.generation) throw new IndexedRetrievalError('STATE_SEMANTIC_IDENTITY_MISMATCH', 'Indexed candidate identity changed.');
  }
}
