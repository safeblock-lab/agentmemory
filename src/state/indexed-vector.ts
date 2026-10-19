import type { StateKV } from './kv.js';
import type { EmbeddingProvider } from '../types.js';
import { IndexedRetrievalError, retrievalRequest, type IndexedStatus } from './indexed-retrieval.js';
import { KV } from './schema.js';
import { logger } from '../logger.js';
import { isStateCounter } from './state-transactions.js';

const sharedVectors = new WeakMap<StateKV, IndexedVector>();
const MAX_QUERY_READINESS_ATTEMPTS = 3;
const RECOVERY_MAX_PAGES = 16;
const RECOVERY_MAX_ROWS = 256;
const RECOVERY_MAX_EMBEDDINGS = 128;
const RECOVERY_TIMEOUT_MS = 60_000;
const RECOVERY_SOURCE_READ_CONCURRENCY = 2;
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
  private recovery: Promise<number> | undefined;
  constructor(private kv: StateKV, provider: EmbeddingProvider) {
    vectorCreated = true;
    this.identity = { index_id: 'observations', model: provider.name, dimensions: provider.dimensions, generation: 'local-v1' };
  }
  async ready(): Promise<void> {
    await this.flush();
    const status = await retrievalRequest<IndexedStatus>(this.kv, { action: 'index_status' });
    const marker = await this.kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus');
    const selected = this.validateBaseReadiness(status, marker);
    if (selected.source_prepared !== true || selected.dirty_count !== '0' || selected.coverage_ready !== true) {
      throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Prepare the complete semantic corpus with the selected embedding identity before searching.');
    }
  }
  recoverDirtySources(provider: EmbeddingProvider): Promise<number> {
    if (this.recovery) return this.recovery;
    const recovery = this.recover(provider);
    this.recovery = recovery;
    void recovery.finally(() => { if (this.recovery === recovery) this.recovery = undefined; }).catch(() => {});
    return recovery;
  }
  private async recover(provider: EmbeddingProvider): Promise<number> {
    let phase = 'identity';
    try {
    if (provider.name !== this.identity.model || provider.dimensions !== this.identity.dimensions) {
      throw new IndexedRetrievalError('STATE_SEMANTIC_IDENTITY_MISMATCH', 'Indexed recovery provider changed.');
    }
    phase = 'flush';
    await this.flush();
    phase = 'status';
    const status = await retrievalRequest<IndexedStatus>(this.kv, { action: 'index_status' });
    phase = 'marker_read';
    const marker = await this.kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus');
    phase = 'readiness';
    const selected = this.validateBaseReadiness(status, marker, true);
    if (selected.source_prepared === true && selected.dirty_count === '0' && selected.coverage_ready === true) {
      phase = 'finish';
      if (marker?.status !== 'pending' || await this.finishRecovery()) return 0;
    }
    const initialDirty = Number(selected.dirty_count);
    if (!Number.isSafeInteger(initialDirty) || initialDirty < 0 || initialDirty > RECOVERY_MAX_EMBEDDINGS) {
      throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery exceeds its source limit.');
    }
    const deadline = Date.now() + RECOVERY_TIMEOUT_MS;
    let processed = 0;
    let embedded = 0;
    for (let pageNumber = 0; pageNumber < RECOVERY_MAX_PAGES; pageNumber++) {
      if (Date.now() >= deadline) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery timed out.');
      phase = 'source_prepare';
      const page = await retrievalRequest<{ processed: number; items: Array<{ source_scope: string; id: string; source_version: string }>; coverage_ready: boolean }>(this.kv, {
        action: 'source_prepare', ...this.identity, max_rows: RECOVERY_MAX_ROWS, max_bytes: 1_048_576,
      });
      if (!Number.isSafeInteger(page.processed) || page.processed < 0 || page.processed > RECOVERY_MAX_ROWS ||
        !Array.isArray(page.items) || page.items.length > RECOVERY_MAX_ROWS || typeof page.coverage_ready !== 'boolean') {
        throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Invalid incremental semantic source page.');
      }
      processed += page.processed;
      embedded += page.items.length;
      if (processed > RECOVERY_MAX_PAGES * RECOVERY_MAX_ROWS || embedded > RECOVERY_MAX_EMBEDDINGS) {
        throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery exceeds its work limit.');
      }
      for (let offset = 0; offset < page.items.length; offset += 32) {
        if (Date.now() >= deadline) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery timed out.');
        const batch = page.items.slice(offset, offset + 32);
        phase = 'source_read';
        const sources: Array<(typeof batch)[number] & { sessionId: string; text: string }> = [];
        for (let sourceOffset = 0; sourceOffset < batch.length; sourceOffset += RECOVERY_SOURCE_READ_CONCURRENCY) {
          if (Date.now() >= deadline) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery timed out.');
          let failed = false;
          let firstFailure: unknown;
          const reads = batch.slice(sourceOffset, sourceOffset + RECOVERY_SOURCE_READ_CONCURRENCY).map(async ref => {
            if (!ref || typeof ref.id !== 'string' || !ref.id || Buffer.byteLength(ref.id) > 256
              || !isStateCounter(ref.source_version) || ref.source_version === '0'
              || typeof ref.source_scope !== 'string' || Buffer.byteLength(ref.source_scope) > 512
              || (ref.source_scope !== KV.memories && (!ref.source_scope.startsWith('mem:obs:') || !ref.source_scope.slice('mem:obs:'.length)))) {
              throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Invalid incremental semantic source reference.');
            }
            const source = await this.kv.getVersioned<{ id: string; title: string; narrative?: string; content?: string; sessionId?: string; sessionIds?: string[]; embeddingReuse?: unknown }>(ref.source_scope, ref.id);
            if (!source.exists || !source.value || source.value.id !== ref.id || source.version !== ref.source_version || typeof source.value.title !== 'string'
              || ref.source_scope !== KV.memories && source.value.sessionId !== ref.source_scope.slice('mem:obs:'.length)) {
              throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Semantic source changed during incremental recovery.');
            }
            if (source.value.embeddingReuse !== undefined) {
              throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Metadata embedding reuse remains pending; automatic embedding is forbidden.');
            }
            const memory = ref.source_scope === KV.memories;
            const content = memory ? source.value.content : source.value.narrative;
            if (content !== undefined && typeof content !== 'string') throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Invalid incremental semantic source content.');
            return { ...ref, sessionId: memory ? source.value.sessionIds?.[0] ?? 'memory' : source.value.sessionId ?? ref.source_scope.slice('mem:obs:'.length), text: `${source.value.title} ${content ?? ''}`.slice(0, 16_000) };
          });
          const settled = await Promise.allSettled(reads.map(read => read.catch(error => {
            if (!failed) { failed = true; firstFailure = error; }
            throw error;
          })));
          if (failed) throw firstFailure;
          for (const result of settled) if (result.status === 'fulfilled') sources.push(result.value);
        }
        phase = 'local_embedding';
        const embeddings = await provider.embedBatch(sources.map(source => source.text));
        if (Date.now() >= deadline) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery timed out.');
        if (embeddings.length !== sources.length || embeddings.some(value => value.length !== provider.dimensions || value.some(number => !Number.isFinite(number)))) {
          throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic embeddings are incomplete.');
        }
        for (let i = 0; i < sources.length; i++) {
          const source = sources[i];
          phase = 'semantic_upsert';
          await this.add(source.id, source.sessionId, embeddings[i], source.text, source.source_scope, source.source_version);
        }
      }
      if (page.coverage_ready) {
        phase = 'finish';
        if (await this.finishRecovery()) return embedded;
      }
    }
    throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic recovery exceeds its page limit.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown recovery failure';
      const candidate = error && typeof error === 'object' && 'code' in error ? error.code : undefined;
      const code = typeof candidate === 'string' && /^STATE_[A-Z0-9_]+$/.test(candidate)
        ? candidate : message.match(/STATE_[A-Z0-9_]+/)?.[0] ?? 'STATE_INDEX_NOT_READY';
      logger.warn('Indexed local dirty recovery failed', { phase, code });
      throw new IndexedRetrievalError(code, `Recovery phase=${phase}; ${message}`);
    }
  }
  private async finishRecovery(): Promise<boolean> {
    await this.flush();
    const status = await retrievalRequest<IndexedStatus>(this.kv, { action: 'index_status' });
    const marker = await this.kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus');
    const selected = this.validateBaseReadiness(status, marker, true);
    if (selected.source_prepared === true && Number(selected.dirty_count) > 0) return false;
    if (selected.source_prepared !== true || selected.dirty_count !== '0' || selected.coverage_ready !== true) {
      throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Incremental semantic source coverage remains incomplete.');
    }
    if (marker?.status === 'pending') {
      const count = Number(selected.count);
      if (!Number.isSafeInteger(count)) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Invalid indexed corpus count.');
      await this.kv.set(KV.config, 'indexed-corpus', { ...this.identity, status: 'ready', count });
    }
    try { await this.ready(); }
    catch (error) {
      await this.markIncomplete();
      if (!(error instanceof IndexedRetrievalError) || error.code !== 'STATE_INDEX_NOT_READY') throw error;
      const fresh = await retrievalRequest<IndexedStatus>(this.kv, { action: 'index_status' });
      const currentMarker = await this.kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus');
      const current = this.validateBaseReadiness(fresh, currentMarker, true);
      if (current.source_prepared === true && Number(current.dirty_count) > 0) return false;
      throw error;
    }
    return true;
  }
  private validateBaseReadiness(status: IndexedStatus, marker: { status: string; model: string; dimensions: number; generation: string } | null, allowPendingMarker = false) {
    const selected = status.semantic?.find(item => item.index_id === this.identity.index_id);
    if (status.version !== 1 || !status.capabilities?.includes('state::semantic_lsh_v1') || !status.capabilities.includes('state::indexed_graph_v1') ||
      ![KV.graphNodes, KV.graphEdges].every(scope => status.graph?.some(item => item.scope === scope && item.status === 'ready')) ||
      !selected || selected.status !== 'ready' || selected.lexical_ready !== true || !Number.isSafeInteger(Number(selected.count)) ||
      !Number.isSafeInteger(Number(selected.lexical_count)) || Number(selected.lexical_count) !== Number(selected.count) ||
      selected.source_kind !== 'agentmemory' || !/^\d+$/.test(String(selected.dirty_count)) ||
      selected.model !== this.identity.model || selected.dimensions !== this.identity.dimensions || selected.generation !== this.identity.generation ||
      (marker?.status !== 'ready' && !(allowPendingMarker && marker?.status === 'pending')) || marker.model !== this.identity.model || marker.dimensions !== this.identity.dimensions || marker.generation !== this.identity.generation) {
      throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Prepare the complete semantic corpus with the selected embedding identity before searching.');
    }
    return selected;
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
  async add(id: string, sessionId: string, embedding: Float32Array, text: string, sourceScope = KV.observations(sessionId), expectedVersion?: string): Promise<void> {
    const source = await this.kv.getVersioned<{ title: string; narrative?: string; content?: string; embeddingReuse?: unknown }>(sourceScope, id);
    const content = sourceScope === KV.memories ? source.value?.content : source.value?.narrative;
    if (!source.exists || !isStateCounter(source.version) || expectedVersion !== undefined && (source.version !== expectedVersion || source.value?.embeddingReuse !== undefined) ||
      `${source.value?.title} ${content ?? ''}`.slice(0, 16_000) !== text) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Searchable source changed before indexed upsert.');
    const bytes = Buffer.alloc(embedding.length * 4);
    for (let i = 0; i < embedding.length; i++) bytes.writeFloatLE(embedding[i], i * 4);
    await this.mutate({ action: 'semantic_upsert', ...this.identity, items: [{ id, session_id: sessionId, embedding: bytes.toString('base64'), text, source_scope: sourceScope, source_version: source.version }] });
  }
  async keywordSearch(query: string, limit: number, project?: string): Promise<Array<{ obsId: string; sessionId: string; score: number }>> {
    return this.queryWithReadinessRetry(async () => {
      const scope = project?.trim() ? { project: project.trim() } : { global: true };
      const response = await retrievalRequest<{ items: Array<{ obsId: string; sessionId: string; score: number }>; model: string; dimensions: number; generation: string }>(this.kv, { action: 'keyword_search', ...this.identity, ...scope, query, limit: Math.min(limit, 100) });
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
  async search(embedding: Float32Array, limit: number, project?: string): Promise<Array<{ obsId: string; sessionId: string; score: number }>> {
    const bytes = Buffer.alloc(embedding.length * 4);
    for (let i = 0; i < embedding.length; i++) bytes.writeFloatLE(embedding[i], i * 4);
    return this.queryWithReadinessRetry(async () => {
      const scope = project?.trim() ? { project: project.trim() } : { global: true };
      const result = await retrievalRequest<{ items: Array<{ obsId: string; sessionId: string; score: number }>; approximate: boolean; budget_exhausted: boolean; candidates: string; model: string; dimensions: number; generation: string }>(this.kv, {
        action: 'semantic_search', ...this.identity, ...scope, embedding: bytes.toString('base64'), limit: Math.min(limit, 100), max_candidates: 4096, probes: semanticProbes,
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
