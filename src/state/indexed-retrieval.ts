import { InvocationError } from 'iii-sdk';
import type { StateKV } from './kv.js';
import type { GraphEdge } from '../types.js';
import type { EmbeddingProvider } from '../types.js';
import { KV } from './schema.js';

export class IndexedRetrievalError extends Error {
  constructor(readonly code: string, detail: string) { super(`${code}: ${detail}`); }
}
export interface IndexedSeed { key: string; id: string; position: string; entity: boolean; observation: boolean }
export interface IndexedEdge { key: string; position: string; value: GraphEdge }
export interface IndexedStatus {
  version: number;
  capabilities: string[];
  graph: Array<{ scope: string; status: string; revision: string }>;
  semantic: Array<{ index_id: string; model: string; dimensions: number; generation: string; status: string; count: number | string; lexical_count: number | string; lexical_ready: boolean; source_kind: string; source_prepared: boolean; dirty_count: number | string; coverage_ready: boolean }>;
}
export function nativeCount(value: unknown): number | null {
  if (typeof value === 'number') return Number.isSafeInteger(value) && value >= 0 ? value : null;
  if (typeof value !== 'string' || !/^\d+$/.test(value)) return null;
  const count = Number(value);
  return Number.isSafeInteger(count) ? count : null;
}

export async function readNativeIndexStatus(kv: StateKV, provider: EmbeddingProvider | null) {
  const [status, marker, projectIndexes] = await Promise.all([
    retrievalRequest<IndexedStatus>(kv, { action: 'index_status' }),
    kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus'),
    kv.get<ProjectIndexProgress>(KV.config, 'project-index-maintenance'),
  ]);
  const selected = provider && status.version === 1 && Array.isArray(status.semantic)
    ? status.semantic.find(item => item.index_id === 'observations' && item.model === provider.name &&
      item.dimensions === provider.dimensions && item.generation === 'local-v1') : undefined;
  if (!selected) return null;
  const lexicalCount = nativeCount(selected.lexical_count);
  const vectorCount = nativeCount(selected.count);
  const dirtyCount = nativeCount(selected.dirty_count);
  const identityMatches = marker?.model === selected.model && marker.dimensions === selected.dimensions && marker.generation === selected.generation;
  const graph = [KV.graphNodes, KV.graphEdges].map(scope => {
    const entry = status.graph?.find(item => item.scope === scope);
    return { scope, status: typeof entry?.status === 'string' ? entry.status : null, revision: typeof entry?.revision === 'string' ? entry.revision : null };
  });
  const graphReady = status.capabilities?.includes('state::indexed_graph_v1') === true && graph.every(item => item.status === 'ready');
  const ready = selected.status === 'ready' && selected.lexical_ready === true &&
    selected.source_kind === 'agentmemory' && selected.source_prepared === true && selected.coverage_ready === true &&
    dirtyCount === 0 && lexicalCount !== null && lexicalCount === vectorCount &&
    identityMatches && marker?.status === 'ready' &&
    status.capabilities?.includes('state::semantic_lsh_v1') === true &&
    graphReady;
  return {
    lexicalCount, vectorCount, dirtyCount, ready, graphReady, graph,
    projectIndexes: projectIndexes ?? null,
    lexicalReady: selected.lexical_ready === true,
    sourcePrepared: selected.source_prepared === true,
    coverageReady: selected.coverage_ready === true,
    identity: { indexId: selected.index_id, model: selected.model, dimensions: selected.dimensions, generation: selected.generation },
  };
}
export async function retrievalRequest<T>(kv: StateKV, payload: Record<string, unknown>, options?: { timeoutMs?: number }): Promise<T> {
  let result: unknown;
  try {
    result = await (options === undefined
      ? kv.retrieval<unknown>(payload)
      : kv.retrieval<unknown>(payload, options));
  } catch (error) {
    if (error instanceof InvocationError && ['STATE_RECENT_CURSOR_STALE', 'STATE_TX_INVALID_REQUEST'].includes(error.code)) {
      throw new IndexedRetrievalError(error.code, 'Native retrieval cursor or request is no longer valid.');
    }
    if (error instanceof InvocationError && error.code === 'STATE_RETRIEVAL_RESOURCE_LIMIT') {
      throw new IndexedRetrievalError('STATE_RETRIEVAL_RESOURCE_LIMIT', 'Native indexed retrieval exceeded its response resource limit.');
    }
    throw error;
  }
  if (!result || typeof result !== 'object') throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Native indexed retrieval is unavailable. Prepare the compatible engine indexes explicitly.');
  const value = result as Record<string, unknown>;
  if (typeof value.error === 'string' || typeof value.code === 'string') {
    throw new IndexedRetrievalError(String(value.code ?? value.error), 'Native indexed retrieval rejected the request.');
  }
  return result as T;
}
export function comparePositions(a: { position: string }, b: { position: string }): number {
  const left = BigInt(a.position), right = BigInt(b.position);
  return left < right ? -1 : left > right ? 1 : 0;
}
export function chunks<T>(items: T[], size = 512): T[][] {
  const result: T[][] = [];
  for (let offset = 0; offset < items.length; offset += size) result.push(items.slice(offset, offset + size));
  return result;
}

interface ProjectIndexProgress {
  next: number;
  nextAttemptAt: number;
  sources: Record<string, { status: string; processed: number; cursor: string; revision: string; incompleteRows?: string }>;
  errorCode?: string;
}

export function createProjectIndexMaintenance(
  kv: StateKV, paused: () => boolean, foregroundBusy: () => boolean,
  log: (message: string) => void,
): () => Promise<void> {
  let running = false;
  return async () => {
    if (paused() || foregroundBusy() || running || !kv.indexedRetrieval) return;
    running = true;
    try {
      const saved = await kv.get<ProjectIndexProgress>(KV.config, 'project-index-maintenance');
      const progress: ProjectIndexProgress = saved ?? { next: 0, nextAttemptAt: 0, sources: {} };
      if (progress.nextAttemptAt > Date.now() || paused() || foregroundBusy()) return;
      const ordinal = Number.isInteger(progress.next) ? Math.abs(progress.next % 3) : 0;
      const source = ['sessions', 'summaries', 'vectors'][ordinal]!;
      try {
        const payload = source === 'vectors'
          ? { action: 'project_vectors_prepare', index_id: 'observations', max_rows: 16, max_bytes: 65_536 }
          : { action: 'project_catalog_prepare', source, max_rows: 16, max_bytes: 65_536 };
        const result = await retrievalRequest<Record<string, unknown>>(kv, payload);
        if (!['pending', 'partial', 'ready'].includes(String(result.status)) ||
            !Number.isSafeInteger(result.processed) || Number(result.processed) < 0 || Number(result.processed) > 16 ||
            typeof result.cursor !== 'string' || typeof result.revision !== 'string') {
          throw new IndexedRetrievalError('STATE_TX_INVALID_RESPONSE', 'Invalid project index maintenance response.');
        }
        progress.sources[source] = { status: String(result.status), processed: Number(result.processed),
          cursor: result.cursor, revision: result.revision,
          ...(typeof result.incomplete_rows === 'string' ? { incompleteRows: result.incomplete_rows } : {}) };
        delete progress.errorCode;
        progress.nextAttemptAt = Date.now() + 60_000;
      } catch (error) {
        progress.errorCode = error instanceof IndexedRetrievalError ? error.code : 'STATE_INDEX_NOT_READY';
        progress.nextAttemptAt = Date.now() + 15 * 60_000;
        log(`Project index maintenance deferred (${progress.errorCode}).`);
      }
      // Native cursors commit with each bounded cut; this record is progress, not authority.
      progress.next = (ordinal + 1) % 3;
      await kv.set(KV.config, 'project-index-maintenance', progress);
    } finally { running = false; }
  };
}
