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
  const [status, marker] = await Promise.all([
    retrievalRequest<IndexedStatus>(kv, { action: 'index_status' }),
    kv.get<{ status: string; model: string; dimensions: number; generation: string }>(KV.config, 'indexed-corpus'),
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
    lexicalReady: selected.lexical_ready === true,
    sourcePrepared: selected.source_prepared === true,
    coverageReady: selected.coverage_ready === true,
    identity: { indexId: selected.index_id, model: selected.model, dimensions: selected.dimensions, generation: selected.generation },
  };
}
export async function retrievalRequest<T>(kv: StateKV, payload: Record<string, unknown>): Promise<T> {
  let result: unknown;
  try {
    result = await kv.retrieval<unknown>(payload);
  } catch (error) {
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
export async function indexedGraphItems<T extends { key: string; position: string }>(
  kv: StateKV, requests: Record<string, unknown>[], signal?: AbortSignal, retain?: (bytes: number) => void,
): Promise<T[]> {
  const merged = new Map<string, T>();
  let generation: string | undefined;
  let bytes = 0;
  for (const payload of requests) {
    signal?.throwIfAborted();
    const result = await retrievalRequest<{ items: T[]; generation: string }>(kv, {
      ...payload, max_items: 16_384, max_bytes: 8 * 1024 * 1024,
    });
    if (!Array.isArray(result.items) || typeof result.generation !== 'string') throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid indexed graph response.');
    if (generation !== undefined && generation !== result.generation) throw new IndexedRetrievalError('STATE_GRAPH_RECOVERY_REQUIRED', 'Graph generation changed between indexed requests.');
    generation = result.generation;
    for (const item of result.items) {
      if (typeof item.key !== 'string' || !/^\d+$/.test(item.position)) throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid indexed insertion position.');
      const previous = merged.get(item.key);
      if (previous) {
        for (const field of ['entity', 'observation'] as const) {
          if (field in previous && field in item) (previous as unknown as Record<string, unknown>)[field] ||= (item as unknown as Record<string, unknown>)[field];
        }
        continue;
      }
      const itemBytes = Buffer.byteLength(JSON.stringify(item));
      bytes += itemBytes;
      if (bytes > 64 * 1024 * 1024) throw new IndexedRetrievalError('STATE_RETRIEVAL_RESOURCE_LIMIT', 'Indexed graph response exceeds 64 MiB.');
      retain?.(itemBytes);
      merged.set(item.key, item);
    }
  }
  signal?.throwIfAborted();
  return [...merged.values()].sort(comparePositions);
}
export function chunks<T>(items: T[], size = 512): T[][] {
  const result: T[][] = [];
  for (let offset = 0; offset < items.length; offset += size) result.push(items.slice(offset, offset + size));
  return result;
}
