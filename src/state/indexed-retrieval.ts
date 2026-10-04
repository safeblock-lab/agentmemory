import { InvocationError } from 'iii-sdk';
import type { StateKV } from './kv.js';
import type { GraphEdge } from '../types.js';

export class IndexedRetrievalError extends Error {
  constructor(readonly code: string, detail: string) { super(`${code}: ${detail}`); }
}
export interface IndexedSeed { key: string; id: string; position: string; entity: boolean; observation: boolean }
export interface IndexedEdge { key: string; position: string; value: GraphEdge }
export interface IndexedStatus {
  version: number;
  capabilities: string[];
  graph: Array<{ scope: string; status: string; revision: string }>;
  semantic: Array<{ index_id: string; model: string; dimensions: number; generation: string; status: string; count: string; lexical_count: string; lexical_ready: boolean; source_kind: string; source_prepared: boolean; dirty_count: string; coverage_ready: boolean }>;
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
