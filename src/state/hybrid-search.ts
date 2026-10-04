import { SearchIndex } from "./search-index.js";
import { VectorIndex } from "./vector-index.js";
import type {
  EmbeddingProvider,
  HybridSearchResult,
  CompressedObservation,
  Memory,
  QueryExpansion,
} from "../types.js";
import { memoryToObservation } from "./memory-utils.js";
import type { StateKV } from "./kv.js";
import { KV } from "./schema.js";
import {
  GraphRetrieval,
  type GraphRetrievalResult,
} from "../functions/graph-retrieval.js";
import { GraphRetrievalResourceError } from "../functions/graph-retrieval-read.js";
import { extractEntitiesFromQuery } from "../functions/query-expansion.js";
import { rerank } from "./reranker.js";
import { IndexedVector, getIndexedVector } from './indexed-vector.js';
import { IndexedRetrievalError } from './indexed-retrieval.js';

const RRF_K = 60;

function normalizeForFusion(scores: number[]): number[] {
  if (scores.some((score) => !Number.isFinite(score))) {
    throw new Error("Search ranking contains a non-finite score.");
  }
  const min = Math.min(...scores);
  const max = Math.max(...scores);
  return scores.map((score) => max === min ? 0 : (score - min) / (max - min));
}

function fuseRerankedResults(
  original: HybridSearchResult[],
  reranked: HybridSearchResult[],
): HybridSearchResult[] {
  if (original.length !== reranked.length) throw new Error("Reranker changed the candidate set.");

  const originalPositions = new Map<string, number>();
  for (const [index, result] of original.entries()) {
    if (originalPositions.has(result.observation.id)) throw new Error("Search candidates contain duplicate observation IDs.");
    originalPositions.set(result.observation.id, index);
  }

  const sourceScores = normalizeForFusion(original.map((result) => result.combinedScore));
  const rerankScores = normalizeForFusion(reranked.map((result) => result.combinedScore));
  const seen = new Set<string>();
  const fused = reranked.map((result, index) => {
    const id = result.observation.id;
    const originalIndex = originalPositions.get(id);
    if (originalIndex === undefined || seen.has(id)) throw new Error("Reranker changed candidate identity.");
    seen.add(id);
    return {
      result: { ...result, combinedScore: sourceScores[originalIndex] + rerankScores[index] },
      originalIndex,
    };
  });
  if (seen.size !== original.length) throw new Error("Reranker omitted search candidates.");

  return fused
    .sort((a, b) => b.result.combinedScore - a.result.combinedScore || a.originalIndex - b.originalIndex)
    .map(({ result }) => result);
}

export class HybridSearch {
  private graphRetrieval: GraphRetrieval;
  private indexedVector: IndexedVector | null;

  constructor(
    private bm25: SearchIndex,
    private vector: VectorIndex | null,
    private embeddingProvider: EmbeddingProvider | null,
    private kv: StateKV,
    private bm25Weight = 0.4,
    private vectorWeight = 0.6,
    private graphWeight = 0.3,
    private rerankEnabled = kv.indexedRetrieval || process.env.RERANK_ENABLED === "true",
  ) {
    this.graphRetrieval = new GraphRetrieval(kv);
    this.indexedVector = kv.indexedRetrieval && embeddingProvider ? getIndexedVector(kv, embeddingProvider) : null;
  }

  async search(
    query: string,
    limit = 20,
    signal?: AbortSignal,
  ): Promise<HybridSearchResult[]> {
    return this.finalRerank(query, await this.tripleStreamSearch(query, limit, undefined, signal), limit, signal);
  }

  async searchWithExpansion(
    query: string,
    limit: number,
    expansion: QueryExpansion,
    signal?: AbortSignal,
  ): Promise<HybridSearchResult[]> {
    const allQueries = [
      query,
      ...expansion.reformulations,
      ...expansion.temporalConcretizations,
    ];

    const allEntities = [
      ...expansion.entityExtractions,
      ...extractEntitiesFromQuery(query),
    ];

    if (allQueries.length > 16) throw new IndexedRetrievalError('STATE_RETRIEVAL_RESOURCE_LIMIT', 'Query expansion exceeds 16 variants.');
    const resultSets: HybridSearchResult[][] = [];
    for (const q of new Set(allQueries)) resultSets.push(await this.tripleStreamSearch(q, limit, allEntities, signal));

    const merged = new Map<string, HybridSearchResult>();
    for (const results of resultSets) {
      for (const r of results) {
        const existing = merged.get(r.observation.id);
        if (!existing || r.combinedScore > existing.combinedScore) {
          merged.set(r.observation.id, r);
        }
      }
    }

    const fused = Array.from(merged.values())
      .sort(
        (a, b) =>
          b.combinedScore - a.combinedScore ||
          (a.observation.id < b.observation.id ? -1 : a.observation.id > b.observation.id ? 1 : 0),
      )
      .slice(0, Math.max(limit, 50));
    return this.finalRerank(query, fused, limit, signal);
  }

  private async tripleStreamSearch(
    query: string,
    limit: number,
    entityHints?: string[],
    signal?: AbortSignal,
  ): Promise<HybridSearchResult[]> {
    const candidateLimit = Math.min(100, Math.max(50, limit * 2));
    if (this.kv.indexedRetrieval && !this.indexedVector) throw new IndexedRetrievalError('STATE_INDEX_NOT_READY', 'Local embedding provider is required.');
    const bm25Results = this.indexedVector
      ? await this.indexedVector.keywordSearch(query, candidateLimit)
      : this.bm25.search(query, limit * 2);

    let vectorResults: Array<{
      obsId: string;
      sessionId: string;
      score: number;
    }> = [];
    let queryEmbedding: Float32Array | null = null;

    if (this.indexedVector && this.embeddingProvider) {
      queryEmbedding = await this.embeddingProvider.embed(query);
      vectorResults = await this.indexedVector.search(queryEmbedding, candidateLimit);
    } else if (this.vector && this.embeddingProvider && this.vector.size > 0) {
      try {
        queryEmbedding = await this.embeddingProvider.embed(query);
        vectorResults = this.vector.search(queryEmbedding, limit * 2);
      } catch {
        // fall through to BM25-only
      }
    }

    const entities =
      entityHints && entityHints.length > 0
        ? entityHints
        : extractEntitiesFromQuery(query);
    const topVectorObs = vectorResults.slice(0, 5).map((result) => result.obsId);
    let graphResults: GraphRetrievalResult[] = [];
    if (entities.length > 0 || topVectorObs.length > 0) {
      try {
        const graph = await this.graphRetrieval.searchByEntitiesAndChunks(
          entities,
          topVectorObs,
          2,
          1,
          limit,
          5,
          signal,
        );
        graphResults = [...graph.entities, ...graph.chunks];
      } catch (error) {
        if (signal?.aborted) {
          throw signal.reason instanceof Error
            ? signal.reason
            : new Error("Graph retrieval was cancelled.");
        }
        if (this.kv.indexedRetrieval) {
          const optionalBudgetExhausted =
            error instanceof GraphRetrievalResourceError ||
            (error instanceof IndexedRetrievalError &&
              error.code === "STATE_RETRIEVAL_RESOURCE_LIMIT");
          if (
            !optionalBudgetExhausted ||
            (bm25Results.length === 0 && vectorResults.length === 0)
          ) {
            throw error;
          }
          console.warn(
            "Graph resource budget exceeded; continuing without graph enrichment.",
          );
        } else {
          console.warn(
            "Graph retrieval failed; continuing without graph enrichment.",
            error instanceof Error ? error.message : String(error),
          );
        }
      }
    }

    const scores = new Map<
      string,
      {
        bm25Rank: number;
        vectorRank: number;
        graphRank: number;
        sessionId: string;
        bm25Score: number;
        vectorScore: number;
        graphScore: number;
        graphContext?: string;
      }
    >();

    bm25Results.forEach((r, i) => {
      scores.set(r.obsId, {
        bm25Rank: i + 1,
        vectorRank: Infinity,
        graphRank: Infinity,
        sessionId: r.sessionId,
        bm25Score: r.score,
        vectorScore: 0,
        graphScore: 0,
      });
    });

    vectorResults.forEach((r, i) => {
      const existing = scores.get(r.obsId);
      if (existing) {
        existing.vectorRank = i + 1;
        existing.vectorScore = r.score;
      } else {
        scores.set(r.obsId, {
          bm25Rank: Infinity,
          vectorRank: i + 1,
          graphRank: Infinity,
          sessionId: r.sessionId,
          bm25Score: 0,
          vectorScore: r.score,
          graphScore: 0,
        });
      }
    });

    graphResults.forEach((r, i) => {
      const existing = scores.get(r.obsId);
      if (existing) {
        existing.graphRank = Math.min(existing.graphRank, i + 1);
        existing.graphScore = Math.max(existing.graphScore, r.score);
        if (r.graphContext && !existing.graphContext) {
          existing.graphContext = r.graphContext;
        }
      } else {
        scores.set(r.obsId, {
          bm25Rank: Infinity,
          vectorRank: Infinity,
          graphRank: i + 1,
          sessionId: r.sessionId,
          bm25Score: 0,
          vectorScore: 0,
          graphScore: r.score,
          graphContext: r.graphContext,
        });
      }
    });

    // Normalize once per query by the best attainable weighted score over
    // the streams that produced results, so configured stream weights
    // survive for single-stream hits and a silent stream carries no penalty.
    const AGREEMENT_BONUS = 0.05;
    const activeWeight =
      (bm25Results.length > 0 ? this.bm25Weight : 0) +
      (vectorResults.length > 0 ? this.vectorWeight : 0) +
      (graphResults.length > 0 ? this.graphWeight : 0);
    const maxAttainable = activeWeight * (1 / (RRF_K + 1));
    const ranked = Array.from(scores.entries()).map(([obsId, s]) => {
      const wB = Number.isFinite(s.bm25Rank) ? this.bm25Weight : 0;
      const wV = Number.isFinite(s.vectorRank) ? this.vectorWeight : 0;
      const wG = Number.isFinite(s.graphRank) ? this.graphWeight : 0;
      const matchedStreams =
        (wB > 0 ? 1 : 0) + (wV > 0 ? 1 : 0) + (wG > 0 ? 1 : 0);
      const weighted =
        wB * (1 / (RRF_K + s.bm25Rank)) +
        wV * (1 / (RRF_K + s.vectorRank)) +
        wG * (1 / (RRF_K + s.graphRank));
      const rrf = maxAttainable > 0 ? weighted / maxAttainable : 0;
      return {
        obsId,
        s,
        combinedScore: rrf * (1 + AGREEMENT_BONUS * (matchedStreams - 1)),
        minRank: Math.min(s.bm25Rank, s.vectorRank, s.graphRank),
      };
    });

    ranked.sort(
      (a, b) =>
        b.combinedScore - a.combinedScore ||
        a.minRank - b.minRank ||
        (a.obsId < b.obsId ? -1 : a.obsId > b.obsId ? 1 : 0),
    );
    const combined = ranked.map(({ obsId, s, combinedScore }) => ({
      obsId,
      sessionId: s.sessionId,
      bm25Score: s.bm25Score,
      vectorScore: s.vectorScore,
      graphScore: s.graphScore,
      graphContext: s.graphContext,
      combinedScore,
    }));

    const retrievalDepth = Math.max(limit, 50);
    const diversified = this.diversifyBySession(combined, retrievalDepth);
    const enriched = await this.enrichResults(diversified, retrievalDepth);

    return enriched;
  }

  private async finalRerank(
    query: string,
    enriched: HybridSearchResult[],
    limit: number,
    signal?: AbortSignal,
  ): Promise<HybridSearchResult[]> {
    signal?.throwIfAborted();
    const rerankWindow = 50;
    if (this.rerankEnabled && enriched.length > 1) {
      const head = enriched.slice(0, rerankWindow);
      const tail = enriched.slice(rerankWindow);
      const reranked = await rerank(query, head, rerankWindow, signal);
      signal?.throwIfAborted();
      if (reranked === head) return enriched.slice(0, limit);
      return fuseRerankedResults(head, reranked).concat(tail).slice(0, limit);
    }

    return enriched.slice(0, limit);
  }

  private diversifyBySession(
    results: Array<{
      obsId: string;
      sessionId: string;
      bm25Score: number;
      vectorScore: number;
      graphScore: number;
      combinedScore: number;
      graphContext?: string;
    }>,
    limit: number,
    maxPerSession = 3,
  ): typeof results {
    const selected: typeof results = [];
    const sessionCounts = new Map<string, number>();

    for (const r of results) {
      const count = sessionCounts.get(r.sessionId) || 0;
      if (count >= maxPerSession) continue;
      selected.push(r);
      sessionCounts.set(r.sessionId, count + 1);
      if (selected.length >= limit) break;
    }

    if (selected.length < limit) {
      for (const r of results) {
        if (selected.length >= limit) break;
        if (!selected.some(s => s.obsId === r.obsId)) {
          selected.push(r);
        }
      }
    }

    return selected;
  }

  private async enrichResults(
    results: Array<{
      obsId: string;
      sessionId: string;
      bm25Score: number;
      vectorScore: number;
      graphScore: number;
      combinedScore: number;
      graphContext?: string;
    }>,
    limit: number,
  ): Promise<HybridSearchResult[]> {
    const sliced = results.slice(0, limit);
    const observations: Array<CompressedObservation | null> = [];
    for (let offset = 0; offset < sliced.length; offset += 4) observations.push(...await Promise.all(
      sliced.slice(offset, offset + 4).map(async (r) => {
        if (!r.sessionId && this.indexedVector) {
          const record = await this.kv.get<{ sessionId: string }>(KV.semanticVectors('observations'), r.obsId);
          if (record) r.sessionId = record.sessionId;
        }
        const obs = await this.kv
          .get<CompressedObservation>(KV.observations(r.sessionId), r.obsId)
          .catch(error => { if (this.indexedVector) throw error; return null; });
        if (obs) return obs;
        // Fallback: indexed entry may originate from mem::remember, which
        // writes to KV.memories with a synthetic sessionId ("memory" or the
        // memory's first associated session). Coerce the Memory record into
        // a CompressedObservation so search/recall surface saved memories.
        const mem = await this.kv
          .get<Memory>(KV.memories, r.obsId)
          .catch(error => { if (this.indexedVector) throw error; return null; });
        return mem ? memoryToObservation(mem) : null;
      }),
    ));
    const enriched: HybridSearchResult[] = [];
    for (let i = 0; i < sliced.length; i++) {
      const obs = observations[i];
      if (obs) {
        enriched.push({
          observation: obs,
          bm25Score: sliced[i].bm25Score,
          vectorScore: sliced[i].vectorScore,
          graphScore: sliced[i].graphScore,
          combinedScore: sliced[i].combinedScore,
          sessionId: sliced[i].sessionId,
          graphContext: sliced[i].graphContext,
        });
      }
    }
    return enriched;
  }
}
