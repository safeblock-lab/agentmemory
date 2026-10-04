import type { HybridSearchResult } from "../types.js";
import { loadReranker, isRerankerLoaded } from "./reranker-runtime.js";

const MAX_CANDIDATES = 50;
const MAX_PENDING_BATCHES = 8;
const MAX_QUERY_CHARACTERS = 256;
const MAX_DOCUMENT_CHARACTERS = 8192;
let busy = false;
const waiters: Array<() => void> = [];

function rerankingCancelled(): Error {
  const error = new Error("Local reranking cancelled.");
  error.name = "AbortError";
  return error;
}

function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  if (signal.reason instanceof Error) throw signal.reason;
  throw rerankingCancelled();
}

async function acquire(signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal);
  if (!busy) {
    busy = true;
    return;
  }
  if (waiters.length >= MAX_PENDING_BATCHES) throw new Error("Local reranker queue is full.");
  return new Promise<void>((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout>;
    const cleanup = () => { clearTimeout(timer); signal?.removeEventListener("abort", cancel); };
    const remove = () => {
      const index = waiters.indexOf(grant);
      if (index >= 0) waiters.splice(index, 1);
    };
    const grant = () => { cleanup(); resolve(); };
    const cancel = () => {
      remove();
      cleanup();
      reject(rerankingCancelled());
    };
    timer = setTimeout(() => {
      remove();
      cleanup();
      reject(new Error("Local reranker queue timed out."));
    }, 60_000);
    waiters.push(grant);
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) cancel();
  });
}

function release(): void {
  const next = waiters.shift();
  if (next) next();
  else busy = false;
}

export async function rerank(
  query: string,
  results: HybridSearchResult[],
  topK = 20,
  signal?: AbortSignal,
): Promise<HybridSearchResult[]> {
  throwIfAborted(signal);
  if (results.length <= 1 || !query.trim() || !Number.isFinite(topK) || topK < 1) return results;

  await acquire(signal);
  try {
    const runtime = await loadReranker();
    throwIfAborted(signal);
    if (!runtime) return results;
    const count = Math.min(results.length, Math.floor(topK), MAX_CANDIDATES);
    const scores: Array<{ result: HybridSearchResult; score: number; index: number }> = [];
    const boundedQuery = runtime.strictBounds ? query : query.slice(0, MAX_QUERY_CHARACTERS);
    const documents = results.slice(0, count).map((result) => {
      const title = result.observation.title || "";
      const narrative = result.observation.narrative || "";
      return runtime.strictBounds ? `${title}\n${narrative}`
        : `${title.slice(0, MAX_DOCUMENT_CHARACTERS)}\n${narrative.slice(0, MAX_DOCUMENT_CHARACTERS)}`.slice(0, MAX_DOCUMENT_CHARACTERS);
    });
    const batchScores = runtime.scoreBatch ? await runtime.scoreBatch(boundedQuery, documents, signal) : undefined;
    throwIfAborted(signal);
    if (batchScores && (batchScores.length !== count || batchScores.some((score) => !Number.isFinite(score)))) {
      throw new Error("Local reranker returned an invalid score batch.");
    }
    for (let index = 0; index < count; index++) {
      const result = results[index];
      throwIfAborted(signal);
      const score = batchScores ? batchScores[index] : await runtime.score(boundedQuery, documents[index]);
      throwIfAborted(signal);
      if (!Number.isFinite(score)) throw new Error("Local reranker returned a non-finite score.");
      scores.push({ result, score, index });
    }
    scores.sort((a, b) => b.score - a.score || a.index - b.index);
    return scores.map(({ result, score }, index) => ({
      ...result,
      combinedScore: score,
      rerankPosition: index + 1,
    }));
  } finally {
    release();
  }
}

export function isRerankerAvailable(): boolean {
  return isRerankerLoaded();
}
