import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { HybridSearch } from "../src/state/hybrid-search.js";
import { SearchIndex } from "../src/state/search-index.js";
import type { HybridSearchResult } from "../src/types.js";

const mocks = vi.hoisted(() => ({ rerank: vi.fn() }));
vi.mock("../src/state/reranker.js", () => ({ rerank: mocks.rerank }));

function result(id: string, combinedScore: number): HybridSearchResult {
  return {
    observation: {
      id,
      sessionId: "session",
      timestamp: "2026-10-03T00:00:00.000Z",
      type: "file_edit",
      title: id,
      subtitle: id,
      facts: [],
      narrative: id,
      concepts: [],
      files: [],
      importance: 5,
    },
    bm25Score: combinedScore,
    vectorScore: combinedScore,
    graphScore: 0,
    combinedScore,
    sessionId: "session",
  };
}

function searchWith(candidates: HybridSearchResult[]): HybridSearch {
  const hybrid = new HybridSearch(new SearchIndex(), null, null, { indexedRetrieval: false } as never);
  const searchable = hybrid as unknown as {
    tripleStreamSearch: (query: string, ...args: unknown[]) => Promise<HybridSearchResult[]>;
  };
  vi.spyOn(searchable, "tripleStreamSearch").mockResolvedValue(candidates);
  return hybrid;
}

describe("HybridSearch semantic fusion and cancellation", () => {
  beforeEach(() => {
    vi.stubEnv("RERANK_ENABLED", "true");
    mocks.rerank.mockReset();
  });

  afterEach(() => vi.unstubAllEnvs());

  it("combines normalized retrieval and reranker scores while keeping a relevant candidate in the limit", async () => {
    const candidates = [result("relevant", 0.9), result("lexical-only", 0.8), result("semantic-only", 0.2)];
    const scores = new Map([["relevant", 0.7], ["lexical-only", 0.65], ["semantic-only", 0.99]]);
    mocks.rerank.mockImplementation(async (_query: string, batch: HybridSearchResult[]) =>
      batch.map((candidate) => ({ ...candidate, combinedScore: scores.get(candidate.observation.id)! })));

    const output = await searchWith(candidates).search("query", 2);

    expect(output.map((candidate) => candidate.observation.id)).toEqual(["relevant", "semantic-only"]);
    expect(output[0].combinedScore).toBeCloseTo(1 + (0.7 - 0.65) / (0.99 - 0.65));
    expect(output[1].combinedScore).toBe(1);
    expect(mocks.rerank).toHaveBeenCalledWith("query", expect.any(Array), 50, undefined);
  });

  it("keeps the original retrieval order when equal fused scores tie", async () => {
    const candidates = [result("source-z", 0.5), result("source-a", 0.5), result("source-m", 0.5)];
    mocks.rerank.mockImplementation(async (_query: string, batch: HybridSearchResult[]) =>
      batch.slice().reverse().map((candidate) => ({ ...candidate, combinedScore: 4 })));

    const output = await searchWith(candidates).search("query", 3);

    expect(output.map((candidate) => candidate.observation.id)).toEqual(["source-z", "source-a", "source-m"]);
  });

  it("preserves retrieval order when the optional reranker is unavailable", async () => {
    const candidates = [result("first", 0.8), result("second", 0.4)];
    mocks.rerank.mockImplementation(async (_query: string, batch: HybridSearchResult[]) => batch);

    const output = await searchWith(candidates).search("query", 2);

    expect(output).toEqual(candidates);
  });

  it("rejects an already-aborted product search before invoking the reranker", async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(searchWith([result("one", 1), result("two", 0.5)]).search("query", 2, controller.signal))
      .rejects.toMatchObject({ name: "AbortError" });
    expect(mocks.rerank).not.toHaveBeenCalled();
  });

  it("passes in-flight cancellation to the reranker and rejects late results", async () => {
    const controller = new AbortController();
    mocks.rerank.mockImplementation(async (
      _query: string,
      batch: HybridSearchResult[],
      _topK: number,
      signal?: AbortSignal,
    ) => new Promise<HybridSearchResult[]>((resolve) => {
      signal?.addEventListener("abort", () => resolve(batch), { once: true });
    }));

    const pending = searchWith([result("one", 1), result("two", 0.5)]).search("query", 2, controller.signal);
    await vi.waitFor(() => expect(mocks.rerank).toHaveBeenCalled());
    expect(mocks.rerank.mock.calls[0][3]).toBe(controller.signal);
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });

  it("passes the expansion search signal into final reranking", async () => {
    const controller = new AbortController();
    mocks.rerank.mockImplementation(async (
      _query: string,
      batch: HybridSearchResult[],
      _topK: number,
      signal?: AbortSignal,
    ) => new Promise<HybridSearchResult[]>((resolve) => {
      signal?.addEventListener("abort", () => resolve(batch), { once: true });
    }));

    const expansion = { reformulations: [], temporalConcretizations: [], entityExtractions: [] };
    const pending = searchWith([result("one", 1), result("two", 0.5)])
      .searchWithExpansion("query", 2, expansion, controller.signal);
    await vi.waitFor(() => expect(mocks.rerank).toHaveBeenCalled());
    expect(mocks.rerank.mock.calls[0][3]).toBe(controller.signal);
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });

  it.each(["integrity", "input", "queue full", "timed out"])("surfaces reranker %s failures from search", async (failure) => {
    const error = new Error(failure);
    mocks.rerank.mockRejectedValue(error);

    await expect(searchWith([result("one", 1), result("two", 0.5)]).search("query", 2)).rejects.toBe(error);
  });
});
