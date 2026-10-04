import { beforeEach, describe, expect, it, vi } from "vitest";
import type { CompressedObservation, EmbeddingProvider, HybridSearchResult } from "../src/types.js";
import type { StateKV } from "../src/state/kv.js";
import { SearchIndex } from "../src/state/search-index.js";
import { HybridSearch } from "../src/state/hybrid-search.js";
import { GraphRetrieval } from "../src/functions/graph-retrieval.js";
import { GraphRetrievalResourceError } from "../src/functions/graph-retrieval-read.js";
import { IndexedRetrievalError } from "../src/state/indexed-retrieval.js";

const mocks = vi.hoisted(() => ({
  keywordSearch: vi.fn(),
  vectorSearch: vi.fn(),
  rerank: vi.fn(),
}));

vi.mock("../src/state/indexed-vector.js", () => ({
  getIndexedVector: () => ({
    keywordSearch: mocks.keywordSearch,
    search: mocks.vectorSearch,
  }),
}));

vi.mock("../src/state/reranker.js", () => ({ rerank: mocks.rerank }));

function makeObservation(id: string): CompressedObservation {
  return {
    id,
    sessionId: "session-1",
    timestamp: "2026-10-04T00:00:00.000Z",
    type: "discovery",
    title: id,
    facts: [],
    narrative: id,
    concepts: [],
    files: [],
    importance: 5,
  };
}

const observations = new Map(
  ["keyword", "vector", "graph"].map((id) => [
    `obs-${id}`,
    makeObservation(`obs-${id}`),
  ]),
);

function makeIndexedSearch(): HybridSearch {
  const kv = {
    indexedRetrieval: true,
    get: async (_scope: string, key: string) => observations.get(key) ?? null,
  } as unknown as StateKV;
  const embeddingProvider = {
    embed: async () => new Float32Array([0.1, 0.2]),
  } as unknown as EmbeddingProvider;
  return new HybridSearch(new SearchIndex(), null, embeddingProvider, kv);
}

function setBaseCandidates(): void {
  mocks.keywordSearch.mockResolvedValue([
    { obsId: "obs-keyword", sessionId: "session-1", score: 2 },
  ]);
  mocks.vectorSearch.mockResolvedValue([
    { obsId: "obs-vector", sessionId: "session-1", score: 0.9 },
  ]);
}

async function search(
  hybrid: HybridSearch,
  signal?: AbortSignal,
): Promise<HybridSearchResult[]> {
  return hybrid.searchWithExpansion(
    "alpha",
    10,
    { reformulations: [], temporalConcretizations: [], entityExtractions: ["Alpha"] },
    signal,
  );
}

beforeEach(() => {
  vi.restoreAllMocks();
  mocks.keywordSearch.mockReset();
  mocks.vectorSearch.mockReset();
  mocks.rerank.mockReset().mockImplementation(
    async (_query: string, results: HybridSearchResult[]) => results,
  );
  setBaseCandidates();
});

describe("indexed hybrid graph resource fallback", () => {
  it.each([
    ["graph reader budget", () => new GraphRetrievalResourceError()],
    [
      "indexed retrieval budget",
      () => new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "bounded response"),
    ],
  ])("retains base candidates after a typed %s failure", async (_label, makeError) => {
    vi.spyOn(GraphRetrieval.prototype, "searchByEntitiesAndChunks")
      .mockRejectedValue(makeError());
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const results = await search(makeIndexedSearch());

    expect(results.map((result) => result.observation.id).sort()).toEqual([
      "obs-keyword",
      "obs-vector",
    ]);
    expect(mocks.rerank).toHaveBeenCalledOnce();
    expect(
      (mocks.rerank.mock.calls[0][1] as HybridSearchResult[])
        .map((result) => result.observation.id)
        .sort(),
    ).toEqual(["obs-keyword", "obs-vector"]);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn).toHaveBeenCalledWith(
      "Graph resource budget exceeded; continuing without graph enrichment.",
    );
  });

  it("keeps normal graph enrichment and reranking intact", async () => {
    vi.spyOn(GraphRetrieval.prototype, "searchByEntitiesAndChunks")
      .mockResolvedValue({
        entities: [{
          obsId: "obs-graph",
          sessionId: "session-1",
          score: 0.8,
          graphContext: "related entity",
          pathLength: 1,
        }],
        chunks: [],
      });

    const results = await search(makeIndexedSearch());

    expect(results.map((result) => result.observation.id).sort()).toEqual([
      "obs-graph",
      "obs-keyword",
      "obs-vector",
    ]);
    expect(results.find((result) => result.observation.id === "obs-graph")?.graphScore)
      .toBeGreaterThan(0);
    expect(mocks.rerank).toHaveBeenCalledOnce();
  });

  it("checks cancellation before accepting a resource fallback", async () => {
    vi.spyOn(GraphRetrieval.prototype, "searchByEntitiesAndChunks")
      .mockRejectedValue(new GraphRetrievalResourceError());
    const controller = new AbortController();
    const reason = new Error("caller cancelled");
    controller.abort(reason);

    await expect(search(makeIndexedSearch(), controller.signal)).rejects.toBe(reason);
    expect(mocks.rerank).not.toHaveBeenCalled();
  });

  it.each([
    ["untyped", () => new Error("unexpected graph failure")],
    [
      "integrity",
      () => new IndexedRetrievalError("STATE_GRAPH_RECOVERY_REQUIRED", "generation changed"),
    ],
    ["transport", () => new Error("graph transport disconnected")],
  ])("keeps %s failures fatal", async (_label, makeError) => {
    const error = makeError();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(GraphRetrieval.prototype, "searchByEntitiesAndChunks")
      .mockRejectedValue(error);

    await expect(search(makeIndexedSearch())).rejects.toBe(error);
    expect(mocks.rerank).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
  });

  it("keeps resource exhaustion fatal when no keyword or vector candidate exists", async () => {
    mocks.keywordSearch.mockResolvedValue([]);
    mocks.vectorSearch.mockResolvedValue([]);
    const error = new GraphRetrievalResourceError();
    vi.spyOn(GraphRetrieval.prototype, "searchByEntitiesAndChunks")
      .mockRejectedValue(error);

    await expect(search(makeIndexedSearch())).rejects.toBe(error);
    expect(mocks.rerank).not.toHaveBeenCalled();
  });
});

it("preserves legacy nonindexed graph fallback", async () => {
  const bm25 = new SearchIndex();
  const base = ["obs-keyword", "obs-vector"].map((id) => ({
    ...makeObservation(id),
    title: `alpha ${id}`,
    narrative: "alpha match",
  }));
  for (const observation of base) bm25.add(observation);
  const kv = {
    indexedRetrieval: false,
    get: async (_scope: string, key: string) => observations.get(key) ?? null,
  } as unknown as StateKV;
  const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
  vi.spyOn(GraphRetrieval.prototype, "searchByEntitiesAndChunks")
    .mockRejectedValue(new Error("legacy graph failure"));

  const results = await new HybridSearch(bm25, null, null, kv, 0.4, 0.6, 0.3, true)
    .searchWithExpansion(
      "alpha",
      10,
      { reformulations: [], temporalConcretizations: [], entityExtractions: ["Alpha"] },
    );

  expect(results.map((result) => result.observation.id).sort()).toEqual([
    "obs-keyword",
    "obs-vector",
  ]);
  expect(warn).toHaveBeenCalledWith(
    "Graph retrieval failed; continuing without graph enrichment.",
    "legacy graph failure",
  );
  expect(mocks.rerank).toHaveBeenCalledOnce();
});
