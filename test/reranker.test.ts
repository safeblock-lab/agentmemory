import { describe, it, expect, vi, afterEach } from "vitest";
import { resolve } from "node:path";

vi.mock("@huggingface/transformers", () => {
  throw new Error("not installed");
});

import { rerank, isRerankerAvailable } from "../src/state/reranker.js";

describe("reranker", () => {
  it("returns results unchanged when @huggingface/transformers is unavailable", async () => {
    const results = [
      {
        observation: {
          id: "o1",
          title: "First",
          narrative: "First result",
        },
        bm25Score: 0.5,
        vectorScore: 0.6,
        graphScore: 0,
        combinedScore: 0.8,
        sessionId: "s1",
      },
      {
        observation: {
          id: "o2",
          title: "Second",
          narrative: "Second result",
        },
        bm25Score: 0.3,
        vectorScore: 0.4,
        graphScore: 0,
        combinedScore: 0.5,
        sessionId: "s1",
      },
    ] as any;

    const reranked = await rerank("test query", results);
    expect(reranked).toEqual(results);
  });

  it("isRerankerAvailable returns false when not loaded", () => {
    expect(isRerankerAvailable()).toBe(false);
  });

  it("handles single result gracefully", async () => {
    const results = [
      {
        observation: { id: "o1", title: "Only" },
        combinedScore: 1.0,
      },
    ] as any;

    const reranked = await rerank("query", results);
    expect(reranked).toHaveLength(1);
  });

  it("handles empty results", async () => {
    const reranked = await rerank("query", []);
    expect(reranked).toHaveLength(0);
  });
});

describe("reranker with loaded pipeline", () => {
  afterEach(() => {
    vi.doUnmock("@huggingface/transformers");
    vi.resetModules();
  });

  it("invokes the @huggingface/transformers model and reorders by score", async () => {
    const tokenizer = vi.fn((query, options) => ({ query, document: options.text_pair }));
    const model = vi.fn(async ({ document }) => ({
      logits: { dims: [1, 1], data: [document.includes("First") ? 4 : -2] },
    }));
    vi.doMock("@huggingface/transformers", () => ({
      AutoTokenizer: { from_pretrained: async () => tokenizer },
      AutoModelForSequenceClassification: { from_pretrained: async () => model },
    }));
    vi.resetModules();

    const { rerank } = await import("../src/state/reranker.js");

    const results = [
      { observation: { id: "o2", title: "Second", narrative: "" }, combinedScore: 0.9 },
      { observation: { id: "o1", title: "First", narrative: "" }, combinedScore: 0.5 },
    ] as any;

    const reranked = await rerank("query", results);

    expect(tokenizer).toHaveBeenCalledWith("query", {
      text_pair: "Second\n", truncation: true, max_length: 512, padding: false,
    });
    expect(model).toHaveBeenCalled();
    expect(reranked[0].observation.id).toBe("o1");
    expect(reranked.map((result) => result.combinedScore)).toEqual([4, -2]);
  });
});

describe("bounded local cross-encoder", () => {
  afterEach(() => {
    vi.doUnmock("@huggingface/transformers");
    vi.resetModules();
  });

  const results = (count = 3) => Array.from({ length: count }, (_, i) => ({
    observation: { id: `o${i}`, title: `Document ${i}`, narrative: "content" },
    combinedScore: 100 - i,
  })) as Parameters<typeof rerank>[1];

  async function setup(infer: (input: { document: string }) => Promise<unknown>) {
    const tokenizer = vi.fn((query: string, options: { text_pair: string }) => ({
      query, document: options.text_pair,
    }));
    const model = vi.fn(infer);
    const loadTokenizer = vi.fn(async () => tokenizer);
    const loadModel = vi.fn(async () => model);
    vi.doMock("@huggingface/transformers", () => ({
      AutoTokenizer: { from_pretrained: loadTokenizer },
      AutoModelForSequenceClassification: { from_pretrained: loadModel },
    }));
    vi.resetModules();
    return { ...await import("../src/state/reranker.js"), tokenizer, model, loadTokenizer, loadModel };
  }

  const logit = (score: number) => ({ logits: { dims: [1, 1], data: [score] } });

  it("keeps ties stable and does not mutate original results", async () => {
    const api = await setup(async () => logit(-3));
    const original = results();
    const output = await api.rerank("query", original);
    expect(output.map((r) => r.observation.id)).toEqual(["o0", "o1", "o2"]);
    expect(output.map((r) => r.combinedScore)).toEqual([-3, -3, -3]);
    expect(original.map((r) => r.combinedScore)).toEqual([100, 99, 98]);
    expect(api.isRerankerAvailable()).toBe(true);
  });

  it.each([
    ["throw", "inference failed"],
    ["NaN", "Invalid cross-encoder relevance logit."],
    ["shape", "Expected one cross-encoder relevance logit."],
  ])("rejects the entire batch on %s after partial scoring", async (failure, expectedError) => {
    let calls = 0;
    const api = await setup(async () => {
      if (++calls === 1) return logit(9);
      if (failure === "throw") throw new Error("inference failed");
      if (failure === "shape") return { logits: { dims: [1, 2], data: [1, 2] } };
      return logit(Number.NaN);
    });
    const original = results();
    const before = structuredClone(original);
    await expect(api.rerank("query", original)).rejects.toThrow(expectedError);
    expect(original).toEqual(before);
  });

  it("rejects with the caller's cancellation reason after inference settles", async () => {
    let unblock!: () => void;
    let markInferenceStarted!: () => void;
    const blocked = new Promise<void>((resolve) => { unblock = resolve; });
    const inferenceStarted = new Promise<void>((resolve) => { markInferenceStarted = resolve; });
    const api = await setup(async () => {
      markInferenceStarted();
      await blocked;
      return logit(1);
    });
    const original = results();
    const before = structuredClone(original);
    const controller = new AbortController();
    const cancellation = new Error("caller cancelled retrieval");
    const ranking = api.rerank("query", original, 20, controller.signal);

    await inferenceStarted;
    controller.abort(cancellation);
    unblock();

    await expect(ranking).rejects.toBe(cancellation);
    expect(original).toEqual(before);
  });

  it("bounds candidates, paired tokens and tokenizer preprocessing input", async () => {
    const api = await setup(async () => logit(1));
    const original = results(60);
    original[0].observation.narrative = "a".repeat(100_000);
    expect(await api.rerank("q".repeat(100_000), original, 100_000)).toHaveLength(50);
    expect(api.model).toHaveBeenCalledTimes(50);
    const [query, options] = api.tokenizer.mock.calls[0];
    expect(query.length).toBeLessThanOrEqual(256);
    expect(options.text_pair.length).toBeLessThanOrEqual(8192);
    expect(options).toMatchObject({ max_length: 512, truncation: true });
    const modelPath = resolve(process.cwd(), ".cache", "agentmemory", "reranker", "Xenova", "ms-marco-MiniLM-L-6-v2");
    expect(api.loadModel).toHaveBeenCalledWith(modelPath, expect.objectContaining({
      dtype: "q8", device: "cpu", local_files_only: true,
      session_options: { intraOpNumThreads: 1, interOpNumThreads: 1 },
    }));
    const cache = resolve(process.cwd(), ".cache", "agentmemory", "reranker");
    expect(api.loadTokenizer).toHaveBeenCalledWith(modelPath, expect.objectContaining({
      local_files_only: true, cache_dir: cache,
    }));
  });

  it("serializes batches fairly, shares one load and rejects excess queued work", async () => {
    let unblock!: () => void;
    const blocked = new Promise<void>((resolve) => { unblock = resolve; });
    let active = 0;
    let peak = 0;
    let first = true;
    const order: string[] = [];
    const api = await setup(async ({ document }) => {
      peak = Math.max(peak, ++active);
      order.push(document);
      if (first) { first = false; await blocked; }
      active--;
      return logit(1);
    });
    const batches = Array.from({ length: 10 }, (_, batch) => results(2).map((r) => ({
      ...r, observation: { ...r.observation, title: `Batch ${batch}` },
    })));
    const pending = batches.map((batch) => api.rerank("query", batch));
    await expect(pending[9]).rejects.toThrow("Local reranker queue is full.");
    await vi.waitFor(() => expect(api.model).toHaveBeenCalledTimes(1));
    unblock();
    await Promise.all(pending.slice(0, 9));
    expect(peak).toBe(1);
    expect(api.loadModel).toHaveBeenCalledTimes(1);
    expect(api.loadTokenizer).toHaveBeenCalledTimes(1);
    expect(order).toEqual(Array.from({ length: 9 }, (_, i) => [`Batch ${i}\ncontent`, `Batch ${i}\ncontent`]).flat());
  });

  it.each([0, -1, Number.NaN, Infinity])("skips invalid topK %s without loading", async (topK) => {
    const api = await setup(async () => logit(1));
    const original = results();
    expect(await api.rerank("query", original, topK)).toBe(original);
    expect(api.loadModel).not.toHaveBeenCalled();
  });

  it("selects a pinned offline candidate explicitly without changing production defaults", async () => {
    const api = await setup(async () => logit(2));
    const runtime = await import("../src/state/reranker-runtime.js");
    expect(runtime.rerankerConfiguration().modelId).toBe("Xenova/ms-marco-MiniLM-L-6-v2");
    const configuration = {
      modelId: "onnx-community/bge-reranker-v2-m3-ONNX",
      revision: "90213ffc6a8e6f051a6331269a0f5526cdd896f6",
      modelPath: resolve(".native-pagination-build/multilingual-model-provision"),
      cacheDirectory: resolve(".native-pagination-build/semantic-quality-correction/combined-bge256/model-cache"),
    };
    expect(() => runtime.configureCandidateReranker({ ...configuration, modelPath: "relative" })).toThrow("Invalid pinned");
    expect(() => runtime.configureCandidateReranker({ ...configuration, revision: "unverified" })).toThrow("Invalid pinned");
    runtime.configureCandidateReranker(configuration);
    expect(runtime.rerankerConfiguration()).toEqual(configuration);
    expect(await api.rerank("query", results())).toHaveLength(3);
    expect(api.loadModel).toHaveBeenCalledWith(configuration.modelPath, expect.objectContaining({
      local_files_only: true, cache_dir: configuration.cacheDirectory, dtype: "q8", device: "cpu",
      session_options: { intraOpNumThreads: 1, interOpNumThreads: 1 },
    }));
    expect(() => runtime.configureCandidateReranker(configuration)).toThrow("immutable");
  });
});
