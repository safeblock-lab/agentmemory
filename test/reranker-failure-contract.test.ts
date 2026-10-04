import { afterEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ loadReranker: vi.fn(), isRerankerLoaded: vi.fn() }));
vi.mock("../src/state/reranker-runtime.js", () => mocks);

import { rerank } from "../src/state/reranker.js";
import type { HybridSearchResult } from "../src/types.js";

function results(): HybridSearchResult[] {
  return ["first", "second"].map((id, index) => ({
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
    bm25Score: 1 - index,
    vectorScore: 1 - index,
    graphScore: 0,
    combinedScore: 1 - index,
    sessionId: "session",
  }));
}

function scorer(scoreBatch: (query: string, documents: string[], signal?: AbortSignal) => Promise<number[]>) {
  return { strictBounds: true as const, score: vi.fn(), scoreBatch: vi.fn(scoreBatch), close: vi.fn(), isClosed: () => false };
}

describe("local reranker failure contract", () => {
  afterEach(() => {
    vi.useRealTimers();
    mocks.loadReranker.mockReset();
    mocks.isRerankerLoaded.mockReset();
  });

  it("keeps the explicit optional-provider fallback when no scorer is available", async () => {
    mocks.loadReranker.mockResolvedValue(null);
    const input = results();

    await expect(rerank("query", input)).resolves.toBe(input);
  });

  it("rejects pre-aborted work before loading the scorer", async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(rerank("query", results(), 20, controller.signal)).rejects.toMatchObject({ name: "AbortError" });
    expect(mocks.loadReranker).not.toHaveBeenCalled();
  });

  it("rejects in-flight work even when the scorer returns after cancellation", async () => {
    const controller = new AbortController();
    mocks.loadReranker.mockResolvedValue(scorer(async (_query, _documents, signal) =>
      new Promise<number[]>((resolve) => signal?.addEventListener("abort", () => resolve([0.9, 0.1]), { once: true }))));
    const pending = rerank("query", results(), 20, controller.signal);
    await vi.waitFor(() => expect(mocks.loadReranker).toHaveBeenCalled());
    controller.abort();

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });

  it.each(["integrity", "input", "timeout"])("propagates scorer %s failures", async (code) => {
    const error = Object.assign(new Error(code), { code });
    mocks.loadReranker.mockResolvedValue(scorer(async () => { throw error; }));

    await expect(rerank("query", results())).rejects.toBe(error);
  });

  it("rejects an invalid score batch instead of returning the original ranking", async () => {
    mocks.loadReranker.mockResolvedValue(scorer(async () => [0.9]));

    await expect(rerank("query", results())).rejects.toThrow("invalid score batch");
  });

  it("rejects saturated queues and admits the next batch after capacity frees", async () => {
    let unblock!: () => void;
    const blocked = new Promise<void>((resolve) => { unblock = resolve; });
    let first = true;
    const runtime = scorer(async () => {
      if (first) { first = false; await blocked; }
      return [0.9, 0.1];
    });
    mocks.loadReranker.mockResolvedValue(runtime);

    const batches = Array.from({ length: 10 }, () => results());
    const pending = batches.map((batch) => rerank("query", batch));
    const overflow = expect(pending[9]).rejects.toThrow("queue is full");
    await vi.waitFor(() => expect(runtime.scoreBatch).toHaveBeenCalledTimes(1));
    await overflow;
    unblock();
    await Promise.all(pending.slice(0, 9));

    expect(runtime.scoreBatch).toHaveBeenCalledTimes(9);
  });

  it("rejects timed out queued batches and releases the active batch", async () => {
    let unblock!: () => void;
    const blocked = new Promise<void>((resolve) => { unblock = resolve; });
    mocks.loadReranker.mockResolvedValue(scorer(async () => {
      await blocked;
      return [0.9, 0.1];
    }));
    const active = rerank("query", results());
    await vi.waitFor(() => expect(mocks.loadReranker).toHaveBeenCalled());

    vi.useFakeTimers();
    const queued = rerank("queued", results());
    const timedOut = expect(queued).rejects.toThrow("queue timed out");
    await vi.advanceTimersByTimeAsync(60_000);
    await timedOut;
    unblock();
    await active;
  });
});
