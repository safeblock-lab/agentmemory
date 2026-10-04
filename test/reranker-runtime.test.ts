import { afterEach, describe, expect, it, vi } from "vitest";
const mocks = vi.hoisted(() => ({ create: vi.fn() }));
vi.mock("../src/state/qwen-reranker.js", () => ({ createQwenReranker: mocks.create }));
afterEach(() => { vi.unstubAllEnvs(); vi.resetModules(); vi.clearAllMocks(); });
describe("normal Qwen reranking path", () => {
  it("loads one configured provider, preserves fields and ranks stable probabilities", async () => {
    vi.stubEnv("RERANK_PROVIDER", "qwen");
    const batch = vi.fn(async () => [.1, .9, .9]);
    mocks.create.mockResolvedValue({ strictBounds: true, scoreBatch: batch, score: vi.fn(), close: vi.fn() });
    const { rerank } = await import("../src/state/reranker.js");
    const inputs = ["a", "b", "c"].map((id) => ({ observation: { id, title: id, narrative: "full" }, combinedScore: 4, vectorScore: .3 })) as Parameters<typeof rerank>[1];
    const output = await rerank("query", inputs);
    expect(output.map((x) => x.observation.id)).toEqual(["b", "c", "a"]);
    expect(output.map((x) => x.combinedScore)).toEqual([.9, .9, .1]);
    expect(output[0].vectorScore).toBe(.3); expect(inputs[0].combinedScore).toBe(4);
    expect(batch).toHaveBeenCalledWith("query", ["a\nfull", "b\nfull", "c\nfull"], undefined);
    expect(mocks.create).toHaveBeenCalledOnce();
  });
  it("surfaces Qwen integrity/inference failures instead of original-ranking fallback", async () => {
    vi.stubEnv("RERANK_PROVIDER", "qwen"); mocks.create.mockRejectedValue(new Error("integrity"));
    const { rerank } = await import("../src/state/reranker.js");
    const inputs = [1, 2].map((id) => ({ observation: { id: String(id), title: "t" }, combinedScore: 1 })) as Parameters<typeof rerank>[1];
    await expect(rerank("q", inputs)).rejects.toThrow("integrity");
  });
  it.each(["timeout", "cancelled", "protocol"])("replaces a closed %s scorer only after cleanup, without retrying the failed request", async (code) => {
    vi.stubEnv("RERANK_PROVIDER", "qwen");
    let closed = false;
    let cleanupDone = false;
    const failure = Object.assign(new Error(code), { code });
    const first = {
      strictBounds: true, isClosed: () => closed, score: vi.fn(),
      scoreBatch: vi.fn(async () => { closed = true; throw failure; }),
      close: vi.fn(async () => { cleanupDone = true; }),
    };
    const second = { strictBounds: true, isClosed: () => false, score: vi.fn(), scoreBatch: vi.fn(async () => [.2, .8]), close: vi.fn() };
    mocks.create.mockResolvedValueOnce(first).mockImplementationOnce(async () => {
      expect(cleanupDone).toBe(true);
      return second;
    });
    const { rerank } = await import("../src/state/reranker.js");
    const { isRerankerLoaded, loadReranker } = await import("../src/state/reranker-runtime.js");
    const inputs = ["a", "b"].map((id) => ({ observation: { id, title: id }, combinedScore: 1 })) as Parameters<typeof rerank>[1];
    await expect(rerank("query", inputs)).rejects.toBe(failure);
    expect(isRerankerLoaded()).toBe(false);
    const outputs = await Promise.all([rerank("next", inputs), rerank("next", inputs)]);
    expect(outputs[0].map((result) => result.observation.id)).toEqual(["b", "a"]);
    expect(first.close).toHaveBeenCalledOnce();
    expect(mocks.create).toHaveBeenCalledTimes(2);
    expect(await loadReranker()).toBe(second);
  });
  it("propagates a rejected in-flight load through explicit cleanup and permits a fresh load", async () => {
    vi.stubEnv("RERANK_PROVIDER", "qwen");
    let rejectLoad!: (error: Error) => void;
    mocks.create.mockReturnValueOnce(new Promise((_, reject) => { rejectLoad = reject; }));
    const { loadReranker, closeReranker, isRerankerLoaded } = await import("../src/state/reranker-runtime.js");
    const failure = new Error("integrity");
    const first = loadReranker();
    const firstResult = expect(first).rejects.toBe(failure);
    const cleanup = closeReranker();
    const cleanupResult = expect(cleanup).rejects.toBe(failure);
    rejectLoad(failure);
    await Promise.all([firstResult, cleanupResult]);
    expect(isRerankerLoaded()).toBe(false);
    const next = { score: vi.fn(), close: vi.fn(), isClosed: () => false };
    mocks.create.mockResolvedValueOnce(next);
    expect(await loadReranker()).toBe(next);
    await closeReranker();
    expect(next.close).toHaveBeenCalledOnce();
  });
  it("does not create another child when closed-runtime cleanup still fails", async () => {
    vi.stubEnv("RERANK_PROVIDER", "qwen");
    let closed = false;
    const cleanupFailure = new Error("child did not exit");
    const scorer = { score: vi.fn(), isClosed: () => closed, close: vi.fn(async () => { closed = true; throw cleanupFailure; }) };
    mocks.create.mockResolvedValueOnce(scorer);
    const { loadReranker, closeReranker } = await import("../src/state/reranker-runtime.js");
    await loadReranker();
    await expect(closeReranker()).rejects.toBe(cleanupFailure);
    await expect(loadReranker()).rejects.toBe(cleanupFailure);
    expect(mocks.create).toHaveBeenCalledOnce();
  });
});
