import { afterEach, describe, expect, it, vi } from "vitest";
const mocks = vi.hoisted(() => ({ spawn: vi.fn(), exists: vi.fn(() => true) }));
vi.mock("node:fs", () => ({ existsSync: mocks.exists }));
vi.mock("../src/state/qwen-reranker-runtime.js", async (original) => {
  const actual = await original<typeof import("../src/state/qwen-reranker-runtime.js")>();
  return { ...actual, QwenProcess: class { constructor(...args: unknown[]) { return mocks.spawn(...args); } } };
});
import { createQwenReranker } from "../src/state/qwen-reranker.js";
import { QwenRuntimeError } from "../src/state/qwen-reranker-runtime.js";
const paths = { python: "python", script: "/scorer", model: "/model", cpuRuntime: "/cpu", gpuRuntime: "/cuda", device: "auto" as const };
const proof = (device: string) => ({ ready: true, device, gpuVerified: device === "gpu", context: 4096, controls: [1, -1].map((delta) => ({
  yesTokenId: 9693, noTokenId: 2152, truncationApplied: false, score: delta > 0 ? .99 : .001, deltaLogitYesMinusNo: delta,
})) });
const worker = (ready: Promise<unknown>) => ({ ready, close: vi.fn(async () => {}), scores: vi.fn(async () => [.8, .2]) });
afterEach(() => { vi.clearAllMocks(); mocks.exists.mockReturnValue(true); });
describe("Qwen provider selection", () => {
  it("keeps verified GPU and passes intact inputs to bounded batch scoring", async () => {
    const gpu = worker(Promise.resolve(proof("gpu"))); mocks.spawn.mockReturnValue(gpu);
    const runtime = await createQwenReranker(paths);
    expect(runtime.device).toBe("gpu");
    expect(await runtime.scoreBatch("query", ["first", "second"])).toEqual([.8, .2]);
    expect(gpu.scores).toHaveBeenCalledWith("query", ["first", "second"], undefined);
    await runtime.close(); expect(gpu.close).toHaveBeenCalledOnce();
  });
  it.each(["gpu-unavailable", "gpu-allocation"])("closes GPU before explicit CPU fallback on %s", async (code) => {
    const gpu = worker(Promise.reject(new QwenRuntimeError(code, "GPU cannot fit")));
    const cpu = worker(Promise.resolve(proof("cpu")));
    mocks.spawn.mockReturnValueOnce(gpu).mockReturnValueOnce(cpu);
    const runtime = await createQwenReranker(paths);
    expect(gpu.close).toHaveBeenCalledOnce(); expect(runtime.device).toBe("cpu");
    expect(runtime.fallbackReason).toBe("GPU cannot fit"); expect(mocks.spawn).toHaveBeenNthCalledWith(2, paths, "cpu");
  });
  it.each(["integrity", "quality", "runtime", "timeout"])("does not hide %s as CPU fallback", async (code) => {
    const gpu = worker(Promise.reject(new QwenRuntimeError(code, "failure"))); mocks.spawn.mockReturnValue(gpu);
    await expect(createQwenReranker(paths)).rejects.toMatchObject({ code });
    expect(mocks.spawn).toHaveBeenCalledOnce(); expect(gpu.close).toHaveBeenCalledOnce();
  });
  it("rejects invalid readiness and missing assets", async () => {
    mocks.spawn.mockReturnValue(worker(Promise.resolve(proof("wrong"))));
    await expect(createQwenReranker(paths)).rejects.toMatchObject({ code: "protocol" });
    mocks.exists.mockReturnValue(false);
    await expect(createQwenReranker(paths)).rejects.toThrow("Qwen assets missing");
  });
  it("rejects GPU readiness without actual offload proof", async () => {
    mocks.spawn.mockReturnValue(worker(Promise.resolve({ ...proof("gpu"), gpuVerified: false })));
    await expect(createQwenReranker(paths)).rejects.toMatchObject({ code: "protocol" });
    expect(mocks.spawn).toHaveBeenCalledOnce();
  });
  it("supports CPU-only and rejects oversized inputs without truncation", async () => {
    const cpu = worker(Promise.resolve(proof("cpu"))); mocks.spawn.mockReturnValue(cpu);
    const runtime = await createQwenReranker({ ...paths, device: "cpu" });
    expect(mocks.spawn).toHaveBeenCalledWith({ ...paths, device: "cpu" }, "cpu");
    await expect(runtime.scoreBatch("q".repeat(257), ["x"])).rejects.toMatchObject({ code: "input" });
    await expect(runtime.score("q".repeat(257), "x")).rejects.toMatchObject({ code: "input" });
    await expect(runtime.scoreBatch("q", ["x".repeat(8193)])).rejects.toMatchObject({ code: "input" });
    expect(cpu.scores).not.toHaveBeenCalled();
  });
});
