import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
const mocks = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn: mocks.spawn }));
import { QwenProcess } from "../src/state/qwen-reranker-runtime.js";
const paths = { python: "python", script: "/scorer", model: "/model", cpuRuntime: "/cpu", gpuRuntime: "/cuda", device: "auto" as const };
function setup() {
  const child = Object.assign(new EventEmitter(), { stdin: new PassThrough(), stdout: new PassThrough(), stderr: new PassThrough(), pid: 123, kill: vi.fn() });
  child.kill.mockImplementation(() => { queueMicrotask(() => child.emit("close", null)); return true; });
  child.stdin.on("finish", () => queueMicrotask(() => child.emit("close", 0)));
  mocks.spawn.mockReturnValue(child);
  return child;
}
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });
describe("owned Qwen IPC", () => {
  it("starts without shell/window/network, validates response IDs and closes on malformed score", async () => {
    const child = setup(); const process = new QwenProcess(paths, "gpu");
    child.stdout.write('{"ready":true}\n'); await process.ready;
    expect(mocks.spawn).toHaveBeenCalledWith("python", expect.arrayContaining(["-I", "-u", "/scorer", "/cuda", "gpu"]), { windowsHide: true, stdio: ["pipe", "pipe", "pipe"] });
    const pending = process.scores("query", ["doc"]); child.stdout.write('{"id":1,"scores":[0.7]}\n');
    expect(await pending).toEqual([.7]);
    const wrong = process.scores("query", ["doc"]); child.stdout.write('{"id":99,"scores":[0.4]}\n');
    await expect(wrong).rejects.toMatchObject({ code: "protocol" }); await process.close();
  });
  it("cancels and reaps the owned worker", async () => {
    const child = setup(); const process = new QwenProcess(paths, "cpu");
    child.stdout.write('{"ready":true}\n'); await process.ready;
    const controller = new AbortController(); const result = process.scores("q", ["d"], controller.signal);
    controller.abort(); await expect(result).rejects.toMatchObject({ code: "cancelled" });
    expect(child.kill).toHaveBeenCalled(); await process.close();
  });
  it("fails and closes on startup deadline", async () => {
    vi.useFakeTimers(); const child = setup(); const process = new QwenProcess(paths, "gpu", 10);
    const failure = expect(process.ready).rejects.toMatchObject({ code: "timeout" });
    await vi.advanceTimersByTimeAsync(11); await failure; expect(child.kill).toHaveBeenCalled(); await process.close();
  });
  it("rejects oversized and nonfinite responses", async () => {
    const child = setup(); const process = new QwenProcess(paths, "gpu");
    child.stdout.write("x".repeat(65537)); await expect(process.ready).rejects.toMatchObject({ code: "protocol" });
    await process.close();
  });
});

