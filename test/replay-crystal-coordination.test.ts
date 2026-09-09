import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { registerReplayFunctions } from "../src/functions/replay.js";
import { batchEffectKey, runBatchCallback, withBatchMutationLocks, withBatchRecordLocks } from "../src/state/batch-effects.js";
import { KV, fingerprintId } from "../src/state/schema.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/search.js", () => ({ getSearchIndex: () => ({ add: vi.fn() }) }));
vi.mock("node:fs/promises", () => ({
  lstat: async () => ({ isSymbolicLink: () => false, isDirectory: () => false, isFile: () => true }),
  readdir: async () => [],
  readFile: async () => JSON.stringify({ type: "user", sessionId: "session", uuid: "u1", timestamp: "2026-01-01T00:00:00Z", cwd: "D:/project", message: { role: "user", content: [{ type: "text", text: "Complete the work" }] } }),
}));
function deferred() { let release!: () => void; return { promise: new Promise<void>((resolve) => { release = resolve; }), resolve: () => release() }; }

describe("replay crystal admission", () => {
  it("waits for crystallize and rereads the shared crystal record", async () => {
    const h = effectHarness(); registerReplayFunctions(h.sdk as never, h.kv);
    const id = fingerprintId("crystal", "session"), entered = deferred(), finish = deferred();
    const callback = runBatchCallback(h.kv, "crystallize", batchEffectKey("batch"), async (_, admit) => {
      await admit(); entered.resolve(); await finish.promise;
      await withBatchRecordLocks([[KV.crystals, id]], async () => { await h.kv.set(KV.crystals, id, { id, sourceActionIds: ["batch-action"], createdAt: "2025-01-01" }); });
      return { success: true };
    });
    await entered.promise;
    const replay = h.call("mem::replay::import-jsonl", { path: "D:/project/input.jsonl" });
    await Promise.resolve(); expect(await h.kv.list(KV.sessions)).toEqual([]);
    finish.resolve(); await callback;
    expect(await replay).toMatchObject({ success: true });
    expect(await h.kv.get(KV.crystals, id)).toMatchObject({ sourceActionIds: ["batch-action"], createdAt: "2025-01-01" });
  });

  it("blocks replay admission throughout maintenance", async () => {
    const h = effectHarness(); registerReplayFunctions(h.sdk as never, h.kv);
    const entered = deferred(), finish = deferred();
    const maintenance = withBatchMutationLocks(h.kv, async () => { entered.resolve(); await finish.promise; });
    await entered.promise;
    const replay = h.call("mem::replay::import-jsonl", { path: "D:/project/input.jsonl" });
    await Promise.resolve(); expect([...h.store]).toEqual([]);
    finish.resolve(); await maintenance;
    expect(await replay).toMatchObject({ success: true });
  });

  it("rejects replay before writes when crystallize is partially applied", async () => {
    const h = effectHarness(); registerReplayFunctions(h.sdk as never, h.kv);
    await h.kv.set(KV.batchCallbacks, "active:crystallize", { state: "started", activeKey: batchEffectKey("partial") });
    const before = structuredClone([...h.store]);
    await expect(h.call("mem::replay::import-jsonl", { path: "D:/project/input.jsonl" })).rejects.toThrow("recovered");
    expect([...h.store]).toEqual(before);
  });
});
