import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { applyBatchEffect, batchEffectKey, effectMetadata, preserveBatchProvenance, runBatchCallback, withBatchMutationLocks, withBatchRecordLocks, withBatchWriterLocks } from "../src/state/batch-effects.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerTemporalGraphFunctions } from "../src/functions/temporal-graph.js";
import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { registerCascadeFunction } from "../src/functions/cascade.js";
import type { GraphEdge, MemoryProvider } from "../src/types.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
function deferred() {
  let resolve!: () => void;
  return { promise: new Promise<void>((r) => { resolve = r; }), resolve: () => resolve() };
}

describe("single-worker writer coordination", () => {
  it("waits for admitted callbacks, allows their nested calls, and blocks new admission during maintenance", async () => {
    const h = effectHarness();
    const entered = deferred(), finish = deferred(), maintenanceEntered = deferred(), finishMaintenance = deferred();
    const order: string[] = [];
    const callback = runBatchCallback(h.kv, "consolidation", batchEffectKey("outer"), async (_, admit) => {
      await admit(); entered.resolve(); await finish.promise;
      await runBatchCallback(h.kv, "reflect", batchEffectKey("child"), async (_, admitChild) => { await admitChild(); order.push("child"); return { success: true }; });
      order.push("callback"); return { success: true };
    });
    await entered.promise;
    const bulk = withBatchMutationLocks(h.kv, async () => { order.push("maintenance"); maintenanceEntered.resolve(); await finishMaintenance.promise; });
    finish.resolve(); await callback; await maintenanceEntered.promise;
    const writer = withBatchWriterLocks(h.kv, ["graph"], async () => { order.push("writer"); });
    await Promise.resolve(); expect(order).toEqual(["child", "callback", "maintenance"]);
    finishMaintenance.resolve(); await Promise.all([bulk, writer]);
    expect(order).toEqual(["child", "callback", "maintenance", "writer"]);
  });

  it("rejects pending and ambiguous admissions without running maintenance or competitors", async () => {
    const h = effectHarness(); const run = vi.fn(async () => true);
    await h.kv.set(KV.batchCallbacks, "active:graph", { state: "started", activeKey: batchEffectKey("pending") });
    await expect(withBatchMutationLocks(h.kv, run)).rejects.toThrow("recovered");
    await expect(withBatchWriterLocks(h.kv, ["graph"], run)).rejects.toThrow("recovered");
    await h.kv.set(KV.batchCallbacks, "active:graph", { state: "started" });
    await expect(withBatchMutationLocks(h.kv, run)).rejects.toThrow("Ambiguous");
    expect(run).not.toHaveBeenCalled();
  });

  it("bounds maintenance waiting without interrupting an admitted callback", async () => {
    vi.useFakeTimers();
    const h = effectHarness(), entered = deferred(), finish = deferred();
    const callback = runBatchCallback(h.kv, "graph", undefined, async () => { entered.resolve(); await finish.promise; return { success: true }; });
    try {
      await entered.promise;
      const run = vi.fn(async () => true);
      const failed = expect(withBatchMutationLocks(h.kv, run)).rejects.toThrow("quiescence timed out");
      await vi.advanceTimersByTimeAsync(30001);
      await failed; expect(run).not.toHaveBeenCalled();
      finish.resolve(); await callback;
      expect(await withBatchMutationLocks(h.kv, async () => true)).toBe(true);
    } finally { finish.resolve(); await callback; vi.useRealTimers(); }
  });

  it("orders and deduplicates record locks independently of caller order", async () => {
    let value = 0;
    await Promise.all(Array.from({ length: 20 }, (_, i) => withBatchRecordLocks(i % 2 ? [["scope", "a"], ["scope", "b"]] : [["scope", "b"], ["scope", "a"], ["scope", "a"]], async () => {
      const read = value; await Promise.resolve(); value = read + 1;
    })));
    expect(value).toBe(20);
  });

  it("accepts 4095→4096, replays at capacity, and rejects overflow before persistence", async () => {
    const h = effectHarness(); const keys = Array.from({ length: 4095 }, (_, i) => batchEffectKey(String(i)));
    await h.kv.set("count", "id", { value: 1, appliedBatchEffects: keys });
    const key = batchEffectKey("last");
    const change = vi.fn((current: { value: number; appliedBatchEffects?: string[] } | null) => ({ ...current!, value: current!.value + 1 }));
    await applyBatchEffect(h.kv, "count", "id", key, change);
    await applyBatchEffect(h.kv, "count", "id", key, change);
    await expect(applyBatchEffect(h.kv, "count", "id", batchEffectKey("overflow"), change)).rejects.toThrow("capacity");
    expect(change).toHaveBeenCalledTimes(1);
    expect(await h.kv.get("count", "id")).toMatchObject({ value: 2 });
    expect(() => effectMetadata({ appliedBatchEffects: [...keys, key, batchEffectKey("overflow")] }, key)).toThrow("Invalid");
    expect(effectMetadata(null)).toEqual({});
  });

  it("preserves receipt metadata and provenance across legacy incoming records", () => {
    const key = batchEffectKey("local");
    const current = { sourceSessionIds: ["local"], frequency: 3, appliedBatchEffects: [key] };
    const next = preserveBatchProvenance(current, { sourceSessionIds: ["remote"], frequency: 1 });
    expect(next).toEqual({ sourceSessionIds: ["local", "remote"], frequency: 3, appliedBatchEffects: [key] });
    expect(current.sourceSessionIds).toEqual(["local"]);
  });

  it("rereads skills under the callback lock before reinforcing", async () => {
    const h = effectHarness();
    await h.kv.set(KV.sessions, "s", { id: "s", status: "completed" });
    await h.kv.set(KV.summaries, "s", { title: "Test", narrative: "Test", keyDecisions: [], filesModified: [], concepts: [] });
    for (let i = 0; i < 3; i++) await h.kv.set(KV.observations("s"), String(i), { title: "Test", narrative: "Test", concepts: [], files: [] });
    const response = '<skill><title>Test</title><trigger>Test</trigger><steps><step>First</step><step>Second</step></steps></skill>';
    registerSkillExtractFunctions(h.sdk as never, h.kv, { summarize: async () => response } as unknown as MemoryProvider);
    const id = fingerprintId("skill", JSON.stringify({ title: "test", trigger: "test", steps: ["first", "second"] }));
    const entered = deferred(), finish = deferred(); const key = batchEffectKey("skill-batch");
    const callback = runBatchCallback(h.kv, "consolidation", key, async (_, admit) => {
      await admit(); entered.resolve(); await finish.promise;
      await h.kv.set(KV.procedural, id, { id, strength: 0.5, frequency: 2, sourceSessionIds: ["batch"], appliedBatchEffects: [key] });
      return { success: true };
    });
    await entered.promise;
    const extraction = h.call("mem::skill-extract", { sessionId: "s" });
    finish.resolve(); await callback;
    expect(await extraction).toMatchObject({ success: true });
    expect(await h.kv.get(KV.procedural, id)).toMatchObject({ frequency: 3, sourceSessionIds: ["batch", "s"], appliedBatchEffects: [key] });
  });

  it("cascade rereads graph records after a concurrent callback", async () => {
    const h = effectHarness(); registerCascadeFunction(h.sdk as never, h.kv);
    await h.kv.set(KV.memories, "m", { sourceObservationIds: ["old"] });
    const entered = deferred(), finish = deferred();
    const callback = runBatchCallback(h.kv, "graph", batchEffectKey("graph-batch"), async (_, admit) => {
      await admit(); entered.resolve(); await finish.promise;
      await h.kv.set(KV.graphNodes, "n", { id: "n", sourceObservationIds: ["old", "batch"], properties: { preserved: true } });
      return { success: true };
    });
    await entered.promise;
    const cascade = h.call("mem::cascade-update", { supersededMemoryId: "m" });
    finish.resolve(); await Promise.all([callback, cascade]);
    expect(await h.kv.get(KV.graphNodes, "n")).toMatchObject({ stale: true, sourceObservationIds: ["old", "batch"], properties: { preserved: true } });
  });
});

const xml = '<entity type="person" name="Alice"></entity><entity type="technology" name="TypeScript"></entity><relationship type="uses" source="Alice" target="TypeScript" weight="0.8"></relationship>';
const payload = { observations: [{ id: "obs", title: "Use TypeScript", narrative: "Alice uses TypeScript", concepts: [], files: [], type: "decision", timestamp: "2025-01-01T00:00:00Z" }] };
describe("temporal retry convergence", () => {
  it.each([KV.graphNodes, KV.graphEdges, KV.graphEdgeHistory, KV.batchCallbacks].flatMap((scope) => [false, true].map((after) => ({ scope, after }))))("converges after failure at $scope, after=$after", async ({ scope, after }) => {
    const h = effectHarness();
    await h.kv.set(KV.graphNodes, "alice", { id: "alice", type: "person", name: "Alice", properties: {}, sourceObservationIds: ["old"], createdAt: "2024-01-01" });
    await h.kv.set(KV.graphNodes, "ts", { id: "ts", type: "technology", name: "TypeScript", properties: {}, sourceObservationIds: ["old"], createdAt: "2024-01-01" });
    await h.kv.set(KV.graphEdges, "old", { id: "old", sourceNodeId: "alice", targetNodeId: "ts", type: "uses", version: 1, isLatest: true });
    const provider = { compress: vi.fn(async () => xml) } as unknown as MemoryProvider;
    registerTemporalGraphFunctions(h.sdk as never, h.kv, provider);
    h.crash(scope, after);
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: false });
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: true });
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: true });
    const edges = await h.kv.list<GraphEdge>(KV.graphEdges);
    expect(edges).toHaveLength(2);
    expect(edges.filter((edge) => edge.isLatest)).toHaveLength(1);
    expect(edges.find((edge) => edge.isLatest)?.version).toBe(2);
    expect(await h.kv.list(KV.graphEdgeHistory)).toHaveLength(1);
  });

  it("fails closed if a partial retry produces different model output", async () => {
    const h = effectHarness(); const compress = vi.fn(async () => xml);
    registerTemporalGraphFunctions(h.sdk as never, h.kv, { compress } as unknown as MemoryProvider);
    h.crash(KV.graphNodes, true);
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: false });
    compress.mockResolvedValue(xml.replace("0.8", "0.9"));
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: false, error: "Temporal extraction result changed during recovery" });
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(0);
  });

  it("does not create a temporal cycle when replaying multiple versions from one result", async () => {
    const h = effectHarness();
    const response = xml + '<relationship type="uses" source="Alice" target="TypeScript" weight="0.9"></relationship>';
    registerTemporalGraphFunctions(h.sdk as never, h.kv, { compress: async () => response } as unknown as MemoryProvider);
    h.crash(KV.batchCallbacks, false, 3);
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: false });
    expect(await h.call("mem::temporal-graph-extract", payload)).toMatchObject({ success: true });
    const edges = await h.kv.list<GraphEdge>(KV.graphEdges);
    expect(edges).toHaveLength(2);
    expect(edges.find((edge) => edge.isLatest)).toMatchObject({ version: 2, weight: 0.9 });
    expect(edges.find((edge) => edge.version === 1)?.supersededBy).toBe(edges.find((edge) => edge.version === 2)?.id);
  });
});
