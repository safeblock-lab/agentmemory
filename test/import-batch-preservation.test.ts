import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { registerExportImportFunction } from "../src/functions/export-import.js";
import { batchEffectKey, runBatchCallback } from "../src/state/batch-effects.js";
import { KV } from "../src/state/schema.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
const empty = { version: "0.9.42", sessions: [], memories: [], summaries: [], observations: {} };
const destinations = [[KV.lessons, "lessons"], [KV.insights, "insights"], [KV.semantic, "semanticMemories"], [KV.procedural, "proceduralMemories"], [KV.graphNodes, "graphNodes"], [KV.graphEdges, "graphEdges"]] as const;
function setup() { const h = effectHarness(); registerExportImportFunction(h.sdk as never, h.kv); return h; }

describe("import preserves durable batch state", () => {
  it.each(destinations)("merges legacy %s records without erasing receipts, provenance, or counters", async (scope, field) => {
    const h = setup(), key = batchEffectKey("local");
    await h.kv.set(scope, "id", { id: "id", sourceSessionIds: ["local"], sourceObservationIds: ["observation"], accessCount: 3, reinforcements: 4, frequency: 5, appliedBatchEffects: [key] });
    const result = await h.call("mem::import", { exportData: { ...empty, [field]: [{ id: "id", sourceSessionIds: ["remote"], accessCount: 1, reinforcements: 1, frequency: 1 }] } });
    expect(result).toMatchObject({ success: true });
    expect(await h.kv.get(scope, "id")).toMatchObject({ sourceSessionIds: ["local", "remote"], sourceObservationIds: ["observation"], accessCount: 3, reinforcements: 4, frequency: 5, appliedBatchEffects: [key] });
  });

  it("rereads after the active callback finishes before preparing the import", async () => {
    const h = setup(), key = batchEffectKey("inflight");
    let admitted!: () => void, release!: () => void;
    const entered = new Promise<void>((resolve) => { admitted = resolve; });
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const callback = runBatchCallback(h.kv, "consolidation", key, async (_, admit) => {
      await admit(); admitted(); await pending;
      await h.kv.set(KV.procedural, "id", { id: "id", frequency: 2, sourceSessionIds: ["batch"], appliedBatchEffects: [key] });
      return { success: true };
    });
    await entered;
    const importing = h.call("mem::import", { exportData: { ...empty, proceduralMemories: [{ id: "id", frequency: 1, sourceSessionIds: ["remote"] }] } });
    release(); await callback;
    expect(await importing).toMatchObject({ success: true });
    expect(await h.kv.get(KV.procedural, "id")).toMatchObject({ frequency: 2, appliedBatchEffects: [key], sourceSessionIds: ["batch", "remote"] });
  });

  it("rejects a combined 4097 receipt union before any imported write", async () => {
    const h = setup(); const keys = Array.from({ length: 4096 }, (_, i) => batchEffectKey(String(i)));
    await h.kv.set(KV.lessons, "full", { id: "full", appliedBatchEffects: keys });
    const before = structuredClone([...h.store]);
    const result = await h.call("mem::import", { exportData: { ...empty, sessions: [{ id: "would-write-first" }], lessons: [{ id: "full", appliedBatchEffects: [batchEffectKey("new")] }] } });
    expect(result).toMatchObject({ success: false, error: expect.stringContaining("capacity") });
    expect([...h.store]).toEqual(before);
  });

  it("accepts the exact 4096 union and repeated import without growing it", async () => {
    const h = setup(); const keys = Array.from({ length: 4095 }, (_, i) => batchEffectKey(String(i)));
    await h.kv.set(KV.lessons, "full", { id: "full", appliedBatchEffects: keys });
    const payload = { exportData: { ...empty, lessons: [{ id: "full", appliedBatchEffects: [batchEffectKey("last")] }] } };
    expect(await h.call("mem::import", payload)).toMatchObject({ success: true });
    expect(await h.call("mem::import", payload)).toMatchObject({ success: true });
    expect((await h.kv.get<{ appliedBatchEffects: string[] }>(KV.lessons, "full"))?.appliedBatchEffects).toHaveLength(4096);
  });

  it("preflights cumulative duplicate IDs and rejects overflow before mutation", async () => {
    const h = setup(); const keys = Array.from({ length: 4095 }, (_, i) => batchEffectKey(String(i)));
    await h.kv.set(KV.lessons, "full", { id: "full", appliedBatchEffects: keys });
    const before = structuredClone([...h.store]);
    const result = await h.call("mem::import", { exportData: { ...empty, lessons: [{ id: "full", appliedBatchEffects: [batchEffectKey("one")] }, { id: "full", appliedBatchEffects: [batchEffectKey("two")] }] } });
    expect(result).toMatchObject({ success: false }); expect([...h.store]).toEqual(before);
  });

  it.each(["metadata", "callback-receipt", "snapshot-metadata"])("refuses replace when %s is protected", async (protection) => {
    const h = setup();
    await h.kv.set(KV.sessions, "keep", { id: "keep" });
    if (protection === "callback-receipt") await h.kv.set(KV.batchCallbacks, `graph:${batchEffectKey("effect")}`, { state: "completed" });
    else await h.kv.set(protection === "metadata" ? KV.lessons : KV.graphSnapshot, "current", { appliedBatchEffects: [batchEffectKey("effect")] });
    const before = structuredClone([...h.store]);
    expect(await h.call("mem::import", { strategy: "replace", exportData: empty })).toMatchObject({ success: false, error: expect.stringContaining("Replace blocked") });
    expect([...h.store]).toEqual(before);
  });

  it("retains legacy replace semantics for an unprotected store", async () => {
    const h = setup(); await h.kv.set(KV.sessions, "old", { id: "old" });
    expect(await h.call("mem::import", { strategy: "replace", exportData: { ...empty, version: "0.3.0", sessions: [{ id: "new" }] } })).toMatchObject({ success: true });
    expect(await h.kv.list(KV.sessions)).toEqual([{ id: "new" }]);
  });

  it("rejects a pending callback before changing the store", async () => {
    const h = setup(); await h.kv.set(KV.batchCallbacks, "active:consolidation", { state: "started", activeKey: batchEffectKey("pending") });
    const before = structuredClone([...h.store]);
    await expect(h.call("mem::import", { exportData: empty })).rejects.toThrow("recovered");
    expect([...h.store]).toEqual(before);
  });

  it.each(["sessions", "memories", "summaries", "profiles", "graphNodes", "graphEdges", "semanticMemories", "proceduralMemories", "actions", "actionEdges", "routines", "signals", "checkpoints", "sentinels", "sketches", "crystals", "facets", "lessons", "insights", "accessLogs"])("rejects malformed %s before replace deletes anything", async (section) => {
    const h = setup();
    await h.kv.set(KV.sessions, "keep", { id: "keep" });
    await h.kv.set(KV.memories, "memory", { id: "memory", content: "Keep previous data" });
    await h.kv.set(KV.accessLog, "memory", { memoryId: "memory", count: 3, recent: [] });
    const before = structuredClone([...h.store]);
    expect(await h.call("mem::import", { strategy: "replace", exportData: { ...empty, [section]: { invalid: true } } })).toMatchObject({ success: false });
    expect([...h.store]).toEqual(before);
  });

  it.each([null, false, [null], [{}], [{ memoryId: 4 }], Array.from({ length: 50001 }, () => ({ memoryId: "memory" }))])("rejects invalid accessLogs before any destructive operation", async (accessLogs) => {
    const h = setup();
    await h.kv.set(KV.memories, "memory", { id: "memory", content: "Keep me" });
    const before = structuredClone([...h.store]);
    expect(await h.call("mem::import", { strategy: "replace", exportData: { ...empty, accessLogs } })).toMatchObject({ success: false });
    expect([...h.store]).toEqual(before);
  });

  it("rejects malformed nested observation records before replace", async () => {
    const h = setup(); await h.kv.set(KV.sessions, "keep", { id: "keep" });
    const before = structuredClone([...h.store]);
    expect(await h.call("mem::import", { strategy: "replace", exportData: { ...empty, observations: { imported: [null] } } })).toMatchObject({ success: false });
    expect([...h.store]).toEqual(before);
  });

  it("normalizes legacy access logs during preflight and then replaces valid data", async () => {
    const h = setup(); await h.kv.set(KV.memories, "old", { id: "old" });
    expect(await h.call("mem::import", { strategy: "replace", exportData: { ...empty, memories: [{ id: "new" }], accessLogs: [{ memoryId: "new", recent: [1, "invalid", 2], count: -1 }] } })).toMatchObject({ success: true });
    expect(await h.kv.get(KV.memories, "old")).toBeNull();
    expect(await h.kv.get(KV.accessLog, "new")).toMatchObject({ memoryId: "new", count: 2, recent: [1, 2], lastAt: "" });
  });
});
