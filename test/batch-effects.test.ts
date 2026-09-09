import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { batchEffectKey, effectMetadata, runBatchCallback } from "../src/state/batch-effects.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerLessonsFunctions } from "../src/functions/lessons.js";
import { registerExportImportFunction } from "../src/functions/export-import.js";
import { MetricsStore } from "../src/eval/metrics-store.js";
import type { ExportData, Lesson } from "../src/types.js";
import { recordAudit } from "../src/functions/audit.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));

describe("durable batch effects", () => {
  it.each([false, true])("retries lesson writes across crash, after=%s", async (after) => {
    const h = effectHarness();
    registerLessonsFunctions(h.sdk as never, h.kv);
    await h.call("mem::lesson-save", { content: "Keep promises awaited" });
    const payload = { content: "Keep promises awaited", batchEffectKey: batchEffectKey("work") };
    h.crash(KV.lessons, after);
    await expect(h.call("mem::lesson-save", payload)).rejects.toThrow();
    await h.call("mem::lesson-save", payload);
    await h.call("mem::lesson-save", payload);
    const lessons = await h.kv.list<Lesson>(KV.lessons);
    expect(lessons).toHaveLength(1);
    expect(lessons[0]).toMatchObject({ confidence: 0.55, reinforcements: 1, appliedBatchEffects: [payload.batchEffectKey] });
  });

  it("serializes legacy reinforcement and duplicate keyed calls", async () => {
    const h = effectHarness();
    registerLessonsFunctions(h.sdk as never, h.kv);
    await h.call("mem::lesson-save", { content: "same lesson" });
    await Promise.all([
      h.call("mem::lesson-save", { content: "same lesson", batchEffectKey: batchEffectKey("one") }),
      h.call("mem::lesson-save", { content: "same lesson", batchEffectKey: batchEffectKey("one") }),
      h.call("mem::lesson-save", { content: "same lesson" }),
      h.call("mem::lesson-strengthen", { lessonId: fingerprintId("lsn", "same lesson") }),
    ]);
    expect((await h.kv.list<Lesson>(KV.lessons))[0].reinforcements).toBe(3);
  });

  it.each([false, true])("records usage once across acknowledgement loss, after=%s", async (after) => {
    const h = effectHarness();
    const metrics = new MetricsStore(h.kv);
    const usage = { inputTokens: 10, outputTokens: 5, totalTokens: 15 };
    const key = batchEffectKey("usage");
    h.crash(KV.metrics, after);
    await expect(metrics.recordLlmUsage("reflection", "fireworks-batch", "model", usage, key)).rejects.toThrow();
    await metrics.recordLlmUsage("reflection", "fireworks-batch", "model", usage, key);
    await new MetricsStore(h.kv).recordLlmUsage("reflection", "fireworks-batch", "model", usage, key);
    expect(await metrics.get("llm:reflection:fireworks-batch:model")).toMatchObject({ totalCalls: 1, totalTokens: 15 });
  });

  it("does not populate usage cache when persistence fails", async () => {
    const h = effectHarness();
    const metrics = new MetricsStore(h.kv);
    h.crash(KV.metrics);
    await expect(metrics.recordLlmUsage("reflection", "provider", "model", {}, batchEffectKey("usage"))).rejects.toThrow();
    expect(await metrics.get("llm:reflection:provider:model")).toBeNull();
    await Promise.all(Array.from({ length: 4 }, (_, i) => new MetricsStore(h.kv).recordLlmUsage("reflection", "provider", "model", {}, i < 2 ? batchEffectKey("same") : undefined)));
    expect(await metrics.get("llm:reflection:provider:model")).toMatchObject({ totalCalls: 3, unreportedUsageCalls: 3 });
  });

  it("persists admission before effects, retries failure, and keeps receipts payload-free", async () => {
    const h = effectHarness();
    const key = batchEffectKey("one");
    const visits: boolean[] = [];
    await expect(runBatchCallback(h.kv, "test", key, async (resuming, admit) => {
      visits.push(resuming);
      await admit();
      throw new Error("crash before callback");
    })).rejects.toThrow();
    const apply = async (resuming: boolean) => { visits.push(resuming); return { success: true }; };
    await runBatchCallback(h.kv, "test", key, apply);
    await runBatchCallback(h.kv, "test", key, apply);
    expect(visits).toEqual([false, true]);
    expect(await h.kv.list(KV.batchCallbacks)).toEqual([{ state: "completed" }]);
    expect(() => effectMetadata({ appliedBatchEffects: Array.from({ length: 4096 }, (_, i) => batchEffectKey(String(i))) }, batchEffectKey("new"))).toThrow("capacity");
  });

  it("exports lesson metadata, excludes receipts/metrics, and restores dedupe", async () => {
    const source = effectHarness();
    registerLessonsFunctions(source.sdk as never, source.kv);
    registerExportImportFunction(source.sdk as never, source.kv);
    const payload = { content: "durable lesson", batchEffectKey: batchEffectKey("exported") };
    await source.call("mem::lesson-save", payload);
    await source.kv.set(KV.batchCallbacks, "private", { state: "started" });
    await source.kv.set(KV.metrics, "private", { totalCalls: 12 });
    const data = await source.call<ExportData>("mem::export");
    expect(data.lessons?.[0].appliedBatchEffects).toEqual([payload.batchEffectKey]);
    expect(JSON.stringify(data)).not.toContain("private");
    const target = effectHarness();
    registerLessonsFunctions(target.sdk as never, target.kv);
    registerExportImportFunction(target.sdk as never, target.kv);
    await target.call("mem::import", { exportData: data });
    await target.call("mem::lesson-save", payload);
    expect((await target.kv.list<Lesson>(KV.lessons))[0].reinforcements).toBe(0);
  });

  it("rejects invalid keys before mutation and blocks replacement while a callback is partial", async () => {
    const h = effectHarness();
    const run = vi.fn(async () => ({ success: true }));
    await expect(runBatchCallback(h.kv, "graph", "bad", run)).rejects.toThrow("Invalid");
    expect(run).not.toHaveBeenCalled();
    const key = batchEffectKey("partial");
    await runBatchCallback(h.kv, "graph", key, async (_resuming, admit) => { await admit(); return { success: false }; });
    await expect(runBatchCallback(h.kv, "graph", batchEffectKey("new"), run)).rejects.toThrow("recovered");
    registerExportImportFunction(h.sdk as never, h.kv);
    await expect(h.call("mem::import", { exportData: { version: "0.9.42", sessions: [], observations: {}, memories: [], summaries: [] } })).rejects.toThrow("recovered");
    await runBatchCallback(h.kv, "graph", key, run);
    await runBatchCallback(h.kv, "graph", batchEffectKey("new"), run);
    expect(run).toHaveBeenCalledTimes(2);
  });

  it("repairs an acknowledged-lost audit without a second entry", async () => {
    const h = effectHarness();
    const key = batchEffectKey("audit");
    h.crash(KV.audit, true);
    await expect(recordAudit(h.kv, "reflect", "mem::reflect", [], {}, undefined, undefined, key)).rejects.toThrow();
    await recordAudit(h.kv, "reflect", "mem::reflect", [], {}, undefined, undefined, key);
    expect(await h.kv.list(KV.audit)).toHaveLength(1);
  });
});
