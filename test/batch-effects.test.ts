import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { batchEffectKey, createGraphBatchCallbackPreflight, effectMetadata, runBatchCallback } from "../src/state/batch-effects.js";
import { graphKV, runGraphJob } from "../src/functions/graph-jobs.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerLessonsFunctions } from "../src/functions/lessons.js";
import { registerExportImportFunction } from "../src/functions/export-import.js";
import { MetricsStore } from "../src/eval/metrics-store.js";
import type { ExportData, Lesson } from "../src/types.js";
import { recordAudit } from "../src/functions/audit.js";
import { StateTransactionError } from "../src/state/state-transactions.js";

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
    const lease = vi.spyOn(h.kv, "lease");
    const getVersioned = vi.spyOn(h.kv, "getVersioned");
    const commitBatch = vi.spyOn(h.kv, "commitBatch");
    const set = vi.spyOn(h.kv, "set");
    await expect(runBatchCallback(h.kv, "graph", "bad", run)).rejects.toThrow("Invalid");
    expect(run).not.toHaveBeenCalled();
    expect(lease).not.toHaveBeenCalled();
    expect(getVersioned).not.toHaveBeenCalled();
    expect(commitBatch).not.toHaveBeenCalled();
    expect(set).not.toHaveBeenCalled();
    expect(h.store.size).toBe(0);

    expect(() => createGraphBatchCallbackPreflight("bad", () => ({ success: true }))).toThrow("Invalid batch effect key");
    const graphState = graphKV(h.kv);
    const inGraphJob = (durableId: string, key: string, callback: () => Promise<unknown>) => runGraphJob(
      graphState,
      "batch_callback",
      { batchEffectKey: key },
      callback,
      durableId,
      createGraphBatchCallbackPreflight(key, (state) => state === "stale" ? { success: true, stale: true } : { success: true }),
    );
    const key = batchEffectKey("partial");
    const partialJobId = "partial-callback-attempt";
    await inGraphJob(partialJobId, key, () => runBatchCallback(graphState, "graph", key, async (_resuming, admit) => { await admit(); return { success: false }; }));
    const frozenRun = vi.fn(async () => runBatchCallback(graphState, "graph", key, run));
    await expect(inGraphJob(partialJobId, key, frozenRun)).resolves.toEqual({ success: false });
    expect(frozenRun).not.toHaveBeenCalled();

    const admissionSnapshot = () => [...h.store.entries()]
      .filter(([address]) => [
        KV.graphJobs,
        KV.graphCheckpoints,
        KV.graphReceipts,
        KV.graphWorkingSnapshots,
        KV.graphInputs(""),
        KV.graphProviderResults(""),
        KV.graphDeltas(""),
        KV.graphPrepared(""),
        KV.graphRemaps(""),
        KV.batchCallbacks,
      ].some((scope) => address.startsWith(`${scope}:`)))
      .sort(([left], [right]) => left.localeCompare(right));
    const beforeCompetingAttempt = admissionSnapshot();
    const competingKey = batchEffectKey("new");
    await expect(inGraphJob("competing-callback-attempt", competingKey, () => runBatchCallback(graphState, "graph", competingKey, run))).rejects.toThrow("recovered");
    expect(admissionSnapshot()).toEqual(beforeCompetingAttempt);
    expect(h.store.get(`${KV.batchCallbacks}:graph:${key}`)).toMatchObject({ state: "started" });
    expect(h.store.get(`${KV.batchCallbacks}:active:graph`)).toMatchObject({ state: "started", activeKey: key });
    expect(h.store.has(`${KV.graphJobs}:competing-callback-attempt`)).toBe(false);
    expect(h.store.has(`${KV.graphInputs("competing-callback-attempt")}:0`)).toBe(false);
    expect(h.store.has(`${KV.graphDeltas("competing-callback-attempt")}:manifest:1`)).toBe(false);
    registerExportImportFunction(h.sdk as never, h.kv);
    await expect(h.call("mem::import", { exportData: { version: "0.9.42", sessions: [], observations: {}, memories: [], summaries: [] } })).rejects.toThrow("recovered");
    await inGraphJob("partial-callback-repair-attempt", key, () => runBatchCallback(graphState, "graph", key, run));
    await inGraphJob("next-callback-attempt", competingKey, () => runBatchCallback(graphState, "graph", competingKey, run));
    expect(run).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["completed", { success: true }, { success: true }],
    ["stale", { success: true, stale: true }, { success: true, stale: true }],
  ] as const)("preflights a %s receipt before a new caller attempt is captured", async (_state, callbackResult, preflightResult) => {
    const h = effectHarness();
    const graphState = graphKV(h.kv);
    const key = batchEffectKey(`terminal-${_state}`);
    const runAttempt = (id: string, callback: () => Promise<unknown>) => runGraphJob(
      graphState,
      "batch_callback",
      { batchEffectKey: key },
      callback,
      id,
      createGraphBatchCallbackPreflight(key, (state) => state === "stale" ? { success: true, stale: true } : { success: true }),
    );
    await expect(runAttempt(`terminal-${_state}-original`, () => runBatchCallback(graphState, "graph", key, async () => callbackResult))).resolves.toEqual(callbackResult);
    const beforePreflight = [...h.store.entries()]
      .filter(([address]) => [KV.graphJobs, KV.graphCheckpoints, KV.graphReceipts, KV.graphWorkingSnapshots, KV.graphInputs(""), KV.graphProviderResults(""), KV.graphDeltas(""), KV.graphPrepared(""), KV.graphRemaps(""), KV.batchCallbacks].some((scope) => address.startsWith(`${scope}:`)))
      .sort(([left], [right]) => left.localeCompare(right));
    const shouldNotRun = vi.fn(async () => ({ success: true }));
    await expect(runAttempt(`terminal-${_state}-duplicate-caller`, shouldNotRun)).resolves.toEqual(preflightResult);
    expect(shouldNotRun).not.toHaveBeenCalled();
    const afterPreflight = [...h.store.entries()]
      .filter(([address]) => [KV.graphJobs, KV.graphCheckpoints, KV.graphReceipts, KV.graphWorkingSnapshots, KV.graphInputs(""), KV.graphProviderResults(""), KV.graphDeltas(""), KV.graphPrepared(""), KV.graphRemaps(""), KV.batchCallbacks].some((scope) => address.startsWith(`${scope}:`)))
      .sort(([left], [right]) => left.localeCompare(right));
    expect(afterPreflight).toEqual(beforePreflight);
    expect(h.store.has(`${KV.graphJobs}:terminal-${_state}-duplicate-caller`)).toBe(false);
    expect(h.store.has(`${KV.graphInputs(`terminal-${_state}-duplicate-caller`)}:0`)).toBe(false);
  });

  it("rejects malformed durable receipts before capturing a graph job", async () => {
    const h = effectHarness();
    const graphState = graphKV(h.kv);
    const key = batchEffectKey("malformed-receipt");
    h.store.set(`${KV.batchCallbacks}:graph:${key}`, { state: "invalid" });
    const before = [...h.store.entries()]
      .filter(([address]) => [KV.graphJobs, KV.graphCheckpoints, KV.graphReceipts, KV.graphWorkingSnapshots, KV.graphInputs(""), KV.graphProviderResults(""), KV.graphDeltas(""), KV.graphPrepared(""), KV.graphRemaps(""), KV.batchCallbacks].some((scope) => address.startsWith(`${scope}:`)))
      .sort(([left], [right]) => left.localeCompare(right));
    await expect(runGraphJob(
      graphState,
      "batch_callback",
      { batchEffectKey: key },
      async () => ({ success: true }),
      "malformed-receipt-attempt",
      createGraphBatchCallbackPreflight(key, () => ({ success: true })),
    )).rejects.toThrow("Ambiguous batch callback receipt state");
    const after = [...h.store.entries()]
      .filter(([address]) => [KV.graphJobs, KV.graphCheckpoints, KV.graphReceipts, KV.graphWorkingSnapshots, KV.graphInputs(""), KV.graphProviderResults(""), KV.graphDeltas(""), KV.graphPrepared(""), KV.graphRemaps(""), KV.batchCallbacks].some((scope) => address.startsWith(`${scope}:`)))
      .sort(([left], [right]) => left.localeCompare(right));
    expect(after).toEqual(before);
    expect(h.store.has(`${KV.graphJobs}:malformed-receipt-attempt`)).toBe(false);
  });

  it("replays an admitted job when a completed callback receipt outlives job finalization", async () => {
    const h = effectHarness();
    const graphState = graphKV(h.kv);
    const key = batchEffectKey("lost-job-finalization-ack");
    const durableId = "lost-job-finalization-attempt";
    const input = { batchEffectKey: key };
    const runAttempt = (run: () => Promise<unknown>) => runGraphJob(
      graphState,
      "batch_callback",
      input,
      run,
      durableId,
      createGraphBatchCallbackPreflight(key, (state) => state === "stale" ? { success: true, stale: true } : { success: true }),
    );
    const effect = vi.fn(async () => ({ success: true }));
    const commitBatch = h.kv.commitBatch.bind(h.kv);
    let injectedFailures = 0;
    const failingCommit = vi.spyOn(h.kv, "commitBatch").mockImplementation(async (guard, prepared) => {
      const receipt = h.store.get(`${KV.batchCallbacks}:graph:${key}`) as { state?: string } | undefined;
      if (receipt?.state === "completed" && injectedFailures < 2) {
        injectedFailures++;
        throw new StateTransactionError("STATE_TX_FAILED");
      }
      return commitBatch(guard, prepared);
    });
    try {
      await expect(runAttempt(() => runBatchCallback(graphState, "graph", key, effect))).rejects.toThrow("STATE_TX_FAILED");
    } finally {
      failingCommit.mockRestore();
    }
    expect(injectedFailures).toBe(2);
    expect(h.store.get(`${KV.batchCallbacks}:graph:${key}`)).toMatchObject({ state: "completed" });
    expect(h.store.get(`${KV.graphJobs}:${durableId}`)).toMatchObject({ state: "staging", captureComplete: true });

    await expect(runAttempt(() => runBatchCallback(graphState, "graph", key, effect))).resolves.toEqual({ success: true });
    expect(effect).toHaveBeenCalledTimes(1);
    expect(h.store.get(`${KV.graphJobs}:${durableId}`)).toMatchObject({ state: "completed" });
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
