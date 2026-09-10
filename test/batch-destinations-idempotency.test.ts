import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { registerCrystallizeFunction } from "../src/functions/crystallize.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { registerLessonsFunctions } from "../src/functions/lessons.js";
import type { GraphSnapshot, Lesson, Insight, ProceduralMemory, FireworksBatchRequest } from "../src/types.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/config.js", () => ({ getGraphExtractionInputTargetChars: () => 10000, isConsolidationEnabled: () => true, getConsolidationDecayDays: () => 30, getConsolidationMinNewSummaries: () => 1 }));

const graphXml = '<entity type="concept" name="A"/><entity type="concept" name="B"/><relationship type="related_to" source="A" target="B" weight="1"/>';
const observation = { id: "obs", title: "title", narrative: "body", facts: [], concepts: [], files: [], type: "discovery" };

describe("batch destination crash recovery", () => {
  it.each([KV.graphNodes, KV.graphNameIndex, KV.graphEdges, KV.graphEdgeKey, KV.graphNodeDegree, KV.graphSnapshot, KV.batchCallbacks])("dedupes graph across committed %s write loss", async (scope) => {
    const h = effectHarness();
    registerGraphFunction(h.sdk as never, h.kv, {} as never);
    const payload = { observations: [observation], batchResponse: graphXml, batchEffectKey: batchEffectKey("graph") };
    h.crash(scope, true, scope === KV.batchCallbacks ? 3 : 1);
    await expect(h.call("mem::graph-extract", payload)).rejects.toThrow();
    await h.call("mem::graph-extract", payload);
    await h.call("mem::graph-extract", payload);
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(1);
    const snap = await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
    expect(snap?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
    expect(Object.values(snap!.topDegrees)).toEqual([1, 1]);
    expect(await h.kv.list(KV.audit)).toHaveLength(1);
  });

  it("merges separate graph callbacks onto canonical endpoints", async () => {
    const h = effectHarness();
    registerGraphFunction(h.sdk as never, h.kv, {} as never);
    await Promise.all(["one", "two", "one"].map((id) => h.call("mem::graph-extract", {
      observations: [observation], batchResponse: graphXml, batchEffectKey: batchEffectKey(id),
    })));
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(1);
    expect((await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current"))?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
  });

  it.each(["", "<entities/><relationships/>"])(
    "keeps empty graph callback %j retryable before applying a later valid result",
    async (batchResponse) => {
    const h = effectHarness();
    const provider = { compress: vi.fn().mockResolvedValue(graphXml) };
    registerGraphFunction(h.sdk as never, h.kv, provider as never);
    const key = batchEffectKey("empty-then-valid-graph");
    const payload = {
      observations: [observation],
      batchEffectKey: key,
    };

    const empty = await h.call("mem::graph-extract", {
      ...payload,
      batchResponse,
    });
    expect(empty).toMatchObject({
      success: false,
      error: "Graph extraction response contained no nodes or edges",
    });
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(0);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(0);
    expect(await h.kv.get(KV.graphSnapshot, "current")).toBeNull();
    expect(await h.kv.get(KV.batchCallbacks, `graph:${key}`)).toBeNull();
    expect(await h.kv.get(KV.batchCallbacks, "active:graph")).toBeNull();
    expect(await h.kv.list(KV.audit)).toHaveLength(0);
    expect(provider.compress).not.toHaveBeenCalled();

    const validPayload = { ...payload, batchResponse: graphXml };
    expect(await h.call("mem::graph-extract", validPayload)).toMatchObject({ success: true });
    const firstSnapshot = await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
    expect(firstSnapshot?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });

    expect(await h.call("mem::graph-extract", validPayload)).toMatchObject({ success: true });
    const secondSnapshot = await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
    expect(secondSnapshot?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(1);
    expect(await h.kv.list(KV.audit)).toHaveLength(1);
    },
  );

  it.each([KV.insights, KV.batchCallbacks])("dedupes reflection after %s write loss", async (scope) => {
    const h = effectHarness();
    registerReflectFunctions(h.sdk as never, h.kv, {} as never);
    const cluster = { concepts: [], facts: [], lessons: [], crystalNarratives: [], factIds: [], lessonIds: [], crystalIds: [] };
    const payload = {
      batchResponse: '<insight confidence="0.7" title="Useful">Use bounded retries</insight>',
      batchCluster: JSON.stringify(cluster),
      batchSourceFingerprint: fingerprintId("fwbreflect", JSON.stringify({ concepts: [], facts: [], lessons: [], crystals: [] })),
      batchEffectKey: batchEffectKey("reflect"),
    };
    h.crash(scope, true, scope === KV.batchCallbacks ? 3 : 1);
    await expect(h.call("mem::reflect", payload)).rejects.toThrow();
    await h.call("mem::reflect", payload);
    await h.call("mem::reflect", payload);
    expect((await h.kv.list<Insight>(KV.insights))[0].reinforcements).toBe(0);
  });

  it.each([KV.crystals, KV.lessons, KV.actions, KV.batchCallbacks])("dedupes crystal and lesson through partial %s failure", async (scope) => {
    const h = effectHarness();
    registerLessonsFunctions(h.sdk as never, h.kv);
    let queued: FireworksBatchRequest | undefined;
    registerCrystallizeFunction(h.sdk as never, h.kv, {} as never, undefined, { async enqueue(request) { queued = request; return { queued: true, workItemId: "queued" }; } });
    await h.kv.set(KV.actions, "action", { id: "action", title: "done", description: "finished", status: "done", createdAt: "2026-01-01", updatedAt: "2026-01-01", tags: [] });
    await h.call("mem::crystallize", { actionIds: ["action"], deferred: true });
    const payload = { actionIds: ["action"], batchResponse: JSON.stringify({ narrative: "Done", keyOutcomes: [], filesAffected: [], lessons: ["Save before acknowledgement"] }), batchSourceFingerprint: queued!.metadata!.sourceFingerprint, batchEffectKey: batchEffectKey("crystal") };
    h.crash(scope, true, scope === KV.batchCallbacks ? 3 : 1);
    try { await h.call("mem::crystallize", payload); } catch { /* receiver receipt write can reject */ }
    expect((await h.call("mem::crystallize", payload)).success).toBe(true);
    await h.call("mem::crystallize", payload);
    expect(await h.kv.list(KV.crystals)).toHaveLength(1);
    expect((await h.kv.list<Lesson>(KV.lessons))[0].reinforcements).toBe(0);
    expect((await h.kv.get<{ crystallizedInto: string }>(KV.actions, "action"))?.crystallizedInto).toBeTruthy();
  });

  it.each([KV.procedural, KV.batchCallbacks])("dedupes procedural counters after %s write loss", async (scope) => {
    const h = effectHarness();
    registerConsolidationPipelineFunction(h.sdk as never, h.kv, {} as never);
    const patterns = [{ content: "Validate inputs", frequency: 2 }, { content: "Persist outputs", frequency: 2 }];
    for (const [i, pattern] of patterns.entries()) await h.kv.set(KV.memories, String(i), { id: String(i), content: pattern.content, sessionIds: ["a", "b"], type: "pattern", isLatest: true });
    await h.kv.set(KV.procedural, "proc", { id: "proc", name: "Check", frequency: 3, strength: 0.5 });
    const payload = { tier: "procedural", force: true, batchResponse: '<procedure name="Check" trigger="start"><step>Read</step></procedure>', batchSourceFingerprint: fingerprintId("fwbconproc", JSON.stringify(patterns)), batchEffectKey: batchEffectKey("proc") };
    h.crash(scope, true, scope === KV.batchCallbacks ? 3 : 1);
    await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow();
    await h.call("mem::consolidate-pipeline", payload);
    await h.call("mem::consolidate-pipeline", payload);
    expect(await h.kv.get<ProceduralMemory>(KV.procedural, "proc")).toMatchObject({ frequency: 4, strength: 0.6 });
  });

  it.each([KV.semantic, KV.state])("resumes semantic facts and checkpoint after %s write loss", async (scope) => {
    const h = effectHarness();
    registerConsolidationPipelineFunction(h.sdk as never, h.kv, {} as never);
    const summaries = Array.from({ length: 5 }, (_, i) => ({ sessionId: String(i), title: `Summary ${i}`, narrative: "Done", concepts: [], createdAt: `2026-01-0${i + 1}` }));
    for (const summary of summaries) await h.kv.set(KV.summaries, summary.sessionId, summary);
    await h.kv.set(KV.semantic, "sem", { id: "sem", fact: "Keep effects durable", accessCount: 2, confidence: 0.5 });
    const selected = [...summaries].reverse();
    const payload = { tier: "semantic", force: true, batchResponse: '<fact confidence="0.7">Keep effects durable</fact><fact confidence="0.6">Retain retries</fact>',
      batchSourceFingerprint: fingerprintId("fwbconsem", JSON.stringify(selected.map((s) => [s.sessionId, s.title, s.narrative, s.concepts, s.createdAt]))), batchEffectKey: batchEffectKey("semantic") };
    h.crash(scope, true);
    await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow();
    await h.call("mem::consolidate-pipeline", payload);
    await h.call("mem::consolidate-pipeline", payload);
    expect(await h.kv.list(KV.semantic)).toHaveLength(2);
    expect(await h.kv.get(KV.semantic, "sem")).toMatchObject({ accessCount: 3, confidence: 0.7 });
    expect(await h.kv.get(KV.state, "semantic-consolidation")).toMatchObject({ processedThrough: "2026-01-05" });
  });

  it.each(["all", "decay"])("dedupes %s decay after a committed write and fixes its effective timestamp", async (tier) => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date("2026-04-01T00:00:00Z"));
      const h = effectHarness();
      registerConsolidationPipelineFunction(h.sdk as never, h.kv, {} as never);
      h.sdk.registerFunction("mem::reflect", async () => ({ success: true }));
      const start = "2026-03-01T00:00:00Z";
      await h.kv.set(KV.semantic, "sem", { id: "sem", strength: 1, updatedAt: start, lastAccessedAt: start });
      await h.kv.set(KV.procedural, "proc", { id: "proc", strength: 1, updatedAt: start });
      const payload = { tier, force: true, batchEffectKey: batchEffectKey(`decay-${tier}`) };
      h.crash(KV.semantic, true);
      await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow();
      vi.setSystemTime(new Date("2026-06-01T00:00:00Z"));
      await h.call("mem::consolidate-pipeline", payload); await h.call("mem::consolidate-pipeline", payload);
      expect(await h.kv.get(KV.semantic, "sem")).toMatchObject({ strength: 0.9 });
      expect(await h.kv.get(KV.procedural, "proc")).toMatchObject({ strength: 0.9 });
    } finally { vi.useRealTimers(); }
  });

  it.each(["reflect", "all"].flatMap((tier) => ["false", "throw"].map((failure) => ({ tier, failure }))))("propagates reflect $failure failure for tier=$tier and remains retryable", async ({ tier, failure }) => {
    const h = effectHarness(); registerConsolidationPipelineFunction(h.sdk as never, h.kv, {} as never);
    const reflect = vi.fn(async () => {
      if (failure === "throw") throw new Error("reflect exploded");
      return { success: false };
    });
    h.sdk.registerFunction("mem::reflect", reflect);
    await h.kv.set(KV.semantic, "untouched", { id: "untouched", strength: 1, updatedAt: "2020-01-01" });
    const key = batchEffectKey(`reflect-failure:${tier}:${failure}`);
    const payload = { tier, force: true, batchEffectKey: key };
    await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow(failure === "throw" ? "reflect exploded" : "Reflection tier reported failure");
    expect(await h.kv.get(KV.semantic, "untouched")).toMatchObject({ strength: 1 });
    expect(await h.kv.get(KV.batchCallbacks, `consolidation:${key}`)).toBeNull();
    reflect.mockResolvedValue({ success: true });
    expect(await h.call("mem::consolidate-pipeline", payload)).toMatchObject({ success: true });
    expect(await h.kv.get(KV.batchCallbacks, `consolidation:${key}`)).toMatchObject({ state: "completed" });
  });
});
