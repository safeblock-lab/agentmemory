import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { registerCrystallizeFunction } from "../src/functions/crystallize.js";
import { registerConsolidationPipelineFunction as registerPipeline } from "../src/functions/consolidation-pipeline.js";
import { legacyReflectSourceFingerprint } from "../src/functions/reflect-retrieval.js";
import type { ConsolidationCanonicalLookup, ConsolidationCanonicalWriter } from "../src/functions/consolidation-selection.js";
import type { ConsolidationDecaySelector } from "../src/functions/consolidation-decay.js";
import { registerLessonsFunctions } from "../src/functions/lessons.js";
import type { GraphSnapshot, Lesson, Insight, ProceduralMemory, FireworksBatchRequest } from "../src/types.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/config.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/config.js")>()),
  getGraphExtractionInputTargetChars: () => 10000,
  isConsolidationEnabled: () => true,
  getConsolidationDecayDays: () => 30,
  getConsolidationMinNewSummaries: () => 1,
}));


function registerConsolidationPipelineFunction(sdk: Parameters<typeof registerPipeline>[0], kv: Parameters<typeof registerPipeline>[1], provider: Parameters<typeof registerPipeline>[2]) {
  const judgedProvider = { ...provider, summarize: vi.fn(async (system: string, prompt: string) => {
    expect(system).toMatch(/^You evaluate publication evidence\./);
    const evidence = JSON.parse(prompt);
    expect(evidence.workflow).toBe("consolidation");
    for (const source of evidence.sources) {
      expect(source.project).toBe("test-project");
      expect(source.content).toBe(JSON.stringify(await kv.get(source.scope, source.key)));
    }
    return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "The callback fixture is supported by its originals." });
  }) };
  const lookup: ConsolidationCanonicalLookup = async (scope, key) => {
    const records = await kv.list<Record<string, unknown>>(KV[scope]);
    const found = records.find(row => String(scope === "semantic" ? row.fact : row.name).toLowerCase() === key);
    return { status: "ready", canonicalId: found ? String(found.id) : null, version: batchEffectKey(JSON.stringify(found ?? null)) };
  };
  const write: ConsolidationCanonicalWriter = async (scope, key, match, proposedId, effect, update) => {
    const id = match.canonicalId ?? proposedId;
    const old = await kv.get<Record<string, unknown>>(KV[scope], id);
    if (!old?.appliedBatchEffects || !(old.appliedBatchEffects as string[]).includes(effect)) {
      const current = await lookup(scope, key);
      if (current.status !== "ready" || current.version !== match.version) throw new Error("canonical fixture source changed");
    }
    const applied = await applyBatchEffect(kv, KV[scope], id, effect, update as never);
    return { applied: !(old?.appliedBatchEffects as string[] | undefined)?.includes(effect), created: !old, id, rowVersion: "1", receipt: { operationKind: "upsert", effectKey: effect,
      operationFingerprint: batchEffectKey(key), id, rowVersion: "1", created: !old, resultFingerprint: batchEffectKey(JSON.stringify(applied)) } };
  };
  const decay: ConsolidationDecaySelector = Object.assign(async (scope: "semantic" | "procedural", sessions: string[], _cursor: string | null, project: string) => ({
    items: (await kv.list<Record<string, unknown>>(KV[scope])).filter(row => row.project === project && (row.sourceSessionIds as string[]).some(id => sessions.includes(id))).map(row => ({ id: String(row.id), rowVersion: "1" })),
    nextCursor: null, membershipGeneration: "1", coverageEpoch: "1",
  }), { apply: async (scope, ref, sessions, project, effect, _operation, update) => {
    const old = await kv.get<Record<string, unknown>>(KV[scope], ref.id);
    if (!old || old.project !== project || !(old.sourceSessionIds as string[]).some(id => sessions.includes(id))) throw new Error("decay fixture scope changed");
    const applied = await applyBatchEffect(kv, KV[scope], ref.id, effect, update as never);
    return { applied: !(old?.appliedBatchEffects as string[] | undefined)?.includes(effect), created: false, id: ref.id, rowVersion: "1", receipt: { operationKind: "decay", effectKey: effect, operationFingerprint: effect, id: ref.id, rowVersion: "1", created: false, resultFingerprint: effect } };
  } } as Pick<ConsolidationDecaySelector, "apply">);
  registerPipeline(sdk, kv, judgedProvider as never, undefined, undefined, undefined, undefined, undefined, lookup, decay, write);
}

const graphXml = '<entity type="concept" name="A"/><entity type="concept" name="B"/><relationship type="related_to" source="A" target="B" weight="1"/>';
const observation = { id: "obs", title: "title", narrative: "body", facts: [], concepts: [], files: [], type: "discovery" };

describe("batch destination crash recovery", () => {
  it.each([KV.graphNodes, KV.graphNameIndex, KV.graphEdges, KV.graphEdgeKey, KV.graphNodeDegree, KV.graphSnapshot, KV.batchCallbacks])("replays a lost %s commit acknowledgement without duplicating graph effects", async (scope) => {
    const h = effectHarness();
    registerGraphFunction(h.sdk as never, h.kv, {} as never);
    const payload = { observations: [observation], batchResponse: graphXml, batchEffectKey: batchEffectKey("graph") };
    h.crashGraphCommit(scope, true);
    const firstAttempt = await h.call("mem::graph-extract", payload);
    expect(firstAttempt).toMatchObject({ success: true });
    expect(h.scopedCommitFailures).toBe(1);
    await h.call("mem::graph-extract", payload);
    await h.call("mem::graph-extract", payload);
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(1);
    const snap = await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
    expect(snap?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
    expect(Object.values(snap!.topDegrees)).toEqual([1, 1]);
    expect(await h.kv.list(KV.audit)).toHaveLength(0);
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
    "pins empty graph callback %j input and requires a new job key for changed output",
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
    await expect(h.call("mem::graph-extract", validPayload)).rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(0);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(0);

    const replacementPayload = { ...validPayload, batchEffectKey: batchEffectKey("empty-then-valid-graph-replacement") };
    expect(await h.call("mem::graph-extract", replacementPayload)).toMatchObject({ success: true });
    const firstSnapshot = await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
    expect(firstSnapshot?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });

    expect(await h.call("mem::graph-extract", replacementPayload)).toMatchObject({ success: true });
    const secondSnapshot = await h.kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
    expect(secondSnapshot?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(1);
    expect(await h.kv.list(KV.audit)).toHaveLength(0);
    },
  );

  it.each([KV.insights, KV.batchCallbacks])("dedupes reflection after %s write loss", async (scope) => {
    const h = effectHarness();
    const judge = vi.fn(async (system: string, prompt: string) => {
      expect(system).toMatch(/^You evaluate publication evidence\./);
      const evidence = JSON.parse(prompt);
      expect(evidence.workflow).toBe("reflection");
      expect(evidence.sources).toHaveLength(2);
      for (const source of evidence.sources) {
        expect(source.project).toBe("test-project");
        expect(source.content).toBe(JSON.stringify(await h.kv.get(source.scope, source.key)));
      }
      return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "The complete originals support bounded retries." });
    });
    registerReflectFunctions(h.sdk as never, h.kv, { summarize: judge } as never);
    await h.kv.set(KV.semantic, "fact-a", { id: "fact-a", project: "test-project", fact: "bounded", confidence: 0.7, updatedAt: "2026-01-01" });
    await h.kv.set(KV.semantic, "fact-b", { id: "fact-b", project: "test-project", fact: "retries", confidence: 0.7, updatedAt: "2026-01-01" });
    const cluster = { concepts: [], facts: [], lessons: [], crystalNarratives: [], factIds: ["fact-a", "fact-b"], lessonIds: [], crystalIds: [] };
    const payload = {
      batchResponse: '<insight confidence="0.7" title="Useful">Use bounded retries</insight>',
      batchCluster: JSON.stringify(cluster),
      project: "test-project", batchSourceFingerprint: await legacyReflectSourceFingerprint(h.kv, cluster, "test-project"),
      batchEffectKey: batchEffectKey("reflect"),
    };
    h.crash(scope, true, scope === KV.batchCallbacks ? 3 : 1);
    await expect(h.call("mem::reflect", payload)).rejects.toThrow();
    await h.call("mem::reflect", payload);
    await h.call("mem::reflect", payload);
    expect(judge).toHaveBeenCalledTimes(1);
    expect(await h.kv.list(KV.insights)).toHaveLength(1);
    expect(await h.kv.get(KV.batchCallbacks, `reflect:${payload.batchEffectKey}`)).toMatchObject({ state: "completed" });
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
    const patterns = [{ id: "0", content: "Validate inputs", frequency: 2 }, { id: "1", content: "Persist outputs", frequency: 2 }];
    for (const id of ["a", "b"]) await h.kv.set(KV.sessions, id, { id, project: "test-project" });
    for (const [i, pattern] of patterns.entries()) await h.kv.set(KV.memories, String(i), { id: String(i), project: "test-project", content: pattern.content, sessionIds: ["a", "b"], type: "pattern", isLatest: true });
    await h.kv.set(KV.procedural, "proc", { id: "proc", name: "Check", frequency: 3, strength: 0.5 });
    const payload = { tier: "procedural", project: "test-project", batchSourceIds: ["0", "1"], force: true, batchResponse: '<procedures><procedure name="Check" trigger="start"><step>Read</step><step>Save</step></procedure></procedures>', batchSourceFingerprint: fingerprintId("fwbconproc", JSON.stringify(patterns.map(({content, frequency}) => ({content, frequency})))), batchEffectKey: batchEffectKey("proc") };
    h.crash(scope, true, scope === KV.batchCallbacks ? 3 : 1);
    await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow();
    await h.call("mem::consolidate-pipeline", payload);
    await h.call("mem::consolidate-pipeline", payload);
    expect(await h.kv.get<ProceduralMemory>(KV.procedural, "proc")).toMatchObject({ frequency: 4, strength: 0.6 });
  });

  it.each([KV.semantic, KV.state])("resumes semantic facts and checkpoint after %s write loss", async (scope) => {
    const h = effectHarness();
    registerConsolidationPipelineFunction(h.sdk as never, h.kv, {} as never);
    const summaries = Array.from({ length: 5 }, (_, i) => ({ sessionId: String(i), project: "test-project", title: `Summary ${i}`, narrative: "Done", concepts: [], createdAt: `2026-01-0${i + 1}` }));
    for (const summary of summaries) {
      await h.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: summary.project });
      await h.kv.set(KV.summaries, summary.sessionId, summary);
    }
    await h.kv.set(KV.semantic, "sem", { id: "sem", fact: "Keep effects durable", accessCount: 2, confidence: 0.5 });
    const selected = [...summaries];
    const payload = { tier: "semantic", project: "test-project", batchSourceIds: summaries.map(s => s.sessionId), force: true, batchResponse: '<fact confidence="0.7">Keep effects durable</fact><fact confidence="0.6">Retain retries</fact>',
      batchSourceFingerprint: fingerprintId("fwbconsem", JSON.stringify(selected.map((s) => [s.sessionId, s.title, s.narrative, s.concepts, s.createdAt]))), batchEffectKey: batchEffectKey("semantic") };
    if (scope === KV.state) {
      const originalSet = h.kv.set.bind(h.kv); let interrupted = false;
      h.kv.set = async (target, key, value) => {
        if (!interrupted && target === KV.state && key === "semantic-consolidation") {
          interrupted = true; h.crash(KV.state, true);
        }
        return originalSet(target, key, value);
      };
    } else h.crash(scope, true);
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
      h.sdk.registerFunction("mem::reflect", async () => ({ success: true, completed: true }));
      const start = "2026-03-01T00:00:00Z";
      await h.kv.set(KV.semantic, "sem", { id: "sem", project: "test-project", sourceSessionIds: ["0"], strength: 1, updatedAt: start, lastAccessedAt: start });
      await h.kv.set(KV.procedural, "proc", { id: "proc", project: "test-project", sourceSessionIds: ["1"], strength: 1, updatedAt: start });
      for (const id of ["0", "1"]) {
        await h.kv.set(KV.sessions, id, { id, project: "test-project" });
        await h.kv.set(KV.summaries, id, { sessionId: id, project: "test-project", title: "summary", narrative: "done", concepts: [], createdAt: start });
        await h.kv.set(KV.memories, id, { id, project: "test-project", content: "pattern", type: "pattern", isLatest: false, sessionIds: [id] });
      }
      const payload = { tier, project: "test-project", batchSourceIds: ["0", "1"], force: true, batchResponse: "<facts/>", batchEffectKey: batchEffectKey(`decay-${tier}`) };
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
    await h.kv.set(KV.sessions, "source", { id: "source", project: "test-project" });
    await h.kv.set(KV.summaries, "source", { sessionId: "source", project: "test-project", title: "summary", narrative: "done", concepts: [], createdAt: "2026-01-01" });
    const payload = { tier, project: "test-project", batchSourceIds: ["source"], force: true, batchResponse: "<facts/>", batchEffectKey: key };
    expect(await h.call("mem::consolidate-pipeline", payload)).toMatchObject({ success: false, partial: true, completed: false,
      results: { reflect: { error: failure === "throw" ? "reflect exploded" : "Reflection tier reported failure" } } });
    expect(await h.kv.get(KV.semantic, "untouched")).toMatchObject({ strength: 1 });
    expect(await h.kv.get(KV.batchCallbacks, `consolidation:${key}`)).toBeNull();
    reflect.mockResolvedValue({ success: true, completed: true } as never);
    expect(await h.call("mem::consolidate-pipeline", payload)).toMatchObject({ success: true });
    expect(await h.kv.get(KV.batchCallbacks, `consolidation:${key}`)).toMatchObject({ state: "completed" });
  });
});
