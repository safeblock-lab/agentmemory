import { describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../src/state/kv.js";
import { fingerprintId, KV } from "../src/state/schema.js";
import type { MemoryProvider, SessionSummary } from "../src/types.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import { pageKey, semanticProjectCheckpointKey } from "../src/functions/consolidation-manifest.js";
import { CONSOLIDATION_LAST_RUN_KEY } from "../src/functions/consolidation-status.js";
import type { ConsolidationDecaySelector } from "../src/functions/consolidation-decay.js";
import { reflectHarness } from "./helpers/reflect-harness.js";
import { memoryToObservation } from "../src/state/memory-utils.js";
import type { Memory } from "../src/types.js";

type Row = Record<string, unknown>;
type Handler = (input: unknown) => Promise<Row>;
const project = "selected-pipeline-project";
const sessionId = "selected-pipeline-session";
const summary: SessionSummary = {
  sessionId, project, title: "Pinned source", narrative: "An exact selected source for pipeline tiers.",
  concepts: ["selection"], keyDecisions: [], filesModified: [], createdAt: "2026-10-10T00:00:00.000Z", observationCount: 1,
};

function harness() {
  const rows = new Map<string, unknown>();
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const put = (scope: string, id: string, value: unknown) => rows.set(key(scope, id), structuredClone(value));
  const kv = {
    indexedRetrieval: true,
    get: vi.fn(async <T>(scope: string, id: string) => structuredClone(rows.get(key(scope, id)) ?? null) as T | null),
    getVersioned: async <T>(scope: string, id: string) => {
      const value = rows.get(key(scope, id));
      return value === undefined ? { exists: false, version: "1", value: null }
        : { exists: true, version: "1", value: structuredClone(value) as T };
    },
    set: vi.fn(async (scope: string, id: string, value: unknown) => { put(scope, id, value); return value; }),
    delete: async (scope: string, id: string) => { rows.delete(key(scope, id)); },
    list: async <T>(scope: string) => [...rows].filter(([entry]) => entry.startsWith(`${scope}\0`)).map(([, value]) => structuredClone(value) as T),
    pages: async function* <T>(scope: string) { yield { items: await this.list<T>(scope), next_cursor: null }; },
  };
  const handlers = new Map<string, Handler>();
  const sdk = {
    registerFunction: (id: string, handler: Handler) => handlers.set(id, handler),
    registerTrigger: vi.fn(), trigger: vi.fn(),
  };
  const sources = [{ id: "selected-observation", sessionId, rowVersion: "1" }];
  put(KV.summaries, sessionId, summary);
  put(KV.sessions, sessionId, { id: sessionId, project });
  const selector = Object.assign(async () => ({ items: [], nextCursor: null, membershipGeneration: "1", coverageEpoch: "1" }), {
    apply: vi.fn(async () => ({ applied: true, created: false, id: "unused", rowVersion: "1", receipt: {
      operationKind: "decay", effectKey: "a".repeat(64), operationFingerprint: "a".repeat(64), id: "unused", rowVersion: "1", created: false,
    } })),
  }) as unknown as ConsolidationDecaySelector;
  registerConsolidationPipelineFunction(sdk as unknown as IIIClient, kv as unknown as StateKV,
    {} as MemoryProvider, undefined, undefined, undefined, undefined, async () => sources, undefined, selector, undefined);
  const call = (input: unknown) => handlers.get("mem::consolidate-pipeline")!(input);
  return { rows, put, kv, sdk, call, sources, selector };
}

describe("selected consolidation pipeline tiers", () => {
  it("runs all through pinned skips, selected reflect and exact empty decay, then replays terminal state without writes", async () => {
    const h = harness();
    let reflectJobId = "";
    let unitCalls = 0;
    h.sdk.trigger.mockImplementation(async (input: unknown) => {
      const request = input as { function_id: string; payload: Row };
      if (request.function_id === "mem::reflect") {
        expect(request.payload).toMatchObject({ project, sessionId, summaryId: sessionId, factIds: [], lessonIds: [], maxClusters: 1 });
        expect((request.payload.selected as Row).sessionId).toBe(sessionId);
        reflectJobId = "selected-reflect-job";
        return { success: true, queued: true, jobId: reflectJobId } as never;
      }
      if (request.function_id === "mem::reflect-unit") {
        unitCalls++;
        if (unitCalls === 1) {
          h.put(REFLECT_JOBS, reflectJobId, { id: reflectJobId, status: "pending", unitSequence: 0 });
          return { success: true, completed: false } as never;
        }
        h.put(REFLECT_JOBS, reflectJobId, { id: reflectJobId, status: "completed", unitSequence: 1 });
        h.put(REFLECT_CONTROL, `${reflectJobId}:result:0`, { insightIds: ["insight-selected"] });
        return { success: true, completed: true, status: "completed" } as never;
      }
      throw new Error(`unexpected trigger ${request.function_id}`);
    });
    const selected = { project, sessionId };
    let result = await h.call({ tier: "all", force: true, project, selected });
    expect(result).toMatchObject({ completed: false, stage: "procedural" });
    result = await h.call({ ...(result.continuation as Row), selected });
    expect(result).toMatchObject({ completed: false, stage: "reflect" });
    result = await h.call({ ...(result.continuation as Row), selected });
    expect(result).toMatchObject({ completed: false, stage: "reflect" });
    result = await h.call({ ...(result.continuation as Row), selected });
    expect(result).toMatchObject({ success: true, completed: false, stage: "decay" });
    result = await h.call({ ...(result.continuation as Row), selected });
    expect(result).toMatchObject({ completed: false, stage: "decay" });
    result = await h.call({ ...(result.continuation as Row), selected });
    expect(result).toMatchObject({ success: true, completed: true, results: { reflect: { insightIds: ["insight-selected"] } } });
    const run = [...h.rows.entries()].find(([entry]) => entry.startsWith(`${KV.state}\0selected-consolidation-run:`))?.[1] as Row;
    expect(run.results).toMatchObject({ decay: { completed: true, selectionCoverage: "selected_canonical_records" } });
    const decay = await h.kv.get<Row>(KV.state, run.decayManifestId as string);
    expect(decay?.recordIds).toEqual({ semantic: [], procedural: [] });
    expect(h.selector.apply).not.toHaveBeenCalled();
    expect((await h.kv.get(KV.config, "consolidation-project-cursor"))).toBeNull();
    const writes = h.kv.set.mock.calls.length;
    const triggers = h.sdk.trigger.mock.calls.length;
    const initialRequestReplay = await h.call({ tier: "all", force: true, project, selected });
    const continuationReplay = await h.call({ tier: "all", force: true, project, selectedRunId: result.selectedRunId, selected });
    expect(initialRequestReplay).toMatchObject({ completed: true, selectedRunId: result.selectedRunId });
    expect(continuationReplay).toMatchObject({ completed: true, selectedRunId: result.selectedRunId });
    expect(h.kv.set).toHaveBeenCalledTimes(writes);
    expect(h.sdk.trigger).toHaveBeenCalledTimes(triggers);
  });

  it("rejects changed pinned summary content before resuming selected work", async () => {
    const h = harness();
    h.sdk.trigger.mockImplementation(async (input: unknown) => {
      const request = input as { function_id: string };
      if (request.function_id === "mem::reflect") return { success: true, queued: true, jobId: "pinned-reflect-job" } as never;
      h.put(REFLECT_JOBS, "pinned-reflect-job", { id: "pinned-reflect-job", status: "pending", unitSequence: 0 });
      return { success: true, completed: false } as never;
    });
    const selected = { project, sessionId };
    const started = await h.call({ tier: "reflect", force: true, project, selected });
    expect(started).toMatchObject({ completed: false, stage: "reflect" });
    const continuation = started.continuation as Row;
    const wrongSession = await h.call({ ...continuation, selected: { ...selected, sessionId: "different-session" } });
    expect(wrongSession).toMatchObject({ success: false, error: "selected_consolidation_session_mismatch" });
    h.put(KV.summaries, sessionId, { ...summary, narrative: "Changed source" });
    const calls = h.sdk.trigger.mock.calls.length;
    const resumed = await h.call({ ...continuation, selected });
    expect(resumed).toMatchObject({ success: false, completed: false, error: "selected_consolidation_summary_source_changed" });
    expect(h.sdk.trigger).toHaveBeenCalledTimes(calls);
  });

  it("keeps explicit semantic source IDs in its own durable continuation", async () => {
    const h = harness();
    const selected = { project, sessionId };
    const started = await h.call({ tier: "semantic", force: true, project, batchSourceIds: [sessionId], selected });
    expect(started).toMatchObject({ completed: false, queued: true });
    const continuation = started.continuation as Row;
    expect(continuation.batchSourceIds).toEqual([sessionId]);
    const resumed = await h.call({ ...continuation, selected });
    expect(resumed).toMatchObject({ completed: false, queued: true, selectedRunId: started.selectedRunId });
    expect(resumed.error).toBeUndefined();
    expect(resumed.continuation).toMatchObject({ batchSourceIds: [sessionId], selectedRunId: started.selectedRunId });
  });

  it("runs standalone selected reflect through its exact completed result checkpoint", async () => {
    const h = harness();
    h.sdk.trigger.mockImplementation(async (input: unknown) => {
      const request = input as { function_id: string };
      if (request.function_id === "mem::reflect") return { success: true, queued: true, jobId: "direct-reflect-job" } as never;
      h.put(REFLECT_JOBS, "direct-reflect-job", { id: "direct-reflect-job", status: "completed", unitSequence: 1 });
      h.put(REFLECT_CONTROL, "direct-reflect-job:result:0", { insightIds: ["direct-insight"] });
      return { success: true, completed: true, status: "completed" } as never;
    });
    const result = await h.call({ tier: "reflect", force: true, project, selected: { project, sessionId } });
    expect(result).toMatchObject({ success: true, completed: true, results: { reflect: { insightIds: ["direct-insight"] } } });
    expect(h.sdk.trigger).toHaveBeenCalledTimes(2);
  });

  it("persists exact insight IDs in the real reflect result checkpoint and keeps completed replay idempotent", async () => {
    const h = reflectHarness();
    const jobId = "selected-real-reflect-job";
    const content = "Validate input at boundaries.";
    const effectKey = batchEffectKey(`${jobId}:0:selected-manifest`);
    const original = await h.kv.get<Memory>(KV.memories, "one");
    const observation = memoryToObservation(original!);
    h.provider.summarize.mockImplementation(async (system: string, prompt: string) => {
      expect(system).toMatch(/^You evaluate publication evidence\./);
      const evidence = JSON.parse(prompt);
      expect(evidence.sources).toMatchObject([{ key: "one", content: JSON.stringify(observation) }]);
      expect(JSON.parse(evidence.candidate).generatedResponse).toContain(content);
      return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "The original supports boundary validation." });
    });
    const cluster = { concepts: ["boundaries"], facts: [], lessons: [], crystalNarratives: [], factIds: [], lessonIds: [], crystalIds: [] };
    const job = { id: jobId, request: { project: "/app", sessionId: "session", query: "boundaries", force: true, maxClusters: 1 },
      project: "/app", seeds: ["boundaries"], status: "pending", ordinal: 0, reservedInsights: 5, appliedInsights: 0,
      processedClusters: 0, attempts: 0, refreshes: 0, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      nextAttemptAt: 0, dispatchPending: false, deferredFamilies: [], unitSequence: 0,
      executionOrigin: "selected", selectedSessionId: "session",
      manifest: { id: "selected-manifest", cluster, sources: [{ id: "one", scope: KV.memories, kind: "memory",
        version: "1", fingerprint: JSON.stringify(original), observation, order: 0, score: 1, bm25Score: 1, vectorScore: 1 }],
        indexIdentity: { index_id: h.index.index_id, model: h.index.model, dimensions: h.index.dimensions, generation: h.index.generation },
        graphGeneration: JSON.stringify(h.graph), deferredFamilies: [], prompt: "selected reflection" },
      captured: { response: `<insights><insight confidence="0.8" title="Boundaries">${content}</insight></insights>`, effectKey, count: 1 } };
    await h.kv.set(KV.sessions, "session", { id: "session", project: "/app" });
    await h.kv.set(REFLECT_JOBS, jobId, job);
    const selected = { sessionId: "session" };
    const completed = await h.call("mem::reflect-unit", { jobId, selected });
    expect(completed).toMatchObject({ success: true, status: "completed" });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    const insightId = fingerprintId("ins", `/app:${content.toLowerCase()}`);
    expect(await h.kv.get(REFLECT_CONTROL, `${jobId}:result:0`)).toMatchObject({ insightIds: [insightId] });
    const beforeReplay = await h.kv.list(KV.insights);
    expect(await h.call("mem::reflect-unit", { jobId, selected })).toMatchObject({ success: true, skipped: true });
    expect(await h.kv.list(KV.insights)).toEqual(beforeReplay);
    expect(h.published).toHaveLength(0);
  });

  it("reuses the exact selected semantic root across tiers without touching global checkpoints", async () => {
    const h = harness();
    const ordinaryCheckpointKey = semanticProjectCheckpointKey(project);
    h.put(KV.state, ordinaryCheckpointKey, { processedThrough: "2026-10-09T00:00:00.000Z", processedSessionIdsAtThrough: ["ordinary"] });
    h.put(KV.config, "consolidation-project-cursor", { project: "ordinary-project" });
    h.put(KV.config, CONSOLIDATION_LAST_RUN_KEY, { status: "ordinary" });
    const selected = { project, sessionId };
    const semanticRequest = { tier: "semantic", force: true, project, batchSourceIds: [sessionId], selected };
    const started = await h.call(semanticRequest);
    const rootId = (started.results as Row).semantic && ((started.results as Row).semantic as Row).manifestId as string;
    expect(rootId).toMatch(/^conmanifest_/);
    const rootKey = pageKey(rootId, "root", 0);
    const root = await h.kv.get<Row>(KV.state, rootKey);
    expect(root).not.toBeNull();
    h.put(KV.state, rootKey, { ...root, phase: "completed", result: { completed: true, sourceCount: 1, appliedEffects: 0, newFacts: 0, newProcedures: 0 } });
    const standalone = await h.call(semanticRequest);
    expect(standalone).toMatchObject({ success: true, completed: true, results: { semantic: { manifestId: rootId, replayed: true } } });
    const all = await h.call({ tier: "all", force: true, project, batchSourceIds: [sessionId], selected });
    expect(all).toMatchObject({ success: true, completed: false, stage: "procedural", results: { semantic: { manifestId: rootId, replayed: true } } });
    expect(h.sdk.trigger).not.toHaveBeenCalled();
    expect(h.kv.get.mock.calls.some(([scope, id]) => scope === KV.state && id === ordinaryCheckpointKey)).toBe(false);
    expect(h.kv.set.mock.calls.some(([scope, id]) => scope === KV.config && ["consolidation-project-cursor", CONSOLIDATION_LAST_RUN_KEY].includes(id))).toBe(false);
    expect(await h.kv.get(KV.state, ordinaryCheckpointKey)).toEqual({ processedThrough: "2026-10-09T00:00:00.000Z", processedSessionIdsAtThrough: ["ordinary"] });
  });
  it("runs standalone selected decay using its pinned source-session scope", async () => {
    const h = harness();
    const selected = { project, sessionId };
    let result = await h.call({ tier: "decay", force: true, project, selected });
    expect(result.success, JSON.stringify(result)).toBe(true);
    expect(result).toMatchObject({ success: true, completed: false, stage: "decay" });
    result = await h.call({ ...(result.continuation as Row), selected });
    expect(result).toMatchObject({ success: true, completed: true,
      results: { decay: { completed: true, selectionCoverage: "selected_source_sessions" } } });
    expect(h.selector.apply).not.toHaveBeenCalled();
    expect(h.sdk.trigger).not.toHaveBeenCalled();
    expect((await h.kv.get(KV.config, "consolidation-project-cursor"))).toBeNull();
  });
});
