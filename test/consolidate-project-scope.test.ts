import { describe, it, expect, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

vi.mock("../src/functions/audit.js", () => ({
  recordAudit: vi.fn(),
}));
vi.mock("../src/providers/_llm-logging.js", () => ({ startLlmCallTelemetry: () => ({ failure: vi.fn(), success: vi.fn() }), extractLlmTokenUsage: () => ({}) }));

import { registerConsolidateFunction } from "../src/functions/consolidate.js";
import { WorkflowAttemptAccounting } from "../src/functions/workflow-attempt-accounting.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, Memory, MemoryProvider, Session } from "../src/types.js";
import { ScopedObservationBudgetError } from "../src/functions/consolidation-selection.js";
import { loadRoot } from "../src/functions/consolidation-manifest.js";

function makeMockKV() {
  const store = new Map<string, Map<string, unknown>>();
  const versions = new Map<string, number>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> =>
      structuredClone((store.get(scope)?.get(key) as T) ?? null),
    getVersioned: async <T>(scope: string, key: string) => ({ exists: store.get(scope)?.has(key) ?? false,
      value: structuredClone((store.get(scope)?.get(key) as T) ?? null), version: String(versions.get(`${scope}:${key}`) ?? 0) }),
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, structuredClone(data));
      versions.set(`${scope}:${key}`, (versions.get(`${scope}:${key}`) ?? 0) + 1);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
  };
}

describe("bounded consolidate capture", () => {
  it("keeps source-index readiness queued before capture without consuming provider calls", async () => {
    const kv = makeMockKV(); const sdk = makeMockSdk(); const provider = makeProvider();
    const select = vi.fn().mockRejectedValue(Object.assign(new Error("projection incomplete"), { code: "STATE_INDEX_NOT_READY" }));
    registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup: vi.fn(), commit: vi.fn() });
    const result = await sdk.trigger("mem::consolidate", { project: "p", minObservations: 3 });
    expect(result).toEqual({ completed: false, queued: true, reason: "source_index_pending", continuation: { project: "p", minObservations: 3 } });
    expect(provider.compress).not.toHaveBeenCalled();
  });
  it("keeps an empty query budget as a recoverable failure and exposes bounded retrieval coverage after retry", async () => {
    const kv = makeMockKV(); const sdk = makeMockSdk(); const provider = makeProvider(); const session = makeSession("budget-session", "p");
    await kv.set(KV.sessions, session.id, session);
    const observations = [0, 1, 2].map(index => makeObs(`budget-${index}`, session.id, "auth"));
    for (const observation of observations) await kv.set(KV.observations(session.id), observation.id, observation);
    const coverage = ["bounded_candidate_budget", "bounded_query_budget"] as const;
    const select = vi.fn(async () => observations.map(observation => ({ id: observation.id, sessionId: session.id, rowVersion: "1", retrievalCoverage: [...coverage] })));
    select.mockRejectedValueOnce(new ScopedObservationBudgetError("query_work_budget", "p", session.id));
    const lookup = vi.fn(async () => ({ current: null, token: {} as never }));
    const commit = vi.fn(async (_project, _title, _match, next, effect) => ({ applied: true, created: true, id: next.id, rowVersion: "2", receipt: {
      operationKind: "evolve" as const, effectKey: effect, operationFingerprint: effect, id: next.id, rowVersion: "2", created: true, resultFingerprint: "fixture" } }));
    const accounting = new WorkflowAttemptAccounting([], []); const attempts = vi.spyOn(accounting, "run");
    registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup, commit }, accounting);
    let result = await sdk.trigger("mem::consolidate", { project: "p", minObservations: 3 });
    expect(result).toMatchObject({ success: false, completed: false, partial: true, queued: false, retryable: true, errorCode: "STATE_RETRIEVAL_RESOURCE_LIMIT",
      diagnostics: { project: "p", sessionId: session.id, minObservations: 3, reason: "query_work_budget", limit: 12 } });
    expect(provider.compress).not.toHaveBeenCalled();
    for (let cut = 0; !result.completed && cut < 100; cut++) result = await sdk.trigger("mem::consolidate", result.continuation);
    expect(result.completed).toBe(true); expect(result.retrievalCoverage).toEqual(coverage);
    const root = (await loadRoot(kv as never, result.captureId))!;
    expect(root.refs.every(ref => JSON.stringify(ref.retrievalCoverage) === JSON.stringify(coverage))).toBe(true);
    expect(attempts).toHaveBeenCalled();
    expect(attempts.mock.calls[0][1]).toMatchObject({ workflow: "consolidation", functionId: "mem::consolidate", phase: "generation" });
    expect(attempts.mock.calls[0][1].unitId).toMatch(/^conrequest_/);
  });
  it("preserves distinct same-title facts, exact duplicates, stable ordering and interrupted canonical replay", async () => {
    async function run(reverse: boolean) {
      const kv = makeMockKV(); const sdk = makeMockSdk(); const session = makeSession("same-title", "p");
      await kv.set(KV.sessions, session.id, session);
      const observations = [0, 1, 2].map(i => makeObs(`obs-${i}`, session.id, "auth"));
      for (const observation of observations) await kv.set(KV.observations(session.id), observation.id, observation);
      const select = vi.fn(async () => (reverse ? [...observations].reverse() : observations).map(observation => ({ id: observation.id, sessionId: session.id, rowVersion: "1" })));
      const provider = makeProvider();
      vi.mocked(provider.compress).mockImplementation(async (_system, prompt) => {
        const fragments = JSON.parse(prompt.slice(prompt.indexOf("[")));
        const fact = fragments[0].sourceId === "obs-1" ? "Rotate signing keys after deployment." : "Validate access tokens before reading private records.";
        return `<memory><type>fact</type><title>Authentication</title><content>${fact}</content><concepts><concept>auth</concept></concepts><files></files><strength>8</strength></memory>`;
      });
      const effects = new Map<string, any>(); const memories = new Map<string, Memory>(); let interrupted = false;
      const lookup = vi.fn(async (_project, title) => ({ current: memories.get(title) ?? null, token: {} as never }));
      const commit = vi.fn(async (_project, title, _match, next, effect) => {
        if (effects.has(effect)) return { ...effects.get(effect), applied: false };
        const result = { applied: true, created: true, id: next.id, rowVersion: "2", receipt: { operationKind: "evolve" as const,
          effectKey: effect, operationFingerprint: effect, id: next.id, rowVersion: "2", created: true, resultFingerprint: "fixture" } };
        memories.set(title, next); effects.set(effect, result);
        if (!interrupted) { interrupted = true; throw new Error("cut after native receipt before page receipt"); }
        return result;
      });
      registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup, commit });
      let result = await sdk.trigger("mem::consolidate", { project: "p", minObservations: 3 });
      for (let cut = 0; !result.completed && cut < 200; cut++) {
        const before = vi.mocked(provider.compress).mock.calls.length;
        result = await sdk.trigger("mem::consolidate", result.continuation);
        expect(vi.mocked(provider.compress).mock.calls.length - before).toBeLessThanOrEqual(2);
      }
      expect(result).toMatchObject({ completed: true, consolidated: 2, candidateCount: 3, examined: 2, appliedEffects: 1, replayedEffects: 1 });
      expect(effects.size).toBe(2); expect(commit).toHaveBeenCalledTimes(3); expect(provider.compress).toHaveBeenCalledTimes(3);
      expect([...memories.values()].map(memory => memory.content).sort()).toEqual(["Rotate signing keys after deployment.", "Validate access tokens before reading private records."]);
      for (const [title, memory] of memories) { expect(title).toBe(memory.title.toLowerCase()); expect(memory.title.length).toBeLessThanOrEqual(80);
        expect(memory.sourceObservationIds).toEqual(memory.content.startsWith("Rotate") ? ["obs-1"] : ["obs-0", "obs-2"]);
        expect(memory.sessionIds).toEqual([session.id]); }
      const replay = await sdk.trigger("mem::consolidate", { consolidationCaptureId: result.captureId });
      expect(replay.completed).toBe(true); expect(provider.compress).toHaveBeenCalledTimes(3); expect(effects.size).toBe(2); expect(commit).toHaveBeenCalledTimes(3);
      return [...memories.keys()].sort();
    }
    expect(await run(false)).toEqual(await run(true));
  });
  it("retains conflicting captured legacy title coverage without retitling an already completed concept", async () => {
    const kv = makeMockKV(); const sdk = makeMockSdk(); const session = makeSession("legacy-session", "p");
    await kv.set(KV.sessions, session.id, session);
    const observations = [0, 1, 2].map(i => ({ ...makeObs(`legacy-${i}`, session.id, "auth"), narrative: `Original evidence ${i}. `.repeat(500) }));
    for (const observation of observations) await kv.set(KV.observations(session.id), observation.id, observation);
    const capture = { id: "old-consolidate-capture", project: "p", observations: observations.map(observation => ({ ...observation, sid: session.id })),
      completedConcepts: [] as string[], minObservations: 3, createdAt: "2026-10-01T00:00:00Z" };
    await kv.set(KV.state, capture.id, capture);
    const provider = makeProvider("Generic setup"); let count = 0;
    vi.mocked(provider.compress).mockImplementation(async () => `<memory><type>fact</type><title>Generic setup</title><content>Independent original fact ${count++}.</content><concepts></concepts><files></files><strength>8</strength></memory>`);
    const commit = vi.fn(); const select = vi.fn(); const lookup = vi.fn();
    registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup, commit });
    let result = await sdk.trigger("mem::consolidate", { consolidationCaptureId: capture.id });
    for (let cut = 0; result.queued && cut < 100; cut++) result = await sdk.trigger("mem::consolidate", result.continuation);
    expect(result).toMatchObject({ completed: false, retained: true, reason: "legacy_distinct_same_title_memories_require_recovery", captureId: capture.id });
    expect(commit).not.toHaveBeenCalled(); expect(select).not.toHaveBeenCalled();
    const requests = (await kv.list<any>(KV.state)).filter(value => value.id?.startsWith("conrequest_"));
    expect(requests.length).toBeGreaterThan(1); expect(requests.every(request => request.status === "completed")).toBe(true);
    await kv.set(KV.state, capture.id, { ...capture, completedConcepts: ["auth"] });
    const before = vi.mocked(provider.compress).mock.calls.length;
    result = await sdk.trigger("mem::consolidate", { consolidationCaptureId: capture.id });
    expect(result).toMatchObject({ completed: false, retained: true }); expect(provider.compress).toHaveBeenCalledTimes(before);
    expect((await kv.get<any>(KV.state, capture.id)).completedConcepts).toEqual(["auth"]); expect(commit).not.toHaveBeenCalled();
    await kv.delete(KV.state, requests[0].id);
    result = await sdk.trigger("mem::consolidate", { consolidationCaptureId: capture.id });
    expect(result).toMatchObject({ completed: false, retained: true, reason: "legacy_completed_memory_coverage_unknown" });
    expect(provider.compress).toHaveBeenCalledTimes(before);
  });
  it("rejects a missing project before native source selection or provider generation", async () => {
    const kv = Object.assign(makeMockKV(), { indexedRetrieval: true });
    const sdk = makeMockSdk();
    const provider = makeProvider();
    const compress = vi.spyOn(provider, "compress");
    const select = vi.fn();
    registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup: vi.fn(), commit: vi.fn() });
    await expect(sdk.trigger("mem::consolidate", {})).rejects.toThrow("consolidate_project_required");
    expect(select).not.toHaveBeenCalled();
    expect(compress).not.toHaveBeenCalled();
  });
  it("uses exact project sources and resumes long fragments without repeating captured calls", async () => {
    const kv = Object.assign(makeMockKV(), { indexedRetrieval: true });
    const sdk = makeMockSdk();
    const session = makeSession("bounded-session", "bounded-project");
    await kv.set(KV.sessions, session.id, session);
    const observations = [0, 1, 2].map(i => ({ ...makeObs(`bounded-${i}`, session.id, "durable-concept"),
      narrative: `Grounded repeatable observation ${i}. `.repeat(220) }));
    for (const observation of observations) await kv.set(KV.observations(session.id), observation.id, observation);
    const select = vi.fn(async () => observations.map(observation => ({ id: observation.id, sessionId: session.id, rowVersion: "1" })));
    const provider = makeProvider("Reusable setup");
    const compress = vi.spyOn(provider, "compress");
    const lookup = vi.fn(async () => ({ current: null, token: { status: "ready" as const, scope: "memories" as const,
      partition: session.project, normalizedKey: "reusable setup", canonicalId: null, rowVersion: null, scopeRevision: "0",
      coverageEpoch: "0", normalizationVersion: "js-lower-v1" as const } }));
    const commit = vi.fn(async (_project, _title, _match, next, effect) => {
      await kv.set(KV.memories, next.id, next);
      return { applied: true, created: true, id: next.id, rowVersion: "2", receipt: { operationKind: "evolve" as const,
        effectKey: effect, operationFingerprint: "capture", id: next.id, rowVersion: "2", created: true, resultFingerprint: "result" } };
    });
    const forbiddenList = vi.spyOn(kv, "list").mockImplementation(() => { throw new Error("global list forbidden"); });
    registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup, commit });
    let result = await sdk.trigger("mem::consolidate", { project: session.project, minObservations: 3 });
    expect(result.completed).toBe(false);
    expect(result.queued).toBe(true);
    expect(compress).toHaveBeenCalledTimes(0);
    for (let i = 0; !result.completed && i < 300; i++) {
      const calls = compress.mock.calls.length;
      result = await sdk.trigger("mem::consolidate", result.continuation);
      expect(compress.mock.calls.length - calls).toBeLessThanOrEqual(2);
    }
    expect(result.completed).toBe(true);
    expect(result.consolidated).toBe(1);
    expect(select).toHaveBeenCalledOnce();
    expect(select).toHaveBeenCalledWith(session.project);
    expect(forbiddenList).not.toHaveBeenCalled();
    expect(commit).toHaveBeenCalledOnce();
    const next = commit.mock.calls[0][3];
    expect(next.project).toBe(session.project);
    expect(next.sourceObservationIds).toEqual(observations.map(observation => observation.id));
    expect(next.sessionIds).toEqual([session.id]);
    const count = compress.mock.calls.length;
    await sdk.trigger("mem::consolidate", { consolidationCaptureId: result.captureId });
    expect(compress).toHaveBeenCalledTimes(count);
    expect(commit).toHaveBeenCalledOnce();
  });
});

function makeMockSdk() {
  const functions = new Map<string, Function>();
  return {
    registerFunction: (id: string, handler: Function) => {
      functions.set(id, handler);
    },
    registerTrigger: () => {},
    trigger: async (
      idOrInput: string | { function_id: string; payload: unknown },
      data?: unknown,
    ) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : (idOrInput as { payload: unknown }).payload;
      const fn = functions.get(id);
      if (!fn) throw new Error(`No function registered: ${id}`);
      return fn(payload);
    },
  };
}

function makeProvider(title = "synthesized memory title"): MemoryProvider {
  return {
    name: "mock",
    compress: vi.fn().mockResolvedValue(
      `<memory>
        <type>pattern</type>
        <title>${title}</title>
        <content>synthesized content about the concept</content>
        <concepts><concept>auth</concept></concepts>
        <files><file>src/auth.ts</file></files>
        <strength>7</strength>
      </memory>`,
    ),
    embed: vi.fn().mockResolvedValue(new Float32Array(384)),
    embedBatch: vi.fn().mockResolvedValue([]),
    dimensions: 384,
    compressionModel: "mock-model",
  };
}

function makeSession(id: string, project: string): Session {
  return {
    id,
    project,
    cwd: `/srv/${project}`,
    startedAt: new Date().toISOString(),
    status: "completed",
    observationCount: 5,
  };
}

function makeObs(id: string, sessionId: string, concept: string): CompressedObservation {
  return {
    id,
    sessionId,
    timestamp: new Date().toISOString(),
    type: "decision",
    title: `${concept} observation ${id}`,
    facts: [`fact about ${concept}`],
    narrative: `detailed narrative about ${concept} pattern usage`,
    concepts: [concept],
    files: ["src/auth.ts"],
    importance: 8,
  };
}

function makeExistingMemory(id: string, title: string, project?: string): Memory {
  return {
    id,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    type: "pattern",
    title,
    content: "existing content",
    concepts: ["auth"],
    files: ["src/auth.ts"],
    sessionIds: [],
    strength: 6,
    version: 1,
    isLatest: true,
    ...(project !== undefined && { project }),
  };
}

describe("project scoped canonical evolution", () => {
  it.each(["web", "api", undefined])("only evolves the exact project winner, previous project=%s", async previousProject => {
    const sdk = makeMockSdk(); const kv = makeMockKV(); const provider = makeProvider("synthesized memory title");
    const previous = makeExistingMemory("old", "synthesized memory title", previousProject);
    await kv.set(KV.memories, previous.id, previous);
    const session = makeSession("sess_api", "api"); await kv.set(KV.sessions, session.id, session);
    const observations = [0, 1, 2].map(i => makeObs(`obs_${i}`, session.id, "auth"));
    for (const observation of observations) await kv.set(KV.observations(session.id), observation.id, observation);
    const select = vi.fn(async () => observations.map(observation => ({ id: observation.id, sessionId: session.id, rowVersion: "1" })));
    const lookup = vi.fn(async (project, normalizedTitle) => ({ current: previous.project === project ? previous : null,
      token: { status: "ready" as const, scope: "memories" as const, partition: project, normalizedKey: normalizedTitle,
        canonicalId: previous.project === project ? previous.id : null, rowVersion: previous.project === project ? "1" : null,
        scopeRevision: "1", coverageEpoch: "1", normalizationVersion: "js-lower-v1" as const } }));
    const commit = vi.fn(async (_project, _title, match, next, effect) => {
      if (match.current) await kv.set(KV.memories, match.current.id, { ...match.current, isLatest: false });
      await kv.set(KV.memories, next.id, next);
      return { applied: true, created: true, id: next.id, rowVersion: "2", receipt: { operationKind: "evolve" as const,
        effectKey: effect, operationFingerprint: "bounded-test", id: next.id, rowVersion: "2", created: true, resultFingerprint: "result" } };
    });
    const list = vi.spyOn(kv, "list").mockImplementation(() => { throw new Error("global list forbidden"); });
    registerConsolidateFunction(sdk as never, kv as never, provider, { select, lookup, commit });
    let result = await sdk.trigger("mem::consolidate", { project: "api", minObservations: 1 });
    for (let cut = 0; result.queued && cut < 300; cut++) result = await sdk.trigger("mem::consolidate", result.continuation);
    expect(result.completed).toBe(true); expect(result.consolidated).toBe(1);
    expect(select).toHaveBeenCalledWith("api"); expect(lookup).toHaveBeenCalledWith("api", "synthesized memory title");
    expect(list).not.toHaveBeenCalled(); expect(commit).toHaveBeenCalledOnce();
    expect((await kv.get<Memory>(KV.memories, previous.id))?.isLatest).toBe(previousProject !== "api");
    const next = commit.mock.calls[0][3]; expect(next.project).toBe("api");
    expect(next.parentId).toBe(previousProject === "api" ? previous.id : undefined);
  });
  it.each([{}, { project: "api" }])("rejects legacy hosts without a bounded selector before list/provider: %j", async input => {
    const sdk = makeMockSdk(); const kv = makeMockKV(); const provider = makeProvider();
    const list = vi.spyOn(kv, "list"); const compress = vi.spyOn(provider, "compress");
    registerConsolidateFunction(sdk as never, kv as never, provider);
    await expect(sdk.trigger("mem::consolidate", input)).rejects.toThrow("consolidate_project_retrieval_unavailable");
    expect(list).not.toHaveBeenCalled(); expect(compress).not.toHaveBeenCalled();
  });
});
