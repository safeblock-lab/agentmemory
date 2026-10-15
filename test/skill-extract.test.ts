import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

function supportedEvidence(system: string) {
  return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)?.[1], verdict: "supported", reason: "Supported by complete sources" });
}

const mockKv = {
  get: vi.fn(),
  set: vi.fn(),
  list: vi.fn(),
  delete: vi.fn(),
};

const mockSdk = {
  registerFunction: vi.fn(),
};

const mockProvider = {
  name: "test",
  compress: vi.fn(),
  summarize: vi.fn(),
};

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

vi.mock("../src/functions/audit.js", () => ({
  recordAudit: vi.fn(),
}));

import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { WorkflowAttemptAccounting } from "../src/functions/workflow-attempt-accounting.js";
import { IndexedRetrievalError } from "../src/state/indexed-retrieval.js";
import { recordAudit } from "../src/functions/audit.js";
import { KV } from "../src/state/schema.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import type { CompressedObservation, ProceduralMemory } from "../src/types.js";
import type { SkillLifecycleIntent } from "../src/functions/skill-lifecycle.js";

describe("skill provenance and step contract", () => {
  function fixture(stepCount = 2, accounting?: WorkflowAttemptAccounting) {
    const records = new Map<string, unknown>();
    const sources = new Map<string, CompressedObservation[]>();
    const kv = {
      get: vi.fn(async (scope: string, id: string) => structuredClone(records.get(`${scope}:${id}`) ?? null)),
      set: vi.fn(async (scope: string, id: string, value: unknown) => {
        records.set(`${scope}:${id}`, structuredClone(value));
        return value;
      }),
    };
    const xml = `<skill><trigger>New project</trigger><title>Setup</title><steps>${
      Array.from({ length: stepCount }, (_, index) => `<step>Perform setup action ${index + 1}</step>`).join("")
    }</steps></skill>`;
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async (system: string) =>
      system.includes("evaluate publication evidence") ? supportedEvidence(system) : xml) };
    const handlers = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => handlers.set(id, handler) } as any,
      kv as any, provider, undefined, undefined, async (_project, sessionId) =>
        (sources.get(sessionId) ?? []).map(({ id, timestamp }) => ({ id, timestamp, sessionId })), accounting);
    const addSession = (sessionId: string, count = 3) => {
      records.set(`${KV.sessions}:${sessionId}`, { id: sessionId, project: "project", status: "completed" });
      records.set(`${KV.summaries}:${sessionId}`, {
        sessionId, project: "project", title: "Setup", narrative: `Complete setup actions in sequence from ${count} observations`,
        concepts: ["setup"], keyDecisions: [], filesModified: [],
      });
      const observations = Array.from({ length: count }, (_, index) => ({
        id: `${sessionId}-observation-${index}`, sessionId, timestamp: "2026-10-01T00:00:00Z",
        type: "discovery", title: "Setup", narrative: "Install dependency then validate",
        importance: 7, concepts: ["setup"], files: [], facts: [],
      } as CompressedObservation));
      sources.set(sessionId, observations);
      for (const observation of observations) records.set(`${KV.observations(sessionId)}:${observation.id}`, observation);
      return observations.map(({ id }) => id);
    };
    const run = (sessionId: string, intent?: SkillLifecycleIntent) => handlers.get("mem::skill-extract")!({ sessionId,
      ...(intent ? { intentId: intent.id, snapshotFingerprint: intent.summaryFingerprint } : {}) });
    return { records, kv, provider, addSession, run };
  }
  it("scopes actual skill generation and publication separately", async () => {
    const accounting = new WorkflowAttemptAccounting([], []); const spy = vi.spyOn(accounting, "run");
    const h = fixture(2, accounting); h.addSession("accounted"); await h.run("accounted");
    expect(spy.mock.calls.map(call => call[1].phase)).toEqual(["generation", "publication"]);
    expect(spy.mock.calls.every(call => call[1].sourceId === "accounted")).toBe(true);
    await h.run("accounted"); expect(spy).toHaveBeenCalledTimes(2);
  });

  beforeEach(() => { vi.mocked(recordAudit).mockReset(); });
  afterEach(() => { vi.mocked(recordAudit).mockReset(); });

  it("merges second-session observations and reinforces only once", async () => {
    const test = fixture();
    const firstIds = test.addSession("first");
    const first = await test.run("first");
    const secondIds = test.addSession("second");
    const second = await test.run("second");
    expect(second.skillId).toBe(first.skillId);
    expect(second.skill).toMatchObject({ sourceObservationIds: [...firstIds, ...secondIds],
      sourceSessionIds: ["first", "second"], frequency: 2, strength: 0.75 });
    const replay = await test.run("second");
    expect(replay.skill.sourceObservationIds).toEqual([...firstIds, ...secondIds]);
    expect(replay.skill.frequency).toBe(2);
    expect(replay.skill.strength).toBe(0.75);
    expect(test.provider.summarize).toHaveBeenCalledTimes(4);
  });

  it("merges new evidence from the same session without reinforcement or duplicates", async () => {
    const test = fixture();
    test.addSession("same");
    await test.run("same");
    const allIds = test.addSession("same", 4);
    const updated = await test.run("same");
    expect(updated.skill).toMatchObject({ sourceObservationIds: allIds, sourceSessionIds: ["same"],
      frequency: 1, strength: 0.6 });
    const calls = test.provider.summarize.mock.calls.length;
    const replay = await test.run("same");
    expect(replay.skill.sourceObservationIds).toEqual(allIds);
    expect(replay.skill.frequency).toBe(1);
    expect(test.provider.summarize).toHaveBeenCalledTimes(calls);
  });

  it("recovers an interrupted reinforced effect and audit without repeating its mutation or provider calls", async () => {
    const test = fixture();
    const firstIds = test.addSession("first");
    await test.run("first");
    const secondIds = test.addSession("second");
    const intent: SkillLifecycleIntent = {
      kind: "intent", id: "second-intent", effectId: batchEffectKey(JSON.stringify(["skill-effect", "second", "fp"])),
      sessionId: "second", summaryFingerprint: "fp", consolidationEnabled: true, skipConsolidation: false,
      providerEligible: true, protectedSignal: false, status: "running", attempts: 1, enqueueFailures: 0,
      createdAt: "2026-10-01T00:00:00Z", updatedAt: "2026-10-01T00:00:00Z",
    };
    test.records.set(`${KV.skillLifecycleIntents}:${intent.id}`, intent);
    test.records.set(`${KV.summaryQueueCompleted}:second`, { fingerprint: "fp" });
    vi.mocked(recordAudit).mockRejectedValueOnce(new Error("audit interrupted"));
    await expect(test.run("second", intent)).rejects.toThrow("audit interrupted");
    const savedIntent = test.records.get(`${KV.skillLifecycleIntents}:${intent.id}`) as SkillLifecycleIntent;
    const savedSkill = test.records.get(`${KV.procedural}:${savedIntent.effectPlan!.skillId}`) as ProceduralMemory;
    expect(savedSkill.sourceObservationIds).toEqual([...firstIds, ...secondIds]);
    expect(savedSkill.frequency).toBe(2);
    expect(savedSkill.appliedBatchEffects).toEqual([intent.effectId]);
    const calls = test.provider.summarize.mock.calls.length;
    const replay = await test.run("second", intent);
    expect(replay).toMatchObject({ extracted: true, reinforced: true, effectReplay: true });
    expect(replay.skill).toEqual(savedSkill);
    expect((await test.run("second", intent)).skill).toEqual(savedSkill);
    expect(test.provider.summarize).toHaveBeenCalledTimes(calls);
  });

  it.each([1, 2, 10, 11])("accepts exactly 2 through 10 steps: %i", async count => {
    const test = fixture(count);
    test.addSession("session");
    const result = await test.run("session");
    const valid = count >= 2 && count <= 10;
    expect(result.success).toBe(valid);
    if (valid) expect(result.skill.steps).toHaveLength(count);
    else {
      expect(result.error).toContain("malformed output");
      expect([...test.records.keys()].filter(key => key.startsWith(`${KV.procedural}:`))).toHaveLength(0);
      expect([...test.records.keys()].filter(key => key.startsWith(`${KV.state}:skillraw:`))).toHaveLength(1);
      await test.run("session");
      expect(test.provider.summarize).toHaveBeenCalledTimes(1);
    }
  });
});

describe("selected skill source recovery", () => {
  it("keeps a strict index preparation result queued without inventing a fragment job", async () => {
    const records = new Map<string, any>([
      ["mem:sessions:s", { id: "s", project: "project", status: "completed" }],
      ["mem:summaries:s", { sessionId: "s", project: "project", title: "Setup", narrative: "Install and validate",
        concepts: ["setup"], keyDecisions: [], filesModified: [] }],
    ]);
    const kv = { indexedRetrieval: true, get: vi.fn(async (scope: string, id: string) => records.get(`${scope}:${id}`) ?? null),
      set: vi.fn(async () => undefined), list: vi.fn() };
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const handlers = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => handlers.set(id, handler) } as any,
      kv as any, provider, undefined, undefined, async () => { throw new IndexedRetrievalError("STATE_INDEX_NOT_READY", "coverage pending"); });

    const result = await handlers.get("mem::skill-extract")!({ sessionId: "s" });

    expect(result).toMatchObject({ success: true, queued: true, completed: false, reason: "skill_index_preparing",
      continuation: { sessionId: "s" } });
    expect(result.jobId).toBeUndefined();
    expect(provider.summarize).not.toHaveBeenCalled();
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("rejects a legacy extractor without a bounded selector before any list/provider call", async () => {
    const kv = { get: vi.fn(async (scope: string) => scope === "mem:sessions" ? { id: "s", project: "project", status: "completed" } : null),
      list: vi.fn(), set: vi.fn() };
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn() }; const handlers = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => handlers.set(id, handler) } as any,
      kv as any, provider);
    await expect(handlers.get("mem::skill-extract")!({ sessionId: "s" })).rejects.toThrow("skill_source_selector_unavailable");
    expect(kv.list).not.toHaveBeenCalled(); expect(provider.summarize).not.toHaveBeenCalled();
  });

  it.each([
    "bounded_candidate_budget",
    ["bounded_candidate_budget", "bounded_candidate_budget"],
    ["unknown_budget"],
    ["bounded_candidate_budget", "bounded_query_budget", "extra_budget"],
  ])("rejects malformed selected retrieval coverage: %j", async retrievalCoverage => {
    const records = new Map<string, any>([
      ["mem:sessions:s", { id: "s", project: "project", status: "completed" }],
      ["mem:summaries:s", { sessionId: "s", project: "project", title: "Setup", narrative: "Install and validate",
        concepts: ["setup"], keyDecisions: [], filesModified: [] }],
    ]);
    const kv = { get: vi.fn(async (scope: string, id: string) => records.get(`${scope}:${id}`) ?? null),
      set: vi.fn(async () => undefined), list: vi.fn() };
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const handlers = new Map<string, Function>();
    const select = async () => [0, 1, 2].map(index => ({ id: `o${index}`, sessionId: "s", timestamp: "2026-10-01",
      ...(index === 0 ? { retrievalCoverage } : {}) })) as any;
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => handlers.set(id, handler) } as any,
      kv as any, provider, undefined, undefined, select);

    await expect(handlers.get("mem::skill-extract")!({ sessionId: "s" }))
      .rejects.toThrow("consolidation_retrieval_coverage_invalid");
    expect(provider.summarize).not.toHaveBeenCalled();
    expect(kv.set).not.toHaveBeenCalled();
  });

  it.each(["stale", "failed"] as const)("unions intent and captured-result coverage on %s replay", async status => {
    const retrievalCoverage = ["bounded_query_budget"];
    const capturedCoverage = ["bounded_candidate_budget"];
    const records = new Map<string, any>([
      [`${KV.sessions}:s`, { id: "s", project: "project", status: "completed" }],
      [`${KV.skillLifecycleIntents}:intent-1`, { kind: "intent", id: "intent-1", sessionId: "s",
        summaryFingerprint: "fp", status, terminalReason: "source_retrieval_resource_limit", retrievalCoverage,
        capturedResult: { kind: "skip", reason: "no_skill", retrievalCoverage: capturedCoverage } }],
    ]);
    const kv = { get: vi.fn(async (scope: string, id: string) => records.get(`${scope}:${id}`) ?? null),
      set: vi.fn(async () => undefined), list: vi.fn() };
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const handlers = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => handlers.set(id, handler) } as any,
      kv as any, provider);

    const result = await handlers.get("mem::skill-extract")!({ sessionId: "s", intentId: "intent-1", snapshotFingerprint: "fp" });

    expect(result.retrievalCoverage).toEqual(["bounded_candidate_budget", "bounded_query_budget"]);
    if (status === "stale") expect(result).toMatchObject({ success: true, stale: true, reason: "summary_superseded" });
    else expect(result).toMatchObject({ success: false, failureCode: "source_retrieval_resource_limit",
      reason: "source_retrieval_resource_limit" });
    expect(kv.get).toHaveBeenCalledTimes(2);
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("returns an explicit resource-limit result before extraction, gate, effect, or audit calls", async () => {
    const records = new Map<string, any>([
      ["mem:sessions:s", { id: "s", project: "project", status: "completed" }],
      ["mem:summaries:s", { sessionId: "s", project: "project", title: "Setup", narrative: "Install and validate",
        concepts: ["setup"], keyDecisions: [], filesModified: [] }],
      [`${KV.skillLifecycleIntents}:intent-1`, { kind: "intent", id: "intent-1", sessionId: "s",
        summaryFingerprint: "fp", status: "queued", attempts: 0 }],
      [`mem:summary-queue:completed:s`, { fingerprint: "fp" }],
    ]);
    const kv = { get: vi.fn(async (scope: string, id: string) => records.get(`${scope}:${id}`) ?? null),
      set: vi.fn(async () => undefined), list: vi.fn() };
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const typeSafe = { evaluateChoice: vi.fn() };
    const handlers = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => handlers.set(id, handler) } as any,
      kv as any, provider, undefined, typeSafe as any, async () => {
        throw new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "native query work budget exhausted");
      });

    const result = await handlers.get("mem::skill-extract")!({ sessionId: "s", intentId: "intent-1", snapshotFingerprint: "fp" });

    expect(result).toMatchObject({ success: false, partial: true, extracted: false, completed: false, queued: false,
      errorCode: "STATE_RETRIEVAL_RESOURCE_LIMIT", failureCode: "source_retrieval_resource_limit",
      reason: "source_retrieval_resource_limit",
      continuation: { sessionId: "s", intentId: "intent-1", snapshotFingerprint: "fp" } });
    expect(provider.summarize).not.toHaveBeenCalled();
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(recordAudit).not.toHaveBeenCalled();
    expect(kv.set).not.toHaveBeenCalled();
  });

  it.each(["7", "8"])("checks selected native row version before generation, actual=%s", async version => {
    const records = new Map<string, any>([
      ["mem:sessions:s", { id: "s", project: "project", status: "completed" }],
      ["mem:summaries:s", { sessionId: "s", project: "project", title: "Setup", narrative: "Install and validate", concepts: ["setup"], keyDecisions: [], filesModified: [] }],
    ]);
    const observations = [0, 1, 2].map(i => ({ id: `v${i}`, sessionId: "s", timestamp: "2026-10-01", title: "Setup",
      narrative: "Install dependency then validate", facts: [], concepts: ["setup"], files: [], importance: 7 }));
    for (const observation of observations) records.set(`mem:obs:s:${observation.id}`, observation);
    const kv = { indexedRetrieval: true,
      get: async (scope: string, id: string) => records.get(`${scope}:${id}`) ?? null,
      getVersioned: vi.fn(async (scope: string, id: string) => ({ exists: true, value: records.get(`${scope}:${id}`), version })),
      set: async (scope: string, id: string, value: unknown) => { records.set(`${scope}:${id}`, value); return value; },
    };
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async (system: string) => system.includes("evaluate publication evidence") ? supportedEvidence(system) :
      '<skill><trigger>New project</trigger><title>Setup</title><steps><step>Install dependency</step><step>Validate</step></steps></skill>') };
    const functions = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => functions.set(id, handler) } as any, kv as any,
      provider, undefined, undefined, async () => observations.map(observation => ({ id: observation.id, sessionId: "s", rowVersion: "7" })));
    const result = functions.get("mem::skill-extract")!({ sessionId: "s" });
    if (version === "8") {
      await expect(result).rejects.toThrow("skill_source_selection_stale");
      expect(provider.summarize).not.toHaveBeenCalled();
    } else {
      expect((await result).extracted).toBe(true);
      expect(kv.getVersioned).toHaveBeenCalledTimes(12);
    }
  });

  it.each([
    { large: false, withCoverage: true },
    { large: false, withCoverage: false },
    { large: "ascii", withCoverage: true },
    { large: "multibyte", withCoverage: true },
  ])("captures exact session/project sources and provider result without scanning the session on replay", async ({ large, withCoverage }) => {
    const records = new Map<string, any>();
    let interruptCursor = large;
    const kv = { indexedRetrieval: true,
      get: vi.fn(async (scope: string, id: string) => structuredClone(records.get(`${scope}:${id}`) ?? null)),
      set: vi.fn(async (scope: string, id: string, value: any) => {
        if (interruptCursor && id.startsWith("skillparts_") && value.evidenceCount === 1) {
          interruptCursor = false; throw new Error("simulated cut after response capture");
        }
        records.set(`${scope}:${id}`, structuredClone(value)); return value;
      }),
      list: vi.fn(() => { throw new Error("unbounded list forbidden"); }),
    };
    const session = { id: "selected-session", project: "project", status: "completed" };
    const summary = { sessionId: session.id, project: session.project, title: "Configure authentication", narrative: "Reusable setup",
      concepts: ["authentication"], keyDecisions: ["Validate token"], filesModified: ["auth.ts"] };
    records.set(`mem:sessions:${session.id}`, session);
    records.set(`mem:summaries:${session.id}`, summary);
    const observations = [0, 1, 2].map(i => ({ id: `selected-${i}`, sessionId: session.id, timestamp: "2026-10-01T00:00:00Z",
      title: `Configure step ${i}`, narrative: large && i === 0 ? (large === "multibyte" ? "配置依赖🔐，安装认证并验证令牌。" : "Install dependency, configure token and validate authentication. ").repeat(1800)
        : `Repeatable action ${i}`, concepts: ["authentication"], facts: [], files: [], importance: 7 }));
    for (const observation of observations) records.set(`mem:obs:${session.id}:${observation.id}`, observation);
    const select = vi.fn(async () => observations.map((observation, index) => ({ id: observation.id, sessionId: session.id,
      timestamp: observation.timestamp,
      ...(withCoverage ? { retrievalCoverage: [index === 0 ? "bounded_query_budget" as const : "bounded_candidate_budget" as const] } : {}) })));
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async (system: string, _prompt: string) => system.includes("evaluate publication evidence") ? supportedEvidence(system) : system.includes("partial evidence")
      ? '<evidence><action>Configure token after installing dependency</action><action>Validate authentication</action></evidence>'
      : '<skill><trigger>Configure authentication</trigger><title>Authentication setup</title><steps><step>Configure token</step><step>Validate token</step></steps></skill>') };
    const functions = new Map<string, Function>();
    registerSkillExtractFunctions({ registerFunction: (id: string, handler: Function) => functions.set(id, handler) } as any,
      kv as any, provider, undefined, undefined, select);
    const run = () => functions.get("mem::skill-extract")!({ sessionId: session.id });
    if (large) {
      await expect(run()).rejects.toThrow("simulated cut after response capture");
      expect(provider.summarize).toHaveBeenCalledOnce();
    }
    let first = await run();
    if (large) expect(provider.summarize).toHaveBeenCalledOnce();
    if (large) expect(first).toMatchObject({ success: true, queued: true, completed: false, reason: "skill_source_fragmentation" });
    const expectedCoverage = withCoverage ? ["bounded_candidate_budget", "bounded_query_budget"] : undefined;
    expect(first.retrievalCoverage).toEqual(expectedCoverage);
    if (large && withCoverage) expect(first.continuation.retrievalCoverage).toEqual(expectedCoverage);
    for (let i = 0; first.queued && i < 20; i++) {
      const calls = provider.summarize.mock.calls.length;
      first = await functions.get("mem::skill-extract")!(first.continuation);
      expect(provider.summarize.mock.calls.length - calls).toBeLessThanOrEqual(1);
    }
    expect(first.success).toBe(true);
    expect(first.extracted).toBe(true);
    expect(first.retrievalCoverage).toEqual(expectedCoverage);
    const calls = provider.summarize.mock.calls.length;
    const second = await run();
    expect(second.success).toBe(true);
    expect(second.retrievalCoverage).toEqual(expectedCoverage);
    expect(select).toHaveBeenCalledOnce();
    expect(select).toHaveBeenCalledWith("project", session.id);
    expect(provider.summarize).toHaveBeenCalledTimes(calls);
    const sourceCapture = [...records.values()].find((value: any) => value?.selectionCoverage === "selected_relevant_observations");
    if (!large) expect(sourceCapture?.retrievalCoverage).toEqual(expectedCoverage);
    const capturedResult = [...records.values()].find((value: any) => value?.kind === "skill");
    expect(capturedResult?.retrievalCoverage).toEqual(expectedCoverage);
    if (large) {
      const fragmented = [...records.entries()].find(([id]) => id.endsWith(":fragmented"))?.[1] as any;
      expect(fragmented.retrievalCoverage).toEqual(expectedCoverage);
    }
    if (large) {
      const judgePrompt = provider.summarize.mock.calls.find(([system]) => system.includes("evaluate publication evidence"))![1];
      const originalChunks = JSON.parse(judgePrompt).sources.filter((source: any) => source.id.startsWith(`${observations[0].id}:chunk:`));
      expect(originalChunks.map((source: any) => JSON.parse(source.content)).sort((a: any, b: any) => a.chunkIndex - b.chunkIndex).map((chunk: any) => chunk.text).join("")).toBe(JSON.stringify(observations[0]));
      expect(calls).toBeGreaterThan(1);
      for (const [system, prompt] of provider.summarize.mock.calls.filter(([system]) => !system.includes("evaluate publication evidence"))) {
        expect(Buffer.byteLength(JSON.stringify({ system, prompt }))).toBeLessThanOrEqual(65_536);
        const fragments = JSON.parse(prompt).fragments ?? [];
        for (const fragment of fragments) {
          expect(Buffer.byteLength(fragment.text)).toBeLessThanOrEqual(4096);
          expect(fragment.text).not.toMatch(/[\uD800-\uDBFF]$/);
          expect(fragment.text).not.toMatch(/^[\uDC00-\uDFFF]/);
        }
      }
      const allFragments = provider.summarize.mock.calls.flatMap(([, prompt]) => JSON.parse(prompt).fragments ?? []);
      for (const observation of observations) {
        const fragments = allFragments.filter(fragment => fragment.sourceId === observation.id);
        const expected = JSON.stringify({ title: observation.title, narrative: observation.narrative, facts: observation.facts,
          concepts: observation.concepts, files: observation.files });
        expect(fragments[0].start).toBe(0);
        for (let i = 1; i < fragments.length; i++) expect(fragments[i].start).toBe(fragments[i - 1].end);
        expect(fragments.at(-1).end).toBe(expected.length);
        expect(fragments.map(fragment => fragment.text).join("")).toBe(expected);
      }
    } else expect(provider.summarize).toHaveBeenCalledTimes(2);
    expect(kv.list).not.toHaveBeenCalled();
    const skills = [...records.entries()].filter(([key]) => key.startsWith("mem:procedural:"));
    expect(skills).toHaveLength(1);
    expect(skills[0][1].sourceObservationIds).toEqual(observations.map(observation => observation.id));
    expect(skills[0][1].frequency).toBe(1);
  });
});

describe("skill-extract", () => {
  let handlers: Record<string, Function>;

  beforeEach(() => {
    vi.clearAllMocks();
    mockKv.get.mockResolvedValue(null);
    mockKv.set.mockReset().mockResolvedValue(undefined);
    mockKv.list.mockResolvedValue([]);

    handlers = {};
    mockSdk.registerFunction.mockImplementation((idOrMeta: any, handler: any) => {
      const id = typeof idOrMeta === "string" ? idOrMeta : idOrMeta.id;
      handlers[id] = handler;
    });

    registerSkillExtractFunctions(mockSdk as any, mockKv as any, mockProvider, undefined, undefined, async (_project, sessionId) =>
      [0, 1, 2, 3, 4].map(i => ({ id: `obs${i}`, sessionId, timestamp: "2026-10-01" })));
  });

  it("registers all skill functions", () => {
    expect(handlers["mem::skill-extract"]).toBeDefined();
    expect(handlers["mem::skill-list"]).toBeDefined();
    expect(handlers["mem::skill-match"]).toBeDefined();
  });

  it("skill-extract requires sessionId", async () => {
    const result = await handlers["mem::skill-extract"]({});
    expect(result.success).toBe(false);
  });

  it("skill-extract returns error for missing session", async () => {
    mockKv.get.mockReturnValue(Promise.resolve(null));
    const result = await handlers["mem::skill-extract"]({
      sessionId: "nonexistent",
    });
    expect(result.success).toBe(false);
    expect(result.error).toContain("session not found");
  });

  it("skill-extract parses LLM response into ProceduralMemory", async () => {
    const persisted = new Map<string, unknown>();
    mockKv.set.mockImplementation(async (scope: string, key: string, value: unknown) => {
      persisted.set(`${scope}:${key}`, structuredClone(value));
    });
    mockKv.get.mockImplementation((scope: string, key: string) => {
      if (persisted.has(`${scope}:${key}`)) return Promise.resolve(structuredClone(persisted.get(`${scope}:${key}`)));
      if (scope === "mem:obs:s1") return Promise.resolve({ id: key, sessionId: "s1", timestamp: "2026-10-01", type: "file_edit",
        title: "Edit auth", narrative: "Configure authentication then validate", importance: 7, concepts: ["JWT"], files: [], facts: [] });
      if (scope === "mem:sessions")
        return Promise.resolve({ id: "s1", project: "test", status: "completed" });
      if (scope === "mem:summaries")
        return Promise.resolve({
          sessionId: "s1",
          project: "test",
          title: "Fix auth bug",
          narrative: "Debugged and fixed JWT expiration",
          keyDecisions: ["Switch to RS256"],
          filesModified: ["auth.ts"],
          concepts: ["authentication", "JWT"],
          createdAt: "2026-10-01T00:00:00Z",
          observationCount: 10,
        });
      return Promise.resolve(null);
    });

    mockKv.list.mockReturnValue(
      Promise.resolve(Array.from({ length: 5 }, (_, i) => ({
        id: `obs${i}`,
        sessionId: "s1",
        timestamp: new Date().toISOString(),
        type: "file_edit",
        title: `Edit auth.ts step ${i}`,
        narrative: `Modified JWT validation logic step ${i}`,
        importance: 7,
        concepts: ["JWT"],
        files: ["auth.ts"],
        facts: [],
      }))),
    );

    mockProvider.summarize.mockImplementation(async (system: string) => system.includes("evaluate publication evidence") ? supportedEvidence(system) : `
<skill>
<trigger>When the agent encounters JWT authentication failures or token expiration issues</trigger>
<title>Fix JWT Token Expiration</title>
<steps>
<step>Check the JWT library configuration for clock skew tolerance</step>
<step>Verify the signing algorithm matches between issuer and verifier</step>
<step>Update token expiration to use RS256 with proper key rotation</step>
</steps>
<expected_outcome>JWT auth works reliably with proper expiration handling</expected_outcome>
<tags>jwt,authentication,security</tags>
</skill>
    `);

    const result = await handlers["mem::skill-extract"]({ sessionId: "s1" });
    expect(result.success).toBe(true);
    expect(result.extracted).toBe(true);
    expect(result.skill.name).toBe("Fix JWT Token Expiration");
    expect(result.skill.steps).toHaveLength(3);
    expect(result.skill.triggerCondition).toContain("JWT");
    expect(mockKv.set).toHaveBeenCalled();
  });

  it.each(["<no-skill/>", "garbled provider output", "<skill><title></title></skill>"])("distinguishes an explicit no-skill decision from malformed output: %s", async (response) => {
    mockKv.get.mockImplementation((scope: string, key: string) => {
      if (scope === "mem:obs:s1") return Promise.resolve({ id: key, sessionId: "s1", timestamp: "2026-10-01", type: "file_read",
        title: "Read file", narrative: "", importance: 5, concepts: [], files: [], facts: [] });
      if (scope === "mem:sessions") return Promise.resolve({ id: "s1", project: "test", status: "completed" });
      if (scope === "mem:summaries")
        return Promise.resolve({
          sessionId: "s1",
          project: "test",
          title: "Explore codebase",
          narrative: "Browsed files",
          keyDecisions: [],
          filesModified: [],
          concepts: [],
          createdAt: "2026-10-01T00:00:00Z",
          observationCount: 3,
        });
      return Promise.resolve(null);
    });
    mockKv.list.mockReturnValue(
      Promise.resolve(Array.from({ length: 4 }, (_, i) => ({
        id: `obs${i}`,
        sessionId: "s1",
        timestamp: new Date().toISOString(),
        type: "file_read",
        title: `Read file ${i}`,
        importance: 5,
        concepts: [],
        files: [],
        facts: [],
        narrative: "",
      }))),
    );
    mockProvider.summarize.mockResolvedValue(response);

    const result = await handlers["mem::skill-extract"]({ sessionId: "s1" });
    expect(result.success).toBe(response === "<no-skill/>");
    if (response === "<no-skill/>") expect(result.extracted).toBe(false);
    else expect(result.error).toMatch(/malformed output/);
    expect(mockKv.set.mock.calls.filter(call => call[0] === "mem:procedural")).toHaveLength(0);
  });

  it("skill-list returns sorted by strength", async () => {
    mockKv.list.mockResolvedValue([
      { id: "s1", name: "Low", strength: 0.3 },
      { id: "s2", name: "High", strength: 0.9 },
    ]);
    const result = await handlers["mem::skill-list"]({});
    expect(result.skills[0].name).toBe("High");
  });

  it("skill-match finds relevant skills", async () => {
    mockKv.list.mockResolvedValue([
      {
        id: "s1",
        name: "Fix JWT Auth",
        triggerCondition: "JWT failures",
        tags: ["jwt", "auth"],
        steps: ["check config", "update key"],
        strength: 0.8,
      },
      {
        id: "s2",
        name: "Deploy Docker",
        triggerCondition: "container deployment",
        tags: ["docker", "deploy"],
        steps: ["build image", "push"],
        strength: 0.7,
      },
    ]);

    const result = await handlers["mem::skill-match"]({
      query: "JWT authentication token expired",
    });
    expect(result.matches.length).toBe(1);
    expect(result.matches[0].skill.name).toBe("Fix JWT Auth");
  });
});
