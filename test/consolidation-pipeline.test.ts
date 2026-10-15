import { describe, it, expect, beforeEach, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

vi.mock("../src/config.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/config.js")>()),
  getConsolidationDecayDays: () => 30,
  getConsolidationMinNewSummaries: () => 5,
  isConsolidationEnabled: vi.fn(() => true),
}));

import { registerConsolidationPipelineFunction as registerPipeline } from "../src/functions/consolidation-pipeline.js";
import { WorkflowAttemptAccounting } from "../src/functions/workflow-attempt-accounting.js";
import { loadRoot, pageKey, semanticProjectCheckpointKey } from "../src/functions/consolidation-manifest.js";
import { fragmentSources, requestInputBytes } from "../src/functions/consolidation-partitions.js";
import { SEMANTIC_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt } from "../src/prompts/consolidation.js";
import { isConsolidationEnabled } from "../src/config.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import { fingerprintId, KV } from "../src/state/schema.js";
import { LlmTaskRouter } from "../src/providers/task-router.js";
import type { SessionSummary, Memory, SemanticMemory, ProceduralMemory, LlmRoutingConfig, LlmCallOptions } from "../src/types.js";
import { canonicalConsolidationId, selectProceduralObservations, selectConsolidationSummaries } from "../src/functions/consolidation-selection.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  const versions = new Map<string, number>();
  return {
    indexedRetrieval: true,
    fixtureStore: store,
    retrieval: async (request?: Record<string, unknown>) => {
      if (request?.action === "canonical_apply" && request.receipt_only === true) return { status: "not_applied" };
      if (request?.action === "canonical_apply") throw new Error("Fixture forbids native canonical writes");
      return { status: "ready", items: [{ project: "test-project" }], has_more: false };
    },
    getVersioned: async <T>(scope: string, key: string) => ({ exists: store.get(scope)?.has(key) ?? false,
      value: structuredClone((store.get(scope)?.get(key) as T) ?? null), version: String(versions.get(`${scope}:${key}`) ?? 0) }),
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      return structuredClone((store.get(scope)?.get(key) as T) ?? null);
    },
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, structuredClone(data));
      versions.set(`${scope}:${key}`, (versions.get(`${scope}:${key}`) ?? 0) + 1);
      if (scope === KV.summaries) {
        const summary = data as SessionSummary;
        if (!store.has(KV.sessions)) store.set(KV.sessions, new Map());
        store.get(KV.sessions)!.set(summary.sessionId, { id: summary.sessionId, project: summary.project });
        versions.set(`${KV.sessions}:${summary.sessionId}`, (versions.get(`${KV.sessions}:${summary.sessionId}`) ?? 0) + 1);
      }
      if (scope === KV.memories && (data as Memory).type === "pattern") {
        const memory = data as Memory; const sid = memory.sessionIds[0] ?? "ses-pattern";
        const project = memory.project ?? "test-project";
        if (!store.has(KV.sessions)) store.set(KV.sessions, new Map());
        store.get(KV.sessions)!.set(sid, { id: sid, project });
        versions.set(`${KV.sessions}:${sid}`, (versions.get(`${KV.sessions}:${sid}`) ?? 0) + 1);
        if (!store.has(KV.summaries)) store.set(KV.summaries, new Map());
        store.get(KV.summaries)!.set(sid, { ...makeSummary(1), sessionId: sid, project }); versions.set(`${KV.summaries}:${sid}`, 1);
        if (!store.has(KV.observations(sid))) store.set(KV.observations(sid), new Map());
        store.get(KV.observations(sid))!.set(memory.id, { id: memory.id, sessionId: sid, timestamp: memory.updatedAt,
          title: memory.title, narrative: memory.content, facts: [], concepts: memory.concepts, files: [], importance: 7 });
        versions.set(`${KV.observations(sid)}:${memory.id}`, 1);
      }
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

function mockSdk() {
  const functions = new Map<string, Function>();
  return {
    registerFunction: (idOrOpts: string | { id: string }, handler: Function) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      functions.set(id, handler);
    },
    registerTrigger: () => {},
    trigger: async (idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      const fn = functions.get(id);
      if (!fn) throw new Error(`No function: ${id}`);
      let result = await fn(payload);
      // Each iteration is a separate public continuation cut, not one unbounded handler invocation.
      for (let cut = 0; id === "mem::consolidate-pipeline" && result.queued && result.continuation && cut < 2000; cut++) {
        const stage = result.results ? Object.values(result.results).find((value: any) => value?.queued) : result;
        if (!(stage as any)?.phase) break;
        result = await fn(result.continuation);
      }
      return result;
    },
  };
}

function registerConsolidationPipelineFunction(...args: Parameters<typeof registerPipeline>) {
  const kv = args[1] as unknown as ReturnType<typeof mockKV>;
  if (!kv.indexedRetrieval) return registerPipeline(...args);
  const provider = args[2];
  args[2] = { ...provider, summarize: async (system, prompt, options) => {
    if (!system.startsWith("You evaluate publication evidence.")) return provider.summarize(system, prompt, options);
    const input = JSON.parse(prompt);
    expect(input.workflow).toBe("consolidation");
    expect(input.sources.length).toBeGreaterThan(0);
    for (const source of input.sources) {
      expect(source.project).toBe(input.project);
      expect(source.content).toBe(JSON.stringify(await kv.get(source.scope, source.key)));
      expect(source.version).toBe((await kv.getVersioned(source.scope, source.key)).version);
    }
    return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)?.[1], verdict: "supported",
      reason: "The fixture candidate is supported by the supplied original records." });
  } };
  args[7] ??= async project => {
    const selected: Array<{ id: string; sessionId: string; rowVersion: string }> = [];
    for (const summary of kv.fixtureStore.get(KV.summaries)?.values() ?? []) {
      const source = summary as SessionSummary; if (source.project !== project) continue;
      const observations = kv.fixtureStore.get(KV.observations(source.sessionId));
      if (observations?.size) for (const [id] of observations) selected.push({ id, sessionId: source.sessionId, rowVersion: (await kv.getVersioned(KV.observations(source.sessionId), id)).version });
      else selected.push({ id: `fixture-ref-${source.sessionId}`, sessionId: source.sessionId, rowVersion: "1" });
    }
    return selected.slice(0, 12);
  };
  args[8] ??= async () => ({ status: "ready", canonicalId: null, version: "1" });
  args[10] ??= async (scope, _key, _match, id, effect, update) => {
    const old = await kv.get<any>(KV[scope], id); const already = old?.appliedBatchEffects?.includes(effect) ?? false;
    await applyBatchEffect(kv as never, KV[scope], id, effect, update);
    return { applied: !already, created: !old, id, rowVersion: "2", receipt: { operationKind: "upsert", effectKey: effect,
      operationFingerprint: "fixture", id, rowVersion: "2", created: !old, resultFingerprint: "fixture" } };
  };
  return registerPipeline(...args);
}

function makeSummary(i: number): SessionSummary {
  return {
    sessionId: `ses_${i}`,
    project: "test-project",
    createdAt: new Date(Date.now() - i * 86400000).toISOString(),
    title: `Session ${i} summary`,
    narrative: `Worked on feature ${i}`,
    keyDecisions: [`Decision ${i}`],
    filesModified: [`src/file${i}.ts`],
    concepts: ["typescript", "testing"],
    observationCount: 5,
  };
}

function makePattern(i: number): Memory {
  return {
    id: `mem_${i}`,
    createdAt: "2026-01-01T00:00:00Z",
    updatedAt: "2026-01-01T00:00:00Z",
    type: "pattern",
    title: `Pattern ${i}`,
    content: `Always do thing ${i}`,
    concepts: ["testing"],
    files: [],
    sessionIds: ["ses_1", "ses_2"],
    strength: 5,
    version: 1,
    isLatest: true,
  };
}

function promptBody(prompt: string): string { return prompt.replace(/^Consolidation manifest: [^\n]+\n\n/, ""); }
function fragmentIds(prompt: string): string[] {
  const body = promptBody(prompt);
  if (!body.startsWith("Analyze these original source fragments")) return [];
  return [...new Set((JSON.parse(body.slice(body.indexOf("\n\n") + 2)) as Array<{ sourceId: string }>).map((f) => f.sourceId))];
}
function candidatesFromPrompt(userPrompt: string): Array<{ fact: string; candidateIds: string[] }> {
  const prefix = "Candidate facts with source evidence:\n\n";
  return JSON.parse(promptBody(userPrompt).slice(prefix.length)) as Array<{ fact: string; candidateIds: string[] }>;
}

function candidateIdsFromPrompt(userPrompt: string): string[] {
  const candidates = candidatesFromPrompt(userPrompt);
  return [...new Set(candidates.flatMap((candidate) => candidate.candidateIds))];
}

describe("Consolidation Pipeline", () => {
  let sdk: ReturnType<typeof mockSdk>;
  let kv: ReturnType<typeof mockKV>;

  beforeEach(() => {
    vi.mocked(isConsolidationEnabled).mockReturnValue(true);
    sdk = mockSdk();
    kv = mockKV();
  });

  it("rejects a legacy pipeline before corpus listing or provider invocation", async () => {
    kv.indexedRetrieval = false;
    const list = vi.spyOn(kv, "list"); const summarize = vi.fn();
    registerConsolidationPipelineFunction(sdk as never, kv as never, { summarize } as never);
    expect(await sdk.trigger("mem::consolidate-pipeline", { tier: "all", project: "test-project" })).toEqual({
      success: false, completed: false, error: "consolidation_indexed_retrieval_required" });
    expect(list).not.toHaveBeenCalled(); expect(summarize).not.toHaveBeenCalled();
  });
  it("queues durable partitions and advances only after every extraction and reduction callback, without duplicate effects", async () => {
    const requests: import("../src/types.js").FireworksBatchRequest[] = [];
    const enqueue = vi.fn(async (request: import("../src/types.js").FireworksBatchRequest) => {
      requests.push(request); return { queued: true, workItemId: request.correlationId! };
    });
    const provider = { summarize: vi.fn(), compress: vi.fn() };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, { enqueue });
    for (let i = 0; i < 5; i++) await kv.set(KV.summaries, `ses_${i}`, { ...makeSummary(i), narrative: "é😀".repeat(6000) });
    const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true }) as { results: { semantic: { queued: boolean } } };
    expect(result.results.semantic.queued).toBe(true);
    expect(requests.length).toBeGreaterThan(0);
    expect(requests.length).toBeLessThanOrEqual(2);
    expect(provider.summarize).not.toHaveBeenCalled();
    const delivered = new Set<string>();
    let completed = false;
    for (let step = 0; step < 1000; step++) {
      const request = requests.find((r) => !delivered.has(r.correlationId!));
      if (!request) break;
      delivered.add(request.correlationId!);
      expect(requestInputBytes(request.systemPrompt, request.userPrompt)).toBeLessThanOrEqual(8192);
      const ids = fragmentIds(request.userPrompt);
      const response = ids.length
        ? `<facts><fact confidence="0.8" sourceIds="${ids.join(",")}">Bounded fact</fact></facts>`
        : `<facts><fact confidence="0.9" candidateIds="${candidateIdsFromPrompt(request.userPrompt).join(",")}">Bounded fact</fact></facts>`;
      const applied = await sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: request.metadata!.consolidationManifestId,
        consolidationPartitionId: request.metadata!.consolidationPartitionId, batchResponse: response }) as { completed: boolean };
      completed = applied.completed;
      if (!completed) expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
    }
    expect(completed).toBe(true);
    const facts = await kv.list<SemanticMemory>(KV.semantic);
    expect(facts).toHaveLength(1);
    expect(facts[0].sourceSessionIds.sort()).toEqual(["ses_0", "ses_1", "ses_2", "ses_3", "ses_4"]);
    expect(facts[0].accessCount).toBe(1);
    const first = requests[0];
    await sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: first.metadata!.consolidationManifestId,
      consolidationPartitionId: first.metadata!.consolidationPartitionId,
      batchResponse: `<facts><fact confidence="0.8" sourceIds="${fragmentIds(first.userPrompt).join(",")}">Bounded fact</fact></facts>` });
    expect(await kv.list(KV.semantic)).toEqual(facts);
  });

  it.each(["dead-letter", "completed"])("preserves the original replacement cohort and checkpoint with a %s sibling", async (siblingState) => {
    const requests: import("../src/types.js").FireworksBatchRequest[] = [];
    const enqueue = vi.fn(async (request: import("../src/types.js").FireworksBatchRequest) => {
      requests.push(request); return { queued: true, workItemId: "fresh-replacement" };
    });
    const provider = { summarize: vi.fn(), compress: vi.fn() };
    const register = () => registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, { enqueue });
    register();
    const cohort = "fwbcohort_0123456789abcdef";
    const checkpoint = { processedThrough: "2026-09-11T00:00:00.000Z", processedSessionIdsAtThrough: ["original-last"] };
    await kv.set(KV.state, cohort, { workItemIds: ["old", "sibling"], checkpoint });
    await kv.set(KV.fireworksBatchWorkItems, "sibling", { id: "sibling", state: siblingState });
    await kv.set(KV.fireworksBatchWorkItems, "fresh-replacement", { id: "fresh-replacement", state: "polling" });
    await kv.set(KV.summaries, "ses_0", makeSummary(0));
    const request = { tier: "semantic", deferred: true, replacementOf: "old", batchSourceIds: ["ses_0"], batchCohort: cohort };
    await sdk.trigger("mem::consolidate-pipeline", request);
    expect(requests[0].metadata?.cohort).toBe(cohort);
    expect(await kv.get(KV.state, cohort)).toEqual({ workItemIds: ["fresh-replacement", "sibling"], checkpoint });
    register();
    await sdk.trigger("mem::consolidate-pipeline", request);
    expect(await kv.get(KV.state, cohort)).toEqual({ workItemIds: ["fresh-replacement", "sibling"], checkpoint });
    const metadata = requests[0].metadata!;
    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", force: true, batchSourceIds: ["ses_0"],
      batchResponse: '<fact confidence="0.8">Replacement fact</fact>', batchEffectKey: batchEffectKey("fresh-replacement"),
      batchSourceFingerprint: metadata.sourceFingerprint, batchCohort: cohort });
    expect(await kv.get(KV.state, "semantic-consolidation")).toEqual(siblingState === "completed" ? checkpoint : null);
  });

  it("reconciles a worker completing while replacement enqueue is suspended", async () => {
    const cohort = "fwbcohort_0123456789abcdef";
    const checkpoint = { processedThrough: "2026-09-11T00:00:00.000Z", processedSessionIdsAtThrough: ["original-last"] };
    await kv.set(KV.state, cohort, { workItemIds: ["old"], checkpoint });
    await kv.set(KV.summaries, "ses_0", makeSummary(0));
    await kv.set(KV.fireworksBatchWorkItems, "fresh", { id: "fresh", state: "polling" });
    let releaseEnqueue!: () => void;
    const holdEnqueue = new Promise<void>((resolve) => { releaseEnqueue = resolve; });
    let signalEnqueue!: (request: import("../src/types.js").FireworksBatchRequest) => void;
    const enqueued = new Promise<import("../src/types.js").FireworksBatchRequest>((resolve) => { signalEnqueue = resolve; });
    const enqueue = vi.fn(async (request: import("../src/types.js").FireworksBatchRequest) => {
      signalEnqueue(request); await holdEnqueue; return { queued: true, workItemId: "fresh" };
    });
    registerConsolidationPipelineFunction(sdk as never, kv as never, {} as never, undefined, undefined, { enqueue });
    const replacing = sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true, replacementOf: "old", batchSourceIds: ["ses_0"], batchCohort: cohort });
    await enqueued;
    await kv.set(KV.fireworksBatchWorkItems, "fresh", { id: "fresh", state: "completed" });
    expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
    releaseEnqueue();
    await replacing;
    expect(await kv.get(KV.state, cohort)).toEqual({ workItemIds: ["fresh"], checkpoint });
    expect(await kv.get(KV.state, "semantic-consolidation")).toEqual(checkpoint);
  });

  it("reconciles a completed replacement after restart between enqueue and cohort update", async () => {
    const cohort = "fwbcohort_0123456789abcdef";
    const checkpoint = { processedThrough: "2026-09-11T00:00:00.000Z", processedSessionIdsAtThrough: ["original-last"] };
    await kv.set(KV.state, cohort, { workItemIds: ["old"], checkpoint });
    await kv.set(KV.summaries, "ses_0", makeSummary(0));
    const enqueue = vi.fn(async () => {
      await kv.set(KV.fireworksBatchWorkItems, "fresh", { id: "fresh", state: "completed" });
      return { queued: true, workItemId: "fresh" };
    });
    const originalSet = kv.set;
    let crash = true;
    vi.spyOn(kv, "set").mockImplementation(async (scope, key, value) => {
      if (scope === KV.state && key === cohort && crash) { crash = false; throw new Error("crash before cohort write"); }
      return originalSet(scope, key, value);
    });
    const register = () => registerConsolidationPipelineFunction(sdk as never, kv as never, {} as never, undefined, undefined, { enqueue });
    register();
    const request = { tier: "semantic", deferred: true, replacementOf: "old", batchSourceIds: ["ses_0"], batchCohort: cohort };
    await sdk.trigger("mem::consolidate-pipeline", request);
    expect(await kv.get(KV.state, cohort)).toMatchObject({ workItemIds: ["old"] });
    register();
    await sdk.trigger("mem::consolidate-pipeline", request);
    expect(await kv.get(KV.state, cohort)).toEqual({ workItemIds: ["fresh"], checkpoint });
    expect(await kv.get(KV.state, "semantic-consolidation")).toEqual(checkpoint);
  });

  it("pipeline skips semantic when fewer than 5 summaries", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    for (let i = 0; i < 3; i++) {
      await kv.set("mem:summaries", `ses_${i}`, makeSummary(i));
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { success: boolean; results: Record<string, unknown> };

    expect(result.success).toBe(true);
    const semantic = result.results.semantic as { skipped: boolean; reason: string };
    expect(semantic.skipped).toBe(true);
    expect(semantic.reason).toContain("fewer than 5");
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("preserves legacy queued facts and attributes missing sources to the validated batch", async () => {
    const summaries = Array.from({ length: 5 }, (_, index) => makeSummary(index));
    for (const summary of summaries) await kv.set(KV.summaries, summary.sessionId, summary);
    const sourceIds = summaries.map((summary) => summary.sessionId);
    const sourceFingerprint = fingerprintId("fwbconsem", JSON.stringify(summaries.map((summary) => [
      summary.sessionId,
      summary.title,
      summary.narrative,
      summary.concepts,
      summary.createdAt,
    ])));
    registerConsolidationPipelineFunction(sdk as never, kv as never, {} as never);

    await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
      force: true,
      batchSourceIds: sourceIds,
      batchResponse: '<fact confidence="0.7">Legacy queued fact</fact><fact confidence="0.6">Another queued fact</fact>',
      batchSourceFingerprint: sourceFingerprint,
      batchEffectKey: batchEffectKey("legacy-semantic"),
    });

    const [firstFact] = await kv.list<SemanticMemory>(KV.semantic);
    expect(firstFact?.sourceSessionIds).toEqual(sourceIds);
    expect(await kv.get(KV.state, "semantic-consolidation")).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it("accepts a valid empty facts envelope through the LLM router", async () => {
    const primary = { name: "primary", compress: vi.fn(), summarize: vi.fn(async () => "<facts/>") };
    const auxiliary = { name: "auxiliary", compress: vi.fn(), summarize: vi.fn(async (_system: string, _prompt: string, _options?: LlmCallOptions) => "<facts/>") };
    const routing: LlmRoutingConfig = {
      routes: { consolidation: "aux", conflict_resolution: "aux" } as LlmRoutingConfig["routes"],
      explicitRoutes: {},
      warnings: [],
    };
    const router = new LlmTaskRouter({
      primary: { provider: primary, model: "primary-model" },
      auxiliary: { provider: auxiliary, model: "aux-model" },
      routing,
    });
    for (let index = 0; index < 5; index++) await kv.set(KV.summaries, `empty-${index}`, { ...makeSummary(index), sessionId: `empty-${index}` });
    registerConsolidationPipelineFunction(sdk as never, kv as never, primary as never, router);

    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", force: true });

    expect(auxiliary.summarize).toHaveBeenCalledTimes(5);
    expect(primary.summarize).not.toHaveBeenCalled();
    expect(await kv.list(KV.semantic)).toHaveLength(0);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it.each(["<facts>garbage</facts>", "<facts><unexpected/></facts>"])(
    "fails closed on unrecognized semantic envelope content (%s)",
    async (response) => {
      const accounting = new WorkflowAttemptAccounting([], []);
      const attempts = vi.spyOn(accounting, "run");
      const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async () => response) };
      for (let index = 0; index < 5; index++) await kv.set(KV.summaries, `invalid-${index}`, { ...makeSummary(index), sessionId: `invalid-${index}` });
      registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never,
        undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, accounting);

      const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", force: true }) as {
        results: { semantic: { error?: string } };
      };

      expect(result.results.semantic.error).toContain("malformed fact output");
      expect(attempts).toHaveBeenCalled();
      expect(attempts.mock.calls[0][1]).toMatchObject({ workflow: "consolidation", functionId: "mem::consolidate-pipeline", phase: "generation" });
      expect(attempts.mock.calls[0][1].unitId).toMatch(/^conrequest_/);
      expect(await kv.list(KV.semantic)).toHaveLength(0);
      expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
    },
  );

  it("pipeline skips procedural when fewer than 2 patterns", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    const mem: Memory = {
      ...makePattern(1),
      sessionIds: ["ses_1", "ses_2"],
    };
    await kv.set("mem:memories", "mem_1", mem);

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "procedural",
    })) as { success: boolean; results: Record<string, unknown> };

    expect(result.success).toBe(true);
    const procedural = result.results.procedural as { skipped: boolean; reason: string };
    expect(procedural.skipped).toBe(true);
    expect(procedural.reason).toContain("fewer than 2");
  });

  it("with enough summaries, creates semantic memories from provider response", async () => {
    const accounting = new WorkflowAttemptAccounting([], []); const attempts = vi.spyOn(accounting, "run");
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(async (_system: string, prompt: string) => fragmentIds(prompt).length
        ? (fragmentIds(prompt).filter((id) => id === "ses_0" || id === "ses_1").length
          ? `<facts><fact confidence="0.9" sourceIds="${fragmentIds(prompt).filter((id) => id === "ses_0" || id === "ses_1").join(",")}">TypeScript is the primary language</fact></facts>` : "<facts/>")
        : `<facts><fact confidence="0.9" candidateIds="${candidateIdsFromPrompt(prompt).join(",")}">TypeScript is the primary language</fact></facts>`),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never,
      undefined, undefined, undefined, undefined, undefined, undefined, undefined, undefined, accounting);

    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, makeSummary(i));
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { success: boolean; results: Record<string, unknown> };

    expect(result.success).toBe(true);
    const semantic = result.results.semantic as { newFacts: number };
    expect(semantic.newFacts).toBe(1);
    const identities = attempts.mock.calls.map(call => call[1]);
    expect(identities.some(value => value.phase === "generation")).toBe(true);
    expect(identities.some(value => value.phase === "publication")).toBe(true);
    expect(identities.every(value => value.functionId === "mem::consolidate-pipeline")).toBe(true);
    const generation = identities.find(value => value.phase === "generation")!;
    const publication = identities.find(value => value.phase === "publication")!;
    expect(generation.jobId).toBe(publication.jobId);
    expect(generation.unitId).not.toBe(publication.unitId);

    const stored = await kv.list<SemanticMemory>("mem:semantic");
    expect(stored.length).toBe(1);
    expect(stored[0].fact).toBe("TypeScript is the primary language");
    expect(stored[0].confidence).toBe(0.9);
    expect(stored[0].sourceSessionIds).toEqual(["ses_0", "ses_1"]);
  });

  it("bounds UTF-8 semantic requests and merges cross-partition evidence with exact provenance", async () => {
    const requests: Array<{ systemPrompt: string; userPrompt: string }> = [];
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (systemPrompt: string, userPrompt: string) => {
        requests.push({ systemPrompt, userPrompt });
        const source = fragmentIds(userPrompt)[0];
        if (promptBody(userPrompt).startsWith("Analyze these original source fragments")) {
          return source === "ses_0" || source === "ses_5"
            ? `<facts><fact confidence="0.8" sourceIds="${source}">Shared retention fact</fact></facts>`
            : "<facts></facts>";
        }
        const candidateIds = candidateIdsFromPrompt(userPrompt).join(",");
        return `<facts><fact confidence="0.95" candidateIds="${candidateIds}">Shared retention fact</fact></facts>`;
      }),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, {
        ...makeSummary(i),
        narrative: "é".repeat(2_500),
      });
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { newFacts: number } } };

    expect((result.results.semantic as unknown as { completed: boolean }).completed).toBe(true);
    expect(requests.length).toBeGreaterThan(2);
    expect(requests.every(({ systemPrompt, userPrompt }) =>
      new TextEncoder().encode(JSON.stringify({ systemPrompt, userPrompt })).byteLength <= 8 * 1024,
    )).toBe(true);
    const inputSources = [...new Set(requests.flatMap(({ userPrompt }) => fragmentIds(userPrompt)))].sort();
    expect(inputSources).toEqual(["ses_0", "ses_1", "ses_2", "ses_3", "ses_4", "ses_5"]);

    const stored = await kv.list<SemanticMemory>(KV.semantic);
    expect(stored).toHaveLength(1);
    expect(stored[0].fact).toBe("Shared retention fact");
    expect(stored[0].sourceSessionIds).toEqual(["ses_0", "ses_5"]);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it.each(["<facts></facts>", "malformed response"])("distinguishes an empty final reduction from malformed output: %s", async (finalResponse) => {
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (_systemPrompt: string, userPrompt: string) => {
        const source = fragmentIds(userPrompt)[0];
        if (promptBody(userPrompt).startsWith("Analyze these original source fragments")) {
          if (finalResponse === "<facts></facts>") return "<facts/>";
          return `<facts><fact confidence="0.6" sourceIds="${source}">Single episode detail</fact></facts>`;
        }
        return finalResponse;
      }),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, {
        ...makeSummary(i),
        narrative: "é".repeat(2_500),
      });
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { error?: string; newFacts?: number } } };

    expect(await kv.list<SemanticMemory>(KV.semantic)).toHaveLength(0);
    if (finalResponse === "<facts></facts>") {
      expect((result.results.semantic as unknown as { completed: boolean }).completed).toBe(true);
      expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toMatchObject({
        processedThrough: expect.any(String),
      });
    } else {
      expect(result.results.semantic.error).toContain("malformed fact output");
      expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
    }
  });

  it("leaves checkpoint untouched when a later bounded request fails, then retries all sources", async () => {
    const evidenceSources: string[] = [];
    let failOneRequest = true;
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (_systemPrompt: string, userPrompt: string) => {
        const source = fragmentIds(userPrompt)[0];
        if (promptBody(userPrompt).startsWith("Analyze these original source fragments")) {
          evidenceSources.push(source!);
          if (failOneRequest && source === "ses_2") {
            failOneRequest = false;
            throw Object.assign(new Error("HTTP 413 Payload Too Large"), { status: 413 });
          }
          return `<facts><fact confidence="0.8" sourceIds="${source}">Shared retention fact</fact></facts>`;
        }
        const candidateIds = candidateIdsFromPrompt(userPrompt).join(",");
        return `<facts><fact confidence="0.95" candidateIds="${candidateIds}">Shared retention fact</fact></facts>`;
      }),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, {
        ...makeSummary(i),
        narrative: "é".repeat(2_500),
      });
    }

    const failed = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { error: string } } };
    expect(failed.results.semantic.error).toContain("HTTP 413");
    expect(await kv.list<SemanticMemory>(KV.semantic)).toHaveLength(0);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();

    const retried = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { newFacts: number } } };
    expect((retried.results.semantic as unknown as { completed: boolean }).completed).toBe(true);
    expect(evidenceSources.filter((id) => id === "ses_0")).toHaveLength(1);
    expect(evidenceSources).toContain("ses_5");
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it("losslessly partitions an oversized UTF-8 source with exact provenance and contiguous ranges", () => {
    const source = { id: "ses_large", version: "revision-7", fields: { narrative: 'é😀"\\n'.repeat(6000), title: "Title" } };
    const fragments = fragmentSources([source], SEMANTIC_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt);
    expect(Buffer.byteLength(source.fields.narrative)).toBeGreaterThan(8192);
    expect(fragments.length).toBeGreaterThan(2);
    for (const [field, text] of Object.entries(source.fields)) {
      const selected = fragments.filter((fragment) => fragment.field === field);
      expect(selected.map((fragment) => fragment.text).join("")).toBe(text);
      let offset = 0;
      for (const fragment of selected) {
        expect(fragment.start).toBe(offset);
        expect(fragment.text).toBe(text.slice(fragment.start, fragment.end));
        expect(fragment).toMatchObject({ sourceId: source.id, version: source.version,
          fingerprint: fingerprintId("consource", JSON.stringify(source)), count: fragments.length });
        expect(fragment.text).not.toMatch(/^[\uDC00-\uDFFF]|[\uD800-\uDBFF]$/);
        expect(requestInputBytes(SEMANTIC_FRAGMENT_SYSTEM,
          `Consolidation manifest: conmanifest_0123456789abcdef\\n\\n${buildConsolidationFragmentPrompt([fragment])}`)).toBeLessThanOrEqual(8192);
        offset = fragment.end;
      }
      expect(offset).toBe(text.length);
    }
    expect(fragments.map((fragment) => fragment.ordinal)).toEqual(fragments.map((_, i) => i + 1));
  });

  it("fails closed when a split semantic response omits source IDs", async () => {
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async () => '<facts><fact confidence="0.8">Unattributed fact</fact></facts>'),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, {
        ...makeSummary(i),
        narrative: "é".repeat(2_500),
      });
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { error: string } } };

    expect(result.results.semantic.error).toContain("omitted source IDs");
    expect(await kv.list<SemanticMemory>(KV.semantic)).toHaveLength(0);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
  });

  it("fails closed when a candidate reducer drops a fact while preserving its source IDs", async () => {
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (_systemPrompt: string, userPrompt: string) => {
        const source = fragmentIds(userPrompt)[0];
        if (promptBody(userPrompt).startsWith("Analyze these original source fragments")) {
          return `<facts>${Array.from({ length: 20 }, (_, index) =>
            `<fact confidence="0.8" sourceIds="${source}">Fact ${source} ${index}</fact>`,
          ).join("")}</facts>`;
        }

        const candidates = candidatesFromPrompt(userPrompt);
        return `<facts>${candidates.slice(1).map((candidate) =>
          `<fact confidence="0.8" candidateIds="${candidate.candidateIds.join(",")}">${candidate.fact}</fact>`,
        ).join("")}</facts>`;
      }),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, {
        ...makeSummary(i),
        narrative: "é".repeat(2_500),
      });
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { error: string } } };

    expect(result.results.semantic.error).toContain("omitted candidate facts");
    expect(await kv.list<SemanticMemory>(KV.semantic)).toHaveLength(0);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
  });

  it("waits for enough new summaries after its initial semantic checkpoint", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(async (_system: string, prompt: string) => fragmentIds(prompt).length
        ? `<facts><fact confidence="0.9" sourceIds="${fragmentIds(prompt).join(",")}">TypeScript is the primary language</fact></facts>`
        : `<facts><fact confidence="0.9" candidateIds="${candidateIdsFromPrompt(prompt).join(",")}">TypeScript is the primary language</fact></facts>`),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 5; i++) {
      await kv.set("mem:summaries", `ses_${i}`, makeSummary(i));
    }

    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic" });
    const capturedCalls = provider.summarize.mock.calls.length;
    expect(capturedCalls).toBeGreaterThan(0);

    await kv.set("mem:summaries", "ses_new", {
      ...makeSummary(20),
      sessionId: "ses_new",
      createdAt: new Date(Date.now() + 86400000).toISOString(),
    });
    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { skipped?: boolean } } };

    expect(result.results.semantic).toMatchObject({ completed: true, reason: "fewer than required new summaries" });
    expect(provider.summarize).toHaveBeenCalledTimes(capturedCalls);
  });

  it("with enough patterns, creates procedural memories from provider response", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn().mockResolvedValue(
        `<procedures><procedure name="Test Workflow" trigger="when writing tests"><step>Create test file</step><step>Write assertions</step></procedure></procedures>`,
      ),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    for (let i = 0; i < 3; i++) {
      await kv.set("mem:memories", `mem_${i}`, makePattern(i));
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "procedural",
    })) as { success: boolean; results: Record<string, unknown> };

    expect(result.success).toBe(true);
    const procedural = result.results.procedural as { newProcedures: number };
    expect(procedural.newProcedures).toBe(1);

    const stored = await kv.list<ProceduralMemory>("mem:procedural");
    expect(stored.length).toBe(1);
    expect(stored[0].name).toBe("Test Workflow");
    expect(stored[0].steps.length).toBe(2);
    expect(stored[0].triggerCondition).toBe("when writing tests");
  });

  it("consolidation records an audit entry", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic" });

    expect(await kv.list(KV.audit)).toHaveLength(0);
  });

  it("pipeline returns early when consolidation is disabled", async () => {
    vi.mocked(isConsolidationEnabled).mockReturnValue(false);
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    const result = (await sdk.trigger("mem::consolidate-pipeline", {})) as {
      success: boolean;
      skipped?: boolean;
      reason?: string;
    };

    expect(result.success).toBe(false);
    expect(result.skipped).toBe(true);
    expect(result.reason).toContain("CONSOLIDATION_ENABLED");
    expect(provider.summarize).not.toHaveBeenCalled();
    vi.mocked(isConsolidationEnabled).mockReturnValue(true);
  });

  it("pipeline proceeds with force=true even when consolidation is disabled", async () => {
    sdk.registerFunction("mem::reflect", async () => ({ success: true }));
    vi.mocked(isConsolidationEnabled).mockReturnValue(false);
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      force: true,
    })) as { success: boolean; results: Record<string, unknown> };

    expect(result.success).toBe(true);
    expect(result.results).toBeDefined();
    vi.mocked(isConsolidationEnabled).mockReturnValue(true);
  });

  async function durableCallbacks(large = false) {
    for (let i = 0; i < 5; i++) await kv.set(KV.summaries, `ses_${i}`, { ...makeSummary(i), narrative: large ? "😀é".repeat(3000) : "Captured source" });
    const requests: import("../src/types.js").FireworksBatchRequest[] = [];
    const enqueue = async (request: import("../src/types.js").FireworksBatchRequest) => {
      requests.push(request);
      await kv.set(KV.fireworksBatchWorkItems, request.correlationId, { ...request, id: request.correlationId, customId: request.correlationId, state: "queued" });
      return { queued: true, workItemId: request.correlationId! };
    };
    const register = () => registerConsolidationPipelineFunction(sdk as never, kv as never, {} as never, undefined, undefined, { enqueue });
    register();
    const response = (request: import("../src/types.js").FireworksBatchRequest) => {
      const ids = fragmentIds(request.userPrompt);
      return ids.length ? `<facts><fact confidence="0.9" sourceIds="${ids.join(",")}">Recovered fact</fact></facts>`
        : `<facts><fact confidence="0.9" candidateIds="${candidateIdsFromPrompt(request.userPrompt).join(",")}">Recovered fact</fact></facts>`;
    };
    const deliver = (request: import("../src/types.js").FireworksBatchRequest, text = response(request)) => sdk.trigger("mem::consolidate-pipeline", {
      consolidationManifestId: request.metadata!.consolidationManifestId, consolidationPartitionId: request.metadata!.consolidationPartitionId, batchResponse: text,
    }) as Promise<{ completed: boolean; stale?: boolean; error?: string }>;
    async function finish() {
      const done = new Set<string>();
      let result;
      for (let step = 0; step < 1000; step++) {
        const request = requests.find((item) => !done.has(item.correlationId!));
        if (!request) return result!;
        done.add(request.correlationId!); result = await deliver(request);
      }
      throw new Error("Bounded test delivery exhausted");
    }
    return { requests, register, response, deliver, finish };
  }

  it("rejects an unknown manifest callback without changing its captured request or watermark", async () => {
    const q = await durableCallbacks();
    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true });
    const first = q.requests[0]; const partition = first.metadata!.consolidationPartitionId;
    const before = structuredClone(await kv.get(KV.state, partition));
    await expect(sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: "conmanifest_wrong",
      consolidationPartitionId: partition, batchResponse: "<facts/>" })).rejects.toThrow("Unknown consolidation partition");
    expect(await kv.get(KV.state, partition)).toEqual(before);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
  });

  it("retains originals and the watermark when sources change before callback effects", async () => {
    const q = await durableCallbacks();
    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true });
    await kv.set(KV.summaries, "ses_0", { ...makeSummary(0), narrative: "Changed after admission" });
    expect(await q.finish()).toMatchObject({ stale: true, completed: false });
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
    expect(await kv.list(KV.semantic)).toHaveLength(0);
  });

  it("preserves the first provider capture on conflicting callback replay", async () => {
    const q = await durableCallbacks();
    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true });
    const first = q.requests[0]; await q.deliver(first);
    await expect(q.deliver(first, "<facts/>")).rejects.toThrow("Conflicting consolidation provider result");
    expect(await kv.get(KV.state, first.metadata!.consolidationPartitionId)).toMatchObject({ status: "completed", responsePages: { bytes: Buffer.byteLength(q.response(first)) } });
  });

  it("resumes after effects but before manifest completion without duplicating reinforcement", async () => {
    const q = await durableCallbacks();
    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true });
    const originalSet = kv.set; let crash = true;
    vi.spyOn(kv, "set").mockImplementation(async (scope, key, value) => {
      if (scope === KV.state && (value as { schema?: number; phase?: string }).schema === 2 && (value as { phase?: string }).phase === "completed" && crash) {
        crash = false; throw new Error("crash after effects");
      }
      return originalSet(scope, key, value);
    });
    expect((await q.finish()).completed).toBe(false);
    expect(await kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeNull();
    expect((await kv.list<SemanticMemory>(KV.semantic))[0].accessCount).toBe(1);
    q.register();
    expect((await q.deliver(q.requests[0])).completed).toBe(true);
    expect((await kv.list<SemanticMemory>(KV.semantic))[0].accessCount).toBe(1);
  });

  it.each(["dead-letter", "completed"])("migrates oversized legacy replacements while preserving a %s sibling and the original watermark", async (state) => {
    const q = await durableCallbacks(true);
    const cohort = "fwbcohort_large";
    const checkpoint = { processedThrough: "2026-09-11T00:00:00.000Z", processedSessionIdsAtThrough: ["original-last"] };
    await kv.set(KV.state, cohort, { workItemIds: ["old", "sibling"], checkpoint });
    await kv.set(KV.fireworksBatchWorkItems, "sibling", { id: "sibling", state });
    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true, replacementOf: "old", batchCohort: cohort, batchSourceIds: ["ses_0"] });
    const migrated = await kv.get<{ workItemIds: string[]; checkpoint: unknown }>(KV.state, cohort);
    expect(migrated!.workItemIds[0]).toMatch(/^conmanifest_/);
    expect(migrated!.checkpoint).toEqual(checkpoint);
    expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
    expect((await q.finish()).completed).toBe(true);
    expect(await kv.get(KV.state, "semantic-consolidation")).toEqual(state === "completed" ? checkpoint : null);
  });

});

describe("bounded project consolidation selection", () => {
  async function sources() {
    const kv = Object.assign(mockKV(), { indexedRetrieval: true });
    const summary = makeSummary(0);
    await kv.set(KV.summaries, summary.sessionId, summary);
    const observations = [0, 1].map(i => ({ id: `obs_${i}`, sessionId: summary.sessionId,
      timestamp: "2026-10-01T00:00:00.000Z", type: "decision", title: "Repeatable setup",
      narrative: `Install dependency and validate configuration ${i}`, facts: [], concepts: ["setup"], files: [], importance: 7 }));
    for (const observation of observations) await kv.set(KV.observations(summary.sessionId), observation.id, observation);
    const hits = observations.map(observation => ({ id: observation.id, rowVersion: "1", sessionId: summary.sessionId }));
    return { kv, summary, hits };
  }

  it("uses canonical legacy IDs and rejects an incomplete index", async () => {
    const lookup = vi.fn(async () => ({ status: "ready" as const, canonicalId: "legacy_random_id", version: "7" }));
    expect(await canonicalConsolidationId(lookup, "semantic", " Fact ")).toEqual({ status: "ready", canonicalId: "legacy_random_id", version: "7" });
    expect(lookup).toHaveBeenCalledWith("semantic", " fact ");
    await expect(canonicalConsolidationId(async () => ({ status: "preparing", continuation: "cursor" }), "procedural", "Setup"))
      .rejects.toThrow("consolidation_canonical_index_preparing");
  });

  it("requires exact project summaries before accepting relevant evidence", async () => {
    const { kv, summary, hits } = await sources();
    await expect(selectConsolidationSummaries(kv as never, "other-project", [summary.sessionId]))
      .rejects.toThrow("selected_consolidation_source_identity_mismatch");
    const sdk = mockSdk();
    registerConsolidationPipelineFunction(sdk as never, kv as never, {} as never, undefined, undefined, undefined, undefined, async () => hits);
    await expect(sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "other-project", batchSourceIds: [summary.sessionId], force: true }))
      .resolves.toMatchObject({ success: false, completed: false, error: "selected_consolidation_source_identity_mismatch" });
    const selected = await selectProceduralObservations(kv as any, summary.project, [summary], hits as any);
    expect(selected).toHaveLength(2);
    expect(selected.every(source => source.sourceKind === "observation" && source.project === summary.project && source.rowVersion === "1")).toBe(true);
    expect(await selectProceduralObservations(kv as any, summary.project, [summary], hits.slice(0, 1) as any)).toHaveLength(1);
  });

  it("selects one catalog project before search and advances the durable cursor only after completion", async () => {
    const { kv, summary, hits } = await sources();
    const retrieval = vi.fn(async () => ({ status: "ready", items: [{ project: summary.project }], has_more: true, next_cursor: "next-project" }));
    Object.assign(kv, { retrieval });
    const select = vi.fn(async () => hits as any);
    const sdk = mockSdk();
    const reflect = vi.fn(async () => ({ success: true, completed: true }));
    sdk.registerFunction("mem::reflect", reflect);
    registerConsolidationPipelineFunction(sdk as any, kv as any, {} as any, undefined, undefined, undefined, undefined, select);
    const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "reflect" });
    expect(result.completed).toBe(true);
    expect(retrieval).toHaveBeenCalledWith({ action: "project_catalog_page", cursor: undefined, limit: 1, max_bytes: 16_384 });
    expect(select).toHaveBeenCalledWith(summary.project);
    expect(reflect.mock.calls[0][0].project).toBe(summary.project);
    expect(await kv.get(KV.config, "consolidation-project-cursor")).toEqual({ cursor: "next-project" });
  });

  it("retains captured provider output while a canonical write is preparing", async () => {
    const { kv, summary, hits } = await sources();
    const sdk = mockSdk();
    const provider = { summarize: vi.fn(async () => '<procedures><procedure name="Setup" trigger="New project"><step>Install</step><step>Validate</step></procedure></procedures>') };
    const lookup = async () => ({ status: "ready" as const, canonicalId: null, version: "1" });
    let preparing = true;
    registerConsolidationPipelineFunction(sdk as any, kv as any, provider as any, undefined, undefined, undefined, undefined,
      async () => hits as any, lookup, undefined, async (scope, _key, _match, id, effect, update) => {
        if (preparing) { preparing = false; throw Object.assign(new Error("preparing"), { code: "STATE_CANONICAL_PREPARING" }); }
        await applyBatchEffect(kv as any, KV[scope], id, effect, update);
        return { applied: true, created: true, id, rowVersion: "2", receipt: { operationKind: "upsert" as const,
          effectKey: effect, operationFingerprint: "capture", id, rowVersion: "2", created: true, resultFingerprint: "result" } };
      });
    const first = await sdk.trigger("mem::consolidate-pipeline", { tier: "procedural", project: summary.project });
    expect(first.queued).toBe(true);
    expect(first.completed).toBe(false);
    expect((await loadRoot(kv as never, first.results.procedural.manifestId))?.phase).toBe("publish");
    const second = await sdk.trigger("mem::consolidate-pipeline", { tier: "procedural", project: summary.project });
    expect(second.completed).toBe(true);
    expect(second.results.procedural.newProcedures).toBe(1);
    expect(provider.summarize).toHaveBeenCalledTimes(2);
  });

  it.each([false, true])("persists reusable procedures and rejects nonsensical output before any effect: %s", async malformed => {
    const { kv, summary, hits } = await sources();
    const sdk = mockSdk();
    const valid = '<procedure name="Setup" trigger="New project"><step>Install dependency</step><step>Validate configuration</step></procedure>';
    const provider = { summarize: vi.fn(async () => `<procedures>${valid}${malformed ? '<procedure name="Bad" trigger=""><step>One</step></procedure>' : ''}</procedures>`) };
    const lookup = vi.fn(async () => ({ status: "ready" as const, canonicalId: null, version: "1" }));
    registerConsolidationPipelineFunction(sdk as any, kv as any, provider as any, undefined, undefined, undefined, undefined,
      async () => hits as any, lookup, undefined,
      async (scope, _key, match, id, effect, update) => {
        expect(match.version).toBe("1");
        await applyBatchEffect(kv as any, KV[scope], match.canonicalId ?? id, effect, update);
        return { applied: true, created: true, id, rowVersion: "2", receipt: { operationKind: "upsert" as const,
          effectKey: effect, operationFingerprint: "captured-input", id, rowVersion: "2", created: true, resultFingerprint: "result" } };
      });
    const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "procedural", project: summary.project });
    expect(result.success).toBe(!malformed);
    const procedures = await kv.list<ProceduralMemory>(KV.procedural);
    expect(procedures).toHaveLength(malformed ? 0 : 1);
    if (!malformed) {
      expect(result.results.procedural.newProcedures).toBe(1);
      expect(procedures[0].steps).toEqual(["Install dependency", "Validate configuration"]);
      expect(procedures[0].sourceSessionIds).toEqual([summary.sessionId]);
      const replay = await sdk.trigger("mem::consolidate-pipeline", { tier: "procedural", project: summary.project });
      expect(replay.results.procedural.newProcedures).toBe(0);
      expect(replay.results.procedural.createdProcedures).toBe(1);
      expect(replay.results.procedural.appliedEffects).toBe(0);
      expect((await kv.list<ProceduralMemory>(KV.procedural))[0].frequency).toBe(1);
      expect(provider.summarize).toHaveBeenCalledTimes(2);
    }
  });
});
