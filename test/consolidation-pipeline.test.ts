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

import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { isConsolidationEnabled } from "../src/config.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import { KV } from "../src/state/schema.js";
import type { SessionSummary, Memory, SemanticMemory, ProceduralMemory } from "../src/types.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      return (store.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
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
      return fn(payload);
    },
  };
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

describe("Consolidation Pipeline", () => {
  let sdk: ReturnType<typeof mockSdk>;
  let kv: ReturnType<typeof mockKV>;

  beforeEach(() => {
    sdk = mockSdk();
    kv = mockKV();
  });

  it("queues all bounded semantic partitions with current source IDs and a durable cohort", async () => {
    const requests: import("../src/types.js").FireworksBatchRequest[] = [];
    const enqueue = vi.fn(async (request: import("../src/types.js").FireworksBatchRequest) => {
      requests.push(request); return { queued: true, workItemId: `fresh-${requests.length}` };
    });
    const provider = { summarize: vi.fn(), compress: vi.fn() };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, { enqueue });
    for (let i = 0; i < 5; i++) await kv.set("mem:summaries", `ses_${i}`, { ...makeSummary(i), narrative: "x".repeat(5000) });
    const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true, replacementOf: "old" });
    expect(result.success).toBe(true);
    expect(requests.length).toBeGreaterThan(1);
    expect(requests.flatMap((r) => JSON.parse(r.metadata!.sourceIds)).sort()).toEqual(["ses_0", "ses_1", "ses_2", "ses_3", "ses_4"]);
    for (const request of requests) {
      expect(request.userPrompt.length).toBeLessThanOrEqual(10000);
      expect(request.metadata?.sourceFingerprint).toBeTruthy();
      expect(request.replacementOf).toBe("old");
    }
    expect(await kv.get("mem:state", requests[0].metadata!.cohort)).toMatchObject({ workItemIds: requests.map((_, i) => `fresh-${i + 1}`) });
    expect(provider.summarize).not.toHaveBeenCalled();
    for (let i = 0; i < requests.length; i++) await kv.set(KV.fireworksBatchWorkItems, `fresh-${i + 1}`, { id: `fresh-${i + 1}`, state: "polling" });
    for (let i = requests.length - 1; i >= 0; i--) {
      const metadata = requests[i].metadata!;
      await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", force: true,
        batchResponse: '<fact confidence="0.8">Bounded fact</fact>', batchEffectKey: batchEffectKey(`fresh-${i + 1}`),
        batchSourceFingerprint: metadata.sourceFingerprint, batchSourceIds: JSON.parse(metadata.sourceIds), batchCohort: metadata.cohort });
      if (i > 0) expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
      await kv.set(KV.fireworksBatchWorkItems, `fresh-${i + 1}`, { id: `fresh-${i + 1}`, state: "completed" });
    }
    expect(await kv.get(KV.state, "semantic-consolidation")).toMatchObject({ processedThrough: expect.any(String) });
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
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn().mockResolvedValue(
        `<facts><fact confidence="0.9">TypeScript is the primary language</fact></facts>`,
      ),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

    for (let i = 0; i < 6; i++) {
      await kv.set("mem:summaries", `ses_${i}`, makeSummary(i));
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { success: boolean; results: Record<string, unknown> };

    expect(result.success).toBe(true);
    const semantic = result.results.semantic as { newFacts: number };
    expect(semantic.newFacts).toBe(1);

    const stored = await kv.list<SemanticMemory>("mem:semantic");
    expect(stored.length).toBe(1);
    expect(stored[0].fact).toBe("TypeScript is the primary language");
    expect(stored[0].confidence).toBe(0.9);
  });

  it("waits for enough new summaries after its initial semantic checkpoint", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn().mockResolvedValue(
        `<facts><fact confidence="0.9">TypeScript is the primary language</fact></facts>`,
      ),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 5; i++) {
      await kv.set("mem:summaries", `ses_${i}`, makeSummary(i));
    }

    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic" });
    expect(provider.summarize).toHaveBeenCalledTimes(1);

    await kv.set("mem:summaries", "ses_new", {
      ...makeSummary(20),
      sessionId: "ses_new",
      createdAt: new Date(Date.now() + 86400000).toISOString(),
    });
    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { skipped?: boolean } } };

    expect(result.results.semantic.skipped).toBe(true);
    expect(provider.summarize).toHaveBeenCalledTimes(1);
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

    const audits = await kv.list("mem:audit");
    expect(audits.length).toBe(1);
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
});
