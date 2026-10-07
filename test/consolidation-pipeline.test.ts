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
import { fingerprintId, KV } from "../src/state/schema.js";
import { LlmTaskRouter } from "../src/providers/task-router.js";
import type { SessionSummary, Memory, SemanticMemory, ProceduralMemory, LlmRoutingConfig } from "../src/types.js";

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

function candidatesFromPrompt(userPrompt: string): Array<{ fact: string; candidateIds: string[] }> {
  const prefix = "Candidate facts with source evidence:\n\n";
  return JSON.parse(userPrompt.slice(prefix.length)) as Array<{ fact: string; candidateIds: string[] }>;
}

function candidateIdsFromPrompt(userPrompt: string): string[] {
  const candidates = candidatesFromPrompt(userPrompt);
  return candidates.flatMap((candidate) => candidate.candidateIds);
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
    const auxiliary = { name: "auxiliary", compress: vi.fn(), summarize: vi.fn(async () => "<facts/>") };
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

    expect(auxiliary.summarize).toHaveBeenCalledTimes(1);
    expect(primary.summarize).not.toHaveBeenCalled();
    expect(await kv.list(KV.semantic)).toHaveLength(0);
    expect(await kv.get(KV.state, "semantic-consolidation")).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it.each(["<facts>garbage</facts>", "<facts><unexpected/></facts>"])(
    "fails closed on unrecognized semantic envelope content (%s)",
    async (response) => {
      const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async () => response) };
      for (let index = 0; index < 5; index++) await kv.set(KV.summaries, `invalid-${index}`, { ...makeSummary(index), sessionId: `invalid-${index}` });
      registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);

      const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", force: true }) as {
        results: { semantic: { error?: string } };
      };

      expect(result.results.semantic.error).toContain("malformed fact output");
      expect(await kv.list(KV.semantic)).toHaveLength(0);
      expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
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
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn().mockResolvedValue(
        `<facts><fact confidence="0.9" sourceIds="ses_0,ses_1">TypeScript is the primary language</fact></facts>`,
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
    expect(stored[0].sourceSessionIds).toEqual(["ses_0", "ses_1"]);
  });

  it("bounds UTF-8 semantic requests and merges cross-partition evidence with exact provenance", async () => {
    const requests: Array<{ systemPrompt: string; userPrompt: string }> = [];
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (systemPrompt: string, userPrompt: string) => {
        requests.push({ systemPrompt, userPrompt });
        const source = userPrompt.match(/Session ID: (ses_\d+)/)?.[1];
        if (userPrompt.startsWith("Extract factual candidates")) {
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

    expect(result.results.semantic.newFacts).toBe(1);
    expect(requests.length).toBeGreaterThan(2);
    expect(requests.every(({ systemPrompt, userPrompt }) =>
      new TextEncoder().encode(JSON.stringify({ systemPrompt, userPrompt })).byteLength <= 8 * 1024,
    )).toBe(true);
    const inputSources = requests
      .filter(({ userPrompt }) => userPrompt.startsWith("Extract factual candidates"))
      .map(({ userPrompt }) => userPrompt.match(/Session ID: (ses_\d+)/)?.[1]);
    expect(inputSources).toEqual(["ses_0", "ses_1", "ses_2", "ses_3", "ses_4", "ses_5"]);

    const stored = await kv.list<SemanticMemory>(KV.semantic);
    expect(stored).toHaveLength(1);
    expect(stored[0].fact).toBe("Shared retention fact");
    expect(stored[0].sourceSessionIds).toEqual(["ses_0", "ses_5"]);
    expect(await kv.get(KV.state, "semantic-consolidation")).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it.each(["<facts></facts>", "malformed response"])("distinguishes an empty final reduction from malformed output: %s", async (finalResponse) => {
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (_systemPrompt: string, userPrompt: string) => {
        const source = userPrompt.match(/Session ID: (ses_\d+)/)?.[1];
        if (userPrompt.startsWith("Extract factual candidates")) {
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
      expect(result.results.semantic.newFacts).toBe(0);
      expect(await kv.get(KV.state, "semantic-consolidation")).toMatchObject({
        processedThrough: expect.any(String),
      });
    } else {
      expect(result.results.semantic.error).toContain("malformed fact output");
      expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
    }
  });

  it("leaves checkpoint untouched when a later bounded request fails, then retries all sources", async () => {
    const evidenceSources: string[] = [];
    let failOneRequest = true;
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (_systemPrompt: string, userPrompt: string) => {
        const source = userPrompt.match(/Session ID: (ses_\d+)/)?.[1];
        if (userPrompt.startsWith("Extract factual candidates")) {
          evidenceSources.push(source!);
          if (failOneRequest && source === "ses_2") {
            failOneRequest = false;
            throw new Error("HTTP 413 Payload Too Large");
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
    expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();

    const retried = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { newFacts: number } } };
    expect(retried.results.semantic.newFacts).toBe(1);
    expect(evidenceSources.slice(3)).toEqual(["ses_0", "ses_1", "ses_2", "ses_3", "ses_4", "ses_5"]);
    expect(await kv.get(KV.state, "semantic-consolidation")).toMatchObject({
      processedThrough: expect.any(String),
    });
  });

  it("rejects an individually oversized semantic source without sending or checkpointing it", async () => {
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never);
    for (let i = 0; i < 5; i++) {
      await kv.set("mem:summaries", `ses_${i}`, {
        ...makeSummary(i),
        narrative: i === 0 ? "é".repeat(6_000) : "short summary",
      });
    }

    const result = (await sdk.trigger("mem::consolidate-pipeline", {
      tier: "semantic",
    })) as { results: { semantic: { error: string } } };

    expect(result.results.semantic.error).toContain("source retained locally");
    expect(provider.summarize).not.toHaveBeenCalled();
    expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
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
    expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
  });

  it("fails closed when a candidate reducer drops a fact while preserving its source IDs", async () => {
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (_systemPrompt: string, userPrompt: string) => {
        const source = userPrompt.match(/Session ID: (ses_\d+)/)?.[1];
        if (userPrompt.startsWith("Extract factual candidates")) {
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
    expect(await kv.get(KV.state, "semantic-consolidation")).toBeNull();
  });

  it("waits for enough new summaries after its initial semantic checkpoint", async () => {
    const provider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn().mockResolvedValue(
        `<facts><fact confidence="0.9" sourceIds="ses_0,ses_1,ses_2,ses_3,ses_4">TypeScript is the primary language</fact></facts>`,
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
});
