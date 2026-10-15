import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerObserveFunction } from "../src/functions/observe.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { pageKey } from "../src/functions/consolidation-manifest.js";
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { KV } from "../src/state/schema.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import type { ConsolidationCanonicalLookup, ConsolidationCanonicalWriter } from "../src/functions/consolidation-selection.js";
import type { CompressedObservation, SessionSummary } from "../src/types.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";
import { mockKV } from "./helpers/mocks.js";
import { effectHarness } from "./batch-effects-harness.js";

type FunctionHandler = (data: unknown) => Promise<unknown>;

function harness() {
  const handlers = new Map<string, FunctionHandler>();
  const sdk = {
    registerFunction(idOrOptions: string | { id: string }, handler: FunctionHandler) {
      handlers.set(typeof idOrOptions === "string" ? idOrOptions : idOrOptions.id, handler);
    },
    async trigger(idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      const handler = handlers.get(id);
      return handler ? handler(payload) : null;
    },
  };
  const kv = mockKV();
  void kv.set("mem:obs-identity:session-test", "__ready__", { version: 1, ready: true });
  const wired = installGraphStateWire(sdk as never, kv as never);
  const revisions = new Map<string, number>();
  const set = wired.kv.set.bind(wired.kv);
  const remove = wired.kv.delete.bind(wired.kv);
  wired.kv.set = async (scope: string, key: string, value: unknown) => {
    const result = await set(scope, key, value);
    revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
    return result;
  };
  wired.kv.delete = async (scope: string, key: string) => {
    await remove(scope, key);
    revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
  };
  Object.assign(wired.kv, {
    retrieval: async (request: Record<string, unknown>) => {
      if (request.action === "canonical_apply" && request.receipt_only === true) return { status: "not_applied" };
      throw new Error(`Unexpected retrieval fixture operation: ${request.action}`);
    },
    scopeRevision: async (scope: string) => ({
      generation: "00000000-0000-0000-0000-000000000001",
      revision: String(revisions.get(scope) ?? 0),
    }),
  });
  const graphTrigger = wired.sdk.trigger.bind(wired.sdk);
  wired.sdk.trigger = async (call: string | { function_id: string; payload: unknown }, data?: unknown) => {
    const functionId = typeof call === "string" ? call : call.function_id;
    const payload = typeof call === "string" ? data : call.payload;
    if (functionId === "state::observation_admit_v1") {
      const request = payload as { session_id: string; observation: CompressedObservation };
      const observation = request.observation;
      await wired.kv.set(KV.observations(request.session_id), observation.id, observation);
      return {
        created: true,
        duplicate_event: false,
        observation_id: observation.id,
        version: "1",
        occurrence_count: 1,
        observation,
      };
    }
    return graphTrigger(call as never, data as never);
  };
  return wired;
}

function selectSummaries(kv: ReturnType<typeof mockKV>) {
  return async (project: string) => (await kv.list<SessionSummary>(KV.summaries))
    .filter((summary) => summary.project === project)
    .map((summary) => ({ id: summary.sessionId, sessionId: summary.sessionId, rowVersion: "1" }));
}

function canonicalAccess(kv: ReturnType<typeof mockKV>) {
  const lookup: ConsolidationCanonicalLookup = async () => ({ status: "ready", version: "1", canonicalId: null });
  const write: ConsolidationCanonicalWriter = async (scope, key, match, proposedId, effect, update) => {
    const id = match.canonicalId ?? proposedId;
    const old = await kv.get<Record<string, unknown>>(KV[scope], id);
    const applied = await applyBatchEffect(kv as never, KV[scope], id, effect, update as never);
    return {
      applied: !(old?.appliedBatchEffects as string[] | undefined)?.includes(effect),
      created: !old,
      id,
      rowVersion: "1",
      receipt: {
        operationKind: "upsert",
        effectKey: effect,
        operationFingerprint: batchEffectKey(key),
        id,
        rowVersion: "1",
        created: !old,
        resultFingerprint: batchEffectKey(JSON.stringify(applied)),
      },
    };
  };
  return { lookup, write };
}

function choice(choice: string, confidence = 0.99) {
  return {
    type: "choice",
    choice,
    confidence,
    probabilities: { [choice]: confidence, other: 1 - confidence },
  };
}

function decisionProvider(overrides: {
  evaluate?: (feature: string, state: unknown, questions: Record<string, unknown>) => Promise<unknown>;
  evaluateChoice?: () => Promise<unknown>;
} = {}) {
  return {
    evaluate: vi.fn(overrides.evaluate ?? (async () => undefined)),
    evaluateChoice: vi.fn(overrides.evaluateChoice ?? (async () => choice("run"))),
  };
}

function semanticFixtureResponse(prompt: string): string {
  const candidatesMarker = "Candidate facts with source evidence:\n\n";
  if (prompt.includes(candidatesMarker)) {
    const candidates = JSON.parse(prompt.slice(prompt.indexOf(candidatesMarker) + candidatesMarker.length)) as Array<{ candidateIds: string[] }>;
    const candidateIds = [...new Set(candidates.flatMap((candidate) => candidate.candidateIds))];
    return `<facts><fact confidence="0.9" candidateIds="${candidateIds.join(",")}">A durable decision.</fact></facts>`;
  }
  const fragmentsMarker = "Analyze these original source fragments with their provenance:\n\n";
  const fragments = JSON.parse(prompt.slice(prompt.indexOf(fragmentsMarker) + fragmentsMarker.length)) as Array<{ sourceId: string }>;
  const sourceIds = [...new Set(fragments.map((fragment) => fragment.sourceId))];
  return `<facts><fact confidence="0.9" sourceIds="${sourceIds.join(",")}">A durable decision.</fact></facts>`;
}

function compressedObservation(
  id: string,
  overrides: Partial<CompressedObservation> = {},
): CompressedObservation {
  return {
    id,
    sessionId: "session-test",
    timestamp: "2020-01-01T00:00:00.000Z",
    type: "other",
    title: `Routine observation ${id}`,
    facts: [],
    narrative: "Routine public lookup with no durable project information.",
    concepts: [],
    files: [],
    importance: 5,
    ...overrides,
  };
}

function sessionSummary(sessionId: string, index: number): SessionSummary {
  return {
    sessionId,
    project: "test-project",
    createdAt: new Date(Date.now() - index * 86_400_000).toISOString(),
    title: `Routine summary ${index}`,
    narrative: "A routine lookup with no reusable fact or project change.",
    keyDecisions: [],
    filesModified: [],
    concepts: [],
    observationCount: 1,
  };
}

beforeEach(() => {
  vi.stubEnv("AGENTMEMORY_TYPESAFE_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_SCORING_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_AUTO_COMPRESS", "false");
  vi.stubEnv("CONSOLIDATION_ENABLED", "true");
  vi.stubEnv("GRAPH_EXTRACTION_ENABLED", "true");
});

afterEach(() => vi.unstubAllEnvs());

describe("TypeSafe pipeline decisions", () => {
  it("batches read-only admission and importance on a bounded privacy-processed state", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({
      evaluate: async () => ({
        admission: choice("keep"),
        importance: {
          type: "score",
          score: 7,
          confidence: 0.9,
          legend: {},
          probabilities: {},
        },
      }),
    });
    registerObserveFunction(sdk as never, kv as never, undefined, undefined, typeSafe as never);

    const result = await sdk.trigger("mem::observe", {
      sessionId: "session-test",
      hookType: "post_tool_use",
      timestamp: new Date().toISOString(),
      data: {
        tool_name: "Read",
        tool_input: { file_path: "src/example.ts" },
        tool_output: "x".repeat(200),
      },
    }) as { observationId: string };

    expect(typeSafe.evaluate).toHaveBeenCalledTimes(1);
    expect(typeSafe.evaluate.mock.calls[0]?.[0]).toBe("admission");
    const state = typeSafe.evaluate.mock.calls[0]?.[1] as { preview: string };
    expect(state.preview.length).toBeLessThanOrEqual(448);
    const stored = await kv.get<CompressedObservation>(KV.observations("session-test"), result.observationId);
    expect(stored?.importance).toBe(8);
  });

  it("never sends tool failures to TypeSafe admission", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider();
    registerObserveFunction(sdk as never, kv as never, undefined, undefined, typeSafe as never);

    const result = await sdk.trigger("mem::observe", {
      sessionId: "session-test",
      hookType: "post_tool_failure",
      timestamp: new Date().toISOString(),
      data: { tool_name: "Bash", error: "command failed" },
    });

    expect(result).toHaveProperty("observationId");
    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(await kv.list(KV.observations("session-test"))).toHaveLength(1);
  });

  it("never sends instruction files or detected secrets to TypeSafe", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider();
    registerObserveFunction(sdk as never, kv as never, undefined, undefined, typeSafe as never);

    for (const [filePath, output] of [
      ["AGENTS.md", "Always preserve user instructions."],
      ["README.md", "An accidental secret sk-proj-abcdefghijklmnopqrstuvwxyz1234567890."],
    ]) {
      await sdk.trigger("mem::observe", {
        sessionId: "session-test",
        hookType: "post_tool_use",
        timestamp: new Date().toISOString(),
        data: { tool_name: "Read", tool_input: { file_path: filePath }, tool_output: output },
      });
    }

    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(await kv.list(KV.observations("session-test"))).toHaveLength(2);
  });

  it("compacts low-value graph input while retaining original observations", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({
      evaluate: async (feature) => feature === "compaction"
        ? { candidate_0: choice("drop") }
        : undefined,
    });
    const provider = {
      name: "test",
      compress: vi.fn(async (_system: string, prompt: string) => {
        expect(prompt).not.toContain("compacted-out-observation");
        return '<entity type="concept" name="graph-gate-test" observations="1"/>';
      }),
    };
    registerGraphFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined, typeSafe as never);
    const observations = Array.from({ length: 7 }, (_, index) => compressedObservation(
      `obs-${index}`,
      index === 6 ? { title: "compacted-out-observation", timestamp: "2020-01-01T00:00:00.000Z" } : {},
    ));
    await kv.set(KV.sessions, "session-test", {
      id: "session-test", project: "test-project", cwd: "D:/test", startedAt: new Date().toISOString(),
      status: "active", observationCount: observations.length,
    });
    for (const observation of observations) {
      await kv.set(KV.observations(observation.sessionId), observation.id, observation);
    }

    const result = await sdk.trigger("mem::graph-extract", { sessionId: "session-test" });

    expect(result).toMatchObject({ success: true, nodesAdded: 1 });
    expect(typeSafe.evaluate).toHaveBeenCalledWith(
      "compaction",
      expect.objectContaining({ candidates: expect.any(Array) }),
      expect.any(Object),
    );
    expect(await kv.list(KV.observations("session-test"))).toHaveLength(7);
  });

  it("gates scheduled graph extraction with bounded state and retains its input", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({ evaluateChoice: async () => choice("skip") });
    const provider = { name: "test", compress: vi.fn() };
    registerGraphFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined, typeSafe as never);
    const observations = Array.from({ length: 7 }, (_, index) => compressedObservation(
      `graph-gate-${index}`,
      { timestamp: "2020-01-01T00:00:00.000Z" },
    ));

    const result = await sdk.trigger("mem::graph-extract", { observations, deferred: true });

    expect(result).toMatchObject({ success: true, skipped: true, observationsRetained: true });
    expect(typeSafe.evaluateChoice).toHaveBeenCalledWith(
      "pipelineGates",
      expect.stringContaining('"workflow":"graph-extraction"'),
      expect.any(String),
      expect.any(Object),
    );
    expect(typeSafe.evaluateChoice.mock.calls[0]?.[1].length).toBeLessThanOrEqual(512);
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("keeps complete automatic consolidation evidence and uses a distinct semantic judge without TypeSafe", async () => {
    const h = harness();
    const { sdk, kv } = h;
    const typeSafe = decisionProvider({
      evaluate: async (feature) => feature === "compaction"
        ? { candidate_0: choice("drop") }
        : undefined,
      evaluateChoice: async () => choice("run"),
    });
    const provider = {
      summarize: vi.fn(async (_system: string, prompt: string) => {
        if (_system.startsWith("You evaluate publication evidence.")) {
          const evidence = JSON.parse(prompt); expect(evidence.sources).toHaveLength(7);
          expect(prompt).toContain("compacted-out-summary");
          return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(_system)![1], verdict: "supported", reason: "The fixture candidate preserves all seven original records." });
        }
        return semanticFixtureResponse(prompt);
      }),
    };
    for (let index = 0; index < 7; index += 1) {
      const summary = sessionSummary(`session-${index}`, index);
      if (index === 6) summary.title = "compacted-out-summary";
      await kv.set(KV.summaries, summary.sessionId, summary);
      await kv.set(KV.sessions, summary.sessionId, {
        id: summary.sessionId, project: "test-project", cwd: "D:/test", startedAt: summary.createdAt,
        status: "completed", observationCount: summary.observationCount,
      });
    }
    Object.assign(kv, { indexedRetrieval: true });
    const canonical = canonicalAccess(kv);
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined,
      typeSafe as never, selectSummaries(kv), canonical.lookup, undefined, canonical.write);

    let result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "test-project", deferred: true });
    let manifestId = (result as { results?: { semantic?: { continuation?: { consolidationManifestId?: string } } } })
      .results?.semantic?.continuation?.consolidationManifestId;
    const progress: string[] = [];
    for (let step = 0; manifestId && step < 40; step += 1) {
      result = await sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: manifestId });
      const root = await kv.get<Record<string, unknown>>(KV.state, pageKey(manifestId, "root", 0));
      const response = result as { error?: unknown; phase?: unknown; completed?: unknown; queued?: unknown };
      progress.push(`${root?.phase ?? "missing"}:capture=${(root?.capture as { source?: number } | undefined)?.source ?? "?"}/${Array.isArray(root?.refs) ? root.refs.length : "?"},quality=${(root?.quality as { source?: number } | undefined)?.source ?? "?"},extract=${(root?.extract as { page?: number; offset?: number } | undefined)?.page ?? "?"}/${root?.fragmentPages ?? "?"}:${(root?.extract as { page?: number; offset?: number } | undefined)?.offset ?? "?"},eligible=${Array.isArray(root?.eligibleSources) ? root.eligibleSources.length : "?"},response=${String(response.error ?? response.phase ?? (response.completed ? "completed" : response.queued ? "queued" : "no-status"))}`);
      manifestId = (result as { continuation?: { consolidationManifestId?: string } }).continuation?.consolidationManifestId;
    }
    expect(manifestId, progress.join(" -> ")).toBeUndefined();

    expect(result).toMatchObject({ success: true });
    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(provider.summarize).toHaveBeenCalledTimes(9);
    expect(provider.summarize.mock.calls.filter(([system]) => system.startsWith("You evaluate publication evidence."))).toHaveLength(1);
    expect(await kv.list(KV.semantic)).toHaveLength(1);
  });

  it("preserves forced consolidation input without invoking TypeSafe", async () => {
    const h = harness();
    const { sdk, kv } = h;
    const typeSafe = decisionProvider();
    const provider = {
      summarize: vi.fn(async (_system: string, prompt: string) => {
        if (_system.startsWith("You evaluate publication evidence.")) {
          const evidence = JSON.parse(prompt); expect(evidence.sources).toHaveLength(7);
          expect(prompt).toContain("forced-summary");
          return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(_system)![1], verdict: "supported", reason: "The forced fixture candidate preserves all seven original records." });
        }
        return semanticFixtureResponse(prompt);
      }),
    };
    for (let index = 0; index < 7; index += 1) {
      const summary = sessionSummary(`forced-session-${index}`, index);
      if (index === 6) summary.title = "forced-summary";
      await kv.set(KV.summaries, summary.sessionId, summary);
      await kv.set(KV.sessions, summary.sessionId, {
        id: summary.sessionId, project: "test-project", cwd: "D:/test", startedAt: summary.createdAt,
        status: "completed", observationCount: summary.observationCount,
      });
    }
    Object.assign(kv, { indexedRetrieval: true });
    const canonical = canonicalAccess(kv);
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined,
      typeSafe as never, selectSummaries(kv), canonical.lookup, undefined, canonical.write);

    let result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "test-project", force: true });
    let manifestId = (result as { results?: { semantic?: { continuation?: { consolidationManifestId?: string } } } })
      .results?.semantic?.continuation?.consolidationManifestId;
    const progress: string[] = [];
    for (let step = 0; manifestId && step < 40; step += 1) {
      result = await sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: manifestId });
      const root = await kv.get<Record<string, unknown>>(KV.state, pageKey(manifestId, "root", 0));
      const response = result as { error?: unknown; phase?: unknown; completed?: unknown; queued?: unknown };
      progress.push(`${root?.phase ?? "missing"}:capture=${(root?.capture as { source?: number } | undefined)?.source ?? "?"}/${Array.isArray(root?.refs) ? root.refs.length : "?"},quality=${(root?.quality as { source?: number } | undefined)?.source ?? "?"},extract=${(root?.extract as { page?: number; offset?: number } | undefined)?.page ?? "?"}/${root?.fragmentPages ?? "?"}:${(root?.extract as { page?: number; offset?: number } | undefined)?.offset ?? "?"},eligible=${Array.isArray(root?.eligibleSources) ? root.eligibleSources.length : "?"},response=${String(response.error ?? response.phase ?? (response.completed ? "completed" : response.queued ? "queued" : "no-status"))}`);
      manifestId = (result as { continuation?: { consolidationManifestId?: string } }).continuation?.consolidationManifestId;
    }
    expect(manifestId, progress.join(" -> ")).toBeUndefined();

    expect(result).toMatchObject({ success: true });
    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(provider.summarize).toHaveBeenCalledTimes(9);
    expect(provider.summarize.mock.calls.filter(([system]) => system.startsWith("You evaluate publication evidence."))).toHaveLength(1);
    expect(provider.summarize.mock.calls.some(([, prompt]) => prompt.includes("forced-summary"))).toBe(true);
    expect(await kv.list(KV.semantic)).toHaveLength(1);
  });

  it("gates only low-value scheduled reflection clusters", async () => {
    const h = effectHarness();
    const { sdk, kv } = h;
    const typeSafe = decisionProvider({ evaluateChoice: async () => choice("skip") });
    for (let index = 0; index < 3; index += 1) {
      await kv.set(KV.semantic, `semantic-${index}`, {
        id: `semantic-${index}`,
        project: "test-project",
        fact: "database caching improves query performance",
        confidence: 0.5,
        updatedAt: "2020-01-01T00:00:00.000Z",
      });
    }
    const provider = {
      summarize: vi.fn(async () => "<no-skill/>"),
    };
    const indexStatus = {
      version: 1,
      capabilities: ["state::semantic_lsh_v1", "state::indexed_graph_v1"],
      graph: [KV.graphNodes, KV.graphEdges].map((scope) => ({ scope, status: "ready", revision: "1" })),
      semantic: [{ index_id: "observations", model: "fixture-embedding", dimensions: 2, generation: "local-v1",
        status: "ready", count: 0, lexical_count: 0, lexical_ready: true, source_kind: "agentmemory",
        source_prepared: true, dirty_count: 0, coverage_ready: true }],
    };
    const retrieval = kv.retrieval.bind(kv);
    kv.retrieval = async (request) => {
      if (request.action === "index_status") return indexStatus as never;
      if (request.action === "keyword_search" || request.action === "semantic_search") {
        return { items: [], model: "fixture-embedding", dimensions: 2, generation: "local-v1" } as never;
      }
      return retrieval(request);
    };
    await kv.set(KV.config, "indexed-corpus", { status: "ready", model: "fixture-embedding", dimensions: 2, generation: "local-v1" });
    const embedding = { name: "fixture-embedding", dimensions: 2,
      embed: async () => new Float32Array([1, 0]), embedBatch: async (texts: string[]) => texts.map(() => new Float32Array([1, 0])) };
    registerReflectFunctions(sdk as never, kv as never, provider as never, undefined, undefined, typeSafe as never, embedding as never);

    const accepted = await h.call<{ jobId: string; queued: boolean }>("mem::reflect", {
      project: "test-project", query: "database caching improves query performance", factIds: ["semantic-0", "semantic-1", "semantic-2"],
      deferred: true, maxClusters: 1,
    });
    expect(accepted).toMatchObject({ success: true, queued: true });
    await h.drainDeliveries();
    const result = await h.call<{ clustersSkipped: number }>("mem::reflect-status", { jobId: accepted.jobId });

    expect(result.clustersSkipped).toBe(1);
    expect(typeSafe.evaluateChoice).toHaveBeenCalledWith(
      "pipelineGates",
      expect.stringContaining('"workflow":"reflection"'),
      expect.any(String),
      expect.any(Object),
    );
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("lets the LLM decline low-signal skill extraction without TypeSafe gating, including force", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({ evaluateChoice: async () => choice("skip") });
    await kv.set(KV.sessions, "session-test", {
      id: "session-test", project: "test-project", cwd: "D:/test", startedAt: new Date().toISOString(),
      status: "completed", observationCount: 3,
    });
    await kv.set(KV.summaries, "session-test", {
      ...sessionSummary("session-test", 0),
      filesModified: [],
    });
    for (let index = 0; index < 3; index += 1) {
      await kv.set(KV.observations("session-test"), `observation-${index}`, compressedObservation(`observation-${index}`));
    }
    const provider = { summarize: vi.fn(async () => "<no-skill/>") };
    registerSkillExtractFunctions(sdk as never, kv as never, provider as never, undefined, typeSafe as never,
      async (_project, sessionId) => (await kv.list<CompressedObservation>(KV.observations(sessionId)))
        .map((observation) => ({ id: observation.id, sessionId, rowVersion: "1", scope: KV.observations(sessionId) })) as never);

    const result = await sdk.trigger("mem::skill-extract", { sessionId: "session-test" });

    expect(result).toMatchObject({ success: true, extracted: false });
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(provider.summarize).toHaveBeenCalledTimes(1);

    typeSafe.evaluateChoice.mockClear();
    await sdk.trigger("mem::skill-extract", { sessionId: "session-test", force: true });
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(provider.summarize).toHaveBeenCalledTimes(1);
  });
});
