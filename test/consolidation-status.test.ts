import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { registerApiTriggers } from "../src/triggers/api.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { pageKey } from "../src/functions/consolidation-manifest.js";
import { registerSkillLifecycleFunctions, type SkillLifecycleSession } from "../src/functions/skill-lifecycle.js";
import { CONSOLIDATION_LAST_RUN_KEY } from "../src/functions/consolidation-status.js";
import { KV } from "../src/state/schema.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import type { ConsolidationCanonicalLookup, ConsolidationCanonicalWriter } from "../src/functions/consolidation-selection.js";
import type { SessionSummary } from "../src/types.js";
import { effectHarness } from "./batch-effects-harness.js";
import {
  describeConsolidation,
  type ConsolidationStatusInput,
} from "../src/functions/consolidation-status.js";

function input(overrides: Partial<ConsolidationStatusInput> = {}): ConsolidationStatusInput {
  return {
    now: new Date("2026-09-25T12:00:00.000Z"),
    enabled: true,
    llmConfigured: true,
    summaries: 0,
    recurringPatterns: 0,
    semanticFacts: 0,
    procedures: 0,
    relations: 0,
    lastRun: null,
    ...overrides,
  };
}

function tier(status: ReturnType<typeof describeConsolidation>, id: string) {
  const found = status.tiers.find((t) => t.id === id);
  if (!found) throw new Error(`tier ${id} missing`);
  return found;
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

describe("describeConsolidation", () => {
  it("says what to set when no LLM key is configured", () => {
    const status = describeConsolidation(input({ enabled: false, llmConfigured: false }));
    expect(tier(status, "semantic").state).toBe("off");
    expect(tier(status, "semantic").detail).toMatch(/add an LLM provider key/);
    expect(tier(status, "procedural").state).toBe("off");
  });

  it("says consolidation was switched off on purpose when a key exists", () => {
    const status = describeConsolidation(input({ enabled: false, llmConfigured: true }));
    expect(tier(status, "semantic").detail).toMatch(/CONSOLIDATION_ENABLED is set to false/);
  });

  it("shows progress toward the semantic threshold and the procedural requirements", () => {
    const status = describeConsolidation(input({ summaries: 3, recurringPatterns: 1 }));
    expect(tier(status, "semantic")).toMatchObject({
      state: "waiting",
      detail: "Waiting: 3 of 5 session summaries needed.",
    });
    const procedural = tier(status, "procedural");
    expect(procedural.state).toBe("waiting");
    expect(procedural.detail).toMatch(/each finished session with a summary and 3\+ observations/);
    expect(procedural.detail).toMatch(/you have 1/);
  });

  it("reports ready once the inputs are there and nothing has run yet", () => {
    const status = describeConsolidation(input({ summaries: 8, recurringPatterns: 2 }));
    expect(tier(status, "semantic").state).toBe("ready");
    expect(tier(status, "procedural").state).toBe("ready");
  });

  it("reports the last run's output and errors per tier", () => {
    const status = describeConsolidation(
      input({
        summaries: 8,
        semanticFacts: 12,
        lastRun: {
          at: "2026-09-25T10:00:00.000Z",
          tier: "all",
          results: {
            semantic: { newFacts: 4, totalSummaries: 8 },
            procedural: { error: "provider timeout" },
          },
        },
      }),
    );
    expect(status.lastRunAt).toBe("2026-09-25T10:00:00.000Z");
    expect(tier(status, "semantic")).toMatchObject({ state: "ran", count: 12, detail: "Last run 2 h ago: 4 new facts." });
    expect(tier(status, "procedural")).toMatchObject({ state: "error", detail: "Last run 2 h ago failed: provider timeout" });
  });

  it("falls back to the input-based state when the last run skipped a tier", () => {
    const status = describeConsolidation(
      input({
        summaries: 2,
        lastRun: {
          at: "2026-09-25T11:59:30.000Z",
          tier: "all",
          results: { semantic: { skipped: true, reason: "fewer than 5 summaries" } },
        },
      }),
    );
    expect(tier(status, "semantic").state).toBe("waiting");
  });

  it("counts relations without depending on consolidation", () => {
    expect(tier(describeConsolidation(input({ enabled: false, relations: 3 })), "relations")).toMatchObject({
      state: "ran",
      detail: "3 links saved.",
    });
    expect(tier(describeConsolidation(input()), "relations").state).toBe("waiting");
  });
});

describe("skill extraction runs inside the consolidation cooldown", () => {
  const saved = { ...process.env };

  beforeEach(() => {
    vi.resetModules();
  });

  afterEach(() => {
    process.env = { ...saved };
  });

  async function stop(env: Record<string, string | undefined>) {
    for (const key of [
      "ANTHROPIC_API_KEY",
      "OPENAI_API_KEY",
      "GEMINI_API_KEY",
      "GOOGLE_API_KEY",
      "OPENROUTER_API_KEY",
      "MINIMAX_API_KEY",
      "CONSOLIDATION_ENABLED",
      "AGENTMEMORY_CONSOLIDATION_COOLDOWN_MS",
    ]) {
      delete process.env[key];
    }
    Object.assign(process.env, env);
    const { registerEventTriggers } = await import("../src/triggers/events.js");
    const handlers = new Map<string, (p: unknown) => Promise<unknown>>();
    const fired: string[] = [];
    const sdk = {
      registerFunction: (id: string, fn: (p: unknown) => Promise<unknown>) => handlers.set(id, fn),
      registerTrigger: () => undefined,
      trigger: vi.fn(async (req: { function_id: string; payload: unknown }) => {
        fired.push(req.function_id);
        const handler = handlers.get(req.function_id);
        return handler ? handler(req.payload) : { success: true };
      }),
    };
    const store = new Map<string, Map<string, unknown>>();
    const kv = {
      get: async <T>(scope: string, key: string): Promise<T | null> => (store.get(scope)?.get(key) as T) ?? null,
      set: async <T>(scope: string, key: string, value: T): Promise<T> => {
        if (!store.has(scope)) store.set(scope, new Map());
        store.get(scope)!.set(key, value);
        return value;
      },
      list: async <T>(scope: string): Promise<T[]> => Array.from(store.get(scope)?.values() ?? []) as T[],
    };
    registerSkillLifecycleFunctions(sdk as never, kv as never);
    registerEventTriggers(sdk as never, kv as never);
    await handlers.get("event::session::stopped")!({ sessionId: "s1" });
    return {
      fired,
      intent: await kv.get<SkillLifecycleSession>(KV.skillLifecycleIntents, "session:s1"),
    };
  }

  it("persists provider-eligible lifecycle intent before due consolidation", async () => {
    const { fired, intent } = await stop({ ANTHROPIC_API_KEY: "sk-test", CONSOLIDATION_ENABLED: "true" });
    expect(fired).toContain("mem::consolidate-pipeline");
    expect(fired).not.toContain("mem::skill-extract");
    expect(intent).toMatchObject({ kind: "session", sessionId: "s1", consolidationEnabled: true, providerEligible: true });
  });

  it("persists provider ineligibility without directly dispatching skill extraction", async () => {
    const { fired, intent } = await stop({ CONSOLIDATION_ENABLED: "true" });
    expect(fired).not.toContain("mem::skill-extract");
    expect(intent).toMatchObject({ kind: "session", consolidationEnabled: true, providerEligible: false });
  });

  it("persists disabled lifecycle state and suppresses consolidation scheduling", async () => {
    const { fired, intent } = await stop({ ANTHROPIC_API_KEY: "sk-test", CONSOLIDATION_ENABLED: "false" });
    expect(fired).not.toContain("mem::skill-extract");
    expect(fired).not.toContain("mem::consolidate-pipeline");
    expect(intent).toMatchObject({ kind: "session", consolidationEnabled: false, providerEligible: true });
  });
});

describe("consolidation status wiring", () => {
  const api = readFileSync("src/triggers/api.ts", "utf-8");
  const viewer = readFileSync("src/viewer/index.html", "utf-8");

  it("persists actual per-tier results from a consolidation run", async () => {
    const h = effectHarness();
    Object.assign(h.kv, { indexedRetrieval: true });
    const summaries = Array.from({ length: 5 }, (_, index): SessionSummary => ({
      sessionId: `status-session-${index}`, project: "test-project",
      createdAt: new Date(Date.UTC(2026, 0, index + 1)).toISOString(),
      title: `Status source ${index}`, narrative: "A durable project decision.", keyDecisions: [],
      filesModified: [], concepts: [], observationCount: 1,
    }));
    summaries.forEach((summary) => {
      h.seed(KV.summaries, summary.sessionId, summary);
      h.seed(KV.sessions, summary.sessionId, {
        id: summary.sessionId, project: "test-project", cwd: "D:/test", startedAt: summary.createdAt,
        status: "completed", observationCount: summary.observationCount,
      });
    });
    const selectSources = async (project: string) => (await h.kv.list<SessionSummary>(KV.summaries))
      .filter((summary) => summary.project === project)
      .map((summary) => ({ id: summary.sessionId, sessionId: summary.sessionId, rowVersion: "1" }));
    const provider = { summarize: vi.fn(async (system: string, prompt: string) => {
      if (system.startsWith("You evaluate publication evidence.")) {
        const evidence = JSON.parse(prompt);
        expect(evidence.sources.map((source: { key: string }) => source.key).sort()).toEqual(summaries.map(source => source.sessionId).sort());
        return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "All original fixture decisions support the candidate." });
      }
      return semanticFixtureResponse(prompt);
    }) };
    const lookup: ConsolidationCanonicalLookup = async () => ({ status: "ready", version: "1", canonicalId: null });
    const write: ConsolidationCanonicalWriter = async (scope, key, match, proposedId, effect, update) => {
      const id = match.canonicalId ?? proposedId;
      const old = await h.kv.get<Record<string, unknown>>(KV[scope], id);
      const applied = await applyBatchEffect(h.kv, KV[scope], id, effect, update as never);
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
    registerConsolidationPipelineFunction(h.sdk as never, h.kv, provider as never, undefined, undefined, undefined,
      undefined, selectSources, lookup, undefined, write);

    let result = await h.call<Record<string, unknown>>("mem::consolidate-pipeline", { tier: "semantic", project: "test-project", force: true });
    let manifestId = (result as { results?: { semantic?: { continuation?: { consolidationManifestId?: string } } } })
      .results?.semantic?.continuation?.consolidationManifestId;
    const progress: string[] = [];
    for (let step = 0; manifestId && step < 32; step += 1) {
      result = await h.call("mem::consolidate-pipeline", { consolidationManifestId: manifestId });
      const root = await h.kv.get<Record<string, unknown>>(KV.state, pageKey(manifestId, "root", 0));
      const response = result as { error?: unknown; phase?: unknown; completed?: unknown; queued?: unknown };
      progress.push(`${root?.phase ?? "missing"}:capture=${(root?.capture as { source?: number } | undefined)?.source ?? "?"}/${Array.isArray(root?.refs) ? root.refs.length : "?"},quality=${(root?.quality as { source?: number } | undefined)?.source ?? "?"},extract=${(root?.extract as { page?: number; offset?: number } | undefined)?.page ?? "?"}/${root?.fragmentPages ?? "?"}:${(root?.extract as { page?: number; offset?: number } | undefined)?.offset ?? "?"},eligible=${Array.isArray(root?.eligibleSources) ? root.eligibleSources.length : "?"},response=${String(response.error ?? response.phase ?? (response.completed ? "completed" : response.queued ? "queued" : "no-status"))}`);
      manifestId = (result as { continuation?: { consolidationManifestId?: string } }).continuation?.consolidationManifestId;
    }
    expect(manifestId, progress.join(" -> ")).toBeUndefined();
    expect(result).toMatchObject({ success: true, completed: true });
    expect(provider.summarize).toHaveBeenCalled();
    expect(await h.kv.list(KV.semantic), JSON.stringify(result)).toHaveLength(1);
    expect(await h.kv.get(KV.config, CONSOLIDATION_LAST_RUN_KEY)).toMatchObject({
      tier: "semantic", results: { semantic: expect.any(Object) },
    });
  });

  it("GET /agentmemory/consolidation/status is registered behind auth", () => {
    expect(api).toMatch(/registerFunction\("api::consolidation-status",\s*async \(req: HttpRequest\): Promise<Response> => \{\s*const authErr = checkAuth\(req, secret\);/);
    expect(api).toMatch(/api_path: "\/agentmemory\/consolidation\/status", http_method: "GET"/);
  });

  it("reuses one bounded set of store scans across status requests", async () => {
    const h = effectHarness();
    const apiSdk = { ...h.sdk, registerTrigger: () => undefined };
    registerApiTriggers(apiSdk as never, h.kv, "test-secret");
    const pageScans = vi.spyOn(h.kv, "pages");
    const request = { headers: { authorization: "Bearer test-secret" } };
    expect((await h.call<{ status_code: number }>("api::consolidation-status", request)).status_code).toBe(200);
    const firstScanCount = pageScans.mock.calls.length;
    expect(firstScanCount).toBeGreaterThan(0);
    expect((await h.call<{ status_code: number }>("api::consolidation-status", request)).status_code).toBe(200);
    expect(pageScans).toHaveBeenCalledTimes(firstScanCount);
  });

  it("the dashboard shows one Memory layers panel instead of the three separate cards", () => {
    expect(viewer).toMatch(/apiGet\('consolidation\/status'\)/);
    expect(viewer).toMatch(/html \+= renderMemoryLayers\(d\.consolidation, semFacts, procItems\);/);
    expect(viewer).not.toMatch(/card-title">Semantic Memory|card-title">Procedural Memory|card-title">Consolidation Status/);
    expect(viewer).not.toMatch(/consolidation-row/);
  });
});
