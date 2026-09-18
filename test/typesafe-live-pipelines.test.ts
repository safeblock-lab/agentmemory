import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  getTypeSafeConfig,
  TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_REFLECTION_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD,
} from "../src/config.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { TypeSafeDecisionProvider, type TypeSafeChoiceAnswer } from "../src/providers/typesafe.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, Memory, SessionSummary } from "../src/types.js";
import { mockKV } from "./helpers/mocks.js";

type Handler = (payload: unknown) => Promise<unknown>;

function harness() {
  const handlers = new Map<string, Handler>();
  const sdk = {
    registerFunction(idOrOptions: string | { id: string }, handler: Handler) {
      handlers.set(typeof idOrOptions === "string" ? idOrOptions : idOrOptions.id, handler);
    },
    async trigger(idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      return handlers.get(id)?.(payload) ?? null;
    },
  };
  return { sdk, kv: mockKV() };
}

function liveProvider() {
  const provider = new TypeSafeDecisionProvider();
  const decisions: TypeSafeChoiceAnswer[] = [];
  const evaluateChoice = provider.evaluateChoice.bind(provider);
  provider.evaluateChoice = async (...args) => {
    const answer = await evaluateChoice(...args);
    if (answer) decisions.push(answer);
    return answer;
  };
  return { provider, decisions };
}

function observation(id: string): CompressedObservation {
  return {
    id,
    sessionId: "typesafe-live",
    timestamp: "2026-01-01T00:00:00.000Z",
    type: "other",
    title: `Routine lookup ${id}`,
    facts: [],
    narrative: "Read a public generated version label without changing the project.",
    concepts: [],
    files: [],
    importance: 3,
  };
}

function summary(index: number): SessionSummary {
  return {
    sessionId: `typesafe-live-${index}`,
    project: "typesafe-live",
    createdAt: new Date(Date.now() - index * 86_400_000).toISOString(),
    title: `Routine dependency lookup ${index}`,
    narrative: "Inspected a public package version with no durable project information.",
    keyDecisions: [],
    filesModified: [],
    concepts: ["dependency"],
    observationCount: 1,
  };
}

function pattern(index: number): Memory {
  return {
    id: `typesafe-live-pattern-${index}`,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    type: "pattern",
    title: `Routine lookup pattern ${index}`,
    content: `Read a generated version label from public metadata source ${index}`,
    concepts: ["metadata"],
    files: [],
    sessionIds: ["session-a", "session-b"],
    strength: 2,
    version: 1,
    isLatest: true,
  };
}

function expectCoherentDecision(
  decision: TypeSafeChoiceAnswer,
  providerCalled: boolean,
  threshold: number,
) {
  expect(["run", "skip"]).toContain(decision.choice);
  expect(decision.confidence).toBeGreaterThanOrEqual(0);
  expect(decision.confidence).toBeLessThanOrEqual(1);
  const shouldSkip = decision.choice === "skip" && decision.confidence >= threshold;
  expect(providerCalled).toBe(!shouldSkip);
}

const runLiveTests = process.env["RUN_TYPESAFE_LIVE_TESTS"] === "true";

describe.skipIf(!runLiveTests)("TypeSafe live pipeline integrations", () => {
  beforeEach(() => {
    vi.stubEnv("AGENTMEMORY_TYPESAFE_ENABLED", "true");
    vi.stubEnv("AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED", "false");
    vi.stubEnv("AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED", "true");
    vi.stubEnv("CONSOLIDATION_ENABLED", "true");
    if (!getTypeSafeConfig().apiKey) throw new Error("TYPESAFE_API_KEY is required for paid live tests.");
  });

  afterEach(() => vi.unstubAllEnvs());

  it("executes the registered graph gate with a real decision", async () => {
    const { sdk, kv } = harness();
    const { provider: typeSafe, decisions } = liveProvider();
    const compress = vi.fn(async () => '<entity type="concept" name="live-graph"/>');
    registerGraphFunction(sdk as never, kv as never, { compress } as never, undefined, undefined, undefined, typeSafe);

    const result = await sdk.trigger("mem::graph-extract", {
      observations: Array.from({ length: 7 }, (_, index) => observation(`graph-${index}`)),
      deferred: true,
    }) as { success?: boolean };

    expect(result.success).toBe(true);
    expect(decisions).toHaveLength(1);
    console.info("[typesafe-live] graph", JSON.stringify(decisions[0]));
    expectCoherentDecision(
      decisions[0]!,
      compress.mock.calls.length > 0,
      TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD,
    );
  }, 30_000);

  it("executes both registered consolidation gates with real decisions", async () => {
    const { sdk, kv } = harness();
    const { provider: typeSafe, decisions } = liveProvider();
    const summarize = vi.fn(async (_system: string, prompt: string) => prompt.includes("procedure")
      ? '<procedure name="Live routine" trigger="metadata lookup"><step>Read metadata</step></procedure>'
      : '<fact confidence="0.8">A package version was inspected</fact>');
    for (let index = 0; index < 7; index += 1) await kv.set(KV.summaries, `summary-${index}`, summary(index));
    for (let index = 0; index < 3; index += 1) await kv.set(KV.memories, `pattern-${index}`, pattern(index));
    registerConsolidationPipelineFunction(sdk as never, kv as never, { summarize } as never, undefined, undefined, undefined, typeSafe);

    const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "all", deferred: true }) as { success?: boolean };

    expect(result.success).toBe(true);
    expect(decisions).toHaveLength(2);
    console.info("[typesafe-live] consolidation", JSON.stringify(decisions));
    expect(["run", "skip"]).toContain(decisions[0]!.choice);
    expect(["run", "skip"]).toContain(decisions[1]!.choice);
    const decisionThresholds = [
      TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD,
      TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD,
    ];
    const expectedCalls = decisions.filter(
      (decision, index) => decision.choice !== "skip" || decision.confidence < decisionThresholds[index]!,
    ).length;
    expect(summarize).toHaveBeenCalledTimes(expectedCalls);
  }, 30_000);

  it("executes the registered reflection gate with a real decision", async () => {
    const { sdk, kv } = harness();
    const { provider: typeSafe, decisions } = liveProvider();
    for (let index = 0; index < 3; index += 1) {
      await kv.set(KV.semantic, `semantic-${index}`, {
        id: `semantic-${index}`,
        fact: "public package metadata can contain a version label",
        confidence: 0.5,
        updatedAt: "2026-01-01T00:00:00.000Z",
      });
    }
    const summarize = vi.fn(async () => "<no-insight/>");
    registerReflectFunctions(sdk as never, kv as never, { summarize } as never, undefined, undefined, typeSafe);

    const result = await sdk.trigger("mem::reflect", { deferred: true, maxClusters: 1 }) as { success?: boolean };

    expect(result.success).toBe(true);
    expect(decisions).toHaveLength(1);
    console.info("[typesafe-live] reflection", JSON.stringify(decisions[0]));
    expectCoherentDecision(
      decisions[0]!,
      summarize.mock.calls.length > 0,
      TYPESAFE_REFLECTION_GATE_CONFIDENCE_THRESHOLD,
    );
  }, 30_000);

  it("executes the registered skill extraction gate with a real decision", async () => {
    const { sdk, kv } = harness();
    const { provider: typeSafe, decisions } = liveProvider();
    await kv.set(KV.sessions, "typesafe-live", { id: "typesafe-live", status: "completed" });
    await kv.set(KV.summaries, "typesafe-live", { ...summary(0), sessionId: "typesafe-live" });
    for (let index = 0; index < 3; index += 1) {
      await kv.set(KV.observations("typesafe-live"), `observation-${index}`, observation(`skill-${index}`));
    }
    const summarize = vi.fn(async () => "<no-skill/>");
    registerSkillExtractFunctions(sdk as never, kv as never, { summarize } as never, undefined, typeSafe);

    const result = await sdk.trigger("mem::skill-extract", { sessionId: "typesafe-live" }) as { success?: boolean };

    expect(result.success).toBe(true);
    expect(decisions).toHaveLength(1);
    console.info("[typesafe-live] skill", JSON.stringify(decisions[0]));
    expectCoherentDecision(
      decisions[0]!,
      summarize.mock.calls.length > 0,
      TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD,
    );
  }, 30_000);
});
