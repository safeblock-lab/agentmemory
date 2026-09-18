import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerObserveFunction } from "../src/functions/observe.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, SessionSummary } from "../src/types.js";
import { mockKV } from "./helpers/mocks.js";

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
  return { sdk, kv: mockKV() };
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
        tool_output: "x".repeat(2_000),
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
      compress: vi.fn(async (_system: string, prompt: string) => {
        expect(prompt).not.toContain("compacted-out-observation");
        return '<entity type="concept" name="graph-gate-test"/>';
      }),
    };
    registerGraphFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined, typeSafe as never);
    const observations = Array.from({ length: 7 }, (_, index) => compressedObservation(
      `obs-${index}`,
      index === 6 ? { title: "compacted-out-observation", timestamp: "2020-01-01T00:00:00.000Z" } : {},
    ));

    const result = await sdk.trigger("mem::graph-extract", { observations });

    expect(result).toMatchObject({ success: true, nodesAdded: 1 });
    expect(typeSafe.evaluate).toHaveBeenCalledWith(
      "compaction",
      expect.objectContaining({ candidates: expect.any(Array) }),
      expect.any(Object),
    );
    expect(await kv.list(KV.observations("session-test"))).toHaveLength(0);
  });

  it("gates scheduled graph extraction with bounded state and retains its input", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({ evaluateChoice: async () => choice("skip") });
    const provider = { compress: vi.fn() };
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

  it("gates automatic semantic consolidation but still runs with compacted input when allowed", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({
      evaluate: async (feature) => feature === "compaction"
        ? { candidate_0: choice("drop") }
        : undefined,
      evaluateChoice: async () => choice("run"),
    });
    const provider = {
      summarize: vi.fn(async (_system: string, prompt: string) => {
        expect(prompt).not.toContain("compacted-out-summary");
        return '<fact confidence="0.8">Selected summary fact</fact>';
      }),
    };
    for (let index = 0; index < 7; index += 1) {
      const summary = sessionSummary(`session-${index}`, index);
      if (index === 6) summary.title = "compacted-out-summary";
      await kv.set(KV.summaries, summary.sessionId, summary);
    }
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined, typeSafe as never);

    const result = await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", deferred: true }) as {
      results: { semantic: { newFacts: number; compactedSummaries: number } };
    };

    expect(typeSafe.evaluate).toHaveBeenCalledTimes(1);
    expect(typeSafe.evaluateChoice).toHaveBeenCalledWith(
      "pipelineGates",
      expect.stringContaining('"workflow":"semantic-consolidation"'),
      expect.any(String),
      expect.any(Object),
    );
    expect(result.results.semantic.compactedSummaries).toBe(1);
    expect(result.results.semantic.newFacts).toBe(1);
  });

  it("preserves forced consolidation input without invoking TypeSafe", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider();
    const provider = {
      summarize: vi.fn(async (_system: string, prompt: string) => {
        expect(prompt).toContain("forced-summary");
        return '<fact confidence="0.8">Forced fact retained</fact>';
      }),
    };
    for (let index = 0; index < 7; index += 1) {
      const summary = sessionSummary(`forced-session-${index}`, index);
      if (index === 6) summary.title = "forced-summary";
      await kv.set(KV.summaries, summary.sessionId, summary);
    }
    registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, undefined, undefined, undefined, typeSafe as never);

    await sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", force: true });

    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(provider.summarize).toHaveBeenCalledTimes(1);
  });

  it("gates only low-value scheduled reflection clusters", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({ evaluateChoice: async () => choice("skip") });
    for (let index = 0; index < 3; index += 1) {
      await kv.set(KV.semantic, `semantic-${index}`, {
        id: `semantic-${index}`,
        fact: "database caching improves query performance",
        confidence: 0.5,
        updatedAt: "2020-01-01T00:00:00.000Z",
      });
    }
    const provider = {
      summarize: vi.fn(async () => "<no-skill/>"),
    };
    registerReflectFunctions(sdk as never, kv as never, provider as never, undefined, undefined, typeSafe as never);

    const result = await sdk.trigger("mem::reflect", { deferred: true, maxClusters: 1 }) as {
      clustersSkipped: number;
    };

    expect(result.clustersSkipped).toBe(1);
    expect(typeSafe.evaluateChoice).toHaveBeenCalledWith(
      "pipelineGates",
      expect.stringContaining('"workflow":"reflection"'),
      expect.any(String),
      expect.any(Object),
    );
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("gates low-signal skill extraction and accepts force to bypass the gate", async () => {
    const { sdk, kv } = harness();
    const typeSafe = decisionProvider({ evaluateChoice: async () => choice("skip") });
    await kv.set(KV.sessions, "session-test", { id: "session-test", status: "completed" });
    await kv.set(KV.summaries, "session-test", {
      ...sessionSummary("session-test", 0),
      filesModified: [],
    });
    for (let index = 0; index < 3; index += 1) {
      await kv.set(KV.observations("session-test"), `observation-${index}`, compressedObservation(`observation-${index}`));
    }
    const provider = { summarize: vi.fn(async () => "<no-skill/>") };
    registerSkillExtractFunctions(sdk as never, kv as never, provider as never, undefined, typeSafe as never);

    const result = await sdk.trigger("mem::skill-extract", { sessionId: "session-test" });

    expect(result).toMatchObject({ success: true, extracted: false, skipped: true });
    expect(typeSafe.evaluateChoice).toHaveBeenCalledWith(
      "pipelineGates",
      expect.stringContaining('"workflow":"skill-extraction"'),
      expect.any(String),
      expect.any(Object),
    );
    expect(provider.summarize).not.toHaveBeenCalled();

    typeSafe.evaluateChoice.mockClear();
    await sdk.trigger("mem::skill-extract", { sessionId: "session-test", force: true });
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(provider.summarize).toHaveBeenCalledTimes(1);
  });
});
