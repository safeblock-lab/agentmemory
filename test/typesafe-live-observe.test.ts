import { randomUUID } from "node:crypto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerObserveFunction } from "../src/functions/observe.js";
import {
  getTypeSafeConfig,
  TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD,
  TYPESAFE_SCORING_CONFIDENCE_THRESHOLD,
} from "../src/config.js";
import { TypeSafeDecisionProvider } from "../src/providers/typesafe.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation } from "../src/types.js";
import { mockKV } from "./helpers/mocks.js";

type Handler = (payload: unknown) => Promise<unknown>;

function createSdk() {
  const handlers = new Map<string, Handler>();
  return {
    registerFunction(idOrOptions: string | { id: string }, handler: Handler) {
      handlers.set(typeof idOrOptions === "string" ? idOrOptions : idOrOptions.id, handler);
    },
    async trigger(
      idOrInput: string | { function_id: string; payload: unknown },
      data?: unknown,
    ) {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      return handlers.get(id)?.(payload) ?? null;
    },
  };
}

function createRealProviderWithCapturedAnswers() {
  const provider = new TypeSafeDecisionProvider();
  const answersByCall: Array<Awaited<ReturnType<typeof provider.evaluate>>> = [];
  const evaluate = provider.evaluate.bind(provider);
  provider.evaluate = async (feature, state, questions) => {
    const answers = await evaluate(feature, state, questions);
    answersByCall.push(answers);
    return answers;
  };
  return { provider, answersByCall };
}

async function runObservation(data: Record<string, unknown>) {
  const sdk = createSdk();
  const kv = mockKV();
  const { provider, answersByCall } = createRealProviderWithCapturedAnswers();
  const sessionId = `typesafe-live-${randomUUID()}`;
  registerObserveFunction(sdk as never, kv as never, undefined, undefined, provider);

  const result = await sdk.trigger("mem::observe", {
    sessionId,
    hookType: "post_tool_use",
    timestamp: new Date().toISOString(),
    data,
  }) as { success?: boolean; skipped?: boolean; reason?: string; observationId?: string };
  const stored = await kv.list<CompressedObservation>(KV.observations(sessionId));

  return { result, stored, answersByCall };
}

const runLiveTests = process.env["RUN_TYPESAFE_LIVE_TESTS"] === "true";

describe.skipIf(!runLiveTests)("TypeSafe live observation integration", () => {
  beforeEach(() => {
    vi.stubEnv("AGENTMEMORY_TYPESAFE_ENABLED", "true");
    vi.stubEnv("AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED", "true");
    vi.stubEnv("AGENTMEMORY_TYPESAFE_SCORING_ENABLED", "true");
    vi.stubEnv("AGENTMEMORY_AUTO_COMPRESS", "false");

    if (!getTypeSafeConfig().apiKey) {
      throw new Error("Set TYPESAFE_API_KEY in the environment or .env to run paid TypeSafe live tests.");
    }
  });

  afterEach(() => vi.unstubAllEnvs());

  it("applies the conservative admission threshold to a routine read", async () => {
    const { result, stored, answersByCall } = await runObservation({
      tool_name: "Read",
      tool_input: { file_path: "docs/generated/version.txt" },
      tool_output: "Version: 1.2.3\nPackage: example-small\n",
    });

    expect(answersByCall).toHaveLength(1);
    const answers = answersByCall[0];
    expect(answers).toBeDefined();
    const admission = answers?.admission;
    expect(admission?.type).toBe("choice");
    if (admission?.type !== "choice") throw new Error("TypeSafe returned no valid admission answer.");

    console.info("[typesafe-live] admission-routine", JSON.stringify(admission));
    expect(admission.choice).toBe("discard");
    if (admission.confidence >= TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD) {
      expect(result).toMatchObject({ success: true, skipped: true, reason: "TypeSafe admission" });
      expect(stored).toHaveLength(0);
    } else {
      expect(result.observationId).toBeTruthy();
      expect(stored).toHaveLength(1);
    }
  }, 30_000);

  it("retains durable project context and stores its bounded importance", async () => {
    const { result, stored, answersByCall } = await runObservation({
      tool_name: "Read",
      tool_input: { file_path: "src/state/schema.ts" },
      tool_output: [
        "Persistent memory layout: observations are keyed by session under mem:obs:<session-id>; reusable semantic facts are stored separately under mem:semantic;",
        "completed session summaries use mem:summaries. Stable identifiers keep each observation attributable to its source session.",
      ].join(" "),
    });

    expect(answersByCall).toHaveLength(1);
    const answers = answersByCall[0];
    expect(answers).toBeDefined();
    const admission = answers?.admission;
    expect(admission?.type).toBe("choice");
    if (admission?.type !== "choice") throw new Error("TypeSafe returned no valid admission answer.");
    expect(admission.choice).toBe("keep");

    expect(result.observationId).toBeTruthy();
    expect(stored).toHaveLength(1);
    expect(stored[0]?.id).toBe(result.observationId);

    const score = answers?.importance;
    expect(score?.type).toBe("score");
    if (score?.type !== "score") throw new Error("TypeSafe returned no valid importance score.");
    console.info("[typesafe-live] admission-valuable", JSON.stringify({ admission, importance: score }));
    expect(score.score).toBeGreaterThanOrEqual(0);
    expect(score.score).toBeLessThanOrEqual(9);

    const importance = stored[0]?.importance;
    expect(importance).toBeGreaterThanOrEqual(1);
    expect(importance).toBeLessThanOrEqual(10);
    expect(Number.isInteger(importance)).toBe(true);
    if (score.confidence >= TYPESAFE_SCORING_CONFIDENCE_THRESHOLD) {
      expect(importance).toBe(Math.max(1, Math.min(10, Math.round(score.score) + 1)));
    }
  }, 30_000);
});
