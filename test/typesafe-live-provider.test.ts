import { beforeAll, describe, expect, it } from "vitest";
import {
  getTypeSafeConfig,
  type TypeSafeFeature,
} from "../src/config.js";
import {
  selectCompactionCandidates,
  type CompactionCandidate,
  type CompactionChoice,
} from "../src/functions/typesafe-compaction.js";
import { TypeSafeDecisionProvider } from "../src/providers/typesafe.js";

const runLiveTests = process.env.RUN_TYPESAFE_LIVE_TESTS?.trim().toLowerCase() === "true";
const liveTestTimeoutMs = 45_000;
const providerTimeoutMs = 30_000;

function createLiveProvider(feature: TypeSafeFeature): TypeSafeDecisionProvider {
  const config = getTypeSafeConfig();
  if (!config.apiKey) {
    throw new Error("Live TypeSafe tests require TYPESAFE_API_KEY in AgentMemory configuration.");
  }
  if (!config.enabled || !config.features[feature]) {
    throw new Error(`Live TypeSafe test requires the ${feature} feature to be enabled in configuration.`);
  }
  return new TypeSafeDecisionProvider({
    config: { ...config, timeoutMs: providerTimeoutMs },
  });
}

function expectProbabilityDistribution(
  probabilities: Readonly<Record<string, number>>,
  expectedKeys: readonly string[],
): void {
  expect(Object.keys(probabilities).sort()).toEqual([...expectedKeys].sort());
  for (const probability of Object.values(probabilities)) {
    expect(probability).toBeGreaterThanOrEqual(0);
    expect(probability).toBeLessThanOrEqual(1);
  }
  const total = Object.values(probabilities).reduce((sum, value) => sum + value, 0);
  expect(Math.abs(total - 1)).toBeLessThanOrEqual(0.02);
}

describe.skipIf(!runLiveTests)("TypeSafe live provider", () => {
  beforeAll(() => {
    if (!getTypeSafeConfig().apiKey) {
      throw new Error("Set TYPESAFE_API_KEY in AgentMemory configuration before enabling live tests.");
    }
  });

  it("returns a bounded Noul score for an explicit synthetic memory request", async () => {
    const provider = createLiveProvider("admission");
    const score = await provider.evaluateNoul(
      "admission",
      {
        observation: "SYNTHETIC TEST: Remember that project ORBIT uses port 4317 for its test server.",
        source: "explicit request to remember a stable project setting",
      },
      "Estimate whether this observation belongs in durable project memory. Return a high value only for explicit, useful, future-facing instructions; return a low value for transient noise.",
      {
        true: "The user explicitly asked to remember a stable project configuration for future sessions.",
        false: "The content is transient, irrelevant, duplicated, or has no future recall value.",
      },
    );

    if (score === undefined) throw new Error("TypeSafe returned no valid Noul answer.");
    expect(score).toBeGreaterThan(0.5);
    expect(score).toBeLessThanOrEqual(1);
  }, liveTestTimeoutMs);

  it("returns a valid Choice answer for an obvious keep decision", async () => {
    const provider = createLiveProvider("admission");
    const answer = await provider.evaluateChoice(
      "admission",
      {
        observation: "SYNTHETIC TEST: The user explicitly says to retain schema marker ORBIT-SYNTH-17 for this project.",
        source: "direct, durable project instruction",
      },
      "Choose whether this observation should be saved as durable project memory.",
      {
        keep: "Explicit, stable project instruction or preference with future recall value.",
        drop: "Transient chatter, duplicate content, or information with no future value.",
      },
    );

    if (!answer) throw new Error("TypeSafe returned no valid Choice answer.");
    expect(answer.choice).toBe("keep");
    expect(answer.confidence).toBeGreaterThanOrEqual(0);
    expect(answer.confidence).toBeLessThanOrEqual(1);
    expectProbabilityDistribution(answer.probabilities, ["keep", "drop"]);
  }, liveTestTimeoutMs);

  it("returns a bounded Score and probabilities for a synthetic durable instruction", async () => {
    const criteria = [
      "Transient detail with no likely future use.",
      "Potentially useful within the current task only.",
      "Stable project fact or explicit instruction useful across future sessions.",
    ];
    const provider = createLiveProvider("scoring");
    const answer = await provider.evaluateScore(
      "scoring",
      {
        observation: "SYNTHETIC TEST: The project requires all generated fixture identifiers to start with ORBIT-FIXTURE-.",
        source: "explicit recurring project constraint",
      },
      "Rate the durability and future usefulness of this observation using the supplied scale.",
      criteria,
    );

    if (!answer) throw new Error("TypeSafe returned no valid Score answer.");
    expect(answer.score).toBeGreaterThan(0.5);
    expect(answer.score).toBeLessThanOrEqual(criteria.length - 1);
    expect(answer.legend).toEqual({ "0": criteria[0], "1": criteria[1], "2": criteria[2] });
    expect(answer.confidence).toBeGreaterThanOrEqual(0);
    expect(answer.confidence).toBeLessThanOrEqual(1);
    expectProbabilityDistribution(answer.probabilities, ["0", "1", "2"]);
  }, liveTestTimeoutMs);

  it("runs compaction selection with a real TypeSafe Choice decision", async () => {
    const provider = createLiveProvider("compaction");
    let decisionCalls = 0;
    const candidates: CompactionCandidate<string>[] = [
      {
        id: "protected-instruction",
        value: "SYNTHETIC TEST: explicit durable instruction",
        disposition: "protected",
      },
      {
        id: "known-duplicate",
        value: "SYNTHETIC TEST: deterministic duplicate",
        disposition: "deterministic-drop",
      },
      {
        id: "heartbeat",
        value: "SYNTHETIC TEST: routine heartbeat",
        disposition: "ambiguous",
        state: "Tool output was only 'pong' from a routine liveness check; there was no request, result, error, or state change.",
      },
    ];
    const decide = async (inputs: readonly { questionId: string; state: string }[]) => {
      decisionCalls += 1;
      const questions = Object.fromEntries(inputs.map(({ questionId }) => [questionId, {
        type: "choice" as const,
        instructions: "Should this observation remain in memory? Drop only empty routine heartbeat output with no future recall value.",
        criteria: {
          keep: "Contains a user request, meaningful result, error, state change, or future recall value.",
          drop: "Routine heartbeat with no result, error, state change, or future recall value.",
        },
      }]));
      const answers = await provider.evaluate("compaction", {
        candidates: inputs.map(({ questionId, state }) => ({ questionId, state })),
      }, questions);
      const choices: Record<string, CompactionChoice> = {};
      for (const [questionId, answer] of Object.entries(answers ?? {})) {
        if (answer.type === "choice" && (answer.choice === "keep" || answer.choice === "drop")) {
          choices[questionId] = { choice: answer.choice, confidence: answer.confidence };
        }
      }
      return Object.keys(choices).length === inputs.length ? choices : undefined;
    };

    const result = await selectCompactionCandidates(candidates, decide);
    const outputIds = [...result.kept, ...result.dropped].map(({ id }) => id).sort();

    expect(decisionCalls).toBe(1);
    expect(outputIds).toEqual(candidates.map(({ id }) => id).sort());
    expect(result.kept.map(({ id }) => id)).toContain("protected-instruction");
    expect(result.dropped.map(({ id }) => id)).toContain("known-duplicate");
    expect(result.fallbackReasons).not.toContain("decision-unavailable");
    expect(result.fallbackReasons).not.toContain("decision-error");
    expect(result.fallbackReasons).not.toContain("invalid-answer");
  }, liveTestTimeoutMs);
});
