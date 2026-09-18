import { describe, expect, it } from "vitest";
import {
  getTypeSafeConfig,
  TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD,
  TYPESAFE_COMPACTION_CONFIDENCE_THRESHOLD,
  TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_REFLECTION_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD,
} from "../src/config.js";
import {
  TypeSafeDecisionProvider,
  type TypeSafeChoiceAnswer,
  type TypeSafeQuestion,
  type TypeSafeScoreAnswer,
} from "../src/providers/typesafe.js";

type ExpectedClass = "negative" | "positive";
type DecisionFeature = "admission" | "pipelineGates" | "compaction";

interface Scenario {
  id: string;
  expected: ExpectedClass;
  text: string;
  expectedScore?: number;
}

interface Workflow {
  id: string;
  feature: DecisionFeature;
  negativeChoice: "discard" | "skip" | "drop";
  positiveChoice: "keep" | "run";
  instructions: string;
  criteria: Record<string, string>;
  state: (scenario: Scenario) => string;
  scenarios: Scenario[];
  currentThreshold: number;
  admissionWithScore?: boolean;
}

interface DecisionSample {
  workflow: string;
  scenarioId: string;
  expected: ExpectedClass;
  repeat: number;
  choice: string;
  confidence: number;
  score?: number;
  scoreConfidence?: number;
  expectedScore?: number;
}

const routine = [
  "Listed a directory and found the same twelve generated files as before.",
  "Read a generic package description already available in public metadata.",
  "Checked standard TypeScript syntax without learning anything project-specific.",
  "Opened the same documentation index again and found no new project context.",
  "Confirmed that the configured timeout matches the documented default and learned nothing else.",
  "Re-read the generated module list only to verify that it remains unchanged.",
];

const durable = [
  "Observations are stored per session while reusable facts use a separate semantic scope.",
  "The package targets Node 20 and emits ESM modules with tests kept under the test directory.",
  "Stable source identifiers connect each stored observation to the session that produced it.",
  "A reusable workflow validates external input, persists the result, then verifies the public contract.",
  "The graph input budget is shared with consolidation and controls when older records are omitted.",
  "Import compatibility requires each release version in both the public type union and supported-version set.",
];

function scenarios(id: string, negative = routine, positive = durable): Scenario[] {
  return [
    ...negative.map((text, index) => ({ id: `${id}-negative-${index}`, expected: "negative" as const, text })),
    ...positive.map((text, index) => ({ id: `${id}-positive-${index}`, expected: "positive" as const, text })),
  ];
}

function jsonState(value: unknown): string {
  return JSON.stringify(value).slice(0, 512);
}

const workflows: Workflow[] = [
  {
    id: "admission",
    feature: "admission",
    negativeChoice: "discard",
    positiveChoice: "keep",
    instructions: "Should this read-only tool observation be retained for future agent work?",
    criteria: {
      keep: "Retain this observation because it contains useful, durable project context.",
      discard: "Discard only if this is clearly routine, reproducible, low-value output.",
    },
    state: (scenario) => jsonState({ preview: scenario.text }),
    scenarios: [
      ...routine.map((text, index) => ({ id: `admission-negative-${index}`, expected: "negative" as const, text, expectedScore: [0, 1, 1, 2, 2, 1][index] })),
      ...durable.map((text, index) => ({ id: `admission-positive-${index}`, expected: "positive" as const, text, expectedScore: [6, 5, 6, 7, 6, 7][index] })),
    ],
    currentThreshold: TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD,
    admissionWithScore: true,
  },
  {
    id: "graph-compaction",
    feature: "compaction",
    negativeChoice: "drop",
    positiveChoice: "keep",
    instructions: "Should graph-extraction candidate candidate_0 remain in the bounded input?",
    criteria: {
      keep: "Keep if this observation may contribute a useful entity, relationship, decision, or discovery.",
      drop: "Drop only if it is clearly routine, reproducible, and unlikely to affect durable graph knowledge.",
    },
    state: (scenario) => jsonState({ type: "other", importance: scenario.expected === "negative" ? 3 : 6, title: scenario.text.slice(0, 64), concepts: [], factCount: scenario.expected === "negative" ? 0 : 1, preview: scenario.text.slice(0, 96), filesCount: 0 }),
    scenarios: scenarios("graph-compaction"),
    currentThreshold: TYPESAFE_COMPACTION_CONFIDENCE_THRESHOLD,
  },
  {
    id: "consolidation-compaction",
    feature: "compaction",
    negativeChoice: "drop",
    positiveChoice: "keep",
    instructions: "Should consolidation input candidate_0 remain in semantic consolidation?",
    criteria: {
      keep: "Keep if this input may add a durable fact or reusable procedure.",
      drop: "Drop only if this input is clearly routine, redundant, and low-value.",
    },
    state: (scenario) => jsonState({ title: scenario.text.slice(0, 56), narrative: scenario.text.slice(0, 96), concepts: [], observationCount: scenario.expected === "negative" ? 1 : 4, createdAt: "2026-01-01T00:00:00.000Z" }),
    scenarios: scenarios("consolidation-compaction"),
    currentThreshold: TYPESAFE_COMPACTION_CONFIDENCE_THRESHOLD,
  },
  {
    id: "graph-gate",
    feature: "pipelineGates",
    negativeChoice: "skip",
    positiveChoice: "run",
    instructions: "Should this scheduled graph extraction run?",
    criteria: { run: "Run if the observations may add useful entities or relationships to the graph.", skip: "Skip only when the batch is clearly routine and unlikely to add any durable graph information." },
    state: (scenario) => jsonState({ workflow: "graph-extraction", observationCount: 7, observationTypes: ["other"], samples: [{ title: scenario.text.slice(0, 32), concepts: [], facts: scenario.expected === "negative" ? 0 : 1, preview: scenario.text.slice(0, 20) }] }),
    scenarios: scenarios("graph-gate"),
    currentThreshold: TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD,
  },
  {
    id: "semantic-gate",
    feature: "pipelineGates",
    negativeChoice: "skip",
    positiveChoice: "run",
    instructions: "Should this scheduled semantic consolidation run?",
    criteria: { run: "Run when these new summaries may contribute useful, distinct durable facts.", skip: "Skip only when this batch is clearly routine or redundant and contains no important decision or change." },
    state: (scenario) => jsonState({ workflow: "semantic-consolidation", summaryCount: 5, existingFactCount: 3, summaries: [{ title: scenario.text.slice(0, 36), conceptCount: scenario.expected === "negative" ? 0 : 2, concepts: [], preview: scenario.text.slice(0, 20) }] }),
    scenarios: scenarios("semantic-gate"),
    currentThreshold: TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD,
  },
  {
    id: "procedural-gate",
    feature: "pipelineGates",
    negativeChoice: "skip",
    positiveChoice: "run",
    instructions: "Should this scheduled recurring-pattern extraction run?",
    criteria: { run: "Run when repeated patterns are likely to form a useful reusable procedure.", skip: "Skip only when these are clearly weak or routine patterns with no durable procedure." },
    state: (scenario) => jsonState({ workflow: "procedural-consolidation", patternCount: 2, patterns: [{ frequency: 2, preview: scenario.text.slice(0, 72) }] }),
    scenarios: scenarios("procedural-gate", routine, [
      "Register the capability, connect its handler, expose its route, then verify the public count.",
      "Create the fixture, call the registered function, and assert both persisted state and result.",
      "Check the feature flag, exercise enabled and disabled routes, then verify fallback behavior.",
      "Synchronize the public type, endpoint registration, manifest metadata, and contract test.",
      "After changing a configuration bound, exercise both sides of the boundary and compare persisted results.",
      "When adding a release field, update every compatibility surface and run the focused contract suite.",
    ]),
    currentThreshold: TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD,
  },
  {
    id: "reflection-gate",
    feature: "pipelineGates",
    negativeChoice: "skip",
    positiveChoice: "run",
    instructions: "Should this scheduled reflection cluster be processed?",
    criteria: { run: "Run if combining these persisted facts and lessons may produce a new useful insight.", skip: "Skip only when the cluster is clearly redundant or too weak to support a durable insight." },
    state: (scenario) => jsonState({ workflow: "reflection", concepts: ["metadata"], factCount: scenario.expected === "negative" ? 1 : 4, lessonCount: scenario.expected === "negative" ? 0 : 2, crystalCount: 0, samples: [scenario.text.slice(0, 48)] }),
    scenarios: scenarios("reflection-gate", routine, [
      "Several sessions show that stable identifiers preserve attribution across storage scopes.",
      "The same input bound protects both graph extraction and semantic consolidation from oversized context.",
      "Repeated module layouts predict where registrations, handlers, and contract tests must change together.",
      "Multiple maintenance tasks share a common rule: validate state before applying a durable effect.",
      "A shared confidence boundary explains why three independent maintenance flows preserve uncertain input.",
      "The same compatibility list governs export typing, import validation, and release assertions.",
    ]),
    currentThreshold: TYPESAFE_REFLECTION_GATE_CONFIDENCE_THRESHOLD,
  },
  {
    id: "skill-gate",
    feature: "pipelineGates",
    negativeChoice: "skip",
    positiveChoice: "run",
    instructions: "Should this completed session be analyzed for a reusable skill?",
    criteria: { run: "Run when the session appears to contain a repeatable procedure that would help future tasks.", skip: "Skip only when the session is clearly exploratory, routine, or unlikely to contain a reusable procedure." },
    state: (scenario) => jsonState({ workflow: "skill-extraction", summaryTitle: scenario.text.slice(0, 48), concepts: [], observationCount: scenario.expected === "negative" ? 3 : 8, observationTypes: ["other"], averageImportance: scenario.expected === "negative" ? 3 : 6, sampleTitles: [scenario.text.slice(0, 24)] }),
    scenarios: scenarios("skill-gate", routine, [
      "Register a tool definition, connect its handler, expose its route, and update the contract test.",
      "Build a fixture, invoke the function, inspect persisted state, and verify observable behavior.",
      "Validate configuration, exercise both flag states, and confirm the safe fallback path.",
      "Update the version type, supported import set, manifests, and release assertion together.",
      "Trace the registered function, prepare bounded state, execute it, and validate both result and storage.",
      "Change the feature setting, test enabled and disabled behavior, then document the operational fallback.",
    ]),
    currentThreshold: TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD,
  },
];

const thresholds = Array.from({ length: 10 }, (_, index) => Number((0.5 + index * 0.05).toFixed(2)));

async function mapLimit<T, R>(items: T[], concurrency: number, mapper: (item: T) => Promise<R>): Promise<R[]> {
  const results = new Array<R>(items.length);
  let cursor = 0;
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (cursor < items.length) {
      const index = cursor++;
      results[index] = await mapper(items[index]!);
    }
  }));
  return results;
}

function thresholdStats(samples: DecisionSample[], workflow: Workflow, threshold: number) {
  let trueNegative = 0;
  let falseNegative = 0;
  let truePositive = 0;
  let falsePositive = 0;
  for (const sample of samples) {
    const acts = sample.choice === workflow.negativeChoice && sample.confidence >= threshold;
    if (sample.expected === "negative") acts ? trueNegative++ : falseNegative++;
    else acts ? falsePositive++ : truePositive++;
  }
  return { threshold, trueNegative, falseNegative, truePositive, falsePositive, noiseRemoval: trueNegative / (trueNegative + falseNegative), falseDiscard: falsePositive / (truePositive + falsePositive) };
}

const runCalibration = process.env["RUN_TYPESAFE_CALIBRATION"] === "true";

describe.skipIf(!runCalibration)("TypeSafe paid threshold calibration", () => {
  it("calibrates every production decision family with repeated labeled cases", async () => {
    if (!getTypeSafeConfig().apiKey) throw new Error("TYPESAFE_API_KEY is required for calibration.");
    const provider = new TypeSafeDecisionProvider();
    const work = workflows.flatMap((workflow) => workflow.scenarios.flatMap((scenario) =>
      Array.from({ length: 2 }, (_, repeat) => ({ workflow, scenario, repeat })),
    ));
    const samples = await mapLimit(work, 3, async ({ workflow, scenario, repeat }): Promise<DecisionSample> => {
      let decision: TypeSafeChoiceAnswer | undefined;
      let score: TypeSafeScoreAnswer | undefined;
      if (workflow.admissionWithScore) {
        const questions: Record<string, TypeSafeQuestion> = {
          admission: { type: "choice", instructions: workflow.instructions, criteria: workflow.criteria },
          importance: {
            type: "score",
            instructions: "Score this read-only observation's durable importance to future work.",
            criteria: [
              "Routine, reproducible detail with little future value",
              "Low-value detail useful only as immediate context",
              "Minor project context or ordinary lookup result",
              "Some reusable detail, but limited durability",
              "Moderately useful project information",
              "Useful detail likely to help a later task",
              "Important project context or a recurring pattern",
              "High-value technical discovery",
              "Very important durable project knowledge",
              "Critical project constraint",
            ],
          },
        };
        const answers = await provider.evaluate("admission", JSON.parse(workflow.state(scenario)), questions);
        if (answers?.admission?.type === "choice") decision = answers.admission;
        if (answers?.importance?.type === "score") score = answers.importance;
      } else if (workflow.feature === "compaction") {
        const answers = await provider.evaluate("compaction", { candidates: [{ questionId: "candidate_0", state: workflow.state(scenario) }] }, {
          candidate_0: { type: "choice", instructions: workflow.instructions, criteria: workflow.criteria },
        });
        if (answers?.candidate_0?.type === "choice") decision = answers.candidate_0;
      } else {
        decision = await provider.evaluateChoice(workflow.feature, workflow.state(scenario), workflow.instructions, workflow.criteria);
      }
      if (!decision) throw new Error(`No valid decision for ${workflow.id}/${scenario.id}/${repeat}`);
      return { workflow: workflow.id, scenarioId: scenario.id, expected: scenario.expected, repeat, choice: decision.choice, confidence: decision.confidence, score: score?.score, scoreConfidence: score?.confidence, expectedScore: scenario.expectedScore };
    });

    const report = workflows.map((workflow) => {
      const own = samples.filter((sample) => sample.workflow === workflow.id);
      const stats = thresholds.map((threshold) => thresholdStats(own, workflow, threshold));
      const zeroFalseDiscards = stats.filter((item) => item.falsePositive === 0);
      const empirical = zeroFalseDiscards[0] ?? stats.at(-1)!;
      const current = thresholdStats(own, workflow, workflow.currentThreshold);
      const stable = workflow.scenarios.filter((scenario) => {
        const choices = new Set(own.filter((sample) => sample.scenarioId === scenario.id).map((sample) => sample.choice));
        return choices.size === 1;
      }).length / workflow.scenarios.length;
      return {
        workflow: workflow.id,
        samples: own.length,
        empiricalThreshold: empirical.threshold,
        currentThreshold: workflow.currentThreshold,
        empirical,
        current,
        choiceStability: stable,
        curve: stats,
      };
    });

    const scoring = samples.filter((sample) => sample.workflow === "admission" && sample.score !== undefined && sample.expectedScore !== undefined);
    const scoringByThreshold = thresholds.map((threshold) => {
      const accepted = scoring.filter((sample) => (sample.scoreConfidence ?? 0) >= threshold);
      const mae = accepted.length === 0 ? null : accepted.reduce((sum, sample) => sum + Math.abs(sample.score! - sample.expectedScore!), 0) / accepted.length;
      return { threshold, coverage: accepted.length / scoring.length, mae };
    });
    console.info("[typesafe-calibration]", JSON.stringify({ calls: work.length, decisions: report, scoring: scoringByThreshold }));

    expect(samples).toHaveLength(work.length);
    expect(report).toHaveLength(workflows.length);
    expect(scoring).toHaveLength(24);
  }, 300_000);
});
