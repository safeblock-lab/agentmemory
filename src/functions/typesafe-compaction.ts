import { TYPESAFE_COMPACTION_CONFIDENCE_THRESHOLD } from "../config.js";

export type CompactionDisposition = "protected" | "deterministic-drop" | "ambiguous";

export type CompactionCandidate<T> =
  | {
      id: string;
      value: T;
      disposition: "protected" | "deterministic-drop";
    }
  | {
      id: string;
      value: T;
      disposition: "ambiguous";
      state: string;
    };

export interface CompactionDecisionInput {
  questionId: string;
  state: string;
}

export interface CompactionChoice {
  choice: "keep" | "drop";
  confidence: number;
}

export type CompactionDecider = (
  candidates: readonly CompactionDecisionInput[],
) => Promise<Readonly<Record<string, CompactionChoice>> | undefined>;

export interface CompactionOptions {
  maxCandidates?: number;
  maxStateChars?: number;
  dropThreshold?: number;
}

export type CompactionFallbackReason =
  | "no-decider"
  | "decision-error"
  | "decision-unavailable"
  | "candidate-limit"
  | "state-limit"
  | "empty-state"
  | "invalid-answer"
  | "low-confidence";

export interface CompactionSelection<T> {
  kept: readonly CompactionCandidate<T>[];
  dropped: readonly CompactionCandidate<T>[];
  fallbackReasons: readonly CompactionFallbackReason[];
}

interface ReviewCandidate<T> {
  index: number;
  candidate: CompactionCandidate<T> & { disposition: "ambiguous" };
  input: CompactionDecisionInput;
}

interface AmbiguousCandidate<T> {
  index: number;
  candidate: CompactionCandidate<T> & { disposition: "ambiguous" };
}

const DEFAULT_MAX_CANDIDATES = 16;
const HARD_MAX_CANDIDATES = 16;
const DEFAULT_MAX_STATE_CHARS = 16_000;
const HARD_MAX_STATE_CHARS = 64_000;
const DEFAULT_DROP_THRESHOLD = TYPESAFE_COMPACTION_CONFIDENCE_THRESHOLD;

function positiveLimit(value: number | undefined, fallback: number, maximum: number): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 1) return fallback;
  return Math.min(Math.floor(value), maximum);
}

function confidenceThreshold(value: number | undefined): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 && value <= 1
    ? value
    : DEFAULT_DROP_THRESHOLD;
}

export async function selectCompactionCandidates<T>(
  candidates: readonly CompactionCandidate<T>[],
  decide?: CompactionDecider,
  options: CompactionOptions = {},
): Promise<CompactionSelection<T>> {
  const maxCandidates = positiveLimit(
    options.maxCandidates,
    DEFAULT_MAX_CANDIDATES,
    HARD_MAX_CANDIDATES,
  );
  const maxStateChars = positiveLimit(
    options.maxStateChars,
    DEFAULT_MAX_STATE_CHARS,
    HARD_MAX_STATE_CHARS,
  );
  const dropThreshold = confidenceThreshold(options.dropThreshold);
  const fallbackReasons = new Set<CompactionFallbackReason>();
  const droppedIndexes = new Set<number>();
  const ambiguousCandidates: AmbiguousCandidate<T>[] = [];
  const reviewCandidates: ReviewCandidate<T>[] = [];

  for (let index = 0; index < candidates.length; index += 1) {
    const candidate = candidates[index]!;
    if (candidate.disposition === "deterministic-drop") {
      droppedIndexes.add(index);
      continue;
    }
    if (candidate.disposition === "ambiguous") {
      ambiguousCandidates.push({ index, candidate });
    }
  }

  for (const { index, candidate } of ambiguousCandidates) {
    const state = candidate.state.trim();
    if (!state) {
      fallbackReasons.add("empty-state");
      continue;
    }
    if (reviewCandidates.length >= maxCandidates) {
      fallbackReasons.add("candidate-limit");
      continue;
    }

    const input = {
      questionId: `candidate_${reviewCandidates.length}`,
      state,
    };
    const nextInputs = [...reviewCandidates.map((item) => item.input), input];
    if (JSON.stringify({ candidates: nextInputs }).length > maxStateChars) {
      fallbackReasons.add("state-limit");
      continue;
    }
    reviewCandidates.push({ index, candidate, input });
  }

  if (reviewCandidates.length > 0) {
    if (!decide) {
      fallbackReasons.add("no-decider");
    } else {
      let answers: Readonly<Record<string, CompactionChoice>> | undefined;
      try {
        answers = await decide(reviewCandidates.map((item) => item.input));
      } catch {
        fallbackReasons.add("decision-error");
      }

      if (!answers && !fallbackReasons.has("decision-error")) {
        fallbackReasons.add("decision-unavailable");
      }
      for (const item of reviewCandidates) {
        const answer = answers?.[item.input.questionId];
        if (
          !answer ||
          (answer.choice !== "keep" && answer.choice !== "drop") ||
          !Number.isFinite(answer.confidence) ||
          answer.confidence < 0 ||
          answer.confidence > 1
        ) {
          if (answers) fallbackReasons.add("invalid-answer");
          continue;
        }
        if (answer.choice === "drop") {
          if (answer.confidence >= dropThreshold) droppedIndexes.add(item.index);
          else fallbackReasons.add("low-confidence");
        }
      }
    }
  }

  return {
    kept: candidates.filter((_, index) => !droppedIndexes.has(index)),
    dropped: candidates.filter((_, index) => droppedIndexes.has(index)),
    fallbackReasons: [...fallbackReasons],
  };
}
