import { describe, expect, it, vi } from "vitest";
import {
  selectCompactionCandidates,
  type CompactionCandidate,
} from "../src/functions/typesafe-compaction.js";

function protectedItem(id: string): CompactionCandidate<string> {
  return { id, value: id, disposition: "protected" };
}

function deterministicDrop(id: string): CompactionCandidate<string> {
  return { id, value: id, disposition: "deterministic-drop" };
}

function ambiguous(id: string, state = `state for ${id}`): CompactionCandidate<string> {
  return { id, value: id, disposition: "ambiguous", state };
}

function ids(candidates: readonly CompactionCandidate<string>[]): string[] {
  return candidates.map((candidate) => candidate.id);
}

describe("selectCompactionCandidates", () => {
  it("keeps protected candidates and applies deterministic drops before TypeSafe", async () => {
    const decide = vi.fn(async () => ({
      candidate_0: { choice: "keep" as const, confidence: 0.99 },
    }));
    const candidates = [
      ambiguous("reviewed"),
      protectedItem("decision"),
      deterministicDrop("duplicate"),
    ];

    const result = await selectCompactionCandidates(candidates, decide);

    expect(decide).toHaveBeenCalledOnce();
    expect(decide).toHaveBeenCalledWith([
      { questionId: "candidate_0", state: "state for reviewed" },
    ]);
    expect(ids(result.kept)).toEqual(["reviewed", "decision"]);
    expect(ids(result.dropped)).toEqual(["duplicate"]);
    expect(result.fallbackReasons).toEqual([]);
  });

  it("drops only high-confidence ambiguous candidates and fails open otherwise", async () => {
    const candidates = [ambiguous("certain"), ambiguous("uncertain"), ambiguous("missing")];
    const result = await selectCompactionCandidates(candidates, async () => ({
      candidate_0: { choice: "drop", confidence: 0.9 },
      candidate_1: { choice: "drop", confidence: 0.84 },
    }));

    expect(ids(result.kept)).toEqual(["uncertain", "missing"]);
    expect(ids(result.dropped)).toEqual(["certain"]);
    expect(result.fallbackReasons).toContain("low-confidence");
    expect(result.fallbackReasons).toContain("invalid-answer");
  });

  it("bounds the single decision batch and keeps overflow candidates", async () => {
    const decide = vi.fn(async () => ({
      candidate_0: { choice: "drop" as const, confidence: 0.99 },
    }));
    const result = await selectCompactionCandidates(
      [
        ambiguous("first"),
        ambiguous("second"),
        protectedItem("decision"),
        deterministicDrop("duplicate"),
      ],
      decide,
      { maxCandidates: 1 },
    );

    expect(decide).toHaveBeenCalledOnce();
    expect(ids(result.dropped)).toEqual(["first", "duplicate"]);
    expect(ids(result.kept)).toEqual(["second", "decision"]);
    expect(result.fallbackReasons).toContain("candidate-limit");
  });

  it("does not send candidates that exceed the serialized-state limit", async () => {
    const decide = vi.fn(async () => ({
      candidate_0: { choice: "drop" as const, confidence: 0.99 },
    }));
    const result = await selectCompactionCandidates(
      [ambiguous("large", "x".repeat(80)), ambiguous("small", "small")],
      decide,
      { maxStateChars: 64 },
    );

    expect(decide).toHaveBeenCalledOnce();
    expect(decide).toHaveBeenCalledWith([
      { questionId: "candidate_0", state: "small" },
    ]);
    expect(ids(result.dropped)).toEqual(["small"]);
    expect(ids(result.kept)).toEqual(["large"]);
    expect(result.fallbackReasons).toContain("state-limit");
  });

  it("preserves ambiguous candidates when the decision call fails", async () => {
    const result = await selectCompactionCandidates(
      [ambiguous("keep-me")],
      async () => {
        throw new Error("provider unavailable");
      },
    );

    expect(ids(result.kept)).toEqual(["keep-me"]);
    expect(result.dropped).toEqual([]);
    expect(result.fallbackReasons).toEqual(["decision-error"]);
  });

  it("preserves ambiguous candidates when no decision callback is available", async () => {
    const result = await selectCompactionCandidates([ambiguous("keep-me")]);

    expect(ids(result.kept)).toEqual(["keep-me"]);
    expect(result.dropped).toEqual([]);
    expect(result.fallbackReasons).toEqual(["no-decider"]);
  });
});
