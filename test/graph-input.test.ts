import { describe, expect, it } from "vitest";
import {
  partitionGraphExtractionObservations,
  selectGraphExtractionObservations,
  splitGraphCompactionUnits,
} from "../src/functions/graph-input.js";
import type { CompressedObservation } from "../src/types.js";

function observation(id: string, type: CompressedObservation["type"], importance: number, narrative = id): CompressedObservation {
  return {
    id,
    sessionId: "session",
    timestamp: `2026-08-24T00:00:0${id.length}Z`,
    type,
    title: id,
    facts: [],
    narrative,
    concepts: [],
    files: [],
    importance,
  };
}

describe("graph extraction input selection", () => {
  it("retains every source observation, including exact-content duplicates", () => {
    const selected = selectGraphExtractionObservations([
      observation("read", "file_read", 2, "same"),
      { ...observation("duplicate", "file_read", 2, "same"), title: "read" },
      observation("decision", "decision", 8),
    ], 10_000);

    expect(selected.map((item) => item.id)).toEqual(["decision", "read", "duplicate"]);
  });

  it("partitions complete observations without cutting an oversized narrative", () => {
    const selected = selectGraphExtractionObservations([
      observation("large", "decision", 10, "x".repeat(10_000)),
      observation("small", "file_read", 1, "small"),
    ], 4_000);

    const partitions = partitionGraphExtractionObservations(selected, 4_000);
    expect(partitions.flat().map((item) => item.id).sort()).toEqual(["large", "small"]);
    expect(partitions.find((part) => part.some((item) => item.id === "large"))![0].narrative)
      .toBe("x".repeat(10_000));
  });

  it("keeps decisions, errors, and edits as verbatim compaction units", () => {
    const rows = [
      observation("decision", "decision", 8, "keep decision"),
      observation("error", "error", 8, "keep error"),
      observation("edit", "file_edit", 8, "keep edit"),
      observation("routine", "conversation", 1, "routine"),
    ];
    const units = splitGraphCompactionUnits(rows, 100);
    expect(units.slice(0, 3).flat().map((item) => item.id)).toEqual([
      "decision",
      "error",
      "edit",
    ]);
    expect(units.flat().map((item) => item.id).sort()).toEqual(
      rows.map((item) => item.id).sort(),
    );
  });
});
