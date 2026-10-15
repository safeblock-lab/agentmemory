import { describe, expect, it } from "vitest";
import { parseTemporalGraphResult } from "../src/functions/temporal-result.js";

const capturedAt = "2026-10-12T00:00:00.000Z";

describe("temporal graph structural result contract", () => {
  it("accepts reordered attributes and preserves explicit temporal markers", () => {
    const result = parseTemporalGraphResult(`<temporal_graph>
      <entities><entity name="Ada" type="person"><alias>Engineer Ada</alias></entity><entity name="Acme" type="organization"/></entities>
      <relationships><relationship target="Acme" valid_to="current" weight="0.8" source="Ada" type="works_at" valid_from="unknown">
        <sentiment>positive</sentiment><reasoning>Joined the company</reasoning>
      </relationship></relationships>
    </temporal_graph>`, ["observation-a"], capturedAt);

    expect(result.kind).toBe("valid-result");
    if (result.kind !== "valid-result") throw new Error("expected non-empty temporal result");
    expect(result.nodes.map(({ name, sourceObservationIds }) => ({ name, sourceObservationIds }))).toEqual([
      { name: "Ada", sourceObservationIds: ["observation-a"] },
      { name: "Acme", sourceObservationIds: ["observation-a"] },
    ]);
    expect(result.edges[0]).toMatchObject({ type: "works_at", weight: 0.8, tcommit: capturedAt,
      context: { reasoning: "Joined the company", sentiment: "positive", confidence: 0.8 } });
    expect(result.edges[0]).not.toHaveProperty("tvalid");
    expect(result.edges[0]).not.toHaveProperty("tvalidEnd");
  });

  it("keeps a structurally valid empty result distinct from malformed nonempty output", () => {
    expect(parseTemporalGraphResult("<temporal_graph><relationships/><entities/></temporal_graph>", [], capturedAt))
      .toEqual({ kind: "valid-empty", nodes: [], edges: [] });
    expect(() => parseTemporalGraphResult("<temporal_graph><entities><entity name=\"lost\" type=\"unknown\"/></entities><relationships/></temporal_graph>", ["observation-a"], capturedAt))
      .toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult("<temporal_graph><entities><entity name=\"lost\" type=\"concept\"/></entities><relationships/></temporal_graph>", [], capturedAt))
      .toThrow("temporal_result_invalid");
  });

  it("rejects non-decimal or out-of-range weights and invalid calendar dates", () => {
    const response = (weight: string, validFrom = "2026-02-28") => `<temporal_graph><entities>
      <entity type="concept" name="A"/><entity type="concept" name="B"/>
      </entities><relationships><relationship type="related_to" source="A" target="B" weight="${weight}" valid_from="${validFrom}"/></relationships></temporal_graph>`;
    expect(() => parseTemporalGraphResult(response("0.0"), ["o"], capturedAt)).toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult(response("1.01"), ["o"], capturedAt)).toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult(response("wat"), ["o"], capturedAt)).toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult(response("0.8", "2026-02-30"), ["o"], capturedAt)).toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult(response("0.8", "current"), ["o"], capturedAt)).toThrow("temporal_result_invalid");
  });

  it("rejects ambiguous endpoints, duplicate singleton children, and invalid source IDs", () => {
    const ambiguous = `<temporal_graph><entities><entity type="concept" name="A"/><entity type="event" name="A"/><entity type="concept" name="B"/></entities>
      <relationships><relationship type="related_to" source="A" target="B" weight="0.8"/></relationships></temporal_graph>`;
    const duplicateReason = `<temporal_graph><entities><entity type="concept" name="A"/><entity type="concept" name="B"/></entities>
      <relationships><relationship type="related_to" source="A" target="B" weight="0.8"><reasoning/><reasoning/></relationship></relationships></temporal_graph>`;
    expect(() => parseTemporalGraphResult(ambiguous, ["o"], capturedAt)).toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult(duplicateReason, ["o"], capturedAt)).toThrow("temporal_result_invalid");
    expect(() => parseTemporalGraphResult("<temporal_graph><entities/><relationships/></temporal_graph>", [" ", "o"], capturedAt))
      .toThrow("temporal_result_invalid");
  });
});
