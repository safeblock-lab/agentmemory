import { describe, expect, it } from "vitest";
import { assessConsolidationComplexity } from "../src/functions/consolidation-complexity.js";

describe("assessConsolidationComplexity", () => {
  it("keeps ordinary prose on the auxiliary route", () => {
    expect(assessConsolidationComplexity({
      prompt: "The team decided to keep the existing cache strategy.",
      maxAuxiliaryInputChars: 100,
    })).toEqual({ complex: false, reasons: [] });
  });

  it("escalates conflicting structured values", () => {
    expect(assessConsolidationComplexity({
      prompt: "deployment_region=eu-west\ndeployment_region=us-east",
    })).toEqual({ complex: true, reasons: ["conflicting_structured_values"] });
  });

  it("escalates temporal conflict markers and auxiliary input limits", () => {
    expect(assessConsolidationComplexity({
      prompt: "This setting was superseded by a later decision.",
      maxAuxiliaryInputChars: 10,
    })).toEqual({
      complex: true,
      reasons: ["auxiliary_input_limit", "temporal_conflict_marker"],
    });
  });
});
