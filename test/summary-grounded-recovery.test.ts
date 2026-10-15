import { describe, expect, it } from "vitest";
import { parseSummaryBudgetConfig } from "../src/config.js";
import { prepareFullOriginalSummaryRequest } from "../src/functions/summary-recovery.js";
import { MAX_SUMMARY_RECOVERY_INPUT_BYTES, SummaryBudgetError } from "../src/functions/summary-budget.js";
import { formatSummaryObservation, type SummaryPromptItem } from "../src/prompts/summary.js";
import { buildReduceItemsPrompt, SUMMARY_RECOVERY_FIDELITY_VERSION } from "../src/functions/summary-queue-prompts.js";

describe("full-original summary recovery request", () => {
  it("builds a bounded request from complete chronological source observations", () => {
    const original = {
      type: "conversation", title: "Prior session configuration", narrative:
        "On August 23, the historical configuration set the context and output parameters.",
      facts: ["The current coherent session contains ingestion work only."],
      files: [".env.local", "scripts/Start-AgentMemory.ps1"], concepts: ["summary parameters"],
    };
    const items: SummaryPromptItem[] = [{ text: formatSummaryObservation(original, 1),
      obsRangeStart: 1, obsRangeEnd: 1, sourcePaths: original.files, modifiedPaths: [] }];
    const request = prepareFullOriginalSummaryRequest(items, parseSummaryBudgetConfig({}));

    expect(request.userPrompt).toContain("Prior session configuration");
    expect(request.userPrompt).toContain("historical configuration set the context and output parameters");
    expect(request.userPrompt).toContain(".env.local");
    expect(request.userPrompt).toContain("current coherent session contains ingestion work only");
    expect(request.userPrompt).not.toContain("Source paths supported as modified files");
    expect(request.systemPrompt).toContain("the enclosing session ID and capture time describe collection, not authorship");
    expect(request.systemPrompt).toMatch(/qualify missing or conflicting provenance/i);
    expect(request.systemPrompt).toContain("files supported as changed in the session being summarized");
    expect(request.systemPrompt).toContain("configuration, script propagation, provider selection, and outcomes as distinct");
    expect(request.inputBytes).toBe(Buffer.byteLength(JSON.stringify([request.systemPrompt, request.userPrompt]), "utf8"));
    expect(request.inputBytes).toBeLessThanOrEqual(MAX_SUMMARY_RECOVERY_INPUT_BYTES);
    expect(request.outputTokens).toBeGreaterThan(0);
  });

  it("rejects a full-original request beyond the dedicated byte ceiling", () => {
    const oversized: SummaryPromptItem[] = [{ text: "x".repeat(MAX_SUMMARY_RECOVERY_INPUT_BYTES),
      obsRangeStart: 1, obsRangeEnd: 1, sourcePaths: [], modifiedPaths: [] }];
    expect(() => prepareFullOriginalSummaryRequest(oversized, parseSummaryBudgetConfig({})))
      .toThrowError(new SummaryBudgetError("summary_recovery_input_limit_exceeded"));
  });

  it("keeps recovery fidelity identity separate and describes chronological supplied sources", () => {
    expect(SUMMARY_RECOVERY_FIDELITY_VERSION).toBe("summary-full-originals-v1");
    const prompt = buildReduceItemsPrompt([{ text: "historical partial", obsRangeStart: 1, obsRangeEnd: 1 }]);
    expect(prompt).toContain("supplied sources (chronological)");
    expect(prompt).not.toContain("one session");
    expect(prompt).not.toContain("continuous session");
  });
});
