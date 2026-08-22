import { describe, expect, it } from "vitest";
import { CASES_PER_TASK, LLM_ROUTING_CASES } from "../eval/fixtures/llm-routing-cases.js";
import { recommendRoute, scoreResponse, summarizeTaskProvider, type RawEvaluationResult } from "../eval/runner/llm-routing-scoring.js";

const testCase = LLM_ROUTING_CASES.find((candidate) => candidate.task === "compression")!;

describe("LLM routing evaluation", () => {
  it("defines fifteen demanding cases for each independently routed task", () => {
    expect(CASES_PER_TASK).toBe(15);
    const byTask = new Map<string, number>();
    for (const candidate of LLM_ROUTING_CASES) byTask.set(candidate.task, (byTask.get(candidate.task) ?? 0) + 1);
    expect([...byTask.values()]).toHaveLength(12);
    expect([...byTask.values()]).toEqual(Array(12).fill(15));
  });

  it("rewards schema-valid answers that preserve critical identifiers", () => {
    const response = JSON.stringify({ summary: "src/cache.ts uses cache_mode=write-through with zod", facts: ["src/cache.ts", "cache_mode=write-through", "zod"] });
    const result = scoreResponse(testCase, response);
    expect(result.valid).toBe(true);
    expect(result.semanticScore).toBe(100);
  });

  it("rejects malformed output, critical loss, and forbidden claims", () => {
    expect(scoreResponse(testCase, "not json").failureReason).toBe("invalid_json");
    expect(scoreResponse(testCase, JSON.stringify({ summary: "src/cache.ts", facts: ["src/cache.ts"] })).failureReason).toBe("critical_loss");
    expect(scoreResponse(testCase, JSON.stringify({ summary: "src/cache.ts uses webpack", facts: ["src/cache.ts", "cache_mode=write-through", "webpack"] })).failureReason).toBe("hallucination");
  });

  it("scores a valid JSON object even when a model adds text around it", () => {
    const response = `Here is the result:\n${JSON.stringify({ summary: "src/cache.ts uses cache_mode=write-through with zod", facts: ["src/cache.ts", "cache_mode=write-through", "zod"] })}\nDone.`;
    expect(scoreResponse(testCase, response).valid).toBe(true);
  });

  it("repairs syntax but still evaluates the recovered content", () => {
    const response = '{"summary":"src/cache.ts uses cache_mode\\=write-through with zod","facts":["src/cache.ts","cache_mode\\=write-through","zod"]}';
    const result = scoreResponse(testCase, response);
    expect(result.syntaxRepaired).toBe(true);
    expect(result.valid).toBe(true);
  });

  it("does not call a faithfully retained negated constraint a hallucination", () => {
    const response = JSON.stringify({
      summary: "Do not enable webpack.",
      facts: ["src/cache.ts", "cache_mode=write-through", "zod"],
    });
    expect(scoreResponse(testCase, response).forbiddenTerms).toEqual([]);
  });

  it("requires the correct direction for a consolidated replacement", () => {
    const conflictCase = LLM_ROUTING_CASES.find((candidate) => candidate.task === "consolidation" && candidate.id.endsWith("conflict"))!;
    const reversed = JSON.stringify({ facts: ["retention_days=30"], supersedes: ["retention_days=14"] });
    expect(scoreResponse(conflictCase, reversed).valid).toBe(false);
  });

  it("keeps auxiliary routing only for strict quality gates", () => {
    const results: RawEvaluationResult[] = Array.from({ length: 10 }, (_, index) => ({
      task: "compression", provider: "aux", latencyMs: index + 1, schemaValid: true, valid: true, semanticScore: 90,
      requiredTerms: 3, retainedTerms: 3, criticalTerms: 2, retainedCriticalTerms: 2, forbiddenTerms: [],
    }));
    const summary = summarizeTaskProvider(results)[0]!;
    expect(recommendRoute(summary)).toBe("aux");
    expect(recommendRoute({ ...summary, successful: 7 })).toBe("primary");
    expect(recommendRoute({ ...summary, total: 15, successful: 13 })).toBe("aux_with_fallback");
  });
});
