import type { LlmTask } from "../../src/types.js";
import { jsonrepair } from "jsonrepair";
import type { LlmRoutingCase } from "../fixtures/llm-routing-cases.js";

export interface ScoredResponse {
  schemaValid: boolean;
  syntaxRepaired: boolean;
  valid: boolean;
  semanticScore: number;
  requiredTerms: number;
  retainedTerms: number;
  criticalTerms: number;
  retainedCriticalTerms: number;
  forbiddenTerms: string[];
  failureReason?: "empty" | "invalid_json" | "schema" | "required_loss" | "critical_loss" | "hallucination";
}

export interface TaskProviderSummary {
  task: LlmTask;
  provider: "aux" | "primary";
  total: number;
  successful: number;
  schemaValid: number;
  syntaxRepaired: number;
  meanSemanticScore: number;
  criticalRetentionRate: number;
  criticalHallucinations: number;
  averageLatencyMs: number;
}

export type RouteRecommendation = "aux" | "aux_with_fallback" | "primary";

export interface RawEvaluationResult extends ScoredResponse {
  task: LlmTask;
  provider: "aux" | "primary";
  latencyMs: number;
  response?: string;
  requestFailure?: "provider_request";
}

function normalize(value: string): string {
  return value.toLowerCase();
}

interface ParsedResponse {
  value: unknown;
  syntaxRepaired: boolean;
}

function parseJson(response: string): ParsedResponse | undefined {
  const trimmed = response.trim();
  const fenced = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i)?.[1] ?? trimmed;
  const first = fenced.indexOf("{");
  const last = fenced.lastIndexOf("}");
  const candidates = [fenced, first >= 0 && last > first ? fenced.slice(first, last + 1) : undefined];
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      return { value: JSON.parse(candidate), syntaxRepaired: false };
    } catch {
      // Try the next strict candidate before accepting a repair.
    }
  }
  for (const candidate of candidates) {
    if (!candidate || !candidate.trim().startsWith("{") || !candidate.trim().endsWith("}")) continue;
    try {
      return { value: JSON.parse(jsonrepair(candidate)), syntaxRepaired: true };
    } catch {
      // An object-shaped but unrepairable response remains invalid.
    }
  }
  return undefined;
}

function hasSchema(value: unknown, testCase: LlmRoutingCase): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const record = value as Record<string, unknown>;
  return Object.entries(testCase.schema).every(([key, kind]) => kind === "array" ? Array.isArray(record[key]) : typeof record[key] === "string");
}

export function scoreResponse(testCase: LlmRoutingCase, response: string): ScoredResponse {
  if (!response.trim()) return emptyResult("empty", testCase);
  const parsed = parseJson(response);
  if (parsed === undefined) return emptyResult("invalid_json", testCase);
  const schemaValid = hasSchema(parsed.value, testCase);
  const text = normalize(JSON.stringify(parsed.value));
  const retainedTerms = testCase.requiredTerms.filter((term) => text.includes(normalize(term))).length;
  const retainedCriticalTerms = testCase.criticalTerms.filter((term) => text.includes(normalize(term))).length;
  const forbiddenTerms = findAssertedForbiddenTerms(testCase.forbiddenTerms, text);
  const requiredLoss = retainedTerms < testCase.requiredTerms.length;
  const criticalLoss = retainedCriticalTerms < testCase.criticalTerms.length;
  const hallucination = forbiddenTerms.length > 0;
  const semanticScore = Math.round(
    (schemaValid ? 20 : 0)
      + (testCase.requiredTerms.length ? (retainedTerms / testCase.requiredTerms.length) * 55 : 55)
      + (testCase.criticalTerms.length ? (retainedCriticalTerms / testCase.criticalTerms.length) * 15 : 15)
      + (hallucination ? 0 : 10),
  );
  return {
    schemaValid,
    syntaxRepaired: parsed.syntaxRepaired,
    valid: schemaValid && !requiredLoss && !criticalLoss && !hallucination,
    semanticScore,
    requiredTerms: testCase.requiredTerms.length,
    retainedTerms,
    criticalTerms: testCase.criticalTerms.length,
    retainedCriticalTerms,
    forbiddenTerms,
    ...(hallucination ? { failureReason: "hallucination" } : criticalLoss ? { failureReason: "critical_loss" } : requiredLoss ? { failureReason: "required_loss" } : !schemaValid ? { failureReason: "schema" } : {}),
  };
}

function findAssertedForbiddenTerms(terms: readonly string[], response: string): string[] {
  return terms.filter((term) => {
    const normalizedTerm = normalize(term);
    let position = response.indexOf(normalizedTerm);
    while (position >= 0) {
      const preceding = response.slice(Math.max(0, position - 48), position);
      if (!/\b(?:do not|don't|never|not|no|without)\b[^.]{0,42}$/i.test(preceding)) return true;
      position = response.indexOf(normalizedTerm, position + normalizedTerm.length);
    }
    return false;
  });
}

function emptyResult(failureReason: "empty" | "invalid_json", testCase: LlmRoutingCase): ScoredResponse {
  return {
    schemaValid: false,
    syntaxRepaired: false,
    valid: false,
    semanticScore: 0,
    requiredTerms: testCase.requiredTerms.length,
    retainedTerms: 0,
    criticalTerms: testCase.criticalTerms.length,
    retainedCriticalTerms: 0,
    forbiddenTerms: [],
    failureReason,
  };
}

export function summarizeTaskProvider(results: readonly RawEvaluationResult[]): TaskProviderSummary[] {
  const groups = new Map<string, RawEvaluationResult[]>();
  for (const result of results) {
    const key = `${result.task}:${result.provider}`;
    groups.set(key, [...(groups.get(key) ?? []), result]);
  }
  return [...groups.values()].map((group) => {
    const first = group[0]!;
    const totalCritical = group.reduce((sum, result) => sum + result.criticalTerms, 0);
    return {
      task: first.task,
      provider: first.provider,
      total: group.length,
      successful: group.filter((result) => result.valid).length,
      schemaValid: group.filter((result) => result.schemaValid).length,
      syntaxRepaired: group.filter((result) => result.syntaxRepaired).length,
      meanSemanticScore: Math.round(group.reduce((sum, result) => sum + result.semanticScore, 0) / group.length),
      criticalRetentionRate: totalCritical === 0 ? 1 : group.reduce((sum, result) => sum + result.retainedCriticalTerms, 0) / totalCritical,
      criticalHallucinations: group.filter((result) => result.forbiddenTerms.length > 0).length,
      averageLatencyMs: Math.round(group.reduce((sum, result) => sum + result.latencyMs, 0) / group.length),
    };
  });
}

export function recommendRoute(auxiliary: TaskProviderSummary): RouteRecommendation {
  const successRate = auxiliary.total === 0 ? 0 : auxiliary.successful / auxiliary.total;
  if (successRate >= 0.9 && auxiliary.meanSemanticScore >= 85 && auxiliary.criticalRetentionRate >= 0.95 && auxiliary.criticalHallucinations === 0) return "aux";
  if (successRate >= 0.8 && auxiliary.meanSemanticScore >= 75 && auxiliary.criticalHallucinations === 0) return "aux_with_fallback";
  return "primary";
}
