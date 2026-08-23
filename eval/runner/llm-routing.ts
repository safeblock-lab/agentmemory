import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { loadConfig } from "../../src/config.js";
import { createAuxiliaryProvider, createProvider } from "../../src/providers/index.js";
import type { MemoryProvider } from "../../src/types.js";
import { CASES_PER_TASK, LLM_ROUTING_CASES, type LlmRoutingCase } from "../fixtures/llm-routing-cases.js";
import { recommendRoute, scoreResponse, summarizeTaskProvider, type RawEvaluationResult } from "./llm-routing-scoring.js";

type ProviderSelection = "aux" | "primary" | "both";

function reportPath(): string {
  const explicit = process.argv.find((value) => value.startsWith("--report="))?.slice("--report=".length);
  return resolve(explicit || `eval/reports/llm-routing-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);
}

function providerSelection(): ProviderSelection {
  const value = process.argv.find((argument) => argument.startsWith("--provider="))?.slice("--provider=".length) ?? "both";
  if (value === "aux" || value === "primary" || value === "both") return value;
  throw new Error("--provider must be aux, primary, or both.");
}

async function evaluateCase(testCase: LlmRoutingCase, provider: MemoryProvider, role: "aux" | "primary"): Promise<RawEvaluationResult> {
  const startedAt = Date.now();
  try {
    const response = testCase.operation === "compress"
      ? await provider.compress(testCase.system, testCase.prompt, { task: testCase.task })
      : await provider.summarize(testCase.system, testCase.prompt, { task: testCase.task });
    return { task: testCase.task, provider: role, latencyMs: Date.now() - startedAt, response, ...scoreResponse(testCase, response) };
  } catch {
    return {
      task: testCase.task,
      provider: role,
      latencyMs: Date.now() - startedAt,
      requestFailure: "provider_request",
      schemaValid: false,
      syntaxRepaired: false,
      valid: false,
      semanticScore: 0,
      requiredTerms: testCase.requiredTerms.length,
      retainedTerms: 0,
      criticalTerms: testCase.criticalTerms.length,
      retainedCriticalTerms: 0,
      forbiddenTerms: [],
      failureReason: "empty",
    };
  }
}

async function main(): Promise<void> {
  const selected = providerSelection();
  const calls = LLM_ROUTING_CASES.length * (selected === "both" ? 2 : 1);
  if ((selected === "primary" || selected === "both") && process.env["AGENTMEMORY_LLM_EVAL_ALLOW_PRIMARY"] !== "true") {
    throw new Error("Refusing live primary evaluation. Set AGENTMEMORY_LLM_EVAL_ALLOW_PRIMARY=true to authorize the bounded paid comparison.");
  }
  if (CASES_PER_TASK !== 15 || LLM_ROUTING_CASES.length !== 180) {
    throw new Error(`Evaluation contract drifted: expected 15 cases per task and 180 fixtures, got ${CASES_PER_TASK} and ${LLM_ROUTING_CASES.length}.`);
  }
  const config = loadConfig();
  if (!config.auxiliaryProvider) throw new Error("An auxiliary LLM configuration is required for comparison.");
  const primary = selected === "aux" ? undefined : createProvider(config.provider);
  const auxiliary = selected === "primary" ? undefined : createAuxiliaryProvider(config.auxiliaryProvider);
  const results: RawEvaluationResult[] = [];
  for (const testCase of LLM_ROUTING_CASES) {
    if (auxiliary) results.push(await evaluateCase(testCase, auxiliary, "aux"));
    if (primary) results.push(await evaluateCase(testCase, primary, "primary"));
  }
  const summaries = summarizeTaskProvider(results);
  const recommendations = Object.fromEntries(summaries.filter((summary) => summary.provider === "aux").map((summary) => [summary.task, recommendRoute(summary)]));
  const report = {
    generatedAt: new Date().toISOString(),
    contract: { casesPerTask: CASES_PER_TASK, providerCalls: calls, fallbackCalls: 0, selectedProvider: selected },
    models: { auxiliary: config.auxiliaryProvider.model, primary: config.provider.model },
    summaries,
    recommendations,
    results,
  };
  const target = reportPath();
  await mkdir(dirname(target), { recursive: true });
  await writeFile(target, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  process.stdout.write(`${JSON.stringify({ report: target, summaries, recommendations }, null, 2)}\n`);
}

main().catch((error: unknown) => {
  process.stderr.write(`${error instanceof Error ? error.message : "LLM routing evaluation failed"}\n`);
  process.exitCode = 1;
});
