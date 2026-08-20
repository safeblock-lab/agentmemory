import { loadConfig } from "../../src/config.js";
import { createAuxiliaryProvider, createProvider } from "../../src/providers/index.js";
import type { LlmTask, MemoryProvider } from "../../src/types.js";

interface EvaluationCase {
  task: LlmTask;
  operation: "compress" | "summarize";
  system: string;
  prompt: string;
  requiredTerms: string[];
}

interface EvaluationResult {
  task: LlmTask;
  provider: "primary" | "aux";
  model: string;
  latencyMs: number;
  requiredTerms: number;
  retainedTerms: number;
  valid: boolean;
}

const CASES: EvaluationCase[] = [
  {
    task: "compression",
    operation: "compress",
    system: "Return concise XML preserving every identifier.",
    prompt: "Decision: use cache_mode=write-through. Changed src/cache.ts. Command: npm test.",
    requiredTerms: ["cache_mode", "src/cache.ts", "npm test"],
  },
  {
    task: "graph_extraction",
    operation: "compress",
    system: "Return XML entities and relationships only.",
    prompt: "src/api.ts imports zod. zod validates request payloads.",
    requiredTerms: ["src/api.ts", "zod"],
  },
  {
    task: "summary",
    operation: "summarize",
    system: "Return a concise structured summary preserving exact facts.",
    prompt: "Repository agentmemory updated src/config.ts. Decision: keep embeddings separate from generative LLM routing.",
    requiredTerms: ["src/config.ts", "embeddings"],
  },
  {
    task: "entity_extraction",
    operation: "compress",
    system: "Extract entities exactly as written.",
    prompt: "Project agentmemory uses Ollama qwen3:4b and Fireworks DeepSeek.",
    requiredTerms: ["agentmemory", "qwen3:4b", "DeepSeek"],
  },
  {
    task: "classification",
    operation: "compress",
    system: "Classify the observation and preserve exact identifiers.",
    prompt: "Fixed bug #814 in src/functions/graph.ts.",
    requiredTerms: ["#814", "src/functions/graph.ts"],
  },
];

async function evaluateCase(
  testCase: EvaluationCase,
  provider: MemoryProvider,
  role: "primary" | "aux",
  model: string,
): Promise<EvaluationResult> {
  const startedAt = Date.now();
  const response = testCase.operation === "compress"
    ? await provider.compress(testCase.system, testCase.prompt)
    : await provider.summarize(testCase.system, testCase.prompt);
  const normalized = response.toLowerCase();
  const retainedTerms = testCase.requiredTerms.filter((term) =>
    normalized.includes(term.toLowerCase()),
  ).length;
  return {
    task: testCase.task,
    provider: role,
    model,
    latencyMs: Date.now() - startedAt,
    requiredTerms: testCase.requiredTerms.length,
    retainedTerms,
    valid: response.trim().length > 0,
  };
}

async function main(): Promise<void> {
  if (process.env["AGENTMEMORY_LLM_EVAL_ALLOW_PRIMARY"] !== "true") {
    throw new Error(
      "Refusing live primary evaluation. Set AGENTMEMORY_LLM_EVAL_ALLOW_PRIMARY=true to authorize paid calls.",
    );
  }
  const config = loadConfig();
  if (!config.auxiliaryProvider) {
    throw new Error("An auxiliary LLM configuration is required for comparison.");
  }
  const primary = createProvider(config.provider);
  const auxiliary = createAuxiliaryProvider(config.auxiliaryProvider);
  const results: EvaluationResult[] = [];
  for (const testCase of CASES) {
    results.push(
      await evaluateCase(testCase, auxiliary, "aux", config.auxiliaryProvider.model),
      await evaluateCase(testCase, primary, "primary", config.provider.model),
    );
  }
  process.stdout.write(`${JSON.stringify({ results }, null, 2)}\n`);
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : "LLM routing evaluation failed";
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
