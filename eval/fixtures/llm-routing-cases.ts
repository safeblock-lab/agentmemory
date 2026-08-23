import type { LlmTask } from "../../src/types.js";

export type EvaluationOperation = "compress" | "summarize";
export type ExpectedValueKind = "string" | "array";

export interface LlmRoutingCase {
  id: string;
  task: LlmTask;
  operation: EvaluationOperation;
  system: string;
  prompt: string;
  schema: Record<string, ExpectedValueKind>;
  requiredTerms: string[];
  criticalTerms: string[];
  forbiddenTerms: string[];
}

interface Scenario {
  id: string;
  input: string;
  terms: string[];
  critical: string[];
  forbidden: string[];
  classification: string;
  relation?: string;
}

interface TaskContract {
  operation: EvaluationOperation;
  schema: Record<string, ExpectedValueKind>;
  instruction: string;
}

const SCENARIOS: readonly Scenario[] = [
  { id: "code-import", input: "Repository comet-memory changed src/cache.ts. src/cache.ts imports zod. Decision: cache_mode=write-through.", terms: ["src/cache.ts", "zod", "cache_mode=write-through"], critical: ["src/cache.ts", "cache_mode=write-through"], forbidden: ["webpack"], classification: "decision", relation: "src/cache.ts|imports|zod" },
  { id: "service-config", input: "Service agentmemory uses Ollama at http://127.0.0.1:11434/v1 with qwen3:4b. The primary model is accounts/fireworks/models/nemotron-lightning-3p5-30b-a3b.", terms: ["agentmemory", "qwen3:4b", "nemotron-lightning-3p5-30b-a3b"], critical: ["qwen3:4b", "nemotron-lightning-3p5-30b-a3b"], forbidden: ["DeepSeek"], classification: "architecture", relation: "agentmemory|uses|qwen3:4b" },
  { id: "preference", input: "Marta prefers PowerShell on Windows and asks that production commands use rtk. Do not use Bash for Windows service operations.", terms: ["Marta", "PowerShell", "rtk", "Bash"], critical: ["PowerShell", "rtk"], forbidden: ["cmd.exe"], classification: "preference", relation: "Marta|prefers|PowerShell" },
  { id: "temporal-change", input: "On 2026-08-20, EMBEDDING_MODEL was qwen3-embedding:0.6b. On 2026-08-22, it remains qwen3-embedding:0.6b; only OPENAI_MODEL changed to Nemotron.", terms: ["2026-08-20", "2026-08-22", "qwen3-embedding:0.6b", "OPENAI_MODEL"], critical: ["qwen3-embedding:0.6b", "OPENAI_MODEL"], forbidden: ["embedding model changed"], classification: "fact", relation: "OPENAI_MODEL|changed_to|Nemotron" },
  { id: "conflict", input: "Older memory says retention_days=30. New approved configuration says retention_days=14. The newer approved value supersedes the older value.", terms: ["retention_days=30", "retention_days=14", "supersedes"], critical: ["retention_days=14"], forbidden: ["retention_days=60"], classification: "decision", relation: "retention_days=14|supersedes|retention_days=30" },
  { id: "command-result", input: "Command npm test -- test/llm-task-router.test.ts passed: 7 tests. The changed file is src/providers/task-router.ts.", terms: ["npm test", "7 tests", "src/providers/task-router.ts"], critical: ["7 tests", "src/providers/task-router.ts"], forbidden: ["8 tests"], classification: "command_run", relation: "npm test|validates|src/providers/task-router.ts" },
  { id: "package", input: "Package @agentmemory/agentmemory version 0.9.32 depends on iii-sdk 0.11.2. Its npm script eval:llm-routing uses tsx.", terms: ["@agentmemory/agentmemory", "0.9.32", "iii-sdk", "eval:llm-routing"], critical: ["0.9.32", "eval:llm-routing"], forbidden: ["0.9.33"], classification: "fact", relation: "@agentmemory/agentmemory|depends_on|iii-sdk" },
  { id: "security-boundary", input: "OPENAI_API_KEY is a secret and must never be logged. AgentMemory only listens on 127.0.0.1 ports 3111, 3112, 3113, and Ollama uses 11434.", terms: ["OPENAI_API_KEY", "127.0.0.1", "3111", "11434"], critical: ["OPENAI_API_KEY", "127.0.0.1"], forbidden: ["0.0.0.0"], classification: "architecture", relation: "AgentMemory|listens_on|127.0.0.1" },
  { id: "error-fix", input: "Bug #814 caused stale graph snapshots after mem::graph-reset. The fix marks old nodes as orphaned before graph extraction merges them.", terms: ["#814", "mem::graph-reset", "orphaned", "graph extraction"], critical: ["#814", "orphaned"], forbidden: ["deleted database"], classification: "bug", relation: "mem::graph-reset|causes|stale graph snapshots" },
  { id: "procedure", input: "Procedure: run npm run build, copy dist to the installed runtime, then start AgentMemory-Watchdog. Keep qwen3:4b warm with keep_alive=-1.", terms: ["npm run build", "AgentMemory-Watchdog", "qwen3:4b", "keep_alive=-1"], critical: ["AgentMemory-Watchdog", "keep_alive=-1"], forbidden: ["restart Windows"], classification: "workflow", relation: "npm run build|precedes|AgentMemory-Watchdog" },
  { id: "noisy-shell", input: "PowerShell output: WARNING: retry 1 ignored. npm.cmd run verify completed with exit code 0 after 428 tests. Do not record retry 1 as a failure. Artifact: packages/project-eval-skill/assets/global-orchestration.bundle.json.", terms: ["npm.cmd run verify", "exit code 0", "428 tests", "global-orchestration.bundle.json"], critical: ["exit code 0", "428 tests"], forbidden: ["exit code 1"], classification: "command_run", relation: "npm.cmd run verify|validates|global-orchestration.bundle.json" },
  { id: "quoted-config", input: "The literal config fragment is {\"AGENTMEMORY_SUMMARY_LLM\":\"aux\",\"AGENTMEMORY_FLOW_COMPRESSION_LLM\":\"aux\"}. Every other AGENTMEMORY_*_LLM route remains primary. This is deliberate, not a missing setting.", terms: ["AGENTMEMORY_SUMMARY_LLM", "aux", "AGENTMEMORY_FLOW_COMPRESSION_LLM", "primary"], critical: ["AGENTMEMORY_SUMMARY_LLM", "AGENTMEMORY_FLOW_COMPRESSION_LLM"], forbidden: ["all routes aux"], classification: "configuration", relation: "AGENTMEMORY_SUMMARY_LLM|routes_to|aux" },
  { id: "multi-session", input: "Session A selected qwen3:4b-nothink for local structured tasks. Session B measured one malformed escape and one fact omission in summary. The decision after review: keep summary on Qwen with Nemotron fallback; do not promote it to primary-only.", terms: ["Session A", "qwen3:4b-nothink", "Session B", "Nemotron fallback"], critical: ["qwen3:4b-nothink", "Nemotron fallback"], forbidden: ["primary-only"], classification: "decision", relation: "summary|falls_back_to|Nemotron" },
  { id: "temporal-contradiction", input: "At 09:00 the watchdog reported Ollama unavailable. At 09:02 it recovered and model agentmemory-qwen3:4b-nothink was resident. The final state is healthy; preserve both timestamps and do not claim the outage persists.", terms: ["09:00", "Ollama unavailable", "09:02", "agentmemory-qwen3:4b-nothink", "healthy"], critical: ["09:02", "healthy"], forbidden: ["outage persists"], classification: "fact", relation: "agentmemory-qwen3:4b-nothink|resident_at|09:02" },
  { id: "security-redaction", input: "Incident note: FIREWORKS_API_KEY was configured, but logs must say only 'credential configured'. The endpoint is https://api.fireworks.ai/inference/v1 and the model is accounts/fireworks/models/nemotron-lightning-3p5-30b-a3b. Never reproduce any key value.", terms: ["FIREWORKS_API_KEY", "credential configured", "api.fireworks.ai/inference/v1", "nemotron-lightning-3p5-30b-a3b"], critical: ["credential configured", "nemotron-lightning-3p5-30b-a3b"], forbidden: ["sk-live-"], classification: "security", relation: "FIREWORKS_API_KEY|configured_for|nemotron-lightning-3p5-30b-a3b" },
] as const;

const TASK_CONTRACTS: Record<LlmTask, TaskContract> = {
  graph_extraction: { operation: "compress", schema: { entities: "array", relations: "array" }, instruction: "Extract entities and directed relations. Encode every relation as source|predicate|target." },
  temporal_graph_extraction: { operation: "compress", schema: { entities: "array", relations: "array", timeline: "array" }, instruction: "Extract entities, directed relations, and temporal facts. Relations use source|predicate|target." },
  consolidation: { operation: "summarize", schema: { facts: "array", supersedes: "array" }, instruction: "Consolidate facts and state replacements explicitly. Encode each replacement as new|supersedes|old." },
  compression: { operation: "compress", schema: { summary: "string", facts: "array" }, instruction: "Compress without losing critical facts or identifiers." },
  summary: { operation: "summarize", schema: { summary: "string", facts: "array" }, instruction: "Summarize exact facts concisely." },
  entity_extraction: { operation: "compress", schema: { entities: "array" }, instruction: "Extract only named entities exactly as written." },
  classification: { operation: "compress", schema: { classification: "string", facts: "array" }, instruction: "Classify the observation using the evidence." },
  reflection: { operation: "summarize", schema: { lesson: "string", facts: "array" }, instruction: "Derive one evidence-backed lesson without inventing facts." },
  conflict_resolution: { operation: "summarize", schema: { resolution: "string", facts: "array" }, instruction: "Resolve conflicts only when the input states an ordering; retain both values." },
  skill_extraction: { operation: "summarize", schema: { skill: "string", steps: "array" }, instruction: "Extract an actionable procedure with evidence-backed steps." },
  query_expansion: { operation: "compress", schema: { queries: "array", entities: "array" }, instruction: "Provide intent-preserving search expansions and named entities." },
  flow_compression: { operation: "compress", schema: { flow: "array", facts: "array" }, instruction: "Compress the workflow into ordered steps while retaining facts." },
};

function systemPrompt(task: LlmTask, contract: TaskContract): string {
  const shape = Object.fromEntries(Object.entries(contract.schema).map(([key, kind]) => [key, kind === "array" ? ["..."] : "..."]));
  return [
    `You are performing AgentMemory task ${task}. ${contract.instruction}`,
    "Return ONLY one strict JSON object, with no markdown or prose outside JSON.",
    `Required JSON shape: ${JSON.stringify(shape)}.`,
    "Preserve exact identifiers, paths, versions, dates, commands, and values from the input.",
    "Do not add facts that are not stated in the input.",
  ].join(" ");
}

export const LLM_ROUTING_CASES: readonly LlmRoutingCase[] = (Object.entries(TASK_CONTRACTS) as Array<[LlmTask, TaskContract]>).flatMap(
  ([task, contract]) => SCENARIOS.map((scenario) => ({
    id: `${task}-${scenario.id}`,
    task,
    operation: contract.operation,
    system: systemPrompt(task, contract),
    prompt: `${scenario.input}\nFor classification, use ${scenario.classification} when applicable.`,
    schema: contract.schema,
    requiredTerms: [...scenario.terms, ...(scenario.relation && (task === "graph_extraction" || task === "temporal_graph_extraction") ? [scenario.relation] : []), ...(task === "consolidation" && scenario.id === "conflict" ? ["retention_days=14|supersedes|retention_days=30"] : []), ...(task === "classification" ? [scenario.classification] : [])],
    criticalTerms: scenario.critical,
    forbiddenTerms: scenario.forbidden,
  })),
);

export const CASES_PER_TASK = SCENARIOS.length;
