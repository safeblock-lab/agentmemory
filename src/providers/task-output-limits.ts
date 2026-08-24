import type { FireworksBatchTask, LlmTask } from "../types.js";

const TASK_OUTPUT_TOKENS: Record<LlmTask, number> = {
  graph_extraction: 512,
  temporal_graph_extraction: 512,
  consolidation: 768,
  compression: 768,
  summary: 768,
  entity_extraction: 384,
  classification: 384,
  reflection: 1024,
  conflict_resolution: 1024,
  skill_extraction: 768,
  query_expansion: 384,
  flow_compression: 768,
};

export function taskOutputTokens(task: LlmTask | undefined, configuredMaximum: number): number {
  return task ? Math.min(configuredMaximum, TASK_OUTPUT_TOKENS[task]) : configuredMaximum;
}

export function batchTaskLlmTask(task: FireworksBatchTask): LlmTask {
  if (task === "graph_extraction") return "graph_extraction";
  if (task === "consolidation") return "consolidation";
  return "reflection";
}
