import type { LlmCallOptions, LlmTask } from "../../src/types.js";

export function evaluationCallOptions(task: LlmTask, thinking: boolean | undefined): LlmCallOptions {
  return thinking === undefined ? { task } : { task, thinking };
}
