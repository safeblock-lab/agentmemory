import { Buffer } from "node:buffer";
import type { SummaryBudgetConfig } from "../types.js";
import type { SummaryPromptItem } from "../prompts/summary.js";
import { SummaryBudgetError, MAX_SUMMARY_RECOVERY_INPUT_BYTES, estimateSummaryTokens, summaryOutputTokenBudget } from "./summary-budget.js";
import { SUMMARY_SYSTEM, buildSummaryItemsPrompt } from "./summary-queue-prompts.js";

export interface FullOriginalSummaryRequest {
  systemPrompt: string;
  userPrompt: string;
  inputBytes: number;
  inputTokens: number;
  outputTokens: number;
}

export function prepareFullOriginalSummaryRequest(
  items: SummaryPromptItem[], config: SummaryBudgetConfig,
): FullOriginalSummaryRequest {
  const userPrompt = buildSummaryItemsPrompt(items);
  const inputBytes = Buffer.byteLength(JSON.stringify([SUMMARY_SYSTEM, userPrompt]), "utf8");
  if (inputBytes > MAX_SUMMARY_RECOVERY_INPUT_BYTES) {
    throw new SummaryBudgetError("summary_recovery_input_limit_exceeded");
  }
  const inputTokens = estimateSummaryTokens(SUMMARY_SYSTEM, userPrompt);
  const outputTokens = summaryOutputTokenBudget(config, SUMMARY_SYSTEM, userPrompt);
  return { systemPrompt: SUMMARY_SYSTEM, userPrompt, inputBytes, inputTokens, outputTokens };
}
