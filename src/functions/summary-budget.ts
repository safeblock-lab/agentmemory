import { Buffer } from "node:buffer";
import type { SummaryBudgetConfig } from "../types.js";
import {
  SUMMARY_SYSTEM, REDUCE_SYSTEM, buildSummaryItemsPrompt, buildReduceItemsPrompt,
  type SummaryPromptItem,
} from "../prompts/summary.js";

export const MAX_SUMMARY_ITEMS = 4096;
export const MAX_SUMMARY_CALLS = 4096;
export const MAX_SUMMARY_DEPTH = 12;
// Covers role framing and the supported Ollama JSON output instruction/schema.
export const SUMMARY_ENVELOPE_TOKENS = 512;

export class SummaryBudgetError extends Error {}

export function estimateSummaryTokens(system: string, prompt: string): number {
  return Buffer.byteLength(JSON.stringify([system, prompt]), "utf8") + SUMMARY_ENVELOPE_TOKENS;
}

export function summaryInputLimit(config: SummaryBudgetConfig): number {
  const limit = config.contextTokens - config.outputTokens - config.safetyMarginTokens;
  for (const [system, prompt] of [
    [SUMMARY_SYSTEM, buildSummaryItemsPrompt([{ text: "", obsRangeStart: 1, obsRangeEnd: 1, fragment: true }])],
    [REDUCE_SYSTEM, buildReduceItemsPrompt([{ text: "", obsRangeStart: 1, obsRangeEnd: 1, fragment: true }])],
  ]) {
    if (estimateSummaryTokens(system, prompt) + 6 > limit) {
      throw new SummaryBudgetError("invalid_summary_budget: context cannot fit fixed prompts, envelope, output and margin");
    }
  }
  return limit;
}

export function isExplicitSummarySizeError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /context_length_exceeded|maximum context length|context (?:window|length).*(?:exceed|limit)|(?:exceed|limit).*context (?:window|length)|too many tokens|prompt (?:is )?too long|input tokens?.*(?:exceed|limit)|token limit exceeded/i.test(message);
}

function prefixEnd(text: string, maxBytes: number): number {
  let bytes = 0;
  let end = 0;
  for (const character of text) {
    const size = Buffer.byteLength(JSON.stringify(character), "utf8") - 2;
    if (bytes + size > maxBytes) break;
    bytes += size;
    end += character.length;
  }
  return end;
}

export function packSummaryItems(
  items: SummaryPromptItem[], system: string,
  buildPrompt: (items: SummaryPromptItem[]) => string,
  inputLimit: number, observationCap = MAX_SUMMARY_ITEMS,
): SummaryPromptItem[][] {
  const groups: SummaryPromptItem[][] = [];
  let group: SummaryPromptItem[] = [];
  let itemCount = 0;
  const append = (item: SummaryPromptItem): void => {
    if (++itemCount > MAX_SUMMARY_ITEMS) throw new SummaryBudgetError("summary_item_limit_exceeded");
    if (group.length && (group.length >= observationCap
      || estimateSummaryTokens(system, buildPrompt([...group, item])) > inputLimit)) {
      groups.push(group);
      group = [];
    }
    group.push(item);
  };
  for (const item of items) {
    if (estimateSummaryTokens(system, buildPrompt([item])) <= inputLimit) {
      append(item);
      continue;
    }
    const fragment = { ...item, text: "", fragment: true };
    const available = inputLimit - estimateSummaryTokens(system, buildPrompt([fragment]));
    let rest = item.text;
    while (rest.length) {
      const end = prefixEnd(rest, available);
      if (!end) throw new SummaryBudgetError("summary_fragment_cannot_fit");
      append({ ...fragment, text: rest.slice(0, end) });
      rest = rest.slice(end);
    }
  }
  if (group.length) groups.push(group);
  return groups;
}

export function summaryProgressSize(items: SummaryPromptItem[]): number {
  return items.reduce((total, item) => total + Buffer.byteLength(JSON.stringify(item.text), "utf8") + 64, 0);
}

export function smallerSummaryLimit(system: string, prompt: (items: SummaryPromptItem[]) => string, limit: number): number {
  const fixed = estimateSummaryTokens(system, prompt([{ text: "", obsRangeStart: 1, obsRangeEnd: 1, fragment: true }]));
  const next = fixed + Math.floor((limit - fixed) / 2);
  if (next <= fixed + 6 || next >= limit) throw new SummaryBudgetError("summary_context_limit_exhausted");
  return next;
}
