import { Buffer } from "node:buffer";
import type { SummaryBudgetConfig } from "../types.js";
import {
  SUMMARY_SYSTEM, REDUCE_SYSTEM, buildSummaryItemsPrompt, buildReduceItemsPrompt,
  type SummaryPromptItem,
} from "../prompts/summary.js";

export const MAX_SUMMARY_ITEMS = 4096;
export const MAX_SUMMARY_CALLS = 4096;
export const MAX_SUMMARY_DEPTH = 12;
export const MIN_SUMMARY_CHUNK_CONTENT_TOKENS = 500;
export const MIN_SUMMARY_REDUCE_CONTENT_TOKENS = 2000;
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

export function summaryChunkInputLimit(config: SummaryBudgetConfig): number {
  const limit = summaryInputLimit(config);
  const emptyItem: SummaryPromptItem = { text: "", obsRangeStart: 1, obsRangeEnd: 1, fragment: true };
  const fixedPromptTokens = Math.max(
    estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([emptyItem])),
    estimateSummaryTokens(REDUCE_SYSTEM, buildReduceItemsPrompt([emptyItem])),
  );
  const concurrencyTarget = Math.ceil(limit / Math.max(1, config.concurrency));
  const minimumContentTarget = fixedPromptTokens + MIN_SUMMARY_CHUNK_CONTENT_TOKENS;
  return Math.min(limit, Math.max(concurrencyTarget, minimumContentTarget));
}

export function summaryReduceInputLimit(config: SummaryBudgetConfig): number {
  const limit = summaryInputLimit(config);
  const emptyFragment: SummaryPromptItem = { text: "", obsRangeStart: 1, obsRangeEnd: 1, fragment: true };
  const fixedPromptTokens = estimateSummaryTokens(REDUCE_SYSTEM, buildReduceItemsPrompt([emptyFragment]));
  const parallelTarget = Math.ceil(limit / Math.min(4, Math.max(1, config.concurrency)));
  const minimumContentTarget = fixedPromptTokens + MIN_SUMMARY_REDUCE_CONTENT_TOKENS;
  return Math.min(limit, Math.max(parallelTarget, minimumContentTarget));
}

export function isExplicitSummarySizeError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /context_length_exceeded|maximum context length|context (?:window|length).*(?:exceed|limit)|(?:exceed|limit).*context (?:window|length)|too many tokens|prompt (?:is )?too long|input tokens?.*(?:exceed|limit)|token limit exceeded/i.test(message);
}

export function packSummaryItems(
  items: SummaryPromptItem[], system: string,
  buildPrompt: (items: SummaryPromptItem[]) => string,
  inputLimit: number, observationCap = MAX_SUMMARY_ITEMS,
): SummaryPromptItem[][] {
  if (!items.length) return [];
  if (items.length > MAX_SUMMARY_ITEMS) throw new SummaryBudgetError("summary_item_limit_exceeded");
  if (!Number.isSafeInteger(observationCap) || observationCap < 1) {
    throw new SummaryBudgetError("summary_observation_cap_invalid");
  }
  if (items.length === 1 && estimateSummaryTokens(system, buildPrompt(items)) <= inputLimit) return [[items[0]]];
  const fragmentBaseTokens = items.reduce((maximum, item) => Math.max(
    maximum, estimateSummaryTokens(system, buildPrompt([{ ...item, text: "", fragment: true }])),
  ), 0);
  const contentTokens = items.reduce((total, item) => total
    + Math.max(0, Buffer.byteLength(JSON.stringify(item.text), "utf8") - 2), 0);
  const countGroups = Math.ceil(items.length / observationCap);
  const perGroupContentLimit = Math.max(1, inputLimit - fragmentBaseTokens);
  const budgetGroups = Math.ceil(contentTokens / perGroupContentLimit);
  const balancedGroups = Math.max(1, countGroups, budgetGroups);
  const balancedContentTarget = Math.max(
    MIN_SUMMARY_CHUNK_CONTENT_TOKENS, Math.ceil(contentTokens / balancedGroups),
  );
  const balancedLimit = balancedGroups === 1 ? inputLimit : Math.min(inputLimit, Math.max(
    fragmentBaseTokens + 6, fragmentBaseTokens + balancedContentTarget,
  ));
  const groups: SummaryPromptItem[][] = [];
  let group: SummaryPromptItem[] = [];
  let itemCount = 0;
  const append = (item: SummaryPromptItem): void => {
    if (++itemCount > MAX_SUMMARY_ITEMS) throw new SummaryBudgetError("summary_item_limit_exceeded");
    group.push(item);
  };
  const flush = (): void => {
    if (!group.length) return;
    groups.push(group);
    group = [];
  };
  const largestFittingPrefix = (item: SummaryPromptItem): number => {
    const characters = Array.from(item.text);
    let low = 1;
    let high = characters.length;
    let best = 0;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const prefix = { ...item, text: characters.slice(0, middle).join(""), fragment: true };
      if (estimateSummaryTokens(system, buildPrompt([...group, prefix])) <= balancedLimit) {
        best = middle;
        low = middle + 1;
      } else high = middle - 1;
    }
    return best;
  };
  for (const item of items) {
    let pending = item;
    while (true) {
      if (group.length >= observationCap) flush();
      if (estimateSummaryTokens(system, buildPrompt([...group, pending])) <= balancedLimit) {
        append(pending);
        break;
      }
      if (group.length) {
        if (estimateSummaryTokens(system, buildPrompt([pending])) <= inputLimit) {
          flush();
          continue;
        }
        const end = largestFittingPrefix(pending);
        if (end >= Array.from(pending.text).length) {
          flush();
          continue;
        }
        if (end > 0) {
          const characters = Array.from(pending.text);
          append({ ...pending, text: characters.slice(0, end).join(""), fragment: true });
          pending = { ...pending, text: characters.slice(end).join(""), fragment: true };
        }
        flush();
        if (!end) continue;
        continue;
      }
      const end = largestFittingPrefix(pending);
      if (!end) throw new SummaryBudgetError("summary_fragment_cannot_fit");
      const characters = Array.from(pending.text);
      append({ ...pending, text: characters.slice(0, end).join(""), fragment: true });
      if (end >= characters.length) break;
      pending = { ...pending, text: characters.slice(end).join(""), fragment: true };
      flush();
    }
  }
  flush();
  for (const result of groups) {
    if (estimateSummaryTokens(system, buildPrompt(result)) > inputLimit) {
      throw new SummaryBudgetError("summary_prompt_exceeds_budget");
    }
  }
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
