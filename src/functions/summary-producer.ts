import type { CompressedObservation, MemoryProvider, SessionSummary, SummaryBudgetConfig } from "../types.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import {
  SUMMARY_SYSTEM, REDUCE_SYSTEM, buildSummaryItemsPrompt, buildReduceItemsPrompt,
  formatSummaryObservation, formatSummaryPartial, type SummaryPromptItem,
} from "../prompts/summary.js";
import { logger } from "../logger.js";
import {
  SummaryBudgetError, MAX_SUMMARY_CALLS, MAX_SUMMARY_DEPTH, summaryInputLimit,
  summaryChunkInputLimit, summaryReduceInputLimit, estimateSummaryTokens, packSummaryItems, summaryProgressSize,
  isExplicitSummarySizeError, smallerSummaryLimit,
} from "./summary-budget.js";
import { parseSummaryXml } from "./summary-xml.js";

interface ProducedSummary {
  response: string;
  mode: "single" | "chunked";
  chunks: number;
  skipped?: number;
}

export function createSummaryProducer(
  provider: MemoryProvider, llmRouter: LlmTaskRouter | undefined,
  config: SummaryBudgetConfig, sessionId: string, project: string,
): (observations: CompressedObservation[]) => Promise<ProducedSummary> {
  const inputLimit = summaryInputLimit(config);
  const chunkInputLimit = summaryChunkInputLimit(config);
  const concurrency = Math.max(1, config.concurrency);
  let calls = 0;
  const call = async (system: string, prompt: string): Promise<string> => {
    if (estimateSummaryTokens(system, prompt) > inputLimit) throw new SummaryBudgetError("summary_prompt_exceeds_budget");
    const operation = (selected: MemoryProvider): Promise<string> => {
      if (++calls > MAX_SUMMARY_CALLS) throw new SummaryBudgetError("summary_call_limit_exceeded");
      return selected.summarize(system, prompt, { task: "summary", outputTokens: config.outputTokens });
    };
    return llmRouter
      ? llmRouter.run("summary", operation, candidate => parseSummaryXml(candidate, sessionId, project, 0) !== null)
      : operation(provider);
  };
  const partial = (summary: SessionSummary, items: SummaryPromptItem[]): SummaryPromptItem => ({
    text: formatSummaryPartial(summary),
    obsRangeStart: items[0].obsRangeStart,
    obsRangeEnd: items[items.length - 1].obsRangeEnd,
  });
  const map = async (items: SummaryPromptItem[], limit: number, depth: number): Promise<SummaryPromptItem[] | null> => {
    if (depth > MAX_SUMMARY_DEPTH) throw new SummaryBudgetError("summary_depth_limit_exceeded");
    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const response = await call(SUMMARY_SYSTEM, buildSummaryItemsPrompt(items));
        const summary = parseSummaryXml(response, sessionId, project, 0);
        if (summary) return [partial(summary, items)];
        logger.warn("Summarize chunk parse failed", { sessionId, attempt, items: items.length });
      } catch (error) {
        if (error instanceof SummaryBudgetError) throw error;
        if (isExplicitSummarySizeError(error)) {
          const nextLimit = smallerSummaryLimit(SUMMARY_SYSTEM, buildSummaryItemsPrompt, limit);
          const groups = packSummaryItems(items, SUMMARY_SYSTEM, buildSummaryItemsPrompt, nextLimit, config.chunkSize);
          logger.warn("Summarize context limit; subdividing", { sessionId, depth, groups: groups.length });
          const results: SummaryPromptItem[] = [];
          for (const group of groups) {
            const result = await map(group, nextLimit, depth + 1);
            if (!result) return null;
            results.push(...result);
          }
          return results;
        }
        logger.warn("Summarize chunk LLM call failed", { sessionId, attempt, reason: "provider_error" });
      }
    }
    return null;
  };
  const reduce = async (initial: SummaryPromptItem[]): Promise<string> => {
    let items = initial;
    let limit = summaryReduceInputLimit(config);
    for (let depth = 0; depth < MAX_SUMMARY_DEPTH; depth++) {
      const groups = packSummaryItems(items, REDUCE_SYSTEM, buildReduceItemsPrompt, limit);
      const next: SummaryPromptItem[] = [];
      let resized = false;
      if (groups.length === 1) {
        try {
          return await call(REDUCE_SYSTEM, buildReduceItemsPrompt(groups[0]));
        } catch (error) {
          if (error instanceof SummaryBudgetError || !isExplicitSummarySizeError(error)) throw error;
          limit = smallerSummaryLimit(REDUCE_SYSTEM, buildReduceItemsPrompt, limit);
          logger.warn("Summarize reduce context limit; subdividing", { sessionId, depth, groups: groups.length });
          continue;
        }
      }
      const summarizeGroup = async (group: SummaryPromptItem[]): Promise<SummaryPromptItem> => {
        for (let attempt = 1; attempt <= 2; attempt++) {
          const response = await call(REDUCE_SYSTEM, buildReduceItemsPrompt(group));
          const summary = parseSummaryXml(response, sessionId, project, 0);
          if (summary) return partial(summary, group);
        }
        throw new SummaryBudgetError("summary_reduce_parse_failed");
      };
      for (let start = 0; start < groups.length; start += concurrency) {
        const settled = await Promise.allSettled(groups.slice(start, start + concurrency).map(summarizeGroup));
        for (const result of settled) {
          if (result.status === "rejected") {
            const error: unknown = result.reason;
            if (error instanceof SummaryBudgetError || !isExplicitSummarySizeError(error)) throw error;
            limit = smallerSummaryLimit(REDUCE_SYSTEM, buildReduceItemsPrompt, limit);
            logger.warn("Summarize reduce context limit; subdividing", { sessionId, depth, groups: groups.length });
            resized = true;
            break;
          }
          next.push(result.value);
        }
        if (resized) break;
      }
      if (resized) continue;
      if (summaryProgressSize(next) >= summaryProgressSize(items)) {
        throw new SummaryBudgetError("summary_reduce_no_progress");
      }
      logger.info("Summarize reduce round", { sessionId, depth, inputItems: items.length, outputItems: next.length });
      items = next;
    }
    throw new SummaryBudgetError("summary_depth_limit_exceeded");
  };
  return async observations => {
    const items = observations.map((observation, index) => ({
      text: formatSummaryObservation(observation, index + 1), obsRangeStart: index + 1, obsRangeEnd: index + 1,
    }));
    let mapLimit = chunkInputLimit;
    let chunks = packSummaryItems(items, SUMMARY_SYSTEM, buildSummaryItemsPrompt, mapLimit, config.chunkSize);
    if (chunks.length === 1) {
      try {
        return { response: await call(SUMMARY_SYSTEM, buildSummaryItemsPrompt(chunks[0])), mode: "single", chunks: 1 };
      } catch (error) {
        if (!isExplicitSummarySizeError(error)) throw error;
        mapLimit = smallerSummaryLimit(SUMMARY_SYSTEM, buildSummaryItemsPrompt, mapLimit);
        chunks = packSummaryItems(items, SUMMARY_SYSTEM, buildSummaryItemsPrompt, mapLimit, config.chunkSize);
      }
    }
    logger.info("Summarize chunking session", {
      sessionId, chunks: chunks.length, concurrency: config.concurrency, totalObservations: observations.length,
    });
    const results: Array<SummaryPromptItem[] | null> = [];
    for (let start = 0; start < chunks.length; start += concurrency) {
      const settled = await Promise.allSettled(chunks.slice(start, start + concurrency)
        .map(chunk => map(chunk, mapLimit, 0)));
      for (const result of settled) {
        if (result.status === "rejected") throw result.reason;
        results.push(result.value);
      }
    }
    const skipped = results.filter(result => result === null).length;
    if (skipped > Math.floor(chunks.length * 0.5)) {
      throw new Error(`too_many_chunks_skipped: ${skipped}/${chunks.length} chunks failed to parse after retry`);
    }
    if (skipped) logger.warn("Summarize chunks partially skipped", { sessionId, skipped, total: chunks.length });
    const partials = results.flatMap(result => result ?? []);
    return { response: await reduce(partials), mode: "chunked", chunks: chunks.length, skipped };
  };
}
