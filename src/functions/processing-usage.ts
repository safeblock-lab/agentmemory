import { randomUUID } from "node:crypto";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { getAgentId } from "../config.js";
import type { LlmUsage, ProcessingUsageEvent, ProcessingUsageFeed } from "../types.js";
import { priceSolUsage } from "./codex-usage.js";
import { logger } from "../logger.js";

const key = () => getAgentId() ?? "default";
export async function recordProcessingUsage(kv: StateKV, usage: LlmUsage, effectKey?: string): Promise<void> {
  const item: ProcessingUsageEvent = { id: effectKey ?? randomUUID(), timestamp: new Date().toISOString() };
  for (const field of ["inputTokens", "cachedInputTokens", "cacheWriteInputTokens", "outputTokens"] as const) {
    const value = usage[field];
    if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) item[field] = value;
  }
  try {
    const result = await kv.update<{ errors?: unknown }>(KV.processingUsage, key(), [
      { type: "append_bounded", path: "items", value: { item, limit: 1000 } },
    ]);
    if (Array.isArray(result?.errors) && result.errors.length) throw new Error("Usage append rejected");
  } catch {
    logger.warn("Processing usage sample could not be recorded");
  }
}

export async function readProcessingUsage(kv: StateKV) {
  const feed = await kv.get<ProcessingUsageFeed>(KV.processingUsage, key());
  const since = Date.now() - 7 * 86_400_000;
  const events = [...new Map((feed?.items ?? []).filter((e) => Date.parse(e.timestamp) >= since).map((e) => [e.id, e])).values()];
  let minUsd = 0, maxUsd = 0, unreportedCalls = 0, missingCacheCalls = 0;
  for (const e of events) {
    if (e.inputTokens === undefined || e.outputTokens === undefined) { unreportedCalls++; continue; }
    const writes = e.cacheWriteInputTokens ?? 0;
    if (writes > e.inputTokens || (e.cachedInputTokens ?? 0) + writes > e.inputTokens) { unreportedCalls++; continue; }
    if (e.cachedInputTokens === undefined || e.cacheWriteInputTokens === undefined) missingCacheCalls++;
    const cached = e.cachedInputTokens ?? 0;
    minUsd += priceSolUsage(e.inputTokens, e.cachedInputTokens ?? e.inputTokens - writes, writes, e.outputTokens);
    maxUsd += priceSolUsage(e.inputTokens, cached, e.cacheWriteInputTokens ?? e.inputTokens - cached, e.outputTokens);
  }
  return { limit: 1000, calls: events.length, unreportedCalls, missingCacheCalls, solEquivalentUsdRange: [minUsd, maxUsd] };
}
