import { createHash } from "node:crypto";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { countContextTokens } from "../utils/context-tokens.js";
import type { ContextUsageEvent, ContextUsageFeed } from "../types.js";
import { getAgentId } from "../config.js";
import { logger } from "../logger.js";

function validCandidateTokens(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value)
    && value >= 0 && value <= 1_000_000;
}

export async function recordContextUsage(kv: StateKV, sessionId: string, data: Record<string, unknown>): Promise<void> {
  if (typeof data.text !== "string" || data.text.length > 32_000
    || typeof data.eventId !== "string" || !/^[a-f0-9-]{36}$/.test(data.eventId)
    || !["session-start", "pre-tool-use", "pre-compact", "mcp-response"].includes(String(data.source))) {
    throw new Error("Invalid context emission metadata");
  }
  const textTokens = countContextTokens(data.text);
  const event: ContextUsageEvent = {
    id: data.eventId, timestamp: new Date().toISOString(),
    session: createHash("sha256").update(sessionId).digest("hex"),
    source: data.source as ContextUsageEvent["source"],
    characters: data.text.length, textTokens,
    ...(validCandidateTokens(data.candidateTokens) ? { candidateTokens: data.candidateTokens } : {}),
  };
  const key = `context-usage:${getAgentId() ?? "default"}`;
  const result = await kv.update<{ errors?: unknown }>(KV.contextUsage, key, [
    { type: "append_bounded", path: "items", value: { item: event, limit: 500 } },
  ]);
  if (Array.isArray(result?.errors) && result.errors.length) throw new Error("Context telemetry update rejected");
}

export async function readContextUsage(kv: StateKV) {
  const feed = await kv.get<ContextUsageFeed>(KV.contextUsage, `context-usage:${getAgentId() ?? "default"}`);
  const unique = new Map<string, ContextUsageEvent>();
  for (const event of feed?.items ?? []) {
    if (typeof event.id === "string" && Number.isSafeInteger(event.textTokens) && event.textTokens >= 0) unique.set(event.id, event);
  }
  const allEvents = [...unique.values()];
  const events = allEvents.filter((event) => event.source !== "mcp-response");
  const mcp = allEvents.filter((event) => event.source === "mcp-response");
  const sessions = new Map<string, number>();
  for (const event of events) sessions.set(event.session, (sessions.get(event.session) ?? 0) + event.textTokens);
  const totals = [...sessions.values()].sort((a, b) => a - b);
  const textTokens = events.reduce((sum, e) => sum + e.textTokens, 0);
  const paired = events.filter((e) => validCandidateTokens(e.candidateTokens));
  const candidateTokens = paired.reduce((sum, e) => sum + (e.candidateTokens ?? 0), 0);
  const pairedEmittedTokens = paired.reduce((sum, e) => sum + e.textTokens, 0);
  return {
    source: "hook-stdout-sample", tokenizer: "o200k_base", limit: 500,
    emissions: events.length, sessions: sessions.size, textTokens,
    mcpResponses: mcp.length, mcpTextTokens: mcp.reduce((sum, e) => sum + e.textTokens, 0),
    meanTokensPerObservedSession: totals.length ? textTokens / totals.length : null,
    medianTokensPerObservedSession: totals.length ? (totals[Math.floor((totals.length - 1) / 2)]! + totals[Math.floor(totals.length / 2)]!) / 2 : null,
    pairedEmissions: paired.length, candidateTokens, pairedEmittedTokens,
    selectionReductionTokens: candidateTokens - pairedEmittedTokens,
    oldest: events.map((e) => e.timestamp).sort()[0] ?? null,
    newest: events.map((e) => e.timestamp).sort().at(-1) ?? null,
  };
}

export function warnContextUsageFailure(): void {
  logger.warn("Context emission telemetry could not be recorded");
}
