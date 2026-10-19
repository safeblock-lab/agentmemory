import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { getAgentId, isAgentScopeIsolated } from "../config.js";
import { readAgentUsage } from "./agent-usage.js";
import type { AgentUsageSnapshot } from "./agent-usage.js";
import type { ContextUsageEvent, ProcessingUsageEvent } from "../types.js";

export interface EvidencePeriod { since: string; until: string; checkedAt: string }
type FeedStatus = "available" | "unavailable" | "invalid";
type FeedSample<T> = {
  status: FeedStatus; limit: number; retainedEvents: number; inPeriodEvents: number;
  retainedLimitReached: boolean;
  invalidEvents: number; duplicateEvents: number; oldest: string | null; newest: string | null;
  coverage: "bounded-retained-sample"; limits: string[]; events: T[];
};
const tokenCount = (value: unknown): value is number => typeof value === "number"
  && Number.isSafeInteger(value) && value >= 0;
const record = (value: unknown): value is Record<string, unknown> => value !== null
  && typeof value === "object" && !Array.isArray(value);

function sample<T extends { id: string; timestamp: string }>(value: unknown, period: EvidencePeriod,
  valid: (item: unknown) => item is T): FeedSample<T> {
  const items = record(value) && Array.isArray(value.items) ? value.items : null;
  const result: FeedSample<T> = {
    status: value == null ? "unavailable" : items ? "available" : "invalid", limit: 500,
    retainedEvents: items?.length ?? 0, inPeriodEvents: 0, invalidEvents: 0, duplicateEvents: 0,
    retainedLimitReached: (items?.length ?? 0) >= 500,
    oldest: null, newest: null, coverage: "bounded-retained-sample", events: [],
    limits: ["At most 500 retained events per configured agent key; missing events cannot be reconstructed.",
      "Retention coverage and capture start are unknown; an empty sample does not prove zero activity."],
  };
  const unique = new Map<string, T>();
  for (const item of (items ?? []).slice(-500)) {
    if (!valid(item)) { result.invalidEvents++; continue; }
    const time = Date.parse(item.timestamp);
    if (!Number.isFinite(time)) { result.invalidEvents++; continue; }
    if (time < Date.parse(period.since) || time > Date.parse(period.until)) continue;
    if (unique.has(item.id)) result.duplicateEvents++;
    unique.set(item.id, item);
  }
  result.events = [...unique.values()];
  result.inPeriodEvents = result.events.length;
  const times = result.events.map(event => event.timestamp).sort();
  result.oldest = times[0] ?? null; result.newest = times.at(-1) ?? null;
  return result;
}

type ContextEvidenceEvent = Pick<ContextUsageEvent, "id" | "timestamp" | "session" | "source" | "textTokens" | "candidateTokens">;
function contextEvent(value: unknown): value is ContextEvidenceEvent {
  return record(value) && typeof value.id === "string" && value.id.length > 0
    && typeof value.timestamp === "string" && typeof value.session === "string"
    && ["session-start", "pre-tool-use", "pre-compact", "mcp-response"].includes(String(value.source))
    && tokenCount(value.textTokens);
}
function processingEvent(value: unknown): value is ProcessingUsageEvent {
  return record(value) && typeof value.id === "string" && value.id.length > 0
    && typeof value.timestamp === "string"
    && ["inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteInputTokens"]
      .every(field => value[field] === undefined || tokenCount(value[field]));
}
function total(values: number[]): number | null {
  const sum = values.reduce((a, b) => a + b, 0);
  return Number.isSafeInteger(sum) ? sum : null;
}

export function aggregateSavingsFeeds(contextFeed: unknown, processingFeed: unknown, period: EvidencePeriod) {
  if (!Number.isFinite(Date.parse(period.since)) || !Number.isFinite(Date.parse(period.until))
    || Date.parse(period.since) > Date.parse(period.until)) throw new Error("Invalid savings evidence period");
  const contextSample = sample(contextFeed, period, contextEvent);
  const processingSample = sample(processingFeed, period, processingEvent);
  const { events: contextEvents, ...contextCoverage } = contextSample;
  const { events: processingEvents, ...processingCoverage } = processingSample;
  const emissions = contextEvents.filter(event => event.source !== "mcp-response");
  const replies = contextEvents.filter(event => event.source === "mcp-response");
  const candidates = emissions.filter(event => tokenCount(event.candidateTokens) && event.candidateTokens <= 1_000_000);
  const contextTotal = (values: number[]) => contextSample.status === "available" ? total(values) : null;
  const candidateTokens = contextTotal(candidates.map(event => event.candidateTokens!));
  const pairedEmittedTokens = contextTotal(candidates.map(event => event.textTokens));
  const reported = processingEvents.filter(event => event.inputTokens !== undefined && event.outputTokens !== undefined
    && (event.cachedInputTokens ?? 0) + (event.cacheWriteInputTokens ?? 0) <= event.inputTokens);
  const processingTotal = (field: "inputTokens" | "outputTokens" | "cachedInputTokens" | "cacheWriteInputTokens") =>
    processingSample.status === "available" && reported.length === processingEvents.length
      && reported.every(event => event[field] !== undefined) ? total(reported.map(event => event[field]!)) : null;
  return {
    context: { ...contextCoverage, emissions: emissions.length, textTokens: contextTotal(emissions.map(event => event.textTokens)),
      mcpResponses: replies.length, mcpTextTokens: contextTotal(replies.map(event => event.textTokens)),
      candidateEvents: candidates.length, candidateTokens, pairedEmittedTokens,
      selectionReductionTokens: candidateTokens === null || pairedEmittedTokens === null ? null : candidateTokens - pairedEmittedTokens,
      tokenizer: "o200k_base", baselineKind: "eligible-memory-selection-only" },
    processing: { ...processingCoverage, calls: processingEvents.length, reportedCalls: reported.length,
      missingProvenanceCalls: processingEvents.filter(event => !event.provider || !event.model || !event.task).length,
      missingUsageCalls: processingEvents.length - reported.length,
      missingCacheCalls: processingEvents.filter(event => event.cachedInputTokens === undefined || event.cacheWriteInputTokens === undefined).length,
      inputTokens: processingTotal("inputTokens"), outputTokens: processingTotal("outputTokens"),
      cachedInputTokens: processingTotal("cachedInputTokens"), cacheWriteInputTokens: processingTotal("cacheWriteInputTokens"),
      reportedInputTokens: processingSample.status === "available" ? total(reported.map(event => event.inputTokens!)) : null,
      reportedOutputTokens: processingSample.status === "available" ? total(reported.map(event => event.outputTokens!)) : null,
      overlapWithAgentUsage: "unknown" as const, disjointTokens: null },
  };
}

export async function readSavingsEvidence(kv: StateKV, suppliedAgentUsage?: AgentUsageSnapshot | null) {
  const checkedAt = new Date().toISOString();
  const agentUsage = isAgentScopeIsolated() ? null : suppliedAgentUsage === undefined
    ? await readAgentUsage().catch(() => null) : suppliedAgentUsage;
  const period: EvidencePeriod = { since: agentUsage?.since ?? new Date(Date.parse(checkedAt) - 7 * 86_400_000).toISOString(),
    until: agentUsage?.until ?? checkedAt, checkedAt };
  const key = getAgentId() ?? "default";
  const feeds = await Promise.allSettled([
    kv.get<unknown>(KV.contextUsage, `context-usage:${key}`), kv.get<unknown>(KV.processingUsage, key),
  ]);
  const evidence = aggregateSavingsFeeds(feeds[0].status === "fulfilled" ? feeds[0].value : null,
    feeds[1].status === "fulfilled" ? feeds[1].value : null, period);
  return {
    period, scope: "available local Codex/Claude profiles; AM telemetry for configured agent key; no project or team attribution",
    agentUsage, ...evidence,
    comparableBaseline: { status: "unavailable" as const, pairs: 0, avoidedReadTokens: null,
      reason: "Retained telemetry has no task/artifact/version/content-required identity linking a retrieval to a comparable without-memory read." },
    limitations: [
      "Profile collectors deduplicate response/message IDs within each client; no cross-client provider request identity is retained.",
      "Agent input includes cache reads and writes; actual total is input plus output, never input plus cached input plus output.",
      "Profile scans are bounded; preserve each client partial/unavailable status and collector limits.",
      "AM processing events lack provider request IDs, model and tariff metadata; overlap with agent usage cannot be subtracted or added safely.",
      "Candidate memory is not avoided file reads; selection reduction may be negative when formatting expands emitted context.",
      "Context tokens estimate one emission, not model consumption or repeated prompt exposure; MCP replies are separate.",
      "A comparable baseline and disjoint matching-period processing are required for measured net savings.",
      ...feeds.flatMap((feed, index) => feed.status === "rejected" ? [`${index === 0 ? "Context" : "Processing"} StateModule read failed; sample unavailable.`] : []),
      ...(!agentUsage ? ["Local agent usage unavailable or agent isolation prevents attribution."] : []),
    ],
  };
}
export type SavingsEvidence = Awaited<ReturnType<typeof readSavingsEvidence>>;
