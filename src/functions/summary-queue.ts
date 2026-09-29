import type { IIIClient } from "iii-sdk";
import type {
  CompressedObservation, MemoryProvider, Session, SessionSummary,
  SummaryQueueIntent, SummaryQueueJob,
} from "../types.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId, generateId } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { getSummaryBudgetConfig } from "../config.js";
import { recordAudit } from "./audit.js";
import { SummaryOutputSchema } from "../eval/schemas.js";
import { validateOutput } from "../eval/validator.js";
import { scoreSummary } from "../eval/quality.js";
import { logger } from "../logger.js";
import {
  REDUCE_SYSTEM, SUMMARY_SYSTEM, buildReduceItemsPrompt, buildSummaryItemsPrompt,
  formatSummaryObservation, formatSummaryPartial, type SummaryPromptItem,
} from "../prompts/summary.js";
import {
  MAX_SUMMARY_CALLS, MAX_SUMMARY_DEPTH, SummaryBudgetError, estimateSummaryTokens,
  packSummaryItems, summaryCallInputLimit, summaryChunkInputLimit,
  summaryOutputTokenBudget, summaryProgressSize, summaryReduceInputLimit,
} from "./summary-budget.js";
import { parseSummaryXml } from "./summary-xml.js";

const DISPATCH_TOPIC = "agentmemory.summary.dispatch";
const UNIT_TOPIC = "agentmemory.summary.unit";
const DISPATCH_BATCH = 12;
const MAX_ATTEMPTS = 6; // Five subscriber retries plus the first delivery.
const FAILED_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_RECOVERY_INTENTS = 100;
const STALE_DELIVERY_MS = 60_000;
const RETRY_BACKOFF_MS = 900_000;

function maxUnitRuntimeMs(): number {
  const configured = Number.parseInt(
    process.env.OPENAI_TIMEOUT_MS ?? process.env.AGENTMEMORY_LLM_TIMEOUT_MS ?? "300000", 10);
  return Number.isSafeInteger(configured) && configured > 0
    ? Math.max(600_000, configured + STALE_DELIVERY_MS)
    : 600_000;
}

function retryEligible(unit: SummaryQueueUnit, now: number): boolean {
  if (unit.attempts === 0) return true;
  if (!unit.lastAttemptAt || unit.attempts >= MAX_ATTEMPTS) return false;
  const due = Date.parse(unit.lastAttemptAt) + RETRY_BACKOFF_MS * 2 ** (unit.attempts - 1);
  return Number.isFinite(due) && now >= due + STALE_DELIVERY_MS;
}

interface SummaryQueueUnit {
  id: string;
  jobId: string;
  stage: "map" | "reduce";
  round: number;
  items: SummaryPromptItem[];
  attempts: number;
  dispatchedAt?: string;
  startedAt?: string;
  lastAttemptAt?: string;
  output?: SummaryPromptItem;
  summary?: SessionSummary;
  lastError?: string;
}

interface ActiveJob { jobId: string }
interface CompletedSnapshot { fingerprint: string; completedAt: string }

function failureCode(error: unknown): string {
  if (error instanceof SummaryBudgetError) return error.message;
  const message = error instanceof Error ? error.message : String(error);
  if (/circuit_breaker_open/i.test(message)) return "circuit_breaker_open";
  if (/timed out|timeout|AbortError/i.test(message)) return "timeout";
  if (/context_length_exceeded|maximum context length|too many tokens/i.test(message)) return "context_limit";
  if (/\b429\b|rate.limit/i.test(message)) return "rate_limited";
  if (/\b5\d\d\b|fetch failed|ECONNRESET|ECONNREFUSED/i.test(message)) return "provider_unavailable";
  return "provider_error";
}

function malformedSummaryError(error: unknown): boolean {
  return error instanceof SummaryBudgetError &&
    (error.message === "summary_parse_failed" || error.message === "summary_validation_failed") ||
    error instanceof Error && error.message === "LLM summary response failed deterministic validation";
}

function unitOutput(summary: SessionSummary, items: SummaryPromptItem[]): SummaryPromptItem {
  return {
    text: formatSummaryPartial(summary),
    obsRangeStart: items[0].obsRangeStart,
    obsRangeEnd: items[items.length - 1].obsRangeEnd,
  };
}

function createUnits(jobId: string, stage: "map" | "reduce", round: number, groups: SummaryPromptItem[][]): SummaryQueueUnit[] {
  if (groups.length === 0 || groups.length > MAX_SUMMARY_CALLS) {
    throw new SummaryBudgetError("summary_call_limit_exceeded");
  }
  return groups.map(items => ({ id: generateId("squ"), jobId, stage, round, items, attempts: 0 }));
}

function summaryItems(observations: CompressedObservation[]): SummaryPromptItem[] {
  return observations.map((observation, index) => ({
    text: formatSummaryObservation(observation, index + 1),
    obsRangeStart: index + 1,
    obsRangeEnd: index + 1,
  }));
}

async function snapshot(kv: StateKV, sessionId: string, project: string): Promise<{
  observations: CompressedObservation[]; items: SummaryPromptItem[]; fingerprint: string;
}> {
  const observations = (await kv.list<CompressedObservation>(KV.observations(sessionId)))
    .filter(observation => observation.title)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id));
  const items = summaryItems(observations);
  return { observations, items, fingerprint: fingerprintId("sq", JSON.stringify([sessionId, project, items])) };
}

async function enqueue(sdk: IIIClient, functionId: string, payload: unknown): Promise<void> {
  const topic = functionId === "mem::summary-unit" ? UNIT_TOPIC : DISPATCH_TOPIC;
  await sdk.trigger({ function_id: "iii::durable::publish", payload: { topic, data: payload } });
}

async function deleteUnits(kv: StateKV, jobId: string, units: SummaryQueueUnit[]): Promise<void> {
  for (let start = 0; start < units.length; start += DISPATCH_BATCH) {
    await Promise.all(units.slice(start, start + DISPATCH_BATCH)
      .map(unit => kv.delete(KV.summaryQueueUnits(jobId), unit.id)));
  }
}

async function cleanupUnits(kv: StateKV, jobId: string): Promise<void> {
  await deleteUnits(kv, jobId, await kv.list<SummaryQueueUnit>(KV.summaryQueueUnits(jobId)));
}

async function writeUnits(kv: StateKV, jobId: string, units: SummaryQueueUnit[]): Promise<void> {
  for (let start = 0; start < units.length; start += DISPATCH_BATCH) {
    await Promise.all(units.slice(start, start + DISPATCH_BATCH)
      .map(unit => kv.set(KV.summaryQueueUnits(jobId), unit.id, unit)));
  }
}

async function loadJobUnits(kv: StateKV, job: SummaryQueueJob): Promise<Array<SummaryQueueUnit | undefined>> {
  const units = await kv.list<SummaryQueueUnit>(KV.summaryQueueUnits(job.id));
  const byId = new Map(units.map(unit => [unit.id, unit]));
  return job.unitIds.map(id => byId.get(id));
}

async function failJob(kv: StateKV, jobId: string, reason: string): Promise<void> {
  await withKeyedLock(`summary-job:${jobId}`, async () => {
    const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
    if (!job || job.status !== "pending") return;
    const failedAt = new Date().toISOString();
    await kv.set(KV.summaryQueueJobs, jobId, {
      ...job, status: "failed", failedAt, failure: reason, updatedAt: failedAt,
    });
    const active = await kv.get<ActiveJob>(KV.summaryQueueActive, job.sessionId);
    if (active?.jobId === jobId) await kv.delete(KV.summaryQueueActive, job.sessionId);
    await recordAudit(kv, "compress", "mem::summary-unit", [job.sessionId],
      { jobId, outcome: "failed", reason }, undefined, undefined, `${jobId}:failed`);
    await cleanupUnits(kv, jobId);
    logger.warn("Summary queue job failed", { sessionId: job.sessionId, jobId, reason });
  });
}

type SummaryMethod = "llm" | "deterministic_fallback";

function summaryFields(summary: SessionSummary) {
  return {
    title: summary.title, narrative: summary.narrative,
    keyDecisions: summary.keyDecisions, filesModified: summary.filesModified,
    concepts: summary.concepts,
  };
}

function validateSummary(summary: SessionSummary): void {
  if (!validateOutput(SummaryOutputSchema, summaryFields(summary), "mem::summary-unit").valid) {
    throw new SummaryBudgetError("summary_validation_failed");
  }
}

function stableSummaryValues(summaries: SessionSummary[], field: "keyDecisions" | "filesModified" | "concepts"): string[] {
  const seen = new Set<string>();
  const merged: string[] = [];
  for (const summary of summaries) {
    for (const value of summary[field]) {
      const normalized = value.trim();
      if (normalized && !seen.has(normalized)) {
        seen.add(normalized);
        merged.push(normalized);
      }
    }
  }
  return merged;
}

function compactSummaryText(value: string, limit: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) return normalized;
  const end = normalized.lastIndexOf(" ", limit - 1);
  return `${normalized.slice(0, end > limit / 2 ? end : limit - 1).trimEnd()}…`;
}

function deterministicSummary(job: SummaryQueueJob, units: SummaryQueueUnit[]): SessionSummary {
  const ordered = [...units].sort((a, b) =>
    a.output!.obsRangeStart - b.output!.obsRangeStart ||
    a.output!.obsRangeEnd - b.output!.obsRangeEnd || a.id.localeCompare(b.id));
  const summaries = ordered.map(unit => unit.summary!);
  summaries.forEach(validateSummary);
  const beginning = summaries[0];
  const progress = summaries[Math.floor(summaries.length / 2)];
  const ending = summaries[summaries.length - 1];
  const title = beginning.title === ending.title
    ? compactSummaryText(ending.title, 100)
    : `${compactSummaryText(beginning.title, 48)} / ${compactSummaryText(ending.title, 48)}`;
  return {
    sessionId: job.sessionId,
    project: job.project,
    createdAt: new Date().toISOString(),
    title: title || "Session summary",
    narrative: [
      `Beginning: ${compactSummaryText(beginning.narrative, 180)}`,
      `Progress: ${compactSummaryText(progress.narrative, 180)}`,
      `Ending: ${compactSummaryText(ending.narrative, 180)}`,
    ].join(" "),
    keyDecisions: stableSummaryValues(summaries, "keyDecisions"),
    filesModified: stableSummaryValues(summaries, "filesModified"),
    concepts: stableSummaryValues(summaries, "concepts"),
    observationCount: job.observationCount,
  };
}

async function completeJob(
  kv: StateKV, job: SummaryQueueJob, summary: SessionSummary, method: SummaryMethod = "llm",
): Promise<void> {
  const fields = summaryFields(summary);
  validateSummary(summary);
  const qualityScore = scoreSummary(fields);
  await kv.set(KV.summaries, job.sessionId, summary);
  await recordAudit(kv, "compress", "mem::summary-unit", [job.sessionId],
    { jobId: job.id, observationCount: job.observationCount, qualityScore, method },
    qualityScore, undefined, `${job.id}:completed`);
  const completedAt = new Date().toISOString();
  await kv.set(KV.summaryQueueCompleted, job.sessionId, {
    fingerprint: job.snapshotFingerprint, completedAt,
  } satisfies CompletedSnapshot);
  await kv.set(KV.summaryQueueJobs, job.id, {
    ...job, status: "completed", completedAt, updatedAt: completedAt,
  });
  const active = await kv.get<ActiveJob>(KV.summaryQueueActive, job.sessionId);
  if (active?.jobId === job.id) await kv.delete(KV.summaryQueueActive, job.sessionId);
  await cleanupUnits(kv, job.id);
  logger.info("Session summarized from queue", {
    sessionId: job.sessionId, observationCount: job.observationCount, qualityScore, method,
  });
}

export function registerSummaryQueueFunctions(
  sdk: IIIClient, kv: StateKV, provider: MemoryProvider, llmRouter?: LlmTaskRouter,
): void {
  sdk.registerFunction("mem::summary-enqueue", async (data: { sessionId?: string } | undefined) => {
    if (typeof data?.sessionId !== "string" || !data.sessionId.trim()) {
      return { success: false, error: "sessionId is required" };
    }
    const sessionId = data.sessionId.trim();
    return withKeyedLock(`summary-session:${sessionId}`, async () => {
      const session = await kv.get<Session>(KV.sessions, sessionId);
      if (!session) return { success: false, error: "session_not_found" };
      const intent = await kv.get<SummaryQueueIntent>(KV.summaryQueueIntents, sessionId);
      if (!intent) await kv.set(KV.summaryQueueIntents, sessionId, {
        sessionId, createdAt: new Date().toISOString(),
      } satisfies SummaryQueueIntent);
      if (provider.name === "noop") return { success: true, queued: false, skipped: "no_provider" };
      const current = await snapshot(kv, sessionId, session.project);
      if (current.observations.length === 0) {
        await kv.delete(KV.summaryQueueIntents, sessionId);
        return { success: true, queued: false, skipped: "no_observations" };
      }
      const active = await kv.get<ActiveJob>(KV.summaryQueueActive, sessionId);
      if (active) {
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, active.jobId);
        if (job?.status === "pending") {
          return { success: true, queued: true, jobId: job.id, deduplicated: true };
        }
        await kv.delete(KV.summaryQueueActive, sessionId);
      }
      const completed = await kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId);
      if (completed?.fingerprint === current.fingerprint &&
          await kv.get<SessionSummary>(KV.summaries, sessionId)) {
        await kv.delete(KV.summaryQueueIntents, sessionId);
        return { success: true, queued: false, completed: true };
      }
      const config = getSummaryBudgetConfig();
      const groups = packSummaryItems(current.items, SUMMARY_SYSTEM, buildSummaryItemsPrompt,
        summaryChunkInputLimit(config), config.chunkSize);
      const jobId = generateId("sqj");
      const units = createUnits(jobId, "map", 0, groups);
      const now = new Date().toISOString();
      const job: SummaryQueueJob = {
        id: jobId, sessionId, project: session.project,
        snapshotFingerprint: current.fingerprint, observationCount: current.observations.length,
        config, createdAt: now, updatedAt: now, status: "pending", stage: "map",
        round: 0, unitIds: units.map(unit => unit.id), sourceProgressSize: summaryProgressSize(current.items),
        dispatchPending: true,
      };
      await writeUnits(kv, jobId, units);
      await kv.set(KV.summaryQueueJobs, jobId, job);
      await kv.set(KV.summaryQueueActive, sessionId, { jobId } satisfies ActiveJob);
      await recordAudit(kv, "compress", "mem::summary-enqueue", [sessionId],
        { jobId, unitCount: units.length, outcome: "queued" }, undefined, undefined, `${jobId}:queued`);
      await enqueue(sdk, "mem::summary-dispatch", { jobId, round: 0, offset: 0 });
      return { success: true, queued: true, jobId };
    });
  });

  sdk.registerFunction("mem::summary-dispatch", async (data: { jobId: string; round: number; offset: number }) => {
    if (data.offset !== 0) return { success: true, skipped: true };
    return withKeyedLock(`summary-job:${data.jobId}`, async () => {
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, data.jobId);
      if (!job || job.status !== "pending" || job.round !== data.round) {
        return { success: true, skipped: true };
      }
      const units = await loadJobUnits(kv, job);
      const waiting = units.filter((unit): unit is SummaryQueueUnit => Boolean(unit && !unit.output));
      const outstanding = waiting.filter(unit => unit.dispatchedAt && unit.attempts === 0).length;
      const available = Math.max(0, DISPATCH_BATCH - outstanding);
      const now = Date.now();
      const selected = waiting.filter(unit => !unit.dispatchedAt && retryEligible(unit, now)).slice(0, available);
      for (const unit of selected) {
        await kv.set(KV.summaryQueueUnits(job.id), unit.id, { ...unit, dispatchedAt: new Date().toISOString() });
        try {
          await enqueue(sdk, "mem::summary-unit", { jobId: job.id, unitId: unit.id });
        } catch (error) {
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, unit);
          throw error;
        }
      }
      if (job.dispatchPending) {
        await kv.set(KV.summaryQueueJobs, job.id, { ...job, dispatchPending: false });
      }
      return { success: true, dispatched: selected.length };
    });
  });
  sdk.registerTrigger({
    type: "durable:subscriber",
    function_id: "mem::summary-dispatch",
    config: { topic: DISPATCH_TOPIC },
  });

  const advance = async (jobId: string): Promise<{
    completed: boolean; nextRound?: number; method?: SummaryMethod;
  }> =>
    withKeyedLock(`summary-job:${jobId}`, async () => {
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
      if (!job || job.status !== "pending") return { completed: false };
      const units = await loadJobUnits(kv, job);
      if (units.some(unit => !unit?.output || !unit.summary)) return { completed: false };
      const complete = units as SummaryQueueUnit[];
      if (complete.length === 1) {
        const summary = { ...complete[0].summary!, observationCount: job.observationCount };
        await completeJob(kv, job, summary);
        return { completed: true, method: "llm" };
      }
      const outputs = complete.map(unit => unit.output!);
      let groups: SummaryPromptItem[][];
      try {
        groups = packSummaryItems(outputs, REDUCE_SYSTEM, buildReduceItemsPrompt,
          summaryReduceInputLimit(job.config));
      } catch (error) {
        const unableToFit = error instanceof SummaryBudgetError &&
          ["summary_fragment_cannot_fit", "summary_prompt_exceeds_budget"].includes(error.message);
        if (job.stage !== "reduce" || !unableToFit) throw error;
        const summary = deterministicSummary(job, complete);
        await completeJob(kv, job, summary, "deterministic_fallback");
        return { completed: true, method: "deterministic_fallback" };
      }
      const sizeProgress = summaryProgressSize(outputs) < job.sourceProgressSize;
      const unitProgress = groups.length < complete.length;
      if (job.stage === "reduce" && !sizeProgress && !unitProgress) {
        const summary = deterministicSummary(job, complete);
        await completeJob(kv, job, summary, "deterministic_fallback");
        return { completed: true, method: "deterministic_fallback" };
      }
      if (job.round >= MAX_SUMMARY_DEPTH) {
        if (job.stage === "reduce") {
          const summary = deterministicSummary(job, complete);
          await completeJob(kv, job, summary, "deterministic_fallback");
          return { completed: true, method: "deterministic_fallback" };
        }
        throw new SummaryBudgetError("summary_depth_limit_exceeded");
      }
      const nextRound = job.round + 1;
      const nextUnits = createUnits(job.id, "reduce", nextRound, groups);
      await writeUnits(kv, job.id, nextUnits);
      await kv.set(KV.summaryQueueJobs, job.id, {
        ...job, stage: "reduce", round: nextRound, unitIds: nextUnits.map(unit => unit.id),
        sourceProgressSize: summaryProgressSize(outputs), updatedAt: new Date().toISOString(),
        dispatchPending: true,
      });
      await deleteUnits(kv, job.id, complete);
      return { completed: false, nextRound };
    });

  sdk.registerFunction("mem::summary-unit", async (data: { jobId: string; unitId: string }) =>
    withKeyedLock(`summary-unit:${data.unitId}`, async () => {
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, data.jobId);
      if (!job || job.status !== "pending") return { success: true, skipped: true };
      const unit = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), data.unitId);
      if (!unit || unit.round !== job.round || unit.stage !== job.stage) {
        if (job.dispatchPending) {
          await enqueue(sdk, "mem::summary-dispatch", { jobId: job.id, round: job.round, offset: 0 });
        }
        return { success: true, skipped: true };
      }
      if (!unit.output) {
        await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
          ...unit, startedAt: new Date().toISOString(),
        });
        const system = unit.stage === "map" ? SUMMARY_SYSTEM : REDUCE_SYSTEM;
        const prompt = unit.stage === "map" ? buildSummaryItemsPrompt(unit.items) : buildReduceItemsPrompt(unit.items);
        try {
          const inputTokens = estimateSummaryTokens(system, prompt);
          if (inputTokens > summaryCallInputLimit(job.config)) throw new SummaryBudgetError("summary_prompt_exceeds_budget");
          const outputTokens = summaryOutputTokenBudget(job.config, system, prompt);
          const call = (selected: MemoryProvider) => selected.summarize(system, prompt, { task: "summary", outputTokens });
          let summary: SessionSummary | null = null;
          for (let responseAttempt = 0; responseAttempt < 2; responseAttempt++) {
            try {
              const response = llmRouter
                ? await llmRouter.run("summary", call, candidate => parseSummaryXml(candidate, job.sessionId, job.project, 0) !== null)
                : await call(provider);
              summary = parseSummaryXml(response, job.sessionId, job.project, job.observationCount);
              if (!summary) throw new SummaryBudgetError("summary_parse_failed");
              if (job.unitIds.length === 1) {
                const fields = {
                  title: summary.title, narrative: summary.narrative,
                  keyDecisions: summary.keyDecisions, filesModified: summary.filesModified,
                  concepts: summary.concepts,
                };
                if (!validateOutput(SummaryOutputSchema, fields, "mem::summary-unit").valid) {
                  throw new SummaryBudgetError("summary_validation_failed");
                }
              }
              break;
            } catch (error) {
              if (!malformedSummaryError(error) || responseAttempt === 1) {
                if (malformedSummaryError(error) && !(error instanceof SummaryBudgetError)) {
                  throw new SummaryBudgetError("summary_parse_failed");
                }
                throw error;
              }
            }
          }
          if (!summary) throw new SummaryBudgetError("summary_parse_failed");
          const latest = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
          if (!latest || latest.status !== "pending" || latest.round !== unit.round) {
            return { success: true, skipped: true };
          }
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
            ...unit, summary, output: unitOutput(summary, unit.items),
            startedAt: undefined, lastError: undefined,
          });
        } catch (error) {
          const attempts = unit.attempts + 1;
          const reason = failureCode(error);
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
            ...unit, attempts, startedAt: undefined,
            lastError: reason, lastAttemptAt: new Date().toISOString(),
          });
          if (attempts >= MAX_ATTEMPTS || error instanceof SummaryBudgetError) {
            await failJob(kv, job.id, reason);
            return { success: false, error: reason, terminal: true };
          }
          await sdk.trigger({ function_id: "mem::summary-dispatch",
            payload: { jobId: job.id, round: job.round, offset: 0 } });
          throw error;
        }
      }
      try {
        const result = await advance(job.id);
        if (result.nextRound !== undefined) {
          await enqueue(sdk, "mem::summary-dispatch", { jobId: job.id, round: result.nextRound, offset: 0 });
        } else if (!result.completed) {
          await sdk.trigger({ function_id: "mem::summary-dispatch",
            payload: { jobId: job.id, round: job.round, offset: 0 } });
        }
        if (result.completed) {
          await sdk.trigger({ function_id: "mem::summary-enqueue", payload: { sessionId: job.sessionId } });
        }
        return { success: true, completed: result.completed, ...(result.method && { method: result.method }) };
      } catch (error) {
        if (error instanceof SummaryBudgetError) {
          await failJob(kv, job.id, error.message);
          return { success: false, error: error.message, terminal: true };
        }
        throw error;
      }
    }));
  sdk.registerTrigger({
    type: "durable:subscriber",
    function_id: "mem::summary-unit",
    config: {
      topic: UNIT_TOPIC,
      queue_config: {
        type: "concurrent", concurrency: 12, maxRetries: 5, backoffDelayMs: RETRY_BACKOFF_MS,
      },
    },
  });

  const reconcilePendingJobs = async (): Promise<{ recovered: number; replayed: number }> => {
    let idle = false;
    try {
      const queue = await sdk.trigger<unknown, { depth?: number; dlq_depth?: number }>({
        function_id: "engine::queue::topic_stats", payload: { topic: UNIT_TOPIC }, timeoutMs: 5000,
      });
      idle = queue.depth === 0 && queue.dlq_depth === 0;
    } catch {
      // New work can still be dispatched; only stale-delivery replay needs an idle signal.
    }

    let recovered = 0;
    let replayed = 0;
    for (const candidate of await kv.list<SummaryQueueJob>(KV.summaryQueueJobs)) {
      if (candidate.status !== "pending") continue;
      if (idle) {
        await withKeyedLock(`summary-job:${candidate.id}`, async () => {
          const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, candidate.id);
          if (!job || job.status !== "pending") return;
          const now = Date.now();
          for (const unit of await loadJobUnits(kv, job)) {
            if (unit && !unit.output && unit.attempts > 0 && !unit.lastAttemptAt) {
              const markedAt = new Date(now).toISOString();
              await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
                ...unit, lastAttemptAt: markedAt, dispatchedAt: unit.dispatchedAt ?? markedAt,
              });
              continue;
            }
            if (!unit || unit.output || !unit.dispatchedAt ||
                now - Date.parse(unit.dispatchedAt) < STALE_DELIVERY_MS ||
                unit.startedAt && now - Date.parse(unit.startedAt) < maxUnitRuntimeMs() ||
                !retryEligible(unit, now)) continue;
            await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
              ...unit, dispatchedAt: undefined, startedAt: undefined,
            });
            replayed++;
          }
        });
      }
      try {
        const progress = await advance(candidate.id);
        if (progress.completed) {
          await sdk.trigger({ function_id: "mem::summary-enqueue",
            payload: { sessionId: candidate.sessionId } });
          recovered++;
          continue;
        }
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, candidate.id);
        if (!job || job.status !== "pending") continue;
        const result = await sdk.trigger<unknown, { dispatched?: number }>({
          function_id: "mem::summary-dispatch",
          payload: { jobId: job.id, round: job.round, offset: 0 },
        });
        if (progress.nextRound !== undefined || result.dispatched) recovered++;
      } catch (error) {
        if (!(error instanceof SummaryBudgetError)) throw error;
        await failJob(kv, candidate.id, error.message);
      }
    }
    if (replayed > 0) logger.warn("Summary queue replayed undelivered units", { replayed });
    return { recovered, replayed };
  };
  sdk.registerFunction("mem::summary-reconcile", reconcilePendingJobs);

  sdk.registerFunction("mem::summary-recover", async () => {
    let { recovered } = await reconcilePendingJobs();
    let cleaned = 0;
    const jobs = await kv.list<SummaryQueueJob>(KV.summaryQueueJobs);
    const coveredSessions = new Set<string>();
    for (const job of jobs) {
      if (job.status === "pending") {
        coveredSessions.add(job.sessionId);
      } else if (job.status === "completed") {
        await sdk.trigger({ function_id: "mem::summary-enqueue", payload: { sessionId: job.sessionId } });
        await cleanupUnits(kv, job.id);
        await kv.delete(KV.summaryQueueJobs, job.id);
        cleaned++;
      } else if (job.failedAt && Date.now() - Date.parse(job.failedAt) >= FAILED_RETENTION_MS) {
        await cleanupUnits(kv, job.id);
        await kv.delete(KV.summaryQueueJobs, job.id);
        const active = await kv.get<ActiveJob>(KV.summaryQueueActive, job.sessionId);
        const intent = await kv.get<SummaryQueueIntent>(KV.summaryQueueIntents, job.sessionId);
        const newerJob = jobs.some(candidate => candidate.sessionId === job.sessionId &&
          candidate.id !== job.id && candidate.createdAt > job.createdAt);
        if (!active && !newerJob && intent && intent.createdAt <= job.failedAt) {
          await kv.delete(KV.summaryQueueIntents, job.sessionId);
        }
        cleaned++;
      } else {
        coveredSessions.add(job.sessionId);
        await cleanupUnits(kv, job.id);
      }
    }
    const intents = await kv.list<SummaryQueueIntent>(KV.summaryQueueIntents);
    const uncovered = intents.filter(intent => !coveredSessions.has(intent.sessionId))
      .sort((a, b) => (a.lastAttemptAt ?? a.createdAt).localeCompare(b.lastAttemptAt ?? b.createdAt))
      .slice(0, MAX_RECOVERY_INTENTS);
    for (const intent of uncovered) {
      const session = await kv.get<Session>(KV.sessions, intent.sessionId);
      if (session?.status !== "completed") continue;
      const result = await sdk.trigger<{ sessionId: string }, { success: boolean; queued?: boolean; skipped?: string }>({
        function_id: "mem::summary-enqueue", payload: { sessionId: intent.sessionId },
      });
      if (!result.success) throw new Error("summary_recovery_enqueue_failed");
      if (result.skipped === "no_provider") {
        await kv.set(KV.summaryQueueIntents, intent.sessionId, {
          ...intent, lastAttemptAt: new Date().toISOString(),
        });
      }
      if (result.queued) recovered++;
    }
    return { success: true, recovered, cleaned };
  });
}
