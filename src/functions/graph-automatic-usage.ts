import { createHash } from "node:crypto";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { getAgentId } from "../config.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { logger } from "../logger.js";
import type { LlmRouteTarget, LlmUsage, ProcessingUsageEvent, ProcessingUsageFeed } from "../types.js";
import type { GraphAutomaticRecoveryResponse } from "../state/graph-automatic-recovery.js";
import type { PreparedGraphAutomaticCall } from "../providers/graph-automatic.js";

interface GraphProviderUsageRecord {
  version: 1;
  attemptId: string;
  effectKey: string;
  slotId: string;
  rootId: string;
  provider: string;
  route: LlmRouteTarget;
  model: string;
  endpoint: string;
  agentId: string;
  dispatchState: "potential" | "response_captured";
  usageState: "pending" | "known" | "partial" | "unknown";
  usage: LlmUsage | null;
  responseModel: string | null;
  captureDigest: string | null;
  costUsd: null;
  recordedAt: string;
  metricsApplied: boolean;
  feedApplied: boolean;
}
type Recorder = (route: LlmRouteTarget, model: string, usage: LlmUsage, effectKey: string) => Promise<void>;
const recorders = new WeakMap<StateKV, { route: LlmRouteTarget; record: Recorder }>();
const key = (attemptId: string) => `graph-provider-usage:v1:${attemptId}`;
const PENDING = "graph-provider-usage:v1:projection-pending";
const repairs = new WeakMap<StateKV, Promise<void>>();

export function installGraphProviderUsage(kv: StateKV, route: LlmRouteTarget, record: Recorder): void { recorders.set(kv, { route, record }); }

async function queue(kv: StateKV, attemptId: string): Promise<void> {
  await withKeyedLock(PENDING, async () => {
    const pending = await kv.get<{ attempts: string[] }>(KV.state, PENDING);
    const attempts = [...new Set([...(pending?.attempts ?? []), attemptId])];
    if (attempts.length > 4096) throw new Error("GRAPH_USAGE_PROJECTION_CAPACITY");
    await kv.set(KV.state, PENDING, { attempts });
  });
}

async function project(kv: StateKV, row: GraphProviderUsageRecord): Promise<void> {
  if (row.usageState === "pending") return;
  const usageState = row.usageState;
  const recorder = recorders.get(kv);
  if (row.usageState === "known" && !row.metricsApplied) {
    if (!recorder) throw new Error("GRAPH_USAGE_RECORDER_UNAVAILABLE");
    if (!row.usage) throw new Error("GRAPH_USAGE_REPORTED_DATA_MISSING");
    await recorder.record(row.route, row.model, row.usage, row.effectKey);
    row = { ...row, metricsApplied: true };
    await kv.set(KV.state, key(row.attemptId), row);
  }
  if (!row.feedApplied) {
    const feed = await kv.get<ProcessingUsageFeed>(KV.processingUsage, row.agentId);
    const projected = [...(feed?.items ?? [])].reverse().find(item => item.id === row.effectKey);
    if (projected?.nativeAttemptId !== row.attemptId || projected.dispatchState !== row.dispatchState || projected.usageState !== row.usageState) {
      const item: ProcessingUsageEvent = { id: row.effectKey, timestamp: row.recordedAt, task: "graph_extraction",
        provider: row.provider, model: row.model, nativeAttemptId: row.attemptId, nativeCaptureDigest: row.captureDigest,
        dispatchState: row.dispatchState, usageState, costUsd: null };
      for (const field of ["inputTokens", "outputTokens", "cachedInputTokens", "cacheWriteInputTokens"] as const) {
        const value = row.usage?.[field];
        if (value !== undefined) item[field] = value;
      }
      const result = await kv.update<{ errors?: unknown }>(KV.processingUsage, row.agentId,
        [{ type: "append_bounded", path: "items", value: { item, limit: 500 } }]);
      if (Array.isArray(result?.errors) && result.errors.length) throw new Error("GRAPH_USAGE_FEED_REJECTED");
    }
    row = { ...row, feedApplied: true };
    await kv.set(KV.state, key(row.attemptId), row);
  }
}

export async function recordGraphProviderUsage(kv: StateKV, receipt: GraphAutomaticRecoveryResponse,
  prepared: PreparedGraphAutomaticCall, raw?: string): Promise<void> {
  if (!receipt.attempt_id || !receipt.binding_digest) return;
  await withKeyedLock(key(receipt.attempt_id), async () => {
    let row = await kv.get<GraphProviderUsageRecord>(KV.state, key(receipt.attempt_id!));
    if (!row) {
      const actualFresh = receipt.command === "dispatch" && receipt.fresh && receipt.dispatch_allowed;
      row = { version: 1, attemptId: receipt.attempt_id!, effectKey: createHash("sha256").update(`graph-provider-usage:v1:${receipt.attempt_id}`).digest("hex"),
        slotId: receipt.slot_id, rootId: receipt.root_id, provider: actualFresh ? prepared.provider : "unknown",
        route: recorders.get(kv)?.route ?? "primary", model: actualFresh ? prepared.binding.model : "unknown",
        endpoint: actualFresh ? prepared.binding.endpoint : "unknown", agentId: getAgentId() ?? "default",
        dispatchState: "potential", usageState: "pending", usage: null, responseModel: null, captureDigest: null,
        costUsd: null, recordedAt: new Date().toISOString(), metricsApplied: false, feedApplied: false };
    }
    if (raw !== undefined && receipt.capture) {
      const observed = prepared.usage(raw);
      const known = observed.usage.inputTokens !== undefined && observed.usage.outputTokens !== undefined;
      const any = observed.usage.inputTokens !== undefined || observed.usage.outputTokens !== undefined || observed.usage.totalTokens !== undefined;
      if (row.captureDigest && row.captureDigest !== receipt.capture.digest) throw new Error("STATE_GRAPH_AUTOMATIC_USAGE_CONFLICT");
      row = { ...row, usage: observed.usage, responseModel: observed.responseModel, captureDigest: receipt.capture.digest,
        dispatchState: "response_captured", usageState: known ? "known" : any ? "partial" : "unknown" };
    } else if (["unknown", "expired", "failed", "terminal"].includes(receipt.state) && row.usageState === "pending") {
      row = { ...row, usageState: "unknown", usage: null };
    }
    await kv.set(KV.state, key(row.attemptId), row);
    if (row.usageState !== "pending") {
      await queue(kv, row.attemptId);
      try { await project(kv, row); } catch { return; }
    }
  });
}

export async function repairGraphProviderUsage(kv: StateKV): Promise<void> {
  const pending = await kv.get<{ attempts: string[] }>(KV.state, PENDING);
  for (const attemptId of (pending?.attempts ?? []).slice(0, 4)) {
    await withKeyedLock(key(attemptId), async () => {
      const row = await kv.get<GraphProviderUsageRecord>(KV.state, key(attemptId));
      if (row) await project(kv, row);
      await withKeyedLock(PENDING, async () => {
        const latest = await kv.get<{ attempts: string[] }>(KV.state, PENDING);
        await kv.set(KV.state, PENDING, { attempts: (latest?.attempts ?? []).filter(value => value !== attemptId) });
      });
    });
  }
}

export function scheduleGraphProviderUsageRepair(kv: StateKV): void {
  if (repairs.has(kv)) return;
  const repair = repairGraphProviderUsage(kv);
  repairs.set(kv, repair);
  void repair.then(() => {
    if (repairs.get(kv) === repair) repairs.delete(kv);
  }, () => {
    if (repairs.get(kv) === repair) repairs.delete(kv);
    logger.warn("Graph provider usage projection remains pending");
  });
}
