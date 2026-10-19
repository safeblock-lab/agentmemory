import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import type { RecentOperation } from "../types.js";
import { PROCESSING_OPERATION_NAMES, recordOperation, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import { logger } from "../logger.js";

function failureDiagnostic(value: unknown, depth = 0): Record<string, unknown> {
  const record = value && typeof value === "object" ? value as Record<string, unknown> : {};
  const message = value instanceof Error ? value.message : record.error;
  const code = typeof record.code === "string" ? record.code : typeof message === "string" ? message.match(/\bSTATE_[A-Z0-9_]+\b/)?.[0] : undefined;
  const identifier = (item: unknown) => typeof item === "string" && /^[A-Za-z0-9_:.-]{1,160}$/.test(item) ? item : undefined;
  return { code: identifier(code), errorName: value instanceof Error ? value.name : undefined,
    error: identifier(message), reason: identifier(record.reason),
    ...(depth < 2 ? { stages: Object.fromEntries(Object.entries(operationResultObject(record.results)).slice(0, 4)
      .map(([name, result]) => [name.slice(0, 64), failureDiagnostic(operationResultObject(result), depth + 1)])) } : {}) };
}

function operationResultObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
}

function operationResultCounts(functionId: string, result: Record<string, unknown>): RecentOperation["counts"] {
  const counts: RecentOperation["counts"] = {};
  const numeric = (key: keyof RecentOperation["counts"], value: unknown) => {
    if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) counts[key] = value;
  };
  if (functionId === "mem::profile" && result.cached === false) {
    const profile = operationResultObject(result.profile);
    numeric("sessionsProcessed", profile.sessionCount);
    numeric("observationsProcessed", profile.totalObservations);
  }
  if (functionId === "mem::skill-extract" && result.extracted === true) {
    numeric(result.reinforced === true ? "skillsReinforced" : "skillsCreated", 1);
  }
  numeric("itemsFailed", result.failed);
  if (result.failed === true || result.success === false || typeof result.error === "string" || result.status === "failed") {
    numeric("itemsFailed", Math.max(1, counts.itemsFailed ?? 0));
  }
  if (typeof result.queued === "number") numeric("unitsQueued", result.queued);
  else if (result.queued === true && [result.jobId, result.intentId, result.manifestId].some(id => typeof id === "string")) {
    numeric("unitsQueued", 1);
  }
  numeric("itemsRemoved", result.evicted ?? result.deleted ?? result.forgotten);
  numeric("itemsCreated", result.consolidated ?? result.newInsights);
  numeric("itemsProcessed", result.clustersProcessed ?? result.compressed ?? result.observationsReviewed ?? result.total);
  if (functionId === "mem::slot-reflect") numeric("itemsCreated", result.applied);
  if (functionId === "mem::temporal-graph-extract" || functionId === "mem::graph::import-graphify") {
    numeric("nodesAdded", result.nodesAdded);
    numeric("edgesAdded", result.edgesAdded);
  }
  if (functionId === "mem::patterns" && Array.isArray(result.patterns)) numeric("itemsCreated", result.patterns.length);
  if (functionId === "mem::generate-rules" && Array.isArray(result.rules)) numeric("itemsCreated", result.rules.length);
  return counts;
}

function operationResultOutcome(result: Record<string, unknown>): {
  outcome: RecentOperation["outcome"]; reason?: RecentOperation["reason"];
} {
  if (result.failed === true || result.status === "failed" || result.success === false || typeof result.error === "string") {
    return { outcome: "failed" };
  }
  if (result.cached === true) return { outcome: "skipped", reason: "cached" };
  if (result.stale === true) return { outcome: "skipped", reason: "stale" };
  if (result.skipped) return { outcome: "skipped", reason: "no_work" };
  if (result.dryRun === true || result.extracted === false || result.profile === null ||
      typeof result.reason === "string" && /^(?:no[ _-]|insufficient[ _-]|too[ _-]few)/i.test(result.reason) ||
      result.compressed === 0) return { outcome: "skipped", reason: "no_work" };
  const stages = operationResultObject(result.results);
  const stageResults = [stages.semantic, stages.procedural].map(operationResultObject);
  if (result.partial === true || result.status === "partial" || typeof result.failed === "number" && result.failed > 0 || stageResults.some(stage => stage.error)) {
    return { outcome: "partial" };
  }
  if (result.paused === true || result.claimed === true || result.status === "pending" ||
      typeof result.status === "string" && result.status.startsWith("waiting_") ||
      result.queued === true || typeof result.queued === "number" && result.queued > 0 ||
      stageResults.some(stage => stage.queued === true)) return { outcome: "queued" };
  return { outcome: "completed", ...(result.usedFallback === true ? { reason: "fallback" as const } : {}) };
}

export function installOperationTracking(sdk: IIIClient, kv: StateKV): void {
  const registerProcessingFunction = sdk.registerFunction.bind(sdk);
  sdk.registerFunction = (functionId, handler, options) => {
    if (!Object.hasOwn(PROCESSING_OPERATION_NAMES, functionId) || typeof handler !== "function") {
      return registerProcessingFunction(functionId, handler, options);
    }
    return registerProcessingFunction(functionId, async (...args) => {
      const started = Date.now();
      try {
        const result = await handler(...args);
        const metadata = operationResultObject(result);
        const { outcome, reason } = operationResultOutcome(metadata);
        if (outcome === "failed" || outcome === "partial") logger.warn("Processing operation returned a failure", { functionId, phase: "handler_result", ...failureDiagnostic(metadata) });
        if ((outcome === "completed" || outcome === "partial" || outcome === "failed") &&
          (functionId === "mem::consolidate-pipeline" && typeof metadata.selectedRunId !== "string" ||
            functionId === "mem::skill-extract" && !operationResultObject(args[0]).intentId)) {
          await markWorkflowStatisticsGap(kv, functionId === "mem::skill-extract" ? "skills" : "consolidation");
        }
        await recordOperation(kv, functionId as RecentOperation["functionId"], outcome,
          operationResultCounts(functionId, metadata), undefined,
          { executionLevel: "unit", durationMs: Date.now() - started, reason, ...(outcome === "failed" ? { errorCode: "operation_failed" } : {}) });
        return result;
      } catch (error) {
        logger.error("Processing operation threw", { functionId, phase: "handler_exception", ...failureDiagnostic(error) });
        await recordOperation(kv, functionId as RecentOperation["functionId"], "failed", { itemsFailed: 1 }, undefined,
          { executionLevel: "unit", durationMs: Date.now() - started, errorCode: "operation_failed" });
        throw error;
      }
    }, options);
  };
}
