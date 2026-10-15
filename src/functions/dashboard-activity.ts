import type { StateKV } from "../state/kv.js";
import { KV, generateId } from "../state/schema.js";
import type { RecentOperation, RecentOperationFeed } from "../types.js";
import { getAgentId } from "../config.js";
import { stripPrivateData } from "./privacy.js";
import { logger } from "../logger.js";
import { recordFailureMinute } from "./dashboard-operational-metrics.js";
export const PROCESSING_OPERATION_NAMES = {
  "mem::skill-extract": "Skill extraction", "mem::profile": "Project profile",
  "mem::consolidate": "Memory consolidation", "mem::consolidate-pipeline": "Consolidation pipeline",
  "mem::patterns": "Pattern analysis", "mem::generate-rules": "Rule generation",
  "mem::reflect": "Reflection", "mem::crystallize": "Crystallization",
  "mem::reflect-unit": "Reflection unit",
  "mem::skill-lifecycle-dispatch": "Queued skill extraction",
  "mem::graph-dispatch-deliver": "Queued graph extraction",
  "mem::flow-compress": "Workflow compression", "mem::enrich-window": "Observation enrichment",
  "mem::enrich-session": "Session enrichment", "mem::compress-file": "File compression",
  "mem::temporal-graph-extract": "Temporal graph extraction", "mem::retention-score": "Retention scoring",
  "mem::retention-evict": "Retention eviction", "mem::auto-forget": "Automatic forgetting",
  "mem::evict": "Memory eviction", "mem::cascade-update": "Cascade update", "mem::heal": "Memory repair",
  "mem::slot-reflect": "Slot reflection", "mem::vision-embed": "Image embedding",
  "mem::migrate": "Memory migration", "mem::graph::import-graphify": "Graph import",
} as const;

export type DashboardActivityItem = RecentOperation;
export interface DashboardActivity {
  source: "native-operation-feed";
  limit: 30;
  items: RecentOperation[];
}

const FUNCTIONS = new Set(["mem::compress", "mem::summarize", "mem::graph-extract", "mem::summary-enqueue", "mem::summary-unit", "mem::summary-final", ...Object.keys(PROCESSING_OPERATION_NAMES)]);
const OUTCOMES = new Set(["completed", "failed", "skipped", "queued", "partial"]);
const COUNTS = ["observationsProcessed", "observationsCompressed", "summariesCreated", "nodesAdded", "edgesAdded", "unitsQueued", "unitsCompleted", "sessionsProcessed", "skillsCreated", "skillsReinforced", "itemsProcessed", "itemsCreated", "itemsRemoved", "itemsFailed"] as const;
const OPERATION_REASONS = new Set(["cached", "no_work", "paused", "deduplicated", "stale", "deterministic_fallback", "fallback"]);
const NAMES: Record<RecentOperation["functionId"], string> = {
  "mem::compress": "Compression", "mem::summarize": "Session summary", "mem::graph-extract": "Graph extraction",
  "mem::summary-enqueue": "Summary queued", "mem::summary-unit": "Summary unit", "mem::summary-final": "Queued session summary",
  ...PROCESSING_OPERATION_NAMES,
};
const PUBLIC_ERRORS = {
  invalid_input: "Required operation input is missing or invalid.",
  session_not_found: "Session was not found.",
  no_observations: "No observations are available.",
  no_provider: "No provider is configured.",
  empty_provider_response: "Provider returned no usable output.",
  parse_failed: "Operation output could not be parsed.",
  validation_failed: "Operation output did not pass validation.",
  compression_failed: "Compression failed.",
  summary_failed: "Session summary failed.",
  graph_extraction_failed: "Graph extraction failed.",
  operation_failed: "Operation failed.",
} as const;

export interface OperationDetails { durationMs?: number; errorCode?: string; reason?: RecentOperation["reason"] }

function publicError(code: unknown): RecentOperation["error"] {
  if (typeof code !== "string") return undefined;
  const safeCode = Object.hasOwn(PUBLIC_ERRORS, code) ? code as keyof typeof PUBLIC_ERRORS : "operation_failed";
  return { code: safeCode, message: PUBLIC_ERRORS[safeCode] };
}

function sanitizedOperation(value: unknown): RecentOperation | null {
  if (!value || typeof value !== "object") return null;
  const entry = value as Record<string, unknown>;
  if (typeof entry.id !== "string" || !/^op_[a-z0-9_]{1,80}$/.test(entry.id)
    || typeof entry.timestamp !== "string" || !Number.isFinite(Date.parse(entry.timestamp))
    || typeof entry.functionId !== "string" || !FUNCTIONS.has(entry.functionId)
    || typeof entry.outcome !== "string" || !OUTCOMES.has(entry.outcome)) return null;
  const counts: RecentOperation["counts"] = {};
  const raw = entry.counts && typeof entry.counts === "object" ? entry.counts as Record<string, unknown> : {};
  for (const key of COUNTS) {
    const count = raw[key];
    if (typeof count === "number" && Number.isSafeInteger(count) && count >= 0) counts[key] = count;
  }
  const agentId = typeof entry.agentId === "string"
    ? stripPrivateData(entry.agentId).replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 128) : undefined;
  const durationMs = typeof entry.durationMs === "number" && Number.isSafeInteger(entry.durationMs) && entry.durationMs >= 0
    ? entry.durationMs : undefined;
  const error = publicError(entry.error && typeof entry.error === "object"
    ? (entry.error as Record<string, unknown>).code : undefined);
  return {
    id: entry.id, timestamp: new Date(entry.timestamp).toISOString(),
    functionId: entry.functionId as RecentOperation["functionId"],
    outcome: entry.outcome as RecentOperation["outcome"], counts,
    name: NAMES[entry.functionId as RecentOperation["functionId"]],
    ...(durationMs !== undefined ? { durationMs } : {}),
    ...(typeof entry.reason === "string" && OPERATION_REASONS.has(entry.reason) ? { reason: entry.reason as RecentOperation["reason"] } : {}),
    ...(error ? { error } : {}),
    ...(agentId ? { agentId } : {}),
  };
}

export async function recordOperation(
  kv: StateKV,
  functionId: RecentOperation["functionId"],
  outcome: RecentOperation["outcome"],
  counts: RecentOperation["counts"],
  agentId = getAgentId(),
  details: OperationDetails = {},
): Promise<void> {
  try {
    const item = sanitizedOperation({
      id: generateId("op"), timestamp: new Date().toISOString(), functionId, outcome, counts, agentId,
      durationMs: details.durationMs, error: publicError(details.errorCode), reason: details.reason,
    });
    if (!item) throw new Error("Invalid operation metadata");
    if (item.outcome === "failed") await recordFailureMinute(kv, Date.parse(item.timestamp), agentId);
    // Native append and eviction share one state mutation; never read/modify/set.
    const result = await kv.update<{ errors?: unknown }>(KV.recentOperations, "current", [
      { type: "append_bounded", path: "items", value: { item, limit: 30 } },
    ]);
    if (Array.isArray(result?.errors) && result.errors.length > 0) throw new Error("Operation feed update rejected");
  } catch {
    logger.warn("Recent operation could not be persisted", { functionId, outcome });
  }
}

export function createDashboardActivity(kv: StateKV) {
  return {
    async snapshot(agentId?: string): Promise<DashboardActivity> {
      const feed = await kv.get<RecentOperationFeed>(KV.recentOperations, "current");
      const items = (Array.isArray(feed?.items) ? feed.items.slice(-30) : [])
        .map(sanitizedOperation).filter((item): item is RecentOperation => item !== null)
        .reverse().filter(item => !agentId || item.agentId === agentId);
      return { source: "native-operation-feed", limit: 30, items };
    },
  };
}
