import type { StateKV } from "../state/kv.js";
import { KV, generateId } from "../state/schema.js";
import type { RecentOperation, RecentOperationFeed } from "../types.js";
import { getAgentId } from "../config.js";
import { stripPrivateData } from "./privacy.js";
import { logger } from "../logger.js";

export type DashboardActivityItem = RecentOperation;
export interface DashboardActivity {
  source: "native-operation-feed";
  limit: 30;
  items: RecentOperation[];
}

const FUNCTIONS = new Set(["mem::compress", "mem::summarize", "mem::graph-extract"]);
const OUTCOMES = new Set(["completed", "failed", "skipped", "queued", "partial"]);
const COUNTS = ["observationsProcessed", "observationsCompressed", "summariesCreated", "nodesAdded", "edgesAdded", "unitsQueued"] as const;

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
  return {
    id: entry.id, timestamp: new Date(entry.timestamp).toISOString(),
    functionId: entry.functionId as RecentOperation["functionId"],
    outcome: entry.outcome as RecentOperation["outcome"], counts,
    ...(agentId ? { agentId } : {}),
  };
}

export async function recordOperation(
  kv: StateKV,
  functionId: RecentOperation["functionId"],
  outcome: RecentOperation["outcome"],
  counts: RecentOperation["counts"],
  agentId = getAgentId(),
): Promise<void> {
  try {
    const item = sanitizedOperation({ id: generateId("op"), timestamp: new Date().toISOString(), functionId, outcome, counts, agentId });
    if (!item) throw new Error("Invalid operation metadata");
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
