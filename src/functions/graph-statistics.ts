import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { logger } from "../logger.js";

export interface GraphSampleStatistics {
  sampleNodes: number | null;
  sampleEdges: number | null;
  sampleNodesByType: Record<string, number>;
  sampleEdgesByType: Record<string, number>;
  coverage: {
    project: string;
    requestedLimit: number;
    returnedNodes: number | null;
    returnedEdges: number | null;
    truncated: boolean;
    warning?: string;
  };
}

export const GRAPH_STATISTICS_SAMPLE_LIMIT = 12;

function sampleTypeCounts(rows: unknown[]) {
  const unique = new Map<string, string>();
  for (const row of rows) {
    if (!row || typeof row !== "object" || !("id" in row) || !("type" in row) ||
        typeof row.id !== "string" || !row.id || typeof row.type !== "string" || !row.type) {
      throw new Error("Invalid graph sample record");
    }
    if (!unique.has(row.id)) unique.set(row.id, row.type);
  }
  const types = new Map<string, number>();
  for (const type of unique.values()) types.set(type, (types.get(type) ?? 0) + 1);
  return { count: unique.size, types: Object.fromEntries(types) };
}

export async function readProjectGraphStatistics(
  kv: StateKV,
  project: string | undefined,
  query: (project: string) => Promise<unknown>,
) {
  const stats = await readGraphStatistics(kv, project);
  if (!project || stats.totalNodes !== null || stats.totalEdges !== null) return stats;
  let sample: GraphSampleStatistics;
  try {
    const result = await query(project);
    if (!result || typeof result !== "object" || !("nodes" in result) || !("edges" in result) ||
        !Array.isArray(result.nodes) || !Array.isArray(result.edges)) {
      throw new Error("Invalid graph sample response");
    }
    const nodes = sampleTypeCounts(result.nodes);
    const edges = sampleTypeCounts(result.edges);
    sample = {
      sampleNodes: nodes.count, sampleEdges: edges.count,
      sampleNodesByType: nodes.types, sampleEdgesByType: edges.types,
      coverage: {
        project, requestedLimit: GRAPH_STATISTICS_SAMPLE_LIMIT,
        returnedNodes: nodes.count, returnedEdges: edges.count,
        truncated: "truncated" in result && result.truncated === true,
        ...("warning" in result && typeof result.warning === "string" ? { warning: result.warning } : {}),
      },
    };
  } catch {
    logger.warn("Project graph sample unavailable", { project, code: "graph_sample_query_failed" });
    sample = {
      sampleNodes: null, sampleEdges: null, sampleNodesByType: {}, sampleEdgesByType: {},
      coverage: {
        project, requestedLimit: GRAPH_STATISTICS_SAMPLE_LIMIT,
        returnedNodes: null, returnedEdges: null, truncated: true, warning: "graph_sample_query_failed",
      },
    };
  }
  return { ...stats, ...sample };
}

function count(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
function counts(value: unknown): value is Record<string, number> {
  return value !== null && typeof value === "object" && !Array.isArray(value) &&
    Object.values(value).every(count);
}

export async function readGraphStatistics(kv: StateKV, project?: string) {
  const unavailable = (reason: string) => ({
    totalNodes: null, totalEdges: null, nodesByType: {}, edgesByType: {},
    fromSnapshot: false, provenance: "unavailable", countsAreCurrent: false,
    partial: true, reason, ...(project ? { project } : {}),
  });
  try {
    // Project only metadata from the singleton cache; topNodes/topEdges never cross the state boundary.
    const pages = kv.pages<Record<string, unknown>>(KV.graphSnapshot, {
      limit: 1, maxBytes: 16_384,
      fields: ["batchInProgress", "dirty", "project", "stats", "updatedAt", "version"],
    });
    const first = await pages.next();
    await pages.return(undefined);
    if (first.done) return unavailable("snapshot_missing_or_ambiguous");
    const page = first.value;
    if (page.next_cursor || page.items.length !== 1) return unavailable("snapshot_missing_or_ambiguous");
    const snapshot = page.items[0];
    if (project && snapshot.project !== project) return unavailable("project_aggregate_unavailable");
    const stats = snapshot.stats as Record<string, unknown> | undefined;
    if (snapshot.version !== 1 || !stats || !count(stats.totalNodes) || !count(stats.totalEdges) ||
        !counts(stats.nodesByType) || !counts(stats.edgesByType) ||
        typeof snapshot.updatedAt !== "string" || !Number.isFinite(Date.parse(snapshot.updatedAt))) {
      return unavailable("snapshot_metadata_invalid");
    }
    return {
      totalNodes: stats.totalNodes, totalEdges: stats.totalEdges,
      nodesByType: stats.nodesByType, edgesByType: stats.edgesByType,
      fromSnapshot: true, provenance: "historical_snapshot", countsAreCurrent: false,
      partial: true, updatedAt: snapshot.updatedAt,
      reason: snapshot.batchInProgress ? "snapshot_batch_in_progress" : snapshot.dirty ? "snapshot_dirty" : "snapshot_historical",
      ...(project ? { project } : {}),
    };
  } catch {
    return unavailable("snapshot_metadata_read_failed");
  }
}
