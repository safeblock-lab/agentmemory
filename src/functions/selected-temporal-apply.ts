import type { GraphNode, GraphEdge } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import { batchEffectKey, preserveBatchProvenance, runBatchCallback, withBatchRecordLocks } from "../state/batch-effects.js";
import { graphCapturedAt } from "./graph-jobs.js";
import { GRAPH_WORK_LIMITS } from "./graph-work-groups.js";

const edgeKey = (edge: GraphEdge) => `${edge.sourceNodeId}|${edge.targetNodeId}|${edge.type}`;
const namespaceId = (project: string, node: GraphNode) => fingerprintId("gn", `temporal:${project}:${node.type}:${node.name}`);
const invalid = (): never => { throw new Error("Selected temporal canonical provenance conflicts"); };

// This helper receives authority only from the frozen, verified temporal unit caller.
export async function applySelectedTemporalGraph(
  kv: StateKV, nodes: GraphNode[], edges: GraphEdge[], observationIds: string[],
  effectKey: string, project: string, sessionId: string,
): Promise<{ success: boolean; nodesAdded?: number; edgesAdded?: number; error?: string }> {
  const sources = new Map<string, boolean>();
  const provenance = async (record: { sourceObservationIds: string[]; stale?: boolean }) => {
    const ids = record.sourceObservationIds;
    if (record.stale || !Array.isArray(ids) || !ids.length || ids.length > GRAPH_WORK_LIMITS.maxResultReferences
      || new Set(ids).size !== ids.length || ids.some(id => typeof id !== "string" || !id || id.length > 4096)) invalid();
    for (const id of ids) {
      if (!sources.has(id)) {
        if (sources.size >= GRAPH_WORK_LIMITS.maxResultReferences) invalid();
        const observation = await kv.get<{ id?: string; sessionId?: string; project?: string; stale?: boolean }>(KV.observations(sessionId), id);
        sources.set(id, !!observation && observation.id === id && observation.sessionId === sessionId
          && !observation.stale && (!observation.project || observation.project === project));
      }
      if (!sources.get(id)) invalid();
    }
  };
  if ((await kv.get<{ project?: string }>(KV.sessions, sessionId))?.project !== project) invalid();
  if (Buffer.byteLength(JSON.stringify({ nodes, edges }), "utf8") > GRAPH_WORK_LIMITS.maxResultReferenceBytes) invalid();
  await provenance({ sourceObservationIds: observationIds });
  return runBatchCallback(kv, "graph", effectKey, async (_resuming, admit, receipt) => {
    const resultHash = batchEffectKey(JSON.stringify({ nodes, edges }));
    if (receipt?.resultHash && receipt.resultHash !== resultHash) invalid();
    if ((await kv.get<{ batchInProgress?: string }>(KV.graphSnapshot, "current"))?.batchInProgress) {
      throw new Error("A batch graph application must be recovered first");
    }
    const now = receipt?.effectTimestamp ?? graphCapturedAt();
    const remap = new Map<string, string>();
    const plannedNodes = new Map<string, GraphNode>();
    const plannedEdges = new Map<string, GraphEdge>();
    const predecessors = new Map<string, GraphEdge>();
    const records: Array<[string, string]> = [];
    for (const node of nodes) {
      if (typeof node.id !== "string" || !node.id || remap.has(node.id) || typeof node.name !== "string" || !node.name
        || typeof node.type !== "string" || !node.type || node.name.length > 4096 || node.type.length > 128) invalid();
      await provenance(node);
      if (node.sourceObservationIds.some(id => !observationIds.includes(id))) invalid();
      const id = namespaceId(project, node);
      remap.set(node.id, id);
      plannedNodes.set(id, { ...node, id, createdAt: now });
      records.push([KV.graphNodes, id], [KV.graphNameIndex, `${node.type}|${node.name}`]);
    }
    for (const [index, edge] of edges.entries()) {
      await provenance(edge);
      if (edge.sourceObservationIds.some(id => !observationIds.includes(id))) invalid();
      const sourceNodeId = remap.get(edge.sourceNodeId), targetNodeId = remap.get(edge.targetNodeId);
      if (!sourceNodeId || !targetNodeId || typeof edge.type !== "string" || !edge.type || edge.type.length > 128
        || !Number.isFinite(edge.weight) || edge.weight < 0 || edge.weight > 1) invalid();
      const id = fingerprintId("ge", `${effectKey}:${index}`);
      if (edge.supersededBy || edge.isLatest !== true || edge.version !== 1) invalid();
      const planned = { ...edge, id, sourceNodeId: sourceNodeId!, targetNodeId: targetNodeId!, createdAt: now, tcommit: now };
      plannedEdges.set(id, planned);
      records.push([KV.graphEdges, id], [KV.graphEdgeKey, edgeKey(planned)]);
    }
    // Discover only exact predecessors, then lock the full bounded write set before admission.
    for (const edge of plannedEdges.values()) {
      const id = await kv.get<string>(KV.graphEdgeKey, edgeKey(edge));
      if (id !== null && (typeof id !== "string" || !id || id.length > 4096)) invalid();
      if (id) records.push([KV.graphEdges, id], [KV.graphEdgeHistory, id]);
    }
    return withBatchRecordLocks(records, async () => {
      for (const [id, node] of plannedNodes) {
        const current = await kv.get<GraphNode>(KV.graphNodes, id);
        if (current) {
          if (current.id !== id || current.name !== node.name || current.type !== node.type) invalid();
          await provenance(current);
          const merged = preserveBatchProvenance(current, { ...current, properties: { ...current.properties, ...node.properties },
            sourceObservationIds: observationIds, aliases: node.aliases, updatedAt: now });
          await provenance(merged);
          plannedNodes.set(id, merged);
        }
      }
      for (const edge of plannedEdges.values()) {
        const key = edgeKey(edge);
        const id = await kv.get<string>(KV.graphEdgeKey, key);
        if (id && !records.some(([scope, locked]) => scope === KV.graphEdges && locked === id)) invalid();
        const current = id ? await kv.get<GraphEdge>(KV.graphEdges, id) : null;
        if (id && (!current || current.id !== id || edgeKey(current) !== key || current.isLatest !== true
          || current.supersededBy || !Number.isSafeInteger(current.version) || current.version! < 1)) invalid();
        if (current) await provenance(current);
        const replay = await kv.get<GraphEdge>(KV.graphEdges, edge.id);
        if (replay && (replay.id !== edge.id || edgeKey(replay) !== key || id !== edge.id)) invalid();
        if (replay) { await provenance(replay); plannedEdges.set(edge.id, replay); continue; }
        if (current) {
          const history = await kv.get<GraphEdge>(KV.graphEdgeHistory, current.id);
          if (history) invalid();
          predecessors.set(edge.id, current);
        }
      }
      await admit({ resultHash, effectTimestamp: now });
      for (const [id, node] of plannedNodes) {
        await kv.set(KV.graphNodes, id, node);
        const key = `${node.type}|${node.name}`;
        const indexed = await kv.get<string>(KV.graphNameIndex, key);
        if (indexed === null || indexed === id) await kv.set(KV.graphNameIndex, key, id);
      }
      const latest = new Map<string, GraphEdge>();
      for (const edge of plannedEdges.values()) {
        if (await kv.get<GraphEdge>(KV.graphEdges, edge.id)) { latest.set(edgeKey(edge), edge); continue; }
        const previous = latest.get(edgeKey(edge)) ?? predecessors.get(edge.id);
        if (previous) {
          const old = { ...previous, isLatest: false, tvalidEnd: previous.tvalidEnd || now, supersededBy: edge.id };
          await kv.set(KV.graphEdges, previous.id, old);
          await kv.set(KV.graphEdgeHistory, previous.id, old);
          edge.version = (previous.version || 1) + 1;
        }
        await kv.set(KV.graphEdges, edge.id, edge);
        await kv.set(KV.graphEdgeKey, edgeKey(edge), edge.id);
        latest.set(edgeKey(edge), edge);
      }
      return { success: true, nodesAdded: nodes.length, edgesAdded: edges.length };
    });
  });
}
