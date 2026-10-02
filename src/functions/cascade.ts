import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import type { Memory, GraphNode, GraphEdge } from "../types.js";
import { recordAudit } from "./audit.js";
import { withBatchWriterLocks, withBatchRecordLocks } from "../state/batch-effects.js";
import { freezeGraphValue, graphCapturedAt, graphKV, registerGraphJobHandler, runGraphJob, withGraphDelta } from "./graph-jobs.js";

export function registerCascadeFunction(sdk: IIIClient, kv: StateKV): void {
  kv = graphKV(kv);
  const cascade = async (request: { supersededMemoryId: string }, durableId?: string) => runGraphJob(kv, "cascade", request, async (input) => {
      const data = input as typeof request;
      if (!data.supersededMemoryId || typeof data.supersededMemoryId !== "string") {
        return { success: false, error: "supersededMemoryId is required" };
      }

      const superseded = await freezeGraphValue(kv, "superseded-memory", () => kv.get<Memory>(KV.memories, data.supersededMemoryId));
      const siblingCount = await freezeGraphValue(kv, "sibling-memory-count", async () => {
        const concepts = new Set((superseded?.concepts ?? []).map((concept) => concept.toLowerCase()));
        let count = 0;
        if (concepts.size >= 2) {
          for await (const memory of kv.values<Memory>(KV.memories)) {
            if (memory.id === data.supersededMemoryId || !memory.isLatest) continue;
            if ((memory.concepts ?? []).filter((concept) => concepts.has(concept.toLowerCase())).length >= 2) count++;
          }
        }
        return count;
      });
      return withBatchWriterLocks(kv, ["graph"], () => withGraphDelta(kv, async () => {
        if (!superseded) {
          return { success: false, error: "superseded memory not found" };
        }

        let flaggedNodes = 0;
        let flaggedEdges = 0;
        const flaggedMemories = siblingCount;

        const obsIds = new Set(superseded.sourceObservationIds || []);

        if (obsIds.size > 0) {
          const now = graphCapturedAt();
          for await (const listed of kv.values<GraphNode>(KV.graphNodes)) {
            await withBatchRecordLocks([[KV.graphNodes, listed.id]], async () => {
              const node = await kv.get<GraphNode>(KV.graphNodes, listed.id);
              if (!node || node.stale) return;
              const overlap = (node.sourceObservationIds ?? []).some((id) => obsIds.has(id));
              if (overlap) {
                node.stale = true;
                node.updatedAt = now;
                await kv.set(KV.graphNodes, node.id, node);
                await recordAudit(kv, "consolidate", "mem::cascade-update", [node.id], {
                  resourceType: "GraphNode",
                  change: "marked stale from superseded memory",
                  supersededMemoryId: data.supersededMemoryId,
                });
                flaggedNodes++;
              }
            });
          }

          for await (const listed of kv.values<GraphEdge>(KV.graphEdges)) {
            await withBatchRecordLocks([[KV.graphEdges, listed.id]], async () => {
              const edge = await kv.get<GraphEdge>(KV.graphEdges, listed.id);
              if (!edge || edge.stale) return;
              const overlap = (edge.sourceObservationIds ?? []).some((id) => obsIds.has(id));
              if (overlap) {
                edge.stale = true;
                await kv.set(KV.graphEdges, edge.id, edge);
                await recordAudit(kv, "consolidate", "mem::cascade-update", [edge.id], {
                  resourceType: "GraphEdge",
                  change: "marked stale from superseded memory",
                  supersededMemoryId: data.supersededMemoryId,
                });
                flaggedEdges++;
              }
            });
          }
        }

        return {
          success: true,
          flagged: {
            nodes: flaggedNodes,
            edges: flaggedEdges,
            siblingMemories: flaggedMemories,
          },
          total: flaggedNodes + flaggedEdges + flaggedMemories,
        };
      }));
    }, durableId);
  sdk.registerFunction("mem::cascade-update", (input: { supersededMemoryId: string }) => cascade(input));
  registerGraphJobHandler(kv, "cascade", (input, id) => cascade(input as { supersededMemoryId: string }, id));
}
