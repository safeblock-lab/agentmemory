import type { ISdk } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import type { Memory, GraphNode, GraphEdge } from "../types.js";
import { recordAudit } from "./audit.js";
import { withBatchWriterLocks, withBatchRecordLocks } from "../state/batch-effects.js";

export function registerCascadeFunction(sdk: ISdk, kv: StateKV): void {
  sdk.registerFunction("mem::cascade-update",
    async (data: { supersededMemoryId: string }) => {
      if (!data.supersededMemoryId || typeof data.supersededMemoryId !== "string") {
        return { success: false, error: "supersededMemoryId is required" };
      }

      return withBatchWriterLocks(kv, ["graph"], async () => {
        const superseded = await kv.get<Memory>(KV.memories, data.supersededMemoryId);
        if (!superseded) {
          return { success: false, error: "superseded memory not found" };
        }

        let flaggedNodes = 0;
        let flaggedEdges = 0;
        let flaggedMemories = 0;

        const obsIds = new Set(superseded.sourceObservationIds || []);

        if (obsIds.size > 0) {
          const now = new Date().toISOString();
          const nodes = await kv.list<GraphNode>(KV.graphNodes);
          for (const listed of nodes) {
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

          const edges = await kv.list<GraphEdge>(KV.graphEdges);
          for (const listed of edges) {
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

        const supersededConcepts = new Set(
          (superseded.concepts ?? []).map((c) => c.toLowerCase()),
        );
        if (supersededConcepts.size >= 2) {
          const allMemories = await kv.list<Memory>(KV.memories);
          for (const mem of allMemories) {
            if (mem.id === data.supersededMemoryId) continue;
            if (!mem.isLatest) continue;

            const sharedCount = (mem.concepts ?? []).filter((c) =>
              supersededConcepts.has(c.toLowerCase()),
            ).length;
            if (sharedCount >= 2) {
              flaggedMemories++;
            }
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
      });
    },
  );
}
