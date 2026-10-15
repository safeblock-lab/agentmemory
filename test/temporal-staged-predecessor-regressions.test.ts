import { describe, expect, it } from "vitest";
import { registerTemporalGraphFunctions } from "../src/functions/temporal-graph.js";
import { KV } from "../src/state/schema.js";
import type { GraphEdge, MemoryProvider } from "../src/types.js";
import { effectHarness } from "./batch-effects-harness.js";

const response = '<temporal_graph><entities><entity type="person" name="Alice"></entity><entity type="library" name="TypeScript"></entity></entities>'
  + '<relationships><relationship type="uses" source="Alice" target="TypeScript" weight="0.8"></relationship>'
  + '<relationship type="uses" source="Alice" target="TypeScript" weight="0.9"></relationship></relationships></temporal_graph>';

describe("temporal staged predecessors", () => {
  it("rejects foreign committed membership, then versions a staged predecessor without a cycle", async () => {
    const h = effectHarness();
    const observation = {
      id: "obs", sessionId: "temporal-session", title: "Use TypeScript", narrative: "Alice uses TypeScript",
      concepts: [], files: [], type: "decision", timestamp: "2025-01-01T00:00:00Z",
    };
    h.seed(KV.sessions, "temporal-session", { id: "temporal-session", project: "test-project" });
    h.seed(KV.observations("temporal-session"), observation.id, observation);
    h.seed(KV.graphNodes, "alice", {
      id: "alice", type: "person", name: "Alice", properties: { project: "test-project" },
      sourceObservationIds: ["foreign-observation"], createdAt: "2024-01-01T00:00:00Z",
    });
    h.seed(KV.graphNodes, "ts", {
      id: "ts", type: "library", name: "TypeScript", properties: { project: "test-project" },
      sourceObservationIds: ["foreign-observation"], createdAt: "2024-01-01T00:00:00Z",
    });
    h.seed(KV.graphEdges, "foreign-edge", {
      id: "foreign-edge", type: "uses", sourceNodeId: "alice", targetNodeId: "ts", weight: 0.3,
      sourceObservationIds: ["foreign-observation"], createdAt: "2024-01-01T00:00:00Z", version: 4, isLatest: true,
    });
    h.seed(KV.graphEdgeKey, "alice|ts|uses", "foreign-edge");
    const retrieval = h.kv.retrieval.bind(h.kv);
    let aggregateLookups = 0;
    h.kv.retrieval = async <T>(request: Record<string, unknown>): Promise<T> => {
      if (request.action === "graph_read" && request.mode === "aggregate" && request.project === "test-project") {
        aggregateLookups++;
        return {
          status: "ready", project: "test-project", items: [], has_more: false,
          aggregate: { node_count: 2, edge_count: 0 },
        } as T;
      }
      return retrieval<T>(request);
    };
    registerTemporalGraphFunctions(h.sdk as never, h.kv, { compress: async () => response } as unknown as MemoryProvider);

    const accepted = await h.call("mem::temporal-graph-extract", { observations: [observation] });
    expect(accepted).toMatchObject({ success: true });
    const deliveries = await h.drainDeliveries();
    expect(deliveries.at(-1) ?? accepted).toMatchObject({ success: true });

    const edges = await h.kv.list<GraphEdge>(KV.graphEdges);
    const generated = edges.filter((edge) => edge.id !== "foreign-edge");
    expect(generated).toHaveLength(2);
    expect(aggregateLookups).toBe(1);
    const previous = generated.find((edge) => edge.version === 1)!;
    const latest = generated.find((edge) => edge.isLatest)!;
    expect(latest.sourceNodeId).toBe("alice");
    expect(latest.targetNodeId).toBe("ts");
    const sourceNode = await h.kv.get<GraphNode>(KV.graphNodes, latest.sourceNodeId);
    const targetNode = await h.kv.get<GraphNode>(KV.graphNodes, latest.targetNodeId);
    expect(sourceNode).toMatchObject({ id: latest.sourceNodeId, name: "Alice" });
    expect(targetNode).toMatchObject({ id: latest.targetNodeId, name: "TypeScript" });
    expect(sourceNode?.sourceObservationIds).toContain(observation.id);
    expect(targetNode?.sourceObservationIds).toContain(observation.id);
    const edgeKey = `${latest.sourceNodeId}|${latest.targetNodeId}|${latest.type}`;
    expect(previous).toMatchObject({
      version: 1, weight: 0.8, isLatest: false, supersededBy: latest.id, sourceObservationIds: [observation.id],
    });
    expect(latest).toMatchObject({ version: 2, weight: 0.9, isLatest: true, sourceObservationIds: [observation.id] });
    expect(latest.supersededBy).toBeUndefined();
    expect(await h.kv.get(KV.graphEdgeKey, edgeKey)).toBe(latest.id);
    expect(await h.kv.get<GraphEdge>(KV.graphEdgeHistory, previous.id)).toMatchObject({
      version: 1, weight: 0.8, isLatest: false, supersededBy: latest.id,
    });
    expect(await h.kv.get<GraphEdge>(KV.graphEdges, "foreign-edge")).toMatchObject({
      version: 4, weight: 0.3, isLatest: true, sourceObservationIds: ["foreign-observation"],
    });
  });
});
