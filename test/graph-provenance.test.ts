import { beforeEach, describe, expect, it, vi } from "vitest";
import { registerGraphFunction } from "../src/functions/graph.js";
import { createFireworksBatchCompletionHandler } from "../src/index.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, GraphEdge, GraphNode } from "../src/types.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> => (store.get(scope)?.get(key) as T) ?? null,
    set: async <T>(scope: string, key: string, value: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, value);
      return value;
    },
    delete: async (scope: string, key: string) => { store.get(scope)?.delete(key); },
    list: async <T>(scope: string): Promise<T[]> => [...(store.get(scope)?.values() ?? [])] as T[],
  };
}

function mockSdk() {
  const functions = new Map<string, (data: unknown) => unknown>();
  return {
    registerFunction: (name: string, handler: (data: unknown) => unknown) => { functions.set(name, handler); },
    registerTrigger: () => {},
    trigger: async (request: { function_id: string; payload: unknown }) => {
      const handler = functions.get(request.function_id);
      if (!handler) throw new Error(`Missing function ${request.function_id}`);
      return handler(request.payload);
    },
  };
}

function observation(id: string): CompressedObservation {
  return {
    id,
    sessionId: "ses-provenance",
    timestamp: "2026-10-05T00:00:00.000Z",
    type: "discovery",
    title: id,
    narrative: `Independent source ${id}`,
    facts: [],
    concepts: [],
    files: [],
    importance: 5,
  };
}

const observations = [observation("obs-a"), observation("obs-b")];

async function extract(xml: string, legacy = false, callback = false) {
  const sdk = mockSdk();
  const kv = mockKV();
  installGraphStateWire(sdk as never, kv as never);
  registerGraphFunction(sdk as never, kv as never, {
    name: "test",
    compress: vi.fn().mockResolvedValue(xml),
    summarize: vi.fn(),
  } as never);
  const result = await sdk.trigger({
    function_id: "mem::graph-extract",
    payload: {
      observations,
      ...(legacy ? { graphJobId: "legacy-provenance-test" } : {}),
      ...(callback ? { batchResponse: xml, batchEffectKey: "c".repeat(64), graphProvenanceVersion: 2 } : {}),
    },
  });
  return {
    result,
    nodes: await kv.list<GraphNode>(KV.graphNodes),
    edges: await kv.list<GraphEdge>(KV.graphEdges),
  };
}

describe("graph observation provenance", () => {
  beforeEach(() => { process.env.GRAPH_EXTRACTION_ENABLED = "true"; });

  it("attributes disjoint entities and relationships only to their cited source", async () => {
    const xml = `<entities>
      <entity type="concept" name="Alpha" observations="1"/>
      <entity type="file" name="alpha.ts" observations="1"/>
      <entity type="concept" name="Beta" observations="2"/>
      <entity type="file" name="beta.ts" observations="2"/>
    </entities><relationships>
      <relationship type="uses" source="Alpha" target="alpha.ts" observations="1"/>
      <relationship type="uses" source="Beta" target="beta.ts" observations="2"/>
    </relationships>`;
    const { result, nodes, edges } = await extract(xml);
    expect(result).toMatchObject({ success: true, nodesAdded: 4, edgesAdded: 2 });
    expect(Object.fromEntries(nodes.map((node) => [node.name, node.sourceObservationIds]))).toEqual({
      Alpha: ["obs-a"], "alpha.ts": ["obs-a"], Beta: ["obs-b"], "beta.ts": ["obs-b"],
    });
    const names = new Map(nodes.map((node) => [node.id, node.name]));
    expect(Object.fromEntries(edges.map((edge) => [names.get(edge.sourceNodeId), edge.sourceObservationIds]))).toEqual({
      Alpha: ["obs-a"], Beta: ["obs-b"],
    });
  });

  it("merges exact citations when the same relationship is supported twice", async () => {
    const xml = `<entities>
      <entity type="concept" name="Alpha" observations="1,2"/>
      <entity type="file" name="alpha.ts" observations="1,2"/>
    </entities><relationships>
      <relationship type="uses" source="Alpha" target="alpha.ts" observations="1"/>
      <relationship type="uses" source="Alpha" target="alpha.ts" observations="2"/>
    </relationships>`;
    for (const callback of [false, true]) {
      const { result, edges } = await extract(xml, false, callback);
      expect(result).toMatchObject({ success: true });
      expect(edges).toHaveLength(1);
      expect(edges[0].sourceObservationIds).toEqual(["obs-a", "obs-b"]);
    }
  });

  it.each([undefined, "", "1,1", "3", "0", "1, 2,1", "abc"])(
    "rejects missing or invalid references %s before persisting provider output",
    async (references) => {
      const attribute = references === undefined ? "" : ` observations="${references}"`;
      const { result, nodes, edges } = await extract(`<entities><entity type="concept" name="Alpha"${attribute}/></entities>`);
      expect(result).toMatchObject({ success: false });
      expect(nodes).toEqual([]);
      expect(edges).toEqual([]);
    },
  );

  it("keeps the frozen legacy response path for an existing job", async () => {
    const { result, nodes } = await extract('<entities><entity type="concept" name="Legacy"/></entities>', true);
    expect(result).toMatchObject({ success: true, nodesAdded: 1 });
    expect(nodes[0].sourceObservationIds).toEqual(["obs-a", "obs-b"]);
  });

  it("keeps exact references in a versioned deferred callback", async () => {
    const xml = '<entities><entity type="concept" name="Only A" observations="1"/><entity type="concept" name="Only B" observations="2"/></entities>';
    const { result, nodes } = await extract(xml, false, true);
    expect(result).toMatchObject({ success: true });
    expect(Object.fromEntries(nodes.map((node) => [node.name, node.sourceObservationIds]))).toEqual({
      "Only A": ["obs-a"], "Only B": ["obs-b"],
    });
  });

  it("transports the version marker through deferred batch callbacks", async () => {
    const trigger = vi.fn().mockResolvedValue({ success: true });
    const complete = createFireworksBatchCompletionHandler({ trigger } as never);
    await complete({ id: "fwbwork-1", task: "graph_extraction", metadata: {
      observations: JSON.stringify(observations), graphProvenanceVersion: "2",
    } } as never, "<entities/>");
    expect(trigger.mock.calls[0][0].payload.graphProvenanceVersion).toBe(2);
  });
});
