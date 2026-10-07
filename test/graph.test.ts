import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { registerGraphFunction } from "../src/functions/graph.js";
import type {
  CompressedObservation,
  GraphSnapshot,
  GraphNode,
  GraphEdge,
  GraphQueryResult,
} from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { persistGraphDelta } from "../src/functions/graph.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      return (store.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
  };
}

function mockSdk() {
  const functions = new Map<string, Function>();
  return {
    registerFunction: (idOrOpts: string | { id: string }, handler: Function) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      functions.set(id, handler);
    },
    registerTrigger: () => {},
    trigger: async (idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      const fn = functions.get(id);
      if (!fn) throw new Error(`No function: ${id}`);
      return fn(payload);
    },
  };
}

const mockProvider = {
  name: "test",
  compress: vi.fn().mockResolvedValue(`<entities>
<entity type="file" name="src/index.ts" observations="1"><property key="path">src/index.ts</property></entity>
<entity type="function" name="main" observations="1"><property key="lang">typescript</property></entity>
</entities>
<relationships>
<relationship type="uses" source="src/index.ts" target="main" weight="0.9" observations="1"/>
</relationships>`),
  summarize: vi.fn(),
};

// Structured fields stay empty so the deterministic heuristic pass
// contributes nothing and these tests keep exercising the LLM XML
// parse + persist path in isolation.
const testObs: CompressedObservation = {
  id: "obs_1",
  sessionId: "ses_1",
  timestamp: "2026-02-01T10:00:00Z",
  type: "file_edit",
  title: "Edit index file",
  facts: ["Modified main function"],
  narrative: "Updated index.ts with main function",
  concepts: [],
  files: [],
  importance: 7,
};

async function withGraphInputTarget<T>(work: () => Promise<T>): Promise<T> {
  const previous = process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"];
  process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"] = "4000";
  try {
    return await work();
  } finally {
    if (previous === undefined) delete process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"];
    else process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"] = previous;
  }
}

describe("Graph Functions", () => {
  let sdk: ReturnType<typeof mockSdk>;
  let kv: ReturnType<typeof mockKV>;
  const ORIG_GRAPH_FLAG = process.env["GRAPH_EXTRACTION_ENABLED"];

  beforeEach(() => {
    sdk = mockSdk();
    kv = mockKV();
    installGraphStateWire(sdk as never, kv as never);
    vi.clearAllMocks();
    process.env["GRAPH_EXTRACTION_ENABLED"] = "true";
    registerGraphFunction(sdk as never, kv as never, mockProvider as never);
  });

  it("partitions replacement sources without dropping observations and rejects an oversized source", async () => {
    const enqueue = vi.fn(async (_request: import("../src/types.js").FireworksBatchRequest) => ({ queued: true, workItemId: "fresh" }));
    registerGraphFunction(sdk as never, kv as never, mockProvider as never, undefined, { enqueue });
    const observations = Array.from({ length: 4 }, (_, i) => ({ ...testObs, id: `obs-${i}`, narrative: "x".repeat(5000) }));
    const result = await sdk.trigger("mem::graph-extract", { observations, deferred: true, replacementOf: "old" });
    expect(result.success).toBe(true);
    expect(enqueue.mock.calls.length).toBeGreaterThan(1);
    const requests = enqueue.mock.calls.map((call) => call[0] as unknown as import("../src/types.js").FireworksBatchRequest);
    expect(requests.flatMap((request) => JSON.parse(request.metadata!.observations).map((o: CompressedObservation) => o.id))).toEqual(observations.map((o) => o.id));
    for (const request of requests) {
      expect(request.userPrompt.length).toBeLessThanOrEqual(10000);
      expect(request.replacementOf).toBe("old");
      expect(request.metadata?.sourceFingerprint).toBeTruthy();
    }
    enqueue.mockClear();
    const oversized = await sdk.trigger("mem::graph-extract", { observations: [{ ...testObs, narrative: "x".repeat(20000) }], deferred: true, replacementOf: "old" });
    expect(oversized.success).toBe(false);
    expect(enqueue).not.toHaveBeenCalled();
  });

  afterEach(() => {
    if (ORIG_GRAPH_FLAG === undefined) delete process.env["GRAPH_EXTRACTION_ENABLED"];
    else process.env["GRAPH_EXTRACTION_ENABLED"] = ORIG_GRAPH_FLAG;
  });

  it("persists structural graph data keyless without calling an LLM", async () => {
    process.env["GRAPH_EXTRACTION_ENABLED"] = "false";
    const compress = vi.fn();
    registerGraphFunction(sdk as never, kv as never, { name: "noop", compress, summarize: vi.fn() });
    const result = await sdk.trigger("mem::graph-extract", { observations: [{ ...testObs, files: ["src/index.ts"], concepts: ["startup"] }] });
    expect(result).toMatchObject({ success: true, nodesAdded: 2, edgesAdded: 1 });
    expect(compress).not.toHaveBeenCalled();
    expect(await kv.list(KV.graphNodes)).toHaveLength(2);
  });

  it("retains structural graph data when routed LLM extraction fails", async () => {
    const provider = { name: "test", compress: vi.fn().mockRejectedValue(new Error("temporary failure")), summarize: vi.fn() };
    registerGraphFunction(sdk as never, kv as never, provider);
    const result = await sdk.trigger("mem::graph-extract", { observations: [{ ...testObs, files: ["src/index.ts"], concepts: ["startup"] }] });
    expect(result).toMatchObject({ success: true, nodesAdded: 2, edgesAdded: 1, llmError: "temporary failure" });
    expect(await kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await kv.list(KV.graphEdges)).toHaveLength(1);
  });

  it("graph-extract creates nodes and edges from XML response", async () => {
    const result = (await sdk.trigger("mem::graph-extract", {
      observations: [testObs],
    })) as { success: boolean; nodesAdded: number; edgesAdded: number };

    expect(result.success).toBe(true);
    expect(result.nodesAdded).toBe(2);
    expect(result.edgesAdded).toBe(1);

    const nodes = await kv.list<GraphNode>("mem:graph:nodes");
    expect(nodes.length).toBe(2);
    expect(nodes.find((n) => n.name === "src/index.ts")).toBeDefined();
    expect(nodes.find((n) => n.name === "main")).toBeDefined();

    const edges = await kv.list<GraphEdge>("mem:graph:edges");
    expect(edges.length).toBe(1);
    expect(edges[0].type).toBe("uses");
  });

  it("graph-extract accepts self-closing entity tags", async () => {
    mockProvider.compress.mockResolvedValueOnce(`<entities>
<entity type="file" name="src/index.ts" observations="1"/>
<entity type="function" name="main" observations="1"><property key="lang">typescript</property></entity>
</entities>
<relationships>
<relationship type="uses" source="src/index.ts" target="main" weight="0.9" observations="1"/>
</relationships>`);

    const result = (await sdk.trigger("mem::graph-extract", {
      observations: [testObs],
    })) as { success: boolean; nodesAdded: number; edgesAdded: number };

    expect(result.success).toBe(true);
    expect(result.nodesAdded).toBe(2);
    expect(result.edgesAdded).toBe(1);

    const nodes = await kv.list<GraphNode>("mem:graph:nodes");
    expect(nodes.some((n) => n.name === "src/index.ts")).toBe(true);
    expect(nodes.some((n) => n.name === "main")).toBe(true);

    const edges = await kv.list<GraphEdge>("mem:graph:edges");
    expect(edges).toHaveLength(1);
    expect(edges[0].type).toBe("uses");
  });

  it("graph-extract tolerates reordered attributes (#635)", async () => {
    // Codex CLI's LLM tends to emit attribute order name→type and
    // source→target→type rather than the hard-coded type-first /
    // type/source/target/weight sequence the old parser required.
    mockProvider.compress.mockResolvedValueOnce(`<entities>
<entity name="src/index.ts" type="file" observations="1"/>
<entity name="main" type="function" observations="1"><property key="lang">typescript</property></entity>
</entities>
<relationships>
<relationship source="src/index.ts" target="main" type="uses" weight="0.9" observations="1"/>
</relationships>`);

    const result = (await sdk.trigger("mem::graph-extract", {
      observations: [testObs],
    })) as { success: boolean; nodesAdded: number; edgesAdded: number };

    expect(result.success).toBe(true);
    expect(result.nodesAdded).toBe(2);
    expect(result.edgesAdded).toBe(1);

    const nodes = await kv.list<GraphNode>("mem:graph:nodes");
    expect(nodes.find((n) => n.name === "src/index.ts")?.type).toBe("file");
    expect(nodes.find((n) => n.name === "main")?.type).toBe("function");

    const edges = await kv.list<GraphEdge>("mem:graph:edges");
    expect(edges).toHaveLength(1);
    expect(edges[0].type).toBe("uses");
    expect(edges[0].weight).toBeCloseTo(0.9, 5);
  });

  it("graph-query with search returns matching nodes", async () => {
    await sdk.trigger("mem::graph-extract", { observations: [testObs] });

    const result = (await sdk.trigger("mem::graph-query", {
      query: "index",
    })) as GraphQueryResult;

    expect(result.nodes.length).toBeGreaterThanOrEqual(1);
    expect(result.nodes.some((n) => n.name.includes("index"))).toBe(true);
  });

  it("graph-query with startNodeId does BFS traversal", async () => {
    await sdk.trigger("mem::graph-extract", { observations: [testObs] });

    const nodes = await kv.list<GraphNode>("mem:graph:nodes");
    const fileNode = nodes.find((n) => n.name === "src/index.ts")!;

    const result = (await sdk.trigger("mem::graph-query", {
      startNodeId: fileNode.id,
      maxDepth: 2,
    })) as GraphQueryResult;

    expect(result.nodes.length).toBeGreaterThanOrEqual(1);
    expect(result.edges.length).toBeGreaterThanOrEqual(1);
    expect(result.depth).toBe(2);
  });

  it("graph-stats returns counts by type", async () => {
    await sdk.trigger("mem::graph-extract", { observations: [testObs] });

    const result = (await sdk.trigger("mem::graph-stats", {})) as {
      totalNodes: number;
      totalEdges: number;
      nodesByType: Record<string, number>;
      edgesByType: Record<string, number>;
    };

    expect(result.totalNodes).toBe(2);
    expect(result.totalEdges).toBe(1);
    expect(result.nodesByType.file).toBe(1);
    expect(result.nodesByType.function).toBe(1);
    expect(result.edgesByType.uses).toBe(1);
  });

  it("graph-extract returns error for empty observations", async () => {
    const result = (await sdk.trigger("mem::graph-extract", {
      observations: [],
    })) as { success: boolean; error: string };

    expect(result.success).toBe(false);
    expect(result.error).toContain("No observations");
  });

  it("graph-extract rejects empty parsed output before writing graph state", async () => {
    mockProvider.compress.mockResolvedValueOnce("<entities/><relationships/>");

    const result = (await sdk.trigger("mem::graph-extract", {
      observations: [testObs],
    })) as { success: boolean; error: string };

    expect(result).toMatchObject({
      success: false,
      error: "Graph extraction response contained no nodes or edges",
    });
    expect(await kv.list(KV.graphNodes)).toHaveLength(0);
    expect(await kv.list(KV.graphEdges)).toHaveLength(0);
    expect(await kv.list(KV.graphNameIndex)).toHaveLength(0);
    expect(await kv.list(KV.graphEdgeKey)).toHaveLength(0);
    expect(await kv.list(KV.graphNodeDegree)).toHaveLength(0);
    expect(await kv.list(KV.graphSnapshot)).toHaveLength(0);
    expect(await kv.list(KV.audit)).toHaveLength(0);
    expect(await kv.list(KV.batchCallbacks)).toHaveLength(0);
  });

  it("graph-extract accepts nodes without relationships", async () => {
    mockProvider.compress.mockResolvedValueOnce(
      '<entities><entity type="concept" name="standalone" observations="1"/></entities>',
    );

    const result = (await sdk.trigger("mem::graph-extract", {
      observations: [testObs],
    })) as { success: boolean; nodesAdded: number; edgesAdded: number };

    expect(result).toMatchObject({
      success: true,
      nodesAdded: 1,
      edgesAdded: 0,
    });
    expect((await kv.get<{ stats: { totalNodes: number; totalEdges: number } }>(
      KV.graphSnapshot,
      "current",
    ))?.stats).toMatchObject({ totalNodes: 1, totalEdges: 0 });
  });

  it("compacts an oversized routine locally and preserves complete source metadata", async () => {
    await withGraphInputTarget(async () => {
      const primary = {
        name: "primary",
        compress: vi.fn(),
        summarize: vi.fn(),
      };
      const localCompactor = {
        name: "resilient(ollama)",
        compress: vi.fn(),
        summarize: vi.fn().mockResolvedValue("routine digest with exact file src/routine.ts"),
      };
      let request: { userPrompt: string; metadata?: Record<string, string> } | undefined;
      const batchQueue = {
        enqueue: vi.fn(async (input: { userPrompt: string; metadata?: Record<string, string> }) => {
          request = input;
          return { queued: true, workItemId: "work-routine" };
        }),
      };
      const localSdk = mockSdk();
      const localKv = mockKV();
      installGraphStateWire(localSdk as never, localKv as never);
      registerGraphFunction(
        localSdk as never,
        localKv as never,
        primary as never,
        undefined,
        batchQueue as never,
        localCompactor as never,
      );
      const source: CompressedObservation = {
        ...testObs,
        id: "routine-large",
        type: "conversation",
        title: "Routine context",
        narrative: "ROUTINE-NARRATIVE-".repeat(1_000),
        files: ["src/routine.ts"],
      };

      const result = await localSdk.trigger("mem::graph-extract", {
        observations: [source],
        deferred: true,
      }) as { success: boolean; queued: boolean };

      expect(result).toMatchObject({ success: true, queued: true });
      expect(localCompactor.summarize).toHaveBeenCalledTimes(1);
      expect(primary.compress).not.toHaveBeenCalled();
      expect(request?.userPrompt).toContain("routine digest with exact file");
      expect(request?.userPrompt).not.toContain(source.narrative);
      const persisted = JSON.parse(request!.metadata!.observations);
      expect(persisted).toHaveLength(1);
      expect(persisted[0]).toMatchObject({ id: source.id, narrative: source.narrative });
    });
  });

  it("compacts an oversized decision locally instead of truncating it", async () => {
    await withGraphInputTarget(async () => {
      const localCompactor = {
        name: "resilient(ollama)",
        compress: vi.fn(),
        summarize: vi.fn().mockResolvedValue("decision digest preserving the selected architecture"),
      };
      let request: { userPrompt: string } | undefined;
      const batchQueue = {
        enqueue: vi.fn(async (input: { userPrompt: string }) => {
          request = input;
          return { queued: true, workItemId: "work-decision" };
        }),
      };
      const localSdk = mockSdk();
      const localKv = mockKV();
      installGraphStateWire(localSdk as never, localKv as never);
      registerGraphFunction(
        localSdk as never,
        localKv as never,
        mockProvider as never,
        undefined,
        batchQueue as never,
        localCompactor as never,
      );
      const source: CompressedObservation = {
        ...testObs,
        id: "decision-large",
        type: "decision",
        title: "Architecture decision",
        narrative: "DECISION-NARRATIVE-".repeat(1_000),
      };

      const result = await localSdk.trigger("mem::graph-extract", {
        observations: [source],
        deferred: true,
      }) as { success: boolean; queued: boolean };

      expect(result).toMatchObject({ success: true, queued: true });
      expect(localCompactor.summarize).toHaveBeenCalledTimes(1);
      expect(request?.userPrompt).toContain("decision digest preserving");
      expect(request?.userPrompt).not.toContain(source.narrative);
    });
  });

  it("does not call the primary when deferred batch queue rejects work", async () => {
    const primary = {
      name: "primary",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    const batchQueue = {
      enqueue: vi.fn().mockResolvedValue({ queued: false, reason: "Batch queue is full" }),
    };
    const localSdk = mockSdk();
    const localKv = mockKV();
    installGraphStateWire(localSdk as never, localKv as never);
    registerGraphFunction(
      localSdk as never,
      localKv as never,
      primary as never,
      undefined,
      batchQueue as never,
    );

    const result = await localSdk.trigger("mem::graph-extract", {
      observations: [testObs],
      deferred: true,
    }) as { success: boolean; deferred: boolean; queued: boolean; error: string };

    expect(result).toMatchObject({ success: false, deferred: true, queued: false });
    expect(result.error).toContain("full");
    expect(batchQueue.enqueue).toHaveBeenCalledTimes(1);
    expect(primary.compress).not.toHaveBeenCalled();
  });

  // #753: an unbounded {} body used to materialize every node+edge in
  // one payload, which exceeded the iii state response channel on
  // large corpora (11k+ nodes) and returned HTTP 500 "Invocation
  // stopped". The fix caps the page at DEFAULT_GRAPH_QUERY_LIMIT (500)
  // and surfaces totalNodes / totalEdges so callers know it was
  // truncated.
  it("caps an unbounded graph-query body to a default page and reports totals", async () => {
    // Seed a graph with more nodes than the default page size.
    const NODE_COUNT = 1200;
    for (let i = 0; i < NODE_COUNT; i++) {
      const node: GraphNode = {
        id: `n_${i.toString().padStart(4, "0")}`,
        type: "concept",
        name: `node-${i}`,
        properties: {},
        firstSeen: "2026-01-01T00:00:00Z",
        lastSeen: "2026-01-01T00:00:00Z",
        observationCount: 1,
      } as GraphNode;
      await kv.set("mem:graph:nodes", node.id, node);
    }
    // A few edges among the first 50 nodes so high-degree ranking has
    // something to grade.
    for (let i = 0; i < 50; i++) {
      const edge: GraphEdge = {
        id: `e_${i}`,
        type: "related_to",
        sourceNodeId: `n_${i.toString().padStart(4, "0")}`,
        targetNodeId: `n_${((i + 1) % 50).toString().padStart(4, "0")}`,
        weight: 1,
        evidence: [],
        firstSeen: "2026-01-01T00:00:00Z",
        lastSeen: "2026-01-01T00:00:00Z",
      } as GraphEdge;
      await kv.set("mem:graph:edges", edge.id, edge);
    }

    // Query coverage uses a ready snapshot; dedicated rebuild cases cover
    // backfilling indexes through the transaction harness.
    const topNodes = (await kv.list<GraphNode>(KV.graphNodes)).slice(0, 500);
    await kv.set(KV.graphSnapshot, "current", {
      version: 1,
      topNodes,
      topEdges: await kv.list<GraphEdge>(KV.graphEdges),
      topDegrees: Object.fromEntries(topNodes.map((node, index) => [node.id, index < 50 ? 2 : 0])),
      stats: {
        totalNodes: NODE_COUNT,
        totalEdges: 50,
        nodesByType: { concept: NODE_COUNT },
        edgesByType: { related_to: 50 },
      },
      updatedAt: "2026-01-01T00:00:00Z",
      dirty: false,
    } satisfies GraphSnapshot);

    const unbounded = (await sdk.trigger(
      "mem::graph-query",
      {},
    )) as GraphQueryResult;

    expect(unbounded.totalNodes).toBe(NODE_COUNT);
    expect(unbounded.nodes.length).toBe(500);
    expect(unbounded.truncated).toBe(true);
    expect(unbounded.limit).toBe(500);
    expect(unbounded.offset).toBe(0);
    // The 50 connected nodes should be on the first page since the
    // default ranks by degree.
    const connectedOnPage = unbounded.nodes.filter((n) => /^n_00[0-4]\d$/.test(n.id));
    expect(connectedOnPage.length).toBe(50);
  });

  it("honors limit and offset for paged graph-query traversal", async () => {
    for (let i = 0; i < 50; i++) {
      const node: GraphNode = {
        id: `p_${i.toString().padStart(3, "0")}`,
        type: "concept",
        name: `node-${i}`,
        properties: {},
        firstSeen: "2026-01-01T00:00:00Z",
        lastSeen: "2026-01-01T00:00:00Z",
        observationCount: 1,
      } as GraphNode;
      await kv.set("mem:graph:nodes", node.id, node);
    }

    await sdk.trigger("mem::graph-snapshot-rebuild", { force: true });

    const page1 = (await sdk.trigger("mem::graph-query", {
      limit: 10,
      offset: 0,
    })) as GraphQueryResult;
    const page2 = (await sdk.trigger("mem::graph-query", {
      limit: 10,
      offset: 10,
    })) as GraphQueryResult;

    expect(page1.nodes.length).toBe(10);
    expect(page2.nodes.length).toBe(10);
    expect(page1.totalNodes).toBe(50);
    expect(page2.totalNodes).toBe(50);
    expect(page1.truncated).toBe(true);
    // The two pages must not overlap.
    const overlap = page1.nodes.filter((n) =>
      page2.nodes.some((p) => p.id === n.id),
    );
    expect(overlap.length).toBe(0);
  });

  it("clamps an explicit limit above the cap to the cap value", async () => {
    for (let i = 0; i < 10; i++) {
      await kv.set("mem:graph:nodes", `c_${i}`, {
        id: `c_${i}`,
        type: "concept",
        name: `n-${i}`,
        properties: {},
        firstSeen: "2026-01-01T00:00:00Z",
        lastSeen: "2026-01-01T00:00:00Z",
        observationCount: 1,
      });
    }

    await sdk.trigger("mem::graph-snapshot-rebuild", { force: true });

    const huge = (await sdk.trigger("mem::graph-query", {
      limit: 999999,
    })) as GraphQueryResult;
    expect(huge.limit).toBeLessThanOrEqual(5000);
    expect(huge.nodes.length).toBe(10);
    expect(huge.truncated).toBe(false);
  });

  it("paginate excludes edges whose endpoints fall outside the page", async () => {
    for (let i = 0; i < 60; i++) {
      await kv.set("mem:graph:nodes", `x_${i.toString().padStart(3, "0")}`, {
        id: `x_${i.toString().padStart(3, "0")}`,
        type: "concept",
        name: `n-${i}`,
        properties: {},
        firstSeen: "2026-01-01T00:00:00Z",
        lastSeen: "2026-01-01T00:00:00Z",
        observationCount: 1,
      });
    }
    // Make the first 10 nodes a tightly connected cluster so they
    // rank highest by degree and land on the page deterministically.
    for (let i = 0; i < 10; i++) {
      const next = (i + 1) % 10;
      await kv.set("mem:graph:edges", `cluster_${i}`, {
        id: `cluster_${i}`,
        type: "related_to",
        sourceNodeId: `x_${i.toString().padStart(3, "0")}`,
        targetNodeId: `x_${next.toString().padStart(3, "0")}`,
        weight: 1,
        evidence: [],
        firstSeen: "2026-01-01T00:00:00Z",
        lastSeen: "2026-01-01T00:00:00Z",
      });
    }
    // Cross-page edge: source in the high-degree cluster (on page),
    // target is an isolated node (degree 1; cluster nodes have
    // degree 2 so the target ranks below the cap).
    await kv.set("mem:graph:edges", "cross", {
      id: "cross",
      type: "related_to",
      sourceNodeId: "x_005",
      targetNodeId: "x_055",
      weight: 1,
      evidence: [],
      firstSeen: "2026-01-01T00:00:00Z",
      lastSeen: "2026-01-01T00:00:00Z",
    });

    await sdk.trigger("mem::graph-snapshot-rebuild", { force: true });

    const page = (await sdk.trigger("mem::graph-query", {
      limit: 10,
      offset: 0,
    })) as GraphQueryResult;
    // The cross-page edge should not appear in the page response —
    // otherwise the viewer renders a dangling line to a node it
    // doesn't have.
    expect(page.edges.find((e) => e.id === "cross")).toBeUndefined();
    // Cluster edges among page nodes ARE present.
    expect(page.edges.filter((e) => e.id.startsWith("cluster_")).length).toBe(10);
    // totalEdges counts every edge in the full result universe.
    expect(page.totalEdges).toBe(11);
  });

  // #814: precomputed snapshot path. The viewer-tab default-cap query
  // and graph-stats both have to work at 75K-node scale where the
  // full kv.list enumeration exceeds the iii invocation budget.
  describe("snapshot cache (#814)", () => {
    async function seed(nodeCount: number, edgeCount: number) {
      for (let i = 0; i < nodeCount; i++) {
        await kv.set("mem:graph:nodes", `n_${i}`, {
          id: `n_${i}`,
          type: i % 3 === 0 ? "file" : "function",
          name: `node-${i}`,
          properties: {},
          sourceObservationIds: [`obs_${i}`],
          firstSeen: "2026-01-01T00:00:00Z",
          lastSeen: "2026-01-01T00:00:00Z",
          observationCount: 1,
          stale: false,
        });
      }
      for (let i = 0; i < edgeCount; i++) {
        const src = `n_${i % nodeCount}`;
        const dst = `n_${(i + 1) % nodeCount}`;
        await kv.set("mem:graph:edges", `e_${i}`, {
          id: `e_${i}`,
          type: i % 2 === 0 ? "uses" : "imports",
          sourceNodeId: src,
          targetNodeId: dst,
          weight: 1,
          evidence: [],
          sourceObservationIds: [`obs_${i}`],
          firstSeen: "2026-01-01T00:00:00Z",
          lastSeen: "2026-01-01T00:00:00Z",
          stale: false,
        });
      }
    }

    function graphNode(
      id: string,
      name: string,
      sourceObservationIds = ["obs_graph_snapshot_guard"],
    ): GraphNode {
      return {
        id,
        type: "concept",
        name,
        properties: {},
        sourceObservationIds,
        createdAt: "2026-09-29T00:00:00.000Z",
      };
    }

    function graphEdge(
      id: string,
      sourceNodeId: string,
      targetNodeId: string,
      sourceObservationIds: string[],
    ): GraphEdge {
      return {
        id,
        type: "related_to",
        sourceNodeId,
        targetNodeId,
        weight: 0.9,
        sourceObservationIds,
        createdAt: "2026-09-29T00:00:00.000Z",
      };
    }

    function populatedSnapshot(node: GraphNode): GraphSnapshot {
      return {
        version: 1,
        topNodes: [node],
        topEdges: [],
        topDegrees: { [node.id]: 0 },
        stats: {
          totalNodes: 39618,
          totalEdges: 28695,
          nodesByType: { concept: 39618 },
          edgesByType: { related_to: 28695 },
        },
        updatedAt: "2026-09-29T00:00:00.000Z",
        dirty: false,
      };
    }

    it("does not replace the graph snapshot when its read times out", async () => {
      const existing = graphNode("gn_existing", "existing");
      const priorSnapshot = populatedSnapshot(existing);
      await kv.set(KV.graphNodes, existing.id, existing);
      await kv.set(KV.graphSnapshot, "current", priorSnapshot);
      const timeoutKV = {
        ...kv,
        get: async <T>(scope: string, key: string): Promise<T | null> => {
          if (scope === KV.graphSnapshot && key === "current") {
            throw new Error("state read timeout");
          }
          return kv.get<T>(scope, key);
        },
      };
      installGraphStateWire(undefined, timeoutKV as never);

      await expect(
        persistGraphDelta(
          timeoutKV as never,
          [graphNode("gn_new", "new")],
          [],
          ["obs_new"],
        ),
      ).rejects.toMatchObject({ code: "STATE_TX_FAILED" });

      expect(await kv.get(KV.graphSnapshot, "current")).toEqual(priorSnapshot);
      expect(await kv.list(KV.graphNodes)).toEqual([existing]);
      expect(await kv.list(KV.graphEdges)).toEqual([]);
    });

    it("refuses graph writes when the stored snapshot is malformed", async () => {
      const existing = graphNode("gn_existing", "existing");
      const malformed = {
        version: 1,
        topNodes: [],
        topEdges: [],
        topDegrees: {},
        stats: { totalNodes: 39618 },
        updatedAt: "2026-09-29T00:00:00.000Z",
        dirty: false,
      };
      await kv.set(KV.graphNodes, existing.id, existing);
      await kv.set(KV.graphSnapshot, "current", malformed);

      await expect(
        persistGraphDelta(
          kv as never,
          [graphNode("gn_new", "new")],
          [],
          ["obs_new"],
        ),
      ).rejects.toThrow("Graph snapshot is malformed");

      expect(await kv.get(KV.graphSnapshot, "current")).toEqual(malformed);
      expect(await kv.list(KV.graphNodes)).toEqual([existing]);
    });

    it("keeps batch graph writes from replacing a malformed snapshot", async () => {
      const malformed = {
        version: 1,
        topNodes: [],
        topEdges: [],
        topDegrees: {},
        stats: { totalNodes: 39618 },
        updatedAt: "2026-09-29T00:00:00.000Z",
        dirty: false,
      };
      await kv.set(KV.graphSnapshot, "current", malformed);

      await expect(
        sdk.trigger("mem::graph-extract", {
          observations: [testObs],
          batchResponse: '<entities><entity type="concept" name="batch-new"/></entities>',
          batchEffectKey: "a".repeat(64),
        }),
      ).rejects.toThrow("Graph snapshot is malformed");

      expect(await kv.get(KV.graphSnapshot, "current")).toEqual(malformed);
      expect(await kv.list(KV.graphNodes)).toHaveLength(0);
    });

    it("allows the first graph write to initialize a genuinely empty store", async () => {
      const result = await persistGraphDelta(
        kv as never,
        [graphNode("gn_first", "first")],
        [],
        ["obs_first"],
      );

      expect(result.newNodeCount).toBe(1);
      expect(await kv.get(KV.graphSnapshot, "current")).toMatchObject({
        version: 1,
        stats: { totalNodes: 1, totalEdges: 0 },
        dirty: false,
      });
    });

    it("caps cached provenance across repeated graph writes without changing canonical rows", async () => {
      const initialIds = Array.from({ length: 100 }, (_, i) => `obs_${String(i).padStart(3, "0")}`);
      const addedIds = Array.from({ length: 10 }, (_, i) => `obs_${String(i + 100).padStart(3, "0")}`);
      const initialNodes = [
        graphNode("gn_a_initial", "alpha", initialIds),
        graphNode("gn_b_initial", "beta", initialIds),
      ];
      const initialEdge = graphEdge("ge_initial", "gn_a_initial", "gn_b_initial", initialIds);
      await persistGraphDelta(kv as never, initialNodes, [initialEdge], initialIds);

      await persistGraphDelta(
        kv as never,
        [
          graphNode("gn_a_next", "alpha", addedIds),
          graphNode("gn_b_next", "beta", addedIds),
        ],
        [graphEdge("ge_next", "gn_a_next", "gn_b_next", addedIds)],
        addedIds,
      );

      const canonicalNodes = await kv.list<GraphNode>(KV.graphNodes);
      const canonicalEdges = await kv.list<GraphEdge>(KV.graphEdges);
      const snapshot = await kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
      expect(canonicalNodes.map((node) => node.sourceObservationIds.length)).toEqual([110, 110]);
      expect(canonicalEdges[0].sourceObservationIds).toHaveLength(110);
      expect(snapshot?.topNodes.map((node) => node.sourceObservationIds.length)).toEqual([64, 64]);
      expect(snapshot?.topEdges[0].sourceObservationIds).toHaveLength(64);
      expect(snapshot?.topNodes[0].sourceObservationIds[0]).toBe("obs_046");
      expect(snapshot?.topNodes[0].sourceObservationIds.at(-1)).toBe("obs_109");
      expect(initialNodes[0].sourceObservationIds).toHaveLength(100);
      expect(initialEdge.sourceObservationIds).toHaveLength(100);
    });

    it("projects both batch snapshot writes and retains batch metadata", async () => {
      const observations = Array.from({ length: 100 }, (_, i) => ({
        ...testObs,
        id: `batch_obs_${i}`,
      }));
      const key = "b".repeat(64);
      const writes: GraphSnapshot[] = [];
      const recordingKV = {
        ...kv,
        set: async <T>(scope: string, id: string, value: T): Promise<T> => {
          if (scope === KV.graphSnapshot && id === "current") {
            writes.push(structuredClone(value as GraphSnapshot));
          }
          return kv.set(scope, id, value);
        },
      };
      const localSdk = mockSdk();
      installGraphStateWire(localSdk as never, recordingKV as never);
      registerGraphFunction(localSdk as never, recordingKV as never, mockProvider as never);

      const result = await localSdk.trigger("mem::graph-extract", {
        observations,
        batchEffectKey: key,
        batchResponse:
          '<entities><entity type="concept" name="batch-alpha"/><entity type="concept" name="batch-beta"/></entities>' +
          '<relationships><relationship type="related_to" source="batch-alpha" target="batch-beta" weight="0.9"/></relationships>',
      });

      expect(result).toMatchObject({ success: true });
      expect(writes).toHaveLength(1);
      expect(writes[0].batchInProgress).toBeUndefined();
      expect(writes[0].appliedBatchEffects).toContain(key);
      expect(await kv.get<GraphSnapshot>(KV.graphSnapshot, "current")).toEqual(writes[0]);
      for (const snapshot of writes) {
        expect(snapshot.topNodes.every((node) => node.sourceObservationIds.length <= 64)).toBe(true);
        expect(snapshot.topEdges.every((edge) => edge.sourceObservationIds.length <= 64)).toBe(true);
      }
      expect((await kv.list<GraphNode>(KV.graphNodes)).map((node) => node.sourceObservationIds)).toEqual([
        observations.map((observation) => observation.id),
        observations.map((observation) => observation.id),
      ]);
      expect((await kv.list<GraphEdge>(KV.graphEdges))[0].sourceObservationIds).toHaveLength(100);
    });

    it("projects rebuilt snapshots while keeping complete canonical provenance", async () => {
      const ids = Array.from({ length: 90 }, (_, i) => `rebuild_obs_${i}`);
      const nodes = [graphNode("gn_rebuild_a", "rebuild alpha", ids), graphNode("gn_rebuild_b", "rebuild beta", ids)];
      const edge = graphEdge("ge_rebuild", nodes[0].id, nodes[1].id, ids);
      for (const node of nodes) await kv.set(KV.graphNodes, node.id, node);
      await kv.set(KV.graphEdges, edge.id, edge);

      const result = (await sdk.trigger("mem::graph-snapshot-rebuild", {
        force: true,
      })) as { success: boolean };

      expect(result.success).toBe(true);
      expect((await kv.get<GraphNode>(KV.graphNodes, nodes[0].id))?.sourceObservationIds).toHaveLength(90);
      const snapshot = await kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
      expect(snapshot?.topNodes.every((node) => node.sourceObservationIds.length === 64)).toBe(true);
      expect(snapshot?.topEdges[0].sourceObservationIds).toHaveLength(64);
      expect(snapshot?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
    });

    it("fails closed when a projected snapshot exceeds its byte bound", async () => {
      const existing = graphNode("gn_existing", "existing");
      const priorSnapshot = populatedSnapshot(existing);
      await kv.set(KV.graphNodes, existing.id, existing);
      await kv.set(KV.graphSnapshot, "current", priorSnapshot);
      const oversized = graphNode("gn_large", "large");
      oversized.properties.payload = "x".repeat(4 * 1024 * 1024);

      await expect(
        persistGraphDelta(kv as never, [oversized], [], ["obs_large"]),
      ).rejects.toThrow(/over the 4194304-byte cache limit/);

      expect(await kv.get(KV.graphSnapshot, "current")).toEqual(priorSnapshot);
    });

    it("does not force snapshot rebuild after a failed snapshot read", async () => {
      const existing = graphNode("gn_existing", "existing");
      const priorSnapshot = populatedSnapshot(existing);
      await kv.set(KV.graphNodes, existing.id, existing);
      await kv.set(KV.graphSnapshot, "current", priorSnapshot);
      const timeoutKV = {
        ...kv,
        get: async <T>(scope: string, key: string): Promise<T | null> => {
          if (scope === KV.graphSnapshot && key === "current") {
            throw new Error("state read timeout");
          }
          return kv.get<T>(scope, key);
        },
        list: async <T>(scope: string): Promise<T[]> => {
          if (scope === KV.graphNodes || scope === KV.graphEdges) graphCorpusEnumerations.push(scope);
          return kv.list<T>(scope);
        },
      };
      const localSdk = mockSdk();
      const graphCorpusEnumerations: string[] = [];
      installGraphStateWire(localSdk as never, timeoutKV as never);
      registerGraphFunction(localSdk as never, timeoutKV as never, mockProvider as never);

      await expect(localSdk.trigger("mem::graph-snapshot-rebuild", {
        force: true,
      })).rejects.toMatchObject({ code: "STATE_TX_FAILED" });

      expect(graphCorpusEnumerations).toEqual([]);
      expect(await kv.get(KV.graphSnapshot, "current")).toEqual(priorSnapshot);
      expect(await kv.list(KV.graphNodes)).toEqual([existing]);
      expect(await kv.list(KV.graphEdges)).toEqual([]);
    });

    it("snapshot-rebuild persists top-degree subgraph + aggregate stats", async () => {
      await seed(50, 100);
      const result = (await sdk.trigger("mem::graph-snapshot-rebuild", { force: true })) as {
        success: boolean;
        totalNodes: number;
        totalEdges: number;
        topNodes: number;
        topEdges: number;
      };
      expect(result.success).toBe(true);
      expect(result.totalNodes).toBe(50);
      expect(result.totalEdges).toBe(100);
      // 50 nodes is below the SNAPSHOT_TOP_NODES cap, so every node
      // lands in the snapshot.
      expect(result.topNodes).toBe(50);

      const snap = await kv.get<{
        version: number;
        topNodes: unknown[];
        stats: { totalNodes: number; nodesByType: Record<string, number> };
      }>("mem:graph:snapshot", "current");
      expect(snap).not.toBeNull();
      expect(snap!.version).toBe(1);
      expect(snap!.stats.totalNodes).toBe(50);
      // nodesByType reflects every type seen.
      expect(snap!.stats.nodesByType["file"]).toBeGreaterThan(0);
      expect(snap!.stats.nodesByType["function"]).toBeGreaterThan(0);
    });

    it("graph-query empty-body branch serves from snapshot once it exists", async () => {
      await seed(20, 30);
      await sdk.trigger("mem::graph-snapshot-rebuild", { force: true });

      const result = (await sdk.trigger("mem::graph-query", {})) as GraphQueryResult;
      expect(result.fromSnapshot).toBe(true);
      expect(result.totalNodes).toBe(20);
      expect(result.totalEdges).toBe(30);
    });

    it("graph-query nodeType filter respects snapshot type counts", async () => {
      await seed(30, 0);
      await sdk.trigger("mem::graph-snapshot-rebuild", { force: true });

      const fileQuery = (await sdk.trigger("mem::graph-query", {
        nodeType: "file",
      })) as GraphQueryResult;
      expect(fileQuery.fromSnapshot).toBe(true);
      // 30 nodes, every 3rd is "file" → 10 files.
      expect(fileQuery.totalNodes).toBe(10);
      for (const n of fileQuery.nodes) {
        expect(n.type).toBe("file");
      }
    });

    it("graph-stats returns from snapshot when not dirty", async () => {
      await seed(15, 25);
      await sdk.trigger("mem::graph-snapshot-rebuild", { force: true });

      const stats = (await sdk.trigger("mem::graph-stats", {})) as {
        totalNodes: number;
        totalEdges: number;
        fromSnapshot: boolean;
      };
      expect(stats.fromSnapshot).toBe(true);
      expect(stats.totalNodes).toBe(15);
      expect(stats.totalEdges).toBe(25);
    });

    it("graph-extract updates snapshot inline (no kv.list, dirty stays false)", async () => {
      // Post-#814 v2 the snapshot is updated incrementally on every
      // extract — no dirty flag bounces. Test asserts that after an
      // extract the snapshot reflects the new nodes/edges.
      await sdk.trigger("mem::graph-extract", { observations: [testObs] });

      const snap = await kv.get<{
        dirty: boolean;
        stats: { totalNodes: number };
      }>("mem:graph:snapshot", "current");
      expect(snap?.dirty).toBe(false);
      // testObs produces 2 nodes (src/index.ts, main) + 1 edge.
      expect(snap?.stats.totalNodes).toBeGreaterThanOrEqual(1);
    });

    it("graph-extract maintains name-index for O(1) dedup on re-extract", async () => {
      // First extract creates nodes.
      await sdk.trigger("mem::graph-extract", { observations: [testObs] });
      const nameIndex = await kv.get<string>(
        "mem:graph:name-index",
        "file|src/index.ts",
      );
      expect(typeof nameIndex).toBe("string");

      // Re-extract the same observation. With name-index lookup the
      // existing node merges; no duplicates.
      await sdk.trigger("mem::graph-extract", { observations: [testObs] });
      const nodes = await kv.list<{ name: string; type: string }>(
        "mem:graph:nodes",
      );
      const fileNodes = nodes.filter(
        (n) => n.name === "src/index.ts" && n.type === "file",
      );
      expect(fileNodes.length).toBe(1);
    });

    it("graph-stats returns empty envelope + warning when no snapshot exists", async () => {
      // Seed nodes but never rebuild the snapshot — simulates a legacy
      // corpus on a post-#814 upgrade.
      await seed(5, 5);

      const stats = (await sdk.trigger("mem::graph-stats", {})) as {
        totalNodes: number;
        totalEdges: number;
        fromSnapshot: boolean;
        warning?: string;
      };
      expect(stats.fromSnapshot).toBe(false);
      expect(stats.totalNodes).toBe(0);
      expect(stats.warning).toMatch(/snapshot-rebuild|graph\/reset/);
    });

    it("graph-reset clears state and writes empty snapshot", async () => {
      await sdk.trigger("mem::graph-extract", { observations: [testObs] });
      const result = (await sdk.trigger("mem::graph-reset", {})) as {
        success: boolean;
        cleared: Record<string, number>;
      };
      expect(result.success).toBe(true);

      const snap = await kv.get<{
        stats: { totalNodes: number };
      }>("mem:graph:snapshot", "current");
      expect(snap?.stats.totalNodes).toBe(0);
    });

    it("graph-reset writes empty snapshot; legacy rows stay as orphans (#825)", async () => {
      await sdk.trigger("mem::graph-extract", { observations: [testObs] });
      // Index entries exist after the extract.
      const nameBefore = await kv.get(
        "mem:graph:name-index",
        "file|src/index.ts",
      );
      expect(nameBefore).not.toBeNull();

      await sdk.trigger("mem::graph-reset", {});

      // Post-#825: reset is enumeration-free. It writes an empty
      // snapshot; the legacy index rows remain on disk as orphans
      // but are never read by any post-#816 code path (hot path
      // reads only the snapshot, which is now empty). Asserting the
      // visible behavior: snapshot empty, hot path returns empty.
      const snap = await kv.get<{
        stats: { totalNodes: number; totalEdges: number };
        resetAt: string;
      }>("mem:graph:snapshot", "current");
      expect(snap?.stats.totalNodes).toBe(0);
      expect(snap?.stats.totalEdges).toBe(0);
      expect(snap?.resetAt).toMatch(/^2026-/);
    });
  });

  // CodeRabbit feedback: cover the timeout-budget fallback path and
  // the oversized-corpus rebuild refusal. The hot path never enumerates
  // any more, but the rebuild endpoint AND the BFS / query branches
  // still call kv.list — both need explicit failure-mode tests.
  describe("budget + tooLarge guards (#814 v2)", () => {
    function slowKV(delayMs: number) {
      const base = mockKV();
      return {
        ...base,
        list: async <T>(scope: string): Promise<T[]> => {
          await new Promise((r) => setTimeout(r, delayMs));
          return base.list<T>(scope);
        },
      };
    }

    it("graph-query startNodeId returns warning envelope when enumeration exceeds budget", async () => {
      const slow = slowKV(7000); // > LIVE_ENUMERATION_BUDGET_MS (6000ms)
      const localSdk = mockSdk();
      registerGraphFunction(localSdk as never, slow as never, mockProvider as never);

      const result = (await localSdk.trigger("mem::graph-query", {
        startNodeId: "n_missing",
      })) as GraphQueryResult;

      expect(result.warning).toBeTruthy();
      expect(result.warning).toMatch(/budget|enumeration/i);
    }, 10000);

    // CodeRabbit raised that slowKV(setTimeout) doesn't simulate a
    // blocked event loop. The real production failure is iii rejecting
    // the trigger with "Invocation stopped" after the worker dies
    // (heartbeat starvation). A rejecting kv.list mock covers that
    // catch-path directly without introducing a busy-wait that would
    // also starve the budget timer and produce a flaky test.
    function rejectingKV() {
      const base = mockKV();
      return {
        ...base,
        list: async <T>(_scope: string): Promise<T[]> => {
          throw new Error("Invocation stopped");
        },
      };
    }

    it("graph-query rejects-from-engine path returns warning envelope (worker-death simulation)", async () => {
      const rejector = rejectingKV();
      const localSdk = mockSdk();
      registerGraphFunction(
        localSdk as never,
        rejector as never,
        mockProvider as never,
      );

      const result = (await localSdk.trigger("mem::graph-query", {
        startNodeId: "n_missing",
      })) as GraphQueryResult;

      expect(result.warning).toBeTruthy();
      expect(result.nodes).toEqual([]);
    });

    it("graph-snapshot-rebuild refuses corpora past REBUILD_SAFE_NODE_CEILING", async () => {
      // Direct-poke the mock store with > 25K node values so kv.list
      // returns them without paying the per-set cost. Each node only
      // needs id/type/name/stale=false for the rebuild path.
      const localKv = mockKV();
      // Walk the implementation detail: mockKV stores entries in a
      // Map under the scope key. Push directly to that map via the
      // public `set` API in a tight loop.
      const COUNT = 25001;
      const sets: Array<Promise<unknown>> = [];
      for (let i = 0; i < COUNT; i++) {
        sets.push(
          localKv.set("mem:graph:nodes", `bn_${i}`, {
            id: `bn_${i}`,
            type: "concept",
            name: `bulk-${i}`,
            properties: {},
            sourceObservationIds: [],
            createdAt: "2026-01-01T00:00:00Z",
            stale: false,
          }),
        );
      }
      await Promise.all(sets);

      const localSdk = mockSdk();
      installGraphStateWire(localSdk as never, localKv as never);
      registerGraphFunction(localSdk as never, localKv as never, mockProvider as never);

      const result = (await localSdk.trigger(
        "mem::graph-snapshot-rebuild",
        { force: true },
      )) as { success: boolean; tooLarge?: boolean; totalNodes?: number };
      expect(result.success).toBe(false);
      expect(result.tooLarge).toBe(true);
      expect(result.totalNodes).toBeGreaterThanOrEqual(25001);
    });

    // #825: new pre-flight refusal when no snapshot exists (signals
    // legacy corpus that would crash on kv.list). force=true bypasses.
    it("graph-snapshot-rebuild refuses on legacy corpus (no snapshot) without force", async () => {
      const localKv = mockKV();
      // Seed nodes but never persist a snapshot → simulates a corpus
      // built on a pre-#814 agentmemory.
      await localKv.set("mem:graph:nodes", "legacy_n", {
        id: "legacy_n",
        type: "concept",
        name: "legacy",
        properties: {},
        sourceObservationIds: [],
        createdAt: "2026-01-01T00:00:00Z",
        stale: false,
      });
      const localSdk = mockSdk();
      installGraphStateWire(localSdk as never, localKv as never);
      registerGraphFunction(localSdk as never, localKv as never, mockProvider as never);

      const result = (await localSdk.trigger(
        "mem::graph-snapshot-rebuild",
        {},
      )) as { success: boolean; legacyCorpus?: boolean; error?: string };
      expect(result.success).toBe(false);
      expect(result.legacyCorpus).toBe(true);
      expect(result.error).toMatch(/graph\/reset|force/);
    });

    it("does not enumerate graph nodes or edges during graph reset", async () => {
      const localKv = mockKV();
      const graphCorpusEnumerations: string[] = [];
      const baseList = localKv.list;
      localKv.list = async <T,>(scope: string): Promise<T[]> => {
        if (scope === KV.graphNodes || scope === KV.graphEdges) graphCorpusEnumerations.push(scope);
        return baseList.call(localKv, scope) as Promise<T[]>;
      };
      const localSdk = mockSdk();
      installGraphStateWire(localSdk as never, localKv as never);
      registerGraphFunction(localSdk as never, localKv as never, mockProvider as never);

      const result = (await localSdk.trigger("mem::graph-reset", {})) as {
        success: boolean;
      };
      expect(result.success).toBe(true);
      expect(graphCorpusEnumerations).toEqual([]);
    });
  });
});
