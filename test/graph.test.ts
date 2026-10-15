import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

vi.mock("iii-sdk", () => ({ InvocationError: class InvocationError extends Error { code = "TEST_INVOCATION_ERROR"; } }));
vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { graphExtractionOptionsCompatible, registerGraphFunction } from "../src/functions/graph.js";
import type {
  CompressedObservation,
  GraphSnapshot,
  GraphNode,
  GraphEdge,
  GraphQueryResult,
} from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { persistGraphDelta } from "../src/functions/graph.js";
import { registerGraphJobRecovery } from "../src/functions/graph-jobs.js";
import { graphRecordDigest } from "../src/functions/graph-job-storage.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";
import * as config from "../src/config.js";

describe("captured graph configuration compatibility", () => {
  const captured = { graphEnabled: true, llmEnabled: true, auxiliary: false, targetChars: 8000,
    typeSafe: false, batchQueue: false, providerName: "captured-provider", localCompactor: false,
    typeSafeCompaction: false, typeSafeMaxStateChars: 512 };
  it("preserves captured interpretation when optional capabilities become enabled", () => {
    expect(graphExtractionOptionsCompatible(captured, { ...captured, typeSafe: true, typeSafeCompaction: true,
      batchQueue: true, localCompactor: true, targetChars: 16000, typeSafeMaxStateChars: 1024 })).toBe(true);
  });
  it.each([
    { providerName: "different" }, { auxiliary: true }, { llmEnabled: false }, { graphEnabled: false },
  ])("rejects a changed provider or an unavailable required capability %j", change => {
    expect(graphExtractionOptionsCompatible(captured, { ...captured, ...change })).toBe(false);
  });
  it("rejects malformed captured limits and loss of an originally required compactor", () => {
    expect(graphExtractionOptionsCompatible({ ...captured, targetChars: NaN }, captured)).toBe(false);
    expect(graphExtractionOptionsCompatible({ ...captured, localCompactor: true }, captured)).toBe(false);
  });
});

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  store.set(KV.config, new Map([["graph-dispatch:active", {
    jobId: null, generation: "1", baselineComplete: true, revision: "0", revisionGeneration: "1",
  }]]));
  store.set(KV.sessions, new Map([["ses_1", { project: "project-a" }]]));
  return {
    indexedRetrieval: true,
    retrieval: async <T>(payload: Record<string, unknown>): Promise<T> => {
      const project = typeof payload.project === "string" ? payload.project : "";
      const projectKnown = [...(store.get(KV.sessions)?.values() ?? [])].some((session) =>
        Boolean(session && typeof session === "object" && (session as { project?: unknown }).project === project),
      );
      const mode = payload.mode;
      const limit = typeof payload.limit === "number" ? Math.min(16, Math.max(1, payload.limit)) : 16;
      const maxBytes = typeof payload.max_bytes === "number" ? payload.max_bytes : 64 * 1024;
      const cursor = typeof payload.cursor === "string" && /^\d+$/.test(payload.cursor)
        ? Number(payload.cursor)
        : 0;
      const nodeRows = [...(store.get(KV.graphNodes)?.entries() ?? [])] as Array<[string, GraphNode]>;
      const edgeRows = [...(store.get(KV.graphEdges)?.entries() ?? [])] as Array<[string, GraphEdge]>;
      const nodes = new Map(nodeRows.map(([key, node]) => [node.id || key, node]));
      const edges = edgeRows.filter(([, edge]) =>
        nodes.has(edge.sourceNodeId) && nodes.has(edge.targetNodeId),
      );
      const partial = (reason: string) => ({
        status: "partial", reason, project, items: [], has_more: false,
      });
      if (!project || !projectKnown) return partial("project_membership_partial") as T;
      let matches: Array<Record<string, unknown>> = [];
      if (mode === "search") {
        const query = typeof payload.query === "string" ? payload.query.toLocaleLowerCase() : "";
        const observationIds = Array.isArray(payload.observation_ids)
          ? new Set(payload.observation_ids.filter((id): id is string => typeof id === "string"))
          : undefined;
        matches = nodeRows.flatMap(([key, node], index) => {
          const text = `${node.name} ${node.type} ${JSON.stringify(node.properties ?? {})}`.toLocaleLowerCase();
          const sourceMatch = observationIds && node.sourceObservationIds?.some((id) => observationIds.has(id));
          if (query && !text.includes(query) || observationIds && !sourceMatch) return [];
          return [{ key, id: node.id, position: String(index + 1), entity: true, observation: false }];
        });
      } else if (mode === "top_n") {
        const degrees = new Map<string, number>();
        for (const [, edge] of edges) {
          degrees.set(edge.sourceNodeId, (degrees.get(edge.sourceNodeId) ?? 0) + 1);
          degrees.set(edge.targetNodeId, (degrees.get(edge.targetNodeId) ?? 0) + 1);
        }
        matches = nodeRows
          .map(([key, node], index) => ({ key, id: node.id, position: String(index + 1), degree: degrees.get(node.id) ?? 0, value: node }))
          .sort((left, right) => Number(right.degree) - Number(left.degree) || String(left.id).localeCompare(String(right.id)));
      } else if (mode === "neighbors") {
        const requested = new Set(Array.isArray(payload.node_ids)
          ? payload.node_ids.filter((id): id is string => typeof id === "string")
          : []);
        matches = edges.flatMap(([key, edge], index) => {
          if (!requested.has(edge.sourceNodeId) && !requested.has(edge.targetNodeId)) return [];
          return [{ key, id: edge.id, position: String(index + 1), value: edge }];
        });
      } else if (mode === "aggregate") {
        const requestedNodes = Array.isArray(payload.node_ids)
          ? payload.node_ids.filter((id): id is string => typeof id === "string")
          : [];
        const requestedEdges = Array.isArray(payload.edge_ids)
          ? payload.edge_ids.filter((id): id is string => typeof id === "string")
          : [];
        const aggregate = {
          node_count: requestedNodes.filter((id) => nodes.has(id)).length,
          edge_count: requestedEdges.filter((id) => edges.some(([, edge]) => edge.id === id)).length,
        };
        return { status: "ready", project, items: [], has_more: false, aggregate } as T;
      } else if (mode === "history" || mode === "differential" || mode === "time") {
        const requested = new Set(Array.isArray(payload.edge_ids)
          ? payload.edge_ids.filter((id): id is string => typeof id === "string")
          : []);
        matches = edges.flatMap(([key, edge], index) => requested.has(edge.id)
          ? [{ key, id: edge.id, position: String(index + 1), value: edge }]
          : []);
      }
      const page = matches.slice(cursor, cursor + limit);
      const hasMore = cursor + page.length < matches.length;
      const result = {
        status: mode === "top_n" ? "partial" : "ready",
        ...(mode === "top_n" ? { reason: "candidate_top_n", approximate: true } : {}),
        project,
        items: page,
        has_more: hasMore,
        ...(hasMore ? { next_cursor: String(cursor + page.length) } : {}),
        ...(mode === "aggregate" ? { aggregate: { node_count: nodes.size, edge_count: edges.length } } : {}),
      };
      if (Buffer.byteLength(JSON.stringify(result), "utf8") > maxBytes) return partial("response_budget") as T;
      return result as T;
    },
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
    scopeRevision: async () => ({ generation: "1", revision: "0" }),
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
  };
}

function mockSdk() {
  const functions = new Map<string, Function>();
  const subscribers = new Map<string, string>();
  const deliveries: Array<{ topic: string; data: unknown }> = [];
  let deliveryObserver: (() => void) | undefined;
  const deliver = async () => {
    const item = deliveries.shift();
    if (!item) return null;
    const handler = functions.get(subscribers.get(item.topic) ?? "");
    if (!handler) throw new Error(`Missing durable subscriber for ${item.topic}`);
    const result = await handler(item.data);
    deliveryObserver?.();
    return result;
  };
  return {
    registerFunction: (idOrOpts: string | { id: string }, handler: Function) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      functions.set(id, handler);
    },
    callFunction: (id: string, payload?: unknown) => {
      const handler = functions.get(id);
      if (!handler) throw new Error(`No function: ${id}`);
      return handler(payload);
    },
    registerTrigger: (trigger: { function_id: string; config: { topic: string } }) => {
      subscribers.set(trigger.config.topic, trigger.function_id);
    },
    trigger: async (idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      if (id === "iii::durable::publish") {
        deliveries.push(payload as { topic: string; data: unknown });
        return { success: true };
      }
      const fn = functions.get(id);
      if (!fn) throw new Error(`No function: ${id}`);
      const result = await fn(payload) as { intentId?: unknown } | null;
      if (id === "mem::graph-extract" && typeof result?.intentId === "string") {
        await functions.get("mem::graph-dispatch")?.({ localOnly: false });
        let delivered: unknown = result;
        while (deliveries.length) delivered = await deliver();
        return delivered;
      }
      return result;
    },
    pendingDeliveries: () => deliveries.length,
    deliverNext: deliver,
    observeDeliveries: (observer: () => void) => { deliveryObserver = observer; },
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

  it("passes lightweight display pages through without hydrating originals", async () => {
    const localKv = mockKV();
    await localKv.set(KV.graphNodes, "n", { id: "n", name: "Original", properties: { provenance: "x".repeat(128 * 1024) } });
    const retrieval = vi.fn(async () => ({ status: "ready", project: "project-a", items: [{ id: "n", type: "file", name: "Display" }], edges: [], has_more: true, next_cursor: "cursor", revision: "1:1", edgeScope: "page", crossPageEdges: "not_loaded", edgesTruncated: false }));
    localKv.retrieval = retrieval as typeof localKv.retrieval;
    const localSdk = mockSdk();
    installGraphStateWire(localSdk as never, localKv as never);
    const get = vi.spyOn(localKv, "get");
    registerGraphFunction(localSdk as never, localKv as never, mockProvider as never);
    const result = await localSdk.trigger("mem::graph-query", { project: "project-a", display: true, limit: 128 });
    expect(result).toMatchObject({ display: true, nodes: [{ id: "n", type: "file", name: "Display" }], nextCursor: "cursor", hasMore: true, edgeScope: "page", crossPageEdges: "not_loaded", edgesTruncated: false });
    expect(get.mock.calls.some(([scope]) => scope === KV.graphNodes || scope === KV.graphEdges)).toBe(false);
    expect(retrieval).toHaveBeenCalledWith(expect.objectContaining({ mode: "browse", limit: 128, project: "project-a" }));
  });

  function browseBoundary(response: Record<string, unknown>) {
    const localKv = mockKV();
    const retrieval = vi.fn(async () => response);
    localKv.retrieval = retrieval as typeof localKv.retrieval;
    const localSdk = mockSdk();
    installGraphStateWire(localSdk as never, localKv as never);
    const get = vi.spyOn(localKv, "get");
    registerGraphFunction(localSdk as never, localKv as never, mockProvider as never);
    return { localSdk, retrieval, get };
  }

  const readyBrowsePage = {
    status: "ready", project: "project-a", items: [], edges: [], has_more: false,
    revision: "1:1", edgeScope: "page", crossPageEdges: "not_loaded", edgesTruncated: false,
  };

  it.each([
    { status: "preparing", reason: "memberships_pending", progress: { phase: "memberships", processed: 16 } },
    { status: "preparing", reason: "stale_graph_revision" },
    { status: "error", reason: "invalid_display_row" },
    { status: "ready" },
  ])("preserves Browse readiness and progress without manufacturing totals: %j", async state => {
    const { localSdk, retrieval, get } = browseBoundary({ ...readyBrowsePage, ...state });
    const result = await localSdk.trigger("mem::graph-query", { display: true, project: "project-a" });
    expect(result).toMatchObject({ display: true, ...state, nodes: [], edges: [], hasMore: false, nextCursor: null });
    expect(result).not.toHaveProperty("totalNodes");
    expect(result).not.toHaveProperty("totalEdges");
    expect(retrieval).toHaveBeenCalledTimes(1);
    expect(get).not.toHaveBeenCalled();
  });

  it.each([
    { status: "preparing", phase: "memberships", processed: 16, continue: true },
    { status: "ready", phase: "ready", processed: 0, continue: false },
    { status: "preparing", phase: "prerequisites", processed: 0, continue: false, reason: "index_not_ready" },
    { status: "error", phase: "display", processed: 0, continue: false, reason: "invalid_display_row" },
  ])("advances exactly one persisted native preparation batch: %j", async preparation => {
    const { localSdk, retrieval, get } = browseBoundary({ scope: "mem:graph:browse", project: "project-a", revision: "1:1", ...preparation });
    const result = await localSdk.trigger("mem::graph-query", { display: true, prepare: true, project: " project-a " });
    expect(result).toEqual({ display: true, project: "project-a", preparation: { ...preparation, revision: "1:1" } });
    expect(result).not.toHaveProperty("nodes");
    expect(result).not.toHaveProperty("edges");
    expect(retrieval).toHaveBeenCalledTimes(1);
    expect(retrieval).toHaveBeenCalledWith({ action: "index_prepare", scope: "mem:graph:browse", project: "project-a", max_rows: 256, max_bytes: 1048576 }, { timeoutMs: 310000 });
    expect(get).not.toHaveBeenCalled();
  });

  it.each([
    { prepare: true }, { display: false, prepare: false }, { display: true, prepare: "true" },
    { display: true, prepare: true, cursor: "opaque" }, { display: true, prepare: true, limit: 128 },
    { display: true, startNodeId: "" }, { display: true, query: "" },
    { display: true, limit: 0 }, { display: true, limit: null }, { display: true, cursor: "😀".repeat(1025) },
  ])("rejects direct ambiguous Browse requests without native effects: %j", async selectors => {
    const { localSdk, retrieval } = browseBoundary(readyBrowsePage);
    await expect(localSdk.trigger("mem::graph-query", { project: "project-a", ...selectors })).rejects.toThrow("STATE_TX_INVALID_REQUEST");
    expect(retrieval).not.toHaveBeenCalled();
  });

  it.each([
    { status: "partial" }, { project: "foreign" }, { has_more: true, next_cursor: "cursor" },
    { status: "preparing", items: [{ id: "n", type: "file", name: "N" }] },
    { edges: [{ id: "e", type: "uses", sourceNodeId: "other", targetNodeId: "other", weight: 1 }] },
    { progress: { phase: "memberships", processed: -1 } },
    { items: [{ id: "n", type: "file", name: "😀".repeat(257) }] },
  ])("rejects invalid native Browse states rather than returning empty success: %j", async invalid => {
    const { localSdk } = browseBoundary({ ...readyBrowsePage, ...invalid });
    await expect(localSdk.trigger("mem::graph-query", { project: "project-a", display: true })).rejects.toThrow("STATE_TX_INVALID_RESPONSE");
  });

  it("keeps projected Unicode labels and page edges without exposing native extras", async () => {
    const node = { id: "n", type: "file", name: "😀".repeat(256), sourceObservationIds: ["original"] };
    const edge = { id: "e", type: "uses", sourceNodeId: "n", targetNodeId: "n", weight: 1, context: { reasoning: "private" } };
    const { localSdk, get } = browseBoundary({ ...readyBrowsePage, items: [node], edges: [edge], edgesTruncated: true });
    const result = await localSdk.trigger("mem::graph-query", { project: "project-a", display: true, prepare: false });
    expect(result.nodes).toEqual([{ id: "n", type: "file", name: node.name }]);
    expect(result.edges).toEqual([{ id: "e", type: "uses", sourceNodeId: "n", targetNodeId: "n", weight: 1 }]);
    expect(result.edgesTruncated).toBe(true);
    expect(get).not.toHaveBeenCalled();
  });

  it("rejects repeated native continuations and oversized Browse envelopes", async () => {
    const { localSdk, retrieval } = browseBoundary({ ...readyBrowsePage, items: [{ id: "n", type: "file", name: "N" }], has_more: true, next_cursor: "cursor" });
    await expect(localSdk.trigger("mem::graph-query", { project: "project-a", display: true, cursor: "cursor" })).rejects.toThrow("STATE_TX_INVALID_RESPONSE");
    retrieval.mockResolvedValue({ ...readyBrowsePage, reason: "x".repeat(65536) });
    await expect(localSdk.trigger("mem::graph-query", { project: "project-a", display: true })).rejects.toThrow("STATE_RETRIEVAL_RESOURCE_LIMIT");
  });

  it("rejects invalid preparation replies without retrying", async () => {
    const { localSdk, retrieval } = browseBoundary({ scope: "mem:graph:browse", project: "project-a", status: "ready", phase: "ready", processed: 0, revision: "1:1", continue: true });
    await expect(localSdk.trigger("mem::graph-query", { project: "project-a", display: true, prepare: true })).rejects.toThrow("STATE_TX_INVALID_RESPONSE");
    expect(retrieval).toHaveBeenCalledTimes(1);
  });

  beforeEach(() => {
    sdk = mockSdk();
    kv = mockKV();
    installGraphStateWire(sdk as never, kv as never);
    vi.clearAllMocks();
    vi.spyOn(config, "isBackgroundRecoveryPaused").mockReturnValue(false);
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
    vi.restoreAllMocks();
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

  it("retains structural graph data and reports grouped extraction as incomplete when routed LLM extraction fails", async () => {
    const provider = { name: "test", compress: vi.fn().mockRejectedValue(new Error("temporary failure")), summarize: vi.fn() };
    registerGraphFunction(sdk as never, kv as never, provider);
    const result = await sdk.trigger("mem::graph-extract", { observations: [{ ...testObs, files: ["src/index.ts"], concepts: ["startup"] }] });
    expect(result).toMatchObject({ success: false, incomplete: true, nodesAdded: 2, edgesAdded: 1, error: "temporary failure" });
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

  it("processes at most one grouped provider unit per durable delivery", async () => {
    const localSdk = mockSdk();
    const localKv = mockKV();
    const harness = installGraphStateWire(localSdk as never, localKv as never);
    const compress = vi.fn().mockResolvedValue(`<entities>
<entity type="file" name="src/grouped.ts" observations="1"/>
</entities>`);
    registerGraphFunction(localSdk as never, localKv as never, { name: "grouped", compress, summarize: vi.fn() });
    const initial = await localSdk.callFunction("mem::graph-extract", {
      observations: [{ ...testObs, narrative: "bounded grouped source ".repeat(1_500) }],
    }) as { success: boolean; queued?: boolean };

    expect(initial).toMatchObject({ success: true, queued: true });
    const callsPerDelivery: number[] = [];
    const originalTrigger = localSdk.trigger.bind(localSdk);
    localSdk.trigger = async (...args: unknown[]) => {
      const call = args[0] as { function_id?: string } | undefined;
      const isGraphDelivery = call?.function_id === "mem::graph-dispatch-deliver";
      const previousCalls = compress.mock.calls.length;
      const delivered = await originalTrigger(...args);
      if (isGraphDelivery) callsPerDelivery.push(compress.mock.calls.length - previousCalls);
      return delivered;
    };
    const results = await harness.drainDeliveries();
    const result = results.at(-1);
    expect(result).toMatchObject({ success: true });
    expect(compress.mock.calls.length).toBeGreaterThan(1);
    expect(callsPerDelivery.length).toBeGreaterThan(0);
    expect(callsPerDelivery.every((count) => count === 1)).toBe(true);
    expect(compress.mock.calls.length).toBe(callsPerDelivery.length + 1);
  });

  it.each([["before effects", 1], ["during effect checkpointing", 2]] as const)(
    "reuses the captured provider result after a cut %s",
    async (_cut, matchingGraphCommit) => {
      const priorTime = Date.now();
      vi.useFakeTimers();
      try {
        const localSdk = mockSdk();
        const localKv = mockKV();
        const harness = installGraphStateWire(localSdk as never, localKv as never);
        registerGraphJobRecovery(localSdk as never, localKv as never);
        const response = `<entities>${Array.from({ length: 280 }, (_, index) =>
          `<entity type="concept" name="c${index}" observations="1"/>`).join("")}</entities>`;
        const compress = vi.fn().mockResolvedValue(response);
        registerGraphFunction(localSdk as never, localKv as never, { name: "captured-result", compress, summarize: vi.fn() });
        const originalCommit = harness.kv.commitBatch.bind(harness.kv);
        let graphNodeCommits = 0;
        let simulatedCut = false;
        harness.kv.commitBatch = async (guard, prepared) => {
          const operations = JSON.parse(prepared.payload_json).operations as Array<{ scope: string; type: string }>;
          const hasNodeSets = operations.some((operation) => operation.scope === KV.graphNodes && operation.type === "set");
          if (hasNodeSets) graphNodeCommits++;
          const shouldCut = !simulatedCut && hasNodeSets && graphNodeCommits === matchingGraphCommit;
          if (shouldCut) {
            simulatedCut = true;
            throw Object.assign(new Error("simulated graph application cut"), { code: "STATE_TX_FAILED" });
          }
          return originalCommit(guard, prepared);
        };

        const initial = await localSdk.callFunction("mem::graph-extract", {
          observations: [{ ...testObs, facts: [], narrative: "cut recovery source" }],
        }) as { queued?: boolean };
        expect(initial).toMatchObject({ queued: true });
        expect(simulatedCut).toBe(true);
        expect(compress).toHaveBeenCalledTimes(1);
        const beforeRecoveryNodes = await localKv.list(KV.graphNodes);
        if (matchingGraphCommit === 1) expect(beforeRecoveryNodes).toHaveLength(0);
        else expect(beforeRecoveryNodes.length).toBeGreaterThan(0);
        expect(beforeRecoveryNodes.length).toBeLessThan(280);

        vi.setSystemTime(priorTime + 31_000);
        const recovery = await localSdk.callFunction("mem::graph-recover", { localOnly: true }) as { recovered?: boolean };
        expect(recovery).toMatchObject({ success: true, recovered: true });
        expect(compress).toHaveBeenCalledTimes(1);
        expect(await localKv.list(KV.graphNodes)).toHaveLength(280);
        const savedJob = (await localKv.list<any>(KV.graphJobs)).find((job) => job?.kind === "extraction");
        expect(savedJob?.state).toBe("completed");
      } finally {
        vi.useRealTimers();
      }
    },
  );

  it("leaves a later grouped provider failure terminal and incomplete", async () => {
    const localSdk = mockSdk();
    const localKv = mockKV();
    const harness = installGraphStateWire(localSdk as never, localKv as never);
    registerGraphJobRecovery(localSdk as never, localKv as never);
    const compress = vi.fn()
      .mockResolvedValueOnce(`<entities><entity type="concept" name="first-unit" observations="1"/></entities>`)
      .mockRejectedValueOnce(new Error("provider unavailable"));
    registerGraphFunction(localSdk as never, localKv as never, { name: "grouped-failure", compress, summarize: vi.fn() });

    const initial = await localSdk.callFunction("mem::graph-extract", {
      observations: [{ ...testObs, narrative: "grouped failure source ".repeat(1_500) }],
    }) as { queued?: boolean };
    expect(initial).toMatchObject({ queued: true });
    const results = await harness.drainDeliveries();
    const result = results.at(-1) as Record<string, unknown>;

    expect(result).toMatchObject({ success: false, incomplete: true, terminalFailure: true, failureCode: "GRAPH_WORK_UNIT_FAILED" });
    expect(compress).toHaveBeenCalledTimes(2);
    expect(results.length).toBeGreaterThan(0);
    expect((await localKv.list<any>(KV.graphJobs)).find((job) => job.kind === "extraction"))
      .toMatchObject({ state: "failed", recoveryStopped: true, failureCode: "GRAPH_WORK_UNIT_FAILED" });
    expect((await localKv.list<any>(KV.graphDispatchIntents))[0]).toMatchObject({ state: "failed" });
  });

  it("resumes a grouped session request before rereading the session source", async () => {
    const localSdk = mockSdk();
    const localKv = mockKV();
    const harness = installGraphStateWire(localSdk as never, localKv as never);
    registerGraphJobRecovery(localSdk as never, localKv as never);
    const compress = vi.fn().mockResolvedValue(`<entities><entity type="concept" name="resumed-unit" observations="1"/></entities>`);
    registerGraphFunction(localSdk as never, localKv as never, { name: "session-resume", compress, summarize: vi.fn() });
    const initial = await localSdk.callFunction("mem::graph-extract", {
      observations: [{ ...testObs, narrative: "session continuation source ".repeat(1_500) }],
    }) as { queued?: boolean };
    expect(initial).toMatchObject({ queued: true });

    const intent = (await localKv.list<any>(KV.graphDispatchIntents))[0];
    const job = (await localKv.list<any>(KV.graphJobs)).find((entry) => entry.id === intent.jobId);
    expect(job).toBeDefined();
    const resumedRequest = { graphProvenanceVersion: 2, graphWorkGroupsVersion: 2, sessionId: "resume-session" };
    const serialized = JSON.stringify(resumedRequest);
    const digest = graphRecordDigest(serialized);
    harness.seed(KV.graphInputs(job.id), "capture:0", serialized);
    harness.seed(KV.graphJobs, job.id, { ...job, captureParts: 1, captureDigest: digest });
    harness.seed(KV.graphDispatchInputs(intent.id), "part:0", serialized);
    harness.seed(KV.graphDispatchIntents, intent.id, { ...intent, digest, captureParts: 1 });

    let sessionReads = 0;
    const originalPages = harness.kv.pages.bind(harness.kv);
    harness.kv.pages = ((scope: string, options?: unknown) => {
      if (scope === KV.observations("resume-session")) {
        sessionReads++;
        throw new Error("resumed grouped work must not reread its session source");
      }
      return originalPages(scope, options as never);
    }) as typeof harness.kv.pages;

    const results = await harness.drainDeliveries();
    const result = results.at(-1);

    expect(sessionReads).toBe(0);
    expect(compress.mock.calls.length).toBeGreaterThan(1);
    expect(result).toMatchObject({ success: true });
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
      project: "project-a",
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
      project: "project-a",
    })) as GraphQueryResult;

    expect(result.nodes.length).toBeGreaterThanOrEqual(1);
    expect(result.edges.length).toBeGreaterThanOrEqual(1);
    expect(result.depth).toBe(2);
  });

  it("graph-stats reports when scoped aggregate counts are unavailable", async () => {
    await sdk.trigger("mem::graph-extract", { observations: [testObs] });

    const result = (await sdk.trigger("mem::graph-stats", { project: "project-a" })) as {
      totalNodes: number | null;
      totalEdges: number | null;
      nodesByType: Record<string, number>;
      edgesByType: Record<string, number>;
      partial: boolean;
      reason: string;
    };

    expect(result).toMatchObject({
      totalNodes: null,
      totalEdges: null,
      nodesByType: {},
      edgesByType: {},
      partial: true,
      reason: "project_aggregate_unavailable",
    });
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

  it("requires project scope for graph browse and caps scoped pages before hydration", async () => {
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

    const unscoped = (await sdk.trigger("mem::graph-query", {})) as GraphQueryResult;
    expect(unscoped.nodes).toEqual([]);
    expect(unscoped.edges).toEqual([]);
    expect(unscoped.truncated).toBe(true);
    expect(unscoped.warning).toMatch(/Project scope is required/);

    const scoped = (await sdk.trigger("mem::graph-query", { project: "project-a" })) as GraphQueryResult;
    expect(scoped.nodes).toHaveLength(16);
    expect(scoped.totalNodes).toBe(16);
    expect(scoped.truncated).toBe(true);
    expect(scoped.limit).toBe(16);
    expect(scoped.edges.length).toBeLessThanOrEqual(16);
    expect(scoped.warning).toMatch(/bounded|partial/i);
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

    await sdk.trigger("mem::graph-snapshot-rebuild", { force: true, project: "project-a" });

    const page1 = (await sdk.trigger("mem::graph-query", {
      project: "project-a",
      limit: 10,
      offset: 0,
    })) as GraphQueryResult;
    const page2 = (await sdk.trigger("mem::graph-query", {
      project: "project-a",
      limit: 10,
      offset: 10,
    })) as GraphQueryResult;

    expect(page1.nodes.length).toBe(10);
    expect(page2.nodes.length).toBe(10);
    expect(page1.totalNodes).toBe(10);
    expect(page2.totalNodes).toBe(10);
    expect(page1.truncated).toBe(true);
    expect(page2.truncated).toBe(true);
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

    await sdk.trigger("mem::graph-snapshot-rebuild", { force: true, project: "project-a" });
    const huge = (await sdk.trigger("mem::graph-query", {
      project: "project-a",
      limit: 999999,
    })) as GraphQueryResult;
    expect(huge.limit).toBe(16);
    expect(huge.nodes.length).toBe(10);
    expect(huge.truncated).toBe(true);
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

    await sdk.trigger("mem::graph-snapshot-rebuild", { force: true, project: "project-a" });

    const page = (await sdk.trigger("mem::graph-query", {
      project: "project-a",
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
    expect(page.totalEdges).toBe(10);
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

      const result = await sdk.trigger("mem::graph-extract", {
          observations: [testObs],
          batchResponse: '<entities><entity type="concept" name="batch-new"/></entities>',
          batchEffectKey: "a".repeat(64),
        }) as { success: boolean; queued?: boolean; deferred?: boolean };
      expect(result).toMatchObject({ success: true, queued: true, deferred: true });

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
      const transactionalKV = recordingKV as unknown as {
        commitBatch: (guard: unknown, prepared: { payload_json: string }) => Promise<unknown>;
      };
      const commitBatch = transactionalKV.commitBatch.bind(transactionalKV);
      transactionalKV.commitBatch = async (guard, prepared) => {
        const body = JSON.parse(prepared.payload_json) as {
          operations?: Array<{ scope?: unknown; key?: unknown; type?: unknown; value?: unknown }>;
        };
        for (const operation of body.operations ?? []) {
          if (operation.type === "set" && operation.scope === KV.graphSnapshot && operation.key === "current") {
            writes.push(structuredClone(operation.value as GraphSnapshot));
          }
        }
        return commitBatch(guard, prepared);
      };
      registerGraphFunction(localSdk as never, recordingKV as never, mockProvider as never);

      const result = await localSdk.trigger("mem::graph-extract", {
        observations,
        batchEffectKey: key,
        batchResponse:
          '<entities><entity type="concept" name="batch-alpha"/><entity type="concept" name="batch-beta"/></entities>' +
          '<relationships><relationship type="related_to" source="batch-alpha" target="batch-beta" weight="0.9"/></relationships>',
      });

      expect(result).toMatchObject({ success: true });
      const persistedSnapshot = await kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
      expect(persistedSnapshot).toBeDefined();
      expect(persistedSnapshot?.batchInProgress).toBeUndefined();
      expect(persistedSnapshot?.appliedBatchEffects).toContain(key);
      expect(writes).toHaveLength(2);
      for (const snapshot of writes) {
        expect(snapshot.batchInProgress).toBeUndefined();
        expect(snapshot.appliedBatchEffects).toContain(key);
      }
      expect(persistedSnapshot).toEqual(writes[1]);
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

    it("returns a bounded project sample without rewriting the canonical graph", async () => {
      const ids = Array.from({ length: 90 }, (_, i) => `rebuild_obs_${i}`);
      const nodes = [graphNode("gn_rebuild_a", "rebuild alpha", ids), graphNode("gn_rebuild_b", "rebuild beta", ids)];
      const edge = graphEdge("ge_rebuild", nodes[0].id, nodes[1].id, ids);
      for (const node of nodes) await kv.set(KV.graphNodes, node.id, node);
      await kv.set(KV.graphEdges, edge.id, edge);

      const result = (await sdk.trigger("mem::graph-snapshot-rebuild", {
        force: true,
        project: "project-a",
      })) as { success: boolean; partial?: boolean; approximate?: boolean; sampledNodes?: number; sampledEdges?: number };

      expect(result).toMatchObject({ success: true, partial: true, approximate: true, sampledNodes: 2, sampledEdges: 1 });
      expect((await kv.get<GraphNode>(KV.graphNodes, nodes[0].id))?.sourceObservationIds).toHaveLength(90);
      const snapshot = await kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
      expect(snapshot).toBeNull();
    });

    it("keeps oversized candidates out of the bounded snapshot while persisting canonical rows", async () => {
      const existing = graphNode("gn_existing", "existing");
      const priorSnapshot = populatedSnapshot(existing);
      await kv.set(KV.graphNodes, existing.id, existing);
      await kv.set(KV.graphSnapshot, "current", priorSnapshot);
      const oversized = graphNode("gn_large", "large");
      oversized.properties.payload = "x".repeat(4 * 1024 * 1024);

      const result = await persistGraphDelta(kv as never, [oversized], [], ["obs_large"]);
      expect(result.newNodeCount).toBe(1);
      expect((await kv.get<GraphNode>(KV.graphNodes, oversized.id))?.properties.payload).toHaveLength(4 * 1024 * 1024);
      const snapshot = await kv.get<GraphSnapshot>(KV.graphSnapshot, "current");
      expect(snapshot?.topNodes.some((node) => node.id === oversized.id)).toBe(false);
      expect(snapshot?.stats.totalNodes).toBe(priorSnapshot.stats.totalNodes + 1);
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

    it("snapshot rebuild returns bounded sample counts without claiming project totals", async () => {
      await seed(50, 100);
      const result = (await sdk.trigger("mem::graph-snapshot-rebuild", { force: true, project: "project-a" })) as {
        success: boolean;
        partial: boolean;
        approximate: boolean;
        sampledNodes: number;
        sampledEdges: number;
        aggregate: { node_count: number; edge_count?: number };
      };
      expect(result).toMatchObject({ success: true, partial: true, approximate: true, sampledNodes: 16 });
      expect(result.sampledEdges).toBeLessThanOrEqual(16);
      expect(result.aggregate.node_count).toBe(16);
      expect(await kv.get(KV.graphSnapshot, "current")).toBeNull();
    });

    it("graph-query browses a bounded project page instead of reading a global snapshot", async () => {
      await seed(20, 30);
      await sdk.trigger("mem::graph-snapshot-rebuild", { force: true, project: "project-a" });

      const result = (await sdk.trigger("mem::graph-query", { project: "project-a" })) as GraphQueryResult;
      expect(result.fromSnapshot).not.toBe(true);
      expect(result.nodes.length).toBe(16);
      expect(result.totalNodes).toBe(16);
      expect(result.truncated).toBe(true);
    });

    it("graph-query nodeType filter keeps the candidate page project-scoped", async () => {
      await seed(30, 0);
      await sdk.trigger("mem::graph-snapshot-rebuild", { force: true, project: "project-a" });

      const fileQuery = (await sdk.trigger("mem::graph-query", {
        project: "project-a",
        nodeType: "file",
      })) as GraphQueryResult;
      expect(fileQuery.fromSnapshot).not.toBe(true);
      expect(fileQuery.totalNodes).toBe(10);
      expect(fileQuery.truncated).toBe(true);
      for (const n of fileQuery.nodes) {
        expect(n.type).toBe("file");
      }
    });

    it("graph-stats does not report cached global totals as project counts", async () => {
      await seed(15, 25);
      await kv.set(KV.graphSnapshot, "current", populatedSnapshot(graphNode("gn_stats", "stats")));

      const stats = (await sdk.trigger("mem::graph-stats", { project: "project-a" })) as {
        totalNodes: number | null;
        totalEdges: number | null;
        fromSnapshot: boolean;
        partial: boolean;
        reason: string;
      };
      expect(stats).toMatchObject({
        fromSnapshot: false,
        totalNodes: null,
        totalEdges: null,
        partial: true,
        reason: "project_aggregate_unavailable",
      });
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

    it("graph-stats distinguishes unavailable snapshot metadata from zero counts", async () => {
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
      expect(stats.totalNodes).toBeNull();
      expect(stats).toMatchObject({ provenance: "unavailable", countsAreCurrent: false, partial: true });
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

    it("graph-query proves project membership without enumerating the graph", async () => {
      const slow = slowKV(7000); // > LIVE_ENUMERATION_BUDGET_MS (6000ms)
      const localSdk = mockSdk();
      registerGraphFunction(localSdk as never, slow as never, mockProvider as never);

      const result = (await localSdk.trigger("mem::graph-query", {
        startNodeId: "n_missing",
        project: "project-a",
      })) as GraphQueryResult;

      expect(result.warning).toBeTruthy();
      expect(result.warning).toMatch(/proven|project/i);
    });

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

    it("graph-query remains partial when the legacy list path would fail", async () => {
      const rejector = rejectingKV();
      const localSdk = mockSdk();
      registerGraphFunction(
        localSdk as never,
        rejector as never,
        mockProvider as never,
      );

      const result = (await localSdk.trigger("mem::graph-query", {
        startNodeId: "n_missing",
        project: "project-a",
      })) as GraphQueryResult;

      expect(result.warning).toBeTruthy();
      expect(result.nodes).toEqual([]);
      expect(result.warning).toMatch(/proven|project/i);
    });

    it("graph-snapshot-rebuild samples large projects through bounded native pages", async () => {
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
        { force: true, project: "project-a" },
      )) as { success: boolean; partial?: boolean; approximate?: boolean; sampledNodes?: number };
      expect(result).toMatchObject({ success: true, partial: true, approximate: true, sampledNodes: 16 });
    });

    it("graph-snapshot-rebuild requires project scope even for a legacy corpus", async () => {
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
      )) as { success: boolean; partial?: boolean; reason?: string; error?: string };
      expect(result.success).toBe(false);
      expect(result.partial).toBe(true);
      expect(result.reason).toBe("project_required");
      expect(result.error).toMatch(/project/i);
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
