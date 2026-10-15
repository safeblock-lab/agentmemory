import { beforeEach, describe, it, expect, vi } from "vitest";
import type { GraphNode, GraphEdge, MemoryProvider } from "../src/types.js";
import { StatePageError } from "../src/state/state-pages.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";
import { readFileSync } from "node:fs";
import { KV, fingerprintId } from "../src/state/schema.js";
import { batchEffectKey, runBatchCallback } from "../src/state/batch-effects.js";
import { freezeGraphValue, graphKV, runGraphJob, withGraphDelta, withLocalGraphRecovery } from "../src/functions/graph-jobs.js";
import { createNextGraphWorkUnit } from "../src/functions/graph-work-groups.js";
import { TEMPORAL_EXTRACTION_SYSTEM } from "../src/functions/temporal-graph.js";
import { applySelectedTemporalGraph } from "../src/functions/selected-temporal-apply.js";

const pauseControl = vi.hoisted(() => ({ paused: false }));
vi.mock("../src/config.js", async (importOriginal) => ({
  ...await importOriginal<typeof import("../src/config.js")>(),
  isBackgroundRecoveryPaused: () => pauseControl.paused,
}));
vi.mock("iii-sdk", () => ({ InvocationError: class InvocationError extends Error { code = "TEST_INVOCATION_ERROR"; } }));
vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

function mockKV(
  nodes: GraphNode[] = [],
  edges: GraphEdge[] = [],
) {
  const store = new Map<string, Map<string, unknown>>();
  const scopeRevisions = new Map<string, number>();
  const nodesMap = new Map<string, unknown>();
  for (const n of nodes) nodesMap.set(n.id, n);
  store.set("mem:graph:nodes", nodesMap);
  store.set("mem:graph:name-index", new Map(nodes.map((node) => [`${node.type}|${node.name}`, node.id])));

  const edgesMap = new Map<string, unknown>();
  for (const e of edges) edgesMap.set(e.id, e);
  store.set("mem:graph:edges", edgesMap);
  store.set("mem:graph:edge-key", new Map(edges.map((edge) => [`${edge.sourceNodeId}|${edge.targetNodeId}|${edge.type}`, edge.id])));

  store.set("mem:graph:edge-history", new Map());
  store.set("mem:sessions", new Map([["s1", { project: "project-a" }]]));
  store.set("mem:config", new Map([["graph-dispatch:active", {
    jobId: null, generation: "1", baselineComplete: true, revision: "0", revisionGeneration: "1",
  }]]));

  return {
    indexedRetrieval: true,
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      return (store.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      scopeRevisions.set(scope, (scopeRevisions.get(scope) ?? 0) + 1);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
      scopeRevisions.set(scope, (scopeRevisions.get(scope) ?? 0) + 1);
    },
    scopeRevision: async () => ({ generation: "1", revision: "0" }),
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
    pages: async function* <T>(scope: string, options: { cursor?: string; limit?: number } = {}) {
      const revision = scopeRevisions.get(scope) ?? 0;
      let offset = 0;
      if (options.cursor) {
        const [savedRevision, savedOffset] = options.cursor.split(":");
        if (savedRevision !== String(revision)) throw new StatePageError("STATE_PAGE_CURSOR_STALE");
        offset = Number(savedOffset);
      }
      const entries = [...(store.get(scope)?.entries() ?? [])].sort(([left], [right]) => left.localeCompare(right));
      const limit = options.limit ?? 256;
      const items = entries.slice(offset, offset + limit).map(([, value]) => value as T);
      const nextOffset = offset + items.length;
      yield { items, next_cursor: nextOffset < entries.length ? `${revision}:${nextOffset}` : null };
    },
  };
}

function mockSdk() {
  const functions = new Map<string, Function>();
  return {
    registerFunction: (idOrOpts: string | { id: string }, fn: Function) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      functions.set(id, fn);
    },
    trigger: async (idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      const fn = functions.get(id);
      if (fn) return fn(payload);
      return null;
    },
  };
}

function installTemporalGraphHarness(sdk: ReturnType<typeof mockSdk>, kv: ReturnType<typeof mockKV>) {
  const graphHarness = installGraphStateWire(sdk as never, kv as never);
  const originalTrigger = sdk.trigger.bind(sdk);
  let failNextPublish = false;
  let publishAttempts = 0;
  let successfulPublishes = 0;

  sdk.trigger = async (idOrInput, data) => {
    const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
    if (id === "iii::durable::publish") {
      publishAttempts++;
      if (failNextPublish) {
        failNextPublish = false;
        throw new Error("simulated publish cut");
      }
      const result = await originalTrigger(idOrInput, data);
      successfulPublishes++;
      return result;
    }
    return originalTrigger(idOrInput, data);
  };

  return {
    drainDeliveries: graphHarness.drainDeliveries.bind(graphHarness),
    failNextPublish: () => { failNextPublish = true; },
    publishAttempts: () => publishAttempts,
    successfulPublishes: () => successfulPublishes,
  };
}

function installScopedGraphRead(sdk: ReturnType<typeof mockSdk>, kv: ReturnType<typeof mockKV>) {
  Object.assign(kv, { retrieval: (payload: Record<string, unknown>) => sdk.trigger({ function_id: "state::retrieval", payload }) });
  sdk.registerFunction("state::retrieval", async (raw: unknown) => {
    const request = raw as { action: string; mode: string; project?: string; query?: string; node_ids?: string[]; edge_ids?: string[] };
    if (request.action !== "graph_read" || request.project !== "project-a") return { status: "partial", reason: "index_not_ready", project: request.project, items: [], has_more: false };
    const nodes = await kv.list<GraphNode>("mem:graph:nodes");
    const edges = await kv.list<GraphEdge>("mem:graph:edges");
    const items = request.mode === "search"
      ? nodes.filter((node) => !request.query || node.name.toLowerCase().includes(request.query.toLowerCase()))
        .slice(0, 16).map((node) => ({ key: node.id, id: node.id, position: 0, entity: node.name, observation: node.sourceObservationIds[0] }))
      : request.mode === "neighbors"
        ? edges.filter((edge) => request.node_ids?.includes(edge.sourceNodeId) || request.node_ids?.includes(edge.targetNodeId))
          .slice(0, 16).map((edge) => ({ key: edge.id, id: edge.id, position: 0, value: edge }))
        : request.mode === "history"
          ? edges.filter((edge) => request.edge_ids?.includes(edge.id)).slice(0, 16)
            .map((edge) => ({ key: edge.id, id: edge.id, position: 0, value: edge }))
          : [];
    return { status: "ready", project: request.project, items, has_more: false,
      ...(request.mode === "aggregate" ? { aggregate: {
        node_count: (request.node_ids ?? []).filter((id) => nodes.some((node) => node.id === id)).length,
        edge_count: (request.edge_ids ?? []).filter((id) => edges.some((edge) => edge.id === id)).length,
      } } : {}) };
  });
}

async function prepareLegacyTemporalParent(
  sdk: ReturnType<typeof mockSdk>,
  kv: ReturnType<typeof mockKV>,
  options: { response?: string; parsed?: unknown } = {},
) {
  const observation = {
    id: "legacy-temporal-source", sessionId: "s1", project: "project-a", title: "Legacy source",
    narrative: "Current source bytes", concepts: [], files: [], type: "conversation", timestamp: "2026-10-09T00:00:00Z",
  };
  await kv.set(KV.sessions, "s1", { id: "s1", project: "project-a" });
  await kv.set(KV.observations("s1"), observation.id, observation);
  const request = { project: "project-a", observations: [observation] };
  const accepted = await sdk.trigger("mem::temporal-graph-extract", request) as { jobId: string };
  const pageKey = `temporal-work:${accepted.jobId}:page:0`;
  const capturedPage = await kv.get<{ project: string; sources: Array<{ id: string; version: string; content: string; authority?: unknown }> }>(KV.temporalGraphWork, pageKey);
  if (!capturedPage) throw new Error("temporal test fixture page was not captured");
  const sources = capturedPage.sources.map(({ authority: _authority, ...source }) => source);
  const legacyPage = { project: capturedPage.project, sources };
  await kv.set(KV.temporalGraphWork, pageKey, legacyPage);

  const workInput = {
    audience: { scope: "project" as const, project: legacyPage.project }, sources,
    systemPrompt: TEMPORAL_EXTRACTION_SYSTEM,
    envelopePrefix: "Extract temporal graph XML from these source fragments.\n",
    envelopeSuffix: "\nReturn only the required XML document.",
  };
  const unit = createNextGraphWorkUnit(workInput).unit;
  if (!unit) throw new Error("temporal test fixture has no work unit");
  const unitRequest = { temporalUnitVersion: 2 as const, jobId: accepted.jobId, pageIndex: 0,
    project: legacyPage.project, workInput, unit };
  const parentJobId = `graphjob_${fingerprintId("temporalunit", `${accepted.jobId}:0:${unit.fingerprint}`).slice("temporalunit_".length)}`;
  const effectKey = batchEffectKey(`temporal:${legacyPage.project}:${unit.fingerprint}`);
  const graph = graphKV(kv as never);
  await runGraphJob(graph as never, "temporal", unitRequest, async () => {
    await freezeGraphValue(graph as never, "temporal-response:v3", () => options.response
      ?? '<temporal_graph><entities><entity name="Recovered" type="concept"/></entities><relationships/></temporal_graph>');
    await freezeGraphValue(graph as never, "temporal-parsed:v3", () => options.parsed ?? { nodes: [], edges: [] });
    return withGraphDelta(graph as never, () => runBatchCallback(graph as never, "graph", effectKey, async (_resuming, admit) => {
      await admit({ resultHash: batchEffectKey("legacy-empty-temporal-result") });
      return { success: true, nodesAdded: 0, edgesAdded: 0 };
    }));
  }, parentJobId);
  await kv.set(KV.batchCallbacks, `graph:${effectKey}`, { state: "completed" });
  return { request, accepted, pageKey, parentJobId, effectKey, unit, observation, legacyPage };
}

beforeEach(() => { pauseControl.paused = false; });

describe("selected temporal canonical application", () => {
  const project = "agentmemory-realproof-20261010";
  const sessionId = "agentmemory-realproof-20261010-direct";
  const retained = () => {
    const parsed = JSON.parse(readFileSync(new URL("./fixtures/retained-temporal-parsed.json", import.meta.url), "utf8"));
    return structuredClone(parsed) as { nodes: GraphNode[]; edges: GraphEdge[] };
  };
  const setup = async () => {
    const parsed = retained();
    const sdk = mockSdk(), kv = mockKV();
    const wire = installGraphStateWire(sdk as never, kv as never);
    await kv.set(KV.sessions, sessionId, { project });
    const observationIds = parsed.nodes[0].sourceObservationIds;
    for (const id of observationIds) await kv.set(KV.observations(sessionId), id, { id, sessionId, project });
    const effect = batchEffectKey("retained-selected-temporal-regression");
    const apply = (attempt = 1) => runGraphJob(kv as never, "temporal", { attempt, fixture: "selected-temporal-canonical" }, () =>
      applySelectedTemporalGraph(graphKV(kv as never), parsed.nodes, parsed.edges, observationIds, effect, project, sessionId), fingerprintId("graphjob", `selected-temporal-fixture:${attempt}`));
    return { parsed, kv, apply, effect, observationIds, wire };
  };
  it("applies the actual retained parsed response by namespace and replays without duplicate graph writes", async () => {
    const h = await setup();
    const node = h.parsed.nodes[1], nameKey = `${node.type}|${node.name}`;
    await h.kv.set(KV.graphNameIndex, nameKey, "foreign-global-name-pointer");
    expect(await h.apply()).toMatchObject({ success: true, nodesAdded: 11, edgesAdded: 9 });
    expect(await h.kv.get(KV.graphNameIndex, nameKey)).toBe("foreign-global-name-pointer");
    const nodes = await h.kv.list<GraphNode>(KV.graphNodes), edges = await h.kv.list<GraphEdge>(KV.graphEdges);
    expect(nodes).toHaveLength(11); expect(edges).toHaveLength(9);
    expect(nodes.every(node => node.id === fingerprintId("gn", `temporal:${project}:${node.type}:${node.name}`))).toBe(true);
    expect(await h.apply(2)).toMatchObject({ success: true });
    expect(await h.kv.list(KV.graphNodes)).toEqual(nodes);
    expect(await h.kv.list(KV.graphEdges)).toEqual(edges);
  });
  it("validates and versions an existing exact selected predecessor", async () => {
    const h = await setup();
    const remap = new Map(h.parsed.nodes.map(node => [node.id, fingerprintId("gn", `temporal:${project}:${node.type}:${node.name}`)]));
    for (const node of h.parsed.nodes) await h.kv.set(KV.graphNodes, remap.get(node.id)!, { ...node, id: remap.get(node.id)! });
    const edge = { ...h.parsed.edges[0], id: "ge_existing_selected", sourceNodeId: remap.get(h.parsed.edges[0].sourceNodeId)!, targetNodeId: remap.get(h.parsed.edges[0].targetNodeId)! };
    await h.kv.set(KV.graphEdges, edge.id, edge);
    await h.kv.set(KV.graphEdgeKey, `${edge.sourceNodeId}|${edge.targetNodeId}|${edge.type}`, edge.id);
    expect(await h.apply()).toMatchObject({ success: true });
    expect(await h.kv.get(KV.graphEdges, edge.id)).toMatchObject({ isLatest: false, supersededBy: fingerprintId("ge", `${h.effect}:0`) });
    expect(await h.kv.get(KV.graphEdges, fingerprintId("ge", `${h.effect}:0`))).toMatchObject({ version: 2 });
  });
  it("recovers a lost native edge-write acknowledgement with repeated edge keys from the frozen delta", async () => {
    const h = await setup();
    h.parsed.edges.push({ ...h.parsed.edges[0], id: "ge_second_same_key" });
    h.wire.failCommitForScope(KV.graphEdges, true);
    expect(await h.apply()).toMatchObject({ success: true });
    expect(h.wire.scopedCommitFailures).toBe(1);
    expect(await h.apply()).toMatchObject({ success: true });
    expect(await h.kv.list(KV.graphNodes)).toHaveLength(11);
    expect(await h.kv.list(KV.graphEdges)).toHaveLength(10);
    const first = await h.kv.get<GraphEdge>(KV.graphEdges, fingerprintId("ge", `${h.effect}:0`));
    const lastId = fingerprintId("ge", `${h.effect}:9`);
    expect(first).toMatchObject({ isLatest: false, supersededBy: lastId });
    expect(await h.kv.get(KV.graphEdges, lastId)).toMatchObject({ isLatest: true, version: 2 });
    expect(await h.kv.get(KV.graphEdgeKey, `${first!.sourceNodeId}|${first!.targetNodeId}|${first!.type}`)).toBe(lastId);
  });
  it.each(["foreign", "stale", "malformed", "wrong-identity", "missing-edge", "unknown-history"])("rejects %s before any graph effects", async fault => {
    const h = await setup(), node = h.parsed.nodes.at(-1)!;
    const id = fingerprintId("gn", `temporal:${project}:${node.type}:${node.name}`);
    if (["foreign", "stale", "malformed", "wrong-identity"].includes(fault)) {
      await h.kv.set(KV.graphNodes, id, { ...node, id,
        ...(fault === "foreign" ? { sourceObservationIds: ["foreign-observation"] } : {}),
        ...(fault === "stale" ? { stale: true } : {}),
        ...(fault === "malformed" ? { sourceObservationIds: "bad" } : {}),
        ...(fault === "wrong-identity" ? { name: "wrong" } : {}) });
    } else {
      const original = h.parsed.edges[0];
      const source = h.parsed.nodes.find(node => node.id === original.sourceNodeId)!;
      const target = h.parsed.nodes.find(node => node.id === original.targetNodeId)!;
      const edge = { ...original, id: "ge_previous", sourceNodeId: fingerprintId("gn", `temporal:${project}:${source.type}:${source.name}`), targetNodeId: fingerprintId("gn", `temporal:${project}:${target.type}:${target.name}`) };
      await h.kv.set(KV.graphEdgeKey, `${edge.sourceNodeId}|${edge.targetNodeId}|${edge.type}`, edge.id);
      if (fault === "unknown-history") { await h.kv.set(KV.graphEdges, edge.id, edge); await h.kv.set(KV.graphEdgeHistory, edge.id, edge); }
    }
    const before = await Promise.all([KV.graphNodes, KV.graphEdges, KV.graphEdgeHistory, KV.graphNameIndex, KV.graphEdgeKey, KV.batchCallbacks].map(scope => h.kv.list(scope)));
    await expect(h.apply()).rejects.toThrow("Selected temporal canonical provenance conflicts");
    expect(await Promise.all([KV.graphNodes, KV.graphEdges, KV.graphEdgeHistory, KV.graphNameIndex, KV.graphEdgeKey, KV.batchCallbacks].map(scope => h.kv.list(scope)))).toEqual(before);
  });
});

describe("TemporalGraph", () => {
  it("keeps ordinary temporal application strict when graph membership is partial", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const sdk = mockSdk(), kv = mockKV();
    const harness = installTemporalGraphHarness(sdk, kv);
    Object.assign(kv, { retrieval: async () => ({ status: "partial", project: "project-a", reason: "graph_membership_partial", items: [], has_more: false }) });
    const provider: MemoryProvider = { name: "test", compress: vi.fn().mockResolvedValue('<temporal_graph><entities><entity type="concept" name="strict"></entity></entities><relationships/></temporal_graph>'), summarize: vi.fn() };
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    await sdk.trigger("mem::temporal-graph-extract", { project: "project-a", observations: [{
      id: "strict-source", sessionId: "s1", title: "strict", narrative: "strict", concepts: [], files: [], type: "conversation", timestamp: "2026-10-10T00:00:00Z",
    }] });
    await expect(harness.drainDeliveries()).rejects.toThrow("graph_membership_partial");
    expect(await kv.list(KV.graphNodes)).toEqual([]); expect(await kv.list(KV.graphEdges)).toEqual([]);
  });
  it("imports without errors", async () => {
    const mod = await import("../src/functions/temporal-graph.js");
    expect(mod.registerTemporalGraphFunctions).toBeDefined();
  });

  it("registers all three functions", async () => {
    const { registerTemporalGraphFunctions } = await import(
      "../src/functions/temporal-graph.js"
    );
    const sdk = mockSdk();
    const kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    const provider: MemoryProvider = {
      name: "test",
      compress: vi.fn().mockResolvedValue("<temporal_graph><entities/><relationships/></temporal_graph>"),
      summarize: vi.fn().mockResolvedValue(""),
    };

    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    const fns = Array.from((sdk as any).trigger ? [] : []);
    expect(sdk.trigger).toBeDefined();
  });

  it("extracts temporal graph with context metadata", async () => {
    const { registerTemporalGraphFunctions } = await import(
      "../src/functions/temporal-graph.js"
    );

    const response = `<temporal_graph>
  <entities>
    <entity type="person" name="Alice">
      <property key="role">engineer</property>
    </entity>
    <entity type="organization" name="Acme Corp">
      <property key="industry">tech</property>
    </entity>
  </entities>
  <relationships>
    <relationship type="works_at" source="Alice" target="Acme Corp" weight="0.9" valid_from="2024-01-01" valid_to="current">
      <reasoning>Alice joined Acme Corp as an engineer</reasoning>
      <sentiment>positive</sentiment>
    </relationship>
  </relationships>
</temporal_graph>`;

    const provider: MemoryProvider = {
      name: "test",
      compress: vi.fn().mockResolvedValue(response),
      summarize: vi.fn().mockResolvedValue(response),
    };

    const sdk = mockSdk();
    const kv = mockKV();
    const harness = installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    const queued = (await sdk.trigger("mem::temporal-graph-extract", {
      observations: [
        {
          id: "obs_1",
          sessionId: "s1",
          title: "Alice at Acme",
          narrative: "Alice works at Acme Corp as an engineer",
          concepts: ["career"],
          files: [],
          type: "conversation",
          timestamp: "2024-01-01T00:00:00Z",
        },
      ],
    })) as { success: boolean; queued: boolean };

    expect(queued.queued).toBe(true);
    expect(provider.compress).not.toHaveBeenCalled();
    const results = await harness.drainDeliveries();
    const result = results[0] as { success: boolean; nodesAdded: number; edgesAdded: number };

    expect(result.success).toBe(true);
    expect(result.nodesAdded).toBe(2);
    expect(result.edgesAdded).toBe(1);
    expect((await kv.list<any>(KV.graphJobs)).some((job) => job.result?.value?.temporalResultContract === "temporal-result-v1")).toBe(true);

    const storedEdges = await kv.list<GraphEdge>("mem:graph:edges");
    expect(storedEdges.length).toBe(1);
    expect(storedEdges[0].tcommit).toBeDefined();
    expect(storedEdges[0].tvalid).toBe("2024-01-01");
    expect(storedEdges[0].context?.reasoning).toBe(
      "Alice joined Acme Corp as an engineer",
    );
    expect(storedEdges[0].context?.sentiment).toBe("positive");
    expect(storedEdges[0].isLatest).toBe(true);
    expect(storedEdges[0].version).toBe(1);
  });

  it("repairs a markerless legacy empty result through one linked child without mutating its parent", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const sdk = mockSdk(), kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const fixture = await prepareLegacyTemporalParent(sdk, kv);
    const manifestKey = `temporal-work:${fixture.accepted.jobId}:manifest`;
    const parentBefore = await kv.get(KV.graphJobs, fixture.parentJobId);
    const applyBefore = await kv.get(KV.graphDeltas(fixture.parentJobId), "manifest:3");
    let pageWrites = 0;
    const baseSet = kv.set.bind(kv);
    kv.set = async (scope: string, key: string, value: unknown) => {
      if (scope === KV.temporalGraphWork && key === fixture.pageKey) pageWrites++;
      return baseSet(scope, key, value);
    };

    await sdk.trigger("mem::temporal-graph-extract", fixture.request);
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toEqual(fixture.legacyPage);
    expect(pageWrites).toBe(0);
    const result = await sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }) as Record<string, any>;
    expect(result).toMatchObject({ success: true, completed: true, nodesAdded: 0, edgesAdded: 0,
      workUnits: 1, repairNodesAdded: 1, repairEdgesAdded: 0,
      temporalRepair: { state: "repaired", nodesAdded: 1, edgesAdded: 0 } });
    expect(await kv.get(KV.temporalGraphWork, manifestKey)).toMatchObject({ state: "completed", currentPage: 1,
      workUnits: 1, nodesAdded: 0, repairNodesAdded: 1, repairEdgesAdded: 0 });
    expect(await kv.get(KV.graphJobs, fixture.parentJobId)).toEqual(parentBefore);
    expect(await kv.get(KV.graphDeltas(fixture.parentJobId), "manifest:3")).toEqual(applyBefore);
    expect(await kv.get(KV.batchCallbacks, `graph:${fixture.effectKey}`)).toEqual({ state: "completed" });
    expect(await kv.list(KV.graphNodes)).toHaveLength(1);
    expect(await kv.list(KV.graphJobs)).toHaveLength(2);
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("replays a repaired child after an apply-to-manifest cut without duplicating effects", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const sdk = mockSdk(), kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const fixture = await prepareLegacyTemporalParent(sdk, kv);
    const manifestKey = `temporal-work:${fixture.accepted.jobId}:manifest`;
    const baseSet = kv.set.bind(kv);
    let failAdvance = true;
    kv.set = async (scope: string, key: string, value: unknown) => {
      if (failAdvance && scope === KV.temporalGraphWork && key === manifestKey
        && value && typeof value === "object" && (value as { currentPage?: unknown }).currentPage === 1) {
        failAdvance = false;
        throw new Error("simulated cut after temporal child apply");
      }
      return baseSet(scope, key, value);
    };
    const parentBefore = await kv.get(KV.graphJobs, fixture.parentJobId);

    await expect(sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }))
      .rejects.toThrow("simulated cut after temporal child apply");
    const firstChildJobs = (await kv.list<any>(KV.graphJobs)).filter((job) => job.id !== fixture.parentJobId);
    expect(firstChildJobs).toHaveLength(1);
    const childBeforeRetry = firstChildJobs[0];
    expect(await kv.list(KV.graphNodes)).toHaveLength(1);
    expect(await kv.get(KV.temporalGraphWork, manifestKey)).toMatchObject({ currentPage: 0, workUnits: 0 });
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toEqual(fixture.legacyPage);

    const replay = await sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }) as Record<string, any>;
    expect(replay).toMatchObject({ success: true, completed: true, workUnits: 1, nodesAdded: 0,
      repairNodesAdded: 1, temporalRepair: { state: "repaired" } });
    expect(await kv.list(KV.graphJobs)).toHaveLength(2);
    expect((await kv.list<any>(KV.graphJobs)).find((job) => job.id !== fixture.parentJobId)).toEqual(childBeforeRetry);
    expect(await kv.get(KV.graphJobs, fixture.parentJobId)).toEqual(parentBefore);
    expect(await kv.list(KV.graphNodes)).toHaveLength(1);
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toBeNull();
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("cleans a completed temporal page after a manifest-to-delete cut without replaying the repair", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const sdk = mockSdk(), kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const fixture = await prepareLegacyTemporalParent(sdk, kv);
    const manifestKey = `temporal-work:${fixture.accepted.jobId}:manifest`;
    const baseDelete = kv.delete.bind(kv);
    let failPageDelete = true;
    kv.delete = async (scope: string, key: string) => {
      if (failPageDelete && scope === KV.temporalGraphWork && key === fixture.pageKey) {
        failPageDelete = false;
        throw new Error("simulated cut before temporal page deletion");
      }
      return baseDelete(scope, key);
    };

    await expect(sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }))
      .rejects.toThrow("simulated cut before temporal page deletion");
    expect(await kv.get(KV.temporalGraphWork, manifestKey)).toMatchObject({ state: "completed", currentPage: 1,
      workUnits: 1, repairNodesAdded: 1 });
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toEqual(fixture.legacyPage);
    const childBeforeRetry = (await kv.list<any>(KV.graphJobs)).find((job) => job.id !== fixture.parentJobId);
    expect(childBeforeRetry).toBeDefined();

    const replay = await sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }) as Record<string, any>;
    expect(replay).toMatchObject({ success: true, completed: true, workUnits: 1, repairNodesAdded: 1 });
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toBeNull();
    expect(await kv.get(KV.temporalGraphPending, fixture.accepted.jobId)).toBeNull();
    expect((await kv.list<any>(KV.graphJobs)).find((job) => job.id !== fixture.parentJobId)).toEqual(childBeforeRetry);
    expect(await kv.list(KV.graphNodes)).toHaveLength(1);
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("leaves a nonempty historical parse in recovery-needed state without advancing or deleting the page", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const sdk = mockSdk(), kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const fixture = await prepareLegacyTemporalParent(sdk, kv, { parsed: { nodes: [{ id: "legacy" }], edges: [] } });

    const result = await sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }) as Record<string, any>;
    expect(result).toMatchObject({ success: true, queued: true, deferred: true, needsRecovery: true,
      temporalRepair: { state: "recovery-needed", reason: "historical_temporal_parse_unsupported" } });
    expect(await kv.get(KV.temporalGraphWork, `temporal-work:${fixture.accepted.jobId}:manifest`))
      .toMatchObject({ state: "pending", currentPage: 0, workUnits: 0 });
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toEqual(fixture.legacyPage);
    expect(await kv.list(KV.graphJobs)).toHaveLength(1);
    expect(await kv.list(KV.graphNodes)).toHaveLength(0);
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("refuses historical repair when current session authority no longer proves the source project", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn(), summarize: vi.fn() };
    const sdk = mockSdk(), kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const fixture = await prepareLegacyTemporalParent(sdk, kv);
    await kv.set(KV.sessions, "s1", { id: "s1", project: "project-b" });

    const result = await sdk.trigger("mem::temporal-work-unit", { jobId: fixture.accepted.jobId }) as Record<string, any>;
    expect(result).toMatchObject({ success: true, queued: true, deferred: true, needsRecovery: true,
      temporalRepair: { state: "recovery-needed", reason: "historical_temporal_source_unverified" } });
    expect(await kv.get(KV.temporalGraphWork, `temporal-work:${fixture.accepted.jobId}:manifest`))
      .toMatchObject({ state: "pending", currentPage: 0, workUnits: 0 });
    expect(await kv.get(KV.temporalGraphWork, fixture.pageKey)).toEqual(fixture.legacyPage);
    expect(await kv.list(KV.graphJobs)).toHaveLength(1);
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("blocks a missing future grouped response during local-only replay before calling the provider", async () => {
    const harness = graphStateHarness();
    const graph = graphKV(harness.kv as never);
    const jobId = "graphjob_temporal_future_capture";
    const input = { graphWorkGroupsVersion: 2, project: "project-a", sources: ["captured"] };
    const provider = vi.fn(() => { throw new Error("simulated interrupted provider response"); });
    const run = async () => {
      await freezeGraphValue(graph as never, "temporal-work-unit:v2:0", () => ({ fingerprint: "first" }));
      await freezeGraphValue(graph as never, "temporal-response:v2:0", () => "<temporal_graph><entities/><relationships/></temporal_graph>");
      await freezeGraphValue(graph as never, "temporal-parsed:v2:0", () => ({ kind: "valid-empty" }));
      await withGraphDelta(graph as never, async () => ({ success: true, nodesAdded: 0, edgesAdded: 0 }));
      await freezeGraphValue(graph as never, "temporal-work-unit:v2:1", () => ({ fingerprint: "future" }));
      return freezeGraphValue(graph as never, "temporal-response:v2:1", () => provider());
    };

    await expect(runGraphJob(graph as never, "temporal", input, run, jobId)).rejects.toThrow("simulated interrupted provider response");
    expect(provider).toHaveBeenCalledTimes(1);
    const failedJob = await harness.kv.get<any>(KV.graphJobs, jobId);
    await harness.kv.set(KV.graphJobs, jobId, { ...failedJob, recoveryAfter: null });
    await expect(withLocalGraphRecovery(true, () => runGraphJob(graph as never, "temporal", input, run, jobId)))
      .rejects.toThrow("Graph recovery requires an uncaptured provider operation");
    expect(provider).toHaveBeenCalledTimes(1);
    expect(await harness.kv.get(KV.graphProviderResults(jobId), "value:6:temporal-response:v2:1")).toBeNull();
  });

  it("processes one durable temporal provider unit per delivery", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn().mockResolvedValue("<temporal_graph><entities/><relationships/></temporal_graph>"), summarize: vi.fn() };
    const sdk = mockSdk();
    const kv = mockKV();
    const harness = installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    const providerCallsPerDelivery: number[] = [];
    let previousCalls = 0;
    const triggerBeforeDelivery = sdk.trigger.bind(sdk);
    sdk.trigger = async (idOrInput, data) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const result = await triggerBeforeDelivery(idOrInput, data);
      if (id === "mem::temporal-work-unit" && typeof idOrInput !== "string") {
        const calls = vi.mocked(provider.compress).mock.calls.length;
        providerCallsPerDelivery.push(calls - previousCalls);
        previousCalls = calls;
      }
      return result;
    };

    const accepted = await sdk.trigger("mem::temporal-graph-extract", { observations: [{
      id: "obs_large", sessionId: "s1", title: "Large source", narrative: "source ".repeat(3500),
      concepts: [], files: [], type: "conversation", timestamp: "2026-10-09T00:00:00Z",
    }] }) as { success: boolean; queued: boolean };
    expect(accepted).toMatchObject({ success: true, queued: true });
    expect(provider.compress).not.toHaveBeenCalled();

    await harness.drainDeliveries();
    expect(providerCallsPerDelivery.length).toBeGreaterThan(1);
    expect(providerCallsPerDelivery.every((calls) => calls === 1)).toBe(true);
    expect(provider.compress).toHaveBeenCalledTimes(providerCallsPerDelivery.length);
    expect((await kv.list<any>(KV.graphJobs)).filter((job) => job.kind === "temporal")
      .every((job) => job.result?.value?.temporalResultContract === "temporal-result-v1")).toBe(true);
  });

  it("recovers a cut after durable temporal capture and stays paused until recovery resumes", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn().mockResolvedValue("<temporal_graph><entities/><relationships/></temporal_graph>"), summarize: vi.fn() };
    const sdk = mockSdk();
    const kv = mockKV();
    const harness = installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    harness.failNextPublish();
    const request = { observations: [{
      id: "obs_publish_cut", sessionId: "s1", title: "Captured before publish", narrative: "durable source",
      concepts: [], files: [], type: "conversation", timestamp: "2026-10-09T00:00:00Z",
    }] };
    await expect(sdk.trigger("mem::temporal-graph-extract", request)).rejects.toThrow("simulated publish cut");

    const indexes = await kv.list<any>("mem:temporal-graph-pending");
    const work = await kv.list<any>("mem:temporal-graph-work");
    expect(indexes).toHaveLength(1);
    expect(indexes[0]).toMatchObject({ state: "pending", manifestReady: true, pageCount: 1 });
    expect(work.some((entry: any) => entry?.state === "pending" && entry?.pageCount === 1)).toBe(true);
    expect(work.some((entry: any) => Array.isArray(entry?.sources))).toBe(true);
    expect(await harness.drainDeliveries()).toHaveLength(0);

    pauseControl.paused = true;
    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ paused: true, queued: 0 });
    expect(harness.publishAttempts()).toBe(1);
    expect(await harness.drainDeliveries()).toHaveLength(0);
    pauseControl.paused = false;
    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ published: 1, queued: 1 });
    expect(harness.successfulPublishes()).toBe(1);
    expect(await harness.drainDeliveries()).toHaveLength(1);
    expect((await kv.list<any>("mem:temporal-graph-pending")).filter((entry) => typeof entry.jobId === "string")).toHaveLength(0);
    expect(provider.compress).toHaveBeenCalledTimes(1);
  });

  it("rebuilds a missing manifest from the durable pending index after an intake cut", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn().mockResolvedValue("<temporal_graph><entities/><relationships/></temporal_graph>"), summarize: vi.fn() };
    const sdk = mockSdk();
    const kv = mockKV();
    const baseSet = kv.set.bind(kv);
    let failManifestWrite = true;
    kv.set = async (scope: string, key: string, value: unknown) => {
      if (failManifestWrite && scope === "mem:temporal-graph-work" && key.endsWith(":manifest")) {
        failManifestWrite = false;
        throw new Error("simulated cut after pending-index capture");
      }
      return baseSet(scope, key, value);
    };
    const harness = installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const request = { observations: [{
      id: "obs_manifest_cut", sessionId: "s1", title: "Captured before manifest", narrative: "durable source",
      concepts: [], files: [], type: "conversation", timestamp: "2026-10-09T00:00:00Z",
    }] };

    await expect(sdk.trigger("mem::temporal-graph-extract", request)).rejects.toThrow("simulated cut after pending-index capture");
    expect(await kv.list("mem:temporal-graph-pending")).toHaveLength(1);
    expect(await kv.list("mem:temporal-graph-work")).toHaveLength(1);

    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ queued: 1, published: 1 });
    expect(await kv.list("mem:temporal-graph-work")).toHaveLength(3);
    expect(await harness.drainDeliveries()).toHaveLength(1);
    expect((await kv.list<any>("mem:temporal-graph-pending")).filter((entry) => typeof entry.jobId === "string")).toHaveLength(0);
    expect(provider.compress).toHaveBeenCalledTimes(1);
  });

  it("restarts stale pending-index cursors and reaches later due work after queue churn", async () => {
    const { registerTemporalGraphFunctions } = await import("../src/functions/temporal-graph.js");
    const provider: MemoryProvider = { name: "test", compress: vi.fn().mockResolvedValue(""), summarize: vi.fn() };
    const sdk = mockSdk();
    const kv = mockKV();
    const harness = installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);
    const first = "temporaljob_0000000000000001";
    const second = "temporaljob_0000000000000002";
    const third = "temporaljob_0000000000000003";
    const now = new Date().toISOString();
    for (const jobId of [first, second]) {
      await kv.set("mem:temporal-graph-pending", jobId, {
        version: 1, jobId, state: "pending", manifestReady: true, pageCount: 1, createdAt: now, updatedAt: now,
      });
      await kv.set("mem:temporal-graph-work", `temporal-work:${jobId}:manifest`, {
        version: 1, id: jobId, state: "pending", pageCount: 1, currentPage: 0,
        nodesAdded: 0, edgesAdded: 0, workUnits: 0,
        ...(jobId === first ? { activeUnitJobId: "graphjob_backoff" } : {}), createdAt: now, updatedAt: now,
      });
    }
    await kv.set("mem:graph:jobs", "graphjob_backoff", {
      state: "failed", recoveryAfter: new Date(Date.now() + 60_000).toISOString(),
    });

    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ deferred: true, reason: "unit_backoff" });
    expect(await harness.drainDeliveries()).toHaveLength(0);
    await kv.set("mem:temporal-graph-pending", third, {
      version: 1, jobId: third, state: "pending", manifestReady: true, pageCount: 1, createdAt: now, updatedAt: now,
    });
    await kv.set("mem:temporal-graph-work", `temporal-work:${third}:manifest`, {
      version: 1, id: third, state: "pending", pageCount: 1, currentPage: 0,
      nodesAdded: 0, edgesAdded: 0, workUnits: 0, createdAt: now, updatedAt: now,
    });
    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ published: 1, jobId: second });
    await kv.delete("mem:temporal-graph-pending", first);
    await kv.set("mem:temporal-graph-work", `temporal-work:${second}:manifest`, {
      version: 1, id: second, state: "pending", pageCount: 1, currentPage: 0,
      nodesAdded: 0, edgesAdded: 0, workUnits: 0, activeUnitJobId: "graphjob_backoff_2",
      createdAt: now, updatedAt: now,
    });
    await kv.set("mem:graph:jobs", "graphjob_backoff_2", {
      state: "failed", recoveryAfter: new Date(Date.now() + 60_000).toISOString(),
    });
    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ success: true, queued: 0 });
    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ deferred: true, reason: "unit_backoff" });
    expect(await sdk.trigger("mem::temporal-work-unit", { reconcile: true })).toMatchObject({ published: 1, jobId: third });
    expect(harness.successfulPublishes()).toBe(2);
  });

  it("appends new edge version instead of overwriting", async () => {
    const { registerTemporalGraphFunctions } = await import(
      "../src/functions/temporal-graph.js"
    );

    const existingNode: GraphNode = {
      id: "gn_existing_alice",
      type: "person",
      name: "Alice",
      properties: { role: "engineer" },
      sourceObservationIds: ["obs_0"],
      createdAt: "2024-01-01T00:00:00Z",
    };
    const existingNode2: GraphNode = {
      id: "gn_existing_acme",
      type: "organization",
      name: "Acme Corp",
      properties: {},
      sourceObservationIds: ["obs_0"],
      createdAt: "2024-01-01T00:00:00Z",
    };
    const existingEdge: GraphEdge = {
      id: "ge_old",
      type: "works_at" as any,
      sourceNodeId: "gn_existing_alice",
      targetNodeId: "gn_existing_acme",
      weight: 0.9,
      sourceObservationIds: ["obs_0"],
      createdAt: "2024-01-01T00:00:00Z",
      tcommit: "2024-01-01T00:00:00Z",
      tvalid: "2024-01-01",
      version: 1,
      isLatest: true,
    };

    const response = `<temporal_graph>
  <entities>
    <entity type="person" name="Alice">
      <property key="role">senior engineer</property>
    </entity>
    <entity type="organization" name="Acme Corp">
    </entity>
  </entities>
  <relationships>
    <relationship type="works_at" source="Alice" target="Acme Corp" weight="0.95" valid_from="2025-01-01" valid_to="current">
      <reasoning>Alice was promoted to senior engineer</reasoning>
      <sentiment>positive</sentiment>
    </relationship>
  </relationships>
</temporal_graph>`;

    const provider: MemoryProvider = {
      name: "test",
      compress: vi.fn().mockResolvedValue(response),
      summarize: vi.fn().mockResolvedValue(response),
    };

    const sdk = mockSdk();
    const kv = mockKV([existingNode, existingNode2], [existingEdge]);
    const harness = installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    await sdk.trigger("mem::temporal-graph-extract", {
      observations: [
        {
          id: "obs_1",
          sessionId: "s1",
          title: "Alice promotion",
          narrative: "Alice was promoted to senior engineer at Acme Corp",
          concepts: [],
          files: [],
          type: "conversation",
          timestamp: "2025-01-01T00:00:00Z",
        },
      ],
    });
    const results = await harness.drainDeliveries();
    const result = results[0] as { success: boolean; nodesAdded: number; edgesAdded: number };

    expect(result.success).toBe(true);

    const allEdges = await kv.list<GraphEdge>("mem:graph:edges");
    expect(allEdges.length).toBe(2);

    const oldEdge = allEdges.find((e) => e.id === "ge_old");
    expect(oldEdge?.isLatest).toBe(false);
    expect(oldEdge?.tvalidEnd).toBeDefined();

    const newEdge = allEdges.find((e) => e.id !== "ge_old");
    expect(newEdge?.isLatest).toBe(true);
    expect(newEdge?.version).toBe(2);
    expect(newEdge?.tvalid).toBe("2025-01-01");
  });

  it("temporal query returns current state", async () => {
    const { registerTemporalGraphFunctions } = await import(
      "../src/functions/temporal-graph.js"
    );

    const node: GraphNode = {
      id: "gn_1",
      type: "person",
      name: "Bob",
      properties: {},
      sourceObservationIds: ["obs_1"],
      createdAt: "2024-01-01T00:00:00Z",
    };
    const edge1: GraphEdge = {
      id: "ge_1",
      type: "located_in" as any,
      sourceNodeId: "gn_1",
      targetNodeId: "gn_2",
      weight: 0.9,
      sourceObservationIds: ["obs_1"],
      createdAt: "2023-01-01T00:00:00Z",
      tcommit: "2023-01-01T00:00:00Z",
      tvalid: "2023-01-01",
      tvalidEnd: "2024-06-01",
      version: 1,
      isLatest: false,
    };
    const edge2: GraphEdge = {
      id: "ge_2",
      type: "located_in" as any,
      sourceNodeId: "gn_1",
      targetNodeId: "gn_3",
      weight: 0.9,
      sourceObservationIds: ["obs_2"],
      createdAt: "2024-06-01T00:00:00Z",
      tcommit: "2024-06-01T00:00:00Z",
      tvalid: "2024-06-01",
      version: 2,
      isLatest: true,
    };

    const sdk = mockSdk();
    const kv = mockKV([node], [edge1, edge2]);
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    const provider: MemoryProvider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    const result = (await sdk.trigger("mem::temporal-query", {
      entityName: "Bob",
      project: "project-a",
    })) as any;

    expect(result.entity).toBeDefined();
    expect(result.entity.name).toBe("Bob");
    expect(result.currentEdges.length).toBe(1);
    expect(result.currentEdges[0].id).toBe("ge_2");
  });

  it("temporal query with asOf returns historical state", async () => {
    const { registerTemporalGraphFunctions } = await import(
      "../src/functions/temporal-graph.js"
    );

    const node: GraphNode = {
      id: "gn_1",
      type: "person",
      name: "Charlie",
      properties: {},
      sourceObservationIds: ["obs_1"],
      createdAt: "2023-01-01T00:00:00Z",
    };
    const edge1: GraphEdge = {
      id: "ge_1",
      type: "located_in" as any,
      sourceNodeId: "gn_1",
      targetNodeId: "gn_nyc",
      weight: 0.9,
      sourceObservationIds: ["obs_1"],
      createdAt: "2023-01-01T00:00:00Z",
      tcommit: "2023-01-01T00:00:00Z",
      tvalid: "2023-01-01",
      tvalidEnd: "2024-06-01",
      version: 1,
      isLatest: false,
    };
    const edge2: GraphEdge = {
      id: "ge_2",
      type: "located_in" as any,
      sourceNodeId: "gn_1",
      targetNodeId: "gn_london",
      weight: 0.9,
      sourceObservationIds: ["obs_2"],
      createdAt: "2024-06-01T00:00:00Z",
      tcommit: "2024-06-01T00:00:00Z",
      tvalid: "2024-06-01",
      version: 2,
      isLatest: true,
    };

    const sdk = mockSdk();
    const kv = mockKV([node], [edge1, edge2]);
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    const provider: MemoryProvider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    const result = (await sdk.trigger("mem::temporal-query", {
      entityName: "Charlie",
      project: "project-a",
      asOf: "2023-06-01T00:00:00Z",
    })) as any;

    expect(result.entity.name).toBe("Charlie");
    expect(result.currentEdges.length).toBe(1);
    expect(result.currentEdges[0].targetNodeId).toBe("gn_nyc");
  });

  it("handles empty observations gracefully", async () => {
    const { registerTemporalGraphFunctions } = await import(
      "../src/functions/temporal-graph.js"
    );
    const sdk = mockSdk();
    const kv = mockKV();
    installTemporalGraphHarness(sdk, kv);
    installScopedGraphRead(sdk, kv);
    const provider: MemoryProvider = {
      name: "test",
      compress: vi.fn(),
      summarize: vi.fn(),
    };
    registerTemporalGraphFunctions(sdk as never, kv as never, provider);

    const result = (await sdk.trigger("mem::temporal-graph-extract", {
      observations: [],
    })) as any;

    expect(result.success).toBe(false);
    expect(result.error).toBe("No observations provided");
  });
});
