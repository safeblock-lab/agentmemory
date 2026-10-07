import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { sha256File } from "../.native-pagination-build/graph-resource/resource-runner.js";
import { KV } from "../.native-pagination-build/graph-resource/oracle/src/state/schema.js";
import { persistGraphDelta as persistOriginalGraphDelta } from "../.native-pagination-build/graph-resource/oracle/src/functions/graph.js";
import type { GraphEdge, GraphNode } from "../.native-pagination-build/graph-resource/oracle/src/types.js";
import { runFrozenOriginalGraphExtraction } from "../.native-pagination-build/graph-resource/original-oracle.js";
import { GRAPH_EXTRACTION_SYSTEM } from "../.native-pagination-build/graph-resource/oracle/src/prompts/graph-extraction.js";
import { CampaignUuidAllocator } from "../.native-pagination-build/graph-resource/campaign-uuid.js";
import { ORIGINAL_ORACLE_RESPONSES } from "../.native-pagination-build/graph-resource/original-oracle.js";

describe("native campaign UUID isolation", () => {
  const graphStack = "Error\n at generateId (D:/agentmemory/src/state/schema.ts:126:23)\n at addEntity (D:/agentmemory/src/functions/graph.ts:776:11)\n at parseGraphXml (D:/agentmemory/src/functions/graph.ts:787:5)";
  const select = (allocator: CampaignUuidAllocator, group: number) => allocator.observeFrozenResponse(
    "state::get_versioned", { scope: "mem:graph:provider-results:fixture", key: `value:${group}:provider-response:part:0` },
    { value: JSON.stringify({ value: ORIGINAL_ORACLE_RESPONSES[group] }) });

  it("keeps original audit gaps while independent SDK calls consume no graph IDs", () => {
    const allocator = new CampaignUuidAllocator(() => "native-sdk-uuid");
    select(allocator, 0);
    expect(allocator.allocate("Error\n at Sdk.trigger (iii.ts:510:34)\n at generateId (unrelated.ts:1:1)")).toBe("native-sdk-uuid");
    const actual = [allocator.allocate(graphStack), allocator.allocate(graphStack), allocator.allocate(graphStack)];
    select(allocator, 1);
    actual.push(allocator.allocate(graphStack), allocator.allocate(graphStack));
    expect(actual.map((id) => Number.parseInt(id.slice(0, 8), 16))).toEqual([1, 2, 3, 5, 6]);
    expect(() => allocator.allocate(graphStack)).toThrow("exceeded");
  });

  it("uses durable original response groups across a new worker and skipped frozen parses", () => {
    const restarted = new CampaignUuidAllocator(() => "native-sdk-uuid");
    select(restarted, 1);
    expect(restarted.allocate(graphStack)).toBe("00000005-0000-4000-8000-000000000000");
    select(restarted, 1);
    expect(restarted.allocate(graphStack)).toBe("00000005-0000-4000-8000-000000000000");
    expect(() => new CampaignUuidAllocator(() => "native").allocate(graphStack)).toThrow("before");
  });

  it("keeps heuristic allocations separate from transport and rejects unknown graph callers", () => {
    const allocator = new CampaignUuidAllocator(() => "native-sdk-uuid");
    const heuristicStack = graphStack.replace("parseGraphXml", "buildHeuristicGraph");
    expect(allocator.allocate(heuristicStack)).toBe("00000001-0000-4000-8000-000000000000");
    expect(allocator.allocate("at Sdk.trigger")).toBe("native-sdk-uuid");
    expect(allocator.allocate(heuristicStack)).toBe("00000002-0000-4000-8000-000000000000");
    expect(() => allocator.allocate(graphStack.replace("parseGraphXml", "unknownCaller"))).toThrow("unclassified");
  });
});

const HERE = dirname(fileURLToPath(import.meta.url));
const RESOURCE_ROOT = resolve(HERE, "../.native-pagination-build/graph-resource");
const ACCEPTED_NATIVE = JSON.parse(
  readFileSync(resolve(HERE, "../.native-pagination-build/local-indexed-verification/native-accepted.json"), "utf8"),
) as { accepted: boolean; executable: string; sha256: string };
const ACCEPTED_NATIVE_MANIFEST = JSON.parse(
  readFileSync(resolve(HERE, "../.native-pagination-build/release-native-current/manifest-verification.json"), "utf8"),
) as { manifestPath: string; manifestSha256: string; artifactSha256: string };
const HISTORICAL_SOURCE_FREEZE = JSON.parse(
  readFileSync(resolve(HERE, "../.native-pagination-build/release-native-current/hash-freeze.json"), "utf8"),
) as { pins: Array<{ path: string; sha256: string }> };
const CURRENT_SOURCE_FREEZE = JSON.parse(
  readFileSync(resolve(HERE, "../artifacts/private/native-storage-v2/candidate/source-freeze.json"), "utf8"),
) as Array<{ path: string; sha256: string }>;
const CURRENT_NATIVE_MANIFEST = JSON.parse(
  readFileSync(resolve(HERE, "../patches/iii-engine/manifest.json"), "utf8"),
) as { artifactPath: string; artifactSha256: string; patchPath: string; patchSha256: string };
const pins = JSON.parse(readFileSync(resolve(RESOURCE_ROOT, "pins.json"), "utf8")) as {
  oracle: { gitRef: string; files: Array<{ path: string; sha256: string }> };
  candidate: { acceptedForRuntime: boolean; sourcePinsMustBeRefreshedAfterMainCorrection: boolean };
  native: { proofPath: string; proofSha256: string; resourceConfigTemplatePath: string; resourceConfigTemplateSha256: string };
};
const scenario = JSON.parse(readFileSync(resolve(RESOURCE_ROOT, "scenarios.json"), "utf8")) as {
  identity: { captureIds: string[]; batchEffectKey: string };
  promptGroups: Array<{ response: string }>;
};
const FIXED_TIME = new Date("2025-04-05T06:07:08.000Z");
const OBSERVATIONS = ["capture-a", "capture-b"];

function graphStore() {
  const scopes = new Map<string, Map<string, unknown>>();
  return {
    async get<T>(scope: string, key: string): Promise<T | null> {
      return structuredClone((scopes.get(scope)?.get(key) as T | undefined) ?? null);
    },
    async set<T>(scope: string, key: string, value: T): Promise<T> {
      let rows = scopes.get(scope);
      if (!rows) scopes.set(scope, (rows = new Map()));
      rows.set(key, structuredClone(value));
      return structuredClone(value);
    },
    async delete(scope: string, key: string): Promise<void> {
      scopes.get(scope)?.delete(key);
    },
    async list<T>(scope: string): Promise<T[]> {
      return structuredClone([...(scopes.get(scope)?.values() ?? [])]) as T[];
    },
    values<T>(scope: string): Array<[string, T]> {
      return [...(scopes.get(scope)?.entries() ?? [])] as Array<[string, T]>;
    },
  };
}

function nodes(): GraphNode[] {
  return [
    { id: "node-writer", type: "function", name: "persistGraphDelta", properties: { layer: "graph" }, sourceObservationIds: ["capture-a"], createdAt: FIXED_TIME.toISOString() },
    { id: "node-file", type: "file", name: "src/functions/graph.ts", properties: { language: "typescript" }, sourceObservationIds: ["capture-a"], createdAt: FIXED_TIME.toISOString() },
    { id: "node-audit", type: "function", name: "recordAudit", properties: { layer: "audit" }, sourceObservationIds: ["capture-b"], createdAt: FIXED_TIME.toISOString() },
  ];
}

function edges(): GraphEdge[] {
  return [
    { id: "edge-writer-file", type: "modifies", sourceNodeId: "node-writer", targetNodeId: "node-file", weight: 0.9, sourceObservationIds: ["capture-a"], createdAt: FIXED_TIME.toISOString() },
    { id: "edge-writer-audit", type: "depends_on", sourceNodeId: "node-writer", targetNodeId: "node-audit", weight: 0.8, sourceObservationIds: ["capture-b"], createdAt: FIXED_TIME.toISOString() },
  ];
}

function stateRows(state: Array<[string, Array<[string, unknown]>]>, scope: string): Array<[string, unknown]> {
  return state.find(([entryScope]) => entryScope === scope)?.[1] ?? [];
}

afterEach(() => vi.useRealTimers());

describe("frozen pre-transaction graph oracle", () => {
  it("captures actual old-source extraction prompt groups and deterministic graph state", async () => {
    const first = await runFrozenOriginalGraphExtraction();
    const replay = await runFrozenOriginalGraphExtraction();
    expect(first.providerCalls).toHaveLength(2);
    expect(first.providerCalls.map((call) => call.systemPrompt)).toEqual([GRAPH_EXTRACTION_SYSTEM, GRAPH_EXTRACTION_SYSTEM]);
    expect(first.providerCalls[0]?.userPrompt).toContain("Original extraction fixture A");
    expect(first.providerCalls[0]?.userPrompt).toContain(`A${"a".repeat(2_000)}`);
    expect(first.providerCalls[0]?.userPrompt).not.toContain("Original extraction fixture B");
    expect(first.providerCalls[1]?.userPrompt).toContain("Original extraction fixture B");
    expect(first.providerCalls[1]?.userPrompt).toContain(`B${"b".repeat(2_000)}`);
    expect(first.providerCalls.map((call) => call.response)).toEqual(scenario.promptGroups.map((group) => group.response));
    expect(first.extractionResult).toMatchObject({ success: true });
    expect(first.queryResult).toMatchObject({ totalNodes: 4, totalEdges: 1, fromSnapshot: true });
    const originalNodes = stateRows(first.state, KV.graphNodes).map(([, value]) => value as GraphNode);
    const originalEdges = stateRows(first.state, KV.graphEdges).map(([, value]) => value as GraphEdge);
    const originalSnapshot = stateRows(first.state, KV.graphSnapshot)[0]?.[1] as Record<string, unknown>;
    expect(new Set(originalNodes.map((node) => node.id)).size).toBe(4);
    expect(stateRows(first.state, KV.graphNameIndex)).toHaveLength(4);
    expect(stateRows(first.state, KV.graphEdgeKey)).toHaveLength(1);
    expect(stateRows(first.state, KV.graphNodeDegree).map(([, value]) => value).sort()).toEqual([0, 0, 1, 1]);
    expect(originalEdges[0]).toMatchObject({ weight: 0.9, sourceObservationIds: ["capture-a"] });
    expect((originalSnapshot.stats as { totalNodes: number; totalEdges: number })).toEqual({
      totalNodes: 4,
      totalEdges: 1,
      nodesByType: { function: 2, file: 1, concept: 1 },
      edgesByType: { modifies: 1 },
    });
    expect(originalSnapshot.updatedAt).toBe("2025-04-05T06:07:08.000Z");
    expect((first.queryResult as { nodes: unknown[] }).nodes).toHaveLength(4);
    expect(first.state).toEqual(replay.state);
    expect(first.providerCalls).toEqual(replay.providerCalls);
  });

  it("keeps the Git HEAD implementation and transitive source bytes immutable", async () => {
    expect(pins.oracle.gitRef).toMatch(/^HEAD:[0-9a-f]{40}$/);
    for (const file of pins.oracle.files) {
      const bytes = readFileSync(resolve(RESOURCE_ROOT, "oracle", file.path));
      expect(createHash("sha256").update(bytes).digest("hex"), file.path).toBe(file.sha256);
    }
    expect(pins.candidate).toMatchObject({ acceptedForRuntime: false, sourcePinsMustBeRefreshedAfterMainCorrection: true });
    expect(ACCEPTED_NATIVE.accepted).toBe(true);
    expect(ACCEPTED_NATIVE.sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(await sha256File(ACCEPTED_NATIVE.executable)).toBe(ACCEPTED_NATIVE.sha256);
    expect(ACCEPTED_NATIVE_MANIFEST.manifestPath).toBe("patches/iii-engine/manifest.json");
    expect(ACCEPTED_NATIVE_MANIFEST.artifactSha256).toBe(ACCEPTED_NATIVE.sha256);
    expect(HISTORICAL_SOURCE_FREEZE.pins.find((pin) => pin.path === ACCEPTED_NATIVE_MANIFEST.manifestPath)?.sha256)
      .toBe(ACCEPTED_NATIVE_MANIFEST.manifestSha256);
    for (const path of [ACCEPTED_NATIVE_MANIFEST.manifestPath, CURRENT_NATIVE_MANIFEST.patchPath]) {
      const pin = CURRENT_SOURCE_FREEZE.find((entry) => entry.path === path);
      expect(pin, path).toBeDefined();
      expect(await sha256File(resolve(HERE, "..", path)), path).toBe(pin?.sha256);
    }
    expect(await sha256File(resolve(HERE, "..", CURRENT_NATIVE_MANIFEST.artifactPath)))
      .toBe(CURRENT_NATIVE_MANIFEST.artifactSha256);
    expect(await sha256File(resolve(RESOURCE_ROOT, "..", "..", pins.native.proofPath))).toBe(pins.native.proofSha256);
    expect(await sha256File(resolve(RESOURCE_ROOT, pins.native.resourceConfigTemplatePath))).toBe(pins.native.resourceConfigTemplateSha256);
  });

  it("freezes weights, provenance, indexes, degrees, snapshots, top edges, and query output", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(FIXED_TIME);
    const kv = graphStore();
    const first = await persistOriginalGraphDelta(kv as never, nodes(), edges(), OBSERVATIONS);
    expect(first).toEqual({ newNodeCount: 3, newEdgeCount: 2 });

    const second = await persistOriginalGraphDelta(kv as never, [
      { ...nodes()[0]!, id: "node-writer-remapped", properties: { callSite: "capture-b" }, sourceObservationIds: ["capture-b"] },
    ], [
      { ...edges()[0]!, id: "edge-writer-file-retry", sourceNodeId: "node-writer-remapped", sourceObservationIds: ["capture-b"] },
    ], ["capture-b"]);
    expect(second).toEqual({ newNodeCount: 0, newEdgeCount: 0 });

    const storedNodes = kv.values<GraphNode>(KV.graphNodes).map(([, value]) => value);
    const storedEdges = kv.values<GraphEdge>(KV.graphEdges).map(([, value]) => value);
    const snapshot = await kv.get<Record<string, unknown>>(KV.graphSnapshot, "current");
    const node = storedNodes.find((value) => value.id === "node-writer");
    const edge = storedEdges.find((value) => value.id === "edge-writer-file");

    expect(storedNodes).toHaveLength(3);
    expect(storedEdges).toHaveLength(2);
    expect(node?.properties).toEqual({ layer: "graph", callSite: "capture-b" });
    expect(node?.sourceObservationIds).toEqual(OBSERVATIONS);
    expect(edge?.weight).toBe(0.9);
    expect(edge?.sourceObservationIds).toEqual(OBSERVATIONS);
    expect(kv.values<string>(KV.graphNameIndex)).toHaveLength(3);
    expect(kv.values<string>(KV.graphEdgeKey)).toHaveLength(2);
    expect(kv.values<number>(KV.graphNodeDegree).map(([, degree]) => degree).sort()).toEqual([1, 1, 2]);
    expect((snapshot?.stats as { totalNodes: number; totalEdges: number }).totalNodes).toBe(3);
    expect((snapshot?.stats as { totalNodes: number; totalEdges: number }).totalEdges).toBe(2);
    expect(snapshot?.topEdges).toHaveLength(2);
    expect(snapshot?.updatedAt).toBe(FIXED_TIME.toISOString());

    const handlers = new Map<string, (input: unknown) => Promise<unknown>>();
    const sdk = { registerFunction: (id: string, handler: (input: unknown) => Promise<unknown>) => handlers.set(id, handler) };
    const { registerGraphFunction } = await import("../.native-pagination-build/graph-resource/oracle/src/functions/graph.js");
    registerGraphFunction(sdk as never, kv as never, { name: "noop" } as never);
    const query = await handlers.get("mem::graph-query")!({}) as { totalNodes: number; totalEdges: number; fromSnapshot?: boolean };
    expect(query).toMatchObject({ totalNodes: 3, totalEdges: 2, fromSnapshot: true });
  });

  it("freezes the distinct callback algorithm and its deterministic replay behavior", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(FIXED_TIME);
    const kv = graphStore();
    const handlers = new Map<string, (input: unknown) => Promise<unknown>>();
    const sdk = { registerFunction: (id: string, handler: (input: unknown) => Promise<unknown>) => handlers.set(id, handler) };
    const { registerGraphFunction } = await import("../.native-pagination-build/graph-resource/oracle/src/functions/graph.js");
    registerGraphFunction(sdk as never, kv as never, { name: "noop" } as never);
    const extract = handlers.get("mem::graph-extract")!;
    const input = {
      observations: [{ id: scenario.identity.captureIds[0] }],
      batchResponse: scenario.promptGroups[0]!.response,
      batchEffectKey: scenario.identity.batchEffectKey,
    };

    await expect(extract(input)).resolves.toMatchObject({ success: true });
    const firstSnapshot = await kv.get<Record<string, unknown>>(KV.graphSnapshot, "current");
    const firstNodes = kv.values<GraphNode>(KV.graphNodes);
    const firstEdges = kv.values<GraphEdge>(KV.graphEdges);
    expect(firstNodes).toHaveLength(2);
    expect(firstEdges).toHaveLength(1);
    expect(firstEdges[0]?.[1]).toMatchObject({ weight: 0.9, sourceObservationIds: [scenario.identity.captureIds[0]] });
    expect(firstSnapshot?.appliedBatchEffects).toEqual([scenario.identity.batchEffectKey]);
    await expect(extract(input)).resolves.toMatchObject({ success: true });
    expect(kv.values<GraphNode>(KV.graphNodes)).toHaveLength(2);
    expect(kv.values<GraphEdge>(KV.graphEdges)).toHaveLength(1);
    expect(await kv.get(KV.graphSnapshot, "current")).toEqual(firstSnapshot);
  });

  it("keeps pre-reset rows orphaned when a later extract reuses their name index", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(FIXED_TIME);
    const kv = graphStore();
    const legacy: GraphNode = { id: "legacy-node", type: "concept", name: "reused topic", properties: {}, sourceObservationIds: ["old-capture"], createdAt: "2025-04-05T05:00:00.000Z" };
    await kv.set(KV.graphNodes, legacy.id, legacy);
    await kv.set(KV.graphNameIndex, "concept|reused topic", legacy.id);
    await kv.set(KV.graphSnapshot, "current", {
      version: 1, topNodes: [], topEdges: [], topDegrees: {},
      stats: { totalNodes: 0, totalEdges: 0, nodesByType: {}, edgesByType: {} },
      updatedAt: FIXED_TIME.toISOString(), dirty: false, resetAt: "2025-04-05T06:00:00.000Z",
    });

    const recreated: GraphNode = { ...legacy, id: "new-node", sourceObservationIds: ["new-capture"], createdAt: "2025-04-05T06:07:08.000Z" };
    await expect(persistOriginalGraphDelta(kv as never, [recreated], [], ["new-capture"]))
      .resolves.toEqual({ newNodeCount: 1, newEdgeCount: 0 });
    expect(await kv.get(KV.graphNameIndex, "concept|reused topic")).toBe("new-node");
    expect(await kv.get(KV.graphNodes, "legacy-node")).toEqual(legacy);
    expect(await kv.get(KV.graphNodes, "new-node")).toMatchObject({ id: "new-node", sourceObservationIds: ["new-capture"] });
  });
});
