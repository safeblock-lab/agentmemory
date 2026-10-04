import { describe, expect, it } from "vitest";
import type { StateKV } from "../src/state/kv.js";
import { IndexedVector } from "../src/state/indexed-vector.js";
import { IndexedRetrievalError } from "../src/state/indexed-retrieval.js";
import { KV } from "../src/state/schema.js";

type QueryAction = "keyword_search" | "semantic_search";
type ReadinessStatus = (call: number) => unknown;
type QueryResponse = (action: string, call: number) => unknown;

function readyStatus(generation = "local-v1"): unknown {
  return {
    version: 1,
    capabilities: ["state::semantic_lsh_v1", "state::indexed_graph_v1"],
    graph: [KV.graphNodes, KV.graphEdges].map((scope) => ({ scope, status: "ready", revision: "1" })),
    semantic: [{
      index_id: "observations",
      model: "local-test",
      dimensions: 4,
      generation,
      status: "ready",
      count: "3",
      lexical_count: "3",
      lexical_ready: true,
      source_kind: "agentmemory",
      source_prepared: true,
      dirty_count: "0",
      coverage_ready: true,
    }],
  };
}

function successfulQuery(action: string): unknown {
  const identity = { model: "local-test", dimensions: 4, generation: "local-v1" };
  const items = [{ obsId: "obs-ready", sessionId: "session-1", score: 0.9 }];
  return action === "semantic_search"
    ? { ...identity, items, approximate: true, budget_exhausted: false, candidates: "1" }
    : { ...identity, items };
}

function makeHarness(
  onQuery: QueryResponse = (action) => successfulQuery(action),
  onStatus: ReadinessStatus = () => readyStatus(),
) {
  const counts: Record<string, number> = {};
  const retrieval = async (payload: Record<string, unknown>): Promise<unknown> => {
    const action = String(payload.action);
    counts[action] = (counts[action] ?? 0) + 1;
    if (action === "index_status") return onStatus(counts[action]);
    return onQuery(action, counts[action]);
  };
  const kv = {
    retrieval,
    get: async (scope: string, key: string) =>
      scope === KV.config && key === "indexed-corpus"
        ? { status: "ready", model: "local-test", dimensions: 4, generation: "local-v1" }
        : null,
    getVersioned: async () => ({
      exists: true,
      version: "7",
      value: { title: "A title", narrative: "A body" },
    }),
    set: async () => undefined,
  } as unknown as StateKV;
  const vector = new IndexedVector(kv, {
    name: "local-test",
    dimensions: 4,
    embed: async () => new Float32Array(4),
  });
  return { vector, counts };
}

describe("indexed query readiness race", () => {
  it.each(["keyword_search", "semantic_search"] as const)(
    "recovers a typed source-readiness race for %s after a fresh readiness check",
    async (action: QueryAction) => {
      const harness = makeHarness((currentAction, call) => {
        if (currentAction === action && call === 1) {
          return { code: "STATE_SEMANTIC_SOURCE_NOT_READY" };
        }
        return successfulQuery(currentAction);
      });

      const results = action === "keyword_search"
        ? await harness.vector.keywordSearch("alpha", 10)
        : await harness.vector.search(new Float32Array(4), 10);

      expect(results.map((result) => result.obsId)).toEqual(["obs-ready"]);
      expect(harness.counts[action]).toBe(2);
      expect(harness.counts.index_status).toBe(2);
    },
  );

  it("stops after three total query attempts", async () => {
    const harness = makeHarness((action) => ({ code: "STATE_SEMANTIC_SOURCE_NOT_READY" }));

    await expect(harness.vector.keywordSearch("alpha", 10)).rejects.toMatchObject({
      code: "STATE_SEMANTIC_SOURCE_NOT_READY",
    });
    expect(harness.counts.keyword_search).toBe(3);
    expect(harness.counts.index_status).toBe(3);
  });

  it("does not query again when readiness changes before a retry", async () => {
    const harness = makeHarness(
      () => ({ code: "STATE_SEMANTIC_SOURCE_NOT_READY" }),
      (call) => readyStatus(call === 1 ? "local-v1" : "changed-generation"),
    );

    await expect(harness.vector.keywordSearch("alpha", 10)).rejects.toMatchObject({
      code: "STATE_INDEX_NOT_READY",
    });
    expect(harness.counts.keyword_search).toBe(1);
    expect(harness.counts.index_status).toBe(2);
  });

  it.each([
    ["identity", (action: string) => ({ ...successfulQuery(action) as object, model: "other-model" })],
    ["resource", () => ({ code: "STATE_RETRIEVAL_RESOURCE_LIMIT" })],
    ["transport", () => { throw new Error("transport disconnected"); }],
    ["untyped", () => { throw new Error("unexpected native query failure"); }],
  ])("does not retry %s query failures", async (_label, response) => {
    const harness = makeHarness(response as QueryResponse);

    await expect(harness.vector.keywordSearch("alpha", 10)).rejects.toBeDefined();
    expect(harness.counts.keyword_search).toBe(1);
    expect(harness.counts.index_status).toBe(1);
  });

  it("keeps a sticky mutation failure visible without retrying a query", async () => {
    const mutationFailure = new IndexedRetrievalError(
      "STATE_RETRIEVAL_RESOURCE_LIMIT",
      "mutation rejected",
    );
    const harness = makeHarness((action) => {
      if (action === "semantic_upsert") throw mutationFailure;
      return successfulQuery(action);
    });

    await expect(harness.vector.add(
      "obs-1",
      "session-1",
      new Float32Array(4),
      "A title A body",
    )).rejects.toBe(mutationFailure);
    await expect(harness.vector.keywordSearch("alpha", 10)).rejects.toBe(mutationFailure);
    expect(harness.counts.semantic_upsert).toBe(1);
    expect(harness.counts.index_status ?? 0).toBe(0);
    expect(harness.counts.keyword_search ?? 0).toBe(0);
  });
});
