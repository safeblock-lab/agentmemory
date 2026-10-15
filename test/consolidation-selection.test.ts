import { describe, expect, it, vi } from "vitest";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import type { EmbeddingProvider, HybridSearchResult } from "../src/types.js";
import { createSkillObservationSelector, selectIndexedContextObservations } from "../src/functions/consolidation-selection.js";

vi.mock("../src/state/indexed-vector.js", () => ({
  getIndexedVector: () => ({ ready: vi.fn(), identity: { index_id: "observations", model: "local", dimensions: 2, generation: "local-v1" } }),
  configuredSemanticProbes: () => 128,
}));
vi.mock("../src/state/reranker.js", () => ({ rerank: async (_query: string, results: HybridSearchResult[]) => results }));

const provider: EmbeddingProvider = {
  name: "local",
  dimensions: 2,
  embed: async () => new Float32Array([1, 0]),
  embedBatch: async () => [],
};

function selectedObservation(approximate: unknown, includeApproximate = true, context?: { reason: string; empty?: boolean }) {
  const identity = { model: "local", dimensions: 2, generation: "local-v1", status: "ready", budget_exhausted: false };
  const item = { obsId: "o", sessionId: "s", score: 0.9, text: "Scoped procedure", source_version: "4", source_scope: "mem:obs:s" };
  const status = { version: 1, metadata_only: true, index_id: "observations", capabilities: ["state::semantic_lsh_v1"], semantic: [
    { index_id: "observations", model: "local", dimensions: 2, generation: "local-v1", status: "ready", lexical_ready: true, source_kind: "agentmemory" },
  ] };
  const marker = { status: "pending", model: "local", dimensions: 2, generation: "local-v1" };
  const prepared = { status: "ready", index_id: "observations", project: "P", source_kind: "observation", processed: 0,
    session_visits: 0, text_bytes: 0, cursor: "", coverage_epoch: "1", project_generation: "1", has_more: false, reason: null };
  const coverage = context ? { status: "partial", reason: context.reason, budget_exhausted: context.reason === "query_work_budget" } : {};
  const lexical = { ...identity, ...coverage, items: context?.empty ? [] : [item] };
  const semantic = { ...identity, ...coverage, ...(includeApproximate ? { approximate } : {}), items: context?.empty ? [] : [item] };
  const kv = {
    retrieval: async (payload: Record<string, unknown>) => {
      if (payload.action === "index_status") return status;
      if (payload.action === "project_vectors_prepare") {
        if (context) throw new Error("context vector preparation forbidden");
        return prepared;
      }
      return payload.action === "semantic_search" ? semantic : lexical;
    },
    get: async (scope: string) => {
      if (scope === KV.config) return marker;
      if (scope === KV.sessions) return { id: "s", project: "P" };
      if (scope === KV.summaries) return { id: "s", sessionId: "s", project: "P", title: "Procedure", narrative: "Scoped source" };
      return null;
    },
  } as unknown as StateKV;
  return context ? selectIndexedContextObservations(kv, provider, "P", "Scoped procedure")
    : createSkillObservationSelector(kv, provider)("P", "s");
}

describe("selected observation search receipt", () => {
  it.each([true, false])("accepts a boolean approximate flag of %s", async approximate => {
    await expect(selectedObservation(approximate)).resolves.toEqual([{ id: "o", sessionId: "s", rowVersion: "4" }]);
  });

  it.each([
    ["missing", undefined, false],
    ["string", "false", true],
  ] as const)("rejects an invalid approximate flag (%s)", async (_label, approximate, includeApproximate) => {
    await expect(selectedObservation(approximate, includeApproximate)).rejects.toThrow("skill_scoped_retrieval_invalid");
  });

  it.each(["catalog_not_ready", "project_mapping_not_ready", "project_source_not_ready", "project_projection_pending"])
    ("keeps available context hits during %s without invoking preparation", async reason => {
      await expect(selectedObservation(true, true, { reason })).resolves.toMatchObject({
        partial: true, observations: [{ ref: { id: "o", sessionId: "s", rowVersion: "4" } }],
      });
    });

  it.each(["project_index_not_ready", "query_work_budget"])("keeps empty context coverage partial for %s", async reason => {
    await expect(selectedObservation(true, true, { reason, empty: true })).resolves.toEqual({ partial: true, observations: [] });
  });

  it("rejects unknown partial reasons for context", async () => {
    await expect(selectedObservation(true, true, { reason: "invented" })).rejects.toThrow("STATE_INDEX_NOT_READY");
  });
});
