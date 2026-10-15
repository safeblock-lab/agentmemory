import { describe, expect, it, vi } from "vitest";
import { CanonicalMemoryConflict, CanonicalMemoryIndex, CanonicalMemoryPending, MAX_CANONICAL_RECORD_BYTES, type CanonicalLookup } from "../src/state/canonical-memory.js";
import type { StateKV } from "../src/state/kv.js";
import type { Memory, SemanticMemory } from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { consolidationRetrievalCoverage, createSkillObservationSelector, createProjectConsolidationSelector,
  selectConsolidationSummaries, selectProceduralObservations } from "../src/functions/consolidation-selection.js";
import type { CompressedObservation, EmbeddingProvider, HybridSearchResult } from "../src/types.js";

const { rerankMode, vectorReady, durableReplace } = vi.hoisted(() => ({ rerankMode: { unavailable: false }, vectorReady: vi.fn(), durableReplace: vi.fn() }));
vi.mock("../src/state/indexed-vector.js", () => ({ getIndexedVector: () => ({
  ready: vectorReady, identity: { index_id: "observations", model: "local", dimensions: 2, generation: "local-v1" } }), configuredSemanticProbes: () => 128 }));
vi.mock("../src/state/reranker.js", () => ({ rerank: async (_query: string, results: HybridSearchResult[]) =>
  rerankMode.unavailable ? results : [...results].reverse() }));
vi.mock("../src/functions/consolidation-manifest.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/functions/consolidation-manifest.js")>(), replacePagedConsolidationRequest: durableReplace,
}));

process.env["VITEST"] = "true";
const entry = await import("../src/index.js");

const effect = "a".repeat(64);
const ready = (id: string | null = null, version = "1") => ({ status: "ready", canonical_id: id,
  row_version: id ? version : null, scope_revision: version, coverage_epoch: "1", normalization_version: "js-lower-v1" });
function committed(payload: Record<string, unknown>, applied = true, created = true, id = "legacy") {
  const receipt = { operation_kind: payload.operation_kind, effect_key: payload.effect_key,
    operation_fingerprint: payload.operation_fingerprint, canonical_id: id, row_version: "3", created, result_fingerprint: "b".repeat(64) };
  return { status: applied ? "applied" : "already_applied", applied, created, canonical_id: id, row_version: "3", result_fingerprint: receipt.result_fingerprint, receipt };
}
function fake(handle: (payload: Record<string, unknown>) => unknown) {
  const retrieval = vi.fn(async (payload: Record<string, unknown>) => handle(payload));
  const getVersioned = vi.fn(async () => ({ exists: false, value: null, version: "0" } as { exists: boolean; value: unknown; version: string }));
  const kv = { retrieval, getVersioned } as unknown as StateKV;
  return { index: new CanonicalMemoryIndex(kv), retrieval, getVersioned };
}
const fact = (accessCount = 1): SemanticMemory => ({ id: "legacy", fact: "Fact", accessCount, sourceSessionIds: ["s"], strength: 0.5,
  sourceMemoryIds: [], confidence: 0.5, lastAccessedAt: "2026-10-01T00:00:00Z", createdAt: "2026-10-01T00:00:00Z", updatedAt: "2026-10-01T00:00:00Z" });
const memory: Memory = { id: "new", project: "P", title: "Title", type: "fact", content: "Grounded content",
  concepts: ["c"], files: [], sessionIds: ["s"], sourceObservationIds: ["o"], strength: 5, version: 1,
  isLatest: true, createdAt: "2026-10-09T00:00:00Z", updatedAt: "2026-10-09T00:00:00Z" };

describe("canonical memory adapter", () => {
  it.each(["ΟΣ", "İ", "𐐀", " Fact "])("preserves exact JavaScript lowercase identity without trimming %s", async input => {
    const f = fake(() => ready());
    await f.index.lookup("semantic", input.toLowerCase());
    expect(f.retrieval).toHaveBeenCalledWith({ action: "canonical_lookup", scope: KV.semantic, partition: "", normalized_key: input.toLowerCase() });
  });
  it("keeps partial identity coverage explicit, never absent", async () => {
    const f = fake(() => ({ status: "partial", continuation: "blocked:legacy", invalid_records: 1 }));
    await expect(f.index.lookup("semantic", "fact")).rejects.toMatchObject({ partial: true, code: "STATE_CANONICAL_PREPARING" });
    expect(f.retrieval).toHaveBeenCalledTimes(1);
  });
  it("executes one bounded preparation cut and yields durable continuation", async () => {
    const f = fake(payload => payload.action === "canonical_lookup"
      ? { status: "preparing", continuation: "next", invalid_records: 0 }
      : { status: "pending", continuation: "cursor:16", cursor: "row16", processed: 16, scope_revision: "1", coverage_epoch: "1", invalid_records: 0 });
    await expect(f.index.lookup("semantic", "fact")).rejects.toBeInstanceOf(CanonicalMemoryPending);
    expect(f.retrieval).toHaveBeenLastCalledWith({ action: "canonical_prepare", scope: KV.semantic, max_rows: 16, max_bytes: 65_536 });
    expect(f.retrieval).toHaveBeenCalledTimes(2);
  });
  it.each([{ ...ready(), row_version: "1" }, { ...ready("legacy"), row_version: null }, { ...ready(), scope_revision: "-1" },
    { ...ready(), normalization_version: "ascii-only" }])("rejects invalid native lookup states %#", async raw => {
    const f = fake(() => raw);
    await expect(f.index.lookup("semantic", "fact")).rejects.toThrow("STATE_TX_INVALID_RESPONSE");
  });
  it("recomputes positive update on conflict with unchanged captured fingerprint", async () => {
    let applies = 0;
    let lookups = 0;
    const f = fake(payload => {
      if (payload.receipt_only) return { status: "not_applied" };
      if (payload.action === "canonical_lookup") return ready("legacy", String(++lookups));
      if (++applies === 1) return { code: "STATE_TX_CONFLICT" };
      return committed(payload, true, false);
    });
    f.getVersioned.mockImplementation(async () => ({ exists: true, value: fact(lookups), version: String(lookups) }));
    const match = await f.index.lookup("semantic", "fact");
    const update = vi.fn((current: SemanticMemory | null) => current ? { ...current, accessCount: current.accessCount + 1 } : fact());
    const result = await f.index.upsert("semantic", "fact", match, "stable-new", effect, update);
    expect(result).toMatchObject({ applied: true, created: false, id: "legacy" });
    const writes = f.retrieval.mock.calls.map(([payload]) => payload).filter(payload => payload.action === "canonical_apply" && !payload.receipt_only);
    expect(writes.map(payload => payload.expected_row_version)).toEqual(["1", "2"]);
    expect(writes.map(payload => (payload.value as ReturnType<typeof fact>).accessCount)).toEqual([2, 3]);
    expect(new Set(writes.map(payload => payload.operation_fingerprint)).size).toBe(1);
    expect(update).toHaveBeenCalledTimes(3);
  });
  it("resolves a concurrent negative create into one canonical update", async () => {
    let attempts = 0;
    const f = fake(payload => payload.receipt_only ? { status: "not_applied" }
      : payload.action === "canonical_lookup" ? ready(attempts ? "legacy" : null)
      : ++attempts === 1 ? { code: "STATE_TX_CONFLICT" } : committed(payload, true, false));
    f.getVersioned.mockImplementation(async () => attempts ? { exists: true, value: fact(), version: "1" } : { exists: false, value: null, version: "0" });
    const match = await f.index.lookup("semantic", "fact");
    const result = await f.index.upsert("semantic", "fact", match, "stable-new", effect, current => current ?? fact());
    expect(result.created).toBe(false);
    expect(attempts).toBe(2);
    const targets = f.retrieval.mock.calls.map(([payload]) => payload).filter(payload => !payload.receipt_only && payload.action === "canonical_apply").map(payload => payload.target_id);
    expect(targets).toEqual(["stable-new", "legacy"]);
  });
  it("stops after three conflicts, retaining explicit pending rather than falling back", async () => {
    const f = fake(payload => payload.receipt_only ? { status: "not_applied" } : payload.action === "canonical_lookup" ? ready() : { code: "STATE_TX_CONFLICT" });
    const match = await f.index.lookup("semantic", "fact");
    await expect(f.index.upsert("semantic", "fact", match, "new", effect, () => fact())).rejects.toBeInstanceOf(CanonicalMemoryConflict);
    expect(f.retrieval.mock.calls.filter(([payload]) => payload.action === "canonical_apply" && !payload.receipt_only)).toHaveLength(3);
  });
  it("uses an independent evolution receipt after later deletion, with no reads or writes", async () => {
    const f = fake(payload => committed(payload, false, true, "original-new"));
    const token: CanonicalLookup = { status: "ready", scope: "memories", partition: "P", normalizedKey: "title", canonicalId: null,
      rowVersion: null, scopeRevision: "9", coverageEpoch: "1", normalizationVersion: "js-lower-v1" };
    const result = await f.index.evolve("P", "title", token, memory, effect);
    expect(result).toMatchObject({ applied: false, created: true, id: "original-new" });
    expect(f.getVersioned).not.toHaveBeenCalled();
    expect(f.retrieval).toHaveBeenCalledTimes(1);
    expect(f.retrieval.mock.calls[0][0]).toMatchObject({ receipt_only: true, target_id: "new", normalized_key: "title", partition: "P" });
  });
  it("recomputes only evolution parent and version after a concurrent winner change", async () => {
    let round = 0;
    const f = fake(payload => {
      if (payload.receipt_only) return { status: "not_applied" };
      if (payload.action === "canonical_lookup") return ready(`old${round}`, String(round + 1));
      if (round++ === 0) return { code: "STATE_TX_CONFLICT" };
      return committed(payload, true, true, "new");
    });
    f.getVersioned.mockImplementation(async (_scope?: string, key?: string) => key === "new"
      ? { exists: false, value: null, version: "0" }
      : { exists: true, value: { ...memory, id: `old${round}`, version: round + 4 }, version: String(round + 1) });
    const token = await f.index.lookup("memories", "title", "P");
    await f.index.evolve("P", "title", token, memory, effect);
    const writes = f.retrieval.mock.calls.map(([payload]) => payload).filter(payload => payload.action === "canonical_apply" && !payload.receipt_only);
    expect(writes.map(payload => (payload.value as Memory).parentId)).toEqual(["old0", "old1"]);
    expect(writes.map(payload => (payload.value as Memory).version)).toEqual([5, 6]);
    expect(writes.map(payload => (payload.value as Memory).content)).toEqual([memory.content, memory.content]);
    expect(writes.map(payload => (payload.value as Memory).sourceObservationIds)).toEqual([["o"], ["o"]]);
    expect(new Set(writes.map(payload => payload.operation_fingerprint)).size).toBe(1);
  });
  it("keeps an oversized canonical update pending without sending a huge write", async () => {
    const f = fake(payload => payload.receipt_only ? { status: "not_applied" } : ready());
    const token = await f.index.lookup("memories", "title", "P");
    await expect(f.index.evolve("P", "title", token, { ...memory, content: "x".repeat(MAX_CANONICAL_RECORD_BYTES) }, effect)).rejects.toMatchObject({ partial: true, code: "STATE_CANONICAL_PREPARING" });
    expect(f.retrieval.mock.calls.filter(([payload]) => payload.action === "canonical_apply" && !payload.receipt_only)).toHaveLength(0);
  });
  it("preserves large legacy effect metadata without appending new markers", async () => {
    const markers = Array.from({ length: 2000 }, (_, id) => id.toString(16).padStart(64, "0"));
    const previous = { ...fact(), appliedBatchEffects: markers };
    const f = fake(payload => payload.receipt_only ? { status: "not_applied" } : payload.action === "canonical_lookup" ? ready("legacy") : committed(payload, true, false));
    f.getVersioned.mockResolvedValue({ exists: true, value: previous, version: "1" });
    const token = await f.index.lookup("semantic", "fact");
    await f.index.upsert("semantic", "fact", token, "stable-new", effect, current => current ? { ...current, accessCount: current.accessCount + 1 } : previous);
    const write = f.retrieval.mock.calls.find(([payload]) => payload.action === "canonical_apply" && !payload.receipt_only)![0];
    expect((write.value as SemanticMemory).appliedBatchEffects).toEqual(markers);
    expect(Buffer.byteLength(JSON.stringify(write.value))).toBeGreaterThan(65_536);
  });
  it("probes decay receipt before reading a deleted record", async () => {
    const f = fake(payload => committed(payload, false, false, "deleted"));
    await expect(f.index.decay("semantic", { id: "deleted", rowVersion: "2" }, ["s"], "P", effect, { at: "fixed" }, current => current)).resolves.toMatchObject({ applied: false, id: "deleted" });
    expect(f.getVersioned).not.toHaveBeenCalled();
    expect(f.retrieval.mock.calls[0][0]).toMatchObject({ receipt_only: true, target_id: "deleted" });
  });
  it("does not recreate a missing decay record after explicit receipt absence", async () => {
    const f = fake(() => ({ status: "not_applied" }));
    await expect(f.index.decay("semantic", { id: "deleted", rowVersion: "2" }, ["s"], "P", effect, {}, current => current)).rejects.toBeInstanceOf(CanonicalMemoryConflict);
    expect(f.retrieval).toHaveBeenCalledTimes(1);
  });
  it("keeps session IDs, versions and project on every source-page request", async () => {
    const f = fake(() => ({ status: "ready", items: [{ id: "a", row_version: "7" }], next_cursor: "next", has_more: true, membership_generation: "4", coverage_epoch: "2" }));
    expect(await f.index.sourcePage("semantic", ["s"], null, "P")).toEqual({ items: [{ id: "a", rowVersion: "7" }], nextCursor: "next", membershipGeneration: "4", coverageEpoch: "2" });
    expect(f.retrieval).toHaveBeenCalledWith({ action: "canonical_source_page", scope: KV.semantic, source_session_ids: ["s"], source_project: "P", limit: 12, max_bytes: 65_536 });
  });
  it("rejects malformed receipts instead of manufacturing successful counts", async () => {
    const f = fake(payload => ({ ...committed(payload, false), receipt: { effect_key: "wrong" } }));
    await expect(f.index.evolve("P", "title", {} as CanonicalLookup, memory, effect)).rejects.toThrow("STATE_TX_INVALID_RESPONSE");
  });
});

describe("bounded source selection", () => {
  const observation: CompressedObservation = { id: "o", sessionId: "s", timestamp: "2026-10-09T00:00:00Z", type: "decision",
    title: "Repeatable release", narrative: "Run the scoped package check before release", facts: [], files: [], concepts: ["release"], importance: 8 };
  const provider: EmbeddingProvider = { name: "local", dimensions: 2, embed: async () => new Float32Array([1, 0]), embedBatch: async () => [] };
  function scopedFake(wrongSession = false) {
    const indexStatus = { version: 1, metadata_only: true, index_id: "observations", capabilities: ["state::semantic_lsh_v1"],
      semantic: [{ index_id: "observations", model: "local", dimensions: 2,
        generation: "local-v1", status: "ready", lexical_ready: true, source_kind: "agentmemory" }] };
    const marker = { status: "pending", model: "local", dimensions: 2, generation: "local-v1" };
    const status = vi.fn(async (): Promise<Record<string, unknown>> => indexStatus);
    const prepare = vi.fn(async (payload: Record<string, unknown>): Promise<Record<string, unknown>> => ({ status: "ready", index_id: "observations",
      project: payload.project, source_kind: "observation", processed: 0, session_visits: 0, text_bytes: 0, cursor: "",
      coverage_epoch: "1", project_generation: "1", has_more: false, reason: null }));
    const search = vi.fn(async (_payload: Record<string, unknown>): Promise<Record<string, unknown>> => ({ model: "local", dimensions: 2, generation: "local-v1", status: "ready", approximate: true, budget_exhausted: false,
      items: [{ obsId: "o", sessionId: wrongSession ? "other" : "s", score: 0.9,
        text: "Run the scoped package check", source_version: "4", source_scope: "mem:obs:s" }] }));
    const retrieval = vi.fn(async (payload: Record<string, unknown>) => payload.action === "index_status" ? status()
      : payload.action === "project_vectors_prepare" ? prepare(payload) : search(payload));
    const get = vi.fn(async (scope: string) => scope === KV.config ? marker : scope === KV.observations("s") ? observation
      : scope === KV.sessions ? { id: "s", project: "P" } : { sessionId: "s", project: "P", title: "Release", narrative: "Grounded decisions" });
    const getVersioned = vi.fn(async () => ({ exists: true, value: observation, version: "4" }));
    return { kv: { retrieval, get, getVersioned } as unknown as StateKV, retrieval, get, getVersioned, search, status, prepare, indexStatus, marker };
  }
  it("prepares only the selected observation project even while unrelated global source coverage is pending", async () => {
    const h = scopedFake();
    vectorReady.mockClear();
    expect(await createProjectConsolidationSelector(h.kv, provider)("P")).toHaveLength(1);
    expect(h.prepare).toHaveBeenCalledExactlyOnceWith({ action: "project_vectors_prepare", index_id: "observations",
      project: "P", source_kind: "observation", max_rows: 16, max_bytes: 65_536 });
    expect(h.retrieval).toHaveBeenCalledWith({ action: "index_status", index_id: "observations", metadata_only: true });
    expect(vectorReady).not.toHaveBeenCalled();
    expect(h.marker.status).toBe("pending");
  });
  it.each(["missing_metadata_only", "full_status", "missing_index", "foreign_index", "foreign_meta", "no_meta", "multiple_meta"])
    ("rejects %s status framing before preparation or embeddings", async defect => {
      const h = scopedFake();
      const response: Record<string, unknown> = { ...h.indexStatus };
      if (defect === "missing_metadata_only") delete response.metadata_only;
      if (defect === "full_status") response.metadata_only = false;
      if (defect === "missing_index") delete response.index_id;
      if (defect === "foreign_index") response.index_id = "foreign";
      if (defect === "foreign_meta") response.semantic = [{ ...h.indexStatus.semantic[0], index_id: "foreign" }];
      if (defect === "no_meta") response.semantic = [];
      if (defect === "multiple_meta") response.semantic = [...h.indexStatus.semantic, { ...h.indexStatus.semantic[0], index_id: "foreign" }];
      h.status.mockResolvedValue(response);
      const embed = vi.fn(provider.embed);
      await expect(createProjectConsolidationSelector(h.kv, { ...provider, embed })("P"))
        .rejects.toMatchObject({ code: "STATE_TX_INVALID_RESPONSE" });
      expect(h.prepare).not.toHaveBeenCalled();
      expect(embed).not.toHaveBeenCalled();
      expect(h.search).not.toHaveBeenCalled();
    });
  it("yields after one preparation cut and embeds only after selected coverage becomes ready", async () => {
    const h = scopedFake();
    const embed = vi.fn(provider.embed);
    h.prepare.mockResolvedValueOnce({ status: "pending", index_id: "observations", project: "P", source_kind: "observation",
      processed: 16, session_visits: 2, text_bytes: 4096, cursor: "native-cursor", coverage_epoch: "1", project_generation: "1",
      has_more: true, reason: "project_projection_pending" });
    const select = createProjectConsolidationSelector(h.kv, { ...provider, embed });
    await expect(select("P")).rejects.toMatchObject({ code: "STATE_INDEX_NOT_READY" });
    expect(h.prepare).toHaveBeenCalledTimes(1);
    expect(embed).not.toHaveBeenCalled();
    expect(h.search).not.toHaveBeenCalled();
    expect(await select("P")).toHaveLength(1);
    expect(h.prepare).toHaveBeenCalledTimes(2);
    expect(embed).toHaveBeenCalledTimes(1);
  });
  it.each(["native_model", "native_dimensions", "native_generation", "marker_generation", "missing_capability"])
    ("rejects %s before preparing, embedding or searching", async defect => {
      const h = scopedFake();
      const embed = vi.fn(provider.embed);
      if (defect === "native_model") h.indexStatus.semantic[0]!.model = "other";
      if (defect === "native_dimensions") h.indexStatus.semantic[0]!.dimensions = 3;
      if (defect === "native_generation") h.indexStatus.semantic[0]!.generation = "old";
      if (defect === "marker_generation") h.marker.generation = "old";
      if (defect === "missing_capability") h.indexStatus.capabilities = [];
      await expect(createProjectConsolidationSelector(h.kv, { ...provider, embed })("P")).rejects.toMatchObject({ code: "STATE_INDEX_NOT_READY" });
      expect(h.prepare).not.toHaveBeenCalled();
      expect(embed).not.toHaveBeenCalled();
      expect(h.search).not.toHaveBeenCalled();
    });
  it.each([{ processed: 17 }, { project: "other" }, { coverage_epoch: "unknown" }, { text_bytes: 1_048_577 }, { has_more: true }])
    ("rejects invalid selected preparation bounds or identity %j", async defect => {
      const h = scopedFake();
      const prepared = h.prepare.getMockImplementation()!;
      h.prepare.mockImplementation(async payload => ({ ...await prepared(payload), ...defect }));
      const embed = vi.fn(provider.embed);
      await expect(createProjectConsolidationSelector(h.kv, { ...provider, embed })("P"))
        .rejects.toMatchObject({ code: "STATE_TX_INVALID_RESPONSE" });
      expect(embed).not.toHaveBeenCalled();
      expect(h.search).not.toHaveBeenCalled();
    });
  it("applies project, exact session and observation scope to lexical and vector searches before limits", async () => {
    const h = scopedFake();
    expect(await createSkillObservationSelector(h.kv, provider)("P", "s")).toEqual([{ id: "o", sessionId: "s", rowVersion: "4" }]);
    expect(h.retrieval).toHaveBeenCalledTimes(4);
    for (const [payload] of h.search.mock.calls) {
      expect(payload).toMatchObject({ project: "P", session_id: "s", source_scope: "mem:obs:s", source_kind: "observation", include_text: true, limit: 12 });
      expect(payload).not.toHaveProperty("global");
    }
    expect(h.get.mock.calls.some(([scope]) => scope.startsWith("mem:obs:"))).toBe(false);
  });
  it("rejects a native session leak instead of filtering after a limit", async () => {
    const h = scopedFake(true);
    await expect(createSkillObservationSelector(h.kv, provider)("P", "s")).rejects.toThrow("skill_scoped_retrieval_invalid");
    expect(h.get.mock.calls.some(([scope]) => scope.startsWith("mem:obs:"))).toBe(false);
  });
  it("balances differently scaled streams in deterministic order when the local reranker is unavailable", async () => {
    rerankMode.unavailable = true;
    try {
      const h = scopedFake();
      h.search.mockImplementation(async (payload: Record<string, unknown>) => ({
        model: "local", dimensions: 2, generation: "local-v1", status: "ready", approximate: true, budget_exhausted: false,
        items: Array.from({ length: 12 }, (_, index) => ({ obsId: `${payload.action === "keyword_search" ? "l" : "v"}${String(index).padStart(2, "0")}`,
          sessionId: "s", score: payload.action === "keyword_search" ? 30 - index : 0.9 - index * 0.01,
          text: "Scoped workflow", source_version: "4", source_scope: "mem:obs:s" })),
      }));
      const selected = await createSkillObservationSelector(h.kv, provider)("P", "s");
      expect(selected.map(ref => ref.id)).toEqual(Array.from({ length: 6 }, (_, index) =>
        [`l${String(index).padStart(2, "0")}`, `v${String(index).padStart(2, "0")}`]).flat());
      for (const [payload] of h.search.mock.calls) {
        expect(payload).toMatchObject({ project: "P", session_id: "s", source_scope: "mem:obs:s", source_kind: "observation", limit: 12 });
      }
      expect(h.getVersioned).not.toHaveBeenCalled();
    } finally { rerankMode.unavailable = false; }
  });
  it("rewards corroborated candidates without comparing lexical and vector score magnitudes", async () => {
    rerankMode.unavailable = true;
    try {
      const h = scopedFake();
      h.search.mockImplementation(async (payload: Record<string, unknown>) => ({
        model: "local", dimensions: 2, generation: "local-v1", status: "ready", approximate: true, budget_exhausted: false,
        items: [payload.action === "keyword_search" ? "lexical" : "vector", "both"].map((obsId, index) => ({ obsId,
          sessionId: "s", score: payload.action === "keyword_search" ? 40 - index : 0.9 - index * 0.1,
          text: "Scoped workflow", source_version: "4", source_scope: "mem:obs:s" })),
      }));
      expect((await createSkillObservationSelector(h.kv, provider)("P", "s")).map(ref => ref.id)).toEqual(["both", "lexical", "vector"]);
    } finally { rerankMode.unavailable = false; }
  });
  it("selects observation kind within project before limits and returns only versioned references", async () => {
    const h = scopedFake();
    expect(await createProjectConsolidationSelector(h.kv, provider)("P")).toEqual([{ id: "o", sessionId: "s", rowVersion: "4", scope: "mem:obs:s" }]);
    for (const [payload] of h.search.mock.calls) {
      expect(payload).toMatchObject({ project: "P", source_kind: "observation", include_text: true, limit: 12 });
      expect(payload).not.toHaveProperty("global");
    }
    expect(h.getVersioned).not.toHaveBeenCalled();
    await expect(createProjectConsolidationSelector(h.kv, provider)("")).rejects.toThrow("project_required");
    expect(h.retrieval).toHaveBeenCalledTimes(4);
  });
  it.each(["project_index_not_ready", "project_authority_incomplete", "projection_source_missing", "projection_budget"])
    ("keeps %s pending rather than interpreting empty projected items as absence", async reason => {
      const h = scopedFake();
      h.search.mockImplementation(async () => ({ model: "local", dimensions: 2, generation: "local-v1", status: "partial",
        approximate: true, budget_exhausted: false, reason, items: [] }));
      await expect(createSkillObservationSelector(h.kv, provider)("P", "s")).rejects.toMatchObject({ code: "STATE_INDEX_NOT_READY" });
      expect(h.getVersioned).not.toHaveBeenCalled();
    });
  it("permits the explicit bounded ANN candidate budget without confusing it with incomplete authority", async () => {
    const h = scopedFake();
    const readyResponse = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => ({ ...await readyResponse(payload),
      ...(payload.action === "semantic_search" ? { status: "partial", reason: "candidate_budget", budget_exhausted: true } : {}) }));
    expect(await createSkillObservationSelector(h.kv, provider)("P", "s")).toEqual([{ id: "o", sessionId: "s", rowVersion: "4",
      retrievalCoverage: ["bounded_candidate_budget"] }]);
  });
  it("preserves usable lexical candidates and their explicit candidate-budget coverage", async () => {
    const h = scopedFake();
    const response = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => ({ ...await response(payload),
      ...(payload.action === "keyword_search" ? { status: "partial", reason: "candidate_budget", budget_exhausted: true } : {}) }));
    expect(await createSkillObservationSelector(h.kv, provider)("P", "s")).toEqual([{ id: "o", sessionId: "s", rowVersion: "4",
      retrievalCoverage: ["bounded_candidate_budget"] }]);
  });
  it("reports an empty lexical candidate budget as a selected resource failure", async () => {
    const h = scopedFake();
    const response = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => ({ ...await response(payload), items: [],
      ...(payload.action === "keyword_search" ? { status: "partial", reason: "candidate_budget", budget_exhausted: true } : {}) }));
    await expect(createSkillObservationSelector(h.kv, provider)("P", "s")).rejects.toMatchObject({ code: "STATE_RETRIEVAL_RESOURCE_LIMIT",
      diagnostics: { reason: "candidate_budget", project: "P", sessionId: "s", sourceKind: "observation", limit: 12 } });
    expect(h.getVersioned).not.toHaveBeenCalled();
  });
  it("rejects lexical candidate-budget partial responses without the exhausted flag", async () => {
    const h = scopedFake();
    const response = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => ({ ...await response(payload),
      ...(payload.action === "keyword_search" ? { status: "partial", reason: "candidate_budget", budget_exhausted: false } : {}) }));
    await expect(createSkillObservationSelector(h.kv, provider)("P", "s")).rejects.toMatchObject({ code: "STATE_INDEX_NOT_READY" });
    expect(h.getVersioned).not.toHaveBeenCalled();
  });
  it("retains nonempty candidates with canonical coverage union when either stream exhausts bounded work", async () => {
    const h = scopedFake();
    const response = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => ({ ...await response(payload), status: "partial", budget_exhausted: true,
      reason: payload.action === "keyword_search" ? "query_work_budget" : "candidate_budget" }));
    expect(await createSkillObservationSelector(h.kv, provider)("P", "s")).toEqual([{ id: "o", sessionId: "s", rowVersion: "4",
      retrievalCoverage: ["bounded_candidate_budget", "bounded_query_budget"] }]);
  });
  it("uses the other stream's valid candidates when one budget-interrupted stream returns none", async () => {
    const h = scopedFake();
    const response = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => payload.action === "keyword_search"
      ? { ...await response(payload), items: [], status: "partial", reason: "query_work_budget", budget_exhausted: true } : response(payload));
    expect(await createSkillObservationSelector(h.kv, provider)("P", "s")).toEqual([{ id: "o", sessionId: "s", rowVersion: "4",
      retrievalCoverage: ["bounded_query_budget"] }]);
  });
  it.each(["query_work_budget", "candidate_budget"])("reports empty interrupted %s as a resource limit with exact selected diagnostics", async reason => {
    const h = scopedFake();
    const response = h.search.getMockImplementation()!;
    h.search.mockImplementation(async payload => ({ ...await response(payload), items: [],
      ...(payload.action === "semantic_search" ? { status: "partial", reason, budget_exhausted: true } : {}) }));
    await expect(createSkillObservationSelector(h.kv, provider)("P", "s")).rejects.toMatchObject({ code: "STATE_RETRIEVAL_RESOURCE_LIMIT",
      diagnostics: { reason, project: "P", sessionId: "s", sourceKind: "observation", limit: 12 } });
    expect(h.prepare).toHaveBeenCalledTimes(1);
    expect(h.search).toHaveBeenCalledTimes(2);
    expect(h.getVersioned).not.toHaveBeenCalled();
  });
  it("preserves coverage union per summary session and procedural reference", async () => {
    const h = scopedFake();
    const refs = [{ id: "a", sessionId: "s", rowVersion: "1", retrievalCoverage: ["bounded_query_budget" as const] },
      { id: "b", sessionId: "s", rowVersion: "2", retrievalCoverage: ["bounded_candidate_budget" as const] }];
    const summaries = await selectConsolidationSummaries(h.kv, "P", undefined, async () => refs);
    expect(summaries[0]!.retrievalCoverage).toEqual(["bounded_candidate_budget", "bounded_query_budget"]);
    expect((await selectProceduralObservations(h.kv, "P", summaries, refs))[0]!.retrievalCoverage).toEqual(["bounded_query_budget"]);
    expect(h.get).not.toHaveBeenCalled();
  });
  it.each([["unknown"], ["bounded_query_budget", "bounded_query_budget"], ["bounded_query_budget", "bounded_candidate_budget", "unknown"]])
    ("rejects invalid or unbounded coverage metadata %j", (...labels) => {
      expect(() => consolidationRetrievalCoverage([{ retrievalCoverage: labels }])).toThrow("consolidation_retrieval_coverage_invalid");
    });
  it("builds summary and procedural references without loading source payloads or inventing frequency", async () => {
    const h = scopedFake();
    const refs = [{ id: "o", sessionId: "s", rowVersion: "4", scope: "mem:obs:s" }, { id: "other", sessionId: "other", rowVersion: "2" }];
    const getVersioned = vi.fn(async (scope: string, id: string) => scope === KV.sessions
      ? { exists: true, version: "2", value: { id, project: "P" } }
      : { exists: true, version: "3", value: { sessionId: id, project: "P", title: "Release", narrative: "Grounded decisions" } });
    const summaries = await selectConsolidationSummaries({ ...h.kv, getVersioned } as StateKV, "P", ["s"]);
    expect(summaries).toEqual([{ sourceKind: "summary", id: "s", sessionId: "s", scope: KV.summaries, project: "P",
      rowVersion: "3", sessionVersion: "2" }]);
    expect(await selectProceduralObservations(h.kv, "P", summaries, refs)).toEqual([{ ...refs[0], sourceKind: "observation", project: "P" }]);
    expect(h.get).not.toHaveBeenCalled();
    expect(getVersioned.mock.calls.map(([scope, id]) => [scope, id])).toEqual([[KV.sessions, "s"], [KV.summaries, "s"]]);
  });
});

describe("durable consolidation entry-point wiring", () => {
  async function entryHarness(metadata: Record<string, string>) {
    const handlers = new Map<string, (data: unknown) => Promise<unknown>>();
    const sdk = { registerFunction: (id: string, handler: (data: unknown) => Promise<unknown>) => handlers.set(id, handler) };
    const work = { id: "fwbwork_durable", task: "consolidation", metadata };
    const kv = { get: vi.fn(async () => work) } as unknown as StateKV;
    const coordinator = { canReplace: vi.fn(async () => false), enqueue: vi.fn() };
    entry.registerFireworksBatchReplacement(sdk as never, kv, coordinator as never);
    return { entry, kv, coordinator, call: (data: unknown) => handlers.get("mem::fireworks-batch-replace")!(data) };
  }
  it("routes durable replay through its request helper before replacement eligibility without widening other branches", async () => {
    durableReplace.mockReset().mockResolvedValue({ success: true, completed: true, replayed: true, manifestId: "root", partitionId: "request" });
    const h = await entryHarness({ tier: "durable", consolidationManifestId: "root", consolidationPartitionId: "request" });
    expect(await h.call({ workItemIds: ["fwbwork_durable"] })).toMatchObject({ success: true, results: [{ workItemId: "fwbwork_durable",
      result: { success: true, completed: true, replayed: true } }] });
    expect(durableReplace).toHaveBeenCalledExactlyOnceWith(h.kv, h.coordinator, expect.any(Function),
      { manifestId: "root", partitionId: "request", workItemId: "fwbwork_durable" });
    expect(h.coordinator.canReplace).not.toHaveBeenCalled();
    expect(h.coordinator.enqueue).not.toHaveBeenCalled();
  });
  it("rejects missing durable identities before invoking request recovery", async () => {
    durableReplace.mockReset();
    const h = await entryHarness({ tier: "durable", consolidationManifestId: "root" });
    expect(await h.call({ workItemIds: ["fwbwork_durable"] })).toMatchObject({ results: [{ result: {
      success: false, error: "Durable consolidation request identity is invalid" } }] });
    expect(durableReplace).not.toHaveBeenCalled();
    expect(h.coordinator.enqueue).not.toHaveBeenCalled();
  });
  it("preserves durable manifest and partition IDs on provider-return callbacks", async () => {
    const h = await entryHarness({ tier: "durable", consolidationManifestId: "root", consolidationPartitionId: "request" });
    const trigger = vi.fn(async () => ({ success: true, queued: true, completed: false }));
    const handler = h.entry.createFireworksBatchCompletionHandler({ trigger } as never);
    const work = { id: "fwbwork_durable", task: "consolidation", metadata: {
      tier: "durable", sourceFingerprint: "request", consolidationManifestId: "root", consolidationPartitionId: "request" } };
    await handler(work as never, "captured response");
    expect(trigger).toHaveBeenCalledExactlyOnceWith({ function_id: "mem::consolidate-pipeline", payload: {
      tier: "durable", batchEffectKey: expect.stringMatching(/^[a-f0-9]{64}$/), force: true, batchResponse: "captured response",
      batchSourceFingerprint: "request", consolidationManifestId: "root", consolidationPartitionId: "request" } });
  });
});
