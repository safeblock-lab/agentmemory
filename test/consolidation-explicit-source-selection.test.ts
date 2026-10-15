import { describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { createProjectConsolidationSelector, fingerprintConsolidationSummaries, selectConsolidationSummaries, selectIndexedObservations,
  type ConsolidationSelector } from "../src/functions/consolidation-selection.js";
import { pageKey } from "../src/functions/consolidation-manifest.js";
import type { StateKV } from "../src/state/kv.js";
import { fingerprintId, KV } from "../src/state/schema.js";
import type { Session, SessionSummary } from "../src/types.js";

type Row = Record<string, unknown>;
type Handler = (input: Row) => Promise<Row>;
const project = "explicit-source-project";
const ids = ["requested-session-a", "requested-session-b", "requested-session-c"];

function harness() {
  const values = new Map<string, unknown>();
  const versions = new Map<string, number>();
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const put = (scope: string, id: string, value: unknown) => {
    values.set(key(scope, id), structuredClone(value));
    versions.set(key(scope, id), (versions.get(key(scope, id)) ?? 0) + 1);
  };
  const getVersioned = vi.fn(async <T>(scope: string, id: string) => {
    const rowKey = key(scope, id);
    const value = values.get(rowKey);
    return { exists: value !== undefined, value: structuredClone(value ?? null) as T | null,
      version: String(versions.get(rowKey) ?? 0) };
  });
  const kv = {
    indexedRetrieval: true,
    get: vi.fn(async <T>(scope: string, id: string) => structuredClone(values.get(key(scope, id)) ?? null) as T | null),
    getVersioned,
    set: vi.fn(async (scope: string, id: string, value: unknown) => { put(scope, id, value); return value; }),
    list: vi.fn(async () => { throw new Error("global state listing is forbidden in this test"); }),
  };
  const handlers = new Map<string, Handler>();
  const sdk = { registerFunction: (id: string, handler: Handler) => handlers.set(id, handler), registerTrigger: vi.fn(), trigger: vi.fn() };
  for (const [index, id] of ids.entries()) {
    const session: Session = { id, project, cwd: "", startedAt: "2026-10-10T00:00:00.000Z", status: "completed", observationCount: 1 };
    const summary: SessionSummary = { sessionId: id, project, createdAt: `2026-10-10T00:0${index}:00.000Z`,
      title: `Requested source ${index}`, narrative: `Current summary for exact source ${index}.`,
      keyDecisions: [], filesModified: [], concepts: ["selection"], observationCount: 1 };
    put(KV.sessions, id, session);
    put(KV.summaries, id, summary);
  }
  const selector = vi.fn<ConsolidationSelector>(async () => []);
  const canonicalLookup = async () => ({ status: "ready" as const, canonicalId: null, version: "1" });
  const canonicalWriter = async (_scope: string, _key: string, _match: unknown, id: string) => ({
    status: "applied" as const, applied: true, created: true, id, rowVersion: "1",
  });
  registerConsolidationPipelineFunction(sdk as unknown as IIIClient, kv as unknown as StateKV, {} as never,
    undefined, undefined, undefined, undefined, selector, canonicalLookup as never, undefined, canonicalWriter as never);
  return { values, versions, put, kv, sdk, selector, call: handlers.get("mem::consolidate-pipeline")! };
}

describe("explicit consolidation source selection", () => {
  it("keeps valid requested summaries even when project top-k returns none and captures their exact row versions", async () => {
    const h = harness();
    const selected = await selectConsolidationSummaries(h.kv as unknown as StateKV, project, ids, h.selector);
    expect(h.selector).not.toHaveBeenCalled();
    expect(selected.map(reference => [reference.sessionId, reference.rowVersion, reference.sessionVersion])).toEqual([
      [ids[0], "1", "1"], [ids[1], "1", "1"], [ids[2], "1", "1"],
    ]);

    const input = { tier: "semantic", force: true, project, batchSourceIds: ids,
      selected: { project, sessionId: ids[0] } };
    const started = await h.call(input);
    if (started.success === false) throw new Error(JSON.stringify(started));
    expect(started.success).not.toBe(false);
    expect(started.error).not.toBe("selected_consolidation_source_identity_mismatch");
    expect(h.selector).not.toHaveBeenCalled();
    const runId = String(started.selectedRunId ?? (started.continuation as Row | undefined)?.selectedRunId ?? "");
    expect(runId).toMatch(/^conrun_/);
    const run = await h.kv.get<Row>(KV.state, `selected-consolidation-run:${runId}`);
    expect(run).toMatchObject({ batchSourceIds: ids, sessionIds: ids });
    const manifestId = String(run?.semanticManifestId ?? "");
    expect(manifestId).toMatch(/^conmanifest_/);
    const root = await h.kv.get<Row>(KV.state, pageKey(manifestId, "root", 0));
    expect(root?.refs).toEqual(ids.map((id, index) => expect.objectContaining({ id, sessionId: id, project, kind: "summary", rowVersion: "1" })));

    const replay = await h.call(input);
    expect(replay.selectedRunId).toBe(runId);
    expect(h.selector).not.toHaveBeenCalled();
  });

  it("rejects duplicate, over-limit, foreign, missing, and changed sources before trusting a broad selection", async () => {
    const h = harness();
    const invoke = (batchSourceIds: unknown) => h.call({ tier: "semantic", force: true, project, batchSourceIds,
      selected: { project, sessionId: ids[0] } });
    const readsBeforeInvalidIds = h.kv.getVersioned.mock.calls.length;
    expect(await invoke([ids[0], ids[0]])).toMatchObject({ success: false, error: "selected_consolidation_source_identity_mismatch" });
    expect(await invoke(Array.from({ length: 13 }, (_, index) => `over-limit-${index}`)))
      .toMatchObject({ success: false, error: "selected_consolidation_source_identity_mismatch" });
    expect(await invoke(["x".repeat(257)])).toMatchObject({ success: false, error: "selected_consolidation_source_identity_mismatch" });
    expect(h.kv.getVersioned).toHaveBeenCalledTimes(readsBeforeInvalidIds);
    expect(h.selector).not.toHaveBeenCalled();

    h.put(KV.sessions, ids[0], { id: ids[0], project: "foreign-project" });
    await expect(selectConsolidationSummaries(h.kv as unknown as StateKV, project, [ids[0]]))
      .rejects.toThrow("selected_consolidation_source_identity_mismatch");
    await expect(selectConsolidationSummaries(h.kv as unknown as StateKV, project, ["missing-session"]))
      .rejects.toThrow("selected_consolidation_source_identity_mismatch");

    h.put(KV.sessions, ids[0], { id: ids[0], project });
    const references = await selectConsolidationSummaries(h.kv as unknown as StateKV, project, [ids[0]]);
    h.put(KV.summaries, ids[0], { ...(await h.kv.get<SessionSummary>(KV.summaries, ids[0]))!, narrative: "Changed after selection" });
    await expect(fingerprintConsolidationSummaries(h.kv as unknown as StateKV, project, references))
      .rejects.toThrow("selected_consolidation_summary_source_changed");
  });

  it("preserves implicit selection and stable source and run identities on replay", async () => {
    const h = harness();
    h.selector.mockResolvedValue([{ id: "selected-observation", sessionId: ids[0], rowVersion: "1" }]);
    const implicit = await selectConsolidationSummaries(h.kv as unknown as StateKV, project, undefined,
      async () => [{ id: "observation", sessionId: ids[0], rowVersion: "1" }]);
    expect(implicit).toEqual([expect.objectContaining({ sourceKind: "summary", id: ids[0], project })]);
    expect(implicit[0]?.rowVersion).toBeUndefined();

    const selected = { project, sessionId: ids[0] };
    const first = await h.call({ tier: "semantic", force: true, project, selected });
    if (first.success === false) throw new Error(JSON.stringify(first));
    const runId = String(first.selectedRunId ?? (first.continuation as Row | undefined)?.selectedRunId ?? "");
    expect(runId).toMatch(/^conrun_/);
    const replay = await h.call({ tier: "semantic", force: true, project, selectedRunId: runId, selected });
    expect(replay.selectedRunId).toBe(runId);
    expect(h.selector).toHaveBeenCalledTimes(1);
    const run = await h.kv.get<Row>(KV.state, `selected-consolidation-run:${runId}`);
    expect(fingerprintId("conrun", JSON.stringify(["semantic", project, run?.selectionId]))).toBe(runId);
  });

  it("scopes all-tier procedural extraction to explicit sessions and pins current observation versions", async () => {
    const h = harness();
    const requested = ids.slice(0, 2);
    const observationIds = requested.map((sessionId, index) => `current-observation-${index}`);
    requested.forEach((sessionId, index) => h.put(KV.observations(sessionId), observationIds[index]!, {
      id: observationIds[index], sessionId, project, title: `Current procedure source ${index}`,
      narrative: "A versioned observation selected for the exact session.", timestamp: "2026-10-10T00:00:00.000Z",
      importance: 8, concepts: ["procedure"], facts: [], type: "workflow",
    }));
    h.selector.mockImplementation(async (_project, sessionIds) => sessionIds?.map((sessionId, index) => ({
      id: observationIds[requested.indexOf(sessionId)] ?? `unexpected-${index}`, sessionId, rowVersion: "1", scope: KV.observations(sessionId),
    })) ?? []);
    const selected = { project, sessionId: requested[0]! };
    const started = await h.call({ tier: "all", force: true, project, batchSourceIds: requested, selected });
    const semantic = (started.results as Row).semantic as Row;
    expect(semantic.manifestId).toMatch(/^conmanifest_/);
    const semanticRootKey = pageKey(String(semantic.manifestId), "root", 0);
    const semanticRoot = await h.kv.get<Row>(KV.state, semanticRootKey);
    h.put(KV.state, semanticRootKey, { ...semanticRoot, phase: "completed", result: { completed: true, sourceCount: requested.length } });

    const atProcedural = await h.call({ ...(started.continuation as Row), selected });
    expect(atProcedural).toMatchObject({ success: true, completed: false, stage: "procedural" });
    const procedural = await h.call({ ...(atProcedural.continuation as Row), selected });
    expect(procedural).toMatchObject({ success: true, completed: false, stage: "procedural" });
    expect(h.selector).toHaveBeenCalledWith(project, requested);
    const run = await h.kv.get<Row>(KV.state, `selected-consolidation-run:${String(started.selectedRunId)}`);
    expect(run?.proceduralSources).toEqual(requested.map((sessionId, index) => expect.objectContaining({
      id: observationIds[index], sessionId, rowVersion: "1", scope: KV.observations(sessionId), sourceKind: "observation",
    })));
    const proceduralRoot = await h.kv.get<Row>(KV.state, pageKey(String(run?.proceduralManifestId), "root", 0));
    expect(proceduralRoot?.refs).toEqual(requested.map((sessionId, index) => expect.objectContaining({
      id: observationIds[index], sessionId, rowVersion: "1", scope: KV.observations(sessionId), kind: "observation",
    })));
  });

  it("keeps valid explicit summaries when a session has no procedural observation hit", async () => {
    const h = harness();
    const requested = [ids[0]!];
    h.selector.mockResolvedValue([]);
    const selected = { project, sessionId: requested[0]! };
    const started = await h.call({ tier: "all", force: true, project, batchSourceIds: requested, selected });
    const semantic = (started.results as Row).semantic as Row;
    const semanticRootKey = pageKey(String(semantic.manifestId), "root", 0);
    h.put(KV.state, semanticRootKey, { ...(await h.kv.get<Row>(KV.state, semanticRootKey)), phase: "completed",
      result: { completed: true, sourceCount: 1 } });
    const atProcedural = await h.call({ ...(started.continuation as Row), selected });
    const procedural = await h.call({ ...(atProcedural.continuation as Row), selected });
    expect(procedural).toMatchObject({ success: true, completed: false, stage: "reflect", results: {
      procedural: { completed: true, skipped: true, sourceCount: 0 },
    } });
    expect(h.selector).toHaveBeenCalledWith(project, requested);
    const run = await h.kv.get<Row>(KV.state, `selected-consolidation-run:${String(started.selectedRunId)}`);
    expect(run?.proceduralSources).toEqual([]);
  });

  it("uses exact session-scoped observation selection for explicit procedural tiers", async () => {
    const h = harness();
    const requested = [ids[0]!];
    const observationId = "procedural-only-observation";
    h.put(KV.observations(requested[0]!), observationId, { id: observationId, sessionId: requested[0], project,
      title: "Current procedure source", narrative: "Indexed observation.", timestamp: "2026-10-10T00:00:00.000Z",
      importance: 8, concepts: ["procedure"], facts: [], type: "workflow" });
    h.selector.mockImplementation(async (_project, sessionIds) => sessionIds?.length
      ? [{ id: observationId, sessionId: requested[0]!, rowVersion: "1", scope: KV.observations(requested[0]!) }] : []);
    const selected = { project, sessionId: requested[0]! };
    const result = await h.call({ tier: "procedural", force: true, project, batchSourceIds: requested, selected });
    expect(result).toMatchObject({ success: true, completed: true, results: {
      procedural: { completed: true, skipped: true, sourceCount: 1 },
    } });
    expect(h.selector).toHaveBeenCalledWith(project, requested);
    const run = await h.kv.get<Row>(KV.state, `selected-consolidation-run:${String(result.selectedRunId)}`);
    expect(run?.proceduralSources).toEqual([expect.objectContaining({ id: observationId, sessionId: requested[0], rowVersion: "1" })]);
  });

  it("routes ordinary explicit procedural calls through exact-session selection", async () => {
    const h = harness();
    const requested = [ids[0]!];
    h.selector.mockResolvedValue([{ id: "ordinary-scoped-observation", sessionId: requested[0], rowVersion: "1",
      scope: KV.observations(requested[0]) }]);
    const result = await h.call({ tier: "procedural", force: true, project, batchSourceIds: requested });
    expect(result).toMatchObject({ success: true, completed: true, results: {
      procedural: { completed: true, skipped: true, sourceCount: 1 },
    } });
    expect(h.selector).toHaveBeenCalledWith(project, requested);
  });
});

function lexicalHarness() {
  const requests: Row[] = [];
  const model = "fixture-embedding";
  const dimensions = 4;
  const marker = { status: "ready", model, dimensions, generation: "local-v1" };
  const kv = {
    get: vi.fn(async (scope: string, id: string) => scope === KV.config && id === "indexed-corpus" ? marker : null),
    retrieval: vi.fn(async (request: Row) => {
      requests.push(request);
      if (request.action === "index_status") return { version: 1, metadata_only: true, index_id: "observations",
        capabilities: ["state::semantic_lsh_v1"], semantic: [
        { index_id: "observations", model, dimensions, generation: "local-v1", status: "ready", lexical_ready: true, source_kind: "agentmemory" },
      ] };
      if (request.action === "project_vectors_prepare") return { status: "ready", index_id: "observations", project,
        source_kind: "observation", has_more: false, coverage_epoch: "1", project_generation: "1", cursor: "", processed: 0,
        session_visits: 0, text_bytes: 0, reason: null };
      return { status: "ready", model, dimensions, generation: "local-v1", items: [], approximate: false, budget_exhausted: false };
    }),
  };
  const provider = { name: model, dimensions, embed: vi.fn(async () => [1, 0, 0, 0]) };
  return { requests, kv, provider };
}

function sessionSelectorHarness() {
  const requests: Row[] = [];
  const model = "fixture-embedding";
  const dimensions = 4;
  const marker = { status: "ready", model, dimensions, generation: "local-v1" };
  const kv = {
    get: vi.fn(async (scope: string, id: string) => scope === KV.config && id === "indexed-corpus" ? marker
      : scope === KV.sessions ? { id, project } : null),
    retrieval: vi.fn(async (request: Row) => {
      requests.push(request);
      if (request.action === "index_status") return { version: 1, metadata_only: true, index_id: "observations",
        capabilities: ["state::semantic_lsh_v1"], semantic: [
          { index_id: "observations", model, dimensions, generation: "local-v1", status: "ready", lexical_ready: true, source_kind: "agentmemory" },
        ] };
      if (request.action === "project_vectors_prepare") return { status: "ready", index_id: "observations", project,
        source_kind: "observation", has_more: false, coverage_epoch: "1", project_generation: "1", cursor: "", processed: 0,
        session_visits: 0, text_bytes: 0, reason: null };
      const sessionId = typeof request.session_id === "string" ? request.session_id : "broad-top-k-session";
      const obsId = typeof request.session_id === "string" ? `observation-${sessionId}` : "broad-top-k-observation";
      return { status: "ready", model, dimensions, generation: "local-v1", items: [{ obsId, sessionId, score: 0.9,
        text: `Indexed observation for ${sessionId}`, source_version: "4", source_scope: KV.observations(sessionId) }],
        approximate: false, budget_exhausted: false };
    }),
  };
  const provider = { name: model, dimensions, embed: vi.fn(async () => [1, 0, 0, 0]) };
  return { requests, kv, provider };
}

describe("bounded lexical consolidation retrieval", () => {
  it("bounds 34 lexical terms while preserving the full semantic query", async () => {
    const h = lexicalHarness();
    const query = Array.from({ length: 34 }, (_, index) => `term${String(index).padStart(2, "0")}`).join(" ");
    await selectIndexedObservations(h.kv as never, h.provider as never, project, query);
    const lexical = h.requests.find(request => request.action === "keyword_search");
    expect(lexical).toBeDefined();
    const terms = String(lexical?.query).split(" ");
    expect(terms).toHaveLength(32);
    expect(new Set(terms).size).toBe(32);
    expect(Buffer.byteLength(String(lexical?.query))).toBeLessThanOrEqual(256);
    expect(h.provider.embed).toHaveBeenCalledWith(query);
  });

  it("deduplicates lexical terms and skips empty or overlong Unicode lexical queries", async () => {
    const duplicate = lexicalHarness();
    const repeated = "Alpha alpha BETA beta";
    await selectIndexedObservations(duplicate.kv as never, duplicate.provider as never, project, repeated);
    expect(duplicate.requests.find(request => request.action === "keyword_search")?.query).toBe("alpha beta");
    expect(duplicate.provider.embed).toHaveBeenCalledWith(repeated);

    for (const query of ["!!!", "ñ".repeat(129)]) {
      const h = lexicalHarness();
      await selectIndexedObservations(h.kv as never, h.provider as never, project, query);
      expect(h.requests.some(request => request.action === "keyword_search")).toBe(false);
      expect(h.requests.some(request => request.action === "semantic_search")).toBe(true);
      expect(h.provider.embed).toHaveBeenCalledWith(query);
    }
  });

  it("queries exact procedural sessions ahead of broad project top-k with one embedding and twelve total refs", async () => {
    const h = sessionSelectorHarness();
    const selector = createProjectConsolidationSelector(h.kv as never, h.provider as never);
    const broad = await selector(project);
    expect(broad.map(reference => reference.sessionId)).toEqual(["broad-top-k-session"]);
    h.requests.length = 0;
    h.provider.embed.mockClear();

    const requested = [...ids, ...Array.from({ length: 9 }, (_, index) => `requested-session-${index}`)];
    const scoped = await selector(project, requested);
    expect(scoped.map(reference => reference.sessionId)).toEqual(requested);
    expect(scoped).toHaveLength(12);
    expect(h.provider.embed).toHaveBeenCalledTimes(1);
    expect(h.provider.embed).toHaveBeenCalledWith(expect.stringContaining(project));
    const searches = h.requests.filter(request => request.action === "keyword_search" || request.action === "semantic_search");
    expect(new Set(searches.map(request => request.session_id))).toEqual(new Set(requested));
    expect(searches.every(request => request.source_scope === KV.observations(String(request.session_id)))).toBe(true);
  });
});
