import { describe, expect, it, vi } from "vitest";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { newRoot, pageKey, type PagedConsolidation } from "../src/functions/consolidation-manifest.js";
import { fingerprintId, KV } from "../src/state/schema.js";
import { aggregateCandidateKey } from "../src/functions/consolidation-reduction.js";
import { REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";

const decay = vi.hoisted(() => vi.fn(async () => ({ completed: true })));
vi.mock("../src/functions/consolidation-decay.js", () => ({ runSelectedConsolidationDecay: decay }));
vi.mock("../src/config.js", async original => ({ ...(await original<object>()), isConsolidationEnabled: () => true }));
type Row = Record<string, unknown>;
const project = "normal-integration";
const policy = { deferred: false, force: true, compaction: false, pipelineGate: false };
function harness() {
  decay.mockClear();
  const rows = new Map<string, unknown>();
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const put = (scope: string, id: string, value: unknown) => rows.set(key(scope, id), structuredClone(value));
  const get = async <T>(scope: string, id: string) => structuredClone(rows.get(key(scope, id)) ?? null) as T | null;
  const kv = { indexedRetrieval: true, get, set: vi.fn(async (scope: string, id: string, value: unknown) => { put(scope, id, value); return value; }),
    retrieval: vi.fn(async (_request: Row): Promise<Row> => ({ status: "not_applied" })),
    getVersioned: async (scope: string, id: string) => ({ exists: rows.has(key(scope, id)), value: await get(scope, id), version: "1" }) };
  const trigger = vi.fn(async (_request: unknown): Promise<Row> => ({ success: true, completed: true, status: "completed", terminal: true }));
  const provider = { summarize: vi.fn() };
  let call!: (input: Row) => Promise<Row>;
  const sdk = { registerFunction: (_id: string, handler: typeof call) => { call = handler; }, trigger };
  const summary = { sessionId: "source", project, createdAt: "2026-10-10T00:00:00Z", narrative: "Original source", concepts: ["integration"] };
  put(KV.sessions, "source", { id: "source", project }); put(KV.summaries, "source", summary);
  const selector = vi.fn(async () => [{ id: "observation", sessionId: "source", rowVersion: "1" }]);
  const register = () => registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never,
    undefined, undefined, undefined, undefined, selector, vi.fn() as never, vi.fn() as never, vi.fn() as never);
  register();
  const input = { tier: "reflect", project, force: true, deferred: false, batchSourceIds: ["source"] };
  const completedRoot = () => {
    const refs = [{ id: "source", sessionId: "source", kind: "summary" as const, project, scope: KV.summaries, rowVersion: "1" }];
    const id = fingerprintId("conmanifest", JSON.stringify(["semantic", refs, null, policy]));
    const root = newRoot(id, "semantic", project, refs, false, policy);
    root.phase = "completed"; root.result = { completed: true, manifestId: id, newFacts: 0, appliedEffects: 0 };
    put(KV.state, pageKey(id, "root", 0), root);
    put(KV.state, `selected-consolidation:${id}`, { id, project, runId: "historical-selected-run" });
    put(KV.state, pageKey(id, "source", 0), { ...refs[0], rowVersion: "1", fingerprint: fingerprintId("consource", JSON.stringify(summary)) });
    return root;
  };
  return { get call() { return call; }, kv, trigger, put, get, register, input, completedRoot, provider, summary, rows };
}

describe("normal consolidation integration", () => {
  it.each(["partial", "failed", "blocked", "completed"])("resolves a real selected reflection jobId with terminal %s exactly once", async status => {
    const h = harness();
    const jobId = "persisted-reflection-child";
    h.put(REFLECT_JOBS, jobId, { id: jobId, status, unitSequence: 1, error: status === "completed" ? undefined : "child_incomplete" });
    h.put(REFLECT_CONTROL, `${jobId}:result:0`, { insightIds: ["insight"] });
    h.trigger.mockImplementation(async (input: unknown) => {
      const request = input as { function_id: string };
      if (request.function_id === "mem::reflect") return { success: true, jobId };
      expect(request.function_id).toBe("mem::reflect-unit");
      return { success: true, jobId, status };
    });
    const input = { ...h.input, selected: { sessionId: "source" } };
    const result = await h.call(input);
    expect(result.completed).toBe(status === "completed");
    expect(result.results).toMatchObject({ reflect: { jobId, status, completed: status === "completed", insightIds: ["insight"] } });
    if (status !== "completed") expect(result).toMatchObject({ terminal: true, queued: false, partial: true });
    expect(await h.call(input)).toMatchObject({ completed: status === "completed" });
    expect(h.trigger).toHaveBeenCalledTimes(2);
    expect(decay).not.toHaveBeenCalled();
  });
  it("replays an exact completed selected root without providers, publication, checkpoint mutation, or marker changes", async () => {
    const h = harness(); const root = h.completedRoot(); const before = structuredClone(h.rows);
    const result = await h.call({ ...h.input, tier: "semantic" });
    expect(result).toMatchObject({ success: true, completed: true, results: { semantic: {
      completed: true, replayed: true, manifestId: root.id, newFacts: 0, appliedEffects: 0 } } });
    expect(h.rows.get(`${KV.state}\0selected-consolidation:${root.id}`)).toEqual(before.get(`${KV.state}\0selected-consolidation:${root.id}`));
    expect(await h.get(KV.state, pageKey(root.id, "root", 0))).toEqual(root);
    expect(h.provider.summarize).not.toHaveBeenCalled(); expect(h.trigger).not.toHaveBeenCalled();
    expect(h.kv.set.mock.calls.filter(([scope, id]) => scope === KV.state && id !== result.consolidationSelectionId))
      .not.toContainEqual(expect.arrayContaining([KV.state, pageKey(root.id, "root", 0)]));
  });
  it.each(["inflight", "source", "policy", "project", "session-project", "result"])("refuses selected replay with %s mismatch", async mismatch => {
    const h = harness(); const root = h.completedRoot();
    if (mismatch === "inflight") root.phase = "publish";
    if (mismatch === "source") h.put(KV.summaries, "source", { ...h.summary, narrative: "Changed" });
    if (mismatch === "policy") root.callerPolicy = { ...policy, force: false };
    if (mismatch === "project") root.project = "foreign";
    if (mismatch === "session-project") h.put(KV.sessions, "source", { id: "source", project: "foreign" });
    if (mismatch === "result") root.result = { completed: true, status: "failed", success: false };
    h.put(KV.state, pageKey(root.id, "root", 0), root);
    const result = await h.call({ ...h.input, tier: "semantic" });
    expect(result.completed).toBe(false); expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(await h.get<PagedConsolidation>(KV.state, pageKey(root.id, "root", 0))).toEqual(root);
  });
  it.each(["valid", "missing", "invalid"])("requires actual canonical ACK for published candidates (%s)", async ack => {
    const h = harness(); const root = h.completedRoot();
    const candidate = { id: "candidate", key: "original fact", fact: "Original fact", confidence: 0.95, sourceSessionIds: ["source"], children: [] };
    root.finalPages = 1; root.publish.page = 1; root.counters.published = 1; root.counters.applied = 1; root.counters.newlyCreated = 1;
    root.checkpoint = { processedThrough: h.summary.createdAt, processedSessionIdsAtThrough: ["source"] };
    root.result = { completed: true, manifestId: root.id, newFacts: 1, appliedEffects: 1 };
    h.put(KV.state, pageKey(root.id, "root", 0), root);
    h.put(KV.state, pageKey(root.id, "candidates", 0), { candidates: [candidate] });
    h.put(KV.state, aggregateCandidateKey(root, candidate), { ...candidate, anchor: "0:0" });
    const canonicalId = fingerprintId("sem", candidate.key);
    h.put(KV.state, pageKey(root.id, "published", "0:0"), { id: canonicalId, applied: true, created: true });
    if (ack !== "missing") h.kv.retrieval.mockImplementation(async request => {
      expect(request).toMatchObject({ action: "canonical_apply", receipt_only: true });
      const fields = { operation_kind: "upsert", effect_key: request.effect_key, operation_fingerprint: request.operation_fingerprint,
        canonical_id: canonicalId, row_version: "2", created: true, result_fingerprint: "a".repeat(64) };
      return { status: "already_applied", applied: false, ...fields, receipt: {
        ...fields, ...(ack === "invalid" ? { operation_fingerprint: "foreign" } : {}) } };
    });
    const result = await h.call({ ...h.input, tier: "semantic" });
    expect(result.completed).toBe(ack === "valid");
    if (ack === "valid") expect(result.results).toMatchObject({ semantic: { replayed: true, newFacts: 0, appliedEffects: 0 } });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(await h.get(KV.state, pageKey(root.id, "root", 0))).toEqual(root);
  });
  it("persists original foreground request and exact continuation across pipeline registration and repeats", async () => {
    const h = harness(); const continuation = { jobId: "job", requestId: "request", project, checkpoint: "checkpoint", cut: 1 };
    h.trigger.mockResolvedValueOnce({ success: true, completed: false, status: "pending", terminal: false, continuation });
    const first = await h.call(h.input);
    expect(first).toMatchObject({ success: true, completed: false, queued: true, continuation: { tier: "reflect", project } });
    const initial = (h.trigger.mock.calls[0][0] as { payload: Row }).payload;
    expect(initial).toMatchObject({ manual: true, project, sessionId: "source", summaryId: "source" });
    expect(initial.requestId).toBeTypeOf("string");
    h.register();
    const second = await h.call(first.continuation as Row);
    expect((h.trigger.mock.calls[1][0] as { payload: Row }).payload).toEqual({ ...initial, continuation });
    expect(second).toMatchObject({ completed: true });
    expect(await h.call(h.input)).toMatchObject({ completed: true });
    expect(h.trigger).toHaveBeenCalledTimes(2);
  });
  it.each([
    { success: true, completed: false, queued: false, status: "pending" },
    { success: true, completed: false, terminal: true, status: "failed" },
    { success: true, completed: true, terminal: true, status: "partial" },
    { success: false, completed: true, terminal: true, error: "failed" },
  ])("never advances all after incomplete or failed reflection: %j", async response => {
    const h = harness(); h.trigger.mockResolvedValue(response);
    // Skip semantic via a durable exact completed root; one observation skips procedural.
    h.completedRoot();
    const result = await h.call({ ...h.input, tier: "all" });
    expect(result.completed).toBe(false); expect(decay).not.toHaveBeenCalled();
    const second = await h.call({ ...h.input, tier: "all" });
    expect(second.completed).toBe(false); expect(h.trigger).toHaveBeenCalledTimes(1);
  });
  it.each([{ force: false }, { deferred: true }, { consolidationCatalogProject: true }])("preserves unattended reflection for %j", async override => {
    const h = harness(); h.trigger.mockResolvedValue({ success: true, queued: true, status: "pending" });
    const result = await h.call({ ...h.input, ...override });
    expect(result.completed).toBe(false);
    expect((h.trigger.mock.calls[0][0] as { payload: Row }).payload).not.toHaveProperty("manual");
  });
  it("binds foreground reflection to original source content and caller policy", async () => {
    const h = harness(); await h.call(h.input);
    const first = (h.trigger.mock.calls[0][0] as { payload: Row }).payload;
    h.put(KV.summaries, "source", { ...h.summary, narrative: "New current summary" });
    await h.call(h.input);
    const second = (h.trigger.mock.calls[1][0] as { payload: Row }).payload;
    expect(second.requestId).not.toEqual(first.requestId);
    expect(second).not.toHaveProperty("continuation");
  });
});
