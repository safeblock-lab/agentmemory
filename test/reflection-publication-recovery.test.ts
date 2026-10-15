import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query: string, results: unknown[]) => results) }));
vi.mock("../src/providers/_fetch.js", () => ({ fetchWithTimeout: vi.fn() }));
vi.mock("../src/providers/provider-attempt-budget.js", () => ({
  withProviderAttemptBudget: vi.fn(async (_binding: unknown, run: () => Promise<unknown>) => run()),
}));
vi.mock("../src/providers/_llm-logging.js", () => ({
  extractLlmTokenUsage: vi.fn(), startLlmCallTelemetry: vi.fn(() => ({ failure: vi.fn(), success: vi.fn() })),
}));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { REFLECT_CONTROL, REFLECT_JOBS, type ReflectJob } from "../src/functions/reflect-queue-types.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { evaluatePublicationEvidence, observePublicationEvidence, type PublicationEvidenceInput } from "../src/functions/publication-evidence.js";
import { OpenAIProvider, OpenAIHttpError } from "../src/providers/openai.js";
import { fetchWithTimeout } from "../src/providers/_fetch.js";
import { withProviderAttemptBudget } from "../src/providers/provider-attempt-budget.js";
import { batchEffectKey } from "../src/state/batch-effects.js";

const generated = '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
const supported = JSON.stringify({ verdict: "supported", reason: "Supported." });
async function pending(large = false) {
  const h = reflectHarness();
  if (large) {
    for (const id of ["one", "two"]) {
      const source = await h.kv.get<Record<string, unknown>>(KV.memories, id);
      await h.kv.set(KV.memories, id, { ...source, content: `Boundary validation ${id}. ${"Repeated input checks. ".repeat(650)}` });
    }
  }
  let jobId = "";
  h.provider.summarize.mockImplementation(async (system: string) => {
    if (!system.includes("You evaluate publication evidence")) return generated;
    const dispatched = await h.kv.get<ReflectJob>(REFLECT_JOBS, jobId);
    expect(dispatched?.captured?.publication?.checkpoint?.state).toBe("intended");
    expect(dispatched?.captured?.evidenceRequestId).toBeUndefined();
    throw Object.assign(new Error("private provider body token"), { status: 503 });
  });
  jobId = (await h.call("mem::reflect", { project: "/app", query: "boundaries", requestId: "original-request" })).jobId;
  await h.call("mem::reflect-unit", { jobId });
  const job = await h.kv.get<ReflectJob>(REFLECT_JOBS, jobId);
  if (!job?.manifest || !job.captured?.publication) throw new Error("fixture_capture_missing");
  const control = { project: "/app", requestId: "original-request", manifestId: job.manifest.id,
    unitSequence: job.unitSequence ?? 0, effectKey: job.captured.effectKey };
  const evidenceId = job.captured.publication.requestId;
  h.provider.summarize.mockClear();
  h.provider.summarize.mockRejectedValue(new Error("replay_must_not_execute"));
  return { h, job, jobId, control, evidenceId };
}
async function retainVerdict(f: Awaited<ReturnType<typeof pending>>, state: "captured" | "decided" = "captured") {
  const record = await f.h.kv.get<Record<string, unknown>>(KV.state, f.evidenceId);
  await f.h.kv.set(KV.state, f.evidenceId, { ...record, state, raw: supported });
}
beforeEach(() => vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0"));
afterEach(() => { vi.unstubAllEnvs(); vi.unstubAllGlobals(); });

describe("exact retained normal reflection publication recovery", () => {
  it("observes intended identity and sanitized diagnostics with zero writes or executors", async () => {
    const f = await pending();
    const writes = f.h.kv.set.mock.calls.length;
    const deletes = f.h.kv.delete.mock.calls.length;
    const result = await f.h.call("mem::reflect-status", { jobId: f.jobId, publication: f.control });
    expect(result.publication).toMatchObject({ requestId: f.evidenceId, state: "intended", hasRaw: false,
      diagnostic: { category: "http_error", status: 503 } });
    expect(JSON.stringify(result)).not.toMatch(/private provider body token|systemPrompt|userPrompt|generationPrompt/);
    expect(f.h.kv.set.mock.calls).toHaveLength(writes);
    expect(f.h.kv.delete.mock.calls).toHaveLength(deletes);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(f.h.kv.list).not.toHaveBeenCalled();
    expect(f.h.kv.pages).not.toHaveBeenCalled();
  });

  it("keeps retained intended unknown with no judge or generation dispatch", async () => {
    const f = await pending();
    const result = await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    const retained = await f.h.kv.get<ReflectJob>(REFLECT_JOBS, f.jobId);
    expect(result).toMatchObject({ status: "blocked", reason: "reflect_publication_pending" });
    expect(retained?.captured).toMatchObject({ response: generated, effectKey: f.control.effectKey,
      publication: { requestId: f.evidenceId, status: "pending", reason: "dispatch_outcome_unknown" } });
    expect(retained?.captured?.evidenceRequestId).toBeUndefined();
    expect(retained?.unitSequence).toBe(f.control.unitSequence);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(await f.h.kv.list(KV.insights)).toHaveLength(0);
  });

  it("observes and retains effective v2 intention before v3 without payload exposure", async () => {
    const f = await pending();
    const current = await f.h.kv.get<Record<string, unknown>>(KV.state, f.evidenceId);
    const request = current?.request as { systemPrompt: string; userPrompt: string };
    const payload = JSON.parse(request.userPrompt);
    const oldCriteria = "Every proposed insight must be supported by the full original sources and the exact source fragments and lineage supplied to generation. Evaluate each factual, causal and benefit claim; shared concepts alone do not support a causal relationship. If the candidate contains multiple insights, each insight must be supported for the candidate to be supported.";
    const userPrompt = JSON.stringify({ ...payload, version: "publication-evidence-v2", criteria: oldCriteria });
    const requestId = fingerprintId("publication-evidence", userPrompt);
    const historical = { requestId, systemPrompt: request.systemPrompt.replace(payload.criteria, oldCriteria), userPrompt };
    await f.h.kv.set(KV.state, requestId, { ...current, version: "publication-evidence-v2", requestId,
      request: historical, identity: JSON.stringify(historical), state: "intended" });
    const observed = await f.h.call("mem::reflect-status", { jobId: f.jobId, publication: f.control });
    expect(observed.publication).toMatchObject({ requestId, version: "publication-evidence-v2", state: "intended" });
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect((await f.h.kv.get<ReflectJob>(REFLECT_JOBS, f.jobId))?.captured?.publication?.requestId).toBe(requestId);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });

  it.each(["captured", "decided"] as const)("links exact supported %s checkpoint without executor and preserves effect", async state => {
    const f = await pending();
    await retainVerdict(f, state);
    const result = await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    const receipt = await f.h.kv.get<Record<string, unknown>>(REFLECT_CONTROL, `${f.jobId}:result:0`);
    expect(result).toMatchObject({ status: "completed" });
    expect(receipt).toMatchObject({ evidenceRequestId: f.evidenceId, effectKey: f.control.effectKey, appliedCount: 1 });
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(await f.h.kv.list(KV.insights)).toHaveLength(1);
  });

  it("observes captured support without persisting a decision", async () => {
    const f = await pending();
    await retainVerdict(f);
    const writes = f.h.kv.set.mock.calls.length;
    const observed = await f.h.call("mem::reflect-status", { jobId: f.jobId, publication: f.control });
    expect(observed.publication).toMatchObject({ state: "captured", decisionStatus: "supported", hasRaw: true });
    expect(f.h.kv.set.mock.calls).toHaveLength(writes);
    expect(await f.h.kv.get(KV.state, f.evidenceId)).toMatchObject({ state: "captured" });
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });

  it("stops after exact retained unit when more source fragments remain", async () => {
    const f = await pending(true);
    expect(f.job.manifest?.nextCursor).toBeTruthy();
    await retainVerdict(f);
    const result = await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect(result).toMatchObject({ status: "blocked", reason: "reflect_replay_unit_complete" });
    const retained = await f.h.kv.get<ReflectJob>(REFLECT_JOBS, f.jobId);
    expect(retained).toMatchObject({ unitSequence: 1, processedClusters: 1, dispatchPending: false });
    expect(retained?.manifest).toBeUndefined();
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });

  it("rejects stale manifest before effect admission and preserves retained capture", async () => {
    const f = await pending();
    await retainVerdict(f);
    const source = await f.h.kv.get<Record<string, unknown>>(KV.memories, "one");
    await f.h.kv.set(KV.memories, "one", { ...source, content: "Changed", version: "2" });
    const result = await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect(result).toMatchObject({ status: "failed", reason: "reflect_source_changed" });
    expect(await f.h.kv.get(KV.batchCallbacks, `reflect:${f.control.effectKey}`)).toBeNull();
    expect((await f.h.kv.get<ReflectJob>(REFLECT_JOBS, f.jobId))?.captured?.response).toBe(generated);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });

  it("reconciles lost result ACK without duplicate insights or reinforcement", async () => {
    const f = await pending();
    await retainVerdict(f);
    const originalSet = f.h.kv.set.getMockImplementation()!;
    let lost = false;
    f.h.kv.set.mockImplementation(async (scope, key, value) => {
      const result = await originalSet(scope, key, value);
      if (!lost && scope === REFLECT_CONTROL && key === `${f.jobId}:result:0`) { lost = true; throw new Error("lost_ack"); }
      return result;
    });
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect(await f.h.kv.get(REFLECT_JOBS, f.jobId)).toMatchObject({ status: "blocked", dispatchPending: false });
    const insights = await f.h.kv.list(KV.insights);
    const source = await f.h.kv.get(KV.memories, "one");
    const result = await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect(result).toMatchObject({ recovered: true, status: "completed" });
    expect(await f.h.kv.list(KV.insights)).toEqual(insights);
    expect(await f.h.kv.get(KV.memories, "one")).toEqual(source);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });

  it.each(["project", "requestId", "manifestId", "unitSequence", "effectKey", "extra"])("fails closed for mismatched %s", async field => {
    const f = await pending();
    const writes = f.h.kv.set.mock.calls.length;
    const control = { ...f.control, [field]: field === "unitSequence" ? 1 : "different" };
    expect(await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: control })).toMatchObject({ success: false });
    expect(f.h.kv.set.mock.calls).toHaveLength(writes);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });

  it("observes missing checkpoint without creating intention or dispatching during replay", async () => {
    const f = await pending();
    await f.h.kv.delete(KV.state, f.evidenceId);
    const writes = f.h.kv.set.mock.calls.length;
    expect(await f.h.call("mem::reflect-status", { jobId: f.jobId, publication: f.control }))
      .toMatchObject({ publication: { status: "unavailable", reason: "checkpoint_not_found" } });
    expect(f.h.kv.set.mock.calls).toHaveLength(writes);
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect(await f.h.kv.get(KV.state, f.evidenceId)).toBeNull();
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
  });
});

describe("shared publication and HTTP boundaries", () => {
  it("never dispatches when durable reference persistence fails", async () => {
    const h = reflectHarness();
    const input: PublicationEvidenceInput = { workflow: "reflection", operationId: "exact-operation", project: "/app", candidate: "Supported claim",
      sources: [{ id: "source", scope: KV.memories, key: "source", version: "1", project: "/app", content: "Supported claim" }], evidenceComplete: true };
    const execute = vi.fn(async () => supported);
    await expect(evaluatePublicationEvidence(h.kv as never, input, execute, async () => { throw new Error("reference_write_failed"); }))
      .rejects.toThrow("reference_write_failed");
    expect(execute).not.toHaveBeenCalled();
    expect(await evaluatePublicationEvidence(h.kv as never, input, execute)).toMatchObject({ status: "pending" });
    expect(execute).not.toHaveBeenCalled();
  });

  it.each(["summary", "skill", "consolidation"] as const)("preserves %s retained intention with no executor replay", async workflow => {
    const h = reflectHarness();
    const input: PublicationEvidenceInput = { workflow, operationId: "exact-operation", project: "/app", candidate: "Supported claim",
      sources: [{ id: "source", scope: KV.memories, key: "source", version: "1", project: "/app", content: "Supported claim" }], evidenceComplete: true };
    const execute = vi.fn(async () => { throw Object.assign(new Error("private-body"), { status: 429 }); });
    const first = await evaluatePublicationEvidence(h.kv as never, input, execute);
    expect(await evaluatePublicationEvidence(h.kv as never, input, execute)).toMatchObject({ status: "pending", requestId: first.requestId });
    expect(await observePublicationEvidence(h.kv as never, input)).toMatchObject({ diagnostic: { category: "http_error", status: 429 } });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("preserves typed HTTP status without exposing provider response or credentials", async () => {
    vi.mocked(fetchWithTimeout).mockResolvedValue(new Response("secret provider body", { status: 400 }));
    const provider = new OpenAIProvider("private-key", "mock-model", 100, "https://provider.invalid");
    const error = await provider.summarize("system", "user").catch((error: unknown) => error);
    expect(error).toBeInstanceOf(OpenAIHttpError);
    expect(error).toMatchObject({ status: 400, message: "OpenAI API error (400)" });
    expect(String(error)).not.toMatch(/secret|private-key/);
    expect(fetchWithTimeout).toHaveBeenCalledTimes(1);
  });
});

describe("exact fresh judge-only disposition", () => {
  async function disposition() {
    const f = await pending();
    const original = await f.h.kv.get<Record<string, unknown>>(KV.state, f.evidenceId);
    await f.h.kv.set(KV.state, f.evidenceId, { ...original, diagnostic: { category: "executor_error" } });
    const observation = await f.h.call("mem::reflect-status", { jobId: f.jobId, publication: f.control });
    const judgeDisposition = { unit: f.control, dispositionId: "fresh-judge-only", checkpoint: observation.publication,
      captureFingerprint: batchEffectKey(generated), budget: { windowId: "new-operator-window", ceiling: 1 } };
    f.h.provider.summarize.mockResolvedValue(supported);
    f.h.kv.retrieval.mockClear();
    vi.mocked(withProviderAttemptBudget).mockClear();
    return { ...f, judgeDisposition };
  }
  it.each([
    ["empty budget", { budget: {} }],
    ["missing window", { budget: { ceiling: 1 } }],
    ["nontext window", { budget: { windowId: 1, ceiling: 1 } }],
    ["zero ceiling", { budget: { windowId: "new-operator-window", ceiling: 0 } }],
    ["fractional ceiling", { budget: { windowId: "new-operator-window", ceiling: 1.5 } }],
    ["unsafe ceiling", { budget: { windowId: "new-operator-window", ceiling: Number.MAX_SAFE_INTEGER + 1 } }],
    ["extra budget field", { budget: { windowId: "new-operator-window", ceiling: 1, extra: true } }],
    ["empty checkpoint", { checkpoint: {} }],
  ] as const)("rejects malformed %s before claiming or selecting an attempt", async (_name, changed) => {
    const f = await disposition();
    const before = structuredClone(f.h.rows);
    const writes = f.h.kv.set.mock.calls.length;
    const deletes = f.h.kv.delete.mock.calls.length;
    expect(await f.h.call("mem::reflect-unit", { jobId: f.jobId,
      judgeDisposition: { ...f.judgeDisposition, ...changed } })).toEqual({ success: false, error: "invalid_reflect_judge_disposition" });
    expect(f.h.kv.set.mock.calls).toHaveLength(writes);
    expect(f.h.kv.delete.mock.calls).toHaveLength(deletes);
    expect(f.h.rows).toEqual(before);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(withProviderAttemptBudget).not.toHaveBeenCalled();
  });
  it.each([
    ["requestId", undefined], ["version", "publication-evidence-v4"], ["state", "unknown"],
    ["hasRaw", "false"], ["responseOverflow", 0], ["createdAt", "yesterday"],
    ["decisionStatus", "approved"], ["diagnostic", {}], ["diagnostic", { category: "other" }],
    ["diagnostic", { category: "executor_error", status: "503" }],
    ["diagnostic", { category: "http_error", status: 600 }],
    ["diagnostic", { category: "executor_error", extra: true }], ["extra", true],
  ] as const)("rejects malformed checkpoint %s before all writes", async (field, value) => {
    const f = await disposition();
    const before = structuredClone(f.h.rows);
    const writes = f.h.kv.set.mock.calls.length;
    expect(await f.h.call("mem::reflect-unit", { jobId: f.jobId, judgeDisposition: { ...f.judgeDisposition,
      checkpoint: { ...f.judgeDisposition.checkpoint, [field]: value } } }))
      .toEqual({ success: false, error: "invalid_reflect_judge_disposition" });
    expect(f.h.kv.set.mock.calls).toHaveLength(writes);
    expect(f.h.rows).toEqual(before);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(withProviderAttemptBudget).not.toHaveBeenCalled();
  });
  it("judges retained generation once under persisted fresh binding and publishes original effect once", async () => {
    const f = await disposition();
    const retainedOriginal = await f.h.kv.get(KV.state, f.evidenceId);
    f.h.provider.summarize.mockImplementation(async (system: string) => {
      expect(system).toContain("You evaluate publication evidence");
      const original = await f.h.kv.get<Record<string, unknown>>(KV.state, f.evidenceId);
      const link = original!.supersededBy as { requestId: string; budgetBinding: unknown };
      expect(link.requestId).not.toBe(f.evidenceId);
      expect(withProviderAttemptBudget).toHaveBeenCalledWith(link.budgetBinding, expect.any(Function));
      expect(await f.h.kv.get(KV.state, link.requestId)).toMatchObject({ state: "intended", supersedes: f.evidenceId });
      return supported;
    });
    const calls = () => f.h.call("mem::reflect-unit", { jobId: f.jobId, judgeDisposition: f.judgeDisposition });
    await Promise.all([calls(), calls()]);
    expect(f.h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(withProviderAttemptBudget).toHaveBeenCalledTimes(1);
    expect(f.h.kv.retrieval.mock.calls.some(([request]) => ["keyword_search", "semantic_search"].includes(request.action))).toBe(false);
    expect(await f.h.kv.get(REFLECT_JOBS, f.jobId)).toMatchObject({ status: "completed", unitSequence: 1, appliedInsights: 1 });
    const result = await f.h.kv.get<Record<string, unknown>>(REFLECT_CONTROL, `${f.jobId}:result:0`);
    expect(result).toMatchObject({ effectKey: f.control.effectKey, appliedCount: 1 });
    expect(result!.evidenceRequestId).not.toBe(f.evidenceId);
    expect(await f.h.kv.get(KV.state, f.evidenceId)).toMatchObject(retainedOriginal);
    const insights = await f.h.kv.list(KV.insights); expect(insights).toHaveLength(1);
    await calls();
    expect(await f.h.kv.list(KV.insights)).toEqual(insights);
    expect(f.h.provider.summarize).toHaveBeenCalledTimes(1);
  });
  it("keeps fresh unknown consumed across disposition and ordinary zero-call replay", async () => {
    const f = await disposition();
    f.h.provider.summarize.mockRejectedValue(new Error("fresh_unknown"));
    const payload = { jobId: f.jobId, judgeDisposition: f.judgeDisposition };
    expect(await f.h.call("mem::reflect-unit", payload)).toMatchObject({ status: "blocked", reason: "reflect_publication_pending" });
    await f.h.call("mem::reflect-unit", payload);
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control });
    expect(f.h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(withProviderAttemptBudget).toHaveBeenCalledTimes(1);
    expect(await f.h.kv.list(KV.insights)).toHaveLength(0);
    const job = await f.h.kv.get<ReflectJob>(REFLECT_JOBS, f.jobId);
    expect(job).toMatchObject({ unitSequence: 0, appliedInsights: 0, manifest: { id: f.control.manifestId }, captured: { response: generated, effectKey: f.control.effectKey } });
  });
  it.each(["captured", "decided"] as const)("publishes selected %s evidence by replay with no second judge", async state => {
    const f = await disposition();
    f.h.provider.summarize.mockRejectedValue(new Error("new_unknown"));
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, judgeDisposition: f.judgeDisposition });
    const original = await f.h.kv.get<{ supersededBy: { requestId: string } }>(KV.state, f.evidenceId);
    const id = original!.supersededBy.requestId;
    const selected = await f.h.kv.get<Record<string, unknown>>(KV.state, id);
    await f.h.kv.set(KV.state, id, { ...selected, state, raw: supported });
    expect(await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control })).toMatchObject({ status: "completed" });
    expect(await f.h.kv.get(REFLECT_CONTROL, `${f.jobId}:result:0`)).toMatchObject({ evidenceRequestId: id, effectKey: f.control.effectKey });
    expect(await f.h.kv.list(KV.insights)).toHaveLength(1);
    expect(f.h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(withProviderAttemptBudget).toHaveBeenCalledTimes(1);
  });
  it("does not call a provider when trusted budget admission rejects the persisted binding", async () => {
    const f = await disposition();
    vi.mocked(withProviderAttemptBudget).mockRejectedValueOnce(new Error("binding_mismatch"));
    const payload = { jobId: f.jobId, judgeDisposition: f.judgeDisposition };
    expect(await f.h.call("mem::reflect-unit", payload)).toMatchObject({ status: "blocked", reason: "reflect_publication_pending" });
    await f.h.call("mem::reflect-unit", payload);
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(withProviderAttemptBudget).toHaveBeenCalledTimes(1);
    expect(await f.h.kv.list(KV.insights)).toHaveLength(0);
  });
  it.each(["capture", "checkpoint", "source", "unit"])("rejects changed %s before a fresh judge or effect", async field => {
    const f = await disposition();
    if (field === "capture") f.judgeDisposition.captureFingerprint = "changed";
    if (field === "checkpoint") f.judgeDisposition.checkpoint.createdAt = "2020-01-01T00:00:00.000Z";
    if (field === "unit") f.judgeDisposition.unit = { ...f.control, unitSequence: 8 };
    if (field === "source") {
      const source = await f.h.kv.get<Record<string, unknown>>(KV.memories, "one");
      await f.h.kv.set(KV.memories, "one", { ...source, content: "Changed", version: "2" });
    }
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, judgeDisposition: f.judgeDisposition });
    expect(f.h.provider.summarize).not.toHaveBeenCalled();
    expect(withProviderAttemptBudget).not.toHaveBeenCalled();
    expect(await f.h.kv.get(KV.batchCallbacks, `reflect:${f.control.effectKey}`)).toBeNull();
    expect(await f.h.kv.list(KV.insights)).toHaveLength(0);
  });
  it("recovers lost publication ACK using selected decision with zero additional calls", async () => {
    const f = await disposition();
    const originalSet = f.h.kv.set.getMockImplementation()!;
    let lost = false;
    f.h.kv.set.mockImplementation(async (scope, key, value) => {
      const result = await originalSet(scope, key, value);
      if (!lost && scope === REFLECT_CONTROL && key === `${f.jobId}:result:0`) { lost = true; throw new Error("lost_ack"); }
      return result;
    });
    await f.h.call("mem::reflect-unit", { jobId: f.jobId, judgeDisposition: f.judgeDisposition });
    const insights = await f.h.kv.list(KV.insights);
    const source = await f.h.kv.get(KV.memories, "one");
    expect(await f.h.call("mem::reflect-unit", { jobId: f.jobId, replayOnly: f.control })).toMatchObject({ recovered: true, status: "completed" });
    expect(await f.h.kv.list(KV.insights)).toEqual(insights);
    expect(await f.h.kv.get(KV.memories, "one")).toEqual(source);
    expect(f.h.provider.summarize).toHaveBeenCalledTimes(1);
  });
});
