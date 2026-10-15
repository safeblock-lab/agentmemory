import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn(async () => undefined) }));
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query: string, results: unknown[]) => results) }));
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";
import { KV } from "../src/state/schema.js";
import { reflectHarness } from "./helpers/reflect-harness.js";

function decisionProvider(choice: "run" | "skip" = "run", confidence = 0.99) {
  return { evaluateChoice: vi.fn(async () => ({ choice, confidence })) };
}

function configureSupportedReflectionResponses(h: ReturnType<typeof reflectHarness>, expectedCandidate?: string) {
  const generatedResponse = '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
  const supportedCandidate = expectedCandidate ?? generatedResponse;
  h.provider.summarize.mockImplementation(async (systemPrompt: string, userPrompt: string) => {
    if (!systemPrompt.includes("You evaluate publication evidence")) return generatedResponse;
    const payload = JSON.parse(userPrompt);
    const candidate = JSON.parse(payload.candidate);
    if (candidate.generatedResponse !== supportedCandidate || !payload.sources.length) {
      throw new Error("publication_evidence_candidate_missing");
    }
    return JSON.stringify({ verdict: "supported", reason: "The complete evidence was evaluated." });
  });
}

function register(h: ReturnType<typeof reflectHarness>, typeSafe: ReturnType<typeof decisionProvider>, batchQueue?: { enqueue: ReturnType<typeof vi.fn> }) {
  registerReflectFunctions(h.sdk as never, h.kv as never, h.provider as never, undefined, batchQueue as never,
    typeSafe as never, h.embedding as never);
}

async function prepareLowSignalGroup(h: ReturnType<typeof reflectHarness>) {
  for (const id of ["one", "two"]) {
    const memory = await h.kv.get(KV.memories, id);
    await h.kv.set(KV.memories, id, { ...memory, strength: 0.5 });
  }
  const accepted = await h.call("mem::reflect", { project: "/app", query: "boundaries", deferred: true, maxClusters: 1 });
  return accepted.jobId as string;
}

describe("reflection queue TypeSafe compatibility", () => {
  beforeEach(() => {
    vi.stubEnv("AGENTMEMORY_TYPESAFE_ENABLED", "true");
    vi.stubEnv("AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED", "true");
    vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
  });
  afterEach(() => vi.unstubAllEnvs());

  it("runs deferred reflection and publication with TypeSafe gate settings enabled", async () => {
    const h = reflectHarness();
    const typeSafe = decisionProvider("skip");
    configureSupportedReflectionResponses(h);
    register(h, typeSafe);
    const jobId = await prepareLowSignalGroup(h);

    const result = await h.call("mem::reflect-unit", { jobId });
    const completed = await h.kv.get<any>(REFLECT_JOBS, jobId);

    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(h.provider.summarize.mock.calls[0]?.[0]).not.toContain("You evaluate publication evidence");
    const publicationCall = h.provider.summarize.mock.calls.find(([system]) => String(system).includes("You evaluate publication evidence"));
    if (!publicationCall) throw new Error("reflection_publication_call_missing");
    const publicationPayload = JSON.parse(String(publicationCall[1]));
    const candidate = JSON.parse(publicationPayload.candidate);
    expect(publicationPayload.sources.map((source: { key: string }) => source.key).sort()).toEqual(["one", "two"]);
    expect(candidate.sourceLineage.map((source: { id: string }) => source.id).sort()).toEqual(["one", "two"]);
    expect(result).toMatchObject({ success: true, status: "completed" });
    expect(completed).toMatchObject({ status: "completed", processedClusters: 1, appliedInsights: 1 });
    expect(await h.kv.list(KV.insights)).toHaveLength(1);

    const insightWrites = h.kv.set.mock.calls.filter(([scope]) => scope === KV.insights).length;
    const publications = h.published.length;
    const replay = await h.call("mem::reflect-unit", { jobId });
    expect(replay).toMatchObject({ success: true, skipped: true });
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(h.kv.set.mock.calls.filter(([scope]) => scope === KV.insights)).toHaveLength(insightWrites);
    expect(h.published).toHaveLength(publications);
    expect(await h.kv.get(REFLECT_JOBS, jobId)).toEqual(completed);
    expect(await h.kv.list(KV.insights)).toHaveLength(1);
  });

  it("retains an unknown generation without redispatch when TypeSafe gate settings are enabled", async () => {
    const h = reflectHarness();
    const typeSafe = decisionProvider("skip");
    register(h, typeSafe);
    h.provider.summarize.mockRejectedValueOnce(new Error("provider unavailable"));
    const jobId = await prepareLowSignalGroup(h);

    await h.call("mem::reflect-unit", { jobId });
    const waiting = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(waiting).toMatchObject({ status: "blocked", error: "reflect_generation_outcome_unknown", generation: { state: "intended" } });
    expect(waiting.gateDecision).toBeUndefined();
    waiting.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, waiting);
    const replay = await h.call("mem::reflect-unit", { jobId });

    expect(replay).toMatchObject({ success: true, skipped: true });
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect((await h.kv.get<any>(REFLECT_JOBS, jobId)).generation).toEqual(waiting.generation);
    expect(await h.kv.list(KV.insights)).toHaveLength(0);
  });

  it("preserves a legacy terminal TypeSafe skip receipt without new effects", async () => {
    const h = reflectHarness();
    const typeSafe = decisionProvider("skip");
    register(h, typeSafe);
    const jobId = await prepareLowSignalGroup(h);
    const job = await h.kv.get<any>(REFLECT_JOBS, jobId);
    const manifestId = "legacy-typesafe-manifest";
    const receiptKey = `${jobId}:skip:0:${manifestId}`;
    const receipt = { manifestId, skipped: true, reason: "typesafe_gate" };
    const completed = { ...job, status: "completed", processedClusters: 1, clustersSkipped: 1,
      appliedInsights: 0, reservedInsights: 0, ordinal: 1, unitSequence: 1, resultFingerprint: "legacy-terminal-result",
      nextAttemptAt: 0, dispatchPending: false, manifest: undefined, captured: undefined, generation: undefined, gateDecision: undefined };
    await h.kv.set(REFLECT_CONTROL, receiptKey, receipt);
    await h.kv.set(REFLECT_JOBS, jobId, completed);
    const writes = h.kv.set.mock.calls.length;
    const deletes = h.kv.delete.mock.calls.length;
    const publications = h.published.length;

    const replay = await h.call("mem::reflect-unit", { jobId });

    expect(replay).toEqual({ success: true, skipped: true });
    expect(await h.kv.get(REFLECT_JOBS, jobId)).toEqual(completed);
    expect(await h.kv.get(REFLECT_CONTROL, receiptKey)).toEqual(receipt);
    expect(h.kv.set.mock.calls).toHaveLength(writes);
    expect(h.kv.delete.mock.calls).toHaveLength(deletes);
    expect(h.published).toHaveLength(publications);
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(await h.kv.list(KV.insights)).toHaveLength(0);
  });

  it("keeps the frozen manifest and captured response when sources change before publication", async () => {
    const h = reflectHarness();
    const typeSafe = decisionProvider("skip");
    register(h, typeSafe);
    const jobId = await prepareLowSignalGroup(h);
    const generatedResponse = '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
    let frozenManifest: unknown;
    h.provider.summarize.mockImplementationOnce(async () => {
      const current = await h.kv.get<any>(REFLECT_JOBS, jobId);
      frozenManifest = structuredClone(current.manifest);
      for (const id of ["one", "two"]) {
        const source = await h.kv.get<any>(KV.memories, id);
        await h.kv.set(KV.memories, id, { ...source, version: "2", content: "Changed source content" });
      }
      return generatedResponse;
    });

    const first = await h.call("mem::reflect-unit", { jobId });
    const retained = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(first).toMatchObject({ status: "failed", reason: "reflect_source_changed" });
    expect(retained).toMatchObject({ status: "failed", error: "reflect_source_changed", captured: { response: generatedResponse } });
    expect(retained.manifest).toEqual(frozenManifest);
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(h.provider.summarize.mock.calls[0]?.[0]).not.toContain("You evaluate publication evidence");
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();

    retained.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, retained);
    const replay = await h.call("mem::reflect-unit", { jobId });
    const replayed = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(replay).toMatchObject({ success: true, skipped: true });
    expect(replayed).toMatchObject({ status: "failed", error: "reflect_source_changed" });
    expect(replayed.manifest).toEqual(frozenManifest);
    expect(replayed.captured.response).toBe(generatedResponse);
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(await h.kv.list(KV.insights)).toHaveLength(0);
  });

  it("uses a captured batch result without evaluating TypeSafe", async () => {
    const h = reflectHarness();
    const batchResponse = '<insights><insight confidence="0.8" title="Boundaries">Validate inputs before applying effects.</insight></insights>';
    configureSupportedReflectionResponses(h, batchResponse);
    const typeSafe = decisionProvider("skip");
    const batchQueue = { enqueue: vi.fn(async () => ({ queued: true })) };
    register(h, typeSafe, batchQueue);
    const jobId = await prepareLowSignalGroup(h);
    await h.call("mem::reflect-unit", { jobId });
    const waiting = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(waiting.status).toBe("waiting_batch");

    await h.call("mem::reflect", {
      batchResponse,
      batchCluster: JSON.stringify(waiting.manifest.cluster), batchSourceFingerprint: waiting.manifest.id,
    });
    await h.call("mem::reflect-unit", { jobId });
    const completed = await h.kv.get<any>(REFLECT_JOBS, jobId);

    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
    expect(batchQueue.enqueue).toHaveBeenCalledTimes(1);
    expect(completed).toMatchObject({ status: "completed", processedClusters: 1, appliedInsights: 1 });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(h.provider.summarize.mock.calls[0]?.[0]).toContain("You evaluate publication evidence.");
    expect(await h.kv.list(KV.insights)).toHaveLength(1);
  });
});
