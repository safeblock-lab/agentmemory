import { describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { currentReflectManifest, freezeReflectSeeds, retrieveReflectManifest } from "../src/functions/reflect-retrieval.js";
import { REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";
import { KV } from "../src/state/schema.js";

const selected = { sessionId: "reflect-session" };

async function selectedJob(h: ReturnType<typeof reflectHarness>): Promise<string> {
  await h.kv.set(KV.sessions, selected.sessionId, { project: "/app" });
  const result = await h.call("mem::reflect", { project: "/app", query: "boundaries", selected });
  return result.jobId as string;
}

async function runSelectedUnit(h: ReturnType<typeof reflectHarness>, jobId: string) {
  return h.call("mem::reflect-unit", { jobId, selected });
}

describe("Reflect empty evidence cache recovery", () => {
  it("completes an authoritative exhausted empty selected seed with a durable identity receipt and unchanged replay", async () => {
    const h = reflectHarness();
    h.candidates.splice(0);
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request),
      ...(["keyword_search", "semantic_search"].includes(request.action) ? { status: "ready" } : {}) }));
    const jobId = await selectedJob(h);
    expect(await runSelectedUnit(h, jobId)).toMatchObject({ status: "completed", reason: "reflect_no_evidence" });
    const completed = await h.kv.get(REFLECT_JOBS, jobId);
    expect(completed).toMatchObject({ processedClusters: 1, clustersSkipped: 1, appliedInsights: 0, ordinal: 1 });
    expect(completed.manifest).toBeUndefined();
    expect(completed.captured).toBeUndefined();
    expect(await h.kv.get(REFLECT_CONTROL, `${jobId}:result:0`)).toMatchObject({ kind: "no_evidence",
      project: "/app", seed: "boundaries", ordinal: 0, indexIdentity: { index_id: "observations", generation: "local-v1" },
      insightIds: [], appliedCount: 0 });
    const writes = h.kv.set.mock.calls.length;
    expect(await runSelectedUnit(h, jobId)).toMatchObject({ skipped: true });
    expect(await h.kv.get(REFLECT_JOBS, jobId)).toEqual(completed);
    expect(h.kv.set).toHaveBeenCalledTimes(writes);
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.published).toHaveLength(0);
  });
  it("advances every empty seed before completing and continues global project exhaustion", async () => {
    const h = reflectHarness();
    h.candidates.splice(0);
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request),
      ...(["keyword_search", "semantic_search"].includes(request.action) ? { status: "ready" } : {}) }));
    const jobId = await selectedJob(h);
    const job = await h.kv.get(REFLECT_JOBS, jobId);
    job.seeds = ["one", "two"];
    await h.kv.set(REFLECT_JOBS, jobId, job);
    expect(await runSelectedUnit(h, jobId)).toMatchObject({ status: "pending" });
    expect(await runSelectedUnit(h, jobId)).toMatchObject({ status: "completed" });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).clustersSkipped).toBe(2);
    const global = await h.call("mem::reflect", { query: "empty-global" });
    await h.call("mem::reflect-unit", { jobId: global.jobId });
    expect(await h.call("mem::reflect-unit", { jobId: global.jobId })).toMatchObject({ status: "pending" });
    expect(await h.call("mem::reflect-unit", { jobId: global.jobId })).toMatchObject({ status: "completed" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("keeps partial empty evidence pending even when the global index is ready", async () => {
    const h = reflectHarness();
    h.candidates.splice(0);
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request),
      ...(["keyword_search", "semantic_search"].includes(request.action) ? { status: "partial", reason: "source_not_ready" } : {}) }));
    const jobId = await selectedJob(h);
    expect(await runSelectedUnit(h, jobId)).toMatchObject({ status: "waiting_index", reason: "reflect_preparation_pending" });
    expect(await h.kv.get(REFLECT_CONTROL, `${jobId}:result:0`)).toBeNull();
    expect((await h.kv.get(REFLECT_JOBS, jobId)).processedClusters).toBe(0);
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("resumes a durable empty receipt after an interrupted advance without reclassifying new evidence", async () => {
    const h = reflectHarness();
    h.candidates.splice(0);
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request),
      ...(["keyword_search", "semantic_search"].includes(request.action) ? { status: "ready" } : {}) }));
    const jobId = await selectedJob(h);
    const originalSet = h.kv.set.getMockImplementation()!;
    let cut = false;
    h.kv.set.mockImplementation(async (scope, key, value: any) => {
      const result = await originalSet(scope, key, value);
      if (!cut && scope === REFLECT_CONTROL && key === `${jobId}:result:0`) { cut = true; throw new Error("cut_after_empty_receipt"); }
      return result;
    });
    await expect(runSelectedUnit(h, jobId)).rejects.toThrow("cut_after_empty_receipt");
    h.kv.set.mockImplementation(originalSet);
    h.candidates.push({ obsId: "one", sessionId: "session", score: 1 });
    const searches = h.kv.retrieval.mock.calls.length;
    expect(await runSelectedUnit(h, jobId)).toMatchObject({ status: "completed", reason: "reflect_no_evidence" });
    expect(h.kv.retrieval).toHaveBeenCalledTimes(searches);
    expect((await h.kv.get(REFLECT_JOBS, jobId))).toMatchObject({ processedClusters: 1, clustersSkipped: 1, appliedInsights: 0 });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("refreshes the same selected job after its empty evidence page gains indexed sources", async () => {
    const h = reflectHarness();
    h.candidates.splice(0);
    const jobId = await selectedJob(h);
    const job = await h.kv.get(REFLECT_JOBS, jobId);
    job.sourcePage = [];
    job.sourcePageBinding = { identity: { index_id: "stale", model: "stale", dimensions: 2, generation: "stale" },
      graphGeneration: "stale", deferredFamilies: ["stale"] };
    await h.kv.set(REFLECT_JOBS, jobId, job);

    const waiting = await runSelectedUnit(h, jobId);
    expect(waiting).toMatchObject({ jobId, status: "waiting_index" });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).sourcePage).toEqual([]);

    const retry = await h.kv.get(REFLECT_JOBS, jobId);
    retry.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, retry);
    h.candidates.push({ obsId: "one", sessionId: "session", score: 1 }, { obsId: "two", sessionId: "session", score: 0.8 });

    const resumed = await runSelectedUnit(h, jobId);
    const recovered = await h.kv.get(REFLECT_JOBS, jobId);
    expect(resumed).toMatchObject({ jobId });
    expect(recovered.manifest.sources.map((source: { id: string }) => source.id)).toEqual(["one", "two"]);
    expect(recovered.sourcePageBinding.identity).toMatchObject({ index_id: "observations", generation: "local-v1" });
    expect(recovered.sourcePageBinding.deferredFamilies).not.toContain("stale");
    const searches = h.kv.retrieval.mock.calls.map(([request]) => request)
      .filter(request => request.action === "keyword_search" || request.action === "semantic_search");
    expect(searches).toHaveLength(4);
    expect(searches.every(request => request.project === "/app" && request.limit === 12)).toBe(true);
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("keeps waiting and retries bounded retrieval while the source page is still empty", async () => {
    const h = reflectHarness();
    h.candidates.splice(0);
    const jobId = await selectedJob(h);
    const job = await h.kv.get(REFLECT_JOBS, jobId);
    job.sourcePage = [];
    await h.kv.set(REFLECT_JOBS, jobId, job);

    for (let attempt = 0; attempt < 2; attempt++) {
      const result = await runSelectedUnit(h, jobId);
      expect(result).toMatchObject({ jobId, status: "waiting_index" });
      const waiting = await h.kv.get(REFLECT_JOBS, jobId);
      expect(waiting.error).toBe("reflect_preparation_pending");
      expect(waiting.manifest).toBeUndefined();
      expect(waiting.sourcePage).toEqual([]);
      waiting.nextAttemptAt = 0;
      await h.kv.set(REFLECT_JOBS, jobId, waiting);
    }

    const searches = h.kv.retrieval.mock.calls.map(([request]) => request)
      .filter(request => request.action === "keyword_search" || request.action === "semantic_search");
    expect(searches).toHaveLength(4);
    expect(searches.every(request => request.project === "/app" && request.limit === 12)).toBe(true);
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });

  it("reuses nonempty snapshots, then rejects them when their canonical source changes", async () => {
    const h = reflectHarness();
    const accepted = await h.call("mem::reflect", { project: "/app", query: "boundaries" });
    const job = await h.kv.get(REFLECT_JOBS, accepted.jobId);
    await freezeReflectSeeds(h.kv as never, job);
    const manifest = await retrieveReflectManifest(h.kv as never, job, h.embedding as never);
    const snapshot = structuredClone(job.sourcePage);
    const callsBeforeReuse = h.kv.retrieval.mock.calls.length;

    const reused = await retrieveReflectManifest(h.kv as never, job, h.embedding as never);
    expect(reused.id).toBe(manifest.id);
    expect(job.sourcePage).toEqual(snapshot);
    expect(h.kv.retrieval).toHaveBeenCalledTimes(callsBeforeReuse);
    expect(h.embedding.embed).toHaveBeenCalledTimes(1);

    job.manifest = manifest;
    const source = await h.kv.get(KV.memories, "one");
    await h.kv.set(KV.memories, "one", { ...source, content: "Changed canonical source" });
    expect(await currentReflectManifest(h.kv as never, job)).toBe(false);
  });

  it("does not mistake an empty page for remaining work after the final seed", async () => {
    const h = reflectHarness();
    const jobId = await selectedJob(h);
    const job = await h.kv.get(REFLECT_JOBS, jobId);
    job.ordinal = 1;
    job.sourcePage = [];
    await h.kv.set(REFLECT_JOBS, jobId, job);

    const result = await runSelectedUnit(h, jobId);

    expect(result).toMatchObject({ success: true, status: "completed" });
    expect(h.embedding.embed).not.toHaveBeenCalled();
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
});
