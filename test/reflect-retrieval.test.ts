import { describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
import { rerank } from "../src/state/reranker.js";
import { reflectHarness } from "./helpers/reflect-harness.js";
import { freezeReflectSeeds, retrieveReflectManifest, currentReflectManifest } from "../src/functions/reflect-retrieval.js";
import { REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";
import { KV } from "../src/state/schema.js";

async function fixture() {
  const h = reflectHarness();
  const { jobId } = await h.call("mem::reflect", { project: "/app", query: "boundaries" });
  const job = await h.kv.get(REFLECT_JOBS, jobId);
  await freezeReflectSeeds(h.kv as never, job);
  return { h, job };
}
describe("Reflect bounded native retrieval", () => {
  for (const [label, metadata, reason] of [
    ["explicit exhausted ready", { status: "ready" }, "reflect_no_evidence"],
    ["legacy without status", {}, "reflect_preparation_pending"],
    ["partial project mapping", { status: "partial", reason: "project_mapping_not_ready" }, "reflect_preparation_pending"],
    ["ready with cursor", { status: "ready", next_cursor: "more" }, "reflect_preparation_pending"],
    ["ready with more", { status: "ready", has_more: true }, "reflect_preparation_pending"],
    ["ready partial coverage", { status: "ready", partial: true }, "reflect_preparation_pending"],
    ["ready exhausted budget", { status: "ready", budget_exhausted: true }, "reflect_preparation_pending"],
  ] as const) {
    it(`distinguishes empty ${label} without borrowing global readiness`, async () => {
      const { h, job } = await fixture();
      h.candidates.splice(0);
      const retrieve = h.kv.retrieval.getMockImplementation()!;
      h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request),
        ...(["keyword_search", "semantic_search"].includes(request.action) ? metadata : {}) }));
      await expect(retrieveReflectManifest(h.kv as never, job, h.embedding as never))
        .rejects.toMatchObject({ status: "waiting_index", message: reason });
      expect(job.sourcePage).toEqual([]);
      expect(h.provider.summarize).not.toHaveBeenCalled();
      expect(h.kv.list).not.toHaveBeenCalled();
    });
  }
  it("requires both streams to be ready before reporting no evidence", async () => {
    const { h, job } = await fixture();
    h.candidates.splice(0);
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request),
      status: request.action === "keyword_search" ? "ready" : "partial" }));
    await expect(retrieveReflectManifest(h.kv as never, job, h.embedding as never))
      .rejects.toMatchObject({ message: "reflect_preparation_pending" });
  });
  it("retains useful authorized positive results during partial project preparation", async () => {
    const { h, job } = await fixture();
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request), status: "partial", reason: "catalog_not_ready" }));
    expect((await retrieveReflectManifest(h.kv as never, job, h.embedding as never)).sources.map(item => item.id)).toEqual(["one", "two"]);
  });
  it("does not describe a failed search as empty evidence", async () => {
    const { h, job } = await fixture();
    h.candidates.splice(0);
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => ({ ...await retrieve(request), status: "failed" }));
    await expect(retrieveReflectManifest(h.kv as never, job, h.embedding as never)).rejects.toThrow("reflect_retrieval_failed");
    expect(job.sourcePage).toBeUndefined();
  });
  it("uses bounded lexical/vector retrieval and reranks only authorized projected sources", async () => {
    const { h, job } = await fixture();
    h.candidates.push({ obsId: "foreign", sessionId: "other", score: 100 });
    await h.kv.set(KV.memories, "foreign", { project: "/other", content: "secret" });
    await h.kv.set(KV.semanticVectors("observations"), "foreign", { sourceScope: KV.memories, sourceVersion: "1" });
    const manifest = await retrieveReflectManifest(h.kv as never, job, h.embedding as never);
    expect(manifest.sources.map(source => source.id)).toEqual(["one", "two"]);
    expect(manifest.prompt).not.toContain("secret");
    expect(h.kv.list).not.toHaveBeenCalled();
    expect(h.kv.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: "keyword_search", project: "/app", limit: 12 }));
    expect(h.kv.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: "semantic_search", project: "/app", limit: 12, max_candidates: 512, probes: 32 }));
    expect(vi.mocked(rerank).mock.calls.at(-1)?.[1].map(item => item.observation.id)).toEqual(["one", "two"]);
    expect(Buffer.byteLength(manifest.prompt)).toBeLessThan(8192);
    job.manifest = manifest;
    expect(await currentReflectManifest(h.kv as never, job)).toBe(true);
    h.graph[0].revision = "2";
    expect(await currentReflectManifest(h.kv as never, job)).toBe(false);
  });
  it("retains oversized native projections as pending rather than decoding or scanning", async () => {
    const { h, job } = await fixture();
    h.projectionFailures.set("one", "oversized");
    await expect(retrieveReflectManifest(h.kv as never, job, h.embedding as never)).rejects.toMatchObject({ status: "waiting_budget", message: "source_requires_partition" });
    expect(h.kv.list).not.toHaveBeenCalled();
    expect(h.kv.get).not.toHaveBeenCalledWith(KV.memories, "one");
  });
  it("does not fall back to corpus scans when index coverage is incomplete", async () => {
    const { h, job } = await fixture();
    h.index.coverage_ready = false;
    await expect(retrieveReflectManifest(h.kv as never, job, h.embedding as never)).rejects.toMatchObject({ status: "waiting_index" });
    expect(h.embedding.embed).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
  });
  it("partitions a UTF8 cluster exceeding the complete prompt budget without truncation", async () => {
    const { h, job } = await fixture();
    for (const id of ["one", "two"]) {
      const memory = await h.kv.get(KV.memories, id);
      memory.content = "é".repeat(2000);
      await h.kv.set(KV.memories, id, memory);
    }
    const manifest = await retrieveReflectManifest(h.kv as never, job, h.embedding as never);
    expect(manifest.nextCursor).toBeTruthy();
    expect(Buffer.byteLength(JSON.stringify({ systemPrompt: (await import("../src/prompts/reflect.js")).REFLECT_SYSTEM, userPrompt: manifest.prompt }))).toBeLessThanOrEqual(8192);
  });
});
