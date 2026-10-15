import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
vi.mock("../src/functions/dashboard-activity.js", async original => ({ ...await original<typeof import("../src/functions/dashboard-activity.js")>(), recordOperation: vi.fn() }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { KV } from "../src/state/schema.js";
import { REFLECT_CONTROL, REFLECT_JOBS, REFLECT_PENDING } from "../src/functions/reflect-queue-types.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import { legacyReflectSourceFingerprint } from "../src/functions/reflect-retrieval.js";
import { registerApiTriggers } from "../src/triggers/api.js";
import { registerMcpEndpoints } from "../src/mcp/server.js";
import { registerReflectFunctions } from "../src/functions/reflect.js";
import { fingerprintId } from "../src/state/schema.js";
const generated = '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
const request = { project: "/app", query: "boundaries", manual: true, requestId: "manual-request" };
function fixture(verdict = "supported") {
  vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "1");
  const h = reflectHarness();
  h.provider.summarize.mockImplementation(async (system: string) => system.includes("You evaluate publication evidence")
    ? JSON.stringify({ requestId: system.match(/requestId must be ([a-z0-9_-]+)/)?.[1], verdict, reason: "Complete originals evaluated." }) : generated);
  return h;
}
async function fragmented(h: ReturnType<typeof reflectHarness>) {
  for (const id of ["one", "two"]) {
    const source = await h.kv.get<any>(KV.memories, id);
    await h.kv.set(KV.memories, id, { ...source, content: `Boundary validation ${id}. ${"Repeated input checks. ".repeat(650)}` });
  }
}
afterEach(() => { vi.unstubAllEnvs(); vi.useRealTimers(); });
describe("manual reflection foreground", () => {
  it("runs one paused foreground unit, stores manual intent, and exact replay makes zero calls", async () => {
    const h = fixture(); await fragmented(h);
    const first = await h.call("mem::reflect", request);
    expect(first).toMatchObject({ success: true, processedClusters: 1, appliedInsights: 1, completed: false });
    expect(first.continuation).toMatchObject({ jobId: first.jobId, requestId: request.requestId, project: "/app", cut: 1 });
    expect(await h.kv.get(REFLECT_PENDING, first.jobId)).toMatchObject({ executionOrigin: "manual" });
    expect(await h.kv.get(REFLECT_JOBS, first.jobId)).toMatchObject({ executionOrigin: "manual", dispatchPending: false, unitSequence: 1 });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    const before = await h.kv.list(KV.insights);
    expect(await h.call("mem::reflect", request)).toEqual(first);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    const secondRequest = { ...request, continuation: first.continuation };
    const second = await h.call("mem::reflect", secondRequest);
    expect(second.processedClusters).toBe(2); expect(h.provider.summarize).toHaveBeenCalledTimes(4);
    expect(await h.call("mem::reflect", secondRequest)).toEqual(second);
    expect(h.provider.summarize).toHaveBeenCalledTimes(4);
    expect(before[0].reinforcements).toBe(0);
    expect(h.published).toEqual([]);
  });
  it("rejects foreign project, request, job, checkpoint and selected/manual mixtures", async () => {
    const h = fixture(); await fragmented(h);
    const first = await h.call("mem::reflect", request);
    for (const changed of [{ project: "/other" }, { requestId: "foreign" }, { jobId: "foreign" }, { checkpoint: "foreign" }, { cut: 3 }]) {
      expect(await h.call("mem::reflect", { ...request, continuation: { ...first.continuation, ...changed } })).toMatchObject({ success: false });
    }
    expect(await h.call("mem::reflect", { ...request, project: "/other" })).toMatchObject({ error: "manual_reflect_identity_mismatch" });
    expect(await h.call("mem::reflect", { ...request, selected: { sessionId: "session" } })).toMatchObject({ success: false });
    expect(await h.call("mem::reflect", { ...request, continuation: { jobId: first.jobId } })).toMatchObject({ success: false });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("all automatic and batch callback paths leave captured manual jobs and orphan intents untouched", async () => {
    const h = fixture("uncertain");
    const first = await h.call("mem::reflect", request);
    const job = await h.kv.get<any>(REFLECT_JOBS, first.jobId);
    expect(job.captured.response).toBe(generated);
    expect(first).toMatchObject({ completed: false, reason: "reflect_publication_uncertain" });
    await h.kv.set(REFLECT_PENDING, "orphan", { id: "orphan", request: job.request, executionOrigin: "manual" });
    const before = structuredClone([...h.rows]);
    vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0"); h.provider.summarize.mockClear();
    expect(await h.call("mem::reflect-unit", { jobId: job.id })).toMatchObject({ reason: "manual_foreground_required" });
    expect(await h.call("mem::reflect-recover")).toMatchObject({ dispatched: 0 });
    expect(await h.call("mem::reflect", { batchResponse: generated, batchCluster: JSON.stringify(job.manifest.cluster), batchSourceFingerprint: job.manifest.id })).toMatchObject({ reason: "manual_foreground_required" });
    expect([...h.rows].filter(([key]) => !key.endsWith("/recovery-cursor"))).toEqual(before);
    expect(h.provider.summarize).not.toHaveBeenCalled(); expect(h.published).toEqual([]);
    const continued = await h.call("mem::reflect", { ...request, continuation: first.continuation });
    expect(continued.reason).toBe("reflect_publication_uncertain");
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("unsupported output advances exactly one unit and retains honest partial reasons", async () => {
    const h = fixture("unsupported"); await fragmented(h);
    const first = await h.call("mem::reflect", request);
    expect(first).toMatchObject({ processedClusters: 1, clustersSkipped: 1, appliedInsights: 0, completed: false,
      deferredFamilies: expect.arrayContaining(["reflect_publication_unsupported"]) });
    expect(await h.kv.list(KV.insights)).toEqual([]); expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("completes the bounded selection without claiming excluded families, and replays without calls", async () => {
    const h = fixture(); const first = await h.call("mem::reflect", request);
    expect(first).toMatchObject({ status: "completed", terminal: true, completed: true, processedClusters: 1,
      coverage: { scope: "bounded-indexed-selection", requestedFamilies: ["memory", "observation"],
        excludedFamilies: ["fact", "lesson", "crystal"], complete: true, globalFamiliesComplete: false } });
    expect(first.continuation).toBeUndefined();
    const before = [...h.rows]; h.provider.summarize.mockClear();
    expect(await h.call("mem::reflect", request)).toEqual(first);
    expect(h.provider.summarize).not.toHaveBeenCalled(); expect([...h.rows]).toEqual(before);
  });
  it("unknown generation is retained without retry; later capture preserves the exact operation cut", async () => {
    vi.useFakeTimers(); const h = fixture(); let finish!: (value: string) => void;
    h.provider.summarize.mockImplementationOnce(() => new Promise<string>(resolve => { finish = resolve; }));
    const pending = h.call("mem::reflect", request);
    await vi.advanceTimersByTimeAsync(45001);
    const first = await pending;
    expect(first).toMatchObject({ completed: false, reason: "reflect_generation_outcome_unknown" });
    const second = await h.call("mem::reflect", { ...request, continuation: first.continuation });
    expect(second.reason).toBe("reflect_generation_outcome_unknown");
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    finish(generated); await vi.advanceTimersByTimeAsync(0);
    expect(await h.kv.get(REFLECT_JOBS, first.jobId)).toMatchObject({ manual: { cuts: 2 }, captured: { response: generated } });
    const third = await h.call("mem::reflect", { ...request, continuation: second.continuation });
    expect(third).toMatchObject({ terminal: true, appliedInsights: 1 }); expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  for (const boundary of ["started", "completed"]) {
    it(`manual continuation recovers ${boundary} acknowledgement after source deletion without new judge`, async () => {
      const h = fixture(); const set = h.kv.set.getMockImplementation()!; let cut = false;
      h.kv.set.mockImplementation(async (scope, key, value: any) => {
        await set(scope, key, value);
        if (!cut && scope === KV.batchCallbacks && key.startsWith("reflect:") && value.state === boundary) { cut = true; throw new Error("lost_ack"); }
        return value;
      });
      const first = await h.call("mem::reflect", request);
      expect(first.completed).toBe(false); expect(first.continuation).toBeTruthy();
      h.kv.set.mockImplementation(set); await h.kv.delete(KV.memories, "one"); h.provider.summarize.mockClear();
      const secondRequest = { ...request, continuation: first.continuation };
      const recovered = await h.call("mem::reflect", secondRequest);
      expect(recovered).toMatchObject({ terminal: true, appliedInsights: 1, processedClusters: 1 });
      expect(await h.call("mem::reflect", secondRequest)).toEqual(recovered);
      expect(h.provider.summarize).not.toHaveBeenCalled();
      expect((await h.kv.list(KV.insights))[0].reinforcements).toBe(0);
    });
  }
  it("paused ordinary captured output without admission never launches a judge or publishes", async () => {
    const h = fixture(); vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
    const ordinary = await h.call("mem::reflect", { project: "/app", query: "boundaries" });
    const set = h.kv.set.getMockImplementation()!;
    h.kv.set.mockImplementation(async (scope, key, value: any) => {
      await set(scope, key, value);
      if (scope === REFLECT_JOBS && value.captured && value.captured.generationValid === undefined) vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "1");
      return value;
    });
    // Flip pause immediately after capture: this in-flight generation must also yield before judge.
    await h.call("mem::reflect-unit", { jobId: ordinary.jobId });
    h.kv.set.mockImplementation(set);
    const job = await h.kv.get<any>(REFLECT_JOBS, ordinary.jobId);
    expect(job.captured).toBeTruthy(); expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    job.nextAttemptAt = 0; await h.kv.set(REFLECT_JOBS, job.id, job);
    h.provider.summarize.mockClear();
    expect(await h.call("mem::reflect-unit", { jobId: job.id })).toMatchObject({ paused: true });
    expect(h.provider.summarize).not.toHaveBeenCalled(); expect(await h.kv.list(KV.insights)).toEqual([]);
  });
  for (const boundary of ["started", "completed"]) {
    it(`paused ordinary captured ${boundary} acknowledgement recovers without provider calls`, async () => {
      const h = fixture(); vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
      const ordinary = await h.call("mem::reflect", { project: "/app", query: "boundaries" });
      const set = h.kv.set.getMockImplementation()!; let cut = false;
      h.kv.set.mockImplementation(async (scope, key, value: any) => {
        await set(scope, key, value);
        if (!cut && scope === KV.batchCallbacks && key.startsWith("reflect:") && value.state === boundary) { cut = true; throw new Error("lost_ack"); }
        return value;
      });
      await h.call("mem::reflect-unit", { jobId: ordinary.jobId });
      h.kv.set.mockImplementation(set); await h.kv.delete(KV.memories, "one"); h.provider.summarize.mockClear();
      const job = await h.kv.get<any>(REFLECT_JOBS, ordinary.jobId); job.nextAttemptAt = 0;
      await h.kv.set(REFLECT_JOBS, job.id, job); vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "1");
    expect(await h.call("mem::reflect-unit", { jobId: job.id })).toMatchObject({ status: "completed" });
      expect(h.provider.summarize).not.toHaveBeenCalled(); expect((await h.kv.list(KV.insights))[0].reinforcements).toBe(0);
    });
  }
  it("force does not let an ordinary job bypass pause, while deferred admissions retain automatic origin", async () => {
    const h = fixture();
    const ordinary = await h.call("mem::reflect", { project: "/app", query: "boundaries", force: true, deferred: true });
    expect(ordinary.queued).toBe(true);
    expect(await h.call("mem::reflect-unit", { jobId: ordinary.jobId })).toMatchObject({ paused: true });
    expect(h.provider.summarize).not.toHaveBeenCalled(); expect(h.published).toHaveLength(1);
    expect((await h.kv.get<any>(REFLECT_JOBS, ordinary.jobId)).executionOrigin).toBeUndefined();
  });
  it("global foreground preserves project pagination and does not consume the next project in the same cut", async () => {
    const h = fixture(); const original = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async data => data.action === "project_catalog_page"
      ? { status: "ready", items: [{ project: data.cursor ? "/other" : "/app" }], has_more: !data.cursor, next_cursor: data.cursor ? undefined : "project-page-2" } : original(data));
    const global = { ...request, project: undefined };
    const first = await h.call("mem::reflect", global);
    expect(first.processedClusters).toBe(0); expect(h.provider.summarize).not.toHaveBeenCalled();
    const second = await h.call("mem::reflect", { ...global, continuation: first.continuation });
    expect(second.processedClusters).toBe(1); expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(await h.kv.get(REFLECT_JOBS, first.jobId)).toMatchObject({ global: { cursor: "project-page-2", hasMore: true, ordinal: 1 } });
    expect(h.kv.retrieval.mock.calls.filter(([data]) => data.action === "project_catalog_page")).toHaveLength(1);
  });
  it("idle between source fragments survives registration restart with the same pending identity", async () => {
    const h = fixture(); await fragmented(h); const first = await h.call("mem::reflect", request);
    const job = await h.kv.get<any>(REFLECT_JOBS, first.jobId); job.manual.startedAt = Date.now() - 3 * 86400_000;
    await h.kv.set(REFLECT_JOBS, job.id, job); h.provider.summarize.mockClear();
    registerReflectFunctions(h.sdk as never, h.kv as never, h.provider as never, undefined, undefined, undefined, h.embedding as never);
    const resumed = await h.call("mem::reflect", { ...request, continuation: first.continuation });
    expect(resumed).toMatchObject({ jobId: first.jobId, processedClusters: 2, completed: false });
    expect(resumed).not.toHaveProperty("deadline"); expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(await h.call("mem::reflect", request)).toEqual(first);
  });
  it("the 256th cut terminates explicitly without calling providers", async () => {
    const h = fixture(); await fragmented(h); const first = await h.call("mem::reflect", request);
    const job = await h.kv.get<any>(REFLECT_JOBS, first.jobId); job.manual.cuts = 256;
    await h.kv.set(REFLECT_JOBS, job.id, job); h.provider.summarize.mockClear();
    const checkpoint = batchEffectKey(JSON.stringify([job.id, job.request.requestId, job.request.project ?? null,
      job.project ?? null, job.ordinal, job.unitSequence ?? 0, job.manifest?.id, job.sourceCursor, job.global, job.manual.cuts]));
    const limited = await h.call("mem::reflect", { ...request, continuation: { ...first.continuation, cut: 256, checkpoint } });
    expect(limited).toMatchObject({ terminal: true, completed: false, status: "partial", reason: "manual_reflect_operation_limit" });
    expect(limited.continuation).toBeUndefined(); expect(h.provider.summarize).not.toHaveBeenCalled();
  });
});
