import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { KV } from "../src/state/schema.js";
import { REFLECT_CONTROL, REFLECT_JOBS, REFLECT_PENDING } from "../src/functions/reflect-queue-types.js";
describe("Reflect durable queue", () => {
  let h: ReturnType<typeof reflectHarness>;
  beforeEach(() => {
    vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0"); h = reflectHarness();
    const generate = '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
    h.provider.summarize.mockImplementation(async (systemPrompt: string, userPrompt: string) => {
      if (systemPrompt.includes("You evaluate publication evidence")) {
        const requestId = systemPrompt.match(/requestId must be ([a-z0-9_-]+)/)?.[1];
        return JSON.stringify({ requestId, verdict: "supported", reason: "The complete source supports the insight." });
      }
      return generate;
    });
  });
  afterEach(() => { vi.unstubAllEnvs(); vi.useRealTimers(); });
  const accept = () => h.call("mem::reflect", { project: "/app", query: "boundaries" });
  for (const mutation of ["changed", "deleted", "moved"] as const) {
    it(`retains original capture and request when source is ${mutation} before effect admission`, async () => {
      const { jobId } = await accept();
      const originalSet = h.kv.set.getMockImplementation()!;
      h.kv.set.mockImplementation(async (scope, key, value: any) => {
        if (scope === KV.batchCallbacks && value.state === "started") throw new Error("cut_before_admission");
        return originalSet(scope, key, value);
      });
      await h.call("mem::reflect-unit", { jobId });
      const captured = await h.kv.get(REFLECT_JOBS, jobId);
      expect(captured.captured.response).toContain("Validate input");
      h.kv.set.mockImplementation(originalSet);
      if (mutation === "deleted") await h.kv.delete(KV.memories, "one");
      else {
        const source = await h.kv.get(KV.memories, "one");
        await h.kv.set(KV.memories, "one", { ...source, ...(mutation === "changed" ? { content: "Changed content" } : { project: "/other" }) });
      }
      captured.nextAttemptAt = 0;
      await h.kv.set(REFLECT_JOBS, jobId, captured);
      h.provider.summarize.mockClear();
      expect(await h.call("mem::reflect-unit", { jobId })).toMatchObject({ status: "failed", reason: "reflect_source_changed" });
      const stale = await h.kv.get(REFLECT_JOBS, jobId);
      expect(stale.request).toEqual(captured.request);
      expect(stale.manifest).toEqual(captured.manifest);
      expect(stale.captured).toEqual(captured.captured);
      expect(stale.sourcePage).toEqual(captured.sourcePage);
      expect(stale.appliedInsights).toBe(0);
      expect(await h.kv.get(REFLECT_PENDING, jobId)).toBeNull();
      await h.call("mem::reflect-unit", { jobId });
      await h.call("mem::reflect-recover");
      expect(h.provider.summarize).not.toHaveBeenCalled();
      expect(await h.kv.list(KV.insights)).toHaveLength(0);
    });
  }
  for (const boundary of ["started", "completed"] as const) {
    for (const mutation of ["deleted", "version_changed"] as const) {
      it(`recovers persisted ${boundary} receipt after source ${mutation} without judge, generation, or duplicate effect`, async () => {
        const { jobId } = await accept();
        const originalSet = h.kv.set.getMockImplementation()!;
        let cut = false;
        h.kv.set.mockImplementation(async (scope, key, value: any) => {
          const result = await originalSet(scope, key, value);
          if (!cut && scope === KV.batchCallbacks && key.startsWith("reflect:") && value.state === boundary) {
            cut = true;
            throw new Error(`cut_after_persisted_${boundary}`);
          }
          return result;
        });
        await h.call("mem::reflect-unit", { jobId });
        const captured = await h.kv.get(REFLECT_JOBS, jobId);
        expect(cut).toBe(true);
        expect(captured.captured).toBeTruthy();
        expect(await h.kv.get(KV.batchCallbacks, `reflect:${captured.captured.effectKey}`)).toMatchObject({ state: boundary });
        h.kv.set.mockImplementation(originalSet);

        if (mutation === "deleted") await h.kv.delete(KV.memories, "one");
        else {
          const source = await h.kv.get<any>(KV.memories, "one");
          await h.kv.set(KV.memories, "one", { ...source, version: "2", content: "Updated source version" });
          await h.kv.set(KV.semanticVectors("observations"), "one", { sourceScope: KV.memories, sourceVersion: "2" });
        }
        const beforeRecovery = await h.kv.list(KV.insights);
        expect(beforeRecovery).toHaveLength(boundary === "completed" ? 1 : 0);
        h.provider.summarize.mockClear();
        h.provider.summarize.mockImplementation(async () => { throw new Error("provider_must_not_run_after_callback_ack"); });
        captured.nextAttemptAt = 0;
        await h.kv.set(REFLECT_JOBS, jobId, captured);

        const recovered = await h.call("mem::reflect-unit", { jobId });
        expect(recovered).toMatchObject({ success: true });
        expect(h.provider.summarize).not.toHaveBeenCalled();
        const insights = await h.kv.list(KV.insights);
        expect(insights).toHaveLength(1);
        expect(insights[0].reinforcements).toBe(0);
        expect(insights[0].appliedBatchEffects).toEqual([captured.captured.effectKey]);
        if (boundary === "completed") expect(insights).toEqual(beforeRecovery);
        expect((await h.kv.get(REFLECT_JOBS, jobId)).appliedInsights).toBe(1);
      });
    }
  }
  it("recovers the exact result receipt and cursor after source deletion without judge or reapply", async () => {
    for (const id of ["one", "two"]) {
      const source = await h.kv.get<any>(KV.memories, id);
      await h.kv.set(KV.memories, id, { ...source, content: `Boundary evidence ${id}. ${"Repeated boundary input. ".repeat(650)}` });
    }
    const { jobId } = await accept();
    const originalSet = h.kv.set.getMockImplementation()!;
    let cut = false;
    h.kv.set.mockImplementation(async (scope, key, value: any) => {
      const result = await originalSet(scope, key, value);
      if (!cut && scope === REFLECT_CONTROL && key === `${jobId}:result:0`) {
        cut = true;
        throw new Error("cut_after_persisted_result_receipt");
      }
      return result;
    });
    await h.call("mem::reflect-unit", { jobId });
    const interrupted = await h.kv.get(REFLECT_JOBS, jobId);
    const manifest = interrupted.manifest;
    const captured = interrupted.captured;
    const receipt = await h.kv.get<any>(REFLECT_CONTROL, `${jobId}:result:0`);
    expect(cut).toBe(true);
    expect(manifest.nextCursor).toBeTruthy();
    expect(receipt).toMatchObject({ manifestId: manifest.id, effectKey: captured.effectKey,
      responseFingerprint: expect.any(String), appliedCount: 1, nextCursor: manifest.nextCursor });
    const beforeRecovery = await h.kv.list(KV.insights);
    expect(beforeRecovery).toHaveLength(1);
    h.kv.set.mockImplementation(originalSet);
    await h.kv.delete(KV.memories, "one");
    await h.kv.delete(KV.memories, "two");
    h.provider.summarize.mockClear();
    h.provider.summarize.mockImplementation(async () => { throw new Error("provider_must_not_run_after_result_receipt"); });
    interrupted.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, interrupted);

    const recovered = await h.call("mem::reflect-unit", { jobId });
    const advanced = await h.kv.get(REFLECT_JOBS, jobId);
    expect(recovered).toMatchObject({ success: true, recovered: true });
    expect(advanced).toMatchObject({ unitSequence: 1, processedClusters: 1, appliedInsights: 1 });
    expect(advanced.sourceCursor).toEqual(manifest.nextCursor);
    expect(advanced.manifest).toBeUndefined();
    expect(advanced.captured).toBeUndefined();
    expect(await h.kv.get<any>(REFLECT_CONTROL, `${jobId}:result:0`)).toEqual(receipt);
    expect(await h.kv.list(KV.insights)).toEqual(beforeRecovery);
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("reports genuine malformed retrieval failure separately from preparation and empty evidence", async () => {
    const { jobId } = await accept();
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => request.action === "keyword_search" ? { items: "invalid" } : retrieve(request));
    for (let attempt = 0; attempt < 3; attempt++) {
      const job = await h.kv.get(REFLECT_JOBS, jobId);
      job.nextAttemptAt = 0;
      await h.kv.set(REFLECT_JOBS, jobId, job);
      await h.call("mem::reflect-unit", { jobId });
    }
    expect((await h.kv.get(REFLECT_JOBS, jobId))).toMatchObject({ status: "failed", error: "reflect_unit_failed", attempts: 3 });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("persists provider output and provenance once across duplicate deliveries", async () => {
    const { jobId } = await accept();
    const result = await h.call("mem::reflect-unit", { jobId });
    expect(result).toMatchObject({ status: "completed" });
    const insights = await h.kv.list(KV.insights);
    expect(insights).toHaveLength(1);
    expect(insights[0]).toMatchObject({ content: "Validate input at boundaries.", sourceMemoryIds: ["one", "two"], reinforcements: 0 });
    expect(insights[0].appliedBatchEffects).toHaveLength(1);
    await h.call("mem::reflect-unit", { jobId });
    expect(await h.kv.list(KV.insights)).toEqual(insights);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  for (const boundary of ["started", "completed"]) {
    it(`resumes a cut at callback ${boundary} with captured output and no repeated effect`, async () => {
      const { jobId } = await accept();
      vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "1");
      expect(await h.call("mem::reflect-unit", { jobId })).toMatchObject({ paused: true });
      expect(h.provider.summarize).not.toHaveBeenCalled();
      vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
      const originalSet = h.kv.set.getMockImplementation()!;
      let interrupted = false;
      h.kv.set.mockImplementation(async (scope, key, value: any) => {
        const persisted = await originalSet(scope, key, value);
        if (!interrupted && scope === KV.batchCallbacks && key.startsWith("reflect:") && value.state === boundary) { interrupted = true; throw new Error(`cut_at_${boundary}`); }
        return persisted;
      });
      await h.call("mem::reflect-unit", { jobId });
      const job = await h.kv.get(REFLECT_JOBS, jobId);
      expect(job.captured.response).toContain("Validate input");
      h.kv.set.mockImplementation(originalSet);
      job.nextAttemptAt = 0;
      await h.kv.set(REFLECT_JOBS, jobId, job);
      vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "1");
      await h.call("mem::reflect-unit", { jobId });
      const insights = await h.kv.list(KV.insights);
      expect(insights).toHaveLength(1);
      expect(insights[0].reinforcements).toBe(0);
      expect((await h.kv.get(REFLECT_JOBS, jobId)).appliedInsights).toBe(1);
      expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    });
  }
  it("recovers a lost publish with one bounded pending page", async () => {
    const trigger = h.sdk.trigger.getMockImplementation()!;
    let lost = false;
    h.sdk.trigger.mockImplementation(async input => {
      if (!lost && input.function_id === "iii::durable::publish") { lost = true; throw new Error("send_lost"); }
      return trigger(input);
    });
    const { jobId } = await accept();
    expect(h.published).toHaveLength(0);
    expect(await h.kv.get(REFLECT_PENDING, jobId)).toBeTruthy();
    expect(await h.call("mem::reflect-recover")).toMatchObject({ dispatched: 1 });
    expect(h.published).toHaveLength(1);
    expect(h.kv.pages).toHaveBeenCalledWith(REFLECT_PENDING, expect.objectContaining({ limit: 16, maxBytes: 32768 }));
  });
  it("rejects mismatched callbacks and retains unknown generation without automatic source refresh", async () => {
    const { jobId } = await accept();
    h.provider.summarize.mockRejectedValueOnce(new Error("provider_unavailable"));
    await h.call("mem::reflect-unit", { jobId });
    const job = await h.kv.get(REFLECT_JOBS, jobId);
    const before = structuredClone(job);
    expect(await h.call("mem::reflect", { batchResponse: "<insights></insights>", batchCluster: JSON.stringify({ ...job.manifest.cluster, unitId: "wrong" }), batchSourceFingerprint: job.manifest.id })).toMatchObject({ stale: true });
    expect(await h.kv.get(REFLECT_JOBS, jobId)).toEqual(before);
    h.projectionFailures.set("one", "stale");
    job.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, job);
    await h.call("mem::reflect-unit", { jobId });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).error).toBe("reflect_generation_outcome_unknown");
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(await h.kv.list(KV.insights)).toHaveLength(0);
  });
  it("retains an unknown generation outcome and never retries the provider", async () => {
    vi.useFakeTimers();
    h.provider.summarize.mockImplementation(() => new Promise(() => {}));
    const { jobId } = await accept();
    const delivery = h.call("mem::reflect-unit", { jobId });
    await vi.advanceTimersByTimeAsync(45001);
    await delivery;
    const job = await h.kv.get(REFLECT_JOBS, jobId);
    expect(job).toMatchObject({ status: "blocked", attempts: 0, error: "reflect_generation_outcome_unknown",
      generation: { state: "intended" } });
    expect(job.nextAttemptAt).toBeGreaterThan(Date.now());
    expect(job.claim).toBeUndefined();
    job.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, job);
    await h.call("mem::reflect-unit", { jobId });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(await h.kv.list(KV.insights)).toHaveLength(0);
  });
  it("bounds unfinished work per project without dropping admitted jobs", async () => {
    for (let index = 0; index < 8; index++) {
      await h.call("mem::reflect", { project: "/app", query: "boundaries", requestId: String(index) });
    }
    await expect(h.call("mem::reflect", { project: "/app", query: "boundaries", requestId: "overflow" })).rejects.toThrow("reflect_project_queue_capacity");
    expect(await h.kv.list(REFLECT_JOBS)).toHaveLength(8);
    expect(await h.kv.list(REFLECT_PENDING)).toHaveLength(8);
  });
  for (const total of [28, 47]) {
    it(`covers every one of ${total} legacy sources before completing the original parent`, async () => {
      const ids = Array.from({ length: total }, (_, index) => `fact-${index}`);
      for (const [index, id] of ids.entries()) await h.kv.set(KV.semantic, id, { id, fact: `Evidence ${index} ` + "é".repeat(200), confidence: 0.8, updatedAt: "2026-10-01",
        provenance: [{ session_id: `session-${index}`, project: index % 2 ? "/other" : "/app" }] });
      h.rows.set("legacy/original", { id: "original", correlationId: "original-correlation", task: "reflection", callbackProtocolVersion: 1,
        has_result: false, has_completion_intent: false, metadata: { sourceFingerprint: "original-fingerprint",
          cluster: JSON.stringify({ concepts: ["boundaries"], factIds: ids, lessonIds: [], crystalIds: [] }) } });
      const { jobId } = await h.call("mem::reflect", { legacyWorkId: "original" });
      const covered = new Set<string>();
      let completed = false;
      for (let delivery = 0; delivery < total * 4 && !completed; delivery++) {
        await h.call("mem::reflect-unit", { jobId });
        const job = await h.kv.get(REFLECT_JOBS, jobId);
        for (const call of h.provider.summarize.mock.calls) {
          if (String(call[0]).includes("You evaluate publication evidence")) continue;
          const prompt = String(call[1]);
          const payload = JSON.parse(prompt.slice(prompt.indexOf("{\"audience\"")));
          for (const fragment of payload.fragments) covered.add(fragment.sourceId);
          expect(Buffer.byteLength(JSON.stringify({ systemPrompt: call[0], userPrompt: call[1] }))).toBeLessThanOrEqual(8192);
          expect(payload.audience).toEqual({ scope: "global-orchestration" });
        }
        if (covered.size < total) expect(job.status).not.toBe("completed");
        completed = job.status === "completed";
      }
      expect(completed).toBe(true); expect([...covered].sort()).toEqual([...ids].sort());
      const providerCalls = h.provider.summarize.mock.calls.length;
      await h.call("mem::reflect-unit", { jobId });
      expect(h.provider.summarize).toHaveBeenCalledTimes(providerCalls);
      expect(h.kv.list).not.toHaveBeenCalled();
      expect(h.kv.get).not.toHaveBeenCalledWith(KV.fireworksBatchWorkItems, "original");
      expect(h.kv.retrieval.mock.calls.some(([request]) => request.action === "keyword_search" || request.action === "semantic_search")).toBe(false);
      expect((await h.kv.get(REFLECT_JOBS, jobId)).legacy).toMatchObject({ workId: "original", correlationId: "original-correlation", sourceFingerprint: "original-fingerprint", offset: total });
    });
  }
  it("pages twenty mixed-project provenance references before admitting the source to provider work", async () => {
    const ids = ["many", "other"];
    for (const id of ids) await h.kv.set(KV.semantic, id, { id, fact: `Fact ${id}`, confidence: 0.7,
      provenance: Array.from({ length: id === "many" ? 20 : 1 }, (_, index) => ({ session_id: `session-${index}`, project: index % 2 ? "/other" : "/app" })) });
    h.rows.set("legacy/provenance", { id: "provenance", correlationId: "correlation", task: "reflection", callbackProtocolVersion: 1,
      has_result: false, has_completion_intent: false, metadata: { sourceFingerprint: "fingerprint", cluster: JSON.stringify({ concepts: ["mixed"], factIds: ids, lessonIds: [], crystalIds: [] }) } });
    const { jobId } = await h.call("mem::reflect", { legacyWorkId: "provenance" });
    await h.call("mem::reflect-unit", { jobId });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect((await h.kv.get(REFLECT_JOBS, jobId)).legacy.provenanceCursor).toBe("12");
    for (let index = 0; index < 4; index++) await h.call("mem::reflect-unit", { jobId });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).status).toBe("completed");
    const receipts = [...h.rows].filter(([key]) => key.startsWith(`mem:reflect:control/${jobId}:provenance:`));
    expect(receipts.flatMap(([, value]) => (value as { provenance: unknown[] }).provenance)).toHaveLength(21);
  });
  it("orchestrates an optional-project request through scoped catalog groups", async () => {
    const { jobId } = await h.call("mem::reflect", { query: "boundaries" });
    for (let index = 0; index < 4; index++) await h.call("mem::reflect-unit", { jobId });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).status).toBe("completed");
    const searches = h.kv.retrieval.mock.calls.map(([request]) => request).filter(request => ["keyword_search", "semantic_search"].includes(request.action));
    expect(searches).toHaveLength(2); expect(searches.every(request => request.project === "/app" && request.global === undefined)).toBe(true);
    expect(h.kv.list).not.toHaveBeenCalled();
  });
  it("processes known project groups during catalog backfill and never completes partial global coverage", async () => {
    const retrieve = h.kv.retrieval.getMockImplementation()!;
    let ready = false;
    h.kv.retrieval.mockImplementation(async request => request.action === "project_catalog_page"
      ? { status: ready ? "ready" : "partial", items: [{ project: "/app" }], has_more: false }
      : retrieve(request));
    const { jobId } = await h.call("mem::reflect", { query: "boundaries" });
    for (let index = 0; index < 3; index++) await h.call("mem::reflect-unit", { jobId });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    const waiting = await h.kv.get(REFLECT_JOBS, jobId);
    expect(waiting.status).toBe("waiting_index");
    expect(waiting.error).toBe("project_catalog_incomplete");
    ready = true; waiting.nextAttemptAt = 0; await h.kv.set(REFLECT_JOBS, jobId, waiting);
    for (let index = 0; index < 4; index++) await h.call("mem::reflect-unit", { jobId });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).status).toBe("completed");
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
});
