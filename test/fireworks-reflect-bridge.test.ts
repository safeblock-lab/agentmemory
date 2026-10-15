import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { KV } from "../src/state/schema.js";
import { REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import type { FireworksBatchConfig } from "../src/types.js";

const config = { enabled: true, accountId: "test", apiKey: "test", model: "test", minBatchItems: 1, maxWaitMs: 0,
  maxBatchItems: 12, maxRequestChars: 65536, maxRequestBytes: 65536, maxResultChars: 65536,
  maxResponseBytes: 65536, maxConcurrency: 1, maxAttempts: 3, maxQueuedItems: 32,
  pollIntervalMs: 60000, pollMaxIntervalMs: 60000, pollDeadlineMs: 86400000 } as FireworksBatchConfig;

describe("Grouped reflect original-work submission gate", () => {
  beforeEach(() => vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0"));
  afterEach(() => vi.unstubAllEnvs());
  function fixture() {
    const h = reflectHarness(); const retrieve = h.kv.retrieval.getMockImplementation()!;
    h.kv.retrieval.mockImplementation(async request => {
      if (request.action === "batch_work_bridge_ack") {
        await h.kv.set(REFLECT_CONTROL, `legacy-ack:${request.key}`, { acknowledged: true, receiptKey: request.receipt_key });
        return { acknowledged: true };
      }
      if (request.action === "batch_work_payload") {
        return { items: request.keys.map((key: string) => { const value = h.rows.get(`${KV.fireworksBatchWorkItems}/${key}`);
          return { key, value: structuredClone(value), version: "1", fingerprint: batchEffectKey(JSON.stringify(value)) }; }), deferred: [] };
      }
      if (request.action === "batch_work_submission_patch") {
        for (const ref of request.items) {
          const current: any = h.rows.get(`${KV.fireworksBatchWorkItems}/${ref.key}`);
          if (batchEffectKey(JSON.stringify(current)) !== ref.expected_fingerprint) return { applied: false };
          await h.kv.set(KV.fireworksBatchWorkItems, ref.key, { ...current, state: "submitted", batchJobId: request.batch_job_id, updatedAt: request.updated_at });
        }
        return { applied: true };
      }
      return retrieve(request);
    });
    const transport = { createDataset: vi.fn(), uploadDataset: vi.fn(), submitJob: vi.fn(async () => ({ remoteJobId: "test-job" })),
      getJobStatus: vi.fn(async () => ({ state: "RUNNING" })), downloadResults: vi.fn() };
    const coordinator = new FireworksBatchCoordinator(h.kv as never, config, transport, vi.fn());
    return { h, transport, coordinator };
  }
  async function legacyFixture() {
    const f = fixture(); const { h } = f; const now = new Date().toISOString();
    h.provider.summarize.mockImplementation(async (system: string, prompt: string) => {
      if (!system.startsWith("You evaluate publication evidence.")) {
        return '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
      }
      const evidence = JSON.parse(prompt);
      expect(evidence.workflow).toBe("reflection");
      expect(evidence.sources).toHaveLength(2);
      expect(evidence.sources.map((source: { key: string }) => source.key).sort()).toEqual(["a", "b"]);
      const candidate = JSON.parse(evidence.candidate);
      expect(candidate.sourceLineage).toHaveLength(2);
      expect(candidate.generationPrompt).toContain("boundaries");
      return JSON.stringify({ verdict: "supported", reason: "The complete originals support validation at boundaries." });
    });
    h.rows.set("legacy/original", { id: "original", correlationId: "correlation", task: "reflection", callbackProtocolVersion: 1,
      has_result: false, has_completion_intent: false, metadata: { sourceFingerprint: "fingerprint", cluster: JSON.stringify({ concepts: ["boundaries"], factIds: ["a", "b"], lessonIds: [], crystalIds: [] }) } });
    for (const id of ["a", "b"]) await h.kv.set(KV.semantic, id, { id, fact: `Validate input at boundaries ${id}`, confidence: 0.8 });
    const original = { id: "original", correlationId: "correlation", customId: "correlation", task: "reflection", model: "test", callbackProtocolVersion: 1,
      systemPrompt: "Original protected prompt", userPrompt: "Original source content", metadata: { sourceFingerprint: "fingerprint", retained: "private provenance" },
      maxTokens: 10, state: "queued", attempts: 0, nextAttemptAt: now, createdAt: now, updatedAt: now, customRetainedField: "preserve" };
    await h.kv.set(KV.fireworksBatchWorkItems, "original", original);
    await h.kv.set(KV.fireworksBatchActiveWork, "current", { version: 1, ids: ["original"], updatedAt: now });
    const { jobId } = await h.call("mem::reflect", { legacyWorkId: "original" });
    return { ...f, original, jobId };
  }
  it("holds original work before provider hydration and adopts only verified complete continuation", async () => {
    const { h, transport, coordinator, original, jobId } = await legacyFixture();
    await coordinator.process();
    expect(transport.submitJob).not.toHaveBeenCalled();
    expect(h.kv.retrieval.mock.calls.some(([r]) => r.action === "batch_work_payload")).toBe(false);
    expect(await h.kv.get(REFLECT_CONTROL, "legacy-ack:original")).toBeNull();
    for (let index = 0; index < 4; index++) await h.call("mem::reflect-unit", { jobId });
    expect((await h.kv.get(REFLECT_JOBS, jobId)).status).toBe("completed");
    await coordinator.process();
    expect(await h.kv.get(REFLECT_CONTROL, "legacy-adoption:original")).toMatchObject({ verified: true, requiredSources: 2, completedSources: 2 });
    expect(h.kv.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: "batch_work_bridge_ack", key: "original", expected_version: "1", expected_fingerprint: "legacy-version-fingerprint" }));
    expect(await h.kv.get(KV.fireworksBatchWorkItems, "original")).toEqual(original);
    expect((await h.kv.get(KV.fireworksBatchActiveWork, "current")).ids).toEqual([]);
    await coordinator.repairKnownWorkItem("original");
    await coordinator.process();
    expect(transport.submitJob).not.toHaveBeenCalled();
  });
  it("does not acknowledge coverage whose effect receipt is absent", async () => {
    const { h, coordinator, jobId } = await legacyFixture();
    for (let index = 0; index < 4; index++) await h.call("mem::reflect-unit", { jobId });
    const result = await h.kv.get(REFLECT_CONTROL, `${jobId}:result:0`);
    await h.kv.delete(KV.batchCallbacks, `reflect:${result.effectKey}`);
    await coordinator.process();
    expect(await h.kv.get(REFLECT_CONTROL, "legacy-ack:original")).toBeNull();
  });
  it("patches submission state without replacing unknown fields from the provider projection", async () => {
    const { h, transport, coordinator } = fixture(); const now = new Date().toISOString();
    const work = { id: "queued", correlationId: "queued", customId: "queued", callbackProtocolVersion: 1, task: "reflection", model: "test",
      systemPrompt: "system", userPrompt: "user", metadata: { unknownPrivateMetadata: "keep" }, unknownField: "keep",
      maxTokens: 10, state: "queued", attempts: 0, nextAttemptAt: now, createdAt: now, updatedAt: now };
    await h.kv.set(KV.fireworksBatchWorkItems, work.id, work);
    await h.kv.set(KV.fireworksBatchActiveWork, "current", { version: 1, ids: [work.id], updatedAt: now });
    await coordinator.process();
    expect(transport.submitJob).toHaveBeenCalledTimes(1);
    expect(await h.kv.get(KV.fireworksBatchWorkItems, work.id)).toMatchObject({ state: "submitted", unknownField: "keep", metadata: work.metadata });
    expect(h.kv.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: "batch_work_payload", keys: ["queued"], max_items: 12, max_bytes: 65536 }));
    expect(h.kv.retrieval).toHaveBeenCalledWith(expect.objectContaining({ action: "batch_work_submission_patch" }));
  });
});
