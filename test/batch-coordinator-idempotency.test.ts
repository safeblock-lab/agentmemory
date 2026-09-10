import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { batchEffectKey, runBatchCallback, applyBatchEffect, withBatchMutationLocks } from "../src/state/batch-effects.js";
import { MetricsStore } from "../src/eval/metrics-store.js";
import { FireworksBatchClient } from "../src/providers/fireworks-batch.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { KV } from "../src/state/schema.js";
import type { BatchEffectMetadata, FireworksBatchConfig, FireworksBatchJob, FireworksBatchWorkItem } from "../src/types.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));

const config: FireworksBatchConfig = {
  enabled: true, accountId: "test", apiKey: "test", model: "model", timeoutMs: 1000,
  minBatchItems: 1, maxWaitMs: 0, maxBatchItems: 10, maxRequestChars: 10000,
  maxRequestBytes: 10000, maxResponseBytes: 10000, maxResultChars: 10000,
  maxConcurrency: 1, maxAttempts: 2, retryBaseMs: 1, retryMaxMs: 10,
  pollIntervalMs: 0, pollMaxIntervalMs: 10, recoveryStaleMs: 1000, maxQueuedItems: 10,
};

describe("parser failure recovery", () => {
  async function seed(count = 1) {
    const h = effectHarness(), now = new Date().toISOString(), jobId = "fwbjob-parser";
    const lastError = "batch result row 1 had no valid response content";
    const item: FireworksBatchWorkItem = {
      id: "work-parser", customId: "row", correlationId: "row", batchJobId: jobId,
      task: "graph_extraction", model: "model", systemPrompt: "system", userPrompt: "user", maxTokens: 128,
      callbackProtocolVersion: 1, state: "dead-letter", attempts: 3, nextAttemptAt: now,
      createdAt: now, updatedAt: now, deadLetteredAt: now, lastError,
    };
    const job: FireworksBatchJob = {
      id: jobId, remoteJobId: jobId, inputDatasetId: `${jobId}-input`, outputDatasetId: `${jobId}-output`,
      model: "model", task: "graph_extraction", workItemIds: [item.id], state: "dead-letter",
      attempts: 3, callbackAttempts: 2, reconciling: true, nextAttemptAt: now, createdAt: now, updatedAt: now, lastError,
    };
    const items = Array.from({ length: count }, (_, index) => index === 0 ? item : {
      ...item, id: `${item.id}-${index}`, customId: `row-${index}`, correlationId: `row-${index}`,
    });
    job.workItemIds = items.map((work) => work.id);
    for (const work of items) await h.kv.set(KV.fireworksBatchWorkItems, work.id, work);
    await h.kv.set(KV.fireworksBatchJobs, jobId, job);
    for (const scope of [KV.fireworksBatchActiveWork, KV.fireworksBatchActiveJobs]) await h.kv.set(scope, "current", { version: 1, ids: [], updatedAt: now });
    await h.kv.set(KV.fireworksBatchActiveJobs, "remote-recovery-v1", { version: 1, pendingJobIds: [], discoveredAt: now, updatedAt: now, completedAt: now, legacyReconciliationVersion: 1 });
    const transport = {
      createDataset: vi.fn(async () => {}), uploadDataset: vi.fn(async () => {}), submitJob: vi.fn(async () => ({ remoteJobId: "forbidden" })),
      listRecentJobIds: vi.fn(async () => [jobId]), getJobStatus: vi.fn(async () => ({ state: "COMPLETED" })),
      downloadResults: vi.fn(async () => items.map((work, index) => JSON.stringify({ custom_id: work.customId, response: { choices: [{ message: { content: `<entity type="concept" name="A${index}"/><entity type="concept" name="B${index}"/><relationship type="related_to" source="A${index}" target="B${index}" weight="1"/>` } }] } })).join("\n")),
    };
    registerGraphFunction(h.sdk as never, h.kv, {} as never);
    const completed = vi.fn(async (work: FireworksBatchWorkItem, content: string) => {
      const result = await h.call("mem::graph-extract", {
        observations: [{ id: "obs", title: "title", narrative: "body", facts: [], concepts: [], files: [], type: "discovery" }],
        batchResponse: content, batchEffectKey: batchEffectKey(work.id),
      });
      if (result.success !== true) throw new Error("Graph callback failed");
    });
    return { h, item, items, job, transport, completed, create: () => new FireworksBatchCoordinator(h.kv, config, transport, completed) };
  }

  it.each([undefined, KV.fireworksBatchWorkItems, KV.fireworksBatchJobs, KV.fireworksBatchActiveWork])("recovers once after restart at %s", async (scope) => {
    const s = await seed();
    if (scope) {
      s.h.crash(scope, true);
      await expect(s.create().process()).rejects.toThrow();
    }
    await s.create().process(); await s.create().process();
    expect(s.transport.submitJob).not.toHaveBeenCalled();
    expect(s.transport.createDataset).not.toHaveBeenCalled();
    expect(s.transport.downloadResults).toHaveBeenCalledTimes(1);
    expect(s.completed).toHaveBeenCalledTimes(1);
    expect(await s.h.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await s.h.kv.list(KV.graphEdges)).toHaveLength(1);
    expect(await s.h.kv.get(KV.graphSnapshot, "current")).toMatchObject({ stats: { totalNodes: 2, totalEdges: 1 } });
    expect(await s.h.kv.list(KV.audit)).toHaveLength(1);
    expect(await s.h.kv.get(KV.batchCallbacks, `graph:${batchEffectKey(s.item.id)}`)).toMatchObject({ state: "completed" });
    expect(await s.h.kv.get(KV.fireworksBatchJobs, s.job.id)).toMatchObject({ state: "completed", parserRecovery: { activatedAt: expect.any(String) } });
    for (const scope of [KV.fireworksBatchActiveWork, KV.fireworksBatchActiveJobs]) expect(await s.h.kv.get(scope, "current")).toMatchObject({ ids: [] });
    expect(await s.h.kv.get(KV.fireworksBatchActiveJobs, "remote-recovery-v1")).toMatchObject({ legacyReconciliationVersion: 2, completedAt: expect.any(String) });
  });

  it.each(["receipt", "active", "intent", "result", "identity", "different-error", "legacy", "already-recovered", "missing-remote", "foreign-remote", "foreign-output", "foreign-account", "foreign-name", "foreign-request"])("blocks unsafe recovery: %s", async (reason) => {
    const s = await seed();
    if (reason === "receipt") await s.h.kv.set(KV.batchCallbacks, `graph:${batchEffectKey(s.item.id)}`, { state: "started" });
    if (reason === "active") await s.h.kv.set(KV.batchCallbacks, "active:graph", { activeKey: batchEffectKey(s.item.id) });
    if (reason === "intent") s.item.completionIntent = { key: batchEffectKey(s.item.id), resultHash: "hash" };
    if (reason === "result") s.item.result = { customId: "row", content: "prior", receivedAt: s.item.createdAt };
    if (reason === "identity") s.item.batchJobId = "another-job";
    if (reason === "different-error") s.item.lastError = "remote failed";
    if (reason === "legacy") delete s.item.callbackProtocolVersion;
    if (reason === "already-recovered") s.job.parserRecovery = { startedAt: s.job.createdAt, activatedAt: s.job.createdAt };
    if (reason === "missing-remote") delete s.job.remoteJobId;
    if (reason === "foreign-remote") s.job.remoteJobId = "fwbjob-foreign";
    if (reason === "foreign-output") s.job.outputDatasetId = "fwbjob-foreign-output";
    if (reason === "foreign-account") s.job.outputDatasetId = `accounts/foreign/datasets/${s.job.id}-output`;
    if (reason === "foreign-name") s.job.remoteJobName = "accounts/test/batchInferenceJobs/fwbjob-foreign";
    if (reason === "foreign-request") s.job.requestedRemoteJobId = "fwbjob-foreign";
    await s.h.kv.set(KV.fireworksBatchWorkItems, s.item.id, s.item);
    await s.h.kv.set(KV.fireworksBatchJobs, s.job.id, s.job);
    await s.create().process(); await s.create().process();
    expect(s.transport.downloadResults).not.toHaveBeenCalled(); expect(s.transport.submitJob).not.toHaveBeenCalled(); expect(s.completed).not.toHaveBeenCalled();
    expect(await s.h.kv.get(KV.fireworksBatchJobs, s.job.id)).toMatchObject({ state: "dead-letter" });
    expect((await s.h.kv.get<FireworksBatchJob>(KV.fireworksBatchJobs, s.job.id))?.parserRecovery).toEqual(s.job.parserRecovery);
  });

  it("recovers seven owned rows after a partial reactivation crash with exact graph effects", async () => {
    const s = await seed(7);
    s.job.outputDatasetId = `accounts/test/datasets/${s.job.id}-output`;
    s.job.remoteJobName = `accounts/test/batchInferenceJobs/${s.job.id}`;
    await s.h.kv.set(KV.fireworksBatchJobs, s.job.id, s.job);
    s.h.crash(KV.fireworksBatchWorkItems, true, 4);
    await expect(s.create().process()).rejects.toThrow();
    expect(s.completed).not.toHaveBeenCalled();
    const interrupted = await s.h.kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems);
    expect(interrupted.filter((work) => work.state === "polling")).toHaveLength(4);
    expect(interrupted.filter((work) => work.state === "dead-letter")).toHaveLength(3);
    await s.create().process();
    const firstSnapshot = await s.h.kv.get(KV.graphSnapshot, "current");
    expect(firstSnapshot).toMatchObject({ stats: { totalNodes: 14, totalEdges: 7 } });
    await s.create().process();
    expect(await s.h.kv.get(KV.graphSnapshot, "current")).toEqual(firstSnapshot);
    expect(s.completed).toHaveBeenCalledTimes(7);
    expect(s.transport.downloadResults).toHaveBeenCalledExactlyOnceWith(s.job.outputDatasetId);
    expect(s.transport.submitJob).not.toHaveBeenCalled();
    expect(s.transport.createDataset).not.toHaveBeenCalled();
    expect(s.transport.uploadDataset).not.toHaveBeenCalled();
    expect(await s.h.kv.list(KV.graphNodes)).toHaveLength(14);
    expect(await s.h.kv.list(KV.graphEdges)).toHaveLength(7);
    expect(await s.h.kv.list(KV.audit)).toHaveLength(7);
    for (const work of s.items) {
      expect(await s.h.kv.get(KV.batchCallbacks, `graph:${batchEffectKey(work.id)}`)).toMatchObject({ state: "completed" });
      expect(await s.h.kv.get(KV.fireworksBatchWorkItems, work.id)).toMatchObject({ state: "completed" });
    }
    expect(await s.h.kv.get(KV.fireworksBatchJobs, s.job.id)).toMatchObject({ state: "completed" });
    for (const scope of [KV.fireworksBatchActiveWork, KV.fireworksBatchActiveJobs]) expect(await s.h.kv.get(scope, "current")).toMatchObject({ ids: [] });
  });
});

describe("coordinator durable completion intent", () => {
  it.each(["before-effect", "after-effect", "before-terminal"])("recovers %s without duplicated effects or usage", async (point) => {
    const h = effectHarness();
    const metrics = new MetricsStore(h.kv);
    let first = true;
    const transport = {
      async createDataset() {}, async uploadDataset() {},
      async submitJob() { return { remoteJobId: "remote" }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() { return JSON.stringify({ custom_id: "row", response: { body: { choices: [{ message: { content: "result" } }], usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 } } } }); },
    };
    const create = () => new FireworksBatchCoordinator(h.kv, config, transport, async (item) => {
      expect(item.completionIntent?.key).toBe(batchEffectKey(item.id));
      if (first && point === "before-effect") h.crash("effect");
      if (first && point === "after-effect") h.crash("effect", true);
      await runBatchCallback(h.kv, "test", batchEffectKey(item.id), async (_resuming, admit) => {
        await admit();
        await applyBatchEffect<BatchEffectMetadata & { count: number }>(h.kv, "effect", "counter", batchEffectKey(item.id), (current) => ({ count: (current?.count ?? 0) + 1 }));
        return { success: true };
      });
      if (first && point === "before-terminal") h.crash(KV.fireworksBatchWorkItems);
    }, (item, usage) => metrics.recordLlmUsage("reflection", "fireworks-batch", item.model, usage, batchEffectKey(item.id)));
    const coordinator = create();
    await coordinator.enqueue({ correlationId: "row", task: "reflection", systemPrompt: "private system", userPrompt: "private prompt" });
    await coordinator.process();
    let jobs = await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs);
    expect(jobs[0].state).toBe("polling");
    expect((await h.kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems))[0].state).not.toBe("dead-letter");
    first = false;
    await h.kv.set(KV.fireworksBatchJobs, jobs[0].id, { ...jobs[0], attempts: config.maxAttempts, nextAttemptAt: new Date(0).toISOString() });
    await create().process();
    expect(await h.kv.get("effect", "counter")).toMatchObject({ count: 1 });
    jobs = await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs);
    expect(jobs[0].state).toBe("completed");
    const item = (await h.kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems))[0];
    expect(item.state).toBe("completed");
    expect(JSON.stringify(item.completionIntent)).not.toMatch(/private|result"/);
    expect(await metrics.get("llm:reflection:fireworks-batch:model")).toMatchObject({ totalCalls: 1, totalTokens: 8 });
    expect(await h.kv.get(KV.fireworksBatchActiveWork, "current")).toMatchObject({ ids: [] });
  });
});

describe("bounded callback recovery and job publication", () => {
  const makeTransport = () => ({
    createDataset: vi.fn(async () => {}), uploadDataset: vi.fn(async () => {}),
    submitJob: vi.fn(async () => ({ remoteJobId: "remote" })),
    getJobStatus: vi.fn(async () => ({ state: "COMPLETED" })),
    downloadResults: vi.fn(async () => JSON.stringify({ custom_id: "row", response: { body: { choices: [{ message: { content: "result" } }], usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 } } } })),
  });
  it.each([false, true])("bounds success:false across restart and preserves partial receipts, terminalFailure=%s", async (terminalFailure) => {
    const h = effectHarness(), transport = makeTransport(), metrics = new MetricsStore(h.kv);
    let calls = 0;
    const create = () => new FireworksBatchCoordinator(h.kv, config, transport, async (item) => {
      calls++;
      expect((await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0].callbackAttempts).toBe(calls);
      await runBatchCallback(h.kv, "graph", batchEffectKey(item.id), async (_, admit) => {
        await admit();
        await applyBatchEffect<BatchEffectMetadata & { count: number }>(h.kv, "effect", "counter", batchEffectKey(item.id), (current) => ({ count: (current?.count ?? 0) + 1 }));
        return { success: false };
      });
      if (terminalFailure && calls === config.maxAttempts) h.crash(KV.fireworksBatchWorkItems);
      return { success: false as const, error: "retryable receiver failure" };
    }, (item, usage) => metrics.recordLlmUsage("reflection", "fireworks-batch", item.model, usage, batchEffectKey(item.id)));
    await create().enqueue({ correlationId: "row", task: "reflection", systemPrompt: "system", userPrompt: "user" });
    for (let i = 0; i < 5; i++) {
      const job = (await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0];
      if (job) await h.kv.set(KV.fireworksBatchJobs, job.id, { ...job, nextAttemptAt: new Date(0).toISOString() });
      try { await create().process(); } catch (error) { expect(terminalFailure).toBe(true); }
    }
    expect(calls).toBe(config.maxAttempts);
    expect(await h.kv.get("effect", "counter")).toMatchObject({ count: 1 });
    expect((await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0]).toMatchObject({ state: "dead-letter", callbackAttempts: config.maxAttempts });
    expect((await h.kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems))[0].state).toBe("dead-letter");
    expect(await h.kv.get(KV.fireworksBatchActiveJobs, "current")).toMatchObject({ ids: [] });
    expect(await h.kv.get(KV.batchCallbacks, "active:graph")).toMatchObject({ state: "started" });
    expect(await metrics.get("llm:reflection:fireworks-batch:model")).toMatchObject({ totalCalls: 1 });
    await expect(withBatchMutationLocks(h.kv, async () => true)).rejects.toThrow("recovered");
  });

  it.each([KV.fireworksBatchActiveJobs, KV.fireworksBatchJobs].flatMap((scope) => [false, true].map((after) => ({ scope, after }))))("recovers publication failure at $scope, after=$after without orphan/duplicate submission", async ({ scope, after }) => {
    const h = effectHarness(), transport = makeTransport(), callback = vi.fn(async () => {});
    const create = () => new FireworksBatchCoordinator(h.kv, config, transport, callback);
    await create().enqueue({ correlationId: "row", task: "reflection", systemPrompt: "system", userPrompt: "user" });
    h.crash(scope, after);
    await expect(create().process()).rejects.toThrow();
    const jobs = await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs);
    const index = await h.kv.get<{ ids: string[] }>(KV.fireworksBatchActiveJobs, "current");
    for (const job of jobs) expect(index?.ids).toContain(job.id);
    await create().process(); await create().process();
    expect(await h.kv.list(KV.fireworksBatchJobs)).toHaveLength(1);
    expect(transport.submitJob.mock.calls.length).toBeLessThanOrEqual(1);
    expect(callback.mock.calls.length).toBeLessThanOrEqual(1);
    expect((await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0].state).toMatch(/completed|dead-letter/);
  });

  it("recovers a lost submit response using the persisted canonical batchInferenceJobId", async () => {
    const h = effectHarness(), callback = vi.fn(async () => {}), retryConfig = { ...config, maxAttempts: 4 };
    const client = new FireworksBatchClient(retryConfig);
    let requestedId: string | null = null, statusCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: string | URL | Request) => {
      const url = new URL(String(input));
      if (url.pathname.endsWith("/batchInferenceJobs")) {
        requestedId = url.searchParams.get("batchInferenceJobId");
        const stored = (await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0];
        expect(stored.requestedRemoteJobId).toBe(requestedId);
        expect(stored.submitAttemptedAt).toBeTruthy();
        throw new Error("network response lost after remote acceptance");
      }
      expect(url.pathname.endsWith(`/batchInferenceJobs/${requestedId}`)).toBe(true);
      if (++statusCalls === 1) throw new Error("network temporarily unavailable");
      return new Response(JSON.stringify({ state: "COMPLETED", name: `accounts/test/batchInferenceJobs/${requestedId}` }), { status: 200, headers: { "Content-Type": "application/json" } });
    }));
    try {
      const transport = { ...makeTransport(), submitJob: client.submitJob.bind(client), getJobStatus: client.getJobStatus.bind(client) };
      const create = () => new FireworksBatchCoordinator(h.kv, retryConfig, transport, callback);
      await create().enqueue({ correlationId: "row", task: "reflection", systemPrompt: "system", userPrompt: "user" });
      await create().process();
      const stored = (await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0];
      expect(requestedId).toBe(stored.id);
      expect(stored).toMatchObject({ state: "polling", requestedRemoteJobId: requestedId });
      await h.kv.set(KV.fireworksBatchJobs, stored.id, { ...stored, nextAttemptAt: new Date(0).toISOString() });
      await create().process();
      expect(callback).toHaveBeenCalledTimes(1);
      expect((await h.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs))[0]).toMatchObject({ state: "completed", remoteJobId: requestedId });
    } finally { vi.unstubAllGlobals(); }
  });

  it("resumes polling-exhausted reconciliation after a crash without resubmitting", async () => {
    const h = effectHarness();
    const now = new Date().toISOString();
    const itemId = "fwbwork-legacy-recovery";
    const jobId = "fwbjob-legacy-recovery";
    await h.kv.set(KV.fireworksBatchWorkItems, itemId, {
      id: itemId, customId: "legacy-recovery", correlationId: "legacy-recovery",
      task: "reflection", model: "model", systemPrompt: "system", userPrompt: "user", maxTokens: 128,
      state: "dead-letter", attempts: config.maxAttempts, nextAttemptAt: now,
      createdAt: now, updatedAt: now, lastError: "batch polling attempts exhausted", deadLetteredAt: now,
    });
    await h.kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId, remoteJobId: jobId, inputDatasetId: "input", outputDatasetId: "output",
      model: "model", task: "reflection", workItemIds: [itemId], state: "dead-letter",
      attempts: config.maxAttempts, nextAttemptAt: now, createdAt: now, updatedAt: now,
      lastError: "batch polling attempts exhausted",
    });
    await h.kv.set(KV.fireworksBatchActiveWork, "current", { version: 1 as const, ids: [], updatedAt: now });
    await h.kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [], updatedAt: now });
    const submitJob = vi.fn(async () => ({ remoteJobId: "must-not-submit" }));
    const completed = vi.fn(async () => {});
    const transport = {
      async createDataset() {}, async uploadDataset() {}, submitJob,
      async listRecentJobIds() { return [jobId]; },
      async getJobStatus() { return { state: "COMPLETED", remoteJobId: jobId }; },
      async downloadResults() { return JSON.stringify({ custom_id: "legacy-recovery", response: { body: { choices: [{ message: { content: "recovered" } }] } } }); },
    };
    const create = () => new FireworksBatchCoordinator(h.kv, config, transport, completed);

    h.crash(KV.fireworksBatchWorkItems);
    await expect(create().process()).rejects.toThrow("injected crash before write");
    await create().process();

    expect(submitJob).not.toHaveBeenCalled();
    expect(completed).toHaveBeenCalledTimes(1);
    await expect(h.kv.get<FireworksBatchJob>(KV.fireworksBatchJobs, jobId))
      .resolves.toMatchObject({ state: "completed", legacyReconciliationAt: expect.any(String) });
  });

  it("does not guess an ambiguous legacy remote identity", async () => {
    const h = effectHarness(), transport = makeTransport(), callback = vi.fn(async () => {});
    const coordinator = new FireworksBatchCoordinator(h.kv, config, transport, callback);
    const queued = await coordinator.enqueue({ correlationId: "row", task: "reflection", systemPrompt: "system", userPrompt: "user" });
    const item = (await h.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, queued.workItemId!))!;
    await h.kv.set(KV.fireworksBatchWorkItems, item.id, { ...item, state: "submitted" });
    await h.kv.set(KV.fireworksBatchJobs, "unknown", { id: "unknown", workItemIds: [item.id], state: "polling", attempts: 0, submitAttemptedAt: new Date(0).toISOString(), nextAttemptAt: new Date(0).toISOString() });
    await h.kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1, ids: ["unknown"] });
    await coordinator.process();
    expect(transport.getJobStatus).not.toHaveBeenCalled(); expect(callback).not.toHaveBeenCalled();
    expect(await h.kv.get(KV.fireworksBatchJobs, "unknown")).toMatchObject({ state: "dead-letter", lastError: expect.stringContaining("no proven remote identity") });
  });
});
