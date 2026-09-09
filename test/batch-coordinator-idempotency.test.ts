import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { batchEffectKey, runBatchCallback, applyBatchEffect, withBatchMutationLocks } from "../src/state/batch-effects.js";
import { MetricsStore } from "../src/eval/metrics-store.js";
import { FireworksBatchClient } from "../src/providers/fireworks-batch.js";
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
