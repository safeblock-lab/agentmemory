import { afterEach, describe, expect, it, vi } from "vitest";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import type { FireworksBatchConfig, FireworksBatchWorkItem } from "../src/types.js";
import { FireworksBatchClient, FireworksBatchError, type FireworksBatchTransport } from "../src/providers/fireworks-batch.js";

function createKv(options: {
  rejectHistoricalLists?: boolean;
  onSet?: (scope: string, key: string | undefined, value: unknown, store: Map<string, unknown>) => void;
} = {}): StateKV {
  const store = new Map<string, unknown>();
  const sdk = {
    async trigger(input: { function_id: string; payload: { scope: string; key?: string; value?: unknown } }) {
      const { scope, key, value } = input.payload;
      if (input.function_id === "state::get") return store.get(`${scope}:${key}`) ?? null;
      if (input.function_id === "state::set") {
        options.onSet?.(scope, key, value, store);
        store.set(`${scope}:${key}`, value);
        return value;
      }
      if (input.function_id === "state::delete") {
        store.delete(`${scope}:${key}`);
        return undefined;
      }
      if (input.function_id === "state::list") {
        if (options.rejectHistoricalLists && (
          scope === KV.fireworksBatchWorkItems
          || scope === KV.fireworksBatchJobs
          || scope === KV.fireworksBatchFingerprints
        )) {
          throw new Error("historical state listing disabled");
        }
        return [...store.entries()]
          .filter(([storedKey]) => storedKey.startsWith(`${scope}:`))
          .map(([, storedValue]) => storedValue);
      }
      throw new Error(`unexpected function ${input.function_id}`);
    },
  };
  return new StateKV(sdk as never);
}

const config: FireworksBatchConfig = {
  enabled: true,
  accountId: "test-account",
  apiKey: "test-key",
  model: "accounts/test/models/test",
  timeoutMs: 1_000,
  minBatchItems: 1,
  maxWaitMs: 60_000,
  maxBatchItems: 10,
  maxRequestChars: 10_000,
  maxRequestBytes: 10_000,
  maxResponseBytes: 16 * 1024 * 1024,
  maxResultChars: 120_000,
  maxConcurrency: 1,
  maxAttempts: 3,
  retryBaseMs: 1,
  retryMaxMs: 10,
  pollIntervalMs: 0,
  pollMaxIntervalMs: 10,
  pollDeadlineMs: 24 * 60 * 60_000,
  recoveryStaleMs: 1_000,
  maxQueuedItems: 10,
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("FireworksBatchClient", () => {
  it("keeps HTTP error diagnostics bounded to safe code and message fields", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
      error: {
        code: "INVALID_ARGUMENT",
        message: "bad request",
        details: [{ prompt: "prompt-secret", apiKey: "key-secret" }],
      },
      prompt: "prompt-secret",
      jsonl: "jsonl-secret",
    }), { status: 400, headers: { "content-type": "application/json" } })));

    const client = new FireworksBatchClient(config);
    let caught: unknown;
    try {
      await client.createDataset("input", 1);
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(FireworksBatchError);
    const error = caught as FireworksBatchError;
    expect(error.operation).toBe("dataset-create");
    expect(error.status).toBe(400);
    expect(error.retryable).toBe(false);
    expect(error.diagnostic).toEqual({ code: "INVALID_ARGUMENT", message: "bad request" });
    expect(error.message).not.toContain("prompt-secret");
    expect(error.message).not.toContain("key-secret");
    expect(error.message).not.toContain("jsonl-secret");
  });
});

describe("FireworksBatchCoordinator", () => {
  it.each(["direct", "body"])("accepts %s response content and usage", async (envelope) => {
    const kv = createKv(), completed = vi.fn(async () => {}), usage = vi.fn(async () => {});
    const body = { choices: [{ message: { content: "valid" } }], usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 } };
    const coordinator = new FireworksBatchCoordinator(kv, config, {
      async createDataset() {}, async uploadDataset() {}, async submitJob() { return { remoteJobId: "remote" }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() { return JSON.stringify({ custom_id: "row", response: envelope === "body" ? { body } : body }); },
    }, completed, usage);
    await coordinator.enqueue({ correlationId: "row", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process(); await coordinator.process();
    expect(completed).toHaveBeenCalledTimes(1);
    expect(completed.mock.calls[0]).toEqual([expect.any(Object), "valid"]);
    expect(usage).toHaveBeenCalledWith(expect.any(Object), { inputTokens: 5, outputTokens: 3, totalTokens: 8, responseChars: 0 });
  });

  it.each([null, [], "invalid", {}, { choices: [] }, { choices: [{ message: { content: 3 } }] }])("rejects invalid explicit body without direct fallback: %j", async (body) => {
    const kv = createKv(), completed = vi.fn(async () => {}), usage = vi.fn(async () => {});
    const coordinator = new FireworksBatchCoordinator(kv, { ...config, maxAttempts: 1 }, {
      async createDataset() {}, async uploadDataset() {}, async submitJob() { return { remoteJobId: "remote" }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() { return JSON.stringify({ custom_id: "row", response: { body, choices: [{ message: { content: "must not fall back" } }], usage: { total_tokens: 8 } } }); },
    }, completed, usage);
    await coordinator.enqueue({ correlationId: "row", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process();
    expect(completed).not.toHaveBeenCalled(); expect(usage).not.toHaveBeenCalled();
    expect(await kv.list(KV.fireworksBatchJobs)).toEqual([expect.objectContaining({ state: "dead-letter", lastError: "batch result row 1 had no valid response content" })]);
  });

  it("publishes work after its record and journals job IDs before their records", async () => {
    const missingRecords: string[] = [];
    const kv = createKv({
      onSet(scope, _key, value, store) {
        if (scope === KV.fireworksBatchJobs) {
          const index = store.get(`${KV.fireworksBatchActiveJobs}:current`) as { ids?: string[] } | undefined;
          if (!index?.ids?.includes(_key!)) missingRecords.push(_key!);
          return;
        }
        if (scope !== KV.fireworksBatchActiveWork) return;
        const ids = (value as { ids?: string[] }).ids ?? [];
        const canonicalScope = scope === KV.fireworksBatchActiveWork
          ? KV.fireworksBatchWorkItems
          : KV.fireworksBatchJobs;
        for (const id of ids) {
          if (!store.has(`${canonicalScope}:${id}`)) missingRecords.push(id);
        }
      },
    });
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-persist-first" }; },
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.enqueue({
      correlationId: "persist-first",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(missingRecords).toEqual([]);
  });

  it("recovers an enqueue journal left before active-index publication", async () => {
    let failActiveIndex = true;
    const kv = createKv({
      onSet(scope) {
        if (failActiveIndex && scope === KV.fireworksBatchActiveWork) {
          failActiveIndex = false;
          throw new Error("simulated active-index crash");
        }
      },
    });
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, minBatchItems: 2 },
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "unused" }; },
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await expect(coordinator.enqueue({
      correlationId: "wal-recovery",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    })).rejects.toThrow("simulated active-index crash");
    await expect(kv.get(KV.fireworksBatchEnqueueIntents, "current")).resolves.toMatchObject({
      intents: [expect.objectContaining({ item: expect.objectContaining({ correlationId: "wal-recovery" }) })],
    });

    failActiveIndex = false;
    await coordinator.process();

    await expect(kv.get<{ ids: string[] }>(KV.fireworksBatchActiveWork, "current"))
      .resolves.toMatchObject({ ids: [expect.any(String)] });
    await expect(kv.get(KV.fireworksBatchEnqueueIntents, "current"))
      .resolves.toMatchObject({ intents: [] });
  });

  it("discovers and reconciles a pre-index remote job without listing local history", async () => {
    const kv = createKv({ rejectHistoricalLists: true });
    const jobId = "fwbjob-historical";
    const workItemId = "fwbwork-historical";
    const now = new Date().toISOString();
    await kv.set(KV.fireworksBatchWorkItems, workItemId, {
      callbackProtocolVersion: 1,
      id: workItemId,
      customId: "historical-1",
      correlationId: "historical-1",
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "system",
      userPrompt: "user",
      maxTokens: 512,
      state: "submitted",
      attempts: 1,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: jobId,
      inputDatasetId: `${jobId}-input`,
      outputDatasetId: `${jobId}-output`,
      model: config.model!,
      task: "graph_extraction",
      workItemIds: [workItemId],
      state: "polling",
      attempts: 1,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "unused" }; },
        async listRecentJobIds() { return [jobId, "unrelated-job"]; },
        async getJobStatus() { return { state: "JOB_STATE_COMPLETED" }; },
        async downloadResults() {
          return JSON.stringify({
            custom_id: "historical-1",
            response: { body: { choices: [{ message: { content: "<graph />" } }] } },
          });
        },
      },
      async (_item, content) => { applied.push(content); },
    );

    await coordinator.process();
    await coordinator.process();

    expect(applied).toEqual(["<graph />"]);
    await expect(kv.get(KV.fireworksBatchJobs, jobId)).resolves.toMatchObject({ state: "completed" });
    await expect(kv.get(KV.fireworksBatchActiveJobs, "remote-recovery-v1")).resolves.toMatchObject({
      pendingJobIds: [],
      completedAt: expect.any(String),
    });
  });

  it("reindexes polling-exhausted jobs once without resubmitting them", async () => {
    const kv = createKv();
    const now = new Date().toISOString();
    const itemId = "fwbwork-legacy-poll";
    const jobId = "fwbjob-legacy-poll";
    await kv.set(KV.fireworksBatchWorkItems, itemId, {
      id: itemId,
      customId: "legacy-poll",
      correlationId: "legacy-poll",
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "system",
      userPrompt: "user",
      maxTokens: 512,
      state: "dead-letter",
      attempts: config.maxAttempts,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
      lastError: "batch polling attempts exhausted",
      deadLetteredAt: now,
    });
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: jobId,
      inputDatasetId: `${jobId}-input`,
      outputDatasetId: `${jobId}-output`,
      model: config.model!,
      task: "graph_extraction",
      workItemIds: [itemId],
      state: "dead-letter",
      attempts: config.maxAttempts,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
      lastError: "batch polling attempts exhausted",
    });
    await kv.set(KV.fireworksBatchActiveWork, "current", { version: 1 as const, ids: [], updatedAt: now });
    await kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [], updatedAt: now });
    const completed = vi.fn(async () => {});
    const listed = vi.fn(async () => [jobId]);
    const submitted = vi.fn(async () => ({ remoteJobId: "must-not-submit" }));
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        submitJob: submitted,
        listRecentJobIds: listed,
        async getJobStatus() { return { state: "COMPLETED", remoteJobId: jobId }; },
        async downloadResults() {
          return JSON.stringify({
            custom_id: "legacy-poll",
            response: { body: { choices: [{ message: { content: "recovered" } }] } },
          });
        },
      },
      completed,
    );

    await coordinator.process();
    await coordinator.process();

    expect(submitted).not.toHaveBeenCalled();
    expect(listed).toHaveBeenCalledTimes(1);
    expect(completed).toHaveBeenCalledTimes(1);
    await expect(kv.get<{ state: string; legacyReconciliationAt?: string }>(KV.fireworksBatchJobs, jobId))
      .resolves.toMatchObject({ state: "completed", legacyReconciliationAt: expect.any(String) });
    await expect(kv.get<{ state: string }>(KV.fireworksBatchWorkItems, itemId))
      .resolves.toMatchObject({ state: "completed" });
  });

  it("quarantines completed jobs with ambiguous non-terminal legacy work", async () => {
    const kv = createKv();
    const now = new Date().toISOString();
    const itemId = "fwbwork-legacy-completed";
    const jobId = "fwbjob-legacy-completed";
    await kv.set(KV.fireworksBatchWorkItems, itemId, {
      callbackProtocolVersion: 1,
      id: itemId,
      customId: "legacy-completed",
      correlationId: "legacy-completed",
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "system",
      userPrompt: "user",
      maxTokens: 512,
      state: "submitted",
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: jobId,
      inputDatasetId: `${jobId}-input`,
      outputDatasetId: `${jobId}-output`,
      model: config.model!,
      task: "graph_extraction",
      workItemIds: [itemId],
      state: "completed",
      attempts: 1,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchActiveWork, "current", { version: 1 as const, ids: [], updatedAt: now });
    await kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [], updatedAt: now });
    const completed = vi.fn(async () => {});
    const status = vi.fn(async () => ({ state: "COMPLETED" }));
    const download = vi.fn(async () => "");
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "must-not-submit" }; },
        async listRecentJobIds() { return [jobId]; },
        getJobStatus: status,
        downloadResults: download,
      },
      completed,
    );

    await coordinator.process();

    expect(status).not.toHaveBeenCalled();
    expect(download).not.toHaveBeenCalled();
    expect(completed).not.toHaveBeenCalled();
    await expect(kv.get<{ state: string }>(KV.fireworksBatchJobs, jobId))
      .resolves.toMatchObject({ state: "completed" });
    await expect(kv.get<{ state: string; lastError?: string }>(KV.fireworksBatchWorkItems, itemId))
      .resolves.toMatchObject({ state: "dead-letter", lastError: expect.stringContaining("ambiguous") });
  });

  it("processes work through bounded indexes when canonical listings are unavailable", async () => {
    const kv = createKv({ rejectHistoricalLists: true });
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() { return { remoteJobId: "remote-indexed" }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() {
        return JSON.stringify({
          custom_id: "indexed-1",
          response: { body: { choices: [{ message: { content: "<graph />" } }] } },
        });
      },
    };
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      transport,
      async (item, content) => { applied.push(`${item.correlationId}:${content}`); },
    );

    await coordinator.enqueue({
      correlationId: "indexed-1",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(applied).toEqual(["indexed-1:<graph />"]);
    await expect(kv.get(KV.fireworksBatchActiveWork, "current")).resolves.toMatchObject({ ids: [] });
    await expect(kv.get(KV.fireworksBatchActiveJobs, "current")).resolves.toMatchObject({ ids: [] });
  });

  it("applies backpressure from active work index without scanning history", async () => {
    const kv = createKv({ rejectHistoricalLists: true });
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, maxQueuedItems: 1 },
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-backpressure" }; },
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    const first = await coordinator.enqueue({
      correlationId: "backpressure-1",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "one",
    });
    const second = await coordinator.enqueue({
      correlationId: "backpressure-2",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "two",
    });

    expect(first.queued).toBe(true);
    expect(second).toEqual({ queued: false, reason: "Batch queue is full" });
  });

  it("repairs a known active work item missing from its index", async () => {
    const kv = createKv({ rejectHistoricalLists: true });
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-repair" }; },
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );
    const request = {
      correlationId: "repair-1",
      task: "graph_extraction" as const,
      systemPrompt: "system",
      userPrompt: "user",
    };
    const queued = await coordinator.enqueue(request);
    await kv.set(KV.fireworksBatchActiveWork, "current", { version: 1 as const, ids: [], updatedAt: new Date().toISOString() });

    await expect(coordinator.enqueue(request)).resolves.toMatchObject({ queued: true, workItemId: queued.workItemId });
    await expect(kv.get<{ ids: string[] }>(KV.fireworksBatchActiveWork, "current")).resolves.toMatchObject({ ids: [queued.workItemId] });
  });

  it("repairs a known active job without enumerating canonical jobs", async () => {
    const kv = createKv({ rejectHistoricalLists: true });
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-known" }; },
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );
    const jobId = "known-job";
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: "remote-known",
      inputDatasetId: "input",
      outputDatasetId: "output",
      model: "accounts/test/models/test",
      task: "graph_extraction",
      workItemIds: [],
      state: "polling",
      attempts: 1,
      nextAttemptAt: new Date().toISOString(),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    });

    await expect(coordinator.repairKnownJob(jobId)).resolves.toBe(true);
    await expect(kv.get<{ ids: string[] }>(KV.fireworksBatchActiveJobs, "current")).resolves.toMatchObject({ ids: [jobId] });
  });

  it("coalesces overlapping process calls into one flight", async () => {
    const kv = createKv();
    let statusCalls = 0;
    let releaseStatus: (() => void) | undefined;
    const statusReleased = new Promise<void>((resolve) => { releaseStatus = resolve; });
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-single-flight" }; },
        async getJobStatus() {
          statusCalls += 1;
          await statusReleased;
          return { state: "PENDING" };
        },
        async downloadResults() { return ""; },
      },
      async () => {},
    );
    await coordinator.enqueue({
      correlationId: "single-flight-1",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });

    const first = coordinator.process();
    const second = coordinator.process();
    await Promise.resolve();
    releaseStatus?.();
    await Promise.all([first, second]);

    expect(statusCalls).toBe(1);
  });

  it("holds compatible work until the minimum batch size or maximum wait", async () => {
    const calls: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() { calls.push("dataset"); },
      async uploadDataset() { calls.push("upload"); },
      async submitJob() { calls.push("submit"); return { remoteJobId: "job-1" }; },
      async getJobStatus() { return { state: "PENDING" }; },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(createKv(), { ...config, minBatchItems: 2 }, transport, async () => {});

    await coordinator.enqueue({ correlationId: "graph-1", task: "graph_extraction", systemPrompt: "system", userPrompt: "one" });
    await coordinator.process();
    expect(calls).toEqual([]);

    await coordinator.enqueue({ correlationId: "graph-2", task: "graph_extraction", systemPrompt: "system", userPrompt: "two" });
    await coordinator.process();
    expect(calls).toContain("submit");
  });

  it("keeps replacement work out of the normal compatible lane", async () => {
    const kv = createKv();
    const uploads: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset(_id, jsonl) { uploads.push(jsonl); },
      async submitJob() { return { remoteJobId: "replacement-lane" }; },
      async getJobStatus() { return { state: "PENDING" }; },
      async downloadResults() { return ""; },
    };
    const now = new Date().toISOString();
    const item = (id: string, replacementOf?: string): FireworksBatchWorkItem => ({
      ...(replacementOf ? { replacementOf } : {}),
      callbackProtocolVersion: 1,
      id,
      customId: id,
      correlationId: id,
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "same-system",
      userPrompt: `prompt-${id}`,
      maxTokens: 8192,
      state: "queued",
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    const normal = item("normal");
    const replacementOne = item("replacement-one", "old-one");
    for (const work of [normal, replacementOne]) {
      await kv.set(KV.fireworksBatchWorkItems, work.id, work);
    }
    await kv.set(KV.fireworksBatchActiveWork, "current", {
      version: 1,
      ids: [normal.id, replacementOne.id],
      updatedAt: now,
    });

    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, minBatchItems: 2, maxWaitMs: 60_000 },
      transport,
      async () => {},
    );
    await coordinator.process();
    expect(uploads).toEqual([]);

    const replacementTwo = item("replacement-two", "old-two");
    await kv.set(KV.fireworksBatchWorkItems, replacementTwo.id, replacementTwo);
    await kv.set(KV.fireworksBatchActiveWork, "current", {
      version: 1,
      ids: [normal.id, replacementOne.id, replacementTwo.id],
      updatedAt: now,
    });
    await coordinator.process();

    expect(uploads).toHaveLength(1);
    expect(uploads[0]).toContain('"custom_id":"replacement-one"');
    expect(uploads[0]).toContain('"custom_id":"replacement-two"');
    expect(uploads[0]).not.toContain('"custom_id":"normal"');
  });

  it("partitions compatible work before the aggregate result budget is exceeded", async () => {
    const kv = createKv();
    const uploads: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset(_id, jsonl) { uploads.push(jsonl); },
      async submitJob() { return { remoteJobId: "result-budget" }; },
      async getJobStatus() { return { state: "PENDING" }; },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(
      kv,
      {
        ...config,
        minBatchItems: 2,
        maxWaitMs: 0,
        maxConcurrency: 2,
        maxResultChars: 30_000,
        maxResponseBytes: 100_000,
      },
      transport,
      async () => {},
    );

    await coordinator.enqueue({ correlationId: "budget-one", task: "graph_extraction", systemPrompt: "system", userPrompt: "one", maxTokens: 4_096 });
    await coordinator.enqueue({ correlationId: "budget-two", task: "graph_extraction", systemPrompt: "system", userPrompt: "two", maxTokens: 4_096 });
    await coordinator.process();
    await coordinator.process();

    expect(uploads).toHaveLength(2);
    expect(uploads.every((jsonl) => jsonl.split("\n").length === 1)).toBe(true);
    expect(uploads.map((jsonl) => (JSON.parse(jsonl) as { custom_id: string }).custom_id).sort())
      .toEqual(["budget-one", "budget-two"]);
  });

  it("dead-letters an individual row that cannot fit the result budget", async () => {
    const kv = createKv();
    const submitted = vi.fn(async () => ({ remoteJobId: "must-not-submit" }));
    const coordinator = new FireworksBatchCoordinator(
      kv,
      {
        ...config,
        maxResultChars: 16_000,
        maxResponseBytes: 64_000,
      },
      {
        async createDataset() {},
        async uploadDataset() {},
        submitJob: submitted,
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.enqueue({
      correlationId: "oversized-result",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "one",
      maxTokens: 4_096,
    });
    await coordinator.process();

    expect(submitted).not.toHaveBeenCalled();
    const items = await kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems);
    expect(items).toEqual([expect.objectContaining({
      customId: "oversized-result",
      state: "dead-letter",
      lastError: expect.stringContaining("result row exceeded configured result character budget and result byte budget"),
    })]);
    await expect(kv.get(KV.fireworksBatchActiveWork, "current")).resolves.toMatchObject({ ids: [] });
  });

  it("flushes one compatible item once it reaches its maximum wait", async () => {
    const calls: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() { calls.push("dataset"); },
      async uploadDataset() { calls.push("upload"); },
      async submitJob() { calls.push("submit"); return { remoteJobId: "job-1" }; },
      async getJobStatus() { return { state: "PENDING" }; },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(createKv(), { ...config, minBatchItems: 2, maxWaitMs: 0 }, transport, async () => {});

    await coordinator.enqueue({ correlationId: "graph-1", task: "graph_extraction", systemPrompt: "system", userPrompt: "one" });
    await coordinator.process();
    expect(calls).toContain("submit");
  });

  it("submits correlated JSONL work and only applies its matching result", async () => {
    const calls: string[] = [];
    const datasetIds: string[] = [];
    const submittedJobIds: string[] = [];
    const statusIds: string[] = [];
    const kv = createKv();
    const transport: FireworksBatchTransport = {
      async createDataset(id, exampleCount) {
        datasetIds.push(id);
        calls.push(`dataset:${exampleCount}`);
      },
      async uploadDataset(_id, jsonl) {
        calls.push(jsonl);
        const row = JSON.parse(jsonl);
        expect(row).toMatchObject({
          custom_id: "graph-1",
          body: { max_tokens: 8192 },
        });
        expect(row.body).not.toHaveProperty("model");
      },
      async submitJob(request) {
        datasetIds.push(request.outputDatasetId);
        submittedJobIds.push(request.jobId);
        calls.push("submit");
        return { remoteJobId: "remote-job-1" };
      },
      async getJobStatus(id) { statusIds.push(id); return { state: "COMPLETED" }; },
      async downloadResults() {
        return JSON.stringify({
          custom_id: "graph-1",
          response: { body: { choices: [{ message: { content: "<graph />" } }] } },
        });
      },
    };
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async (item, content) => {
      applied.push(`${item.correlationId}:${content}`);
    });

    await coordinator.enqueue({
      correlationId: "graph-1",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(calls.filter((call) => call.startsWith("dataset:"))).toEqual(["dataset:1"]);
    expect(datasetIds).toEqual([
      expect.stringMatching(/^fwbjob-[a-z0-9-]+-input$/),
      expect.stringMatching(/^fwbjob-[a-z0-9-]+-output$/),
    ]);
    expect(datasetIds.every((id) => !id.includes("_"))).toBe(true);
    expect(submittedJobIds).toEqual([expect.stringMatching(/^fwbjob-[a-z0-9-]+$/)]);
    expect(submittedJobIds.every((id) => !id.includes("_"))).toBe(true);
    expect(calls).toContain("submit");
    expect(statusIds).toEqual(["remote-job-1"]);
    await expect(kv.list<{ remoteJobId?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ remoteJobId: "remote-job-1" }),
    ]);
    expect(applied).toEqual(["graph-1:<graph />"]);
  });

  it("dead-letters HTTP submit failures without polling a local job ID", async () => {
    const kv = createKv();
    let statusCalls = 0;
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() {
        throw new FireworksBatchError("job-submit", "remote request failed (400)", {
          status: 400,
          diagnostic: { code: "INVALID_ARGUMENT", message: "invalid batch request" },
        });
      },
      async getJobStatus() {
        statusCalls += 1;
        return { state: "PENDING" };
      },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async () => {});

    await coordinator.enqueue({
      correlationId: "submit-400",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    const jobs = await kv.list<{ state: string; remoteJobId?: string; lastError?: string }>(KV.fireworksBatchJobs);
    const items = await kv.list<{ state: string; lastError?: string }>(KV.fireworksBatchWorkItems);
    expect(statusCalls).toBe(0);
    expect(jobs[0]).toMatchObject({ state: "dead-letter" });
    expect(jobs[0]?.remoteJobId).toBeUndefined();
    expect(jobs[0]?.lastError).toContain("job-submit (400)");
    expect(jobs[0]?.lastError).toContain("INVALID_ARGUMENT");
    expect(items[0]).toMatchObject({ state: "dead-letter" });
  });

  it("reconciles an ambiguous network failure only after job submission", async () => {
    const kv = createKv();
    const statusIds: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() {
        throw new FireworksBatchError("job-submit", "network request failed", { retryable: true });
      },
      async getJobStatus(id) {
        statusIds.push(id);
        return { state: "PENDING" };
      },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async () => {});

    await coordinator.enqueue({
      correlationId: "submit-network",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    const jobs = await kv.list<{ id: string; state: string; remoteJobId?: string }>(KV.fireworksBatchJobs);
    expect(statusIds).toEqual([jobs[0]?.id]);
    expect(jobs[0]).toMatchObject({ state: "polling" });
    expect(jobs[0]?.remoteJobId).toBeUndefined();
  });

  it("preserves canonical identities when an accepted submit response is ambiguous", async () => {
    const kv = createKv();
    const statusIds: string[] = [];
    const downloadIds: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() {
          throw new FireworksBatchError("job-submit", "network request failed", { retryable: true });
        },
        async getJobStatus(id) {
          statusIds.push(id);
          return {
            state: "JOB_STATE_COMPLETED",
            remoteJobName: "accounts/test-account/batchInferenceJobs/canonical-after-timeout",
            outputDatasetId: "accounts/test-account/datasets/output-after-timeout",
          };
        },
        async downloadResults(id) {
          downloadIds.push(id);
          return JSON.stringify({
            custom_id: "ambiguous-canonical",
            response: { body: { choices: [{ message: { content: "recovered" } }] } },
          });
        },
      },
      async () => {},
    );

    await coordinator.enqueue({
      correlationId: "ambiguous-canonical",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(statusIds).toHaveLength(1);
    expect(downloadIds).toEqual(["accounts/test-account/datasets/output-after-timeout"]);
    await expect(kv.list<{
      state: string;
      remoteJobId?: string;
      remoteJobName?: string;
      inputDatasetId: string;
      outputDatasetId: string;
    }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({
        state: "completed",
        remoteJobId: "canonical-after-timeout",
        remoteJobName: "accounts/test-account/batchInferenceJobs/canonical-after-timeout",
        inputDatasetId: expect.stringMatching(/-input$/),
        outputDatasetId: "accounts/test-account/datasets/output-after-timeout",
      }),
    ]);
  });

  it("reconciles an ambiguous submit against a discovered canonical job ID", async () => {
    const kv = createKv();
    const statusIds: string[] = [];
    const listLimits: number[] = [];
    const downloadIds: string[] = [];
    let requestedInputDatasetId = "";
    let submitCalls = 0;
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob(input) {
        submitCalls += 1;
        requestedInputDatasetId = input.inputDatasetId;
        throw new FireworksBatchError("job-submit", "network request failed", { retryable: true });
      },
      async listRecentJobIds(limit) {
        listLimits.push(limit);
        return ["canonical-after-timeout"];
      },
      async getJobStatus(id) {
        statusIds.push(id);
        if (id !== "canonical-after-timeout") {
          throw new FireworksBatchError("job-status", "remote job was not found", { status: 404 });
        }
        return {
          state: "JOB_STATE_COMPLETED",
          remoteJobId: id,
          remoteJobName: "accounts/test-account/batchInferenceJobs/canonical-after-timeout",
          inputDatasetId: `accounts/test-account/datasets/${requestedInputDatasetId}`,
          outputDatasetId: "accounts/test-account/datasets/output-after-timeout",
        };
      },
      async downloadResults(id) {
        downloadIds.push(id);
        return JSON.stringify({
          custom_id: "ambiguous-discovered",
          response: { body: { choices: [{ message: { content: "recovered" } }] } },
        });
      },
    };
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      transport,
      async () => {},
    );

    await coordinator.enqueue({
      correlationId: "ambiguous-discovered",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(submitCalls).toBe(1);
    expect(listLimits).toContain(32);
    expect(statusIds).toEqual(["canonical-after-timeout"]);
    expect(downloadIds).toEqual(["accounts/test-account/datasets/output-after-timeout"]);
    await expect(kv.list<{ state: string; remoteJobId?: string; remoteJobName?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({
        state: "completed",
        remoteJobId: "canonical-after-timeout",
        remoteJobName: "accounts/test-account/batchInferenceJobs/canonical-after-timeout",
      }),
    ]);
  });

  it("bounds ambiguous remote reconciliation and never re-submits after exhaustion", async () => {
    const kv = createKv();
    let submitCalls = 0;
    let listCalls = 0;
    let statusCalls = 0;
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() {
        submitCalls += 1;
        throw new FireworksBatchError("job-submit", "network request failed", { retryable: true });
      },
      async listRecentJobIds() {
        listCalls += 1;
        return Array.from({ length: 128 }, (_, index) => `unrelated-${index}`);
      },
      async getJobStatus() {
        statusCalls += 1;
        throw new FireworksBatchError("job-status", "remote job was not found", { status: 404 });
      },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async () => {});

    await coordinator.enqueue({
      correlationId: "ambiguous-exhaustion",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    for (let cycle = 0; cycle < 4; cycle += 1) {
      await coordinator.process();
      const jobs = await kv.list<{ id: string; state: string; nextAttemptAt: string }>(KV.fireworksBatchJobs);
      const job = jobs[0];
      if (!job || job.state === "dead-letter") break;
      await kv.set(KV.fireworksBatchJobs, job.id, { ...job, nextAttemptAt: new Date(0).toISOString() });
    }

    expect(submitCalls).toBe(1);
    expect(listCalls).toBeLessThanOrEqual(4);
    expect(statusCalls).toBeLessThanOrEqual(96);
    await expect(kv.list<{ state: string; lastError?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({
        state: "dead-letter",
        lastError: expect.stringContaining("identity reconciliation exhausted"),
      }),
    ]);
    await expect(kv.list<{ state: string }>(KV.fireworksBatchWorkItems)).resolves.toEqual([
      expect.objectContaining({ state: "dead-letter" }),
    ]);
  });

  it("keeps nonterminal polls alive beyond maxAttempts until the deadline", async () => {
    const kv = createKv();
    const now = new Date(0).toISOString();
    const itemId = "fwbwork-attempt-limit";
    const jobId = "fwbjob-attempt-limit";
    await kv.set(KV.fireworksBatchWorkItems, itemId, {
      callbackProtocolVersion: 1,
      id: itemId,
      customId: "attempt-limit",
      correlationId: "attempt-limit",
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "system",
      userPrompt: "user",
      maxTokens: 512,
      state: "submitted",
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: "remote-attempt-limit",
      inputDatasetId: "input",
      outputDatasetId: "output",
      model: config.model!,
      task: "graph_extraction",
      workItemIds: [itemId],
      state: "polling",
      attempts: 2,
      nextAttemptAt: now,
      pollDeadlineAt: new Date(Date.now() + 60_000).toISOString(),
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchActiveWork, "current", { version: 1 as const, ids: [itemId], updatedAt: now });
    await kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [jobId], updatedAt: now });
    let statusCalls = 0;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "unused" }; },
        async getJobStatus() { statusCalls++; return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.process();

    expect(statusCalls).toBeGreaterThanOrEqual(1);
    await expect(kv.get<{ state: string; attempts: number }>(KV.fireworksBatchJobs, jobId))
      .resolves.toMatchObject({ state: "polling", attempts: 2 });
    await expect(kv.get<{ state: string }>(KV.fireworksBatchWorkItems, itemId))
      .resolves.toMatchObject({ state: "submitted" });
    await expect(kv.get<{ ids: string[] }>(KV.fireworksBatchActiveJobs, "current"))
      .resolves.toMatchObject({ ids: [jobId] });
  });

  it("does not spend failure attempts on pending remote statuses", async () => {
    const kv = createKv();
    let statusCalls = 0;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, maxAttempts: 1, pollDeadlineMs: 60_000 },
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-pending" }; },
        async getJobStatus() { statusCalls++; return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.enqueue({
      correlationId: "pending-survives",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();
    await coordinator.process();

    expect(statusCalls).toBeGreaterThanOrEqual(3);
    await expect(kv.list<{ state: string; attempts: number; pollAttempts?: number }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "polling", attempts: 1, pollAttempts: expect.any(Number) }),
    ]);
    await expect(kv.list<{ state: string }>(KV.fireworksBatchWorkItems)).resolves.toEqual([
      expect.objectContaining({ state: "submitted" }),
    ]);
  });

  it("dead-letters a remote job when its polling deadline expires", async () => {
    const kv = createKv();
    const now = new Date(0).toISOString();
    const itemId = "fwbwork-poll-deadline";
    const jobId = "fwbjob-poll-deadline";
    await kv.set(KV.fireworksBatchWorkItems, itemId, {
      callbackProtocolVersion: 1,
      id: itemId,
      customId: "poll-deadline",
      correlationId: "poll-deadline",
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "system",
      userPrompt: "user",
      maxTokens: 512,
      state: "submitted",
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: "remote-poll-deadline",
      inputDatasetId: "input",
      outputDatasetId: "output",
      model: config.model!,
      task: "graph_extraction",
      workItemIds: [itemId],
      state: "polling",
      attempts: 1,
      nextAttemptAt: now,
      pollDeadlineAt: now,
      createdAt: now,
      updatedAt: now,
    });
    await kv.set(KV.fireworksBatchActiveWork, "current", { version: 1 as const, ids: [itemId], updatedAt: now });
    await kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [jobId], updatedAt: now });
    let statusCalls = 0;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "unused" }; },
        async getJobStatus() { statusCalls++; return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.process();

    expect(statusCalls).toBe(0);
    await expect(kv.get<{ state: string; lastError?: string }>(KV.fireworksBatchJobs, jobId))
      .resolves.toMatchObject({ state: "dead-letter", lastError: "batch polling deadline exceeded" });
    await expect(kv.get<{ state: string; lastError?: string }>(KV.fireworksBatchWorkItems, itemId))
      .resolves.toMatchObject({ state: "dead-letter", lastError: "batch polling deadline exceeded" });
  });

  it("starts a fresh polling deadline for an active legacy job", async () => {
    const kv = createKv();
    const old = new Date(0).toISOString();
    const itemId = "fwbwork-legacy-active-deadline";
    const jobId = "fwbjob-legacy-active-deadline";
    await kv.set(KV.fireworksBatchWorkItems, itemId, {
      callbackProtocolVersion: 1,
      id: itemId,
      customId: "legacy-active-deadline",
      correlationId: "legacy-active-deadline",
      task: "graph_extraction",
      model: config.model!,
      systemPrompt: "system",
      userPrompt: "user",
      maxTokens: 512,
      state: "submitted",
      attempts: 0,
      nextAttemptAt: old,
      createdAt: old,
      updatedAt: old,
    });
    await kv.set(KV.fireworksBatchJobs, jobId, {
      id: jobId,
      remoteJobId: "remote-legacy-active-deadline",
      inputDatasetId: "input",
      outputDatasetId: "output",
      model: config.model!,
      task: "graph_extraction",
      workItemIds: [itemId],
      state: "polling",
      attempts: 1,
      nextAttemptAt: old,
      submitAttemptedAt: old,
      createdAt: old,
      updatedAt: old,
    });
    await kv.set(KV.fireworksBatchActiveWork, "current", {
      version: 1 as const,
      ids: [itemId],
      updatedAt: old,
    });
    await kv.set(KV.fireworksBatchActiveJobs, "current", {
      version: 1 as const,
      ids: [jobId],
      updatedAt: old,
    });
    let statusCalls = 0;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, pollDeadlineMs: 60_000 },
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "unused" }; },
        async getJobStatus() {
          statusCalls++;
          return { state: "PENDING" };
        },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.process();

    expect(statusCalls).toBeGreaterThan(0);
    const job = await kv.get<{ state: string; pollDeadlineAt?: string }>(KV.fireworksBatchJobs, jobId);
    expect(job).toMatchObject({ state: "polling", pollDeadlineAt: expect.any(String) });
    expect(Date.parse(job!.pollDeadlineAt!)).toBeGreaterThan(Date.now());
  });

  it("sends fitting JSONL prefixes and does not starve later rows after an oversize row", async () => {
    const kv = createKv();
    const smallLine = (customId: string, userPrompt: string) => JSON.stringify({
      custom_id: customId,
      body: {
        messages: [{ role: "system", content: "system" }, { role: "user", content: userPrompt }],
        max_tokens: 8192,
      },
    });
    const maxRequestBytes = Buffer.byteLength(smallLine("small-1", "one"), "utf8");
    const uploadedIds: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset(_id, jsonl) {
        uploadedIds.push(...jsonl.split("\n").map((line) => (JSON.parse(line) as { custom_id: string }).custom_id));
      },
      async submitJob() { return { remoteJobId: `remote-${uploadedIds.at(-1)}` }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() {
        return uploadedIds.slice(-1).map((customId) => JSON.stringify({
          custom_id: customId,
          response: { body: { choices: [{ message: { content: customId } }] } },
        })).join("\n");
      },
    };
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, maxBatchItems: 3, maxRequestBytes },
      transport,
      async () => {},
    );

    await coordinator.enqueue({ correlationId: "small-1", task: "graph_extraction", systemPrompt: "system", userPrompt: "one" });
    await coordinator.enqueue({ correlationId: "oversize", task: "graph_extraction", systemPrompt: "system", userPrompt: "x".repeat(1_000) });
    await coordinator.enqueue({ correlationId: "small-2", task: "graph_extraction", systemPrompt: "system", userPrompt: "two" });

    await coordinator.process();
    await coordinator.process();

    expect(uploadedIds).toEqual(["small-1", "small-2"]);
    await expect(kv.list<{ customId: string; state: string; lastError?: string }>(KV.fireworksBatchWorkItems)).resolves.toEqual(expect.arrayContaining([
      expect.objectContaining({ customId: "oversize", state: "dead-letter", lastError: expect.stringContaining("exceeded configured byte limit") }),
      expect.objectContaining({ customId: "small-1", state: "completed" }),
      expect.objectContaining({ customId: "small-2", state: "completed" }),
    ]));
  });

  it("packs JSONL against the provider character limit as well as bytes", async () => {
    const kv = createKv();
    const smallLine = (customId: string, userPrompt: string) => JSON.stringify({
      custom_id: customId,
      body: {
        messages: [{ role: "system", content: "system" }, { role: "user", content: userPrompt }],
        max_tokens: 8192,
      },
    });
    const uploadedIds: string[] = [];
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset(_id, jsonl) {
        uploadedIds.push(...jsonl.split("\n").map((line) => (JSON.parse(line) as { custom_id: string }).custom_id));
      },
      async submitJob() { return { remoteJobId: `remote-${uploadedIds.at(-1)}` }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() {
        return uploadedIds.slice(-1).map((customId) => JSON.stringify({
          custom_id: customId,
          response: { body: { choices: [{ message: { content: customId } }] } },
        })).join("\n");
      },
    };
    const maxRequestChars = smallLine("small-char-1", "one").length + 1;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, maxBatchItems: 3, maxRequestChars, maxRequestBytes: 20_000 },
      transport,
      async () => {},
    );

    await coordinator.enqueue({ correlationId: "small-char-1", task: "graph_extraction", systemPrompt: "system", userPrompt: "one" });
    await coordinator.enqueue({ correlationId: "oversize-char", task: "graph_extraction", systemPrompt: "system", userPrompt: "x".repeat(maxRequestChars - "system".length) });
    await coordinator.enqueue({ correlationId: "small-char-2", task: "graph_extraction", systemPrompt: "system", userPrompt: "two" });

    await coordinator.process();
    await coordinator.process();

    expect(uploadedIds).toEqual(["small-char-1", "small-char-2"]);
    await expect(kv.list<{ customId: string; state: string; lastError?: string }>(KV.fireworksBatchWorkItems)).resolves.toEqual(expect.arrayContaining([
      expect.objectContaining({ customId: "oversize-char", state: "dead-letter", lastError: expect.stringContaining("character limit") }),
      expect.objectContaining({ customId: "small-char-1", state: "completed" }),
      expect.objectContaining({ customId: "small-char-2", state: "completed" }),
    ]));
  });

  it("keeps large persisted provenance outside the remote prompt budget", async () => {
    const kv = createKv();
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "job-metadata" }; },
        async getJobStatus() { return { state: "PENDING" }; },
        async downloadResults() { return ""; },
      },
      async () => {},
    );
    const metadata = { observations: JSON.stringify({ narrative: "x".repeat(25_000) }) };

    const queued = await coordinator.enqueue({
      correlationId: "graph-large-provenance",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "compact remote prompt",
      metadata,
    });

    expect(queued.queued).toBe(true);
    const persisted = await kv.list<{ metadata?: Record<string, string> }>(KV.fireworksBatchWorkItems);
    expect(persisted[0]?.metadata).toEqual(metadata);
  });

  it("marks stale results and permits a current replacement to be queued", async () => {
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() { return { remoteJobId: "job-1" }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() {
        return JSON.stringify({
          custom_id: "reflect-1",
          response: { body: { choices: [{ message: { content: "<insight />" } }] } },
        });
      },
    };
    const coordinator = new FireworksBatchCoordinator(
      createKv(),
      config,
      transport,
      async () => "stale",
    );

    const first = await coordinator.enqueue({
      correlationId: "reflect-1",
      task: "reflection",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();
    const replacement = await coordinator.enqueue({
      correlationId: "reflect-2",
      task: "reflection",
      systemPrompt: "system",
      userPrompt: "user",
    });

    expect(first.workItemId).toBeDefined();
    expect(replacement.workItemId).toBeDefined();
    expect(replacement.workItemId).not.toBe(first.workItemId);
  });

  it("accounts for successful and error files before completing a remote job", async () => {
    const kv = createKv();
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, maxBatchItems: 2 },
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-accounting" }; },
        async getJobStatus() { return { state: "JOB_STATE_COMPLETED" }; },
        async downloadResults() {
          return {
            resultRows: [JSON.stringify({
              custom_id: "accounted-success",
              response: { body: { choices: [{ message: { content: "<graph />" } }] } },
            })],
            errorRows: [JSON.stringify({
              custom_id: "accounted-error",
              error: { code: "INVALID_ARGUMENT", message: "row rejected" },
            })],
          };
        },
      },
      async (item, content) => { applied.push(`${item.customId}:${content}`); },
    );

    await coordinator.enqueue({ correlationId: "accounted-success", task: "graph_extraction", systemPrompt: "system", userPrompt: "one" });
    await coordinator.enqueue({ correlationId: "accounted-error", task: "graph_extraction", systemPrompt: "system", userPrompt: "two" });
    await coordinator.process();

    expect(applied).toEqual(["accounted-success:<graph />"]);
    await expect(kv.list<{ state: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "completed" }),
    ]);
    const items = await kv.list<{ customId: string; state: string; lastError?: string }>(KV.fireworksBatchWorkItems);
    expect(items.find((item) => item.customId === "accounted-success")).toMatchObject({ state: "completed" });
    expect(items.find((item) => item.customId === "accounted-error")).toMatchObject({
      state: "dead-letter",
      lastError: "code=INVALID_ARGUMENT; message=row rejected",
    });
  });

  it.each([
    ["missing", ""],
    ["malformed", "not-json"],
  ])("keeps a completed job retryable when %s rows are invalid", async (_caseName, downloaded) => {
    const kv = createKv();
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-invalid-result" }; },
        async getJobStatus() { return { state: "COMPLETED" }; },
        async downloadResults() { return downloaded; },
      },
      async (_item, content) => { applied.push(content); },
    );

    await coordinator.enqueue({ correlationId: "invalid-result", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process();

    expect(applied).toEqual([]);
    await expect(kv.list<{ state: string; attempts: number; lastError?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "polling", attempts: 2 }),
    ]);
  });

  it("ignores unknown result rows but rejects duplicate owned rows", async () => {
    const kv = createKv();
    const resultRow = JSON.stringify({
      custom_id: "owned-row",
      response: { body: { choices: [{ message: { content: "<graph />" } }] } },
    });
    const unknownRow = JSON.stringify({
      custom_id: "unknown-row",
      response: { body: { choices: [{ message: { content: "ignore" } }] } },
    });
    let downloaded = `${unknownRow}\n${resultRow}`;
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-unknown" }; },
        async getJobStatus() { return { state: "COMPLETED" }; },
        async downloadResults() { return downloaded; },
      },
      async (_item, content) => { applied.push(content); },
    );

    await coordinator.enqueue({ correlationId: "owned-row", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process();
    expect(applied).toEqual(["<graph />"]);

    const job = (await kv.list<{ id: string }>(KV.fireworksBatchJobs))[0]!;
    await kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [job.id], updatedAt: new Date().toISOString() });
    await kv.set(KV.fireworksBatchJobs, job.id, {
      ...(await kv.get<Record<string, unknown>>(KV.fireworksBatchJobs, job.id))!,
      state: "polling",
      nextAttemptAt: new Date(0).toISOString(),
    });
    downloaded = `${resultRow}\n${resultRow}`;
    await coordinator.process();

    expect(applied).toEqual(["<graph />"]);
    await expect(kv.list<{ state: string; attempts: number }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "polling", attempts: 2 }),
    ]);
  });

  it("does not duplicate prior callbacks when a later row fails", async () => {
    const kv = createKv();
    const applied: string[] = [];
    let calls = 0;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      { ...config, maxBatchItems: 2 },
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-callback-failure" }; },
        async getJobStatus() { return { state: "COMPLETED" }; },
        async downloadResults() {
          return [
            JSON.stringify({ custom_id: "callback-first", response: { body: { choices: [{ message: { content: "first" } }] } } }),
            JSON.stringify({ custom_id: "callback-second", response: { body: { choices: [{ message: { content: "second" } }] } } }),
          ].join("\n");
        },
      },
      async (item, content) => {
        calls++;
        if (item.customId === "callback-second" && calls === 2) {
          return { success: false, error: "graph callback failed" };
        }
        applied.push(`${item.customId}:${content}`);
      },
    );

    await coordinator.enqueue({ correlationId: "callback-first", task: "graph_extraction", systemPrompt: "system", userPrompt: "one" });
    await coordinator.enqueue({ correlationId: "callback-second", task: "graph_extraction", systemPrompt: "system", userPrompt: "two" });
    await coordinator.process();

    expect(applied).toEqual(["callback-first:first"]);
    expect(calls).toBe(2);
    const job = (await kv.list<{ id: string; state: string }>(KV.fireworksBatchJobs))[0]!;
    await kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1 as const, ids: [job.id], updatedAt: new Date().toISOString() });
    await kv.set(KV.fireworksBatchJobs, job.id, {
      ...(await kv.get<Record<string, unknown>>(KV.fireworksBatchJobs, job.id))!,
      state: "polling",
      nextAttemptAt: new Date(0).toISOString(),
    });
    await coordinator.process();

    expect(applied).toEqual(["callback-first:first", "callback-second:second"]);
    expect(calls).toBe(3);
  });

  it("uses canonical remote job and output dataset identities", async () => {
    const kv = createKv();
    const statusIds: string[] = [];
    const downloadIds: string[] = [];
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() {
          return {
            remoteJobId: "local-request-id",
            remoteJobName: "accounts/test-account/batchInferenceJobs/remote-canonical",
            outputDatasetId: "accounts/test-account/datasets/output-canonical",
          };
        },
        async getJobStatus(id) {
          statusIds.push(id);
          return {
            state: "JOB_STATE_COMPLETED",
            remoteJobName: "accounts/test-account/batchInferenceJobs/remote-canonical",
            outputDatasetId: "accounts/test-account/datasets/output-canonical",
          };
        },
        async downloadResults(id) {
          downloadIds.push(id);
          return JSON.stringify({ custom_id: "canonical-row", response: { body: { choices: [{ message: { content: "ok" } }] } } });
        },
      },
      async () => {},
    );

    await coordinator.enqueue({ correlationId: "canonical-row", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process();

    expect(statusIds).toEqual(["remote-canonical"]);
    expect(downloadIds).toEqual(["accounts/test-account/datasets/output-canonical"]);
    await expect(kv.list<{ remoteJobId?: string; remoteJobName?: string; outputDatasetId: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({
        remoteJobId: "remote-canonical",
        remoteJobName: "accounts/test-account/batchInferenceJobs/remote-canonical",
        outputDatasetId: "accounts/test-account/datasets/output-canonical",
      }),
    ]);
  });

  it("treats EXPIRED as result-bearing when Fireworks saved output rows", async () => {
    const kv = createKv();
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() { return { remoteJobId: "remote-expired" }; },
        async getJobStatus() { return { state: "JOB_STATE_EXPIRED" }; },
        async downloadResults() {
          return JSON.stringify({ custom_id: "expired-row", response: { body: { choices: [{ message: { content: "saved" } }] } } });
        },
      },
      async () => {},
    );

    await coordinator.enqueue({ correlationId: "expired-row", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process();

    await expect(kv.list<{ state: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "completed" }),
    ]);
  });

  it("reconciles retryable 5xx submit responses after the durable submit marker", async () => {
    const kv = createKv();
    let submitCalls = 0;
    let statusCalls = 0;
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() {
        submitCalls += 1;
        throw new FireworksBatchError("job-submit", "remote request failed (503)", {
          status: 503,
          retryable: true,
          diagnostic: { code: "UNAVAILABLE", message: "remote service unavailable" },
        });
      },
      async getJobStatus(id) {
        statusCalls += 1;
        expect(id).toMatch(/^fwbjob-/);
        return { state: "PENDING" };
      },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async () => {});

    await coordinator.enqueue({
      correlationId: "submit-503",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(submitCalls).toBe(1);
    expect(statusCalls).toBe(1);
    await expect(kv.list<{ state: string; submitAttemptedAt?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "polling", submitAttemptedAt: expect.any(String) }),
    ]);
  });

  it("reconciles a provider submit validation failure using remote discovery", async () => {
    const kv = createKv();
    const statusIds: string[] = [];
    let requestedInputDatasetId = "";
    let submitCalls = 0;
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob(input) {
        submitCalls += 1;
        requestedInputDatasetId = input.inputDatasetId;
        throw new FireworksBatchError("job-submit", "remote response field outputDatasetId was invalid");
      },
      async listRecentJobIds() {
        return ["canonical-after-validation-error"];
      },
      async getJobStatus(id) {
        statusIds.push(id);
        return {
          state: "PENDING",
          remoteJobId: id,
          inputDatasetId: `accounts/test-account/datasets/${requestedInputDatasetId}`,
        };
      },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async () => {});

    await coordinator.enqueue({
      correlationId: "submit-no-safe-id",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    await coordinator.process();

    expect(submitCalls).toBe(1);
    expect(statusIds).toEqual(["canonical-after-validation-error"]);
    await expect(kv.list<{ state: string; remoteJobId?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "polling", remoteJobId: "canonical-after-validation-error" }),
    ]);
  });

  it("reconciles malformed 2xx submit responses and exhausts bounded discovery", async () => {
    const kv = createKv();
    let submitCalls = 0;
    let listCalls = 0;
    let statusCalls = 0;
    const transport: FireworksBatchTransport = {
      async createDataset() {},
      async uploadDataset() {},
      async submitJob() {
        submitCalls += 1;
        throw new FireworksBatchError("job-submit", "remote response was not valid JSON", { status: 200 });
      },
      async listRecentJobIds() {
        listCalls += 1;
        return Array.from({ length: 128 }, (_, index) => `unrelated-malformed-${index}`);
      },
      async getJobStatus() {
        statusCalls += 1;
        throw new FireworksBatchError("job-status", "remote job was not found", { status: 404 });
      },
      async downloadResults() { return ""; },
    };
    const coordinator = new FireworksBatchCoordinator(kv, config, transport, async () => {});

    await coordinator.enqueue({
      correlationId: "submit-malformed-2xx",
      task: "graph_extraction",
      systemPrompt: "system",
      userPrompt: "user",
    });
    for (let cycle = 0; cycle < 4; cycle += 1) {
      await coordinator.process();
      const jobs = await kv.list<{ id: string; state: string; nextAttemptAt: string }>(KV.fireworksBatchJobs);
      const job = jobs[0];
      if (!job || job.state === "dead-letter") break;
      await kv.set(KV.fireworksBatchJobs, job.id, { ...job, nextAttemptAt: new Date(0).toISOString() });
    }

    expect(submitCalls).toBe(1);
    expect(listCalls).toBeLessThanOrEqual(4);
    expect(statusCalls).toBeLessThanOrEqual(96);
    await expect(kv.list<{ state: string; lastError?: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({
        state: "dead-letter",
        lastError: expect.stringContaining("identity reconciliation exhausted"),
      }),
    ]);
    await expect(kv.list<{ state: string }>(KV.fireworksBatchWorkItems)).resolves.toEqual([
      expect.objectContaining({ state: "dead-letter" }),
    ]);
  });

  it("only treats explicit already-existing submit diagnostics as ambiguous", async () => {
    const kv = createKv();
    let statusCalls = 0;
    const coordinator = new FireworksBatchCoordinator(
      kv,
      config,
      {
        async createDataset() {},
        async uploadDataset() {},
        async submitJob() {
          throw new FireworksBatchError("job-submit", "remote request failed (409)", {
            status: 409,
            diagnostic: { code: "ALREADY_EXISTS", message: "job already exists" },
          });
        },
        async getJobStatus(id) {
          statusCalls++;
          expect(id).toMatch(/^fwbjob-/);
          return { state: "PENDING" };
        },
        async downloadResults() { return ""; },
      },
      async () => {},
    );

    await coordinator.enqueue({ correlationId: "duplicate-submit", task: "graph_extraction", systemPrompt: "system", userPrompt: "user" });
    await coordinator.process();

    expect(statusCalls).toBe(1);
    await expect(kv.list<{ state: string }>(KV.fireworksBatchJobs)).resolves.toEqual([
      expect.objectContaining({ state: "polling" }),
    ]);
  });
});
