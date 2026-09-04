import { afterEach, describe, expect, it, vi } from "vitest";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import type { FireworksBatchConfig } from "../src/types.js";
import { FireworksBatchClient, FireworksBatchError, type FireworksBatchTransport } from "../src/providers/fireworks-batch.js";

function createKv(): StateKV {
  const store = new Map<string, unknown>();
  const sdk = {
    async trigger(input: { function_id: string; payload: { scope: string; key?: string; value?: unknown } }) {
      const { scope, key, value } = input.payload;
      if (input.function_id === "state::get") return store.get(`${scope}:${key}`) ?? null;
      if (input.function_id === "state::set") {
        store.set(`${scope}:${key}`, value);
        return value;
      }
      if (input.function_id === "state::list") {
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
  maxResponseBytes: 10_000,
  maxResultChars: 10_000,
  maxConcurrency: 1,
  maxAttempts: 3,
  retryBaseMs: 1,
  retryMaxMs: 10,
  pollIntervalMs: 0,
  pollMaxIntervalMs: 10,
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
          body: { max_tokens: 512 },
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
});
