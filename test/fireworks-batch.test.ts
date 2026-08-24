import { describe, expect, it } from "vitest";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { StateKV } from "../src/state/kv.js";
import type { FireworksBatchConfig } from "../src/types.js";
import type { FireworksBatchTransport } from "../src/providers/fireworks-batch.js";

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
    const transport: FireworksBatchTransport = {
      async createDataset(_id, exampleCount) { calls.push(`dataset:${exampleCount}`); },
      async uploadDataset(_id, jsonl) {
        calls.push(jsonl);
        expect(JSON.parse(jsonl)).toMatchObject({ custom_id: "graph-1" });
      },
      async submitJob() { calls.push("submit"); return { remoteJobId: "job-1" }; },
      async getJobStatus() { return { state: "COMPLETED" }; },
      async downloadResults() {
        return JSON.stringify({
          custom_id: "graph-1",
          response: { body: { choices: [{ message: { content: "<graph />" } }] } },
        });
      },
    };
    const applied: string[] = [];
    const coordinator = new FireworksBatchCoordinator(createKv(), config, transport, async (item, content) => {
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
    expect(calls).toContain("submit");
    expect(applied).toEqual(["graph-1:<graph />"]);
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
