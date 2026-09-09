import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { FireworksBatchCoordinator } from "../src/functions/fireworks-batch.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import { KV } from "../src/state/schema.js";
import type { FireworksBatchConfig, FireworksBatchWorkItem, FireworksBatchEnqueueJournal } from "../src/types.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
const config: FireworksBatchConfig = {
  enabled: true, accountId: "test", apiKey: "test", model: "model", timeoutMs: 1000,
  minBatchItems: 1, maxWaitMs: 0, maxBatchItems: 10, maxRequestChars: 10000,
  maxRequestBytes: 10000, maxResponseBytes: 10000, maxResultChars: 10000,
  maxConcurrency: 1, maxAttempts: 2, retryBaseMs: 1, retryMaxMs: 10,
  pollIntervalMs: 0, pollMaxIntervalMs: 10, recoveryStaleMs: 1000, maxQueuedItems: 10,
};
const request = { correlationId: "row", task: "reflection" as const, systemPrompt: "system", userPrompt: "user" };
function setup() {
  const h = effectHarness(); const usage = vi.fn(async () => {}), completed = vi.fn(async (_item: FireworksBatchWorkItem, _content: string) => {});
  const transport = {
    createDataset: vi.fn(async () => {}), uploadDataset: vi.fn(async () => {}),
    submitJob: vi.fn(async () => ({ remoteJobId: "remote" })),
    getJobStatus: vi.fn(async () => ({ state: "COMPLETED" })),
    downloadResults: vi.fn(async () => JSON.stringify({ custom_id: "row", response: { body: { choices: [{ message: { content: "result" } }], usage: { prompt_tokens: 5, completion_tokens: 3, total_tokens: 8 } } } })),
  };
  const create = () => new FireworksBatchCoordinator(h.kv, config, transport, completed, usage);
  return { ...h, completed, usage, transport, create };
}

describe("batch callback protocol upgrade", () => {
  it("persists the marker in the enqueue journal before work persistence and recovers it", async () => {
    const h = setup(); h.crash(KV.fireworksBatchWorkItems);
    await expect(h.create().enqueue(request)).rejects.toThrow();
    const journal = await h.kv.get<FireworksBatchEnqueueJournal>(KV.fireworksBatchEnqueueIntents, "current");
    expect(journal?.intents[0].item.callbackProtocolVersion).toBe(1);
    expect(await h.kv.get(KV.fireworksBatchWorkItems, journal!.intents[0].id)).toBeNull();
    await h.create().process();
    expect(h.completed).toHaveBeenCalledTimes(1);
    expect(h.completed.mock.calls[0][0]).toMatchObject({ callbackProtocolVersion: 1 });
  });

  it.each(["queued", "submitted", "polling"] as const)("drains unmarked %s work without callbacks or new submission", async (state) => {
    const h = setup(); const queued = await h.create().enqueue(request);
    const item = (await h.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, queued.workItemId!))!;
    delete item.callbackProtocolVersion; item.state = state;
    item.completionIntent = { key: batchEffectKey(item.id), resultHash: batchEffectKey("ambiguous") };
    await h.kv.set(KV.fireworksBatchWorkItems, item.id, item);
    await h.create().process();
    expect(await h.kv.get(KV.fireworksBatchWorkItems, item.id)).toMatchObject({ state: "dead-letter", lastError: expect.stringContaining("ambiguous") });
    expect(h.completed).not.toHaveBeenCalled(); expect(h.usage).not.toHaveBeenCalled(); expect(h.transport.submitJob).not.toHaveBeenCalled();
    expect(await h.create().enqueue(request)).toEqual({ queued: true, workItemId: item.id });
    expect(await h.kv.list(KV.fireworksBatchWorkItems)).toHaveLength(1);
    expect(await h.kv.get(KV.fireworksBatchActiveWork, "current")).toMatchObject({ ids: [] });
  });

  it("quarantines legacy work discovered through a remote job without an active work index", async () => {
    const h = setup(); const queued = await h.create().enqueue(request);
    const item = (await h.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, queued.workItemId!))!;
    delete item.callbackProtocolVersion; item.state = "submitted";
    await h.kv.set(KV.fireworksBatchWorkItems, item.id, item);
    await h.kv.set(KV.fireworksBatchActiveWork, "current", { version: 1, ids: [] });
    await h.kv.set(KV.fireworksBatchJobs, "job", { id: "job", remoteJobId: "remote", outputDatasetId: "output", inputDatasetId: "input", workItemIds: [item.id], state: "polling", attempts: 0, nextAttemptAt: new Date(0).toISOString() });
    await h.kv.set(KV.fireworksBatchActiveJobs, "current", { version: 1, ids: ["job"] });
    await h.create().process();
    expect(h.transport.downloadResults).toHaveBeenCalled();
    expect(h.completed).not.toHaveBeenCalled(); expect(h.usage).not.toHaveBeenCalled();
    expect(await h.kv.get(KV.fireworksBatchWorkItems, item.id)).toMatchObject({ state: "dead-letter" });
    expect(await h.kv.get(KV.fireworksBatchJobs, "job")).toMatchObject({ state: "completed" });
  });

  it.each(["completed", "stale", "failed", "dead-letter"] as const)("leaves terminal legacy %s records untouched", async (state) => {
    const h = setup(); const queued = await h.create().enqueue(request);
    const item = (await h.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, queued.workItemId!))!;
    delete item.callbackProtocolVersion; item.state = state;
    await h.kv.set(KV.fireworksBatchWorkItems, item.id, item);
    await h.create().process(); await h.create().enqueue(request);
    expect(await h.kv.get(KV.fireworksBatchWorkItems, item.id)).toEqual(item);
    expect(h.completed).not.toHaveBeenCalled(); expect(h.usage).not.toHaveBeenCalled();
  });

  it.each([false, true])("recovers quarantine persistence failure, after=%s", async (after) => {
    const h = setup(); const queued = await h.create().enqueue(request);
    const item = (await h.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, queued.workItemId!))!;
    delete item.callbackProtocolVersion;
    await h.kv.set(KV.fireworksBatchWorkItems, item.id, item); h.crash(KV.fireworksBatchWorkItems, after);
    await expect(h.create().process()).rejects.toThrow();
    await h.create().process();
    expect(await h.kv.get(KV.fireworksBatchWorkItems, item.id)).toMatchObject({ state: "dead-letter" });
    expect(h.completed).not.toHaveBeenCalled(); expect(h.usage).not.toHaveBeenCalled();
  });
});
