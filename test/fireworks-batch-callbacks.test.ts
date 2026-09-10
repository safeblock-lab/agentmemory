import { describe, expect, it, vi } from "vitest";
import type { ISdk } from "iii-sdk";
import type { FireworksBatchWorkItem } from "../src/types.js";
import { effectHarness } from "./batch-effects-harness.js";
import { KV } from "../src/state/schema.js";

process.env["VITEST"] = "true";
const { createFireworksBatchCompletionHandler, registerFireworksBatchReplacement } = await import("../src/index.js");

function workItem(
  task: FireworksBatchWorkItem["task"],
  metadata: Record<string, string> = {},
): FireworksBatchWorkItem {
  const now = new Date().toISOString();
  return {
    id: "work-1",
    customId: "custom-1",
    correlationId: "correlation-1",
    task,
    model: "accounts/fireworks/models/test",
    systemPrompt: "system",
    userPrompt: "user",
    maxTokens: 128,
    metadata,
    state: "submitted",
    attempts: 0,
    nextAttemptAt: now,
    createdAt: now,
    updatedAt: now,
  };
}

function sdkWithTrigger(trigger: ReturnType<typeof vi.fn>): ISdk {
  return { trigger } as unknown as ISdk;
}

describe("Fireworks batch completion callbacks", () => {
  it("replacement reloads current graph sources and forwards explicit consolidation IDs", async () => {
    const h = effectHarness();
    const graph = vi.fn(async () => ({ success: true, queued: true }));
    const consolidation = vi.fn(async () => ({ success: true, queued: true }));
    h.sdk.registerFunction("mem::graph-extract", graph);
    h.sdk.registerFunction("mem::consolidate-pipeline", consolidation);
    const eligible = vi.fn(async () => true);
    registerFireworksBatchReplacement(h.sdk as never, h.kv, { canReplace: eligible } as never);
    const oldGraph = { ...workItem("graph_extraction", { observations: JSON.stringify([{ id: "obs", sessionId: "session", narrative: "stale" }]) }), id: "fwbwork_graph" };
    await h.kv.set(KV.fireworksBatchWorkItems, oldGraph.id, oldGraph);
    await h.kv.set(KV.observations("session"), "obs", { id: "obs", sessionId: "session", narrative: "current" });
    const oldCon = { ...workItem("consolidation", { tier: "semantic", sourceIds: '["session"]', cohort: "fwbcohort_0123456789abcdef" }), id: "fwbwork_con" };
    await h.kv.set(KV.fireworksBatchWorkItems, oldCon.id, oldCon);
    await h.call("mem::fireworks-batch-replace", { workItemIds: [oldGraph.id, oldCon.id] });
    expect(graph).toHaveBeenCalledWith({ observations: [{ id: "obs", sessionId: "session", narrative: "current" }], deferred: true, replacementOf: oldGraph.id });
    expect(consolidation).toHaveBeenCalledWith({ tier: "semantic", force: true, deferred: true, replacementOf: oldCon.id, batchSourceIds: ["session"], batchCohort: "fwbcohort_0123456789abcdef" });
    expect(await h.kv.get(KV.fireworksBatchWorkItems, oldGraph.id)).toEqual(oldGraph);
    consolidation.mockClear();
    await h.kv.set(KV.fireworksBatchWorkItems, oldCon.id, { ...oldCon, metadata: { tier: "semantic", sourceFingerprint: "old" } });
    const residual = await h.call("mem::fireworks-batch-replace", { workItemIds: [oldCon.id] });
    expect(JSON.stringify(residual)).toContain("source identities cannot be proven");
    expect(consolidation).not.toHaveBeenCalled();
    eligible.mockResolvedValue(false);
    graph.mockClear();
    await h.call("mem::fireworks-batch-replace", { workItemIds: [oldGraph.id] });
    expect(graph).not.toHaveBeenCalled();
  });

  it("passes partition identity and cohort into consolidation callbacks", async () => {
    const trigger = vi.fn(async () => ({ success: true }));
    await createFireworksBatchCompletionHandler(sdkWithTrigger(trigger))(workItem("consolidation", {
      tier: "semantic", sourceFingerprint: "fingerprint", sourceIds: '["session"]', cohort: "cohort",
    }), "result");
    expect(trigger).toHaveBeenCalledWith(expect.objectContaining({ payload: expect.objectContaining({ batchSourceIds: ["session"], batchCohort: "cohort", batchSourceFingerprint: "fingerprint" }) }));
  });

  it.each([
    ["graph_extraction", { observations: "[]" }, "mem::graph-extract"],
    ["reflection", { cluster: "{}" }, "mem::reflect"],
    ["crystallization", { actionIds: '["action-1"]' }, "mem::crystallize"],
    ["consolidation", { tier: "semantic" }, "mem::consolidate-pipeline"],
  ] as const)("propagates a sanitized failure for %s", async (task, metadata, functionId) => {
    const trigger = vi.fn().mockResolvedValue({
      success: false,
      error: "remote failure https://internal.example/jobs/1 Bearer secret-token token=abc\n\tprompt=private-input",
    });
    const handler = createFireworksBatchCompletionHandler(sdkWithTrigger(trigger));

    const result = await handler(workItem(task, metadata), "batch output");

    expect(result).toEqual({
      success: false,
      error: "remote failure [redacted-url] Bearer [redacted] token=[redacted] prompt=[redacted]",
    });
    expect(trigger).toHaveBeenCalledTimes(1);
    expect(trigger.mock.calls[0]?.[0]).toMatchObject({ function_id: functionId });
  });

  it("uses the fallback when a callback failure has no safe message", async () => {
    const trigger = vi.fn().mockResolvedValue({
      success: false,
      error: { stack: "secret internals" },
    });
    const handler = createFireworksBatchCompletionHandler(sdkWithTrigger(trigger));

    const result = await handler(
      workItem("reflection", { cluster: "{}" }),
      "batch output",
    );

    expect(result).toEqual({ success: false, error: "batch reflection failed" });
  });

  it("evaluates failure before stale handling", async () => {
    const trigger = vi.fn().mockResolvedValue({
      success: false,
      stale: true,
      error: "stale callback leaked https://internal.example/details",
    });
    const handler = createFireworksBatchCompletionHandler(sdkWithTrigger(trigger));

    const result = await handler(
      workItem("reflection", { cluster: "{}" }),
      "batch output",
    );

    expect(result).toEqual({
      success: false,
      error: "stale callback leaked [redacted-url]",
    });
    expect(trigger).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["reflection", { cluster: "{}" }, "mem::reflect"],
    ["crystallization", { actionIds: '["action-1"]' }, "mem::crystallize"],
    ["consolidation", { tier: "semantic" }, "mem::consolidate-pipeline"],
  ] as const)("propagates deferred %s failures", async (task, metadata, functionId) => {
    const trigger = vi.fn()
      .mockResolvedValueOnce({ success: true, stale: true })
      .mockResolvedValueOnce({
        success: false,
        stale: true,
        error: "deferred failure https://internal.example/retry secret=private",
      });
    const handler = createFireworksBatchCompletionHandler(sdkWithTrigger(trigger));

    const result = await handler(workItem(task, metadata), "batch output");

    expect(result).toEqual({
      success: false,
      error: "deferred failure [redacted-url] secret=[redacted]",
    });
    expect(trigger).toHaveBeenCalledTimes(2);
    expect(trigger.mock.calls[0]?.[0]).toMatchObject({ function_id: functionId });
    expect(trigger.mock.calls[1]?.[0]).toMatchObject({
      function_id: functionId,
      payload: { deferred: true },
    });
  });
});
