import { describe, expect, it, vi } from "vitest";
import type { ISdk } from "iii-sdk";
import type { FireworksBatchWorkItem } from "../src/types.js";

process.env["VITEST"] = "true";
const { createFireworksBatchCompletionHandler } = await import("../src/index.js");

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
