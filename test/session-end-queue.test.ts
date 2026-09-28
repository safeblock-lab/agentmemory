import { describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";
import { KV } from "../src/state/schema.js";

type Handler = (request: { body: unknown }) => Promise<{
  status_code: number;
  body: unknown;
}>;

function setup(result: unknown, reject = false, intentReject = false) {
  const handlers = new Map<string, Handler>();
  const trigger = vi.fn(async () => {
    if (reject) throw new Error("private provider detail");
    return result;
  });
  const update = vi.fn(async () => ({}));
  const set = vi.fn(async () => {
    if (intentReject) throw new Error("private state detail");
    return {};
  });
  registerApiTriggers({
    registerFunction: (id: string, handler: Handler) => handlers.set(id, handler),
    registerTrigger: () => undefined,
    trigger,
  } as never, { set, update } as never);
  return { end: handlers.get("api::session::end")!, trigger, set, update };
}

describe("POST /agentmemory/session/end summary handoff", () => {
  it("acknowledges only after the queue has accepted the session", async () => {
    const { end, trigger, set, update } = setup({ success: true, queued: true, jobId: "job_1" });
    const response = await end({ body: { sessionId: "session_1" } });

    expect(response).toEqual({ status_code: 200, body: { success: true } });
    expect(KV.summaryQueueIntents).toBe("mem:summary-queue:intents");
    expect(set).toHaveBeenCalledWith("mem:summary-queue:intents", "session_1", {
      sessionId: "session_1",
      createdAt: expect.any(String),
    });
    expect(update).toHaveBeenCalledOnce();
    expect(trigger).toHaveBeenCalledWith({
      function_id: "event::session::stopped",
      payload: { sessionId: "session_1" },
    });
    expect(set.mock.invocationCallOrder[0]).toBeLessThan(update.mock.invocationCallOrder[0]);
    expect(update.mock.invocationCallOrder[0]).toBeLessThan(trigger.mock.invocationCallOrder[0]);
  });

  it("does not complete the session when the recovery intent cannot be persisted", async () => {
    const { end, update, trigger } = setup(undefined, false, true);
    expect(await end({ body: { sessionId: "session_1" } })).toEqual({
      status_code: 503,
      body: { success: false, error: "summary_enqueue_failed" },
    });
    expect(update).not.toHaveBeenCalled();
    expect(trigger).not.toHaveBeenCalled();
  });

  it("accepts a benign skip without requiring a queued job", async () => {
    const { end } = setup({ success: true, queued: false, skipped: "no_observations" });
    expect(await end({ body: { sessionId: "session_1" } })).toEqual({
      status_code: 200,
      body: { success: true },
    });
  });

  it.each([
    [{ success: false, error: "private provider detail" }],
    [undefined],
  ])("reports enqueue failure without claiming success", async (result) => {
    const { end } = setup(result);
    expect(await end({ body: { sessionId: "session_1" } })).toEqual({
      status_code: 503,
      body: { success: false, error: "summary_enqueue_failed" },
    });
  });

  it("reports rejected dispatch without exposing its cause", async () => {
    const { end } = setup(undefined, true);
    expect(await end({ body: { sessionId: "session_1" } })).toEqual({
      status_code: 503,
      body: { success: false, error: "summary_enqueue_failed" },
    });
  });
});
