import { describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";
import { KV } from "../src/state/schema.js";

type Handler = (request: { body: unknown }) => Promise<{
  status_code: number;
  body: unknown;
}>;

function setup(result: unknown, reject = false, intentReject = false, initialIntent?: unknown) {
  const handlers = new Map<string, Handler>();
  const intents = new Map<string, unknown>();
  if (initialIntent !== undefined) intents.set("session_1", initialIntent);
  const trigger = vi.fn(async () => {
    if (reject) throw new Error("private provider detail");
    return result;
  });
  const update = vi.fn(async () => ({}));
  const get = vi.fn(async (scope: string, key: string) =>
    scope === KV.summaryQueueIntents ? intents.get(key) ?? null : null);
  const set = vi.fn(async (scope: string, key: string, value: unknown) => {
    if (intentReject) throw new Error("private state detail");
    if (scope === KV.summaryQueueIntents) intents.set(key, structuredClone(value));
    return {};
  });
  registerApiTriggers({
    registerFunction: (id: string, handler: Handler) => handlers.set(id, handler),
    registerTrigger: () => undefined,
    trigger,
  } as never, { get, set, update } as never);
  return { end: handlers.get("api::session::end")!, trigger, get, set, update };
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

  it("preserves a genuine selected origin across repeated session close", async () => {
    const selectedIntent = {
      sessionId: "session_1",
      createdAt: "2026-10-10T23:00:00.000Z",
      executionOrigin: "selected",
    };
    const { end, set, get } = setup({ success: true, queued: true }, false, false, selectedIntent);

    expect((await end({ body: { sessionId: "session_1" } })).status_code).toBe(200);
    expect((await end({ body: { sessionId: "session_1" } })).status_code).toBe(200);

    expect(get).toHaveBeenCalledTimes(2);
    expect(set).toHaveBeenNthCalledWith(1, KV.summaryQueueIntents, "session_1", selectedIntent);
    expect(set).toHaveBeenNthCalledWith(2, KV.summaryQueueIntents, "session_1", selectedIntent);
  });

  it("does not copy an origin from an intent for a different session", async () => {
    const { end, set } = setup({ success: true }, false, false, {
      sessionId: "foreign_session", createdAt: "2026-10-10T23:00:00.000Z", executionOrigin: "selected",
    });

    await end({ body: { sessionId: "session_1" } });

    expect(set).toHaveBeenCalledWith(KV.summaryQueueIntents, "session_1", {
      sessionId: "session_1", createdAt: expect.any(String),
    });
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
