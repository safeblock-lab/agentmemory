import { describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";
import { registerSnapshotFunction } from "../src/functions/snapshot.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { effectHarness } from "./batch-effects-harness.js";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

type Response = { status_code: number; body: unknown };

function registerSnapshot(result: unknown, secret = "") {
  const handlers = new Map<string, (request: unknown) => Promise<Response>>();
  const trigger = vi.fn(async () => result);
  const sdk = {
    registerFunction: (id: string, handler: (request: unknown) => Promise<Response>) => handlers.set(id, handler),
    registerTrigger: () => {},
    trigger,
  };
  registerApiTriggers(sdk as never, { list: async () => [], get: async () => null } as never, secret);
  return { handle: handlers.get("api::snapshot-create")!, trigger };
}

describe("api::snapshot-create status", () => {
  it("returns 201 with the created snapshot", async () => {
    const result = { success: true, snapshot: { commitHash: "abc1234" } };
    const { handle, trigger } = registerSnapshot(result);
    expect(await handle({ body: { message: "manual" }, headers: {} })).toEqual({ status_code: 201, body: result });
    expect(trigger).toHaveBeenCalledWith({ function_id: "mem::snapshot-create", payload: { message: "manual" }, timeoutMs: 600000 });
  });

  it.each([
    { success: false, deferred: true, retryable: true, code: "BATCH_MAINTENANCE_BUSY", details: { activeAdmissions: 1 } },
    { success: false, code: "BATCH_MAINTENANCE_BUSY" },
  ])("returns 503 without hiding busy details: %j", async (result) => {
    const { handle } = registerSnapshot(result);
    expect(await handle({ headers: {} })).toEqual({ status_code: 503, body: result });
  });

  it.each([{ success: false, error: "Snapshot failed" }, null, undefined])("returns 500 for unsuccessful results: %j", async (result) => {
    const { handle } = registerSnapshot(result);
    expect(await handle({ headers: {} })).toEqual({ status_code: 500, body: result });
  });

  it("preserves the disabled endpoint response", async () => {
    const { handle, trigger } = registerSnapshot(null);
    trigger.mockRejectedValueOnce(new Error("Function not registered"));
    expect(await handle({ headers: {} })).toEqual({ status_code: 404, body: { error: "Snapshots not enabled" } });
  });

  it("rejects unauthorized callers before creating a snapshot", async () => {
    const { handle, trigger } = registerSnapshot({ success: true }, "test-secret");
    expect(await handle({ headers: {} })).toEqual({ status_code: 401, body: { error: "unauthorized" } });
    expect(trigger).not.toHaveBeenCalled();
    expect((await handle({ headers: { authorization: "Bearer test-secret" } })).status_code).toBe(201);
  });

  it("returns 503 while a real semantic callback awaits its provider, then preserves its completed facts", async () => {
    vi.useFakeTimers();
    const h = effectHarness();
    const sdk = { ...h.sdk, registerTrigger: () => {} };
    const snapshotDir = resolve(".native-pagination-build/background-health-20261004/verification/snapshot-busy-test");
    let entered!: () => void, finish!: () => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const providerResult = new Promise<void>((resolve) => { finish = resolve; });
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async () => {
        entered(); await providerResult;
        return '<facts><fact confidence="0.9" sourceIds="ses_0,ses_1">Preserve source evidence</fact></facts>';
      }),
    };
    registerApiTriggers(sdk as never, h.kv, "");
    registerSnapshotFunction(sdk as never, h.kv, snapshotDir);
    registerConsolidationPipelineFunction(sdk as never, h.kv, provider as never);
    for (let index = 0; index < 5; index++) {
      h.seed("mem:summaries", `ses_${index}`, {
        sessionId: `ses_${index}`, title: "Source evidence", narrative: "Retain original records",
        concepts: ["evidence"], createdAt: new Date(2026, 0, index + 1).toISOString(),
      });
    }
    const consolidation = h.call("api::consolidate-pipeline", { headers: {}, body: { tier: "semantic", force: true } });
    try {
      await started;
      const snapshot = h.call<Response>("api::snapshot-create", { headers: {}, body: {} });
      await vi.advanceTimersByTimeAsync(30001);
      expect(await snapshot).toMatchObject({ status_code: 503, body: {
        success: false, deferred: true, retryable: true, code: "BATCH_MAINTENANCE_BUSY",
        details: { families: [{ family: "consolidation", count: 1 }] },
      } });
      expect(existsSync(snapshotDir)).toBe(false);
      finish();
      expect(await consolidation).toMatchObject({ status_code: 200, body: { results: { semantic: { newFacts: 1 } } } });
      expect(await h.kv.list("mem:semantic")).toMatchObject([{ sourceSessionIds: ["ses_0", "ses_1"] }]);
      expect(await h.kv.get("mem:state", "semantic-consolidation")).toBeTruthy();
      expect(provider.summarize).toHaveBeenCalledOnce();
    } finally { finish(); await consolidation; vi.useRealTimers(); }
  });
});
