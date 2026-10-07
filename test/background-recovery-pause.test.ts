import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { isBackgroundRecoveryPaused } from "../src/config.js";
import { registerSummaryQueueFunctions } from "../src/functions/summary-queue.js";
import { KV } from "../src/state/schema.js";
import type { MemoryProvider } from "../src/types.js";

const PAUSE_ENV = "AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED";
let originalPauseValue: string | undefined;

beforeEach(() => {
  originalPauseValue = process.env[PAUSE_ENV];
});

afterEach(() => {
  if (originalPauseValue === undefined) delete process.env[PAUSE_ENV];
  else process.env[PAUSE_ENV] = originalPauseValue;
});

function createQueueHarness() {
  const rows = new Map<string, Map<string, unknown>>();
  const operations: Array<{ method: string; scope: string; key?: string }> = [];
  const handlers = new Map<string, (payload: never) => Promise<unknown>>();
  const triggers: string[] = [];
  const providerCalls: string[] = [];
  const kv = {
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      operations.push({ method: "get", scope, key });
      return (rows.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, value: T): Promise<T> => {
      operations.push({ method: "set", scope, key });
      if (!rows.has(scope)) rows.set(scope, new Map());
      rows.get(scope)!.set(key, value);
      return value;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      operations.push({ method: "delete", scope, key });
      rows.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      operations.push({ method: "list", scope });
      return [...(rows.get(scope)?.values() ?? [])] as T[];
    },
  };
  const sdk = {
    registerFunction: (id: string, handler: (payload: never) => Promise<unknown>) => handlers.set(id, handler),
    registerTrigger: () => undefined,
    trigger: async ({ function_id }: { function_id: string }) => {
      triggers.push(function_id);
      return { success: true };
    },
  };
  const provider: MemoryProvider = {
    name: "test",
    compress: async () => "",
    summarize: async () => {
      providerCalls.push("summarize");
      return "";
    },
  };
  registerSummaryQueueFunctions(sdk as never, kv as never, provider);
  return {
    kv,
    rows,
    operations,
    triggers,
    providerCalls,
    invoke: (id: string, payload: unknown = {}) => handlers.get(id)!(payload as never),
  };
}

describe("background recovery pause", () => {
  it("is off by default and opts in with the documented value", () => {
    delete process.env[PAUSE_ENV];
    expect(isBackgroundRecoveryPaused()).toBe(false);

    process.env[PAUSE_ENV] = "1";
    expect(isBackgroundRecoveryPaused()).toBe(true);

    process.env[PAUSE_ENV] = "0";
    expect(isBackgroundRecoveryPaused()).toBe(false);
  });

  it("stores summary intent and pauses queue work before listing observations or changing jobs", async () => {
    process.env[PAUSE_ENV] = "1";
    const h = createQueueHarness();
    await h.kv.set(KV.sessions, "session", {
      id: "session", project: "project", cwd: ".", startedAt: "2026-10-07T00:00:00Z",
      status: "completed", observationCount: 1,
    });
    h.operations.length = 0;

    expect(await h.invoke("mem::summary-enqueue", { sessionId: "session" }))
      .toMatchObject({ success: true, queued: false, paused: true });
    expect(await h.kv.get(KV.summaryQueueIntents, "session"))
      .toMatchObject({ sessionId: "session" });
    expect(h.operations.some(({ method }) => method === "list")).toBe(false);
    expect(h.operations.some(({ scope }) => scope === KV.observations("session"))).toBe(false);
    expect(await h.kv.list(KV.summaryQueueJobs)).toEqual([]);
    expect(h.providerCalls).toEqual([]);
    expect(h.triggers).toEqual([]);

    h.operations.length = 0;
    expect(await h.invoke("mem::summary-dispatch", { jobId: "job", round: 0, offset: 0 }))
      .toMatchObject({ success: true, paused: true });
    expect(await h.invoke("mem::summary-unit", { jobId: "job", unitId: "unit" }))
      .toMatchObject({ success: true, skipped: true, paused: true });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ paused: true });
    expect(await h.invoke("mem::summary-recover")).toMatchObject({ paused: true });
    expect(h.operations).toEqual([]);
    expect(h.providerCalls).toEqual([]);
    expect(h.triggers).toEqual([]);
  });
});
