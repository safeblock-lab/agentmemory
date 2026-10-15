import { afterEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { registerSummaryQueueFunctions, summarySourceFingerprint, validManualSummaryContinuation } from "../src/functions/summary-queue.js";
import { registerSummarizeFunction } from "../src/functions/summarize.js";
import { registerApiTriggers } from "../src/triggers/api.js";
import { KV } from "../src/state/schema.js";
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
vi.mock("../src/functions/dashboard-activity.js", async original => ({ ...await original<typeof import("../src/functions/dashboard-activity.js")>(), recordOperation: vi.fn() }));
vi.mock("../src/config.js", async original => ({ ...await original<typeof import("../src/config.js")>(),
  isBackgroundRecoveryPaused: () => true,
  getSummaryBudgetConfig: () => ({ contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
    maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 }) }));
async function harness() {
  const rows = new Map<string, Map<string, any>>();
  const revisions = new Map<string, number>();
  const kv = {
    get: async (scope: string, key: string) => rows.get(scope)?.get(key) ?? null,
    set: vi.fn(async (scope: string, key: string, value: unknown) => {
      if (!rows.has(scope)) rows.set(scope, new Map()); rows.get(scope)!.set(key, structuredClone(value));
      revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
    }),
    delete: async (scope: string, key: string) => {
      if (rows.get(scope)?.delete(key)) revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
    },
    list: async (scope: string) => [...(rows.get(scope)?.values() ?? [])],
    scopeRevision: async (scope: string) => ({ generation: "test", revision: String(revisions.get(scope) ?? 0) }),
    getVersioned: async (scope: string, key: string) => ({ exists: rows.get(scope)?.has(key) ?? false,
      value: rows.get(scope)?.get(key) ?? null, version: JSON.stringify(rows.get(scope)?.get(key)) }),
    pages: async function* (scope: string) { yield { items: [...(rows.get(scope)?.values() ?? [])], next_cursor: null }; },
  };
  await kv.set(KV.sessions, "s", { id: "s", project: "p", cwd: "/tmp", status: "active", observationCount: 3, startedAt: "2026-10-11" });
  for (let i = 0; i < 3; i++) await kv.set(KV.observations("s"), `o${i}`, {
    id: `o${i}`, sessionId: "s", timestamp: `2026-10-11T00:00:0${i}Z`, type: "conversation",
    title: `Inspection ${i}`, narrative: "Inspected existing behavior. ".repeat(7), facts: [], files: [], concepts: [] });
  const handlers = new Map<string, (data: any) => Promise<any>>();
  const sdk = { registerFunction: (id: string, fn: any) => handlers.set(id, fn), registerTrigger: vi.fn(),
    trigger: vi.fn(async ({ function_id, payload }: any) => {
      if (handlers.has(function_id)) return handlers.get(function_id)!(payload);
      throw new Error(`Unexpected effect: ${function_id}`);
    }) };
  let verdict = "supported";
  const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async (system: string) => {
    if (system.startsWith("You evaluate publication evidence")) return JSON.stringify({
      verdict, reason: "Original evidence supports inspection" });
    return "<summary><title>Inspection</title><narrative>Inspected existing behavior without changes.</narrative><decisions></decisions><files></files><concepts></concepts></summary>";
  }) };
  registerSummaryQueueFunctions(sdk as any, kv as any, provider as any);
  registerSummarizeFunction(sdk as any, kv as any, provider as any);
  return { kv, sdk, handlers, provider, rows, verdict: (value: string) => { verdict = value; },
    start: () => handlers.get("mem::summarize")!({ sessionId: "s", manual: true }),
    resume: (continuation: unknown) => handlers.get("mem::summarize")!({ sessionId: "s", manual: true, continuation }) };
}
afterEach(() => vi.useRealTimers());
describe("ordinary manual foreground summary", () => {
  it("completes several explicit cuts while background recovery remains paused, and replays each response without calls", async () => {
    const h = await harness();
    for (const observation of await h.kv.list(KV.observations("s")))
      await h.kv.set(KV.observations("s"), observation.id, { ...observation, narrative: observation.narrative.repeat(4) });
    let result = await h.start();
    expect(result).toMatchObject({ success: true, completed: false, continuation: { sessionId: "s", project: "p" } });
    const job = await h.kv.get(KV.summaryQueueJobs, result.continuation.jobId);
    expect(job.unitIds.length).toBeGreaterThan(1);
    expect(job.executionOrigin).toBe("manual"); expect(h.provider.summarize).not.toHaveBeenCalled();
    let cuts = 0;
    while (!result.completed && cuts++ < 20) {
      const token = result.continuation;
      result = await h.resume(token);
      const after = h.provider.summarize.mock.calls.length;
      expect(await h.resume(token)).toEqual(result);
      expect(h.provider.summarize.mock.calls.length).toBe(after);
    }
    expect(result).toMatchObject({ success: true, completed: true, summary: { sessionId: "s", title: "Inspection" } });
    expect(cuts).toBeGreaterThan(1);
    expect((await h.kv.get(KV.summaryQueueJobs, job.id)).status).toBe("completed");
    expect(h.sdk.trigger.mock.calls.some(([call]) => call.function_id === "iii::durable::publish")).toBe(false);
    expect(await h.handlers.get("mem::summary-reconcile")!({})).toMatchObject({ paused: true });
  });
  it("rejects foreign, altered, stale and extra continuation fields without calls", async () => {
    const h = await harness(); const first = await h.start(); const token = first.continuation;
    for (const change of [{ sessionId: "foreign" }, { project: "foreign" }, { sourceFingerprint: "foreign" },
      { jobId: "foreign" }, { unitId: "foreign" }, { deliveryId: "foreign" }, { round: 9 }, { step: 9 }, { selected: {} }]) {
      expect((await h.resume({ ...token, ...change })).success).toBe(false);
    }
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("stops on changed source, limits and busy state without hidden retries", async () => {
    const h = await harness(); const first = await h.start(); const token = first.continuation;
    const operation = await h.kv.get(KV.state, token.operationId);
    await h.kv.set(KV.state, token.operationId, { ...operation, maxRequests: 0 });
    expect(await h.resume(token)).toMatchObject({ success: false, error: "manual_summary_limit_exceeded" });
    expect(await h.kv.get(KV.summaryQueueJobs, token.jobId)).toMatchObject({
      status: "failed", failure: "manual_summary_limit_exceeded",
    });
    expect(await h.kv.get(KV.summaryQueueActive, "s")).toBeNull();
    expect(h.provider.summarize).not.toHaveBeenCalled();

    const busyHarness = await harness(); const busyFirst = await busyHarness.start();
    const busyToken = busyFirst.continuation;
    const unit = await busyHarness.kv.get(KV.summaryQueueUnits(busyToken.jobId), busyToken.unitId);
    await busyHarness.kv.set(KV.summaryQueueUnits(busyToken.jobId), busyToken.unitId,
      { ...unit, startedAt: new Date().toISOString() });
    expect(await busyHarness.resume(busyToken)).toMatchObject({ success: true, busy: true, completed: false });
    expect(busyHarness.provider.summarize).not.toHaveBeenCalled();
  });
  it("retains an unsupported judgement and does not regenerate on replay", async () => {
    const h = await harness(); h.verdict("unsupported"); let result = await h.start();
    for (let i = 0; i < 20 && result.success && !result.completed; i++) result = await h.resume(result.continuation);
    expect(result).toMatchObject({ success: false }); expect(await h.kv.get(KV.summaries, "s")).toBeNull();
  });
  it("recovers canonical publication after lost ACK and removed sources with zero new provider calls", async () => {
    const h = await harness(); let result = await h.start(); let token: any; let failed = false;
    const set = h.kv.set.getMockImplementation()!;
    h.kv.set.mockImplementation(async (scope, key, value) => {
      if (scope === KV.summaryQueueCompleted && !failed) { failed = true; throw new Error("lost ACK"); }
      return set(scope, key, value);
    });
    for (let i = 0; i < 20 && !failed; i++) {
      token = result.continuation;
      try { result = await h.resume(token); } catch (error) { expect(String(error)).toContain("lost ACK"); }
    }
    expect(failed).toBe(true); expect(await h.kv.get(KV.summaries, "s")).not.toBeNull();
    h.rows.get(KV.observations("s"))!.clear(); const before = h.provider.summarize.mock.calls.length;
    expect(await h.resume(token)).toMatchObject({ success: true, completed: true, summary: { sessionId: "s" } });
    expect(h.provider.summarize.mock.calls.length).toBe(before);
  });
  it("fails stale original sources before any provider call", async () => {
    const h = await harness(); const result = await h.start();
    const obs = await h.kv.get(KV.observations("s"), "o0");
    await h.kv.set(KV.observations("s"), "o0", { ...obs, narrative: "Changed source" });
    expect(await h.resume(result.continuation)).toMatchObject({ success: false, error: "selection_source_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it.each(["maxRequests"])("enforces %s before dispatch", async field => {
    const h = await harness(); const first = await h.start(); const token = first.continuation;
    const operation = await h.kv.get(KV.state, token.operationId);
    await h.kv.set(KV.state, token.operationId, { ...operation, [field]: 0 });
    expect(await h.resume(token)).toMatchObject({ success: false, error: "manual_summary_limit_exceeded" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("resumes the same ordinary operation after long idle and worker restart", async () => {
    const h = await harness(); const first = await h.start(); const token = first.continuation;
    const operation = await h.kv.get(KV.state, token.operationId);
    expect(operation.deadline).toBeUndefined(); expect(first.deadline).toBeUndefined();
    await h.kv.set(KV.state, token.operationId, { ...operation, deadline: 0 });
    registerSummaryQueueFunctions(h.sdk as any, h.kv as any, h.provider as any);
    expect(await h.resume(token)).toMatchObject({ success: true, completed: true });
    expect((await h.kv.get(KV.summaryQueueJobs, token.jobId)).status).toBe("completed");
  });
  it("bounds a timed-out provider, retains unknown dispatch, and never automatically retries it", async () => {
    const h = await harness(); const first = await h.start();
    vi.useFakeTimers(); h.provider.summarize.mockImplementation(() => new Promise<string>(() => {}));
    const pending = h.resume(first.continuation);
    await vi.waitFor(() => expect(h.provider.summarize).toHaveBeenCalledTimes(1));
    await vi.advanceTimersByTimeAsync(150000);
    const result = await pending;
    expect(result).toMatchObject({ success: false, completed: false, error: "timeout" });
    expect(await h.resume(first.continuation)).toEqual(result);
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    const job = await h.kv.get(KV.summaryQueueJobs, first.continuation.jobId);
    expect(await h.kv.get(KV.state, `${job.id}:raw:${first.continuation.unitId}`)).toMatchObject({ state: "intended" });
  });
  it("terminalizes an exhausted request without an ACK while preserving its unknown provider intent", async () => {
    const h = await harness(); const first = await h.start(); const token = first.continuation;
    const operation = await h.kv.get(KV.state, token.operationId);
    const unresolvedRequest = { request: token };
    await h.kv.set(KV.state, token.operationId, { ...operation, step: 1,
      maxRequests: 1, deadline: Date.now() - 1 });
    await h.kv.set(KV.state, `${token.operationId}:request:${token.step}`, unresolvedRequest);
    const rawKey = `${token.jobId}:raw:${token.unitId}`;
    const rawIntent = { state: "intended" as const };
    await h.kv.set(KV.state, rawKey, rawIntent);

    expect(await h.resume(token)).toMatchObject({
      success: false, completed: false, error: "manual_summary_limit_exceeded",
    });
    expect(await h.kv.get(KV.summaryQueueJobs, token.jobId)).toMatchObject({
      status: "failed", failure: "manual_summary_limit_exceeded",
    });
    expect(await h.kv.get(KV.summaryQueueActive, "s")).toBeNull();
    expect(await h.kv.get(KV.state, rawKey)).toEqual(rawIntent);
    expect(await h.kv.get(KV.state, `${token.operationId}:request:${token.step}`)).toEqual(unresolvedRequest);
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("preserves a late raw response after terminalization and blocks its output admission", async () => {
    const h = await harness(); const first = await h.start();
    let finishProvider!: (raw: string) => void;
    h.provider.summarize.mockImplementation(() => new Promise<string>(resolve => { finishProvider = resolve; }));
    vi.useFakeTimers();
    const pending = h.resume(first.continuation);
    await vi.waitFor(() => expect(finishProvider).toBeTypeOf("function"));
    const jobId = first.continuation.jobId;
    const job = await h.kv.get(KV.summaryQueueJobs, jobId);
    await h.kv.set(KV.summaryQueueJobs, jobId, { ...job, status: "failed", failure: "manual_summary_limit_exceeded" });
    await h.kv.delete(KV.summaryQueueActive, "s");
    await vi.advanceTimersByTimeAsync(150_000);
    expect(await pending).toMatchObject({ success: false });

    const raw = "<summary><title>Late</title><narrative>Captured after the request timed out.</narrative></summary>";
    finishProvider(raw);
    await vi.advanceTimersByTimeAsync(0);
    expect(await h.kv.get(KV.state, `${jobId}:raw:${first.continuation.unitId}`)).toEqual({ state: "captured", raw });
    const lateUnit = await h.kv.get(KV.summaryQueueUnits(jobId), first.continuation.unitId);
    expect(lateUnit.output).toBeUndefined();
    expect(lateUnit.summary).toBeUndefined();
    expect(await h.kv.get(KV.summaryQueueJobs, jobId)).toMatchObject({ status: "failed" });
  });
  it("adopts only an unadmitted normal intent and never schedules its backlog", async () => {
    const h = await harness();
    await h.kv.set(KV.summaryQueueIntents, "s", { sessionId: "s", createdAt: "old" });
    const result = await h.start();
    expect(result).toMatchObject({ success: true, completed: false });
    expect(await h.kv.get(KV.summaryQueueIntents, "s")).toMatchObject({ executionOrigin: "manual" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("does not adopt a selected job or rewrite selected intent", async () => {
    const h = await harness(); await h.kv.set(KV.summaryQueueIntents, "s", { sessionId: "s", createdAt: "old", executionOrigin: "selected" });
    expect(await h.start()).toMatchObject({ success: false, error: "selection_origin_mismatch" });
    expect(await h.kv.get(KV.summaryQueueIntents, "s")).toMatchObject({ executionOrigin: "selected" });
  });
});


describe("manual REST and viewer continuation", () => {
  it("whitelists public fields and rejects invalid or foreign continuation at the registered REST boundary", async () => {
    const handlers = new Map<string, any>(); const trigger = vi.fn(async () => ({ success: true }));
    registerApiTriggers({ registerFunction: (id: string, fn: any) => handlers.set(id, fn), registerTrigger: vi.fn(), trigger } as any,
      {} as any, undefined as any, undefined as any);
    const api = handlers.get("api::summarize");
    expect(await api({ body: { sessionId: "s", unrelated: 99 } })).toMatchObject({ status_code: 200 });
    expect(trigger).toHaveBeenLastCalledWith({ function_id: "mem::summarize", timeoutMs: 360_000,
      payload: { sessionId: "s", manual: true } });
    trigger.mockClear();
    expect(await api({ body: { sessionId: "s", selected: { fake: true } } })).toMatchObject({ status_code: 200 });
    expect(trigger).toHaveBeenLastCalledWith({ function_id: "mem::summarize", timeoutMs: 360_000, payload: { sessionId: "s", manual: true } });
    trigger.mockClear();
    expect(await api({ body: { sessionId: "s", continuation: { jobId: "x" } } })).toMatchObject({ status_code: 400 });
    expect(trigger).not.toHaveBeenCalled();
    const h = await harness(); const first = await h.start();
    expect(validManualSummaryContinuation(first.continuation)).toBe(true);
    await api({ body: { sessionId: "s", continuation: first.continuation, unrelated: 1 } });
    expect(trigger).toHaveBeenLastCalledWith({ function_id: "mem::summarize", timeoutMs: 360_000, payload: { sessionId: "s", manual: true, continuation: first.continuation } });
  });
  it("the actual viewer function advances only its requested operation and stops on busy, error or ceiling", async () => {
    const html = readFileSync("src/viewer/index.html", "utf8");
    const source = html.slice(html.indexOf("async function summarizeSession("), html.indexOf("async function loadLessons("));
    for (const stop of ["done", "busy", "error", "ceiling"]) {
      const button = { textContent: "Summarize", disabled: false }; const calls: any[] = []; const timers: any[] = [];
      const continuation = { operationId: "op", sessionId: "s", step: 0 };
      const pending = { success: true, completed: false, continuation, maxRequests: 2, deadline: Date.now() + 10000 };
      const apiPost = vi.fn(async (_path: string, payload: any) => {
        calls.push(payload);
        if (calls.length === 1 || stop === "ceiling") return pending;
        if (stop === "busy") return { ...pending, busy: true };
        if (stop === "error") return { success: false, error: "safe" };
        return { success: true, completed: true, summary: { title: "Complete" } };
      });
      const run = runInNewContext(source + "; summarizeSession", { apiPost, Date, Number, setTimeout: (fn: any, delay: number) => timers.push({ fn, delay }) });
      await run("s", button);
      expect(button.textContent).toBe(stop === "done" ? "Done" : stop === "busy" ? "Busy" : stop === "error" ? "Failed" : "Limit reached");
      expect(calls.length).toBe(stop === "ceiling" ? 3 : 2);
      expect(calls.slice(1).every(value => value.sessionId === "s" && value.continuation.operationId === "op")).toBe(true);
      expect(button.disabled).toBe(true); expect(timers[0].delay).toBe(2000); timers[0].fn(); expect(button.disabled).toBe(false);
    }
  });
});
