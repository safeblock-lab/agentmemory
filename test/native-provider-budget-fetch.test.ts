import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { auxiliary, context, harness, primary } from "./native-provider-budget-harness.js";
import type { ProviderBudgetResponse } from "../src/state/provider-attempt-budget.js";
import { FallbackChainProvider } from "../src/providers/fallback-chain.js";
vi.mock("../src/config.js", () => ({ getEnvVar: () => undefined }));

beforeEach(() => vi.resetModules());
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });
async function setup(ceiling = 4, expectedContext?: typeof context) {
  const budget = await import("../src/providers/provider-attempt-budget.js");
  const fetcher = await import("../src/providers/_fetch.js");
  const mock = harness(ceiling, expectedContext);
  budget.installProviderAttemptBudgetTransport(mock.transport);
  const raw = vi.fn(async (_url: string, _init?: RequestInit) => new Response(null, { status: 200 }));
  vi.stubGlobal("fetch", raw);
  const call = (url = primary, timeout = 5000, signal?: AbortSignal) => fetcher.fetchWithTimeout(url, {
    method: "POST", headers: { Authorization: "Bearer secret-header" }, body: "private prompt", signal,
  }, timeout);
  return { ...mock, ...budget, call, raw };
}
describe("native State admission at the raw fetch boundary (mocked transport only)", () => {
  it("accounts all primary retries and auxiliary fallback with distinct fresh IDs", async () => {
    const m = await setup();
    m.raw.mockImplementation(async () => new Response(null, { status: m.raw.mock.calls.length <= 3 ? 503 : 200, headers: { "Retry-After": "0" } }));
    const provider = (name: string, url: string) => ({ name, compress: async () => { const response = await m.call(url); if (!response.ok) throw new Error("unavailable"); return name; }, summarize: async () => name });
    const chain = new FallbackChainProvider([provider("primary", primary), provider("auxiliary", auxiliary)]);
    expect(await m.withProviderAttemptBudget(m.binding, () => chain.compress("", ""))).toBe("auxiliary");
    const consumes = m.transport.mock.calls.map(([r]) => r).filter((r) => r.action === "provider_budget_consume");
    expect(consumes).toHaveLength(4);
    expect(new Set(consumes.map((r) => r.attempt_id)).size).toBe(4);
    expect(m.raw).toHaveBeenCalledTimes(4);
    expect(await m.getProviderAttemptBudgetReceipt(m.binding.operationId)).toMatchObject({ consumed: 4, state: "completed" });
  });
  it("denies an exhausted fallback without another raw dispatch", async () => {
    const m = await setup(1);
    await expect(m.withProviderAttemptBudget(m.binding, async () => { await m.call(); await m.call(auxiliary); })).rejects.toThrow();
    expect(m.raw).toHaveBeenCalledTimes(1);
    expect((await m.getProviderAttemptBudgetReceipt(m.binding.operationId))?.consumed).toBe(1);
  });
  it.each(["duplicate", "wrong-attempt", "wrong-claim", "status", "malformed"])("%s ACK authorizes zero dispatch", async (mode) => {
    const m = await setup();
    m.transport.mockImplementation(async (request) => {
      const ack = await m.native(request);
      if (request.action !== "provider_budget_consume") return ack;
      if (mode === "duplicate") return { ...ack, fresh: false, status: "duplicate" };
      if (mode === "wrong-attempt") return { ...ack, attempt_id: "old-attempt" };
      if (mode === "wrong-claim") return { ...ack, claim_id: "other-claim" };
      if (mode === "status") return { ...ack, fresh: false, status: "status" };
      return { ...ack, receipt: { ...ack.receipt, remaining: -1 } } as ProviderBudgetResponse;
    });
    await expect(m.withProviderAttemptBudget(m.binding, () => m.call())).rejects.toThrow();
    expect(m.raw).not.toHaveBeenCalled();
    expect(m.receipt().consumed).toBe(1);
  });
  it("lost ACK keeps spend; caught failure cannot auto-reconsume or dispatch", async () => {
    const m = await setup();
    m.transport.mockImplementation(async (request) => { const ack = await m.native(request); if (request.action === "provider_budget_consume") throw new Error("transport lost ACK"); return ack; });
    await expect(m.withProviderAttemptBudget(m.binding, async () => {
      await m.call().catch(() => {});
      await m.call(auxiliary).catch(() => {});
    })).rejects.toThrow("attempt_outcome_unknown");
    expect(m.raw).not.toHaveBeenCalled();
    expect(m.transport.mock.calls.filter(([r]) => r.action === "provider_budget_consume")).toHaveLength(1);
    expect(m.receipt()).toMatchObject({ consumed: 1, state: "unknown" });
  });
  it("admission latency consumes the absolute deadline; late positive ACK cannot dispatch", async () => {
    const m = await setup();
    vi.useFakeTimers();
    let resolveAck!: (value: ProviderBudgetResponse) => void;
    let late: ProviderBudgetResponse | undefined;
    m.transport.mockImplementation(async (request) => {
      const ack = await m.native(request);
      if (request.action !== "provider_budget_consume") return ack;
      late = ack;
      return new Promise((resolve) => { resolveAck = resolve; });
    });
    const outcome = m.withProviderAttemptBudget(m.binding, () => m.call(primary, 40)).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(45);
    expect(await outcome).toBeInstanceOf(Error);
    resolveAck(late!);
    await Promise.resolve();
    expect(m.raw).not.toHaveBeenCalled();
    expect(m.receipt()).toMatchObject({ consumed: 1, state: "unknown" });
  });
  it.each(["pre", "during"])("%s-aborted call cannot dispatch", async (when) => {
    const m = await setup();
    const controller = new AbortController();
    if (when === "pre") controller.abort();
    m.transport.mockImplementation(async (request) => { const ack = await m.native(request); if (request.action === "provider_budget_consume") controller.abort(); return ack; });
    await expect(m.withProviderAttemptBudget(m.binding, () => m.call(primary, 5000, controller.signal))).rejects.toThrow();
    expect(m.raw).not.toHaveBeenCalled();
    expect(m.receipt().consumed).toBe(when === "pre" ? 0 : 1);
  });
  it("native time is rechecked after positive ACK before fetch", async () => {
    const m = await setup();
    vi.useFakeTimers();
    m.transport.mockImplementation(async (request) => { const ack = await m.native(request); if (request.action === "provider_budget_consume") vi.setSystemTime(Date.now() + 100); return ack; });
    await expect(m.withProviderAttemptBudget(m.binding, () => m.call(primary, 50))).rejects.toThrow("deadline_exceeded");
    expect(m.raw).not.toHaveBeenCalled();
  });
  it("elapsed admission cap rejects ACK even before its delayed timer callback", async () => {
    const m = await setup();
    vi.useFakeTimers();
    m.transport.mockImplementation(async (request) => { const ack = await m.native(request); if (request.action === "provider_budget_consume") vi.setSystemTime(Date.now() + 3100); return ack; });
    await expect(m.withProviderAttemptBudget(m.binding, () => m.call(primary, 10000))).rejects.toThrow("admission_outcome_unknown");
    expect(m.raw).not.toHaveBeenCalled();
    expect(m.receipt()).toMatchObject({ consumed: 1, state: "unknown" });
  });
  it("scoped redirects are manual; unscoped retry/redirect behavior is preserved", async () => {
    const m = await setup();
    await m.withProviderAttemptBudget(m.binding, () => m.call());
    expect(m.raw.mock.calls[0]?.[1]?.redirect).toBe("manual");
    m.raw.mockClear();
    m.raw.mockImplementation(async () => new Response(null, { status: m.raw.mock.calls.length === 1 ? 429 : 200, headers: { "Retry-After": "0" } }));
    await m.call();
    expect(m.raw).toHaveBeenCalledTimes(2);
    expect(m.raw.mock.calls[0]?.[1]?.redirect).toBeUndefined();
  });
  it("denies endpoint mismatch; receipts never contain header/body/query payload", async () => {
    const m = await setup();
    await expect(m.withProviderAttemptBudget(m.binding, () => m.call("https://unapproved.test/v1/chat"))).rejects.toThrow();
    expect(m.raw).not.toHaveBeenCalled();
    const rendered = JSON.stringify(await m.getProviderAttemptBudgetReceipt(m.binding.operationId));
    expect(rendered).not.toMatch(/secret-header|private prompt|unapproved/);
  });
  it("snapshots context and requires configured equality before callback", async () => {
    const m = await setup(4, context);
    const binding = { ...m.binding, context: { ...context } };
    await m.withProviderAttemptBudget(binding, async () => { binding.context.jobId = "mutated"; await m.call(); });
    expect(m.transport.mock.calls[0]?.[0]).toMatchObject({ context });
    expect(m.raw).toHaveBeenCalledTimes(1);
  });
  it("mismatched context is assertion failure and cannot grant authority", async () => {
    const m = await setup(4, context);
    const run = vi.fn(async () => m.call());
    await expect(m.withProviderAttemptBudget({ ...m.binding, context: { ...context, phase: "other" } }, run)).rejects.toThrow();
    expect(run).not.toHaveBeenCalled(); expect(m.raw).not.toHaveBeenCalled();
  });
  it("duplicate claim and missing transport never invoke callback", async () => {
    const budget = await import("../src/providers/provider-attempt-budget.js");
    const m = harness(); const run = vi.fn(async () => undefined);
    await expect(budget.withProviderAttemptBudget(m.binding, run)).rejects.toThrow("transport_unavailable");
    budget.installProviderAttemptBudgetTransport(async (request) => ({ ...await m.native(request), fresh: false, status: "duplicate" }));
    await expect(budget.withProviderAttemptBudget(m.binding, run)).rejects.toThrow("claim_not_fresh");
    expect(run).not.toHaveBeenCalled();
    expect(() => budget.installProviderAttemptBudgetTransport(m.transport)).toThrow();
  });
});
