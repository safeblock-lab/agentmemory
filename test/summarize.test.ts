import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerSummarizeFunction } from "../src/functions/summarize.js";
import { registerApiTriggers } from "../src/triggers/api.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { withKeyedLock } from "../src/state/keyed-mutex.js";
import type { CompressedObservation, Session } from "../src/types.js";
import { WorkflowAttemptAccounting, type WorkflowAttemptIdentity } from "../src/functions/workflow-attempt-accounting.js";
const attempts = vi.hoisted(() => ({ enter: vi.fn() }));
vi.mock("../src/providers/provider-attempt-budget.js", () => ({
  withProviderAttemptBudget: async (binding: unknown, run: () => Promise<unknown>) => { attempts.enter(binding); return run(); },
}));

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
vi.mock("../src/functions/dashboard-activity.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/functions/dashboard-activity.js")>(), recordOperation: vi.fn(),
}));

export async function harness(count = 1, accounting?: WorkflowAttemptAccounting) {
  const rows = new Map<string, Map<string, unknown>>();
  const kv = {
    get: async <T>(scope: string, key: string) => rows.get(scope)?.get(key) as T ?? null,
    set: vi.fn(async (scope: string, key: string, value: unknown) => {
      if (!rows.has(scope)) rows.set(scope, new Map());
      rows.get(scope)!.set(key, structuredClone(value));
    }),
    getVersioned: async <T>(scope: string, key: string) => ({ exists: rows.get(scope)?.has(key) ?? false,
      value: rows.get(scope)?.get(key) as T ?? null, version: JSON.stringify(rows.get(scope)?.get(key)) }),
    list: async <T>(scope: string) => [...(rows.get(scope)?.values() ?? [])] as T[],
    pages: async function* <T>(scope: string, options: { limit: number }) {
      const values = [...(rows.get(scope)?.values() ?? [])] as T[];
      for (let start = 0; start < values.length; start += options.limit)
        yield { items: values.slice(start, start + options.limit), next_cursor: start + options.limit < values.length ? "more" : null };
    },
  };
  const session: Session = { id: "s", project: "project", cwd: "/tmp", startedAt: "2026-10-11", status: "active", observationCount: count };
  await kv.set(KV.sessions, "s", session);
  for (let i = 0; i < count; i++) await kv.set(KV.observations("s"), `o${i}`, {
    id: `o${i}`, sessionId: "s", timestamp: "2026-10-11", type: "tool_use", title: `Read source ${i}`,
    narrative: "Opened src/a.ts to inspect existing behavior. No changes were made.", facts: ["Inspected existing behavior"],
    files: ["src/a.ts"], concepts: [],
  } as CompressedObservation);
  let handler: (data: unknown) => Promise<any>;
  const sdk = { registerFunction: (_id: string, fn: typeof handler) => { handler = fn; },
    trigger: vi.fn(async () => ({ success: true, queued: true, jobId: "queue-job", continuation: { jobId: "queue-job" } })) };
  let verdict = "supported";
  let generated = "<summary><title>Inspection</title><narrative>The existing source behavior was inspected without making changes.</narrative><decisions></decisions><files></files><concepts></concepts></summary>";
  const provider = { name: "test", compress: vi.fn(),
    prepareRequest: (_operation: string, system: string, prompt: string, options: unknown) => ({
      endpoint: "POST https://provider.example/v1/chat/completions", body: JSON.stringify({ system, prompt, options }) }),
    summarize: vi.fn(async (system: string, prompt: string) => {
      if (system.startsWith("You evaluate publication evidence")) {
        return verdict === "invalid" ? "bad json" : JSON.stringify({ verdict, reason: "Controlled semantic fixture" });
      }
      return generated;
    }),
  };
  registerSummarizeFunction(sdk as any, kv as any, provider as any, undefined, undefined, accounting);
  kv.set.mockClear();
  return { kv, rows, sdk, provider, run: (data: unknown = { sessionId: "s" }) => handler!(data),
    verdict: (value: string) => { verdict = value; }, raw: (value: string) => { generated = value; } };
}

describe("direct summary public contract", () => {
  const original = { ...process.env };
  beforeEach(() => { process.env.AGENTMEMORY_SUMMARY_CONTEXT_TOKENS = "1000000";
    process.env.AGENTMEMORY_SUMMARY_OUTPUT_TOKENS = "8192"; process.env.AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS = "4096";
    delete process.env.SUMMARIZE_CHUNK_SIZE; });
  afterEach(() => { process.env = { ...original }; });
  it("dispatches generation and publication through distinct daemon identities, replay dispatches neither", async () => {
    const accounting = new WorkflowAttemptAccounting([], []);
    const run = vi.spyOn(accounting, "run");
    const h = await harness(1, accounting);
    expect((await h.run()).success).toBe(true);
    const identities = run.mock.calls.map(call => call[1]);
    expect(identities.map(value => value.phase)).toEqual(["generation", "publication"]);
    expect(identities[0]).toMatchObject({ functionId: "mem::summarize", sourceId: "s", continuation: "direct" });
    expect(identities[0].unitId).not.toBe(identities[1].unitId);
    await h.run(); expect(run).toHaveBeenCalledTimes(2); expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("denies a scoped unmatched direct unit before any provider call", async () => {
    const accounting = new WorkflowAttemptAccounting([{ workflow: "summary", project: "project", sourceId: "s" }], []);
    const h = await harness(1, accounting);
    expect(await h.run()).toMatchObject({ success: false, error: "workflow_binding_identity_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(await h.run()).toMatchObject({ success: false, error: "summary_dispatch_outcome_unknown" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("uses separately approved private bindings at actual direct dispatch; capture replay makes no claims", async () => {
    const discovery = new WorkflowAttemptAccounting([], []); const spy = vi.spyOn(discovery, "run");
    await (await harness(1, discovery)).run();
    const identities = spy.mock.calls.map(call => call[1]);
    const bindings = identities.map((identity, index) => ({ ...identity, operationId: `approved-${index}`, windowId: "private-window", ceiling: 2 }));
    attempts.enter.mockClear();
    const h = await harness(1, new WorkflowAttemptAccounting([identities[0]], bindings));
    expect((await h.run()).success).toBe(true);
    expect(attempts.enter.mock.calls.map(call => call[0])).toEqual(bindings.map(value => ({
      operationId: value.operationId, windowId: value.windowId, ceiling: value.ceiling,
      context: { workflow: value.workflow, functionId: value.functionId, sourceId: value.sourceId,
        jobId: value.jobId, unitId: value.unitId, phase: value.phase,
        continuationId: fingerprintId("workflow-continuation", value.continuation) } })));
    await h.run(); expect(attempts.enter).toHaveBeenCalledTimes(2); expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("preserves the completed success shape after generation plus judge", async () => {
    const h = await harness();
    expect(await h.run()).toMatchObject({ success: true, summary: { sessionId: "s", observationCount: 1 }, qualityScore: expect.any(Number) });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("waits for the shared canonical lock before publishing the summary", async () => {
    const h = await harness();
    let signalLockHeld!: () => void;
    let releaseLock!: () => void;
    const lockHeld = new Promise<void>(resolve => { signalLockHeld = resolve; });
    const lockRelease = new Promise<void>(resolve => { releaseLock = resolve; });
    const heldLock = withKeyedLock("summary-canonical:s", async () => {
      signalLockHeld();
      await lockRelease;
    });
    await lockHeld;

    let settled = false;
    const result = h.run().finally(() => { settled = true; });
    try {
      await vi.waitFor(async () => {
        expect(await h.kv.get(KV.state, "direct-summary:s")).toMatchObject({
          effect: expect.objectContaining({ sessionId: "s", project: "project" }),
        });
      });
      await new Promise(resolve => setTimeout(resolve, 0));
      expect(await h.kv.get(KV.summaries, "s")).toBeNull();
      expect(settled).toBe(false);
    } finally {
      releaseLock();
    }

    await heldLock;
    expect(await result).toMatchObject({ success: true, summary: { sessionId: "s" } });
    expect(await h.kv.get(KV.summaries, "s")).toMatchObject({ sessionId: "s", project: "project" });
  });
  it.each(["deleted", "replaced", "project-changed"] as const)(
    "rejects late publication when the session is %s before the canonical write",
    async change => {
      const h = await harness();
      const originalSet = h.kv.set.getMockImplementation()!;
      h.kv.set.mockImplementation(async (scope, key, value) => {
        await originalSet(scope, key, value);
        if (scope !== KV.state || key !== "direct-summary:s" || !(value as any)?.effect) return;
        const sessions = h.rows.get(KV.sessions)!;
        if (change === "deleted") sessions.delete("s");
        else if (change === "replaced") {
          const current = sessions.get("s") as Session;
          sessions.set("s", { ...current, startedAt: "2026-10-12T00:00:00.000Z" });
        } else {
          const current = sessions.get("s") as Session;
          sessions.set("s", { ...current, project: "replacement-project" });
        }
      });

      expect(await h.run()).toMatchObject({ success: false, error: "summary_source_changed" });
      expect(h.provider.summarize).toHaveBeenCalledTimes(2);
      expect(h.kv.set.mock.calls.filter(([scope]) => scope === KV.summaries)).toHaveLength(0);
      expect(await h.kv.get(KV.summaries, "s")).toBeNull();
    },
  );
  it("returns no_observations with zero calls", async () => {
    const h = await harness(0);
    expect(await h.run()).toEqual({ success: false, error: "no_observations" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("rejects missing session and input", async () => {
    const h = await harness();
    expect(await h.run({})).toMatchObject({ success: false });
    expect(await h.run({ sessionId: "missing" })).toMatchObject({ error: "session_not_found" });
  });
  it.each([129, 4096])("queues %i observations from an active session without partial publication", async count => {
    const h = await harness(count);
    expect(await h.run()).toMatchObject({ success: true, queued: true, completed: false, jobId: "queue-job" });
    expect(h.sdk.trigger).toHaveBeenCalledWith({ function_id: "mem::summary-enqueue", payload: { sessionId: "s" } });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(await h.kv.get(KV.summaries, "s")).toBeNull();
    expect((await h.kv.get<Session>(KV.sessions, "s"))!.status).toBe("active");
  });
  it("queues configured chunking even when original evidence fits", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "1";
    const h = await harness(2);
    expect(await h.run()).toMatchObject({ queued: true, completed: false });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("resumes a completed queue with the historical success shape", async () => {
    const h = await harness(129);
    h.sdk.trigger.mockImplementationOnce(async () => {
      await h.kv.set(KV.summaries, "s", { sessionId: "s", project: "project", title: "Queued", narrative: "A grounded complete queued result.",
        keyDecisions: [], filesModified: [], concepts: [], observationCount: 129 });
      return { success: true, completed: true } as any;
    });
    expect(await h.run()).toMatchObject({ success: true, summary: { title: "Queued" }, qualityScore: expect.any(Number) });
  });
  it("invalid raw is retained with no automatic retry", async () => {
    const h = await harness(); h.raw("<garbage/>");
    expect(await h.run()).toMatchObject({ error: "parse_failed" });
    expect(await h.run()).toMatchObject({ error: "parse_failed" });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(await h.kv.get(KV.state, "direct-summary:s")).toMatchObject({ raw: "<garbage/>" });
  });
});

describe("manual summary REST adapter", () => {
  async function adapter() {
    const h = await harness();
    const handlers = new Map<string, (request: any) => Promise<any>>();
    const trigger = vi.fn(async () => ({ success: true, completed: true }));
    const registerTrigger = vi.fn();
    registerApiTriggers({ registerFunction: (id: string, fn: (request: any) => Promise<any>) => handlers.set(id, fn),
      registerTrigger, trigger } as never, h.kv as never, "test-secret");
    return { ...h, trigger, registerTrigger, handlers,
      request: (body: unknown) => handlers.get("api::summarize")!({ body }) };
  }
  it("uses ordinary continuation routing and drops removed provider controls", async () => {
    const h = await adapter();
    const continuation = { operationId: "recovery", step: 0, jobId: "child", project: "project", sessionId: "s",
      sourceFingerprint: "source", round: 0, unitId: "unit", deliveryId: "delivery", recoveryGeneration: "full-originals-v2" };
    expect(await h.request({ sessionId: "s", continuation, providerCalls: 0, providerBinding: { fake: true }, prepare: true }))
      .toEqual({ status_code: 200, body: { success: true, completed: true } });
    expect(h.trigger).toHaveBeenCalledWith({ function_id: "mem::summarize", timeoutMs: 360_000, payload: {
      sessionId: "s", manual: true, continuation,
    } });
  });
  it("retains bearer authentication middleware for summarize", async () => {
    const h = await adapter();
    expect(h.registerTrigger).toHaveBeenCalledWith({ type: "http", function_id: "api::summarize",
      config: { api_path: "/agentmemory/summarize", http_method: "POST", middleware_function_ids: ["middleware::api-auth"] } });
    const authenticate = h.handlers.get("middleware::api-auth")!;
    expect(await authenticate({ request: { headers: {} } })).toMatchObject({ action: "respond", response: { status_code: 401 } });
    expect(await authenticate({ request: { headers: { authorization: "Bearer test-secret" } } })).toEqual({ action: "continue" });
  });
});
