import { beforeEach, describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import { StatePageError } from "../src/state/state-pages.js";
import { registerGraphDispatch, type GraphDispatchIntent } from "../src/functions/graph-dispatch.js";
import { graphRecordDigest } from "../src/functions/graph-job-storage.js";

const control = vi.hoisted(() => ({ paused: false, blocker: null as null | { id: string }, local: false }));
vi.mock("../src/config.js", async (original) => ({ ...await original<typeof import("../src/config.js")>(), isBackgroundRecoveryPaused: () => control.paused }));
vi.mock("../src/functions/graph-jobs.js", () => ({
  graphDispatchBlocker: vi.fn(async () => control.blocker),
  graphResult: (value: unknown) => value,
  withLocalGraphRecovery: async (local: boolean, run: () => Promise<unknown>) => { control.local = local; return run(); },
  withSelectedGraphOrigin: async (_selected: unknown, run: () => Promise<unknown>) => run(),
}));

describe("durable graph dispatch", () => {
  let rows: Map<string, any>;
  let handlers: Record<string, (data?: any) => Promise<any>>;
  let published: any[];
  let kv: StateKV;
  let execute: ReturnType<typeof vi.fn>;
  let intake: ReturnType<typeof registerGraphDispatch>;
  let trigger: ReturnType<typeof vi.fn>;
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const intent = () => [...rows.values()].find((value) => value.version === 1 && value.captureParts) as GraphDispatchIntent;
  beforeEach(() => {
    rows = new Map(); handlers = {}; published = [];
    control.paused = false; control.blocker = null; control.local = false;
    kv = {
      get: vi.fn(async (scope, id) => structuredClone(rows.get(key(scope, id)) ?? null)),
      set: vi.fn(async (scope, id, value) => { rows.set(key(scope, id), structuredClone(value)); return value; }),
      pages: vi.fn(async function* () { yield { items: [], next_cursor: null }; }),
    } as unknown as StateKV;
    trigger = vi.fn(async ({ function_id, payload }) => {
      if (function_id === "iii::durable::publish") { published.push(payload.data); return {}; }
      if (function_id === "mem::graph-recover") return {};
      throw new Error(`Unexpected trigger ${function_id}`);
    });
    const sdk = {
      registerFunction: (id: string, handler: typeof handlers[string]) => { handlers[id] = handler; },
      registerTrigger: vi.fn(),
      trigger,
    } as unknown as IIIClient;
    execute = vi.fn(async () => ({ success: true, extracted: 2 }));
    intake = registerGraphDispatch(sdk, kv, execute);
  });

  async function blockedIntake() {
    control.blocker = { id: "protected-existing-job" };
    rows.set(key(KV.graphJobs, "protected-existing-job"), { state: "staging", checkpoint: { applied: 27 } });
    return intake({ observations: [], batchResponse: "captured result" });
  }

  it("keeps immutable intake and existing checkpoint when blocked", async () => {
    expect(await blockedIntake()).toMatchObject({ queued: true, blockingJobId: "protected-existing-job" });
    const saved = structuredClone(intent());
    const capture = rows.get(key(KV.graphDispatchInputs(saved.id), "part:0"));
    await intake({ batchResponse: "captured result", observations: [] });
    expect(intent().id).toBe(saved.id);
    expect(rows.get(key(KV.graphDispatchInputs(saved.id), "part:0"))).toBe(capture);
    expect(rows.get(key(KV.graphJobs, "protected-existing-job"))).toEqual({ state: "staging", checkpoint: { applied: 27 } });
    expect(execute).not.toHaveBeenCalled();
  });

  it("honors pause without losing intake or running recovery", async () => {
    control.paused = true;
    expect(await intake({ observations: [] })).toMatchObject({ queued: true });
    expect(intent().state).toBe("pending");
    expect(await handlers["mem::graph-dispatch"]()).toMatchObject({ paused: true, queued: 0 });
    expect(execute).not.toHaveBeenCalled();
    expect(kv.pages).not.toHaveBeenCalled();
  });

  it("rejects stale delivery and replays captured completion without execution", async () => {
    await blockedIntake();
    expect(await handlers["mem::graph-dispatch-deliver"]({ intentId: intent().id, deliveryId: "foreign", localOnly: true })).toMatchObject({ stale: true });
    rows.set(key(KV.graphJobs, intent().jobId), { state: "completed", captureDigest: intent().digest, result: { success: true, extracted: 9 } });
    expect(await intake({ observations: [], batchResponse: "captured result" })).toMatchObject({ extracted: 9 });
    expect(intent().state).toBe("completed");
    expect(execute).not.toHaveBeenCalled();
  });

  it("does not accept a completed job with conflicting capture identity", async () => {
    await blockedIntake();
    rows.set(key(KV.graphJobs, intent().jobId), { state: "completed", captureDigest: "wrong" });
    await expect(intake({ observations: [], batchResponse: "captured result" })).rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(intent().state).toBe("pending");
  });

  it("resumes saved scan cursor and submits bounded local recovery delivery", async () => {
    await blockedIntake(); control.blocker = null;
    rows.set(key(KV.graphDispatchIntents, intent().id), { ...intent(), retryAfter: undefined });
    rows.set(key(KV.config, "graph-dispatch:scan"), { cursor: "saved" });
    vi.mocked(kv.pages).mockImplementation(async function* () { yield { items: [intent()], next_cursor: "next" } as any; });
    expect(await handlers["mem::graph-dispatch"]()).toMatchObject({ queued: 1, scanned: 1 });
    expect(kv.pages).toHaveBeenCalledWith(KV.graphDispatchIntents, { cursor: "saved", limit: 4, maxBytes: 131072 });
    expect(rows.get(key(KV.config, "graph-dispatch:scan"))).toEqual({ cursor: "next" });
    expect(await handlers["mem::graph-dispatch-deliver"](published[0])).toMatchObject({ extracted: 2 });
    expect(control.local).toBe(true);
    expect(intent().state).toBe("completed");
    await handlers["mem::graph-dispatch-deliver"](published[0]);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("resets stale cursor and retains pending inputs", async () => {
    await blockedIntake(); control.blocker = null;
    vi.mocked(kv.pages).mockImplementation(async function* () { throw new StatePageError("STATE_PAGE_CURSOR_STALE"); });
    expect(await handlers["mem::graph-dispatch"]()).toMatchObject({ deferred: true, failureCode: "STATE_PAGE_CURSOR_STALE" });
    expect(rows.get(key(KV.config, "graph-dispatch:scan"))).toEqual({ cursor: null });
    expect(intent().state).toBe("pending");
  });

  it("allows scheduled provider continuation only when explicitly requested", async () => {
    await handlers["mem::graph-dispatch"]({ localOnly: false });
    expect(trigger).toHaveBeenCalledWith({ function_id: "mem::graph-recover", payload: { localOnly: false } });
    trigger.mockClear();
    await handlers["mem::graph-dispatch"]();
    expect(trigger).toHaveBeenCalledWith({ function_id: "mem::graph-recover", payload: { localOnly: true } });
  });

  it("keeps selected dispatch inside the local-only provider boundary", async () => {
    rows.set(key(KV.sessions, "s"), { project: "p" });
    const sourceFingerprint = graphRecordDigest(JSON.stringify({ graphProvenanceVersion: 2, graphWorkGroupsVersion: 2, observations: [], sessionId: "s" }));
    await intake({ observations: [], sessionId: "s", graphWorkGroupsVersion: 2, selected: { project: "p", sessionId: "s", sourceFingerprint } });
    expect(execute).toHaveBeenCalledTimes(1);
    expect(control.local).toBe(true);
  });

  it("disposes a stopped intent once while preserving its capture and job checkpoint", async () => {
    await blockedIntake();
    const saved = structuredClone(intent());
    const stopped = { state: "failed", recoveryStopped: true, failureCode: "GRAPH_EXECUTION_FAILED", captureDigest: saved.digest, checkpoint: { ordinal: 31 } };
    rows.set(key(KV.graphJobs, saved.jobId), stopped);
    control.blocker = null;
    vi.mocked(kv.pages).mockImplementation(async function* () { yield { items: [intent()], next_cursor: null } as any; });
    expect(await handlers["mem::graph-dispatch"]({ localOnly: false })).toMatchObject({ queued: 0 });
    expect(intent().state).toBe("failed");
    expect(rows.get(key(KV.graphJobs, saved.jobId))).toEqual(stopped);
    expect(rows.get(key(KV.graphDispatchInputs(saved.id), "part:0"))).toContain("captured result");
    await handlers["mem::graph-dispatch"]({ localOnly: false });
    expect(execute).not.toHaveBeenCalled();
    expect(published).toHaveLength(0);
  });

  it("retains capture and retry state when local recovery needs a provider", async () => {
    execute.mockRejectedValue(Object.assign(new Error("provider needed"), { name: "GraphProviderRecoveryRequired" }));
    expect(await intake({ observations: [], batchResponse: "frozen" })).toMatchObject({ queued: true, providerRequired: true });
    expect(intent()).toMatchObject({ state: "pending", attempts: 1, providerRequired: true });
    expect(rows.get(key(KV.graphDispatchInputs(intent().id), "part:0"))).toContain("frozen");
  });

  it("keeps grouped parent work pending across one-unit deliveries", async () => {
    execute.mockImplementation(async (_request, jobId) => {
      const progress = { graphContinuation: true, workUnits: execute.mock.calls.length, nodesAdded: 1, edgesAdded: 0 };
      if (execute.mock.calls.length < 3) return progress;
      rows.set(key(KV.graphJobs, jobId), {
        state: "completed", captureDigest: intent().digest,
        result: { success: true, nodesAdded: 3, edgesAdded: 0, workUnits: 3 },
      });
      return { success: true, nodesAdded: 3, edgesAdded: 0, workUnits: 3 };
    });

    expect(await intake({ observations: [], graphWorkGroupsVersion: 2 })).toMatchObject({ queued: true });
    expect(intent()).toMatchObject({ state: "pending", attempts: 0, groupedWorkProgress: { workUnits: 1 } });
    expect(published).toHaveLength(1);
    expect(execute).toHaveBeenCalledTimes(1);

    expect(await handlers["mem::graph-dispatch-deliver"](published.shift())).toMatchObject({ queued: true });
    expect(intent()).toMatchObject({ state: "pending", attempts: 0, groupedWorkProgress: { workUnits: 2 } });
    expect(published).toHaveLength(1);
    expect(execute).toHaveBeenCalledTimes(2);

    expect(await handlers["mem::graph-dispatch-deliver"](published.shift())).toMatchObject({
      success: true, nodesAdded: 3, workUnits: 3,
    });
    expect(intent().state).toBe("completed");
    expect(execute).toHaveBeenCalledTimes(3);
  });
});
