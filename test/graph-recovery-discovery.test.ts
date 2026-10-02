import { describe, expect, it, vi } from "vitest";
import { registerGraphJobHandler, registerGraphJobRecovery, runGraphJob, withCompletedGraphRead } from "../src/functions/graph-jobs.js";
import { graphJson, graphRecordDigest } from "../src/functions/graph-job-storage.js";
import { KV } from "../src/state/schema.js";
import { prepareStateCommitBatch, StateTransactionError, type StateGraphGuard } from "../src/state/state-transactions.js";
import type { GraphControlState } from "../src/types.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

const release = (guard: StateGraphGuard) => ({ action: "release" as const, owner_id: guard.owner_id, generation: guard.generation, fence: guard.fence });

async function fixture(applying = true) {
  const h = graphStateHarness();
  const nativeRead = h.kv.getVersioned.bind(h.kv);
  // Native read_allowed permits discovery of control/current during recovery.
  h.kv.getVersioned = async function <T>(scope: string, key: string, guard?: StateGraphGuard) {
    if (scope === KV.graphControl && key === "current" && !guard) {
      const row = h.rows.get(JSON.stringify([scope, key]));
      if (row) return structuredClone(row) as { value: T; version: string };
    }
    return nativeRead<T>(scope, key, guard);
  };
  const input = { observations: [{ title: "Unicode 🧠", content: "frozen" }] };
  const text = JSON.stringify(input);
  const job = { version: 1, id: "recovery-job", generation: "1", kind: "extraction", state: "staging", createdAt: "2026-01-01T00:00:00.000Z", updatedAt: "2026-01-01T00:00:00.000Z", inputCount: 1, captureParts: 1, captureDigest: graphRecordDigest(text), captureComplete: true };
  h.seed(KV.graphJobs, job.id, graphJson(job));
  h.seed(KV.graphInputs(job.id), "capture:0", text);
  if (applying) {
    const guard = await h.kv.lease({ action: "acquire", owner_id: "old-worker", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    const capture = await h.kv.commitBatch(guard, prepareStateCommitBatch({
      identity: { generation: "1", job_id: job.id, logical_delta_id: "capture", chunk_ordinal: 0 },
      expected_checkpoint_version: "0",
      checkpoint: { generation: "1", job_id: job.id, logical_delta_id: "capture", delta_ordinal: 0, next_chunk_ordinal: 1, visibility: "complete" },
      operations: [{ type: "check", scope: KV.graphSnapshot, key: "current", expected_version: "0" }],
    }));
    await h.kv.commitBatch(guard, prepareStateCommitBatch({
      identity: { generation: "1", job_id: job.id, logical_delta_id: "delta:1", chunk_ordinal: 0 },
      expected_checkpoint_version: capture.checkpoint_version,
      checkpoint: { generation: "1", job_id: job.id, logical_delta_id: "delta:1", delta_ordinal: 1, next_chunk_ordinal: 1, visibility: "applying" },
      operations: [{ type: "set", scope: KV.graphNodes, key: "partial", expected_version: "0", value: { id: "partial" } }],
    }));
    await h.kv.lease(release(guard));
    const control = (await h.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value!;
    expect(control.recovery).toEqual({ generation: "1", job_id: job.id, logical_delta_id: "delta:1", delta_ordinal: 1, next_chunk_ordinal: 1, visibility: "applying" });
    const proofLease = await h.kv.lease({ action: "acquire", owner_id: "fixture-proof", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    expect((await h.kv.getVersioned(KV.graphCheckpoints, job.id, proofLease)).value).toEqual(control.recovery);
    await h.kv.lease(release(proofLease));
  }
  const originalValues = h.kv.values.bind(h.kv);
  const enumeration = vi.fn();
  h.kv.values = async function* <T>(scope: string) {
    enumeration(scope);
    const current = (await h.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
    if (current?.recovery && scope === KV.graphJobs) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    yield* originalValues<T>(scope);
  };
  const pointRead = h.kv.getVersioned.bind(h.kv);
  const reads = vi.spyOn(h.kv, "getVersioned");
  registerGraphJobRecovery(h.sdk as never, h.kv as never);
  const productionInvocations = vi.fn();
  const recover = () => {
    productionInvocations();
    return h.sdk.trigger({ function_id: "mem::graph-recover", payload: {} });
  };
  return { h, job, input, recover, enumeration, reads, pointRead, productionInvocations };
}

describe("guarded graph recovery discovery", () => {
  it("reconstructs partial recovery using point reads and releases before handler acquisition", async () => {
    const f = await fixture();
    const handler = vi.fn(async (input: unknown) => {
      expect(input).toEqual(f.input);
      const current = (await f.h.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value!;
      expect(current.lease).toBeNull();
      const guard = await f.h.kv.lease({ action: "acquire", owner_id: "handler", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
      await f.h.kv.lease(release(guard));
    });
    registerGraphJobHandler(f.h.kv as never, "extraction", handler);
    await expect(f.recover()).resolves.toEqual({ success: true, recovered: true, jobId: f.job.id });
    expect(f.productionInvocations).toHaveBeenCalledOnce();
    expect(handler).toHaveBeenCalledOnce();
    expect(f.enumeration).not.toHaveBeenCalled();
    const protectedReads = f.reads.mock.calls.filter(([scope]) => scope === KV.graphJobs || scope === KV.graphInputs(f.job.id));
    expect(protectedReads.length).toBeGreaterThan(1);
    expect(protectedReads.every(([, , guard]) => guard?.owner_id && guard.fence && guard.generation)).toBe(true);
    await expect(withCompletedGraphRead(f.h.kv as never, async () => "plain")).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });
  });

  it("allows the handler entrypoint to reread recovery under its own fence", async () => {
    const f = await fixture();
    const body = vi.fn(async (input: unknown) => { expect(input).toEqual(f.input); throw new Error("body reached after guarded recovery"); });
    registerGraphJobHandler(f.h.kv as never, "extraction", (input, id) => runGraphJob(f.h.kv as never, "extraction", input, body, id));
    await expect(f.recover()).rejects.toThrow("body reached after guarded recovery");
    expect(body).toHaveBeenCalledOnce();
    expect(f.enumeration).not.toHaveBeenCalled();
    expect((await f.h.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value?.lease).toBeNull();
  });

  it("rejects another live lease without invoking handler or enumerating", async () => {
    const f = await fixture();
    const handler = vi.fn();
    registerGraphJobHandler(f.h.kv as never, "extraction", handler);
    await f.h.kv.lease({ action: "acquire", owner_id: "competitor", generation: "1", ttl_ms: 10_000 });
    await expect(f.recover()).rejects.toMatchObject({ code: "STATE_TX_LEASE_BUSY" });
    expect(handler).not.toHaveBeenCalled();
    expect(f.enumeration).not.toHaveBeenCalled();
  });

  it.each(["fence", "generation", "expiry"])("rejects a %s change after discovery acquires its lease", async (race) => {
    const f = await fixture();
    const handler = vi.fn();
    registerGraphJobHandler(f.h.kv as never, "extraction", handler);
    const read = f.pointRead;
    const rows = [...f.h.rows].filter(([key]) => !key.includes(KV.graphControl));
    const receipts = f.h.receipts.size;
    let changed = false;
    f.reads.mockImplementation(async (scope, key, guard) => {
      if (scope === KV.graphControl && guard && !changed) {
        changed = true;
        const current = (await read<GraphControlState>(scope, key)).value!;
        if (race === "fence") {
          await f.h.kv.lease(release(guard));
          await f.h.kv.lease({ action: "acquire", owner_id: "competitor", generation: "1", ttl_ms: 10_000 });
        } else {
          f.h.seed(scope, key, graphJson({ ...current, ...(race === "generation" ? { generation: "2" } : { lease: { ...current.lease!, expires_at_ms: 0 } }) }));
        }
      }
      return read(scope, key, guard);
    });
    await expect(f.recover()).rejects.toMatchObject({ code: race === "generation" ? "STATE_TX_GENERATION_STALE" : "STATE_TX_FENCED" });
    expect(changed).toBe(true);
    expect(handler).not.toHaveBeenCalled();
    expect(f.enumeration).not.toHaveBeenCalled();
    expect(f.h.receipts.size).toBe(receipts);
    expect([...f.h.rows].filter(([key]) => !key.includes(KV.graphControl))).toEqual(rows);
  });

  it("takes over an expired lease and rejects the former fence", async () => {
    const f = await fixture();
    const old = await f.h.kv.lease({ action: "acquire", owner_id: "expired-worker", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    const now = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(now + 10_001);
    const handler = vi.fn();
    registerGraphJobHandler(f.h.kv as never, "extraction", handler);
    try {
      await expect(f.recover()).resolves.toMatchObject({ recovered: true });
      expect(handler).toHaveBeenCalledOnce();
      await expect(f.h.kv.getVersioned(KV.graphJobs, f.job.id, old)).rejects.toMatchObject({ code: "STATE_TX_FENCED" });
    } finally { clock.mockRestore(); }
  });

  it.each([
    ["missing metadata", "STATE_GRAPH_RECOVERY_REQUIRED"],
    ["corrupt metadata", "STATE_GRAPH_RECOVERY_REQUIRED"],
    ["wrong identity", "STATE_GRAPH_RECOVERY_REQUIRED"],
    ["stale generation", "STATE_TX_GENERATION_STALE"],
    ["missing fragment", "STATE_GRAPH_RECOVERY_REQUIRED"],
    ["corrupt fragment", "STATE_TX_INVALID_RESPONSE"],
    ["wrong digest", "STATE_TX_REPLAY_CONFLICT"],
    ["invalid part count", "STATE_TX_INVALID_RESPONSE"],
    ["invalid digest", "STATE_TX_INVALID_RESPONSE"],
    ["incomplete applying capture", "STATE_GRAPH_RECOVERY_REQUIRED"],
    ["mismatched checkpoint", "STATE_GRAPH_RECOVERY_REQUIRED"],
  ])("preserves recovery on %s", async (scenario, code) => {
    const f = await fixture();
    const handler = vi.fn();
    registerGraphJobHandler(f.h.kv as never, "extraction", handler);
    if (scenario === "missing metadata") f.h.seed(KV.graphJobs, f.job.id, null, false);
    if (scenario === "corrupt metadata") f.h.seed(KV.graphJobs, f.job.id, 42);
    if (scenario === "wrong identity") f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, id: "other" }));
    if (scenario === "stale generation") f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, generation: "0" }));
    if (scenario === "missing fragment") f.h.seed(KV.graphInputs(f.job.id), "capture:0", null, false);
    if (scenario === "corrupt fragment") f.h.seed(KV.graphInputs(f.job.id), "capture:0", 42);
    if (scenario === "wrong digest") f.h.seed(KV.graphInputs(f.job.id), "capture:0", "{}");
    if (scenario === "invalid part count") f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, captureParts: -1 }));
    if (scenario === "invalid digest") f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, captureDigest: "bad" }));
    if (scenario === "incomplete applying capture") f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, captureComplete: false }));
    if (scenario === "mismatched checkpoint") f.h.seed(KV.graphCheckpoints, f.job.id, graphJson({ job_id: f.job.id, generation: "1", logical_delta_id: "other" }));
    const before = (await f.h.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value?.recovery;
    const receipts = f.h.receipts.size;
    const rows = [...f.h.rows].filter(([key]) => !key.includes(KV.graphControl));
    await expect(f.recover()).rejects.toMatchObject({ code });
    expect(handler).not.toHaveBeenCalled();
    expect(f.enumeration).not.toHaveBeenCalled();
    expect(f.h.receipts.size).toBe(receipts);
    expect([...f.h.rows].filter(([key]) => !key.includes(KV.graphControl))).toEqual(rows);
    expect((await f.h.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value?.recovery).toEqual(before);
  });

  it("keeps queued discovery and absent-job behavior without recovery", async () => {
    const f = await fixture(false);
    const handler = vi.fn(async () => undefined);
    registerGraphJobHandler(f.h.kv as never, "extraction", handler);
    await expect(f.recover()).resolves.toMatchObject({ recovered: true });
    expect(f.enumeration).toHaveBeenCalledWith(KV.graphJobs);
    f.h.seed(KV.graphJobs, f.job.id, null, false);
    await expect(f.recover()).resolves.toEqual({ success: true, recovered: false });
  });

  it("invalidates an incomplete initial capture before effects", async () => {
    const f = await fixture(false);
    f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, captureComplete: false }));
    await expect(f.recover()).resolves.toMatchObject({ success: false, recovered: false, jobId: f.job.id });
    expect((await f.h.kv.getVersioned<{ state: string }>(KV.graphJobs, f.job.id)).value?.state).toBe("invalidated");
  });

  it("returns completed replay without running the body", async () => {
    const f = await fixture(false);
    f.h.seed(KV.graphJobs, f.job.id, graphJson({ ...f.job, state: "completed", result: { value: { exact: true } } }));
    const body = vi.fn();
    await expect(runGraphJob(f.h.kv as never, "extraction", f.input, body, f.job.id)).resolves.toEqual({ exact: true });
    expect(body).not.toHaveBeenCalled();
  });
});
