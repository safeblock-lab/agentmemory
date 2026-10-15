import { describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import { graphRecordDigest } from "../src/functions/graph-job-storage.js";
import { registerGraphJobHandler, registerGraphJobRecovery, runGraphJob, withGraphDelta, withSelectedGraphOrigin, yieldGraphJob } from "../src/functions/graph-jobs.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";
import { prepareStateCommitBatch, type StateGraphGuard } from "../src/state/state-transactions.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
const origin = { project: "p", sessionId: "s", sourceFingerprint: "source", jobId: "intent-job" };
const input = { source: "bounded mock source" };
async function fixture() {
  const h = graphStateHarness();
  const kv = h.kv as unknown as StateKV;
  const revision = await kv.scopeRevision(KV.graphJobs);
  await kv.set(KV.config, "graph-dispatch:active", { jobId: null, generation: "1", fence: "0", baselineComplete: true,
    revision: revision.revision, revisionGeneration: revision.generation });
  registerGraphJobRecovery(h.sdk as unknown as IIIClient, kv);
  const recover = (payload = {}) => h.sdk.trigger({ function_id: "mem::graph-recover", payload });
  return { h, kv, recover };
}
describe("native selected graph origin isolation", () => {
  it("persists origin outside the canonical captured request and leaves default jobs unmarked", async () => {
    const f = await fixture();
    await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "selected"));
    expect(await f.kv.get(KV.graphJobs, "selected")).toMatchObject({ selectedOrigin: origin, captureDigest: graphRecordDigest(JSON.stringify(input)) });
    expect(await f.kv.get(KV.graphInputs("selected"), "capture:0")).toBe(JSON.stringify(input));
  });
  it("blocks ordinary pointer recovery without pause configuration and preserves job/checkpoint/handler", async () => {
    const f = await fixture();
    await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "selected"));
    const handler = vi.fn(); registerGraphJobHandler(f.kv, "extraction", handler);
    const before = await f.kv.get(KV.graphJobs, "selected");
    const checkpoint = await f.kv.get(KV.graphCheckpoints, "selected");
    const pointer = await f.kv.get(KV.config, "graph-dispatch:active");
    expect(await f.recover()).toMatchObject({ recovered: false, reason: "selected_foreground_required" });
    expect(handler).not.toHaveBeenCalled();
    expect(await f.kv.get(KV.graphJobs, "selected")).toEqual(before);
    expect(await f.kv.get(KV.graphCheckpoints, "selected")).toEqual(checkpoint);
    expect(await f.kv.get(KV.config, "graph-dispatch:active")).toEqual(pointer);
  });
  it("blocks native applying recovery before checkpoint reconstruction or handler execution", async () => {
    const f = await fixture();
    const jobId = "selected-applying";
    f.h.seed(KV.graphJobs, jobId, { version: 1, id: jobId, generation: "1", kind: "extraction", state: "applying", selectedOrigin: origin,
      createdAt: "2026-10-10T00:00:00Z", captureComplete: true, captureParts: 1, captureDigest: graphRecordDigest(JSON.stringify(input)) });
    const guard = await f.kv.lease({ action: "acquire", owner_id: "fixture", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    const capture = await f.kv.commitBatch(guard, prepareStateCommitBatch({ identity: { generation: "1", job_id: jobId, logical_delta_id: "capture", chunk_ordinal: 0 },
      expected_checkpoint_version: "0", checkpoint: { generation: "1", job_id: jobId, logical_delta_id: "capture", delta_ordinal: 0, next_chunk_ordinal: 1, visibility: "complete" },
      operations: [{ type: "check", scope: KV.graphSnapshot, key: "current", expected_version: "0" }] }));
    await f.kv.commitBatch(guard, prepareStateCommitBatch({ identity: { generation: "1", job_id: jobId, logical_delta_id: "delta:1", chunk_ordinal: 0 },
      expected_checkpoint_version: capture.checkpoint_version, checkpoint: { generation: "1", job_id: jobId, logical_delta_id: "delta:1", delta_ordinal: 1, next_chunk_ordinal: 1, visibility: "applying" },
      operations: [{ type: "set", scope: KV.graphNodes, key: "partial", expected_version: "0", value: { id: "partial" } }] }));
    await f.kv.lease({ action: "release", owner_id: guard.owner_id, generation: guard.generation, fence: guard.fence });
    const read = f.kv.getVersioned.bind(f.kv);
    f.kv.getVersioned = async function<T>(scope: string, key: string, guard?: StateGraphGuard) {
      if (scope === KV.graphControl && !guard) return structuredClone(f.h.rows.get(JSON.stringify([scope, key]))) as { value: T; version: string };
      return read<T>(scope, key, guard);
    };
    const before = [...f.h.rows].filter(([key]) => !key.includes(KV.graphControl));
    const receipts = f.h.receipts.size;
    const handler = vi.fn(); registerGraphJobHandler(f.kv, "extraction", handler);
    expect(await f.recover()).toMatchObject({ recovered: false, reason: "selected_foreground_required" });
    expect(handler).not.toHaveBeenCalled(); expect(f.h.receipts.size).toBe(receipts);
    expect([...f.h.rows].filter(([key]) => !key.includes(KV.graphControl))).toEqual(before);
  });
  it("rejects ordinary direct job adoption and mismatched selected identity", async () => {
    const f = await fixture();
    await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "selected"));
    const body = vi.fn();
    await expect(runGraphJob(f.kv, "extraction", input, body, "selected")).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });
    await expect(withSelectedGraphOrigin({ ...origin, sessionId: "other" }, () => runGraphJob(f.kv, "extraction", input, body, "selected")))
      .rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(body).not.toHaveBeenCalled();
  });
  it("resumes and replays exact selected work without changing source identity", async () => {
    const f = await fixture();
    await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "selected"));
    const body = vi.fn(async () => { await withGraphDelta(f.kv, async () => "captured-prefix"); return { useful: "completed" }; });
    expect(await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, body, "selected"))).toEqual({ useful: "completed" });
    expect(await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, body, "selected"))).toEqual({ useful: "completed" });
    expect(body).toHaveBeenCalledOnce();
  });
  it("never converts an automatic pending job to selected work", async () => {
    const f = await fixture();
    await runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "automatic");
    const before = await f.kv.get(KV.graphJobs, "automatic");
    await expect(withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, async () => "bad", "automatic")))
      .rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(await f.kv.get(KV.graphJobs, "automatic")).toEqual(before);
  });
  it("blocks scan adoption of a selected job before writing an active pointer", async () => {
    const f = await fixture();
    await withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "selected"));
    await f.kv.delete(KV.config, "graph-dispatch:active");
    expect(await f.recover()).toMatchObject({ recovered: false, reason: "selected_foreground_required" });
    expect(await f.kv.get(KV.config, "graph-dispatch:active")).toBeNull();
  });
  it("retains automatic recovery and does not inherit origin outside its invocation", async () => {
    const f = await fixture();
    await withSelectedGraphOrigin(origin, async () => undefined);
    await runGraphJob(f.kv, "extraction", input, async () => yieldGraphJob("next"), "automatic");
    const handler = vi.fn(async (captured, id) => runGraphJob(f.kv, "extraction", captured, async () => "normal", id));
    registerGraphJobHandler(f.kv, "extraction", handler);
    expect(await f.recover()).toMatchObject({ recovered: true, jobId: "automatic" });
    expect(handler).toHaveBeenCalledOnce();
    expect(await f.kv.get(KV.graphJobs, "automatic")).not.toHaveProperty("selectedOrigin");
  });
  it("does not revive protected stopped historical jobs or permit selected conversion", async () => {
    const f = await fixture();
    const job = { version: 1, id: "graphjob_mv0567t0_7c392765f08d", kind: "extraction", generation: "1", state: "failed", recoveryStopped: true,
      captureComplete: true, captureParts: 1, captureDigest: graphRecordDigest(JSON.stringify(input)) };
    f.h.seed(KV.graphJobs, job.id, job);
    const body = vi.fn();
    await expect(withSelectedGraphOrigin(origin, () => runGraphJob(f.kv, "extraction", input, body, job.id))).rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(await f.kv.get(KV.graphJobs, job.id)).toEqual(job); expect(body).not.toHaveBeenCalled();
  });
});
