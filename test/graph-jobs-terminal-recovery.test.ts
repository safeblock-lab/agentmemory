import { describe, expect, it, vi } from "vitest";
import { KV } from "../src/state/schema.js";
import {
  graphKV,
  registerGraphJobHandler,
  registerGraphJobRecovery,
  runGraphJob,
  withGraphDelta,
} from "../src/functions/graph-jobs.js";
import { StateTransactionError } from "../src/state/state-transactions.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

describe("terminal graph staging failures", () => {
  it.each([
    ["state record limit", new StateTransactionError("STATE_RECORD_TOO_LARGE"), "STATE_RECORD_TOO_LARGE"],
    ["HTTP 413", Object.assign(new Error("payload too large"), { status: 413 }), "HTTP_413"],
  ])("stops automatic recovery after a %s before graph rows are applied", async (_label, failure, failureCode) => {
    const harness = graphStateHarness();
    const graph = graphKV(harness.kv as never);
    const jobId = "oversized-extraction-job";
    const publishedSnapshot = { version: 1, marker: "published-before-failure" };
    await harness.kv.set(KV.graphSnapshot, "current", publishedSnapshot);

    await expect(runGraphJob(graph as never, "extraction", { source: "frozen" }, async () => {
      await withGraphDelta(graph as never, async () => {
        await graph.set(KV.graphNodes, "uncommitted-node", { id: "uncommitted-node" });
        throw failure;
      });
    }, jobId)).rejects.toBe(failure);

    const job = (await harness.kv.list(KV.graphJobs) as Array<Record<string, unknown>>)
      .find((entry) => entry.id === jobId);
    expect(job).toMatchObject({ state: "failed", failureCode, recoveryStopped: true });
    expect(await harness.kv.get(KV.graphNodes, "uncommitted-node")).toBeNull();
    expect(await harness.kv.get(KV.graphSnapshot, "current")).toEqual(publishedSnapshot);
    expect(await harness.kv.get(KV.graphCheckpoints, jobId)).toMatchObject({ visibility: "complete" });
    expect(await harness.kv.get(KV.graphControl, "current")).toMatchObject({ generation: "1", recovery: null });
    expect(harness.receipts.size).toBeGreaterThan(0);

    const recoverHandler = vi.fn(async () => undefined);
    registerGraphJobHandler(harness.kv as never, "extraction", recoverHandler);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);
    await expect(harness.sdk.trigger({ function_id: "mem::graph-recover", payload: {} }))
      .resolves.toMatchObject({ success: true, recovered: false });
    expect(recoverHandler).not.toHaveBeenCalled();

    await expect(runGraphJob(graph as never, "extraction", { source: "frozen" }, async () => undefined, jobId))
      .rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });
  });

  it("persists bounded backoff without counting deferred retries as failures", async () => {
    const harness = graphStateHarness();
    const graph = graphKV(harness.kv as never);
    const jobId = "temporarily-failing-extraction-job";
    const input = { source: "frozen" };
    const transientFailure = new Error("temporary extraction timeout");
    const failExtraction = async () => withGraphDelta(graph as never, async () => {
      throw transientFailure;
    });

    await expect(runGraphJob(graph as never, "extraction", input, failExtraction, jobId))
      .rejects.toBe(transientFailure);
    const firstJob = (await harness.kv.list(KV.graphJobs) as Array<Record<string, unknown>>)
      .find((entry) => entry.id === jobId)!;
    expect(firstJob).toMatchObject({ state: "failed", recoveryDeltaId: "delta:1", recoveryAttempts: 1, recoveryStopped: false });
    expect(Date.parse(String(firstJob.recoveryAfter))).toBeGreaterThan(Date.now());

    const recoverHandler = vi.fn(async () => undefined);
    registerGraphJobHandler(harness.kv as never, "extraction", recoverHandler);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);
    await expect(harness.sdk.trigger({ function_id: "mem::graph-recover", payload: {} }))
      .resolves.toMatchObject({ success: true, recovered: false, retryAfter: firstJob.recoveryAfter });
    await expect(runGraphJob(graph as never, "extraction", input, failExtraction, jobId))
      .rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });

    const deferredJob = (await harness.kv.list(KV.graphJobs) as Array<Record<string, unknown>>)
      .find((entry) => entry.id === jobId);
    expect(deferredJob).toMatchObject({ recoveryAttempts: 1, recoveryAfter: firstJob.recoveryAfter });
    expect(recoverHandler).not.toHaveBeenCalled();
  });

  it("terminalizes an extraction whose durable staging checkpoint exceeds the limit", async () => {
    const harness = graphStateHarness();
    const nativeRead = harness.kv.getVersioned.bind(harness.kv);
    harness.kv.getVersioned = async function <T>(scope: string, key: string, guard?: never) {
      if (scope === KV.graphControl && key === "current" && !guard) {
        const row = harness.rows.get(JSON.stringify([scope, key]));
        if (row) return structuredClone(row) as never;
      }
      return nativeRead<T>(scope, key, guard);
    };
    const jobId = "runaway-extraction-job";
    const checkpoint = {
      generation: "1",
      job_id: jobId,
      logical_delta_id: "delta:4",
      delta_ordinal: 4,
      next_chunk_ordinal: 6_465_363,
      visibility: "staging" as const,
    };
    const now = new Date().toISOString();
    const job = {
      version: 1,
      id: jobId,
      generation: "1",
      kind: "extraction",
      state: "failed",
      createdAt: now,
      updatedAt: now,
      inputCount: 1,
      captureParts: 1,
      captureDigest: "0".repeat(64),
      captureComplete: true,
    };
    harness.seed(KV.graphJobs, jobId, job as never);
    harness.seed(KV.graphCheckpoints, jobId, checkpoint as never);
    harness.seed(KV.graphControl, "current", {
      version: 1,
      generation: "1",
      fence: "0",
      lease: null,
      recovery: checkpoint,
    } as never);

    const handler = vi.fn(async () => ({ recovered: true }));
    registerGraphJobHandler(harness.kv as never, "extraction", handler);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);
    await expect(harness.sdk.trigger({ function_id: "mem::graph-recover", payload: {} }))
      .resolves.toMatchObject({ success: true, recovered: false, jobId, stopped: true });

    const stoppedJob = (await harness.kv.list(KV.graphJobs) as Array<Record<string, unknown>>)
      .find((entry) => entry.id === jobId);
    expect(stoppedJob).toMatchObject({ state: "failed", failureCode: "STATE_TX_LIMIT_EXCEEDED", recoveryStopped: true });
    expect(handler).not.toHaveBeenCalled();
    expect(await harness.kv.get(KV.graphCheckpoints, jobId)).toMatchObject({ visibility: "complete" });
    expect(await harness.kv.get(KV.graphControl, "current")).toMatchObject({ generation: "1", recovery: null });
  });
});
