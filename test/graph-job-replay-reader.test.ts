import { describe, expect, it } from "vitest";
import { freezeGraphValue, graphKV, readCompletedGraphJobReplay, runGraphJob, withGraphDelta, yieldGraphJob } from "../src/functions/graph-jobs.js";
import { KV } from "../src/state/schema.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

async function completedTemporalFixture() {
  const harness = graphStateHarness();
  const graph = graphKV(harness.kv as never);
  const jobId = "graphjob_replay_reader";
  await runGraphJob(graph as never, "temporal", { source: "captured" }, async () => {
    await freezeGraphValue(graph as never, "raw", () => "<temporal_graph/>");
    await freezeGraphValue(graph as never, "parsed", () => ({ nodes: [], edges: [] }));
    await withGraphDelta(graph as never, async () => ({ success: true, nodesAdded: 0, edgesAdded: 0 }));
    return { success: true };
  }, jobId);
  return { harness, graph, jobId };
}

describe("completed graph job replay reader", () => {
  it("returns only requested digest-verified values and completed delta results", async () => {
    const { harness, graph, jobId } = await completedTemporalFixture();
    const callbackKey = "a".repeat(64);
    await harness.kv.set(KV.batchCallbacks, `graph:${callbackKey}`, { state: "completed" });
    const replay = await readCompletedGraphJobReplay(graph as never, {
      jobId, values: [{ ordinal: 1, name: "raw" }, { ordinal: 2, name: "parsed" }],
      deltas: [3], callbackKeys: [callbackKey],
    });
    expect(replay.input).toEqual({ source: "captured" });
    expect(replay.values.map(({ value }) => value)).toEqual(["<temporal_graph/>", { nodes: [], edges: [] }]);
    expect(replay.deltas).toMatchObject([{ ordinal: 3, result: { success: true, nodesAdded: 0, edgesAdded: 0 } }]);
    expect(replay.callbacks).toEqual([{ key: callbackKey, receipt: { state: "completed" } }]);
    expect(replay.job).toMatchObject({ id: jobId, kind: "temporal" });
  });

  it("refuses incomplete parents and corrupt frozen values", async () => {
    const { harness, graph, jobId } = await completedTemporalFixture();
    const job = await harness.kv.get<Record<string, unknown>>(KV.graphJobs, jobId);
    harness.seed(KV.graphJobs, jobId, { ...job, state: "staging" } as never);
    await expect(readCompletedGraphJobReplay(graph as never, {
      jobId, values: [{ ordinal: 1, name: "raw" }], deltas: [],
    })).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });

    harness.seed(KV.graphJobs, jobId, job as never);
    harness.seed(KV.graphProviderResults(jobId), "value:1:raw:part:0", "tampered");
    await expect(readCompletedGraphJobReplay(graph as never, {
      jobId, values: [{ ordinal: 1, name: "raw" }], deltas: [],
    })).rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
  });

  it("rejects uncaptured refs and a non-completed continuation", async () => {
    const { graph, jobId } = await completedTemporalFixture();
    await expect(readCompletedGraphJobReplay(graph as never, {
      jobId, values: [{ ordinal: 99, name: "missing" }], deltas: [],
    })).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });

    const harness = graphStateHarness();
    const pendingGraph = graphKV(harness.kv as never);
    const pendingId = "graphjob_replay_pending";
    await runGraphJob(pendingGraph as never, "temporal", { source: "captured" }, async () => yieldGraphJob({ progress: true }), pendingId);
    await expect(readCompletedGraphJobReplay(pendingGraph as never, {
      jobId: pendingId, values: [], deltas: [],
    })).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });
  });
});
