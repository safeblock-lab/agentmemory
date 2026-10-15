import { describe, expect, it, vi } from "vitest";
import { graphStateHarness } from "./helpers/graph-state-harness.js";
import { KV } from "../src/state/schema.js";
import { StateTransactionError } from "../src/state/state-transactions.js";
import { freezeGraphHandledValue, graphKV, registerGraphJobHandler, registerGraphJobRecovery, runGraphJob, withLocalGraphRecovery } from "../src/functions/graph-jobs.js";
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));

async function setup() {
  const h = graphStateHarness();
  const revision = await h.kv.scopeRevision(KV.graphJobs);
  await h.kv.set(KV.config, "graph-dispatch:active", { jobId: null, generation: "1", fence: "0", baselineComplete: true,
    revision: revision.revision, revisionGeneration: revision.generation });
  const graph = graphKV(h.kv as never);
  const provider = vi.fn(async () => "captured response");
  const run = (_input: unknown, id: string) => runGraphJob(graph, "extraction", {},
    () => freezeGraphHandledValue(graph, "provider-response", provider), id);
  registerGraphJobHandler(h.kv as never, "extraction", run);
  registerGraphJobRecovery(h.sdk as never, h.kv as never);
  return { h, graph, provider, run };
}

describe("durable graph provider intent", () => {
  it("records local no-dispatch then resumes once and replays with zero new calls", async () => {
    const { h, provider, run } = await setup();
    await expect(withLocalGraphRecovery(true, () => run({}, "graphjob_intent"))).rejects.toMatchObject({ name: "GraphProviderRecoveryRequired" });
    expect(provider).not.toHaveBeenCalled();
    const before = await h.kv.list<any>(KV.graphProviderResults("graphjob_intent"));
    expect(before).toContainEqual(expect.objectContaining({ version: 1, state: "not-dispatched" }));
    expect(await h.sdk.trigger({ function_id: "mem::graph-recover", payload: { localOnly: false } })).toMatchObject({ recovered: true });
    expect(provider).toHaveBeenCalledTimes(1);
    expect(await run({}, "graphjob_intent")).toBe("captured response");
    expect(provider).toHaveBeenCalledTimes(1);
    const after = await h.kv.list<any>(KV.graphProviderResults("graphjob_intent"));
    expect(after.find(row => row.state === "intended")?.id).toBe(before.find(row => row.state === "not-dispatched")?.id);
  });

  it("never repeats a provider after its response capture fails", async () => {
    const { h, provider, run } = await setup();
    const commit = h.kv.commitBatch.bind(h.kv);
    let failCapture = true;
    h.kv.commitBatch = async (guard, prepared) => {
      const operations = JSON.parse(prepared.payload_json).operations;
      if (failCapture && operations.some((op: any) => op.key?.includes("provider-response:part:"))) throw new StateTransactionError("STATE_TX_FAILED");
      return commit(guard, prepared);
    };
    await expect(run({}, "graphjob_unknown")).rejects.toBeDefined();
    expect(provider).toHaveBeenCalledTimes(1);
    failCapture = false;
    vi.useFakeTimers();
    vi.setSystemTime(Date.now() + 31_000);
    try {
    expect(await h.sdk.trigger({ function_id: "mem::graph-recover", payload: { localOnly: false } })).toMatchObject({ providerOutcomeUnknown: true });
    expect(provider).toHaveBeenCalledTimes(1);
    expect(await h.kv.list<any>(KV.graphProviderResults("graphjob_unknown"))).toContainEqual(expect.objectContaining({ state: "intended" }));
    } finally { vi.useRealTimers(); }
  });

  it("does not dispatch when intent persistence fails", async () => {
    const { h, provider, run } = await setup();
    const commit = h.kv.commitBatch.bind(h.kv);
    h.kv.commitBatch = async (guard, prepared) => {
      if (JSON.parse(prepared.payload_json).operations.some((op: any) => op.key?.startsWith("provider-intent:"))) throw new StateTransactionError("STATE_TX_FAILED");
      return commit(guard, prepared);
    };
    await expect(run({}, "graphjob_intentwrite")).rejects.toBeDefined();
    expect(provider).not.toHaveBeenCalled();
  });

  it("requires an exact operator no-dispatch disposition for a legacy slot", async () => {
    const { h, provider, run } = await setup();
    const jobId = "graphjob_legacy";
    await expect(withLocalGraphRecovery(true, () => run({}, jobId))).rejects.toMatchObject({ name: "GraphProviderRecoveryRequired" });
    const checkpoint = (await h.kv.getVersioned<any>(KV.graphCheckpoints, jobId)).value!;
    h.seed(KV.graphProviderResults(jobId), `provider-intent:${checkpoint.delta_ordinal}:provider-response`, null, false, "2");
    expect(await h.sdk.trigger({ function_id: "mem::graph-recover", payload: { localOnly: false } })).toMatchObject({ providerOutcomeUnknown: true });
    expect(provider).not.toHaveBeenCalled();
    const current = await h.kv.getVersioned<any>(KV.graphCheckpoints, jobId);
    const proof = { jobId, generation: current.value!.generation, checkpointVersion: current.version, chunkOrdinal: current.value!.next_chunk_ordinal };
    await expect(h.sdk.trigger({ function_id: "mem::graph-recover", payload: { localOnly: false,
      providerContinuation: { ...proof, chunkOrdinal: proof.chunkOrdinal + 1 } } })).rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(provider).not.toHaveBeenCalled();
    expect(await h.sdk.trigger({ function_id: "mem::graph-recover", payload: { localOnly: false, providerContinuation: proof } })).toMatchObject({ recovered: true });
    expect(provider).toHaveBeenCalledTimes(1);
    expect(await run({}, jobId)).toBe("captured response");
    expect(provider).toHaveBeenCalledTimes(1);
  });
});
