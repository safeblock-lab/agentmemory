import { describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import { graphRecordDigest } from "../src/functions/graph-job-storage.js";
import { graphDispatchBlocker, runGraphJob, withSelectedGraphOrigin } from "../src/functions/graph-jobs.js";
import { registerGraphDispatch, type GraphDispatchRequest } from "../src/functions/graph-dispatch.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

const origin = { project: "p", sessionId: "s", sourceFingerprint: "selected-source", intentId: "selected-intent", jobId: "selected-intent-job" };
const canonicalInput = "{\"graphProvenanceVersion\":2,\"graphWorkGroupsVersion\":2,\"observations\":[],\"sessionId\":\"s\"}";

describe("selected graph wire isolation", () => {
  it("excludes injected transport metadata from immutable identity and rejects changed domain input", async () => {
    const graph = graphStateHarness();
    const kv = graph.kv as unknown as StateKV;
    await kv.set(KV.sessions, "s", { id: "s", project: "p" });
    const execute = vi.fn(async () => ({ success: true, nodesAdded: 1 }));
    const intake = registerGraphDispatch(graph.sdk as unknown as IIIClient, kv, execute);
    const fingerprint = graphRecordDigest(canonicalInput);
    const base = { sessionId: "s", observations: [], graphWorkGroupsVersion: 2 as const };
    const selection = { project: "p", sessionId: "s", sourceFingerprint: fingerprint };

    const first = await intake({ ...base, selected: selection, _caller_worker_id: "engine-worker-a" } as GraphDispatchRequest);
    const second = await intake({ ...base, selected: selection, _caller_worker_id: "engine-worker-b" } as GraphDispatchRequest);
    expect(first).toMatchObject({ success: true, nodesAdded: 1 });
    expect(second).toMatchObject({ success: true, completed: true });
    expect(execute).toHaveBeenCalledOnce();

    const intentId = `graphintent_${fingerprint}`;
    expect(await kv.get(KV.graphDispatchInputs(intentId), "part:0")).toBe(canonicalInput);
    expect(await kv.get(KV.graphDispatchIntents, intentId)).toMatchObject({ digest: fingerprint, state: "completed" });
    await expect(intake({ ...base, force: true, selected: selection } as GraphDispatchRequest))
      .rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(execute).toHaveBeenCalledOnce();
  });

  it("runs and replays the exact selected job while preserving foreign pending history and pointer", async () => {
    const graph = graphStateHarness();
    const kv = graph.kv as unknown as StateKV;
    await kv.getVersioned(KV.graphControl, "current");
    const protectedIds = ["graphjob_mv0567t0_7c392765f08d", "graphjob_muzu6a4k_c7aa10a891eb"];
    const protectedJobs = protectedIds.map(id => ({ version: 1, id, generation: "1", kind: "extraction", state: "staging",
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z", captureParts: 1,
      captureDigest: "historical-capture", captureComplete: false }));
    for (const job of protectedJobs) graph.seed(KV.graphJobs, job.id, job);
    const pointer = { jobId: protectedIds[0], generation: "1", fence: "0", baselineComplete: false };
    const discovery = { generation: "1", revision: "historical-revision", cursor: "historical-cursor" };
    await kv.set(KV.config, "graph-dispatch:active", pointer);
    await kv.set(KV.config, "graph-dispatch:discovery", discovery);
    const beforeJobs = await Promise.all(protectedIds.map(id => kv.get(KV.graphJobs, id)));
    const body = vi.fn(async () => ({ useful: "selected result" }));

    expect(await withSelectedGraphOrigin(origin, () => graphDispatchBlocker(kv))).toBeNull();
    expect(await withSelectedGraphOrigin(origin, () => runGraphJob(kv, "extraction", { source: "captured" }, body, "selected-unit")))
      .toEqual({ useful: "selected result" });
    expect(await withSelectedGraphOrigin(origin, () => runGraphJob(kv, "extraction", { source: "captured" }, body, "selected-unit")))
      .toEqual({ useful: "selected result" });
    expect(body).toHaveBeenCalledOnce();
    expect(await Promise.all(protectedIds.map(id => kv.get(KV.graphJobs, id)))).toEqual(beforeJobs);
    expect(await kv.get(KV.config, "graph-dispatch:active")).toEqual(pointer);
    expect(await kv.get(KV.config, "graph-dispatch:discovery")).toEqual(discovery);

    await expect(withSelectedGraphOrigin({ ...origin, sourceFingerprint: "changed-source" },
      () => runGraphJob(kv, "extraction", { source: "captured" }, body, "selected-unit")))
      .rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    await expect(withSelectedGraphOrigin(origin,
      () => runGraphJob(kv, "extraction", { source: "changed" }, body, "selected-unit")))
      .rejects.toMatchObject({ code: "STATE_TX_REPLAY_CONFLICT" });
    expect(body).toHaveBeenCalledOnce();
    expect(await Promise.all(protectedIds.map(id => kv.get(KV.graphJobs, id)))).toEqual(beforeJobs);
    expect(await kv.get(KV.config, "graph-dispatch:active")).toEqual(pointer);
    expect(await kv.get(KV.config, "graph-dispatch:discovery")).toEqual(discovery);
  });

  it("still honors an explicit graph recovery pointer during selected work", async () => {
    const job = { id: "maintenance-job", generation: "1", state: "applying" };
    const kv = {
      getVersioned: vi.fn(async () => ({ value: { generation: "1", recovery: { job_id: job.id } }, version: "1" })),
      get: vi.fn(async (scope: string, id: string) => scope === KV.graphJobs && id === job.id ? job : null),
    } as unknown as StateKV;

    expect(await withSelectedGraphOrigin(origin, () => graphDispatchBlocker(kv))).toEqual(job);
  });
});
