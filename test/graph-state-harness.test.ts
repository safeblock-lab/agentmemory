import { describe, expect, it } from "vitest";
import { withCompletedGraphRead } from "../src/functions/graph-jobs.js";
import { KV } from "../src/state/schema.js";
import { prepareStateCommitBatch, type StateCommitBatchInput, type StateGraphGuard } from "../src/state/state-transactions.js";
import { effectHarness } from "./batch-effects-harness.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

function batch(overrides: Partial<StateCommitBatchInput> = {}): StateCommitBatchInput {
  return {
    identity: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", chunk_ordinal: 0 },
    expected_checkpoint_version: "0",
    checkpoint: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", delta_ordinal: 0, next_chunk_ordinal: 1, visibility: "applying" },
    operations: [{ type: "set", scope: KV.graphNodes, key: "node-a", expected_version: "0", value: { weight: 1 } }],
    ...overrides,
  };
}

describe("isolated graph native-wire harness", () => {
  it("keeps absent, present-null, and tombstone versions distinct", async () => {
    const harness = graphStateHarness();
    harness.seed(KV.graphNodes, "present-null", null, true, "7");
    harness.seed(KV.graphNodes, "tombstone", null, false, "9");
    await expect(harness.kv.getVersioned(KV.graphNodes, "absent")).resolves.toEqual({ exists: false, value: null, version: "0" });
    await expect(harness.kv.getVersioned(KV.graphNodes, "present-null")).resolves.toEqual({ exists: true, value: null, version: "7" });
    await expect(harness.kv.getVersioned(KV.graphNodes, "tombstone")).resolves.toEqual({ exists: false, value: null, version: "9" });
  });

  it("fences plain writes, competing leases, stale guards, and altered digests", async () => {
    const harness = graphStateHarness();
    const guard = await harness.kv.lease({ action: "acquire", owner_id: "owner-a", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    await expect(harness.kv.lease({ action: "acquire", owner_id: "owner-b", generation: "1", ttl_ms: 10_000 })).rejects.toMatchObject({ code: "STATE_TX_LEASE_BUSY" });
    await expect(harness.sdk.trigger({ function_id: "state::set", payload: { scope: KV.graphNodes, key: "x", value: 1 } })).rejects.toMatchObject({ code: "STATE_TX_FENCED" });
    const prepared = prepareStateCommitBatch(batch());
    await expect(harness.kv.commitBatch({ ...guard, fence: "99" }, prepared)).rejects.toMatchObject({ code: "STATE_TX_FENCED" });
    await expect(harness.kv.commitBatch(guard, { ...prepared, payload_digest: "0".repeat(64) })).rejects.toMatchObject({ code: "STATE_TX_INVALID_REQUEST" });
  });

  it("rejects row conflicts before applying any operation", async () => {
    const harness = graphStateHarness();
    harness.seed(KV.graphNodes, "first", { value: "before" });
    const guard = await harness.kv.lease({ action: "acquire", owner_id: "writer", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    const prepared = prepareStateCommitBatch(batch({ operations: [
      { type: "set", scope: KV.graphNodes, key: "first", expected_version: "1", value: { value: "after" } },
      { type: "set", scope: KV.graphNodes, key: "second", expected_version: "8", value: { value: "new" } },
    ] }));
    await expect(harness.kv.commitBatch(guard, prepared)).rejects.toMatchObject({ code: "STATE_TX_CONFLICT" });
    await expect(harness.kv.get(KV.graphNodes, "first")).resolves.toEqual({ value: "before" });
    await expect(harness.kv.get(KV.graphNodes, "second")).resolves.toBeNull();
    expect(harness.receipts.size).toBe(0);
  });

  it("distinguishes a pre-commit failure from a lost acknowledgement after receipt", async () => {
    const harness = effectHarness({ nativeGraphWire: true });
    const guard = await harness.kv.lease({ action: "acquire", owner_id: "writer", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    const applying = prepareStateCommitBatch(batch());
    harness.crashGraphCommit(KV.graphNodes, false);
    await expect(harness.kv.commitBatch(guard, applying)).rejects.toMatchObject({ code: "STATE_TX_FAILED" });
    await expect(harness.kv.get(KV.graphNodes, "node-a")).resolves.toBeNull();

    const firstReceipt = await harness.kv.commitBatch(guard, applying);
    expect(firstReceipt.payload_digest).toBe(applying.payload_digest);
    await expect(harness.kv.get(KV.graphNodes, "node-a")).resolves.toEqual({ weight: 1 });

    const completeInput = batch({
      identity: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", chunk_ordinal: 1 },
      expected_checkpoint_version: "1",
      checkpoint: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", delta_ordinal: 0, next_chunk_ordinal: 2, visibility: "complete" },
      operations: [{ type: "set", scope: KV.graphSnapshot, key: "current", expected_version: "0", value: { complete: true } }],
    });
    const complete = prepareStateCommitBatch(completeInput);
    harness.crashGraphCommit(KV.graphSnapshot, true);
    await expect(harness.kv.commitBatch(guard, complete)).rejects.toMatchObject({ code: "STATE_TX_FAILED" });
    const committed = await harness.kv.getVersioned(KV.graphSnapshot, "current");
    expect(committed).toEqual({ exists: true, value: { complete: true }, version: "1" });

    const replay = await harness.kv.commitBatch(guard, complete);
    expect(replay.payload_digest).toBe(complete.payload_digest);
    expect(await harness.kv.getVersioned(KV.graphSnapshot, "current")).toEqual(committed);
  });

  it("blocks graph reads behind an unfinished delta and publishes its completed snapshot", async () => {
    const harness = graphStateHarness();
    const guard = await harness.kv.lease({ action: "acquire", owner_id: "writer", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    await harness.kv.commitBatch(guard, prepareStateCommitBatch(batch()));
    await expect(harness.kv.getVersioned(KV.graphNodes, "node-a")).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });
    await harness.kv.commitBatch(guard, prepareStateCommitBatch(batch({
      identity: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", chunk_ordinal: 1 },
      expected_checkpoint_version: "1",
      checkpoint: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", delta_ordinal: 0, next_chunk_ordinal: 2, visibility: "complete" },
      operations: [{ type: "set", scope: KV.graphSnapshot, key: "current", expected_version: "0", value: null }],
    })));
    await harness.kv.lease({ action: "release", owner_id: guard.owner_id, generation: guard.generation, fence: guard.fence });
    await expect(harness.kv.getVersioned(KV.graphSnapshot, "current")).resolves.toMatchObject({ exists: true, value: null, version: "1" });
  });

  it("replays a lost generation-advance receipt under a current guard and rejects unseen old work", async () => {
    const harness = graphStateHarness();
    const oldGuard = await harness.kv.lease({ action: "acquire", owner_id: "old", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
    const reset = batch({
      checkpoint: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", delta_ordinal: 0, next_chunk_ordinal: 1, visibility: "complete" },
      advance_generation: true,
      operations: [{ type: "set", scope: KV.graphSnapshot, key: "current", expected_version: "0", value: { resetAt: "frozen" } }],
    });
    const prepared = prepareStateCommitBatch(reset);
    harness.loseAcknowledgment();
    await expect(harness.kv.commitBatch(oldGuard, prepared)).rejects.toMatchObject({ code: "STATE_TX_FAILED" });
    expect(harness.receipts.size).toBe(1);
    await expect(harness.kv.lease({
      action: "release",
      owner_id: oldGuard.owner_id,
      generation: oldGuard.generation,
      fence: oldGuard.fence,
    })).rejects.toMatchObject({ code: "STATE_TX_GENERATION_STALE" });
    const current = await harness.kv.lease({ action: "acquire", owner_id: "new", generation: "2", ttl_ms: 10_000 }) as StateGraphGuard;
    const replay = await harness.kv.commitBatch(current, prepared);
    expect(replay).toEqual([...harness.receipts.values()][0]);
    expect(await harness.kv.getVersioned(KV.graphSnapshot, "current")).toMatchObject({ version: "1", value: { resetAt: "frozen" } });
    const unseen = batch({ identity: { generation: "1", job_id: "unseen", logical_delta_id: "delta:0", chunk_ordinal: 0 }, checkpoint: { generation: "1", job_id: "unseen", logical_delta_id: "delta:0", delta_ordinal: 0, next_chunk_ordinal: 1, visibility: "applying" } });
    await expect(harness.kv.commitBatch(current, prepareStateCommitBatch(unseen))).rejects.toMatchObject({ code: "STATE_TX_GENERATION_STALE" });
  });

  it("rejects a mixed read across a writer even after the barrier clears", async () => {
    const harness = graphStateHarness();
    harness.seed(KV.graphSnapshot, "current", { revision: "old" });
    await expect(withCompletedGraphRead(harness.kv as never, async () => {
      const before = await harness.kv.get(KV.graphSnapshot, "current");
      const guard = await harness.kv.lease({ action: "acquire", owner_id: "reader-race-writer", generation: "1", ttl_ms: 10_000 }) as StateGraphGuard;
      await harness.kv.commitBatch(guard, prepareStateCommitBatch(batch()));
      await harness.kv.commitBatch(guard, prepareStateCommitBatch(batch({
        identity: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", chunk_ordinal: 1 },
        expected_checkpoint_version: "1",
        checkpoint: { generation: "1", job_id: "job-1", logical_delta_id: "delta:0", delta_ordinal: 0, next_chunk_ordinal: 2, visibility: "complete" },
        operations: [{ type: "set", scope: KV.graphSnapshot, key: "current", expected_version: "1", value: { revision: "new" } }],
      })));
      await harness.kv.lease({ action: "release", owner_id: guard.owner_id, generation: guard.generation, fence: guard.fence });
      const after = await harness.kv.get(KV.graphSnapshot, "current");
      return { before, after };
    })).rejects.toMatchObject({ code: "STATE_GRAPH_RECOVERY_REQUIRED" });
  });
});
