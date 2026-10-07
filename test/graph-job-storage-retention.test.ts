import { describe, expect, it } from "vitest";
import { GraphJobStorage, type GraphDeltaPreparation } from "../src/functions/graph-job-storage.js";
import { KV } from "../src/state/schema.js";
import type { StateGraphGuard } from "../src/state/state-transactions.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

describe("minimum graph staging writes", () => {
  it("keeps repeated identical writes and reads from growing receipts", async () => {
    const harness = graphStateHarness();
    const guard = await harness.kv.lease({ action: "acquire", owner_id: "staging", generation: "1", ttl_ms: 120_000 }) as StateGraphGuard;
    const storage = new GraphJobStorage(harness.kv as never, guard, "job");
    const delta: GraphDeltaPreparation = { id: "capture", ordinal: 0, attempt: "test", phase: "preparing", capturedAt: "frozen", shadowCount: 0 };
    const scope = KV.graphInputs("job");
    await storage.stage(scope, "value", { stable: true, optional: undefined }, delta);
    const receiptCount = harness.receipts.size;
    const checkpointVersion = storage.checkpointVersion;
    for (let index = 0; index < 100; index++) await storage.stage(scope, "value", { stable: true }, delta);
    expect(harness.receipts.size).toBe(receiptCount);
    expect(storage.checkpointVersion).toBe(checkpointVersion);
    await storage.stage(scope, "value", { stable: false }, delta);
    expect(harness.receipts.size).toBe(receiptCount + 1);
    expect(await storage.get(scope, "value")).toEqual({ stable: false });

    const facade = storage.facade(delta);
    await facade.get(KV.graphNodes, "absent");
    const afterFirstRead = harness.receipts.size;
    for (let index = 0; index < 100; index++) expect(await facade.get(KV.graphNodes, "absent")).toBeNull();
    expect(harness.receipts.size).toBe(afterFirstRead);
    expect(delta.shadowCount).toBe(1);
  });
});
