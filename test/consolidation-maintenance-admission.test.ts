import { describe, expect, it, vi } from "vitest";
import {
  BatchMaintenanceBusyError,
  withBatchMutationLocks,
  withConsolidationAdmission,
} from "../src/state/batch-effects.js";
import { effectHarness } from "./batch-effects-harness.js";

function deferred() {
  let resolve!: () => void;
  return { promise: new Promise<void>((done) => { resolve = done; }), resolve: () => resolve() };
}

describe("consolidation maintenance admission", () => {
  it("reports active consolidation to maintenance and clears it after callback failure", async () => {
    vi.useFakeTimers();
    const h = effectHarness({ nativeGraphWire: false });
    const entered = deferred();
    const release = deferred();
    const failure = new Error("consolidation failed");
    const admitted = withConsolidationAdmission(async () => {
      entered.resolve();
      await release.promise;
      throw failure;
    });

    try {
      await entered.promise;
      const maintenanceRun = vi.fn(async () => true);
      const waiting = withBatchMutationLocks(h.kv, maintenanceRun).catch((error: unknown) => error);
      await Promise.resolve();
      await vi.advanceTimersByTimeAsync(30_000);

      const busy = await waiting;
      expect(busy).toBeInstanceOf(BatchMaintenanceBusyError);
      expect(busy).toMatchObject({
        code: "BATCH_MAINTENANCE_BUSY",
        details: {
          waitedMs: 30_000,
          activeAdmissions: 1,
          families: [{ family: "consolidation", count: 1, oldestMs: 30_000 }],
        },
      });
      expect(maintenanceRun).not.toHaveBeenCalled();

      release.resolve();
      await expect(admitted).rejects.toBe(failure);
      await expect(withBatchMutationLocks(h.kv, async () => "maintenance complete")).resolves.toBe("maintenance complete");
    } finally {
      release.resolve();
      await admitted.catch(() => undefined);
      vi.useRealTimers();
    }
  });
});
