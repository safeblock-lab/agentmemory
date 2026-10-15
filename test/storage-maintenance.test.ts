import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/functions/graph-retention.js", () => ({ pruneTerminalGraphWork: vi.fn(async () => ({ deleted: 0, logicalBytes: 0, storedBytes: 0, pending: false })) }));
vi.mock("../src/functions/observation-identity.js", () => ({ reconstructLegacyIdentity: vi.fn(() => null) }));
import { registerStorageMaintenance } from "../src/functions/storage-maintenance.js";
import { KV } from "../src/state/schema.js";
import type { StorageMaintenanceState, StoragePhysicalSample } from "../src/types.js";

function physical(pageCount: number, databaseBytes: number): StoragePhysicalSample {
  return {
    page_size: 4096, page_count: pageCount, freelist_count: 8, reusable_bytes: 32768,
    allocated_page_bytes: pageCount * 4096, auto_vacuum: 2, journal_mode: "wal",
    database_bytes: databaseBytes, wal_bytes: 0,
  };
}

function initialState(): StorageMaintenanceState {
  return {
    version: 1, updatedAt: "2026-09-24T00:00:00.000Z", cleanupActivatedAt: "2026-09-20T00:00:00.000Z",
    lastSuccessfulCompactionAt: "2026-09-22T00:00:00.000Z",
    compaction: {
      pending: true, nextEligibleAt: null, nextMaxPages: 1, minIntervalMs: 5000,
      backoffUntil: null, attempts: 0, progressCuts: 0, completedPolls: 0, deferredPolls: 0,
      failures: 0, overlapSkips: 0, lastStatus: null, lastReason: null, lastErrorCode: null, overlap: null,
    },
    totals: { graphRowsDeleted: 0, technicalRowsDeleted: 0, rawStripped: 0, operationRowsDeleted: 0,
      logicalBytesDeleted: 0, storedBytesDeleted: 0, unresolvedExclusions: 0, unknownAgeExclusions: 0 },
    observationIndex: { sessionsCursor: null, sessionId: null, nextSessionCursor: null, observationsCursor: null,
      scanned: 0, indexed: 0, unindexable: 0, conflicts: 0, readySessions: 0, complete: false },
    diagnostics: { inProgress: { cursor: null, categoryVersion: null, sampledAt: null, coverage: null,
      rowsScanned: 0, categories: [], physical: null }, samples: [] },
    technical: { scopeIndex: 0, cursors: {}, observationSessionsCursor: null, observationSessionId: null,
      observationNextSessionCursor: null, observationCursor: null, eventSessionsCursor: null, eventSessionId: null,
      eventNextSessionCursor: null, eventCursor: null, summaryJobsCursor: null, summaryJobId: null,
      summaryJobsNextCursor: null, summaryUnitsCursor: null, summaryJobReady: false },
  };
}

type HarnessOptions = {
  stateSaveError?: Error;
  staleStateAfterRefresh?: boolean;
  refreshResponses?: unknown[];
  releaseResponse?: unknown;
  reclaimResponse?: unknown;
  reclaimError?: Error;
};

function register(
  initial: StorageMaintenanceState,
  result: unknown,
  acquire: "acquired" | "busy" = "acquired",
  recoveredExpired = false,
  options: HarnessOptions = {},
) {
  const handlers = new Map<string, (...args: unknown[]) => Promise<unknown>>();
  let saved = structuredClone(initial);
  const sdk = {
    registerFunction: (id: string, handler: (...args: unknown[]) => Promise<unknown>) => handlers.set(id, handler),
    trigger: vi.fn(async (input: { payload?: { action?: string; value?: StorageMaintenanceState; release?: boolean; fence?: number } }) => {
      const action = input.payload?.action;
      if (action === "maintenance_lease_acquire") return acquire === "busy"
        ? { status: "busy", fence: 7, expires_at: new Date(Date.now() + 30_000).toISOString() }
        : { status: "acquired", fence: 7, expires_at: new Date(Date.now() + 120_000).toISOString(), recovered_expired: recoveredExpired };
      if (action === "maintenance_lease_refresh") return options.refreshResponses?.shift()
        ?? { status: "refreshed", fence: 7, expires_at: new Date(Date.now() + 120_000).toISOString() };
      if (action === "maintenance_state_save") {
        if (options.stateSaveError) throw options.stateSaveError;
        if (options.staleStateAfterRefresh && options.refreshResponses?.length === 0) throw new Error("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
        if (input.payload?.release && options.releaseResponse !== undefined) return options.releaseResponse;
        saved = structuredClone(input.payload!.value!);
        return { status: "saved", fence: input.payload!.fence, version: "1", released: input.payload!.release };
      }
      if (action === "reclaim") {
        if (options.reclaimError) throw options.reclaimError;
        if (options.reclaimResponse !== undefined) return options.reclaimResponse;
      }
      if (action === "diagnostics") return {
        category_version: 1, sampled_at: new Date(Date.now()).toISOString(), consistency: "keyset_live",
        complete: true, done: true, rows_scanned: 0, categories: [], cursor: null,
        physical: physical(100, 409_600), coverage: "state_rows only",
      };
      return result;
    }),
  };
  const kv = {
    get: vi.fn(async (scope: string) => scope === KV.storageMaintenance ? structuredClone(saved) : null),
    set: vi.fn(async (_scope: string, _key: string, value: StorageMaintenanceState) => { saved = structuredClone(value); return value; }),
  };
  registerStorageMaintenance(sdk as never, kv as never);
  return { handlers, sdk, kv, getSaved: () => saved };
}

describe("storage maintenance compaction timestamp", () => {
  afterEach(() => vi.useRealTimers());

  it("does not redispatch an unknown state-save acknowledgement from catch or finally", async () => {
    const harness = register(initialState(), {}, "acquired", false, { stateSaveError: new Error("lost acknowledgement") });
    const response = await harness.handlers.get("mem::storage-maintenance")!({ action: "tick" }) as { success: boolean };
    expect(response.success).toBe(false);
    const saves = harness.sdk.trigger.mock.calls.filter(([input]) => input.payload?.action === "maintenance_state_save");
    expect(saves).toHaveLength(1);
    expect(harness.kv.set).not.toHaveBeenCalled();
    expect(harness.getSaved()).toEqual(initialState());
  });

  it("does not overwrite successor state or release after stale refresh", async () => {
    const original = initialState();
    const harness = register(original, {}, "acquired", false, {
      staleStateAfterRefresh: true,
      refreshResponses: [{ status: "stale", fence: 8, expires_at: null }],
    });
    await harness.handlers.get("mem::storage-maintenance")!({ action: "reclaim" });
    const saves = harness.sdk.trigger.mock.calls.filter(([input]) => input.payload?.action === "maintenance_state_save");
    expect(saves).toHaveLength(2);
    expect(saves.every(([input]) => input.payload?.release === false)).toBe(true);
    expect(harness.getSaved().observationIndex).toEqual(original.observationIndex);
    expect(harness.kv.set).not.toHaveBeenCalled();
  });

  it("updates the success timestamp only when pages were actually reclaimed", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const result = {
      mode: "incremental", requires_explicit_full_rebuild: false, checkpoint_busy: false,
      wal_frames: 0, checkpointed_frames: 0, checkpoint_performed: false,
      status: "progress", reason: "reclaimed", elapsed_ms: 4, pages_reclaimed: 1,
      physical_bytes_reclaimed: 4096, before: physical(100, 409600), after: physical(99, 405504),
    };
    const harness = register(initialState(), result);
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "reclaim" }) as { state: StorageMaintenanceState };
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-24T12:00:00.000Z");
    expect(harness.sdk.trigger.mock.calls.map(([input]) => (input as { payload: { action: string } }).payload.action).filter((action) => action !== "maintenance_state_save"))
      .toEqual(["maintenance_lease_acquire", "maintenance_lease_refresh", "maintenance_lease_refresh", "reclaim"]);
  });

  it("preserves the previous date after a deferred poll with no reclaimed pages", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const result = {
      mode: "incremental", requires_explicit_full_rebuild: false, checkpoint_busy: false,
      wal_frames: 0, checkpointed_frames: 0, checkpoint_performed: false,
      status: "deferred", reason: "cooldown", elapsed_ms: 0, pages_reclaimed: 0,
      physical_bytes_reclaimed: 0, before: physical(100, 409600), after: physical(100, 409600),
    };
    const harness = register(initialState(), result);
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "reclaim" }) as { state: StorageMaintenanceState };
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-22T00:00:00.000Z");
  });

  it("preserves the previous date when an idle reclaim poll is not eligible", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const state = initialState();
    state.compaction.backoffUntil = "2026-09-24T13:00:00.000Z";
    const harness = register(state, {});
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "reclaim" }) as { state: StorageMaintenanceState };
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-22T00:00:00.000Z");
    expect(harness.sdk.trigger.mock.calls.map(([input]) => (input as { payload: { action: string } }).payload.action).filter((action) => action !== "maintenance_state_save"))
      .toEqual(["maintenance_lease_acquire", "maintenance_lease_refresh"]);
  });

  it("records an effective blocking migration only when it reduces allocated storage", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const result = {
      status: "complete", reason: "incremental_enabled", elapsed_ms: 100,
      blocking_rebuild: true, before: { ...physical(100, 409600), auto_vacuum: 0 },
      after: physical(80, 327680),
    };
    const harness = register(initialState(), result);
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "migrate_incremental", allow_blocking: true, max_ms: 500 }) as { state: StorageMaintenanceState };
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-24T12:00:00.000Z");
  });

  it("does not count released pages as physical success when the file did not shrink", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const result = {
      mode: "incremental", requires_explicit_full_rebuild: false, checkpoint_busy: false,
      wal_frames: 0, checkpointed_frames: 0, checkpoint_performed: false,
      status: "progress", reason: "reclaimed", elapsed_ms: 4, pages_reclaimed: 1,
      physical_bytes_reclaimed: 0, before: physical(100, 409600), after: physical(100, 409600),
    };
    const harness = register(initialState(), result);
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "reclaim" }) as { state: StorageMaintenanceState };
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-22T00:00:00.000Z");
  });

  it("does not read or write maintenance state before native lease admission", async () => {
    const harness = register(initialState(), {}, "busy");
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const original = harness.getSaved();
    const response = await handler({ action: "reclaim" }) as { busy: boolean; state: StorageMaintenanceState };
    expect(response.busy).toBe(true);
    expect(harness.sdk.trigger.mock.invocationCallOrder[0]).toBeLessThan(harness.kv.get.mock.invocationCallOrder[0]);
    expect(harness.kv.set).not.toHaveBeenCalled();
    expect(response.state.compaction).toEqual(original.compaction);
    expect(response.state.observationIndex).toEqual(original.observationIndex);
    expect(harness.getSaved()).toEqual(original);
    expect(harness.sdk.trigger).toHaveBeenCalledTimes(1);
  });

  it("flags free pages that have remained pending for a day", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const state = initialState() as StorageMaintenanceState & { maintenanceReview: {
      footprintBaselineBytes: number | null; footprintBaselineAt: string | null;
      freePagePendingSince: string | null; dueReasons: string[];
    } };
    state.maintenanceReview = {
      footprintBaselineBytes: null, footprintBaselineAt: null,
      freePagePendingSince: "2026-09-23T11:59:59.000Z", dueReasons: [],
    };
    const harness = register(state, {});
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "diagnostics" }) as { state: StorageMaintenanceState & { maintenanceReview: { dueReasons: string[] } } };
    expect(response.state.maintenanceReview.dueReasons).toContain("free_pages_24h");
  });

  it("establishes the growth baseline only after an activated, completed cleanup pass", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const state = initialState() as StorageMaintenanceState & { maintenanceReview: {
      footprintBaselineBytes: number | null; footprintBaselineAt: string | null;
      freePagePendingSince: string | null; dueReasons: string[];
    } };
    state.maintenanceReview = {
      footprintBaselineBytes: null, footprintBaselineAt: null,
      freePagePendingSince: null, dueReasons: [],
    };
    const harness = register(state, {});
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    await handler({ action: "diagnostics" });
    expect((harness.getSaved() as typeof state).maintenanceReview.footprintBaselineBytes).toBeNull();
    const saved = harness.getSaved() as typeof state;
    saved.lastCleanup = {
      startedAt: "2026-09-24T11:59:00.000Z", completedAt: "2026-09-24T11:59:30.000Z",
      graphRowsDeleted: 0, technicalCandidatesProcessed: 0, technicalDeleted: 0,
      rawStripped: 0, deletedLogicalBytes: 0, deletedStoredBytes: 0,
      operationRowsDeleted: 0, skipped: 0, excludedUnresolved: 0,
      excludedUnknownAge: 0, pending: false, exclusions: [],
    };
    const secondHarness = register(saved, {});
    await secondHarness.handlers.get("mem::storage-maintenance")!({ action: "diagnostics" });
    expect((secondHarness.getSaved() as typeof state).maintenanceReview).toMatchObject({
      footprintBaselineBytes: 409_600, footprintBaselineAt: "2026-09-24T12:00:00.000Z",
    });
  });

  it("requests review after database plus WAL footprint grows by twenty percent", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const state = initialState() as StorageMaintenanceState & { maintenanceReview: {
      footprintBaselineBytes: number | null; footprintBaselineAt: string | null;
      freePagePendingSince: string | null; dueReasons: string[];
    } };
    state.maintenanceReview = {
      footprintBaselineBytes: 330_000, footprintBaselineAt: "2026-09-20T00:00:00.000Z",
      freePagePendingSince: null, dueReasons: [],
    };
    const harness = register(state, {});
    const response = await harness.handlers.get("mem::storage-maintenance")!({ action: "diagnostics" }) as {
      state: typeof state;
    };
    expect(response.state.maintenanceReview.dueReasons).toContain("growth_20_percent");
  });

  it("records an expired native lease as an unknown prior outcome", async () => {
    const result = {
      mode: "incremental", requires_explicit_full_rebuild: false, checkpoint_busy: false,
      wal_frames: 0, checkpointed_frames: 0, checkpoint_performed: false,
      status: "complete", reason: "no_free_pages", elapsed_ms: 1, pages_reclaimed: 0,
      physical_bytes_reclaimed: 0, before: physical(100, 409600), after: physical(100, 409600),
    };
    const harness = register(initialState(), result, "acquired", true);
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "reclaim" }) as {
      state: StorageMaintenanceState & { maintenanceHistory: Array<{ action: string; outcome: string }> };
    };
    expect(response.state.maintenanceHistory[0]).toMatchObject({
      action: "lease_recovery", outcome: "outcome_unknown", reason: "expired_owner",
    });
  });

  it("keeps periodic reclaim behind the explicit cleanup activation gate", async () => {
    const state = initialState();
    delete state.cleanupActivatedAt;
    state.observationIndex.complete = true;
    state.diagnostics.lastComplete = {
      sampledAt: "2026-09-24T00:00:00.000Z", categoryVersion: 1,
      coverage: "state_rows only", rowsScanned: 0, categories: [], physical: physical(100, 409600),
    };
    const harness = register(state, {});
    const handler = harness.handlers.get("mem::storage-maintenance")!;
    const response = await handler({ action: "tick" }) as { success: boolean; code?: string; cleanupActivated?: boolean };
    const actions = harness.sdk.trigger.mock.calls.map(([input]) =>
      (input as { payload: { action: string } }).payload.action);
    expect(response).toMatchObject({ success: true, cleanupActivated: false });
    expect(actions).not.toContain("reclaim");
  });

  it.each([
    ["invalid native response", { reclaimResponse: { status: "complete" } }],
    ["unknown transport outcome", { reclaimError: new Error("connection lost after dispatch") }],
  ])("preserves the lease marker and skips release after a reclaim %s", async (_label, override) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const harness = register(initialState(), {}, "acquired", false, {
      refreshResponses: [
        { status: "refreshed", fence: 7, expires_at: "2026-09-24T12:03:00.000Z" },
        { status: "refreshed", fence: 7, expires_at: "2026-09-24T12:04:00.000Z" },
      ],
      ...override,
    });
    const response = await harness.handlers.get("mem::storage-maintenance")!({ action: "reclaim" }) as {
      deferred: boolean;
      state: StorageMaintenanceState & { maintenanceHistory: Array<{ action: string; outcome: string }> };
    };
    const actions = harness.sdk.trigger.mock.calls.map(([input]) =>
      (input as { payload: { action: string } }).payload.action);
    expect(response.deferred).toBe(true);
    expect(response.state.compaction.overlap?.expiresAt).toBe("2026-09-24T12:04:00.000Z");
    expect(response.state.maintenanceHistory).toContainEqual(expect.objectContaining({
      action: "reclaim", outcome: "outcome_unknown", reason: "storage_error",
    }));
    expect(actions).toContain("reclaim");
    expect(actions).not.toContain("maintenance_lease_release");
  });

  it("aborts before reclaim when a lease refresh is stale", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const harness = register(initialState(), {}, "acquired", false, {
      refreshResponses: [{ status: "stale", fence: 6, expires_at: null }],
    });
    const response = await harness.handlers.get("mem::storage-maintenance")!({ action: "reclaim" }) as {
      code: string;
      state: StorageMaintenanceState;
    };
    const actions = harness.sdk.trigger.mock.calls.map(([input]) =>
      (input as { payload: { action: string } }).payload.action);
    expect(response.code).toBe("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
    expect(response.state.compaction.attempts).toBe(0);
    expect(response.state.compaction.overlap?.expiresAt).toBe("2026-09-24T12:02:00.000Z");
    expect(actions).not.toContain("reclaim");
    expect(actions).not.toContain("maintenance_lease_release");
  });

  it("retains the overlap marker and records history after an unknown release acknowledgement", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const result = {
      mode: "incremental", requires_explicit_full_rebuild: false, checkpoint_busy: false,
      wal_frames: 0, checkpointed_frames: 0, checkpoint_performed: false,
      status: "complete", reason: "no_free_pages", elapsed_ms: 1, pages_reclaimed: 0,
      physical_bytes_reclaimed: 0, before: physical(100, 409600), after: physical(100, 409600),
    };
    const harness = register(initialState(), result, "acquired", false, {
      releaseResponse: { status: "stale", fence: 6, expires_at: null },
    });
    const response = await harness.handlers.get("mem::storage-maintenance")!({ action: "reclaim" }) as {
      state: StorageMaintenanceState & { maintenanceHistory: Array<{ action: string; outcome: string; reason: string }> };
    };
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-22T00:00:00.000Z");
    expect(response.state.compaction.overlap?.expiresAt).toBe("2026-09-24T12:02:00.000Z");
    expect(response.state.maintenanceHistory).toContainEqual(expect.objectContaining({
      action: "lease_release", outcome: "outcome_unknown", reason: "release_failed",
    }));
    expect(harness.sdk.trigger.mock.calls.map(([input]) =>
      (input as { payload: { action: string } }).payload.action)).toContain("maintenance_state_save");
  });

  it.each([
    ["foreground pressure", "deferred", "foreground_pressure"],
    ["migration required", "migration_required", "auto_vacuum_none"],
  ])("defers %s without success timestamp or migration replay", async (_label, status, reason) => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-24T12:00:00.000Z"));
    const result = {
      mode: "incremental", requires_explicit_full_rebuild: false,
      checkpoint_busy: reason === "foreground_pressure", wal_frames: 0, checkpointed_frames: 0,
      checkpoint_performed: false, status, reason, elapsed_ms: 0, pages_reclaimed: 0,
      physical_bytes_reclaimed: 0, before: physical(100, 409600), after: physical(100, 409600),
    };
    const harness = register(initialState(), result);
    const response = await harness.handlers.get("mem::storage-maintenance")!({ action: "reclaim" }) as {
      result: { status: string; reason: string };
      state: StorageMaintenanceState;
    };
    const actions = harness.sdk.trigger.mock.calls.map(([input]) =>
      (input as { payload: { action: string } }).payload.action);
    expect(response.result).toMatchObject({ status, reason });
    expect(response.state.lastSuccessfulCompactionAt).toBe("2026-09-22T00:00:00.000Z");
    expect(actions.filter((action) => action === "reclaim")).toHaveLength(1);
    expect(actions).not.toContain("migrate_incremental");
  });
});
