import { describe, expect, it } from "vitest";
import { createDashboardOperationalMetrics, recordFailureMinute } from "../src/functions/dashboard-operational-metrics.js";
import { KV } from "../src/state/schema.js";

function kvWith(values: Record<string, unknown> = {}, graphMetadata: () => Promise<unknown> = async () => null) {
  const state = new Map(Object.entries(values).map(([key, value]) => [key, structuredClone(value)]));
  const updates: Array<Array<Record<string, unknown>>> = [];
  return {
    state,
    updates,
    graphStorageMetadata: graphMetadata,
    async get(scope: string, key: string) { return structuredClone(state.get(`${scope}/${key}`) ?? null); },
    async update(scope: string, key: string, ops: Array<Record<string, unknown>>) {
      updates.push(structuredClone(ops));
      const storageKey = `${scope}/${key}`;
      const value = (state.get(storageKey) as Record<string, unknown> | undefined) ?? {};
      for (const op of ops) {
        const path = String(op.path);
        if (op.type === "increment") value[path] = Number(value[path] ?? 0) + Number(op.by);
        else if (op.type === "remove") delete value[path];
        else if (op.type === "set") value[path] = op.value;
      }
      state.set(storageKey, value);
      return { ...value, errors: [] };
    },
  };
}

function bucket(minute: number): string { return `m${minute.toString(36)}`; }

describe("dashboard operational metrics", () => {
  it("keeps maintenance failures, pressure and physical progress distinct", async () => {
    const engine = async () => ({ residentBytes: null, sampledAt: null, source: "unavailable" as const });
    const read = async (value: unknown) => createDashboardOperationalMetrics(kvWith({ [`${KV.storageMaintenance}/current`]: value }) as never, engine)
      .read(new Date("2026-10-13T18:00:00Z"), false);
    expect((await read({ version: 1, compaction: null })).maintenance?.state).toBe("unavailable");
    expect((await read({ version: 1, compaction: { lastErrorCode: "SECRET DETAIL" } })).maintenance)
      .toMatchObject({ state: "dependency_failure", reason: "storage_error" });
    expect((await read({ version: 1, compaction: { lastStatus: "deferred", lastReason: "foreground_pressure" } })).maintenance)
      .toMatchObject({ state: "deferred", reason: "foreground_pressure" });
    const reclaimed = { at: "2026-10-13T17:00:00Z", pagesReclaimed: 2, physicalBytesReclaimed: 0 };
    const noProgress = await read({ version: 1, compaction: {}, lastReclaim: reclaimed });
    expect(noProgress.lastSuccessfulCompactionAt).toBeNull();
    expect(noProgress.maintenance?.state).toBe("no_progress");
    const progress = await read({ version: 1, compaction: {}, lastReclaim: { ...reclaimed, physicalBytesReclaimed: 8192 } });
    expect(progress.lastSuccessfulCompactionAt).toBe(reclaimed.at);
    expect(progress.maintenance).toMatchObject({ state: "progress", physicalBytesReclaimed: 8192 });
    expect((await read({ version: 1, compaction: {}, lastSuccessfulCompactionAt: reclaimed.at, lastReclaim: reclaimed })).lastSuccessfulCompactionAt).toBe(reclaimed.at);
  });
  it("counts the exact completed interval and uses seven-scope native graph metadata", async () => {
    const checkedAt = new Date("2026-09-24T12:00:45.000Z");
    const asOf = Math.floor(checkedAt.getTime() / 60_000) * 60_000;
    const endMinute = asOf / 60_000;
    const startMinute = endMinute - 24 * 60;
    const sampleAt = "2026-09-24T11:54:00.000Z";
    const graphMetadata = {
      status: "ready", source: "native_scoped_metadata", complete: true,
      rows: 4, logical_bytes: 2_861, stored_bytes: 4_096, sampled_at: sampleAt,
      scope_count: 7, coverage: "all seven durable graph scopes",
    };
    const kv = kvWith({
      [`${KV.dashboardFailureMinutes}/global`]: {
        schemaVersion: 1,
        coverageSince: new Date(asOf - 24 * 60 * 60_000).toISOString(),
        [bucket(startMinute)]: 1,
        [bucket(endMinute - 1)]: 2,
        [bucket(endMinute)]: 100,
        [bucket(startMinute - 1)]: 500,
      },
      [`${KV.storageMaintenance}/current`]: {
        diagnostics: { lastComplete: {
          sampledAt: sampleAt, categoryVersion: 1, coverage: "state_rows only", rowsScanned: 10,
          categories: [
            { category: "durable_graph", rows: 2, logical_bytes: 0, stored_bytes: 1024, tombstones: 0 },
            { category: "graph_technical", rows: 3, logical_bytes: 0, stored_bytes: 2048, tombstones: 0 },
            { category: "other_state", rows: 90, logical_bytes: 0, stored_bytes: 999_999, tombstones: 0 },
          ],
          physical: { auto_vacuum: 2 },
        } },
        lastSuccessfulCompactionAt: "2026-09-24T10:00:00.000Z",
      },
    }, async () => graphMetadata);
    const readMetrics = createDashboardOperationalMetrics(kv as never, async () => ({
      residentBytes: 12_345, sampledAt: checkedAt.toISOString(), source: "iii.exe",
    }));

    const result = await readMetrics.read(checkedAt, false);
    expect(result.asOf).toBe("2026-09-24T12:00:00.000Z");
    expect(result.failures24h).toMatchObject({ count: 3, complete: true, intervalEnd: result.asOf });
    expect(result.failures24h.intervalStart).toBe("2026-09-23T12:00:00.000Z");
    expect(result.graphStorage).toEqual({
      storedBytes: 4_096, logicalBytes: 2_861, rows: 4, sampleAt,
      coverage: "all seven durable graph scopes", complete: true, source: "native_scoped_metadata",
    });
    expect(result.engineMemory.residentBytes).toBe(12_345);
    expect(result.lastSuccessfulCompactionAt).toBe("2026-09-24T10:00:00.000Z");
    expect(result.automaticCompaction).toEqual({ enabled: true, state: "enabled", autoVacuumMode: 2 });
  });

  it("persists first-read coverage without inventing zero before a complete window", async () => {
    const now = new Date("2026-09-24T12:00:00.000Z");
    const engine = async () => ({ residentBytes: null, sampledAt: null, source: "unavailable" as const, reason: "remote_engine" as const });
    const emptyKv = kvWith();
    const empty = createDashboardOperationalMetrics(emptyKv as never, engine);
    expect((await empty.read(now, false)).failures24h).toMatchObject({ count: null, complete: false, reason: "warming_up" });
    const initialized = emptyKv.state.get(`${KV.dashboardFailureMinutes}/global`) as Record<string, unknown>;
    expect(initialized).toMatchObject({ schemaVersion: 1, coverageSince: now.toISOString() });
    expect((await empty.read(new Date("2026-09-25T12:01:00.000Z"), false)).failures24h)
      .toMatchObject({ count: 0, complete: true });

    const warming = createDashboardOperationalMetrics(kvWith({
      [`${KV.dashboardFailureMinutes}/global`]: { schemaVersion: 1, coverageSince: "2026-09-24T11:00:00.000Z" },
    }) as never, engine);
    expect((await warming.read(now, false)).failures24h).toMatchObject({ count: null, reason: "warming_up" });

    const invalid = createDashboardOperationalMetrics(kvWith({
      [`${KV.dashboardFailureMinutes}/global`]: { coverageInvalid: true },
    }) as never, engine);
    expect((await invalid.read(now, false)).failures24h).toMatchObject({ count: null, reason: "coverage_invalid" });
  });

  it("prunes expired minute keys in the same bounded atomic update as the new increment", async () => {
    const currentMinute = Math.floor(Date.parse("2026-09-24T12:00:00.000Z") / 60_000);
    const existing = {
      schemaVersion: 1,
      coverageSince: "2026-09-23T00:00:00.000Z",
      ...Object.fromEntries(Array.from({ length: 1441 }, (_, index) => [bucket(currentMinute - 1440 + index), 1])),
    };
    const kv = kvWith({ [`${KV.dashboardFailureMinutes}/global`]: existing });
    await recordFailureMinute(kv as never, (currentMinute + 1) * 60_000);
    const updated = kv.state.get(`${KV.dashboardFailureMinutes}/global`) as Record<string, unknown>;
    const buckets = Object.keys(updated).filter((key) => /^m[0-9a-z]+$/.test(key));
    expect(buckets).toHaveLength(1441);
    expect(Buffer.byteLength(JSON.stringify(updated), "utf8")).toBeLessThan(64 * 1024);
    expect(updated[bucket(currentMinute - 1440)]).toBeUndefined();
    expect(updated[bucket(currentMinute + 1)]).toBe(1);
    expect(kv.updates[0]).toHaveLength(2);
    expect(kv.updates[0]!.map((op) => op.type)).toEqual(["remove", "increment"]);
  });

  it("does not substitute aggregate storage diagnostic categories for native graph metadata", async () => {
    const kv = kvWith({ [`${KV.storageMaintenance}/current`]: { diagnostics: { lastComplete: {
      sampledAt: "2026-09-24T11:54:00.000Z", categoryVersion: 1, coverage: "state_rows only",
      rowsScanned: 10, categories: [
        { category: "durable_graph", rows: 2, logical_bytes: 0, stored_bytes: 1024, tombstones: 0 },
        { category: "graph_technical", rows: 3, logical_bytes: 0, stored_bytes: 2048, tombstones: 0 },
      ], physical: { auto_vacuum: 2 },
    } } } });
    const metrics = createDashboardOperationalMetrics(kv as never, async () => ({
      residentBytes: null, sampledAt: null, source: "unavailable" as const,
    }));

    expect((await metrics.read(new Date("2026-09-24T12:00:00.000Z"), false)).graphStorage).toEqual({
      storedBytes: null, logicalBytes: null, rows: null, sampleAt: null, coverage: null,
      complete: false, source: "unavailable", reason: "scoped_metadata_unavailable",
    });
  });

  it("keeps unavailable and stale graph metadata unavailable", async () => {
    const emptyMetadata = {
      source: "unavailable", complete: false, rows: null, logical_bytes: null, stored_bytes: null,
      sampled_at: null, scope_count: 7, coverage: "all seven durable graph scopes",
    };
    for (const metadata of [
      { ...emptyMetadata, status: "unavailable", reason: "metadata_not_prepared" },
      { ...emptyMetadata, status: "stale", reason: "metadata_stale" },
    ]) {
      const metrics = createDashboardOperationalMetrics(kvWith({}, async () => metadata) as never, async () => ({
        residentBytes: null, sampledAt: null, source: "unavailable" as const,
      }));
      expect((await metrics.read(new Date("2026-09-24T12:00:00.000Z"), false)).graphStorage).toEqual({
        storedBytes: null, logicalBytes: null, rows: null, sampleAt: null,
        coverage: "all seven durable graph scopes", complete: false,
        source: "unavailable", reason: "scoped_metadata_unavailable",
      });
    }
  });

  it("keeps graph metadata read errors and incomplete scope coverage unavailable", async () => {
    const engine = async () => ({ residentBytes: null, sampledAt: null, source: "unavailable" as const });
    const readError = createDashboardOperationalMetrics(kvWith({}, async () => { throw new Error("native read failed"); }) as never, engine);
    expect((await readError.read(new Date("2026-09-24T12:00:00.000Z"), false)).graphStorage)
      .toEqual({
        storedBytes: null, logicalBytes: null, rows: null, sampleAt: null, coverage: null,
        complete: false, source: "unavailable", reason: "scoped_metadata_unavailable",
      });

    const incomplete = createDashboardOperationalMetrics(kvWith({}, async () => ({
      status: "ready", source: "native_scoped_metadata", complete: true,
      rows: 4, logical_bytes: 2_861, stored_bytes: 4_096, sampled_at: "2026-09-24T11:54:00.000Z",
      scope_count: 6, coverage: "six of seven durable graph scopes",
    })) as never, engine);
    expect((await incomplete.read(new Date("2026-09-24T12:00:00.000Z"), false)).graphStorage)
      .toMatchObject({ storedBytes: null, logicalBytes: null, rows: null, complete: false, source: "unavailable" });
  });

  it("preserves a complete native measurement of an empty graph as zero", async () => {
    const zeroMetadata = {
      status: "ready", source: "native_scoped_metadata", complete: true,
      rows: 0, logical_bytes: 0, stored_bytes: 0, sampled_at: "2026-09-24T11:54:00.000Z",
      scope_count: 7, coverage: "all seven durable graph scopes",
    };
    const metrics = createDashboardOperationalMetrics(kvWith({}, async () => zeroMetadata) as never, async () => ({
      residentBytes: null, sampledAt: null, source: "unavailable" as const,
    }));
    expect((await metrics.read(new Date("2026-09-24T12:00:00.000Z"), false)).graphStorage).toEqual({
      storedBytes: 0, logicalBytes: 0, rows: 0, sampleAt: "2026-09-24T11:54:00.000Z",
      coverage: "all seven durable graph scopes", complete: true, source: "native_scoped_metadata",
    });
  });
});
