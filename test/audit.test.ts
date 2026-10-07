import { describe, it, expect, beforeEach, vi } from "vitest";
import type { AuditEntry } from "../src/types.js";
import { KV } from "../src/state/schema.js";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { recordAudit, queryAudit } from "../src/functions/audit.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      return (store.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
  };
}

function legacyAudit(id: string, timestamp: string, operation: AuditEntry["operation"] = "observe"): AuditEntry {
  return {
    id,
    timestamp,
    operation,
    functionId: `mem::${id}`,
    targetIds: [],
    details: {},
  };
}

describe("Audit Functions", () => {
  let kv: ReturnType<typeof mockKV>;

  beforeEach(() => {
    kv = mockKV();
  });

  it("does not persist per-operation audit rows or effect-key markers", async () => {
    const get = vi.spyOn(kv, "get");
    const set = vi.spyOn(kv, "set");

    for (let index = 0; index < 100; index++) {
      await recordAudit(
        kv as never,
        "observe",
        "mem::compress",
        [`obs_${index}`],
        { index },
        undefined,
        undefined,
        "same-effect-key",
      );
    }

    expect(get).not.toHaveBeenCalled();
    expect(set).not.toHaveBeenCalled();
    expect(await kv.list(KV.audit)).toEqual([]);
  });

  it("queryAudit reads legacy entries sorted by timestamp desc", async () => {
    await kv.set(KV.audit, "aud_early", legacyAudit("aud_early", "2026-01-01T00:00:00.000Z"));
    await kv.set(KV.audit, "aud_late", legacyAudit("aud_late", "2026-01-02T00:00:00.000Z", "delete"));

    const entries = await queryAudit(kv as never);
    expect(entries.map((entry) => entry.id)).toEqual(["aud_late", "aud_early"]);
  });

  it("queryAudit filters legacy entries by operation", async () => {
    await kv.set(KV.audit, "aud_observe", legacyAudit("aud_observe", "2026-01-01T00:00:00.000Z"));
    await kv.set(KV.audit, "aud_delete", legacyAudit("aud_delete", "2026-01-02T00:00:00.000Z", "delete"));
    await kv.set(KV.audit, "aud_observe_2", legacyAudit("aud_observe_2", "2026-01-03T00:00:00.000Z"));

    const entries = await queryAudit(kv as never, { operation: "observe" });
    expect(entries.map((entry) => entry.id)).toEqual(["aud_observe_2", "aud_observe"]);
  });

  it("queryAudit filters legacy entries by date range", async () => {
    await kv.set(KV.audit, "aud_early", legacyAudit("aud_early", "2026-01-01T00:00:00.000Z"));
    await kv.set(KV.audit, "aud_late", legacyAudit("aud_late", "2026-01-02T00:00:00.000Z", "delete"));

    const entries = await queryAudit(kv as never, {
      dateFrom: "2026-01-02T00:00:00.000Z",
    });
    expect(entries.map((entry) => entry.id)).toEqual(["aud_late"]);

    const entriesBefore = await queryAudit(kv as never, {
      dateTo: "2026-01-01T00:00:00.000Z",
    });
    expect(entriesBefore.map((entry) => entry.id)).toEqual(["aud_early"]);
  });

  it("queryAudit respects the requested limit", async () => {
    for (let index = 0; index < 10; index++) {
      const entry = legacyAudit(`aud_${index}`, `2026-01-${String(index + 1).padStart(2, "0")}T00:00:00.000Z`);
      await kv.set(KV.audit, entry.id, entry);
    }

    const entries = await queryAudit(kv as never, { limit: 3 });
    expect(entries).toHaveLength(3);
  });
});
