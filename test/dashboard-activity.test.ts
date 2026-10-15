import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { recordOperation } from "../src/functions/dashboard-activity.js";
import { KV } from "../src/state/schema.js";

function memoryKv(initial: Record<string, unknown> = {}) {
  const values = new Map<string, Record<string, unknown>>(
    Object.entries(initial).map(([key, value]) => [key, structuredClone(value) as Record<string, unknown>]),
  );
  return {
    values,
    async get(scope: string, key: string) { return structuredClone(values.get(`${scope}/${key}`) ?? null); },
    async update(scope: string, key: string, ops: Array<Record<string, unknown>>) {
      const storageKey = `${scope}/${key}`;
      const value = values.get(storageKey) ?? {};
      for (const op of ops) {
        const path = String(op.path);
        if (op.type === "increment") value[path] = Number(value[path] ?? 0) + Number(op.by);
        else if (op.type === "remove") delete value[path];
        else if (op.type === "set") value[path] = op.value;
        else if (op.type === "append_bounded") {
          const append = op.value as { item: unknown; limit: number };
          const items = Array.isArray(value[path]) ? value[path] as unknown[] : [];
          value[path] = [...items, append.item].slice(-append.limit);
        }
      }
      values.set(storageKey, value);
      return { ...value, errors: [] };
    },
  };
}

describe("dashboard operation activity", () => {
  it("counts only failed operations in bounded minute buckets and hashes isolated agent keys", async () => {
    const kv = memoryKv();
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      await recordOperation(kv as never, "mem::graph-extract", "failed", { itemsFailed: 1 }, "agent/private-path");
      await recordOperation(kv as never, "mem::graph-extract", "partial", { itemsFailed: 1 }, "agent/private-path");

      const hashedAgent = createHash("sha256").update("agent/private-path").digest("hex").slice(0, 32);
      const counterKey = `${KV.dashboardFailureMinutes}/agent:${hashedAgent}`;
      const counters = kv.values.get(counterKey);
      expect(counters?.coverageSince).toEqual(expect.any(String));
      expect(Object.entries(counters ?? {}).filter(([key]) => /^m[0-9a-z]+$/.test(key))).toHaveLength(1);
      const minuteBucket = Object.entries(counters ?? {}).find(([key]) => /^m[0-9a-z]+$/.test(key));
      expect(minuteBucket?.[1]).toBe(1);
      expect([...kv.values.keys()]).toContain(counterKey);
      expect(JSON.stringify([...kv.values.keys()])).not.toContain("private-path");
      expect(counters?.schemaVersion).toBe(1);
      expect(counters?.coverageInvalid).toBeUndefined();
      expect(stderr).not.toHaveBeenCalled();
    } finally {
      stderr.mockRestore();
    }
  });

  it("marks failure coverage invalid when the atomic counter update fails", async () => {
    const kv = memoryKv();
    const originalUpdate = kv.update.bind(kv);
    let first = true;
    kv.update = async (...args) => {
      if (first) { first = false; throw new Error("state write failed"); }
      return originalUpdate(...args);
    };
    const stderr = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    try {
      await recordOperation(kv as never, "mem::graph-extract", "failed", { itemsFailed: 1 }, "agent");
      const hashedAgent = createHash("sha256").update("agent").digest("hex").slice(0, 32);
      expect(kv.values.get(`${KV.dashboardFailureMinutes}/agent:${hashedAgent}`)?.coverageInvalid).toBe(true);
      expect(stderr).toHaveBeenCalled();
    } finally {
      stderr.mockRestore();
    }
  });

  it("preserves concurrent increments through atomic per-minute updates", async () => {
    const kv = memoryKv();
    await Promise.all(Array.from({ length: 40 }, () =>
      recordOperation(kv as never, "mem::graph-extract", "failed", { itemsFailed: 1 }, "shared-agent"),
    ));
    const hashedAgent = createHash("sha256").update("shared-agent").digest("hex").slice(0, 32);
    const counters = kv.values.get(`${KV.dashboardFailureMinutes}/agent:${hashedAgent}`) ?? {};
    const minuteBucket = Object.entries(counters).find(([key]) => /^m[0-9a-z]+$/.test(key));
    expect(minuteBucket?.[1]).toBe(40);
  });
});
