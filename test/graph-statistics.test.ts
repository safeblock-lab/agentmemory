import { describe, expect, it, vi } from "vitest";
import { readGraphStatistics } from "../src/functions/graph-statistics.js";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";

const snapshot = { version: 1, updatedAt: "2026-10-08T23:10:38.898Z", dirty: false,
  stats: { totalNodes: 77674, totalEdges: 119388, nodesByType: { concept: 77674 }, edgesByType: { related_to: 119388 } } };
function fixture(items: unknown[], next_cursor: string | null = null) {
  const pages = vi.fn(async function* () { yield { items, next_cursor }; });
  const get = vi.fn(() => { throw new Error("full snapshot hydration forbidden"); });
  return { kv: { pages, get } as unknown as StateKV, pages, get };
}
describe("graph statistics metadata", () => {
  it("returns actual historical counts with timestamp and no graph hydration", async () => {
    const f = fixture([snapshot]);
    expect(await readGraphStatistics(f.kv)).toMatchObject({ totalNodes: 77674, totalEdges: 119388,
      countsAreCurrent: false, fromSnapshot: true, provenance: "historical_snapshot", updatedAt: snapshot.updatedAt });
    expect(f.pages).toHaveBeenCalledWith(KV.graphSnapshot, { limit: 1, maxBytes: 16384,
      fields: ["batchInProgress", "dirty", "project", "stats", "updatedAt", "version"] });
    expect(f.get).not.toHaveBeenCalled();
  });
  it("preserves zero counts", async () => {
    const f = fixture([{ ...snapshot, stats: { totalNodes: 0, totalEdges: 0, nodesByType: {}, edgesByType: {} } }]);
    expect(await readGraphStatistics(f.kv)).toMatchObject({ totalNodes: 0, totalEdges: 0, fromSnapshot: true });
  });
  it("never presents a global snapshot as project totals", async () => {
    expect(await readGraphStatistics(fixture([snapshot]).kv, "alpha")).toMatchObject({ totalNodes: null, reason: "project_aggregate_unavailable" });
  });
  it("accepts only exact project snapshot metadata", async () => {
    expect(await readGraphStatistics(fixture([{ ...snapshot, project: "alpha" }]).kv, "alpha")).toMatchObject({ totalNodes: 77674, project: "alpha" });
  });
  it.each([[{ ...snapshot, stats: { ...snapshot.stats, totalNodes: -1 } }], [snapshot, snapshot]])("rejects malformed or ambiguous data", async (...items) => {
    expect(await readGraphStatistics(fixture(items).kv)).toMatchObject({ totalNodes: null, fromSnapshot: false });
  });
  it("does not claim a truncated projection is an authoritative singleton", async () => {
    expect(await readGraphStatistics(fixture([snapshot], "more").kv)).toMatchObject({ totalNodes: null });
  });
});
