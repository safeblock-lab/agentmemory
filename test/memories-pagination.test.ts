import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";
import { viewerMemories, viewerPageOptions } from "../src/functions/viewer-memory-page.js";

type Row = { id: string; isLatest: boolean; createdAt?: string; updatedAt?: string };
function pageFixture(rows: Row[], nextCursor: string | null = null) {
  const calls: Array<{ scope: string; options: Record<string, unknown> }> = [];
  const kv = {
    list: vi.fn(() => { throw new Error("unbounded memory list forbidden"); }),
    pages: async function* (scope: string, options: Record<string, unknown>) {
      calls.push({ scope, options });
      yield { items: rows, next_cursor: nextCursor };
      throw new Error("unrequested second page forbidden");
    },
  };
  return { kv, calls };
}

describe("memories + export pagination (#544)", () => {
  const api = readFileSync("src/triggers/api.ts", "utf-8");

  it("count=true uses the same latest scope and keeps partial totals unknown", async () => {
    const rows = [{ id: "latest", isLatest: true }, { id: "old", isLatest: false }];
    const { kv } = pageFixture(rows);
    expect(await viewerMemories(kv as never, { latest: "true", count: "true" }))
      .toMatchObject({ total: 1, latestCount: 1, returnedCount: 1, partial: false });
    expect(await viewerMemories(kv as never, { latest: "true" }))
      .toMatchObject({ total: 1, memories: [rows[0]] });
    const partial = pageFixture(rows, "next");
    expect(await viewerMemories(partial.kv as never, { latest: "true", count: "true" }))
      .toMatchObject({ total: null, latestCount: null, partial: true, returnedCount: 1, nextCursor: "next", countScope: "page" });
    expect(kv.list).not.toHaveBeenCalled();
    expect(partial.kv.list).not.toHaveBeenCalled();
  });

  it("forwards an opaque cursor and rejects nonzero offset without scanning prior pages", async () => {
    const { kv, calls } = pageFixture([{ id: "last", isLatest: true }]);
    expect(await viewerMemories(kv as never, { cursor: "last-page", limit: "2" }))
      .toMatchObject({ partial: true, total: null, nextCursor: null, countScope: "page" });
    expect(calls).toHaveLength(1);
    expect(calls[0].options).toMatchObject({ cursor: "last-page", limit: 2, maxBytes: 65536 });
    expect(() => viewerPageOptions({ offset: "1" })).toThrow("viewer_cursor_required");
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("caps requests at 16 rows and 65536 bytes before fetching", async () => {
    const { kv, calls } = pageFixture([]);
    await viewerMemories(kv as never, { limit: "5000" });
    expect(calls).toHaveLength(1);
    expect(calls[0].options).toMatchObject({ limit: 16, maxBytes: 65536 });
    for (const limit of ["0", "-1", "1.5", "NaN"])
      expect(() => viewerPageOptions({ limit })).toThrow("viewer_invalid_limit");
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("api::export passes through maxSessions + offset query params", () => {
    expect(api).toMatch(/query_params\?\.\["maxSessions"\]/);
    expect(api).toMatch(/query_params\?\.\["offset"\]/);
    // The payload object is named `payload` in our handler; assert it is
    // forwarded to mem::export rather than the previous empty object.
    expect(api).toMatch(
      /sdk\.trigger\(\{\s*function_id:\s*"mem::export",\s*payload,/m,
    );
  });

  it("viewer dashboard counts memories via count=true and the tab caps its fetch", () => {
    const viewer = readFileSync("src/viewer/index.html", "utf-8");
    expect(viewer).toMatch(/memories\?latest=true&count=true/);
    expect(viewer).not.toMatch(/memories\?latest=true&limit=500/);
    expect(viewer).toMatch(/memories\?latest=true&limit=16/);
  });

  it("api::memories orders a complete result newest first regardless of KV key order", async () => {
    const { kv } = pageFixture([
      { id: "a-old", isLatest: true, createdAt: "2026-10-01T00:00:00Z" },
      { id: "z-new", isLatest: true, createdAt: "2026-10-11T00:00:00Z" },
    ]);
    const result = await viewerMemories(kv as never, { latest: "true" });
    expect("memories" in result && result.memories?.map(row => row.id)).toEqual(["z-new", "a-old"]);
  });
});
