import { describe, expect, it, vi } from "vitest";
import { viewerMemories } from "../src/functions/viewer-memory-page.js";

type Memory = Record<string, unknown> & { id: string };

function memoryPage(rows: Memory[], nextCursor: string | null = null) {
  const pages = vi.fn(async function* () {
    yield { items: rows, next_cursor: nextCursor };
    throw new Error("viewer must not fetch another page");
  });
  const kv = { pages, list: vi.fn(() => { throw new Error("unbounded list forbidden"); }) };
  return { kv, pages };
}

describe("viewer memory ordering", () => {
  it("sorts the complete bounded result newest first regardless of KV key order", async () => {
    const { kv } = memoryPage([
      { id: "a-old", isLatest: true, createdAt: "2026-10-01T00:00:00Z" },
      { id: "z-new", isLatest: true, createdAt: "2026-10-11T00:00:00Z" },
    ]);

    const result = await viewerMemories(kv as never, { latest: "true" });

    expect("memories" in result && result.memories?.map(row => row.id)).toEqual(["z-new", "a-old"]);
    expect(result).toMatchObject({ partial: false, total: 2, returnedCount: 2 });
  });

  it("prefers valid updatedAt, falls back to createdAt, and orders equal or invalid dates by id", async () => {
    const { kv } = memoryPage([
      { id: "tie-b", isLatest: true, createdAt: "2026-10-08T00:00:00Z" },
      { id: "invalid-b", isLatest: true, createdAt: "not-a-date", updatedAt: "also-invalid" },
      { id: "updated", isLatest: true, createdAt: "2026-10-20T00:00:00Z", updatedAt: "2026-10-09T00:00:00Z" },
      { id: "invalid-a", isLatest: true },
      { id: "fallback", isLatest: true, createdAt: "2026-10-10T00:00:00Z", updatedAt: "not-a-date" },
      { id: "tie-a", isLatest: true, createdAt: "2026-10-08T00:00:00Z" },
      { id: "not-latest", isLatest: false, createdAt: "2026-10-12T00:00:00Z" },
    ]);

    const result = await viewerMemories(kv as never, { latest: "true" });

    expect("memories" in result && result.memories?.map(row => row.id))
      .toEqual(["fallback", "updated", "tie-a", "tie-b", "invalid-a", "invalid-b"]);
  });

  it("sorts only rows returned by a partial cursor page and preserves page metadata", async () => {
    const { kv, pages } = memoryPage([
      { id: "sample-old", isLatest: true, createdAt: "2026-10-06T00:00:00Z" },
      { id: "sample-new", isLatest: true, createdAt: "2026-10-08T00:00:00Z" },
    ], "next-page");

    const result = await viewerMemories(kv as never, { cursor: "current-page", latest: "true" });

    expect("memories" in result && result.memories?.map(row => row.id)).toEqual(["sample-new", "sample-old"]);
    expect(result).toMatchObject({ partial: true, total: null, nextCursor: "next-page", scanned: 2, returnedCount: 2, countScope: "page" });
    expect(pages).toHaveBeenCalledTimes(1);
  });

  it("applies the same bounded ordering to version-validated project memories", async () => {
    const session = { session_id: "session-1", source_key: "session-1", row_version: "1",
      session: { id: "session-1", project: "p" } };
    const rows: Memory[] = [
      { id: "project-old", isLatest: true, createdAt: "2026-10-01T00:00:00Z" },
      { id: "project-new", isLatest: true, createdAt: "2026-10-11T00:00:00Z" },
    ];
    const retrieval = vi.fn(async (payload: Record<string, unknown>) => {
      if (payload.action === "project_sessions_page") return { items: [session], next_cursor: null, partial: false,
        status: "ready", revision: 1, epoch: 1 };
      if (payload.action === "canonical_source_page") return { status: "ready",
        items: rows.map(row => ({ id: row.id, row_version: "1" })), next_cursor: null };
      if (payload.action === "reflect_sources") return { items: rows.map(row => ({ scope: "mem:memories", key: row.id, value: row })), deferred: [] };
      throw new Error(`unexpected retrieval action: ${String(payload.action)}`);
    });
    const kv = { retrieval, get: vi.fn(async () => null), list: vi.fn(() => { throw new Error("unbounded list forbidden"); }) };

    const result = await viewerMemories(kv as never, { project: "p", latest: "true" });

    expect("memories" in result && result.memories?.map(row => row.id)).toEqual(["project-new", "project-old"]);
    expect(result).toMatchObject({ partial: false, total: 2, returnedCount: 2 });
  });
});
