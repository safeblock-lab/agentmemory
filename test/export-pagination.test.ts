import { describe, expect, it, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { registerExportImportFunction } from "../src/functions/export-import.js";
import { SAFE_PAYLOAD_BYTES } from "../src/state/frame-guard.js";
import { KV } from "../src/state/schema.js";
import type { ExportCollection, ExportData, GraphNode } from "../src/types.js";
import { EXPORT_COLLECTIONS } from "../src/types.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

const makeNode = (id: string, properties: Record<string, unknown> = {}): GraphNode => ({
  id,
  type: "concept",
  name: id,
  properties,
  sourceObservationIds: [],
  createdAt: "2026-10-06T00:00:00.000Z",
});

const collectionScopes: Record<ExportCollection, { scope: string; keyField: string }> = {
  sessions: { scope: KV.sessions, keyField: "id" },
  observations: { scope: KV.observations("session-a"), keyField: "id" },
  memories: { scope: KV.memories, keyField: "id" },
  summaries: { scope: KV.summaries, keyField: "sessionId" },
  profiles: { scope: KV.profiles, keyField: "project" },
  graphNodes: { scope: KV.graphNodes, keyField: "id" },
  graphEdges: { scope: KV.graphEdges, keyField: "id" },
  semanticMemories: { scope: KV.semantic, keyField: "id" },
  proceduralMemories: { scope: KV.procedural, keyField: "id" },
  actions: { scope: KV.actions, keyField: "id" },
  actionEdges: { scope: KV.actionEdges, keyField: "id" },
  routines: { scope: KV.routines, keyField: "id" },
  signals: { scope: KV.signals, keyField: "id" },
  checkpoints: { scope: KV.checkpoints, keyField: "id" },
  sentinels: { scope: KV.sentinels, keyField: "id" },
  sketches: { scope: KV.sketches, keyField: "id" },
  crystals: { scope: KV.crystals, keyField: "id" },
  facets: { scope: KV.facets, keyField: "id" },
  lessons: { scope: KV.lessons, keyField: "id" },
  insights: { scope: KV.insights, keyField: "id" },
  accessLogs: { scope: KV.accessLog, keyField: "memoryId" },
};

describe("cursor-based export", () => {
  it("exports every graph node across bounded cursors, including a 1.32 MB node", async () => {
    const source = graphStateHarness();
    await source.kv.set(KV.graphNodes, "node-large", makeNode("node-large", { payload: "x".repeat(1_320_550) }));
    await source.kv.set(KV.graphNodes, "node-a", makeNode("node-a"));
    await source.kv.set(KV.graphNodes, "node-b", makeNode("node-b"));
    registerExportImportFunction(source.sdk as never, source.kv as never);

    const pages: ExportData[] = [];
    let cursor: string | undefined;
    for (;;) {
      const page = await source.sdk.trigger("mem::export", { collection: "graphNodes", cursor, limit: 2 }) as ExportData;
      expect(page.pagination?.collection).toBe("graphNodes");
      expect(page.pagination?.collectionRevision).toBeTruthy();
      expect(page.pagination?.collectionFingerprint).toBeUndefined();
      pages.push(page);
      cursor = page.pagination?.nextCursor;
      if (!page.pagination?.hasMore) break;
      expect(cursor).toBeTruthy();
    }

    const nodes = pages.flatMap((page) => page.graphNodes ?? []);
    expect(nodes.map((node) => node.id)).toEqual(["node-large", "node-a", "node-b"]);
    expect(nodes[0].properties.payload).toBe("x".repeat(1_320_550));
    expect(pages.at(-1)?.pagination?.total).toBe(3);
    expect(pages.slice(0, -1).every((page) => page.pagination?.total === undefined)).toBe(true);

    const target = graphStateHarness();
    registerExportImportFunction(target.sdk as never, target.kv as never);
    for (const page of pages) {
      const imported = await target.sdk.trigger("mem::import", { exportData: page, strategy: "merge" }) as { success: boolean };
      expect(imported.success).toBe(true);
    }
    const restored = await target.sdk.trigger("mem::export", {}) as ExportData;
    expect(restored.graphNodes?.map((node) => node.id)).toEqual(nodes.map((node) => node.id));
    expect(restored.graphNodes?.[0].properties.payload).toBe("x".repeat(1_320_550));
  });

  it("rejects a continuation when the collection changes between pages", async () => {
    const harness = graphStateHarness();
    await harness.kv.set(KV.graphNodes, "node-a", makeNode("node-a"));
    await harness.kv.set(KV.graphNodes, "node-b", makeNode("node-b"));
    registerExportImportFunction(harness.sdk as never, harness.kv as never);
    const first = await harness.sdk.trigger("mem::export", { collection: "graphNodes", limit: 1 }) as ExportData;
    await harness.kv.delete(KV.graphNodes, "node-b");
    await harness.kv.set(KV.graphNodes, "node-c", makeNode("node-c"));
    const second = await harness.sdk.trigger("mem::export", {
      collection: "graphNodes", cursor: first.pagination?.nextCursor, limit: 1,
    }) as { success: boolean; error: string };
    expect(second).toMatchObject({ success: false, error: "STATE_EXPORT_COLLECTION_CHANGED" });
  });

  it("checks revisions after point reads and fails if a write lands during the page", async () => {
    const harness = graphStateHarness();
    await harness.kv.set(KV.graphNodes, "node-a", makeNode("node-a"));
    registerExportImportFunction(harness.sdk as never, harness.kv as never);
    const get = harness.kv.get.bind(harness.kv);
    let mutated = false;
    vi.spyOn(harness.kv, "get").mockImplementation(async (scope: string, key: string) => {
      const record = await get(scope, key);
      if (!mutated && scope === KV.graphNodes) {
        mutated = true;
        await harness.kv.set(scope, "node-b", makeNode("node-b"));
      }
      return record;
    });
    const result = await harness.sdk.trigger("mem::export", { collection: "graphNodes", limit: 1 }) as { success: boolean; error: string };
    expect(result).toMatchObject({ success: false, error: "STATE_EXPORT_COLLECTION_CHANGED" });
  });

  it("rolls observations across session scopes and guards both the session list and observation prefix", async () => {
    const harness = graphStateHarness();
    await harness.kv.set(KV.sessions, "session-a", { id: "session-a" });
    await harness.kv.set(KV.sessions, "session-b", { id: "session-b" });
    for (const [sessionId, id] of [["session-a", "a1"], ["session-a", "a2"], ["session-b", "b1"]]) {
      await harness.kv.set(KV.observations(sessionId), id, { id, text: id });
    }
    registerExportImportFunction(harness.sdk as never, harness.kv as never);

    const ids: string[] = [];
    let cursor: string | undefined;
    let finalTotal: number | undefined;
    for (;;) {
      const page = await harness.sdk.trigger("mem::export", { collection: "observations", cursor, limit: 1 }) as ExportData;
      for (const [sessionId, observations] of Object.entries(page.observations ?? {})) {
        ids.push(...observations.map((observation) => `${sessionId}:${observation.id}`));
      }
      cursor = page.pagination?.nextCursor;
      if (!page.pagination?.hasMore) {
        finalTotal = page.pagination?.total;
        break;
      }
    }
    expect(ids).toEqual(["session-a:a1", "session-a:a2", "session-b:b1"]);
    expect(finalTotal).toBe(3);

    const first = await harness.sdk.trigger("mem::export", { collection: "observations", limit: 1 }) as ExportData;
    await harness.kv.set(KV.sessions, "session-c", { id: "session-c" });
    const changed = await harness.sdk.trigger("mem::export", {
      collection: "observations", cursor: first.pagination?.nextCursor, limit: 1,
    }) as { success: boolean; error: string };
    expect(changed).toMatchObject({ success: false, error: "STATE_EXPORT_COLLECTION_CHANGED" });
  });

  it("keeps the next observation when it does not fit the remaining page budget", async () => {
    const harness = graphStateHarness();
    await harness.kv.set(KV.sessions, "session-a", { id: "session-a" });
    await harness.kv.set(KV.observations("session-a"), "large", {
      id: "large", text: "x".repeat(SAFE_PAYLOAD_BYTES - 64 * 1024 - 64),
    });
    await harness.kv.set(KV.observations("session-a"), "small", { id: "small", text: "small" });
    registerExportImportFunction(harness.sdk as never, harness.kv as never);

    const first = await harness.sdk.trigger("mem::export", {
      collection: "observations", limit: 10,
    }) as ExportData;
    expect(first.observations["session-a"]?.map((observation) => observation.id)).toEqual(["large"]);
    expect(first.pagination?.hasMore).toBe(true);
    expect(first.pagination?.nextCursor).toBeTruthy();

    const second = await harness.sdk.trigger("mem::export", {
      collection: "observations", cursor: first.pagination?.nextCursor, limit: 10,
    }) as ExportData;
    expect(second.observations["session-a"]?.map((observation) => observation.id)).toEqual(["small"]);
    expect(second.pagination?.hasMore).toBe(false);
    expect(second.pagination?.total).toBe(2);
  });

  it("limits work across empty observation scopes and returns a continuation", async () => {
    const harness = graphStateHarness();
    for (let index = 0; index < 20; index++) {
      const id = `empty-${String(index).padStart(2, "0")}`;
      await harness.kv.set(KV.sessions, id, { id });
    }
    const pages = vi.spyOn(harness.kv, "pages");
    registerExportImportFunction(harness.sdk as never, harness.kv as never);

    const page = await harness.sdk.trigger("mem::export", {
      collection: "observations", limit: 1,
    }) as ExportData;
    expect(page.observations).toEqual({});
    expect(page.pagination?.hasMore).toBe(true);
    expect(page.pagination?.nextCursor).toBeTruthy();
    expect(pages).toHaveBeenCalledTimes(17);
  });

  it("pages all 21 collections through projected keys and point reads", async () => {
    expect(EXPORT_COLLECTIONS).toHaveLength(21);
    const harness = graphStateHarness();
    const list = vi.spyOn(harness.kv, "list").mockImplementation(async (scope: string) => {
      throw new Error(`full list is not allowed for export page: ${scope}`);
    });
    for (const collection of EXPORT_COLLECTIONS) {
      if (collection === "observations") {
        await harness.kv.set(KV.sessions, "session-a", { id: "session-a" });
        await harness.kv.set(KV.observations("session-a"), "observation-a", { id: "observation-a" });
        continue;
      }
      const { scope, keyField } = collectionScopes[collection];
      const record = { id: "record-a", [keyField]: "record-a" };
      await harness.kv.set(scope, "record-a", record);
    }
    registerExportImportFunction(harness.sdk as never, harness.kv as never);
    for (const collection of EXPORT_COLLECTIONS) {
      const result = await harness.sdk.trigger("mem::export", { collection, limit: 5 }) as ExportData;
      expect(result.pagination?.collectionRevision).toBeTruthy();
      expect(result.pagination?.total).toBe(collection === "sessions" ? 2 : 1);
      expect(result.pagination?.hasMore).toBe(false);
    }
    expect(list).not.toHaveBeenCalled();
  });

  it("reports an unsplittable record over the transport ceiling explicitly", async () => {
    const harness = graphStateHarness();
    await harness.kv.set(KV.memories, "large-memory", {
      id: "large-memory", content: "x".repeat(SAFE_PAYLOAD_BYTES + 1),
    });
    registerExportImportFunction(harness.sdk as never, harness.kv as never);
    const result = await harness.sdk.trigger("mem::export", {
      collection: "memories", limit: 1,
    }) as { success: boolean; oversized?: boolean; bytes?: number; version?: string };
    expect(result.success).toBe(false);
    expect(result.oversized).toBe(true);
    expect(result.bytes).toBeGreaterThan(SAFE_PAYLOAD_BYTES);
    expect(result.version).toBeUndefined();
  });
});
