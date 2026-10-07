import { describe, expect, it, vi } from "vitest";
import { StateKV } from "../src/state/kv.js";
import { collectProjectedRecords, countProjectedRecordIds } from "../src/functions/projected-record-reader.js";

type RecordRow = { id: string; value: string };

function createPagedKV(rows: RecordRow[], missingIds = new Set<string>()) {
  let activeGets = 0;
  let maxActiveGets = 0;
  const trigger = vi.fn(async (request: {
    function_id: string;
    payload: Record<string, unknown>;
  }) => {
    if (request.function_id === "state::list_page") {
      expect(request.payload.fields).toEqual(["id"]);
      const offset = request.payload.cursor ? Number(request.payload.cursor) : 0;
      const limit = request.payload.limit as number;
      const pageRows = rows.slice(offset, offset + limit);
      const nextOffset = offset + pageRows.length;
      return {
        items: pageRows.map(({ id }) => ({ id })),
        next_cursor: nextOffset < rows.length ? String(nextOffset) : null,
      };
    }
    if (request.function_id === "state::get") {
      const id = request.payload.key as string;
      activeGets++;
      maxActiveGets = Math.max(maxActiveGets, activeGets);
      await new Promise((resolve) => setTimeout(resolve, 1));
      activeGets--;
      return missingIds.has(id) ? null : rows.find((row) => row.id === id) ?? null;
    }
    throw new Error(`Unexpected function: ${request.function_id}`);
  });
  return { kv: new StateKV({ trigger } as never), trigger, get maxActiveGets() { return maxActiveGets; } };
}

describe("projected record reader", () => {
  it("reads every projected row in order with bounded point-read concurrency", async () => {
    const rows = Array.from({ length: 270 }, (_, index) => ({
      id: `node-${String(index).padStart(3, "0")}`,
      value: `value-${index}`,
    }));
    const fixture = createPagedKV(rows);

    await expect(collectProjectedRecords<RecordRow>(fixture.kv, "mem:graph:nodes")).resolves.toEqual(rows);
    expect(fixture.trigger.mock.calls.filter(([call]) => call.function_id === "state::list_page")).toHaveLength(2);
    expect(fixture.trigger.mock.calls.filter(([call]) => call.function_id === "state::get")).toHaveLength(rows.length);
    expect(fixture.maxActiveGets).toBeLessThanOrEqual(8);
    expect(fixture.maxActiveGets).toBeGreaterThan(1);
  });

  it("fails the scan if a projected row disappears instead of silently skipping it", async () => {
    const rows = [
      { id: "node-1", value: "first" },
      { id: "node-2", value: "second" },
    ];
    const { kv, trigger } = createPagedKV(rows, new Set(["node-2"]));

    await expect(collectProjectedRecords<RecordRow>(kv, "mem:graph:nodes"))
      .rejects.toThrow("STATE_GRAPH_PROJECTED_RECORD_MISSING");
    expect(trigger.mock.calls.filter(([call]) => call.function_id === "state::get")).toHaveLength(2);
  });

  it("counts projected IDs without reading full records", async () => {
    const rows = Array.from({ length: 1_050 }, (_, index) => ({ id: `node-${index}`, value: "large" }));
    const fixture = createPagedKV(rows);

    await expect(countProjectedRecordIds(fixture.kv, "mem:graph:nodes")).resolves.toBe(rows.length);
    expect(fixture.trigger.mock.calls.filter(([call]) => call.function_id === "state::list_page")).toHaveLength(2);
    expect(fixture.trigger.mock.calls.filter(([call]) => call.function_id === "state::get")).toHaveLength(0);
  });
});
