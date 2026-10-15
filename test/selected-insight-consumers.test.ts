import { beforeEach, describe, expect, it } from "vitest";
import { reflectHarness } from "./helpers/reflect-harness.js";
import { KV } from "../src/state/schema.js";

function insight(id: string, project: string, confidence: number, title: string, content: string, tags: string[]) {
  const now = new Date().toISOString();
  return {
    id, project, confidence, title, content, tags, createdAt: now, updatedAt: now,
    reinforcements: 1, sourceConceptCluster: [], sourceMemoryIds: [], sourceLessonIds: [], sourceCrystalIds: [], decayRate: 0.05,
  };
}

describe("selected insight consumers", () => {
  let h: ReturnType<typeof reflectHarness>;
  beforeEach(() => { h = reflectHarness(); });

  it("lists and searches only exact selected insights while retaining filters, scoring, and search audit", async () => {
    const primary = insight("ins_primary", "/app", 0.9, "Payment decline recovery", "Map processor decline codes", ["payments"]);
    const partial = insight("ins_partial", "/app", 0.8, "Payment cache note", "Keep a short cache", ["cache"]);
    const foreign = insight("ins_foreign", "/other", 1, "Payment decline recovery", "Unrelated source", ["payments"]);
    for (const item of [primary, partial, foreign]) h.rows.set(`${KV.insights}/${item.id}`, item);

    const listed = await h.call("mem::insight-list", {
      selected: { project: "/app", insightIds: [partial.id, primary.id] }, project: "/app", minConfidence: 0.85, limit: 1,
    });
    const searched = await h.call("mem::insight-search", {
      query: "payment decline", selected: { project: "/app", insightIds: [partial.id, primary.id] }, project: "/app", limit: 1,
    });

    expect(listed).toMatchObject({ success: true, insights: [primary] });
    expect(searched.insights.map((item: { id: string }) => item.id)).toEqual([primary.id]);
    expect(h.kv.get.mock.calls.filter(([scope]) => scope === KV.insights).map(([, id]) => id)).toEqual([
      partial.id, primary.id, partial.id, primary.id,
    ]);
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("rejects foreign canonical project records and mismatched outer project before any global list", async () => {
    const foreign = insight("ins_foreign", "/other", 0.9, "Boundary protection", "Private project insight", ["security"]);
    h.rows.set(`${KV.insights}/${foreign.id}`, foreign);

    const result = await h.call("mem::insight-list", {
      selected: { project: "/app", insightIds: [foreign.id] },
    });
    const outerMismatch = await h.call("mem::insight-search", {
      query: "security", selected: { project: "/app", insightIds: [foreign.id] }, project: "/other",
    });

    expect(result).toMatchObject({ success: false, error: "selected insight project scope mismatch" });
    expect(outerMismatch).toMatchObject({ success: false, error: "selected insight project does not match project filter" });
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("accepts generated Unicode content beyond 512 bytes while rejecting content over 64 KiB", async () => {
    const longContent = insight("ins_long", "/app", 0.9, "Long insight", `payment ${"ñ".repeat(300)}`, ["payments"]);
    h.rows.set(`${KV.insights}/${longContent.id}`, longContent);
    const accepted = await h.call("mem::insight-search", {
      query: "payment", selected: { project: "/app", insightIds: [longContent.id] },
    });
    longContent.content = "ñ".repeat(32_769);
    h.rows.set(`${KV.insights}/${longContent.id}`, longContent);
    const oversized = await h.call("mem::insight-list", {
      selected: { project: "/app", insightIds: [longContent.id] },
    });

    expect(accepted.insights.map((item: { id: string }) => item.id)).toEqual([longContent.id]);
    expect(oversized).toMatchObject({ success: false, error: "selected insight record is invalid" });
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it.each([
    [],
    ["ins_one", "ins_one"],
    Array.from({ length: 13 }, (_, index) => `ins_${index}`),
  ])("rejects invalid selected insight IDs before point reads: %j", async insightIds => {
    const result = await h.call("mem::insight-list", { selected: { project: "/app", insightIds } });

    expect(result).toMatchObject({ success: false, error: "selected insight scope is invalid" });
    expect(h.kv.get).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("retains ordinary insight listing and rejects invalid selected consumer limits", async () => {
    const one = insight("ins_one", "/app", 0.9, "One", "Relevant content", ["relevant"]);
    h.rows.set(`${KV.insights}/${one.id}`, one);

    const ordinary = await h.call("mem::insight-list", { project: "/app", limit: 1 });
    const invalid = await h.call("mem::insight-search", {
      query: "relevant", selected: { project: "/app", insightIds: [one.id] }, limit: -1,
    });

    expect(ordinary).toMatchObject({ success: true, insights: [one] });
    expect(invalid).toMatchObject({ success: false, error: "limit must be a non-negative safe integer" });
    expect(h.kv.list).toHaveBeenCalledTimes(1);
  });
});
