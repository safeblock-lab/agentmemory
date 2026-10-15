import { beforeEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { KV } from "../src/state/schema.js";
import { buildReflectPrompt, REFLECT_SYSTEM } from "../src/prompts/reflect.js";

describe("Reflect generation prompt", () => {
  it("bounds attribution, scope, and conclusions to the source evidence", () => {
    expect(REFLECT_SYSTEM).toContain("Keep claims within the subject, time, setting, and conditions supported by the sources.");
    expect(REFLECT_SYSTEM).toContain("Attribute actions only to actors the sources identify.");
    expect(REFLECT_SYSTEM).toContain("Distinguish what sources record as configured or changed from what was exercised, measured, or verified.");
    expect(REFLECT_SYSTEM).toContain("Do not infer attribution, causation, outcomes, or guarantees unless the evidence directly supports them.");
    expect(REFLECT_SYSTEM).toContain("Preserve useful cross-source implications when direct evidence is incomplete, but state them as qualified possibilities rather than established outcomes.");
    expect(REFLECT_SYSTEM).toContain("Treat repeated summaries of one event as one evidence chain, not independent confirmation or proof of broader behavior.");
  });

  it("preserves the source wording and its uncertainty in the generation input", () => {
    const historicalNote = "One recorded project configuration used a local provider; no security review is described.";
    const qualifiedLesson = "This may be useful for controlled local runs.";
    const prompt = buildReflectPrompt({
      concepts: ["provider routing"],
      facts: [{ fact: historicalNote, confidence: 0.8 }],
      lessons: [{ content: qualifiedLesson, confidence: 0.6 }],
      crystalNarratives: [],
    });

    expect(prompt).toContain(historicalNote);
    expect(prompt).toContain(qualifiedLesson);
  });
});

describe("Reflect public functions", () => {
  let h: ReturnType<typeof reflectHarness>;
  beforeEach(() => { h = reflectHarness(); });
  it("accepts durable work without provider calls or corpus materialization", async () => {
    const accepted = await h.call("mem::reflect", { project: "/app", query: "boundaries" });
    expect(accepted).toMatchObject({ success: true, queued: true, completed: false });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
    expect(h.published).toHaveLength(1);
    expect(await h.call("mem::reflect", { project: "/app", query: "boundaries" })).toMatchObject({ jobId: accepted.jobId });
  });
  it("rejects oversized requests before admission", async () => {
    await expect(h.call("mem::reflect", { project: "/app", query: "é".repeat(129) })).rejects.toThrow("invalid_reflect_query");
    expect(h.published).toHaveLength(0);
  });
  it("lists and searches nondeleted insights with project and confidence filters", async () => {
    const now = new Date().toISOString();
    for (const [id, project, confidence, deleted] of [["high", "/app", 0.9, false], ["low", "/other", 0.4, false], ["deleted", "/app", 1, true]] as const) {
      await h.kv.set(KV.insights, id, { id, project, confidence, deleted, title: "Boundary security", content: "Layered protection", tags: ["security"], createdAt: now });
    }
    expect((await h.call("mem::insight-list")).insights.map((i: any) => i.id)).toEqual(["high", "low"]);
    expect((await h.call("mem::insight-list", { project: "/app", minConfidence: 0.5 })).insights).toHaveLength(1);
    expect((await h.call("mem::insight-search", { query: "security", project: "/app" })).insights.map((i: any) => i.id)).toEqual(["high"]);
    expect(await h.call("mem::insight-search", { query: "" })).toMatchObject({ success: false });
  });
  it("decays old insights incrementally and soft deletes weak unrepeated insights", async () => {
    for (const [id, confidence, reinforcements] of [["old", 0.8, 1], ["weak", 0.12, 0]] as const) {
      await h.kv.set(KV.insights, id, { id, confidence, reinforcements, decayRate: 0.05, createdAt: new Date(Date.now() - 21 * 86400000).toISOString() });
    }
    expect(await h.call("mem::insight-decay-sweep")).toMatchObject({ decayed: 1, softDeleted: 1 });
    expect((await h.kv.get(KV.insights, "old")).confidence).toBeCloseTo(0.65);
    expect((await h.kv.get(KV.insights, "weak")).deleted).toBe(true);
    expect(await h.call("mem::insight-decay-sweep")).toMatchObject({ decayed: 0, softDeleted: 0 });
  });
});
