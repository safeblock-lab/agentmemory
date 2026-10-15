import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query, items) => items) }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { parseReflectionResult } from "../src/functions/reflect-result.js";
import { countReflectResult, REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";
import { KV } from "../src/state/schema.js";

const raw = `<insights>${Array.from({ length: 5 }, (_, i) => `<insight confidence="0.9"><title>Boundary ${i}</title>Validate boundary ${i}.</insight>`).join("")}</insights>`;
const business = { project: "/app", query: "boundaries", manual: true, requestId: "complete-contract" };
async function fixture(controlled = false) {
  vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "1");
  const h = reflectHarness();
  h.provider.summarize.mockImplementation(async (system: string) => {
    return system.includes("You evaluate publication evidence") ? JSON.stringify({ verdict: "supported", reason: "Originals support all five." }) : raw;
  });
  if (controlled) {
    await h.kv.set(KV.sessions, "session", { project: "/app" });
    await h.kv.set(KV.summaries, "session", { sessionId: "session", project: "/app", title: "Boundaries", concepts: ["boundaries"] });
  }
  return h;
}

afterEach(() => { vi.unstubAllEnvs(); vi.useRealTimers(); });

describe("reflection structural result contract", () => {
  it("uses one interpretation for the retained nested-title five-insight shape, count and persistence", async () => {
    const h = await fixture();
    expect(parseReflectionResult(raw)).toHaveLength(5);
    expect(countReflectResult(raw)).toBe(5);
    const result = await h.call("mem::reflect", business);
    expect(result).toMatchObject({ status: "completed", appliedInsights: 5, processedClusters: 1 });
    expect(await h.kv.list(KV.insights)).toHaveLength(5);
    expect(await h.kv.get(REFLECT_CONTROL, `${result.jobId}:result:0`)).toMatchObject({ appliedCount: 5 });
    expect(await h.call("mem::reflect", business)).toEqual(result);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("accepts explicit empty, unordered attributes and equivalent title forms", () => {
    expect(parseReflectionResult("<insights/> ")).toEqual([]);
    expect(parseReflectionResult('<insights><insight title="A" confidence="1.0">Content</insight></insights>'))
      .toEqual([{ title: "A", content: "Content", confidence: 1 }]);
    expect(parseReflectionResult('<insights><insight confidence="0" title="A"><title>A</title>Content</insight></insights>')[0]?.confidence).toBe(0);
  });
  it.each(["NaN", "Infinity", "0.8junk", "-0.1", "1.1", "", "0.8 "])("rejects confidence %j without partial parsing or clamping", confidence => {
    expect(() => parseReflectionResult(`<insights><insight confidence="${confidence}" title="A">Content</insight></insights>`)).toThrow("reflect_generation_invalid");
  });
  it.each([
    "not XML", "<insights>unexpected text</insights>", "<insights><unknown/></insights>",
    '<insights><insight confidence="0.8" title="A"><title>B</title>Content</insight></insights>',
    '<insights><insight confidence="0.8" title="A" extra="x">Content</insight></insights>',
    raw.replace("</insights>", '<insight confidence="0.8" title="Six">Content</insight></insights>'),
  ])("rejects nonempty invalid output and retains it without a judge or regeneration: %s", async response => {
    const h = await fixture(); h.provider.summarize.mockResolvedValue(response);
    const result = await h.call("mem::reflect", business);
    expect(result).toMatchObject({ status: "failed", terminal: true, completed: false, reason: "reflect_generation_invalid" });
    expect((await h.kv.get(REFLECT_JOBS, result.jobId)).captured.response).toBe(response);
    expect(await h.call("mem::reflect", business)).toEqual(result);
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(await h.kv.list(KV.insights)).toEqual([]);
  });
});

describe("reflection ambiguous dispatch recovery", () => {
  it("refuses a new identity after an ambiguous judge dispatch without creating a successor", async () => {
    const h = await fixture(true);
    h.provider.summarize.mockImplementationOnce(async () => raw).mockImplementationOnce(async () => { throw new Error("outcome_unknown"); });
    const blocked = await h.call("mem::reflect", business);
    expect(blocked).toMatchObject({ status: "blocked", reason: "reflect_publication_pending" });
    const original = await h.kv.get(REFLECT_JOBS, blocked.jobId);
    const recovery = { ...business, requestId: "crash-recovery", sessionId: "session", recoveryOfJobId: blocked.jobId };
    const before = await h.kv.list(REFLECT_JOBS);
    const denied = await h.call("mem::reflect", recovery);
    expect(denied).toMatchObject({ success: false, error: "invalid_reflect_recovery_source" });
    expect(await h.kv.get(REFLECT_JOBS, blocked.jobId)).toEqual(original);
    expect(await h.kv.list(REFLECT_JOBS)).toEqual(before);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("refuses recovery from a predecessor without controlled session provenance", async () => {
    const h = await fixture();
    h.provider.summarize.mockImplementationOnce(async () => raw).mockImplementationOnce(async () => { throw new Error("outcome_unknown"); });
    const blocked = await h.call("mem::reflect", business);
    expect(blocked).toMatchObject({ status: "blocked", reason: "reflect_publication_pending" });
    const original = await h.kv.get(REFLECT_JOBS, blocked.jobId);
    const before = await h.kv.list(REFLECT_JOBS);
    await h.kv.set(KV.sessions, "recovery-session", { project: "/app" });
    const denied = await h.call("mem::reflect", { ...business, requestId: "uncontrolled-recovery", sessionId: "recovery-session",
      recoveryOfJobId: blocked.jobId });
    expect(denied).toMatchObject({ success: false, error: "invalid_reflect_recovery_source" });
    expect(await h.kv.get(REFLECT_JOBS, blocked.jobId)).toEqual(original);
    expect(await h.kv.list(REFLECT_JOBS)).toEqual(before);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("late judge capture resolves the next continuation while the original blocked replay stays frozen", async () => {
    vi.useFakeTimers();
    const h = await fixture(); let finish!: (response: string) => void;
    h.provider.summarize.mockImplementation(async (system: string) => system.includes("You evaluate publication evidence")
      ? new Promise<string>(resolve => { finish = resolve; }) : raw);
    const running = h.call("mem::reflect", business);
    await vi.advanceTimersByTimeAsync(60_001);
    const blocked = await running;
    expect(blocked).toMatchObject({ status: "blocked", completed: false, terminal: false, reason: "reflect_publication_pending" });
    expect(blocked.continuation).toBeTruthy();
    expect(await h.call("mem::reflect", business)).toEqual(blocked);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    finish(JSON.stringify({ verdict: "supported", reason: "Late supported response." }));
    await vi.advanceTimersByTimeAsync(0);
    const next = { ...business, continuation: blocked.continuation };
    expect(await h.call("mem::reflect", next)).toMatchObject({ status: "completed", completed: true, appliedInsights: 5 });
    expect(await h.call("mem::reflect", business)).toEqual(blocked);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(await h.kv.list(KV.insights)).toHaveLength(5);
  });
  it("rejects linked recovery against changed frozen sources without touching old captures or dispatching", async () => {
    const h = await fixture(true);
    h.provider.summarize.mockRejectedValueOnce(new Error("outcome_unknown"));
    const blocked = await h.call("mem::reflect", business);
    const original = await h.kv.get(REFLECT_JOBS, blocked.jobId);
    await h.kv.set(KV.memories, "one", { ...await h.kv.get(KV.memories, "one"), content: "Changed" });
    expect(await h.call("mem::reflect", { ...business, requestId: "changed-recovery", sessionId: "session",
      recoveryOfJobId: blocked.jobId })).toMatchObject({ success: false, error: "invalid_reflect_recovery_source" });
    expect(await h.kv.get(REFLECT_JOBS, blocked.jobId)).toEqual(original);
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
  });
});
