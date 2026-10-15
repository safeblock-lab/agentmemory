import { describe, expect, it, vi } from "vitest";
import { runSelectedConsolidationDecay, type ConsolidationDecaySelector } from "../src/functions/consolidation-decay.js";
import { CanonicalMemoryConflict, CanonicalMemoryPending, type CanonicalSourcePage, type CanonicalWriteResult } from "../src/state/canonical-memory.js";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import type { SemanticMemory } from "../src/types.js";

function harness() {
  const state = new Map<string, unknown>();
  const rows = new Map<string, SemanticMemory>();
  const receipts = new Map<string, CanonicalWriteResult>();
  const kv = { get: vi.fn(async (scope: string, id: string) => scope === KV.summaries ? { sessionId: id, project: "P" }
    : structuredClone(state.get(id) ?? null)), set: vi.fn(async (_scope: string, id: string, value: unknown) => {
      state.set(id, structuredClone(value)); return value;
    }) } as unknown as StateKV;
  const page = (ids: string[], nextCursor: string | null = null): CanonicalSourcePage => ({
    items: ids.map(id => ({ id, rowVersion: "1" })), nextCursor, membershipGeneration: "1", coverageEpoch: "1" });
  const select = vi.fn(async (scope: string, _sessions: string[], cursor: string | null) => scope === "procedural" ? page([])
    : cursor === "second" ? page(["b"]) : page(["a"], "second"));
  const apply: ConsolidationDecaySelector["apply"] = vi.fn(async (_scope, ref, _sessions, _project, effect, _operation, update) => {
    const receipt = receipts.get(effect);
    if (receipt) return { ...receipt, applied: false };
    const current = rows.get(ref.id);
    if (!current) throw new CanonicalMemoryConflict();
    rows.set(ref.id, update(current) as SemanticMemory);
    const result: CanonicalWriteResult = { id: ref.id, rowVersion: "2", applied: true, created: false,
      receipt: { operationKind: "decay", id: ref.id, rowVersion: "2", effectKey: effect, operationFingerprint: "a".repeat(64), resultFingerprint: "b".repeat(64), created: false } };
    receipts.set(effect, result);
    return result;
  });
  const selector: ConsolidationDecaySelector = Object.assign(select, { apply });
  for (const id of ["a", "b"]) rows.set(id, { id, fact: id, confidence: 1, sourceSessionIds: ["s"], sourceMemoryIds: [],
    accessCount: 1, strength: 1, createdAt: "2026-01-01T00:00:00Z", updatedAt: "2026-01-01T00:00:00Z", lastAccessedAt: "2026-01-01T00:00:00Z" });
  return { kv, state, rows, receipts, selector, select, apply, page };
}
const input = { project: "P", sessionIds: ["s"], sourceVersion: "summary-v1" };
describe("selected consolidation decay", () => {
  it("runs again in a later decay period while same-period replay does not change strength", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      vi.setSystemTime(new Date("2026-01-10T00:00:00Z"));
      const h = harness();
      const first = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
      await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
      await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
      expect(await runSelectedConsolidationDecay(h.kv, h.selector, 30, input)).toMatchObject({ completed: true, manifestId: first.manifestId });
      expect(h.receipts.size).toBe(2);
      expect(h.rows.get("a")!.strength).toBe(1);
      vi.setSystemTime(new Date("2026-03-20T00:00:00Z"));
      const later = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
      expect(later.manifestId).not.toBe(first.manifestId);
      expect(h.rows.get("a")!.strength).toBeCloseTo(0.81);
      const strength = h.rows.get("a")!.strength;
      await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
      expect(h.rows.get("a")!.strength).toBe(strength);
      expect(h.receipts.size).toBe(4);
    } finally { vi.useRealTimers(); }
  });
  it("explains zero work when decay is disabled and keeps its identity stable", async () => {
    const h = harness();
    const first = await runSelectedConsolidationDecay(h.kv, h.selector, 0, input);
    expect(first).toMatchObject({ completed: true, processed: 0, reason: "decay_disabled" });
    expect(await runSelectedConsolidationDecay(h.kv, h.selector, 0, input)).toEqual(first);
    expect(h.select).not.toHaveBeenCalled();
    expect(h.apply).not.toHaveBeenCalled();
  });
  it("does not decay or move timestamps backward when a captured cut resumes after a newer update", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      vi.setSystemTime(new Date("2026-02-15T00:00:00Z"));
      const h = harness();
      h.selector.apply = vi.fn().mockRejectedValueOnce(new CanonicalMemoryPending("prepare"));
      const first = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
      const current = { ...h.rows.get("a")!, strength: 0.7, updatedAt: "2026-03-01T00:00:00Z", lastDecayAt: "2026-01-01T00:00:00Z" };
      h.rows.set("a", current);
      vi.setSystemTime(new Date("2026-04-01T00:00:00Z"));
      h.selector.apply = h.apply;
      await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
      expect(h.rows.get("a")).toEqual(current);
      expect(h.receipts.size).toBe(1);
    } finally { vi.useRealTimers(); }
  });
  it("processes one bounded page per invocation, preserving source versions and both scopes", async () => {
    const h = harness();
    const first = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
    expect(first).toMatchObject({ completed: false, processed: 1, selectionCoverage: "selected_source_sessions" });
    const id = String(first.manifestId);
    expect(h.select).toHaveBeenCalledWith("semantic", ["s"], null, "P");
    expect(h.apply).toHaveBeenCalledWith("semantic", { id: "a", rowVersion: "1" }, ["s"], "P", expect.any(String),
      expect.objectContaining({ project: "P", sessionIds: ["s"], decayDays: 30 }), expect.any(Function));
    const strength = h.rows.get("a")!.strength;
    expect(strength).toBeLessThan(1);
    const second = await runSelectedConsolidationDecay(h.kv, h.selector, 999, { manifestId: id });
    expect(second).toMatchObject({ completed: false, processed: 2 });
    expect(h.select).toHaveBeenLastCalledWith("semantic", ["s"], "second", "P");
    const final = await runSelectedConsolidationDecay(h.kv, h.selector, 999, { manifestId: id });
    expect(final).toMatchObject({ completed: true, processed: 2 });
    await runSelectedConsolidationDecay(h.kv, h.selector, 999, { manifestId: id });
    expect(h.apply).toHaveBeenCalledTimes(2);
    expect(h.rows.get("a")!.strength).toBe(strength);
    expect(h.receipts.size).toBe(2);
  });
  it("recovers an acknowledged effect after checkpoint write interruption and later deletion", async () => {
    const h = harness();
    const originalSet = h.kv.set.bind(h.kv);
    let interrupted = false;
    h.kv.set = vi.fn(async (scope, id, value) => {
      const pending = (value as { pending?: { done: string[] } }).pending;
      if (!interrupted && pending?.done.length) { interrupted = true; throw new Error("simulated checkpoint interruption"); }
      return originalSet(scope, id, value);
    });
    await expect(runSelectedConsolidationDecay(h.kv, h.selector, 30, input)).rejects.toThrow("checkpoint interruption");
    expect(h.receipts.size).toBe(1);
    const manifestId = [...h.state.keys()][0];
    h.rows.delete("a");
    const resumed = await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId });
    expect(resumed).toMatchObject({ completed: false, processed: 1 });
    expect(h.rows.has("a")).toBe(false);
    expect(h.receipts.size).toBe(1);
    expect(h.select).toHaveBeenCalledTimes(1);
  });
  it("resumes a checkpoint with the full supported opaque cursor budget", async () => {
    const h = harness();
    const cursor = "x".repeat(4096);
    h.select.mockResolvedValueOnce(h.page(["a"], cursor));
    h.selector.apply = vi.fn().mockRejectedValueOnce(new CanonicalMemoryPending("prepare"));
    const first = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
    h.selector.apply = h.apply;
    expect(await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) }))
      .toMatchObject({ completed: false, processed: 1 });
    await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
    expect(h.select).toHaveBeenLastCalledWith("semantic", ["s"], cursor, "P");
  });
  it("reselects exact versioned references after an exhausted atomic conflict", async () => {
    const h = harness();
    const atomicApply = h.selector.apply;
    h.selector.apply = vi.fn().mockRejectedValueOnce(new CanonicalMemoryConflict()).mockImplementation(atomicApply);
    const first = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
    expect(first).toMatchObject({ completed: false, queued: true, reason: "canonical_state_conflict", processed: 0 });
    expect(h.receipts.size).toBe(0);
    const resumed = await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
    expect(resumed.processed).toBe(1);
    expect(h.select).toHaveBeenCalledTimes(2);
    expect(h.receipts.size).toBe(1);
  });
  it("restarts stale membership cursor while preserving exactly-once effects", async () => {
    const h = harness();
    const first = await runSelectedConsolidationDecay(h.kv, h.selector, 30, input);
    h.select.mockRejectedValueOnce(new CanonicalMemoryConflict());
    const conflicted = await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
    expect(conflicted).toMatchObject({ completed: false, reason: "canonical_state_conflict", processed: 0 });
    const repeated = await runSelectedConsolidationDecay(h.kv, h.selector, 30, { manifestId: String(first.manifestId) });
    expect(repeated.processed).toBe(1);
    expect(h.receipts.size).toBe(1);
    expect(h.select).toHaveBeenLastCalledWith("semantic", ["s"], null, "P");
  });
  it("does not turn preparation or malformed pages into completed work", async () => {
    const h = harness();
    h.select.mockRejectedValueOnce(new CanonicalMemoryPending("continue"));
    expect(await runSelectedConsolidationDecay(h.kv, h.selector, 30, input)).toMatchObject({ completed: false, reason: "canonical_index_preparing" });
    h.select.mockResolvedValueOnce(h.page(["a", "a"]));
    await expect(runSelectedConsolidationDecay(h.kv, h.selector, 30, input)).rejects.toThrow("consolidation_decay_page_invalid");
    expect(h.receipts.size).toBe(0);
  });
  it("rejects unscoped sources before reading or writing effects", async () => {
    const h = harness();
    await expect(runSelectedConsolidationDecay(h.kv, h.selector, 30, { ...input, sessionIds: Array.from({ length: 13 }, (_, n) => String(n)) })).rejects.toThrow("sources_invalid");
    expect(h.select).not.toHaveBeenCalled();
    expect(h.apply).not.toHaveBeenCalled();
  });
});
