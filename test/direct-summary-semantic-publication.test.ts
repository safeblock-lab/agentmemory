import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { registerSummarizeFunction } from "../src/functions/summarize.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, Session } from "../src/types.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
vi.mock("../src/functions/dashboard-activity.js", () => ({ recordOperation: vi.fn() }));

export async function harness(count = 1) {
  const rows = new Map<string, Map<string, unknown>>();
  const kv = {
    get: async <T>(scope: string, key: string) => rows.get(scope)?.get(key) as T ?? null,
    set: vi.fn(async (scope: string, key: string, value: unknown) => {
      if (!rows.has(scope)) rows.set(scope, new Map());
      rows.get(scope)!.set(key, structuredClone(value));
    }),
    getVersioned: async <T>(scope: string, key: string) => ({ exists: rows.get(scope)?.has(key) ?? false,
      value: rows.get(scope)?.get(key) as T ?? null, version: JSON.stringify(rows.get(scope)?.get(key)) }),
    list: async <T>(scope: string) => [...(rows.get(scope)?.values() ?? [])] as T[],
    pages: async function* <T>(scope: string, options: { limit: number }) {
      const values = [...(rows.get(scope)?.values() ?? [])] as T[];
      for (let start = 0; start < values.length; start += options.limit)
        yield { items: values.slice(start, start + options.limit), next_cursor: start + options.limit < values.length ? "more" : null };
    },
  };
  const session: Session = { id: "s", project: "project", cwd: "/tmp", startedAt: "2026-10-11", status: "active", observationCount: count };
  await kv.set(KV.sessions, "s", session);
  for (let i = 0; i < count; i++) await kv.set(KV.observations("s"), `o${i}`, {
    id: `o${i}`, sessionId: "s", timestamp: "2026-10-11", type: "tool_use", title: `Read source ${i}`,
    narrative: "Opened src/a.ts to inspect existing behavior. No changes were made.", facts: ["Inspected existing behavior"],
    files: ["src/a.ts"], concepts: [],
  } as CompressedObservation);
  let handler: (data: unknown) => Promise<any>;
  const sdk = { registerFunction: (_id: string, fn: typeof handler) => { handler = fn; },
    trigger: vi.fn(async () => ({ success: true, queued: true, jobId: "queue-job", continuation: { jobId: "queue-job" } })) };
  let verdict = "supported";
  let generated = "<summary><title>Inspection</title><narrative>The existing source behavior was inspected without making changes.</narrative><decisions></decisions><files></files><concepts></concepts></summary>";
  const provider = { name: "test", compress: vi.fn(),
    prepareRequest: (_operation: string, system: string, prompt: string, options: unknown) => ({
      endpoint: "POST https://provider.example/v1/chat/completions", body: JSON.stringify({ system, prompt, options }) }),
    summarize: vi.fn(async (system: string, prompt: string) => {
      if (system.startsWith("You evaluate publication evidence")) {
        const requestId = system.match(/requestId must be ([a-zA-Z0-9_:-]+)/)?.[1];
        return verdict === "invalid" ? "bad json" : JSON.stringify({ ...(requestId && { requestId }), verdict,
          reason: "Controlled semantic fixture" });
      }
      return generated;
    }),
  };
  registerSummarizeFunction(sdk as any, kv as any, provider as any);
  kv.set.mockClear();
  return { kv, rows, sdk, provider, run: (data: unknown = { sessionId: "s" }) => handler!(data),
    verdict: (value: string) => { verdict = value; }, raw: (value: string) => { generated = value; } };
}

import { recordOperation } from "../src/functions/dashboard-activity.js";
import { summarySourceFingerprint } from "../src/functions/summary-queue.js";

describe("direct semantic publication durability", () => {
  const original = { ...process.env };
  beforeEach(() => { process.env.AGENTMEMORY_SUMMARY_CONTEXT_TOKENS = "1000000";
    process.env.AGENTMEMORY_SUMMARY_OUTPUT_TOKENS = "8192"; process.env.AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS = "4096";
    delete process.env.SUMMARIZE_CHUNK_SIZE; });
  afterEach(() => { process.env = { ...original }; });
  it.each(["unsupported", "uncertain", "invalid"])("retains rejected candidate/raw for %s without regeneration", async verdict => {
    const h = await harness();
    h.raw("<summary><title>Changed</title><narrative>Changed src/a.ts because its defect caused the crash.</narrative><decisions></decisions><files><file>src/a.ts</file></files><concepts></concepts></summary>");
    h.verdict(verdict);
    const expected = verdict === "invalid" ? "invalid_response" : verdict;
    expect(await h.run()).toMatchObject({ success: false, error: `summary_publication_${expected}` });
    expect(await h.run()).toMatchObject({ success: false, error: `summary_publication_${expected}` });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(await h.kv.get(KV.summaries, "s")).toBeNull();
    expect(await h.kv.get(KV.state, "direct-summary:s")).toMatchObject({ raw: expect.any(String), summary: { filesModified: ["src/a.ts"] } });
  });
  it("supported paraphrase publishes and exact completed replay has zero calls/writes", async () => {
    const h = await harness();
    const result = await h.run();
    expect(result.success).toBe(true);
    h.provider.summarize.mockClear(); h.kv.set.mockClear();
    expect(await h.run()).toEqual(result);
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.set).not.toHaveBeenCalled();
  });
  it.each(["source", "project"])("rechecks %s after supported judge before effects", async changed => {
    const h = await harness();
    const invoke = h.provider.summarize.getMockImplementation()!;
    h.provider.summarize.mockImplementation(async (system, prompt) => {
      const raw = await invoke(system, prompt);
      if (system.startsWith("You evaluate")) {
        const scope = changed === "source" ? KV.observations("s") : KV.sessions;
        const key = changed === "source" ? "o0" : "s";
        const row = await h.kv.get<Record<string, unknown>>(scope, key);
        await h.kv.set(scope, key, { ...row, ...(changed === "source" ? { narrative: "Changed after judge" } : { project: "other" }) });
      }
      return raw;
    });
    expect(await h.run()).toMatchObject({ success: false, error: "summary_source_changed" });
    expect(await h.kv.get(KV.summaries, "s")).toBeNull();
  });
  it.each(["change", "delete"])("lost ACK then source %s recovers exact applied canonical with no new calls/write", async action => {
    const h = await harness();
    const write = h.kv.set.getMockImplementation()!;
    let cut = true;
    h.kv.set.mockImplementation(async (scope, key, value: any) => {
      if (cut && scope === KV.state && key === "direct-summary:s" && value.completed) { cut = false; throw new Error("cut before ACK"); }
      return write(scope, key, value);
    });
    expect(await h.run()).toMatchObject({ success: false, error: "cut before ACK" });
    const canonical = await h.kv.get(KV.summaries, "s");
    if (action === "delete") h.rows.get(KV.observations("s"))!.delete("o0");
    else await h.kv.set(KV.observations("s"), "o0", { id: "o0", title: "Changed", narrative: "different" });
    h.provider.summarize.mockClear(); h.kv.set.mockClear();
    expect(await h.run()).toMatchObject({ success: true, summary: canonical });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.set.mock.calls.filter(([scope]) => scope === KV.summaries)).toHaveLength(0);
  });
  it("replays captured raw after interruption before parsed candidate", async () => {
    const h = await harness(); const write = h.kv.set.getMockImplementation()!; let cut = true;
    h.kv.set.mockImplementation(async (scope, key, value: any) => {
      if (cut && scope === KV.state && value.summary && !value.effect) { cut = false; throw new Error("parse persistence interrupted"); }
      return write(scope, key, value);
    });
    expect(await h.run()).toMatchObject({ success: false });
    expect(await h.run()).toMatchObject({ success: true });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("prepared effect alone cannot recover missing canonical with changed source on exact continuation", async () => {
    const h = await harness(); const write = h.kv.set.getMockImplementation()!;
    h.kv.set.mockImplementation(async (scope, key, value) => {
      if (scope === KV.summaries) throw new Error("before canonical"); return write(scope, key, value);
    });
    expect(await h.run()).toMatchObject({ success: false });
    await write(KV.observations("s"), "o0", { ...await h.kv.get(KV.observations("s"), "o0") as object, narrative: "changed" });
    h.provider.summarize.mockClear();
    const retained = await h.kv.get<any>(KV.state, "direct-summary:s");
    expect(await h.run({ sessionId: "s", selected: { project: "project", sessionId: "s",
      sourceFingerprint: retained.sourceFingerprint } })).toMatchObject({ success: false, error: "selection_source_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("rejected direct candidate preserves historical canonical bytes", async () => {
    const h = await harness(); const legacy = { id: "historical", narrative: "Legacy stored bytes", extra: ["keep"] };
    await h.kv.set(KV.summaries, "s", legacy); h.verdict("unsupported");
    expect(await h.run()).toMatchObject({ success: false, error: "summary_publication_unsupported" });
    expect(await h.kv.get(KV.summaries, "s")).toEqual(legacy);
  });
  it("ambiguous dispatch intention never automatically calls provider again", async () => {
    const h = await harness(); h.provider.summarize.mockRejectedValueOnce(new Error("transport unknown"));
    expect(await h.run()).toMatchObject({ success: false });
    expect(await h.run()).toMatchObject({ error: "summary_dispatch_outcome_unknown" });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
  });
  it("oversized raw is bounded and replay refuses regeneration", async () => {
    const h = await harness(); h.raw("x".repeat(65537));
    expect(await h.run()).toMatchObject({ error: "summary_response_limit_exceeded" });
    expect(await h.run()).toMatchObject({ error: "summary_response_limit_exceeded" });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    const checkpoint = await h.kv.get<Record<string, unknown>>(KV.state, "direct-summary:s");
    expect(checkpoint?.overflow).toBe(true); expect(checkpoint?.raw).toBeUndefined();
  });
  it("applied effect recovery still requires exact selected identity", async () => {
    const h = await harness(); const write = h.kv.set.getMockImplementation()!;
    h.kv.set.mockImplementation(async (scope, key, value: any) => {
      if (value.completed) throw new Error("ACK cut"); return write(scope, key, value);
    });
    expect(await h.run()).toMatchObject({ success: false });
    h.provider.summarize.mockClear();
    expect(await h.run({ sessionId: "s", selected: { project: "project", sessionId: "s", sourceFingerprint: "wrong" } }))
      .toMatchObject({ success: false, error: "selection_source_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });  it("retains early/failure telemetry and avoids duplicate failed replay activity", async () => {
    const h = await harness(0); vi.mocked(recordOperation).mockClear();
    await h.run({}); await h.run({ sessionId: "missing" }); await h.run();
    expect(vi.mocked(recordOperation).mock.calls.map(call => [call[2], call[5]?.errorCode]))
      .toEqual([["failed", "invalid_input"], ["failed", "session_not_found"], ["skipped", "no_observations"]]);
    const rejected = await harness(); rejected.verdict("unsupported"); vi.mocked(recordOperation).mockClear();
    await rejected.run(); await rejected.run();
    expect(vi.mocked(recordOperation).mock.calls.filter(call => call[2] === "failed")).toHaveLength(1);
    expect(vi.mocked(recordOperation).mock.calls[0][5]).toMatchObject({ durationMs: expect.any(Number), errorCode: "summary_failed" });
  });  it("archives rejected A and processes new normal snapshot B without losing A captures/verdict", async () => {
    const h = await harness(); h.verdict("unsupported");
    const fingerprintA = await summarySourceFingerprint(h.kv as any, "s", "project");
    expect(await h.run()).toMatchObject({ error: "summary_publication_unsupported" });
    const attemptA = await h.kv.get<any>(KV.state, "direct-summary:s");
    const verdictEntries = [...h.rows.get(KV.state)!.entries()].filter(([, value]: any) => value?.decision?.status === "unsupported");
    h.provider.summarize.mockClear();
    expect(await h.run()).toMatchObject({ error: "summary_publication_unsupported" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    const original = await h.kv.get<any>(KV.observations("s"), "o0");
    await h.kv.set(KV.observations("s"), "o0", { ...original, narrative: "New inspection evidence B." });
    expect(await h.run({ sessionId: "s", selected: { project: "project", sessionId: "s", sourceFingerprint: fingerprintA } }))
      .toMatchObject({ error: "selection_source_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    h.verdict("supported");
    expect(await h.run()).toMatchObject({ success: true });
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
    expect(await h.kv.get(KV.state, `${attemptA.id}:direct-attempt`)).toEqual(attemptA);
    for (const [key, value] of verdictEntries) expect(await h.kv.get(KV.state, key)).toEqual(value);
    // Returning to captured A also reuses its rejected verdict instead of regenerating it.
    await h.kv.set(KV.observations("s"), "o0", original); h.provider.summarize.mockClear();
    expect(await h.run()).toMatchObject({ error: "summary_publication_unsupported" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });
  it("new normal snapshot cannot abandon an unknown dispatch without raw", async () => {
    const h = await harness(); h.provider.summarize.mockRejectedValueOnce(new Error("unknown dispatch"));
    await h.run();
    const source = await h.kv.get<any>(KV.observations("s"), "o0");
    await h.kv.set(KV.observations("s"), "o0", { ...source, narrative: "New B" });
    h.provider.summarize.mockClear();
    expect(await h.run()).toMatchObject({ error: "summary_source_changed" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });  it("normal new snapshot can re-enqueue after delegated queue admission", async () => {
    const h = await harness(129);
    expect(await h.run()).toMatchObject({ queued: true });
    const previous = await h.kv.get<any>(KV.state, "direct-summary:s");
    expect(previous.intended).toBeUndefined();
    const source = await h.kv.get<any>(KV.observations("s"), "o0");
    await h.kv.set(KV.observations("s"), "o0", { ...source, narrative: "New queue snapshot" });
    expect(await h.run()).toMatchObject({ queued: true, completed: false });
    expect(h.sdk.trigger).toHaveBeenCalledTimes(2);
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(await h.kv.get(KV.state, `${previous.id}:direct-attempt`)).toEqual(previous);
  });
  it("definitively captured overflow can be archived for a new normal snapshot", async () => {
    const h = await harness(); h.raw("x".repeat(65537));
    expect(await h.run()).toMatchObject({ error: "summary_response_limit_exceeded" });
    const source = await h.kv.get<any>(KV.observations("s"), "o0");
    await h.kv.set(KV.observations("s"), "o0", { ...source, narrative: "New snapshot after overflow" });
    h.raw("<summary><title>Inspection</title><narrative>A new source observation was inspected without changes.</narrative><decisions></decisions><files></files><concepts></concepts></summary>");
    expect(await h.run()).toMatchObject({ success: true });
    expect(h.provider.summarize).toHaveBeenCalledTimes(3);
  });});
