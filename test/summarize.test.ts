import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

vi.mock("../src/state/schema.js", () => ({
  KV: {
    sessions: "sessions",
    summaries: "summaries",
    observations: (sessionId: string) => `obs:${sessionId}`,
    audit: "audit",
  },
}));

vi.mock("../src/eval/schemas.js", () => ({
  SummaryOutputSchema: {},
}));

vi.mock("../src/eval/validator.js", () => ({
  validateOutput: () => ({ valid: true, result: { errors: [] } }),
}));

vi.mock("../src/eval/quality.js", () => ({
  scoreSummary: () => 100,
}));

vi.mock("../src/functions/audit.js", () => ({
  safeAudit: vi.fn(),
}));

import { registerSummarizeFunction } from "../src/functions/summarize.js";
import { estimateSummaryTokens } from "../src/functions/summary-budget.js";
import type {
  CompressedObservation,
  Session,
  MemoryProvider,
} from "../src/types.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    store,
    get: async <T>(scope: string, key: string): Promise<T | null> =>
      (store.get(scope)?.get(key) as T) ?? null,
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
  };
}

function mockSdk() {
  const functions = new Map<string, Function>();
  return {
    functions,
    registerFunction: (id: string, handler: Function) => {
      functions.set(id, handler);
    },
    registerTrigger: () => {},
    trigger: async () => ({}),
  };
}

function makeObs(i: number, sessionId: string): CompressedObservation {
  return {
    id: `obs_${i}`,
    sessionId,
    timestamp: new Date().toISOString(),
    type: "conversation",
    title: `obs ${i}`,
    facts: [`fact ${i}`],
    narrative: `narrative for obs ${i}`,
    concepts: [],
    files: [`src/file_${i}.ts`],
    importance: 5,
  };
}

function makeProvider(responses: string[]): MemoryProvider & {
  calls: Array<{ system: string; user: string }>;
} {
  const calls: Array<{ system: string; user: string }> = [];
  let i = 0;
  return {
    name: "test",
    calls,
    compress: async () => "",
    summarize: async (system: string, user: string) => {
      calls.push({ system, user });
      const r = responses[i] ?? responses[responses.length - 1];
      i += 1;
      return r;
    },
  };
}

function summaryXml(opts: {
  title: string;
  narrative?: string;
  decisions?: string[];
  files?: string[];
  concepts?: string[];
}): string {
  const d = (opts.decisions ?? []).map((x) => `<decision>${x}</decision>`).join("");
  const f = (opts.files ?? []).map((x) => `<file>${x}</file>`).join("");
  const c = (opts.concepts ?? []).map((x) => `<concept>${x}</concept>`).join("");
  return `<summary>
<title>${opts.title}</title>
<narrative>${opts.narrative ?? "narrative"}</narrative>
<decisions>${d}</decisions>
<files>${f}</files>
<concepts>${c}</concepts>
</summary>`;
}

function mapCalls(calls: Array<{ system: string; user: string }>) {
  return calls.filter((call) => call.system.includes("session summarizer"));
}

function observationRange(prompt: string): [number, number] {
  const indexes = [...prompt.matchAll(/^\[(\d+)\]\s/gm)].map((match) => Number(match[1]));
  if (!indexes.length) throw new Error("map prompt contains no observation indexes");
  return [Math.min(...indexes), Math.max(...indexes)];
}

function reducedRanges(prompt: string): Array<[number, number]> {
  return [...prompt.matchAll(/obs (\d+)-(\d+)/g)]
    .map((match) => [Number(match[1]), Number(match[2])]);
}

async function setupHandler(opts: {
  sessionId: string;
  obsCount: number;
  provider: MemoryProvider;
}) {
  const sdk = mockSdk();
  const kv = mockKV();
  const session: Session = {
    id: opts.sessionId,
    project: "test-project",
    cwd: "/tmp",
    startedAt: new Date().toISOString(),
    status: "completed",
    observationCount: opts.obsCount,
  };
  await kv.set("sessions", opts.sessionId, session);
  for (let i = 0; i < opts.obsCount; i++) {
    const o = makeObs(i, opts.sessionId);
    await kv.set(`obs:${opts.sessionId}`, o.id, o);
  }
  registerSummarizeFunction(sdk as any, kv as any, opts.provider);
  const handler = sdk.functions.get("mem::summarize")!;
  return { handler, kv };
}

describe("mem::summarize chunking", () => {
  const ORIGINAL_ENV = { ...process.env };

  beforeEach(() => {
    // Keep chunking tests independent of the developer's ~/.agentmemory/.env.
    process.env.AGENTMEMORY_SUMMARY_CONTEXT_TOKENS = "1000000";
    process.env.AGENTMEMORY_SUMMARY_OUTPUT_TOKENS = "8192";
    process.env.AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS = "4096";
    delete process.env.SUMMARIZE_CHUNK_SIZE;
    delete process.env.SUMMARIZE_CHUNK_CONCURRENCY;
  });

  it("keeps original observationCount after splitting one giant Unicode observation", async () => {
    process.env.AGENTMEMORY_SUMMARY_CONTEXT_TOKENS = "4096";
    process.env.AGENTMEMORY_SUMMARY_OUTPUT_TOKENS = "512";
    process.env.AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS = "256";
    const provider = makeProvider([summaryXml({ title: "giant" })]);
    const { handler, kv } = await setupHandler({ sessionId: "giant", obsCount: 1, provider });
    await kv.set("obs:giant", "obs_0", { ...makeObs(0, "giant"), narrative: "🧠漢字".repeat(2000) });
    const result = await handler({ sessionId: "giant" });
    expect(result.success).toBe(true);
    expect(provider.calls.length).toBeGreaterThan(2);
    expect(await kv.get("summaries", "giant")).toMatchObject({ observationCount: 1 });
  });

  it("does not persist a summary when the reserve leaves no prompt capacity", async () => {
    process.env.AGENTMEMORY_SUMMARY_CONTEXT_TOKENS = "5000";
    const provider = makeProvider([summaryXml({ title: "invalid budget" })]);
    const { handler, kv } = await setupHandler({ sessionId: "invalid-budget", obsCount: 1, provider });
    const result = await handler({ sessionId: "invalid-budget" });
    expect(result.success).toBe(false);
    expect(result.error).toContain("invalid_summary_budget");
    expect(provider.calls).toHaveLength(0);
    expect(await kv.get("summaries", "invalid-budget")).toBeNull();
  });

  it("empty sessions make no provider calls", async () => {
    const provider = makeProvider([summaryXml({ title: "empty" })]);
    const { handler, kv } = await setupHandler({ sessionId: "empty", obsCount: 0, provider });
    expect(await handler({ sessionId: "empty" })).toMatchObject({ success: false, error: "no_observations" });
    expect(provider.calls).toHaveLength(0);
    expect(await kv.get("summaries", "empty")).toBeNull();
  });

  it("does not persist a reducer result when reduction makes no progress", async () => {
    process.env.AGENTMEMORY_SUMMARY_CONTEXT_TOKENS = "4096";
    process.env.AGENTMEMORY_SUMMARY_OUTPUT_TOKENS = "512";
    process.env.AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS = "256";
    process.env.SUMMARIZE_CHUNK_SIZE = "1";
    const provider = makeProvider([summaryXml({ title: "no progress", narrative: "x".repeat(1400) })]);
    const { handler, kv } = await setupHandler({ sessionId: "no-progress", obsCount: 4, provider });
    expect(await handler({ sessionId: "no-progress" })).toMatchObject({ success: false, error: "summary_reduce_no_progress" });
    expect(await kv.get("summaries", "no-progress")).toBeNull();
  });

  afterEach(() => {
    process.env = { ...ORIGINAL_ENV };
  });

  it("small session takes the single-call path (no chunking, no reduce)", async () => {
    const provider = makeProvider([
      summaryXml({
        title: "Small session",
        decisions: ["decision A"],
        files: ["src/a.ts"],
        concepts: ["concept-a"],
      }),
    ]);
    const { handler, kv } = await setupHandler({
      sessionId: "ses_small",
      obsCount: 10,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_small" });

    expect(result.success).toBe(true);
    expect(provider.calls).toHaveLength(1);
    expect(provider.calls[0].user).toContain("Session observations (10 total)");
    const stored: any = await kv.get("summaries", "ses_small");
    expect(stored?.title).toBe("Small session");
  });

  it("large session map-reduces within the per-call ceiling", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1"; // serial keeps call ordering deterministic
    const provider = makeProvider([
      summaryXml({ title: "Chunk 1", decisions: ["dA"], files: ["src/a.ts"], concepts: ["ca"] }),
      summaryXml({ title: "Chunk 2", decisions: ["dB"], files: ["src/b.ts"], concepts: ["cb"] }),
      summaryXml({ title: "Chunk 3", decisions: ["dC"], files: ["src/c.ts"], concepts: ["cc"] }),
      summaryXml({ title: "Chunk 4", decisions: ["dD"], files: ["src/d.ts"], concepts: ["cd"] }),
      summaryXml({
        title: "Merged",
        decisions: ["dA", "dB", "dC", "dD"],
        files: ["src/a.ts", "src/b.ts", "src/c.ts", "src/d.ts"],
        concepts: ["ca", "cb", "cc", "cd"],
      }),
    ]);
    const { handler, kv } = await setupHandler({
      sessionId: "ses_large",
      obsCount: 250,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_large" });

    expect(result.success).toBe(true);
    const chunks = mapCalls(provider.calls);
    expect(chunks.length).toBeGreaterThanOrEqual(4);
    expect(provider.calls).toHaveLength(chunks.length + 1);
    expect(provider.calls.every(call => estimateSummaryTokens(call.system, call.user) <= 7500)).toBe(true);
    expect(provider.calls[3].system).toContain("session summarizer");
    const reduceCall = provider.calls.at(-1)!;
    expect(reduceCall.system).toContain("merging multiple partial summaries");
    expect(reduceCall.user).toContain(`Chunk 1 of ${chunks.length}`);
    expect(reduceCall.user).toContain(`Chunk ${chunks.length} of ${chunks.length}`);

    const stored: any = await kv.get("summaries", "ses_large");
    expect(stored?.title).toBe("Merged");
    // observationCount on the persisted summary should reflect the full session,
    // not just the final chunk.
    expect(stored?.observationCount).toBe(250);
    expect(stored?.keyDecisions).toEqual(["dA", "dB", "dC", "dD"]);
  });

  it("SUMMARIZE_CHUNK_SIZE env override is respected", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "50";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1";
    const provider = makeProvider([
      summaryXml({ title: "chunk" }),
      summaryXml({ title: "chunk" }),
      summaryXml({ title: "chunk" }),
      summaryXml({ title: "chunk" }),
      summaryXml({ title: "merged" }),
    ]);
    const { handler } = await setupHandler({
      sessionId: "ses_env",
      obsCount: 175,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_env" });

    expect(result.success).toBe(true);
    // 175 obs ÷ 50 = 4 chunks (last chunk has 25) + 1 reduce = 5 calls.
    const chunks = mapCalls(provider.calls);
    expect(chunks.length).toBeGreaterThanOrEqual(4);
    expect(provider.calls).toHaveLength(chunks.length + 1);
    const counts = chunks.map((call) => Number(call.user.match(/^Session observations \((\d+) total\)/)?.[1]));
    expect(counts.every((count) => count > 0 && count <= 50)).toBe(true);
    expect(counts.reduce((total, count) => total + count, 0)).toBe(175);
  });

  it("flaky chunk: parse fails once, retried, then succeeds — no skip", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1";
    const provider = makeProvider([
      summaryXml({ title: "ok1" }),
      "<garbage/>",                  // chunk 2 attempt 1: parse-fail
      summaryXml({ title: "ok2" }),  // chunk 2 attempt 2 (retry): success
      summaryXml({ title: "ok3" }),
      summaryXml({ title: "merged" }),
    ]);
    const { handler, kv } = await setupHandler({
      sessionId: "ses_flaky",
      obsCount: 250,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_flaky" });

    expect(result.success).toBe(true);
    const chunks = mapCalls(provider.calls);
    const uniqueChunkPrompts = new Set(chunks.map((call) => call.user));
    expect(chunks.length).toBe(uniqueChunkPrompts.size + 1);
    expect(provider.calls.at(-1)?.system).toContain("merging multiple partial summaries");
    const stored: any = await kv.get("summaries", "ses_flaky");
    expect(stored?.title).toBe("merged");
  });

  it("persistently-broken chunk is skipped, reduce still runs on remaining partials", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1";
    const provider = makeProvider([
      summaryXml({ title: "ok1" }),
      "<garbage/>", "<garbage/>",   // chunk 2: both attempts parse-fail
      summaryXml({ title: "ok3" }),
      summaryXml({ title: "merged-with-skip" }),
    ]);
    const { handler, kv } = await setupHandler({
      sessionId: "ses_skip",
      obsCount: 250,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_skip" });

    expect(result.success).toBe(true);
    const chunks = mapCalls(provider.calls);
    const uniquePrompts = [...new Set(chunks.map((call) => call.user))];
    expect(chunks.length).toBe(uniquePrompts.length + 1);
    const reduceCall = provider.calls.at(-1)!;
    expect(reduceCall.system).toContain("merging multiple partial summaries");
    const expectedRanges = uniquePrompts
      .filter((_, index) => index !== 1)
      .map(observationRange);
    expect(reducedRanges(reduceCall.user)).toEqual(expectedRanges);
    const stored: any = await kv.get("summaries", "ses_skip");
    expect(stored?.title).toBe("merged-with-skip");
  });

  it("too many skipped chunks bails out with a clear error", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1";
    // 3 chunks, 2 fully broken → >50% skipped → bail.
    const provider = makeProvider([
      summaryXml({ title: "ok1" }),
      "<garbage/>", "<garbage/>",
      "<garbage/>", "<garbage/>",
    ]);
    const { handler } = await setupHandler({
      sessionId: "ses_too_broken",
      obsCount: 250,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_too_broken" });

    expect(result.success).toBe(false);
    const skipped = result.error?.match(/too_many_chunks_skipped: (\d+)\/(\d+)/);
    expect(skipped).not.toBeNull();
    expect(Number(skipped?.[1])).toBeGreaterThan(Number(skipped?.[2]) / 2);
  });

  it("provider error on one chunk after retry is skipped, not propagated", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1";
    let i = 0;
    const provider: MemoryProvider & { calls: any[] } = {
      name: "test",
      calls: [],
      compress: async () => "",
      summarize: async (system: string, user: string) => {
        (provider as any).calls.push({ system, user });
        i += 1;
        if (i === 1) return summaryXml({ title: "ok1" });
        // chunk 2: both attempts throw (e.g. provider 400)
        if (i === 2 || i === 3) throw new Error("OpenAI API error (400): content rejected");
        if (i === 4) return summaryXml({ title: "ok3" });
        return summaryXml({ title: "merged-with-skip" });
      },
    };
    const { handler, kv } = await setupHandler({
      sessionId: "ses_net",
      obsCount: 250,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_net" });

    expect(result.success).toBe(true);
    const chunks = mapCalls((provider as any).calls);
    expect(chunks.length).toBe(new Set(chunks.map((call) => call.user)).size + 1);
    expect((provider as any).calls.at(-1).system).toContain("merging multiple partial summaries");
    const stored: any = await kv.get("summaries", "ses_net");
    expect(stored?.title).toBe("merged-with-skip");
  });

  it("every chunk failing on provider error trips too_many_chunks_skipped", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "1";
    // 3 chunks, all chunk calls throw → 3/3 skipped → bail.
    const provider: MemoryProvider & { calls: any[] } = {
      name: "test",
      calls: [],
      compress: async () => "",
      summarize: async (system: string, user: string) => {
        (provider as any).calls.push({ system, user });
        throw new Error("OpenAI API error (400): invalid request");
      },
    };
    const { handler } = await setupHandler({
      sessionId: "ses_all_400",
      obsCount: 250,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_all_400" });

    expect(result.success).toBe(false);
    const skipped = result.error?.match(/too_many_chunks_skipped: (\d+)\/(\d+)/);
    expect(skipped).not.toBeNull();
    expect(skipped?.[1]).toBe(skipped?.[2]);
  });

  it("chunks run in parallel batches according to SUMMARIZE_CHUNK_CONCURRENCY", async () => {
    process.env.SUMMARIZE_CHUNK_SIZE = "100";
    process.env.SUMMARIZE_CHUNK_CONCURRENCY = "2";
    let inflight = 0;
    let maxInflight = 0;
    const provider: MemoryProvider & { calls: any[] } = {
      name: "test",
      calls: [],
      compress: async () => "",
      summarize: async (system: string, user: string) => {
        (provider as any).calls.push({ system, user });
        inflight += 1;
        maxInflight = Math.max(maxInflight, inflight);
        // Yield to event loop so siblings can also enter before we resolve.
        await new Promise((r) => setTimeout(r, 5));
        inflight -= 1;
        if (system.includes("merging")) return summaryXml({ title: "merged" });
        return summaryXml({ title: "ok" });
      },
    };
    const { handler } = await setupHandler({
      sessionId: "ses_par",
      obsCount: 400, // 4 chunks at chunkSize=100
      provider,
    });

    const result: any = await handler({ sessionId: "ses_par" });

    expect(result.success).toBe(true);
    // 4 chunks at concurrency 2 → max 2 in flight at once during the chunk phase.
    // Reduce is a single call so doesn't bump it.
    expect(maxInflight).toBe(2);
  });

  // #783: markdown-wrapped XML used to silently fail parsing because
  // the tag regex looked for <title> in the raw payload. stripXmlWrappers
  // now peels ```xml ... ``` fences and conversational pre/postamble
  // before the regex runs.
  it("parses a summary even when the LLM wraps XML in markdown fences", async () => {
    const wrappedXml = "Here's the summary:\n```xml\n" + summaryXml({
      title: "wrapped",
      narrative: "n",
      decisions: ["d1"],
      files: ["src/a.ts"],
      concepts: ["c1"],
    }) + "\n```\nLet me know if you need anything else.";
    const provider = makeProvider([wrappedXml]);
    const { handler, kv } = await setupHandler({
      sessionId: "ses_md",
      obsCount: 1,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_md" });

    expect(result.success).toBe(true);
    expect(result.summary.title).toBe("wrapped");
    const stored = await kv.get("summaries", "ses_md");
    expect((stored as any).title).toBe("wrapped");
  });

  it("retries the final summarize once on first-attempt parse failure", async () => {
    // First call returns garbage (no <title>), second returns valid XML.
    // The chunk-level retry is bypassed for a 1-obs session (no chunking),
    // so this exercises the new final-summarize retry path.
    const provider = makeProvider([
      "not xml, just a sentence with no tags",
      summaryXml({ title: "second-attempt" }),
    ]);
    const { handler } = await setupHandler({
      sessionId: "ses_retry",
      obsCount: 1,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_retry" });

    expect(result.success).toBe(true);
    expect(result.summary.title).toBe("second-attempt");
    expect((provider as any).calls.length).toBeGreaterThanOrEqual(2);
  });

  it("returns parse_failed only after both attempts fail", async () => {
    const provider = makeProvider([
      "garbage one",
      "garbage two",
    ]);
    const { handler } = await setupHandler({
      sessionId: "ses_fail",
      obsCount: 1,
      provider,
    });

    const result: any = await handler({ sessionId: "ses_fail" });

    expect(result.success).toBe(false);
    expect(result.error).toBe("parse_failed");
  });
});
