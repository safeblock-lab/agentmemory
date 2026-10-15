import { afterEach, describe, expect, it } from "vitest";
import { mkdir, writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { collectClaudeUsage } from "../src/functions/claude-usage.js";
import { collectCodexUsage, USAGE_LIMITS } from "../src/functions/codex-usage.js";

const now = new Date("2026-10-09T12:00:00Z");
const roots: string[] = [];
async function fixture(lines: unknown[], name = "session.jsonl") {
  const root = join(process.cwd(), ".codex-task/claude-codex-usage", `fixture-${crypto.randomUUID()}`);
  roots.push(root); await mkdir(root, { recursive: true });
  await writeFile(join(root, name), lines.map(line => typeof line === "string" ? line : JSON.stringify(line)).join("\n"));
  return root;
}
const claude = (id = "msg1", output = 10, extra = {}) => ({ type: "assistant", sessionId: "s1", requestId: "req1",
  timestamp: "2026-10-09T10:00:00Z", message: { id, model: "claude-sonnet-4-5", content: [{ text: "PRIVATE" }],
    usage: { input_tokens: 100, cache_read_input_tokens: 800, cache_creation_input_tokens: 100, output_tokens: output } }, ...extra });
afterEach(async () => { for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true }); });

describe("local agent usage readers", () => {
  it("sums Claude's three input categories, merges streaming and fork copies once, retains model only", async () => {
    const root = await fixture([claude("msg1", 1), claude(), claude("msg1", 5, { sessionId: "fork" }), claude("msg2", 20)]);
    const result = await collectClaudeUsage({ root, now });
    expect(result).toMatchObject({ requests: 2, sessions: 1, inputTokens: 2000, cachedInputTokens: 1600,
      cacheWriteInputTokens: 200, outputTokens: 30, cacheHitRatio: .8, apiEquivalentUsd: null, status: "available" });
    expect(result.models).toEqual({ "claude-sonnet-4-5": 2 });
    expect(JSON.stringify(result)).not.toContain("PRIVATE");
  });
  it("counts distinct messages in one request and excludes old/future timestamps and non-assistant events", async () => {
    const root = await fixture([claude(), claude("msg2"), claude("old", 10, { timestamp: "2026-10-01T00:00:00Z" }),
      claude("future", 10, { timestamp: "2026-10-10T00:00:00Z" }), claude("user", 10, { type: "user" })]);
    expect((await collectClaudeUsage({ root, now })).requests).toBe(2);
  });
  it("rejects invalid usage and malformed JSON, with no transcript or error payload exposure", async () => {
    const invalid = claude(); invalid.message.usage.input_tokens = -1;
    const root = await fixture(["{bad", invalid, claude()]);
    expect(await collectClaudeUsage({ root, now })).toMatchObject({ requests: 1, malformedRecords: 2, partial: true, status: "partial" });
  });
  it("handles empty and missing clients normally", async () => {
    const root = await fixture([]);
    expect(await collectClaudeUsage({ root, now })).toMatchObject({ requests: 0, partial: false, status: "empty" });
    expect(await collectClaudeUsage({ root: join(root, "missing"), now })).toMatchObject({ requests: 0, partial: false, status: "unavailable" });
  });
  it.each(["input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens", "output_tokens"])(
    "does not turn absent Claude category %s into measured zero", async category => {
      const missing = claude();
      Reflect.deleteProperty(missing.message.usage, category);
      const root = await fixture([missing]);
      expect(await collectClaudeUsage({ root, now })).toMatchObject({ requests: 0, malformedRecords: 1,
        partial: true, status: "partial", cacheHitRatio: null, apiEquivalentUsd: null });
    });
  it("retains explicit zero cache categories as measured values", async () => {
    const explicit = claude();
    explicit.message.usage.cache_read_input_tokens = 0;
    explicit.message.usage.cache_creation_input_tokens = 0;
    const root = await fixture([explicit]);
    expect(await collectClaudeUsage({ root, now })).toMatchObject({ requests: 1, inputTokens: 100,
      cachedInputTokens: 0, cacheWriteInputTokens: 0, cacheHitRatio: 0, partial: false });
  });
  it("discards an oversized line without retaining it and recovers the following valid record", async () => {
    const root = await fixture(["x".repeat(USAGE_LIMITS.lineBytes + 100000), claude()]);
    expect(await collectClaudeUsage({ root, now })).toMatchObject({ requests: 1, oversizedLines: 1, partial: true });
  });
  it("caps bytes, discovered entries, files, records and elapsed work explicitly", async () => {
    const root = await fixture([claude(), claude("msg2")]);
    await writeFile(join(root, "other.jsonl"), JSON.stringify(claude("msg3")));
    const bytes = await collectClaudeUsage({ root, now, limits: { bytes: 100 } });
    expect(bytes.bytesRead).toBeLessThanOrEqual(100); expect(bytes.partial).toBe(true);
    const discovery = await collectClaudeUsage({ root, now, limits: { entries: 1 } });
    expect(discovery.entriesVisited).toBeLessThanOrEqual(2); expect(discovery.partial).toBe(true);
    expect((await collectClaudeUsage({ root, now, limits: { files: 1 } })).filesRead).toBe(1);
    expect(await collectClaudeUsage({ root, now, limits: { records: 1 } })).toMatchObject({ requests: 1, partial: true });
    expect(await collectClaudeUsage({ root, now, limits: { milliseconds: 0 } })).toMatchObject({ filesRead: 0, partial: true });
  });
  it("bounds recursive discovery, never follows symlink entries", async () => {
    const root = await fixture([]);
    for (let index = 0; index < 5; index++) await mkdir(join(root, `dir${index}`));
    expect(await collectClaudeUsage({ root, now, limits: { directories: 2 } })).toMatchObject({ partial: true });
  });
  it("Codex counts only own individual response records, not cumulative events or fork copies", async () => {
    const usage = { input_tokens: 1000, cached_input_tokens: 800, cache_write_input_tokens: 100, output_tokens: 20 };
    const response = { type: "token_usage_record", timestamp: "2026-10-09T10:00:00Z",
      payload: { thread_id: "s1", response_id: "r1", model: "gpt-6.1-sol", usage } };
    const root = await fixture([{ type: "session_meta", payload: { id: "s1" } }, response, response,
      { ...response, payload: { ...response.payload, thread_id: "parent", response_id: "inherited" } },
      { type: "event_msg", payload: { type: "token_count", total_token_usage: usage } }]);
    const result = await collectCodexUsage({ root, now });
    expect(result).toMatchObject({ requests: 1, inputTokens: 1000, cacheHitRatio: .8, status: "available" });
    expect(result.apiEquivalentUsd).toBeGreaterThan(0);
  });
  it("Codex unknown models remain measured and unpriced instead of receiving Sol prices", async () => {
    const root = await fixture([{ type: "session_meta", payload: { id: "s1" } }, { type: "token_usage_record",
      timestamp: "2026-10-09T10:00:00Z", payload: { thread_id: "s1", response_id: "r1", model: "other",
        usage: { input_tokens: 100, cached_input_tokens: 0, cache_write_input_tokens: 0, output_tokens: 10 } } }]);
    expect(await collectCodexUsage({ root, now })).toMatchObject({ requests: 1, inputTokens: 100,
      apiEquivalentUsd: null, inputUsdPerMillion: null, unpricedRequests: 1 });
  });
  it("caps retained metadata and safely handles model names matching object prototype properties", async () => {
    const large = claude("x".repeat(257));
    const prototype = claude("safe"); prototype.message.model = "__proto__";
    const root = await fixture([large, prototype]);
    const result = await collectClaudeUsage({ root, now });
    expect(result).toMatchObject({ requests: 1, malformedRecords: 1, partial: true });
    expect(result.models["__proto__"]).toBe(1);
  });
  it("marks an unreadable root partial without returning filesystem diagnostics", async () => {
    const root = await fixture([]);
    const result = await collectClaudeUsage({ root: join(root, "session.jsonl"), now });
    expect(result).toMatchObject({ partial: true, requests: 0, status: "partial" });
    expect(JSON.stringify(result)).not.toContain(root);
  });
  it("prevents unsafe aggregate token numbers", async () => {
    const first = claude("max"), second = claude("next");
    first.message.usage = { input_tokens: Number.MAX_SAFE_INTEGER, cache_read_input_tokens: 0,
      cache_creation_input_tokens: 0, output_tokens: 0 };
    const result = await collectClaudeUsage({ root: await fixture([first, second]), now });
    expect(result.partial).toBe(true); expect(Number.isSafeInteger(result.inputTokens)).toBe(true);
  });
  it("rejects contradictory input categories across duplicate snapshots instead of combining their maxima", async () => {
    const first = claude("same", 5), conflict = claude("same", 20);
    conflict.message.usage.input_tokens = 200;
    conflict.message.usage.cache_read_input_tokens = 700;
    const result = await collectClaudeUsage({ root: await fixture([first, conflict]), now });
    expect(result).toMatchObject({ requests: 1, inputTokens: 1000, cachedInputTokens: 800,
      cacheWriteInputTokens: 100, outputTokens: 5, malformedRecords: 1, partial: true });
  });
});
