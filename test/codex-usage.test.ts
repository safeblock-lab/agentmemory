import { afterEach, expect, it } from "vitest";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { collectCodexUsage } from "../src/functions/codex-usage.js";
import { collectClaudeUsage } from "../src/functions/claude-usage.js";
import { combineAgentUsage } from "../src/functions/agent-usage.js";
const roots: string[] = [], now = new Date("2026-10-09T12:00:00Z"), timestamp = "2026-10-09T11:00:00Z";
async function fixture(lines: unknown[]) {
  const root = join(process.cwd(), ".codex-task/measured-model-pricing", `fixture-${crypto.randomUUID()}`);
  roots.push(root); await mkdir(root, { recursive: true });
  await writeFile(join(root, "session.jsonl"), lines.map(line => JSON.stringify(line)).join("\n"));
  return root;
}
afterEach(async () => { for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true }); });
const codex = (model: string, id = model, extra = {}) => ({ type: "token_usage_record", timestamp,
  payload: { thread_id: "s1", response_id: id, model, usage: { input_tokens: 1000,
    cached_input_tokens: 800, cache_write_input_tokens: 100, output_tokens: 20, reasoning_output_tokens: 15 }, ...extra } });
const claude = (id: string, extra = {}) => ({ type: "assistant", sessionId: "s1", timestamp,
  message: { id, model: "claude-sonnet-4-6", usage: { input_tokens: 100, cache_read_input_tokens: 800,
    cache_creation_input_tokens: 100, cache_creation: { ephemeral_5m_input_tokens: 40, ephemeral_1h_input_tokens: 60 }, output_tokens: 20 }, ...extra } });
it("reconciles deduplicated mixed models, per-model categories and combined shares", async () => {
  const c = await collectCodexUsage({ now, root: await fixture([{ type: "session_meta", payload: { id: "s1" } },
    codex("gpt-6.1-sol"), codex("gpt-6-astra"), codex("gpt-6-astra"), codex("unpriced")]) });
  const a = await collectClaudeUsage({ now, root: await fixture([claude("m1"), claude("m1"), claude("m2")]) });
  const combined = combineAgentUsage(c, a);
  expect(combined).toMatchObject({ requests: 5, inputTokens: 5000, outputTokens: 100 });
  expect(combined.modelUsage.map(row => row.requestShare)).toEqual([.2, .2, .2, .4]);
  expect(combined.modelUsage.reduce((sum, row) => sum + row.inputTokens, 0)).toBe(combined.inputTokens);
  expect(combined.modelUsage.reduce((sum, row) => sum + row.outputTokens, 0)).toBe(combined.outputTokens);
  expect(c.reasoningOutputTokens).toBe(45);
  expect(c.modelUsage[0].pricing.costsUsd.output).toBe(.0002);
  expect(a.modelUsage[0].pricing.costsUsd.cacheWrite).toBeCloseTo(.00102);
  expect(combined.pricing).toMatchObject({ unpricedRequests: 1, costsUsd: { total: null } });
  expect(combined.pricing.pricedSubtotalUsd).toBeGreaterThan(0);
  expect(c.modelUsage[0].requestShare).toBeCloseTo(1 / 3);
});
it("reads recorded tariff metadata without exposing transcript or retained response IDs", async () => {
  const c = await collectCodexUsage({ now, root: await fixture([{ type: "session_meta", payload: { id: "s1" } },
    codex("gpt-6.1-sol", "PRIVATE_ID", { service_tier: "fast", provider: "openai", inference_geo: "global", text: "PRIVATE_TEXT" })]) });
  expect(c.modelUsage[0].pricing.scenarioRequests).toBe(0);
  expect(c.modelUsage[0].pricing.weightedRatesUsdPerMillion.output).toBe(20);
  expect(JSON.stringify(c)).not.toMatch(/PRIVATE_ID|PRIVATE_TEXT/);
});
it("rejects contradictory Claude tariff metadata on a repeated response", async () => {
  const result = await collectClaudeUsage({ now, root: await fixture([claude("m1", { service_tier: "standard" }),
    claude("m1", { service_tier: "batch" })]) });
  expect(result).toMatchObject({ requests: 1, partial: true, malformedRecords: 1 });
  expect(result.modelUsage[0].pricing.weightedRatesUsdPerMillion.output).toBeCloseTo(15);
});
it("correlates exact own-turn model changes without backfilling earlier or unknown contexts", async () => {
  const unmodelled = (id: string, turn_id: string, thread_id = "s1") => {
    const value = codex("unused", id);
    const { model: _model, ...payload } = value.payload;
    return { ...value, payload: { ...payload, turn_id, thread_id } };
  };
  const root = await fixture([{ type: "session_meta", payload: { id: "s1", model_provider: "openai" } },
    unmodelled("before", "t1"),
    { type: "turn_context", payload: { turn_id: "t1", model: "gpt-6.1-sol" } }, unmodelled("sol", "t1"),
    { type: "turn_context", payload: { turn_id: "t2", model: "gpt-6-astra" } }, unmodelled("astra", "t2"),
    unmodelled("parent-copy", "t2", "parent"), unmodelled("missing-turn", "absent"),
    { type: "turn_context", payload: { turn_id: "t3" } }, unmodelled("unknown-turn-model", "t3"),
    { type: "turn_context", payload: { turn_id: "t1", model: "new-unknown-model" } }, unmodelled("new-unknown", "t1"),
    { type: "session_meta", payload: { id: "s2" } },
    { ...unmodelled("new-session", "t2"), payload: { ...unmodelled("new-session", "t2").payload, thread_id: "s2" } },
  ]);
  const result = await collectCodexUsage({ root, now });
  expect(result.models).toEqual({ unknown: 4, "gpt-6.1-sol": 1, "gpt-6-astra": 1, "new-unknown-model": 1 });
  expect(result.requests).toBe(7); expect(result.sessions).toBe(2);
  expect(result.modelUsage.find(row => row.model === "gpt-6-astra")?.pricing.weightedRatesUsdPerMillion.output).toBe(50);
});
it("bounds retained turn-context maps and leaves omitted context unpriced", async () => {
  const result = await collectCodexUsage({ now, limits: { records: 1 }, root: await fixture([
    { type: "session_meta", payload: { id: "s1" } },
    { type: "turn_context", payload: { turn_id: "t1", model: "gpt-6.1-sol" } },
    { type: "turn_context", payload: { turn_id: "t2", model: "gpt-6-astra" } },
    { ...codex("unused", "r"), payload: { thread_id: "s1", turn_id: "t2", response_id: "r",
      usage: codex("unused").payload.usage } },
  ]) });
  expect(result).toMatchObject({ requests: 1, partial: true, models: { unknown: 1 }, unpricedRequests: 1 });
});
it("uses exact-turn tariff metadata and defined response overrides without guessing modifiers", async () => {
  const response = (id: string, extra = {}) => ({ type: "token_usage_record", timestamp, payload: {
    thread_id: "s1", turn_id: "t1", response_id: id, usage: codex("unused").payload.usage, ...extra } });
  const result = await collectCodexUsage({ now, root: await fixture([
    { type: "session_meta", payload: { id: "s1", model_provider: "openai" } },
    { type: "turn_context", payload: { turn_id: "t1", model: "gpt-6.1-sol", service_tier: "fast",
      inference_geo: "global", speed: "standard" } }, response("fast"), response("standard", { service_tier: "standard" }),
  ]) });
  expect(result.modelUsage[0].pricing).toMatchObject({ scenarioRequests: 0, unpricedRequests: 0 });
  expect(result.modelUsage[0].pricing.costsUsd.output).toBeCloseTo(.0006);
  expect(result.modelUsage[0].pricing.weightedRatesUsdPerMillion.output).toBeCloseTo(15);
});
it("rejects explicitly foreign thread/session context rather than poisoning own-turn pricing", async () => {
  const result = await collectCodexUsage({ now, root: await fixture([
    { type: "session_meta", payload: { id: "s1" } },
    { type: "turn_context", payload: { turn_id: "t1", model: "gpt-6.1-sol" } },
    { type: "turn_context", payload: { turn_id: "t1", thread_id: "parent", model: "gpt-6-astra" } },
    { type: "turn_context", payload: { turn_id: "t1", session_id: "parent", model: "gpt-6-astra" } },
    { type: "token_usage_record", timestamp, payload: { thread_id: "s1", turn_id: "t1", response_id: "r",
      usage: codex("unused").payload.usage } },
  ]) });
  expect(result).toMatchObject({ requests: 1, models: { "gpt-6.1-sol": 1 }, partial: true, malformedRecords: 2 });
});
