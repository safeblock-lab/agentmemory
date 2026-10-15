import { describe, expect, it } from "vitest";
import { addModelUsage, finalizeModelUsage, newModelUsage, priceUsage, summarizePricing } from "../src/functions/usage-pricing.js";

const usage = { input: 1000, cached: 700, writes: 100, output: 20 };
describe("recorded model API-equivalent pricing", () => {
  it("prices four exclusive categories for Sol without effort assumptions", () => {
    const priced = priceUsage("gpt-6.1-sol", usage);
    expect(priced.costs).toEqual({ input: .0004, cacheRead: .00007, cacheWrite: .00025, output: .0002, total: .00092 });
    expect(priced.scenario).toBe(true);
    expect(priced.uncachedEquivalentUsd).toBeCloseTo(.00225);
  });
  it.each(["gpt-6-astra", "gpt-6-luna", "gpt-6.1-sol"])("uses each model's own tariff: %s", model => {
    const priced = priceUsage(model, usage);
    expect(priced.costs.total).not.toBeNull();
    expect(priced.costs.input).not.toBe(priced.costs.output);
  });
  it("applies long-context thresholds per full request, including cached tokens", () => {
    const below = priceUsage("gpt-6.1-sol", { input: 272000, cached: 272000, writes: 0, output: 100 });
    const above = priceUsage("gpt-6.1-sol", { input: 272001, cached: 272001, writes: 0, output: 100 });
    expect(below.longContext).toBe(false); expect(above.longContext).toBe(true);
    expect(above.costs.cacheRead).toBeCloseTo(272001 * .2 / 1e6);
    expect(above.costs.output).toBeCloseTo(.0015);
  });
  it.each([["fast", 2], ["priority", 2], ["batch", .5], ["flex", .5], ["ultrafast", 6]] as const)(
    "honors recorded OpenAI %s tier", (tier, multiplier) => {
      expect(priceUsage("gpt-6.1-sol", usage, { tier }).costs.total).toBeCloseTo(.00092 * multiplier);
    });
  it("does not guess unknown models, moving aliases, tiers, platforms or geography", () => {
    for (const model of ["unknown", "__proto__", "gpt-6.2-sol", "gpt-daybreak-blue-latest"])
      expect(priceUsage(model, usage).costs.total).toBeNull();
    for (const metadata of [{ tier: "auto" }, { platform: "bedrock" }, { geography: "eu" }, { platform: "anthropic" }])
      expect(priceUsage("gpt-6.1-sol", usage, metadata).costs.total).toBeNull();
  });
  it("Claude cache-write TTL uncertainty preserves known category subtotal but no invented total", () => {
    const priced = priceUsage("claude-sonnet-4-5", usage);
    expect(priced.costs).toMatchObject({ input: .0006, cacheWrite: null, output: .0003, total: null });
    expect(priced.costs.cacheRead).toBeCloseTo(.00021);
    expect(priced.reason).toBe("cache_write_tariff_unknown");
  });
  it("prices mixed five-minute/hour writes separately and stacks batch/geography", () => {
    const priced = priceUsage("claude-sonnet-4-6", usage,
      { writes5m: 40, writes1h: 60, tier: "batch", geography: "us", platform: "anthropic" });
    expect(priced.costs.cacheWrite).toBeCloseTo((40 * 3.75 + 60 * 6) * .55 / 1e6);
    expect(priced.scenario).toBe(false);
    expect(priced.costs.total).not.toBeNull();
  });
  it("does not assume a TTL from incomplete or contradictory TTL counts", () => {
    for (const metadata of [{ writes5m: 100 }, { writes5m: 30, writes1h: 60 }, { writes5m: -1, writes1h: 101 }])
      expect(priceUsage("claude-sonnet-4-5", usage, metadata).costs.cacheWrite).toBeNull();
  });
  it("uses full standard context for newer Claude and the explicit Haiku threshold", () => {
    const input = 900000;
    expect(priceUsage("claude-opus-4-6", { input, cached: 0, writes: 0, output: 0 }).costs.input).toBe(4.5);
    expect(priceUsage("claude-haiku-5-5", { input: 100000, cached: 0, writes: 0, output: 0 }).costs.input).toBe(.01);
    expect(priceUsage("claude-haiku-5-5", { input: 100001, cached: 0, writes: 0, output: 0 }).costs.input).toBeCloseTo(.0500005);
    expect(priceUsage("claude-sonnet-4-5", { input, cached: 0, writes: 0, output: 0 }).reason).toBe("legacy_long_context_tariff_unknown");
  });
  it("honors supported Claude fast mode and rejects unsupported modifiers", () => {
    const short = { input: 1000, cached: 0, writes: 0, output: 100 };
    expect(priceUsage("claude-opus-5-5", short, { speed: "fast" }).costs.total).toBe(.012);
    expect(priceUsage("claude-opus-4-7", short, { speed: "fast" }).costs.total).toBeNull();
    expect(priceUsage("claude-opus-5-5", short, { speed: "fast", tier: "batch" }).costs.total).toBeNull();
  });
  it("prices a named dated snapshot without conflating unknown families", () => {
    expect(priceUsage("claude-sonnet-4-5-20250929", { ...usage, writes: 0 }).costs.total).not.toBeNull();
    expect(priceUsage("claude-other-20250929", usage).costs.total).toBeNull();
  });
  it("rejects malformed category boundaries rather than producing negative charges", () => {
    for (const bad of [{ ...usage, input: 1 }, { ...usage, output: -1 }, { ...usage, input: Infinity }])
      expect(priceUsage("gpt-6.1-sol", bad).costs.total).toBeNull();
  });
  it("retains unknown models in share denominators and separates subtotal from incomplete total", () => {
    const known = newModelUsage("codex", "gpt-6.1-sol"), unknown = newModelUsage("claude", "unknown");
    addModelUsage(known, usage, {}); addModelUsage(unknown, { input: 3000, cached: 0, writes: 0, output: 80 }, {});
    const rows = finalizeModelUsage([known, unknown], { requests: 2, inputTokens: 4000, outputTokens: 100 });
    expect(rows[0]).toMatchObject({ requestShare: .5, inputTokenShare: .25, outputTokenShare: .2 });
    expect(rows.reduce((sum, row) => sum + row.totalTokenShare!, 0)).toBeCloseTo(1);
    expect(summarizePricing(rows)).toMatchObject({ unpricedRequests: 1, pricedSubtotalUsd: .00092, costsUsd: { total: null } });
  });
  it("suppresses unsafe token-share denominators", () => {
    const row = newModelUsage("codex", "unknown");
    expect(finalizeModelUsage([row], { requests: 1, inputTokens: Number.MAX_SAFE_INTEGER, outputTokens: 1 })[0].totalTokenShare).toBeNull();
  });
});
