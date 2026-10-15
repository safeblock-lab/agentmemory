export const PRICING_SOURCES = {
  openai: "https://developers.openai.com/api/docs/pricing",
  anthropic: "https://platform.claude.com/docs/en/about-claude/pricing",
} as const;
export const PRICING_CHECKED_AT = "2026-10-09";
type Rates = { input: number; cacheRead: number; cacheWrite: number; output: number };
type Tariff = Rates & { threshold?: number; inputMultiplier?: number; outputMultiplier?: number; provider: "openai" | "anthropic" };
const openai = (input: number, cacheRead: number, output: number): Tariff =>
  ({ input, cacheRead, cacheWrite: input * 1.25, output, threshold: 272_000, inputMultiplier: 2, outputMultiplier: 1.5, provider: "openai" });
const anthropic = (input: number, output: number, cacheRead = input * .1): Tariff =>
  ({ input, output, cacheRead, cacheWrite: input * 1.25, provider: "anthropic" });
const tariffs: Record<string, Tariff> = {
  "gpt-6.1-sol": openai(2, .1, 10), "gpt-6-sol": openai(2, .2, 10),
  "gpt-6-astra": openai(10, 1, 50), "gpt-6-luna": openai(.1, .01, .5),
  "gpt-5.6-sol": openai(4, .4, 20),
  "gpt-5.3-codex": { input: 1.75, cacheRead: .175, cacheWrite: 0, output: 14, provider: "openai" },
  "claude-fable-5-1": anthropic(10, 50, .25), "claude-mythos-5-1": anthropic(10, 50, .25),
  "claude-opus-5-5": anthropic(4, 20, .2), "claude-sonnet-5-5": anthropic(2, 10, .1),
  "claude-haiku-5-5": { ...anthropic(.1, .5, .01), threshold: 100_000, inputMultiplier: 5, outputMultiplier: 5 },
  "claude-fable-5": anthropic(10, 50), "claude-mythos-5": anthropic(10, 50),
  "claude-opus-5": anthropic(5, 25), "claude-opus-4-8": anthropic(5, 25),
  "claude-opus-4-7": anthropic(5, 25), "claude-opus-4-6": anthropic(5, 25),
  "claude-opus-4-5": anthropic(5, 25), "claude-opus-4-1": anthropic(15, 75),
  "claude-opus-4": anthropic(15, 75), "claude-sonnet-5": anthropic(2, 10),
  "claude-sonnet-4-6": anthropic(3, 15), "claude-sonnet-4-5": anthropic(3, 15),
  "claude-sonnet-4": anthropic(3, 15), "claude-haiku-4-5": anthropic(1, 5), "claude-haiku-3-5": anthropic(.8, 4),
};
export type UsagePricingMetadata = { tier?: string; platform?: string; geography?: string; speed?: string;
  writes5m?: number; writes1h?: number };
export type MeasuredUsage = { input: number; cached: number; writes: number; output: number };
const categories = ["input", "cacheRead", "cacheWrite", "output"] as const;
type Category = typeof categories[number];
type Costs = Record<Category, number | null> & { total: number | null };
export type ModelUsage = { client: "codex" | "claude"; model: string; requests: number;
  inputTokens: number; cachedInputTokens: number; cacheWriteInputTokens: number; outputTokens: number;
  requestShare: number | null; inputTokenShare: number | null; outputTokenShare: number | null; totalTokenShare: number | null;
  pricing: { basis: "current-direct-api-equivalent"; currency: "USD"; checkedAt: string; source: string | null;
    costsUsd: Costs; pricedSubtotalUsd: number; unpricedRequests: number; scenarioRequests: number;
    reasons: string[]; weightedRatesUsdPerMillion: Record<Category, number | null> } };

export function priceUsage(model: string, usage: MeasuredUsage, metadata: UsagePricingMetadata = {}) {
  // Dated model snapshots use their explicitly named family; moving "latest" aliases stay unpriced.
  const key = model.replace(/-\d{8}$/, "");
  const tariff = Object.hasOwn(tariffs, key) ? tariffs[key] : undefined;
  const source = key === "gpt-6-sol" ? "https://developers.openai.com/api/docs/models/gpt-6-sol"
    : tariff ? PRICING_SOURCES[tariff.provider] : null;
  const costs: Costs = { input: null, cacheRead: null, cacheWrite: null, output: null, total: null };
  const reason = (value: string) => ({ costs, source,
    reason: value, scenario: false, longContext: false, uncachedEquivalentUsd: null as number | null });
  if (![usage.input, usage.cached, usage.writes, usage.output].every(value => Number.isSafeInteger(value) && value >= 0)
    || usage.cached + usage.writes > usage.input) return reason("invalid_token_categories");
  if (!tariff) return reason("unknown_model_tariff");
  if (metadata.platform && !["openai", "anthropic", "direct"].includes(metadata.platform)) return reason("unsupported_platform");
  if (metadata.platform === "openai" && tariff.provider !== "openai"
    || metadata.platform === "anthropic" && tariff.provider !== "anthropic") return reason("platform_model_mismatch");
  const tier = metadata.tier ?? "standard";
  let multiplier = 1;
  if (tariff.provider === "openai") {
    if (metadata.speed && metadata.speed !== "standard") return reason("unsupported_speed");
    if (["batch", "flex"].includes(tier) && key.startsWith("gpt-6")) multiplier = .5;
    else if (["priority", "fast"].includes(tier) && (key.startsWith("gpt-6") || key === "gpt-5.3-codex")) multiplier = 2;
    else if (tier === "ultrafast" && ["gpt-6.1-sol", "gpt-6-astra"].includes(key)) multiplier = 6;
    else if (!["standard", "default"].includes(tier)) return reason("unsupported_service_tier");
    if (metadata.geography && metadata.geography !== "global") return reason("unsupported_geography");
  } else {
    if (tier === "batch") multiplier = .5;
    else if (tier !== "standard" && tier !== "default") return reason("unsupported_service_tier");
    if (metadata.speed === "fast") {
      if (tier === "batch" || !["claude-opus-5-5", "claude-opus-5", "claude-opus-4-8", "claude-opus-4-6"].includes(key))
        return reason("unsupported_speed");
      if (key !== "claude-opus-4-6") multiplier *= 2;
    } else if (metadata.speed && metadata.speed !== "standard") return reason("unsupported_speed");
    if (metadata.geography === "us" && /claude-(?:opus-4-[6-8]|sonnet-4-6|.+-5(?:-|$))/.test(key)) multiplier *= 1.1;
    else if (metadata.geography && metadata.geography !== "global") return reason("unsupported_geography");
    // The current table does not establish the historical >200k legacy beta tariff.
    if (usage.input > 200_000 && ["claude-sonnet-4", "claude-sonnet-4-5"].includes(key)) return reason("legacy_long_context_tariff_unknown");
  }
  const longContext = tariff.threshold !== undefined && usage.input > tariff.threshold;
  const rates = { input: tariff.input, cacheRead: tariff.cacheRead, cacheWrite: tariff.cacheWrite, output: tariff.output };
  for (const field of categories) rates[field] *= multiplier * (longContext ? field === "output"
    ? tariff.outputMultiplier ?? 1 : tariff.inputMultiplier ?? 1 : 1);
  costs.input = (usage.input - usage.cached - usage.writes) * rates.input / 1e6;
  costs.cacheRead = usage.cached * rates.cacheRead / 1e6;
  costs.output = usage.output * rates.output / 1e6;
  let missingWrites = false;
  if (tariff.provider === "anthropic" && usage.writes > 0) {
    const { writes5m, writes1h } = metadata;
    if (writes5m !== undefined && writes1h !== undefined && Number.isSafeInteger(writes5m) && writes5m >= 0
      && Number.isSafeInteger(writes1h) && writes1h >= 0 && writes5m + writes1h === usage.writes)
      costs.cacheWrite = (writes5m * rates.cacheWrite + writes1h * rates.input * 2) / 1e6;
    else missingWrites = true;
  } else if (usage.writes > 0 && tariff.cacheWrite === 0) missingWrites = true;
  else costs.cacheWrite = usage.writes * rates.cacheWrite / 1e6;
  if (!missingWrites) costs.total = categories.reduce((sum, field) => sum + (costs[field] ?? 0), 0);
  return { costs, source, reason: missingWrites ? "cache_write_tariff_unknown" : null,
    scenario: !metadata.platform || !metadata.tier || !metadata.geography,
    longContext, uncachedEquivalentUsd: costs.total === null ? null
      : costs.total + usage.cached * (rates.input - rates.cacheRead) / 1e6 };
}

export function newModelUsage(client: ModelUsage["client"], model: string): ModelUsage {
  return { client, model, requests: 0, inputTokens: 0, cachedInputTokens: 0, cacheWriteInputTokens: 0, outputTokens: 0,
    requestShare: null, inputTokenShare: null, outputTokenShare: null, totalTokenShare: null,
    pricing: { basis: "current-direct-api-equivalent", currency: "USD", checkedAt: PRICING_CHECKED_AT,
      source: null, costsUsd: { input: 0, cacheRead: 0, cacheWrite: 0, output: 0, total: 0 }, pricedSubtotalUsd: 0,
      unpricedRequests: 0, scenarioRequests: 0, reasons: [], weightedRatesUsdPerMillion: { input: null, cacheRead: null, cacheWrite: null, output: null } } };
}
export function addModelUsage(row: ModelUsage, usage: MeasuredUsage, metadata: UsagePricingMetadata) {
  const priced = priceUsage(row.model, usage, metadata);
  row.requests++; row.inputTokens += usage.input; row.cachedInputTokens += usage.cached;
  row.cacheWriteInputTokens += usage.writes; row.outputTokens += usage.output;
  row.pricing.source = priced.source;
  if (priced.reason) {
    row.pricing.unpricedRequests++;
    if (!row.pricing.reasons.includes(priced.reason)) row.pricing.reasons.push(priced.reason);
  }
  if (priced.scenario) row.pricing.scenarioRequests++;
  for (const category of categories) {
    const value = priced.costs[category];
    if (value !== null) row.pricing.pricedSubtotalUsd += value;
    row.pricing.costsUsd[category] = value === null || row.pricing.costsUsd[category] === null ? null
      : row.pricing.costsUsd[category]! + value;
  }
  row.pricing.costsUsd.total = priced.costs.total === null || row.pricing.costsUsd.total === null ? null
    : row.pricing.costsUsd.total + priced.costs.total;
  return priced;
}
export function finalizeModelUsage(rows: ModelUsage[], totals: { requests: number; inputTokens: number | null; outputTokens: number | null }) {
  const share = (value: number, denominator: number | null) => denominator !== null && denominator > 0
    && Number.isSafeInteger(denominator) ? value / denominator : null;
  const total = totals.inputTokens === null || totals.outputTokens === null ? null : totals.inputTokens + totals.outputTokens;
  return rows.map(row => {
    row.requestShare = share(row.requests, totals.requests); row.inputTokenShare = share(row.inputTokens, totals.inputTokens);
    row.outputTokenShare = share(row.outputTokens, totals.outputTokens);
    row.totalTokenShare = share(row.inputTokens + row.outputTokens, total);
    const tokens = { input: row.inputTokens - row.cachedInputTokens - row.cacheWriteInputTokens,
      cacheRead: row.cachedInputTokens, cacheWrite: row.cacheWriteInputTokens, output: row.outputTokens };
    for (const category of categories) row.pricing.weightedRatesUsdPerMillion[category] = row.pricing.costsUsd[category] !== null
      && tokens[category] > 0 ? row.pricing.costsUsd[category]! * 1e6 / tokens[category] : null;
    return row;
  });
}
export function summarizePricing(rows: ModelUsage[]) {
  const initial = rows.length ? 0 : null;
  const costsUsd: Costs = { input: initial, cacheRead: initial, cacheWrite: initial, output: initial, total: initial };
  for (const row of rows) for (const field of [...categories, "total"] as const)
    costsUsd[field] = costsUsd[field] === null || row.pricing.costsUsd[field] === null ? null
      : costsUsd[field]! + row.pricing.costsUsd[field]!;
  return { basis: "current-direct-api-equivalent", currency: "USD", checkedAt: PRICING_CHECKED_AT, costsUsd,
    pricedSubtotalUsd: rows.reduce((sum, row) => sum + row.pricing.pricedSubtotalUsd, 0),
    unpricedRequests: rows.reduce((sum, row) => sum + row.pricing.unpricedRequests, 0),
    scenarioRequests: rows.reduce((sum, row) => sum + row.pricing.scenarioRequests, 0),
    isInvoice: false, sources: PRICING_SOURCES };
}
