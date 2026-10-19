import { collectCodexUsage } from "./codex-usage.js";
import type { UsageSnapshot } from "./codex-usage.js";
import { collectClaudeUsage } from "./claude-usage.js";
import { finalizeModelUsage, summarizePricing } from "./usage-pricing.js";

export function combineAgentUsage(codex: UsageSnapshot, claude: UsageSnapshot) {
  const active = [codex, claude].filter(client => client.requests > 0);
  const total = (field: "inputTokens" | "cachedInputTokens" | "cacheWriteInputTokens" | "outputTokens") => {
    const value = active.reduce((sum, client) => sum + client[field], 0);
    return Number.isSafeInteger(value) && value >= 0 ? value : null;
  };
  const inputTokens = total("inputTokens"), cachedInputTokens = total("cachedInputTokens");
  const cacheWriteInputTokens = total("cacheWriteInputTokens"), outputTokens = total("outputTokens");
  const overflow = [inputTokens, cachedInputTokens, cacheWriteInputTokens, outputTokens].includes(null);
  const fullyPriced = !overflow && active.length > 0 && active.every(client => client.inputApiEquivalentUsd !== null);
  const inputApiEquivalentUsd = fullyPriced
    ? active.reduce((sum, client) => sum + (client.inputApiEquivalentUsd ?? 0), 0) : null;
  const modelUsage = finalizeModelUsage(active.flatMap(client => client.modelUsage.map(row => ({ ...row,
    pricing: { ...row.pricing, weightedRatesUsdPerMillion: { ...row.pricing.weightedRatesUsdPerMillion } } }))),
  { requests: active.reduce((sum, client) => sum + client.requests, 0), inputTokens, outputTokens });
  const pricing = summarizePricing(modelUsage);
  if (overflow) {
    pricing.costsUsd = { input: null, cacheRead: null, cacheWrite: null, output: null, total: null };
    pricing.pricedSubtotalUsd = 0;
  }
  return {
    source: "local-agent-response-usage", scope: "local profiles only; last 7 days", clients: { codex, claude },
    since: codex.since, until: codex.until, checkedAt: codex.checkedAt,
    requests: active.reduce((sum, client) => sum + client.requests, 0), inputTokens, cachedInputTokens,
    cacheWriteInputTokens, outputTokens,
    cacheHitRatio: !overflow && inputTokens !== null && cachedInputTokens !== null && inputTokens > 0
      ? cachedInputTokens / inputTokens : null,
    modelUsage, pricing, shareDenominator: "all sampled deduplicated responses, including unpriced models; input includes cache reads and writes; total = input + output",
    inputApiEquivalentUsd, inputUsdPerMillion: inputApiEquivalentUsd !== null && inputTokens !== null && inputTokens > 0
      ? inputApiEquivalentUsd / inputTokens * 1_000_000 : null,
    partial: overflow || codex.partial || claude.partial, unavailable: codex.unavailable && claude.unavailable,
  };
}
export type AgentUsageSnapshot = ReturnType<typeof combineAgentUsage>;
let cached: AgentUsageSnapshot | undefined;
let pending: Promise<AgentUsageSnapshot> | undefined;
// The measured local seven-day profile exceeds the collectors' interactive defaults.
const profileLimits = { bytes: 2 * 1024 ** 3, lineBytes: 4 * 1024 ** 2,
  records: 1_000_000, milliseconds: 30_000, files: 2048, entries: 16384, directories: 1024 };
export async function readAgentUsage(): Promise<AgentUsageSnapshot> {
  if (cached && Date.now() - Date.parse(cached.checkedAt) < 5 * 60_000) return cached;
  return pending ??= (async () => {
    const now = new Date();
    // Sequential collectors share a window and keep disk parsing single-flight.
    const codex = await collectCodexUsage({ now, limits: profileLimits });
    const claude = await collectClaudeUsage({ now, limits: profileLimits });
    return cached = combineAgentUsage(codex, claude);
  })().finally(() => { pending = undefined; });
}
