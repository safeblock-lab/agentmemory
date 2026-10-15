import type { StateKV } from "../state/kv.js";
import type { FunctionMetrics } from "../types.js";
import { readContextUsage } from "./context-usage.js";
import { PRICING_CHECKED_AT, PRICING_SOURCES } from "./usage-pricing.js";
import { readAgentUsage } from "./agent-usage.js";
import { isAgentScopeIsolated, isContextInjectionEnabled } from "../config.js";
import { readProcessingUsage } from "./processing-usage.js";

export async function readSavingsReport(kv: StateKV, metrics: FunctionMetrics[]) {
  const [context, agentUsage, processingSample] = await Promise.all([
    readContextUsage(kv),
    // A local profile's rollouts do not carry agentmemory agent identities.
    isAgentScopeIsolated() ? Promise.resolve(null) : readAgentUsage(),
    readProcessingUsage(kv),
  ]);
  const llm = metrics.filter((m) => m.functionId.startsWith("llm:"));
  const recordedInputTokens = llm.reduce((sum, m) => sum + (m.inputTokens ?? 0), 0);
  const recordedOutputTokens = llm.reduce((sum, m) => sum + (m.outputTokens ?? 0), 0);
  return {
    pricing: { basis: "current-direct-api-equivalent", currency: "USD", checkedAt: PRICING_CHECKED_AT,
      sources: PRICING_SOURCES, model: null, effort: null, isInvoice: false },
    context, agentUsage, codex: agentUsage?.clients.codex ?? null,
    processingSample: { limit: processingSample.limit, calls: processingSample.calls,
      unreportedCalls: processingSample.unreportedCalls, missingCacheCalls: processingSample.missingCacheCalls },
    configuration: { serverContextInjectionEnabled: isContextInjectionEnabled(), hookEnvironmentObserved: false },
    selectionScenario: {
      baseline: "all eligible candidate memory before selection; one exposure; excludes processing",
      cachedShortContextUsd: null,
      uncachedShortContextUsd: null,
      profileCacheWeightedUsd: agentUsage?.inputUsdPerMillion == null ? null
        : context.selectionReductionTokens * agentUsage.inputUsdPerMillion / 1_000_000,
    },
    processing: {
      scope: "lifetime recorded LLM metrics; not the local agent sample period",
      calls: llm.reduce((sum, m) => sum + m.totalCalls, 0),
      unreportedCalls: llm.reduce((sum, m) => sum + (m.unreportedUsageCalls ?? 0), 0),
      recordedInputTokens, recordedOutputTokens,
      costUsd: null,
      pricingReason: "Legacy processing metrics lack recorded model, cache categories and tariff metadata.",
    },
    netSavingsUsd: null,
    limitations: [
      "Local agent usage covers available local profiles only, not colleagues or centralized team usage.",
      "API-equivalent valuation, not a subscription refund or invoice; unknown model/platform tariffs are not substituted.",
      "Cache discounts belong to the model cache; they are not agentmemory savings.",
      "Context emission confirms stdout was written, not that the model consumed it.",
      "MCP text replies are counted separately; client transformations, tool schemas and repeated prompt exposures are not attributed.",
      "o200k_base is a local text estimate; model server tokenization and framing may differ.",
      "Selection valuation is a hypothetical exposure at the observed model mix and cache-weighted input rate, not measured monetary savings.",
      "Current direct API tariffs are a dated scenario, not historical charges; missing platform, tier and geography use the explicitly labelled standard/global scenario.",
      "Candidate context is eligible memory before selection, not proven avoided file reads.",
      "Processing metrics may omit failed calls, embeddings and providers without usage reporting.",
      "Net savings require a comparable without-memory baseline and matching processing periods.",
    ],
  };
}

export type SavingsReport = Awaited<ReturnType<typeof readSavingsReport>>;
