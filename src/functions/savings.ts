import type { StateKV } from "../state/kv.js";
import type { FunctionMetrics } from "../types.js";
import { isAgentScopeIsolated, isContextInjectionEnabled } from "../config.js";
import { PRICING_CHECKED_AT, PRICING_SOURCES } from "./usage-pricing.js";
import { readAgentUsage } from "./agent-usage.js";
import { readSavingsEvidence } from "./savings-evidence.js";
import { buildNetSavingsReport } from "./net-savings.js";

async function boundedAgentUsage() {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([readAgentUsage().catch(() => null), new Promise<null>(resolve => {
      timer = setTimeout(() => resolve(null), 1500);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
}

function addKnownTokens(input: number | null, output: number | null): number | null {
  if (input === null || output === null) return null;
  const total = input + output;
  return Number.isSafeInteger(total) ? total : null;
}

export async function readSavingsReport(kv: StateKV, metrics: FunctionMetrics[]) {
  const agentUsage = isAgentScopeIsolated() ? null : await boundedAgentUsage();
  const evidence = await readSavingsEvidence(kv, agentUsage);
  const netSavings = buildNetSavingsReport(evidence);
  const context = {
    source: "hook-stdout-sample", tokenizer: evidence.context.tokenizer, limit: evidence.context.limit,
    emissions: evidence.context.emissions, sessions: null, textTokens: evidence.context.textTokens,
    mcpResponses: evidence.context.mcpResponses, mcpTextTokens: evidence.context.mcpTextTokens,
    meanTokensPerObservedSession: null, medianTokensPerObservedSession: null,
    pairedEmissions: evidence.context.candidateEvents, candidateTokens: evidence.context.candidateTokens,
    pairedEmittedTokens: evidence.context.pairedEmittedTokens,
    selectionReductionTokens: evidence.context.selectionReductionTokens,
    oldest: evidence.context.oldest, newest: evidence.context.newest,
    coverage: {
      status: evidence.context.status, retainedEvents: evidence.context.retainedEvents,
      inPeriodEvents: evidence.context.inPeriodEvents, retainedLimitReached: evidence.context.retainedLimitReached,
      invalidEvents: evidence.context.invalidEvents, duplicateEvents: evidence.context.duplicateEvents,
      limits: evidence.context.limits,
    },
  };
  const processingSample = {
    limit: evidence.processing.limit, calls: evidence.processing.calls,
    unreportedCalls: evidence.processing.missingUsageCalls,
    missingCacheCalls: evidence.processing.missingCacheCalls,
    missingProvenanceCalls: evidence.processing.missingProvenanceCalls,
    inputTokens: evidence.processing.reportedInputTokens,
    outputTokens: evidence.processing.reportedOutputTokens,
    totalTokens: addKnownTokens(evidence.processing.reportedInputTokens, evidence.processing.reportedOutputTokens),
    solEquivalentUsdRange: null,
    overlapWithAgentUsage: evidence.processing.overlapWithAgentUsage,
    disjointTokens: evidence.processing.disjointTokens,
    coverage: {
      status: evidence.processing.status, retainedEvents: evidence.processing.retainedEvents,
      inPeriodEvents: evidence.processing.inPeriodEvents, retainedLimitReached: evidence.processing.retainedLimitReached,
      invalidEvents: evidence.processing.invalidEvents, duplicateEvents: evidence.processing.duplicateEvents,
      oldest: evidence.processing.oldest, newest: evidence.processing.newest,
      limits: evidence.processing.limits,
    },
  };
  const llm = metrics.filter((m) => m.functionId.startsWith("llm:"));
  const recordedInputTokens = llm.reduce((sum, m) => sum + (m.inputTokens ?? 0), 0);
  const recordedOutputTokens = llm.reduce((sum, m) => sum + (m.outputTokens ?? 0), 0);
  return {
    pricing: { basis: "current-direct-api-equivalent", currency: "USD", checkedAt: PRICING_CHECKED_AT,
      sources: PRICING_SOURCES, model: null, effort: null, isInvoice: false },
    context, agentUsage, codex: agentUsage?.clients.codex ?? null,
    processingSample,
    configuration: { serverContextInjectionEnabled: isContextInjectionEnabled(), hookEnvironmentObserved: false },
    selectionScenario: {
      baseline: "all eligible candidate memory before selection; one exposure; excludes processing",
      cachedShortContextUsd: null,
      uncachedShortContextUsd: null,
      profileCacheWeightedUsd: agentUsage?.inputUsdPerMillion == null || context.selectionReductionTokens === null
        ? null : context.selectionReductionTokens * agentUsage.inputUsdPerMillion / 1_000_000,
    },
    processing: {
      scope: "lifetime recorded LLM metrics; not the local agent sample period",
      calls: llm.reduce((sum, m) => sum + m.totalCalls, 0),
      unreportedCalls: llm.reduce((sum, m) => sum + (m.unreportedUsageCalls ?? 0), 0),
      recordedInputTokens, recordedOutputTokens,
      costUsd: null,
      pricingReason: "Legacy processing metrics lack recorded model, cache categories and tariff metadata.",
    },
    netSavings,
    netSavingsUsd: null,
    limitations: [
      ...netSavings.limitations,
      ...netSavings.identified.reasons,
      ...(!agentUsage ? ["The local profile usage sample is pending or unavailable; telemetry is reported independently."] : []),
      "Local agent usage covers available local profiles only, not colleagues or centralized team usage.",
      "API-equivalent valuation is not a subscription refund or invoice; unknown model/platform tariffs are not substituted.",
      "Cache discounts belong to the model cache; they are not agentmemory savings.",
      "Context emission confirms stdout was written, not that the model consumed it.",
      "MCP text replies are counted separately; client transformations, tool schemas and repeat prompt exposures are not attributed.",
      "o200k_base is a local text estimate; model server tokenization and framing may differ.",
      "Selection valuation is a hypothetical exposure at the observed model mix and cache-weighted input rate, not measured monetary savings.",
      "Candidate context is eligible memory before selection, not proven avoided file reads.",
      "Processing sample USD costs are unknown because provider, model, request and tariff metadata are missing.",
      "Processing metrics may omit failed calls, embeddings and providers without usage reporting.",
    ],
  };
}

export type SavingsReport = Awaited<ReturnType<typeof readSavingsReport>>;
