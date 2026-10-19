import type { SavingsEvidence } from "./savings-evidence.js";

export type TokenCount = number | null;
export type NumericRange = readonly [number, number] | null;

export interface NetSavingsEstimate {
  avoidedReadTokens: TokenCount;
  contextConsumedTokens: TokenCount;
  processingTokens: TokenCount;
  netTokens: TokenCount;
  denominatorTokens: TokenCount;
  percent: number | null;
}

export interface ContextExposureScenario {
  exposures: 0 | 1;
  contextTokensApplied: TokenCount;
  netTokens: TokenCount;
  percentRange: NumericRange;
}

export interface NetSavingsReport {
  metric: "tokens";
  status: "identified" | "unavailable";
  period: SavingsEvidence["period"];
  scope: string;
  identified: NetSavingsEstimate & { reasons: string[] };
  observed: {
    agentInputTokens: TokenCount;
    agentOutputTokens: TokenCount;
    agentTotalTokens: TokenCount;
    agentPartial: boolean | null;
    contextHookTokens: TokenCount;
    mcpTextTokens: TokenCount;
    contextEmissionTokens: TokenCount;
    candidateTokens: TokenCount;
    selectionReductionTokens: TokenCount;
    processingInputTokens: TokenCount;
    processingOutputTokens: TokenCount;
    processingTokens: TokenCount;
    processingOverlap: string;
    processingDisjointTokens: TokenCount;
    coverage: {
      context: SavingsEvidence["context"];
      processing: SavingsEvidence["processing"];
      agentClients: {
        codex: { partial: boolean; unavailable: boolean; requests: number | null } | null;
        claude: { partial: boolean; unavailable: boolean; requests: number | null } | null;
      };
    };
  };
  processingShareOfObservedSpend: {
    percentRange: NumericRange;
    denominatorRange: NumericRange;
  };
  scenarioSensitivity: {
    kind: "zero-credited-avoided-benefit-contribution";
    label: string;
    creditedAvoidedReadTokens: 0;
    denominatorRange: NumericRange;
    contextExposureScenarios: readonly [ContextExposureScenario, ContextExposureScenario];
    replacement: { status: "unavailable"; pairs: number; reason: string };
    missingCosts: { usdNet: null; reason: string };
  };
  netSavingsUsd: null;
  limitations: string[];
}

interface NetSavingsInputs {
  avoidedReadTokens: TokenCount;
  contextConsumedTokens: TokenCount;
  processingTokens: TokenCount;
  agentInputTokens: TokenCount;
  agentOutputTokens: TokenCount;
  disjointProcessingTokens: TokenCount;
}

function isTokenCount(value: TokenCount): value is number {
  return value !== null && Number.isSafeInteger(value) && value >= 0;
}

function addTokenCounts(...values: TokenCount[]): TokenCount {
  if (!values.every(isTokenCount)) return null;
  const total = values.reduce((sum, value) => sum + value, 0);
  return Number.isSafeInteger(total) ? total : null;
}

function subtractTokenCounts(value: TokenCount, ...costs: TokenCount[]): TokenCount {
  if (!isTokenCount(value)) return null;
  const totalCosts = addTokenCounts(...costs);
  if (totalCosts === null) return null;
  const result = value - totalCosts;
  return Number.isSafeInteger(result) ? result : null;
}

function orderedRange(first: number, second: number): NumericRange {
  if (!Number.isFinite(first) || !Number.isFinite(second)) return null;
  return [Math.min(first, second), Math.max(first, second)];
}

function percentageRange(numerator: number, denominatorRange: NumericRange): NumericRange {
  if (!denominatorRange || denominatorRange.some(value => value <= 0)) return null;
  return orderedRange(100 * numerator / denominatorRange[0], 100 * numerator / denominatorRange[1]);
}

function ratioRange(numerator: TokenCount, denominatorRange: NumericRange): NumericRange {
  if (!isTokenCount(numerator) || !denominatorRange || denominatorRange.some(value => value <= 0)) return null;
  return orderedRange(100 * numerator / denominatorRange[0], 100 * numerator / denominatorRange[1]);
}

export function estimateNetSavings(input: NetSavingsInputs): NetSavingsEstimate {
  const avoidedReadTokens = isTokenCount(input.avoidedReadTokens) ? input.avoidedReadTokens : null;
  const contextConsumedTokens = isTokenCount(input.contextConsumedTokens) ? input.contextConsumedTokens : null;
  const processingTokens = isTokenCount(input.processingTokens) ? input.processingTokens : null;
  const netTokens = subtractTokenCounts(avoidedReadTokens, contextConsumedTokens, processingTokens);
  const denominatorTokens = addTokenCounts(
    input.agentInputTokens,
    input.agentOutputTokens,
    input.disjointProcessingTokens,
  );
  const denominator = denominatorTokens !== null && denominatorTokens > 0 ? denominatorTokens : null;
  return {
    avoidedReadTokens,
    contextConsumedTokens,
    processingTokens,
    netTokens,
    denominatorTokens: denominator,
    percent: netTokens !== null && denominator !== null ? 100 * netTokens / denominator : null,
  };
}

function calculateContextExposureScenario(
  exposures: 0 | 1,
  processingTokens: TokenCount,
  contextEmissionTokens: TokenCount,
  denominatorRange: NumericRange,
): ContextExposureScenario {
  const contextTokensApplied = exposures === 0 ? 0 : contextEmissionTokens;
  const netTokens = isTokenCount(processingTokens) && isTokenCount(contextTokensApplied)
    ? subtractTokenCounts(0, processingTokens, contextTokensApplied) : null;
  return {
    exposures,
    contextTokensApplied,
    netTokens,
    percentRange: netTokens === null ? null : percentageRange(netTokens, denominatorRange),
  };
}

type AgentClientSnapshot = NonNullable<NonNullable<SavingsEvidence["agentUsage"]>["clients"]["codex"]>;

function availableClient(snapshot: AgentClientSnapshot | null)
  : { partial: boolean; unavailable: boolean; requests: number | null } | null {
  if (!snapshot) return null;
  return { partial: snapshot.partial, unavailable: snapshot.unavailable, requests: snapshot.requests };
}

export function buildNetSavingsReport(evidence: SavingsEvidence): NetSavingsReport {
  const agentInputTokens = evidence.agentUsage?.inputTokens ?? null;
  const agentOutputTokens = evidence.agentUsage?.outputTokens ?? null;
  const agentTotalTokens = addTokenCounts(agentInputTokens, agentOutputTokens);
  const contextHookTokens = evidence.context.textTokens;
  const mcpTextTokens = evidence.context.mcpTextTokens;
  const contextEmissionTokens = addTokenCounts(contextHookTokens, mcpTextTokens);
  const processingInputTokens = evidence.processing.reportedInputTokens;
  const processingOutputTokens = evidence.processing.reportedOutputTokens;
  const processingSampleTokens = addTokenCounts(processingInputTokens, processingOutputTokens);
  const denominatorRange = agentTotalTokens !== null && processingSampleTokens !== null
    ? orderedRange(agentTotalTokens, addTokenCounts(agentTotalTokens, processingSampleTokens) ?? NaN)
    : null;
  const estimate = estimateNetSavings({
    avoidedReadTokens: evidence.comparableBaseline.avoidedReadTokens,
    contextConsumedTokens: null,
    processingTokens: null,
    agentInputTokens,
    agentOutputTokens,
    disjointProcessingTokens: evidence.processing.disjointTokens,
  });
  const processingFeedCompleteForPeriod = evidence.processing.coverage !== "bounded-retained-sample";
  const reasons = [
    ...(evidence.comparableBaseline.pairs === 0 ? [evidence.comparableBaseline.reason] : []),
    "Context emissions are not linked to model consumption or repeat exposure.",
    ...(!processingFeedCompleteForPeriod ? ["The bounded processing feed does not establish full-period coverage."] : []),
    ...(evidence.processing.overlapWithAgentUsage === "unknown" ? ["Processing overlap with profile usage is unknown, so a nonduplicated denominator cannot be established."] : []),
  ];
  const scenarios: readonly [ContextExposureScenario, ContextExposureScenario] = [
    calculateContextExposureScenario(0, processingSampleTokens, contextEmissionTokens, denominatorRange),
    calculateContextExposureScenario(1, processingSampleTokens, contextEmissionTokens, denominatorRange),
  ];
  return {
    metric: "tokens",
    status: estimate.netTokens !== null && estimate.percent !== null ? "identified" : "unavailable",
    period: evidence.period,
    scope: evidence.scope,
    identified: { ...estimate, reasons },
    observed: {
      agentInputTokens,
      agentOutputTokens,
      agentTotalTokens,
      agentPartial: evidence.agentUsage?.partial ?? null,
      contextHookTokens,
      mcpTextTokens,
      contextEmissionTokens,
      candidateTokens: evidence.context.candidateTokens,
      selectionReductionTokens: evidence.context.selectionReductionTokens,
      processingInputTokens,
      processingOutputTokens,
      processingTokens: processingSampleTokens,
      processingOverlap: evidence.processing.overlapWithAgentUsage,
      processingDisjointTokens: evidence.processing.disjointTokens,
      coverage: {
        context: evidence.context,
        processing: evidence.processing,
        agentClients: {
          codex: availableClient(evidence.agentUsage?.clients.codex ?? null),
          claude: availableClient(evidence.agentUsage?.clients.claude ?? null),
        },
      },
    },
    processingShareOfObservedSpend: {
      percentRange: ratioRange(processingSampleTokens, denominatorRange),
      denominatorRange,
    },
    scenarioSensitivity: {
      kind: "zero-credited-avoided-benefit-contribution",
      label: "Retained sample contribution over observed period spend; not actual net savings, causal attribution, or a confidence interval.",
      creditedAvoidedReadTokens: 0,
      denominatorRange,
      contextExposureScenarios: scenarios,
      replacement: {
        status: "unavailable",
        pairs: evidence.comparableBaseline.pairs,
        reason: evidence.comparableBaseline.reason,
      },
      missingCosts: {
        usdNet: null,
        reason: "The processing sample has no provider, model, request identity, or tariff metadata for matching-period cost attribution.",
      },
    },
    netSavingsUsd: null,
    limitations: evidence.limitations,
  };
}
