import { isLlmLoggingEnabled } from "../config.js";
import { logger } from "../logger.js";

export type LlmTokenUsage = {
  inputTokens?: number;
  cachedInputTokens?: number;
  cacheWriteInputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
};

type LlmCallDetails = {
  provider: string;
  model: string;
  operation: "compress" | "summarize" | "describe_image";
  thinkingRequest?: "enabled" | "disabled";
};

type LlmSuccessDetails = {
  httpStatus?: number;
  usage?: LlmTokenUsage;
  responseChars: number;
  reasoningReturned?: boolean;
};

type LlmFailureDetails = {
  httpStatus?: number;
  errorKind: "network" | "timeout" | "provider_response" | "invalid_response";
};

export function extractLlmTokenUsage(value: unknown): LlmTokenUsage | undefined {
  if (!value || typeof value !== "object") return undefined;
  const usage = value as Record<string, unknown>;
  const detailsValue = usage["prompt_tokens_details"] ?? usage["input_tokens_details"];
  const details = detailsValue && typeof detailsValue === "object" ? detailsValue as Record<string, unknown> : {};
  const cachedInputTokens = validCount(usage["cached_input_tokens"] ?? usage["cache_read_input_tokens"] ?? details["cached_tokens"]);
  const cacheWriteInputTokens = validCount(usage["cache_write_input_tokens"] ?? usage["cache_creation_input_tokens"]);
  let inputTokens = validCount(usage["prompt_tokens"] ?? usage["input_tokens"]);
  // Anthropic input_tokens excludes cached reads/writes; OpenAI prompt_tokens includes them.
  if (inputTokens !== undefined && usage["prompt_tokens"] === undefined
    && (usage["cache_read_input_tokens"] !== undefined || usage["cache_creation_input_tokens"] !== undefined)) {
    inputTokens += (cachedInputTokens ?? 0) + (cacheWriteInputTokens ?? 0);
  }
  const outputTokens = validCount(usage["completion_tokens"] ?? usage["output_tokens"]);
  const totalTokens = validCount(usage["total_tokens"]);
  if (inputTokens === undefined && outputTokens === undefined && totalTokens === undefined) {
    return undefined;
  }
  return {
    inputTokens, outputTokens, totalTokens,
    ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
    ...(cacheWriteInputTokens === undefined ? {} : { cacheWriteInputTokens }),
  };
}

export function startLlmCallTelemetry(details: LlmCallDetails): {
  success: (result: LlmSuccessDetails) => void;
  failure: (result: LlmFailureDetails) => void;
} {
  const enabled = isLlmLoggingEnabled();
  const startedAt = Date.now();
  let completed = false;
  if (enabled) logger.info("LLM call started", details);

  const terminal = (outcome: "completed" | "failed", fields: Record<string, unknown>) => {
    if (!enabled || completed) return;
    completed = true;
    logger.info(`LLM call ${outcome}`, {
      ...details,
      durationMs: Date.now() - startedAt,
      ...fields,
    });
  };

  return {
    success: (result) => terminal("completed", result),
    failure: (result) => terminal("failed", result),
  };
}

function validCount(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : undefined;
}
