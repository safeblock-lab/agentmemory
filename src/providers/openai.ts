import type { LlmCallOptions, LlmResponseFormat, MemoryProvider, OpenAIReasoningEffort } from "../types.js";
import { getEnvVar, isDeepSeekThinkingEnabled } from "../config.js";
import { fetchWithTimeout } from "./_fetch.js";
import { extractLlmTokenUsage, startLlmCallTelemetry } from "./_llm-logging.js";
import { taskOutputTokens } from "./task-output-limits.js";
import { AsyncLocalStorage } from "node:async_hooks";
import {
  DEFAULT_AZURE_API_VERSION,
  buildAuthHeaders,
  buildChatUrl,
  detectAzure,
  isLocalOllamaUrl,
  normalizeBaseUrl,
} from "./_openai-shared.js";

const DEFAULT_TIMEOUT_MS = 60_000;
interface StrictGraphResponseBinding {
  endpoint: string; model: string; timeoutMs: number; maxRequestBytes: number; maxResponseBytes: number; maxOutputTokens: number;
}
const strictGraphResponse = new AsyncLocalStorage<{ binding: Readonly<StrictGraphResponseBinding>; admit: () => Promise<void>; used: boolean }>();
export function withStrictGraphResponse<T>(binding: StrictGraphResponseBinding, admit: () => Promise<void>, run: () => Promise<T>): Promise<T> {
  return strictGraphResponse.run({ binding: Object.freeze({ ...binding }), admit, used: false }, run);
}

export class OpenAIHttpError extends Error {
  constructor(readonly status: number) {
    super(`OpenAI API error (${status})`);
    this.name = "OpenAIHttpError";
  }
}

export interface OpenAIProviderOptions {
  timeoutMs?: number;
  reasoningEffort?: OpenAIReasoningEffort;
  noThink?: boolean;
  keepAlive?: string;
}

/**
 * OpenAI-compatible LLM provider.
 *
 * Uses raw fetch (no SDK) to support any OpenAI-compatible endpoint:
 *   - OpenAI official
 *   - Azure OpenAI (auto-detected from .openai.azure.com host)
 *   - DeepSeek
 *   - 硅基流动 (SiliconFlow)
 *   - vLLM / LM Studio / Ollama (with OpenAI compatibility layer)
 *   - Any other proxy implementing /v1/chat/completions
 *
 * Required env vars:
 *   OPENAI_API_KEY  — API key
 *
 * Optional:
 *   OPENAI_BASE_URL          — base URL without path (default: https://api.openai.com).
 *                              Azure: https://<resource>.openai.azure.com/openai/deployments/<deployment>
 *   OPENAI_MODEL             — model name (default: gpt-5.6-luna)
 *   OPENAI_API_VERSION       — Azure api-version query param (default: 2024-08-01-preview)
 *   OPENAI_TIMEOUT_MS        — outbound fetch timeout in ms (OpenAI-scoped alias,
 *                              takes precedence over AGENTMEMORY_LLM_TIMEOUT_MS
 *                              for back-compat with the v0.9.17 shipping name).
 *   AGENTMEMORY_LLM_TIMEOUT_MS — outbound fetch timeout in ms shared across all
 *                              raw-fetch LLM + embedding providers. Used when
 *                              OPENAI_TIMEOUT_MS is not set. Default: 60000.
 *   MAX_TOKENS               — max output tokens (default: from config or 4096)
 *   OPENAI_REASONING_EFFORT  — "low" | "medium" | "high" | "none"
 *                              Passthrough for reasoning models (e.g. Ollama Cloud
 *                              thinking models). Set to "none" to ensure
 *                              message.content is populated instead of only
 *                              message.reasoning.
 */
export class OpenAIProvider implements MemoryProvider {
  name = "openai";
  private apiKey: string;
  private model: string;
  private maxTokens: number;
  private baseUrl: string;
  private reasoningEffort?: string;
  private timeoutMs: number;
  private noThink: boolean;
  private keepAlive?: string;
  private isAzure: boolean;
  private azureApiVersion: string;

  constructor(
    apiKey: string,
    model: string,
    maxTokens: number,
    baseURL?: string,
    options: OpenAIProviderOptions = {},
  ) {
    this.apiKey = apiKey;
    this.model = model;
    this.maxTokens = maxTokens;
    this.baseUrl = normalizeBaseUrl(baseURL || getEnvVar("OPENAI_BASE_URL"));
    this.reasoningEffort = options.reasoningEffort ??
      getEnvVar("OPENAI_REASONING_EFFORT") ?? undefined;
    this.timeoutMs = options.timeoutMs ?? resolveTimeout();
    this.noThink = options.noThink ?? false;
    this.keepAlive = options.keepAlive;
    this.azureApiVersion =
      getEnvVar("OPENAI_API_VERSION") || DEFAULT_AZURE_API_VERSION;
    this.isAzure = detectAzure(this.baseUrl);
  }

  async compress(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(systemPrompt, userPrompt, "compress", options);
  }

  async summarize(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(systemPrompt, userPrompt, "summarize", options);
  }

  private serializeRequest(
    operation: "compress" | "summarize", systemPrompt: string, userPrompt: string, options?: LlmCallOptions,
  ): { endpoint: string; body: string; thinkingRequest?: "enabled" | "disabled" } {
    const url = buildChatUrl(this.baseUrl, this.isAzure, this.azureApiVersion);
    const isDeepSeek = isDeepSeekUrl(this.baseUrl);
    const thinkingOverride = options?.thinking;
    const noThink = thinkingOverride === undefined ? this.noThink : !thinkingOverride;
    const thinkingRequest = isDeepSeek
      ? (thinkingOverride === undefined
        ? (this.noThink || !isDeepSeekThinkingEnabled() ? "disabled" : "enabled")
        : (thinkingOverride ? "enabled" : "disabled"))
      : undefined;
    const body: Record<string, unknown> = {
      model: this.model,
      max_tokens: taskOutputTokens(options?.task, this.maxTokens, options?.outputTokens),
      // OpenAI API spec defines `stream` as defaulting to false, so omitting
      // it should yield a JSON response. Some OpenAI-compatible proxies
      // (notably 9Router < 0.4.56 — see decolua/9router#1260) default to
      // text/event-stream when `stream` is absent, which crashes the
      // `response.json()` call below with `Unexpected token 'd', "data: {"id"...`.
      // Send it explicitly so non-spec endpoints route to non-streaming too.
      stream: false,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
    };
    if (noThink) {
      if (isLocalOllamaUrl(this.baseUrl)) {
        body.think = false;
      } else {
        body.reasoning_effort = "none";
        if (!isDeepSeek) body.think = false;
      }
    } else {
      if (thinkingOverride === true && isLocalOllamaUrl(this.baseUrl)) {
        body.think = true;
      }
      const reasoningEffort = thinkingOverride === true && this.reasoningEffort === "none"
        ? undefined
        : this.reasoningEffort;
      if (reasoningEffort && (!isDeepSeek || thinkingRequest === "enabled")) {
        body.reasoning_effort = reasoningEffort;
      }
    }
    if (thinkingRequest) {
      body.thinking = { type: thinkingRequest };
    }
    if (this.keepAlive) body.keep_alive = this.keepAlive;
    if (options?.responseFormat !== undefined) {
      validateResponseFormat(options.responseFormat);
      body.response_format = options.responseFormat;
    }

    return { endpoint: url, body: JSON.stringify(body), thinkingRequest };
  }

  private async call(
    systemPrompt: string,
    userPrompt: string,
    operation: "compress" | "summarize",
    options?: LlmCallOptions,
  ): Promise<string> {
    const prepared = this.serializeRequest(operation, systemPrompt, userPrompt, options);
    const url = prepared.endpoint;
    const strict = strictGraphResponse.getStore();
    if (strict) {
      const binding = strict.binding;
      const body = JSON.parse(prepared.body) as { model?: unknown; max_tokens?: unknown };
      if (strict.used || !this.apiKey || operation !== "compress" || options?.task !== "graph_extraction"
        || url !== binding.endpoint || this.model !== binding.model || body.model !== binding.model
        || !Number.isSafeInteger(body.max_tokens) || Number(body.max_tokens) < 1 || Number(body.max_tokens) > binding.maxOutputTokens
        || Buffer.byteLength(prepared.body) > binding.maxRequestBytes) throw new Error("strict_graph_response_preflight_rejected");
      strict.used = true;
      const deadline = Date.now() + binding.timeoutMs;
      await strict.admit();
      if (Date.now() >= deadline) throw new Error("strict_graph_response_deadline");
      const response = await fetch(url, {
        method: "POST", headers: buildAuthHeaders(this.apiKey, this.isAzure), body: prepared.body,
        redirect: "error", signal: AbortSignal.timeout(Math.max(1, deadline - Date.now())),
      });
      if (!response.ok) { await response.body?.cancel(); throw new OpenAIHttpError(response.status); }
      if (!response.body) throw new Error("strict_graph_response_body_missing");
      const reader = response.body.getReader(), chunks: Uint8Array[] = [];
      let bytes = 0;
      try {
        while (true) {
          const part = await reader.read();
          if (part.done) break;
          bytes += part.value.byteLength;
          if (bytes > binding.maxResponseBytes) { await reader.cancel(); throw new Error("strict_graph_response_limit"); }
          chunks.push(part.value);
        }
      } finally { reader.releaseLock(); }
      const result = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { choices?: Array<{ message?: { content?: unknown } }>; usage?: unknown };
      const content = result.choices?.[0]?.message?.content;
      if (typeof content !== "string" || !content.trim()) throw new Error("strict_graph_response_content_missing");
      options.onUsage?.({ ...extractLlmTokenUsage(result.usage), responseChars: content.length });
      return content;
    }
    const isDeepSeek = isDeepSeekUrl(this.baseUrl);
    const thinkingRequest = prepared.thinkingRequest;
    const telemetry = startLlmCallTelemetry({
      provider: isDeepSeek ? "deepseek" : "openai",
      model: this.model,
      operation,
      thinkingRequest,
    });
    // Bound the request via the shared fetchWithTimeout helper, which
    // owns the AbortController + clearTimeout cleanup for every raw-fetch
    // provider (minimax, openrouter, gemini, openrouter-embed, etc.).
    // OPENAI_TIMEOUT_MS keeps its v0.9.17 meaning (OpenAI-scoped alias,
    // takes precedence); when unset we fall through to
    // AGENTMEMORY_LLM_TIMEOUT_MS and finally the 60s default. See #446.
    let response: Response;
    try {
      response = await fetchWithTimeout(
        url,
        {
          method: "POST",
          headers: buildAuthHeaders(this.apiKey, this.isAzure),
          body: prepared.body,
        },
        this.timeoutMs,
      );
    } catch (err) {
      const aborted = err instanceof Error && (err.name === "AbortError" || err.name === "TimeoutError");
      telemetry.failure({ errorKind: aborted ? "timeout" : "network" });
      if (aborted) {
        throw new Error(
          `OpenAI API request timed out after ${this.timeoutMs}ms — set OPENAI_TIMEOUT_MS (or AGENTMEMORY_LLM_TIMEOUT_MS) to raise the bound or check the provider status.`,
        );
      }
      throw err;
    }

    if (!response.ok) {
      telemetry.failure({ httpStatus: response.status, errorKind: "provider_response" });
      await response.body?.cancel().catch(() => undefined);
      throw new OpenAIHttpError(response.status);
    }

    let data: {
      choices?: Array<{
        message?: { content?: string; reasoning?: string; reasoning_content?: string };
      }>;
      usage?: unknown;
    };
    try {
      data = (await response.json()) as typeof data;
    } catch (error) {
      if (error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError")) {
        telemetry.failure({ errorKind: "timeout" });
        throw new Error(`OpenAI API response timed out after ${this.timeoutMs}ms`);
      }
      telemetry.failure({ httpStatus: response.status, errorKind: "invalid_response" });
      throw new Error("OpenAI returned an invalid JSON response.");
    }
    const message = data.choices?.[0]?.message;
    const content = message?.content;
    if (content) {
      const usage = extractLlmTokenUsage(data.usage);
      telemetry.success({
        httpStatus: response.status,
        usage,
        responseChars: content.length,
        reasoningReturned: Boolean(message?.reasoning || message?.reasoning_content),
      });
      options?.onUsage?.({ ...usage, responseChars: content.length });
      return content;
    }
    // Fallback: some thinking models return reasoning but no content.
    // DeepSeek V4 / Qwen3 / GLM / Kimi return `reasoning_content`;
    // older OpenAI o-series + some compatibles return `reasoning`. #627
    const reasoning = message?.reasoning ?? message?.reasoning_content;
    if (reasoning) {
      const usage = extractLlmTokenUsage(data.usage);
      telemetry.success({
        httpStatus: response.status,
        usage,
        responseChars: reasoning.length,
        reasoningReturned: true,
      });
      options?.onUsage?.({ ...usage, responseChars: reasoning.length });
      return reasoning;
    }
    telemetry.failure({ httpStatus: response.status, errorKind: "invalid_response" });
    throw new Error(
      `OpenAI returned unexpected response: ${JSON.stringify(data).slice(0, 200)}`,
    );
  }
}

function isDeepSeekUrl(baseUrl: string): boolean {
  try {
    const hostname = new URL(baseUrl).hostname.toLowerCase().replace(/\.+$/, "");
    return hostname === "deepseek.com" || hostname.endsWith(".deepseek.com");
  } catch {
    return false;
  }
}

function validateResponseFormat(value: unknown): asserts value is LlmResponseFormat {
  if (!isPlainRecord(value)) throw new TypeError("Invalid OpenAI response format.");
  if (value.type === "json_object") {
    if (!hasOnlyOwnKeys(value, ["type"])) throw new TypeError("Invalid OpenAI response format.");
    return;
  }
  if (value.type !== "json_schema" || !hasOnlyOwnKeys(value, ["type", "json_schema"])) {
    throw new TypeError("Invalid OpenAI response format.");
  }

  const jsonSchema = value.json_schema;
  if (!isPlainRecord(jsonSchema) || !hasOnlyOwnKeys(jsonSchema, ["name", "strict", "schema"]) ||
    typeof jsonSchema.name !== "string" || jsonSchema.name.trim().length === 0 ||
    typeof jsonSchema.strict !== "boolean" || !isJsonSchema(jsonSchema.schema)) {
    throw new TypeError("Invalid OpenAI JSON schema response format.");
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function hasOnlyOwnKeys(value: object, keys: string[]): boolean {
  const ownKeys = Reflect.ownKeys(value);
  return ownKeys.length === keys.length && keys.every(key => Object.hasOwn(value, key));
}

function isJsonSchema(value: unknown, ancestors = new Set<object>()): value is Record<string, unknown> {
  if (!isPlainRecord(value) || ancestors.has(value)) return false;
  ancestors.add(value);
  try {
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string") return false;
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !("value" in descriptor) || !isJsonValue(descriptor.value, ancestors)) {
        return false;
      }
    }
    return true;
  } finally {
    ancestors.delete(value);
  }
}

function isJsonValue(value: unknown, ancestors: Set<object>): boolean {
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (typeof value !== "object" || ancestors.has(value)) return false;
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Object.getPrototypeOf(value) !== Array.prototype || Reflect.ownKeys(value).length !== value.length + 1) {
        return false;
      }
      for (let index = 0; index < value.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor?.enumerable || !("value" in descriptor) || !isJsonValue(descriptor.value, ancestors)) {
          return false;
        }
      }
      return true;
    }
    if (!isPlainRecord(value)) return false;
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== "string") return false;
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !("value" in descriptor) || !isJsonValue(descriptor.value, ancestors)) {
        return false;
      }
    }
    return true;
  } finally {
    ancestors.delete(value);
  }
}

// Resolves the outbound-fetch timeout for the OpenAI LLM path.
// Precedence (preserving v0.9.17 behaviour):
//   1. OPENAI_TIMEOUT_MS       — OpenAI-scoped alias (back-compat)
//   2. AGENTMEMORY_LLM_TIMEOUT_MS — global LLM/embedding timeout (#446)
//   3. 60 000 ms default
function resolveTimeout(): number {
  const openaiRaw = getEnvVar("OPENAI_TIMEOUT_MS");
  const openai = parsePositiveInt(openaiRaw);
  if (openai !== undefined) return openai;

  const globalRaw = getEnvVar("AGENTMEMORY_LLM_TIMEOUT_MS");
  const globalMs = parsePositiveInt(globalRaw);
  if (globalMs !== undefined) return globalMs;

  return DEFAULT_TIMEOUT_MS;
}

function parsePositiveInt(raw: string | null | undefined): number | undefined {
  if (!raw) return undefined;
  const trimmed = raw.trim();
  // Reject malformed values like "30ms" or "1_000" — parseInt would
  // silently return 30 / 1, swallowing user typos as valid timeouts.
  // The regex enforces pure digits (no sign, no trailing units, no
  // separators) before we hand off to Number.
  if (!/^\d+$/.test(trimmed)) return undefined;
  const n = Number(trimmed);
  return Number.isFinite(n) && n > 0 ? n : undefined;
}
