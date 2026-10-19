import type { LlmCallOptions, LlmResponseFormat, MemoryProvider, OpenAIReasoningEffort, OpenAIHttpProfile, StructuredLlmRequest, PreparedStructuredLlmCall } from "../types.js";
import { getEnvVar, isDeepSeekThinkingEnabled, parseOpenAIHttpProfile } from "../config.js";
import { createHash } from "node:crypto";
import { singleStructuredDispatch } from "./structured-output-contract.js";
import { fetchWithTimeout } from "./_fetch.js";
import { readHttpErrorDiagnostic, type HttpErrorDiagnostic } from "./http-error-diagnostic.js";
import { extractLlmTokenUsage, startLlmCallTelemetry } from "./_llm-logging.js";
import { taskOutputTokens } from "./task-output-limits.js";
import { AsyncLocalStorage } from "node:async_hooks";
import { GRAPH_AUTOMATIC_CONTRACT, type GraphAutomaticPreparation, type PreparedGraphAutomaticCall } from "./graph-automatic.js";
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

export interface OpenAIRequestSerializationSettings {
  httpProfile?: OpenAIHttpProfile;
  model: string;
  maxTokens: number;
  baseUrl: string;
  isAzure: boolean;
  azureApiVersion: string;
  reasoningEffort?: string;
  noThink: boolean;
  keepAlive?: string;
  deepSeekThinkingEnabled: boolean;
}

export interface OpenAIRequestSerializationResult {
  endpoint: string;
  body: string;
  thinkingRequest?: "enabled" | "disabled";
}

export function serializeOpenAIRequest(
  settings: OpenAIRequestSerializationSettings,
  operation: "compress" | "summarize",
  systemPrompt: string,
  userPrompt: string,
  options?: LlmCallOptions,
): OpenAIRequestSerializationResult {
  const profile = parseOpenAIHttpProfile(settings.httpProfile);
  if (profile === "validated-json-text" && options?.responseFormat !== undefined
    && (!options.outputContract || typeof options.outputContract.version !== "string"
      || !options.outputContract.version.trim()
      || Object.keys(options.outputContract).length !== 1
      || options.responseFormat.type !== "json_schema" || options.responseFormat.json_schema.strict !== true)) {
    throw new Error("structured_output_binding_required");
  }
  if (profile === "validated-json-text" && options && Object.keys(options).some(key =>
    !["task", "thinking", "outputTokens", "responseFormat", "onUsage", "outputContract"].includes(key))) {
    throw new Error("structured_output_profile_options_incompatible");
  }
  if (profile === "validated-json-text" && (options?.thinking !== undefined || settings.noThink
    || settings.reasoningEffort !== undefined || settings.keepAlive !== undefined)) {
    throw new Error("structured_output_profile_options_incompatible");
  }
  const url = buildChatUrl(settings.baseUrl, settings.isAzure, settings.azureApiVersion);
  const isDeepSeek = isDeepSeekUrl(settings.baseUrl);
  const thinkingOverride = options?.thinking;
  const noThink = thinkingOverride === undefined ? settings.noThink : !thinkingOverride;
  const thinkingRequest = isDeepSeek
    ? (thinkingOverride === undefined
      ? (settings.noThink || !settings.deepSeekThinkingEnabled ? "disabled" : "enabled")
      : (thinkingOverride ? "enabled" : "disabled"))
    : undefined;
  const body: Record<string, unknown> = {
    model: settings.model,
    max_tokens: taskOutputTokens(options?.task, settings.maxTokens, options?.outputTokens),
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
  if (profile === "legacy-native" && noThink) {
    if (isLocalOllamaUrl(settings.baseUrl)) {
      body.think = false;
    } else {
      body.reasoning_effort = "none";
      if (!isDeepSeek) body.think = false;
    }
  } else if (profile === "legacy-native") {
    if (thinkingOverride === true && isLocalOllamaUrl(settings.baseUrl)) {
      body.think = true;
    }
    const reasoningEffort = thinkingOverride === true && settings.reasoningEffort === "none"
      ? undefined
      : settings.reasoningEffort;
    if (reasoningEffort && (!isDeepSeek || thinkingRequest === "enabled")) {
      body.reasoning_effort = reasoningEffort;
    }
  }
  if (profile === "legacy-native" && thinkingRequest) {
    body.thinking = { type: thinkingRequest };
  }
  if (profile === "legacy-native" && settings.keepAlive) body.keep_alive = settings.keepAlive;
  if (options?.responseFormat !== undefined) {
    validateResponseFormat(options.responseFormat);
    if (profile === "legacy-native") body.response_format = options.responseFormat;
  }

  return { endpoint: url, body: JSON.stringify(body), thinkingRequest: profile === "legacy-native" ? thinkingRequest : undefined };
}

export function withStrictGraphResponse<T>(binding: StrictGraphResponseBinding, admit: () => Promise<void>, run: () => Promise<T>): Promise<T> {
  return strictGraphResponse.run({ binding: Object.freeze({ ...binding }), admit, used: false }, run);
}

export class OpenAIHttpError extends Error {
  constructor(readonly status: number, readonly diagnostic: HttpErrorDiagnostic = {}, readonly model?: string) {
    super(`OpenAI API error (${status})`);
    this.name = "OpenAIHttpError";
  }
}

export interface OpenAIProviderOptions {
  httpProfile?: OpenAIHttpProfile;
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
  private readonly httpProfile: OpenAIHttpProfile;

  prepareGraphAutomaticCall?: (request: GraphAutomaticPreparation) => PreparedGraphAutomaticCall;

  private buildGraphAutomaticCall(request: GraphAutomaticPreparation): PreparedGraphAutomaticCall {
    if (this.httpProfile !== "validated-json-text" || !this.apiKey) throw new Error("GRAPH_AUTOMATIC_PROVIDER_UNSUPPORTED");
    const serialized = this.serializeRequest("compress", request.systemPrompt, request.userPrompt, { task: "graph_extraction" });
    const target = new URL(serialized.endpoint);
    if (!["http:", "https:"].includes(target.protocol) || target.username || target.password || target.search || target.hash
      || serialized.endpoint !== `${target.origin}${target.pathname}`) throw new Error("GRAPH_AUTOMATIC_PROVIDER_UNSUPPORTED");
    const contractualEndpoint = `POST ${serialized.endpoint}`;
    const template = JSON.parse(serialized.body) as { messages: Array<{ content: string | null }> };
    template.messages[1].content = null;
    const maxResponseBytes = 64 * 1024;
    const timeoutMs = Math.min(this.timeoutMs, 120_000);
    const declaration = {
      endpoint: contractualEndpoint, model: this.model, timeout_ms: timeoutMs, max_response_bytes: maxResponseBytes,
      value_name_template: request.grouped ? "project-work-response:v3:{unit}" : "provider-response",
      ...(request.grouped ? { unit_pointer: "/continuationState/activeUnit/id" } : {}),
      contract: GRAPH_AUTOMATIC_CONTRACT, request_template: template,
      segments: [{ request_pointer: "/messages/1/content", sources: [{ scope: "provider_values" as const,
        key: "value:{delta}:automatic-source", pointer: "/value" }], prefix: "", separator: "", suffix: "" }],
    };
    const profile = { id: `graph-openai-v1-${createHash("sha256").update(JSON.stringify(declaration)).digest("hex")}`, ...declaration };
    const binding = { request_json: serialized.body, endpoint: contractualEndpoint, model: this.model,
      timeout_ms: timeoutMs, max_response_bytes: maxResponseBytes, contract: GRAPH_AUTOMATIC_CONTRACT, segments: [] };
    let used = false;
    return { provider: this.name, binding, source: request.userPrompt, profile, renew: () => this.buildGraphAutomaticCall(request), regenerationSource: {
      systemPrompt: request.systemPrompt, userPrompt: request.userPrompt, sourceIds: [...(request.sourceIds ?? [])],
      completesSource: request.completesSource, nodesAdded: request.nodesAdded, edgesAdded: request.edgesAdded,
    },
      dispatch: async (permit, capture) => {
        if (used || permit.binding.request_json !== binding.request_json || permit.binding.endpoint !== binding.endpoint
          || permit.binding.model !== binding.model || permit.binding.timeout_ms !== binding.timeout_ms
          || permit.binding.max_response_bytes !== binding.max_response_bytes || permit.timeout_ms <= 0) throw new Error("GRAPH_AUTOMATIC_PROVIDER_BINDING_MISMATCH");
        used = true;
        const response = await fetch(serialized.endpoint, { method: "POST", headers: buildAuthHeaders(this.apiKey, this.isAzure),
          body: serialized.body, redirect: "error", signal: AbortSignal.timeout(permit.timeout_ms) });
        if (!response.ok) { await response.body?.cancel(); throw new OpenAIHttpError(response.status); }
        if (!response.body) throw new Error("GRAPH_AUTOMATIC_RESPONSE_MISSING");
        const reader = response.body.getReader(), decoder = new TextDecoder("utf-8", { fatal: true });
        let bytes = 0;
        try {
          while (true) {
            const part = await reader.read();
            if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes > maxResponseBytes) throw new Error("GRAPH_AUTOMATIC_RESPONSE_LIMIT");
            const text = decoder.decode(part.value, { stream: true });
            if (text) await capture(text);
          }
          const tail = decoder.decode();
          if (tail) await capture(tail);
        } catch (error) { await reader.cancel().catch(() => undefined); throw error; }
        finally { reader.releaseLock(); }
      },
      decode: raw => {
        const result = JSON.parse(raw) as { choices?: Array<{ message?: { content?: unknown } }> };
        const content = result.choices?.[0]?.message?.content;
        if (typeof content !== "string" || !content.trim()) throw new Error("GRAPH_AUTOMATIC_RESPONSE_CONTENT_MISSING");
        return content;
      },
      usage: raw => {
        try {
          const value = JSON.parse(raw) as { usage?: unknown; model?: unknown };
          const usage = extractLlmTokenUsage(value.usage);
          const valid = usage && Object.values(usage).every(count => count === undefined || Number.isSafeInteger(count) && count >= 0);
          return { usage: { ...(valid ? usage : {}), responseChars: raw.length },
            responseModel: typeof value.model === "string" && value.model.length <= 256 ? value.model : null };
        } catch { return { usage: { responseChars: raw.length }, responseModel: null }; }
      },
    };
  }

  constructor(
    apiKey: string,
    model: string,
    maxTokens: number,
    baseURL?: string,
    options: OpenAIProviderOptions = {},
  ) {
    this.apiKey = apiKey;
    this.httpProfile = parseOpenAIHttpProfile(options.httpProfile ?? getEnvVar("OPENAI_HTTP_PROFILE"));
    if (this.httpProfile === "validated-json-text") this.prepareGraphAutomaticCall = this.buildGraphAutomaticCall.bind(this);
    this.model = model;
    this.maxTokens = maxTokens;
    this.baseUrl = normalizeBaseUrl(baseURL || getEnvVar("OPENAI_BASE_URL"));
    this.reasoningEffort = options.reasoningEffort ??
      getEnvVar("OPENAI_REASONING_EFFORT") ?? undefined;
    this.timeoutMs = options.timeoutMs ?? resolveOpenAITimeoutMs();
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

  prepareStructuredCall(request: StructuredLlmRequest): PreparedStructuredLlmCall {
    const { operation, systemPrompt, userPrompt, contractVersion } = request;
    if (!request.contractVersion?.trim() || request.options?.responseFormat?.type !== "json_schema"
      || request.options.responseFormat.json_schema.strict !== true
      || (request.operation !== "compress" && request.operation !== "summarize")
      || typeof request.systemPrompt !== "string" || typeof request.userPrompt !== "string" || !this.apiKey) {
      throw new Error("structured_output_contract_invalid");
    }
    if (request.options.outputContract && request.options.outputContract.version !== contractVersion) {
      throw new Error("structured_output_contract_mismatch");
    }
    validateResponseFormat(request.options.responseFormat);
    const options: LlmCallOptions = {
      ...request.options,
      responseFormat: JSON.parse(JSON.stringify(request.options.responseFormat)) as LlmResponseFormat,
      outputContract: { version: request.contractVersion },
    };
    const prepared = this.serializeRequest(operation, systemPrompt, userPrompt, options);
    return singleStructuredDispatch({
      provider: this.name, model: this.model, endpoint: prepared.endpoint, profile: this.httpProfile,
      operation: request.operation, task: options.task, contractVersion: request.contractVersion,
      responseFormat: JSON.stringify(options.responseFormat), timeoutMs: this.timeoutMs,
      requestDigest: createHash("sha256").update(prepared.body).digest("hex"),
    }, () => this.call(systemPrompt, userPrompt, operation, options, prepared));
  }

  private serializeRequest(
    operation: "compress" | "summarize", systemPrompt: string, userPrompt: string, options?: LlmCallOptions,
  ): OpenAIRequestSerializationResult {
    const isDeepSeek = isDeepSeekUrl(this.baseUrl);
    const deepSeekThinkingEnabled = isDeepSeek && options?.thinking === undefined && !this.noThink
      ? isDeepSeekThinkingEnabled()
      : false;
    return serializeOpenAIRequest({
      httpProfile: this.httpProfile,
      model: this.model,
      maxTokens: this.maxTokens,
      baseUrl: this.baseUrl,
      isAzure: this.isAzure,
      azureApiVersion: this.azureApiVersion,
      reasoningEffort: this.reasoningEffort,
      noThink: this.noThink,
      keepAlive: this.keepAlive,
      deepSeekThinkingEnabled,
    }, operation, systemPrompt, userPrompt, options);
  }

  private async call(
    systemPrompt: string,
    userPrompt: string,
    operation: "compress" | "summarize",
    options?: LlmCallOptions,
    boundRequest?: OpenAIRequestSerializationResult,
  ): Promise<string> {
    if (options?.outputContract && !boundRequest) throw new Error("structured_output_binding_required");
    const prepared = boundRequest ?? this.serializeRequest(operation, systemPrompt, userPrompt, options);
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
      const diagnostic = await readHttpErrorDiagnostic(response);
      throw new OpenAIHttpError(response.status, diagnostic, this.model);
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
    if (typeof content === "string" && (options?.outputContract ? true : options?.responseFormat ? content.trim() : content)) {
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
    if (options?.outputContract || options?.responseFormat) {
      telemetry.failure({ httpStatus: response.status, errorKind: "invalid_response" });
      throw new Error("structured_output_content_missing");
    }
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
export function resolveOpenAITimeoutMs(): number {
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
