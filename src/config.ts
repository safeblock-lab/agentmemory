import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import pc from "picocolors";
import type {
  AgentMemoryConfig,
  ProviderConfig,
  EmbeddingConfig,
  FallbackConfig,
  ClaudeBridgeConfig,
  TeamConfig,
  AuxiliaryLlmConfig,
  FireworksBatchConfig,
  LlmRouteTarget,
  LlmRoutingConfig,
  LlmTask,
  OpenAIReasoningEffort,
  SummaryBudgetConfig,
} from "./types.js";

function safeParseInt(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = parseInt(value, 10);
  return Number.isNaN(parsed) ? fallback : parsed;
}

const DATA_DIR = join(homedir(), ".agentmemory");
const ENV_FILE = join(DATA_DIR, ".env");
const OPENROUTER_KEYS_FILE = join(DATA_DIR, "openrouter-keys.json");

let warnPremiumModelShown = false;

const AUX_LLM_DEFAULT_TIMEOUT_MS = 60_000;
const AUX_LLM_DEFAULT_MAX_TOKENS = 4096;
const AUX_LLM_DEFAULT_MAX_INPUT_CHARS = 120_000;
const AUX_LLM_MAX_TIMEOUT_MS = 10 * 60_000;
const AUX_LLM_MAX_TOKENS = 1_000_000;
const AUX_LLM_MAX_INPUT_CHARS = 10_000_000;

const FIREWORKS_BATCH_DEFAULT_TIMEOUT_MS = 120_000;
const FIREWORKS_BATCH_DEFAULT_MIN_ITEMS = 16;
const FIREWORKS_BATCH_DEFAULT_MAX_WAIT_MS = 60 * 60_000;
const FIREWORKS_BATCH_DEFAULT_MAX_ITEMS = 32;
const FIREWORKS_BATCH_DEFAULT_MAX_REQUEST_CHARS = 120_000;
const FIREWORKS_BATCH_DEFAULT_MAX_REQUEST_BYTES = 8 * 1024 * 1024;
const FIREWORKS_BATCH_DEFAULT_MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
const FIREWORKS_BATCH_DEFAULT_MAX_RESULT_CHARS = 120_000;
const FIREWORKS_BATCH_DEFAULT_MAX_CONCURRENCY = 1;
const FIREWORKS_BATCH_DEFAULT_MAX_ATTEMPTS = 3;
const FIREWORKS_BATCH_DEFAULT_RETRY_BASE_MS = 2_000;
const FIREWORKS_BATCH_DEFAULT_RETRY_MAX_MS = 60_000;
const FIREWORKS_BATCH_DEFAULT_POLL_INTERVAL_MS = 60_000;
const FIREWORKS_BATCH_DEFAULT_POLL_MAX_INTERVAL_MS = 120_000;
const FIREWORKS_BATCH_DEFAULT_POLL_DEADLINE_MS = 24 * 60 * 60_000;
const FIREWORKS_BATCH_DEFAULT_RECOVERY_STALE_MS = 15 * 60_000;
const FIREWORKS_BATCH_DEFAULT_MAX_QUEUED_ITEMS = 1_000;
const GRAPH_EXTRACTION_DEFAULT_INPUT_TARGET_CHARS = 32_000;
const CONSOLIDATION_DEFAULT_MIN_NEW_SUMMARIES = 5;
const TYPESAFE_DEFAULT_TIMEOUT_MS = 5_000;
const TYPESAFE_MAX_TIMEOUT_MS = 30_000;
const TYPESAFE_DEFAULT_MAX_STATE_CHARS = 16_000;
const TYPESAFE_MAX_STATE_CHARS = 64_000;
export const TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD = 0.75;
export const TYPESAFE_COMPACTION_CONFIDENCE_THRESHOLD = 0.85;
export const TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD = 0.60;
export const TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD = 0.60;
export const TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD = 0.65;
export const TYPESAFE_REFLECTION_GATE_CONFIDENCE_THRESHOLD = 0.80;
export const TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD = 0.75;
export const TYPESAFE_SCORING_CONFIDENCE_THRESHOLD = 0.55;

const TYPESAFE_FEATURE_ENV = {
  compaction: "AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED",
  admission: "AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED",
  pipelineGates: "AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED",
  scoring: "AGENTMEMORY_TYPESAFE_SCORING_ENABLED",
} as const;

export type TypeSafeFeature = keyof typeof TYPESAFE_FEATURE_ENV;

export interface TypeSafeConfig {
  enabled: boolean;
  apiKey: string;
  timeoutMs: number;
  maxStateChars: number;
  features: Record<TypeSafeFeature, boolean>;
}

const LLM_ROUTE_ENV = {
  graph_extraction: "AGENTMEMORY_GRAPH_LLM",
  temporal_graph_extraction: "AGENTMEMORY_TEMPORAL_GRAPH_LLM",
  consolidation: "AGENTMEMORY_CONSOLIDATION_LLM",
  compression: "AGENTMEMORY_COMPRESSION_LLM",
  summary: "AGENTMEMORY_SUMMARY_LLM",
  entity_extraction: "AGENTMEMORY_ENTITY_EXTRACTION_LLM",
  classification: "AGENTMEMORY_CLASSIFICATION_LLM",
  reflection: "AGENTMEMORY_REFLECTION_LLM",
  conflict_resolution: "AGENTMEMORY_CONFLICT_RESOLUTION_LLM",
  skill_extraction: "AGENTMEMORY_SKILL_EXTRACTION_LLM",
  query_expansion: "AGENTMEMORY_QUERY_EXPANSION_LLM",
  flow_compression: "AGENTMEMORY_FLOW_COMPRESSION_LLM",
} as const satisfies Record<LlmTask, string>;

const LLM_THINKING_ENV = {
  graph_extraction: `${LLM_ROUTE_ENV.graph_extraction}_THINKING`,
  temporal_graph_extraction: `${LLM_ROUTE_ENV.temporal_graph_extraction}_THINKING`,
  consolidation: `${LLM_ROUTE_ENV.consolidation}_THINKING`,
  compression: `${LLM_ROUTE_ENV.compression}_THINKING`,
  summary: `${LLM_ROUTE_ENV.summary}_THINKING`,
  entity_extraction: `${LLM_ROUTE_ENV.entity_extraction}_THINKING`,
  classification: `${LLM_ROUTE_ENV.classification}_THINKING`,
  reflection: `${LLM_ROUTE_ENV.reflection}_THINKING`,
  conflict_resolution: `${LLM_ROUTE_ENV.conflict_resolution}_THINKING`,
  skill_extraction: `${LLM_ROUTE_ENV.skill_extraction}_THINKING`,
  query_expansion: `${LLM_ROUTE_ENV.query_expansion}_THINKING`,
  flow_compression: `${LLM_ROUTE_ENV.flow_compression}_THINKING`,
} as const satisfies Record<LlmTask, string>;

const DEFAULT_LLM_ROUTES = {
  graph_extraction: "aux",
  temporal_graph_extraction: "aux",
  consolidation: "aux",
  compression: "aux",
  summary: "aux",
  entity_extraction: "aux",
  classification: "aux",
  reflection: "primary",
  conflict_resolution: "primary",
  skill_extraction: "aux",
  query_expansion: "aux",
  flow_compression: "aux",
} as const satisfies Record<LlmTask, LlmRouteTarget>;

type EnvSource = Record<string, string | undefined>;

interface AuxiliaryLlmConfigResult {
  config?: AuxiliaryLlmConfig;
  warnings: string[];
}

interface FireworksBatchConfigResult {
  config: FireworksBatchConfig;
  warnings: string[];
}
// Parsed ~/.agentmemory/.env, memoized for the process lifetime. getMergedEnv()
// runs on every config getter (~20 of them), so without this cache a single
// request would readFileSync + reparse the file dozens of times. The file is
// boot-static, so read it from disk once and reuse the result. Tests that
// mutate the file between cases reset the module (clearing this via reload) or
// call __resetEnvFileCache().
let envFileCache: Record<string, string> | undefined;

function loadEnvFile(): Record<string, string> {
  if (envFileCache) return envFileCache;
  if (!existsSync(ENV_FILE)) {
    envFileCache = {};
    return envFileCache;
  }
  const content = readFileSync(ENV_FILE, "utf-8");
  const vars: Record<string, string> = {};
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let val = trimmed.slice(eqIdx + 1).trim();
    const quoteChar = val[0] === '"' || val[0] === "'" ? val[0] : "";
    if (quoteChar) {
      const closeIdx = val.indexOf(quoteChar, 1);
      if (closeIdx !== -1) val = val.slice(1, closeIdx);
    } else {
      const hashIdx = val.indexOf(" #");
      if (hashIdx !== -1) val = val.slice(0, hashIdx).trim();
    }
    vars[key] = val;
  }
  envFileCache = vars;
  return envFileCache;
}

// Test hook: clears the memoized .env so the next loadEnvFile() re-reads disk
// within the same module instance. vi.resetModules() reloads this module and
// resets the cache on its own; this exists for tests that mutate the file
// without a module reload.
export function __resetEnvFileCache(): void {
  envFileCache = undefined;
}

function hasRealValue(v: string | undefined): v is string {
  return typeof v === "string" && v.trim().length > 0;
}

function parseBoundedAuxInt(
  env: EnvSource,
  key: string,
  fallback: number,
  max: number,
  warnings: string[],
): number {
  const raw = env[key];
  if (!hasRealValue(raw)) return fallback;
  const trimmed = raw.trim();
  if (!/^\d+$/.test(trimmed)) {
    warnings.push(`${key} must be a positive integer; using ${fallback}.`);
    return fallback;
  }
  const parsed = Number(trimmed);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > max) {
    warnings.push(`${key} must be between 1 and ${max}; using ${fallback}.`);
    return fallback;
  }
  return parsed;
}

function parseAuxBoolean(
  env: EnvSource,
  key: string,
  fallback: boolean,
  warnings: string[],
): boolean {
  const raw = env[key];
  if (!hasRealValue(raw)) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return true;
  if (normalized === "false" || normalized === "0") return false;
  warnings.push(`${key} must be true, false, 1, or 0; using ${fallback}.`);
  return fallback;
}

function parseOptionalAuxBoolean(
  env: EnvSource,
  key: string,
  warnings: string[],
): boolean | undefined {
  const raw = env[key];
  if (!hasRealValue(raw)) return undefined;
  const normalized = raw.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return true;
  if (normalized === "false" || normalized === "0") return false;
  warnings.push(`${key} must be true, false, 1, or 0; ignoring thinking override.`);
  return undefined;
}

function parseAuxBaseUrl(raw: string | undefined): string | undefined {
  if (!hasRealValue(raw) || raw.length > 2048) return undefined;
  try {
    const url = new URL(raw.trim());
    if (url.protocol !== "http:" && url.protocol !== "https:") return undefined;
    if (url.username || url.password) return undefined;
    return url.toString().replace(/\/+$/, "");
  } catch {
    return undefined;
  }
}

function isLocalOllamaBaseUrl(baseURL: string): boolean {
  try {
    const url = new URL(baseURL);
    return url.protocol === "http:"
      && url.port === "11434"
      && ["localhost", "127.0.0.1", "[::1]", "::1"].includes(url.hostname);
  } catch {
    return false;
  }
}

function parseKeepAlive(raw: string | undefined): string | undefined {
  if (!hasRealValue(raw) || raw.length > 64) return undefined;
  const trimmed = raw.trim();
  return /^(?:-1|0|(?:\d+(?:\.\d+)?(?:ms|s|m|h))+)$/.test(trimmed)
    ? trimmed
    : undefined;
}

function parseAuxiliaryLlmConfig(env: EnvSource): AuxiliaryLlmConfigResult {
  const warnings: string[] = [];
  const auxKeys = [
    "AGENTMEMORY_AUX_LLM_PROVIDER",
    "AGENTMEMORY_AUX_LLM_BASE_URL",
    "AGENTMEMORY_AUX_LLM_API_KEY",
    "AGENTMEMORY_AUX_LLM_MODEL",
    "AGENTMEMORY_AUX_LLM_TIMEOUT_MS",
    "AGENTMEMORY_AUX_LLM_MAX_TOKENS",
    "AGENTMEMORY_AUX_LLM_MAX_INPUT_CHARS",
    "AGENTMEMORY_AUX_LLM_REASONING_EFFORT",
    "AGENTMEMORY_AUX_LLM_NOTHINK",
    "AGENTMEMORY_AUX_LLM_KEEP_ALIVE",
  ];
  if (!auxKeys.some((key) => hasRealValue(env[key]))) {
    return { warnings };
  }

  const baseURL = parseAuxBaseUrl(env["AGENTMEMORY_AUX_LLM_BASE_URL"]);
  const model = env["AGENTMEMORY_AUX_LLM_MODEL"]?.trim();
  if (!baseURL || !model || model.length > 512) {
    warnings.push(
      "Auxiliary LLM configuration ignored: AGENTMEMORY_AUX_LLM_BASE_URL must be an absolute http(s) URL without credentials and AGENTMEMORY_AUX_LLM_MODEL must be set.",
    );
    return { warnings };
  }

  const providerRaw = env["AGENTMEMORY_AUX_LLM_PROVIDER"]?.trim().toLowerCase();
  const provider = providerRaw || (isLocalOllamaBaseUrl(baseURL) ? "ollama" : "openai");
  if (provider !== "openai" && provider !== "ollama") {
    warnings.push("AGENTMEMORY_AUX_LLM_PROVIDER must be openai or ollama; auxiliary configuration ignored.");
    return { warnings };
  }
  if (provider === "ollama" && !isLocalOllamaBaseUrl(baseURL)) {
    warnings.push("AGENTMEMORY_AUX_LLM_PROVIDER=ollama requires a local http://localhost:11434 base URL; auxiliary configuration ignored.");
    return { warnings };
  }

  const timeoutMs = parseBoundedAuxInt(
    env,
    "AGENTMEMORY_AUX_LLM_TIMEOUT_MS",
    AUX_LLM_DEFAULT_TIMEOUT_MS,
    AUX_LLM_MAX_TIMEOUT_MS,
    warnings,
  );
  const maxTokens = parseBoundedAuxInt(
    env,
    "AGENTMEMORY_AUX_LLM_MAX_TOKENS",
    AUX_LLM_DEFAULT_MAX_TOKENS,
    AUX_LLM_MAX_TOKENS,
    warnings,
  );
  const maxInputChars = parseBoundedAuxInt(
    env,
    "AGENTMEMORY_AUX_LLM_MAX_INPUT_CHARS",
    AUX_LLM_DEFAULT_MAX_INPUT_CHARS,
    AUX_LLM_MAX_INPUT_CHARS,
    warnings,
  );
  const noThink = parseAuxBoolean(
    env,
    "AGENTMEMORY_AUX_LLM_NOTHINK",
    provider === "ollama",
    warnings,
  );

  const reasoningRaw = env["AGENTMEMORY_AUX_LLM_REASONING_EFFORT"]?.trim().toLowerCase();
  const validReasoning = new Set<OpenAIReasoningEffort>([
    "none",
    "low",
    "medium",
    "high",
  ]);
  const reasoningEffort = reasoningRaw && validReasoning.has(reasoningRaw as OpenAIReasoningEffort)
    ? reasoningRaw as OpenAIReasoningEffort
    : undefined;
  if (reasoningRaw && !reasoningEffort) {
    warnings.push(
      "AGENTMEMORY_AUX_LLM_REASONING_EFFORT must be none, low, medium, or high; omitting it.",
    );
  }
  if (noThink && reasoningEffort && reasoningEffort !== "none") {
    warnings.push(
      "AGENTMEMORY_AUX_LLM_NOTHINK overrides AGENTMEMORY_AUX_LLM_REASONING_EFFORT with none.",
    );
  }

  const keepAliveRaw = env["AGENTMEMORY_AUX_LLM_KEEP_ALIVE"];
  const keepAlive = parseKeepAlive(keepAliveRaw);
  if (hasRealValue(keepAliveRaw) && !keepAlive) {
    warnings.push(
      "AGENTMEMORY_AUX_LLM_KEEP_ALIVE must be -1, 0, or a duration using ms, s, m, or h; omitting it.",
    );
  }

  return {
    config: {
      provider,
      baseURL,
      apiKey: env["AGENTMEMORY_AUX_LLM_API_KEY"]?.trim() || "",
      model,
      timeoutMs,
      maxTokens,
      maxInputChars,
      reasoningEffort: noThink ? "none" : reasoningEffort,
      noThink,
      keepAlive,
    },
    warnings,
  };
}

function parseFireworksBatchConfig(
  env: EnvSource,
  auxiliaryProvider: AuxiliaryLlmConfig | undefined,
): FireworksBatchConfigResult {
  const warnings: string[] = [];
  const parse = (key: string, fallback: number, max: number): number =>
    parseBoundedAuxInt(env, key, fallback, max, warnings);

  const defaults: FireworksBatchConfig = {
    enabled: false,
    timeoutMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_TIMEOUT_MS",
      FIREWORKS_BATCH_DEFAULT_TIMEOUT_MS,
      10 * 60_000,
    ),
    minBatchItems: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MIN_ITEMS",
      FIREWORKS_BATCH_DEFAULT_MIN_ITEMS,
      FIREWORKS_BATCH_DEFAULT_MAX_ITEMS,
    ),
    maxWaitMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_WAIT_MS",
      FIREWORKS_BATCH_DEFAULT_MAX_WAIT_MS,
      24 * 60 * 60_000,
    ),
    maxBatchItems: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_ITEMS",
      FIREWORKS_BATCH_DEFAULT_MAX_ITEMS,
      1_000,
    ),
    maxRequestChars: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_REQUEST_CHARS",
      FIREWORKS_BATCH_DEFAULT_MAX_REQUEST_CHARS,
      10_000_000,
    ),
    maxRequestBytes: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_REQUEST_BYTES",
      FIREWORKS_BATCH_DEFAULT_MAX_REQUEST_BYTES,
      128 * 1024 * 1024,
    ),
    maxResponseBytes: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_RESPONSE_BYTES",
      FIREWORKS_BATCH_DEFAULT_MAX_RESPONSE_BYTES,
      128 * 1024 * 1024,
    ),
    maxResultChars: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_RESULT_CHARS",
      FIREWORKS_BATCH_DEFAULT_MAX_RESULT_CHARS,
      10_000_000,
    ),
    maxConcurrency: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_CONCURRENCY",
      FIREWORKS_BATCH_DEFAULT_MAX_CONCURRENCY,
      32,
    ),
    maxAttempts: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_ATTEMPTS",
      FIREWORKS_BATCH_DEFAULT_MAX_ATTEMPTS,
      10,
    ),
    retryBaseMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_RETRY_BASE_MS",
      FIREWORKS_BATCH_DEFAULT_RETRY_BASE_MS,
      10 * 60_000,
    ),
    retryMaxMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_RETRY_MAX_MS",
      FIREWORKS_BATCH_DEFAULT_RETRY_MAX_MS,
      60 * 60_000,
    ),
    pollIntervalMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_POLL_INTERVAL_MS",
      FIREWORKS_BATCH_DEFAULT_POLL_INTERVAL_MS,
      60 * 60_000,
    ),
    pollMaxIntervalMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_POLL_MAX_INTERVAL_MS",
      FIREWORKS_BATCH_DEFAULT_POLL_MAX_INTERVAL_MS,
      24 * 60 * 60_000,
    ),
    pollDeadlineMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_POLL_DEADLINE_MS",
      FIREWORKS_BATCH_DEFAULT_POLL_DEADLINE_MS,
      FIREWORKS_BATCH_DEFAULT_POLL_DEADLINE_MS,
    ),
    recoveryStaleMs: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_RECOVERY_STALE_MS",
      FIREWORKS_BATCH_DEFAULT_RECOVERY_STALE_MS,
      7 * 24 * 60 * 60_000,
    ),
    maxQueuedItems: parse(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_QUEUED_ITEMS",
      FIREWORKS_BATCH_DEFAULT_MAX_QUEUED_ITEMS,
      100_000,
    ),
  };

  const enabledRaw = env["AGENTMEMORY_FIREWORKS_BATCH_ENABLED"];
  defaults.minBatchItems = Math.min(defaults.minBatchItems, defaults.maxBatchItems);
  const explicitlyEnabled = hasRealValue(enabledRaw) &&
    (enabledRaw!.trim().toLowerCase() === "true" || enabledRaw!.trim() === "1");
  const explicitlyDisabled = hasRealValue(enabledRaw) &&
    (enabledRaw!.trim().toLowerCase() === "false" || enabledRaw!.trim() === "0");
  if (hasRealValue(enabledRaw) && !explicitlyEnabled && !explicitlyDisabled) {
    warnings.push(
      "AGENTMEMORY_FIREWORKS_BATCH_ENABLED must be true, false, 1, or 0; Batch remains disabled.",
    );
  }
  if (!explicitlyEnabled) return { config: defaults, warnings };

  const accountRaw =
    env["AGENTMEMORY_FIREWORKS_BATCH_ACCOUNT_ID"] || env["FIREWORKS_ACCOUNT_ID"];
  const accountId = accountRaw?.trim();
  if (!accountId || accountId.length > 128 || !/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(accountId)) {
    warnings.push(
      "Fireworks Batch ignored: set AGENTMEMORY_FIREWORKS_BATCH_ACCOUNT_ID (or FIREWORKS_ACCOUNT_ID) to a valid account identifier.",
    );
    return { config: defaults, warnings };
  }
  let primaryHost = "";
  try {
    primaryHost = new URL(env["OPENAI_BASE_URL"] ?? "").hostname.toLowerCase();
  } catch {}
  const primaryIsFireworks = primaryHost === "api.fireworks.ai";
  const configuredApiKey =
    env["AGENTMEMORY_FIREWORKS_BATCH_API_KEY"]?.trim() ||
    env["FIREWORKS_API_KEY"]?.trim() ||
    (primaryIsFireworks ? env["OPENAI_API_KEY"]?.trim() : undefined);
  const configuredModel =
    env["AGENTMEMORY_FIREWORKS_BATCH_MODEL"]?.trim() ||
    env["FIREWORKS_MODEL"]?.trim() ||
    (primaryIsFireworks ? env["OPENAI_MODEL"]?.trim() : undefined);
  let apiKey = configuredApiKey;
  let model = configuredModel;
  let auxiliaryHost = "";
  try {
    auxiliaryHost = auxiliaryProvider ? new URL(auxiliaryProvider.baseURL).hostname.toLowerCase() : "";
  } catch {}
  if ((!apiKey || !model) && auxiliaryHost === "api.fireworks.ai") {
    apiKey ||= auxiliaryProvider?.apiKey;
    model ||= auxiliaryProvider?.model;
  }
  if (!apiKey || !model) {
    warnings.push(
      "Fireworks Batch ignored: set Fireworks credentials/model directly, or configure OPENAI_BASE_URL=https://api.fireworks.ai with OPENAI_API_KEY and OPENAI_MODEL. A Fireworks auxiliary provider remains a legacy fallback.",
    );
    return { config: defaults, warnings };
  }
  if (defaults.retryMaxMs < defaults.retryBaseMs) {
    warnings.push(
      "AGENTMEMORY_FIREWORKS_BATCH_RETRY_MAX_MS must be at least AGENTMEMORY_FIREWORKS_BATCH_RETRY_BASE_MS; using the retry base for the maximum.",
    );
    defaults.retryMaxMs = defaults.retryBaseMs;
  }
  if (defaults.pollMaxIntervalMs < defaults.pollIntervalMs) {
    warnings.push(
      "AGENTMEMORY_FIREWORKS_BATCH_POLL_MAX_INTERVAL_MS must be at least AGENTMEMORY_FIREWORKS_BATCH_POLL_INTERVAL_MS; using the poll interval for the maximum.",
    );
    defaults.pollMaxIntervalMs = defaults.pollIntervalMs;
  }
  if (defaults.maxRequestBytes < defaults.maxRequestChars) {
    warnings.push(
      "AGENTMEMORY_FIREWORKS_BATCH_MAX_REQUEST_BYTES is below the character limit; request bytes remain the enforced lower bound.",
    );
  }

  return {
    config: {
      ...defaults,
      enabled: true,
      accountId,
      apiKey,
      model,
    },
    warnings,
  };
}

function parseLlmRoutingConfig(
  env: EnvSource,
  hasAuxiliaryProvider: boolean,
  initialWarnings: string[] = [],
): LlmRoutingConfig {
  const warnings = [...initialWarnings];
  const routes = { ...DEFAULT_LLM_ROUTES } as Record<LlmTask, LlmRouteTarget>;
  const explicitRoutes: Partial<Record<LlmTask, LlmRouteTarget>> = {};
  const thinking: Partial<Record<LlmTask, boolean>> = {};
  let explicitlyRequestsAuxiliary = false;

  for (const task of Object.keys(LLM_ROUTE_ENV) as LlmTask[]) {
    const key = LLM_ROUTE_ENV[task];
    const raw = env[key];
    if (!hasRealValue(raw)) continue;
    const value = raw.trim().toLowerCase();
    if (value !== "primary" && value !== "aux") {
      warnings.push(`${key} must be primary or aux; using ${routes[task]}.`);
      continue;
    }
    routes[task] = value;
    explicitRoutes[task] = value;
    explicitlyRequestsAuxiliary ||= value === "aux";
  }

  for (const task of Object.keys(LLM_THINKING_ENV) as LlmTask[]) {
    const value = parseOptionalAuxBoolean(env, LLM_THINKING_ENV[task], warnings);
    if (value !== undefined) thinking[task] = value;
  }

  if (explicitlyRequestsAuxiliary && !hasAuxiliaryProvider) {
    warnings.push(
      "An LLM route explicitly selects aux, but no valid auxiliary provider is configured; primary fallback will be used.",
    );
  }

  return { routes, explicitRoutes, thinking, warnings };
}

// Hydrate ~/.agentmemory/.env into process.env at boot. loadEnvFile() is
// otherwise only consumed via getMergedEnv(), which the many modules that
// read raw process.env["X"] never call — so .env-only values were silently
// ignored by them. Copy the file's vars into process.env, but only when the
// key is currently unset so a real process.env value still wins (this
// preserves the {...fileEnv, ...process.env} precedence getMergedEnv uses).
export function hydrateProcessEnvFromFile(): void {
  for (const [k, v] of Object.entries(loadEnvFile())) {
    if (process.env[k] === undefined) process.env[k] = v;
  }
}

function detectProvider(env: Record<string, string>): ProviderConfig {
  const maxTokens = parseInt(env["MAX_TOKENS"] || "4096", 10);

  if (hasRealValue(env["AGENTMEMORY_GEMINI_ACCOUNTS_DIR"])) {
    return {
      provider: "gemini",
      model: env["GEMINI_MODEL"] || "gemini-3.7-flash",
      maxTokens,
      geminiAccountsDir: env["AGENTMEMORY_GEMINI_ACCOUNTS_DIR"].trim(),
      openRouterKeysFile: existsSync(OPENROUTER_KEYS_FILE)
        ? OPENROUTER_KEYS_FILE
        : undefined,
    };
  }

  // OpenAI-compatible: supports OpenAI, DeepSeek, SiliconFlow, Azure, vLLM, LM Studio
  if (hasRealValue(env["OPENAI_API_KEY"]) && env["OPENAI_API_KEY_FOR_LLM"] !== "false") {
    return {
      provider: "openai",
      model: env["OPENAI_MODEL"] || "gpt-5.6-luna",
      maxTokens,
      baseURL: env["OPENAI_BASE_URL"],
    };
  }

  // MiniMax: Anthropic-compatible API, requires raw fetch to avoid SDK stainless headers
  if (hasRealValue(env["MINIMAX_API_KEY"])) {
    return {
      provider: "minimax",
      model: env["MINIMAX_MODEL"] || "MiniMax-M3",
      maxTokens,
    };
  }

  if (hasRealValue(env["ANTHROPIC_API_KEY"])) {
    return {
      provider: "anthropic",
      model: env["ANTHROPIC_MODEL"] || "claude-sonnet-5",
      maxTokens,
      baseURL: env["ANTHROPIC_BASE_URL"],
    };
  }
  if (hasRealValue(env["GEMINI_API_KEY"]) || hasRealValue(env["GOOGLE_API_KEY"])) {
    if (!hasRealValue(env["GEMINI_API_KEY"]) && hasRealValue(env["GOOGLE_API_KEY"])) {
      process.stderr.write(
        "[agentmemory] GOOGLE_API_KEY detected — treating as GEMINI_API_KEY. " +
          "Set GEMINI_API_KEY in ~/.agentmemory/.env to silence this warning.\n",
      );
    }
    return {
      provider: "gemini",
      model: env["GEMINI_MODEL"] || "gemini-3.7-flash",
      maxTokens,
    };
  }
  if (hasRealValue(env["OPENROUTER_API_KEY"])) {
    const model = env["OPENROUTER_MODEL"] || "anthropic/claude-sonnet-5";
    // warn when the configured OpenRouter model is in the
    // premium tier and likely to burn money on background compression.
    // Captured workload data shows ~$5/35h on claude-sonnet-4 vs
    // ~$0.46/35h on deepseek-v4-pro for the same compression mix.
    // Heuristic match avoids hard-coding a pricing table.
    if (
      !warnPremiumModelShown &&
      /sonnet|opus|gpt-5\.\d+-sol|gpt-4o(?!.*mini)|gpt-4-turbo/i.test(model) &&
      env["AGENTMEMORY_SUPPRESS_COST_WARNING"] !== "1" &&
      env["AGENTMEMORY_SUPPRESS_COST_WARNING"] !== "true"
    ) {
      warnPremiumModelShown = true;
      process.stderr.write(
        `[agentmemory] OPENROUTER_MODEL=${model} is in the premium tier. ` +
          `Background compression on this model can cost $5+/day under active use. ` +
          `Cheaper alternatives with comparable quality for memory compression: ` +
          `deepseek/deepseek-v4-flash-0731, deepseek/deepseek-v4-pro, qwen/qwen3-coder. ` +
          `See README "Cost-aware model selection" for the full table. ` +
          `Set AGENTMEMORY_SUPPRESS_COST_WARNING=1 to silence.\n`,
      );
    }
    return {
      provider: "openrouter",
      model,
      maxTokens,
    };
  }

  const allowAgentSdk = env["AGENTMEMORY_ALLOW_AGENT_SDK"] === "true";
  if (!allowAgentSdk) {
    process.stderr.write(
      pc.dim(
        "[agentmemory] No LLM provider key set — running zero-LLM with BM25 search. " +
          "Set EMBEDDING_PROVIDER=local for on-device semantic embeddings. " +
          "Set ANTHROPIC_API_KEY (or GEMINI/OPENAI/OPENROUTER/MINIMAX), or AGENTMEMORY_GEMINI_ACCOUNTS_DIR in ~/.agentmemory/.env for LLM compression and summaries. " +
          "Agent-SDK fallback stays off by default to avoid a Stop-hook recursion loop; opt in with AGENTMEMORY_AUTO_COMPRESS=true + AGENTMEMORY_ALLOW_AGENT_SDK=true.\n",
      ),
    );
    return {
      provider: "noop",
      model: "noop",
      maxTokens,
    };
  }

  process.stderr.write(
    "[agentmemory] WARNING: agent-sdk fallback enabled via AGENTMEMORY_ALLOW_AGENT_SDK=true. " +
      "This spawns @anthropic-ai/claude-agent-sdk child sessions that can trigger the Stop-hook " +
      "recursion loop. A SDK-child env marker is set to block re-entry, " +
      "but prefer setting a real API key in ~/.agentmemory/.env instead.\n",
  );
  return {
    provider: "agent-sdk",
    model: "claude-sonnet-5",
    maxTokens,
  };
}

export function loadConfig(): AgentMemoryConfig {
  const env = getMergedEnv();

  const provider = detectProvider(env);
  const auxiliary = parseAuxiliaryLlmConfig(env);
  const fireworksBatch = parseFireworksBatchConfig(env, auxiliary.config);
  const llmRouting = parseLlmRoutingConfig(
    env,
    Boolean(auxiliary.config),
    [...auxiliary.warnings, ...fireworksBatch.warnings],
  );

  // Port quartet: REST is the anchor; streams/engine derive from it
  // unless individually overridden. Default anchor 3111 yields the
  // canonical 3112 streams / 49134 engine pair, but `III_REST_PORT=3211`
  // auto-picks 3212 + 49234 so a second instance doesn't collide.
  const restPort = parseInt(env["III_REST_PORT"] || "3111", 10) || 3111;
  const streamsPort =
    parseInt(env["III_STREAM_PORT"] || env["III_STREAMS_PORT"] || "", 10) ||
    restPort + 1;
  const viewerPort =
    parseInt(env["III_VIEWER_PORT"] || "", 10) || restPort + 2;
  const engineUrl =
    env["III_ENGINE_URL"] ||
    `ws://localhost:${
      parseInt(env["III_ENGINE_PORT"] || "", 10) || restPort + 46023
    }`;

  return {
    engineUrl,
    restPort,
    streamsPort,
    viewerPort,
    provider,
    auxiliaryProvider: auxiliary.config,
    fireworksBatch: fireworksBatch.config,
    llmRouting,
    tokenBudget: safeParseInt(env["TOKEN_BUDGET"], 2000),
    maxObservationsPerSession: safeParseInt(env["MAX_OBS_PER_SESSION"], 500),
    compressionModel: provider.model,
    dataDir: DATA_DIR,
  };
}

function getMergedEnv(
  overrides?: Record<string, string>,
): Record<string, string> {
  const fileEnv = loadEnvFile();
  return { ...fileEnv, ...process.env, ...overrides } as Record<string, string>;
}

export function getEnvVar(key: string): string | undefined {
  return getMergedEnv()[key];
}

export function parseSummaryBudgetConfig(env: Record<string, string | undefined>): SummaryBudgetConfig {
  const integer = (key: string, fallback: number): number => {
    const raw = env[key];
    if (raw === undefined) return fallback;
    const value = Number(raw);
    if (!raw.trim() || !Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`invalid_summary_budget: ${key} must be a positive finite integer`);
    }
    return value;
  };
  const config = {
    contextTokens: integer("AGENTMEMORY_SUMMARY_CONTEXT_TOKENS", 131072),
    outputTokens: integer("AGENTMEMORY_SUMMARY_OUTPUT_TOKENS", 8192),
    safetyMarginTokens: integer("AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS", 4096),
    chunkSize: integer("SUMMARIZE_CHUNK_SIZE", 400),
    concurrency: integer("SUMMARIZE_CHUNK_CONCURRENCY", 12),
  };
  if (config.concurrency > 32) {
    throw new Error("invalid_summary_budget: SUMMARIZE_CHUNK_CONCURRENCY must be at most 32");
  }
  if (config.contextTokens - config.outputTokens - config.safetyMarginTokens <= 0) {
    throw new Error("invalid_summary_budget: context must exceed output plus safety margin");
  }
  return config;
}

export function getSummaryBudgetConfig(): SummaryBudgetConfig {
  return parseSummaryBudgetConfig(getMergedEnv());
}

function parseBooleanSetting(
  env: EnvSource,
  key: string,
  fallback: boolean,
): boolean {
  const normalized = env[key]?.trim().toLowerCase();
  if (normalized === "true" || normalized === "1") return true;
  if (normalized === "false" || normalized === "0") return false;
  return fallback;
}

export function getTypeSafeConfig(): TypeSafeConfig {
  const env = getMergedEnv();
  const configuredMaxStateChars = parseBoundedAuxInt(
    env,
    "AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS",
    TYPESAFE_DEFAULT_MAX_STATE_CHARS,
    TYPESAFE_MAX_STATE_CHARS,
    [],
  );
  const features: Record<TypeSafeFeature, boolean> = {
    compaction: parseBooleanSetting(env, TYPESAFE_FEATURE_ENV.compaction, false),
    admission: parseBooleanSetting(env, TYPESAFE_FEATURE_ENV.admission, true),
    pipelineGates: parseBooleanSetting(env, TYPESAFE_FEATURE_ENV.pipelineGates, true),
    scoring: parseBooleanSetting(env, TYPESAFE_FEATURE_ENV.scoring, true),
  };
  const apiKey = env["TYPESAFE_API_KEY"]?.trim() ?? "";

  return {
    enabled: parseBooleanSetting(env, "AGENTMEMORY_TYPESAFE_ENABLED", false),
    apiKey: apiKey.length <= 4_096 ? apiKey : "",
    timeoutMs: parseBoundedAuxInt(
      env,
      "AGENTMEMORY_TYPESAFE_TIMEOUT_MS",
      TYPESAFE_DEFAULT_TIMEOUT_MS,
      TYPESAFE_MAX_TIMEOUT_MS,
      [],
    ),
    maxStateChars: configuredMaxStateChars >= 256
      ? configuredMaxStateChars
      : TYPESAFE_DEFAULT_MAX_STATE_CHARS,
    features,
  };
}

export function isTypeSafeFeatureEnabled(feature: TypeSafeFeature): boolean {
  const config = getTypeSafeConfig();
  return config.enabled && config.features[feature];
}

// DeepSeek thinking is intentionally controlled only by AgentMemory's env
// file. A process-level setting must not accidentally enable it.
export function isDeepSeekThinkingEnabled(): boolean {
  return loadEnvFile()["deepseek_thinking"] === "true";
}

export function isLlmLoggingEnabled(): boolean {
  return getMergedEnv()["AGENTMEMORY_LLM_LOGGING"] === "true";
}

export function isDropStaleIndexEnabled(): boolean {
  return getMergedEnv()["AGENTMEMORY_DROP_STALE_INDEX"] === "true";
}

export function detectLlmProviderKind(): "llm" | "noop" {
  const env = getMergedEnv();
  if (
    hasRealValue(env["AGENTMEMORY_GEMINI_ACCOUNTS_DIR"]) ||
    hasRealValue(env["ANTHROPIC_API_KEY"]) ||
    hasRealValue(env["GEMINI_API_KEY"]) ||
    hasRealValue(env["GOOGLE_API_KEY"]) ||
    hasRealValue(env["OPENROUTER_API_KEY"]) ||
    hasRealValue(env["MINIMAX_API_KEY"]) ||
    (hasRealValue(env["OPENAI_API_KEY"]) &&
      env["OPENAI_API_KEY_FOR_LLM"] !== "false") ||
    Boolean(parseAuxiliaryLlmConfig(env).config)
  ) {
    return "llm";
  }
  return "noop";
}

export function loadEmbeddingConfig(): EmbeddingConfig {
  const env = getMergedEnv();
  let bm25Weight = parseFloat(env["BM25_WEIGHT"] || "0.4");
  let vectorWeight = parseFloat(env["VECTOR_WEIGHT"] || "0.6");
  bm25Weight =
    isNaN(bm25Weight) || bm25Weight < 0 ? 0.4 : Math.min(bm25Weight, 1);
  vectorWeight =
    isNaN(vectorWeight) || vectorWeight < 0 ? 0.6 : Math.min(vectorWeight, 1);
  return {
    provider: env["EMBEDDING_PROVIDER"] || undefined,
    bm25Weight,
    vectorWeight,
  };
}

export function detectEmbeddingProvider(
  env?: Record<string, string>,
): string | null {
  const source = env ?? getMergedEnv();
  const forced = source["EMBEDDING_PROVIDER"];
  if (forced) return forced;

  if (source["GEMINI_API_KEY"]) return "gemini";
  if (source["OPENAI_API_KEY"]) return "openai";
  if (source["VOYAGE_API_KEY"]) return "voyage";
  if (source["COHERE_API_KEY"]) return "cohere";
  if (source["OPENROUTER_API_KEY"]) return "openrouter";
  return null;
}

export function loadClaudeBridgeConfig(): ClaudeBridgeConfig {
  const env = getMergedEnv();
  const enabled = env["CLAUDE_MEMORY_BRIDGE"] === "true";
  const projectPath = env["CLAUDE_PROJECT_PATH"] || "";
  const lineBudget = safeParseInt(env["CLAUDE_MEMORY_LINE_BUDGET"], 200);
  let memoryFilePath = "";
  if (enabled && projectPath) {
    // Claude Code stores project memory at
    //   ~/.claude/projects/<slug>/memory/MEMORY.md
    // where <slug> is the project path with `/` and `\` swapped for `-`.
    // The leading `-` from an absolute POSIX path is preserved (Claude
    // Code keeps it; stripping it produced a slug Claude never reads).
    // The `memory/` subdirectory holds MEMORY.md (the index) plus one
    // per-topic `.md` file per memory (verified against Claude Code 2.x).
    const safePath = projectPath.replace(/[/\\]/g, "-");
    memoryFilePath = join(
      homedir(),
      ".claude",
      "projects",
      safePath,
      "memory",
      "MEMORY.md",
    );
  }
  return { enabled, projectPath, memoryFilePath, lineBudget };
}

export function loadTeamConfig(): TeamConfig | null {
  const env = getMergedEnv();
  const teamId = env["TEAM_ID"];
  const userId = env["USER_ID"];
  if (!teamId || !userId) return null;
  const mode = env["TEAM_MODE"] === "shared" ? "shared" : "private";
  return { teamId, userId, mode };
}

// optional AGENT_ID env for multi-agent memory isolation.
// Returns null when unset so memory stays unscoped (legacy behavior).
// Trimmed + length-capped to keep KV writes well-formed.
//
// Filtering is gated by AGENTMEMORY_AGENT_SCOPE:
//   "shared"   (default) — tag everything, do not filter recall paths
//   "isolated"           — tag everything AND filter recall paths
export function loadAgentScope(): {
  agentId: string;
  mode: "shared" | "isolated";
} | null {
  const env = getMergedEnv();
  const raw = env["AGENT_ID"];
  if (!raw) return null;
  const agentId = raw.trim().slice(0, 128);
  if (!agentId) return null;
  const mode = env["AGENTMEMORY_AGENT_SCOPE"] === "isolated"
    ? "isolated"
    : "shared";
  return { agentId, mode };
}

export function getAgentId(): string | undefined {
  return loadAgentScope()?.agentId;
}

// True only when AGENT_ID is set AND scope=isolated. Recall paths
// consult this to decide whether to filter.
export function isAgentScopeIsolated(): boolean {
  return loadAgentScope()?.mode === "isolated";
}

// Floor for the git-snapshot timer. A zero/negative SNAPSHOT_INTERVAL would
// make setInterval fire on roughly every event-loop tick, saturating the
// worker with back-to-back full-state snapshots + git commits. Anything below
// this floor is treated as a misconfiguration and falls back to the default.
const SNAPSHOT_INTERVAL_DEFAULT_SECONDS = 3600;
const MIN_SNAPSHOT_INTERVAL_SECONDS = 1;

export function loadSnapshotConfig(): {
  enabled: boolean;
  interval: number;
  dir: string;
} {
  const env = getMergedEnv();
  const rawInterval = safeParseInt(
    env["SNAPSHOT_INTERVAL"],
    SNAPSHOT_INTERVAL_DEFAULT_SECONDS,
  );
  const interval =
    rawInterval >= MIN_SNAPSHOT_INTERVAL_SECONDS
      ? rawInterval
      : SNAPSHOT_INTERVAL_DEFAULT_SECONDS;
  return {
    enabled: env["SNAPSHOT_ENABLED"] === "true",
    interval,
    dir: env["SNAPSHOT_DIR"] || join(homedir(), ".agentmemory", "snapshots"),
  };
}

export function isGraphExtractionEnabled(): boolean {
  return getMergedEnv()["GRAPH_EXTRACTION_ENABLED"] === "true";
}

export function getGraphBatchSize(): number {
  return safeParseInt(getMergedEnv()["GRAPH_EXTRACTION_BATCH_SIZE"], 10);
}

export function getGraphExtractionInputTargetChars(): number {
  const env = getMergedEnv();
  const configured = safeParseInt(
    env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"] ??
      env["AGENTMEMORY_GRAPH_MAX_INPUT_CHARS"],
    GRAPH_EXTRACTION_DEFAULT_INPUT_TARGET_CHARS,
  );
  return Math.max(4_000, Math.min(120_000, configured));
}

/** Backwards-compatible alias for integrations using the old name. */
export function getGraphExtractionMaxInputChars(): number {
  return getGraphExtractionInputTargetChars();
}

// window for the smart-search followup-rate diagnostic. A second
// search arriving within this many seconds (with disjoint results)
// counts as a "follow-up" — a directional signal that the first result
// set didn't satisfy. Long values overcount (legitimate refinement
// looks like a follow-up); short values undercount.
const FOLLOWUP_WINDOW_DEFAULT_SECONDS = 30;

export function getFollowupWindowSeconds(): number {
  return safeParseInt(
    getMergedEnv()["AGENTMEMORY_FOLLOWUP_WINDOW_SECONDS"],
    FOLLOWUP_WINDOW_DEFAULT_SECONDS,
  );
}

export function isConsolidationEnabled(): boolean {
  const env = getMergedEnv();
  const explicit = env["CONSOLIDATION_ENABLED"];
  if (explicit === "false" || explicit === "0") return false;
  if (explicit === "true" || explicit === "1") return true;
  return hasLLMProviderConfigured(env);
}

function hasLLMProviderConfigured(env: Record<string, string | undefined>): boolean {
  const provider = (env["AGENTMEMORY_PROVIDER"] || "").toLowerCase();
  if (provider === "noop") return false;
  const openaiKeyForLlm =
    env["OPENAI_API_KEY"] &&
    (env["OPENAI_API_KEY_FOR_LLM"] || "").toLowerCase() !== "false";
  return Boolean(
    env["AGENTMEMORY_GEMINI_ACCOUNTS_DIR"] ||
      env["ANTHROPIC_API_KEY"] ||
      openaiKeyForLlm ||
      env["OPENROUTER_API_KEY"] ||
      env["GEMINI_API_KEY"] ||
      env["GOOGLE_API_KEY"] ||
      env["MINIMAX_API_KEY"] ||
      env["OPENAI_BASE_URL"] ||
      provider === "agent-sdk" ||
      parseAuxiliaryLlmConfig(env).config,
  );
}

// Per-observation LLM compression is OFF by default as of 0.8.8.
// When disabled, observations are captured and indexed via a synthetic
// (zero-LLM) compression path so recall/search still works. Users who want
// richer LLM-generated summaries can set AGENTMEMORY_AUTO_COMPRESS=true in
// ~/.agentmemory/.env — but should expect their Claude API token usage to
// climb proportionally with session tool-use frequency.
export function isAutoCompressEnabled(): boolean {
  return getMergedEnv()["AGENTMEMORY_AUTO_COMPRESS"] === "true";
}

// Hook-level context injection into Claude Code's conversation is OFF by
// default as of 0.8.10. When disabled, pre-tool-use and
// session-start hooks still POST observations for background capture, but
// never write context to stdout — so Claude Code doesn't inject an extra
// ~4000-char blob into every tool turn. 0.8.8 stopped the agentmemory-side
// Claude calls (via ANTHROPIC_API_KEY); this stops the Claude Code-side
// token burn where every tool call silently grew the model input window.
// Users who want the in-conversation context injection explicitly opt in
// with AGENTMEMORY_INJECT_CONTEXT=true and get a loud startup warning.
export function isContextInjectionEnabled(): boolean {
  return getMergedEnv()["AGENTMEMORY_INJECT_CONTEXT"] === "true";
}

export function getConsolidationDecayDays(): number {
  return safeParseInt(getMergedEnv()["CONSOLIDATION_DECAY_DAYS"], 30);
}

export function getConsolidationMinNewSummaries(): number {
  const configured = safeParseInt(
    getMergedEnv()["AGENTMEMORY_CONSOLIDATION_MIN_NEW_SUMMARIES"],
    CONSOLIDATION_DEFAULT_MIN_NEW_SUMMARIES,
  );
  return Math.max(1, Math.min(20, configured));
}

// Cooldown between corpus consolidations triggered by session stop. The Stop
// hook fires per agent turn and posts /session/end, so without this every turn
// would kick a full LLM semantic-merge + reflect + crystallize. Debounced to at
// most once per window. Set to 0 to disable the debounce (consolidate on every
// stop). Default 5 minutes.
const CONSOLIDATION_COOLDOWN_DEFAULT_MS = 300000;

export function getConsolidationCooldownMs(): number {
  const raw = safeParseInt(
    getMergedEnv()["AGENTMEMORY_CONSOLIDATION_COOLDOWN_MS"],
    CONSOLIDATION_COOLDOWN_DEFAULT_MS,
  );
  return raw >= 0 ? raw : CONSOLIDATION_COOLDOWN_DEFAULT_MS;
}

export const INDEX_SAVE_INTERVAL_DEFAULT_MS = 600_000;

export function getIndexSaveIntervalMs(): number {
  const raw = safeParseInt(
    getMergedEnv()["AGENTMEMORY_INDEX_SAVE_INTERVAL_MS"],
    INDEX_SAVE_INTERVAL_DEFAULT_MS,
  );
  return raw > 0 ? raw : INDEX_SAVE_INTERVAL_DEFAULT_MS;
}

export const VECTOR_BUCKET_SIZE_DEFAULT = 500;

export function getVectorBucketSize(): number {
  const raw = safeParseInt(getMergedEnv()["AGENTMEMORY_VECTOR_BUCKET_SIZE"], VECTOR_BUCKET_SIZE_DEFAULT);
  return raw > 0 ? raw : VECTOR_BUCKET_SIZE_DEFAULT;
}

export const VECTOR_BACKFILL_MAX_DEFAULT = 500;

export function getVectorBackfillMax(): number {
  const raw = safeParseInt(getMergedEnv()["AGENTMEMORY_VECTOR_BACKFILL_MAX"], VECTOR_BACKFILL_MAX_DEFAULT);
  return raw > 0 ? raw : VECTOR_BACKFILL_MAX_DEFAULT;
}

export function isVectorBackfillAllEnabled(): boolean {
  return getMergedEnv()["AGENTMEMORY_VECTOR_BACKFILL"] === "all";
}

export function isStandaloneMcp(): boolean {
  return getMergedEnv()["STANDALONE_MCP"] === "true";
}

export function getStandalonePersistPath(): string {
  const env = getMergedEnv();
  return (
    env["STANDALONE_PERSIST_PATH"] ||
    join(homedir(), ".agentmemory", "standalone.json")
  );
}

const VALID_PROVIDERS = new Set([
  "anthropic",
  "gemini",
  "openrouter",
  "agent-sdk",
  "minimax",
  "openai",
]);

export function loadFallbackConfig(): FallbackConfig {
  const env = getMergedEnv();
  const raw = env["FALLBACK_PROVIDERS"] || "";
  const allowAgentSdk = env["AGENTMEMORY_ALLOW_AGENT_SDK"] === "true";
  const providers = raw
    .split(",")
    .map((p) => p.trim())
    .filter(
      (p): p is FallbackConfig["providers"][number] =>
        Boolean(p) && VALID_PROVIDERS.has(p),
    )
    .filter((p) => {
      // Honor the same safety gate as detectProvider: agent-sdk is only
      // permitted as a fallback target when the user has explicitly opted
      // in. Without this filter, a user could set FALLBACK_PROVIDERS=agent-sdk
      // and re-introduce the Stop-hook recursion loop even though
      // detectProvider() returned the noop provider.
      if (p === "agent-sdk" && !allowAgentSdk) {
        process.stderr.write(
          "[agentmemory] Ignoring FALLBACK_PROVIDERS entry 'agent-sdk' " +
            "(AGENTMEMORY_ALLOW_AGENT_SDK is not 'true'). The agent-sdk " +
            "fallback can spawn Claude Agent SDK child sessions that trigger " +
            "the Stop-hook recursion loop. Opt in explicitly " +
            "with AGENTMEMORY_ALLOW_AGENT_SDK=true if this is intentional.\n",
        );
        return false;
      }
      return true;
    });
  return { providers };
}
