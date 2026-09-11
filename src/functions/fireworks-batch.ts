import type { FireworksBatchActiveIndex, FireworksBatchConfig, FireworksBatchEnqueueIntent, FireworksBatchEnqueueJournal, FireworksBatchJob, FireworksBatchRequest, FireworksBatchWorkItem, LlmUsage } from "../types.js";
import { fingerprintId, generateId, KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { batchEffectKey } from "../state/batch-effects.js";
import { FireworksBatchError, type FireworksBatchRemoteStatus, type FireworksBatchTransport } from "../providers/fireworks-batch.js";
import { logger } from "../logger.js";
import { batchTaskLlmTask, taskOutputTokens } from "../providers/task-output-limits.js";

export interface FireworksBatchQueue {
  enqueue(request: FireworksBatchRequest): Promise<{ queued: boolean; workItemId?: string; reason?: string }>;
}

type CompletedHandler = (
  item: FireworksBatchWorkItem,
  content: string,
) => Promise<"stale" | void | false | { success: false; error?: string }>;

type UsageHandler = (item: FireworksBatchWorkItem, usage: LlmUsage) => Promise<void>;

// Metadata is persisted in the local queue and is not part of the JSONL
// request uploaded to Fireworks. Keep a separate local bound so provenance
// can be retained without consuming the remote prompt budget.
const MAX_PERSISTED_METADATA_CHARS = 2_000_000;
const ACTIVE_INDEX_KEY = "current";
const ENQUEUE_JOURNAL_KEY = "current";
const REMOTE_RECOVERY_KEY = "remote-recovery-v1";
const MAX_ACTIVE_INDEX_IDS = 4_096;
const MAX_ACTIVE_INDEX_CHARS = 256_000;
const MAX_ENQUEUE_INTENTS = 4_096;
const MAX_ENQUEUE_JOURNAL_BYTES = 8 * 1024 * 1024;
const MAX_REMOTE_RECONCILIATION_CANDIDATES = 32;
const MAX_REMOTE_RECONCILIATION_ATTEMPTS = 3;
const MAX_POLL_DEADLINE_MS = 24 * 60 * 60_000;
const POLLING_EXHAUSTED_ERROR = "batch polling attempts exhausted";
const POLLING_DEADLINE_ERROR = "batch polling deadline exceeded";
const LEGACY_COMPLETED_AMBIGUOUS_ERROR = "Legacy completed batch contained non-terminal work; callback outcome is ambiguous; automatic replay blocked.";
const ACTIVE_WORK_STATES = new Set(["queued", "submitted", "polling"]);
const ACTIVE_JOB_STATES = new Set(["queued", "submitted", "polling"]);
const TERMINAL_WORK_STATES = new Set(["completed", "stale", "failed", "dead-letter"]);
const ENQUEUE_LOCK = "fireworks-batch:enqueue";
const ESTIMATED_RESULT_CHARS_PER_TOKEN = 4;
const ESTIMATED_RESULT_ROW_OVERHEAD_CHARS = 768;
const MAX_RESULT_UTF8_BYTES_PER_CHAR = 4;
const MAX_RESULT_AGGREGATE_BYTES = 64 * 1024 * 1024;

class FireworksBatchReconciliationError extends Error {}
class FireworksBatchCallbackError extends Error {}

type ParsedBatchRow =
  | { customId: string; kind: "result"; content: string; usage?: LlmUsage }
  | { customId: string; kind: "error"; error: string };

interface FireworksBatchRemoteRecovery {
  version: 1;
  pendingJobIds: string[];
  discoveredAt: string;
  updatedAt: string;
  completedAt?: string;
  legacyReconciliationVersion?: 1 | 2 | 3;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function boundedActiveIds(value: unknown): string[] {
  const rawIds = Array.isArray(value)
    ? value
    : value && typeof value === "object"
      ? ((value as { ids?: unknown[]; workItemIds?: unknown[]; jobIds?: unknown[] }).ids
        ?? (value as { workItemIds?: unknown[] }).workItemIds
        ?? (value as { jobIds?: unknown[] }).jobIds
        ?? [])
      : [];
  const ids: string[] = [];
  const seen = new Set<string>();
  let chars = 2;
  for (const rawId of rawIds) {
    if (typeof rawId !== "string" || rawId.length === 0 || rawId.length > 512 || seen.has(rawId)) continue;
    const additionalChars = (ids.length === 0 ? 0 : 1) + rawId.length + 2;
    if (ids.length >= MAX_ACTIVE_INDEX_IDS || chars + additionalChars > MAX_ACTIVE_INDEX_CHARS) break;
    seen.add(rawId);
    ids.push(rawId);
    chars += additionalChars;
  }
  return ids;
}

function validEnqueueIntent(value: unknown): value is FireworksBatchEnqueueIntent {
  if (!isRecord(value) || value.version !== 1 || typeof value.id !== "string" || value.id.length === 0
    || typeof value.fingerprint !== "string" || value.fingerprint.length === 0
    || !isRecord(value.item) || value.item.id !== value.id
    || typeof value.createdAt !== "string" || typeof value.updatedAt !== "string") {
    return false;
  }
  return typeof value.item.customId === "string"
    && typeof value.item.correlationId === "string"
    && typeof value.item.task === "string"
    && typeof value.item.model === "string"
    && typeof value.item.systemPrompt === "string"
    && typeof value.item.userPrompt === "string"
    && typeof value.item.maxTokens === "number"
    && typeof value.item.state === "string"
    && typeof value.item.attempts === "number"
    && typeof value.item.nextAttemptAt === "string"
    && typeof value.item.createdAt === "string"
    && typeof value.item.updatedAt === "string";
}

function boundedEnqueueIntents(value: unknown): FireworksBatchEnqueueIntent[] {
  const rawIntents = isRecord(value) && Array.isArray(value.intents) ? value.intents : [];
  const intents: FireworksBatchEnqueueIntent[] = [];
  const seen = new Set<string>();
  for (const rawIntent of rawIntents) {
    if (!validEnqueueIntent(rawIntent) || seen.has(rawIntent.id)) continue;
    seen.add(rawIntent.id);
    intents.push(rawIntent);
    if (intents.length >= MAX_ENQUEUE_INTENTS) break;
  }
  return intents;
}

function enqueueJournalFits(intents: FireworksBatchEnqueueIntent[]): boolean {
  return Buffer.byteLength(JSON.stringify({ version: 1, intents, updatedAt: new Date().toISOString() }), "utf8")
    <= MAX_ENQUEUE_JOURNAL_BYTES;
}

function retryAt(config: FireworksBatchConfig, attempts: number): string {
  const delay = Math.min(config.retryMaxMs, config.retryBaseMs * 2 ** Math.max(0, attempts - 1));
  return new Date(Date.now() + delay).toISOString();
}

function boundedPollDeadlineMs(config: FireworksBatchConfig): number {
  const configured = config.pollDeadlineMs;
  if (!Number.isFinite(configured) || configured === undefined || configured < 1) return MAX_POLL_DEADLINE_MS;
  return Math.min(MAX_POLL_DEADLINE_MS, Math.floor(configured));
}

function deadlineFrom(base: string | undefined, config: FireworksBatchConfig): string {
  const baseMs = base ? Date.parse(base) : Number.NaN;
  const start = Number.isFinite(baseMs) ? baseMs : Date.now();
  return new Date(start + boundedPollDeadlineMs(config)).toISOString();
}

function pollRetryAt(config: FireworksBatchConfig, observations: number): string {
  const base = Number.isFinite(config.pollIntervalMs) ? Math.max(0, Math.floor(config.pollIntervalMs)) : 0;
  const max = Number.isFinite(config.pollMaxIntervalMs)
    ? Math.max(base, Math.floor(config.pollMaxIntervalMs))
    : base;
  const delay = Math.min(max, base * 2 ** Math.max(0, observations - 1));
  return new Date(Date.now() + delay).toISOString();
}

function incrementAttempts(attempts: number | undefined, maxAttempts: number): { attempts: number; exhausted: boolean } {
  const limit = Math.max(1, Math.floor(maxAttempts));
  const current = typeof attempts === "number" && Number.isFinite(attempts) && attempts >= 0
    ? Math.floor(attempts)
    : 0;
  const next = Math.min(limit, current + 1);
  return { attempts: next, exhausted: next >= limit };
}

function responseBody(value: unknown): Record<string, unknown> | undefined {
  if (!isRecord(value) || !isRecord(value.response)) return undefined;
  const response = value.response;
  if (response.status_code !== undefined && response.status_code !== 200) return undefined;
  const body = Object.hasOwn(response, "body") ? response.body : response;
  return isRecord(body) && body.error == null ? body : undefined;
}

function resultContent(value: unknown): string | undefined {
  const choices = responseBody(value)?.choices;
  if (!Array.isArray(choices) || choices.length !== 1 || !isRecord(choices[0]) || !isRecord(choices[0].message)) return undefined;
  const content = choices[0].message.content;
  return typeof content === "string" && content.trim().length > 0 ? content : undefined;
}

function resultUsage(value: unknown): LlmUsage | undefined {
  const raw = responseBody(value)?.usage;
  if (!isRecord(raw)) return undefined;
  const count = (value: unknown): number | undefined =>
    typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : undefined;
  const inputTokens = count(raw.prompt_tokens ?? raw.input_tokens);
  const outputTokens = count(raw.completion_tokens ?? raw.output_tokens);
  const totalTokens = count(raw.total_tokens);
  if (inputTokens === undefined && outputTokens === undefined && totalTokens === undefined) return undefined;
  return { inputTokens, outputTokens, totalTokens, responseChars: 0 };
}

function rowErrorMessage(value: unknown): string {
  if (typeof value === "string") return safeRemoteStatusMessage(value) || "remote batch row failed";
  if (isRecord(value)) {
    const code = safeRemoteStatusMessage(value.code);
    const message = safeRemoteStatusMessage(value.message);
    if (code && message) return `code=${code}; message=${message}`.slice(0, 512);
    if (code || message) return (code || message)!;
  }
  return "remote batch row failed";
}

function jsonlText(value: unknown, label: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) {
    throw new FireworksBatchReconciliationError(`batch ${label} file was not text or rows`);
  }
  return value.map((row, index) => {
    if (typeof row === "string") return row;
    if (isRecord(row)) return JSON.stringify(row);
    throw new FireworksBatchReconciliationError(`batch ${label} row ${index + 1} was invalid`);
  }).join("\n");
}

function normalizeDownloadedResults(value: unknown): { resultText: string; errorText: string } {
  if (typeof value === "string") return { resultText: value, errorText: "" };
  if (!isRecord(value)) {
    throw new FireworksBatchReconciliationError("batch result files were invalid");
  }
  const resultText = jsonlText(
    value.resultRows ?? value.results ?? value.result ?? value.output ?? value.success,
    "result",
  );
  const errorText = jsonlText(
    value.errorRows ?? value.errors ?? value.error ?? value.failed ?? value.failure,
    "error",
  );
  if (resultText === undefined && errorText === undefined) {
    throw new FireworksBatchReconciliationError("batch result files were missing");
  }
  return { resultText: resultText ?? "", errorText: errorText ?? "" };
}

function parseJsonlRows(text: string, label: "result" | "error"): ParsedBatchRow[] {
  const rows: ParsedBatchRow[] = [];
  for (const [index, line] of text.split(/\r?\n/).entries()) {
    if (!line.trim()) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      throw new FireworksBatchReconciliationError(`batch ${label} row ${index + 1} was malformed JSON`);
    }
    if (!isRecord(parsed) || typeof parsed.custom_id !== "string" || parsed.custom_id.length === 0) {
      throw new FireworksBatchReconciliationError(`batch ${label} row ${index + 1} had no valid custom_id`);
    }
    if (parsed.error !== undefined && parsed.error !== null) {
      if (typeof parsed.error !== "string" && !isRecord(parsed.error)) {
        throw new FireworksBatchReconciliationError(`batch ${label} row ${index + 1} had an invalid error`);
      }
      rows.push({ customId: parsed.custom_id, kind: "error", error: rowErrorMessage(parsed.error) });
      continue;
    }
    const content = resultContent(parsed);
    if (content === undefined) {
      throw new FireworksBatchReconciliationError(`batch ${label} row ${index + 1} had no valid response content`);
    }
    const choices = responseBody(parsed)?.choices;
    const finishReason = Array.isArray(choices) && isRecord(choices[0]) ? choices[0].finish_reason : undefined;
    if (finishReason !== undefined && finishReason !== "stop") {
      const reason = finishReason === "length" ? "length" : "invalid";
      throw new FireworksBatchReconciliationError(`batch ${label} row ${index + 1} had incomplete response (finish_reason=${reason})`);
    }
    rows.push({ customId: parsed.custom_id, kind: "result", content, usage: resultUsage(parsed) });
  }
  return rows;
}

function completionFailure(value: unknown): string | undefined {
  if (value === false) return "batch completion callback returned failure";
  if (!isRecord(value) || value.success !== false) return undefined;
  const message = safeRemoteStatusMessage(value.error);
  return message
    ? message
    : "batch completion callback returned failure";
}

function compatibilityKey(item: FireworksBatchWorkItem): string {
  const lane = item.replacementOf ? "replacement" : "normal";
  return `${lane}\0${item.task}\0${item.model}\0${item.maxTokens}\0${item.systemPrompt}`;
}

function oldestCreatedAt(items: FireworksBatchWorkItem[]): number {
  return Math.min(...items.map((item) => Date.parse(item.createdAt)).filter(Number.isFinite));
}

function fireworksResourceId(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "batch";
}

function isSafeRemoteJobId(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= 512
    && !/[\u0000-\u0020\u007f/?#\\]/.test(value);
}

function isSafeRemoteResource(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && value.length <= 1_024
    && !/[\u0000-\u0020\u007f?#\\]/.test(value);
}

function lastPathSegment(value: string): string {
  const segment = value.split("/").filter(Boolean).at(-1);
  return segment || value;
}

function normalizeRemoteJobId(value: unknown): { id: string; name?: string } | undefined {
  if (value === undefined || value === null) return undefined;
  if (isSafeRemoteJobId(value)) return { id: value };
  if (isSafeRemoteResource(value)) {
    const id = lastPathSegment(value);
    if (id !== value && isSafeRemoteJobId(id)) return { id, name: value };
  }
  throw new FireworksBatchReconciliationError("remote response included an invalid job identity");
}

function optionalRemoteResource(value: unknown, label: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (!isSafeRemoteResource(value)) {
    throw new FireworksBatchReconciliationError(`remote response included an invalid ${label}`);
  }
  return value;
}

function safeRemoteStatusMessage(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  return value
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\bhttps?:\/\/[^\s<>"']+/gi, "[redacted-url]")
    .replace(/\bBearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [redacted]")
    .replace(/(["']?)(authorization|api[-_ ]?key|access[-_ ]?token|token|secret|password)\1\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:bearer\s+)?[^,;\s}]+)/gi, "$2=[redacted]")
    .replace(/(["']?)((?:user|system)?prompt|messages?|jsonl)\1\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^,;\s}]+)/gi, "$2=[redacted]")
    .slice(0, 512) || undefined;
}

function safeBatchErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof FireworksBatchReconciliationError) return error.message.slice(0, 1_024);
  if (!(error instanceof FireworksBatchError)) return fallback;
  const status = error.status === undefined ? "" : ` (${error.status})`;
  const details = [
    error.diagnostic.code ? `code=${error.diagnostic.code}` : undefined,
    error.diagnostic.message ? `message=${error.diagnostic.message}` : undefined,
    error.diagnostic.code || error.diagnostic.message ? undefined : error.message,
  ].filter((detail): detail is string => Boolean(detail));
  return `${error.operation}${status}: ${details.join("; ")}`.slice(0, 1024);
}

function isAmbiguousSubmitError(error: unknown): boolean {
  if (error instanceof FireworksBatchError) {
    if (error.operation !== "job-submit") return false;
    const diagnostic = `${error.diagnostic.message ?? ""} ${error.message}`;
    if (error.status === undefined) {
      return true;
    }
    if (error.status >= 200 && error.status < 300) return true;
    if (error.status >= 500) return error.retryable;
    if ((error.status === 408 || error.status === 425 || error.status === 429) && error.retryable) return true;
    if (error.status !== 400 && error.status !== 409) return false;
    const code = error.diagnostic.code?.toUpperCase();
    if (code === "ALREADY_EXISTS" || code === "ALREADY_SUBMITTED" || code === "DUPLICATE") return true;
    return /(?:already[ _-]?exists|already[ _-]?submitted|idempotenc(?:y|e))/i.test(diagnostic);
  }
  if (!(error instanceof Error)) return false;
  return /\b(?:network|transport|timeout|timed out|connection|socket|fetch failed|abort)\b|(?:response was not valid JSON|did not include a safe job ID|malformed)/i.test(error.message);
}

function batchJsonlLine(item: FireworksBatchWorkItem): string {
  return JSON.stringify({
    custom_id: item.customId,
    body: {
      messages: [
        { role: "system", content: item.systemPrompt },
        { role: "user", content: item.userPrompt },
      ],
      max_tokens: item.maxTokens,
    },
  });
}

interface EstimatedResultSize {
  chars: number;
  bytes: number;
}

interface ResultBudget {
  chars: number;
  bytes: number;
}

function estimatedResultSize(item: FireworksBatchWorkItem): EstimatedResultSize {
  const chars = item.maxTokens * ESTIMATED_RESULT_CHARS_PER_TOKEN
    + ESTIMATED_RESULT_ROW_OVERHEAD_CHARS
    + item.customId.length;
  return { chars, bytes: chars * MAX_RESULT_UTF8_BYTES_PER_CHAR };
}

function resultBudget(config: FireworksBatchConfig): ResultBudget {
  const chars = Number.isFinite(config.maxResultChars) && config.maxResultChars >= 0
    ? Math.floor(config.maxResultChars)
    : 0;
  const responseBytes = Number.isFinite(config.maxResponseBytes) && config.maxResponseBytes >= 0
    ? Math.floor(config.maxResponseBytes)
    : 0;
  return {
    chars,
    bytes: Math.min(
      MAX_RESULT_AGGREGATE_BYTES,
      responseBytes * 32,
      chars * MAX_RESULT_UTF8_BYTES_PER_CHAR,
    ),
  };
}

function resultLimitNames(size: EstimatedResultSize, budget: ResultBudget): string[] {
  return [
    size.chars > budget.chars ? "result character budget" : undefined,
    size.bytes > budget.bytes ? "result byte budget" : undefined,
  ].filter((limit): limit is string => Boolean(limit));
}

function fittingBatchPrefix(
  items: FireworksBatchWorkItem[],
  maxBytes: number,
  maxChars: number,
  maxResultChars: number,
  maxResultBytes: number,
): FireworksBatchWorkItem[] {
  const prefix: FireworksBatchWorkItem[] = [];
  let bytes = 0;
  let chars = 0;
  let resultChars = 0;
  let resultBytes = 0;
  for (const item of items) {
    const line = batchJsonlLine(item);
    const separator = prefix.length === 0 ? 0 : 1;
    const lineBytes = Buffer.byteLength(line, "utf8");
    const nextBytes = bytes + separator + lineBytes;
    const nextChars = chars + separator + line.length;
    const estimated = estimatedResultSize(item);
    const nextResultChars = resultChars + separator + estimated.chars;
    const nextResultBytes = resultBytes + separator + estimated.bytes;
    if (nextBytes > maxBytes || nextChars > maxChars
      || nextResultChars > maxResultChars || nextResultBytes > maxResultBytes) break;
    prefix.push(item);
    bytes = nextBytes;
    chars = nextChars;
    resultChars = nextResultChars;
    resultBytes = nextResultBytes;
  }
  return prefix;
}

function remoteCandidateId(value: unknown): string | undefined {
  if (isSafeRemoteJobId(value)) return value;
  if (!isSafeRemoteResource(value)) return undefined;
  const id = lastPathSegment(value);
  return id !== value && isSafeRemoteJobId(id) ? id : undefined;
}

function remoteResourceIdentity(value: unknown): string | undefined {
  if (!isSafeRemoteResource(value)) return undefined;
  return lastPathSegment(value);
}

function remoteStatusBelongsToJob(
  job: FireworksBatchJob,
  candidate: string,
  status: FireworksBatchRemoteStatus,
  requestedId: string | undefined,
): boolean {
  if (requestedId && candidate === requestedId) return true;
  const statusIds = [remoteCandidateId(status.remoteJobId), remoteCandidateId(status.remoteJobName)]
    .filter((id): id is string => Boolean(id));
  if (requestedId && statusIds.includes(requestedId)) return true;
  const localResources = [remoteResourceIdentity(job.inputDatasetId), remoteResourceIdentity(job.outputDatasetId)]
    .filter((id): id is string => Boolean(id));
  const remoteResources = [remoteResourceIdentity(status.inputDatasetId), remoteResourceIdentity(status.outputDatasetId)]
    .filter((id): id is string => Boolean(id));
  return remoteResources.some((remoteId) => localResources.includes(remoteId));
}

export class FireworksBatchCoordinator implements FireworksBatchQueue {
  async canReplace(workItemId: string): Promise<boolean> {
    const old = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, workItemId);
    if (!old || old.id !== workItemId || !old.batchJobId || old.state !== "dead-letter") return false;
    const job = await this.kv.get<FireworksBatchJob>(KV.fireworksBatchJobs, old.batchJobId);
    if (!job || job.id !== old.batchJobId || job.state !== "dead-letter"
      || (!/^batch result row [1-9][0-9]* had (?:no valid response content|incomplete response \(finish_reason=(?:length|invalid)\))$/.test(job.lastError ?? "")
        && job.lastError !== "Batch callback attempts exhausted; reconcile partial downstream receipts before replacement work")
      || !/^fwbjob-[a-z0-9-]+$/.test(job.id) || job.remoteJobId !== job.id
      || (job.requestedRemoteJobId !== undefined && job.requestedRemoteJobId !== job.id)
      || (job.remoteJobName !== undefined && job.remoteJobName !== `accounts/${this.config.accountId}/batchInferenceJobs/${job.id}`)
      || !job.workItemIds.includes(old.id) || new Set(job.workItemIds).size !== job.workItemIds.length
      || ![`${job.id}-output`, `accounts/${this.config.accountId}/datasets/${job.id}-output`].includes(job.outputDatasetId)) return false;
    if (old.task !== job.task || old.model !== job.model || old.lastError !== job.lastError
      || old.callbackProtocolVersion !== 1 || old.result !== undefined || old.completionIntent !== undefined) return false;
    for (const destination of ["graph", "consolidation", "crystallize", "lessons", "reflect"]) {
      const key = batchEffectKey(old.id);
      const receipt = await this.kv.get(KV.batchCallbacks, `${destination}:${key}`);
      const active = await this.kv.get<{ activeKey?: string }>(KV.batchCallbacks, `active:${destination}`);
      if (receipt != null || active?.activeKey === key) return false;
    }
    return true;
  }
  private processInFlight: Promise<void> | undefined;

  constructor(
    private readonly kv: StateKV,
    private readonly config: FireworksBatchConfig,
    private readonly transport: FireworksBatchTransport | undefined,
    private readonly onCompleted: CompletedHandler,
    private readonly onUsage?: UsageHandler,
  ) {}

  private activeIndexCapacity(scope: string): number {
    const configured = scope === KV.fireworksBatchActiveWork
      ? this.config.maxQueuedItems
      : this.config.maxConcurrency;
    if (!Number.isFinite(configured)) return 0;
    return Math.max(0, Math.min(MAX_ACTIVE_INDEX_IDS, Math.floor(configured)));
  }

  private async readActiveIds(scope: string): Promise<string[]> {
    return boundedActiveIds(await this.kv.get<FireworksBatchActiveIndex | string[]>(scope, ACTIVE_INDEX_KEY));
  }

  private async writeActiveIds(scope: string, ids: string[]): Promise<void> {
    const index: FireworksBatchActiveIndex = {
      version: 1,
      ids: boundedActiveIds(ids),
      updatedAt: new Date().toISOString(),
    };
    await this.kv.set(scope, ACTIVE_INDEX_KEY, index);
  }

  private async readEnqueueIntents(): Promise<FireworksBatchEnqueueIntent[]> {
    return boundedEnqueueIntents(await this.kv.get<FireworksBatchEnqueueJournal>(
      KV.fireworksBatchEnqueueIntents,
      ENQUEUE_JOURNAL_KEY,
    ));
  }

  private async writeEnqueueIntents(intents: FireworksBatchEnqueueIntent[]): Promise<void> {
    if (!enqueueJournalFits(intents)) {
      throw new FireworksBatchReconciliationError("batch enqueue journal exceeded configured limits");
    }
    const journal: FireworksBatchEnqueueJournal = {
      version: 1,
      intents,
      updatedAt: new Date().toISOString(),
    };
    await this.kv.set(KV.fireworksBatchEnqueueIntents, ENQUEUE_JOURNAL_KEY, journal);
  }

  private async recoverEnqueueIntentsUnsafe(): Promise<void> {
    const intents = await this.readEnqueueIntents();
    if (intents.length === 0) return;
    const remaining: FireworksBatchEnqueueIntent[] = [];
    for (const intent of intents) {
      const existing = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, intent.id);
      if (!ACTIVE_WORK_STATES.has(intent.item.state)) continue;
      if (existing && existing.id !== intent.id) {
        remaining.push(intent);
        continue;
      }
      if (existing && !ACTIVE_WORK_STATES.has(existing.state)) continue;
      if (!existing) {
        await this.kv.set(KV.fireworksBatchWorkItems, intent.id, intent.item);
      }
      const fingerprintOwner = await this.kv.get<string>(KV.fireworksBatchFingerprints, intent.fingerprint);
      if (fingerprintOwner && fingerprintOwner !== intent.id) {
        remaining.push(intent);
        continue;
      }
      if (!fingerprintOwner) {
        await this.kv.set(KV.fireworksBatchFingerprints, intent.fingerprint, intent.id);
      }
      if (await this.addActiveId(KV.fireworksBatchActiveWork, intent.id)) continue;
      remaining.push(intent);
    }
    if (remaining.length !== intents.length) await this.writeEnqueueIntents(remaining);
  }

  private async recoverEnqueueIntents(): Promise<void> {
    await withKeyedLock(ENQUEUE_LOCK, () => this.recoverEnqueueIntentsUnsafe());
  }

  private async removeEnqueueIntent(id: string): Promise<void> {
    const intents = await this.readEnqueueIntents();
    const remaining = intents.filter((intent) => intent.id !== id);
    if (remaining.length !== intents.length) await this.writeEnqueueIntents(remaining);
  }

  private async addActiveId(scope: string, id: string): Promise<boolean> {
    if (!id || this.activeIndexCapacity(scope) === 0) return false;
    return withKeyedLock(`fireworks-batch:index:${scope}`, async () => {
      const ids = await this.readActiveIds(scope);
      if (ids.includes(id)) return true;
      if (ids.length >= this.activeIndexCapacity(scope)) return false;
      await this.writeActiveIds(scope, [...ids, id]);
      return true;
    });
  }

  private async removeActiveId(scope: string, id: string): Promise<void> {
    if (!id) return;
    await withKeyedLock(`fireworks-batch:index:${scope}`, async () => {
      const ids = await this.readActiveIds(scope);
      if (!ids.includes(id)) return;
      await this.writeActiveIds(scope, ids.filter((candidate) => candidate !== id));
    });
  }

  private async persistIndexedJob(job: FireworksBatchJob): Promise<boolean> {
    return withKeyedLock(`fireworks-batch:index:${KV.fireworksBatchActiveJobs}`, async () => {
      const ids = await this.readActiveIds(KV.fireworksBatchActiveJobs);
      if (ids.length >= this.activeIndexCapacity(KV.fireworksBatchActiveJobs)) return false;
      // The index is the recovery pointer. A crash before the job write leaves
      // a removable dangling pointer, never an undiscoverable durable job.
      await this.writeActiveIds(KV.fireworksBatchActiveJobs, [...ids, job.id]);
      await this.kv.set(KV.fireworksBatchJobs, job.id, job);
      return true;
    });
  }

  private async readActiveWorkItems(): Promise<FireworksBatchWorkItem[]> {
    return withKeyedLock(`fireworks-batch:index:${KV.fireworksBatchActiveWork}`, async () => {
      const ids = await this.readActiveIds(KV.fireworksBatchActiveWork);
      const loaded = await Promise.all(ids.map(async (id) => ({
        id,
        item: await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id),
      })));
      const activeIds: string[] = [];
      const items: FireworksBatchWorkItem[] = [];
      for (const entry of loaded) {
        if (!entry.item || entry.item.id !== entry.id || !ACTIVE_WORK_STATES.has(entry.item.state)) continue;
        activeIds.push(entry.id);
        items.push(entry.item);
      }
      if (activeIds.length !== ids.length) await this.writeActiveIds(KV.fireworksBatchActiveWork, activeIds);
      return items;
    });
  }

  private async readActiveJobs(): Promise<FireworksBatchJob[]> {
    return withKeyedLock(`fireworks-batch:index:${KV.fireworksBatchActiveJobs}`, async () => {
      const ids = await this.readActiveIds(KV.fireworksBatchActiveJobs);
      const loaded = await Promise.all(ids.map(async (id) => ({
        id,
        job: await this.kv.get<FireworksBatchJob>(KV.fireworksBatchJobs, id),
      })));
      const activeIds: string[] = [];
      const jobs: FireworksBatchJob[] = [];
      for (const entry of loaded) {
        if (!entry.job || entry.job.id !== entry.id || !ACTIVE_JOB_STATES.has(entry.job.state)) continue;
        activeIds.push(entry.id);
        jobs.push(entry.job);
      }
      if (activeIds.length !== ids.length) await this.writeActiveIds(KV.fireworksBatchActiveJobs, activeIds);
      return jobs;
    });
  }

  async repairKnownWorkItem(workItemId: string): Promise<boolean> {
    if (typeof workItemId !== "string" || workItemId.length === 0) return false;
    const item = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, workItemId);
    if (!item || !ACTIVE_WORK_STATES.has(item.state)) return false;
    await this.readActiveWorkItems();
    return this.addActiveId(KV.fireworksBatchActiveWork, item.id);
  }

  async repairKnownJob(jobId: string): Promise<boolean> {
    if (typeof jobId !== "string" || jobId.length === 0) return false;
    const job = await this.kv.get<FireworksBatchJob>(KV.fireworksBatchJobs, jobId);
    if (!job || !ACTIVE_JOB_STATES.has(job.state)) return false;
    await this.readActiveJobs();
    return this.addActiveId(KV.fireworksBatchActiveJobs, job.id);
  }

  private async quarantineCompletedLegacyJob(job: FireworksBatchJob): Promise<boolean> {
    const items = await Promise.all(job.workItemIds.map((id) => this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)));
    const ambiguous = items.filter((item): item is FireworksBatchWorkItem => item !== null && !TERMINAL_WORK_STATES.has(item.state));
    if (ambiguous.length === 0) return true;
    if (!job.legacyReconciliationAt) {
      job.legacyReconciliationAt = new Date().toISOString();
      job.updatedAt = job.legacyReconciliationAt;
      await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    }
    await this.markDeadLetter(ambiguous.map((item) => item.id), LEGACY_COMPLETED_AMBIGUOUS_ERROR);
    return true;
  }

  private async reindexPollingExhaustedJob(job: FireworksBatchJob): Promise<boolean> {
    const started = Boolean(job.legacyReconciliationAt);
    if (job.state !== "dead-letter" || job.lastError !== POLLING_EXHAUSTED_ERROR) return false;
    if (!isSafeRemoteJobId(job.remoteJobId) || !isSafeRemoteResource(job.outputDatasetId)) return false;

    const items = await Promise.all(job.workItemIds.map((id) => this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)));
    if (items.some((item) => !item)) return false;
    const ownedItems = items.filter((item): item is FireworksBatchWorkItem => Boolean(item));
    if (ownedItems.some((item) => item.completionIntent
      || (TERMINAL_WORK_STATES.has(item.state)
        && !(item.state === "dead-letter" && item.lastError === POLLING_EXHAUSTED_ERROR)))) {
      return false;
    }

    const now = new Date().toISOString();
    if (!started) {
      job.legacyReconciliationAt = now;
      job.updatedAt = now;
      await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    }
    for (const item of ownedItems) {
      if (!TERMINAL_WORK_STATES.has(item.state)
        || (item.state === "dead-letter" && item.lastError === POLLING_EXHAUSTED_ERROR)) {
        item.callbackProtocolVersion = 1;
        item.state = "polling";
        item.nextAttemptAt = now;
        delete item.lastError;
        delete item.deadLetteredAt;
        item.updatedAt = now;
        await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
      }
      if (ACTIVE_WORK_STATES.has(item.state)) await this.addActiveId(KV.fireworksBatchActiveWork, item.id);
    }

    job.state = "polling";
    job.attempts = 0;
    job.nextAttemptAt = now;
    job.pollAttempts = 0;
    if (!job.pollDeadlineAt || !Number.isFinite(Date.parse(job.pollDeadlineAt)) || Date.parse(job.pollDeadlineAt) <= Date.now()) {
      job.pollDeadlineAt = deadlineFrom(now, this.config);
    }
    delete job.lastError;
    job.updatedAt = now;
    await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    return true;
  }

  private async reindexParserFailureJob(job: FireworksBatchJob): Promise<boolean> {
    if (job.state !== "dead-letter" || job.parserRecovery?.activatedAt
      || !/^batch result row [1-9][0-9]* had no valid response content$/.test(job.lastError ?? "")
      || !job.reconciling || !isSafeRemoteJobId(job.remoteJobId)
      || !isSafeRemoteResource(job.outputDatasetId) || job.workItemIds.length === 0
      || new Set(job.workItemIds).size !== job.workItemIds.length) return false;
    const expectedOutput = `${job.id}-output`;
    const account = `accounts/${this.config.accountId}`;
    if (!/^fwbjob-[a-z0-9-]+$/.test(job.id) || job.remoteJobId !== job.id
      || (job.requestedRemoteJobId !== undefined && job.requestedRemoteJobId !== job.id)
      || (job.remoteJobName !== undefined && job.remoteJobName !== `${account}/batchInferenceJobs/${job.id}`)
      || (job.outputDatasetId !== expectedOutput && job.outputDatasetId !== `${account}/datasets/${expectedOutput}`)) return false;

    const items = await Promise.all(job.workItemIds.map((id) => this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)));
    const owned: FireworksBatchWorkItem[] = [];
    const customIds = new Set<string>();
    for (const [index, item] of items.entries()) {
      if (!item || item.id !== job.workItemIds[index] || item.batchJobId !== job.id
        || item.task !== job.task || item.model !== job.model || item.callbackProtocolVersion !== 1
        || item.completionIntent !== undefined || item.result !== undefined
        || typeof item.customId !== "string" || !item.customId || customIds.has(item.customId)
        || !((item.state === "dead-letter" && item.lastError === job.lastError)
          || (job.parserRecovery && item.state === "polling" && item.lastError === undefined))) return false;
      customIds.add(item.customId);
      const key = batchEffectKey(item.id);
      for (const destination of ["consolidation", "crystallize", "graph", "lessons", "reflect"]) {
        const [receipt, active] = await Promise.all([
          this.kv.get(KV.batchCallbacks, `${destination}:${key}`),
          this.kv.get<{ activeKey?: string }>(KV.batchCallbacks, `active:${destination}`),
        ]);
        if ((receipt !== null && receipt !== undefined) || active?.activeKey === key) return false;
      }
      owned.push(item);
    }

    const now = new Date().toISOString();
    if (!job.parserRecovery) {
      job.parserRecovery = { startedAt: now };
      job.updatedAt = now;
      await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    }
    for (const item of owned) {
      item.state = "polling";
      item.nextAttemptAt = now;
      item.updatedAt = now;
      delete item.lastError;
      delete item.deadLetteredAt;
      await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
      await this.addActiveId(KV.fireworksBatchActiveWork, item.id);
    }
    job.state = "polling";
    job.attempts = 0;
    job.callbackAttempts = 0;
    job.nextAttemptAt = now;
    job.updatedAt = now;
    job.parserRecovery.activatedAt = now;
    delete job.lastError;
    await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    return true;
  }

  private async repairRecentRemoteJobs(): Promise<void> {
    if (!this.transport?.listRecentJobIds) return;
    let recovery = await this.kv.get<FireworksBatchRemoteRecovery>(
      KV.fireworksBatchActiveJobs,
      REMOTE_RECOVERY_KEY,
    );
    if (recovery?.completedAt && recovery.legacyReconciliationVersion === 3) return;
    if (recovery?.completedAt) {
      try {
        recovery.pendingJobIds = boundedActiveIds(await this.transport.listRecentJobIds(MAX_ACTIVE_INDEX_IDS));
        delete recovery.completedAt;
        recovery.updatedAt = new Date().toISOString();
        await this.kv.set(KV.fireworksBatchActiveJobs, REMOTE_RECOVERY_KEY, recovery);
      } catch (error) {
        logger.warn("Fireworks Batch legacy recovery discovery failed", {
          error: safeBatchErrorMessage(error, "remote job discovery failed"),
        });
        return;
      }
    }
    if (!recovery) {
      try {
        const now = new Date().toISOString();
        recovery = {
          version: 1,
          pendingJobIds: boundedActiveIds(await this.transport.listRecentJobIds(MAX_ACTIVE_INDEX_IDS)),
          discoveredAt: now,
          updatedAt: now,
        };
        await this.kv.set(KV.fireworksBatchActiveJobs, REMOTE_RECOVERY_KEY, recovery);
      } catch (error) {
        logger.warn("Fireworks Batch remote recovery discovery failed", {
          error: safeBatchErrorMessage(error, "remote job discovery failed"),
        });
        return;
      }
    }

    const activeIds = await this.readActiveIds(KV.fireworksBatchActiveJobs);
    let available = Math.max(0, this.activeIndexCapacity(KV.fireworksBatchActiveJobs) - activeIds.length);
    const remaining: string[] = [];
    let repaired = 0;
    for (const jobId of recovery.pendingJobIds) {
      const job = await this.kv.get<FireworksBatchJob>(KV.fireworksBatchJobs, jobId);
      if (!job || job.id !== jobId) continue;
      if (job.state === "completed") {
        await this.quarantineCompletedLegacyJob(job);
        continue;
      }
      if (job.state === "dead-letter") {
        await this.reindexParserFailureJob(job);
        await this.reindexPollingExhaustedJob(job);
      }
      if (!ACTIVE_JOB_STATES.has(job.state)) continue;
      if (available === 0) {
        remaining.push(jobId);
        continue;
      }
      if (await this.addActiveId(KV.fireworksBatchActiveJobs, jobId)) {
        available--;
        repaired++;
      } else {
        remaining.push(jobId);
      }
    }
    const now = new Date().toISOString();
    recovery.pendingJobIds = remaining;
    recovery.updatedAt = now;
    if (remaining.length === 0) {
      recovery.completedAt = now;
      recovery.legacyReconciliationVersion = 3;
    }
    await this.kv.set(KV.fireworksBatchActiveJobs, REMOTE_RECOVERY_KEY, recovery);
    if (repaired > 0) {
      logger.info("Fireworks Batch recovered remote jobs into the active index", { repaired });
    }
  }

  private async reconcileAmbiguousSubmission(job: FireworksBatchJob): Promise<{
    attempted: boolean;
    candidate?: string;
    status?: FireworksBatchRemoteStatus;
  }> {
    const listRecentJobIds = this.transport?.listRecentJobIds;
    if (!listRecentJobIds) return { attempted: false };

    const requestedId = remoteCandidateId(job.requestedRemoteJobId);
    let listedIds: string[] = [];
    try {
      listedIds = boundedActiveIds(await listRecentJobIds(MAX_REMOTE_RECONCILIATION_CANDIDATES));
    } catch (error) {
      logger.warn("Fireworks Batch remote reconciliation discovery failed", {
        jobId: job.id,
        error: safeBatchErrorMessage(error, "remote job discovery failed"),
      });
    }

    const candidates: string[] = [];
    const seen = new Set<string>();
    const discoveryLimit = requestedId
      ? MAX_REMOTE_RECONCILIATION_CANDIDATES - 1
      : MAX_REMOTE_RECONCILIATION_CANDIDATES;
    for (const listedId of listedIds) {
      const candidate = remoteCandidateId(listedId);
      if (!candidate || seen.has(candidate) || candidates.length >= discoveryLimit) continue;
      seen.add(candidate);
      candidates.push(candidate);
    }
    if (requestedId && !seen.has(requestedId)) candidates.push(requestedId);

    for (const candidate of candidates) {
      let status: FireworksBatchRemoteStatus;
      try {
        status = await this.transport!.getJobStatus(candidate);
      } catch {
        continue;
      }
      if (!remoteStatusBelongsToJob(job, candidate, status, requestedId)) continue;
      delete job.remoteReconciliationAttempts;
      return { attempted: true, candidate, status };
    }

    const previousAttempts = Number.isInteger(job.remoteReconciliationAttempts)
      && (job.remoteReconciliationAttempts ?? 0) >= 0
      ? job.remoteReconciliationAttempts!
      : 0;
    const attempts = previousAttempts + 1;
    job.remoteReconciliationAttempts = attempts;
    if (attempts >= Math.max(1, Math.min(MAX_REMOTE_RECONCILIATION_ATTEMPTS, Math.floor(this.config.maxAttempts)))) {
      await this.exhaustRemoteReconciliation(job);
    } else {
      job.state = "polling";
      job.nextAttemptAt = retryAt(this.config, attempts);
      job.updatedAt = new Date().toISOString();
      await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    }
    return { attempted: true };
  }

  private async exhaustRemoteReconciliation(job: FireworksBatchJob): Promise<void> {
    const error = "batch submission identity reconciliation exhausted; automatic recovery blocked";
    job.state = "dead-letter";
    job.lastError = error;
    job.updatedAt = new Date().toISOString();
    await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    await this.markDeadLetter(job.workItemIds, error);
    await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
  }

  private applyRemoteMetadata(
    job: FireworksBatchJob,
    metadata: Partial<FireworksBatchRemoteStatus> & {
      remoteJobId?: unknown;
      remoteJobName?: unknown;
      inputDatasetId?: unknown;
      outputDatasetId?: unknown;
    },
  ): void {
    const identity = normalizeRemoteJobId(metadata.remoteJobId);
    if (identity) {
      job.remoteJobId = identity.id;
      if (identity.name) job.remoteJobName = identity.name;
    }
    const name = optionalRemoteResource(metadata.remoteJobName, "job name");
    if (name) {
      const id = lastPathSegment(name);
      if (!isSafeRemoteJobId(id)) {
        throw new FireworksBatchReconciliationError("remote response included an invalid job name");
      }
      job.remoteJobName = name;
      job.remoteJobId = id;
    }
    const inputDatasetId = optionalRemoteResource(metadata.inputDatasetId, "input dataset ID");
    if (inputDatasetId) job.inputDatasetId = inputDatasetId;
    const outputDatasetId = optionalRemoteResource(metadata.outputDatasetId, "output dataset ID");
    if (outputDatasetId) job.outputDatasetId = outputDatasetId;
  }

  async enqueue(request: FireworksBatchRequest): Promise<{ queued: boolean; workItemId?: string; reason?: string }> {
    if (!this.config.enabled || !this.transport || !this.config.model) {
      return { queued: false, reason: "Fireworks Batch is disabled" };
    }
    const remotePromptChars = request.systemPrompt.length + request.userPrompt.length;
    const metadataChars = (Object.values(request.metadata ?? {}) as string[])
      .reduce((total, value) => total + value.length, 0);
    if (!request.systemPrompt || !request.userPrompt || remotePromptChars > this.config.maxRequestChars) {
      return { queued: false, reason: "Batch request exceeded configured limits" };
    }
    const maxTokens = request.maxTokens ?? (request.task === "graph_extraction" || request.task === "consolidation"
      ? 8192 : taskOutputTokens(batchTaskLlmTask(request.task), 1024));
    if (!Number.isSafeInteger(maxTokens) || maxTokens < 1 || maxTokens > 16384) {
      return { queued: false, reason: "Batch output budget must be an integer between 1 and 16384" };
    }
    if (metadataChars > MAX_PERSISTED_METADATA_CHARS) {
      return { queued: false, reason: "Batch provenance metadata exceeded local limits" };
    }
    return withKeyedLock(ENQUEUE_LOCK, async () => {
      await this.recoverEnqueueIntentsUnsafe();
      if (request.replacementOf && (!request.metadata?.sourceFingerprint || !await this.canReplace(request.replacementOf))) {
        return { queued: false, reason: "Replacement source is unsafe or missing its fingerprint" };
      }
      const fingerprint = request.replacementOf
        ? fingerprintId("fwbrepl", `${request.replacementOf}\0${request.metadata!.sourceFingerprint}`)
        : fingerprintId("fwb", `${request.task}\0${request.systemPrompt}\0${request.userPrompt}`);
      const existingId = await this.kv.get<string>(KV.fireworksBatchFingerprints, fingerprint);
      if (existingId) {
        const existing = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, existingId);
        if (existing?.state === "completed" && request.metadata?.sourceFingerprint
          && existing.metadata?.sourceFingerprint === request.metadata.sourceFingerprint) {
          return { queued: true, workItemId: existing.id };
        }
        if (request.replacementOf && existing?.replacementOf === request.replacementOf && TERMINAL_WORK_STATES.has(existing.state)) {
          return { queued: true, workItemId: existing.id };
        }
        if (existing && existing.callbackProtocolVersion !== 1) {
          await this.quarantineLegacyWork(existing);
          // A new ID would silently replay an effect whose legacy outcome is unknown.
          return { queued: true, workItemId: existing.id };
        }
        if (existing && request.metadata?.batchEffectKey && TERMINAL_WORK_STATES.has(existing.state)) {
          return { queued: true, workItemId: existing.id };
        }
        if (existing && ACTIVE_WORK_STATES.has(existing.state)) {
          const repaired = await this.repairKnownWorkItem(existing.id);
          return repaired
            ? { queued: true, workItemId: existing.id }
            : { queued: false, reason: "Batch queue is full" };
        }
      }
      const pending = await this.readActiveWorkItems();
      if (pending.length >= this.activeIndexCapacity(KV.fireworksBatchActiveWork)) {
        return { queued: false, reason: "Batch queue is full" };
      }
      const now = new Date().toISOString();
      const item: FireworksBatchWorkItem = {
        ...(request.replacementOf ? { replacementOf: request.replacementOf } : {}),
        callbackProtocolVersion: 1,
        id: generateId("fwbwork"),
        customId: request.correlationId,
        correlationId: request.correlationId,
        task: request.task,
        model: request.model || this.config.model!,
        systemPrompt: request.systemPrompt,
        userPrompt: request.userPrompt,
        maxTokens,
        metadata: request.metadata,
        state: "queued",
        attempts: 0,
        nextAttemptAt: now,
        createdAt: now,
        updatedAt: now,
      };
      const intent: FireworksBatchEnqueueIntent = {
        version: 1,
        id: item.id,
        fingerprint,
        item,
        createdAt: now,
        updatedAt: now,
      };
      const intents = await this.readEnqueueIntents();
      if (intents.length >= MAX_ENQUEUE_INTENTS) {
        return { queued: false, reason: "Batch queue is full" };
      }
      await this.writeEnqueueIntents([...intents, intent]);
      try {
        await Promise.all([
          this.kv.set(KV.fireworksBatchWorkItems, item.id, item),
          this.kv.set(KV.fireworksBatchFingerprints, fingerprint, item.id),
        ]);
        const indexed = await this.addActiveId(KV.fireworksBatchActiveWork, item.id);
        if (!indexed) {
          return { queued: false, reason: "Batch queue is full" };
        }
        await this.removeEnqueueIntent(item.id);
      } catch (error) {
        await this.removeActiveId(KV.fireworksBatchActiveWork, item.id);
        throw error;
      }
      logger.info("Fireworks Batch work queued", { task: item.task, workItemId: item.id });
      return { queued: true, workItemId: item.id };
    });
  }

  async process(): Promise<void> {
    if (!this.config.enabled || !this.transport) return;
    if (this.processInFlight) return this.processInFlight;
    const run = (async () => {
      await this.recoverEnqueueIntents();
      for (const item of await this.readActiveWorkItems()) await this.quarantineLegacyWork(item);
      await this.repairRecentRemoteJobs();
      await this.pollSubmitted();
      await this.submitQueued();
      await this.pollSubmitted();
    })();
    this.processInFlight = run;
    try {
      await run;
    } finally {
      if (this.processInFlight === run) this.processInFlight = undefined;
    }
  }

  private async submitQueued(): Promise<void> {
    const transport = this.transport;
    if (!transport) return;
    const now = new Date().toISOString();
    const [items, jobs] = await Promise.all([
      this.readActiveWorkItems(),
      this.readActiveJobs(),
    ]);
    if (jobs.length >= this.activeIndexCapacity(KV.fireworksBatchActiveJobs)) return;
    const groups = new Map<string, FireworksBatchWorkItem[]>();
    for (const item of items.filter((candidate) => candidate.state === "queued" && candidate.nextAttemptAt <= now)) {
      if (await this.quarantineLegacyWork(item)) continue;
      const key = compatibilityKey(item);
      const group = groups.get(key) ?? [];
      group.push(item);
      groups.set(key, group);
    }
    const eligible = [...groups.values()]
      .map((group) => group.sort((left, right) => left.createdAt.localeCompare(right.createdAt)).slice(0, this.config.maxBatchItems))
      .filter((group) => group.length >= this.config.minBatchItems || Date.now() - oldestCreatedAt(group) >= this.config.maxWaitMs)
      .sort((left, right) => oldestCreatedAt(left) - oldestCreatedAt(right));
    const budget = resultBudget(this.config);
    for (const group of eligible) {
      let remaining = group;
      while (remaining.length > 0) {
        const compatible = fittingBatchPrefix(
          remaining,
          this.config.maxRequestBytes,
          this.config.maxRequestChars,
          budget.chars,
          budget.bytes,
        );
        if (compatible.length === 0) {
          const oversized = remaining[0];
          const row = batchJsonlLine(oversized);
          const rowBytes = Buffer.byteLength(row, "utf8");
          const estimated = estimatedResultSize(oversized);
          const limits = [
            rowBytes > this.config.maxRequestBytes ? "byte limit" : undefined,
            row.length > this.config.maxRequestChars ? "character limit" : undefined,
            ...resultLimitNames(estimated, budget),
          ].filter((limit): limit is string => Boolean(limit));
          const requestLimits = limits.filter((limit) => limit === "byte limit" || limit === "character limit");
          const resultLimits = limits.filter((limit) => limit !== "byte limit" && limit !== "character limit");
          const category = resultLimits.length > 0 && requestLimits.length === 0
            ? "result row"
            : "request row";
          const error = `${category} exceeded configured ${limits.join(" and ") || "request limits"} (${row.length} chars, ${rowBytes} bytes; estimated result ${estimated.chars} chars, ${estimated.bytes} bytes)`;
          await this.markDeadLetter([oversized.id], error);
          logger.warn("Fireworks Batch work item exceeded request or result limits", {
            task: oversized.task,
            workItemId: oversized.id,
            rowBytes,
            estimatedResultChars: estimated.chars,
            estimatedResultBytes: estimated.bytes,
          });
          remaining = remaining.slice(1);
          continue;
        }

        const first = compatible[0];
        const jobId = fireworksResourceId(generateId("fwbjob"));
        const inputDatasetId = `${jobId}-input`;
        const outputDatasetId = `${jobId}-output`;
        const jsonl = compatible.map(batchJsonlLine).join("\n");
        const job: FireworksBatchJob = {
          id: jobId,
          requestedRemoteJobId: jobId,
          inputDatasetId,
          outputDatasetId,
          model: first.model,
          task: first.task,
          workItemIds: compatible.map((item) => item.id),
          state: "submitted",
          attempts: 1,
          pollAttempts: 0,
          nextAttemptAt: new Date(Date.now() + this.config.pollIntervalMs).toISOString(),
          pollDeadlineAt: deadlineFrom(now, this.config),
          createdAt: now,
          updatedAt: now,
        };
        if (!await this.persistIndexedJob(job)) return;
        await Promise.all(compatible.map(async (item) => {
          item.state = "submitted";
          item.batchJobId = job.id;
          item.updatedAt = now;
          await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
        }));
        logger.info("Fireworks Batch submitting work", { jobId, task: first.task, itemCount: compatible.length });
        let submissionAccepted = false;
        try {
          await transport.createDataset(inputDatasetId, compatible.length);
          await transport.uploadDataset(inputDatasetId, jsonl);
          job.submitAttemptedAt = new Date().toISOString();
          job.pollDeadlineAt = deadlineFrom(job.submitAttemptedAt, this.config);
          job.updatedAt = job.submitAttemptedAt;
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          const submission = await transport.submitJob({ jobId, inputDatasetId, outputDatasetId, model: job.model, maxTokens: first.maxTokens });
          submissionAccepted = true;
          this.applyRemoteMetadata(job, submission);
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        } catch (error) {
          const message = safeBatchErrorMessage(error, "batch submission failed");
          const ambiguous = Boolean(job.submitAttemptedAt) && (submissionAccepted || isAmbiguousSubmitError(error));
          job.state = ambiguous ? "polling" : "dead-letter";
          job.lastError = message;
          if (ambiguous) job.nextAttemptAt = new Date(0).toISOString();
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          if (ambiguous) {
            logger.warn("Fireworks Batch submit uncertain; scheduling remote reconciliation", { jobId: job.id, error: message });
          } else {
            await this.markDeadLetter(job.workItemIds, message);
            logger.warn("Fireworks Batch submit failed; work dead-lettered", { jobId: job.id, error: message });
          }
        }
        return;
      }
    }
  }

  private async ensurePollDeadline(job: FireworksBatchJob): Promise<string> {
    const existing = typeof job.pollDeadlineAt === "string" ? Date.parse(job.pollDeadlineAt) : Number.NaN;
    if (Number.isFinite(existing) && existing <= Date.now() + MAX_POLL_DEADLINE_MS) return job.pollDeadlineAt!;
    job.pollDeadlineAt = deadlineFrom(undefined, this.config);
    job.updatedAt = new Date().toISOString();
    await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    return job.pollDeadlineAt;
  }

  private async pollSubmitted(): Promise<void> {
    const now = new Date().toISOString();
    const jobs = await this.readActiveJobs();
    for (const job of jobs.filter((candidate) => (candidate.state === "submitted" || candidate.state === "polling") && candidate.nextAttemptAt <= now).slice(0, this.activeIndexCapacity(KV.fireworksBatchActiveJobs))) {
      const maxAttempts = Math.max(1, Math.floor(this.config.maxAttempts));
      if (job.reconciling && (!Number.isInteger(job.callbackAttempts ?? 0) || (job.callbackAttempts ?? 0) < 0 || (job.callbackAttempts ?? 0) >= maxAttempts)) {
        await this.exhaustCallbacks(job);
        continue;
      }
      if (!job.reconciling && !job.remoteJobId && !job.submitAttemptedAt) {
        job.state = "dead-letter";
        job.lastError = "batch submission has no proven remote identity; automatic recovery blocked";
        job.updatedAt = new Date().toISOString();
        await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        await this.markDeadLetter(job.workItemIds, job.lastError);
        await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
        continue;
      }
      const reconciliation = !job.remoteJobId && job.submitAttemptedAt
        ? await this.reconcileAmbiguousSubmission(job)
        : { attempted: false as const };
      if (reconciliation.attempted && !reconciliation.status) continue;
      const remoteJobId = job.remoteJobId
        || reconciliation.candidate
        || (job.submitAttemptedAt && job.requestedRemoteJobId && isSafeRemoteJobId(job.requestedRemoteJobId) ? job.requestedRemoteJobId : undefined);
      if (!remoteJobId) {
        job.state = "dead-letter";
        job.lastError = "batch submission has no proven remote identity; automatic recovery blocked";
        job.updatedAt = new Date().toISOString();
        await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        await this.markDeadLetter(job.workItemIds, job.lastError);
        await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
        continue;
      }
      if (!job.reconciling) {
        const pollDeadlineAt = await this.ensurePollDeadline(job);
        if (Date.parse(pollDeadlineAt) <= Date.now()) {
          job.state = "dead-letter";
          job.lastError = POLLING_DEADLINE_ERROR;
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          await this.markDeadLetter(job.workItemIds, job.lastError);
          await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
          continue;
        }
      }
      try {
        const status = reconciliation.status
          ? {
            ...reconciliation.status,
            ...(reconciliation.status.remoteJobId || reconciliation.status.remoteJobName
              ? {}
              : { remoteJobId: reconciliation.candidate }),
          }
          : job.reconciling ? { state: "COMPLETED" } : await this.transport!.getJobStatus(remoteJobId);
        this.applyRemoteMetadata(job, status);
        job.updatedAt = new Date().toISOString();
        await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        const state = status.state.toUpperCase();
        if (state.includes("SUCCEEDED") || state.includes("COMPLETED") || state.includes("EXPIRED")) {
          job.reconciling = true;
          job.callbackAttempts = incrementAttempts(job.callbackAttempts ?? 0, this.config.maxAttempts).attempts;
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          await this.applyResults(job);
          job.state = "completed";
          job.completedAt = new Date().toISOString();
          job.updatedAt = job.completedAt;
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
        } else if (state.includes("FAILED") || state.includes("CANCELLED")) {
          job.state = "dead-letter";
          job.lastError = safeRemoteStatusMessage(status.message) || "remote batch failed";
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          await this.markDeadLetter(job.workItemIds, job.lastError);
          await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
        } else {
          const previousPollAttempts = typeof job.pollAttempts === "number"
            && Number.isFinite(job.pollAttempts) && job.pollAttempts >= 0
            ? Math.floor(job.pollAttempts)
            : 0;
          job.pollAttempts = previousPollAttempts + 1;
          job.state = "polling";
          delete job.lastError;
          job.nextAttemptAt = pollRetryAt(this.config, job.pollAttempts);
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        }
      } catch (error) {
        const applying = job.reconciling && (error instanceof FireworksBatchCallbackError || (
          await Promise.all(job.workItemIds.map((id) => this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)))
        ).some((item) => item?.completionIntent && !TERMINAL_WORK_STATES.has(item.state)));
        if (applying) {
          if ((job.callbackAttempts ?? 0) >= maxAttempts) {
            await this.exhaustCallbacks(job);
            continue;
          }
          job.state = "polling";
          job.lastError = safeBatchErrorMessage(error, "batch callback failed");
          job.nextAttemptAt = retryAt(this.config, Math.max(1, job.callbackAttempts ?? 1));
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          continue;
        }
        const attempt = incrementAttempts(job.attempts, this.config.maxAttempts);
        job.attempts = attempt.attempts;
        job.updatedAt = new Date().toISOString();
        job.lastError = safeBatchErrorMessage(error, "batch polling failed");
        if (attempt.exhausted) {
          job.state = "dead-letter";
          await this.markDeadLetter(job.workItemIds, job.lastError);
        } else {
          job.state = "polling";
          job.nextAttemptAt = retryAt(this.config, job.attempts);
        }
        await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        if (job.state === "dead-letter") await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
      }
    }
  }

  private async applyResults(job: { outputDatasetId: string; workItemIds: string[] }): Promise<void> {
    const downloaded = await this.transport!.downloadResults(job.outputDatasetId);
    const { resultText, errorText } = normalizeDownloadedResults(downloaded);
    if (resultText.length + errorText.length > this.config.maxResultChars) {
      throw new FireworksBatchReconciliationError("batch result exceeded configured limits");
    }
    const rows = [
      ...parseJsonlRows(resultText, "result"),
      ...parseJsonlRows(errorText, "error"),
    ];
    const items = await Promise.all(job.workItemIds.map((id) => this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)));
    if (items.some((item) => !item)) {
      throw new FireworksBatchReconciliationError("batch result referenced missing local work");
    }
    const ownedItems = items.filter((item): item is FireworksBatchWorkItem => Boolean(item));
    const byCustomId = new Map<string, FireworksBatchWorkItem>();
    for (const item of ownedItems) {
      if (byCustomId.has(item.customId)) {
        throw new FireworksBatchReconciliationError("batch work contained duplicate custom_id");
      }
      byCustomId.set(item.customId, item);
    }

    const ownedRows = new Map<string, ParsedBatchRow>();
    let unknownRows = 0;
    for (const row of rows) {
      if (!byCustomId.has(row.customId)) {
        unknownRows++;
        continue;
      }
      if (ownedRows.has(row.customId)) {
        throw new FireworksBatchReconciliationError("batch result contained duplicate custom_id");
      }
      ownedRows.set(row.customId, row);
    }
    if (unknownRows > 0) {
      logger.warn("Fireworks Batch result included unknown custom IDs", { workItemCount: job.workItemIds.length, unknownRows });
    }

    for (const item of ownedItems) {
      await this.quarantineLegacyWork(item);
      if (!ownedRows.has(item.customId) && !TERMINAL_WORK_STATES.has(item.state)) {
        throw new FireworksBatchReconciliationError(`batch result missing custom_id ${item.customId}`);
      }
    }

    for (const item of ownedItems) {
      if (TERMINAL_WORK_STATES.has(item.state)) continue;
      const row = ownedRows.get(item.customId);
      if (!row) continue;
      if (row.kind === "error") {
        await this.markDeadLetter([item.id], row.error);
        continue;
      }
      const intent = { key: batchEffectKey(item.id), resultHash: batchEffectKey(JSON.stringify([row.content, row.usage])) };
      if (item.completionIntent && (item.completionIntent.key !== intent.key || item.completionIntent.resultHash !== intent.resultHash)) {
        throw new FireworksBatchCallbackError("batch completion result changed during retry");
      }
      let completion: Awaited<ReturnType<CompletedHandler>>;
      try {
        if (!item.completionIntent) {
          item.completionIntent = intent;
          await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
        }
        if (row.usage && this.onUsage) await this.onUsage(item, row.usage);
        completion = await this.onCompleted(item, row.content);
        const failure = completionFailure(completion);
        if (failure) throw new Error(failure);
      } catch (error) {
        throw new FireworksBatchCallbackError(safeBatchErrorMessage(error, "batch callback failed"));
      }
      item.state = completion === "stale" ? "stale" : "completed";
      const receivedAt = new Date().toISOString();
      item.result = { customId: item.customId, content: row.content, receivedAt };
      item.updatedAt = receivedAt;
      await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
      await this.removeActiveId(KV.fireworksBatchActiveWork, item.id);
    }
  }

  private async markDeadLetter(ids: string[], error: string | undefined): Promise<void> {
    const now = new Date().toISOString();
    await Promise.all(ids.map(async (id) => {
      const item = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id);
      if (!item || TERMINAL_WORK_STATES.has(item.state)) return;
      item.state = "dead-letter";
      item.deadLetteredAt = now;
      item.lastError = error;
      item.updatedAt = now;
      await this.kv.set(KV.fireworksBatchWorkItems, id, item);
      await this.removeActiveId(KV.fireworksBatchActiveWork, id);
    }));
  }

  private async quarantineLegacyWork(item: FireworksBatchWorkItem): Promise<boolean> {
    if (item.callbackProtocolVersion === 1) return false;
    if (!TERMINAL_WORK_STATES.has(item.state)) {
      await this.markDeadLetter([item.id], "Legacy callback outcome is ambiguous; automatic replay blocked. Reconcile downstream effects before submitting distinct replacement work.");
      item.state = "dead-letter";
    }
    return true;
  }

  private async exhaustCallbacks(job: FireworksBatchJob): Promise<void> {
    const error = "Batch callback attempts exhausted; reconcile partial downstream receipts before replacement work";
    // Retain receiver receipts: a partial destination must remain blocked,
    // never appear recovered merely because automatic retries are exhausted.
    await this.markDeadLetter(job.workItemIds, error);
    job.state = "dead-letter";
    job.lastError = error;
    job.updatedAt = new Date().toISOString();
    await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    await this.removeActiveId(KV.fireworksBatchActiveJobs, job.id);
  }
}
