import { createHash } from "node:crypto";
import { runWorkflowAttempt, type WorkflowAttemptAccounting } from "./workflow-attempt-accounting.js";
import type { IIIClient } from "iii-sdk";
import type {
  CompressedObservation, MemoryProvider, Session, SessionSummary,
  SummaryQueueIntent as StoredSummaryQueueIntent, SummaryQueueJob as StoredSummaryQueueJob,
} from "../types.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { StateKV } from "../state/kv.js";
import { readSummarySnapshotSource } from "./summary-snapshot-source.js";
import { KV, fingerprintId, generateId } from "../state/schema.js";
import { evaluatePublicationEvidence, observePublicationEvidence, preparePublicationEvidence, type PublicationEvidenceSource } from "./publication-evidence.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { getSummaryBudgetConfig, isBackgroundRecoveryPaused } from "../config.js";
import { recordAudit } from "./audit.js";
import { publicationExecutor } from "./publication-call-options.js";
import { registerSummaryPublicationRecovery } from "./summary-jobs.js";
import { SummaryOutputSchema } from "../eval/schemas.js";
import { validateOutput } from "../eval/validator.js";
import { scoreSummary } from "../eval/quality.js";
import { logger } from "../logger.js";
import {
  formatSummaryObservation, formatSummaryPartial, type SummaryPromptItem,
} from "../prompts/summary.js";
import {
  REDUCE_SYSTEM, SUMMARY_GENERATION_REVISION, SUMMARY_RECOVERY_FIDELITY_VERSION, SUMMARY_SYSTEM,
  buildReduceItemsPrompt, buildSummaryItemsPrompt,
} from "./summary-queue-prompts.js";
import {
  MAX_SUMMARY_CALLS, MAX_SUMMARY_DEPTH, MAX_SUMMARY_ITEMS, SummaryBudgetError, estimateSummaryTokens,
  packSummaryItems, summaryCallInputLimit, summaryChunkInputLimit,
  summaryOutputTokenBudget, summaryProgressSize, summaryReduceInputLimit,
} from "./summary-budget.js";
import { parseSummaryXml } from "./summary-xml.js";
import {
  groundSummarySourcePaths, summarySourcePathLiterals,
  captureSummaryEvidence, getSummaryEvidenceRoot, readSummaryEvidenceSource, verifySummaryEvidence,
  type SummaryEvidenceJob, type SummaryEvidenceRoot, type SummarySourceReference,
} from "./summary-source-grounding.js";
import { recordOperation } from "./dashboard-activity.js";
import { prepareFullOriginalSummaryRequest } from "./summary-recovery.js";
import { assertZeroSpendSummaryPredecessor, constructSummaryRecovery, summaryConstructionKey,
  summaryRecoveryReady, zeroSpendSummaryIdentity, type SummaryRecoveryConstruction } from "./summary-zero-spend-recovery.js";
import { claimSummaryPromptCorrection, eligibleSummaryPromptPredecessor, summaryPromptCorrectionId,
  summaryPromptCorrectionKey, type SummaryPromptCorrection } from "./summary-prompt-correction.js";

const DISPATCH_TOPIC = "agentmemory.summary.dispatch";
const UNIT_TOPIC = "agentmemory.summary.unit";
const DISPATCH_BATCH = 12;
const MAX_ATTEMPTS = 6; // Five subscriber retries plus the first delivery.
const FAILED_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const MAX_RECOVERY_INTENTS = 100;
const STALE_DELIVERY_MS = 60_000;
const RETRY_BACKOFF_MS = 30_000;
const SUMMARY_CALL_TIMEOUT_MS = 150_000;
const MAX_UNIT_RUNTIME_MS = SUMMARY_CALL_TIMEOUT_MS + STALE_DELIVERY_MS;
type SummaryQueueJob = StoredSummaryQueueJob & {
  executionOrigin?: "selected" | "manual";
  recoveryGeneration?: "full-originals-v2";
};
type SummaryQueueIntent = StoredSummaryQueueIntent & { executionOrigin?: "selected" | "manual" };
async function summaryRecoveryStep<T>(phase: string, context: { jobId?: string; sessionId?: string }, run: () => Promise<T>): Promise<T> {
  try { return await run(); }
  catch (error) {
    const record = error && typeof error === "object" ? error as { code?: unknown; name?: unknown; message?: unknown } : {};
    const identifier = (value: unknown) => typeof value === "string" && /^[A-Za-z0-9_:.-]{1,128}$/.test(value) ? value : undefined;
    const stateCode = String(record.message ?? error).match(/\bSTATE_[A-Z0-9_]+\b/)?.[0];
    logger.warn("Summary recovery step failed", { phase, ...context,
      code: identifier(record.code), errorClass: identifier(record.name), stateCode });
    throw error;
  }
}

class ManualCutComplete extends Error {}
export interface ManualSummaryContinuation {
  operationId: string; step: number; jobId: string; project: string; sessionId: string;
  sourceFingerprint: string; round: number; unitId: string; deliveryId: string;
  recoveryGeneration?: "full-originals-v2";
}
type SummaryRecoveryGeneration = "full-originals-v1" | "full-originals-v2";
const CURRENT_SUMMARY_RECOVERY_GENERATION = "full-originals-v2";
export interface ManualOperation {
  id: string; sessionId: string; jobId: string; project: string; sourceFingerprint: string;
  step: number; maxRequests: number; deadline?: number;
  continuation?: ManualSummaryContinuation;
}
type ManualRecoveryResolution =
  | { kind: "job"; job: SummaryQueueJob }
  | { kind: "response"; response: Record<string, unknown> };
interface ManualTerminalizationRequest {
  activeLease: "required" | "matching-or-empty";
  legacyRetirement?: true;
}

async function withSummaryCallTimeout<T>(call: () => Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      call(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error("Summary provider timed out")), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

function retryEligible(unit: SummaryQueueUnit, now: number): boolean {
  if (unit.attempts === 0) return true;
  if (!unit.lastAttemptAt || unit.attempts >= MAX_ATTEMPTS) return false;
  const due = Date.parse(unit.lastAttemptAt) + RETRY_BACKOFF_MS;
  return Number.isFinite(due) && now >= due;
}

export interface SummaryQueueUnit {
  id: string;
  jobId: string;
  stage: "map" | "reduce";
  round: number;
  items: SummaryPromptItem[];
  attempts: number;
  dispatchedAt?: string;
  deliveryId?: string;
  startedAt?: string;
  lastAttemptAt?: string;
  outputInputHash?: string;
  output?: SummaryPromptItem;
  summary?: SessionSummary;
  lastError?: string;
}
interface SummaryUnitLineage {
  inputHash: string;
  sources: PublicationEvidenceSource[];
  children: string[];
}
interface ValidatedSummaryUnit {
  inputHash: string; outputHash: string; requestId: string;
  output: SummaryPromptItem; summary?: SessionSummary;
}

interface ActiveJob { jobId: string }

async function setSummaryActiveJob(kv: StateKV, sessionId: string, jobId: string): Promise<void> {
  await withKeyedLock(`summary-active:${sessionId}`, () =>
    kv.set(KV.summaryQueueActive, sessionId, { jobId } satisfies ActiveJob));
}

async function releaseSummaryActiveJob(kv: StateKV, sessionId: string, jobId: string): Promise<void> {
  await withKeyedLock(`summary-active:${sessionId}`, async () => {
    const active = await kv.get<ActiveJob>(KV.summaryQueueActive, sessionId);
    if (active?.jobId === jobId) await kv.delete(KV.summaryQueueActive, sessionId);
  });
}

interface CompletedSnapshot { sessionId: string; fingerprint: string; completedAt: string; jobId?: string }
interface FullOriginalRecovery {
  sourceJob: SummaryQueueJob;
  root: SummaryEvidenceRoot;
  sources: PublicationEvidenceSource[];
  observations: CompressedObservation[];
  items: SummaryPromptItem[];
  sourcePaths: string[];
  systemPrompt: string;
  userPrompt: string;
  inputBytes: number;
  inputTokens: number;
  outputTokens: number;
}

interface SelectedSummary {
  project: string;
  sessionId: string;
  sourceFingerprint: string;
  jobId?: string;
  round?: number;
  unitId?: string;
  deliveryId?: string;
  manual?: true;
  manualStep?: number;
}

export function validManualSummaryContinuation(value: unknown): value is ManualSummaryContinuation {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  const strings = ["operationId", "jobId", "project", "sessionId", "sourceFingerprint", "unitId", "deliveryId"];
  const allowed = new Set([...strings, "step", "round", "recoveryGeneration"]);
  const keys = Object.keys(row);
  const hasGeneration = Object.prototype.hasOwnProperty.call(row, "recoveryGeneration");
  return (keys.length === 9 || hasGeneration && keys.length === 10) && keys.every(key => allowed.has(key)) &&
    (!hasGeneration || row.recoveryGeneration === CURRENT_SUMMARY_RECOVERY_GENERATION) &&
    strings.every(key => typeof row[key] === "string" &&
    (row[key] as string).length > 0 && (row[key] as string).length <= 512 && row[key] === (row[key] as string).trim()) &&
    Number.isSafeInteger(row.step) && (row.step as number) >= 0 && Number.isSafeInteger(row.round) && (row.round as number) >= 0;
}

function manualRecoveryRequestId(
  job: Pick<SummaryQueueJob, "id" | "project" | "sessionId" | "snapshotFingerprint">,
  generation: SummaryRecoveryGeneration = "full-originals-v1",
): string {
  return fingerprintId("summary-recovery", JSON.stringify([SUMMARY_RECOVERY_FIDELITY_VERSION,
    generation, job.id, job.project, job.sessionId, job.snapshotFingerprint]));
}

function manualRecoveryContinuation(
  job: SummaryQueueJob, generation: SummaryRecoveryGeneration = CURRENT_SUMMARY_RECOVERY_GENERATION,
): ManualSummaryContinuation | undefined {
  if (!Array.isArray(job.unitIds)) return undefined;
  const unitId = job.unitIds.at(-1);
  if (!unitId || job.failure !== "summary_publication_invalid_response" || job.recoveryMode !== undefined ||
      job.recoveryOfJobId !== undefined || job.recoveryGeneration !== undefined) return undefined;
  const operationId = manualRecoveryRequestId(job, generation);
  return { operationId, step: 0, jobId: job.id, project: job.project, sessionId: job.sessionId,
    sourceFingerprint: job.snapshotFingerprint, round: job.round, unitId, deliveryId: operationId,
    ...(generation === CURRENT_SUMMARY_RECOVERY_GENERATION && { recoveryGeneration: generation }) };
}
const sameContinuation = (a: ManualSummaryContinuation | undefined, b: ManualSummaryContinuation) =>
  !!a && Object.keys(a).length === Object.keys(b).length &&
    Object.keys(b).every(key => a[key as keyof ManualSummaryContinuation] === b[key as keyof ManualSummaryContinuation]);
function manualRecoveryIdentity(value: ManualSummaryContinuation): string {
  const identity = [value.operationId, value.step, value.jobId, value.project, value.sessionId,
    value.sourceFingerprint, value.round, value.unitId, value.deliveryId];
  if (value.recoveryGeneration) identity.push(value.recoveryGeneration);
  return JSON.stringify(identity);
}
function manualSelection(value: unknown, unit: boolean): SelectedSummary | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const row = value as Record<string, unknown>;
  const { step, ...control } = row;
  if (step !== undefined && (!Number.isSafeInteger(step) || (step as number) < 0) ||
      !validSelection(control, unit)) return undefined;
  return { ...control, manual: true,
    ...(step !== undefined && { manualStep: step }) } as SelectedSummary;
}

function validSelection(value: unknown, unit: boolean): value is SelectedSummary {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const fields = value as Record<string, unknown>;
  const allowed = new Set(["project", "sessionId", "sourceFingerprint", "jobId", ...(unit ? ["round", "unitId", "deliveryId"] : [])]);
  if (Object.keys(fields).some(key => !allowed.has(key)) ||
      fields.jobId !== undefined && (typeof fields.jobId !== "string" || !fields.jobId.trim() ||
        fields.jobId.length > 128 || fields.jobId !== fields.jobId.trim())) return false;
  if (![fields.project, fields.sessionId, fields.sourceFingerprint].every(field =>
    typeof field === "string" && field.length > 0 && field === field.trim())) return false;
  return !unit || typeof fields.jobId === "string" && fields.jobId.length > 0 &&
    typeof fields.unitId === "string" && fields.unitId.length > 0 &&
    Number.isSafeInteger(fields.round) && (fields.round as number) >= 0 &&
    (fields.deliveryId === undefined || typeof fields.deliveryId === "string" && fields.deliveryId.length > 0);
}

function failureCode(error: unknown): string {
  if (error instanceof SummaryBudgetError) return error.message;
  const message = error instanceof Error ? error.message : String(error);
  if (/circuit_breaker_open/i.test(message)) return "circuit_breaker_open";
  if (/timed out|timeout|AbortError/i.test(message)) return "timeout";
  if (/context_length_exceeded|maximum context length|too many tokens/i.test(message)) return "context_limit";
  if (/\b429\b|rate.limit/i.test(message)) return "rate_limited";
  if (/\b5\d\d\b|fetch failed|ECONNRESET|ECONNREFUSED/i.test(message)) return "provider_unavailable";
  return "provider_error";
}

function unitOutput(summary: SessionSummary, items: SummaryPromptItem[]): SummaryPromptItem {
  return {
    text: formatSummaryPartial(summary),
    obsRangeStart: items[0].obsRangeStart,
    obsRangeEnd: items[items.length - 1].obsRangeEnd,
    sourcePaths: [...new Set(items.flatMap(item => item.sourcePaths ?? []))],
    modifiedPaths: [...new Set(items.flatMap(item => item.modifiedPaths ?? []))],
  };
}

function createUnits(jobId: string, stage: "map" | "reduce", round: number, groups: SummaryPromptItem[][]): SummaryQueueUnit[] {
  if (groups.length === 0 || groups.length > MAX_SUMMARY_CALLS) {
    throw new SummaryBudgetError("summary_call_limit_exceeded");
  }
  return groups.map(items => ({ id: generateId("squ"), jobId, stage, round, items, attempts: 0 }));
}

function summaryItems(observations: CompressedObservation[]): SummaryPromptItem[] {
  return observations.map((observation, index) => ({
    text: formatSummaryObservation(observation, index + 1),
    obsRangeStart: index + 1,
    obsRangeEnd: index + 1,
    sourcePaths: summarySourcePathLiterals([observation]),
    modifiedPaths: [],
  }));
}

export interface SummaryPathAuthorityJob {
  id?: string;
  sessionId: string;
  project: string;
  snapshotFingerprint: string;
  observationCount: number;
}

export interface SummaryPathAuthorityUnit {
  stage: "map" | "reduce";
  items: SummaryPromptItem[];
}

interface SummaryPathAuthorityCheckpoint {
  version: 1;
  checkpointId: string;
  inputHash: string;
  snapshotFingerprint: string;
  observationCount: number;
  root: SummaryEvidenceRoot;
  sourceGeneration: string;
  sourceRevision: string;
  referenceHash: string;
  nextOrdinal: number;
  pathPageCount: number;
}

const SUMMARY_PATH_AUTHORITY_BATCH = 12;
const SUMMARY_PATH_AUTHORITY_PAGE_BYTES = 16_384;

export async function resolveSummaryUnitPathAuthority(
  kv: StateKV,
  job: SummaryPathAuthorityJob,
  unit: SummaryPathAuthorityUnit,
): Promise<{ sourcePaths: string[]; modifiedPaths: string[] }> {
  if (unit.items.every(item => Array.isArray(item.sourcePaths) && Array.isArray(item.modifiedPaths))) {
    return {
      sourcePaths: [...new Set(unit.items.flatMap(item => item.sourcePaths ?? []))],
      modifiedPaths: [...new Set(unit.items.flatMap(item => item.modifiedPaths ?? []))],
    };
  }
  if (unit.stage === "map") {
    const sources = unit.items.map(item => ({ narrative: item.text }));
    return {
      sourcePaths: summarySourcePathLiterals(sources),
      modifiedPaths: [],
    };
  }

  if (!job.id || !Number.isSafeInteger(job.observationCount) || job.observationCount < 1)
    throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
  const evidenceJob: SummaryEvidenceJob = { id: job.id, sessionId: job.sessionId, project: job.project,
    observationCount: job.observationCount };
  const root = await getSummaryEvidenceRoot(kv, evidenceJob);
  if (!root || root.count !== job.observationCount)
    throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
  const scope = KV.observations(job.sessionId);
  const sourceRevision = await kv.scopeRevision(scope);
  if (typeof sourceRevision?.generation !== "string" || !sourceRevision.generation ||
      typeof sourceRevision.revision !== "string" || !sourceRevision.revision)
    throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
  const referenceHashForRoot = async (): Promise<string> => {
    const digest = createHash("sha256");
    for (let pageIndex = 0; pageIndex < root.pages; pageIndex++) {
      const references = await kv.get<SummarySourceReference[]>(KV.state, `${job.id}:source-page:${pageIndex}`);
      const expectedLength = Math.min(12, root.count - pageIndex * 12);
      if (!Array.isArray(references) || references.length !== expectedLength ||
          references.some((reference, index) => !reference || reference.ordinal !== pageIndex * 12 + index + 1 ||
            typeof reference.id !== "string" || typeof reference.version !== "string" || typeof reference.hash !== "string"))
        throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
      const serialized = JSON.stringify(references);
      if (serialized === undefined) throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
      digest.update(serialized);
    }
    return digest.digest("hex");
  };
  const referenceHash = await referenceHashForRoot();
  const inputHash = fingerprintId("summary-input", JSON.stringify(unit.items));
  const checkpointKey = `${job.id}:path-authority:${inputHash}`;
  let checkpoint = await kv.get<SummaryPathAuthorityCheckpoint>(KV.state, checkpointKey);
  const validCheckpoint = (value: SummaryPathAuthorityCheckpoint | undefined): value is SummaryPathAuthorityCheckpoint =>
    !!value && value.version === 1 && typeof value.checkpointId === "string" && value.checkpointId.length > 0 &&
    value.inputHash === inputHash && value.snapshotFingerprint === job.snapshotFingerprint &&
    value.observationCount === job.observationCount && typeof value.root === "object" && value.root !== null &&
    value.root.count === root.count &&
    value.root.pages === root.pages && value.root.mode === root.mode &&
    value.sourceGeneration === sourceRevision.generation && value.sourceRevision === sourceRevision.revision &&
    value.referenceHash === referenceHash && Number.isSafeInteger(value.nextOrdinal) &&
    value.nextOrdinal >= 1 && value.nextOrdinal <= root.count + 1 &&
    Number.isSafeInteger(value.pathPageCount) && value.pathPageCount >= 0;
  if (checkpoint && !validCheckpoint(checkpoint)) {
    await kv.delete(KV.state, checkpointKey);
    checkpoint = undefined;
  }

  const pathPagesPrefix = (checkpointId: string) => `${checkpointKey}:${checkpointId}:paths:`;
  const readCheckpointPaths = async (value: SummaryPathAuthorityCheckpoint): Promise<Set<string>> => {
    const paths = new Set<string>();
    for (let page = 0; page < value.pathPageCount; page++) {
      const items = await kv.get<string[]>(KV.state, `${pathPagesPrefix(value.checkpointId)}${page}`);
      if (!Array.isArray(items) || items.some(item => typeof item !== "string"))
        throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
      for (const item of items) paths.add(item);
    }
    return paths;
  };
  const pathKey = (path: string) => path.replace(/\\/g, "/").replace(/^\.\//, "").toLowerCase();

  if (!checkpoint) {
    checkpoint = {
      version: 1, checkpointId: generateId("sap"), inputHash,
      snapshotFingerprint: job.snapshotFingerprint, observationCount: job.observationCount,
      root, sourceGeneration: sourceRevision.generation, sourceRevision: sourceRevision.revision,
      referenceHash, nextOrdinal: 1, pathPageCount: 0,
    };
    await kv.set(KV.state, checkpointKey, checkpoint);
  }

  let paths: Set<string>;
  try { paths = await readCheckpointPaths(checkpoint); }
  catch {
    await kv.delete(KV.state, checkpointKey);
    throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
  }
  const pathKeys = new Set([...paths].map(pathKey));
  const writePathBatch = async (items: string[], startPage: number): Promise<number> => {
    let page: string[] = [];
    let pageIndex = startPage;
    for (const item of items) {
      const candidate = [...page, item];
      if (page.length && Buffer.byteLength(JSON.stringify(candidate), "utf8") > SUMMARY_PATH_AUTHORITY_PAGE_BYTES) {
        await kv.set(KV.state, `${pathPagesPrefix(checkpoint!.checkpointId)}${pageIndex++}`, page);
        page = [item];
      } else {
        if (!page.length && Buffer.byteLength(JSON.stringify(candidate), "utf8") > SUMMARY_PATH_AUTHORITY_PAGE_BYTES)
          throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
        page = candidate;
      }
    }
    if (page.length) await kv.set(KV.state, `${pathPagesPrefix(checkpoint!.checkpointId)}${pageIndex++}`, page);
    return pageIndex - startPage;
  };

  while (checkpoint.nextOrdinal <= root.count) {
    const batchEnd = Math.min(root.count, checkpoint.nextOrdinal + SUMMARY_PATH_AUTHORITY_BATCH - 1);
    const newPaths = new Map<string, string>();
    for (let ordinal = checkpoint.nextOrdinal; ordinal <= batchEnd; ordinal++) {
      const { observation } = await readSummaryEvidenceSource(kv, evidenceJob, ordinal);
      for (const path of summarySourcePathLiterals([observation])) {
        const key = pathKey(path);
        if (!pathKeys.has(key)) {
          pathKeys.add(key);
          newPaths.set(key, path);
        }
      }
    }
    const written = await writePathBatch([...newPaths.values()], checkpoint.pathPageCount);
    for (const path of newPaths.values()) paths.add(path);
    checkpoint = { ...checkpoint, nextOrdinal: batchEnd + 1, pathPageCount: checkpoint.pathPageCount + written };
    await kv.set(KV.state, checkpointKey, checkpoint);
  }
  const latestRevision = await kv.scopeRevision(scope);
  if (latestRevision?.generation !== checkpoint.sourceGeneration || latestRevision?.revision !== checkpoint.sourceRevision ||
      await referenceHashForRoot() !== checkpoint.referenceHash)
    throw new SummaryBudgetError("summary_source_changed");
  return { sourcePaths: [...paths], modifiedPaths: [] };
}

async function snapshot(kv: StateKV, sessionId: string, project: string, bounded = false): Promise<{
  observations: CompressedObservation[]; items: SummaryPromptItem[]; fingerprint: string;
}> {
  let source: CompressedObservation[];
  if (bounded) {
    source = [];
    for await (const page of kv.pages<CompressedObservation>(KV.observations(sessionId), { limit: 13, maxBytes: 16_384 })) {
      source.push(...page.items);
      if (source.length > 12 || Buffer.byteLength(JSON.stringify(source), "utf8") > 16_384 || page.next_cursor) {
        throw new SummaryBudgetError("selected_snapshot_limit_exceeded");
      }
    }
  } else source = await summaryRecoveryStep("snapshot.observations", { sessionId },
    () => readSummarySnapshotSource(kv, KV.observations(sessionId)));
  const observations = source
    .filter(observation => observation.title)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id));
  const items = summaryItems(observations);
  const fingerprintItems = items.map(({ sourcePaths: _sourcePaths, modifiedPaths: _modifiedPaths, ...item }) => item);
  return { observations, items, fingerprint: fingerprintId("sq", JSON.stringify([sessionId, project, fingerprintItems])) };
}

async function normalManualSnapshot(kv: StateKV, sessionId: string, project: string): Promise<{
  observations: CompressedObservation[]; items: SummaryPromptItem[]; fingerprint: string;
}> {
  const scope = KV.observations(sessionId);
  const source: CompressedObservation[] = [];
  try {
    const before = await kv.scopeRevision(scope);
    if (typeof before?.generation !== "string" || !before.generation ||
        typeof before.revision !== "string" || !before.revision)
      throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
    for await (const page of kv.pages<CompressedObservation>(scope, { limit: 13, maxBytes: 16_384 })) {
      source.push(...page.items);
      if (source.length > MAX_SUMMARY_ITEMS) {
        throw new SummaryBudgetError("summary_source_grounding_snapshot_limit_exceeded");
      }
    }
    const after = await kv.scopeRevision(scope);
    if (typeof after?.generation !== "string" || !after.generation ||
        typeof after.revision !== "string" || !after.revision)
      throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
    if (before.generation !== after.generation || before.revision !== after.revision) {
      throw new SummaryBudgetError("summary_source_changed");
    }
  } catch (error) {
    if (error instanceof SummaryBudgetError) throw error;
    if (typeof error === "object" && error !== null &&
        (error as { code?: unknown }).code === "STATE_RECORD_TOO_LARGE") {
      throw new SummaryBudgetError("summary_source_grounding_snapshot_limit_exceeded");
    }
    throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
  }
  const observations = source
    .filter(observation => observation.title)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id));
  const items = summaryItems(observations);
  const fingerprintItems = items.map(({ sourcePaths: _sourcePaths, modifiedPaths: _modifiedPaths, ...item }) => item);
  return { observations, items, fingerprint: fingerprintId("sq", JSON.stringify([sessionId, project, fingerprintItems])) };
}

export async function summarySourceFingerprint(kv: StateKV, sessionId: string, project: string): Promise<string> {
  return (await snapshot(kv, sessionId, project, true)).fingerprint;
}

async function exactSelectedCompletion(
  kv: StateKV,
  completed: CompletedSnapshot | null,
  activeJob: SummaryQueueJob | null,
  sessionId: string,
  project: string,
  sourceFingerprint: string,
  observationCount: number,
  selection: SelectedSummary,
): Promise<boolean> {
  if (completed?.sessionId !== sessionId || completed.fingerprint !== sourceFingerprint ||
      typeof completed.completedAt !== "string" || !completed.completedAt) return false;
  if (completed.jobId !== undefined && (typeof completed.jobId !== "string" || !completed.jobId.trim())) return false;
  const jobId = completed.jobId ?? selection.jobId;
  if (!jobId || selection.jobId !== undefined && selection.jobId !== jobId) return false;
  const job = activeJob?.id === jobId ? activeJob : await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
  if (!job || job.id !== jobId || job.status !== "completed" || job.executionOrigin !== "selected" ||
      job.completedAt !== completed.completedAt || job.sessionId !== sessionId || job.project !== project ||
      job.snapshotFingerprint !== sourceFingerprint || job.observationCount !== observationCount) return false;
  const summary = await kv.get<SessionSummary>(KV.summaries, sessionId);
  return summary?.sessionId === sessionId && summary.project === project &&
    summary.observationCount === observationCount;
}

async function enqueue(sdk: IIIClient, functionId: string, payload: unknown): Promise<void> {
  const topic = functionId === "mem::summary-unit" ? UNIT_TOPIC : DISPATCH_TOPIC;
  await sdk.trigger({ function_id: "iii::durable::publish", payload: { topic, data: payload } });
}

async function deleteUnits(kv: StateKV, jobId: string, units: SummaryQueueUnit[]): Promise<void> {
  for (let start = 0; start < units.length; start += DISPATCH_BATCH) {
    await Promise.all(units.slice(start, start + DISPATCH_BATCH)
      .map(unit => kv.delete(KV.summaryQueueUnits(jobId), unit.id)));
  }
}

async function cleanupUnits(kv: StateKV, jobId: string): Promise<void> {
  await deleteUnits(kv, jobId, await kv.list<SummaryQueueUnit>(KV.summaryQueueUnits(jobId)));
}

async function writeUnits(kv: StateKV, jobId: string, units: SummaryQueueUnit[]): Promise<void> {
  for (let start = 0; start < units.length; start += DISPATCH_BATCH) {
    await Promise.all(units.slice(start, start + DISPATCH_BATCH)
      .map(unit => kv.set(KV.summaryQueueUnits(jobId), unit.id, unit)));
  }
}

async function loadJobUnits(kv: StateKV, job: SummaryQueueJob): Promise<Array<SummaryQueueUnit | undefined>> {
  const units = await kv.list<SummaryQueueUnit>(KV.summaryQueueUnits(job.id));
  const byId = new Map(units.map(unit => [unit.id, unit]));
  return job.unitIds.map(id => byId.get(id));
}

async function failJob(kv: StateKV, jobId: string, reason: string): Promise<void> {
  await withKeyedLock(`summary-job:${jobId}`, () => failJobLocked(kv, jobId, reason));
}

async function failJobLocked(kv: StateKV, jobId: string, reason: string): Promise<void> {
  const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
  if (!job || job.status !== "pending") return;
  const failedAt = new Date().toISOString();
  await kv.set(KV.state, `${job.sessionId}:failed-summary:${job.snapshotFingerprint}`, { jobId, reason });
  await kv.set(KV.summaryQueueJobs, jobId, {
    ...job, status: "failed", failedAt, failure: reason, updatedAt: failedAt,
  });
  await releaseSummaryActiveJob(kv, job.sessionId, jobId);
  await recordAudit(kv, "compress", "mem::summary-unit", [job.sessionId],
    { jobId, outcome: "failed", reason }, undefined, undefined, `${jobId}:failed`);
  await recordOperation(kv, "mem::summary-final", "failed", {}, undefined,
    { errorCode: "summary_failed", executionId: job.id, executionLevel: "workflow", occurredAt: failedAt });
  logger.warn("Summary queue job failed", { sessionId: job.sessionId, jobId, reason });
}

async function withSummaryUnitLocks<T>(unitIds: string[], action: () => Promise<T>, index = 0): Promise<T> {
  const ordered = index === 0
    ? [...new Set(unitIds)].sort((left, right) => left.localeCompare(right))
    : unitIds;
  if (index >= ordered.length) return action();
  return withKeyedLock(`summary-unit:${ordered[index]}`, () => withSummaryUnitLocks(ordered, action, index + 1));
}

type SummaryMethod = "llm" | "deterministic_fallback";
interface SummaryPublicationEffect {
  summary: SessionSummary;
  completedAt: string;
  method?: SummaryMethod;
  previousCanonicalIdentity?: string | null;
}

function summaryFields(summary: SessionSummary) {
  return {
    title: summary.title, narrative: summary.narrative,
    keyDecisions: summary.keyDecisions, filesModified: summary.filesModified,
    concepts: summary.concepts,
  };
}

function summaryPublicationIdentity(summary: SessionSummary): string {
  return JSON.stringify([summary.sessionId, summary.project, summary.createdAt, summary.title, summary.narrative,
    summary.keyDecisions, summary.filesModified, summary.concepts, summary.observationCount]);
}

function validateSummary(summary: SessionSummary): void {
  if (!validateOutput(SummaryOutputSchema, summaryFields(summary), "mem::summary-unit").valid) {
    throw new SummaryBudgetError("summary_validation_failed");
  }
}

function stableSummaryValues(summaries: SessionSummary[], field: "keyDecisions" | "filesModified" | "concepts"): string[] {
  const seen = new Set<string>();
  const merged: string[] = [];
  for (const summary of summaries) {
    for (const value of summary[field]) {
      const normalized = value.trim();
      if (normalized && !seen.has(normalized)) {
        seen.add(normalized);
        merged.push(normalized);
      }
    }
  }
  return merged;
}

function compactSummaryText(value: string, limit: number): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) return normalized;
  const end = normalized.lastIndexOf(" ", limit - 1);
  return `${normalized.slice(0, end > limit / 2 ? end : limit - 1).trimEnd()}…`;
}

function deterministicSummary(job: SummaryQueueJob, units: SummaryQueueUnit[]): SessionSummary {
  const ordered = [...units].sort((a, b) =>
    a.output!.obsRangeStart - b.output!.obsRangeStart ||
    a.output!.obsRangeEnd - b.output!.obsRangeEnd || a.id.localeCompare(b.id));
  const summaries = ordered.map(unit => unit.summary!);
  summaries.forEach(validateSummary);
  const beginning = summaries[0];
  const progress = summaries[Math.floor(summaries.length / 2)];
  const ending = summaries[summaries.length - 1];
  const title = beginning.title === ending.title
    ? compactSummaryText(ending.title, 100)
    : `${compactSummaryText(beginning.title, 48)} / ${compactSummaryText(ending.title, 48)}`;
  return {
    sessionId: job.sessionId,
    project: job.project,
    createdAt: new Date().toISOString(),
    title: title || "Session summary",
    narrative: [
      `Beginning: ${compactSummaryText(beginning.narrative, 180)}`,
      `Progress: ${compactSummaryText(progress.narrative, 180)}`,
      `Ending: ${compactSummaryText(ending.narrative, 180)}`,
    ].join(" "),
    keyDecisions: stableSummaryValues(summaries, "keyDecisions"),
    filesModified: stableSummaryValues(summaries, "filesModified"),
    concepts: stableSummaryValues(summaries, "concepts"),
    observationCount: job.observationCount,
  };
}

async function completeJob(
  sdk: IIIClient, kv: StateKV, job: SummaryQueueJob, summary: SessionSummary, method: SummaryMethod = "llm",
  foreground = false, publicationRecovery = false,
): Promise<void> {
  const fields = summaryFields(summary);
  validateSummary(summary);
  const qualityScore = scoreSummary(fields);
  const receiptKey = `${job.id}:publication-effect`;
  const latest = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
  if (!latest || !(latest.status === "pending" || publicationRecovery && latest.status === "failed"
      && latest.failure === "summary_publication_pending") || latest.sessionId !== job.sessionId ||
      latest.project !== job.project || latest.snapshotFingerprint !== job.snapshotFingerprint)
    throw new SummaryBudgetError("summary_publication_effect_mismatch");
  const effect = await withKeyedLock(`summary-canonical:${job.sessionId}`, async () => {
    const session = await kv.get<Session>(KV.sessions, job.sessionId);
    if (!session || session.id !== job.sessionId || session.project !== job.project)
      throw new SummaryBudgetError("summary_source_changed");
    const current = await kv.get<SessionSummary>(KV.summaries, job.sessionId);
    let receipt = await kv.get<SummaryPublicationEffect>(KV.state, receiptKey);
    if (receipt) {
      if (!receipt.summary || JSON.stringify(summaryFields(receipt.summary)) !== JSON.stringify(fields) ||
          receipt.summary.sessionId !== job.sessionId || receipt.summary.project !== job.project ||
          receipt.summary.observationCount !== job.observationCount || typeof receipt.completedAt !== "string" ||
          receipt.method !== undefined && receipt.method !== "llm" && receipt.method !== "deterministic_fallback")
        throw new SummaryBudgetError("summary_publication_effect_mismatch");
      const canonicalIdentity = current ? summaryPublicationIdentity(current) : null;
      const effectIdentity = summaryPublicationIdentity(receipt.summary);
      if (canonicalIdentity !== effectIdentity) {
        const safeRestore = receipt.previousCanonicalIdentity !== undefined
          ? canonicalIdentity === receipt.previousCanonicalIdentity
          : current === null;
        if (!safeRestore) throw new SummaryBudgetError("summary_publication_effect_mismatch");
        await kv.set(KV.summaries, job.sessionId, receipt.summary);
      }
      return receipt;
    }
    const previousCanonicalIdentity = current ? summaryPublicationIdentity(current) : null;
    receipt = { summary, completedAt: new Date().toISOString(), method, previousCanonicalIdentity };
    await kv.set(KV.state, receiptKey, receipt);
    await kv.set(KV.summaries, job.sessionId, receipt.summary);
    return receipt;
  });
  summary = effect.summary;
  method = effect.method ?? method;
  await recordAudit(kv, "compress", "mem::summary-unit", [job.sessionId],
    { jobId: job.id, observationCount: job.observationCount, qualityScore, method },
    qualityScore, undefined, `${job.id}:completed`);
  const completedAt = effect.completedAt;
  const marker = {
    sessionId: job.sessionId, fingerprint: job.snapshotFingerprint, completedAt, jobId: job.id,
  } satisfies CompletedSnapshot;
  if (JSON.stringify(await kv.get(KV.summaryQueueCompleted, job.sessionId)) !== JSON.stringify(marker))
    await kv.set(KV.summaryQueueCompleted, job.sessionId, marker);
  if (!foreground) try {
    const result: unknown = await sdk.trigger({
      function_id: "mem::skill-lifecycle-summary-ready",
      payload: { sessionId: job.sessionId, snapshotFingerprint: job.snapshotFingerprint },
    });
    if (typeof result !== "object" || result === null ||
        !("success" in result) || result.success !== true) {
      logger.warn("Skill lifecycle admission deferred", { sessionId: job.sessionId, reason: "reported_failure" });
    }
  } catch {
    logger.warn("Skill lifecycle admission deferred", { sessionId: job.sessionId, reason: "trigger_rejected" });
  }
  await kv.set(KV.summaryQueueJobs, job.id, {
    ...job, status: "completed", completedAt, updatedAt: completedAt,
  });
  await releaseSummaryActiveJob(kv, job.sessionId, job.id);
  await cleanupUnits(kv, job.id);
  await recordOperation(kv, "mem::summary-final", "completed",
    { observationsProcessed: job.observationCount, summariesCreated: 1 }, undefined,
    { durationMs: Math.max(0, Date.now() - Date.parse(job.createdAt)),
      executionId: job.id, executionLevel: "workflow", occurredAt: completedAt,
      ...(method === "deterministic_fallback" ? { reason: "deterministic_fallback" } : {}) });
  logger.info("Session summarized from queue", {
    sessionId: job.sessionId, observationCount: job.observationCount, qualityScore, method,
  });
}

export function registerSummaryQueueFunctions(
  sdk: IIIClient, kv: StateKV, provider: MemoryProvider, llmRouter?: LlmTaskRouter,
  accounting?: WorkflowAttemptAccounting,
): void {
  const activeUnits = new Map<string, number>();
  registerSummaryPublicationRecovery(sdk, kv, provider, llmRouter, summaryFields,
    (job, summary) => completeJob(sdk, kv, job, summary, "llm", true, true));
  const recoverAppliedPublication = async (job: SummaryQueueJob, foreground: boolean): Promise<SummaryMethod | undefined> => {
    const effect = await kv.get<SummaryPublicationEffect>(KV.state, `${job.id}:publication-effect`);
    if (!effect) return undefined;
    if (!effect.summary || effect.summary.sessionId !== job.sessionId || effect.summary.project !== job.project ||
        effect.summary.observationCount !== job.observationCount || typeof effect.completedAt !== "string")
      throw new SummaryBudgetError("summary_publication_effect_mismatch");
    const method = effect.method ?? "llm";
    await completeJob(sdk, kv, job, effect.summary, method, foreground);
    return method;
  };
  const recoverySourceJob = async (job: SummaryQueueJob): Promise<SummaryQueueJob> => {
    if (job.recoveryMode !== "full-originals-v1" || !job.recoveryOfJobId)
      throw new SummaryBudgetError("summary_recovery_identity_mismatch");
    const source = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.recoveryOfJobId);
    if (!source || source.status !== "failed" || source.failure !== "summary_publication_invalid_response" ||
        source.recoveryMode !== undefined || source.recoveryOfJobId !== undefined || source.recoveryGeneration !== undefined ||
        source.executionOrigin !== "manual" || source.sessionId !== job.sessionId || source.project !== job.project ||
        source.snapshotFingerprint !== job.snapshotFingerprint || source.observationCount !== job.observationCount)
      throw new SummaryBudgetError("summary_recovery_not_available");
    return source;
  };
  const manualRecoveryRootForMarker = async (marked: SummaryQueueJob): Promise<SummaryQueueJob | undefined> => {
    if (marked.status !== "failed" || marked.executionOrigin !== "manual") return undefined;
    if (marked.failure === "summary_publication_invalid_response" && marked.recoveryMode === undefined &&
        marked.recoveryOfJobId === undefined && marked.recoveryGeneration === undefined) return marked;
    if (marked.failure !== "manual_summary_limit_exceeded" &&
        marked.failure !== "summary_publication_effect_mismatch") return undefined;
    if (marked.recoveryMode !== "full-originals-v1" || marked.recoveryGeneration !== undefined ||
        !marked.recoveryOfJobId || marked.unitIds.length !== 1) return undefined;
    const parent = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, marked.recoveryOfJobId);
    if (!parent || parent.id === marked.id || parent.status !== "failed" ||
        parent.failure !== "summary_publication_invalid_response" || parent.executionOrigin !== "manual" ||
        parent.recoveryMode !== undefined || parent.recoveryOfJobId !== undefined || parent.recoveryGeneration !== undefined ||
        parent.sessionId !== marked.sessionId || parent.project !== marked.project ||
        parent.snapshotFingerprint !== marked.snapshotFingerprint || parent.observationCount !== marked.observationCount ||
        marked.id !== fingerprintId("sqj", manualRecoveryRequestId(parent, "full-originals-v1"))) return undefined;
    return parent;
  };
  const fullOriginalRecovery = async (job: SummaryQueueJob): Promise<FullOriginalRecovery> => {
    const sourceJob = await recoverySourceJob(job);
    const session = await kv.get<Session>(KV.sessions, job.sessionId);
    if (!session || session.project !== job.project) throw new SummaryBudgetError("summary_source_changed");
    const current = await normalManualSnapshot(kv, job.sessionId, job.project);
    if (current.fingerprint !== job.snapshotFingerprint || current.fingerprint !== sourceJob.snapshotFingerprint)
      throw new SummaryBudgetError("summary_source_changed");
    const root = await getSummaryEvidenceRoot(kv, sourceJob);
    if (!root || root.mode !== "complete" || root.count !== job.observationCount)
      throw new SummaryBudgetError("summary_recovery_evidence_incomplete");
    const sources = await verifySummaryEvidence(kv, sourceJob, root);
    if (sources.length !== job.observationCount) throw new SummaryBudgetError("summary_source_changed");
    const request = prepareFullOriginalSummaryRequest(current.items, job.config);
    return { sourceJob, root, sources, observations: current.observations, items: current.items,
      sourcePaths: summarySourcePathLiterals(current.observations), ...request };
  };
  const terminalizeManualJob = async (
    candidate: SummaryQueueJob, request: ManualTerminalizationRequest,
  ): Promise<"terminalized" | "live" | "conflict" | { completed: SessionSummary }> => {
    if (candidate.status !== "pending" || candidate.executionOrigin !== "manual" || candidate.unitIds.length === 0)
      return "conflict";
    return withSummaryUnitLocks(candidate.unitIds, () => withKeyedLock(`summary-job:${candidate.id}`, async () => {
      const [active, latest, operation] = await Promise.all([
        kv.get<ActiveJob>(KV.summaryQueueActive, candidate.sessionId),
        kv.get<SummaryQueueJob>(KV.summaryQueueJobs, candidate.id),
        kv.get<ManualOperation>(KV.state, `${candidate.id}:manual`),
      ]);
      if (!latest || latest.status !== "pending" || latest.id !== candidate.id ||
          latest.executionOrigin !== "manual" || latest.sessionId !== candidate.sessionId ||
          latest.project !== candidate.project || latest.snapshotFingerprint !== candidate.snapshotFingerprint ||
          latest.observationCount !== candidate.observationCount || latest.round !== candidate.round || latest.stage !== candidate.stage ||
          latest.recoveryOfJobId !== candidate.recoveryOfJobId || latest.recoveryMode !== candidate.recoveryMode ||
          latest.recoveryGeneration !== candidate.recoveryGeneration ||
          latest.unitIds.length !== candidate.unitIds.length || latest.unitIds.some((id, index) => id !== candidate.unitIds[index]) ||
          request.activeLease === "required" && active?.jobId !== latest.id ||
          request.activeLease === "matching-or-empty" && active && active.jobId !== latest.id ||
          !operation || operation.id !== `${latest.id}:manual` || operation.jobId !== latest.id ||
          operation.sessionId !== latest.sessionId || operation.project !== latest.project ||
          operation.sourceFingerprint !== latest.snapshotFingerprint ||
          !Number.isSafeInteger(operation.step) || operation.step < 0 ||
          !Number.isSafeInteger(operation.maxRequests) || operation.maxRequests < 0) return "conflict";
      if (operation.continuation && (!validManualSummaryContinuation(operation.continuation) ||
          operation.continuation.operationId !== operation.id || operation.continuation.jobId !== latest.id ||
          operation.continuation.project !== latest.project || operation.continuation.sessionId !== latest.sessionId ||
          operation.continuation.sourceFingerprint !== latest.snapshotFingerprint ||
          !latest.unitIds.includes(operation.continuation.unitId) || operation.continuation.round !== latest.round))
        return "conflict";
      try {
        if (await recoverAppliedPublication(latest, true)) {
          const summary = await kv.get<SessionSummary>(KV.summaries, latest.sessionId);
          if (summary && summary.sessionId === latest.sessionId && summary.project === latest.project &&
              summary.observationCount === latest.observationCount) return { completed: summary };
          return "conflict";
        }
      } catch (error) {
        if (error instanceof SummaryBudgetError &&
            ["summary_publication_effect_mismatch", "summary_source_changed"].includes(error.message)) return "conflict";
        throw error;
      }
      const units = await loadJobUnits(kv, latest);
      if (units.some(unit => !unit || unit.jobId !== latest.id || unit.round !== latest.round || unit.stage !== latest.stage))
        return "conflict";
      if (request.legacyRetirement) {
        try { await fullOriginalRecovery(latest); }
        catch (error) { if (error instanceof SummaryBudgetError) return "conflict"; throw error; }
      }
      const legacyExpired = request.legacyRetirement === true && latest.recoveryGeneration === undefined &&
        latest.recoveryMode === "full-originals-v1" && operation.deadline !== undefined &&
        Number.isFinite(operation.deadline) && Date.now() >= operation.deadline;
      const canContinue = !legacyExpired && operation.step < operation.maxRequests;
      if (canContinue) return "live";
      await failJobLocked(kv, latest.id, "manual_summary_limit_exceeded");
      return "terminalized";
    }));
  };
  const evidenceRoot = async (job: SummaryQueueJob): Promise<SummaryEvidenceRoot> => {
    if (job.recoveryMode === "full-originals-v1") return (await fullOriginalRecovery(job)).root;
    const existing = await getSummaryEvidenceRoot(kv, job);
    if (existing) return existing;
    const current = job.executionOrigin === "manual"
      ? await normalManualSnapshot(kv, job.sessionId, job.project)
      : await snapshot(kv, job.sessionId, job.project);
    if (current.fingerprint !== job.snapshotFingerprint) throw new SummaryBudgetError("summary_source_changed");
    return captureSummaryEvidence(kv, job, current.observations);
  };
  const verifyJobEvidence = async (job: SummaryQueueJob, root: SummaryEvidenceRoot): Promise<PublicationEvidenceSource[]> => {
    if (job.recoveryMode === "full-originals-v1") {
      const recovery = await fullOriginalRecovery(job);
      if (recovery.root.count !== root.count || recovery.root.mode !== root.mode || recovery.root.pages !== root.pages)
        throw new SummaryBudgetError("summary_source_changed");
      return recovery.sources;
    }
    return verifySummaryEvidence(kv, job, root);
  };
  const judge = async (job: SummaryQueueJob, summary: SessionSummary | string, sources: PublicationEvidenceSource[],
    operationId: string, foreground: boolean, selection?: SelectedSummary, unitId = selection?.unitId ?? null): Promise<string> => {
    const input = { workflow: "summary" as const, operationId, project: job.project,
      candidate: typeof summary === "string" ? summary : JSON.stringify(summaryFields(summary)), sources, evidenceComplete: true };
    let result = await evaluatePublicationEvidence(kv, input);
    if (result.status === "unavailable") {
      const request = preparePublicationEvidence(input);
      if ("status" in request) throw new SummaryBudgetError(`summary_publication_${request.status}`);
      result = await evaluatePublicationEvidence(kv, input, publicationExecutor(provider, llmRouter, "summary",
        { task: "summary", outputTokens: 1024 }, (prepared, dispatch) => {
          const execute = () => selection?.manual ? withSummaryCallTimeout(dispatch, SUMMARY_CALL_TIMEOUT_MS) : dispatch();
          return runWorkflowAttempt(accounting, kv, { workflow: "summary", functionId: "mem::summary-unit",
            project: job.project, sourceId: job.sessionId, sourceFingerprint: job.snapshotFingerprint,
            jobId: job.id, unitId: prepared.requestId, phase: "publication",
            continuation: JSON.stringify([job.round, job.stage, unitId, operationId]) }, execute);
        }));
    }
    if (result.status === "pending" && selection?.manual) throw new ManualCutComplete();
    if (result.status !== "supported") throw new SummaryBudgetError(`summary_publication_${result.status}`);
    return result.requestId;
  };
  const completedPromptCorrection = async (job: SummaryQueueJob): Promise<string | null> => {
    const childId = summaryPromptCorrectionId(job);
    const journal = await kv.get<SummaryPromptCorrection>(KV.state, summaryPromptCorrectionKey(childId));
    if (journal?.state !== "ready" || journal.revision !== SUMMARY_GENERATION_REVISION
      || journal.predecessorJobId !== job.id || journal.jobId !== childId
      || journal.sessionId !== job.sessionId || journal.project !== job.project
      || journal.sourceFingerprint !== job.snapshotFingerprint) return null;
    const marker = await kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, job.sessionId);
    if (marker?.jobId !== childId || marker.fingerprint !== job.snapshotFingerprint) return null;
    const summary = await kv.get<SessionSummary>(KV.summaries, job.sessionId);
    return summary?.sessionId === job.sessionId && summary.project === job.project
      && summary.observationCount === job.observationCount ? childId : null;
  };
  const correctUnsupportedSummary = async (candidate: SummaryQueueJob): Promise<boolean> => {
    if (!eligibleSummaryPromptPredecessor(candidate)) return false;
    return withKeyedLock(`summary-session:${candidate.sessionId}`, () =>
      withKeyedLock(`summary-job:${candidate.id}`, async () => {
        const row = await kv.getVersioned<SummaryQueueJob>(KV.summaryQueueJobs, candidate.id);
        const job = row.value;
        if (!row.exists || !job || !eligibleSummaryPromptPredecessor(job)) return false;
        if (await completedPromptCorrection(job)) return true;
        const marker = await kv.get<{ jobId: string; reason: string }>(KV.state,
          `${job.sessionId}:failed-summary:${job.snapshotFingerprint}`);
        if (marker?.jobId !== job.id || marker.reason !== job.failure
          || await kv.get(KV.summaryQueueActive, job.sessionId)) return false;
        const current = await snapshot(kv, job.sessionId, job.project, true);
        if (current.fingerprint !== job.snapshotFingerprint || current.observations.length !== 1) return false;
        const unit = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), job.unitIds[0]);
        if (!unit || unit.id !== job.unitIds[0] || unit.jobId !== job.id || unit.stage !== "map"
          || unit.round !== 0 || !unit.summary || !unit.output || unit.startedAt
          || unit.summary.sessionId !== job.sessionId || unit.summary.project !== job.project
          || unit.summary.observationCount !== job.observationCount
          || unit.outputInputHash !== inputHash(unit)) return false;
        const capture = await kv.get<{ state: string; inputHash?: string; raw?: string; overflow?: boolean }>(
          KV.state, `${job.id}:raw:${unit.id}`);
        if (capture?.state !== "captured" || capture.inputHash !== inputHash(unit)
          || typeof capture.raw !== "string" || !capture.raw || capture.overflow
          || Buffer.byteLength(capture.raw, "utf8") > 65_536) return false;
        const root = await getSummaryEvidenceRoot(kv, job);
        if (!root || root.mode !== "complete" || root.count !== 1) return false;
        const sources = await verifyJobEvidence(job, root);
        const verdict = await observePublicationEvidence(kv, {
          workflow: "summary", operationId: job.id, project: job.project,
          candidate: JSON.stringify(summaryFields(unit.summary)), sources, evidenceComplete: true,
        });
        if (!("state" in verdict) || verdict.state !== "decided" || !verdict.hasRaw
          || verdict.responseOverflow || verdict.decisionStatus !== "unsupported" || verdict.diagnostic)
          return false;
        const latest = await kv.getVersioned<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
        if (!latest.exists || latest.version !== row.version
          || await kv.get(KV.summaryQueueActive, job.sessionId)) return false;
        const now = new Date().toISOString();
        const admission = await claimSummaryPromptCorrection(kv, job, verdict.requestId, now);
        if (!admission) return false;
        const next = await snapshot(kv, job.sessionId, job.project, true);
        if (next.fingerprint !== job.snapshotFingerprint
          || await kv.get(KV.summaryQueueActive, job.sessionId)) return false;
        const config = getSummaryBudgetConfig();
        const groups = packSummaryItems(next.items, SUMMARY_SYSTEM, buildSummaryItemsPrompt,
          summaryChunkInputLimit(config), config.chunkSize);
        if (groups.length !== 1) return false;
        const units = createUnits(admission.jobId, "map", 0, groups);
        const child: SummaryQueueJob = {
          id: admission.jobId, sessionId: job.sessionId, project: job.project,
          snapshotFingerprint: job.snapshotFingerprint, observationCount: job.observationCount,
          config, createdAt: now, updatedAt: now, status: "pending", stage: "map", round: 0,
          unitIds: units.map(value => value.id), sourceProgressSize: summaryProgressSize(next.items),
          generationRevision: SUMMARY_GENERATION_REVISION, promptCorrectionRevision: SUMMARY_GENERATION_REVISION,
          promptCorrectionOfJobId: job.id, dispatchPending: true,
        };
        const childRoot = await captureSummaryEvidence(kv, child, next.observations);
        if (childRoot.mode !== "complete" || childRoot.count !== root.count)
          throw new SummaryBudgetError("summary_prompt_correction_source_mismatch");
        await writeUnits(kv, child.id, units);
        await kv.set(KV.summaryQueueJobs, child.id, child);
        await setSummaryActiveJob(kv, child.sessionId, child.id);
        await kv.set(KV.state, summaryPromptCorrectionKey(child.id), { ...admission, state: "ready" });
        await recordAudit(kv, "compress", "mem::summary-enqueue", [job.sessionId],
          { jobId: child.id, predecessorJobId: job.id, correctionRevision: SUMMARY_GENERATION_REVISION,
            unitCount: units.length, outcome: "queued" }, undefined, undefined, `${child.id}:queued`);
        await enqueue(sdk, "mem::summary-dispatch", { jobId: child.id, round: 0, offset: 0 });
        await recordOperation(kv, "mem::summary-enqueue", "queued", { unitsQueued: units.length }, undefined,
          { executionId: child.id, executionLevel: "workflow", occurredAt: now });
        return true;
      })).catch(error => {
        if (!(error instanceof SummaryBudgetError)) throw error;
        logger.warn("Summary prompt correction deferred", { jobId: candidate.id,
          reason: /^[A-Za-z0-9_:.-]{1,160}$/.test(error.message) ? error.message : "summary_source_unavailable" });
        return false;
      });
  };
  const publish = async (job: SummaryQueueJob, summary: SessionSummary, method: SummaryMethod,
    foreground: boolean, selection?: SelectedSummary, units: SummaryQueueUnit[] = []): Promise<void> => {
    const root = await evidenceRoot(job);
    const originals = await verifyJobEvidence(job, root);
    if (root.mode === "complete") await judge(job, summary, originals, job.id, foreground, selection);
    else if (units.length === 1 && JSON.stringify(summaryFields(summary)) === JSON.stringify(summaryFields(units[0].summary!))) {
      if (!await validateUnit(job, units[0], foreground, selection))
        throw new SummaryBudgetError("summary_legacy_lineage_unavailable");
    } else {
      const sources = await validatedChildren(job, units);
      await judge(job, summary, sources, `${job.id}:fallback:${job.round}`, foreground, selection);
    }
    await verifyJobEvidence(job, root);
    await completeJob(sdk, kv, job, summary, method, foreground);
  };
  const inputHash = (unit: SummaryQueueUnit): string => fingerprintId("summary-input", JSON.stringify(unit.items));
  const outputHash = (unit: Pick<SummaryQueueUnit, "summary" | "output">): string =>
    fingerprintId("summary-output", JSON.stringify([unit.summary && summaryFields(unit.summary), unit.output]));
  const lineageKey = (jobId: string, unitId: string): string => `${jobId}:lineage:${unitId}`;
  const validationKey = (jobId: string, unitId: string): string => `${jobId}:validated:${unitId}`;
  const validatedChildren = async (job: SummaryQueueJob, units: SummaryQueueUnit[]): Promise<PublicationEvidenceSource[]> => {
    const sources: PublicationEvidenceSource[] = [];
    for (const unit of units) {
      const child = await kv.get<ValidatedSummaryUnit>(KV.state, validationKey(job.id, unit.id));
      if (!child || child.inputHash !== inputHash(unit) || child.outputHash !== outputHash(unit))
        throw new SummaryBudgetError("summary_child_evidence_unavailable");
      sources.push({ id: unit.id, scope: KV.state, key: validationKey(job.id, unit.id), version: child.outputHash,
        project: job.project, sessionId: job.sessionId, content: child.output.text });
    }
    return sources;
  };
  const storeLineage = async (job: SummaryQueueJob, unit: SummaryQueueUnit, parents?: SummaryQueueUnit[], offsets?: Map<string | number, number>): Promise<void> => {
    const previous = unit.stage === "map"
      ? await kv.get<SummaryUnitLineage>(KV.state, lineageKey(job.id, unit.id)) : null;
    const children = new Set<string>();
    const sources: PublicationEvidenceSource[] = [];
    for (let index = 0; index < unit.items.length; index++) {
      const item = unit.items[index];
      let provenance: unknown;
      if (unit.stage === "map") {
        if (item.obsRangeStart !== item.obsRangeEnd) throw new SummaryBudgetError("summary_input_lineage_mismatch");
        const source = await readSummaryEvidenceSource(kv, job, item.obsRangeStart);
        const offset = offsets?.get(item.obsRangeStart) ?? 0;
        if (source.text.slice(offset, offset + item.text.length) !== item.text || !item.text)
          throw new SummaryBudgetError("summary_input_lineage_mismatch");
        offsets?.set(item.obsRangeStart, offset + item.text.length);
        provenance = { source: source.reference, scope: KV.observations(job.sessionId),
          key: source.reference.id, fragmentStart: offset, fragmentEnd: offset + item.text.length };
      } else {
        const parent = parents?.find(parent => parent.output?.obsRangeStart === item.obsRangeStart &&
          parent.output.obsRangeEnd === item.obsRangeEnd &&
          parent.output.text.slice(offsets?.get(parent.id) ?? 0, (offsets?.get(parent.id) ?? 0) + item.text.length) === item.text);
        if (!parent) throw new SummaryBudgetError("summary_legacy_lineage_unavailable");
        await validatedChildren(job, [parent]);
        const offset = offsets?.get(parent.id) ?? 0;
        offsets?.set(parent.id, offset + item.text.length);
        children.add(parent.id);
        provenance = [{ unitId: parent.id, outputHash: outputHash(parent), fragmentStart: offset, fragmentEnd: offset + item.text.length }];
      }
      sources.push({ id: `${unit.id}:${index}`, scope: KV.state, key: `${lineageKey(job.id, unit.id)}:${index}`,
        version: fingerprintId("summary-input-item", JSON.stringify(item)), project: job.project,
        sessionId: job.sessionId, content: JSON.stringify({ text: item.text, provenance }) });
    }
    if (previous) {
      if (previous.children.length !== 0 || previous.sources.length !== sources.length)
        throw new SummaryBudgetError("summary_input_lineage_mismatch");
      for (let index = 0; index < sources.length; index++) {
        const old = previous.sources[index], source = sources[index];
        if (old.id !== source.id || old.scope !== source.scope || old.key !== source.key ||
            old.project !== source.project || old.sessionId !== source.sessionId ||
            old.content !== source.content || typeof old.version !== "string" || !old.version)
          throw new SummaryBudgetError("summary_input_lineage_mismatch");
        // Keep publication checkpoint identity when the exact source proof is unchanged.
        sources[index] = old;
      }
    }
    await kv.set(KV.state, lineageKey(job.id, unit.id), { inputHash: inputHash(unit), sources,
      children: [...children] } satisfies SummaryUnitLineage);
  };
  const initializeMapLineage = async (job: SummaryQueueJob, units: SummaryQueueUnit[]): Promise<void> => {
    const offsets = new Map<string | number, number>();
    for (const unit of units) await storeLineage(job, unit, undefined, offsets);
    if (offsets.size !== job.observationCount) throw new SummaryBudgetError("summary_input_lineage_mismatch");
    for (const [ordinal, end] of offsets) {
      if (typeof ordinal !== "number" || (await readSummaryEvidenceSource(kv, job, ordinal)).text.length !== end)
        throw new SummaryBudgetError("summary_input_lineage_mismatch");
    }
  };
  const restoreMapLineage = async (job: SummaryQueueJob, unit: SummaryQueueUnit): Promise<void> =>
    withKeyedLock(`summary-map-lineage:${job.id}`, async () => {
      const existing = await kv.get<SummaryUnitLineage>(KV.state, lineageKey(job.id, unit.id));
      if (existing?.inputHash === inputHash(unit)) return;
      const units = await loadJobUnits(kv, job);
      if (job.stage !== "map" || units.some(value => !value || value.jobId !== job.id ||
          value.stage !== "map" || value.round !== job.round) ||
          !units.some(value => value?.id === unit.id && inputHash(value) === inputHash(unit)))
        throw new SummaryBudgetError("summary_input_lineage_mismatch");
      // Fragment offsets belong to the whole ordered map, including preceding units.
      await initializeMapLineage(job, units as SummaryQueueUnit[]);
    });
  const validateUnit = async (job: SummaryQueueJob, unit: SummaryQueueUnit, foreground: boolean,
    selection?: SelectedSummary): Promise<boolean> => {
    if (!unit.summary || !unit.output) return true;
    const existing = await kv.get<ValidatedSummaryUnit>(KV.state, validationKey(job.id, unit.id));
    if (existing) {
      if (existing.inputHash !== inputHash(unit) || existing.outputHash !== outputHash(unit))
        throw new SummaryBudgetError("summary_validation_capture_mismatch");
      return true;
    }
    let lineage = await kv.get<SummaryUnitLineage>(KV.state, lineageKey(job.id, unit.id));
    if (unit.stage === "map" && (!lineage || lineage.inputHash !== inputHash(unit))) {
      await restoreMapLineage(job, unit);
      lineage = await kv.get<SummaryUnitLineage>(KV.state, lineageKey(job.id, unit.id));
    }
    if (!lineage && unit.stage === "reduce") {
      const parents: SummaryQueueUnit[] = [];
      for (const item of unit.items) {
        if (item.obsRangeEnd - item.obsRangeStart + 1 > 128) throw new SummaryBudgetError("summary_legacy_lineage_unavailable");
        const parentId = fingerprintId("legacy-summary-parent", JSON.stringify([job.id, unit.round, item]));
        const parent: SummaryQueueUnit = { id: parentId, jobId: job.id, stage: "map", round: unit.round - 1,
          items: [], output: item, attempts: 0 };
        const sources: PublicationEvidenceSource[] = [];
        const lineageSources: PublicationEvidenceSource[] = [];
        for (let ordinal = item.obsRangeStart; ordinal <= item.obsRangeEnd; ordinal++) {
          const source = await readSummaryEvidenceSource(kv, job, ordinal);
          parent.items.push({ text: source.text, obsRangeStart: ordinal, obsRangeEnd: ordinal });
          const original = { id: source.reference.id, scope: KV.observations(job.sessionId), key: source.reference.id,
            version: source.reference.version, project: job.project, sessionId: job.sessionId };
          sources.push({ ...original, content: JSON.stringify(source.observation) });
          lineageSources.push({ ...original, content: JSON.stringify({ text: source.text,
            provenance: { source: source.reference, fragmentStart: 0, fragmentEnd: source.text.length } }) });
        }
        let receipt = await kv.get<ValidatedSummaryUnit>(KV.state, validationKey(job.id, parentId));
        if (!receipt) {
          const request = preparePublicationEvidence({ workflow: "summary", operationId: `${job.id}:legacy:${parentId}`,
            project: job.project, candidate: item.text, sources, evidenceComplete: true });
          if ("status" in request) throw new SummaryBudgetError("summary_legacy_lineage_unavailable");
          const requestId = await judge(job, item.text, sources, `${job.id}:legacy:${parentId}`, foreground, selection, unit.id);
          receipt = { inputHash: inputHash(parent), outputHash: outputHash(parent), requestId, output: item };
          await kv.set(KV.state, lineageKey(job.id, parentId), { inputHash: inputHash(parent), sources: lineageSources, children: [] });
          await kv.set(KV.state, validationKey(job.id, parentId), receipt);
        }
        if (receipt.inputHash !== inputHash(parent) || receipt.outputHash !== outputHash(parent))
          throw new SummaryBudgetError("summary_validation_capture_mismatch");
        parents.push(parent);
      }
      await storeLineage(job, unit, parents, new Map());
      lineage = await kv.get<SummaryUnitLineage>(KV.state, lineageKey(job.id, unit.id));
    }
    if (!lineage || lineage.inputHash !== inputHash(unit)) throw new SummaryBudgetError("summary_legacy_lineage_unavailable");
    await verifyLineage(job, lineage);
    // Legacy lineage proves the inputs, but only the generation receipt binds a cached output to them.
    if (unit.outputInputHash !== inputHash(unit)) {
      await kv.delete(KV.state, `${job.id}:raw:${unit.id}`);
      await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
        ...unit, outputInputHash: undefined, output: undefined, summary: undefined, attempts: 0,
        dispatchedAt: undefined, deliveryId: undefined, startedAt: undefined,
        lastAttemptAt: undefined, lastError: undefined,
      });
      return false;
    }
    const requestId = await judge(job, unit.summary, lineage.sources, `${job.id}:unit:${unit.id}`, foreground, selection, unit.id);
    await kv.set(KV.state, validationKey(job.id, unit.id), { inputHash: inputHash(unit), outputHash: outputHash(unit),
      requestId, output: unit.output, summary: unit.summary } satisfies ValidatedSummaryUnit);
    return true;
  };
  const verifyLineage = async (job: SummaryQueueJob, lineage: SummaryUnitLineage, visited = new Set<string>()): Promise<void> => {
    const session = await kv.get<Session>(KV.sessions, job.sessionId);
    if (!session || session.project !== job.project) throw new SummaryBudgetError("summary_source_changed");
    for (const source of lineage.sources) {
      const data = JSON.parse(source.content) as { text: string; provenance: { source?: { ordinal: number }; fragmentStart: number; fragmentEnd: number } | { unitId: string; outputHash: string; fragmentStart: number; fragmentEnd: number }[] };
      if (!Array.isArray(data.provenance)) {
        const ordinal = data.provenance.source?.ordinal;
        if (!ordinal) throw new SummaryBudgetError("summary_input_lineage_mismatch");
        const current = await readSummaryEvidenceSource(kv, job, ordinal);
        if (current.text.slice(data.provenance.fragmentStart, data.provenance.fragmentEnd) !== data.text)
          throw new SummaryBudgetError("summary_source_changed");
      } else for (const parent of data.provenance) {
        const child = await kv.get<ValidatedSummaryUnit>(KV.state, validationKey(job.id, parent.unitId));
        if (!child || child.outputHash !== parent.outputHash ||
            child.output.text.slice(parent.fragmentStart, parent.fragmentEnd) !== data.text)
          throw new SummaryBudgetError("summary_child_evidence_unavailable");
      }
    }
    for (const childId of lineage.children) if (!visited.has(childId)) {
      if (visited.size >= MAX_SUMMARY_CALLS * (MAX_SUMMARY_DEPTH + 1)) throw new SummaryBudgetError("summary_depth_limit_exceeded");
      visited.add(childId);
      const child = await kv.get<SummaryUnitLineage>(KV.state, lineageKey(job.id, childId));
      if (!child) throw new SummaryBudgetError("summary_child_evidence_unavailable");
      await verifyLineage(job, child, visited);
    }
  };
  const enqueueSummary = async (data: { sessionId?: string; project?: string; action?: unknown; selected?: unknown;
    manual?: unknown; recoveryOfJobId?: string; recoveryRequestId?: string } | undefined) => {
    if (data?.action !== undefined) {
      if (data.action !== "snapshot" || typeof data.sessionId !== "string" || !data.sessionId.trim() ||
          typeof data.project !== "string" || !data.project.trim() || data.selected !== undefined) {
        return { success: false, error: "invalid_snapshot_request" };
      }
      const session = await kv.get<Session>(KV.sessions, data.sessionId);
      if (!session || session.project !== data.project) return { success: false, error: "selection_identity_mismatch" };
      try {
        const current = await snapshot(kv, data.sessionId, data.project, true);
        return { success: true, sourceFingerprint: current.fingerprint, observationCount: current.observations.length };
      } catch {
        return { success: false, unavailable: true, error: "selected_snapshot_unavailable" };
      }
    }
    const manual = data?.manual !== undefined;
    const foreground = manual || data !== undefined && Object.prototype.hasOwnProperty.call(data, "selected");
    if (manual && data?.selected !== undefined || foreground && !(manual ? manualSelection(data?.manual, false) : validSelection(data?.selected, false)))
      return { success: false, error: "invalid_selection" };
    const selection = manual ? manualSelection(data?.manual, false) : foreground ? data!.selected as SelectedSummary : undefined;
    const origin = manual ? "manual" : "selected";
    if (typeof data?.sessionId !== "string" || !data.sessionId.trim()) {
      return { success: false, error: "sessionId is required" };
    }
    const sessionId = data.sessionId.trim();
    return withKeyedLock(`summary-session:${sessionId}`, async () => {
      const session = await kv.get<Session>(KV.sessions, sessionId);
      if (!session) return { success: false, error: "session_not_found" };
      const existingIntent = await kv.get<SummaryQueueIntent>(KV.summaryQueueIntents, sessionId);
      const existingActive = await kv.get<ActiveJob>(KV.summaryQueueActive, sessionId);
      const existingJob = existingActive ? await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, existingActive.jobId) : null;
      if (!foreground && (existingIntent?.executionOrigin || existingJob?.executionOrigin)) {
        return { success: true, reason: "selected_foreground_required" };
      }
      if (selection && (selection.sessionId !== sessionId || selection.project !== session.project)) {
        return { success: false, error: "selection_identity_mismatch" };
      }
      let selectedSnapshot: Awaited<ReturnType<typeof snapshot>> | undefined;
      try {
        selectedSnapshot = selection
          ? manual ? await normalManualSnapshot(kv, sessionId, session.project) : await snapshot(kv, sessionId, session.project, true)
          : undefined;
      } catch (error) {
        if (manual) return { success: false, error: error instanceof SummaryBudgetError
          ? error.message : "summary_source_grounding_snapshot_unavailable" };
        throw error;
      }
      if (selection && selectedSnapshot?.fingerprint !== selection.sourceFingerprint) {
        return { success: false, error: "selection_source_mismatch" };
      }
      if (selection) {
        if (existingJob?.status === "pending" && (existingJob.project !== session.project || existingJob.sessionId !== sessionId ||
            existingJob.snapshotFingerprint !== selection.sourceFingerprint)) {
          return { success: false, error: "selection_source_mismatch" };
        }
        if (existingJob?.status === "pending" && existingJob.executionOrigin !== origin) {
          return { success: false, error: "selection_origin_mismatch" };
        }
        if (existingJob?.status === "pending" && selection.jobId !== undefined && selection.jobId !== existingJob.id) {
          return { success: false, error: "selection_identity_mismatch" };
        }
        if (!manual && existingJob?.status !== "pending" && selectedSnapshot &&
            await exactSelectedCompletion(kv,
              await kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId), existingJob,
              sessionId, session.project, selectedSnapshot.fingerprint, selectedSnapshot.observations.length, selection)) {
          await kv.delete(KV.summaryQueueIntents, sessionId);
          return { success: true, queued: false, completed: true };
        }
        if (selection.jobId !== undefined && existingJob?.status !== "pending") {
          return { success: false, error: "selection_identity_mismatch" };
        }
      }
      if (foreground && existingIntent?.executionOrigin && existingIntent.executionOrigin !== origin) {
        return { success: false, error: "selection_origin_mismatch" };
      }
      const intent = await kv.get<SummaryQueueIntent>(KV.summaryQueueIntents, sessionId);
      if (!intent || manual && !intent.executionOrigin) await kv.set(KV.summaryQueueIntents, sessionId, {
        sessionId, createdAt: new Date().toISOString(), ...(foreground && { executionOrigin: origin }),
      } satisfies SummaryQueueIntent);
      if (isBackgroundRecoveryPaused() && !foreground) {
        return { success: true, queued: false, paused: true };
      }
      if (provider.name === "noop" && !foreground) return { success: true, queued: false, skipped: "no_provider" };
      const current = selectedSnapshot ?? await snapshot(kv, sessionId, session.project);
      const retained = await kv.get<{ jobId: string; reason: string }>(KV.state,
        `${sessionId}:failed-summary:${current.fingerprint}`);
      let recoveryOf: SummaryQueueJob | null = null;
      if (data?.recoveryOfJobId !== undefined || data?.recoveryRequestId !== undefined) {
        if (!manual || typeof data.recoveryOfJobId !== "string" || typeof data.recoveryRequestId !== "string")
          return { success: false, error: "invalid_summary_recovery" };
        recoveryOf = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, data.recoveryOfJobId);
        if (!recoveryOf || recoveryOf.status !== "failed" || recoveryOf.failure !== "summary_publication_invalid_response" ||
            recoveryOf.executionOrigin !== "manual" || recoveryOf.sessionId !== sessionId || recoveryOf.project !== session.project ||
            recoveryOf.snapshotFingerprint !== current.fingerprint || manualRecoveryRequestId(recoveryOf) !== data.recoveryRequestId)
          return { success: false, error: "summary_recovery_not_available" };
        const recoveryJobId = fingerprintId("sqj", data.recoveryRequestId);
        const priorRecovery = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, recoveryJobId);
        if (priorRecovery) {
          if (priorRecovery.sessionId !== sessionId || priorRecovery.project !== session.project ||
              priorRecovery.snapshotFingerprint !== current.fingerprint || priorRecovery.executionOrigin !== "manual")
            return { success: false, error: "summary_recovery_identity_mismatch" };
          if (priorRecovery.status === "pending") return { success: true, queued: true, jobId: recoveryJobId, deduplicated: true,
            continuation: { jobId: recoveryJobId, round: priorRecovery.round, unitIds: priorRecovery.unitIds } };
          if (priorRecovery.status === "completed") return { success: true, queued: false, completed: true, jobId: recoveryJobId };
          return { success: false, queued: false, retained: true, jobId: recoveryJobId,
            error: priorRecovery.failure ?? "summary_job_failed" };
        }
      }
      if (retained && (!recoveryOf || retained.jobId !== recoveryOf.id)) {
        const retainedJob = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, retained.jobId);
        const corrected = retainedJob ? await completedPromptCorrection(retainedJob) : null;
        if (corrected) return { success: true, queued: false, completed: true, jobId: corrected };
        const recoveryRoot = manual && retainedJob ? await manualRecoveryRootForMarker(retainedJob) : undefined;
        const continuation = recoveryRoot ? manualRecoveryContinuation(recoveryRoot) : undefined;
        return { success: false, queued: false, retained: true, jobId: retained.jobId, error: retained.reason,
          ...(continuation && { continuation }) };
      }
      if (current.observations.length === 0) {
        await kv.delete(KV.summaryQueueIntents, sessionId);
        return { success: true, queued: false, skipped: "no_observations" };
      }
      const active = await kv.get<ActiveJob>(KV.summaryQueueActive, sessionId);
      if (active) {
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, active.jobId);
        if (job?.status === "pending") {
          if (recoveryOf && job.id !== fingerprintId("sqj", data!.recoveryRequestId!))
            return { success: false, error: "summary_recovery_conflict" };
          if (foreground && job.snapshotFingerprint !== current.fingerprint) {
            return { success: false, error: "selection_source_mismatch" };
          }
          return { success: true, queued: true, jobId: job.id, deduplicated: true,
            ...(foreground && { continuation: { jobId: job.id, round: job.round, unitIds: job.unitIds } }) };
        }
        await releaseSummaryActiveJob(kv, sessionId, active.jobId);
      }
      const completed = await kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId);
      if (!recoveryOf && completed?.fingerprint === current.fingerprint &&
          await kv.get<SessionSummary>(KV.summaries, sessionId)) {
        if (foreground && !manual) return { success: false, error: "selection_origin_mismatch" };
        await kv.delete(KV.summaryQueueIntents, sessionId);
        return { success: true, queued: false, completed: true };
      }
      const config = getSummaryBudgetConfig();
      const groups = packSummaryItems(current.items, SUMMARY_SYSTEM, buildSummaryItemsPrompt,
        summaryChunkInputLimit(config), config.chunkSize);
      const jobId = recoveryOf ? fingerprintId("sqj", data!.recoveryRequestId!) : generateId("sqj");
      const units = createUnits(jobId, "map", 0, groups);
      const now = new Date().toISOString();
      const job: SummaryQueueJob = {
        id: jobId, sessionId, project: session.project,
        snapshotFingerprint: current.fingerprint, observationCount: current.observations.length,
        config, createdAt: now, updatedAt: now, status: "pending", stage: "map",
        round: 0, unitIds: units.map(unit => unit.id), sourceProgressSize: summaryProgressSize(current.items),
        generationRevision: SUMMARY_GENERATION_REVISION,
        dispatchPending: true,
        ...(foreground && { executionOrigin: origin }),
      };
      const root = await captureSummaryEvidence(kv, job, current.observations);
      if (root.mode === "hierarchical") await initializeMapLineage(job, units);
      await writeUnits(kv, jobId, units);
      await kv.set(KV.summaryQueueJobs, jobId, job);
      await setSummaryActiveJob(kv, sessionId, jobId);
      await recordAudit(kv, "compress", "mem::summary-enqueue", [sessionId],
        { jobId, unitCount: units.length, outcome: "queued" }, undefined, undefined, `${jobId}:queued`);
      if (!foreground) await enqueue(sdk, "mem::summary-dispatch", { jobId, round: 0, offset: 0 });
      await recordOperation(kv, "mem::summary-enqueue", "queued", { unitsQueued: units.length }, undefined,
        { executionId: job.id, executionLevel: "workflow", occurredAt: job.createdAt });
      return { success: true, queued: true, jobId,
        ...(foreground && { continuation: { jobId, round: 0, unitIds: job.unitIds } }) };
    });
  };
  const runManualRecovery = async (request: ManualSummaryContinuation, failedJob: SummaryQueueJob, zeroSpendPredecessor?: SummaryQueueJob): Promise<Record<string, unknown>> => {
    const generation: SummaryRecoveryGeneration = request.recoveryGeneration ?? "full-originals-v1";
    const identity = manualRecoveryIdentity(request);
    const resultKey = `manual-summary-recovery:${request.operationId}`;
    const prior = await kv.get<{ identity: string; response: Record<string, unknown> }>(KV.state, resultKey);
    if (prior) return prior.identity === identity ? prior.response : { success: false, error: "summary_recovery_identity_mismatch" };
    const recoveryJobId = fingerprintId("sqj", request.operationId);
    const resolve = async (): Promise<ManualRecoveryResolution> => {
      const source = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, failedJob.id);
      if (!source || source.status !== "failed" || source.failure !== "summary_publication_invalid_response" ||
          source.executionOrigin !== "manual" || !zeroSpendPredecessor && !sameContinuation(manualRecoveryContinuation(source, generation), request))
        return { kind: "response", response: { success: false, error: "summary_recovery_not_available" } };
      const session = await kv.get<Session>(KV.sessions, request.sessionId);
      if (!session || session.project !== request.project || source.project !== session.project ||
          source.snapshotFingerprint !== request.sourceFingerprint)
        return { kind: "response", response: { success: false, error: "summary_recovery_identity_mismatch" } };
      if (zeroSpendPredecessor) {
        const predecessor = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, zeroSpendPredecessor.id);
        if (!predecessor) return { kind: "response", response: { success: false, error: "summary_zero_spend_recovery_not_available" } };
        try {
          await assertZeroSpendSummaryPredecessor(kv, predecessor, source,
            fingerprintId("sqj", manualRecoveryRequestId(source, CURRENT_SUMMARY_RECOVERY_GENERATION)));
        } catch (error) { return { kind: "response", response: { success: false,
          error: error instanceof SummaryBudgetError ? error.message : "summary_zero_spend_recovery_not_available" } }; }
      }
      const existing = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, recoveryJobId);
      const journal = await kv.get<SummaryRecoveryConstruction>(KV.state, summaryConstructionKey(recoveryJobId));
      if (journal) {
        try {
          if (journal.identity !== identity || journal.predecessorJobId !== (zeroSpendPredecessor?.id ?? source.id) ||
              journal.rootJobId !== source.id || journal.job.id !== recoveryJobId ||
              journal.job.sessionId !== source.sessionId || journal.job.project !== source.project ||
              journal.job.snapshotFingerprint !== source.snapshotFingerprint || journal.job.observationCount !== source.observationCount ||
              journal.job.recoveryOfJobId !== source.id || journal.job.recoveryMode !== "full-originals-v1" ||
              (journal.job.recoveryGeneration ?? "full-originals-v1") !== generation ||
              journal.job.zeroSpendRecoveryOfJobId !== zeroSpendPredecessor?.id || journal.unit.id !== fingerprintId("squ",
                JSON.stringify([request.operationId, source.id, source.project, source.sessionId, source.snapshotFingerprint])))
            throw new SummaryBudgetError("summary_recovery_identity_mismatch");
          if (journal.state === "creating") await fullOriginalRecovery(journal.job);
          return { kind: "job", job: await constructSummaryRecovery(kv, journal) };
        } catch (error) { return { kind: "response", response: { success: false,
          error: error instanceof SummaryBudgetError ? error.message : "summary_recovery_construction_unavailable" } }; }
      }
      if (existing) {
        if (existing.id !== recoveryJobId || existing.recoveryMode !== "full-originals-v1" ||
            (existing.recoveryGeneration ?? "full-originals-v1") !== generation ||
            existing.recoveryOfJobId !== source.id || existing.zeroSpendRecoveryOfJobId !== zeroSpendPredecessor?.id || existing.executionOrigin !== "manual" ||
            existing.sessionId !== request.sessionId || existing.project !== request.project ||
            existing.snapshotFingerprint !== request.sourceFingerprint ||
            existing.observationCount !== source.observationCount)
          return { kind: "response", response: { success: false, error: "summary_recovery_identity_mismatch" } };
        if (existing.status === "pending") {
          try { await fullOriginalRecovery(existing); }
          catch (error) { return { kind: "response", response: { success: false, error: error instanceof SummaryBudgetError
            ? error.message : "summary_source_grounding_snapshot_unavailable" } }; }
        }
        return { kind: "job", job: existing };
      }
      const active = await kv.get<ActiveJob>(KV.summaryQueueActive, request.sessionId);
      const activeJob = active ? await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, active.jobId) : null;
      if (active && (!activeJob || activeJob.status === "pending") && zeroSpendPredecessor)
        return { kind: "response", response: { success: false, error: "summary_recovery_conflict" } };
      if (activeJob?.status === "pending") {
        const expectedLegacyChildId = fingerprintId("sqj", manualRecoveryRequestId(source, "full-originals-v1"));
        const isDirectLegacyChild = activeJob.id === expectedLegacyChildId &&
          activeJob.executionOrigin === "manual" && activeJob.recoveryMode === "full-originals-v1" &&
          activeJob.recoveryGeneration === undefined && activeJob.recoveryOfJobId === source.id &&
          activeJob.sessionId === source.sessionId && activeJob.project === source.project &&
          activeJob.snapshotFingerprint === source.snapshotFingerprint &&
          activeJob.observationCount === source.observationCount && activeJob.unitIds.length === 1 &&
          activeJob.stage === "map" && activeJob.round === 0;
        const terminalized = generation === CURRENT_SUMMARY_RECOVERY_GENERATION && isDirectLegacyChild
          ? await terminalizeManualJob(activeJob, { activeLease: "required", legacyRetirement: true }) : "conflict";
        if (typeof terminalized === "object") {
          return { kind: "response", response: { success: true, completed: true,
            summary: terminalized.completed, regeneratedFrom: source.id } };
        }
        if (terminalized !== "terminalized")
          return { kind: "response", response: { success: false, error: "summary_recovery_conflict" } };
      }
      let recovery: FullOriginalRecovery;
      let config: SummaryQueueJob["config"];
      try {
        config = getSummaryBudgetConfig();
        const candidate: SummaryQueueJob = {
          id: recoveryJobId, sessionId: request.sessionId, project: request.project,
          snapshotFingerprint: request.sourceFingerprint, observationCount: source.observationCount,
          config, createdAt: "", updatedAt: "", status: "pending", stage: "map",
          round: 0, unitIds: [], sourceProgressSize: 0, executionOrigin: "manual",
          recoveryMode: "full-originals-v1", recoveryOfJobId: source.id,
          ...(generation === CURRENT_SUMMARY_RECOVERY_GENERATION && { recoveryGeneration: generation }),
        };
        recovery = await fullOriginalRecovery(candidate);
      } catch (error) {
        return { kind: "response", response: { success: false, error: error instanceof SummaryBudgetError
          ? error.message : "summary_source_grounding_snapshot_unavailable" } };
      }
      const currentActive = await kv.get<ActiveJob>(KV.summaryQueueActive, request.sessionId);
      if (currentActive) {
        const currentActiveJob = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, currentActive.jobId);
        if (currentActiveJob?.status === "pending")
          return { kind: "response", response: { success: false, error: "summary_recovery_conflict" } };
        await releaseSummaryActiveJob(kv, request.sessionId, currentActive.jobId);
      }
      const now = new Date().toISOString();
      const unit = { id: fingerprintId("squ", JSON.stringify([request.operationId, source.id,
        source.project, source.sessionId, source.snapshotFingerprint])), jobId: recoveryJobId, stage: "map" as const, round: 0,
        items: [{ text: "", obsRangeStart: 1, obsRangeEnd: source.observationCount,
          sourcePaths: [], modifiedPaths: [] }], attempts: 0 } satisfies SummaryQueueUnit;
      const created: SummaryQueueJob = {
        id: recoveryJobId, sessionId: request.sessionId, project: request.project,
        snapshotFingerprint: request.sourceFingerprint, observationCount: source.observationCount,
        config, createdAt: now, updatedAt: now, status: "pending", stage: "map",
        round: 0, unitIds: [unit.id], sourceProgressSize: summaryProgressSize(recovery.items),
        executionOrigin: "manual", recoveryMode: "full-originals-v1", recoveryOfJobId: source.id,
        ...(generation === CURRENT_SUMMARY_RECOVERY_GENERATION && { recoveryGeneration: generation }),
      };
      if (zeroSpendPredecessor) created.zeroSpendRecoveryOfJobId = zeroSpendPredecessor.id;
      const operation: ManualOperation = { id: `${created.id}:manual`, sessionId: created.sessionId,
        jobId: created.id, project: created.project, sourceFingerprint: created.snapshotFingerprint,
        step: 0, maxRequests: generation === CURRENT_SUMMARY_RECOVERY_GENERATION ? 2 : 1 };
      try {
        await constructSummaryRecovery(kv, { state: "creating", identity,
          predecessorJobId: zeroSpendPredecessor?.id ?? source.id, rootJobId: source.id, job: created, unit, operation });
      } catch (error) {
        if (!(error instanceof SummaryBudgetError)) throw error;
        return { kind: "response", response: { success: false, error: error.message } };
      }
      await recordOperation(kv, "mem::summary-enqueue", "queued", { unitsQueued: 1 });
      return { kind: "job", job: created };
    };
    const resolution = await withKeyedLock(`summary-session:${request.sessionId}`, () => zeroSpendPredecessor
      ? withSummaryUnitLocks(zeroSpendPredecessor.unitIds, () =>
        withKeyedLock(`summary-job:${zeroSpendPredecessor.id}`, resolve)) : resolve());
    if (resolution.kind === "response") {
      if (resolution.response.success === true && resolution.response.completed === true)
        await kv.set(KV.state, resultKey, { identity, response: resolution.response });
      return resolution.response;
    }
    const job = resolution.job;
    if (job.status === "completed") {
      const effect = await kv.get<{ summary: SessionSummary; completedAt: string }>(KV.state, `${job.id}:publication-effect`);
      const summary = await kv.get<SessionSummary>(KV.summaries, job.sessionId);
      const published = effect?.summary;
      const validReceipt = !!effect && !!published && !!summary && typeof effect.completedAt === "string" &&
        effect.completedAt === job.completedAt &&
        published.sessionId === job.sessionId && published.project === job.project &&
        published.observationCount === job.observationCount &&
        summaryPublicationIdentity(published) === summaryPublicationIdentity(summary);
      const response = validReceipt
        ? { success: true, completed: true, summary: published, regeneratedFrom: failedJob.id }
        : { success: false, error: "summary_publication_effect_mismatch" };
      await kv.set(KV.state, resultKey, { identity, response });
      return response;
    }
    if (job.status !== "pending") {
      const response = { success: false, error: job.failure ?? "summary_job_failed" };
      await kv.set(KV.state, resultKey, { identity, response });
      return response;
    }
    if (await kv.get(KV.state, summaryConstructionKey(job.id)))
      await recordAudit(kv, "compress", "mem::summary-enqueue", [request.sessionId],
        { jobId: job.id, unitCount: 1, outcome: "queued", recoveryMode: "full-originals-v1",
          recoveryGeneration: generation, ...(zeroSpendPredecessor && { zeroSpendRecoveryOfJobId: zeroSpendPredecessor.id }) },
        undefined, undefined, `${job.id}:queued`);
    const id = `${job.id}:manual`;
    const existingOperation = await kv.get<ManualOperation>(KV.state, id);
    let operation: ManualOperation;
    if (existingOperation) {
      operation = existingOperation;
    } else {
      operation = { id, sessionId: job.sessionId, jobId: job.id, project: job.project,
        sourceFingerprint: job.snapshotFingerprint, step: 0,
        maxRequests: generation === CURRENT_SUMMARY_RECOVERY_GENERATION ? 2 : 1 };
      await kv.set(KV.state, id, operation);
    }
    if (operation.continuation) {
      const response = { success: true, queued: true, completed: false, jobId: job.id,
        regeneratedFrom: failedJob.id, continuation: operation.continuation,
        maxRequests: operation.maxRequests };
      await kv.set(KV.state, resultKey, { identity, response });
      return response;
    }
    const units = await loadJobUnits(kv, job);
    const unit = units.find(value => value && !value.output) ?? units[0];
    if (!unit) {
      const response = { success: false, error: "summary_unit_missing" };
      await kv.set(KV.state, resultKey, { identity, response });
      return response;
    }
    const dispatched = await sdk.trigger({ function_id: "mem::summary-dispatch", payload: {
      jobId: job.id, round: job.round, offset: 0,
      manual: { project: job.project, sessionId: job.sessionId, sourceFingerprint: job.snapshotFingerprint,
        jobId: job.id, round: job.round, unitId: unit.id },
    } }) as { success: boolean; continuation?: { deliveryId: string }; busy?: boolean; skipped?: boolean };
    if (!dispatched.success || !dispatched.continuation?.deliveryId) {
      const response = { ...dispatched, completed: false, regeneratedFrom: failedJob.id };
      await kv.set(KV.state, resultKey, { identity, response });
      return response;
    }
    const continuation: ManualSummaryContinuation = { operationId: operation.id, step: operation.step,
      jobId: job.id, project: job.project, sessionId: job.sessionId, sourceFingerprint: job.snapshotFingerprint,
      round: job.round, unitId: unit.id, deliveryId: dispatched.continuation.deliveryId };
    operation = { ...operation, continuation };
    await kv.set(KV.state, operation.id, operation);
    const response = { success: true, queued: true, completed: false, jobId: job.id,
      regeneratedFrom: failedJob.id, continuation, maxRequests: operation.maxRequests };
    await kv.set(KV.state, resultKey, { identity, response });
    return response;
  };
  const runManual = async (data: { sessionId?: string; continuation?: unknown; zeroSpendRecoveryOfJobId?: unknown }) => {
    const requestedSessionId = data.sessionId;
    const requestedContinuation = data.continuation;
    const predecessorId = data.zeroSpendRecoveryOfJobId;
    if (predecessorId !== undefined && (typeof predecessorId !== "string" || !predecessorId.trim() ||
        predecessorId.length > 128 || predecessorId !== predecessorId.trim() || requestedContinuation !== undefined))
      return { success: false, error: "invalid_manual_summary_request" };
    if (typeof requestedSessionId !== "string" || !requestedSessionId.trim() ||
        requestedContinuation !== undefined && !validManualSummaryContinuation(requestedContinuation))
      return { success: false, error: "invalid_manual_summary_request" };
    const sessionId = requestedSessionId.trim();
    return withKeyedLock(`manual-summary:${sessionId}`, async () => {
      const supplied = requestedContinuation;
      if (typeof predecessorId === "string") {
        const predecessor = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, predecessorId);
        const root = predecessor?.recoveryOfJobId
          ? await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, predecessor.recoveryOfJobId) : null;
        if (!predecessor || !root || predecessor.sessionId !== sessionId || !Array.isArray(predecessor.unitIds))
          return { success: false, error: "summary_zero_spend_recovery_not_available" };
        const base = manualRecoveryContinuation(root);
        if (!base) return { success: false, error: "summary_zero_spend_recovery_not_available" };
        const operationId = zeroSpendSummaryIdentity(predecessor, root);
        return runManualRecovery({ ...base, operationId, deliveryId: operationId }, root, predecessor);
      }
      if (supplied && supplied.sessionId !== sessionId) return { success: false, error: "manual_summary_identity_mismatch" };
      if (supplied) {
        const failedJob = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, supplied.jobId);
        const generation: SummaryRecoveryGeneration = supplied.recoveryGeneration ?? "full-originals-v1";
        if (failedJob && sameContinuation(manualRecoveryContinuation(failedJob, generation), supplied)) {
          return runManualRecovery(supplied, failedJob);
        }
      }
      let operation: ManualOperation | null;
      if (!supplied) {
        const session = await kv.get<Session>(KV.sessions, sessionId);
        if (!session) return { success: false, error: "session_not_found" };
        let sourceFingerprint: string;
        try {
          sourceFingerprint = (await normalManualSnapshot(kv, sessionId, session.project)).fingerprint;
        } catch (error) {
          return { success: false, error: error instanceof SummaryBudgetError
            ? error.message : "summary_source_grounding_snapshot_unavailable" };
        }
        const queued = await enqueueSummary({ sessionId, manual: { sessionId, project: session.project, sourceFingerprint } });
        if (queued.success && "completed" in queued && queued.completed === true) {
          const summary = await kv.get<SessionSummary>(KV.summaries, sessionId);
          if (summary?.sessionId === sessionId && summary.project === session.project) return { success: true, completed: true, summary };
        }
        if (!queued.success || !("jobId" in queued) || typeof queued.jobId !== "string") return queued;
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, queued.jobId);
        if (!job || job.executionOrigin !== "manual") return { success: false, error: "manual_summary_origin_mismatch" };
        if (!await summaryRecoveryReady(kv, job)) return { success: false, error: "summary_recovery_construction_pending" };
        const id = `${job.id}:manual`;
        operation = await kv.get<ManualOperation>(KV.state, id);
        if (!operation) {
          operation = { id, sessionId, jobId: job.id, project: job.project, sourceFingerprint: job.snapshotFingerprint,
            step: 0, maxRequests: Math.min(MAX_SUMMARY_CALLS, job.unitIds.length * (MAX_SUMMARY_DEPTH + 1)) * 2 + 2 };
          await kv.set(KV.state, id, operation);
        }
      } else {
        operation = await kv.get<ManualOperation>(KV.state, supplied.operationId);
        if (!operation || operation.sessionId !== sessionId || operation.id !== `${operation.jobId}:manual` ||
            supplied.jobId !== operation.jobId || supplied.project !== operation.project ||
            supplied.sourceFingerprint !== operation.sourceFingerprint)
          return { success: false, error: "manual_summary_identity_mismatch" };
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, operation.jobId);
        if (!job || job.executionOrigin !== "manual" || job.sessionId !== sessionId || job.project !== operation.project ||
            job.snapshotFingerprint !== operation.sourceFingerprint)
          return { success: false, error: "manual_summary_identity_mismatch" };
        const previous = await kv.get<{ request: ManualSummaryContinuation; response?: Record<string, unknown> }>(KV.state,
          `${operation.id}:request:${supplied.step}`);
        if (previous?.response && sameContinuation(previous.request, supplied)) return previous.response;
        if (!sameContinuation(operation.continuation, supplied))
          return { success: false, error: "manual_summary_stale_continuation" };
      }
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, operation.jobId);
      if (!job || job.executionOrigin !== "manual" || job.sessionId !== sessionId || job.project !== operation.project ||
          job.snapshotFingerprint !== operation.sourceFingerprint) return { success: false, error: "manual_summary_identity_mismatch" };
      const finish = async (): Promise<Record<string, unknown> | undefined> => {
        return withKeyedLock(`summary-job:${job.id}`, async () => {
          const latest = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
          if (!latest) return { success: false, completed: false, error: "manual_summary_identity_mismatch" };
          if (latest.status === "completed") {
            const [effect, summary] = await Promise.all([
              kv.get<SummaryPublicationEffect>(KV.state, `${job.id}:publication-effect`),
              kv.get<SessionSummary>(KV.summaries, sessionId),
            ]);
            if (effect?.summary && effect.completedAt === latest.completedAt && summary &&
                summaryPublicationIdentity(effect.summary) === summaryPublicationIdentity(summary))
              return { success: true, completed: true, summary };
            return { success: false, completed: false, error: "summary_publication_effect_mismatch" };
          }
          if (latest.status !== "pending")
            return { success: false, completed: false, error: latest.failure ?? "summary_job_failed" };
          try {
            if (await recoverAppliedPublication(latest, true)) {
              const [completedJob, effect, summary] = await Promise.all([
                kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id),
                kv.get<SummaryPublicationEffect>(KV.state, `${job.id}:publication-effect`),
                kv.get<SessionSummary>(KV.summaries, sessionId),
              ]);
              if (completedJob?.status === "completed" && effect?.summary && effect.completedAt === completedJob.completedAt &&
                  summary && summaryPublicationIdentity(effect.summary) === summaryPublicationIdentity(summary))
                return { success: true, completed: true, summary };
              return { success: false, completed: false, error: "summary_publication_effect_mismatch" };
            }
          } catch (error) {
            if (!(error instanceof SummaryBudgetError)) throw error;
            return { success: false, completed: false, error: error.message };
          }
        });
      };
      const terminalizeForLimit = async (): Promise<Record<string, unknown>> => {
        const terminalized = await withKeyedLock(`summary-session:${sessionId}`, () =>
          terminalizeManualJob(job, { activeLease: "matching-or-empty" }));
        if (typeof terminalized === "object") return { success: true, completed: true, summary: terminalized.completed };
        if (terminalized === "terminalized")
          return { success: false, completed: false, error: "manual_summary_limit_exceeded" };
        return { success: false, completed: false,
          error: terminalized === "live" ? "manual_summary_stale_continuation" : "summary_recovery_conflict" };
      };
      if (!await summaryRecoveryReady(kv, job))
        return { success: false, completed: false, error: "summary_recovery_construction_pending" };
      const completed = await finish();
      if (completed) return completed;
      let result: Record<string, unknown> = { success: true, completed: false };
      if (supplied) {
        const requestKey = `${operation.id}:request:${supplied.step}`;
        const previous = await kv.get(KV.state, requestKey);
        const unresolvedPrevious = !!previous && typeof previous === "object" &&
          !(previous as { response?: unknown }).response;
        if (unresolvedPrevious) {
          const [unit, capture] = await Promise.all([
            kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), supplied.unitId),
            kv.get<{ state: "intended" | "captured"; inputHash?: string; raw?: string; overflow?: true }>(KV.state,
              `${job.id}:raw:${supplied.unitId}`),
          ]);
          const staleCapture = !!unit && !!capture && capture.inputHash !== inputHash(unit);
          if (staleCapture) await kv.delete(KV.state, `${job.id}:raw:${supplied.unitId}`);
          const capturedRaw = !!unit && capture?.state === "captured" && capture.inputHash === inputHash(unit) &&
            (typeof capture.raw === "string" || capture.overflow === true);
          const identifiedUnit = !!unit && unit.id === supplied.unitId && unit.jobId === job.id &&
            unit.round === supplied.round && job.unitIds.includes(unit.id);
          if (!identifiedUnit || unit.output === undefined && !capturedRaw && !staleCapture)
            return { success: false, completed: false, error: "summary_dispatch_outcome_unknown" };
        }
        if (!previous && operation.step === supplied.step) {
          if (operation.step >= operation.maxRequests) return terminalizeForLimit();
          operation = { ...operation, step: operation.step + 1 };
          await kv.set(KV.state, operation.id, operation);
        }
        if (!previous) await kv.set(KV.state, requestKey, { request: supplied });
        if (job.round === supplied.round) {
          try {
            result = { completed: false, ...await sdk.trigger({ function_id: "mem::summary-unit", payload: {
              jobId: job.id, unitId: supplied.unitId, deliveryId: supplied.deliveryId,
              manual: { project: job.project, sessionId, sourceFingerprint: job.snapshotFingerprint,
                jobId: job.id, round: supplied.round, unitId: supplied.unitId, deliveryId: supplied.deliveryId } },
              timeoutMs: SUMMARY_CALL_TIMEOUT_MS * 2 + STALE_DELIVERY_MS }) as Record<string, unknown> };
          } catch (error) {
            if (error instanceof SummaryBudgetError) return { success: false, completed: false, error: error.message };
            throw error;
          }
        }
        const done = await finish();
        if (done) result = done;
      }
      if (result.success === true && result.completed !== true && !result.busy && !result.skipped) {
        const latest = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
        if (!latest || latest.status !== "pending") return { success: false, error: "summary_job_failed" };
        const units = await loadJobUnits(kv, latest);
        const unit = units.find(value => value && !value.output) ?? units[0];
        if (!unit) return { success: false, error: "summary_unit_missing" };
        const dispatched = await sdk.trigger({ function_id: "mem::summary-dispatch", payload: {
          jobId: latest.id, round: latest.round, offset: 0,
          manual: { project: latest.project, sessionId, sourceFingerprint: latest.snapshotFingerprint,
            jobId: latest.id, round: latest.round, unitId: unit.id },
        } }) as { success: boolean; continuation?: { deliveryId: string }; busy?: boolean; skipped?: boolean };
        if (!dispatched.success || !dispatched.continuation?.deliveryId) result = { ...dispatched, completed: false };
        else {
          const continuation: ManualSummaryContinuation = { operationId: operation.id, step: operation.step,
            jobId: latest.id, project: latest.project, sessionId, sourceFingerprint: latest.snapshotFingerprint,
            round: latest.round, unitId: unit.id, deliveryId: dispatched.continuation.deliveryId };
          operation = { ...operation, continuation };
          await kv.set(KV.state, operation.id, operation);
          result = { ...result, queued: true, completed: false, continuation,
            maxRequests: operation.maxRequests };
        }
      }
      if (supplied && (operation.step > supplied.step || result.completed === true))
        await kv.set(KV.state, `${operation.id}:request:${supplied.step}`, { request: supplied, response: result });
      return result;
    });
  };
  sdk.registerFunction("mem::summary-enqueue", async (data: Parameters<typeof enqueueSummary>[0] & { continuation?: unknown; zeroSpendRecoveryOfJobId?: unknown }) => {
    if (data?.zeroSpendRecoveryOfJobId !== undefined && data.action !== "manual")
      return { success: false, error: "invalid_manual_summary_request" };
    return data?.action === "manual" ? runManual(data) : enqueueSummary(data);
  });

  sdk.registerFunction("mem::summary-dispatch", async (data: { jobId: string; round: number; offset: number; selected?: unknown; manual?: unknown }) => {
    if (Object.prototype.hasOwnProperty.call(data, "selected") || data.manual !== undefined) {
      const selection = data.manual !== undefined ? manualSelection(data.manual, true) : validSelection(data.selected, true) ? data.selected : undefined;
      if (!selection || data.manual !== undefined && data.selected !== undefined || selection.deliveryId !== undefined || data.offset !== 0) {
        return { success: false, error: "invalid_selection" };
      }
      return withKeyedLock(`summary-unit:${selection.unitId}`, () =>
        withKeyedLock(`summary-job:${data.jobId}`, async () => {
          const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, data.jobId);
          if (job?.status === "pending" && job.executionOrigin !== (selection.manual ? "manual" : "selected")) return { success: false, error: "selection_origin_mismatch" };
          if (!job || selection.jobId !== data.jobId || selection.round !== data.round || job.round !== data.round ||
              selection.project !== job.project || selection.sessionId !== job.sessionId ||
              selection.sourceFingerprint !== job.snapshotFingerprint || !job.unitIds.includes(selection.unitId!)) {
            return { success: false, error: "selection_identity_mismatch" };
          }
          if (!await summaryRecoveryReady(kv, job)) return { success: false, error: "summary_recovery_construction_pending" };
          const session = await kv.get<Session>(KV.sessions, job.sessionId);
          if (!session || session.project !== selection.project) {
            return { success: false, error: "selection_source_mismatch" };
          }
          let current: Awaited<ReturnType<typeof snapshot>>;
          try {
            current = selection.manual
              ? await normalManualSnapshot(kv, job.sessionId, job.project)
              : await snapshot(kv, job.sessionId, job.project, true);
          } catch (error) {
            if (!selection.manual) throw error;
            return { success: false, error: error instanceof SummaryBudgetError
              ? error.message : "summary_source_grounding_snapshot_unavailable" };
          }
          if (current.fingerprint !== selection.sourceFingerprint) return { success: false, error: "selection_source_mismatch" };
          if (job.status !== "pending") return { success: true, dispatched: 0, completed: job.status === "completed", skipped: true };
          const unit = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), selection.unitId!);
          if (!unit || unit.id !== selection.unitId || unit.jobId !== job.id || unit.round !== job.round || unit.stage !== job.stage) {
            return { success: false, error: "selection_identity_mismatch" };
          }
          const now = Date.now();
          if (unit.startedAt && now - Date.parse(unit.startedAt) < MAX_UNIT_RUNTIME_MS) {
            return { success: true, dispatched: 0, busy: true };
          }
          if (!unit.output && !retryEligible(unit, now)) return { success: true, dispatched: 0, skipped: true };
          const replay = unit.deliveryId && unit.dispatchedAt && (!unit.lastAttemptAt ||
            Date.parse(unit.dispatchedAt) > Date.parse(unit.lastAttemptAt));
          const deliveryId = replay || unit.output ? unit.deliveryId : generateId("sqd");
          if (!replay && !unit.output) {
            await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
              ...unit, deliveryId, dispatchedAt: new Date(now).toISOString(), startedAt: undefined,
            });
          }
          return { success: true, dispatched: replay || unit.output ? 0 : 1,
            continuation: { jobId: job.id, round: job.round, unitId: unit.id, deliveryId },
            ...(replay && { deduplicated: true }) };
        }));
    }
    if (isBackgroundRecoveryPaused()) return { success: true, paused: true, dispatched: 0 };
    if (data.offset !== 0) return { success: true, skipped: true };
    return withKeyedLock(`summary-job:${data.jobId}`, async () => {
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, data.jobId);
      if (job?.executionOrigin) return { success: true, reason: "selected_foreground_required" };
      if (!job || job.status !== "pending" || job.round !== data.round) {
        return { success: true, skipped: true };
      }
      if (!await summaryRecoveryReady(kv, job)) return { success: false, error: "summary_recovery_construction_pending" };
      const units = await loadJobUnits(kv, job);
      const waiting = units.filter((unit): unit is SummaryQueueUnit => Boolean(unit && !unit.output));
      const outstanding = waiting.filter(unit => unit.dispatchedAt && unit.attempts === 0).length;
      const available = Math.max(0, DISPATCH_BATCH - outstanding);
      const now = Date.now();
      const selected = waiting.filter(unit => !unit.dispatchedAt && retryEligible(unit, now)).slice(0, available);
      for (const unit of selected) {
        const deliveryId = generateId("sqd");
        await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
          ...unit, dispatchedAt: new Date().toISOString(), deliveryId,
        });
        try {
          await enqueue(sdk, "mem::summary-unit", { jobId: job.id, unitId: unit.id, deliveryId });
        } catch (error) {
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, unit);
          throw error;
        }
      }
      if (job.dispatchPending) {
        await kv.set(KV.summaryQueueJobs, job.id, { ...job, dispatchPending: false });
      }
      return { success: true, dispatched: selected.length };
    });
  });
  sdk.registerTrigger({
    type: "durable:subscriber",
    function_id: "mem::summary-dispatch",
    config: { topic: DISPATCH_TOPIC },
  });

  const advance = async (jobId: string, foreground = false, selectedGeneration = false, selection?: SelectedSummary): Promise<{
    completed: boolean; nextRound?: number; method?: SummaryMethod;
  }> =>
    withKeyedLock(`summary-job:${jobId}`, async () => {
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
      if (!job || job.status !== "pending") return { completed: false };
      if (!await summaryRecoveryReady(kv, job)) throw new SummaryBudgetError("summary_recovery_construction_pending");
      const applied = await summaryRecoveryStep("advance.publication_effect", { jobId: job.id, sessionId: job.sessionId },
        () => recoverAppliedPublication(job, foreground));
      if (applied) return { completed: true, method: applied };
      const units = await loadJobUnits(kv, job);
      if (units.some(unit => !unit?.output || !unit.summary)) return { completed: false };
      const complete = units as SummaryQueueUnit[];
      const root = await summaryRecoveryStep("advance.evidence_root", { jobId: job.id, sessionId: job.sessionId },
        () => evidenceRoot(job));
      if (root.mode === "hierarchical") {
        for (const unit of complete) {
          if (!await validateUnit(job, unit, foreground, selection)) return { completed: false };
        }
      }
      const outputs = complete.map(unit => unit.output!);
      const continueReduction = async (groups: SummaryPromptItem[][]) => {
        if (job.round >= MAX_SUMMARY_DEPTH) throw new SummaryBudgetError("summary_depth_limit_exceeded");
        const nextRound = job.round + 1;
        const nextUnits = createUnits(job.id, "reduce", nextRound, groups);
        if (root.mode === "hierarchical") {
          const offsets = new Map<string | number, number>();
          for (const unit of nextUnits) await storeLineage(job, unit, complete, offsets);
          if (complete.some(unit => offsets.get(unit.id) !== unit.output!.text.length))
            throw new SummaryBudgetError("summary_input_lineage_mismatch");
        }
        await writeUnits(kv, job.id, nextUnits);
        await kv.set(KV.summaryQueueJobs, job.id, {
          ...job, stage: "reduce", round: nextRound, unitIds: nextUnits.map(unit => unit.id),
          sourceProgressSize: summaryProgressSize(outputs), updatedAt: new Date().toISOString(), dispatchPending: true,
        });
        await deleteUnits(kv, job.id, complete);
        return { completed: false, nextRound };
      };
      const fallback = async () => {
        if (selectedGeneration) throw new SummaryBudgetError("selected_summary_reduction_incomplete");
        const summary = deterministicSummary(job, complete);
        await kv.set(KV.state, `${job.id}:fallback:${job.round}`, summary);
        const sources = root.mode === "complete" ? await verifySummaryEvidence(kv, job, root) : await validatedChildren(job, complete);
        const prepared = preparePublicationEvidence({ workflow: "summary", operationId: `${job.id}:fallback:${job.round}`,
          project: job.project, candidate: JSON.stringify(summaryFields(summary)), sources, evidenceComplete: true });
        if ("status" in prepared) {
          if (job.round >= MAX_SUMMARY_DEPTH) throw new SummaryBudgetError("summary_publication_evidence_limit_exceeded");
          return continueReduction(packSummaryItems(outputs, REDUCE_SYSTEM, buildReduceItemsPrompt,
            Math.min(summaryReduceInputLimit(job.config), 100_000), 128));
        }
        await publish(job, summary, "deterministic_fallback", foreground, selection, complete);
        return { completed: true, method: "deterministic_fallback" as const };
      };
      if (complete.length === 1) {
        const summary = { ...complete[0].summary!, observationCount: job.observationCount };
        await publish(job, summary, "llm", foreground, selection, complete);
        return { completed: true, method: "llm" };
      }
      let groups: SummaryPromptItem[][];
      try {
        groups = packSummaryItems(outputs, REDUCE_SYSTEM, buildReduceItemsPrompt,
          summaryReduceInputLimit(job.config));
      } catch (error) {
        const unableToFit = error instanceof SummaryBudgetError &&
          ["summary_fragment_cannot_fit", "summary_prompt_exceeds_budget"].includes(error.message);
        if (job.stage !== "reduce" || !unableToFit) throw error;
        return fallback();
      }
      const sizeProgress = summaryProgressSize(outputs) < job.sourceProgressSize;
      const unitProgress = groups.length < complete.length;
      if (job.stage === "reduce" && !sizeProgress && !unitProgress) {
        return fallback();
      }
      if (job.round >= MAX_SUMMARY_DEPTH) {
        if (job.stage === "reduce") {
          return fallback();
        }
        throw new SummaryBudgetError("summary_depth_limit_exceeded");
      }
      return continueReduction(groups);
    });

  sdk.registerFunction("mem::summary-unit", async (data: { jobId: string; unitId: string; deliveryId?: string; selected?: unknown; manual?: unknown }) => {
    const foreground = Object.prototype.hasOwnProperty.call(data, "selected") || data.manual !== undefined;
    const supplied = data.manual !== undefined ? manualSelection(data.manual, true) : validSelection(data.selected, true) ? data.selected : undefined;
    if (foreground && (!supplied || data.manual !== undefined && data.selected !== undefined)) return { success: false, error: "invalid_selection" };
    const selection = supplied;
    if (isBackgroundRecoveryPaused() && !foreground) return { success: true, skipped: true, paused: true };
    return withKeyedLock(`summary-unit:${data.unitId}`, async () => {
      const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, data.jobId);
      if (!foreground && job?.executionOrigin) return { success: true, reason: "selected_foreground_required" };
      if (selection) {
        if (job?.status === "pending" && job.executionOrigin !== (selection.manual ? "manual" : "selected")) return { success: false, error: "selection_origin_mismatch" };
        if (!job || selection.jobId !== data.jobId || selection.unitId !== data.unitId ||
            selection.deliveryId !== data.deliveryId || selection.project !== job.project ||
            selection.sessionId !== job.sessionId || selection.sourceFingerprint !== job.snapshotFingerprint ||
            selection.round !== job.round || !job.unitIds.includes(data.unitId)) {
          return { success: false, error: "selection_identity_mismatch" };
        }
      }
      if (!job || job.status !== "pending") return { success: true, skipped: true,
        ...(foreground && job?.status === "completed" && { completed: true }) };
      if (!await summaryRecoveryReady(kv, job)) return { success: false, error: "summary_recovery_construction_pending" };
      const unit = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), data.unitId);
      if (!unit || unit.round !== job.round || unit.stage !== job.stage) {
        if (job.dispatchPending && !foreground) {
          await enqueue(sdk, "mem::summary-dispatch", { jobId: job.id, round: job.round, offset: 0 });
        }
        return { success: true, skipped: true };
      }
      if (selection && (selection.deliveryId !== unit.deliveryId || unit.id !== data.unitId || unit.jobId !== job.id)) {
        return { success: false, error: "selection_identity_mismatch" };
      }
      if (unit.deliveryId && (data.deliveryId !== unit.deliveryId || !unit.dispatchedAt)) {
        return { success: true, skipped: true };
      }
      const applied = await withKeyedLock(`summary-job:${job.id}`, async () => {
        const latest = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
        if (latest?.status === "completed") return { completed: true };
        if (latest?.status !== "pending") return undefined;
        const method = await recoverAppliedPublication(latest, foreground);
        return method ? { completed: true, method } : undefined;
      });
      if (applied) return { success: true, ...applied };
      if (selection) {
        const session = await kv.get<Session>(KV.sessions, job.sessionId);
        if (!session || session.project !== selection.project)
          return { success: false, error: "selection_source_mismatch" };
        let current: Awaited<ReturnType<typeof snapshot>>;
        try {
          current = selection.manual
            ? await normalManualSnapshot(kv, job.sessionId, job.project)
            : await snapshot(kv, job.sessionId, job.project, true);
        } catch (error) {
          if (!selection.manual) throw error;
          return { success: false, error: error instanceof SummaryBudgetError
            ? error.message : "summary_source_grounding_snapshot_unavailable" };
        }
        if (current.fingerprint !== selection.sourceFingerprint)
          return { success: false, error: "selection_source_mismatch" };
      }
      if (foreground && unit.startedAt && Date.now() - Date.parse(unit.startedAt) < MAX_UNIT_RUNTIME_MS) {
        return { success: true, skipped: true, busy: true };
      }
      if (!unit.output && !retryEligible(unit, Date.now())) {
        return { success: true, skipped: true };
      }
      const requestInputHash = inputHash(unit);
      let pathAuthority: Awaited<ReturnType<typeof resolveSummaryUnitPathAuthority>> | undefined;
      let recoveryMaterial: FullOriginalRecovery | undefined;
      if (!unit.output) {
        try {
          if (job.recoveryMode === "full-originals-v1") {
            recoveryMaterial = await fullOriginalRecovery(job);
            pathAuthority = { sourcePaths: recoveryMaterial.sourcePaths, modifiedPaths: [] };
          } else pathAuthority = await resolveSummaryUnitPathAuthority(kv, job, unit);
        } catch (error) {
          const attempts = unit.attempts + 1;
          const reason = failureCode(error);
          await recordOperation(kv, "mem::summary-unit", "failed", {}, undefined,
            { errorCode: "summary_failed", executionId: `${job.id}:${unit.id}`, executionLevel: "unit" });
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
            ...unit, attempts, startedAt: undefined,
            lastError: reason, lastAttemptAt: new Date().toISOString(),
          });
          if (attempts >= MAX_ATTEMPTS || error instanceof SummaryBudgetError) {
            await failJob(kv, job.id, reason);
            return { success: false, error: reason, terminal: true };
          }
          if (foreground) return { success: false, completed: false, error: reason };
          await sdk.trigger({ function_id: "mem::summary-dispatch",
            payload: { jobId: job.id, round: job.round, offset: 0 } });
          throw error;
        }
      }
      const captureKey = `${job.id}:raw:${unit.id}`;
      let generatedThisDelivery = false;
      let capture = await kv.get<{ state: "intended" | "captured"; inputHash?: string; raw?: string; overflow?: true }>(KV.state, captureKey);
      if (capture && capture.inputHash !== requestInputHash) {
        await kv.delete(KV.state, captureKey);
        capture = undefined;
      }
      if (!unit.output && capture?.state === "intended") return { success: false, completed: false,
        error: "summary_dispatch_outcome_unknown" };
      if (foreground && unit.output && (!unit.summary || unit.summary.sessionId !== job.sessionId ||
          unit.summary.project !== job.project)) return { success: false, error: "selection_capture_mismatch" };
      if (!unit.output) {
        await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
          ...unit, startedAt: new Date().toISOString(),
        });
        let system = unit.stage === "map" ? SUMMARY_SYSTEM : REDUCE_SYSTEM;
        let prompt = unit.stage === "map" ? buildSummaryItemsPrompt(unit.items) : buildReduceItemsPrompt(unit.items);
        activeUnits.set(unit.id, Date.now());
        try {
          recoveryMaterial = job.recoveryMode === "full-originals-v1" ? await fullOriginalRecovery(job) : undefined;
          system = recoveryMaterial?.systemPrompt ?? system;
          prompt = recoveryMaterial?.userPrompt ?? prompt;
          const inputTokens = estimateSummaryTokens(system, prompt);
          if (!recoveryMaterial && inputTokens > summaryCallInputLimit(job.config))
            throw new SummaryBudgetError("summary_prompt_exceeds_budget");
          const outputTokens = recoveryMaterial?.outputTokens ?? summaryOutputTokenBudget(job.config, system, prompt);
          const call = async (selected: MemoryProvider) => {
            generatedThisDelivery = true;
            const raw = await selected.summarize(system, prompt, { task: "summary", outputTokens });
            const captured: typeof capture = { state: "captured", inputHash: requestInputHash,
              ...(Buffer.byteLength(raw, "utf8") > 65_536 ? { overflow: true as const } : { raw }) };
            await withKeyedLock(`summary-job:${job.id}`, async () => {
              const [latest, currentUnit, previous] = await Promise.all([
                kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id),
                kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), unit.id),
                kv.get<typeof capture>(KV.state, captureKey),
              ]);
              if (!latest || latest.sessionId !== job.sessionId || latest.project !== job.project ||
                  latest.snapshotFingerprint !== job.snapshotFingerprint || !latest.unitIds.includes(unit.id) ||
                  !currentUnit || currentUnit.id !== unit.id || currentUnit.jobId !== job.id ||
                  currentUnit.round !== unit.round || currentUnit.stage !== unit.stage)
                throw new SummaryBudgetError("summary_capture_identity_mismatch");
              if (previous?.state === "captured") {
                if (previous.inputHash !== captured.inputHash || previous.raw !== captured.raw ||
                    previous.overflow !== captured.overflow)
                  throw new SummaryBudgetError("summary_capture_identity_mismatch");
                capture = previous;
                return;
              }
              if (previous?.state !== "intended")
                throw new SummaryBudgetError("summary_capture_identity_mismatch");
              capture = captured;
              await kv.set(KV.state, captureKey, capture);
            });
            return raw;
          };
          const execute = () => withSummaryCallTimeout(
            () => llmRouter ? llmRouter.run("summary", call, () => true) : call(provider),
            SUMMARY_CALL_TIMEOUT_MS,
          );
          if (!capture) await kv.set(KV.state, captureKey, { state: "intended", inputHash: requestInputHash });
          if (capture?.overflow) throw new SummaryBudgetError("summary_response_limit_exceeded");
          const response = capture?.raw ?? await runWorkflowAttempt(accounting, kv,
            { workflow: "summary", functionId: "mem::summary-unit", project: job.project,
              sourceId: job.sessionId, sourceFingerprint: job.snapshotFingerprint, jobId: job.id,
              unitId: unit.id, phase: "generation", continuation: JSON.stringify([unit.round, unit.stage]) }, execute);
          if (capture?.overflow) throw new SummaryBudgetError("summary_response_limit_exceeded");
          let summary = parseSummaryXml(response, job.sessionId, job.project, job.observationCount);
          if (!summary) throw new SummaryBudgetError("summary_parse_failed");
          if (!pathAuthority) throw new SummaryBudgetError("summary_source_grounding_snapshot_unavailable");
          summary = groundSummarySourcePaths(summary, pathAuthority.sourcePaths, pathAuthority.modifiedPaths);
          if (job.unitIds.length === 1) validateSummary(summary);
          const admitted = await withKeyedLock(`summary-job:${job.id}`, async () => {
            const [latest, currentUnit] = await Promise.all([
              kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id),
              kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), unit.id),
            ]);
            if (!latest || latest.status !== "pending" || latest.round !== unit.round || latest.stage !== unit.stage ||
                !latest.unitIds.includes(unit.id) || !currentUnit || currentUnit.jobId !== job.id ||
                currentUnit.round !== unit.round || currentUnit.stage !== unit.stage ||
                inputHash(currentUnit) !== requestInputHash) return false;
            if (currentUnit.output || currentUnit.summary) {
              if (currentUnit.summary && JSON.stringify(summaryFields(currentUnit.summary)) === JSON.stringify(summaryFields(summary)) &&
                  JSON.stringify(currentUnit.output) === JSON.stringify(unitOutput(summary, unit.items)) &&
                  currentUnit.outputInputHash === requestInputHash) return true;
              throw new SummaryBudgetError("summary_capture_identity_mismatch");
            }
            await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
              ...currentUnit, summary, output: unitOutput(summary, unit.items), outputInputHash: requestInputHash,
              startedAt: undefined, lastError: undefined,
            });
            return true;
          });
          if (!admitted) return { success: true, skipped: true };
          await recordOperation(kv, "mem::summary-unit", "completed", { unitsCompleted: 1 }, undefined,
            { executionId: `${job.id}:${unit.id}`, executionLevel: "unit" });
        } catch (error) {
          const attempts = unit.attempts + 1;
          const reason = failureCode(error);
          await recordOperation(kv, "mem::summary-unit", "failed", {}, undefined,
            { errorCode: "summary_failed", executionId: `${job.id}:${unit.id}`, executionLevel: "unit" });
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
            ...unit, attempts, startedAt: undefined,
            lastError: reason, lastAttemptAt: new Date().toISOString(),
          });
          if (attempts >= MAX_ATTEMPTS || error instanceof SummaryBudgetError) {
            await failJob(kv, job.id, reason);
            return { success: false, error: reason, terminal: true };
          }
          if (foreground) return { success: false, completed: false, error: reason };
          await sdk.trigger({ function_id: "mem::summary-dispatch",
            payload: { jobId: job.id, round: job.round, offset: 0 } });
          throw error;
        } finally {
          activeUnits.delete(unit.id);
        }
      }
      try {
        const cached = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(job.id), unit.id);
        const root = await evidenceRoot(job);
        if (root.mode === "hierarchical" && cached?.summary && cached.output) await validateUnit(job, cached, foreground, selection);
        const result = await advance(job.id, foreground, !selection?.manual && foreground && generatedThisDelivery, selection);
        if (foreground) {
          const latest = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, job.id);
          return { success: true, completed: result.completed, ...(result.method && { method: result.method }),
            ...(!result.completed && latest && { continuation: {
              jobId: latest.id, round: latest.round, unitIds: latest.unitIds,
            } }) };
        }
        if (result.nextRound !== undefined) {
          await enqueue(sdk, "mem::summary-dispatch", { jobId: job.id, round: result.nextRound, offset: 0 });
        } else if (!result.completed) {
          await sdk.trigger({ function_id: "mem::summary-dispatch",
            payload: { jobId: job.id, round: job.round, offset: 0 } });
        }
        if (result.completed) {
          await sdk.trigger({ function_id: "mem::summary-enqueue", payload: { sessionId: job.sessionId } });
        }
        return { success: true, completed: result.completed, ...(result.method && { method: result.method }) };
      } catch (error) {
        if (error instanceof ManualCutComplete) return { success: true, completed: false };
        if (error instanceof SummaryBudgetError) {
          await failJob(kv, job.id, error.message);
          return { success: false, error: error.message, terminal: true };
        }
        throw error;
      }
    });
  });
  sdk.registerTrigger({
    type: "durable:subscriber",
    function_id: "mem::summary-unit",
    config: {
      topic: UNIT_TOPIC,
      queue_config: {
        type: "concurrent", concurrency: 12, maxRetries: 5, backoffDelayMs: RETRY_BACKOFF_MS,
      },
    },
  });

  const reconcilePendingJobs = async (): Promise<{ success: true; recovered: number; replayed: number; paused?: true }> => {
    if (isBackgroundRecoveryPaused()) return { success: true, recovered: 0, replayed: 0, paused: true };
    let idle = false;
    try {
      const queue = await sdk.trigger<unknown, { depth?: number; dlq_depth?: number }>({
        function_id: "engine::queue::topic_stats", payload: { topic: UNIT_TOPIC }, timeoutMs: 5000,
      });
      idle = queue.depth === 0 && queue.dlq_depth === 0;
    } catch {
      // New work can still be dispatched; only stale-delivery replay needs an idle signal.
    }

    let recovered = 0;
    let replayed = 0;
    for (const candidate of await kv.list<SummaryQueueJob>(KV.summaryQueueJobs)) {
      if (candidate.status !== "pending" || candidate.executionOrigin || !await summaryRecoveryReady(kv, candidate)) continue;
      await withKeyedLock(`summary-job:${candidate.id}`, async () => {
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, candidate.id);
        if (!job || job.status !== "pending" || job.executionOrigin) return;
        const now = Date.now();
        for (const unit of await loadJobUnits(kv, job)) {
          if (unit && !unit.output && unit.attempts > 0 && !unit.lastAttemptAt) {
            const markedAt = new Date(now).toISOString();
            await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
              ...unit, lastAttemptAt: markedAt, dispatchedAt: unit.dispatchedAt ?? markedAt,
            });
            continue;
          }
          const activeSince = unit && activeUnits.get(unit.id);
          if (!unit || unit.output || !unit.dispatchedAt ||
              (activeSince !== undefined && now - activeSince < MAX_UNIT_RUNTIME_MS) ||
              (unit.attempts === 0 && !idle &&
                now - Date.parse(unit.dispatchedAt) < MAX_UNIT_RUNTIME_MS) ||
              (unit.attempts === 0 && now - Date.parse(unit.dispatchedAt) < STALE_DELIVERY_MS) ||
              unit.startedAt && now - Date.parse(unit.startedAt) < MAX_UNIT_RUNTIME_MS ||
              !retryEligible(unit, now)) continue;
          await kv.set(KV.summaryQueueUnits(job.id), unit.id, {
            ...unit, dispatchedAt: undefined, startedAt: undefined,
            deliveryId: generateId("sqd"),
          });
          replayed++;
        }
      });
      try {
        const progress = await summaryRecoveryStep("reconcile.advance", { jobId: candidate.id, sessionId: candidate.sessionId },
          () => advance(candidate.id));
        if (progress.completed) {
          await summaryRecoveryStep("reconcile.completed_enqueue", { jobId: candidate.id, sessionId: candidate.sessionId },
            () => sdk.trigger({ function_id: "mem::summary-enqueue", payload: { sessionId: candidate.sessionId } }));
          recovered++;
          continue;
        }
        const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, candidate.id);
        if (!job || job.status !== "pending") continue;
        const result = await sdk.trigger<unknown, { dispatched?: number }>({
          function_id: "mem::summary-dispatch",
          payload: { jobId: job.id, round: job.round, offset: 0 },
        });
        if (progress.nextRound !== undefined || result.dispatched) recovered++;
      } catch (error) {
        if (!(error instanceof SummaryBudgetError)) throw error;
        await failJob(kv, candidate.id, error.message);
      }
    }
    if (replayed > 0) logger.warn("Summary queue replayed undelivered units", { replayed });
    return { success: true, recovered, replayed };
  };
  sdk.registerFunction("mem::summary-reconcile", reconcilePendingJobs);

  sdk.registerFunction("mem::summary-recover", async () => {
    if (isBackgroundRecoveryPaused()) return { success: true, recovered: 0, cleaned: 0, paused: true };
    let { recovered } = await summaryRecoveryStep("recover.reconcile", {}, reconcilePendingJobs);
    let cleaned = 0;
    const jobs = await summaryRecoveryStep("recover.jobs", {}, () => kv.list<SummaryQueueJob>(KV.summaryQueueJobs));
    const coveredSessions = new Set<string>();
    for (const job of jobs) {
      if (job.executionOrigin) { coveredSessions.add(job.sessionId); continue; }
      if (job.status === "pending") {
        coveredSessions.add(job.sessionId);
      } else if (job.status === "completed") {
        await summaryRecoveryStep("recover.completed_enqueue", { jobId: job.id, sessionId: job.sessionId },
          () => sdk.trigger({ function_id: "mem::summary-enqueue", payload: { sessionId: job.sessionId } }));
        await summaryRecoveryStep("recover.cleanup_units", { jobId: job.id, sessionId: job.sessionId }, () => cleanupUnits(kv, job.id));
        await kv.delete(KV.summaryQueueJobs, job.id);
        cleaned++;
      } else if (eligibleSummaryPromptPredecessor(job) && await correctUnsupportedSummary(job)) {
        coveredSessions.add(job.sessionId);
        recovered++;
      } else if (job.failedAt && Date.now() - Date.parse(job.failedAt) >= FAILED_RETENTION_MS) {
        await summaryRecoveryStep("recover.cleanup_units", { jobId: job.id, sessionId: job.sessionId }, () => cleanupUnits(kv, job.id));
        await kv.delete(KV.summaryQueueJobs, job.id);
        const active = await kv.get<ActiveJob>(KV.summaryQueueActive, job.sessionId);
        const intent = await kv.get<SummaryQueueIntent>(KV.summaryQueueIntents, job.sessionId);
        const newerJob = jobs.some(candidate => candidate.sessionId === job.sessionId &&
          candidate.id !== job.id && candidate.createdAt > job.createdAt);
        if (!active && !newerJob && intent && intent.createdAt <= job.failedAt) {
          await kv.delete(KV.summaryQueueIntents, job.sessionId);
        }
        cleaned++;
      } else {
        coveredSessions.add(job.sessionId);
      }
    }
    const intents = await summaryRecoveryStep("recover.intents", {}, () => kv.list<SummaryQueueIntent>(KV.summaryQueueIntents));
    const uncovered = intents.filter(intent => !intent.executionOrigin && !coveredSessions.has(intent.sessionId))
      .sort((a, b) => (a.lastAttemptAt ?? a.createdAt).localeCompare(b.lastAttemptAt ?? b.createdAt))
      .slice(0, MAX_RECOVERY_INTENTS);
    for (const intent of uncovered) {
      const session = await summaryRecoveryStep("recover.intent_session", { sessionId: intent.sessionId },
        () => kv.get<Session>(KV.sessions, intent.sessionId));
      if (session?.status !== "completed") continue;
      const result = await summaryRecoveryStep("recover.intent_enqueue", { sessionId: intent.sessionId },
        () => sdk.trigger<{ sessionId: string }, { success: boolean; queued?: boolean; skipped?: string; retained?: boolean }>({
          function_id: "mem::summary-enqueue", payload: { sessionId: intent.sessionId },
        }));
      if (!result.success && !result.retained) throw new Error("summary_recovery_enqueue_failed");
      if (result.skipped === "no_provider") {
        await kv.set(KV.summaryQueueIntents, intent.sessionId, {
          ...intent, lastAttemptAt: new Date().toISOString(),
        });
      }
      if (result.queued) recovered++;
    }
    return { success: true, recovered, cleaned };
  });
}
