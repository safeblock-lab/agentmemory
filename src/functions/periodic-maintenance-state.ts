import type {
  NativeCompactionApply,
  NativeCompactionStatus,
  NativeCompactionStep,
} from "../state/native-compaction.js";
import { validateNativeCompactionRequest } from "../state/native-compaction.js";
import type { StorageGrowthSample, WorkflowTerminalReceipt } from "../types.js";
import type { StorageReclaimResult } from "../state/storage.js";

export const PERIODIC_GROWTH_THRESHOLD_BYTES = 1_000_000_000;
export const PERIODIC_COMPACTION_BATCH = { limit: 128, max_bytes: 1_048_576, max_ms: 2500 } as const;
export const PERIODIC_COPY_LIMIT = 1024;
export const PERIODIC_COLLECTION_LIMIT = 4096;
export const PERIODIC_ACCEPTANCE = {
  baselineBytes: 5_711_050_488,
  minimumCombinedNetBytes: 285_552_525,
} as const;

export interface ComparableFootprint {
  sampledAt: string;
  categoryVersion: number;
  coverage: string;
  databaseBytes: number;
  walBytes: number;
  observedBytes: number;
}

export interface PeriodicCompactionProgress {
  workflowTerminal?: WorkflowTerminalReceipt;
  cycle: PeriodicCompactionCycle | null;
  cycleRaw: unknown | null;
  growthBaseline: ComparableFootprint | null;
  growthBaselineRaw: unknown | null;
  pendingRequest: NativeCompactionApply | null;
  pendingRequestRaw: unknown | null;
  recoveryRequired: boolean;
  lastStatus: NativeCompactionStatus | null;
  lastAppliedRequest: NativeCompactionApply | null;
  lastApplyResult: NativeCompactionStatus | null;
  lastOperationId: string | null;
  lastDisposition: string | null;
  lastErrorCode: string | null;
  lastNotApplied: PeriodicCompactionNotApplied | null;
  lastNotAppliedRaw: unknown | null;
}

export interface PeriodicCompactionNotApplied {
  request: NativeCompactionApply;
  receiptStatus: "not_found";
  receiptOperationId: string;
  observedGeneration: string;
  observedPhase: NativeCompactionStatus["phase"];
  verifiedAt: string;
  currentLease: PeriodicMaintenanceLease;
}

export interface PeriodicCompactionCycle {
  startedAt: string;
  layoutComplete: boolean;
}

export interface PeriodicExecutionAdmission {
  baselineBytes: number;
  minimumNetBytes: number;
  aggregateCandidateAccepted: boolean;
  executionAuthorized: boolean;
  conservativeCombinedNetBytes: number;
  runtimeCompatible: boolean;
}

export interface PeriodicMaintenanceLease {
  ownerId: string;
  fence: number;
}

export type PeriodicCompactionDisposition =
  | "baseline_established"
  | "growth_below_threshold"
  | "sample_incomparable"
  | "admission_pending"
  | "runtime_compatibility_pending"
  | "cancelled"
  | "batch_applied"
  | "receipt_reconciled"
  | "pending_receipt_not_found"
  | "pending_fence_stale"
  | "pending_generation_changed"
  | "manual_reconciliation_required"
  | "previous_request_not_applied"
  | "terminal_evidence_pending"
  | "candidate_terminal";

export interface PeriodicCompactionResult {
  disposition: PeriodicCompactionDisposition;
  growthBytes: number | null;
  operationId: string | null;
  status: NativeCompactionStatus | null;
}

export interface RuntimePromotionEvidence {
  acceptedByMain: boolean;
  runtimeFormat: 3;
  allSixFrontsTerminal: boolean;
  measuredCombinedNetBytes: number;
  rollbackCopiesAccounted: boolean;
  preservesFloat32: boolean;
  preservesOrder: boolean;
  preservesProvenance: boolean;
  preservesReplay: boolean;
  preservesMonotonicSpend: boolean;
}

const STEPS: NativeCompactionStep[] = ["activate", "copy", "cut", "rollback", "collect"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function count(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function counter(value: unknown): value is string {
  return typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value)
    && BigInt(value) <= 18_446_744_073_709_551_615n;
}

function compactionStep(value: unknown): NativeCompactionStep | null {
  return STEPS.find((step) => step === value) ?? null;
}

function compactionPhase(value: unknown): NativeCompactionStatus["phase"] | null {
  if (value === "legacy" || value === "copy" || value === "compact" || value === "rollback") return value;
  return null;
}

function parseFootprint(value: unknown): ComparableFootprint | null {
  if (!isRecord(value) || typeof value.sampledAt !== "string" || !Number.isFinite(Date.parse(value.sampledAt))
    || !count(value.categoryVersion) || typeof value.coverage !== "string" || !value.coverage
    || !count(value.databaseBytes) || !count(value.walBytes) || !count(value.observedBytes)
    || value.observedBytes !== value.databaseBytes + value.walBytes) return null;
  return {
    sampledAt: value.sampledAt,
    categoryVersion: value.categoryVersion,
    coverage: value.coverage,
    databaseBytes: value.databaseBytes,
    walBytes: value.walBytes,
    observedBytes: value.observedBytes,
  };
}

function parseCycle(value: unknown): PeriodicCompactionCycle | null {
  if (!isRecord(value) || typeof value.startedAt !== "string"
    || !Number.isFinite(Date.parse(value.startedAt)) || typeof value.layoutComplete !== "boolean") return null;
  return { startedAt: value.startedAt, layoutComplete: value.layoutComplete };
}

export function measuredFootprint(sample: StorageGrowthSample): ComparableFootprint | null {
  const physical = sample.physical;
  if (!Number.isSafeInteger(sample.categoryVersion) || sample.categoryVersion < 0
    || !sample.coverage || !Number.isFinite(Date.parse(sample.sampledAt))
    || !count(physical.database_bytes) || !count(physical.wal_bytes)) return null;
  const observedBytes = physical.database_bytes + physical.wal_bytes;
  if (!Number.isSafeInteger(observedBytes)) return null;
  return {
    sampledAt: sample.sampledAt,
    categoryVersion: sample.categoryVersion,
    coverage: sample.coverage,
    databaseBytes: physical.database_bytes,
    walBytes: physical.wal_bytes,
    observedBytes,
  };
}

function parseApplyRequest(value: unknown): NativeCompactionApply | null {
  if (!isRecord(value) || typeof value.operation_id !== "string"
    || typeof value.expected_generation !== "string" || !count(value.limit) || !count(value.max_bytes)
    || !count(value.max_ms) || typeof value.lease_owner_id !== "string" || !count(value.lease_fence)) return null;
  const step = compactionStep(value.step);
  if (!step) return null;
  const request: NativeCompactionApply = {
    operation_id: value.operation_id,
    expected_generation: value.expected_generation,
    step,
    limit: value.limit,
    max_bytes: value.max_bytes,
    max_ms: value.max_ms,
    lease_owner_id: value.lease_owner_id,
    lease_fence: value.lease_fence,
  };
  try {
    validateNativeCompactionRequest({ action: "compact_apply", request });
    return request;
  } catch {
    return null;
  }
}

function parseStatus(value: unknown): NativeCompactionStatus | null {
  const phase = isRecord(value) ? compactionPhase(value.phase) : null;
  if (!isRecord(value) || value.format !== 3 || !counter(value.generation)
    || typeof value.active !== "boolean" || !phase
    || !isRecord(value.cursor) || typeof value.cursor.scope !== "string" || typeof value.cursor.key !== "string"
    || typeof value.rows_complete !== "boolean" || typeof value.vectors_complete !== "boolean"
    || typeof value.complete !== "boolean" || value.physical_bytes_reclaimed !== null
    || (value.gc_complete !== undefined && typeof value.gc_complete !== "boolean")
    || (value.retained_inline_present !== undefined && typeof value.retained_inline_present !== "boolean")) return null;
  return {
    format: 3,
    generation: value.generation,
    active: value.active,
    phase,
    cursor: { scope: value.cursor.scope, key: value.cursor.key },
    rows_complete: value.rows_complete,
    vectors_complete: value.vectors_complete,
    complete: value.complete,
    physical_bytes_reclaimed: null,
    ...(typeof value.operation_id === "string" ? { operation_id: value.operation_id } : {}),
    ...(count(value.processed_count) ? { processed_count: value.processed_count } : {}),
    ...(count(value.input_bytes) ? { input_bytes: value.input_bytes } : {}),
    ...(typeof value.gc_complete === "boolean" ? { gc_complete: value.gc_complete } : {}),
    ...(typeof value.retained_inline_present === "boolean" ? { retained_inline_present: value.retained_inline_present } : {}),
  };
}

function parseNotApplied(value: unknown): PeriodicCompactionNotApplied | null {
  if (!isRecord(value)) return null;
  const request = parseApplyRequest(value.request);
  const phase = compactionPhase(value.observedPhase);
  const lease = isRecord(value.currentLease) ? value.currentLease : null;
  if (!request || request.step === "rollback" || !phase || phase === "rollback"
    || value.receiptStatus !== "not_found" || value.receiptOperationId !== request.operation_id
    || value.observedGeneration !== request.expected_generation
    || typeof value.verifiedAt !== "string" || !Number.isFinite(Date.parse(value.verifiedAt))
    || !lease || typeof lease.ownerId !== "string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(lease.ownerId)
    || lease.ownerId === request.lease_owner_id || !count(lease.fence) || lease.fence <= request.lease_fence) return null;
  return { request, receiptStatus: "not_found", receiptOperationId: request.operation_id,
    observedGeneration: request.expected_generation, observedPhase: phase, verifiedAt: value.verifiedAt,
    currentLease: { ownerId: lease.ownerId, fence: lease.fence } };
}

export function emptyPeriodicCompactionProgress(): PeriodicCompactionProgress {
  return {
    cycle: null, cycleRaw: null, growthBaseline: null, growthBaselineRaw: null,
    pendingRequest: null, pendingRequestRaw: null, recoveryRequired: false,
    lastStatus: null, lastAppliedRequest: null, lastApplyResult: null,
    lastOperationId: null, lastDisposition: null, lastErrorCode: null,
    lastNotApplied: null, lastNotAppliedRaw: null,
  };
}

export function normalizePeriodicCompactionProgress(value: unknown): PeriodicCompactionProgress {
  const defaults = emptyPeriodicCompactionProgress();
  const row = isRecord(value) ? value : {};
  const rawBaseline = row.growthBaseline;
  const growthBaseline = rawBaseline == null ? null : parseFootprint(rawBaseline);
  const invalidBaseline = rawBaseline != null && growthBaseline === null;
  const rawCycle = row.cycle;
  const cycle = rawCycle == null ? null : parseCycle(rawCycle);
  const invalidCycle = rawCycle != null && cycle === null;
  const rawPending = row.pendingRequest;
  const pendingRequest = rawPending == null ? null : parseApplyRequest(rawPending);
  const invalidPending = rawPending != null && pendingRequest === null;
  const rawNotApplied = row.lastNotApplied;
  const lastNotApplied = rawNotApplied == null ? null : parseNotApplied(rawNotApplied);
  const invalidNotApplied = rawNotApplied != null && lastNotApplied === null;
  const lastAppliedRequest = parseApplyRequest(row.lastAppliedRequest);
  const parsedApplyResult = parseStatus(row.lastApplyResult);
  const validApplyResult = lastAppliedRequest && parsedApplyResult
    && parsedApplyResult.operation_id === lastAppliedRequest.operation_id
    && count(parsedApplyResult.processed_count) && parsedApplyResult.processed_count <= lastAppliedRequest.limit * 6
    && count(parsedApplyResult.input_bytes) && parsedApplyResult.input_bytes <= lastAppliedRequest.max_bytes
    ? parsedApplyResult : null;
  const receipt = isRecord(row.workflowTerminal) ? row.workflowTerminal : null;
  const workflowTerminal: WorkflowTerminalReceipt | undefined = receipt
    && typeof receipt.executionId === "string" && typeof receipt.agentId === "string"
    && typeof receipt.occurredAt === "string" && Number.isFinite(Date.parse(receipt.occurredAt))
    && (receipt.outcome === "completed" || receipt.outcome === "failed" || receipt.outcome === "partial")
    ? { executionId: receipt.executionId, agentId: receipt.agentId, occurredAt: receipt.occurredAt, outcome: receipt.outcome } : undefined;
  return {
    ...(workflowTerminal ? { workflowTerminal } : {}),
    cycle,
    cycleRaw: invalidCycle ? rawCycle : row.cycleRaw ?? null,
    growthBaseline,
    growthBaselineRaw: invalidBaseline ? rawBaseline : row.growthBaselineRaw ?? null,
    pendingRequest,
    pendingRequestRaw: invalidPending ? rawPending : row.pendingRequestRaw ?? null,
    recoveryRequired: invalidBaseline || invalidPending || invalidCycle || invalidNotApplied || row.recoveryRequired === true
      || row.cycleRaw != null || row.growthBaselineRaw != null || row.pendingRequestRaw != null || row.lastNotAppliedRaw != null,
    lastNotApplied,
    lastNotAppliedRaw: invalidNotApplied ? rawNotApplied : row.lastNotAppliedRaw ?? null,
    lastStatus: parseStatus(row.lastStatus),
    lastAppliedRequest,
    lastApplyResult: validApplyResult,
    lastOperationId: typeof row.lastOperationId === "string" ? row.lastOperationId : defaults.lastOperationId,
    lastDisposition: typeof row.lastDisposition === "string" ? row.lastDisposition : defaults.lastDisposition,
    lastErrorCode: typeof row.lastErrorCode === "string" && /^STATE_[A-Z0-9_]{1,80}$/.test(row.lastErrorCode)
      ? row.lastErrorCode : defaults.lastErrorCode,
  };
}

export function runtimePromotionReady(evidence: RuntimePromotionEvidence): boolean {
  return evidence.acceptedByMain && evidence.runtimeFormat === 3 && evidence.allSixFrontsTerminal
    && count(evidence.measuredCombinedNetBytes)
    && evidence.measuredCombinedNetBytes >= PERIODIC_ACCEPTANCE.minimumCombinedNetBytes
    && evidence.rollbackCopiesAccounted && evidence.preservesFloat32 && evidence.preservesOrder
    && evidence.preservesProvenance && evidence.preservesReplay && evidence.preservesMonotonicSpend;
}

export function resetGrowthBaselineAfterPromotion(
  progress: PeriodicCompactionProgress,
  sample: StorageGrowthSample,
  evidence: RuntimePromotionEvidence,
): boolean {
  const baseline = measuredFootprint(sample);
  const status = progress.lastStatus;
  const request = progress.lastAppliedRequest;
  const applied = progress.lastApplyResult;
  const terminal = status !== null && request !== null && applied !== null
    && status.phase === "compact" && status.gc_complete === true && request.step === "collect"
    && applied.operation_id === request.operation_id && applied.generation === status.generation
    && applied.gc_complete === true && applied.retained_inline_present === false;
  if (!baseline || !terminal || progress.pendingRequest || progress.recoveryRequired
    || !runtimePromotionReady(evidence)) return false;
  progress.growthBaseline = baseline;
  progress.growthBaselineRaw = null;
  progress.lastDisposition = "baseline_reset_after_accepted_promotion";
  return true;
}

export function resetGrowthBaselineAfterCompleteCycle(
  progress: PeriodicCompactionProgress,
  sample: StorageGrowthSample,
  reclaim: StorageReclaimResult,
): boolean {
  const baseline = measuredFootprint(sample);
  const previous = progress.growthBaseline;
  const status = progress.lastStatus;
  const request = progress.lastAppliedRequest;
  const applied = progress.lastApplyResult;
  const nativeComplete = status !== null && request !== null && applied !== null
    && status.phase === "compact" && status.complete && status.rows_complete && status.vectors_complete
    && status.gc_complete === true && request.step === "collect"
    && applied.operation_id === request.operation_id && applied.generation === status.generation
    && applied.complete && applied.rows_complete && applied.vectors_complete
    && applied.gc_complete === true && applied.retained_inline_present === false;
  const reclaimComplete = completeMaintenanceReclaim(reclaim);
  if (!baseline || !previous || !progress.cycle?.layoutComplete || !nativeComplete || !reclaimComplete
    || progress.pendingRequest || progress.pendingRequestRaw !== null || progress.recoveryRequired
    || baseline.categoryVersion !== previous.categoryVersion || baseline.coverage !== previous.coverage
    || Date.parse(baseline.sampledAt) <= Date.parse(previous.sampledAt)) return false;
  progress.growthBaseline = baseline;
  progress.growthBaselineRaw = null;
  progress.cycle = null;
  progress.cycleRaw = null;
  progress.lastDisposition = "automatic_cycle_complete";
  return true;
}

export function completeMaintenanceReclaim(reclaim: StorageReclaimResult): boolean {
  const checkpointComplete = reclaim.checkpoint_performed
    ? reclaim.checkpointed_frames === reclaim.wal_frames
    : reclaim.wal_frames === 0 && reclaim.checkpointed_frames === 0;
  return reclaim.maintenance === true && reclaim.max_chunk_pages === 128
    && reclaim.elapsed_ms <= 3_000 && reclaim.pages_reclaimed <= 4_096
    && reclaim.status === "complete" && !reclaim.checkpoint_busy
    && !["foreground_pressure", "checkpoint_busy", "reclaim_in_flight", "time_budget", "cooldown"].includes(reclaim.reason)
    && reclaim.after.freelist_count === 0 && reclaim.after.wal_bytes === 0
    && checkpointComplete;
}
