import type { StateKV } from "../state/kv.js";
import { getAgentId } from "../config.js";
import { observeWorkflowTerminal, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import type { IIIClient } from "iii-sdk";
import { generateId } from "../state/schema.js";
import { triggerStateStorage } from "../state/storage.js";
import type {
  NativeCompactionApply,
  NativeCompactionReceiptResult,
  NativeCompactionStatus,
  NativeCompactionStep,
} from "../state/native-compaction.js";
import type { StorageGrowthSample } from "../types.js";
import {
  PERIODIC_ACCEPTANCE,
  PERIODIC_COMPACTION_BATCH,
  PERIODIC_COPY_LIMIT,
  PERIODIC_COLLECTION_LIMIT,
  PERIODIC_GROWTH_THRESHOLD_BYTES,
  measuredFootprint,
  type PeriodicCompactionDisposition,
  type PeriodicCompactionProgress,
  type PeriodicCompactionResult,
  type PeriodicExecutionAdmission,
  type PeriodicMaintenanceLease,
} from "./periodic-maintenance-state.js";

export interface PeriodicCompactionInput {
  kv: StateKV;
  sdk: IIIClient;
  progress: PeriodicCompactionProgress;
  layoutComplete: boolean;
  sample: StorageGrowthSample;
  admission: PeriodicExecutionAdmission;
  lease: PeriodicMaintenanceLease;
  refreshSameLease: () => Promise<void>;
  persistProgress: () => Promise<void>;
  isCancelled: () => boolean;
}

export function preservePeriodicLease(disposition: PeriodicCompactionDisposition): boolean {
  return disposition === "pending_receipt_not_found" || disposition === "pending_fence_stale"
    || disposition === "pending_generation_changed" || disposition === "manual_reconciliation_required";
}

function count(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

export function isPeriodicAdmissionReady(admission: PeriodicExecutionAdmission): boolean {
  return admission.baselineBytes === PERIODIC_ACCEPTANCE.baselineBytes
    && admission.minimumNetBytes === PERIODIC_ACCEPTANCE.minimumCombinedNetBytes
    && admission.aggregateCandidateAccepted && admission.executionAuthorized
    && count(admission.conservativeCombinedNetBytes)
    && admission.conservativeCombinedNetBytes >= PERIODIC_ACCEPTANCE.minimumCombinedNetBytes;
}

function errorCode(error: unknown): string | null {
  if (typeof error === "object" && error !== null && "code" in error
    && typeof error.code === "string" && /^STATE_[A-Z0-9_]{1,80}$/.test(error.code)) return error.code;
  const message = error instanceof Error ? error.message : typeof error === "string" ? error : "";
  return message.match(/STATE_[A-Z0-9_]{1,80}/)?.[0] ?? null;
}

function sameFence(request: NativeCompactionApply, lease: PeriodicMaintenanceLease): boolean {
  return request.lease_owner_id === lease.ownerId && request.lease_fence === lease.fence;
}

function operationStatus(
  progress: PeriodicCompactionProgress,
  request: NativeCompactionApply,
  value: NativeCompactionStatus,
): void {
  progress.lastStatus = value;
  progress.lastAppliedRequest = request;
  progress.lastApplyResult = value;
  progress.lastOperationId = request.operation_id;
  progress.pendingRequest = null;
  progress.pendingRequestRaw = null;
  progress.lastDisposition = "applied";
  progress.lastErrorCode = null;
}

async function saveApplied(
  input: PeriodicCompactionInput,
  request: NativeCompactionApply,
  value: NativeCompactionStatus,
): Promise<void> {
  operationStatus(input.progress, request, value);
  if (terminal(value, input.progress)) {
    if (input.progress.workflowTerminal?.executionId !== request.operation_id) input.progress.workflowTerminal = {
      executionId: request.operation_id, agentId: getAgentId(), occurredAt: new Date().toISOString(), outcome: "completed" };
  }
  try {
    await input.persistProgress();
    if (terminal(value, input.progress) && input.progress.workflowTerminal) await observeWorkflowTerminal(input.kv, {
      ...input.progress.workflowTerminal, workflow: "maintenance", producer: "periodic-compaction-candidate", executionLevel: "workflow" });
  } catch (error) {
    await markWorkflowStatisticsGap(input.kv, "maintenance");
    input.progress.pendingRequest = request;
    input.progress.pendingRequestRaw = null;
    input.progress.lastDisposition = "outcome_unknown";
    throw error;
  }
}

function terminal(status: NativeCompactionStatus, progress: PeriodicCompactionProgress): boolean {
  const request = progress.lastAppliedRequest;
  const applied = progress.lastApplyResult;
  return request !== null && applied !== null && status.phase === "compact" && status.complete
    && status.rows_complete && status.vectors_complete && status.gc_complete === true
    && request.step === "collect" && applied.operation_id === request.operation_id
    && applied.generation === status.generation && applied.complete && applied.rows_complete
    && applied.vectors_complete && applied.gc_complete === true
    && applied.retained_inline_present === false;
}

function nextStep(status: NativeCompactionStatus): NativeCompactionStep | null {
  if (!status.active && status.phase === "legacy") return "activate";
  if (status.phase === "copy") return status.complete ? "cut" : "copy";
  if (status.phase === "rollback") return status.complete ? null : "rollback";
  if (status.phase === "compact") return status.rows_complete ? "collect" : "copy";
  return null;
}

function result(
  disposition: PeriodicCompactionDisposition,
  growthBytes: number | null,
  progress: PeriodicCompactionProgress,
): PeriodicCompactionResult {
  return {
    disposition,
    growthBytes,
    operationId: progress.pendingRequest?.operation_id ?? progress.lastOperationId,
    status: progress.lastStatus,
  };
}

async function readStatus(sdk: IIIClient): Promise<NativeCompactionStatus> {
  return triggerStateStorage<NativeCompactionStatus>(sdk, { action: "compact_status" });
}

async function readReceipt(
  sdk: IIIClient,
  request: NativeCompactionApply,
): Promise<NativeCompactionReceiptResult> {
  return triggerStateStorage<NativeCompactionReceiptResult>(sdk, {
    action: "compact_receipt", request,
  });
}

async function apply(
  sdk: IIIClient,
  request: NativeCompactionApply,
): Promise<NativeCompactionStatus> {
  return triggerStateStorage<NativeCompactionStatus>(sdk, {
    action: "compact_apply", request,
  });
}

async function reconcilePending(input: PeriodicCompactionInput): Promise<PeriodicCompactionResult | null> {
  const request = input.progress.pendingRequest;
  if (!request) return null;
  const saved = await readReceipt(input.sdk, request);
  if (saved.status === "applied") {
    await saveApplied(input, request, saved.result);
    return result(input.isCancelled() ? "cancelled" : "receipt_reconciled", null, input.progress);
  }
  if (input.isCancelled()) return result("cancelled", null, input.progress);
  const current = await readStatus(input.sdk);
  input.progress.lastStatus = current;
  await input.persistProgress();
  if (current.generation !== request.expected_generation) {
    input.progress.lastDisposition = "generation_changed_before_retry";
    await input.persistProgress();
    return result("pending_generation_changed", null, input.progress);
  }
  if (!sameFence(request, input.lease)) return result("pending_fence_stale", null, input.progress);
  if (!isPeriodicAdmissionReady(input.admission)) return result("admission_pending", null, input.progress);
  if (!input.admission.runtimeCompatible) return result("runtime_compatibility_pending", null, input.progress);

  await input.refreshSameLease();
  try {
    const applied = await apply(input.sdk, request);
    await saveApplied(input, request, applied);
    return result("receipt_reconciled", null, input.progress);
  } catch (error) {
    const refreshed = await readReceipt(input.sdk, request);
    if (refreshed.status === "applied") {
      await saveApplied(input, request, refreshed.result);
      return result("receipt_reconciled", null, input.progress);
    }
    const after = await readStatus(input.sdk);
    input.progress.lastStatus = after;
    await input.persistProgress();
    if (after.generation !== request.expected_generation) {
      input.progress.lastDisposition = "generation_changed_after_unknown_retry";
      await input.persistProgress();
      return result("pending_generation_changed", null, input.progress);
    }
    const code = errorCode(error);
    if (code) {
      input.progress.lastErrorCode = code;
      input.progress.lastDisposition = "apply_rejected";
      await input.persistProgress();
      throw error;
    }
    input.progress.lastDisposition = "outcome_unknown";
    input.progress.lastErrorCode = null;
    await input.persistProgress();
    return result("pending_receipt_not_found", null, input.progress);
  }
}

export async function runPeriodicCompactionCandidate(
  input: PeriodicCompactionInput,
): Promise<PeriodicCompactionResult> {
  if (input.progress.recoveryRequired) {
    return result("manual_reconciliation_required", null, input.progress);
  }
  const current = measuredFootprint(input.sample);
  let growthBytes: number | null = null;
  if (current && input.progress.growthBaseline
    && input.progress.growthBaseline.categoryVersion === current.categoryVersion
    && input.progress.growthBaseline.coverage === current.coverage
    && Date.parse(current.sampledAt) > Date.parse(input.progress.growthBaseline.sampledAt)) {
    growthBytes = current.observedBytes - input.progress.growthBaseline.observedBytes;
  }
  if (!input.progress.cycle && input.progress.pendingRequest && input.progress.growthBaseline) {
    input.progress.cycle = { startedAt: new Date().toISOString(), layoutComplete: input.layoutComplete };
    await input.persistProgress();
  } else if (input.progress.cycle && input.layoutComplete && !input.progress.cycle.layoutComplete) {
    input.progress.cycle.layoutComplete = true;
    await input.persistProgress();
  }
  const pending = await reconcilePending(input);
  if (pending) return { ...pending, growthBytes };
  if (!current) return result("sample_incomparable", null, input.progress);
  if (!input.progress.growthBaseline) {
    input.progress.growthBaseline = current;
    await input.persistProgress();
    return result("baseline_established", 0, input.progress);
  }
  if (!input.progress.cycle) {
    if (growthBytes === null) return result("sample_incomparable", null, input.progress);
    if (growthBytes < PERIODIC_GROWTH_THRESHOLD_BYTES) {
      return result("growth_below_threshold", growthBytes, input.progress);
    }
    if (!isPeriodicAdmissionReady(input.admission)) return result("admission_pending", growthBytes, input.progress);
    if (!input.admission.runtimeCompatible) {
      return result("runtime_compatibility_pending", growthBytes, input.progress);
    }
    input.progress.cycle = { startedAt: new Date().toISOString(), layoutComplete: input.layoutComplete };
    await input.persistProgress();
  }
  if (!isPeriodicAdmissionReady(input.admission)) return result("admission_pending", growthBytes, input.progress);
  if (!input.admission.runtimeCompatible) {
    return result("runtime_compatibility_pending", growthBytes, input.progress);
  }
  if (input.isCancelled()) return result("cancelled", growthBytes, input.progress);

  const status = await readStatus(input.sdk);
  input.progress.lastStatus = status;
  await input.persistProgress();
  if (terminal(status, input.progress)) {
    const workflowTerminal = input.progress.workflowTerminal;
    if (workflowTerminal && workflowTerminal.executionId === input.progress.lastAppliedRequest?.operation_id) await observeWorkflowTerminal(input.kv, {
      ...workflowTerminal, workflow: "maintenance", producer: "periodic-compaction-candidate", executionLevel: "workflow" });
    else await markWorkflowStatisticsGap(input.kv, "maintenance");
    input.progress.lastDisposition = "candidate_terminal_pending_cycle_completion";
    await input.persistProgress();
    return result("candidate_terminal", growthBytes, input.progress);
  }
  const step = nextStep(status);
  if (!step) return result("terminal_evidence_pending", growthBytes, input.progress);

  await input.refreshSameLease();
  const request: NativeCompactionApply = {
    operation_id: generateId("storage-compaction"),
    expected_generation: status.generation,
    step,
    ...PERIODIC_COMPACTION_BATCH,
    limit: step === "collect" ? PERIODIC_COLLECTION_LIMIT : step === "copy" ? PERIODIC_COPY_LIMIT : PERIODIC_COMPACTION_BATCH.limit,
    lease_owner_id: input.lease.ownerId,
    lease_fence: input.lease.fence,
  };
  input.progress.pendingRequest = request;
  input.progress.pendingRequestRaw = null;
  input.progress.lastDisposition = "pending_apply";
  input.progress.lastErrorCode = null;
  await input.persistProgress();
  if (input.isCancelled()) {
    input.progress.pendingRequest = null;
    input.progress.lastDisposition = "cancelled_before_apply";
    await input.persistProgress();
    return result("cancelled", growthBytes, input.progress);
  }

  try {
    const applied = await apply(input.sdk, request);
    await saveApplied(input, request, applied);
    return result("batch_applied", growthBytes, input.progress);
  } catch (error) {
    const saved = await readReceipt(input.sdk, request);
    if (saved.status === "applied") {
      await saveApplied(input, request, saved.result);
      return result("receipt_reconciled", growthBytes, input.progress);
    }
    const code = errorCode(error);
    if (code) {
      const after = await readStatus(input.sdk);
      input.progress.lastStatus = after;
      await input.persistProgress();
      if (after.generation !== request.expected_generation) {
        input.progress.lastDisposition = "generation_changed_after_apply_error";
        await input.persistProgress();
        return result("pending_generation_changed", growthBytes, input.progress);
      }
      input.progress.lastErrorCode = code;
      input.progress.lastDisposition = "apply_rejected";
      await input.persistProgress();
      throw error;
    }
    const pending = await reconcilePending(input);
    if (pending) return { ...pending, growthBytes };
    input.progress.lastDisposition = "outcome_unknown";
    input.progress.lastErrorCode = null;
    await input.persistProgress();
    return result("pending_receipt_not_found", growthBytes, input.progress);
  }
}
