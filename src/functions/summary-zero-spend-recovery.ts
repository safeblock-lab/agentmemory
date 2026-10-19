import type { StateKV } from "../state/kv.js";
import type { SummaryQueueJob } from "../types.js";
import { KV, fingerprintId } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { SummaryBudgetError } from "./summary-budget.js";
import type { ManualOperation, SummaryQueueUnit } from "./summary-queue.js";
import { summaryPromptCorrectionReady } from "./summary-prompt-correction.js";

export interface SummaryRecoveryConstruction {
  state: "creating" | "ready";
  identity: string;
  predecessorJobId: string;
  rootJobId: string;
  job: SummaryQueueJob;
  unit: SummaryQueueUnit;
  operation: ManualOperation;
}

export const summaryConstructionKey = (jobId: string): string => `${jobId}:recovery-construction`;
const conflict = (): never => { throw new SummaryBudgetError("summary_recovery_identity_mismatch"); };
const exact = (left: unknown, right: unknown): boolean => JSON.stringify(left) === JSON.stringify(right);

export function zeroSpendSummaryIdentity(predecessor: SummaryQueueJob, root: SummaryQueueJob): string {
  return fingerprintId("summary-zero-spend-recovery", JSON.stringify([
    predecessor.id, root.id, root.project, root.sessionId, root.snapshotFingerprint,
  ]));
}

export async function summaryRecoveryReady(kv: StateKV, job: SummaryQueueJob): Promise<boolean> {
  if (!await summaryPromptCorrectionReady(kv, job)) return false;
  const journal = await kv.get<SummaryRecoveryConstruction>(KV.state, summaryConstructionKey(job.id));
  if (!journal) return job.zeroSpendRecoveryOfJobId === undefined;
  return journal.state === "ready" && journal.job.id === job.id && journal.job.sessionId === job.sessionId &&
    journal.job.project === job.project && journal.job.snapshotFingerprint === job.snapshotFingerprint &&
    journal.job.recoveryOfJobId === job.recoveryOfJobId &&
    journal.job.zeroSpendRecoveryOfJobId === job.zeroSpendRecoveryOfJobId;
}

// The caller holds the session and predecessor locks. The short active lock is always last.
export async function constructSummaryRecovery(kv: StateKV, proposed: SummaryRecoveryConstruction): Promise<SummaryQueueJob> {
  const key = summaryConstructionKey(proposed.job.id);
  let journal = await kv.get<SummaryRecoveryConstruction>(KV.state, key);
  if (journal) {
    if (journal.identity !== proposed.identity || journal.predecessorJobId !== proposed.predecessorJobId ||
        journal.rootJobId !== proposed.rootJobId || journal.job.id !== proposed.job.id ||
        !["creating", "ready"].includes(journal.state)) conflict();
    if (journal.state === "ready") {
      const current = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, journal.job.id);
      if (!current || !await summaryRecoveryReady(kv, current)) conflict();
      return current;
    }
  } else {
    journal = proposed;
    await kv.set(KV.state, key, journal);
  }
  if (journal.job.status !== "pending" || journal.job.executionOrigin !== "manual" ||
      journal.job.recoveryMode !== "full-originals-v1" || journal.job.recoveryOfJobId !== journal.rootJobId ||
      journal.job.unitIds.length !== 1 || journal.job.unitIds[0] !== journal.unit.id ||
      journal.unit.jobId !== journal.job.id || journal.unit.attempts !== 0 || journal.unit.output !== undefined ||
      journal.unit.summary !== undefined || journal.unit.deliveryId !== undefined || journal.unit.startedAt !== undefined ||
      journal.operation.id !== `${journal.job.id}:manual` || journal.operation.jobId !== journal.job.id ||
      journal.operation.sessionId !== journal.job.sessionId || journal.operation.project !== journal.job.project ||
      journal.operation.sourceFingerprint !== journal.job.snapshotFingerprint || journal.operation.step !== 0 ||
      journal.operation.continuation !== undefined)
    conflict();
  const fill = async (scope: string, rowKey: string, value: unknown) => {
    const existing = await kv.get(scope, rowKey);
    if (existing !== null) { if (!exact(existing, value)) conflict(); }
    else await kv.set(scope, rowKey, value);
  };
  await fill(KV.summaryQueueUnits(journal.job.id), journal.unit.id, journal.unit);
  await fill(KV.summaryQueueJobs, journal.job.id, journal.job);
  await fill(KV.state, journal.operation.id, journal.operation);
  await withKeyedLock(`summary-active:${journal.job.sessionId}`, async () => {
    const active = await kv.get<{ jobId: string }>(KV.summaryQueueActive, journal.job.sessionId);
    if (active && active.jobId !== journal.job.id) {
      const owner = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, active.jobId);
      if (!owner || owner.status === "pending") throw new SummaryBudgetError("summary_recovery_conflict");
    }
    if (active?.jobId !== journal.job.id)
      await kv.set(KV.summaryQueueActive, journal.job.sessionId, { jobId: journal.job.id });
  });
  await kv.set(KV.state, key, { ...journal, state: "ready" });
  return journal.job;
}

export async function assertZeroSpendSummaryPredecessor(
  kv: StateKV, predecessor: SummaryQueueJob, root: SummaryQueueJob, expectedPredecessorId: string,
): Promise<void> {
  if (predecessor.id !== expectedPredecessorId || predecessor.id === root.id ||
      predecessor.status !== "failed" || predecessor.failure !== "manual_summary_limit_exceeded" ||
      predecessor.executionOrigin !== "manual" || predecessor.recoveryMode !== "full-originals-v1" ||
      predecessor.recoveryGeneration !== "full-originals-v2" || predecessor.recoveryOfJobId !== root.id ||
      predecessor.zeroSpendRecoveryOfJobId !== undefined || predecessor.completedAt !== undefined ||
      root.status !== "failed" || root.failure !== "summary_publication_invalid_response" ||
      root.executionOrigin !== "manual" || root.recoveryOfJobId !== undefined || root.recoveryMode !== undefined ||
      root.recoveryGeneration !== undefined || root.zeroSpendRecoveryOfJobId !== undefined ||
      predecessor.sessionId !== root.sessionId || predecessor.project !== root.project ||
      predecessor.snapshotFingerprint !== root.snapshotFingerprint ||
      predecessor.observationCount !== root.observationCount || predecessor.stage !== "map" || predecessor.round !== 0 ||
      !Array.isArray(predecessor.unitIds) || predecessor.unitIds.length !== 1 ||
      !Number.isSafeInteger(predecessor.observationCount) || predecessor.observationCount < 1)
    throw new SummaryBudgetError("summary_zero_spend_recovery_not_available");
  const operation = await kv.get<ManualOperation>(KV.state, `${predecessor.id}:manual`);
  const unit = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(predecessor.id), predecessor.unitIds[0]);
  const continuation = operation?.continuation;
  if (!operation || operation.id !== `${predecessor.id}:manual` || operation.jobId !== predecessor.id ||
      operation.sessionId !== predecessor.sessionId || operation.project !== predecessor.project ||
      operation.sourceFingerprint !== predecessor.snapshotFingerprint || operation.step !== 0 ||
      operation.maxRequests !== 2 ||
      !unit || unit.id !== predecessor.unitIds[0] || unit.jobId !== predecessor.id || unit.stage !== "map" ||
      unit.round !== 0 || unit.attempts !== 0 || unit.output !== undefined || unit.summary !== undefined ||
      !Array.isArray(unit.items) || unit.items.length !== 1 || unit.items[0]?.text !== "" ||
      unit.items[0].obsRangeStart !== 1 || unit.items[0].obsRangeEnd !== predecessor.observationCount ||
      !Array.isArray(unit.items[0].sourcePaths) || unit.items[0].sourcePaths.length !== 0 ||
      !Array.isArray(unit.items[0].modifiedPaths) || unit.items[0].modifiedPaths.length !== 0 ||
      unit.startedAt !== undefined || unit.lastAttemptAt !== undefined || unit.lastError !== undefined ||
      !continuation || Object.keys(continuation).length !== 9 || continuation.operationId !== operation.id ||
      continuation.jobId !== predecessor.id || continuation.sessionId !== predecessor.sessionId ||
      continuation.project !== predecessor.project || continuation.sourceFingerprint !== predecessor.snapshotFingerprint ||
      continuation.step !== 0 || continuation.round !== 0 || continuation.unitId !== unit.id ||
      typeof unit.deliveryId !== "string" || !unit.deliveryId || unit.deliveryId.length > 512 ||
      unit.deliveryId !== unit.deliveryId.trim() || continuation.deliveryId !== unit.deliveryId ||
      typeof unit.dispatchedAt !== "string" || !Number.isFinite(Date.parse(unit.dispatchedAt)))
    throw new SummaryBudgetError("summary_zero_spend_recovery_not_available");
  const units = await kv.list<SummaryQueueUnit>(KV.summaryQueueUnits(predecessor.id));
  if (units.length !== 1 || !exact(units[0], unit)) throw new SummaryBudgetError("summary_zero_spend_recovery_not_available");
  const keys = [`${predecessor.id}:raw:${unit.id}`, `${predecessor.id}:validated:${unit.id}`,
    `${predecessor.id}:lineage:${unit.id}`, `${predecessor.id}:publication-effect`, `${predecessor.id}:fallback:0`,
    ...Array.from({ length: operation.maxRequests + 1 }, (_, step) => `${operation.id}:request:${step}`)];
  const evidence = await Promise.all(keys.map(key => kv.get(KV.state, key)));
  const completed = await kv.get<{ jobId?: string }>(KV.summaryQueueCompleted, predecessor.sessionId);
  if (evidence.some(row => row !== null) || completed?.jobId === predecessor.id)
    throw new SummaryBudgetError("summary_zero_spend_recovery_not_available");
  // Generation persists raw intent before dispatch; judgment requires its captured, parsed output.
  // With neither raw nor output, the supported path cannot have created a publication checkpoint.
}
