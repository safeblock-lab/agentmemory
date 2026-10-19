import type { IIIClient } from 'iii-sdk';
import type { StateKV } from '../state/kv.js';
import type { StorageGrowthSample } from '../types.js';
import { triggerStateStorage, type StorageReclaimResult } from '../state/storage.js';
import { isLayoutAdmissionAccepted, runPeriodicLayouts, releaseUnusedLayoutGraphTurn, type LayoutCycle, type LayoutAdmission } from './periodic-layouts.js';
import { runPeriodicCompactionCandidate, preservePeriodicLease } from './periodic-maintenance.js';
import { type PeriodicCompactionProgress,
  completeMaintenanceReclaim, resetGrowthBaselineAfterCompleteCycle,
  type PeriodicExecutionAdmission } from './periodic-maintenance-state.js';

export const PHYSICAL_GROWTH_COVERAGE = 'physical_sqlite_db_wal_v1';
const PHYSICAL_MAINTENANCE_WINDOW_MS = 45_000;
const MAX_COMPACTION_STEPS_PER_WINDOW = 1_024;
const MAX_RECLAIM_STEPS_PER_WINDOW = 16;
const MAX_LAYOUT_WINDOWS = 4;
const LAYOUT_WINDOW_MS = 20_000;

type PhysicalMaintenanceResult = {
  preserveLease: boolean; layouts: string; periodic: string; reclaimed: boolean;
  progressMade: boolean; incomplete: boolean; elapsedMs: number;
  cancellationReason: "foreground_pressure" | "deadline" | null;
  foregroundPressure: StorageForegroundPressure | null;
};

export interface StorageMaintenanceContinuation {
  version: 1;
  kind: "progress" | "graph_queued" | "foreground" | "busy" | "idle" | "complete" | "hold";
  reason: string;
  incomplete: boolean;
  progressMade: boolean;
  retryAfterMs: number | null;
}

function nativeProgress(value: unknown): Record<string, unknown> | null {
  if (typeof value !== 'object' || value === null) return null;
  const status = value as Record<string, unknown>;
  const field = (parent: unknown, key: string): unknown => typeof parent === 'object' && parent !== null
    ? (parent as Record<string, unknown>)[key] : undefined;
  const count = (value: unknown) => Number.isSafeInteger(value) && Number(value) >= 0 ? Number(value) : undefined;
  const collect = status.collect_progress;
  return { phase: status.phase, scope: field(status.cursor, 'scope'), key: field(status.cursor, 'key'),
    rows: status.rows_complete, vectors: status.vectors_complete, gc: status.gc_complete,
    stage: count(field(collect, 'stage')), inspected: count(field(collect, 'inspected_total')), deleted: count(field(collect, 'deleted_total')),
    seed: field(field(collect, 'concurrent_gc'), 'seed_complete'), copySeed: field(status.copy_progress, 'seed_complete') };
}

function nativeAdvanced(before: Record<string, unknown> | null, after: Record<string, unknown> | null): boolean {
  if (!before || !after) return false;
  return ['phase', 'scope', 'key', 'stage'].some(key => before[key] !== undefined && after[key] !== undefined
    && before[key] !== after[key])
    || ['rows', 'vectors', 'gc', 'seed', 'copySeed'].some(key => before[key] === false && after[key] === true)
    || ['inspected', 'deleted'].some(key => typeof before[key] === 'number' && typeof after[key] === 'number'
      && Number(after[key]) > Number(before[key]));
}

export interface StorageForegroundPressure {
  busy: boolean;
  phase: "active" | "cooldown" | "idle";
  observedAt: string;
  activeRequests: number;
  activeFunctions: Array<{ functionId: string; requests: number }>;
  activeFunctionsTruncated: boolean;
  lastCompletedFunctionId: string | null;
  lastCompletedAt: string | null;
  lastCompletedAgeMs: number | null;
  quietWindowRemainingMs: number;
}

export async function runAdmittedPhysicalMaintenance(input: {
  sdk: IIIClient; kv: StateKV; layoutCycle: LayoutCycle; layoutAdmission: LayoutAdmission;
  periodicCompaction: PeriodicCompactionProgress; periodicAdmission: PeriodicExecutionAdmission;
  lease: { ownerId: string; fence: number }; refresh: () => Promise<void>; persist: () => Promise<void>;
  reclaimMaintenance: () => Promise<{ result: StorageReclaimResult | null; outcomeUnknown: boolean }>;
  isForegroundBusy?: () => boolean;
  foregroundPressureSnapshot?: () => StorageForegroundPressure;
}): Promise<PhysicalMaintenanceResult> {
  const startedAt = Date.now();
  const deadline = startedAt + PHYSICAL_MAINTENANCE_WINDOW_MS;
  let progressMade = false;
  let cancellationReason: PhysicalMaintenanceResult["cancellationReason"] = null;
  let foregroundPressure: StorageForegroundPressure | null = null;
  const isCancelled = () => {
    if (cancellationReason !== null) return true;
    if (Date.now() >= deadline) cancellationReason = 'deadline';
    else if (input.isForegroundBusy?.() === true) {
      cancellationReason = 'foreground_pressure';
      foregroundPressure = input.foregroundPressureSnapshot?.() ?? null;
    }
    return cancellationReason !== null;
  };
  const finish = (result: Omit<PhysicalMaintenanceResult, 'cancellationReason' | 'foregroundPressure'
    | 'progressMade' | 'incomplete' | 'elapsedMs'>): PhysicalMaintenanceResult =>
    ({ ...result, cancellationReason, foregroundPressure, progressMade,
      incomplete: input.layoutCycle.active || input.periodicCompaction.cycle !== null,
      elapsedMs: Math.max(0, Date.now() - startedAt) });
  const currentCancellationReason = (): PhysicalMaintenanceResult['cancellationReason'] => cancellationReason;
  const layoutDeadline = Math.min(deadline - 25_000, Date.now() + LAYOUT_WINDOW_MS);
  const layoutCancelled = () => isCancelled() || Date.now() >= layoutDeadline;
  const physical = await triggerStateStorage<StorageGrowthSample['physical']>(input.sdk, { action: 'physical' });
  // This sample contains no corpus census and never replaces diagnostics.lastComplete.
  const sample: StorageGrowthSample = { sampledAt: new Date().toISOString(), categoryVersion: 1,
    coverage: PHYSICAL_GROWTH_COVERAGE, rowsScanned: 0, categories: [], physical };
  let layouts: Awaited<ReturnType<typeof runPeriodicLayouts>> = { preserveLease: false, disposition: 'window_yielded' };
  for (let window = 0; window < MAX_LAYOUT_WINDOWS && !layoutCancelled(); window++) {
    layouts = await runPeriodicLayouts({ sdk: input.sdk, cycle: input.layoutCycle,
      admission: input.layoutAdmission, sample, lease: input.lease, refresh: input.refresh, persist: input.persist,
      isCancelled: layoutCancelled });
    progressMade ||= layouts.progressMade === true;
    if (layouts.preserveLease || layouts.turnComplete || layouts.disposition !== 'bounded_cycle_progress') break;
  }
  releaseUnusedLayoutGraphTurn(input.lease);
  if (layouts.preserveLease) return finish({ preserveLease: true, layouts: layouts.disposition, periodic: 'deferred', reclaimed: false });
  const layoutComplete = isLayoutAdmissionAccepted(input.layoutAdmission)
    && (layouts.disposition === 'cycle_complete'
      || (!input.layoutCycle.active && input.layoutCycle.familyIndex === 3
        && input.layoutCycle.pending === null && !input.layoutCycle.recoveryRequired));
  let periodic: Awaited<ReturnType<typeof runPeriodicCompactionCandidate>> | null = null;
  for (let step = 0; step < MAX_COMPACTION_STEPS_PER_WINDOW && !isCancelled(); step++) {
    let observed = nativeProgress(input.periodicCompaction.lastStatus);
    let appliedProgress = false;
    periodic = await runPeriodicCompactionCandidate({ sdk: input.sdk, kv: input.kv,
      progress: input.periodicCompaction, layoutComplete, sample, admission: input.periodicAdmission,
      lease: input.lease, refreshSameLease: input.refresh, persistProgress: async () => {
        const status = input.periodicCompaction.lastStatus;
        const request = input.periodicCompaction.lastAppliedRequest;
        const applied = input.periodicCompaction.lastApplyResult;
        const current = nativeProgress(status);
        if (input.periodicCompaction.lastDisposition === 'applied' && status && request && applied
          && status.operation_id === request.operation_id && applied.operation_id === request.operation_id
          && status.generation === applied.generation) appliedProgress ||= nativeAdvanced(observed, current);
        observed = current;
        await input.persist();
      }, isCancelled });
    if (periodic.disposition === 'batch_applied' || periodic.disposition === 'receipt_reconciled')
      progressMade ||= appliedProgress;
    if (preservePeriodicLease(periodic.disposition) || input.periodicCompaction.pendingRequest !== null
      || input.periodicCompaction.recoveryRequired) {
      return finish({ preserveLease: true, layouts: layouts.disposition, periodic: periodic.disposition, reclaimed: false });
    }
    if (periodic.disposition !== 'batch_applied' && periodic.disposition !== 'receipt_reconciled') break;
  }
  if (!periodic) return finish({ preserveLease: false, layouts: layouts.disposition,
    periodic: currentCancellationReason() === 'foreground_pressure' ? 'foreground_pressure' : 'window_expired', reclaimed: false });
  if (periodic.disposition !== 'candidate_terminal' || !layoutComplete) {
    return finish({ preserveLease: false, layouts: layouts.disposition, periodic: periodic.disposition, reclaimed: false });
  }

  let lastReclaim: StorageReclaimResult | null = null;
  for (let step = 0; step < MAX_RECLAIM_STEPS_PER_WINDOW && !isCancelled(); step++) {
    const outcome = await input.reclaimMaintenance();
    if (outcome.outcomeUnknown) {
      return finish({ preserveLease: true, layouts: layouts.disposition, periodic: periodic.disposition, reclaimed: false });
    }
    if (!outcome.result) break;
    lastReclaim = outcome.result;
    progressMade ||= lastReclaim.pages_reclaimed > 0 || lastReclaim.checkpointed_frames > 0;
    if (completeMaintenanceReclaim(lastReclaim)) {
      if (isCancelled()) return finish({ preserveLease: false, layouts: layouts.disposition,
        periodic: periodic.disposition, reclaimed: true });
      const completedPhysical = await triggerStateStorage<StorageGrowthSample['physical']>(input.sdk, { action: 'physical' });
      const completedSample: StorageGrowthSample = {
        ...sample, sampledAt: new Date().toISOString(), physical: completedPhysical,
      };
      if (!isCancelled() && resetGrowthBaselineAfterCompleteCycle(input.periodicCompaction, completedSample, lastReclaim)) {
        await input.persist();
      }
      return finish({ preserveLease: false, layouts: layouts.disposition, periodic: periodic.disposition, reclaimed: true });
    }
    if (lastReclaim.status !== 'progress'
      || (lastReclaim.pages_reclaimed === 0 && lastReclaim.checkpointed_frames === 0)
      || lastReclaim.reason === 'foreground_pressure' || lastReclaim.checkpoint_busy) break;
  }
  return finish({ preserveLease: false, layouts: layouts.disposition, periodic: periodic.disposition,
    reclaimed: lastReclaim !== null && completeMaintenanceReclaim(lastReclaim) });
}
