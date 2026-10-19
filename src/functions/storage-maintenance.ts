import { markWorkflowStatisticsGap } from "./dashboard-activity.js";
import type { IIIClient } from "iii-sdk";
import { runAdmittedPhysicalMaintenance, type StorageForegroundPressure, type StorageMaintenanceContinuation } from "./physical-maintenance.js";
import { runPeriodicLayouts, emptyLayoutCycle, normalizeLayoutCycle, isLayoutAdmissionAccepted, bindLayoutGraphTurn, LAYOUT_LEASE_CONTEXT_RECOVERY_VERSION, type LayoutCycle, type LayoutAdmission } from "./periodic-layouts.js";
import { requestGraphWorkTurn, peekGraphWorkQueue, type GraphWorkTurn } from "../state/graph-work-queue.js";
import { runPeriodicCompactionCandidate, preservePeriodicLease } from "./periodic-maintenance.js";
import { emptyPeriodicCompactionProgress, normalizePeriodicCompactionProgress,
  type PeriodicCompactionProgress, type PeriodicExecutionAdmission } from "./periodic-maintenance-state.js";
import { logger } from "../logger.js";
import { loadSnapshotConfig } from "../config.js";
import { collectClosedSnapshotTemporary } from "./snapshot.js";
import { recordAudit } from "./audit.js";
import type {
  StorageCleanupSummary, StorageCompactionProgress, StorageDiagnosticProgress,
  StorageGrowthSample, StorageMaintenanceState,
} from "../types.js";
import { reconstructLegacyIdentity } from "./observation-identity.js";
import { pruneTerminalGraphWork } from "./graph-retention.js";
import { STORAGE_RETENTION, RETENTION_USE_INTEGRATION, hasRetentionProtection, retentionEligibility, terminalRetentionOrigin } from "./storage-retention.js";
import { generateId, KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { StatePageError } from "../state/state-pages.js";
import {
  triggerStateStorage, type ObservationIndexResult, type StorageCandidate,
  type StorageDiagnostics, type StorageMigrationResult, type StorageReclaimResult,
  type TechnicalPruneResult, type StorageMaintenanceLeaseRequest, type StorageMaintenanceLeaseResponse,
} from "../state/storage.js";

const DAY_MS = 24 * 60 * 60 * 1000;
const RETENTION_MS = STORAGE_RETENTION.completedBodies.inactivityMs;
const MAX_CANDIDATES = 24;
const RECLAIM_PAGE_MS = 25;
const RECLAIM_MAX_PAGES = 16;
const RECLAIM_MAINTENANCE_PAGE_MS = 3_000;
const RECLAIM_MAINTENANCE_MAX_PAGES = 4_096;
const RECLAIM_MIN_INTERVAL_MS = 5_000;
const RECLAIM_IDLE_POLL_MS = 60_000;
const RECLAIM_ERROR_BACKOFF_MS = 15 * 60_000;
const RECLAIM_OVERLAP_LEASE_MS = 5 * 60_000;
const active = new WeakSet<StateKV>();
const maintenanceClosing = new WeakSet<StateKV>();
const maintenanceRuns = new WeakMap<StateKV, Set<Promise<unknown>>>();
export function beginStorageMaintenanceDrain(kv: StateKV): Promise<void> {
  maintenanceClosing.add(kv);
  return Promise.allSettled([...(maintenanceRuns.get(kv) ?? [])]).then(() => undefined);
}
type StorageMaintenancePhysicalObservation = {
  sampleAt: string;
  layouts: string | null;
  periodic: string | null;
  cancellationReason: "foreground_pressure" | "deadline" | null;
  preserveLease: boolean;
  progressMade: boolean;
  incomplete: boolean;
  reclaimed: boolean;
  elapsedMs: number;
};
const lastPhysicalObservations = new WeakMap<StateKV, StorageMaintenancePhysicalObservation>();
export type StorageMaintenanceForegroundObservation = {
  sampleAt: string;
  busy: boolean;
  phase: StorageForegroundPressure["phase"];
  activeRequests: number;
  activeFunctionCount: number;
  quietWindowRemainingMs: number;
};
export type StorageMaintenanceControllerSnapshot = {
  sampledAt: string;
  started: boolean;
  stopped: boolean;
  running: boolean;
  lastWindowStartedAt: string | null;
  lastWindowEndedAt: string | null;
  lastResultKind: StorageMaintenanceContinuation["kind"] | null;
  lastResultReason: string | null;
  lastResultAt: string | null;
  lastDecision: { at: string; kind: "paused" | "foreground" | "cooldown" | "dispatch" | "result" | "stopped"; reason: string } | null;
  nextDueAt: string | null;
  cooldownUntil: string | null;
  cooldownRemainingMs: number;
  wakePending: boolean;
  paused: boolean;
  lastObservedForeground: StorageMaintenanceForegroundObservation | null;
};
type LayoutTurnTicket = { turn?: GraphWorkTurn; waited: boolean; claimed: boolean };
const layoutTurnTickets = new WeakMap<StateKV, LayoutTurnTicket>();
function releaseLayoutTurn(kv: StateKV, ticket: LayoutTurnTicket): void {
  if (ticket.turn?.granted) ticket.turn.release();
  if (layoutTurnTickets.get(kv) === ticket) layoutTurnTickets.delete(kv);
}
export function releaseUnclaimedStorageMaintenanceTurn(kv: StateKV): void {
  const ticket = layoutTurnTickets.get(kv);
  if (ticket?.turn?.granted && !ticket.claimed) releaseLayoutTurn(kv, ticket);
}
function continuation(kind: StorageMaintenanceContinuation["kind"], reason: string, incomplete = true,
  progressMade = false, retryAfterMs: number | null = null): StorageMaintenanceContinuation {
  return { version: 1, kind, reason, incomplete, progressMade, retryAfterMs };
}
const stateWriters = new WeakMap<StateKV, { sdk: IIIClient; lease: MaintenanceLease; unavailable?: boolean }>();
type Row = Record<string, unknown>;

type MaintenanceHistoryEntry = {
  at: string;
  action: "tick" | "diagnostics" | "activate" | "reclaim" | "migrate_incremental" | "lease_recovery" | "lease_busy" | "lease_release";
  outcome: string;
  reason: string | null;
  elapsedMs: number | null;
  pagesReclaimed: number | null;
  physicalBytesReclaimed: number | null;
  before?: StoragePhysicalSample;
  after?: StoragePhysicalSample;
  errorCode?: string;
};

type StoragePhysicalSample = StorageGrowthSample["physical"];
type MaintenanceReview = {
  footprintBaselineBytes: number | null;
  footprintBaselineAt: string | null;
  freePagePendingSince: string | null;
  dueReasons: Array<"growth_20_percent" | "free_pages_24h">;
};
type MaintenanceState = StorageMaintenanceState & {
  layoutCycle: LayoutCycle;
  layoutAdmission?: LayoutAdmission;
  periodicCompaction: PeriodicCompactionProgress;
  periodicAdmission?: PeriodicExecutionAdmission;
  maintenanceHistory: MaintenanceHistoryEntry[];
  maintenanceReview: MaintenanceReview;
};

type MaintenanceLease = { ownerId: string; fence: number; expiresAt: string };

class StorageMaintenanceOutcomeUnknown extends Error {
  constructor(cause: unknown) {
    super(typeof cause === "string" && /^STATE_[A-Z0-9_]{1,80}$/.test(cause) ? cause : errorCode(cause));
    this.name = "StorageMaintenanceOutcomeUnknown";
  }
}

const EMPTY_DIAGNOSTIC: StorageDiagnosticProgress = {
  cursor: null, categoryVersion: null, sampledAt: null, coverage: null,
  rowsScanned: 0, categories: [], physical: null,
};

function emptyCompaction(): StorageCompactionProgress {
  return {
    pending: true, nextEligibleAt: null, nextMaxPages: 1,
    minIntervalMs: RECLAIM_MIN_INTERVAL_MS, backoffUntil: null,
    attempts: 0, progressCuts: 0, completedPolls: 0, deferredPolls: 0,
    failures: 0, overlapSkips: 0, lastStatus: null, lastReason: null,
    lastErrorCode: null, overlap: null,
  };
}

function emptyState(): MaintenanceState {
  return {
    version: 1, updatedAt: new Date().toISOString(),
    compaction: emptyCompaction(),
    layoutCycle: emptyLayoutCycle(),
    periodicCompaction: emptyPeriodicCompactionProgress(),
    totals: {
      graphRowsDeleted: 0, technicalRowsDeleted: 0, rawStripped: 0,
      operationRowsDeleted: 0, logicalBytesDeleted: 0, storedBytesDeleted: 0,
      unresolvedExclusions: 0, unknownAgeExclusions: 0,
    },
    observationIndex: {
      sessionsCursor: null, sessionId: null, nextSessionCursor: null,
      observationsCursor: null, scanned: 0, indexed: 0, unindexable: 0,
      conflicts: 0, readySessions: 0, complete: false,
    },
    diagnostics: { inProgress: { ...EMPTY_DIAGNOSTIC }, samples: [] },
    maintenanceHistory: [],
    maintenanceReview: {
      footprintBaselineBytes: null, footprintBaselineAt: null,
      freePagePendingSince: null, dueReasons: [],
    },
    technical: {
      scopeIndex: 0, cursors: {}, observationSessionsCursor: null,
      observationSessionId: null, observationNextSessionCursor: null,
      observationCursor: null, eventSessionsCursor: null, eventSessionId: null,
      eventNextSessionCursor: null, eventCursor: null,
      summaryJobsCursor: null, summaryJobId: null, summaryJobsNextCursor: null,
      summaryUnitsCursor: null, summaryJobReady: false,
    },
  };
}

async function readState(kv: StateKV): Promise<MaintenanceState> {
  const state = await kv.get<StorageMaintenanceState>(KV.storageMaintenance, "current");
  if (state?.version !== 1) return emptyState();
  const defaults = emptyState();
  const savedCompaction = state.compaction ?? defaults.compaction;
  const saved = state as MaintenanceState;
  const savedReview = isRow(saved.maintenanceReview) ? saved.maintenanceReview : defaults.maintenanceReview;
  return {
    ...defaults, ...state,
    layoutCycle: normalizeLayoutCycle(saved.layoutCycle),
    layoutAdmission: saved.layoutAdmission,
    periodicCompaction: normalizePeriodicCompactionProgress(saved.periodicCompaction),
    periodicAdmission: saved.periodicAdmission,
    compaction: {
      ...defaults.compaction, ...savedCompaction,
      nextMaxPages: Number.isInteger(savedCompaction.nextMaxPages)
        ? Math.max(1, Math.min(RECLAIM_MAX_PAGES, savedCompaction.nextMaxPages)) : 1,
      minIntervalMs: Number.isInteger(savedCompaction.minIntervalMs)
        ? Math.max(RECLAIM_MIN_INTERVAL_MS, savedCompaction.minIntervalMs) : RECLAIM_MIN_INTERVAL_MS,
      overlap: isRow(savedCompaction.overlap)
        && typeof savedCompaction.overlap.ownerId === "string"
        && typeof savedCompaction.overlap.startedAt === "string"
        && typeof savedCompaction.overlap.expiresAt === "string"
        ? savedCompaction.overlap as StorageCompactionProgress["overlap"] : null,
    },
    totals: { ...defaults.totals, ...state.totals },
    observationIndex: { ...defaults.observationIndex, ...state.observationIndex },
    diagnostics: {
      ...defaults.diagnostics, ...state.diagnostics,
      inProgress: { ...EMPTY_DIAGNOSTIC, ...state.diagnostics?.inProgress },
      samples: Array.isArray(state.diagnostics?.samples) ? state.diagnostics.samples.slice(-30) : [],
    },
    maintenanceHistory: Array.isArray(saved.maintenanceHistory) ? saved.maintenanceHistory.slice(-30) : [],
    maintenanceReview: {
      footprintBaselineBytes: Number.isSafeInteger(savedReview.footprintBaselineBytes)
        && Number(savedReview.footprintBaselineBytes) >= 0 ? Number(savedReview.footprintBaselineBytes) : null,
      footprintBaselineAt: typeof savedReview.footprintBaselineAt === "string" ? savedReview.footprintBaselineAt : null,
      freePagePendingSince: typeof savedReview.freePagePendingSince === "string" ? savedReview.freePagePendingSince : null,
      dueReasons: Array.isArray(savedReview.dueReasons)
        ? savedReview.dueReasons.filter((reason): reason is MaintenanceReview["dueReasons"][number] =>
          reason === "growth_20_percent" || reason === "free_pages_24h") : [],
    },
    technical: { ...defaults.technical, ...state.technical, cursors: state.technical?.cursors ?? {} },
  };
}

async function saveState(kv: StateKV, state: MaintenanceState, release = false): Promise<void> {
  const writer = stateWriters.get(kv);
  if (!writer || writer.unavailable) throw new StorageMaintenanceOutcomeUnknown("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
  state.updatedAt = new Date().toISOString();
  try {
    await triggerStateStorage(writer.sdk, {
      action: "maintenance_state_save", owner_id: writer.lease.ownerId,
      fence: writer.lease.fence, value: state, release,
    });
  } catch (error) {
    writer.unavailable = true;
    throw new StorageMaintenanceOutcomeUnknown(error);
  }
}

function isRow(value: unknown): value is Row {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorCode(error: unknown): string {
  if (isRow(error) && typeof error.code === "string" && /^STATE_[A-Z0-9_]{1,80}$/.test(error.code)) return error.code;
  const message = error instanceof Error ? error.message : "";
  return message.match(/STATE_[A-Z0-9_]{1,80}/)?.[0] ?? "STATE_STORAGE_FAILED";
}

function appendHistory(state: MaintenanceState, entry: MaintenanceHistoryEntry): void {
  state.maintenanceHistory = [...state.maintenanceHistory, entry].slice(-30);
}

async function triggerMaintenanceLease(
  sdk: IIIClient, request: StorageMaintenanceLeaseRequest,
): Promise<StorageMaintenanceLeaseResponse> {
  try {
    return await triggerStateStorage<StorageMaintenanceLeaseResponse>(sdk, request);
  } catch (error) {
    throw new StorageMaintenanceOutcomeUnknown(error);
  }
}

async function triggerFencedReclaim(
  sdk: IIIClient, lease: MaintenanceLease, maxPages: number, mode: "ordinary" | "maintenance",
): Promise<StorageReclaimResult> {
  try {
    const request = mode === "maintenance"
      ? { action: "reclaim" as const, maintenance: true as const, checkpoint: "truncate" as const,
        max_pages: RECLAIM_MAINTENANCE_MAX_PAGES, max_ms: RECLAIM_MAINTENANCE_PAGE_MS,
        lease_owner_id: lease.ownerId, lease_fence: lease.fence }
      : { action: "reclaim" as const, max_pages: maxPages, max_ms: RECLAIM_PAGE_MS,
        lease_owner_id: lease.ownerId, lease_fence: lease.fence };
    return await triggerStateStorage<StorageReclaimResult>(sdk, request);
  } catch (error) {
    throw new StorageMaintenanceOutcomeUnknown(error);
  }
}

function isStaleCursor(error: unknown): boolean {
  return error instanceof StatePageError
    && ["STATE_PAGE_CURSOR_STALE", "STATE_PAGE_CURSOR_INVALID"].includes(error.code);
}

function emptyInProgress(): StorageDiagnosticProgress {
  return { ...EMPTY_DIAGNOSTIC, categories: [] };
}

function mergeCategories(left: StorageGrowthSample["categories"], right: StorageDiagnostics["categories"]): StorageGrowthSample["categories"] {
  const merged = new Map(left.map((row) => [row.category, { ...row }]));
  for (const row of right) {
    const previous = merged.get(row.category) ?? { category: row.category, rows: 0, logical_bytes: 0, stored_bytes: 0, tombstones: 0 };
    previous.rows += row.rows;
    previous.logical_bytes += row.logical_bytes;
    previous.stored_bytes += row.stored_bytes;
    previous.tombstones += row.tombstones;
    merged.set(row.category, previous);
  }
  return [...merged.values()].sort((a, b) => a.category.localeCompare(b.category));
}

function updateMaintenanceReview(state: MaintenanceState, sample: StorageGrowthSample): void {
  const review = state.maintenanceReview;
  const footprintBytes = sample.physical.database_bytes + sample.physical.wal_bytes;
  if (review.footprintBaselineBytes === null && state.cleanupActivatedAt
    && state.lastCleanup && !state.lastCleanup.pending) {
    review.footprintBaselineBytes = footprintBytes;
    review.footprintBaselineAt = sample.sampledAt;
  }
  const due = new Set(review.dueReasons);
  if (review.footprintBaselineBytes !== null && review.footprintBaselineBytes > 0
    && footprintBytes >= review.footprintBaselineBytes * 1.2) {
    due.add("growth_20_percent");
  }
  if (sample.physical.freelist_count > 0) {
    review.freePagePendingSince ??= sample.sampledAt;
    if (Date.parse(sample.sampledAt) - Date.parse(review.freePagePendingSince) >= DAY_MS) {
      due.add("free_pages_24h");
    }
  } else {
    review.freePagePendingSince = null;
    due.delete("free_pages_24h");
  }
  review.dueReasons = [...due];
}

async function diagnosticsStep(sdk: IIIClient, kv: StateKV, state: MaintenanceState): Promise<void> {
  const progress = state.diagnostics.inProgress;
  const page = await triggerStateStorage<StorageDiagnostics>(sdk, {
    action: "diagnostics", ...(progress.cursor ? { cursor: progress.cursor } : {}),
    limit: 256, max_bytes: 1_048_576, max_ms: 2500,
  });
  const sameVersion = progress.categoryVersion === null || progress.categoryVersion === page.category_version;
  const prior = sameVersion ? progress : emptyInProgress();
  const next: StorageDiagnosticProgress = {
    cursor: page.cursor, categoryVersion: page.category_version,
    sampledAt: page.sampled_at, coverage: page.coverage,
    rowsScanned: prior.rowsScanned + page.rows_scanned,
    categories: mergeCategories(prior.categories, page.categories),
    physical: page.physical,
  };
  if (page.done && page.complete) {
    const sample: StorageGrowthSample = {
      sampledAt: page.sampled_at, categoryVersion: page.category_version,
      coverage: page.coverage, rowsScanned: next.rowsScanned,
      categories: next.categories, physical: page.physical,
    };
    state.diagnostics.lastComplete = sample;
    state.diagnostics.samples = [...state.diagnostics.samples, sample].slice(-30);
    updateMaintenanceReview(state, sample);
    state.diagnostics.inProgress = emptyInProgress();
  } else {
    state.diagnostics.inProgress = next;
  }
  await saveState(kv, state);
}

function readyMarker(value: unknown): boolean {
  return isRow(value) && value.version === 1 && value.ready === true;
}

async function observationIndexStep(sdk: IIIClient, kv: StateKV, state: MaintenanceState): Promise<void> {
  const progress = state.observationIndex;
  if (progress.complete) return;
  if (!progress.sessionId) {
    let page;
    try {
      page = (await kv.pages<{ id: string }>(KV.sessions, {
        ...(progress.sessionsCursor ? { cursor: progress.sessionsCursor } : {}),
        limit: 1, maxBytes: 4096, fields: ["id"],
      }).next()).value;
    } catch (error) {
      if (!isStaleCursor(error)) throw error;
      progress.sessionsCursor = null;
      await saveState(kv, state);
      return;
    }
    if (!page?.items.length) {
      progress.complete = true;
      await saveState(kv, state);
      return;
    }
    const sessionId = page.items[0]?.id;
    if (typeof sessionId !== "string" || !sessionId) {
      throw new Error("STATE_OBSERVATION_INDEX_PENDING: legacy session has no stable id");
    }
    const marker = await kv.get(KV.observationIdentities(sessionId), "__ready__");
    if (readyMarker(marker)) {
      progress.readySessions++;
      progress.sessionsCursor = page.next_cursor;
      if (page.next_cursor === null) progress.complete = true;
    } else {
      progress.sessionId = sessionId;
      progress.nextSessionCursor = page.next_cursor;
      progress.observationsCursor = null;
    }
    await saveState(kv, state);
    return;
  }

  const sessionId = progress.sessionId;
  let page;
  try {
    page = (await kv.pages<{ id: string }>(KV.observations(sessionId), {
      ...(progress.observationsCursor ? { cursor: progress.observationsCursor } : {}),
      limit: 4, maxBytes: 16_384, fields: ["id"],
    }).next()).value;
  } catch (error) {
    if (!isStaleCursor(error)) throw error;
    progress.observationsCursor = null;
    progress.conflicts++;
    await saveState(kv, state);
    return;
  }
  if (page) {
    for (const item of page.items) {
      if (typeof item.id !== "string" || !item.id) { progress.unindexable++; continue; }
      const row = await kv.getVersioned<Row>(KV.observations(sessionId), item.id);
      if (!row.exists || !row.value) continue;
      const identity = reconstructLegacyIdentity(row.value);
      if (!identity) { progress.unindexable++; progress.scanned++; continue; }
      if (identity.canonicalIdentity.length > 524_288) {
        throw new Error("STATE_OBSERVATION_INDEX_PENDING: reconstructible legacy row exceeds the bounded index request");
      }
      try {
        await triggerStateStorage<ObservationIndexResult>(sdk, {
          action: "observation_index", session_id: sessionId,
          observation_id: item.id, identity_digest: identity.identityDigest,
          canonical_identity: identity.canonicalIdentity, expected_version: row.version,
        });
        progress.indexed++;
      } catch (error) {
        if (errorCode(error) !== "STATE_TX_CONFLICT") throw error;
        progress.conflicts++;
        progress.observationsCursor = null;
        await saveState(kv, state);
        return;
      }
      progress.scanned++;
    }
    if (page.next_cursor) {
      progress.observationsCursor = page.next_cursor;
    } else {
      await kv.set(KV.observationIdentities(sessionId), "__ready__", { version: 1, ready: true });
      progress.readySessions++;
      progress.sessionId = null;
      progress.observationsCursor = null;
      progress.sessionsCursor = progress.nextSessionCursor;
      if (progress.nextSessionCursor === null) progress.complete = true;
      progress.nextSessionCursor = null;
    }
  }
  await saveState(kv, state);
}

function expiryMs(row: Row): number | null {
  const fields = ["completedAt", "endedAt", "updatedAt", "lastSeenAt", "lastAt", "occurredAt", "timestamp", "capturedAt", "createdAt", "startedAt"];
  let newest = Number.NEGATIVE_INFINITY;
  for (const field of fields) {
    if (!Object.hasOwn(row, field) || row[field] === undefined || row[field] === null) continue;
    if (typeof row[field] !== "string") return null;
    const at = Date.parse(row[field] as string);
    if (!Number.isFinite(at)) return null;
    newest = Math.max(newest, at);
  }
  if (Object.hasOwn(row, "at")) {
    if (typeof row.at !== "number" || !Number.isFinite(row.at)) return null;
    newest = Math.max(newest, row.at);
  }
  return Number.isFinite(newest) ? newest : null;
}

function olderThanRetention(row: Row, now: number): boolean {
  if (expiryMs(row) === null || hasRetentionProtection(row)) return false;
  const origin = terminalRetentionOrigin(row) ?? row.timestamp ?? row.occurredAt ?? row.capturedAt
    ?? row.createdAt ?? row.startedAt ?? row.lastAt
    ?? (typeof row.at === "number" && Number.isFinite(row.at) ? new Date(row.at).toISOString() : undefined);
  return retentionEligibility("completedBodies", { origin, dependenciesClosed: true }, now).eligible;
}

interface RetentionPolicy {
  scope: string;
  keyField: string;
  fields: string[];
  eligible: (row: Row, key: string, kv: StateKV) => Promise<boolean>;
}

const policies: RetentionPolicy[] = [
  { scope: KV.audit, keyField: "id", fields: ["id", "timestamp"], eligible: async () => true },
  { scope: KV.accessLog, keyField: "memoryId", fields: ["memoryId", "lastAt"], eligible: async () => true },
  { scope: KV.recentSearches, keyField: "sessionId", fields: ["sessionId", "at"], eligible: async () => true },
  { scope: KV.routineRuns, keyField: "id", fields: ["id", "status", "startedAt", "completedAt"], eligible: async (row) => row.status === "completed" },
  { scope: KV.signals, keyField: "id", fields: ["id", "createdAt", "readAt", "expiresAt"], eligible: async (row) => Boolean(row.readAt) || (typeof row.expiresAt === "string" && Date.parse(row.expiresAt) <= Date.now()) },
  { scope: KV.summaryQueueIntents, keyField: "sessionId", fields: ["sessionId", "createdAt", "lastAttemptAt"], eligible: async (row, key, kv) => {
    if (row.sessionId !== key || typeof row.lastAttemptAt !== "string") return false;
    const [activeJob, summary] = await Promise.all([
      kv.get(KV.summaryQueueActive, key), kv.get(KV.summaries, key),
    ]);
    return !activeJob && Boolean(summary);
  } },
];

async function staticCandidates(kv: StateKV, state: MaintenanceState, now: number): Promise<{
  candidates: StorageCandidate[]; unknownAge: number; unresolved: number;
}> {
  const policy = policies[state.technical.scopeIndex % policies.length]!;
  const cursor = state.technical.cursors[policy.scope] ?? undefined;
  let page;
  try {
    page = (await kv.pages<Row>(policy.scope, { ...(cursor ? { cursor } : {}), limit: 24, maxBytes: 65_536, fields: policy.fields }).next()).value;
  } catch (error) {
    if (!isStaleCursor(error)) throw error;
    state.technical.cursors[policy.scope] = null;
    await saveState(kv, state);
    return { candidates: [], unknownAge: 0, unresolved: 0 };
  }
  state.technical.cursors[policy.scope] = page?.next_cursor ?? null;
  if (!page?.next_cursor) state.technical.scopeIndex = (state.technical.scopeIndex + 1) % policies.length;
  const candidates: StorageCandidate[] = [];
  let unknownAge = 0;
  let unresolved = 0;
  for (const projected of page?.items ?? []) {
    const identity = projected[policy.keyField];
    if (typeof identity !== "string" || !identity) { unresolved++; continue; }
    const key = identity;
    if (!olderThanRetention(projected, now)) {
      if (expiryMs(projected) === null) unknownAge++;
      continue;
    }
    if (!(await policy.eligible(projected, key, kv))) { unresolved++; continue; }
    const current = await kv.getVersioned<Row>(policy.scope, key);
    if (!current.exists || !current.value || !olderThanRetention(current.value, now)
      || !(await policy.eligible(current.value, key, kv))) { unresolved++; continue; }
    candidates.push({ scope: policy.scope, key, expected_version: current.version });
    if (candidates.length >= 8) break;
  }
  await saveState(kv, state);
  return { candidates, unknownAge, unresolved };
}

async function temporalCandidates(kv: StateKV, state: MaintenanceState, now: number): Promise<{
  candidates: StorageCandidate[]; unknownAge: number; unresolved: number;
}> {
  const progress = state.technical.temporal ??= { cursor: null, jobId: null, pageIndex: 0 };
  const result = { candidates: [] as StorageCandidate[], unknownAge: 0, unresolved: 0 };
  if (!progress.jobId) {
    try {
      const page = (await kv.pages<Row>(KV.temporalGraphWork, { ...(progress.cursor ? { cursor: progress.cursor } : {}),
        limit: 1, maxBytes: 65_536, fields: ['version', 'id', 'state', 'pageCount', 'currentPage', 'cursor', 'activeUnitJobId', 'createdAt', 'updatedAt'] }).next()).value;
      progress.cursor = page?.next_cursor ?? null;
      const row = page?.items[0];
      if (row && typeof row.id === 'string' && /^temporaljob_[a-f0-9]{16}$/.test(row.id)) progress.jobId = row.id;
    } catch (error) {
      if (!isStaleCursor(error)) throw error;
      progress.cursor = null;
    }
    progress.pageIndex = 0;
  }
  if (!progress.jobId) return result;
  const key = `temporal-work:${progress.jobId}:manifest`;
  const parent = await kv.getVersioned<Row>(KV.temporalGraphWork, key);
  const row = parent.value;
  if (!row || row.version !== 1 || row.id !== progress.jobId || row.state !== 'completed' ||
      !Number.isSafeInteger(row.pageCount) || Number(row.pageCount) < 1 || row.currentPage !== row.pageCount ||
      row.activeUnitJobId != null || row.cursor != null || !olderThanRetention(row, now)) {
    if (row && expiryMs(row) === null) result.unknownAge++;
    else result.unresolved++;
    progress.jobId = null;
    return result;
  }
  const preserved = { scope: KV.temporalGraphWork, key, expected_version: parent.version };
  const pending = await kv.getVersioned(KV.temporalGraphPending, progress.jobId);
  if (pending.exists) result.candidates.push({ scope: KV.temporalGraphPending, key: progress.jobId, expected_version: pending.version, preserved });
  for (let probe = 0; probe < 4 && progress.pageIndex < Number(row.pageCount); probe++, progress.pageIndex++) {
    const pageKey = `temporal-work:${progress.jobId}:page:${progress.pageIndex}`;
    const page = await kv.getVersioned(KV.temporalGraphWork, pageKey);
    if (page.exists) result.candidates.push({ scope: KV.temporalGraphWork, key: pageKey, expected_version: page.version, preserved });
  }
  if (progress.pageIndex >= Number(row.pageCount)) {
    if (!pending.exists && result.candidates.length === 0) result.candidates.push({ scope: KV.temporalGraphWork, key, expected_version: parent.version });
    progress.jobId = null;
  }
  return result;
}

async function queueCandidates(kv: StateKV, state: MaintenanceState, now: number): Promise<{
  candidates: StorageCandidate[]; unknownAge: number; unresolved: number;
}> {
  const progress = state.technical;
  const candidates: StorageCandidate[] = [];
  let unknownAge = 0;
  let unresolved = 0;
  if (progress.summaryJobReady && progress.summaryJobId) {
    // Completed job identities remain available for replay and recovery.
    progress.summaryJobsCursor = progress.summaryJobsNextCursor;
    progress.summaryJobId = null;
    progress.summaryJobsNextCursor = null;
    progress.summaryUnitsCursor = null;
    progress.summaryJobReady = false;
    return { candidates, unknownAge, unresolved };
  }

  if (!progress.summaryJobId) {
    let page;
    try {
      page = (await kv.pages<Row>(KV.summaryQueueJobs, {
        ...(progress.summaryJobsCursor ? { cursor: progress.summaryJobsCursor } : {}),
        limit: 1, maxBytes: 8192, fields: ["id", "sessionId", "status", "createdAt", "updatedAt", "completedAt"],
      }).next()).value;
    } catch (error) {
      if (!isStaleCursor(error)) throw error;
      progress.summaryJobsCursor = null;
      return { candidates, unknownAge, unresolved };
    }
    if (!page?.items.length) {
      progress.summaryJobsCursor = null;
      return { candidates, unknownAge, unresolved };
    }
    progress.summaryJobsCursor = page.next_cursor;
    const projected = page.items[0]!;
    const id = projected.id;
    if (typeof id !== "string" || projected.status !== "completed") {
      if (projected.status === "failed") unresolved++;
      return { candidates, unknownAge, unresolved };
    }
    const job = await kv.getVersioned<Row>(KV.summaryQueueJobs, id);
    if (!job.value || job.value.status !== "completed" || !olderThanRetention(job.value, now)) {
      if (job.value && expiryMs(job.value) === null) unknownAge++;
      return { candidates, unknownAge, unresolved };
    }
    if (typeof job.value.sessionId !== "string" || job.value.dispatchPending === true
      || ["recoveryOfJobId", "zeroSpendRecoveryOfJobId", "recoveryMode", "recoveryGeneration"].some((field) => job.value?.[field] != null)) {
      unresolved++;
      return { candidates, unknownAge, unresolved };
    }
    const [summary, completed, effect] = await Promise.all([
      kv.get<Row>(KV.summaries, job.value.sessionId),
      kv.get<Row>(KV.summaryQueueCompleted, job.value.sessionId),
      kv.get<Row>(KV.state, `${id}:publication-effect`),
    ]);
    if (!summary || completed?.jobId !== id || completed.fingerprint !== job.value.snapshotFingerprint
      || JSON.stringify(effect?.summary) !== JSON.stringify(summary)) {
      unresolved++;
      return { candidates, unknownAge, unresolved };
    }
    const activeJob = await kv.get(KV.summaryQueueActive, job.value.sessionId);
    if (activeJob) { unresolved++; return { candidates, unknownAge, unresolved }; }
    progress.summaryJobId = id;
    progress.summaryJobsNextCursor = page.next_cursor;
    progress.summaryUnitsCursor = null;
  }

  const jobId = progress.summaryJobId;
  if (!jobId) return { candidates, unknownAge, unresolved };
  const unitScope = KV.summaryQueueUnits(jobId);
  let page;
  try {
    page = (await kv.pages<Row>(unitScope, {
      ...(progress.summaryUnitsCursor ? { cursor: progress.summaryUnitsCursor } : {}),
      limit: 12, maxBytes: 32_768, fields: ["id", "startedAt", "lastAttemptAt"],
    }).next()).value;
  } catch (error) {
    if (!isStaleCursor(error)) throw error;
    progress.summaryUnitsCursor = null;
    return { candidates, unknownAge, unresolved: unresolved + 1 };
  }
  for (const unit of page?.items ?? []) {
    if (typeof unit.id !== "string") { unresolved++; continue; }
    const current = await kv.getVersioned<Row>(unitScope, unit.id);
    if (!current.exists || !current.value || !olderThanRetention(current.value, now)) {
      if (current.value && expiryMs(current.value) === null) unknownAge++;
      continue;
    }
    const parent = await kv.getVersioned<Row>(KV.summaryQueueJobs, jobId);
    const parentRow = parent.value;
    if (!parentRow || parentRow.status !== "completed" || !olderThanRetention(parentRow, now)
      || typeof parentRow.sessionId !== "string" || await kv.get(KV.summaryQueueActive, parentRow.sessionId)) {
      unresolved++;
      continue;
    }
    candidates.push({ scope: unitScope, key: unit.id, expected_version: current.version,
      preserved: { scope: KV.summaryQueueJobs, key: jobId, expected_version: parent.version } });
    if (candidates.length >= 8) break;
  }
  progress.summaryUnitsCursor = page?.next_cursor ?? null;
  if (!page?.next_cursor) progress.summaryJobReady = true;
  return { candidates, unknownAge, unresolved };
}

async function operationsPrune(sdk: IIIClient, kv: StateKV, state: MaintenanceState): Promise<{ deleted: number; logicalBytes: number }> {
  const current = await kv.getVersioned(KV.recentOperations, "current");
  if (!current.exists || !current.value) {
    state.operationsCompactedAt = new Date().toISOString();
    return { deleted: 0, logicalBytes: 0 };
  }
  const result = await triggerStateStorage<{ deleted_count: number; deleted_logical_bytes: number }>(sdk, {
    action: "prune_operations", expected_version: current.version, max_ms: 1200,
  });
  state.operationsCompactedAt = new Date().toISOString();
  return { deleted: result.deleted_count, logicalBytes: result.deleted_logical_bytes };
}

function timestampAfter(now: number, delayMs: number): string {
  return new Date(now + delayMs).toISOString();
}

function updateReclaimState(
  state: MaintenanceState,
  result: StorageReclaimResult,
  now: number,
  mode: "ordinary" | "maintenance",
): void {
  const progress = state.compaction;
  if (mode === "ordinary") {
    progress.nextMaxPages = result.next_max_pages ?? progress.nextMaxPages;
    progress.minIntervalMs = Math.max(RECLAIM_MIN_INTERVAL_MS, result.min_interval_ms ?? progress.minIntervalMs);
  }
  progress.lastStatus = result.status;
  progress.lastReason = result.reason;
  progress.lastErrorCode = null;
  progress.backoffUntil = null;

  const hasReusablePages = result.after.freelist_count > 0;
  if (result.status === "progress") {
    progress.progressCuts++;
    progress.pending = hasReusablePages;
    progress.nextEligibleAt = timestampAfter(now, progress.pending ? progress.minIntervalMs : RECLAIM_IDLE_POLL_MS);
  } else if (result.status === "complete") {
    progress.completedPolls++;
    progress.pending = hasReusablePages;
    progress.nextEligibleAt = timestampAfter(now, progress.pending ? progress.minIntervalMs : RECLAIM_IDLE_POLL_MS);
  } else if (result.status === "migration_required") {
    progress.deferredPolls++;
    progress.pending = false;
    progress.nextEligibleAt = timestampAfter(now, RECLAIM_IDLE_POLL_MS);
  } else {
    progress.deferredPolls++;
    progress.pending = !["auto_vacuum_not_incremental", "auto_vacuum_none"].includes(result.reason);
    const delay = result.reason === "cooldown"
      ? progress.minIntervalMs
      : RECLAIM_IDLE_POLL_MS;
    progress.nextEligibleAt = timestampAfter(now, delay);
  }

  state.lastReclaim = {
    at: new Date(now).toISOString(), mode: result.mode,
    pages: result.pages_reclaimed, checkpointBusy: result.checkpoint_busy,
    before: result.before, after: result.after,
    status: result.status, reason: result.reason,
    pagesReclaimed: result.pages_reclaimed,
    physicalBytesReclaimed: result.physical_bytes_reclaimed,
    nextMaxPages: progress.nextMaxPages, minIntervalMs: progress.minIntervalMs,
    checkpointPerformed: result.checkpoint_performed,
  };
  appendHistory(state, {
    at: new Date(now).toISOString(), action: "reclaim", outcome: result.status,
    reason: result.reason, elapsedMs: result.elapsed_ms,
    pagesReclaimed: result.pages_reclaimed, physicalBytesReclaimed: result.physical_bytes_reclaimed,
    before: result.before, after: result.after,
  });
  if (result.physical_bytes_reclaimed > 0) {
    state.lastSuccessfulCompactionAt = new Date(now).toISOString();
  }
}

async function reclaim(
  sdk: IIIClient,
  kv: StateKV,
  state: MaintenanceState,
  lease: MaintenanceLease,
  force = false,
  mode: "ordinary" | "maintenance" = "ordinary",
): Promise<{ result: StorageReclaimResult | null; outcomeUnknown: boolean }> {
  const now = Date.now();
  const progress = state.compaction;
  const backoffUntil = progress.backoffUntil ? Date.parse(progress.backoffUntil) : 0;
  if (backoffUntil > now) return { result: null, outcomeUnknown: false };
  const nextEligibleAt = progress.nextEligibleAt ? Date.parse(progress.nextEligibleAt) : 0;
  if (mode === "ordinary" && !force && nextEligibleAt > now) return { result: null, outcomeUnknown: false };
  progress.attempts++;
  await saveState(kv, state);

  try {
    const renewed = await triggerMaintenanceLease(sdk, {
      action: "maintenance_lease_refresh", owner_id: lease.ownerId,
      fence: lease.fence, lease_ms: RECLAIM_OVERLAP_LEASE_MS,
    });
    if (renewed.status !== "refreshed" || renewed.fence !== lease.fence) {
      throw new StorageMaintenanceOutcomeUnknown("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
    }
    lease.expiresAt = renewed.expires_at;
    progress.overlap = { ownerId: lease.ownerId, startedAt: new Date(now).toISOString(), expiresAt: lease.expiresAt };
    const maxPages = mode === "maintenance" ? RECLAIM_MAINTENANCE_MAX_PAGES
      : Math.max(1, Math.min(RECLAIM_MAX_PAGES, progress.nextMaxPages));
    const result = await triggerFencedReclaim(sdk, lease, maxPages, mode);
    updateReclaimState(state, result, Date.now(), mode);
    return { result, outcomeUnknown: false };
  } catch (error) {
    const code = errorCode(error);
    progress.failures++;
    progress.pending = true;
    progress.lastStatus = "deferred";
    progress.lastReason = "storage_error";
    progress.lastErrorCode = code;
    progress.backoffUntil = timestampAfter(Date.now(), RECLAIM_ERROR_BACKOFF_MS);
    progress.nextEligibleAt = progress.backoffUntil;
    appendHistory(state, {
      at: new Date().toISOString(), action: "reclaim", outcome: "outcome_unknown",
      reason: "storage_error", elapsedMs: null, pagesReclaimed: null,
      physicalBytesReclaimed: null, errorCode: code,
    });
    logger.warn("Storage reclamation deferred", { code, retryAt: progress.nextEligibleAt });
    return { result: null, outcomeUnknown: true };
  } finally {
    await saveState(kv, state);
  }
}

async function migrateIncremental(
  sdk: IIIClient,
  kv: StateKV,
  state: MaintenanceState,
  maxMs: number,
): Promise<StorageMigrationResult> {
  const result = await triggerStateStorage<StorageMigrationResult>(sdk, {
    action: "migrate_incremental", allow_blocking: true, max_ms: maxMs,
  });
  const now = Date.now();
  state.compaction.pending = result.after.freelist_count > 0;
  state.compaction.lastStatus = "complete";
  state.compaction.lastReason = result.reason;
  state.compaction.lastErrorCode = null;
  state.compaction.backoffUntil = null;
  state.compaction.nextEligibleAt = timestampAfter(
    now,
    state.compaction.pending ? state.compaction.minIntervalMs : RECLAIM_IDLE_POLL_MS,
  );
  if (result.blocking_rebuild && result.after.database_bytes < result.before.database_bytes) {
    state.lastSuccessfulCompactionAt = new Date(now).toISOString();
  }
  appendHistory(state, {
    at: new Date(now).toISOString(), action: "migrate_incremental", outcome: result.status,
    reason: result.reason, elapsedMs: result.elapsed_ms,
    pagesReclaimed: Math.max(0, result.before.page_count - result.after.page_count),
    physicalBytesReclaimed: Math.max(0, result.before.database_bytes - result.after.database_bytes),
    before: result.before, after: result.after,
  });
  await saveState(kv, state);
  return result;
}

async function cleanupStep(sdk: IIIClient, kv: StateKV, state: MaintenanceState): Promise<StorageCleanupSummary> {
  const startedAt = new Date().toISOString();
  const now = Date.now();
  const graph = await pruneTerminalGraphWork(kv);
  const staticRows = await staticCandidates(kv, state, now);
  const queue = await queueCandidates(kv, state, now);
  const temporal = await temporalCandidates(kv, state, now);
  const candidates = [...temporal.candidates, ...staticRows.candidates, ...queue.candidates]
    .slice(0, MAX_CANDIDATES);
  let technical: TechnicalPruneResult = {
    deleted_count: 0, raw_stripped_count: 0, deleted_logical_bytes: 0,
    deleted_stored_bytes: 0, physical_bytes_reclaimed: null, skipped_count: 0,
    processed_count: 0, cutoff_at_ms: now - RETENTION_MS, done: true, input_logical_bytes: 0,
  };
  if (candidates.length) {
    technical = await triggerStateStorage<TechnicalPruneResult>(sdk, {
      action: "prune_technical", candidates, max_bytes: 1_048_576, max_ms: 1800,
    });
  }
  const ops = await operationsPrune(sdk, kv, state);
  const logicalBytes = graph.logicalBytes + technical.deleted_logical_bytes + ops.logicalBytes;
  const storedBytes = graph.storedBytes + technical.deleted_stored_bytes;
  const exclusions = [
    "Failed summary jobs and their partial units are retained; useful-output preservation is unresolved.",
    "Observation identity maps remain as durable indexes while canonical observations are retained.",
    "Observation event receipts remain permanent for deduplication and lost-acknowledgement recovery.",
    "Unknown or malformed timestamps and original observation fields without a separate exact retained copy are retained.",
    "Lifetime metrics and current health records are retained; their timestamps do not prove expiry.",
    "Both protected graph families graphjob_muzu6a4k_c7aa10a891eb and graphjob_mv0567t0_7c392765f08d remain excluded.",
  ];
  const summary: StorageCleanupSummary = {
    startedAt, completedAt: new Date().toISOString(), graphRowsDeleted: graph.deleted,
    technicalCandidatesProcessed: technical.processed_count, technicalDeleted: technical.deleted_count,
    rawStripped: technical.raw_stripped_count, deletedLogicalBytes: logicalBytes,
    deletedStoredBytes: storedBytes, operationRowsDeleted: ops.deleted,
    skipped: technical.skipped_count, excludedUnresolved: staticRows.unresolved + queue.unresolved + temporal.unresolved,
    excludedUnknownAge: staticRows.unknownAge + queue.unknownAge + temporal.unknownAge,
    pending: graph.pending || !state.observationIndex.complete,
    exclusions,
  };
  state.lastCleanup = summary;
  state.totals.graphRowsDeleted += summary.graphRowsDeleted;
  state.totals.technicalRowsDeleted += summary.technicalDeleted;
  state.totals.rawStripped += summary.rawStripped;
  state.totals.operationRowsDeleted += summary.operationRowsDeleted;
  state.totals.logicalBytesDeleted += summary.deletedLogicalBytes;
  state.totals.storedBytesDeleted += summary.deletedStoredBytes;
  state.totals.unresolvedExclusions += summary.excludedUnresolved;
  state.totals.unknownAgeExclusions += summary.excludedUnknownAge;
  if (logicalBytes > 0 || storedBytes > 0) {
    state.compaction.pending = true;
    state.compaction.nextEligibleAt = new Date().toISOString();
  }
  await saveState(kv, state);
  return summary;
}

function growth(state: MaintenanceState): Record<string, unknown> | null {
  const current = state.diagnostics.lastComplete;
  if (!current) return null;
  const previous = [...state.diagnostics.samples].reverse().find((sample) => sample.sampledAt !== current.sampledAt
    && sample.categoryVersion === current.categoryVersion && sample.coverage === current.coverage);
  if (!previous) return null;
  const sum = (rows: StorageGrowthSample["categories"], field: "logical_bytes" | "stored_bytes") => rows.reduce((total, row) => total + row[field], 0);
  return {
    from: previous.sampledAt, to: current.sampledAt,
    logicalBytes: sum(current.categories, "logical_bytes") - sum(previous.categories, "logical_bytes"),
    storedBytes: sum(current.categories, "stored_bytes") - sum(previous.categories, "stored_bytes"),
    databaseBytes: current.physical.database_bytes - previous.physical.database_bytes,
    walBytes: current.physical.wal_bytes - previous.physical.wal_bytes,
    comparable: true,
  };
}

export function registerStorageMaintenance(sdk: IIIClient, kv: StateKV, isForegroundBusy: () => boolean = () => false,
  foregroundPressureSnapshot?: () => StorageForegroundPressure, onGraphTurnGranted?: () => void,
  controllerSnapshot?: () => StorageMaintenanceControllerSnapshot): void {
  const run = async (request?: {
    action?: unknown; allow_blocking?: unknown; max_ms?: unknown; closed_owner_id?: unknown;
  }) => {
    const action = typeof request?.action === "string" ? request.action : "tick";
    let returnedContinuation: StorageMaintenanceContinuation | undefined;
    const respond = <T extends Record<string, unknown>>(value: T, next: StorageMaintenanceContinuation) => {
      returnedContinuation = next;
      return { ...value, continuation: next };
    };
    if (!["tick", "status", "diagnostics", "activate", "reclaim", "migrate_incremental", "collect_snapshot_temporary"].includes(action)) {
      return { success: false, code: "STATE_STORAGE_INVALID_ACTION" };
    }
    if (action === "status") {
      const state = await readState(kv);
      const ticket = layoutTurnTickets.get(kv);
      const lastPhysical = lastPhysicalObservations.get(kv);
      const localRuntime = {
        sampledAt: new Date().toISOString(),
        controller: controllerSnapshot?.() ?? null,
        maintenanceActive: active.has(kv),
        maintenanceDraining: maintenanceClosing.has(kv),
        layoutTicketPresent: ticket !== undefined,
        layoutTicketGranted: ticket?.turn?.granted ?? false,
        layoutTicketClaimed: ticket?.claimed ?? false,
        graphWorkQueue: peekGraphWorkQueue(kv),
        lastPhysicalWindow: lastPhysical === undefined ? null : { ...lastPhysical },
      };
      return { success: true, state, layoutLeaseContextRecoveryVersion: LAYOUT_LEASE_CONTEXT_RECOVERY_VERSION,
        localRuntime,
        retentionPolicy: STORAGE_RETENTION, retentionUseIntegration: RETENTION_USE_INTEGRATION, growth: growth(state), maintenanceReview: state.maintenanceReview };
    }
    if (action === "migrate_incremental" && (request?.allow_blocking !== true || !Number.isInteger(request.max_ms)
      || Number(request.max_ms) < 1 || Number(request.max_ms) > 86_400_000)) {
      return { success: false, code: "STATE_STORAGE_INVALID_REQUEST" };
    }
    if (action === "collect_snapshot_temporary" && (typeof request?.closed_owner_id !== "string"
      || !/^[0-9a-f-]{36}$/.test(request.closed_owner_id))) return { success: false, code: "STATE_STORAGE_INVALID_REQUEST" };
    if (active.has(kv)) {
      const waiting = layoutTurnTickets.get(kv);
      if (waiting?.turn?.granted && !waiting.claimed) releaseLayoutTurn(kv, waiting);
      return respond({ success: true, busy: true }, continuation("busy", "local_running"));
    }
    let layoutTicket: LayoutTurnTicket | undefined;
    if (action === "tick" || action === "activate" || action === "reclaim") {
      const observed = await readState(kv);
      if (isLayoutAdmissionAccepted(observed.layoutAdmission)) {
        let ticket = layoutTurnTickets.get(kv);
        if (!ticket) {
          ticket = { waited: false, claimed: false };
          const queued = ticket;
          layoutTurnTickets.set(kv, queued);
          queued.turn = requestGraphWorkTurn(kv, () => {
            if (!queued.waited || layoutTurnTickets.get(kv) !== queued || !queued.turn?.granted || queued.claimed) return;
            if (onGraphTurnGranted) {
              try { onGraphTurnGranted(); }
              catch { releaseLayoutTurn(kv, queued); }
              return;
            }
            void sdk.trigger({ function_id: "mem::storage-maintenance", payload: { action: "tick" } }).catch(() => {
              if (!queued.claimed) releaseLayoutTurn(kv, queued);
            });
          }, { category: "layout", functionId: "mem::storage-maintenance" });
          queued.waited = !queued.turn.granted;
        }
        if (ticket.claimed) return respond({ success: true, busy: true }, continuation("busy", "local_running"));
        if (!ticket.turn?.granted) return respond({ success: true, deferred: true, reason: "graph_turn_queued" },
          continuation("graph_queued", "graph_turn_queued", true, false, 60_000));
        ticket.claimed = true;
        layoutTicket = ticket;
      }
    }
    const pendingTurn = layoutTurnTickets.get(kv);
    if (pendingTurn && pendingTurn !== layoutTicket) return respond({ success: true, deferred: true, reason: "graph_turn_queued" },
      continuation("graph_queued", "graph_turn_queued", true, false, 60_000));
    if (active.has(kv)) {
      if (layoutTicket) releaseLayoutTurn(kv, layoutTicket);
      return respond({ success: true, busy: true }, continuation("busy", "local_running"));
    }
    active.add(kv);
    let state: MaintenanceState | null = null;
    let lease: MaintenanceLease | null = null;
    let preserveLeaseUntilExpiry = false;
    let unbindLayoutTurn: (() => void) | undefined;
    const ownerId = generateId("storage-maintenance");
    const startedAt = new Date().toISOString();
    const hold = (reason: string) => continuation("hold", reason, true, false,
      Math.max(60_000, Math.min(RECLAIM_OVERLAP_LEASE_MS,
        lease ? Date.parse(lease.expiresAt) - Date.now() : RECLAIM_OVERLAP_LEASE_MS)));
    try {
      const admitted = await triggerMaintenanceLease(sdk, {
        action: "maintenance_lease_acquire", owner_id: ownerId, lease_ms: RECLAIM_OVERLAP_LEASE_MS,
      });
      if (admitted.status === "busy") {
        state = await readState(kv);
        return respond({ success: true, busy: true, reason: "native_lease_busy", leaseExpiresAt: admitted.expires_at, state },
          continuation("busy", "native_lease_busy", true, false,
            Math.max(5_000, Math.min(RECLAIM_OVERLAP_LEASE_MS, Date.parse(admitted.expires_at) - Date.now()))));
      }
      if (admitted.status !== "acquired") {
        throw new StorageMaintenanceOutcomeUnknown("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
      }
      lease = { ownerId, fence: admitted.fence, expiresAt: admitted.expires_at };
      if (layoutTicket?.turn) unbindLayoutTurn = bindLayoutGraphTurn(lease, layoutTicket.turn);
      stateWriters.set(kv, { sdk, lease });
      state = await readState(kv);
      state.compaction.overlap = { ownerId, startedAt, expiresAt: lease.expiresAt };
      if (admitted.recovered_expired) {
        appendHistory(state, {
          at: new Date().toISOString(), action: "lease_recovery", outcome: "outcome_unknown",
          reason: "expired_owner", elapsedMs: null, pagesReclaimed: null, physicalBytesReclaimed: null,
        });
      }
      await saveState(kv, state);

      const refreshLease = async (): Promise<void> => {
        if (!lease || !state) throw new Error("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
        const refreshed = await triggerMaintenanceLease(sdk, {
          action: "maintenance_lease_refresh", owner_id: lease.ownerId,
          fence: lease.fence, lease_ms: RECLAIM_OVERLAP_LEASE_MS,
        });
        if (refreshed.status !== "refreshed" || refreshed.fence !== lease.fence) {
          throw new StorageMaintenanceOutcomeUnknown("STATE_STORAGE_MAINTENANCE_LEASE_FENCED");
        }
        lease.expiresAt = refreshed.expires_at;
        state.compaction.overlap = { ownerId: lease.ownerId, startedAt, expiresAt: lease.expiresAt };
      };

      if (action === "collect_snapshot_temporary") {
        if (!state.cleanupActivatedAt) return { success: false, code: "STATE_STORAGE_CLEANUP_NOT_ACTIVATED" };
        if (admitted.recovered_expired) return { success: false, deferred: true, code: "STATE_STORAGE_MAINTENANCE_RECOVERY_REQUIRED" };
        const closedOwner = request?.closed_owner_id;
        if (typeof closedOwner !== "string") throw new Error("STATE_STORAGE_INVALID_REQUEST");
        const result = await collectClosedSnapshotTemporary(kv, loadSnapshotConfig().dir, closedOwner, refreshLease);
        if (result.collected) await recordAudit(kv, "delete", "mem::storage-maintenance", [closedOwner], {
          resource: "snapshot_temporary", reason: "closed_owned_temporary",
        });
        return { success: true, result };
      }

      if (action === "activate") {
        if (!state.diagnostics.lastComplete || !state.observationIndex.complete) {
          return { success: false, code: "STATE_STORAGE_BASELINE_OR_INDEX_PENDING", state };
        }
        await refreshLease();
        state.cleanupActivatedAt ??= new Date().toISOString();
        await saveState(kv, state);
        return { success: true, activatedAt: state.cleanupActivatedAt, state };
      }
      if (action === "reclaim") {
        if (!state.cleanupActivatedAt) return { success: false, code: "STATE_STORAGE_CLEANUP_NOT_ACTIVATED" };
        await refreshLease();
        const outcome = await reclaim(sdk, kv, state, lease!, true);
        preserveLeaseUntilExpiry = outcome.outcomeUnknown;
        return outcome.result
          ? { success: true, result: outcome.result, state }
          : { success: true, deferred: true, state };
      }
      if (action === "migrate_incremental") {
        await refreshLease();
        const result = await migrateIncremental(sdk, kv, state, Number(request.max_ms));
        return { success: true, result, state };
      }
      if (action === "diagnostics") {
        await refreshLease();
        await diagnosticsStep(sdk, kv, state);
        return { success: true, state };
      }
      if (state.layoutAdmission?.acceptedByMain === true && state.periodicAdmission?.executionAuthorized === true) {
        await refreshLease();
        const physical = await runAdmittedPhysicalMaintenance({ sdk, kv, layoutCycle: state.layoutCycle,
          layoutAdmission: state.layoutAdmission, periodicCompaction: state.periodicCompaction,
          periodicAdmission: state.periodicAdmission, lease: lease!, refresh: refreshLease,
          persist: () => saveState(kv, state!),
          reclaimMaintenance: () => reclaim(sdk, kv, state!, lease!, true, "maintenance"), isForegroundBusy,
          foregroundPressureSnapshot });
        lastPhysicalObservations.set(kv, {
          sampleAt: new Date().toISOString(),
          layouts: /^[a-z_]{1,128}$/.test(physical.layouts) ? physical.layouts : null,
          periodic: /^[a-z_]{1,128}$/.test(physical.periodic) ? physical.periodic : null,
          cancellationReason: physical.cancellationReason,
          preserveLease: physical.preserveLease, progressMade: physical.progressMade,
          incomplete: physical.incomplete, reclaimed: physical.reclaimed, elapsedMs: physical.elapsedMs,
        });
        preserveLeaseUntilExpiry = physical.preserveLease;
        let next: StorageMaintenanceContinuation;
        if (physical.preserveLease || state.layoutCycle.recoveryRequired || state.layoutCycle.pending !== null || state.layoutCycle.pendingRaw !== null
          || state.periodicCompaction.recoveryRequired || state.periodicCompaction.pendingRequest !== null
          || state.periodicCompaction.pendingRequestRaw !== null) next = hold("native_reconciliation_required");
        else if (physical.cancellationReason === "foreground_pressure") next = continuation("foreground", "foreground_pressure",
          physical.incomplete, physical.progressMade, Math.max(1_000,
            Math.min(10_000, (physical.foregroundPressure?.quietWindowRemainingMs ?? 5_000) + 250)));
        else if (physical.incomplete && physical.progressMade) next = continuation("progress", "bounded_progress", true, true,
          Math.max(1_000, Math.min(10_000, Math.ceil(physical.elapsedMs / 4))));
        else if (physical.incomplete) next = continuation("busy", "no_progress");
        else next = continuation(state.periodicCompaction.lastDisposition === "automatic_cycle_complete" ? "complete" : "idle",
          "cycle_idle", false, physical.progressMade, 60_000);
        return respond({ success: true, physicalOnly: true, physical,
          retentionCleanupActivated: Boolean(state.cleanupActivatedAt), state }, next);
      }
      if (!state.diagnostics.lastComplete || state.observationIndex.complete) {
        await refreshLease();
        await diagnosticsStep(sdk, kv, state);
      }
      await refreshLease();
      await observationIndexStep(sdk, kv, state);
      state = await readState(kv);
      let cleanup: StorageCleanupSummary | null = null;
      let compaction: StorageReclaimResult | null = null;
      if (state.cleanupActivatedAt) {
        await refreshLease();
        cleanup = await cleanupStep(sdk, kv, state);
        const sample = state.diagnostics.lastComplete;
        if (sample) {
          const layouts = await runPeriodicLayouts({sdk,cycle:state.layoutCycle,admission:state.layoutAdmission,sample,
            lease:lease!,refresh:refreshLease,persist:()=>saveState(kv,state!)});
          if (layouts.preserveLease) {preserveLeaseUntilExpiry=true;return {success:true,deferred:true,layouts,state};}
          const periodic = await runPeriodicCompactionCandidate({
            sdk, kv, progress: state.periodicCompaction,
            layoutComplete: layouts.disposition === "cycle_complete"
              || (!state.layoutCycle.active && state.layoutCycle.familyIndex === 3
                && state.layoutCycle.pending === null && !state.layoutCycle.recoveryRequired),
            sample,
            admission: state.periodicAdmission ?? {
              baselineBytes: 5_711_050_488, minimumNetBytes: 285_552_525,
              aggregateCandidateAccepted: false, executionAuthorized: false,
              conservativeCombinedNetBytes: 0, runtimeCompatible: false,
            },
            lease: lease!, refreshSameLease: refreshLease,
            persistProgress: () => saveState(kv,state!), isCancelled: () => false,
          }).catch(async error => { await markWorkflowStatisticsGap(kv, "maintenance"); throw error; });
          preserveLeaseUntilExpiry = preservePeriodicLease(periodic.disposition)
            || state.periodicCompaction.pendingRequest !== null || state.periodicCompaction.recoveryRequired;
          if (preserveLeaseUntilExpiry) return { success: true, deferred: true, periodic, state };
        }
        await refreshLease();
        const outcome = await reclaim(sdk, kv, state, lease!);
        compaction = outcome.result;
        preserveLeaseUntilExpiry = outcome.outcomeUnknown;
      }
      return respond({ success: true, baselineComplete: Boolean(state.diagnostics.lastComplete), indexComplete: state.observationIndex.complete,
        cleanupActivated: Boolean(state.cleanupActivatedAt), cleanup,
        compaction: compaction?.status ?? state.compaction.lastStatus,
        state, maintenanceReview: state.maintenanceReview }, preserveLeaseUntilExpiry
          ? hold("native_reconciliation_required") : continuation("idle", "ordinary_maintenance", false, false, 60_000));
    } catch (error) {
      const code = errorCode(error);
      preserveLeaseUntilExpiry = lease !== null || error instanceof StorageMaintenanceOutcomeUnknown
        || preserveLeaseUntilExpiry;
      if (state) {
        state.compaction.failures++;
        state.compaction.pending = true;
        state.compaction.lastStatus = "deferred";
        state.compaction.lastReason = "storage_error";
        state.compaction.lastErrorCode = code;
        state.compaction.backoffUntil = timestampAfter(Date.now(), RECLAIM_ERROR_BACKOFF_MS);
        state.compaction.nextEligibleAt = state.compaction.backoffUntil;
        if (lease) state.compaction.overlap = { ownerId, startedAt, expiresAt: lease.expiresAt };
        appendHistory(state, {
          at: new Date().toISOString(), action: action as MaintenanceHistoryEntry["action"],
          outcome: preserveLeaseUntilExpiry ? "outcome_unknown" : "error", reason: "storage_error",
          elapsedMs: null, pagesReclaimed: null, physicalBytesReclaimed: null, errorCode: code,
        });
        await saveState(kv, state).catch(() => undefined);
      }
      logger.warn("Storage maintenance step paused", { code });
      return respond({ success: false, code, ...(state ? { state } : {}) }, hold("storage_error"));
    } finally {
      if (returnedContinuation && preserveLeaseUntilExpiry)
        Object.assign(returnedContinuation, hold("native_reconciliation_required"));
      if (lease && state && !preserveLeaseUntilExpiry) {
        try {
          state.compaction.overlap = null;
          await saveState(kv, state, true);
        } catch (error) {
          if (returnedContinuation) Object.assign(returnedContinuation, hold("lease_release_unknown"));
          const code = errorCode(error);
          state.compaction.overlap = { ownerId, startedAt, expiresAt: lease.expiresAt };
          state.compaction.nextEligibleAt = lease.expiresAt;
          appendHistory(state, {
            at: new Date().toISOString(), action: "lease_release", outcome: "outcome_unknown",
            reason: "release_failed", elapsedMs: null, pagesReclaimed: null,
            physicalBytesReclaimed: null, errorCode: code,
          });
          logger.warn("Storage maintenance lease release deferred", { code, retryAt: lease.expiresAt });
          // An unknown final-save acknowledgement may already have released this fence.
          // Preserve the previously durable overlap; never redispatch the snapshot.
        }
      } else if (lease && state) {
        state.compaction.overlap = { ownerId, startedAt, expiresAt: lease.expiresAt };
        await saveState(kv, state).catch(() => undefined);
      }
      active.delete(kv);
      stateWriters.delete(kv);
      unbindLayoutTurn?.();
      if (layoutTicket) releaseLayoutTurn(kv, layoutTicket);
    }
  };
  sdk.registerFunction("mem::storage-maintenance", (request?: {
    action?: unknown; allow_blocking?: unknown; max_ms?: unknown; closed_owner_id?: unknown;
  }) => {
    if (request?.action === "status") return run(request);
    if (maintenanceClosing.has(kv)) return Promise.resolve({ success: true, deferred: true, reason: "shutdown_draining",
      continuation: continuation("hold", "shutdown_draining", true, false, 300_000) });
    let runs = maintenanceRuns.get(kv);
    if (!runs) { runs = new Set(); maintenanceRuns.set(kv, runs); }
    const admittedRuns = runs;
    const pending = run(request);
    admittedRuns.add(pending);
    void pending.then(() => admittedRuns.delete(pending), () => admittedRuns.delete(pending));
    return pending;
  });
}
