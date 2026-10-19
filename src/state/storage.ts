import { isNativeCompactionRequest, validateNativeCompactionRequest, validNativeCompactionResponse, type NativeCompactionRequest } from "./native-compaction.js";
import type { IIIClient } from "iii-sdk";
import type { StorageMaintenanceState } from "../types.js";
import { isGraphAutomaticRecoveryRequest, validateGraphAutomaticRecoveryRequest, createGraphAutomaticRecoveryTransport,
  type GraphAutomaticRecoveryRequest } from "./graph-automatic-recovery.js";
import { isProviderBudgetRequest, validateProviderBudgetRequest, validProviderBudgetResponse,
  type ProviderBudgetRequest } from "./provider-attempt-budget.js";

export interface StorageCursor {
  scope: string;
  key: string;
}

export interface StoragePhysicalStats {
  page_size: number;
  page_count: number;
  freelist_count: number;
  reusable_bytes: number;
  allocated_page_bytes: number;
  auto_vacuum: 0 | 1 | 2;
  journal_mode: string;
  database_bytes: number;
  wal_bytes: number;
}

export interface StorageCategoryStats {
  category: string;
  rows: number;
  logical_bytes: number;
  stored_bytes: number;
  tombstones: number;
}

export interface StorageDiagnostics {
  category_version: number;
  sampled_at: string;
  consistency: "keyset_live";
  complete: boolean;
  done: boolean;
  rows_scanned: number;
  categories: StorageCategoryStats[];
  cursor: StorageCursor | null;
  physical: StoragePhysicalStats;
  coverage: string;
}

export interface GraphStorageMetadata {
  status: "ready" | "unavailable" | "preparing" | "stale";
  source: "native_scoped_metadata" | "unavailable";
  complete: boolean;
  rows: number | null;
  logical_bytes: number | null;
  stored_bytes: number | null;
  sampled_at: string | null;
  scope_count: number;
  coverage: string;
  reason?: "metadata_not_prepared" | "metadata_preparing" | "metadata_stale" | "metadata_limit_exceeded";
}

export interface GraphStorageMetadataPrepareOptions {
  limit: number;
  max_bytes: number;
  max_ms: number;
}

export interface GraphStorageMetadataPrepareResult {
  status: "complete" | "preparing" | "stale" | "unavailable";
  complete: boolean;
  rows: number | null;
  logical_bytes: number | null;
  stored_bytes: number | null;
  sampled_at: string | null;
  scope_count: number;
  scopes_completed: number;
  rows_scanned: number;
  reason?: "metadata_stale" | "metadata_limit_exceeded";
}

export interface StorageCandidate {
  scope: string;
  key: string;
  expected_version: string;
  preserved?: {
    scope: string;
    key: string;
    expected_version: string;
  };
}

export interface RetentionResourceReference {
  scope: string;
  key: string;
  expected_version: string;
}
export const RETENTION_CONTEXT_USE_LIMITS = {
  maxResources: 128,
  maxReferenceBytes: 4096,
  maxRequestBytes: 1_048_576,
  replayWindowMs: 86_400_000,
  futureSkewMs: 60_000,
} as const;
export interface RetentionContextDeliveryRequest {
  action: "retention_context_delivery";
  event_id: string;
  task_id: string;
  project: string;
  resources: RetentionResourceReference[];
}
export interface RetentionContextDeliveryResult {
  event_id: string;
  observed_at: string;
  admitted: number;
  skipped: number;
  replayed: boolean;
}

export type StorageMaintenanceLeaseRequest =
  | { action: "maintenance_lease_acquire"; owner_id: string; lease_ms: number }
  | { action: "maintenance_lease_refresh"; owner_id: string; fence: number; lease_ms: number }
  | { action: "maintenance_lease_release"; owner_id: string; fence: number };

export type StorageMaintenanceLeaseResponse =
  | { status: "acquired"; fence: number; expires_at: string; recovered_expired: boolean }
  | { status: "busy"; fence: number; expires_at: string }
  | { status: "refreshed"; fence: number; expires_at: string }
  | { status: "released"; fence: number }
  | { status: "stale"; fence: number | null; expires_at: string | null };

export type StorageMaintenanceSnapshot = StorageMaintenanceState & { maintenanceHistory: unknown[] };

export type StorageReclaimRequest =
  | {
      action: "reclaim";
      maintenance?: never;
      checkpoint?: never;
      max_pages: number;
      max_ms: number;
      lease_owner_id: string;
      lease_fence: number;
    }
  | {
      action: "reclaim";
      maintenance: true;
      checkpoint: "truncate";
      max_pages: number;
      max_ms: number;
      lease_owner_id: string;
      lease_fence: number;
    };

export type StateStorageRequest =
  | { action: "physical" }
  | GraphAutomaticRecoveryRequest
  | NativeCompactionRequest
  | ProviderBudgetRequest
  | RetentionContextDeliveryRequest
  | StorageMaintenanceLeaseRequest
  | { action: "maintenance_state_save"; owner_id: string; fence: number; value: StorageMaintenanceSnapshot; release: boolean }
  | { action: "diagnostics"; cursor?: StorageCursor; limit: number; max_bytes: number; max_ms: number }
  | { action: "graph_metadata" }
  | ({ action: "graph_metadata_prepare" } & GraphStorageMetadataPrepareOptions)
  | { action: "prune_technical"; candidates: StorageCandidate[]; max_bytes: number; max_ms: number }
  | { action: "prune_operations"; expected_version: string; max_ms: number }
  | StorageReclaimRequest
  | { action: "migrate_incremental"; allow_blocking: true; max_ms: number }
  | {
      action: "observation_index";
      session_id: string;
      observation_id: string;
      identity_digest: string;
      canonical_identity: string;
      expected_version: string;
    };

export interface TechnicalPruneResult {
  deleted_count: number;
  raw_stripped_count: number;
  deleted_logical_bytes: number;
  deleted_stored_bytes: number;
  physical_bytes_reclaimed: number | null;
  skipped_count: number;
  processed_count: number;
  cutoff_at_ms: number;
  done: boolean;
  input_logical_bytes: number;
}

export interface OperationsPruneResult {
  deleted_count: number;
  retained_count: number;
  deleted_logical_bytes: number;
  version: string;
}

interface StorageReclaimResultBase {
  mode: "incremental" | "checkpoint_only";
  requires_explicit_full_rebuild: boolean;
  checkpoint_busy: boolean;
  wal_frames: number;
  checkpointed_frames: number;
  checkpoint_performed: boolean;
  status: "progress" | "deferred" | "migration_required" | "complete";
  reason: string;
  elapsed_ms: number;
  pages_reclaimed: number;
  physical_bytes_reclaimed: number;
  before: StoragePhysicalStats;
  after: StoragePhysicalStats;
}

export interface StorageOrdinaryReclaimResult extends StorageReclaimResultBase {
  maintenance?: undefined;
  max_chunk_pages?: undefined;
  checkpoint_performed: false;
  next_max_pages?: number;
  min_interval_ms?: number;
}

export interface StorageMaintenanceReclaimResult extends StorageReclaimResultBase {
  maintenance: true;
  min_interval_ms: 0;
  max_chunk_pages: 128;
  next_max_pages?: never;
}

export type StorageReclaimResult = StorageOrdinaryReclaimResult | StorageMaintenanceReclaimResult;

function validReclaimResponse(request: StorageReclaimRequest, value: Record<string, unknown>): boolean {
  const common = (value.mode === "incremental" || value.mode === "checkpoint_only")
    && typeof value.requires_explicit_full_rebuild === "boolean"
    && typeof value.checkpoint_busy === "boolean"
    && isCount(value.wal_frames)
    && isCount(value.checkpointed_frames)
    && ["progress", "deferred", "migration_required", "complete"].includes(String(value.status))
    && typeof value.reason === "string"
    && isCount(value.elapsed_ms)
    && isCount(value.pages_reclaimed)
    && isCount(value.physical_bytes_reclaimed)
    && isPhysical(value.before)
    && isPhysical(value.after);
  if (!common) return false;
  if (request.maintenance === true) {
    return value.maintenance === true
      && value.min_interval_ms === 0
      && value.max_chunk_pages === 128
      && typeof value.checkpoint_performed === "boolean"
      && value.next_max_pages === undefined;
  }
  return value.maintenance === undefined
    && value.max_chunk_pages === undefined
    && value.checkpoint_performed === false
    && (value.next_max_pages === undefined || (isCount(value.next_max_pages)
      && value.next_max_pages >= 1 && value.next_max_pages <= 16))
    && (value.min_interval_ms === undefined || isCount(value.min_interval_ms));
}

export interface StorageMigrationResult {
  status: "complete";
  reason: string;
  elapsed_ms: number;
  blocking_rebuild?: boolean;
  before: StoragePhysicalStats;
  after: StoragePhysicalStats;
}

export interface ObservationIndexResult {
  indexed: boolean;
  canonical_id: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isCount(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) >= 0;
}

function isVersion(value: unknown): value is string {
  return typeof value === "string" && /^(0|[1-9][0-9]{0,19})$/.test(value);
}

function isTimestamp(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function isMaintenanceOwner(value: unknown): value is string {
  return typeof value === "string" && value.length >= 1 && value.length <= 128
    && /^[A-Za-z0-9._:-]+$/.test(value);
}

function isFence(value: unknown): value is number {
  return isCount(value) && value > 0;
}

function isPhysical(value: unknown): value is StoragePhysicalStats {
  if (!isRecord(value)) return false;
  const autoVacuum = value.auto_vacuum;
  return isCount(value.page_size)
    && isCount(value.page_count)
    && isCount(value.freelist_count)
    && isCount(value.reusable_bytes)
    && isCount(value.allocated_page_bytes)
    && (autoVacuum === 0 || autoVacuum === 1 || autoVacuum === 2)
    && typeof value.journal_mode === "string"
    && isCount(value.database_bytes)
    && isCount(value.wal_bytes);
}

function validResponse(request: StateStorageRequest, value: unknown): boolean {
  if (!isRecord(value)) return false;
  const action = request.action;
  if (action === "physical") return isPhysical(value);
  if (action === "retention_context_delivery") {
    return typeof value.event_id === "string" && isTimestamp(value.observed_at)
      && isCount(value.admitted) && isCount(value.skipped) && typeof value.replayed === "boolean";
  }
  if (action === "maintenance_state_save") return value.status === "saved" && isFence(value.fence)
    && isVersion(value.version) && value.version !== "0" && typeof value.released === "boolean";
  if (action === "maintenance_lease_acquire") {
    return isFence(value.fence) && isTimestamp(value.expires_at)
      && (value.status === "busy" || (value.status === "acquired" && typeof value.recovered_expired === "boolean"));
  }
  if (action === "maintenance_lease_refresh" || action === "maintenance_lease_release") {
    if (value.status === "stale") return (value.fence === null || isFence(value.fence))
      && (value.expires_at === null || isTimestamp(value.expires_at));
    return isFence(value.fence) && (action === "maintenance_lease_refresh"
      ? value.status === "refreshed" && isTimestamp(value.expires_at) : value.status === "released");
  }
  if (action === "graph_metadata") {
    if (value.scope_count !== 7 || typeof value.coverage !== "string") return false;
    if (value.status === "ready") {
      return value.source === "native_scoped_metadata" && value.complete === true
        && isCount(value.rows) && isCount(value.logical_bytes) && isCount(value.stored_bytes)
        && isTimestamp(value.sampled_at);
    }
    return ["unavailable", "preparing", "stale"].includes(String(value.status))
      && value.source === "unavailable" && value.complete === false
      && value.rows === null && value.logical_bytes === null && value.stored_bytes === null
      && value.sampled_at === null
      && ["metadata_not_prepared", "metadata_preparing", "metadata_stale", "metadata_limit_exceeded"].includes(String(value.reason));
  }
  if (action === "graph_metadata_prepare") {
    const common = value.scope_count === 7 && isCount(value.scopes_completed)
      && value.scopes_completed <= 7 && isCount(value.rows_scanned)
      && typeof value.complete === "boolean";
    if (!common) return false;
    if (value.status === "complete") {
      return value.complete === true && value.scopes_completed === 7
        && isCount(value.rows) && isCount(value.logical_bytes) && isCount(value.stored_bytes)
        && isTimestamp(value.sampled_at);
    }
    return ["preparing", "stale", "unavailable"].includes(String(value.status)) && value.complete === false
      && value.rows === null && value.logical_bytes === null && value.stored_bytes === null
      && value.sampled_at === null
      && (value.status !== "stale" || value.reason === "metadata_stale")
      && (value.status !== "unavailable" || value.reason === "metadata_limit_exceeded");
  }
  if (action === "diagnostics") {
    return isCount(value.category_version)
      && typeof value.sampled_at === "string"
      && value.consistency === "keyset_live"
      && typeof value.complete === "boolean"
      && typeof value.done === "boolean"
      && isCount(value.rows_scanned)
      && Array.isArray(value.categories)
      && value.categories.every((row) => isRecord(row)
        && typeof row.category === "string"
        && isCount(row.rows)
        && isCount(row.logical_bytes)
        && isCount(row.stored_bytes)
        && isCount(row.tombstones))
      && (value.cursor === null || (isRecord(value.cursor)
        && typeof value.cursor.scope === "string"
        && typeof value.cursor.key === "string"))
      && isPhysical(value.physical)
      && typeof value.coverage === "string";
  }
  if (action === "prune_technical") {
    return isCount(value.deleted_count)
      && isCount(value.raw_stripped_count)
      && isCount(value.deleted_logical_bytes)
      && isCount(value.deleted_stored_bytes)
      && (value.physical_bytes_reclaimed === null || isCount(value.physical_bytes_reclaimed))
      && isCount(value.skipped_count)
      && isCount(value.processed_count)
      && isCount(value.cutoff_at_ms)
      && typeof value.done === "boolean"
      && isCount(value.input_logical_bytes);
  }
  if (action === "prune_operations") {
    return isCount(value.deleted_count)
      && isCount(value.retained_count)
      && isCount(value.deleted_logical_bytes)
      && isVersion(value.version);
  }
  if (request.action === "reclaim") return validReclaimResponse(request, value);
  if (action === "migrate_incremental") {
    return value.status === "complete"
      && typeof value.reason === "string"
      && isCount(value.elapsed_ms)
      && (value.blocking_rebuild === undefined || typeof value.blocking_rebuild === "boolean")
      && isPhysical(value.before)
      && isPhysical(value.after);
  }
  return typeof value.indexed === "boolean" && typeof value.canonical_id === "string";
}

function validateRequest(request: StateStorageRequest): void {
  if (request.action === "physical") return;
  if (request.action === "retention_context_delivery") {
    const validText = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0
      && Buffer.byteLength(value, "utf8") <= RETENTION_CONTEXT_USE_LIMITS.maxReferenceBytes;
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(request.event_id)
      || !validText(request.task_id) || !validText(request.project) || !Array.isArray(request.resources)
      || request.resources.length < 1 || request.resources.length > RETENTION_CONTEXT_USE_LIMITS.maxResources
      || Object.keys(request).some(key => !["action", "event_id", "task_id", "project", "resources"].includes(key))) {
      throw new Error("STATE_STORAGE_INVALID_REQUEST");
    }
    const seen = new Set<string>();
    for (const resource of request.resources) {
      if (!resource || !validText(resource.scope) || !resource.scope.startsWith("mem:obs:") || resource.scope.length <= 8
        || !validText(resource.key) || !isVersion(resource.expected_version) || resource.expected_version === "0"
        || BigInt(resource.expected_version) > 18_446_744_073_709_551_615n
        || Object.keys(resource).some(key => !["scope", "key", "expected_version"].includes(key))) throw new Error("STATE_STORAGE_INVALID_REQUEST");
      const identity = JSON.stringify([resource.scope, resource.key]);
      if (seen.has(identity)) throw new Error("STATE_RETENTION_RESOURCE_VERSION_CONFLICT");
      seen.add(identity);
    }
    if (Buffer.byteLength(JSON.stringify(request), "utf8") > RETENTION_CONTEXT_USE_LIMITS.maxRequestBytes) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    return;
  }
  if (isGraphAutomaticRecoveryRequest(request)) { validateGraphAutomaticRecoveryRequest(request); return; }
  if (isNativeCompactionRequest(request)) {validateNativeCompactionRequest(request);return;}
  if (isProviderBudgetRequest(request)) {
    validateProviderBudgetRequest(request);
    return;
  }
  if (request.action === "maintenance_state_save") {
    const value = request.value as unknown;
    if (!isMaintenanceOwner(request.owner_id) || !isFence(request.fence) || typeof request.release !== "boolean"
      || !isRecord(value) || value.version !== 1 || !isTimestamp(value.updatedAt)
      || ![value.compaction, value.totals, value.diagnostics, value.observationIndex, value.technical].every(isRecord)
      || !Array.isArray(value.maintenanceHistory) || value.maintenanceHistory.length > 30
      || Buffer.byteLength(JSON.stringify(value), "utf8") > 1_048_576) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    return;
  }
  if (request.action === "maintenance_lease_acquire" || request.action === "maintenance_lease_refresh"
    || request.action === "maintenance_lease_release") {
    if (!isMaintenanceOwner(request.owner_id)
      || (request.action !== "maintenance_lease_acquire" && !isFence(request.fence))
      || (request.action !== "maintenance_lease_release" && (!Number.isInteger(request.lease_ms)
        || request.lease_ms < 1000 || request.lease_ms > 300_000))) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    return;
  }
  if (request.action === "diagnostics") {
    if (!Number.isInteger(request.limit) || request.limit < 1 || request.limit > 1024
      || !Number.isInteger(request.max_bytes) || request.max_bytes < 1024 || request.max_bytes > 1_048_576
      || !Number.isInteger(request.max_ms) || request.max_ms < 1 || request.max_ms > 3000) {
      throw new Error("STATE_STORAGE_INVALID_REQUEST");
    }
    if (request.cursor && (!request.cursor.scope || !request.cursor.key)) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    return;
  }
  if (request.action === "graph_metadata") return;
  if (request.action === "graph_metadata_prepare") {
    if (!Number.isInteger(request.limit) || request.limit < 1 || request.limit > 1024
      || !Number.isInteger(request.max_bytes) || request.max_bytes < 8192 || request.max_bytes > 1_048_576
      || !Number.isInteger(request.max_ms) || request.max_ms < 1 || request.max_ms > 3000) {
      throw new Error("STATE_STORAGE_INVALID_REQUEST");
    }
    return;
  }
  if (request.action === "prune_technical") {
    if (!Array.isArray(request.candidates) || request.candidates.length > 128
      || !Number.isInteger(request.max_bytes) || request.max_bytes < 1 || request.max_bytes > 4_194_304
      || !Number.isInteger(request.max_ms) || request.max_ms < 1 || request.max_ms > 3000) {
      throw new Error("STATE_STORAGE_INVALID_REQUEST");
    }
    for (const candidate of request.candidates) {
      if (!candidate || !candidate.scope || !candidate.key || !isVersion(candidate.expected_version)
        || (candidate.preserved !== undefined && (!candidate.preserved.scope || !candidate.preserved.key
          || !isVersion(candidate.preserved.expected_version)))) {
        throw new Error("STATE_STORAGE_INVALID_REQUEST");
      }
    }
    return;
  }
  if (request.action === "prune_operations") {
    if (!isVersion(request.expected_version) || !Number.isInteger(request.max_ms)
      || request.max_ms < 1 || request.max_ms > 3000) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    return;
  }
  if (request.action === "reclaim") {
    const maintenance = request.maintenance === true;
    const requestKeys = maintenance
      ? ["action", "maintenance", "checkpoint", "max_pages", "max_ms", "lease_owner_id", "lease_fence"]
      : ["action", "max_pages", "max_ms", "lease_owner_id", "lease_fence"];
    const keys = Object.keys(request);
    if ((maintenance && request.checkpoint !== "truncate")
      || keys.length !== requestKeys.length || keys.some(key => !requestKeys.includes(key))
      || !isMaintenanceOwner(request.lease_owner_id) || !isFence(request.lease_fence)
      || !Number.isInteger(request.max_pages) || request.max_pages < 1 || request.max_pages > (maintenance ? 4096 : 16)
      || !Number.isInteger(request.max_ms) || request.max_ms < 1 || request.max_ms > (maintenance ? 3000 : 25)) {
      throw new Error("STATE_STORAGE_INVALID_REQUEST");
    }
    return;
  }
  if (request.action === "migrate_incremental") {
    if (request.allow_blocking !== true || !Number.isInteger(request.max_ms)
      || request.max_ms < 1 || request.max_ms > 86_400_000) {
      throw new Error("STATE_STORAGE_INVALID_REQUEST");
    }
    return;
  }
  if (!request.session_id || !request.observation_id || !/^[a-f0-9]{64}$/i.test(request.identity_digest)
    || !request.canonical_identity || !isVersion(request.expected_version)) {
    throw new Error("STATE_STORAGE_INVALID_REQUEST");
  }
}

export async function triggerStateStorage<T extends object>(
  sdk: IIIClient,
  request: StateStorageRequest,
): Promise<T> {
  validateRequest(request);
  if (isGraphAutomaticRecoveryRequest(request)) return await createGraphAutomaticRecoveryTransport(sdk)(request.request) as T;
  const result = await sdk.trigger<StateStorageRequest, unknown>({
    function_id: "state::storage",
    payload: request,
    ...(request.action === "retention_context_delivery" ? { timeoutMs: 5000 } : {}),
  });
  if (isNativeCompactionRequest(request)) {
    if (!validNativeCompactionResponse(request,result)) throw new Error("STATE_STORAGE_INVALID_RESPONSE");
    return result as T;
  }
  if (isProviderBudgetRequest(request)) {
    if (!validProviderBudgetResponse(request, result)) throw new Error("STATE_STORAGE_INVALID_RESPONSE");
    return result as T;
  }
  if (!validResponse(request, result)) throw new Error("STATE_STORAGE_INVALID_RESPONSE");
  if (request.action === "retention_context_delivery") {
    const admitted = result as RetentionContextDeliveryResult;
    if (admitted.event_id !== request.event_id || admitted.admitted + admitted.skipped !== request.resources.length) {
      throw new Error("STATE_STORAGE_INVALID_RESPONSE");
    }
  }
  if (request.action === "maintenance_state_save") {
    const saved = result as { fence: number; released: boolean };
    if (saved.fence !== request.fence || saved.released !== request.release) throw new Error("STATE_STORAGE_INVALID_RESPONSE");
  }
  if (request.action === "reclaim" && (result as StorageReclaimResult).pages_reclaimed > request.max_pages) {
    throw new Error("STATE_STORAGE_INVALID_RESPONSE");
  }
  if ((request.action === "maintenance_lease_refresh" || request.action === "maintenance_lease_release")
    && (result as StorageMaintenanceLeaseResponse).status !== "stale"
    && (result as StorageMaintenanceLeaseResponse).fence !== request.fence) {
    throw new Error("STATE_STORAGE_INVALID_RESPONSE");
  }
  if (request.action === "prune_technical" && (result as TechnicalPruneResult).processed_count > request.candidates.length) {
    throw new Error("STATE_STORAGE_INVALID_RESPONSE");
  }
  if (request.action === "graph_metadata_prepare"
    && (result as GraphStorageMetadataPrepareResult).rows_scanned > request.limit) {
    throw new Error("STATE_STORAGE_INVALID_RESPONSE");
  }
  return result as T;
}

export async function triggerSnapshotStorage(
  sdk: IIIClient, request: import("./snapshot-read-view.js").SnapshotReadRequest, timeoutMs: number,
): Promise<unknown> {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300_000) throw new Error("STATE_SNAPSHOT_INVALID_REQUEST");
  return sdk.trigger({ function_id: "state::storage", payload: request, timeoutMs });
}
