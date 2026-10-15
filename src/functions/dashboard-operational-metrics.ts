import { createHash } from "node:crypto";
import type { StateKV } from "../state/kv.js";
import type { GraphStorageMetadata } from "../state/storage.js";
import { KV } from "../state/schema.js";
import type { StorageMaintenanceState } from "../types.js";
import { logger } from "../logger.js";
import type { EngineMemoryMetric } from "./engine-memory.js";
import type { DashboardActivity } from "./dashboard-activity.js";

export interface Failures24hMetric {
  count: number | null;
  intervalStart: string;
  intervalEnd: string;
  complete: boolean;
  coverageSince: string | null;
  reason?: "uninitialized" | "warming_up" | "coverage_invalid" | "counter_unavailable";
}

export interface GraphStorageMetric {
  storedBytes: number | null;
  logicalBytes: number | null;
  rows: number | null;
  sampleAt: string | null;
  coverage: string | null;
  complete: boolean;
  source: "native_scoped_metadata" | "unavailable";
  reason?: "scoped_metadata_unavailable" | "sample_unavailable" | "sample_invalid";
}

export interface AutomaticCompactionMetric {
  enabled: boolean | null;
  state: "enabled" | "paused" | "requires_migration" | "unsupported_mode" | "unknown";
  autoVacuumMode: 0 | 1 | 2 | null;
}

export interface DashboardOperationalMetrics {
  asOf: string;
  failures24h: Failures24hMetric;
  graphStorage: GraphStorageMetric;
  engineMemory: EngineMemoryMetric;
  lastSuccessfulCompactionAt: string | null;
  automaticCompaction: AutomaticCompactionMetric;
  maintenance?: { state: "unavailable" | "dependency_failure" | "deferred" | "no_progress" | "progress"; reason: string; physicalBytesReclaimed: number | null };
  workflows?: ReturnType<typeof buildWorkflowStatus>;
}

export interface WorkflowStatusInputs {
  backgroundRecoveryPaused: boolean;
  providerAvailable: boolean;
  providerCircuitOpen: boolean;
  nativeAvailable: boolean;
  slotsAvailable?: boolean;
  activity: DashboardActivity | null;
  configured: Record<"graph" | "consolidation" | "compression" | "reflection" | "snapshots" | "maintenance", boolean>;
}

export function buildWorkflowStatus(input: WorkflowStatusInputs) {
  const definitions = [
    { key: "summaries", label: "Session summaries", enabled: true, provider: true, paused: true, functions: ["mem::summarize", "mem::summary-final", "mem::summary-unit"] },
    { key: "skills", label: "Skill extraction", enabled: true, provider: true, paused: true, functions: ["mem::skill-extract"] },
    { key: "graph", label: "Graph extraction", enabled: input.configured.graph, provider: true, paused: true, functions: ["mem::graph-extract"] },
    { key: "reflection", label: "Reflection", enabled: input.configured.reflection, provider: true, paused: true, functions: ["mem::reflect", "mem::reflect-unit"] },
    { key: "consolidation", label: "Consolidation pipeline", enabled: input.configured.consolidation, provider: true, paused: true, functions: ["mem::consolidate", "mem::consolidate-pipeline"] },
    { key: "compression", label: "LLM compression", enabled: input.configured.compression, provider: true, paused: false, functions: ["mem::compress"] },
    { key: "snapshots", label: "Automatic snapshots", enabled: input.configured.snapshots, provider: false, paused: false, functions: ["mem::snapshot-create"] },
    { key: "maintenance", label: "Storage maintenance", enabled: true, provider: false, paused: !input.configured.maintenance, functions: ["mem::storage-maintenance"] },
  ];
  return {
    backgroundRecoveryPaused: input.backgroundRecoveryPaused,
    evidenceCoverage: "Last 30 recorded operations; absence does not prove a workflow has never run.",
    items: definitions.map(definition => {
      const observed = input.activity?.items.slice(0, 30).find(item => definition.functions.includes(item.functionId)
        && (item.outcome === "completed" || item.outcome === "failed" || item.outcome === "partial"));
      let state: "enabled" | "paused" | "disabled" | "unavailable" | "dependency_failure" | "failed" = "enabled";
      let reason = "eligible_execution_not_proven";
      if (!definition.enabled) { state = "disabled"; reason = "configuration_disabled"; }
      else if (definition.paused && input.backgroundRecoveryPaused) { state = "paused"; reason = "background_recovery_paused"; }
      else if (definition.provider && !input.providerAvailable) { state = "dependency_failure"; reason = "provider_unavailable"; }
      else if (definition.provider && input.providerCircuitOpen) { state = "dependency_failure"; reason = "provider_circuit_open"; }
      else if (definition.key === "reflection" && input.slotsAvailable === false) { state = "dependency_failure"; reason = "slots_disabled"; }
      else if (["graph", "maintenance"].includes(definition.key) && !input.nativeAvailable) { state = "dependency_failure"; reason = "native_state_unavailable"; }
      else if (observed?.outcome === "failed") { state = "failed"; reason = "last_observed_execution_failed"; }
      else if (!input.activity) { state = "unavailable"; reason = "execution_evidence_unavailable"; }
      return {
        key: definition.key, label: definition.label, configured: definition.enabled, state, reason,
        lastObservedAt: observed?.timestamp ?? null, lastOutcome: observed?.outcome ?? null,
      };
    }),
  };
}

const MINUTE_MS = 60_000;
const WINDOW_MINUTES = 24 * 60;
const BUCKET_PREFIX = "m";
const BUCKET_KEY = /^m[0-9a-z]{1,10}$/;
const counterWriteFailures = new WeakSet<StateKV>();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function safeCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function validIso(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function counterKey(agentId?: string): string {
  if (!agentId) return "global";
  const digest = createHash("sha256").update(agentId).digest("hex").slice(0, 32);
  return `agent:${digest}`;
}

function failureMetric(value: unknown, asOfMs: number): Failures24hMetric {
  const intervalStartMs = asOfMs - WINDOW_MINUTES * MINUTE_MS;
  const base = {
    intervalStart: new Date(intervalStartMs).toISOString(),
    intervalEnd: new Date(asOfMs).toISOString(),
  };
  if (isRecord(value) && value.coverageInvalid === true) {
    const coverageSince = validIso(value.coverageSince) ? value.coverageSince : null;
    return { ...base, count: null, complete: false, coverageSince, reason: "coverage_invalid" };
  }
  if (!isRecord(value) || value.schemaVersion !== 1 || !validIso(value.coverageSince)) {
    return { ...base, count: null, complete: false, coverageSince: null, reason: "uninitialized" };
  }
  const coverageSinceMs = Date.parse(value.coverageSince);
  if (coverageSinceMs > intervalStartMs) {
    return { ...base, count: null, complete: false, coverageSince: value.coverageSince, reason: "warming_up" };
  }

  let count = 0;
  for (const [key, bucketCount] of Object.entries(value)) {
    if (!BUCKET_KEY.test(key)) continue;
    const minute = Number.parseInt(key.slice(BUCKET_PREFIX.length), 36);
    if (!Number.isSafeInteger(minute) || !safeCount(bucketCount)) {
      return { ...base, count: null, complete: false, coverageSince: value.coverageSince, reason: "counter_unavailable" };
    }
    const bucketAt = minute * MINUTE_MS;
    if (bucketAt >= intervalStartMs && bucketAt < asOfMs) count += bucketCount;
    if (!Number.isSafeInteger(count)) {
      return { ...base, count: null, complete: false, coverageSince: value.coverageSince, reason: "counter_unavailable" };
    }
  }
  return { ...base, count, complete: true, coverageSince: value.coverageSince };
}

function graphStorageMetric(value: GraphStorageMetadata | null): GraphStorageMetric {
  if (value?.status === "ready" && value.source === "native_scoped_metadata" && value.complete
    && value.scope_count === 7 && safeCount(value.rows) && safeCount(value.logical_bytes)
    && safeCount(value.stored_bytes) && validIso(value.sampled_at) && typeof value.coverage === "string" && value.coverage.length > 0) {
    return {
      storedBytes: value.stored_bytes, logicalBytes: value.logical_bytes, rows: value.rows,
      sampleAt: value.sampled_at, coverage: value.coverage, complete: true,
      source: "native_scoped_metadata",
    };
  }
  return {
    storedBytes: null, logicalBytes: null, rows: null,
    sampleAt: null, coverage: value?.coverage ?? null, complete: false,
    source: "unavailable", reason: "scoped_metadata_unavailable",
  };
}

function lastSuccessfulCompactionAt(state: StorageMaintenanceState | null): string | null {
  if (validIso(state?.lastSuccessfulCompactionAt)) return state.lastSuccessfulCompactionAt;
  const last = state?.lastReclaim;
  if (last && validIso(last.at) && safeCount(last.physicalBytesReclaimed) && last.physicalBytesReclaimed > 0) return last.at;
  return null;
}

function maintenanceMetric(value: unknown): NonNullable<DashboardOperationalMetrics["maintenance"]> {
  if (!isRecord(value) || value.version !== 1 || !isRecord(value.compaction)) {
    return { state: "unavailable", reason: "maintenance_state_unavailable", physicalBytesReclaimed: null };
  }
  const progress = value.compaction;
  const last = isRecord(value.lastReclaim) ? value.lastReclaim : null;
  const bytes = safeCount(last?.physicalBytesReclaimed) ? last.physicalBytesReclaimed : null;
  if (progress.lastErrorCode) return { state: "dependency_failure", reason: "storage_error", physicalBytesReclaimed: bytes };
  if (progress.lastStatus === "migration_required") return { state: "dependency_failure", reason: "migration_required", physicalBytesReclaimed: bytes };
  if (progress.lastStatus === "deferred") return {
    state: "deferred", reason: progress.lastReason === "foreground_pressure" ? "foreground_pressure" : "maintenance_deferred", physicalBytesReclaimed: bytes,
  };
  return { state: bytes === null ? "unavailable" : bytes > 0 ? "progress" : "no_progress", reason: bytes === null ? "physical_measurement_unavailable" : bytes > 0 ? "physical_bytes_reclaimed" : "no_physical_reduction", physicalBytesReclaimed: bytes };
}

function automaticCompactionMetric(
  state: StorageMaintenanceState | null,
  automaticMaintenancePaused: boolean,
): AutomaticCompactionMetric {
  const mode = state?.diagnostics?.lastComplete?.physical?.auto_vacuum;
  const autoVacuumMode = mode === 0 || mode === 1 || mode === 2 ? mode : null;
  if (automaticMaintenancePaused) return { enabled: false, state: "paused", autoVacuumMode };
  if (mode === 2) return { enabled: true, state: "enabled", autoVacuumMode };
  if (mode === 0) return { enabled: false, state: "requires_migration", autoVacuumMode };
  if (mode === 1) return { enabled: false, state: "unsupported_mode", autoVacuumMode };
  return { enabled: null, state: "unknown", autoVacuumMode };
}

export function createDashboardOperationalMetrics(
  kv: StateKV,
  readEngineMemory: () => Promise<EngineMemoryMetric>,
) {
  return {
    async read(now: Date, automaticMaintenancePaused: boolean, agentId?: string): Promise<DashboardOperationalMetrics> {
      const asOfMs = Math.floor(now.getTime() / MINUTE_MS) * MINUTE_MS;
      const counterRecordKey = counterKey(agentId);
      const [counterRead, maintenance, engineMemory, graphMetadata] = await Promise.all([
        readOrInitializeFailureCounter(kv, counterRecordKey, now.getTime()),
        kv.get<StorageMaintenanceState>(KV.storageMaintenance, "current").catch(() => null),
        readEngineMemory().catch(() => ({ residentBytes: null, sampledAt: null, source: "unavailable", reason: "probe_failed" } as const)),
        kv.graphStorageMetadata().catch(() => null),
      ]);
      const failures = counterWriteFailures.has(kv)
        ? {
            intervalStart: new Date(asOfMs - WINDOW_MINUTES * MINUTE_MS).toISOString(),
            intervalEnd: new Date(asOfMs).toISOString(), count: null, complete: false,
            coverageSince: null, reason: "coverage_invalid" as const,
          }
        : counterRead.failed
        ? {
            intervalStart: new Date(asOfMs - WINDOW_MINUTES * MINUTE_MS).toISOString(),
            intervalEnd: new Date(asOfMs).toISOString(), count: null, complete: false,
            coverageSince: null, reason: "counter_unavailable" as const,
          }
        : failureMetric(counterRead.value, asOfMs);
      return {
        asOf: new Date(asOfMs).toISOString(),
        failures24h: failures,
        graphStorage: graphStorageMetric(graphMetadata),
        engineMemory,
        lastSuccessfulCompactionAt: lastSuccessfulCompactionAt(maintenance),
        automaticCompaction: automaticCompactionMetric(maintenance, automaticMaintenancePaused),
        maintenance: maintenanceMetric(maintenance),
      };
    },
  };
}

async function readOrInitializeFailureCounter(
  kv: StateKV,
  recordKey: string,
  coverageStartMs: number,
): Promise<{ value: unknown; failed: boolean }> {
  let value: unknown;
  try {
    value = await kv.get<unknown>(KV.dashboardFailureMinutes, recordKey);
  } catch {
    return { value: null, failed: true };
  }
  if (value !== null && value !== undefined) return { value, failed: false };
  try {
    const result = await kv.update<{ errors?: unknown }>(KV.dashboardFailureMinutes, recordKey, [
      { type: "set", path: "schemaVersion", value: 1 },
      { type: "set", path: "coverageSince", value: new Date(coverageStartMs).toISOString() },
    ]);
    if (Array.isArray(result?.errors) && result.errors.length > 0) throw new Error("counter_initialization_rejected");
    value = await kv.get<unknown>(KV.dashboardFailureMinutes, recordKey);
    return { value, failed: false };
  } catch {
    counterWriteFailures.add(kv);
    logger.warn("Dashboard failure counter coverage could not be initialized");
    return { value: null, failed: true };
  }
}

export async function recordFailureMinute(kv: StateKV, failedAtMs: number, agentId?: string): Promise<void> {
  if (!Number.isSafeInteger(failedAtMs) || failedAtMs < 0) return;
  const minute = Math.floor(failedAtMs / MINUTE_MS);
  const key = `${BUCKET_PREFIX}${minute.toString(36)}`;
  const recordKey = counterKey(agentId);
  const current = await kv.get<unknown>(KV.dashboardFailureMinutes, recordKey).catch(() => null);
  const counts = isRecord(current) ? current : {};
  const stale = Object.keys(counts)
    .filter((field) => {
      if (!BUCKET_KEY.test(field)) return false;
      const bucket = Number.parseInt(field.slice(BUCKET_PREFIX.length), 36);
      return !Number.isSafeInteger(bucket) || bucket < minute - WINDOW_MINUTES;
    })
    .slice(0, 64);
  const ops: Array<{ type: string; path: string; by?: number; value?: unknown }> = [
    ...stale.map((path) => ({ type: "remove", path })),
    { type: "increment", path: key, by: 1 },
  ];
  if (!isRecord(current) || current.schemaVersion !== 1 || !validIso(current.coverageSince)) {
    ops.push({ type: "set", path: "schemaVersion", value: 1 });
    ops.push({ type: "set", path: "coverageSince", value: new Date(failedAtMs).toISOString() });
  }
  try {
    const result = await kv.update<{ errors?: unknown }>(KV.dashboardFailureMinutes, recordKey, ops);
    if (Array.isArray(result?.errors) && result.errors.length > 0) throw new Error("counter_update_rejected");
  } catch {
    counterWriteFailures.add(kv);
    try {
      const result = await kv.update<{ errors?: unknown }>(KV.dashboardFailureMinutes, recordKey, [
        { type: "set", path: "coverageInvalid", value: true },
      ]);
      if (Array.isArray(result?.errors) && result.errors.length > 0) throw new Error("coverage_update_rejected");
    } catch {
      // The status reader reports unavailable if its backing state cannot be read.
    }
    logger.warn("Dashboard failure counter coverage invalidated");
  }
}
