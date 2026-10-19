import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { randomBytes } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import { triggerStateStorage, type RetentionResourceReference, type RetentionContextDeliveryResult } from "../state/storage.js";

const DAY = 86_400_000;
export function newRetentionContextEventId(): string {
  const bytes = randomBytes(16);
  bytes.writeUIntBE(Date.now(), 0, 6);
  bytes[6] = (bytes[6] & 0x0f) | 0x70;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
export async function recordPublishedContextUse(sdk: IIIClient, taskId: string, project: string,
  references: RetentionResourceReference[]): Promise<RetentionContextDeliveryResult> {
  const resources = new Map<string, RetentionResourceReference>();
  for (const reference of references) {
    const identity = JSON.stringify([reference.scope, reference.key]);
    const prior = resources.get(identity);
    if (prior && prior.expected_version !== reference.expected_version) throw new Error("STATE_RETENTION_RESOURCE_VERSION_CONFLICT");
    resources.set(identity, reference);
  }
  return triggerStateStorage<RetentionContextDeliveryResult>(sdk, {
    action: "retention_context_delivery", event_id: newRetentionContextEventId(), task_id: taskId,
    project, resources: [...resources.values()],
  });
}
function renewable(inactivityMs: number, environmentKey: string) {
  const configured = process.env[environmentKey];
  const hours = configured === undefined ? inactivityMs * 4 / 3_600_000 : Number(configured);
  if (!Number.isFinite(hours) || hours * 3_600_000 < inactivityMs || hours > 365 * 24) {
    throw new Error(`RETENTION_INVALID_MAXIMUM_AGE: ${environmentKey}`);
  }
  return { inactivityMs, maximumAgeMs: Math.floor(hours * 3_600_000) };
}
export const STORAGE_RETENTION = {
  viewer: renewable(DAY, "AGENTMEMORY_RETENTION_VIEWER_MAX_AGE_HOURS"),
  tools: renewable(7 * DAY, "AGENTMEMORY_RETENTION_TOOLS_MAX_AGE_HOURS"),
  completedBodies: renewable(7 * DAY, "AGENTMEMORY_RETENTION_COMPLETED_MAX_AGE_HOURS"),
  backups: { inactivityMs: 30 * DAY, maximumAgeMs: 30 * DAY },
} as const;
export type RetentionCategory = keyof typeof STORAGE_RETENTION;
export interface TrustedRetentionUse {
  producer: "server-task-context-delivery" | "server-confirmed-task-use";
  taskId: string;
  occurredAt: string;
}
export const RETENTION_USE_INTEGRATION = {
  status: "native_source_integrated_runtime_pending",
  requiredProducer: "server-confirmed task context delivery or task use",
  rejectedSignals: ["client claims", "generic access", "indexing", "maintenance", "retrieval candidates"],
} as const;
export interface RetentionEvidence {
  origin: unknown;
  meaningfulUse?: unknown;
  protected?: boolean;
  dependenciesClosed: boolean;
}
export type RetentionDecision = { eligible: boolean; expiresAt: string | null; reason: string };
export function retentionEligibility(category: RetentionCategory, evidence: RetentionEvidence, now = Date.now()): RetentionDecision {
  const excluded = (reason: string): RetentionDecision => ({ eligible: false, expiresAt: null, reason });
  if (!Number.isFinite(now)) return excluded("invalid_clock");
  if (evidence.protected) return excluded("protected");
  if (!evidence.dependenciesClosed) return excluded("dependencies_open");
  const origin = typeof evidence.origin === "string" ? Date.parse(evidence.origin) : NaN;
  if (!Number.isFinite(origin)) return excluded("unknown_origin");
  if (origin > now) return excluded("future_origin");
  const signal = evidence.meaningfulUse;
  let use = origin;
  if (signal !== undefined) {
    if (typeof signal !== "object" || signal === null || Array.isArray(signal)) return excluded("invalid_meaningful_use");
    const event = signal as Partial<TrustedRetentionUse>;
    if (!["server-task-context-delivery", "server-confirmed-task-use"].includes(String(event.producer))
      || typeof event.taskId !== "string" || !event.taskId.trim() || event.taskId.length > 4096
      || typeof event.occurredAt !== "string") return excluded("invalid_meaningful_use");
    use = Date.parse(event.occurredAt);
  }
  if (!Number.isFinite(use) || use < origin || use > now) return excluded("invalid_meaningful_use");
  const policy = STORAGE_RETENTION[category];
  const expires = Math.min(use + policy.inactivityMs, origin + policy.maximumAgeMs);
  return { eligible: now >= expires, expiresAt: new Date(expires).toISOString(), reason: now >= expires ? "expired" : "retained" };
}

export function hasRetentionProtection(row: Record<string, unknown>): boolean {
  return ["protected", "keep", "pinned", "retentionProtected", "rollbackRequired", "replayRequired", "recoveryStopped", "dispatchPending"].some(key => row[key] === true)
    || ["recovery", "lease", "activeUnitJobId", "recoveryOfJobId", "zeroSpendRecoveryOfJobId", "recoveryMode", "recoveryGeneration"].some(key => row[key] != null)
    || ["references", "dependentJobIds", "protectedBy"].some(key => row[key] !== undefined && (!Array.isArray(row[key]) || row[key].length > 0));
}

// updatedAt is accepted only on a terminal, immutably closed job. Maintenance reads never update it.
export function terminalRetentionOrigin(row: Record<string, unknown>): unknown {
  if (row.state !== "completed" && row.state !== "invalidated" && row.status !== "completed") return undefined;
  return row.completedAt ?? row.closedAt ?? row.updatedAt;
}

export async function cleanupAuthorized(kv: StateKV): Promise<boolean> {
  const state = await kv.get<{ cleanupActivatedAt?: string }>(KV.storageMaintenance, "current");
  return typeof state?.cleanupActivatedAt === "string" && Number.isFinite(Date.parse(state.cleanupActivatedAt));
}
