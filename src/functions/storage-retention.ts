import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";

const DAY = 86_400_000;
function renewable(inactivityMs: number, environmentKey: string) {
  const configured = process.env[environmentKey];
  const hours = configured === undefined ? inactivityMs * 4 / 3_600_000 : Number(configured);
  if (!Number.isFinite(hours) || hours * 3_600_000 < inactivityMs || hours > 365 * 24) {
    throw new Error(`RETENTION_INVALID_MAXIMUM_AGE: ${environmentKey}`);
  }
  return { inactivityMs, maximumAgeMs: hours * 3_600_000 };
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
  status: "integration_needed",
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
