import type { HookPayload, Memory } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { logger } from "../logger.js";

const SKIP_REASONS = ["empty_content", "recognized_telemetry", "duplicate_event", "exact_duplicate", "configured_policy"] as const;
export type AdmissionSkipReason = typeof SKIP_REASONS[number];
const MAX_AGGREGATE = Number.MAX_SAFE_INTEGER;
interface AdmissionGrowthCounters {
  version: 1;
  updatedAt: string;
  reasons: Record<AdmissionSkipReason, { attempts: number; inputBytes: number }>;
}

export async function recordAdmissionSkip(kv: StateKV, reason: AdmissionSkipReason, inputBytes: number): Promise<void> {
  try {
    await withKeyedLock("admission-growth-counters", async () => {
      const current = await kv.get<AdmissionGrowthCounters>(KV.metrics, "admission-growth-v1");
      const valid = (value: unknown): number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
      const reasons = {} as AdmissionGrowthCounters["reasons"];
      for (const key of SKIP_REASONS) {
        const previous = current?.version === 1 ? current.reasons?.[key] : undefined;
        reasons[key] = { attempts: Math.min(MAX_AGGREGATE, valid(previous?.attempts) + (key === reason ? 1 : 0)),
          inputBytes: Math.min(MAX_AGGREGATE, valid(previous?.inputBytes) + (key === reason ? valid(inputBytes) : 0)) };
      }
      await kv.set<AdmissionGrowthCounters>(KV.metrics, "admission-growth-v1", { version: 1, updatedAt: new Date().toISOString(), reasons });
    });
  } catch {
    logger.warn("Admission growth counters unavailable", { reason });
  }
}

function emptyJson(value: unknown, depth = 0): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string") return !value.trim();
  if (depth >= 16) return false;
  if (Array.isArray(value)) return value.every(item => emptyJson(item, depth + 1));
  if (typeof value === "object") {
    if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) return false;
    const entries = Object.entries(value);
    if (entries.some(([key]) => /error|failure|warning|decision|change|diff|patch|source|origin|attribution|evidence|permission|security|constraint/i.test(key))) return false;
    // An unknown empty field can encode a reset or a domain result, rather than missing content.
    if (entries.some(([key]) => !["prompt", "userPrompt", "tool_input", "tool_output", "message", "text", "title", "content", "narrative"].includes(key))) return false;
    return entries.every(([, item]) => emptyJson(item, depth + 1));
  }
  return false;
}

export function emptyObservationContent(payload: HookPayload): boolean {
  // The occurrence itself is evidence even when a lifecycle/failure event has no body.
  if (["session_start", "session_end", "stop", "subagent_start", "subagent_stop", "task_completed", "post_tool_failure"].includes(payload.hookType)) return false;
  if (!["prompt_submit", "pre_tool_use", "post_tool_use", "notification", "pre_compact"].includes(payload.hookType)) return false;
  return emptyJson(payload.data);
}

export function isContextEmissionTelemetry(payload: HookPayload): boolean {
  if (payload.hookType !== "notification" || !payload.data || typeof payload.data !== "object" || Array.isArray(payload.data)) return false;
  const data = payload.data as Record<string, unknown>;
  return data.event === "context_emitted_v1"
    && Object.keys(data).every(key => ["event", "eventId", "source", "text", "candidateTokens"].includes(key))
    && typeof data.text === "string" && data.text.length <= 32_000
    && typeof data.eventId === "string" && /^[a-f0-9-]{36}$/.test(data.eventId)
    && ["session-start", "pre-tool-use", "pre-compact", "mcp-response"].includes(String(data.source))
    && (data.candidateTokens === undefined || typeof data.candidateTokens === "number" && Number.isSafeInteger(data.candidateTokens) && data.candidateTokens >= 0);
}

export type AdmissionAction = "store" | "update" | "merge" | "skip";
export type MemoryAttribution = NonNullable<Memory["attribution"]>;
export type MemorySourceKind = MemoryAttribution["sourceKind"];
export type AttributedMemory = Memory;
export interface AdmissionRequest {
  content: string;
  type: Memory["type"];
  project?: string;
  agentId?: string;
  targetMemoryId?: string;
  expectedVersion?: number;
  action?: "store" | "update" | "merge";
  attribution: MemoryAttribution;
  concepts: string[];
  files: string[];
  sourceObservationIds: string[];
}
export type AdmissionDecision = {
  action: AdmissionAction;
  reason: "new_content" | "exact_duplicate" | "additional_evidence" | "explicit_revision";
  current?: AttributedMemory;
};

export function sameMemoryScope(existing: Memory, request: AdmissionRequest): boolean {
  return existing.project === request.project && existing.agentId === request.agentId && existing.type === request.type;
}

function sameAttribution(existing: AttributedMemory, request: AdmissionRequest): boolean {
  return (existing.attribution?.sourceKind ?? "unknown") === request.attribution.sourceKind
    && existing.attribution?.actorId === request.attribution.actorId;
}

export function decideMemoryAdmission(request: AdmissionRequest, candidates: AttributedMemory[]): AdmissionDecision {
  let current: AttributedMemory | undefined;
  if (request.targetMemoryId) {
    current = candidates.find(candidate => candidate.id === request.targetMemoryId);
    if (!current || !sameMemoryScope(current, request)) throw new Error("memory_admission_target_invalid");
    if (current.version !== request.expectedVersion) throw new Error("memory_admission_version_conflict");
    if (!sameAttribution(current, request)) throw new Error("memory_admission_attribution_conflict");
    if (current.isLatest === false) {
      const revision = candidates.filter(candidate => candidate.parentId === current?.id && candidate.version === Number(request.expectedVersion) + 1
        && candidate.isLatest !== false && sameMemoryScope(candidate, request) && sameAttribution(candidate, request)
        && candidate.content === request.content).sort((left, right) => left.id.localeCompare(right.id))[0];
      const contains = (retained: string[] = [], incoming: string[] = []) => incoming.every(value => retained.includes(value));
      if (!revision || (request.action !== "update" && request.action !== "merge")
        || !contains(revision.sourceObservationIds, request.sourceObservationIds) || !contains(revision.concepts, request.concepts)
        || !contains(revision.files, request.files) || !contains(revision.attribution?.evidenceObservationIds, request.attribution.evidenceObservationIds)) {
        throw new Error("memory_admission_version_conflict");
      }
      current = revision;
    }
    if (request.action === "update" && current.content !== request.content) {
      return { action: "update", reason: "explicit_revision", current };
    }
    if (current.content !== request.content) throw new Error("memory_admission_merge_requires_exact_content");
  } else {
    if (request.action === "update" || request.action === "merge") throw new Error("memory_admission_target_required");
    current = candidates.filter(candidate => candidate.isLatest !== false && sameMemoryScope(candidate, request)
      && candidate.content === request.content && sameAttribution(candidate, request))
      .sort((left, right) => left.id.localeCompare(right.id))[0];
  }
  if (!current) return { action: "store", reason: "new_content" };
  const adds = (incoming: string[], retained: string[] = []) => incoming.some(value => !retained.includes(value));
  if (adds(request.sourceObservationIds, current.sourceObservationIds) || adds(request.concepts, current.concepts)
    || adds(request.files, current.files) || adds(request.attribution.evidenceObservationIds, current.attribution?.evidenceObservationIds)) {
    return { action: "merge", reason: "additional_evidence", current };
  }
  return { action: "skip", reason: "exact_duplicate", current };
}

