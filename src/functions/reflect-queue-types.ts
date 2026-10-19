import type { WorkflowTerminalReceipt } from "../types.js";
import type { CompressedObservation } from "../types.js";
import type { GraphWorkCursor } from "./graph-work-groups.js";
import { parseReflectionResult } from "./reflect-result.js";
import type { PublicationEvidenceObservation, PublicationEvidenceStatus, PublicationJudgeDisposition } from "./publication-evidence.js";
import { validPublicationJudgeDisposition } from "./publication-evidence.js";

export const REFLECT_JOBS = "mem:reflect:jobs";
export const REFLECT_PENDING = "mem:reflect:pending";
export const REFLECT_CONTROL = "mem:reflect:control";
export const REFLECT_TOPIC = "agentmemory.reflect.unit";
export const REFLECT_PROMPT_BYTES = 8192;
export const REFLECT_MAX_SOURCES = 12;
export interface ManualReflectContinuation {
  jobId: string; requestId: string; project: string | null; checkpoint: string; cut: number;
}
export interface ReflectForegroundControl { project: string; requestId: string }
export interface ReflectPublicationReplay extends ReflectForegroundControl {
  manifestId: string; unitSequence: number; effectKey: string;
}
export interface ReflectJudgeDisposition extends PublicationJudgeDisposition {
  unit: ReflectPublicationReplay;
  captureFingerprint: string;
}
export function validReflectJudgeDisposition(value: unknown): value is ReflectJudgeDisposition {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  const hasMigration = Object.hasOwn(row, "migration");
  return Object.keys(row).length === (hasMigration ? 6 : 5) && validReflectPublicationReplay(row.unit)
    && validReflectText(row.dispositionId) && validReflectText(row.captureFingerprint)
    && validPublicationJudgeDisposition({ dispositionId: row.dispositionId, checkpoint: row.checkpoint, budget: row.budget,
      ...(hasMigration ? { migration: row.migration } : {}) });
}
export function validReflectPublicationReplay(value: unknown): value is ReflectPublicationReplay {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  return Object.keys(row).length === 5 && [row.project, row.requestId, row.manifestId, row.effectKey].every(validReflectText)
    && Number.isSafeInteger(row.unitSequence) && Number(row.unitSequence) >= 0;
}
export function validReflectForegroundControl(value: unknown): value is ReflectForegroundControl {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  return Object.keys(row).length === 2 && validReflectText(row.project) && validReflectText(row.requestId);
}
export function validManualReflectContinuation(value: unknown): value is ManualReflectContinuation {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  return Object.keys(row).length === 5 && [row.jobId, row.requestId, row.checkpoint].every(validReflectText)
    && (row.project === null || validReflectText(row.project)) && Number.isSafeInteger(row.cut) && Number(row.cut) >= 1 && Number(row.cut) <= 256;
}
export interface ReflectRequest {
  manual?: true; continuation?: ManualReflectContinuation;
  project?: string; sessionId?: string; summaryId?: string; requestId?: string; query?: string;
  maxClusters?: number; deferred?: boolean; force?: boolean;
  factIds?: string[]; lessonIds?: string[]; crystalIds?: string[];
  legacyWorkId?: string;
  recoveryOfJobId?: string;
  batchResponse?: string; batchCluster?: string; batchSourceFingerprint?: string; batchEffectKey?: string;
}
export interface ConceptCluster {
  concepts: string[]; facts: Array<{ fact: string; confidence: number }>;
  lessons: Array<{ content: string; confidence: number }>; crystalNarratives: string[];
  factIds: string[]; lessonIds: string[]; crystalIds: string[];
  memoryIds?: string[]; observationIds?: string[]; jobId?: string; unitId?: string;
}
export interface ReflectSource {
  id: string; scope: string; version: string; fingerprint: string; order: number;
  score: number; bm25Score: number; vectorScore: number; observation: CompressedObservation;
  kind: "fact" | "lesson" | "crystal" | "memory" | "observation";
  provenanceReceipt?: string;
}
export interface ReflectManifest {
  id: string; cluster: ConceptCluster; sources: ReflectSource[];
  indexIdentity: { index_id: string; model: string; dimensions: number; generation: string };
  graphGeneration: string; deferredFamilies: string[]; prompt: string;
  nextCursor?: GraphWorkCursor | null; grouped?: boolean;
}
export interface ReflectGateDecision {
  manifestId: string;
  choice: "run" | "skip";
  outcome: "decision" | "fail-open";
}
export interface ReflectEmptyEvidence {
  project: string; seed: string; ordinal: number;
  seedBinding?: ReflectJob["seedBinding"];
  indexIdentity: ReflectManifest["indexIdentity"]; graphGeneration: string;
}
export type ReflectStatus = "pending" | "blocked" | "waiting_index" | "waiting_graph" | "waiting_scope" |
  "waiting_budget" | "waiting_batch" | "completed" | "partial" | "failed";
export interface ReflectJob {
  workflowTerminal?: WorkflowTerminalReceipt;
  id: string; request: ReflectRequest; project?: string; seeds: string[];
  executionOrigin?: "selected" | "manual"; selectedSessionId?: string;
  manual?: { cuts: number; startedAt: number };
  recoveryOfJobId?: string;
  seedBinding?: { scope: string; id: string; version: string; fingerprint: string };
  status: ReflectStatus; ordinal: number; reservedInsights: number; appliedInsights: number;
  processedClusters: number; clustersSkipped?: number; gateDecision?: ReflectGateDecision;
  refreshes: number; attempts: number; waits?: number; createdAt: string;
  updatedAt: string; nextAttemptAt: number; dispatchPending: boolean; dispatchedAt?: number;
  claim?: { id: string; expiresAt: number }; manifest?: ReflectManifest;
  generation?: { state: "intended" | "overflow"; effectKey: string; responseBytes?: number; responseFingerprint?: string };
  captured?: { response: string; effectKey: string; count?: number; insightIds?: string[]; generationValid?: boolean; evidenceRequestId?: string;
    publication?: { requestId: string; checkpoint?: PublicationEvidenceObservation; status?: PublicationEvidenceStatus; reason?: string } };
  deferredFamilies: string[]; error?: string;
  sourcePage?: ReflectSource[]; sourceCursor?: GraphWorkCursor | null; unitSequence?: number;
  sourcePageBinding?: { identity: ReflectManifest["indexIdentity"]; graphGeneration: string; deferredFamilies: string[] };
  resultFingerprint?: string;
  global?: { projects: string[]; ordinal: number; cursor?: string; hasMore: boolean; partial: boolean; initialized: boolean };
  legacy?: { workId: string; workVersion: string; workFingerprint: string; correlationId: string; sourceFingerprint: string; concepts: string[];
    refs: Array<{ scope: string; key: string; kind: ReflectSource["kind"] }>; offset: number;
    provenanceCursor?: string; provenanceVersion?: string; provenanceSequence: number; provenanceFingerprint: string };
  continuation?: { jobId: string; workId: string; correlationId: string; sourceFingerprint: string; requiredSources: number;
    completedSources: number; completedUnits: number; resultFingerprint: string; provenanceFingerprint: string; sourceIdsFingerprint: string };
}
export interface ReflectPending { id: string }

export const validReflectText = (value: unknown): value is string => typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= 256;
export function countReflectResult(response: string): number {
  return parseReflectionResult(response).length;
}
export function validateReflectRequest(data: ReflectRequest): ReflectRequest {
  if (!data || typeof data !== "object") throw new Error("invalid_reflect_request");
  for (const field of ["project", "sessionId", "summaryId", "requestId", "query", "legacyWorkId", "recoveryOfJobId"] as const) {
    if (data[field] !== undefined && !validReflectText(data[field])) throw new Error(`invalid_reflect_${field}`);
  }
  if (data.maxClusters !== undefined && (!Number.isInteger(data.maxClusters) || data.maxClusters < 1 || data.maxClusters > 20)) throw new Error("invalid_reflect_maxClusters");
  const groups = [data.factIds, data.lessonIds, data.crystalIds];
  if (groups.some(ids => ids !== undefined && (!Array.isArray(ids) || ids.some(id => !validReflectText(id)))) || groups.reduce((sum, ids) => sum + (ids?.length ?? 0), 0) > 12) throw new Error("invalid_reflect_source_ids");
  return { project: data.project, sessionId: data.sessionId, summaryId: data.summaryId, requestId: data.requestId,
    query: data.query, maxClusters: data.maxClusters ?? 4, deferred: data.deferred === true, force: data.force === true,
    factIds: data.factIds, lessonIds: data.lessonIds, crystalIds: data.crystalIds, legacyWorkId: data.legacyWorkId,
    ...(data.recoveryOfJobId && { recoveryOfJobId: data.recoveryOfJobId }) };
}
