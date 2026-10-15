import type { Memory } from "../types.js";

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

