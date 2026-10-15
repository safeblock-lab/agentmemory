import { runWorkflowAttempt, type WorkflowAttemptAccounting } from "./workflow-attempt-accounting.js";
import type { IIIClient } from "iii-sdk";
import type {
  CompressedObservation,
  Memory,
  Session,
  MemoryProvider,
} from "../types.js";
import { KV, fingerprintId } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { recordAudit } from "./audit.js";

const CONSOLIDATION_SYSTEM = `You are a memory consolidation engine. Given a set of related observations from coding sessions, synthesize them into a single long-term memory.

Output XML:
<memory>
  <type>pattern|preference|architecture|bug|workflow|fact</type>
  <title>Concise memory title (max 80 chars)</title>
  <content>2-4 sentence description of the learned insight</content>
  <concepts>
    <concept>key term</concept>
  </concepts>
  <files>
    <file>relevant/file/path</file>
  </files>
  <strength>1-10 how confident/important this memory is</strength>
</memory>`;

import { getXmlTag, getXmlChildren } from "../prompts/xml.js";
import { ConsolidationRequests, fragmentSources, boundedPartitions, type RequestCheckpoint } from "./consolidation-partitions.js";
import { batchEffectKey } from "../state/batch-effects.js";
import type { CanonicalWriteResult } from "./consolidation-selection.js";
import type { CanonicalLookup } from "../state/canonical-memory.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { consolidationRetrievalCoverage, ScopedObservationBudgetError, type ConsolidationSelector } from "./consolidation-selection.js";
import { loadRoot, newRoot, saveRoot, runPagedCut, assertRootIdentity, consolidationIsPending, sourceRetrievalLimited } from "./consolidation-manifest.js";
import { memoryFields } from "./consolidation-reduction.js";

export interface ConsolidateMemoryMatch { current: Memory | null; token: CanonicalLookup }
export interface ConsolidateRetrieval {
  select: ConsolidationSelector;
  lookup: (project: string, normalizedTitle: string) => Promise<ConsolidateMemoryMatch>;
  commit: (project: string, normalizedTitle: string, match: ConsolidateMemoryMatch, next: Memory, effectKey: string) => Promise<CanonicalWriteResult>;
}
interface ConsolidateCapture {
  id: string;
  project: string;
  observations: Array<CompressedObservation & { sid: string }>;
  completedConcepts: string[];
  minObservations: number;
  createdAt: string;
}
class ConsolidateContinuation extends Error {}

async function consolidateLegacyCapture(
  kv: StateKV, provider: MemoryProvider, retrieval: ConsolidateRetrieval | undefined,
  data: { project?: string; minObservations?: number; consolidationCaptureId?: string },
  accounting?: WorkflowAttemptAccounting,
): Promise<Record<string, unknown>> {
  if (!retrieval) throw new Error("consolidate_project_retrieval_unavailable");
  if (!data.consolidationCaptureId && (!Number.isSafeInteger(data.minObservations ?? 10) || (data.minObservations ?? 10) < 1)) {
    throw new Error("consolidate_min_observations_invalid");
  }
  let capture = data.consolidationCaptureId ? await kv.get<ConsolidateCapture>(KV.state, data.consolidationCaptureId) : null;
  if (data.consolidationCaptureId && !capture) throw new Error("consolidate_capture_missing");
  if (!capture) {
    const project = data.project?.trim();
    if (!project) throw new Error("consolidate_project_required");
    const selected = await retrieval.select(project);
    if (selected.length > 12) throw new Error("consolidate_selection_invalid");
    const observations: ConsolidateCapture["observations"] = [];
    let bytes = 0;
    for (const hit of selected) {
      if (!("observation" in hit)) throw new Error("consolidate_legacy_capture_required");
      if (observations.some(source => source.id === hit.observation.id)) continue;
      if ((await kv.get<Session>(KV.sessions, hit.sessionId))?.project !== project) throw new Error("consolidate_source_project_mismatch");
      const observation = await kv.get<CompressedObservation>(KV.observations(hit.sessionId), hit.observation.id);
      if (!observation || observation.sessionId !== hit.sessionId || observation.timestamp !== hit.observation.timestamp) throw new Error("consolidate_source_changed");
      bytes += Buffer.byteLength(JSON.stringify(observation));
      if (bytes > 65_536) throw new Error("consolidate_selection_invalid");
      if (observation.title && observation.importance >= 5) observations.push({ ...observation, sid: hit.sessionId });
    }
    const id = fingerprintId("consolidate", JSON.stringify([project, observations, data.minObservations ?? 10]));
    capture = await kv.get<ConsolidateCapture>(KV.state, id) ?? { id, project, observations, completedConcepts: [],
      minObservations: data.minObservations ?? 10, createdAt: new Date().toISOString() };
    await kv.set(KV.state, id, capture);
  }
  if (capture.observations.length > 12 || Buffer.byteLength(JSON.stringify(capture.observations)) > 70_000 || !capture.project) throw new Error("consolidate_capture_invalid");
  const minObservations = capture.minObservations;
  if (!Number.isSafeInteger(minObservations) || minObservations < 1) throw new Error("consolidate_min_observations_invalid");
  if (capture.observations.length < minObservations) return { completed: true, consolidated: 0, reason: "insufficient_selected_observations",
    selectedSourceIds: capture.observations.map(source => source.id), selectionCoverage: "selected_relevant_observations" };
  const groups = new Map<string, ConsolidateCapture["observations"]>();
  for (const observation of capture.observations) for (const concept of new Set(observation.concepts.map(value => value.toLowerCase()))) {
    if (!concept.trim()) continue;
    groups.set(concept, [...(groups.get(concept) ?? []), observation]);
  }
  const eligible = [...groups].filter(([, sources]) => sources.length >= 3).sort(([a], [b]) => a.localeCompare(b));
  let calls = 0;
  const retained = (reason: string) => ({ completed: false, partial: true, retained: true, reason,
    captureId: capture.id, consolidated: capture.completedConcepts.length,
    continuation: { consolidationCaptureId: capture.id } });
  try {
    for (const [concept, observations] of eligible) {
      const manifestId = fingerprintId("conconcept", JSON.stringify([capture.id, concept]));
      const requests = new ConsolidationRequests(kv, manifestId, async (system, prompt, request) => {
        if (calls >= 2) throw new ConsolidateContinuation();
        calls++;
        return runWorkflowAttempt(accounting, kv, { workflow: "consolidation", functionId: "mem::consolidate",
          project: capture!.project, sourceId: capture!.id, sourceFingerprint: manifestId, jobId: capture!.id,
          unitId: request.id, phase: "generation", continuation: manifestId }, () => provider.compress(system, prompt));
      });
      const prompt = (fragments: unknown[]) => `Concept: ${concept}\nCaptured source fragments:\n${JSON.stringify(fragments)}`;
      const fragments = fragmentSources(observations.map(source => ({ id: source.id, version: source.timestamp,
        fields: { title: source.title, narrative: source.narrative, facts: JSON.stringify(source.facts), files: JSON.stringify(source.files) } })), CONSOLIDATION_SYSTEM, prompt);
      let responses: string[] = [];
      const sessionIds = [...new Set(observations.map(source => source.sid))];
      const checked = (response: string): string => {
        response = response.trim().replace(/^```(?:xml)?\s*/i, "").replace(/\s*```$/, "").trim();
        if (Buffer.byteLength(response) > 4096 || !/^<memory>[\s\S]*<\/memory>$/.test(response)
          || !["pattern", "preference", "architecture", "bug", "workflow", "fact"].includes(getXmlTag(response, "type"))
          || !getXmlTag(response, "title").trim() || !getXmlTag(response, "content").trim() || !parseMemoryXml(response, sessionIds)) {
          throw new Error("consolidate_provider_output_invalid");
        }
        return response;
      };
      const partitions = boundedPartitions(fragments, prompt, CONSOLIDATION_SYSTEM);
      for (const partition of partitions) {
        if (capture.completedConcepts.includes(concept)) {
          const userPrompt = `Consolidation manifest: ${manifestId}\n\n${prompt(partition)}`;
          const id = fingerprintId("conrequest", JSON.stringify([manifestId, CONSOLIDATION_SYSTEM, userPrompt]));
          const previous = await kv.get<RequestCheckpoint>(KV.state, id);
          if (!previous || previous.status !== "completed" || typeof previous.response !== "string") {
            if (partitions.length > 1) return retained("legacy_completed_memory_coverage_unknown");
            continue;
          }
          responses.push(checked(previous.response));
        } else responses.push(checked((await requests.request(CONSOLIDATION_SYSTEM, prompt(partition)))!));
      }
      const capturedTitles = new Map<string, string>();
      for (const response of responses) {
        const title = getXmlTag(response, "title").trim().toLowerCase();
        const previous = capturedTitles.get(title);
        if (previous && previous !== response) return retained("legacy_distinct_same_title_memories_require_recovery");
        capturedTitles.set(title, response);
      }
      if (capture.completedConcepts.includes(concept)) continue;
      for (let round = 0; responses.length > 1 && round < 8; round++) {
        const merge = (parts: string[]) => `Concept: ${concept}\nMerge these grounded partial memories into one reusable memory. Preserve substantive facts.\n${JSON.stringify(parts)}`;
        const partitions = boundedPartitions(responses, merge, CONSOLIDATION_SYSTEM);
        const next: string[] = [];
        for (const partition of partitions) next.push(partition.length === 1 ? partition[0]
          : checked((await requests.request(CONSOLIDATION_SYSTEM, merge(partition)))!));
        if (next.length >= responses.length) throw new Error("consolidate_merge_not_reducing");
        responses = next;
      }
      if (responses.length !== 1) throw new Error("consolidate_merge_incomplete");
      for (const source of observations) {
        const current = await kv.get<CompressedObservation>(KV.observations(source.sid), source.id);
        if (!current || JSON.stringify({ ...current, sid: source.sid }) !== JSON.stringify(source)
          || (await kv.get<Session>(KV.sessions, source.sid))?.project !== capture.project) throw new Error("consolidate_source_changed");
      }
      const parsed = parseMemoryXml(responses[0], sessionIds)!;
      const match = await retrieval.lookup(capture.project, parsed.title.toLowerCase());
      if (match.current && (match.current.project !== capture.project || !match.current.isLatest)) throw new Error("consolidate_canonical_scope_mismatch");
      const id = fingerprintId("mem", `${manifestId}:${parsed.title.toLowerCase()}`);
      const next: Memory = { ...parsed, id, project: capture.project, createdAt: capture.createdAt, updatedAt: capture.createdAt,
        sourceObservationIds: observations.map(source => source.id), version: (match.current?.version ?? 0) + 1,
        ...(match.current ? { parentId: match.current.id, supersedes: [match.current.id, ...(match.current.supersedes ?? [])] } : {}) };
      const effect = batchEffectKey(manifestId);
      const committed = await retrieval.commit(capture.project, parsed.title.toLowerCase(), match, next, effect);
      await recordAudit(kv, "consolidate", "mem::consolidate", [committed.id], { concept, sourceIds: next.sourceObservationIds,
        created: committed.created, applied: committed.applied, rowVersion: committed.rowVersion }, undefined, undefined, effect);
      capture.completedConcepts.push(concept);
      await kv.set(KV.state, capture.id, capture);
    }
    return { completed: true, consolidated: capture.completedConcepts.length, captureId: capture.id,
      totalObservations: capture.observations.length, selectedSourceIds: capture.observations.map(source => source.id),
      selectionCoverage: "selected_relevant_observations", ...(eligible.length ? {} : { reason: "insufficient_repeated_concept_evidence" }) };
  } catch (error) {
    const pending = consolidationIsPending(error);
    if (!(error instanceof ConsolidateContinuation) && !pending) throw error;
    return { completed: false, queued: true, consolidated: capture.completedConcepts.length,
      continuation: { consolidationCaptureId: capture.id }, ...(pending ? { reason: "canonical_index_pending" } : {}),
      selectionCoverage: "selected_relevant_observations" };
  }
}

function parseMemoryXml(
  xml: string,
  sessionIds: string[],
): Omit<Memory, "id" | "createdAt" | "updatedAt"> | null {
  const type = getXmlTag(xml, "type");
  const title = getXmlTag(xml, "title");
  const content = getXmlTag(xml, "content");
  if (!type || !title || !content) return null;

  const validTypes = new Set([
    "pattern",
    "preference",
    "architecture",
    "bug",
    "workflow",
    "fact",
  ]);

  return {
    type: (validTypes.has(type) ? type : "fact") as Memory["type"],
    title,
    content,
    concepts: getXmlChildren(xml, "concepts", "concept"),
    files: getXmlChildren(xml, "files", "file"),
    sessionIds,
    strength: Math.max(
      1,
      Math.min(10, parseInt(getXmlTag(xml, "strength") || "5", 10) || 5),
    ),
    version: 1,
    isLatest: true,
  };
}

export function registerConsolidateFunction(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  retrieval?: ConsolidateRetrieval,
  accounting?: WorkflowAttemptAccounting,
): void {
  sdk.registerFunction("mem::consolidate", 
    async (data: { project?: string; minObservations?: number; consolidationCaptureId?: string }) => {
      if (!retrieval) throw new Error("consolidate_project_retrieval_unavailable");
      let root = data.consolidationCaptureId ? await loadRoot(kv, data.consolidationCaptureId) : null;
      if (data.consolidationCaptureId && !root) {
        const legacy = await kv.get<ConsolidateCapture>(KV.state, data.consolidationCaptureId);
        if (!legacy) throw new Error("consolidate_capture_missing");
        return withKeyedLock(`consolidate:selected:${legacy.project}`, () => consolidateLegacyCapture(kv, provider, retrieval, data, accounting));
      }
      if (!root) {
        const project = data.project?.trim();
        if (!project) throw new Error("consolidate_project_required");
        const minimum = data.minObservations ?? 10;
        if (!Number.isSafeInteger(minimum) || minimum < 1) throw new Error("consolidate_min_observations_invalid");
        let selected: Awaited<ReturnType<ConsolidationSelector>>;
        try { selected = await retrieval.select(project); }
        catch (error) {
          if (sourceRetrievalLimited(error)) return { success: false, completed: false, partial: true, queued: false,
            errorCode: "STATE_RETRIEVAL_RESOURCE_LIMIT", reason: "source_retrieval_resource_limit", retryable: true,
            diagnostics: { ...(error instanceof ScopedObservationBudgetError ? error.diagnostics : {}), project, minObservations: minimum }, continuation: { project, minObservations: minimum } };
          if (!consolidationIsPending(error)) throw error;
          return { completed: false, queued: true, reason: "source_index_pending",
            continuation: { project, minObservations: minimum } };
        }
        const refs = selected.map(hit => {
          if ("observation" in hit) throw new Error("consolidate_versioned_selection_required");
          return { id: hit.id, sessionId: hit.sessionId, rowVersion: hit.rowVersion,
            scope: KV.observations(hit.sessionId), project, kind: "observation" as const,
            ...(hit.retrievalCoverage ? { retrievalCoverage: consolidationRetrievalCoverage([hit]) } : {}) };
        });
        const id = fingerprintId("consolidate", JSON.stringify([project, refs, minimum]));
        root = await loadRoot(kv, id);
        if (root) assertRootIdentity(root, "memory", project, refs);
        if (!root) {
          root = newRoot(id, "memory", project, refs); root.minObservations = minimum;
          await saveRoot(kv, root);
        }
      }
      return await runPagedCut(kv, root.id, manifest => new ConsolidationRequests(kv, manifest.id,
        (system, prompt, request) => runWorkflowAttempt(accounting, kv,
          { workflow: "consolidation", functionId: "mem::consolidate", project: manifest.project,
            sourceId: manifest.id, sourceFingerprint: manifest.id, jobId: manifest.id, unitId: request.id,
            phase: "generation", continuation: manifest.phase }, () => provider.compress(system, prompt))), async (manifest, candidate) => {
        const parsed = memoryFields(candidate.memoryXml!);
        if (candidate.memoryTitle) parsed.title = candidate.memoryTitle;
        const match = await retrieval.lookup(manifest.project, candidate.key);
        if (match.current && (match.current.project !== manifest.project || !match.current.isLatest)) throw new Error("consolidate_canonical_scope_mismatch");
        const id = fingerprintId("mem", `${manifest.id}:${candidate.key}`);
        const next: Memory = { ...parsed, id, project: manifest.project, createdAt: manifest.createdAt, updatedAt: manifest.createdAt,
          sourceObservationIds: candidate.sourceObservationIds ?? [], sessionIds: candidate.sourceSessionIds,
          version: (match.current?.version ?? 0) + 1, isLatest: true,
          ...(match.current ? { parentId: match.current.id, supersedes: [match.current.id, ...(match.current.supersedes ?? [])] } : {}) };
        const effect = batchEffectKey(`${manifest.id}:${candidate.key}`);
        const result = await retrieval.commit(manifest.project, candidate.key, match, next, effect);
        await recordAudit(kv, "consolidate", "mem::consolidate", [result.id], { captureId: manifest.id, created: result.created,
          applied: result.applied, rowVersion: result.rowVersion }, undefined, undefined, effect);
        return result;
      });
    },
  );
}
