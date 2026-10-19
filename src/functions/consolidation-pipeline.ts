import { observeWorkflowTerminal, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import { getAgentId } from "../config.js";
import type { WorkflowTerminalReceipt } from "../types.js";
import { runWorkflowAttempt, type WorkflowAttemptAccounting } from "./workflow-attempt-accounting.js";
import type { IIIClient } from "iii-sdk";
import { createHash } from "node:crypto";
type SelectedConsolidationControl = { project?: string; sessionId?: string; sourceFingerprint?: string };
type SelectedPipelineTier = "semantic" | "procedural" | "all" | "reflect" | "decay";
type SelectedPipelineStage = "semantic" | "procedural" | "reflect" | "decay" | "completed";
interface SelectedPipelineRun {
  workflowTerminal?: WorkflowTerminalReceipt;
  id: string; tier: SelectedPipelineTier; project: string; selectionId: string; sessionId: string;
  summaryFingerprint: string; sessionIds: string[]; stage: SelectedPipelineStage; createdAt: string;
  semanticManifestId?: string; proceduralManifestId?: string; reflectJobId?: string; decayManifestId?: string;
  selected: ConsolidationSelection[]; summaryIds: string[]; batchSourceIds?: string[]; proceduralSources?: ProceduralObservationReference[];
  results: Record<string, unknown>;
  terminalReflection?: { status: string; error?: string };
}
type ConsolidationPipelineInput = {
  tier?: string; force?: boolean; project?: string; batchResponse?: string; batchSourceFingerprint?: string;
  deferred?: boolean; batchEffectKey?: string; replacementOf?: string; batchSourceIds?: string[];
  batchCohort?: string; consolidationManifestId?: string; consolidationPartitionId?: string; decayManifestId?: string;
  consolidationSelectionId?: string; consolidationCatalogProject?: boolean; selectedRunId?: string;
  selected?: SelectedConsolidationControl;
};
import type {
  SemanticMemory,
  ProceduralMemory,
  SessionSummary,
  Memory,
  MemoryProvider,
  FireworksBatchWorkItem,
} from "../types.js";
import { KV, fingerprintId, generateId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { consolidationRetrievalFailureAction, SelectedObservationPreparationPending } from "./consolidation-selection.js";
import {
  SEMANTIC_MERGE_SYSTEM,
  buildSemanticMergePrompt,
} from "../prompts/consolidation.js";
import {
  ConsolidationRequests, acceptConsolidationResponse,
  requestInputBytes, type ConsolidationManifest,
} from "./consolidation-partitions.js";
import { recordAudit } from "./audit.js";
import { registerConsolidationResponseRepair } from "./consolidation-response-repair.js";
import { registerConsolidationPublicationRecovery } from "./consolidation-publication-recovery.js";
import {
  getConsolidationDecayDays,
  getConsolidationMinNewSummaries,
  isConsolidationEnabled,
} from "../config.js";
import { CONSOLIDATION_LAST_RUN_KEY } from "./consolidation-status.js";
import { logger } from "../logger.js";
import { assessConsolidationComplexity } from "./consolidation-complexity.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import { applyBatchEffect, batchEffectKey, runBatchCallback, withConsolidationAdmission } from "../state/batch-effects.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { selectConsolidationSummaries, fingerprintConsolidationSummaries, selectProceduralObservations, canonicalConsolidationId,
  consolidationRetrievalCoverage, ScopedObservationBudgetError, type ConsolidationSelection, type ConsolidationSummaryReference,
  type ProceduralObservationReference,
  type ConsolidationSelector, type ConsolidationCanonicalLookup, type ConsolidationCanonicalWriter } from "./consolidation-selection.js";
import { loadRoot, newRoot, saveRoot, pageKey, immutablePage, boundedBytes, semanticProjectCheckpointKey, runPagedCut, assertRootIdentity, consolidationIsPending, sourceRetrievalLimited, type PagedConsolidation, type PagedSource, type SourceHeader } from "./consolidation-manifest.js";
import { candidatePage, aggregateCandidateKey, publicationCandidate, consolidationPublicationEvidence, type AggregateCandidate } from "./consolidation-reduction.js";
import { isStateCounter } from "../state/state-transactions.js";
import { preparePublicationEvidence, evaluatePublicationEvidence, type PublicationEvidenceInput } from "./publication-evidence.js";
import { publicationExecutor } from "./publication-call-options.js";
import { proceduralCallOptions } from "./consolidation-procedural-format.js";
import { prepareStructuredCall } from "../providers/structured-output-contract.js";
import { readSource, verifyCapturedSource } from "./consolidation-capture.js";
import { REFLECT_CONTROL, REFLECT_JOBS, validManualReflectContinuation, type ReflectJob, type ReflectRequest } from "./reflect-queue-types.js";
import { runSelectedConsolidationDecay, type ConsolidationDecaySelector } from "./consolidation-decay.js";
import { retrievalRequest } from "../state/indexed-retrieval.js";
function stageFailureDetails(stage?: Record<string, unknown>): Record<string, unknown> {
  if (!stage) return {};
  return { ...(stage.retained === true ? { retained: true } : {}),
    ...(typeof stage.reason === "string" ? { reason: stage.reason } : {}),
    ...(typeof stage.error === "string" ? { error: stage.error } : {}),
    ...(typeof stage.errorCode === "string" ? { errorCode: stage.errorCode } : {}),
    ...(stage.diagnostics && typeof stage.diagnostics === "object" && !Array.isArray(stage.diagnostics) ? { diagnostics: stage.diagnostics } : {}) };
}
function stageFailed(stage: Record<string, unknown>): boolean {
  return stage.partial === true || stage.deferred === true || stage.retained === true || stage.success === false || Boolean(stage.error)
    || stage.status === "failed" || stage.status === "partial" || stage.terminal === true && stage.completed !== true;
}
interface CanonicalPublicationOperation {
  scope: "semantic" | "procedural"; key: string; id: string; effect: string; initial: SemanticMemory | ProceduralMemory;
}
interface LegacyPublicationPlan {
  project: string; resultHash: string; effectTimestamp: string; stage: "semantic" | "procedural";
  stageResult: Record<string, unknown>; operations: CanonicalPublicationOperation[];
}
function validateBatchSourceIds(value: unknown): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || value.length < 1 || value.length > 12) throw new Error("selected_consolidation_source_identity_mismatch");
  const ids: string[] = [];
  let bytes = 2;
  for (const item of value) {
    if (typeof item !== "string" || !item.trim() || Buffer.byteLength(item) > 256 || ids.includes(item)) {
      throw new Error("selected_consolidation_source_identity_mismatch");
    }
    bytes += Buffer.byteLength(item) + 3;
    if (bytes > 65_536) throw new Error("consolidation_source_partition_required");
    ids.push(item);
  }
  return ids;
}

function legacyConsolidationCallbackStage(sourceFingerprint: string | undefined, response: string): "semantic" | "procedural" | undefined {
  if (sourceFingerprint !== undefined) {
    if (/^fwbconsem_[a-f0-9]{16}$/.test(sourceFingerprint)) return "semantic";
    if (/^fwbconproc_[a-f0-9]{16}$/.test(sourceFingerprint)) return "procedural";
    return undefined;
  }
  const semantic = /<\s*\/?\s*(?:facts?|fact)\b/i.test(response);
  const procedural = /<\s*\/?\s*(?:procedures?|procedure)\b/i.test(response);
  if (semantic === procedural) return undefined;
  return semantic ? "semantic" : "procedural";
}

function legacyConsolidationCallbackResultHash(
  stage: string,
  project: string,
  sourceIds: string[] | undefined,
  sourceFingerprint: string | undefined,
  response: string | undefined,
  decayDays: number,
): string {
  return fingerprintId("concallback", JSON.stringify([
    stage,
    project,
    sourceIds ?? [],
    sourceFingerprint ?? null,
    response === undefined ? null : fingerprintId("conresponse", response),
    decayDays,
  ]));
}

const SEMANTIC_CHECKPOINT_KEY = "semantic-consolidation";
const SEMANTIC_ANCHOR_SUMMARIES = 5;
const SEMANTIC_NEW_SUMMARIES_PER_RUN = 15;
const MAX_SEMANTIC_REQUEST_INPUT_BYTES = 8 * 1024 - 128;

interface SemanticFactCandidate {
  fact: string;
  confidence: number;
  sourceSessionIds: string[];
  candidateIds: string[];
}


function normalizeSemanticFactsResponse(response: string): string {
  return response.trim().replace(/^```(?:xml)?\s*/i, "").replace(/\s*```$/, "").trim();
}

function isSemanticFactsResponse(response: string): boolean {
  const normalized = normalizeSemanticFactsResponse(response);
  if (/^<facts\s*\/>$/.test(normalized)) return true;
  const envelope = /^<facts>([\s\S]*)<\/facts>$/.exec(normalized);
  if (!envelope) return false;
  const body = envelope[1];
  const factRegex = /<fact\s+confidence="[^"]+"(?:\s+(?:sourceIds|candidateIds)="[^"]*")?>[^<]+<\/fact>/g;
  const factTagCount = body.match(/<fact\b/g)?.length ?? 0;
  if (factTagCount === 0) return body.trim().length === 0;
  let parsedTagCount = 0;
  while (factRegex.exec(body) !== null) parsedTagCount++;
  return parsedTagCount === factTagCount && body.replace(factRegex, "").trim().length === 0;
}

function isLegacyBareSemanticFactsResponse(response: string): boolean {
  const normalized = normalizeSemanticFactsResponse(response);
  return /^(?:<fact\s+confidence="[^"]+"(?:\s+sourceIds="[^"]*")?>[^<]+<\/fact>\s*)+$/.test(normalized);
}

function parseSemanticFacts(
  response: string,
  allowedSessionIds: readonly string[],
  allowLegacyOutput: boolean,
): SemanticFactCandidate[] {
  if (!isSemanticFactsResponse(response) && !(allowLegacyOutput && isLegacyBareSemanticFactsResponse(response))) {
    throw new Error("Semantic provider returned malformed fact output; source retained locally");
  }
  const allowed = new Set(allowedSessionIds);
  const candidates: SemanticFactCandidate[] = [];
  const factRegex = /<fact\s+confidence="([^"]+)"(?:\s+sourceIds="([^"]*)")?>([^<]+)<\/fact>/g;
  const normalized = normalizeSemanticFactsResponse(response);
  const factTagCount = normalized.match(/<fact\b/g)?.length ?? 0;
  let match;
  let parsedTagCount = 0;
  while ((match = factRegex.exec(normalized)) !== null) {
    parsedTagCount++;
    const confidenceValue = Number.parseFloat(match[1]);
    const confidence = Number.isNaN(confidenceValue) ? 0.5 : confidenceValue;
    const fact = match[3].trim();
    if (!fact) throw new Error("Semantic provider returned an empty fact; source retained locally");
    if (match[2] === undefined && !allowLegacyOutput) {
      throw new Error("Semantic provider omitted source IDs; source retained locally");
    }
    const reportedSources = match[2]?.split(",").map((id) => id.trim()).filter(Boolean);
    if (reportedSources?.some((id) => !allowed.has(id))) {
      throw new Error("Semantic provider returned unknown source IDs; source retained locally");
    }
    const sourceSessionIds = match[2] === undefined
      ? [...allowed]
      : [...new Set(reportedSources)];
    if (sourceSessionIds.length === 0) {
      throw new Error("Semantic provider returned a fact without valid source IDs; source retained locally");
    }
    const normalizedSources = [...sourceSessionIds].sort();
    const candidateId = fingerprintId("semcand", JSON.stringify([fact.toLowerCase(), normalizedSources]));
    candidates.push({ fact, confidence, sourceSessionIds, candidateIds: [candidateId] });
  }
  if (parsedTagCount !== factTagCount) {
    throw new Error("Semantic provider returned malformed fact output; source retained locally");
  }
  return candidates;
}

function validateProceduralResponse(response: string): void {
  const normalized = normalizeSemanticFactsResponse(response);
  if (/^<procedures\s*\/>$/.test(normalized)) return;
  const envelope = /^<procedures>([\s\S]*)<\/procedures>$/.exec(normalized);
  if (!envelope) throw new Error("Malformed procedural output; originals retained locally");
  const regex = /<procedure\s+name="([^"]+)"\s+trigger="([^"]+)">([\s\S]*?)<\/procedure>/g;
  let parsed = 0;
  for (const match of envelope[1].matchAll(regex)) {
    const steps = [...match[3].matchAll(/<step>([^<]+)<\/step>/g)];
    if (!match[1].trim() || !match[2].trim() || steps.length < 2 || steps.some(step => !step[1].trim())
      || match[3].replace(/<step>([^<]+)<\/step>/g, "").trim()) {
      throw new Error("Empty or unsupported procedural candidate; originals retained locally");
    }
    parsed++;
  }
  if (parsed !== (envelope[1].match(/<procedure\b/g)?.length ?? 0) || envelope[1].replace(regex, "").trim()) {
    throw new Error("Malformed procedural candidate; originals retained locally");
  }
}

async function advanceSemanticCheckpoint(kv: StateKV, next: SemanticCheckpoint | undefined, key = SEMANTIC_CHECKPOINT_KEY): Promise<void> {
  if (!next) return;
  await withKeyedLock(`semantic-watermark:${key}`, async () => {
    const current = await kv.get<SemanticCheckpoint>(KV.state, key);
    if (current && current.processedThrough > next.processedThrough) return;
    await kv.set(KV.state, key, current?.processedThrough === next.processedThrough ? {
      ...next, processedSessionIdsAtThrough: [...new Set([...current.processedSessionIdsAtThrough, ...next.processedSessionIdsAtThrough])],
    } : next);
  });
}

interface SemanticCheckpoint {
  processedThrough: string;
  processedSessionIdsAtThrough: string[];
}

interface SemanticInput {
  summaries: SessionSummary[];
  checkpoint?: SemanticCheckpoint;
}

// Call only while holding the cohort lock, after any current effect was admitted.
async function reconcileSemanticCohort(kv: StateKV, key: string, currentEffectKey?: string): Promise<void> {
  const cohort = await kv.get<{ workItemIds: string[]; checkpoint?: SemanticCheckpoint }>(KV.state, key);
  if (!cohort?.checkpoint || !cohort.workItemIds.length
    || (currentEffectKey && !cohort.workItemIds.some((id) => batchEffectKey(id) === currentEffectKey))) return;
  const members = await Promise.all(cohort.workItemIds.map(async (id) => {
    if (id.startsWith("conmanifest_")) return (await loadRoot(kv, id))?.phase === "completed" || (await kv.get<ConsolidationManifest>(KV.state, id))?.status === "completed";
    const work = await kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id);
    return work?.id === id && (work.state === "completed" || batchEffectKey(work.id) === currentEffectKey);
  }));
  if (!members.every(Boolean)) return;
  await advanceSemanticCheckpoint(kv, cohort.checkpoint);
}

function isAfterCheckpoint(summary: SessionSummary, checkpoint: SemanticCheckpoint): boolean {
  if (summary.createdAt > checkpoint.processedThrough) return true;
  return summary.createdAt === checkpoint.processedThrough &&
    !checkpoint.processedSessionIdsAtThrough.includes(summary.sessionId);
}

function checkpointFor(summary: SessionSummary): SemanticCheckpoint {
  return {
    processedThrough: summary.createdAt,
    processedSessionIdsAtThrough: [summary.sessionId],
  };
}

function selectSemanticInput(
  summaries: SessionSummary[],
  checkpoint: SemanticCheckpoint | null,
  minNewSummaries: number,
): SemanticInput {
  const newestFirst = [...summaries].sort((left, right) =>
    right.createdAt.localeCompare(left.createdAt),
  );
  if (!checkpoint) {
    const selected = newestFirst.slice(0, 20);
    const newest = newestFirst[0];
    return { summaries: selected, checkpoint: newest ? checkpointFor(newest) : undefined };
  }

  const newSummaries = newestFirst
    .filter((summary) => isAfterCheckpoint(summary, checkpoint))
    .sort((left, right) => left.createdAt.localeCompare(right.createdAt));
  if (newSummaries.length < minNewSummaries) return { summaries: [] };

  const selectedNew = newSummaries.slice(0, SEMANTIC_NEW_SUMMARIES_PER_RUN);
  const anchors = newestFirst
    .filter((summary) => !isAfterCheckpoint(summary, checkpoint))
    .slice(0, SEMANTIC_ANCHOR_SUMMARIES);
  return {
    summaries: [...anchors, ...selectedNew],
    checkpoint: checkpointFor(selectedNew[selectedNew.length - 1]),
  };
}


export function registerConsolidationPipelineFunction(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  llmRouter?: LlmTaskRouter,
  auxiliaryMaxInputChars?: number,
  batchQueue?: FireworksBatchQueue,
  _unusedProvider?: unknown,
  selectSources?: ConsolidationSelector,
  lookupCanonical?: ConsolidationCanonicalLookup,
  selectDecay?: ConsolidationDecaySelector,
  writeCanonical?: ConsolidationCanonicalWriter,
  accounting?: WorkflowAttemptAccounting,
): void {
  registerConsolidationResponseRepair(sdk, kv);
  registerConsolidationPublicationRecovery(sdk, kv, provider, llmRouter);
  const projectCursorKey = "consolidation-project-cursor";
  type ProjectCursor = { cursor?: string; project?: string; nextCursor?: string; hasMore?: boolean; selected?: ConsolidationSelection[] };
  async function chooseProject(): Promise<string | null> {
    const progress = await kv.get<ProjectCursor>(KV.config, projectCursorKey) ?? {};
    if (progress.project) return progress.project;
    const page = await retrievalRequest<{ status: string; items: Array<{ project: string }>; next_cursor?: string; has_more: boolean }>(kv,
      { action: "project_catalog_page", cursor: progress.cursor, limit: 1, max_bytes: 16_384 });
    if (page.status !== "ready") throw new Error("consolidation_project_catalog_preparing");
    if (!Array.isArray(page.items) || page.items.length > 1 || Buffer.byteLength(JSON.stringify(page)) > 16_384
      || page.items.some(item => typeof item.project !== "string" || !item.project.trim())) throw new Error("consolidation_project_catalog_invalid");
    if (!page.items.length) return null;
    await kv.set(KV.config, projectCursorKey, { cursor: progress.cursor, project: page.items[0].project,
      nextCursor: page.next_cursor, hasMore: page.has_more });
    return page.items[0].project;
  }
  async function evaluateEvidence(input: PublicationEvidenceInput, sourceId = input.operationId, continuation: string = input.workflow) {
    const replay = await evaluatePublicationEvidence(kv, input);
    if (replay.status !== "unavailable" || replay.reason !== "executor_unavailable") return replay;
    const request = preparePublicationEvidence(input);
    if ("status" in request) return request;
    return evaluatePublicationEvidence(kv, input, publicationExecutor(provider, llmRouter, "consolidation", {},
      (request, execute) => runWorkflowAttempt(accounting, kv, { workflow: "consolidation", functionId: "mem::consolidate-pipeline",
        project: input.project, sourceId, sourceFingerprint: sourceId,
        jobId: sourceId, unitId: request.requestId, phase: "publication", continuation }, execute)));
  }
  async function legacyEvidence(kind: "semantic" | "procedural", project: string, operationId: string,
    candidate: string, originals: Array<SessionSummary | Memory>) {
    const sources: PublicationEvidenceInput["sources"] = [];
    const refs: PagedSource[] = [];
    for (const original of originals) {
      const summary = "sessionId" in original;
      const id = summary ? original.sessionId : original.id;
      const sessionId = summary ? original.sessionId : original.sessionIds[0];
      const ref: PagedSource = { id, sessionId, scope: summary ? KV.summaries : KV.memories,
        kind: summary ? "summary" : "memory", project };
      const source = await readSource(kv, ref);
      if (source.json !== JSON.stringify(original)) throw new Error("consolidation_source_changed_before_effects");
      refs.push({ ...ref, rowVersion: source.version });
      sources.push({ id, key: id, scope: ref.scope, sessionId, project, version: source.version, content: source.json });
    }
    const evidence = await evaluateEvidence({ workflow: "consolidation", operationId: `${kind}:${operationId}`,
      project, candidate, sources, evidenceComplete: true });
    const verify = async () => {
      for (let index = 0; index < refs.length; index++) {
        if ((await readSource(kv, refs[index])).json !== sources[index].content) throw new Error("consolidation_source_changed_before_effects");
      }
    };
    return { evidence, verify };
  }
  function publicationOperation(current: PagedConsolidation, candidate: AggregateCandidate) {
    const now = current.createdAt;
    const effect = current.legacy?.effectKey ?? batchEffectKey(current.id);
    if (current.kind === "semantic") {
      const id = fingerprintId("sem", candidate.key);
      return { scope: "semantic" as const, key: candidate.key, id, effect,
        initial: { id, fact: candidate.fact!, confidence: candidate.confidence!, sourceSessionIds: candidate.sourceSessionIds,
          sourceMemoryIds: [], accessCount: 1, lastAccessedAt: now, strength: candidate.confidence!, createdAt: now, updatedAt: now } };
    }
    const id = fingerprintId("proc", candidate.key);
    return { scope: "procedural" as const, key: candidate.key, id,
      effect: current.legacy ? batchEffectKey(`${current.id}:${JSON.stringify([candidate.name!.toLowerCase(), candidate.trigger, candidate.steps])}`) : effect,
      initial: { id, name: candidate.name!, steps: candidate.steps!, triggerCondition: candidate.trigger!, frequency: 1,
        sourceSessionIds: candidate.sourceSessionIds, strength: 0.5, createdAt: now, updatedAt: now } };
  }
  async function recoverCanonicalOperation(operation: CanonicalPublicationOperation) {
    const fingerprint = createHash("sha256").update(JSON.stringify(["upsert", operation.scope, operation.key,
      operation.effect, operation.id, operation.initial])).digest("hex");
    const raw = await retrievalRequest<Record<string, unknown>>(kv, { action: "canonical_apply", receipt_only: true,
      scope: KV[operation.scope], partition: "", operation_kind: "upsert", target_id: operation.id,
      effect_key: operation.effect, operation_fingerprint: fingerprint, normalized_key: operation.key });
    if (raw.status === "not_applied" && Object.keys(raw).length === 1) return null;
    const receipt = raw.receipt;
    if (raw.status !== "already_applied" || raw.applied !== false || typeof raw.created !== "boolean"
      || typeof raw.canonical_id !== "string" || !raw.canonical_id || !isStateCounter(raw.row_version)
      || typeof raw.result_fingerprint !== "string" || !/^[a-f0-9]{64}$/.test(raw.result_fingerprint)
      || !receipt || typeof receipt !== "object" || Array.isArray(receipt)) throw new Error("consolidation_canonical_receipt_invalid");
    const fields = receipt as Record<string, unknown>;
    if (fields.operation_kind !== "upsert" || fields.effect_key !== operation.effect || fields.operation_fingerprint !== fingerprint
      || fields.canonical_id !== raw.canonical_id || fields.row_version !== raw.row_version || fields.created !== raw.created
      || fields.result_fingerprint !== raw.result_fingerprint) throw new Error("consolidation_canonical_receipt_invalid");
    return { applied: false, created: raw.created, id: raw.canonical_id, rowVersion: raw.row_version };
  }
  async function runManifest(manifest: ConsolidationManifest | PagedConsolidation, selected?: SelectedConsolidationControl): Promise<Record<string, unknown>> {
    let root = await loadRoot(kv, manifest.id);
    if (!root) {
      if (!("sources" in manifest)) throw new Error("consolidation_manifest_missing");
      if (manifest.status === "completed" && manifest.tier === "semantic") return { ...manifest.result, completed: true, replayed: true, manifestId: manifest.id, newFacts: 0, newProcedures: 0, appliedEffects: 0 };
      const sources = manifest.sources as Array<Record<string, unknown>>;
      const project = String(sources[0]?.project ?? "");
      if (!project || sources.length > 12) return { completed: false, partial: true, retained: true, manifestId: manifest.id, error: "consolidation_legacy_project_identity_required" };
      const refs: PagedSource[] = sources.map(source => {
        const summary = manifest.tier === "semantic";
        const observation = source.sourceKind === "observation";
        const id = String(summary ? source.sessionId : source.id);
        const sessionId = String(summary || observation ? source.sessionId : (source.sessionIds as string[] | undefined)?.[0] ?? "");
        return { id, sessionId, project, kind: summary ? "summary" : observation ? "observation" : "memory",
          scope: summary ? KV.summaries : observation ? KV.observations(sessionId) : KV.memories };
      });
      root = newRoot(manifest.id, manifest.tier, project, refs, manifest.deferred);
      root.createdAt = manifest.createdAt; root.checkpoint = manifest.checkpoint; root.phase = "legacy";
      root.legacy = { originalId: manifest.id, source: 0, fragment: 0, imported: false,
        effectKey: batchEffectKey(manifest.id), completed: manifest.status === "completed", ...(manifest.legacyCohort ? { cohort: manifest.legacyCohort } : {}) };
      await saveRoot(kv, root);
    }
    const selectedOrigin = await kv.get<{ project?: string; id?: string; runId?: string }>(KV.state, `selected-consolidation:${root.id}`);
    const selectedExecution = Boolean(selected || selectedOrigin);
    if (selectedOrigin && root.phase === "completed") {
      const identityRefs = root.refs.map(ref => ({ id: ref.id, sessionId: ref.sessionId, kind: ref.kind, project: ref.project, scope: ref.scope,
        ...(ref.rowVersion !== undefined ? { rowVersion: ref.rowVersion } : {}),
        ...(ref.retrievalCoverage !== undefined ? { retrievalCoverage: ref.retrievalCoverage } : {}) }));
      const callerPolicy = root.callerPolicy ? { deferred: root.callerPolicy.deferred, force: root.callerPolicy.force,
        compaction: root.callerPolicy.compaction, pipelineGate: root.callerPolicy.pipelineGate } : undefined;
      const expectedId = fingerprintId("conmanifest", JSON.stringify([root.kind, identityRefs, null,
        ...(callerPolicy ? [callerPolicy] : [])]));
      if (selectedOrigin.id !== root.id || selectedOrigin.project !== root.project || root.legacy || root.deferred
        || !root.callerPolicy || !root.callerPolicy.force || root.callerPolicy.deferred || root.callerPolicy.compaction
        || root.callerPolicy.pipelineGate || expectedId !== root.id || root.result?.completed !== true
        || root.result.manifestId !== root.id || stageFailed(root.result)
        || root.result.appliedEffects !== undefined && root.result.appliedEffects !== root.counters.applied
        || root.result.newFacts !== undefined && root.result.newFacts !== (root.kind === "semantic" ? root.counters.newlyCreated : 0)
        || root.result.newProcedures !== undefined && root.result.newProcedures !== (root.kind === "procedural" ? root.counters.newlyCreated : 0)) {
        return { success: false, completed: false, error: "selected_consolidation_replay_identity_mismatch" };
      }
      // Completion may add a checkpoint after identity capture; source and publication receipts remain authoritative.
      for (let index = 0; index < root.refs.length; index++) await verifyCapturedSource(kv, root, index);
      if (!Number.isSafeInteger(root.finalPages) || root.finalPages < 0 || root.finalPages > 12) {
        return { success: false, completed: false, error: "selected_consolidation_replay_page_limit" };
      }
      let published = 0;
      for (let pageNumber = 0; pageNumber < root.finalPages; pageNumber++) {
        const page = await candidatePage(kv, root, pageNumber);
        if (page.candidates.length > 12) throw new Error("selected_consolidation_replay_page_limit");
        for (const [offset, candidate] of page.candidates.entries()) {
          const aggregate = await kv.get<AggregateCandidate>(KV.state, aggregateCandidateKey(root, candidate));
          if (!aggregate) throw new Error("consolidation_aggregate_missing");
          if (aggregate.anchor !== `${pageNumber}:${offset}`) continue;
          const effect = await kv.get<{ rejected?: boolean; id?: string }>(KV.state, pageKey(root.id, "published", aggregate.anchor));
          if (!effect) throw new Error("consolidation_canonical_receipt_missing");
          if (effect.rejected === true) continue;
          const receipt = await recoverCanonicalOperation(publicationOperation(root, await publicationCandidate(kv, root, aggregate)));
          if (!receipt || receipt.id !== effect.id) throw new Error("consolidation_canonical_receipt_invalid");
          published++;
        }
      }
      if (published !== root.counters.published) throw new Error("consolidation_canonical_receipt_missing");
      return { ...root.result, completed: true, replayed: true, manifestId: root.id, captureId: root.id,
        newFacts: 0, newProcedures: 0, appliedEffects: 0 };
    }
    if (!selectedExecution && root.kind === "semantic" && !root.legacy && !root.checkpointKey) {
      root.checkpointKey = semanticProjectCheckpointKey(root.project);
      root.historicalBaselineCheckpoint = root.baselineCheckpoint;
      if (root.baselineCheckpoint && !["capture", "quality"].includes(root.phase)) {
        root.phase = "retained"; root.error = "consolidation_scoped_checkpoint_provenance_unknown";
      } else root.baselineCheckpoint = await kv.get<SemanticCheckpoint>(KV.state, root.checkpointKey) ?? undefined;
      await saveRoot(kv, root);
    }
    if (selected && (!selectedOrigin || root.legacy || root.deferred || selectedOrigin.project !== root.project || selectedOrigin.id !== root.id)) {
      return { success: false, completed: false, error: "selected_consolidation_origin_conflict" };
    }
    if (root.deferred && accounting?.isScoped({ workflow: "consolidation",
      project: root.project, sourceId: root.id })) throw new Error("workflow_scoped_batch_dispatch_unavailable");
    const manifestRequests: { current?: ConsolidationRequests } = {};
    const runCut = () => runPagedCut(kv, root!.id, current => manifestRequests.current = new ConsolidationRequests(kv, current.id, (system, prompt, request, prepared) => {
      return runWorkflowAttempt(accounting, kv, { workflow: "consolidation", functionId: "mem::consolidate-pipeline",
        project: current.project, sourceId: current.id, sourceFingerprint: current.id, jobId: current.id,
        unitId: request.id, phase: "generation", continuation: current.phase }, async () => {
          if (prepared) return prepared.execute();
          const options = proceduralCallOptions(request.outputContract);
          if (!llmRouter) return provider.summarize(system, prompt, options);
          const task = assessConsolidationComplexity({ prompt, maxAuxiliaryInputChars: auxiliaryMaxInputChars }).complex ? "conflict_resolution" : "consolidation";
          if (!request.outputContract) return llmRouter.run(task,
            selectedProvider => selectedProvider.summarize(system, prompt), response => typeof response === "string" && response.trim().length > 0);
          // Preserve the chosen route while retaining malformed responses and preventing schema-error fallback calls.
          const outcome = await llmRouter.run(task, async (selectedProvider): Promise<
            { ok: true; response: string } | { ok: false; error: unknown }
          > => {
            try { return { ok: true, response: await selectedProvider.summarize(system, prompt, options) }; }
            catch (error) { return { ok: false, error }; }
          }, () => true);
          if (!outcome.ok) throw outcome.error;
          return outcome.response;
        });
    }, selectedExecution ? undefined : current.deferred ? batchQueue : undefined, undefined, Boolean(current.legacy?.completed),
    (system, prompt, contract) => {
      const responseFormat = contract.options.responseFormat;
      if (responseFormat.type !== "json_schema") throw new Error("procedural_output_schema_required");
      const request = { operation: "summarize" as const, systemPrompt: system, userPrompt: prompt,
        contractVersion: contract.version, options: { responseFormat } };
      if (!llmRouter) return prepareStructuredCall(provider, request);
      const task = assessConsolidationComplexity({ prompt, maxAuxiliaryInputChars: auxiliaryMaxInputChars }).complex ? "conflict_resolution" : "consolidation";
      return llmRouter.prepareStructuredCall(task, request);
    }, prompt => {
      if (!llmRouter) return typeof provider.prepareStructuredCall === "function";
      const task = assessConsolidationComplexity({ prompt, maxAuxiliaryInputChars: auxiliaryMaxInputChars }).complex ? "conflict_resolution" : "consolidation";
      return llmRouter.canPrepareStructuredCall(task);
    }),
    async (current, candidate) => {
        const now = current.createdAt;
        const operation = publicationOperation(current, candidate);
        const scope = operation.scope;
        const match = await canonicalConsolidationId(lookupCanonical, scope, candidate.key);
        if (!writeCanonical) throw new Error("consolidation_canonical_commit_unavailable");
        const id = operation.id;
        const effect = operation.effect;
        if (operation.scope === "semantic") return writeCanonical<SemanticMemory>(operation.scope, candidate.key, match, id, effect, value => value ? {
          ...value, accessCount: value.accessCount + 1, updatedAt: now, lastAccessedAt: now,
          confidence: Math.max(value.confidence, candidate.confidence!),
          sourceSessionIds: [...new Set([...value.sourceSessionIds, ...candidate.sourceSessionIds])],
        } : operation.initial);
        return writeCanonical<ProceduralMemory>(operation.scope, candidate.key, match, id, effect, value => value ? {
          ...value, frequency: value.frequency + 1, updatedAt: now, strength: Math.min(1, value.strength + 0.1),
          sourceSessionIds: [...new Set([...value.sourceSessionIds, ...candidate.sourceSessionIds])],
        } : operation.initial);
      }, undefined, async (current, candidate) => {
        return evaluateEvidence(await consolidationPublicationEvidence(kv, current, candidate), current.id, current.phase);
      }, (current, candidate) => recoverCanonicalOperation(publicationOperation(current, candidate)));
    const result: Record<string, unknown> = root.callerPolicy ? await withConsolidationAdmission(runCut) : await runCut();
    if (result.completed !== true && result.queued === true && manifestRequests.current?.pending !== true) {
      result.queued = false;
      result.status = result.reason === "consolidation_continuation" ? "awaiting_dependency" : "running";
    }
    if (result.completed) {
      const completed = (await loadRoot(kv, root.id))!;
      if (completed.typesafeDecision?.pipelineGate !== "skip" && !completed.checkpoint && completed.kind === "semantic") {
        const headers = await Promise.all(completed.refs.map((_, index) => kv.get<SourceHeader>(KV.state, pageKey(completed.id, "source", index))));
        const newest = headers.filter((header): header is SourceHeader => Boolean(header?.createdAt)).sort((left, right) => right.createdAt!.localeCompare(left.createdAt!))[0];
        if (newest) completed.checkpoint = { processedThrough: newest.createdAt!, processedSessionIdsAtThrough: headers.filter(header => header?.createdAt === newest.createdAt).map(header => header!.sessionId) };
        await saveRoot(kv, completed);
      }
      if (!selectedExecution) {
        if (completed.legacy?.cohort) await withKeyedLock(`semantic-cohort:${completed.legacy.cohort}`, () => reconcileSemanticCohort(kv, completed.legacy!.cohort!));
        else if (completed.kind === "semantic" && completed.typesafeDecision?.pipelineGate !== "skip") await advanceSemanticCheckpoint(kv, completed.checkpoint, completed.checkpointKey ?? SEMANTIC_CHECKPOINT_KEY);
      }
    }
    if (!selectedExecution) {
      const previous = await kv.get<{ results?: Record<string, unknown> }>(KV.config, CONSOLIDATION_LAST_RUN_KEY);
      const failed = result.partial === true || result.retained === true || result.success === false || Boolean(result.error);
      await kv.set(KV.config, CONSOLIDATION_LAST_RUN_KEY, { at: new Date().toISOString(), tier: root.kind,
        results: { ...(previous?.results ?? {}), [root.kind]: result }, status: failed ? "partial" : result.completed === true ? "completed" : result.queued === true ? "queued" : "awaiting_dependency" });
    }
    return result;
  }

  async function startManifest<T>(tier: "semantic" | "procedural", sources: T[], deferred: boolean, checkpoint?: SemanticCheckpoint, legacyCohort?: string, replacementOf?: string,
    retrievalCoverage: PagedConsolidation["retrievalCoverage"] = [], callerPolicy?: PagedConsolidation["callerPolicy"], selected?: SelectedConsolidationControl,
    selectedRunId?: string): Promise<Record<string, unknown>> {
    const entries = sources as Array<Record<string, unknown>>;
    const refs: PagedSource[] = entries.map(source => {
      const kind = tier === "semantic" ? "summary" : source.sourceKind === "observation" ? "observation" : "memory";
      const id = String(kind === "summary" ? source.sessionId : source.id);
      const sessionId = String(source.sessionId ?? (source.sessionIds as string[] | undefined)?.[0] ?? "");
      return { id, sessionId, kind, project: String(source.project ?? ""), scope: kind === "summary" ? KV.summaries : kind === "observation" ? KV.observations(sessionId) : KV.memories,
        ...(typeof source.rowVersion === "string" ? { rowVersion: source.rowVersion } : {}),
        ...(source.retrievalCoverage ? { retrievalCoverage: consolidationRetrievalCoverage([{ retrievalCoverage: source.retrievalCoverage }]) } : {}) };
    });
    const id = fingerprintId("conmanifest", JSON.stringify([tier, refs, checkpoint ?? null, ...(legacyCohort ? [legacyCohort, replacementOf] : []), ...(callerPolicy ? [callerPolicy] : [])]));
    let root = await loadRoot(kv, id);
    if (root) assertRootIdentity(root, tier, refs[0]?.project ?? "", refs, callerPolicy);
    if (!root) { root = newRoot(id, tier, refs[0]?.project ?? "", refs, deferred && Boolean(batchQueue), callerPolicy); root.checkpoint = checkpoint;
      root.retrievalCoverage = [...new Set([...root.retrievalCoverage, ...retrievalCoverage])].sort();
      if (tier === "semantic" && !legacyCohort) {
        if (!selected) {
          root.checkpointKey = semanticProjectCheckpointKey(root.project);
          root.baselineCheckpoint = await kv.get<SemanticCheckpoint>(KV.state, root.checkpointKey) ?? undefined;
          root.minimumNewSummaries = getConsolidationMinNewSummaries();
        }
      }
      if (selected) await kv.set(KV.state, `selected-consolidation:${id}`, { project: root.project, id, ...(selectedRunId ? { runId: selectedRunId } : {}) });
      await saveRoot(kv, root); }
    if (legacyCohort && replacementOf) {
      root.legacy = { originalId: id, source: refs.length, fragment: 0, imported: true, verified: true, cohort: legacyCohort };
      await saveRoot(kv, root);
      await withKeyedLock(`semantic-cohort:${legacyCohort}`, async () => {
        const cohort = await kv.get<{ workItemIds: string[]; checkpoint?: SemanticCheckpoint }>(KV.state, legacyCohort);
        if (!cohort) throw new Error("Original consolidation cohort missing; watermark retained locally");
        await kv.set(KV.state, legacyCohort, { ...cohort, workItemIds: [...new Set(cohort.workItemIds.map(member => member === replacementOf ? id : member))] });
      });
    }
    return runManifest(root, selected);
  }

  const selectedRunKey = (id: string) => `selected-consolidation-run:${id}`;
  const SELECTED_OUTPUT_SCAN_PAGES = 12;
  const selectedTierStages = (tier: SelectedPipelineTier): SelectedPipelineStage[] => tier === "all"
    ? ["semantic", "procedural", "reflect", "decay"] : [tier];
  async function selectedPublishedIds(run: SelectedPipelineRun, tier: "semantic" | "procedural"): Promise<string[]> {
    const manifestId = tier === "semantic" ? run.semanticManifestId : run.proceduralManifestId;
    if (!manifestId) return [];
    const root = await loadRoot(kv, manifestId);
    if (!root || root.kind !== tier || root.project !== run.project || root.phase !== "completed") throw new Error("selected_consolidation_manifest_mismatch");
    const ids: string[] = [];
    const pagesToRead = Math.min(root.finalPages, SELECTED_OUTPUT_SCAN_PAGES);
    for (let pageNumber = 0; pageNumber < pagesToRead && ids.length < 12; pageNumber++) {
      const page = await candidatePage(kv, root, pageNumber);
      if (page.candidates.length > 12) throw new Error("selected_consolidation_output_page_limit");
      for (const candidate of page.candidates) {
        if (!candidate.sourceSessionIds.some(sessionId => run.sessionIds.includes(sessionId))) continue;
        const aggregate = await kv.get<{ anchor: string; sourceSessionIds: string[] }>(KV.state, aggregateCandidateKey(root, candidate));
        if (!aggregate?.anchor || !aggregate.sourceSessionIds.some(sessionId => run.sessionIds.includes(sessionId))) continue;
        const effect = await kv.get<{ rejected?: boolean; id?: string }>(KV.state, pageKey(root.id, "published", aggregate.anchor));
        if (effect && !effect.rejected && effect.id && !ids.includes(effect.id)) ids.push(effect.id);
        if (ids.length >= 12) break;
      }
    }
    if (ids.length < 12 && pagesToRead === SELECTED_OUTPUT_SCAN_PAGES && pagesToRead < root.finalPages) {
      throw new Error("selected_consolidation_output_scan_limit");
    }
    return ids;
  }
  async function runSelectedPipeline(data: ConsolidationPipelineInput): Promise<Record<string, unknown>> {
    const tier = (data.tier ?? "all") as SelectedPipelineTier;
    if (!data.force || data.deferred || !data.project?.trim() || !data.selected
      || !["semantic", "procedural", "all", "reflect", "decay"].includes(tier)) {
      return { success: false, completed: false, error: "selected_consolidation_requires_explicit_direct_tier" };
    }
    const project = data.project.trim();
    if (data.selected.project !== undefined && data.selected.project !== project) return { success: false, completed: false, error: "selected_consolidation_project_mismatch" };
    let activeRun: SelectedPipelineRun | undefined;
    try {
      const batchSourceIds = validateBatchSourceIds(data.batchSourceIds);
      const requestedRunId = data.selectedRunId;
      let run = requestedRunId ? await kv.get<SelectedPipelineRun>(KV.state, selectedRunKey(requestedRunId)) : null;
      if (run && (run.id !== requestedRunId || run.tier !== tier || run.project !== project ||
          data.consolidationSelectionId !== undefined && run.selectionId !== data.consolidationSelectionId ||
          batchSourceIds !== undefined && JSON.stringify(batchSourceIds) !== JSON.stringify(run.batchSourceIds))) {
        throw new Error("selected_consolidation_run_identity_mismatch");
      }
      if (!run) {
        let selected: ConsolidationSelection[];
        let selectionId = data.consolidationSelectionId;
        if (selectionId) {
          const saved = await kv.get<{ project: string; tier: string; selected: ConsolidationSelection[] }>(KV.state, selectionId);
          if (!saved || saved.project !== project || saved.tier !== tier || !Array.isArray(saved.selected)) throw new Error("consolidation_selection_identity_mismatch");
          selected = saved.selected;
        } else if (batchSourceIds !== undefined && tier !== "procedural") {
          selected = [];
        } else if (batchSourceIds !== undefined && tier === "procedural") {
          if (!selectSources) throw new Error("consolidation_selector_unavailable");
          selected = await selectSources(project, batchSourceIds);
        } else {
          if (!selectSources) throw new Error("consolidation_selector_unavailable");
          selected = await selectSources(project);
        }
        if (!Array.isArray(selected) || selected.length > 12 || Buffer.byteLength(JSON.stringify(selected)) > 65_536) {
          throw new Error("consolidation_source_partition_required");
        }
        if (tier === "procedural" && batchSourceIds?.some(id => !selected.some(reference => reference.sessionId === id))) {
          throw new Error("selected_consolidation_source_identity_mismatch");
        }
        const summaries = await selectConsolidationSummaries(kv, project, batchSourceIds,
          batchSourceIds === undefined || tier === "procedural" ? async () => selected : undefined);
        if (!summaries.length || summaries.length > 12) throw new Error("consolidation_no_relevant_sources");
        const rows = await Promise.all(summaries.map(summary => kv.get<SessionSummary>(KV.summaries, summary.sessionId)));
        if (rows.some((summary, index) => !summary || summary.project !== project || summary.sessionId !== summaries[index]!.sessionId)) {
          throw new Error("selected_consolidation_summary_source_changed");
        }
        const summaryFingerprint = batchSourceIds === undefined ? fingerprintId("consummary", JSON.stringify(rows))
          : await fingerprintConsolidationSummaries(kv, project, summaries);
        if (!selectionId) {
          selectionId = fingerprintId("conselection", JSON.stringify([project, tier, selected,
            batchSourceIds === undefined ? null : summaries.map(summary => [summary.sessionId, summary.sessionVersion, summary.rowVersion])]));
          const priorSelection = await kv.get<{ project: string; tier: string; selected: ConsolidationSelection[] }>(KV.state, selectionId);
          if (priorSelection && (priorSelection.project !== project || priorSelection.tier !== tier
            || JSON.stringify(priorSelection.selected) !== JSON.stringify(selected))) throw new Error("consolidation_selection_identity_mismatch");
          if (!priorSelection) await kv.set(KV.state, selectionId, { project, tier, selected });
        }
        const selectedSessionId = data.selected.sessionId ?? summaries[0]!.sessionId;
        if (!summaries.some(summary => summary.sessionId === selectedSessionId)) throw new Error("selected_consolidation_session_mismatch");
        const expectedRunId = fingerprintId("conrun", JSON.stringify([tier, project, selectionId]));
        if (requestedRunId && requestedRunId !== expectedRunId) throw new Error("selected_consolidation_run_identity_mismatch");
        const id = requestedRunId ?? expectedRunId;
        const priorRun = await kv.get<SelectedPipelineRun>(KV.state, selectedRunKey(id));
        if (priorRun) {
          if (priorRun.id !== id || priorRun.tier !== tier || priorRun.project !== project || priorRun.selectionId !== selectionId
            || priorRun.summaryFingerprint !== summaryFingerprint
            || JSON.stringify(priorRun.batchSourceIds) !== JSON.stringify(batchSourceIds ? summaries.map(summary => summary.sessionId) : undefined)) {
            throw new Error("selected_consolidation_run_identity_mismatch");
          }
          run = priorRun;
        } else {
          const initial = selectedTierStages(tier)[0]!;
          run = { id, tier, project, selectionId: selectionId!, sessionId: selectedSessionId, summaryFingerprint,
            sessionIds: summaries.map(summary => summary.sessionId), summaryIds: summaries.map(summary => summary.id),
            ...(batchSourceIds ? { batchSourceIds: summaries.map(summary => summary.sessionId) } : {}),
            selected, stage: initial, createdAt: new Date().toISOString(), results: {} };
          await kv.set(KV.state, selectedRunKey(run.id), run);
        }
      }
      activeRun = run;
      if (data.selected.sessionId !== undefined && data.selected.sessionId !== run.sessionId) throw new Error("selected_consolidation_session_mismatch");
      const directSourceRefs = run.batchSourceIds
        ? await selectConsolidationSummaries(kv, project, run.batchSourceIds) : undefined;
      const sourceRows = await Promise.all(run.sessionIds.map(sessionId => kv.get<SessionSummary>(KV.summaries, sessionId)));
      const currentSummaryFingerprint = directSourceRefs
        ? await fingerprintConsolidationSummaries(kv, project, directSourceRefs) : fingerprintId("consummary", JSON.stringify(sourceRows));
      if (sourceRows.some((summary, index) => !summary || summary.project !== run!.project || summary.sessionId !== run!.sessionIds[index])
        || currentSummaryFingerprint !== run.summaryFingerprint) {
        throw new Error("selected_consolidation_summary_source_changed");
      }
      const continuation = { tier, project, force: true, selected: data.selected, consolidationSelectionId: run.selectionId, selectedRunId: run.id,
        ...(run.batchSourceIds ? { batchSourceIds: run.batchSourceIds } : {}) };
      const completeResult = async () => {
        if (run!.workflowTerminal?.outcome === "completed") await observeWorkflowTerminal(kv, { ...run!.workflowTerminal,
          workflow: "consolidation", producer: "selected-consolidation-pipeline", executionLevel: "workflow" });
        else await markWorkflowStatisticsGap(kv, "consolidation");
        return { success: true, completed: true, queued: false, project, selectedRunId: run!.id,
          selectedSourceIds: run!.sessionIds, sourceCount: run!.sessionIds.length, selectionCoverage: "selected_relevant_sources",
          results: structuredClone(run!.results) };
      };
      if (run.terminalReflection) {
        if (run.workflowTerminal) await observeWorkflowTerminal(kv, { ...run.workflowTerminal,
          workflow: "consolidation", producer: "selected-consolidation-pipeline", executionLevel: "workflow" });
        else await markWorkflowStatisticsGap(kv, "consolidation");
        return { success: run.terminalReflection.status === "partial", completed: false,
        terminal: true, partial: true, queued: false, project, selectedRunId: run.id,
        error: run.terminalReflection.error, results: run.results };
      }
      if (run.stage === "completed") return completeResult();
      const stages = selectedTierStages(tier);
      const nextStage = (current: SelectedPipelineStage): SelectedPipelineStage => {
        const index = stages.indexOf(current);
        return index < 0 || index + 1 >= stages.length ? "completed" : stages[index + 1]!;
      };
      const summaries = directSourceRefs ?? await selectConsolidationSummaries(kv, project, run.sessionIds, async () => run!.selected);
      const policy: NonNullable<PagedConsolidation["callerPolicy"]> = { deferred: false, force: true, compaction: false, pipelineGate: false };
      const continueRun = (stage: Record<string, unknown>) => ({ ...stage, success: stage.success !== false && !stageFailed(stage), completed: false,
        project, selectedRunId: run!.id, stage: run!.stage, results: structuredClone(run!.results), continuation: { ...continuation,
          ...(run!.semanticManifestId ? { semanticManifestId: run!.semanticManifestId } : {}),
          ...(run!.proceduralManifestId ? { proceduralManifestId: run!.proceduralManifestId } : {}),
          ...(run!.reflectJobId ? { reflectJobId: run!.reflectJobId } : {}),
          ...(run!.decayManifestId ? { decayManifestId: run!.decayManifestId } : {}) } });
      let stageResult: Record<string, unknown>;
      if (run.stage === "semantic") {
        if ((tier === "all" || tier === "semantic") && (summaries.length >= 5 || run.batchSourceIds?.length)) {
          stageResult = await startManifest("semantic", summaries, false, undefined, undefined, undefined,
            consolidationRetrievalCoverage(run.selected), policy, data.selected, run.id);
          run.semanticManifestId = String(stageResult.manifestId ?? (stageResult.continuation as Record<string, unknown> | undefined)?.consolidationManifestId ?? run.semanticManifestId ?? "") || undefined;
        } else stageResult = { completed: true, skipped: true, reason: "fewer than 5 summaries", sourceCount: summaries.length };
      } else if (run.stage === "procedural") {
        if (!run.proceduralSources) {
          if (run.batchSourceIds && tier === "all" && !selectSources) throw new Error("consolidation_selector_unavailable");
          const selected = run.batchSourceIds && tier === "all"
            ? await selectSources!(project, run.batchSourceIds) : run.selected;
          if (run.batchSourceIds && selected.some(reference => !run.batchSourceIds!.includes(reference.sessionId))) {
            throw new Error("selected_consolidation_source_identity_mismatch");
          }
          if (tier === "procedural" && run.batchSourceIds?.some(id => !selected.some(reference => reference.sessionId === id))) {
            throw new Error("selected_consolidation_source_identity_mismatch");
          }
          run.proceduralSources = await selectProceduralObservations(kv, project, summaries, selected);
          await kv.set(KV.state, selectedRunKey(run.id), run);
        }
        const sources = await selectProceduralObservations(kv, project, summaries, run.proceduralSources);
        if (JSON.stringify(sources) !== JSON.stringify(run.proceduralSources)) {
          throw new Error("selected_consolidation_source_identity_mismatch");
        }
        if (sources.length >= 2) {
          stageResult = await startManifest("procedural", sources, false, undefined, undefined, undefined,
            consolidationRetrievalCoverage(sources), policy, data.selected, run.id);
          run.proceduralManifestId = String(stageResult.manifestId ?? (stageResult.continuation as Record<string, unknown> | undefined)?.consolidationManifestId ?? run.proceduralManifestId ?? "") || undefined;
        } else stageResult = { completed: true, skipped: true, reason: "fewer than 2 recurring patterns", sourceCount: sources.length };
      } else if (run.stage === "reflect") {
        const factIds = await selectedPublishedIds(run, "semantic");
        const lessonIds = await selectedPublishedIds(run, "procedural");
        if (factIds.length + lessonIds.length > 12) throw new Error("selected_reflect_source_limit");
        const reflectSelected = { sessionId: run.sessionId };
        if (!run.reflectJobId) {
          const accepted = await sdk.trigger({ function_id: "mem::reflect", payload: { project, sessionId: run.sessionId,
            summaryId: run.sessionId, factIds, lessonIds, requestId: fingerprintId("conreflect", run.id), maxClusters: 1,
            selected: reflectSelected } }) as Record<string, unknown>;
          if (accepted.success === false) throw new Error(String(accepted.error ?? "selected_reflect_admission_failed"));
          if (typeof accepted.jobId !== "string") {
            stageResult = { completed: true, skipped: true, reason: "selected_reflect_no_job" };
          } else {
            run.reflectJobId = accepted.jobId;
            await kv.set(KV.state, selectedRunKey(run.id), run);
            stageResult = await sdk.trigger({ function_id: "mem::reflect-unit", payload: { jobId: run.reflectJobId, selected: reflectSelected } }) as Record<string, unknown>;
          }
        } else stageResult = await sdk.trigger({ function_id: "mem::reflect-unit", payload: { jobId: run.reflectJobId, selected: reflectSelected } }) as Record<string, unknown>;
        if (run.reflectJobId) {
          const job = await kv.get<ReflectJob>(REFLECT_JOBS, run.reflectJobId);
          if (job && ["completed", "partial", "failed", "blocked"].includes(job.status)) {
            const insightIds: string[] = [];
            for (let sequence = 0; sequence < (job.unitSequence ?? 0); sequence++) {
              const receipt = await kv.get<{ insightIds?: string[] }>(REFLECT_CONTROL, `${job.id}:result:${sequence}`);
              for (const id of receipt?.insightIds ?? []) if (!insightIds.includes(id) && insightIds.length < 12) insightIds.push(id);
            }
            run.results.reflect = { completed: job.status === "completed", jobId: job.id, status: job.status, insightIds };
            if (job.status !== "completed") {
              run.terminalReflection = { status: job.status, error: job.error ?? "selected_reflect_incomplete" };
              run.workflowTerminal ??= { executionId: run.id, agentId: getAgentId(), occurredAt: new Date().toISOString(),
                outcome: job.status === "partial" ? "partial" : "failed" };
              await kv.set(KV.state, selectedRunKey(run.id), run);
              await observeWorkflowTerminal(kv, { ...run.workflowTerminal,
                workflow: "consolidation", producer: "selected-consolidation-pipeline", executionLevel: "workflow" });
              return { success: job.status === "partial", completed: false, terminal: true, partial: true,
                queued: false, selectedRunId: run.id, error: run.terminalReflection.error, results: run.results };
            }
            stageResult = { completed: true, jobId: job.id, status: job.status, insightIds };
          } else return continueRun({ ...stageResult, completed: false, queued: false });
        }
      } else if (run.stage === "decay") {
        const recordIds = tier === "all" ? { semantic: await selectedPublishedIds(run, "semantic"), procedural: await selectedPublishedIds(run, "procedural") } : undefined;
        const decayInput = run.decayManifestId ? { manifestId: run.decayManifestId } : {
          project, sessionIds: run.sessionIds, sourceVersion: JSON.stringify([run.summaryFingerprint, run.semanticManifestId ?? null, run.proceduralManifestId ?? null]),
          ...(recordIds ? { recordIds } : {}) };
        stageResult = await runSelectedConsolidationDecay(kv, selectDecay, getConsolidationDecayDays(), decayInput as Parameters<typeof runSelectedConsolidationDecay>[3]);
        if (typeof stageResult.manifestId === "string") run.decayManifestId = stageResult.manifestId;
        if (stageResult.completed !== true) {
          await kv.set(KV.state, selectedRunKey(run.id), run);
          return continueRun(stageResult);
        }
      } else throw new Error("selected_consolidation_stage_invalid");
      run.results[run.stage] = stageResult;
      if (stageResult.completed === true) run.stage = nextStage(run.stage);
      if (run.stage === "completed") run.workflowTerminal ??= { executionId: run.id,
        agentId: getAgentId(), occurredAt: new Date().toISOString(), outcome: "completed" };
      await kv.set(KV.state, selectedRunKey(run.id), run);
      return run.stage === "completed" ? completeResult() : continueRun(stageResult);
    } catch (error) {
      if (consolidationIsPending(error)) return { success: true, completed: false, queued: false, status: "awaiting_dependency",
        reason: "source_index_pending", project, ...(activeRun ? { selectedRunId: activeRun.id, stage: activeRun.stage, results: structuredClone(activeRun.results) } : {}),
        ...(error instanceof SelectedObservationPreparationPending ? { diagnostics: error.diagnostics } : {}),
        continuation: { ...data, ...(activeRun ? { selectedRunId: activeRun.id, consolidationSelectionId: activeRun.selectionId } : {}) } };
      return { success: false, completed: false, partial: true, error: error instanceof Error ? error.message : "selected_consolidation_failed",
        ...(data.selectedRunId ? { selectedRunId: data.selectedRunId } : {}) };
    }
  }

  sdk.registerFunction("mem::consolidate-pipeline", async (data?: ConsolidationPipelineInput) => {
      if (!kv.indexedRetrieval) return { success: false, completed: false, error: "consolidation_indexed_retrieval_required" };
      if (data?.selected) return runSelectedPipeline(data);
      if (data?.decayManifestId) {
        const result = await runSelectedConsolidationDecay(kv, selectDecay, getConsolidationDecayDays(), { manifestId: data.decayManifestId });
        return { success: true, ...result };
      }
      if (data?.consolidationManifestId) {
        if (data.batchResponse !== undefined || data.consolidationPartitionId !== undefined) {
          if (!data.consolidationPartitionId || typeof data.batchResponse !== "string") return { success: false, error: "Missing durable consolidation callback payload" };
          await acceptConsolidationResponse(kv, data.consolidationManifestId, data.consolidationPartitionId, data.batchResponse);
        }
        const manifest = await loadRoot(kv, data.consolidationManifestId) ?? await kv.get<ConsolidationManifest>(KV.state, data.consolidationManifestId);
        if (!manifest) return { success: false, error: "Consolidation manifest missing; source retained locally" };
        const result = await runManifest(manifest, data.selected);
        return { success: !result.error, ...result };
      }
      if (kv.indexedRetrieval && (data?.batchResponse !== undefined || data?.batchEffectKey !== undefined)
        && (typeof data.batchResponse !== "string" || typeof data.batchEffectKey !== "string" || !/^[a-f0-9]{64}$/.test(data.batchEffectKey))) {
        return { success: false, error: "Missing legacy consolidation callback identity" };
      }
      if (kv.indexedRetrieval && !data?.batchEffectKey && !data?.batchResponse && !data?.replacementOf) {
        const tier = data?.tier ?? "all";
        if (!data?.force && !isConsolidationEnabled()) return { success: false, skipped: true, completed: false, reason: "Consolidation disabled: CONSOLIDATION_ENABLED=true required" };
        if (!["all", "semantic", "reflect", "procedural", "decay"].includes(tier)) return { success: false, error: "invalid_consolidation_tier" };
        const invocationPolicy: NonNullable<PagedConsolidation["callerPolicy"]> = {
          deferred: data?.deferred === true,
          force: data?.force === true,
          compaction: false,
          pipelineGate: false,
        };
        const catalogSelected = data?.consolidationCatalogProject === true || !data?.project?.trim();
        let selectedProject = data?.project?.trim();
        let activeSelectionId = data?.consolidationSelectionId;
        let selectionPhase = "select_project";
        try {
          const project = selectedProject || await chooseProject(); selectedProject = project ?? undefined;
          if (!project) return { success: true, completed: true, sourceCount: 0, reason: "project_catalog_empty" };
          if ((tier === "all" || tier === "procedural") && !selectSources) throw new Error("consolidation_selector_unavailable");
          if ((tier === "all" || tier === "semantic" || tier === "procedural") && (!lookupCanonical || !writeCanonical)) {
            throw new Error("consolidation_canonical_commit_unavailable");
          }
          const savedProject = catalogSelected ? await kv.get<ProjectCursor>(KV.config, projectCursorKey) : null;
          selectionPhase = "load_selection";
          const savedSelection = data?.consolidationSelectionId ? await kv.get<{ project: string; tier: string; selected: ConsolidationSelection[];
            batchSourceIds?: string[]; sourceVersions?: Array<[string, string, string]>; proceduralSources?: ProceduralObservationReference[] }>(KV.state, data.consolidationSelectionId) : null;
          if (data?.consolidationSelectionId && (!savedSelection || savedSelection.project !== project || savedSelection.tier !== tier)) throw new Error("consolidation_selection_identity_mismatch");
          const batchSourceIds = validateBatchSourceIds(data?.batchSourceIds);
          const explicitSummarySources = batchSourceIds !== undefined;
          selectionPhase = "select_sources";
          const selected = explicitSummarySources && tier !== "procedural" ? [] : savedSelection?.selected ?? savedProject?.selected
            ?? (explicitSummarySources ? selectSources ? await selectSources(project, batchSourceIds) : [] : selectSources ? await selectSources(project) : []);
          if (tier === "procedural" && batchSourceIds?.some(id => !selected.some(reference => reference.sessionId === id))) {
            throw new Error("selected_consolidation_source_identity_mismatch");
          }
          const retrievalCoverage = consolidationRetrievalCoverage(selected.map(ref => ({ retrievalCoverage: "retrievalCoverage" in ref ? ref.retrievalCoverage : undefined })));
          if (selected.length > 12 || Buffer.byteLength(JSON.stringify(selected)) > 65_536) throw new Error("consolidation_source_partition_required");
          if (!selected.length && !batchSourceIds?.length) return { success: true, completed: true, project,
            selectedSourceIds: [], sourceCount: 0, selectionCoverage: "selected_relevant_sources", results: {
              [tier]: { completed: true, skipped: true, reason: "no_relevant_sources" } } };
          if ((tier === "all" || tier === "decay") && !selectDecay) throw new Error("consolidation_decay_index_unavailable");
          selectionPhase = "select_summaries";
          const summaries = await selectConsolidationSummaries(kv, project, batchSourceIds,
            selectSources ? async () => selected : undefined);
          const sourceVersions = explicitSummarySources ? summaries.map(summary => [summary.sessionId, summary.sessionVersion ?? "", summary.rowVersion ?? ""] as [string, string, string]) : undefined;
          if (sourceVersions?.some(([, sessionVersion, summaryVersion]) => !sessionVersion || !summaryVersion)) {
            throw new Error("selected_consolidation_source_identity_mismatch");
          }
          const expectedSelectionId = fingerprintId("conselection", explicitSummarySources
            ? JSON.stringify([project, tier, selected, sourceVersions]) : JSON.stringify([project, tier, selected]));
          const selectionId = data?.consolidationSelectionId ?? expectedSelectionId;
          if (explicitSummarySources && data?.consolidationSelectionId && data.consolidationSelectionId !== expectedSelectionId) {
            throw new Error("consolidation_selection_identity_mismatch");
          }
          if (savedSelection && explicitSummarySources
            && (JSON.stringify(savedSelection.batchSourceIds) !== JSON.stringify(summaries.map(summary => summary.sessionId))
              || JSON.stringify(savedSelection.sourceVersions) !== JSON.stringify(sourceVersions))) {
            throw new Error("consolidation_selection_identity_mismatch");
          }
          selectionPhase = "save_selection";
          if (!savedSelection) await kv.set(KV.state, selectionId, { project, tier, selected,
            ...(explicitSummarySources ? { batchSourceIds: summaries.map(summary => summary.sessionId), sourceVersions } : {}) });
          activeSelectionId = selectionId;
          if (savedProject && !savedProject.selected) await kv.set(KV.config, projectCursorKey, { ...savedProject, selected });
          const results: Record<string, unknown> = {};
          if (tier === "all" || tier === "semantic") {
            selectionPhase = "semantic_manifest";
            results.semantic = summaries.length >= 5 || data?.batchSourceIds?.length ? await startManifest("semantic", summaries, Boolean(data?.deferred), undefined, undefined, undefined, retrievalCoverage, invocationPolicy, data?.selected)
              : { completed: true, skipped: true, newFacts: 0, reason: "fewer than 5 summaries", sourceCount: summaries.length };
            const stage = results.semantic as Record<string, unknown>;
            if (stage.completed !== true || stageFailed(stage)) return { ...stageFailureDetails(stage), success: !stageFailed(stage), completed: false, queued: stage.queued === true && !stageFailed(stage), partial: stageFailed(stage), project,
              results, continuation: { tier, project, force: data?.force, deferred: data?.deferred, batchSourceIds: data?.batchSourceIds, consolidationSelectionId: selectionId, consolidationCatalogProject: catalogSelected } };
          }
          if (tier === "all" || tier === "procedural") {
            selectionPhase = "procedural_manifest";
            let proceduralSources = savedSelection?.proceduralSources;
            if (proceduralSources !== undefined) {
              const verified = await selectProceduralObservations(kv, project, summaries, proceduralSources);
              if (JSON.stringify(verified) !== JSON.stringify(proceduralSources)) throw new Error("consolidation_selection_identity_mismatch");
              proceduralSources = verified;
            }
            if (explicitSummarySources && tier === "all" && !proceduralSources) {
              const scoped = await selectSources?.(project, batchSourceIds) ?? [];
              if (!Array.isArray(scoped) || scoped.length > 12 || Buffer.byteLength(JSON.stringify(scoped)) > 65_536
                || scoped.some(reference => !batchSourceIds!.includes(reference.sessionId))) {
                throw new Error("selected_consolidation_source_identity_mismatch");
              }
              proceduralSources = await selectProceduralObservations(kv, project, summaries, scoped);
              await kv.set(KV.state, selectionId, { project, tier, selected, batchSourceIds: summaries.map(summary => summary.sessionId),
                sourceVersions, proceduralSources });
            }
            const sources = proceduralSources ?? await selectProceduralObservations(kv, project, summaries, selected);
            results.procedural = sources.length >= 2
              ? await startManifest("procedural", sources, Boolean(data?.deferred), undefined, undefined, undefined,
                explicitSummarySources ? consolidationRetrievalCoverage(sources) : retrievalCoverage, invocationPolicy, data?.selected)
              : { completed: true, skipped: true, newProcedures: 0, reason: "fewer than 2 recurring patterns", sourceCount: sources.length };
            const stage = results.procedural as Record<string, unknown>;
            if (stage.completed !== true || stageFailed(stage)) return { ...stageFailureDetails(stage), success: !stageFailed(stage), completed: false, queued: stage.queued === true && !stageFailed(stage), partial: stageFailed(stage), project,
              results, continuation: { tier, project, force: data?.force, deferred: data?.deferred, batchSourceIds: data?.batchSourceIds, consolidationSelectionId: selectionId, consolidationCatalogProject: catalogSelected } };
          }
          const continuation = { tier, project, force: data?.force, deferred: data?.deferred, batchSourceIds: data?.batchSourceIds,
            consolidationSelectionId: selectionId, consolidationCatalogProject: catalogSelected };
          if (tier === "all" || tier === "reflect") {
            selectionPhase = "reflection";
            const foreground = data?.force === true && data.deferred !== true && !catalogSelected;
            if (foreground) {
              const sourceFingerprint = await fingerprintConsolidationSummaries(kv, project, summaries);
              const request: ReflectRequest = { manual: true, project, sessionId: summaries[0]!.sessionId, summaryId: summaries[0]!.sessionId,
                requestId: fingerprintId("conreflect", JSON.stringify([selectionId, sourceFingerprint, invocationPolicy])) };
              const key = pageKey(request.requestId!, "normal-reflection", 0);
              results.reflect = await withKeyedLock(`normal-reflection:${key}`, async () => {
                type Receipt = { request: ReflectRequest; response?: Record<string, unknown> };
                let receipt = await kv.get<Receipt>(KV.state, key);
                if (!receipt) { receipt = { request }; await kv.set(KV.state, key, receipt); }
                if (JSON.stringify(receipt.request) !== JSON.stringify(request)) throw new Error("consolidation_reflect_identity_mismatch");
                const previous = receipt.response;
                if (previous && (previous.completed === true || previous.terminal === true || stageFailed(previous))) return previous;
                if (previous && !validManualReflectContinuation(previous.continuation)) {
                  return { ...previous, success: false, completed: false, error: "consolidation_reflect_continuation_missing" };
                }
                const response: unknown = await sdk.trigger({ function_id: "mem::reflect", payload: {
                  ...receipt.request, ...(previous ? { continuation: previous.continuation } : {}) } });
                if (!response || typeof response !== "object" || Array.isArray(response)) throw new Error("consolidation_reflect_response_invalid");
                receipt.response = response as Record<string, unknown>;
                await kv.set(KV.state, key, receipt);
                return receipt.response;
              });
            } else results.reflect = await sdk.trigger({ function_id: "mem::reflect", payload: {
              project, sessionId: summaries[0]!.sessionId, summaryId: summaries[0]!.sessionId,
              requestId: fingerprintId("conreflect", JSON.stringify([project, summaries.map(summary => summary.sessionId)])),
            } });
            const stage = results.reflect as Record<string, unknown>;
            if (stage.completed !== true || stageFailed(stage)) return { ...stageFailureDetails(stage), success: !stageFailed(stage),
              completed: false, queued: !stageFailed(stage) && stage.queued === true, partial: stageFailed(stage), project,
              results, ...(!stageFailed(stage) && stage.terminal !== true ? { continuation } : {}) };
          }
          if (tier === "all") {
            selectionPhase = "decay";
            results.decay = await runSelectedConsolidationDecay(kv, selectDecay, getConsolidationDecayDays(), {
              project, sessionIds: summaries.map(summary => summary.sessionId), sourceVersion: JSON.stringify(summaries.map(summary => summary.sessionId)),
            });
          }
          if (tier === "decay") results.decay = await runSelectedConsolidationDecay(kv, selectDecay, getConsolidationDecayDays(), {
            project, sessionIds: summaries.map(summary => summary.sessionId), sourceVersion: JSON.stringify(summaries.map(summary => summary.sessionId)),
          });
          const stages = Object.values(results) as Record<string, unknown>[];
          const partial = stages.some(stageFailed);
          const incomplete = stages.some(stage => stage.completed !== true);
          const queued = !partial && stages.some(stage => stage.queued === true);
          const failure = stages.find(stage => stage.retained === true)
            ?? stages.find(stage => typeof stage.errorCode === "string" || typeof stage.error === "string") ?? stages.find(stageFailed);
          if (catalogSelected && !partial && !incomplete) {
            const progress = await kv.get<ProjectCursor>(KV.config, projectCursorKey);
            if (progress?.project === project) await kv.set(KV.config, projectCursorKey,
              { ...(progress.hasMore ? { cursor: progress.nextCursor } : {}) });
          }
          return { ...stageFailureDetails(failure), success: !partial, partial, queued, completed: !partial && !incomplete,
            status: partial ? "partial" : !incomplete ? "completed" : queued ? "queued" : "awaiting_dependency", project,
            selectedSourceIds: summaries.map(summary => summary.sessionId), sourceCount: summaries.length,
            selectionCoverage: "selected_relevant_sources", retrievalCoverage, results,
            ...(partial || incomplete ? { continuation } : {}) };
        } catch (error) {
          const message = error instanceof Error ? error.message : "";
          logger.warn("Consolidation selection failed", { phase: selectionPhase, tier,
            retrievalAction: consolidationRetrievalFailureAction(error),
            ...(error instanceof ScopedObservationBudgetError ? { retrievalBudget: error.diagnostics } : {}),
            errorClass: error instanceof Error ? error.name : undefined,
            code: message.match(/\bSTATE_[A-Z0-9_]+\b/)?.[0],
            category: /^[a-z][a-z0-9_]{0,159}$/.test(message) ? message : undefined });
          if (sourceRetrievalLimited(error)) return { success: false, completed: false, partial: true, queued: false,
            errorCode: "STATE_RETRIEVAL_RESOURCE_LIMIT", reason: "source_retrieval_resource_limit", retryable: true,
            diagnostics: { ...(error instanceof ScopedObservationBudgetError ? error.diagnostics : {}), retrievalAction: consolidationRetrievalFailureAction(error), project: selectedProject, tier }, continuation: { tier, project: selectedProject, force: data?.force,
              deferred: data?.deferred, batchSourceIds: data?.batchSourceIds, consolidationSelectionId: data?.consolidationSelectionId, consolidationCatalogProject: catalogSelected } };
          if (consolidationIsPending(error)) return { success: true, queued: false, completed: false, status: "awaiting_dependency", reason: "source_index_pending",
            ...(error instanceof SelectedObservationPreparationPending ? { diagnostics: error.diagnostics } : {}),
            continuation: { ...data, tier, project: selectedProject, force: data?.force, deferred: data?.deferred, batchSourceIds: data?.batchSourceIds,
              consolidationSelectionId: activeSelectionId, consolidationCatalogProject: catalogSelected } };
          return { success: false, partial: true, completed: false, error: error instanceof Error ? error.message : "consolidation_selection_failed" };
        }
      }
      return runBatchCallback(kv, "consolidation", data?.batchEffectKey, async (resuming, admit, receipt) => {
      const legacyProject = data?.project ?? (data?.batchSourceIds?.[0]
        ? (await kv.get<SessionSummary>(KV.summaries, data.batchSourceIds[0]))?.project : undefined);
      if (!legacyProject) return { success: false, completed: false, retained: true, reason: "legacy_project_identity_required" };
      if (!data?.force && !isConsolidationEnabled()) {
        return { success: false, skipped: true, reason: "Consolidation disabled: set CONSOLIDATION_ENABLED=true or configure an LLM provider (ANTHROPIC_API_KEY / OPENAI_API_KEY / OPENROUTER_API_KEY / GEMINI_API_KEY / GOOGLE_API_KEY / MINIMAX_API_KEY / OPENAI_BASE_URL / AGENTMEMORY_PROVIDER=agent-sdk)" };
      }
      const requestedTier = data?.tier || "all";
      const callbackStage = requestedTier === "all" && data?.batchResponse !== undefined
        ? legacyConsolidationCallbackStage(data.batchSourceFingerprint, data.batchResponse)
        : undefined;
      if (requestedTier === "all" && data?.batchResponse !== undefined && !callbackStage) {
        return { success: false, completed: false, retained: true, reason: data.batchSourceFingerprint
          ? "legacy_consolidation_stage_identity_invalid" : "legacy_consolidation_stage_identity_required" };
      }
      const tier = requestedTier;
      const decayDays = getConsolidationDecayDays();
      const callbackResultHash = data?.batchResponse !== undefined
        ? legacyConsolidationCallbackResultHash(callbackStage ?? tier, legacyProject, data.batchSourceIds, data.batchSourceFingerprint,
          data.batchResponse, decayDays)
        : undefined;
      if (callbackResultHash && receipt?.resultHash && receipt.resultHash !== callbackResultHash) {
        return { success: false, completed: false, retained: true, reason: "legacy_consolidation_callback_identity_conflict" };
      }
      const legacyPlanKey = data.batchEffectKey && callbackResultHash ? fingerprintId("conlegacy-plan", JSON.stringify([data.batchEffectKey, callbackResultHash])) : undefined;
      const legacyPlan = legacyPlanKey ? await kv.get<LegacyPublicationPlan>(KV.state, legacyPlanKey) : null;
      let recoveredLegacyStage: Record<string, unknown> | undefined;
      if (legacyPlan) {
        boundedBytes(legacyPlan);
        if (legacyPlan.project !== legacyProject || legacyPlan.resultHash !== callbackResultHash || !["semantic", "procedural"].includes(legacyPlan.stage)
          || !legacyPlan.stageResult || !Array.isArray(legacyPlan.operations)
          || !legacyPlan.operations.length || legacyPlan.operations.some(operation => operation.effect !== data.batchEffectKey
            || !["semantic", "procedural"].includes(operation.scope) || !operation.key || !operation.id || !operation.initial)) {
          throw new Error("consolidation_legacy_publication_plan_invalid");
        }
        let recovered = true;
        for (const operation of legacyPlan.operations) if (!await recoverCanonicalOperation(operation)) recovered = false;
        if (recovered) {
          recoveredLegacyStage = { ...legacyPlan.stageResult, appliedEffects: 0, replayedEffects: legacyPlan.operations.length };
          if (requestedTier !== "all") {
            const results = { [legacyPlan.stage]: recoveredLegacyStage };
            await kv.set(KV.config, CONSOLIDATION_LAST_RUN_KEY, { at: legacyPlan.effectTimestamp, tier, results, status: "completed" });
            return { success: true, partial: false, queued: false, completed: true, results,
              canonicalAckRecovered: true, appliedEffects: 0, replayedEffects: legacyPlan.operations.length };
          }
        }
      }
      const effectTimestamp = legacyPlan?.effectTimestamp ?? receipt?.effectTimestamp ?? new Date().toISOString();
      const results: Record<string, unknown> = recoveredLegacyStage && legacyPlan ? { [legacyPlan.stage]: recoveredLegacyStage } : {};

      if ((tier === "all" || tier === "semantic") && callbackStage !== "procedural" && !recoveredLegacyStage) {
        const summaries = kv.indexedRetrieval
          ? await hydrateLegacySummaries(kv, legacyProject, await selectConsolidationSummaries(kv, legacyProject, receipt?.semanticSourceIds ?? data?.batchSourceIds, selectSources))
          : await rejectUnboundedConsolidationSources<SessionSummary>();
        const existingSemantic = [] as SemanticMemory[];

        if (summaries.length >= 5 || data?.batchSourceIds?.length || (resuming && data?.batchResponse)) {
          const checkpoint = await kv.get<SemanticCheckpoint>(
            KV.state,
            SEMANTIC_CHECKPOINT_KEY,
          );
          let semanticInput = receipt?.semanticSourceIds ? {
            summaries: summaries.filter((summary) => receipt.semanticSourceIds!.includes(summary.sessionId)),
            checkpoint: receipt.semanticCheckpoint,
          } : selectSemanticInput(
            summaries,
            resuming ? null : checkpoint,
            getConsolidationMinNewSummaries(),
          );
          if (!resuming && (data?.deferred || data?.batchSourceIds)) {
            const candidates = data.batchSourceIds
              ? data.batchSourceIds.map((id) => summaries.find((summary) => summary.sessionId === id)).filter((summary): summary is SessionSummary => Boolean(summary))
              : summaries.filter((summary) => data?.replacementOf || !checkpoint || isAfterCheckpoint(summary, checkpoint)).sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.sessionId.localeCompare(b.sessionId));
            if (data.batchSourceIds && candidates.length !== data.batchSourceIds.length) return { success: true, stale: true };
            const selected = candidates;
            const last = selected[selected.length - 1];
            semanticInput = { summaries: selected, checkpoint: last ? {
              processedThrough: last.createdAt,
              processedSessionIdsAtThrough: [...new Set([
                ...(checkpoint?.processedThrough === last.createdAt ? checkpoint.processedSessionIdsAtThrough : []),
                ...selected.filter((summary) => summary.createdAt === last.createdAt).map((summary) => summary.sessionId),
              ])],
            } : undefined };
          }
          const sourceSummaries = semanticInput.summaries;
          const recentSummaries = sourceSummaries;
          const compactedSummaryCount = 0;
          if (recentSummaries.length === 0 && !resuming) {
            results.semantic = {
              skipped: true,
              reason: `fewer than ${getConsolidationMinNewSummaries()} new summaries`,
            };
          } else {

            const sourceFingerprint = fingerprintId("fwbconsem", JSON.stringify(
              recentSummaries.map((summary) => [
                summary.sessionId,
                summary.title,
                summary.narrative,
                summary.concepts,
                summary.createdAt,
              ]),
            ));

            const inferredSemanticIdentity = requestedTier === "all" && callbackStage === "semantic"
              && data?.batchResponse !== undefined && data.batchSourceFingerprint === undefined;
            if (!resuming && data?.batchResponse && data.batchSourceFingerprint !== sourceFingerprint && !inferredSemanticIdentity) {
              if (requestedTier === "all") {
                return { success: false, completed: false, retained: true, reason: "legacy_consolidation_stage_identity_mismatch" };
              }
              return { success: true, stale: true };
            }

            try {
              if (!data?.batchResponse) {
                if (data?.replacementOf && data.batchCohort && batchQueue) {
                  const userPrompt = buildSemanticMergePrompt(recentSummaries);
                  if (requestInputBytes(SEMANTIC_MERGE_SYSTEM, userPrompt) <= MAX_SEMANTIC_REQUEST_INPUT_BYTES) {
                    const replacementKey = fingerprintId("conreplacement", JSON.stringify([data.batchCohort, data.replacementOf, sourceFingerprint]));
                    let replacement = await kv.get<{ workItemId: string }>(KV.state, replacementKey);
                    if (!replacement) {
                      const queued = await batchQueue.enqueue({ task: "consolidation", correlationId: replacementKey,
                        systemPrompt: SEMANTIC_MERGE_SYSTEM, userPrompt, replacementOf: data.replacementOf,
                        metadata: { tier: "semantic", sourceFingerprint, sourceIds: JSON.stringify(recentSummaries.map((source) => source.sessionId)), cohort: data.batchCohort } });
                      if (!queued.queued || !queued.workItemId) throw new Error(queued.reason ?? "Consolidation replacement was rejected");
                      replacement = { workItemId: queued.workItemId };
                      await kv.set(KV.state, replacementKey, replacement);
                    }
                    await withKeyedLock(`semantic-cohort:${data.batchCohort}`, async () => {
                      const cohort = await kv.get<{ workItemIds: string[]; checkpoint?: SemanticCheckpoint }>(KV.state, data.batchCohort!);
                      if (!cohort) throw new Error("Original consolidation cohort missing; watermark retained locally");
                      await kv.set(KV.state, data.batchCohort!, { ...cohort, workItemIds: [...new Set(cohort.workItemIds.map((id) => id === data.replacementOf ? replacement!.workItemId : id))] });
                      await reconcileSemanticCohort(kv, data.batchCohort!);
                    });
                    results.semantic = { queued: true, completed: false, workItemId: replacement.workItemId };
                  } else {
                    results.semantic = await startManifest("semantic", recentSummaries, Boolean(data?.deferred), undefined, data.batchCohort, data.replacementOf);
                  }
                } else {
                results.semantic = await startManifest("semantic", recentSummaries, Boolean(data?.deferred), semanticInput.checkpoint);
                }
              } else {
                const sourceSessionIds = recentSummaries.map((summary) => summary.sessionId);
                const factCandidates = parseSemanticFacts(data.batchResponse, sourceSessionIds, true);
                const publication = factCandidates.length ? await legacyEvidence("semantic", legacyProject,
                  callbackResultHash ?? fingerprintId("conlegacy", data.batchResponse), JSON.stringify(factCandidates), recentSummaries) : undefined;
                if (publication && publication.evidence.status !== "supported") return {
                  success: publication.evidence.status === "unsupported", completed: publication.evidence.status === "unsupported",
                  retained: publication.evidence.status !== "unsupported", partial: publication.evidence.status !== "unsupported",
                  reason: publication.evidence.status === "unsupported" ? "no_supported_candidates" : `consolidation_publication_${publication.evidence.status}`,
                  evidence: publication.evidence, results: { semantic: { newFacts: 0, rejected: factCandidates.length } } };
                if (legacyPlanKey && callbackResultHash && factCandidates.length) await immutablePage(kv, legacyPlanKey, {
                  project: legacyProject, resultHash: callbackResultHash, effectTimestamp, stage: "semantic",
                  stageResult: { newFacts: 0, totalSummaries: summaries.length, compactedSummaries: compactedSummaryCount },
                  operations: factCandidates.map(candidate => {
                    const id = fingerprintId("sem", candidate.fact.toLowerCase());
                    return { scope: "semantic", key: candidate.fact.toLowerCase(), id, effect: data.batchEffectKey,
                      initial: { id, fact: candidate.fact, confidence: candidate.confidence, sourceSessionIds: candidate.sourceSessionIds,
                        sourceMemoryIds: [], accessCount: 1, lastAccessedAt: effectTimestamp, strength: candidate.confidence,
                        createdAt: effectTimestamp, updatedAt: effectTimestamp } };
                  }) });
                const emptyLegacyAllResponse = requestedTier === "all" && callbackStage === "semantic" && factCandidates.length === 0;
                if (!emptyLegacyAllResponse) {
                  await admit({ semanticSourceIds: recentSummaries.map((summary) => summary.sessionId), semanticCheckpoint: semanticInput.checkpoint,
                    effectTimestamp, ...(callbackResultHash ? { resultHash: callbackResultHash } : {}) });
                }
                let newFacts = 0;
                const now = effectTimestamp;

                for (const candidate of factCandidates) {
                  const { fact, confidence, sourceSessionIds: factSourceIds } = candidate;

                  const canonicalMatch = kv.indexedRetrieval ? await canonicalConsolidationId(lookupCanonical, "semantic", fact) : null;
                  const existing = canonicalMatch ? undefined : existingSemantic.find(
                    (s) => s.fact.toLowerCase() === fact.toLowerCase(),
                  );
                  await publication?.verify();
                  if (data?.batchEffectKey) {
                    const id = canonicalMatch ? fingerprintId("sem", fact.toLowerCase()) : existing?.id ?? fingerprintId("sem", `${data.batchEffectKey}:${fact.toLowerCase()}`);
                    const update = (current: SemanticMemory | null): SemanticMemory => current ? {
                      ...current, accessCount: current.accessCount + 1, lastAccessedAt: now, updatedAt: now,
                      confidence: Math.max(current.confidence, confidence),
                      sourceSessionIds: [...new Set([...(current.sourceSessionIds ?? []), ...factSourceIds])],
                    } : {
                      id, fact, confidence, sourceSessionIds: factSourceIds, sourceMemoryIds: [],
                      accessCount: 1, lastAccessedAt: now, strength: confidence, createdAt: now, updatedAt: now,
                    };
                    if (canonicalMatch && !writeCanonical) throw new Error("consolidation_canonical_commit_unavailable");
                    if (canonicalMatch) {
                      const result = await writeCanonical!("semantic", fact.toLowerCase(), canonicalMatch, id, data.batchEffectKey, update);
                      if (result.applied && result.created) newFacts++;
                    }
                    else await applyBatchEffect(kv, KV.semantic, id, data.batchEffectKey, update);
                    continue;
                  }
                  if (existing) {
                    existing.accessCount++;
                    existing.lastAccessedAt = now;
                    existing.updatedAt = now;
                    existing.confidence = Math.max(existing.confidence, confidence);
                    existing.sourceSessionIds = [...new Set([...existing.sourceSessionIds, ...factSourceIds])];
                    await kv.set(KV.semantic, existing.id, existing);
                  } else {
                    const sem: SemanticMemory = {
                      id: generateId("sem"),
                      fact,
                      confidence,
                      sourceSessionIds: factSourceIds,
                      sourceMemoryIds: [],
                      accessCount: 1,
                      lastAccessedAt: now,
                      strength: confidence,
                      createdAt: now,
                      updatedAt: now,
                    };
                    await kv.set(KV.semantic, sem.id, sem);
                    existingSemantic.push(sem);
                    newFacts++;
                  }
                }
                results.semantic = { newFacts, totalSummaries: summaries.length, compactedSummaries: compactedSummaryCount };
                if (data?.batchCohort) {
                  await withKeyedLock(`semantic-cohort:${data.batchCohort}`, () => reconcileSemanticCohort(kv, data.batchCohort!, data.batchEffectKey));
                } else if (!emptyLegacyAllResponse && semanticInput.checkpoint && (!checkpoint || semanticInput.checkpoint.processedThrough >= checkpoint.processedThrough)) {
                  await advanceSemanticCheckpoint(kv, semanticInput.checkpoint);
                }
              }
            } catch (err) {
              if (data?.batchEffectKey) throw err;
              const msg = err instanceof Error ? err.message : String(err);
              logger.error("Semantic consolidation failed", { error: msg });
              results.semantic = { error: msg };
            }
          }
        } else {
          results.semantic = {
            skipped: true,
            reason: "fewer than 5 summaries",
          };
        }
      }

      if ((tier === "all" || tier === "reflect") && callbackStage !== "procedural") {
        try {
          const reflectionSources = kv.indexedRetrieval
            ? await hydrateLegacySummaries(kv, legacyProject, await selectConsolidationSummaries(kv, legacyProject, data?.batchSourceIds, selectSources))
            : await rejectUnboundedConsolidationSources<SessionSummary>();
          const latest = [...reflectionSources].filter((summary) => !data?.project || summary.project === data.project)
            .sort((left, right) => right.createdAt.localeCompare(left.createdAt) || left.sessionId.localeCompare(right.sessionId))[0];
          const reflectResult = await sdk.trigger({
            function_id: "mem::reflect", payload: {
              project: data?.project,
              sessionId: latest?.sessionId,
              summaryId: latest?.sessionId,
              requestId: fingerprintId("conreflect", JSON.stringify([data?.batchEffectKey ?? null, data?.project ?? null, latest ?? null])),
            }
          });
          if (reflectResult && typeof reflectResult === "object" && "success" in reflectResult && reflectResult.success === false) {
            throw new Error("Reflection tier reported failure");
          }
          results.reflect = reflectResult && typeof reflectResult === "object" ? {
            ...reflectResult,
            accepted: true,
            queued: !("completed" in reflectResult && reflectResult.completed === true),
            partial: "status" in reflectResult && reflectResult.status === "partial",
          } : { error: "Reflection acceptance response was invalid", completed: false };
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Reflect tier failed", { error: msg });
          results.reflect = { error: msg, completed: false };
        }
      }

      if (tier === "all" && results.reflect && stageFailed(results.reflect as Record<string, unknown>)) {
        return { success: false, partial: true, completed: false, results, ...(recoveredLegacyStage ? { canonicalAckRecovered: true } : {}) };
      }

      if ((tier === "all" || tier === "procedural") && callbackStage !== "semantic" && !recoveredLegacyStage) {
        if (kv.indexedRetrieval && (!data?.batchSourceIds?.length || data.batchSourceIds.length > 12)) throw new Error("consolidation_exact_pattern_sources_required");
        const memories = [] as Memory[];
        if (kv.indexedRetrieval) {
          let bytes = 0;
          for (const id of data!.batchSourceIds!) {
            const memory = await kv.get<Memory>(KV.memories, id);
            if (!memory) throw new Error("consolidation_pattern_source_missing");
            bytes += Buffer.byteLength(JSON.stringify(memory));
            if (bytes > 65_536) throw new Error("consolidation_source_partition_required");
            memories.push(memory);
          }
        }
        if (kv.indexedRetrieval && memories.some(memory => memory.project !== data?.project)) throw new Error("consolidation_source_scope_mismatch");
        let patternSources = memories
          .filter((m) => m.isLatest && m.type === "pattern")
          .filter((m) => !data?.batchSourceIds || data.batchSourceIds.includes(m.id))
          .map((m) => ({
            id: m.id,
            content: m.content,
            frequency: m.sessionIds.length || 1,
            updatedAt: m.updatedAt,
            version: m.version,
          }))
          .filter((p) => p.content.trim().length > 0);
        if (data?.batchSourceIds && patternSources.length !== data.batchSourceIds.length) return { success: true, stale: true };
        const patterns = patternSources.map(({ content, frequency }) => ({ content, frequency }));

        if (patterns.length >= 1 || data?.batchSourceIds?.length || (resuming && data?.batchResponse)) {
          const sourceFingerprint = fingerprintId("fwbconproc", JSON.stringify(patterns));

          const inferredProceduralIdentity = requestedTier === "all" && callbackStage === "procedural"
            && data?.batchResponse !== undefined && data.batchSourceFingerprint === undefined;
          if (!resuming && data?.batchResponse && data.batchSourceFingerprint !== sourceFingerprint && !inferredProceduralIdentity) {
            if (requestedTier === "all") {
              return { success: false, completed: false, retained: true, reason: "legacy_consolidation_stage_identity_mismatch" };
            }
            return { success: true, stale: true };
          }

          try {
            if (!data?.batchResponse) {
              results.procedural = await startManifest("procedural", patternSources, Boolean(data?.deferred));
            } else {
              const response = data.batchResponse;
              if (kv.indexedRetrieval) validateProceduralResponse(response);

              const procRegex =
                /<procedure\s+name="([^"]+)"\s+trigger="([^"]+)">([\s\S]*?)<\/procedure>/g;
              const publication = /<procedure\b/.test(response) ? await legacyEvidence("procedural", legacyProject,
                callbackResultHash ?? fingerprintId("conlegacy", response), response,
                memories.filter(memory => patternSources.some(source => source.id === memory.id))) : undefined;
              if (publication && publication.evidence.status !== "supported") return {
                success: publication.evidence.status === "unsupported", completed: publication.evidence.status === "unsupported",
                retained: publication.evidence.status !== "unsupported", partial: publication.evidence.status !== "unsupported",
                reason: publication.evidence.status === "unsupported" ? "no_supported_candidates" : `consolidation_publication_${publication.evidence.status}`,
                evidence: publication.evidence, results: { procedural: { newProcedures: 0 } } };
              if (legacyPlanKey && callbackResultHash && publication) await immutablePage(kv, legacyPlanKey, {
                project: legacyProject, resultHash: callbackResultHash, effectTimestamp, stage: "procedural",
                stageResult: { newProcedures: 0, patternsAnalyzed: patterns.length, compactedPatterns: 0 },
                operations: [...response.matchAll(procRegex)].map(match => {
                  const id = fingerprintId("proc", match[1].toLowerCase());
                  return { scope: "procedural", key: match[1].toLowerCase(), id, effect: data.batchEffectKey,
                    initial: { id, name: match[1], steps: [...match[3].matchAll(/<step>([^<]+)<\/step>/g)].map(step => step[1].trim()),
                      triggerCondition: match[2], frequency: 1, sourceSessionIds: [], strength: 0.5, createdAt: effectTimestamp, updatedAt: effectTimestamp } };
                }) });
              await admit({ effectTimestamp, ...(callbackResultHash ? { resultHash: callbackResultHash } : {}) });
              let match;
              let newProcs = 0;
              const now = effectTimestamp;
              const existingProcs = [] as ProceduralMemory[];

              while ((match = procRegex.exec(response)) !== null) {
                const name = match[1];
                const trigger = match[2];
                const stepsBlock = match[3];
                const steps: string[] = [];

                const stepRegex = /<step>([^<]+)<\/step>/g;
                let stepMatch;
                while ((stepMatch = stepRegex.exec(stepsBlock)) !== null) {
                  steps.push(stepMatch[1].trim());
                }

                const canonicalMatch = kv.indexedRetrieval ? await canonicalConsolidationId(lookupCanonical, "procedural", name) : null;
                const existing = canonicalMatch ? undefined : existingProcs.find(
                  (p) => p.name.toLowerCase() === name.toLowerCase(),
                );
                await publication?.verify();
                if (data?.batchEffectKey) {
                  const id = canonicalMatch ? fingerprintId("proc", name.toLowerCase()) : existing?.id ?? fingerprintId("proc", `${data.batchEffectKey}:${name.toLowerCase()}`);
                  const update = (current: ProceduralMemory | null): ProceduralMemory => current ? {
                    ...current, frequency: current.frequency + 1, updatedAt: now, strength: Math.min(1, current.strength + 0.1),
                  } : { id, name, steps, triggerCondition: trigger, frequency: 1, sourceSessionIds: [], strength: 0.5, createdAt: now, updatedAt: now };
                  if (canonicalMatch && !writeCanonical) throw new Error("consolidation_canonical_commit_unavailable");
                  if (canonicalMatch) {
                    const result = await writeCanonical!("procedural", name.toLowerCase(), canonicalMatch, id, data.batchEffectKey, update);
                    if (result.applied && result.created) newProcs++;
                  }
                  else await applyBatchEffect(kv, KV.procedural, id, data.batchEffectKey, update);
                  continue;
                }
                if (existing) {
                  existing.frequency++;
                  existing.updatedAt = now;
                  existing.strength = Math.min(1, existing.strength + 0.1);
                  await kv.set(KV.procedural, existing.id, existing);
                } else {
                  const proc: ProceduralMemory = {
                    id: generateId("proc"),
                    name,
                    steps,
                    triggerCondition: trigger,
                    frequency: 1,
                    sourceSessionIds: [],
                    strength: 0.5,
                    createdAt: now,
                    updatedAt: now,
                  };
                  await kv.set(KV.procedural, proc.id, proc);
                  newProcs++;
                }
              }
              results.procedural = {
                newProcedures: newProcs,
                patternsAnalyzed: patterns.length,
                compactedPatterns: 0,
              };
            }
          } catch (err) {
            if (data?.batchEffectKey) throw err;
            const msg = err instanceof Error ? err.message : String(err);
            logger.error("Procedural extraction failed", { error: msg });
            results.procedural = { error: msg };
          }
        } else {
          results.procedural = {
            skipped: true,
            reason: "no nonempty pattern evidence",
          };
        }
      }

      if ((tier === "all" || tier === "decay") && callbackStage !== "procedural") {
        if (kv.indexedRetrieval) {
          const summaries = await hydrateLegacySummaries(kv, legacyProject, await selectConsolidationSummaries(kv, legacyProject, receipt?.semanticSourceIds ?? data?.batchSourceIds, selectSources));
          const effectTimestamp = receipt?.effectTimestamp ?? new Date().toISOString();
          await admit({ ...(callbackResultHash ? { resultHash: callbackResultHash } : {}), effectTimestamp });
          const initialDecay = await runSelectedConsolidationDecay(kv, selectDecay, decayDays, { project: data?.project ?? "",
            sessionIds: summaries.map(summary => summary.sessionId), sourceVersion: JSON.stringify(summaries.map(summary => summary.createdAt)), effectTimestamp });
          results.decay = initialDecay.queued === true && typeof initialDecay.manifestId === "string"
            ? await runSelectedConsolidationDecay(kv, selectDecay, decayDays, { manifestId: initialDecay.manifestId })
            : initialDecay;
        } else {
          throw new Error("consolidation_indexed_retrieval_required");
        }
      }

      if (process.env["OBSIDIAN_AUTO_EXPORT"] === "true") {
        try {
          await sdk.trigger({ function_id: "mem::obsidian-export", payload: { batchEffectKey: data?.batchEffectKey } });
          results.obsidianExport = { success: true };
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Obsidian auto-export failed", { error: msg });
          results.obsidianExport = { success: false, error: msg };
        }
      }

      const audit = recordAudit(kv, "consolidate", "mem::consolidate-pipeline", [], {
        tier,
        results,
      }, undefined, undefined, data?.batchEffectKey);
      if (data?.batchEffectKey) await audit.catch(() => { });
      else await audit;

      const stages = Object.values(results).filter((result): result is Record<string, unknown> => Boolean(result) && typeof result === "object");
      const partial = stages.some((result) => Boolean(result.error) || result.partial === true);
      const queued = stages.some((result) => result.queued === true);
      await kv.set(KV.config, CONSOLIDATION_LAST_RUN_KEY, {
        at: new Date().toISOString(), tier, results, status: partial ? "partial" : queued ? "queued" : "completed",
      });

      logger.info("Consolidation pipeline complete", { tier, results });
      return { success: !partial, partial, queued, completed: !partial && !queued, results,
        ...(recoveredLegacyStage ? { canonicalAckRecovered: true } : {}),
        ...(queued ? { continuation: stages.find(result => result.queued === true)?.continuation } : {}) };
    }, () => recordAudit(kv, "consolidate", "mem::consolidate-pipeline", [], {}, undefined, undefined, data?.batchEffectKey));
    },
  );
}

async function rejectUnboundedConsolidationSources<T>(): Promise<T[]> {
  throw new Error("consolidation_project_selector_required");
}

async function hydrateLegacySummaries(kv: StateKV, project: string, refs: ConsolidationSummaryReference[]): Promise<SessionSummary[]> {
  const result: SessionSummary[] = []; let bytes = 0;
  for (const ref of refs) {
    const summary = await kv.get<SessionSummary>(KV.summaries, ref.sessionId);
    if (!summary || summary.sessionId !== ref.sessionId || summary.project !== project) throw new Error("consolidation_legacy_source_scope_mismatch");
    bytes += Buffer.byteLength(JSON.stringify(summary));
    if (bytes > 65_536) throw new Error("consolidation_legacy_identity_capture_required_original_retained");
    result.push(summary);
  }
  return result;
}
