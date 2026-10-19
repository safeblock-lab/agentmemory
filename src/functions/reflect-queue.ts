import { observeWorkflowTerminal, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { fingerprintId, generateId } from "../state/schema.js";
import { KV } from "../state/schema.js";
import { batchEffectKey, runBatchCallback } from "../state/batch-effects.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { isBackgroundRecoveryPaused, getAgentId } from "../config.js";
import { StatePageError } from "../state/state-pages.js";
import { IndexedRetrievalError } from "../state/indexed-retrieval.js";
import { retrievalRequest } from "../state/indexed-retrieval.js";
import type { MemoryProvider, EmbeddingProvider } from "../types.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import { REFLECT_SYSTEM } from "../prompts/reflect.js";
import { ReflectWaiting, ReflectNoEvidence, freezeReflectSeeds, retrieveReflectManifest, currentReflectManifest, prepareReflectGroup } from "./reflect-retrieval.js";
import { evaluatePublicationEvidence, observePublicationEvidence, preparePublicationEvidence, supersedePublicationJudge, type PublicationEvidenceInput, type PublicationEvidenceResult } from "./publication-evidence.js";
import { withProviderAttemptBudget } from "../providers/provider-attempt-budget.js";
import { publicationExecutor } from "./publication-call-options.js";
import { recordAudit } from "./audit.js";
import { stripPrivateData } from "./privacy.js";
import { constructReflectRecovery, reflectRecoveryReady, validateReflectRecoveryReplay, ReflectRecoveryError, type ReflectManualOperation } from "./reflect-zero-spend-recovery.js";
import { REFLECT_JOBS, REFLECT_PENDING, REFLECT_CONTROL, REFLECT_TOPIC, validReflectText as validText, validReflectForegroundControl, validReflectPublicationReplay, validReflectJudgeDisposition, validateReflectRequest, countReflectResult, validManualReflectContinuation, type ReflectJudgeDisposition, type ManualReflectContinuation, type ReflectForegroundControl, type ReflectJob as StoredReflectJob, type ReflectRequest, type ReflectPending, type ConceptCluster, type ReflectManifest, type ReflectEmptyEvidence } from "./reflect-queue-types.js";
export type SelectedReflectControl = { sessionId: string };
type ReflectJob = StoredReflectJob;

type Apply = (response: string, cluster: ConceptCluster, project: string | undefined, limit: number, effect: string) => Promise<{ count: number; insightIds: string[] }>;
const terminal = (job: ReflectJob): boolean => ["completed", "partial", "failed", "blocked"].includes(job.status);
const manualTerminal = (job: ReflectJob): boolean => terminal(job) && job.status !== "blocked";
const jobIdentity = (request: ReflectRequest, origin?: "manual" | "selected") => fingerprintId("reflectjob",
  request.legacyWorkId ? `legacy:${request.legacyWorkId}` : `${origin === "manual" ? "manual:" : ""}${JSON.stringify(request)}`);
const scopedCoverage = (job: ReflectJob) => ({
  scope: job.legacy ? "legacy-sources" : "bounded-indexed-selection",
  requestedFamilies: job.legacy ? [...new Set(job.legacy.refs.map(ref => ref.kind))] : ["memory", "observation",
    ...(job.request.factIds !== undefined ? ["fact"] : []),
    ...(job.request.lessonIds !== undefined ? ["lesson"] : []),
    ...(job.request.crystalIds !== undefined ? ["crystal"] : [])],
  excludedFamilies: job.legacy ? [] : [
    ...(job.request.factIds === undefined ? ["fact"] : []),
    ...(job.request.lessonIds === undefined ? ["lesson"] : []),
    ...(job.request.crystalIds === undefined ? ["crystal"] : [])],
  complete: job.status === "completed", globalFamiliesComplete: false,
});
export function registerReflectQueue(sdk: IIIClient, kv: StateKV, provider: MemoryProvider, apply: Apply,
  llmRouter?: LlmTaskRouter, batchQueue?: FireworksBatchQueue, embedding?: EmbeddingProvider) {
  const manualExecutions = new Map<string, string>();
  const readJob = async (id: string): Promise<ReflectJob | null> => {
    const job = await kv.get<ReflectJob>(REFLECT_JOBS, id);
    if (job && ["completed", "partial", "failed"].includes(job.status)) {
      if (!job.workflowTerminal || job.workflowTerminal.outcome !== job.status) await markWorkflowStatisticsGap(kv, "reflection");
      else await observeWorkflowTerminal(kv, { ...job.workflowTerminal, workflow: "reflection", producer: "reflect-queue", executionLevel: "workflow" });
    }
    return job;
  };
  const save = async (job: ReflectJob) => {
    job.updatedAt = new Date().toISOString();
    if (["completed", "partial", "failed"].includes(job.status) && !job.workflowTerminal) {
      const outcome = job.status === "completed" ? "completed" : job.status === "partial" ? "partial" : "failed";
      job.workflowTerminal = { executionId: job.id, agentId: getAgentId(), occurredAt: job.updatedAt, outcome };
    }
    await kv.set(REFLECT_JOBS, job.id, job);
    if (job.workflowTerminal) {
      if (job.status !== job.workflowTerminal.outcome) await markWorkflowStatisticsGap(kv, "reflection");
      else await observeWorkflowTerminal(kv, { ...job.workflowTerminal, workflow: "reflection", producer: "reflect-queue", executionLevel: "workflow" });
    }
  };
  const admissionKey = (request: ReflectRequest) => `admission:${request.project ?? "unscoped"}`;
  const releaseAdmission = (job: ReflectJob) => withKeyedLock(admissionKey(job.request), async () => {
    const current = await kv.get<{ ids: string[] }>(REFLECT_CONTROL, admissionKey(job.request));
    if (current) await kv.set(REFLECT_CONTROL, admissionKey(job.request), { ids: current.ids.filter(id => id !== job.id) });
  });
  const publish = async (job: ReflectJob) => {
    if (!await reflectRecoveryReady(kv, job)) return;
    if (job.executionOrigin) return;
    if (terminal(job) || job.nextAttemptAt > Date.now()) return;
    // The persisted pending key survives either a lost send or a lost acknowledgment.
    await sdk.trigger({ function_id: "iii::durable::publish", payload: { topic: REFLECT_TOPIC, data: { jobId: job.id } }, timeoutMs: 5000 });
  };
  const evidenceInput = (job: ReflectJob, captured: NonNullable<ReflectJob["captured"]>, manifest: ReflectManifest): PublicationEvidenceInput => {
    const project = job.project ?? "unscoped";
    return { workflow: "reflection", operationId: `${job.id}:reflect:${job.unitSequence ?? 0}:${manifest.id}`, project,
      candidate: JSON.stringify({ jobId: job.id, unitSequence: job.unitSequence ?? 0, manifestId: manifest.id,
        generatedResponse: captured.response, generationPrompt: manifest.prompt,
        sourceLineage: manifest.sources.map(source => ({ id: source.id, scope: source.scope, version: source.version,
          fingerprint: source.fingerprint, provenanceReceipt: source.provenanceReceipt })) }),
      sources: manifest.sources.map(source => ({ id: JSON.stringify([source.scope, source.id]), scope: source.scope,
        key: source.id, version: source.version, project,
        content: stripPrivateData(JSON.stringify(source.observation)) })), evidenceComplete: manifest.sources.length > 0 };
  };
  const judge = async (job: ReflectJob, captured: NonNullable<ReflectJob["captured"]>, manifest: ReflectManifest, replayOnly = false,
    disposition?: ReflectJudgeDisposition): Promise<PublicationEvidenceResult> => {
    const input = evidenceInput(job, captured, manifest);
    const observe = async (checkpoint: import("./publication-evidence.js").PublicationEvidenceObservation) => {
      captured.publication = { requestId: checkpoint.requestId, checkpoint };
      await save(job);
    };
    const retain = async (result: PublicationEvidenceResult) => {
      const checkpoint = await observePublicationEvidence(kv, input);
      captured.publication = { requestId: result.requestId, status: result.status,
        reason: result.status === "pending" || result.status === "unavailable" ? result.reason : result.status,
        ...("state" in checkpoint ? { checkpoint } : {}) };
      await save(job);
      return result;
    };
    if (!disposition) {
      const result = await evaluatePublicationEvidence(kv, input, undefined, observe);
      if (replayOnly || result.status !== "unavailable") return retain(result);
    }
    const request = preparePublicationEvidence(input);
    if ("status" in request) return request;
    const execute = publicationExecutor(provider, llmRouter, "reflection", {}, (prepared, dispatch) =>
      disposition ? withProviderAttemptBudget({ operationId: prepared.requestId,
        windowId: disposition.budget.windowId, ceiling: disposition.budget.ceiling }, dispatch) : dispatch());
    return retain(disposition
      ? await supersedePublicationJudge(kv, input, { dispositionId: disposition.dispositionId,
        checkpoint: disposition.checkpoint, budget: disposition.budget,
        ...(disposition.migration ? { migration: disposition.migration } : {}) }, execute, observe)
      : await evaluatePublicationEvidence(kv, input, execute, observe));
  };
  const rejectCandidate = async (job: ReflectJob, manifest: ReflectManifest, reason: string, fingerprint: string) => {
    if (!await currentReflectManifest(kv, job)) throw new ReflectWaiting("waiting_index", "reflect_source_changed");
    const sequence = job.unitSequence ?? 0;
    const effectKey = batchEffectKey(JSON.stringify(["reflect-publication-rejected", job.id, sequence, manifest.id, reason, fingerprint]));
    const receiptKey = `${job.id}:result:${sequence}`;
    const existing = await kv.get<{ kind?: string; manifestId?: string; effectKey?: string; reason?: string; responseFingerprint?: string }>(REFLECT_CONTROL, receiptKey);
    if (existing && (existing.kind !== "publication_rejected" || existing.manifestId !== manifest.id ||
      existing.effectKey !== effectKey || existing.reason !== reason || existing.responseFingerprint !== fingerprint)) {
      throw new Error("reflect_publication_receipt_mismatch");
    }
    if (!existing) await kv.set(REFLECT_CONTROL, receiptKey, { kind: "publication_rejected", manifestId: manifest.id,
      effectKey, reason, responseFingerprint: fingerprint, insightIds: [], appliedCount: 0, nextCursor: manifest.nextCursor });
    job.deferredFamilies = [...new Set([...job.deferredFamilies, reason])];
    await recordAudit(kv, "reflect", "mem::reflect-unit", [], { jobId: job.id, ordinal: job.ordinal,
      skipped: true, reason, manifestId: manifest.id }, undefined, undefined, effectKey);
    await advanceManifest(job, manifest, effectKey, { skipped: true });
  };
  const advanceManifest = async (job: ReflectJob, completedManifest: ReflectManifest,
    effectKey: string, outcome: { skipped?: boolean; appliedCount?: number; replayOnly?: boolean }) => {
    const sequence = job.unitSequence ?? 0;
    const nextCursor = completedManifest.nextCursor;
    const nextSourcePage = nextCursor ? job.sourcePage : undefined;
    const nextOrdinal = !nextCursor && (!job.legacy || job.legacy.offset >= job.legacy.refs.length) ? job.ordinal + 1 : job.ordinal;
    const groupComplete = !nextSourcePage && nextOrdinal >= job.seeds.length && (!job.legacy || job.legacy.offset >= job.legacy.refs.length);
    if (groupComplete && job.global) {
      await kv.set(REFLECT_CONTROL, `${job.id}:project:${fingerprintId("reflectsource", JSON.stringify(job.project))}`, { completed: true, units: sequence + 1 });
    }
    job.resultFingerprint = batchEffectKey(JSON.stringify([job.resultFingerprint ?? "", completedManifest.id, effectKey]));
    job.unitSequence = sequence + 1;
    job.processedClusters++;
    if (outcome.skipped) {
      job.clustersSkipped = (job.clustersSkipped ?? 0) + 1;
      job.reservedInsights = Math.max(0, job.reservedInsights - 5);
    } else {
      job.appliedInsights += outcome.appliedCount ?? 0;
    }
    job.attempts = 0; job.refreshes = 0; job.waits = 0;
    job.sourceCursor = nextCursor;
    if (!nextCursor) {
      job.sourcePage = undefined; job.sourcePageBinding = undefined;
      if (nextOrdinal > job.ordinal) job.ordinal = nextOrdinal;
    }
    job.manifest = undefined; job.captured = undefined; job.generation = undefined; job.gateDecision = undefined; job.error = undefined;
    if (groupComplete && job.global) {
      job.global.ordinal++; job.project = undefined; job.seeds = []; job.ordinal = 0; job.seedBinding = undefined;
    }
    job.status = groupComplete && !job.global ? (job.deferredFamilies.length ? "partial" : "completed") : "pending";
    if (outcome.replayOnly && job.status === "pending") {
      job.status = "blocked"; job.error = "reflect_replay_unit_complete";
    }
    job.nextAttemptAt = 0; job.dispatchPending = !terminal(job) && !job.executionOrigin;
    if (job.status === "completed" && job.legacy) job.continuation = {
      jobId: job.id, workId: job.legacy.workId, correlationId: job.legacy.correlationId,
      sourceFingerprint: job.legacy.sourceFingerprint, requiredSources: job.legacy.refs.length,
      completedSources: job.legacy.offset, completedUnits: job.unitSequence,
      resultFingerprint: job.resultFingerprint!, provenanceFingerprint: job.legacy.provenanceFingerprint,
      sourceIdsFingerprint: batchEffectKey(JSON.stringify(job.legacy.refs.map(ref => [ref.scope, ref.key]).sort())) };
    await save(job);
    if (terminal(job)) { await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job); }
  };
  const advanceEmptyEvidence = async (job: ReflectJob, empty: ReflectNoEvidence) => {
    const sequence = job.unitSequence ?? 0;
    const receipt = { kind: "no_evidence", ...empty.binding, insightIds: [], appliedCount: 0 };
    const receiptKey = `${job.id}:result:${sequence}`;
    const existing = await kv.get(REFLECT_CONTROL, receiptKey);
    if (existing && JSON.stringify(existing) !== JSON.stringify(receipt)) throw new Error("reflect_empty_receipt_mismatch");
    if (!existing) await kv.set(REFLECT_CONTROL, receiptKey, receipt);
    job.resultFingerprint = batchEffectKey(JSON.stringify([job.resultFingerprint ?? "", receiptKey, receipt]));
    job.unitSequence = sequence + 1; job.processedClusters++; job.clustersSkipped = (job.clustersSkipped ?? 0) + 1;
    job.ordinal++; job.attempts = 0; job.refreshes = 0; job.waits = 0;
    job.sourcePage = undefined; job.sourcePageBinding = undefined; job.sourceCursor = undefined;
    const groupComplete = job.ordinal >= job.seeds.length;
    if (groupComplete && job.global) {
      await kv.set(REFLECT_CONTROL, `${job.id}:project:${fingerprintId("reflectsource", JSON.stringify(job.project))}`,
        { completed: true, units: sequence + 1 });
      job.global.ordinal++; job.project = undefined; job.seeds = []; job.ordinal = 0; job.seedBinding = undefined;
    }
    job.status = groupComplete && !job.global ? (job.deferredFamilies.length ? "partial" : "completed") : "pending";
    job.error = "reflect_no_evidence"; job.nextAttemptAt = 0;
    job.dispatchPending = !terminal(job) && !job.executionOrigin;
    await save(job);
    if (terminal(job)) { await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job); }
  };
  const admit = async (input: ReflectRequest & { selected?: SelectedReflectControl }) => {
    const request = validateReflectRequest(input);
    const origin = input.manual ? "manual" : input.selected ? "selected" : undefined;
    if (!request.project && request.sessionId) {
      const session = await kv.get<{ project?: string }>(KV.sessions, request.sessionId);
      if (validText(session?.project)) request.project = session.project;
    }
    if (input.selected) {
      const session = await kv.get<{ project?: string }>(KV.sessions, input.selected.sessionId);
      if (!request.project || session?.project !== request.project || request.deferred || request.legacyWorkId)
        throw new Error("invalid_selected_reflect_request");
    }
    if (request.legacyWorkId && (request.project || request.query || request.sessionId || request.summaryId || request.factIds || request.lessonIds || request.crystalIds)) throw new Error("legacy_reflect_ambiguous_request");
    const id = jobIdentity(request, origin);
    return withKeyedLock(`reflect-job:${id}`, async () => {
      let job = await readJob( id);
      if (job && !await reflectRecoveryReady(kv, job)) return { success: false, error: "reflect_recovery_construction_incomplete" };
      if (job && !terminal(job) && origin !== job.executionOrigin) throw new Error("selected_reflect_origin_conflict");
      if (job?.executionOrigin === "selected" && (!input.selected || input.selected.sessionId !== job.selectedSessionId)) return { success: false, error: "selected_foreground_required" };
      if (!job) {
        await kv.set(REFLECT_PENDING, id, { id, request, ...(origin && { executionOrigin: origin }) });
        try { await withKeyedLock(admissionKey(request), async () => {
          const current = await kv.get<{ ids: string[] }>(REFLECT_CONTROL, admissionKey(request)) ?? { ids: [] };
          if (!current.ids.includes(id) && current.ids.length >= 8) throw new Error("reflect_project_queue_capacity");
          await kv.set(REFLECT_CONTROL, admissionKey(request), { ids: [...new Set([...current.ids, id])] });
        }); } catch (error) { await kv.delete(REFLECT_PENDING, id); throw error; }
        const now = new Date().toISOString();
        job = { id, request, project: request.project, seeds: [], status: "pending", ordinal: 0, reservedInsights: 0,
          appliedInsights: 0, processedClusters: 0, clustersSkipped: 0, unitSequence: 0, attempts: 0, refreshes: 0, createdAt: now, updatedAt: now,
          nextAttemptAt: 0, dispatchPending: !origin, deferredFamilies: [],
          ...(input.manual && { executionOrigin: "manual", manual: { cuts: 0, startedAt: Date.now() } }),
          ...(input.selected && { executionOrigin: "selected", selectedSessionId: input.selected.sessionId }) };
        try { if (request.legacyWorkId) {
          type LegacyWork = { id: string; task: string; callbackProtocolVersion: number; correlationId: string;
            has_result: boolean; has_completion_intent: boolean; metadata: { cluster: string; sourceFingerprint: string; project?: string } };
          const projected = await retrievalRequest<{ item?: LegacyWork; version?: string; fingerprint?: string }>(kv, { action: "reflect_legacy_work", key: request.legacyWorkId, max_bytes: 65536 });
          const work = projected.item;
          if (!work || work.id !== request.legacyWorkId || work.task !== "reflection" || work.callbackProtocolVersion !== 1 ||
            work.has_result !== false || work.has_completion_intent !== false || !validText(work.correlationId) ||
            !validText(projected.version) || !validText(projected.fingerprint) ||
            !validText(work.metadata?.sourceFingerprint) || typeof work.metadata.cluster !== "string" || Buffer.byteLength(work.metadata.cluster) > 65536) throw new Error("legacy_reflect_not_safe_to_bridge");
          const receipt = await kv.get(KV.batchCallbacks, `reflect:${batchEffectKey(work.id)}`);
          const active = await kv.get<{ activeKey?: string }>(KV.batchCallbacks, "active:reflect");
          if (receipt || active?.activeKey === batchEffectKey(work.id)) throw new Error("legacy_reflect_effect_already_started");
          const cluster = JSON.parse(work.metadata.cluster) as ConceptCluster;
          const lists = [cluster.factIds, cluster.lessonIds, cluster.crystalIds];
          if (!Array.isArray(cluster.concepts) || cluster.concepts.length > 12 || cluster.concepts.some(item => !validText(item)) ||
            lists.some(ids => !Array.isArray(ids) || ids.some(id => !validText(id))) || lists.reduce((sum, ids) => sum + ids.length, 0) > 512) throw new Error("legacy_reflect_invalid_sources");
          job.project = undefined;
          job.legacy = { workId: work.id, workVersion: projected.version!, workFingerprint: projected.fingerprint!, correlationId: work.correlationId, sourceFingerprint: work.metadata.sourceFingerprint,
            concepts: cluster.concepts, refs: [...cluster.factIds.map(key => ({ scope: KV.semantic, key, kind: "fact" as const })),
              ...cluster.lessonIds.map(key => ({ scope: KV.lessons, key, kind: "lesson" as const })),
              ...cluster.crystalIds.map(key => ({ scope: KV.crystals, key, kind: "crystal" as const }))],
            offset: 0, provenanceSequence: 0, provenanceFingerprint: "" };
          if (job.legacy.refs.length < 2) throw new Error("legacy_reflect_insufficient_sources");
          await kv.set(REFLECT_CONTROL, `legacy-bridge:${work.id}`, { jobId: job.id, workId: work.id,
            correlationId: work.correlationId, sourceFingerprint: work.metadata.sourceFingerprint });
        } } catch (error) { await kv.delete(REFLECT_PENDING, id); await releaseAdmission(job); throw error; }
        // Write the recovery intent first: an interrupted job creation can be completed by admission retry.
        await save(job);
      }
      if (!origin) try { await publish(job); } catch { job.dispatchPending = true; await save(job); }
      return { success: true, queued: true, jobId: id, status: job.status, completed: job.status === "completed", terminal: terminal(job),
        processedClusters: job.processedClusters, clustersSkipped: job.clustersSkipped ?? 0,
        appliedInsights: job.appliedInsights, deferredFamilies: job.deferredFamilies, coverage: scopedCoverage(job) };
    });
  };
  const manualCheckpoint = (job: ReflectJob) => batchEffectKey(JSON.stringify([job.id, job.request.requestId,
    job.request.project ?? null, job.project ?? null, job.ordinal, job.unitSequence ?? 0, job.manifest?.id,
    job.sourceCursor, job.global, job.manual?.cuts]));
  const accept = async (input: ReflectRequest & { selected?: SelectedReflectControl }) => {
    if (input.manual === undefined) {
      if (input.continuation !== undefined || input.recoveryOfJobId !== undefined) return { success: false, error: "invalid_manual_reflect_request" };
      return admit(input);
    }
    if (input.manual !== true || input.selected || input.deferred || input.legacyWorkId || !validText(input.requestId)
      || input.continuation !== undefined && !validManualReflectContinuation(input.continuation)
      ) {
      return { success: false, error: "invalid_manual_reflect_request" };
    }
    const operation = fingerprintId("reflectmanual", input.requestId);
    type Operation = ReflectManualOperation;
    const context = await kv.get<Operation>(REFLECT_CONTROL, operation);
    const request = validateReflectRequest({ ...input, sessionId: input.sessionId ?? context?.sessionId,
      recoveryOfJobId: input.recoveryOfJobId ?? context?.request?.recoveryOfJobId });
    if (request.sessionId) {
      const session = validText(request.sessionId) ? await kv.get<{ project?: string }>(KV.sessions, request.sessionId) : null;
      if (!request.project || session?.project !== request.project) return { success: false, error: "invalid_manual_reflect_scope" };
    }
    return withKeyedLock(`reflect-manual:${operation}`, async () => {
      const identity = batchEffectKey(JSON.stringify(request));
      const binding = await kv.get<Operation>(REFLECT_CONTROL, operation);
      if (binding && binding.identity !== identity) return { success: false, error: "manual_reflect_identity_mismatch" };
      if (!binding && input.continuation) return { success: false, error: "manual_reflect_continuation_mismatch" };
      if (binding && request.recoveryOfJobId) {
        const cached = await kv.get<{ checkpoint: string; response?: Record<string, unknown> }>(REFLECT_CONTROL, `${operation}:cut:${input.continuation?.cut ?? 0}`);
        if (cached?.response) {
          const job = await readJob( binding.jobId);
          if (!job || job.id !== binding.jobId || job.executionOrigin !== "manual" || !job.manual) return { success: false, error: "manual_reflect_origin_mismatch" };
          if (batchEffectKey(JSON.stringify(job.request)) !== identity || job.project !== request.project
            || binding.request && batchEffectKey(JSON.stringify(binding.request)) !== identity) return { success: false, error: "manual_reflect_identity_mismatch" };
          const continuation = input.continuation;
          if (continuation && (continuation.jobId !== job.id || continuation.requestId !== request.requestId
            || continuation.project !== (request.project ?? null) || continuation.checkpoint !== cached.checkpoint)) return { success: false, error: "manual_reflect_continuation_mismatch" };
          if (job.id !== jobIdentity(request, "manual")) return { success: false, error: "reflect_recovery_construction_conflict" };
          try { await validateReflectRecoveryReplay(kv, job, request, operation, identity); }
          catch (error) {
            if (error instanceof ReflectRecoveryError) return { success: false, error: error.message };
            throw error;
          }
          return cached.response;
        }
      }
      let admitted = binding;
      if (request.recoveryOfJobId) {
        try {
          admitted = await constructReflectRecovery(kv, request, operation, identity, jobIdentity(request, "manual"),
            predecessor => evidenceInput(predecessor, predecessor.captured!, predecessor.manifest!));
        } catch (error) {
          if (error instanceof ReflectRecoveryError) return { success: false, error: error.message };
          throw error;
        }
      }
      if (!admitted) {
        const create = async (): Promise<Operation | { success: false; error: string }> => {
          const result = await admit({ ...request, manual: true });
          if (!("jobId" in result) || !validText(result.jobId)) return { success: false, error: "manual_reflect_admission_failed" };
          const created = { identity, jobId: result.jobId, request, ...(request.sessionId && { sessionId: request.sessionId }) };
          await kv.set(REFLECT_CONTROL, operation, created);
          return created;
        };
        const created = await create();
        if ("success" in created) return created;
        admitted = created;
      }
      let job = await readJob( admitted.jobId);
      if (!job || job.executionOrigin !== "manual" || !job.manual) return { success: false, error: "manual_reflect_origin_mismatch" };
      if (request.sessionId && request.sessionId !== job.request.sessionId) return { success: false, error: "manual_reflect_identity_mismatch" };
      if (request.sessionId && !admitted.sessionId) {
        admitted.sessionId = request.sessionId;
        await kv.set(REFLECT_CONTROL, operation, admitted);
      }
      const continuation = input.continuation;
      if (continuation && (continuation.jobId !== job.id || continuation.requestId !== request.requestId
        || continuation.project !== (request.project ?? null))) return { success: false, error: "manual_reflect_continuation_mismatch" };
      const cut = continuation?.cut ?? 0;
      const receiptKey = `${operation}:cut:${cut}`;
      type Cut = { checkpoint: string; sequence: number; response?: Record<string, unknown> };
      let receipt = await kv.get<Cut>(REFLECT_CONTROL, receiptKey);
      if (receipt && continuation && receipt.checkpoint !== continuation.checkpoint) return { success: false, error: "manual_reflect_continuation_mismatch" };
      if (receipt?.response) return receipt.response;
      if (!receipt) {
        if (cut !== job.manual.cuts || continuation && continuation.checkpoint !== manualCheckpoint(job)) return { success: false, error: "manual_reflect_continuation_mismatch" };
        receipt = { checkpoint: manualCheckpoint(job), sequence: job.unitSequence ?? 0 };
        await kv.set(REFLECT_CONTROL, receiptKey, receipt);
      }
      const limit = cut >= 256;
      if (limit && !manualTerminal(job)) { job.status = "partial"; job.error = "manual_reflect_operation_limit"; await save(job); await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job); }
      if (!limit && !manualTerminal(job) && receipt.sequence === (job.unitSequence ?? 0)) {
        const token = generateId("reflectmanualcut");
        manualExecutions.set(job.id, token);
        try { await runUnit({ jobId: job.id, manualToken: token,
          ...(request.sessionId && { selected: { sessionId: request.sessionId } }) }); }
        finally { manualExecutions.delete(job.id); }
      }
      job = (await readJob( admitted.jobId))!;
      job.manual!.cuts = cut + 1;
      job.dispatchPending = false;
      if (cut + 1 >= 256 && !manualTerminal(job)) {
        job.status = "partial"; job.error = "manual_reflect_operation_limit";
        await save(job);
        await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job);
      }
      await save(job);
      const exhausted = limit || cut + 1 >= 256;
      const next: ManualReflectContinuation | undefined = manualTerminal(job) || exhausted ? undefined : {
        jobId: job.id, requestId: request.requestId!, project: request.project ?? null,
        checkpoint: manualCheckpoint(job), cut: cut + 1 };
      const response = { success: true, jobId: job.id, requestId: request.requestId, status: job.status,
        completed: job.status === "completed", terminal: manualTerminal(job) || exhausted, continuation: next,
        maxRequests: 256,
        processedClusters: job.processedClusters, clustersSkipped: job.clustersSkipped ?? 0,
        appliedInsights: job.appliedInsights, deferredFamilies: job.deferredFamilies, coverage: scopedCoverage(job),
        reason: exhausted && !terminal(job) ? "manual_reflect_operation_limit" : job.error };
      await kv.set(REFLECT_CONTROL, receiptKey, { ...receipt, response });
      return response;
    });
  };
  const acceptBatch = async (data: ReflectRequest, cluster: ConceptCluster) => {
    return withKeyedLock(`reflect-job:${cluster.jobId}`, async () => {
      const job = await readJob( cluster.jobId!);
      if (job && !await reflectRecoveryReady(kv, job)) return { success: false, error: "reflect_recovery_construction_incomplete" };
      const manifest = job?.manifest;
      if (job?.executionOrigin === "manual") return { success: true, reason: "manual_foreground_required" };
      if (!job || terminal(job) || !manifest || manifest.cluster.unitId !== cluster.unitId || manifest.id !== data.batchSourceFingerprint) return { success: true, stale: true };
      if (typeof data.batchResponse !== "string") throw new Error("reflect_result_invalid");
      if (!job.captured) {
        const effectKey = batchEffectKey(`${job.id}:${job.ordinal}:${manifest.id}`);
        if (Buffer.byteLength(data.batchResponse) > 16384) {
          job.generation = { state: "overflow", effectKey, responseBytes: Buffer.byteLength(data.batchResponse),
            responseFingerprint: fingerprintId("reflect-response", data.batchResponse) };
        } else {
          job.captured = { response: data.batchResponse, effectKey };
        }
        await save(job);
        if (job.captured) {
          try {
            job.captured.count = Math.min(countReflectResult(data.batchResponse), 50 - job.appliedInsights);
            job.captured.generationValid = true;
          } catch { job.captured.generationValid = false; job.status = "failed"; job.error = "reflect_generation_invalid"; }
          await save(job);
        }
      }
      if (terminal(job)) { await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job); return { success: false, jobId: job.id, status: job.status, error: job.error }; }
      job.status = "pending"; job.nextAttemptAt = 0; job.dispatchPending = !job.executionOrigin;
      await save(job);
      try { await publish(job); } catch { /* Persisted intent is recovered by the bounded recovery cut. */ }
      return { success: true, queued: true, jobId: job.id, status: job.status };
    });
  };
  const runUnit = async (data: { jobId: string; selected?: SelectedReflectControl; manualToken?: string;
    foreground?: unknown; replayOnly?: unknown; judgeDisposition?: unknown }) => {
    if (!validText(data?.jobId)) return { success: false, error: "invalid_reflect_job" };
    if (data.judgeDisposition !== undefined && (!validReflectJudgeDisposition(data.judgeDisposition)
      || data.replayOnly !== undefined)) return { success: false, error: "invalid_reflect_judge_disposition" };
    const disposition = validReflectJudgeDisposition(data.judgeDisposition) ? data.judgeDisposition : undefined;
    const replayControl = disposition?.unit ?? data.replayOnly;
    if (replayControl !== undefined && !validReflectPublicationReplay(replayControl))
      return { success: false, error: "invalid_reflect_replay_request" };
    if (replayControl !== undefined && (data.foreground !== undefined || data.selected !== undefined || data.manualToken !== undefined))
      return { success: false, error: "conflicting_reflect_execution_controls" };
    const replayOnly = replayControl !== undefined;
    let foregroundControl: ReflectForegroundControl | undefined;
    if (validReflectPublicationReplay(replayControl)) {
      foregroundControl = { project: replayControl.project, requestId: replayControl.requestId };
    } else if (data.foreground !== undefined) {
      if (!validReflectForegroundControl(data.foreground))
        return { success: false, error: "invalid_reflect_foreground_request" };
      foregroundControl = data.foreground;
    }
    if (foregroundControl !== undefined && (data.selected !== undefined || data.manualToken !== undefined))
      return { success: false, error: "conflicting_reflect_execution_controls" };
    return withKeyedLock(`reflect-job:${data.jobId}`, async () => {
      const job = await readJob( data.jobId);
      if (job && !await reflectRecoveryReady(kv, job)) return { success: false, error: "reflect_recovery_construction_incomplete" };
      if (foregroundControl !== undefined) {
        if (!job || job.id !== data.jobId) return { success: false, error: "reflect_foreground_job_not_found" };
        if (job.executionOrigin !== undefined || job.request.manual === true)
          return { success: false, error: "reflect_foreground_origin_mismatch" };
        if (job.project !== foregroundControl.project || job.request.project !== foregroundControl.project ||
          job.request.requestId !== foregroundControl.requestId)
          return { success: false, error: "reflect_foreground_identity_mismatch" };
      }
      if (replayControl !== undefined && validReflectPublicationReplay(replayControl)) {
        if (!job?.manifest || !job.captured || job.generation || job.legacy || job.global || job.continuation
          || job.manifest.id !== replayControl.manifestId || (job.unitSequence ?? 0) !== replayControl.unitSequence
          || job.captured.effectKey !== replayControl.effectKey
          || batchEffectKey(`${job.id}:${job.ordinal}:${job.manifest.id}`) !== replayControl.effectKey)
          return { success: false, error: "reflect_replay_identity_mismatch" };
        if (disposition && (batchEffectKey(job.captured.response) !== disposition.captureFingerprint
          || job.captured.evidenceRequestId || job.captured.insightIds))
          return { success: false, error: "reflect_judge_capture_identity_mismatch" };
      }
      const foreground = foregroundControl !== undefined;
      const manual = job?.executionOrigin === "manual" && typeof data.manualToken === "string" && manualExecutions.get(data.jobId) === data.manualToken;
      if (job?.executionOrigin === "manual" && !manual) return { success: true, reason: "manual_foreground_required" };
      if (data.manualToken && !manual) return { success: false, error: "invalid_manual_reflect_execution" };
      if (job && await kv.get(REFLECT_CONTROL, `recovery-successor:${job.id}`)) return {
        success: false, jobId: job.id, status: "blocked", reason: "reflect_recovery_linked_successor" };
      if (job?.executionOrigin === "selected" && (!data.selected || data.selected.sessionId !== job.selectedSessionId)) return { success: true, reason: "selected_foreground_required" };
      if (data.selected) {
        const session = await kv.get<{ project?: string }>(KV.sessions, data.selected.sessionId);
        if (!job || job.executionOrigin !== "selected" && !manual || session?.project !== job.project)
          return { success: false, error: "invalid_selected_reflect_request" };
      }
      const recoverableTerminal = replayOnly && (job?.status === "blocked" || job?.status === "failed")
        || job?.status === "blocked" && (manual || data.selected);
      if (!job || terminal(job) && !recoverableTerminal || !manual && !data.selected && !foreground && job.nextAttemptAt > Date.now()) return { success: true, skipped: true };
      if (isBackgroundRecoveryPaused() && !job.manifest && !job.captured && !data.selected && !manual && !foreground) return { success: true, paused: true };
      if (job.claim && job.claim.expiresAt > Date.now()) return { success: true, claimed: true };
      if (!data.selected) {
        job.claim = { id: generateId("reflectclaim"), expiresAt: Date.now() + 120000 };
        job.dispatchPending = false; job.dispatchedAt = Date.now();
        await save(job);
      }
      try {
        if (!replayOnly && !job.manifest && !await prepareReflectGroup(kv, job)) {
          job.status = "pending"; job.dispatchPending = !job.executionOrigin; await save(job);
          return { success: true, jobId: job.id, status: job.status, preparing: true };
        }
        if (!job.project && job.global && job.global.initialized && !job.global.hasMore && job.global.ordinal >= job.global.projects.length) {
          job.status = job.deferredFamilies.length ? "partial" : "completed";
          await save(job); await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job);
          return { success: true, status: job.status };
        }
        if (!replayOnly) { await freezeReflectSeeds(kv, job); await save(job); }
        if (!job.manifest) {
          const emptyReceipt = await kv.get<ReflectEmptyEvidence & { kind?: string }>(REFLECT_CONTROL,
            `${job.id}:result:${job.unitSequence ?? 0}`);
          if (emptyReceipt?.kind === "no_evidence") {
            if (job.legacy || job.captured || emptyReceipt.project !== job.project || emptyReceipt.ordinal !== job.ordinal ||
              emptyReceipt.seed !== job.seeds[job.ordinal] || JSON.stringify(emptyReceipt.seedBinding) !== JSON.stringify(job.seedBinding) ||
              !emptyReceipt.indexIdentity || typeof emptyReceipt.graphGeneration !== "string") throw new Error("reflect_empty_receipt_mismatch");
            await advanceEmptyEvidence(job, new ReflectNoEvidence({ project: emptyReceipt.project, seed: emptyReceipt.seed,
              ordinal: emptyReceipt.ordinal, seedBinding: emptyReceipt.seedBinding,
              indexIdentity: emptyReceipt.indexIdentity, graphGeneration: emptyReceipt.graphGeneration }));
            return { success: true, jobId: job.id, status: job.status, reason: job.error };
          }
          if (job.ordinal >= job.seeds.length && !job.sourcePage?.length) {
            job.status = job.deferredFamilies.length ? "partial" : "completed";
            await save(job); await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job);
            return { success: true, status: job.status };
          }
          job.manifest = await retrieveReflectManifest(kv, job, embedding);
          job.reservedInsights = Math.min(50, job.reservedInsights + 5);
          job.deferredFamilies = [...new Set([...job.deferredFamilies, ...job.manifest.deferredFamilies])];
          await save(job);
        }
        if (!job.manifest) throw new Error("reflect_manifest_missing");
        const manifest = job.manifest;
        const sequence = job.unitSequence ?? 0;
        const effectKey = batchEffectKey(`${job.id}:${job.ordinal}:${manifest.id}`);
        const resultKey = `${job.id}:result:${sequence}`;
        const priorResult = await kv.get<{ kind?: string; manifestId?: string; effectKey?: string; reason?: string;
          responseFingerprint?: string; appliedCount?: number; insightIds?: string[] }>(REFLECT_CONTROL, resultKey);
        if (replayOnly && priorResult?.kind === "publication_rejected")
          throw new ReflectWaiting("blocked", "reflect_publication_rejected");
        if (priorResult?.kind === "publication_rejected") {
          const rejectedEffect = batchEffectKey(JSON.stringify(["reflect-publication-rejected", job.id, sequence,
            manifest.id, priorResult.reason, priorResult.responseFingerprint]));
          if (priorResult.manifestId !== manifest.id || priorResult.effectKey !== rejectedEffect ||
            !priorResult.reason || !priorResult.responseFingerprint || job.captured?.response &&
            priorResult.responseFingerprint !== batchEffectKey(job.captured.response)) throw new Error("reflect_publication_receipt_mismatch");
          job.deferredFamilies = [...new Set([...job.deferredFamilies, priorResult.reason])];
          await advanceManifest(job, manifest, priorResult.effectKey, { skipped: true });
          return { success: true, jobId: job.id, status: job.status, skipped: true, recovered: true };
        }
        if (priorResult) {
          if (priorResult.manifestId !== manifest.id || priorResult.effectKey !== effectKey ||
            job.captured?.response && priorResult.responseFingerprint !== batchEffectKey(job.captured.response)) {
            throw new Error("reflect_result_receipt_mismatch");
          }
          await advanceManifest(job, manifest, effectKey, { appliedCount: priorResult.appliedCount ??
            job.captured?.count ?? priorResult.insightIds?.length ?? 0, replayOnly });
          return { success: true, jobId: job.id, status: job.status, recovered: true };
        }
        let callbackReceipt = job.captured && await kv.get<{ state?: string }>(KV.batchCallbacks, `reflect:${job.captured.effectKey}`);
        if (callbackReceipt?.state === "stale") throw new ReflectWaiting("waiting_index", "reflect_source_changed");
        const effectAcknowledged = callbackReceipt?.state === "started" || callbackReceipt?.state === "completed";
        if (isBackgroundRecoveryPaused() && !data.selected && !manual && !foreground && !effectAcknowledged) return { success: true, paused: true };
        if (!job.captured) {
          if (replayOnly) throw new ReflectWaiting("blocked", "reflect_replay_capture_required");
          if (!await currentReflectManifest(kv, job)) throw new ReflectWaiting("waiting_index", "reflect_source_changed");
          if (job.generation?.state === "intended") throw new ReflectWaiting("blocked", "reflect_generation_outcome_unknown");
          if (job.generation?.state === "overflow") {
            throw new ReflectWaiting("failed", "reflect_result_limit");
          }
          if (!manifest.sources.length) throw new ReflectWaiting("waiting_index", "insufficient_indexed_evidence");
          if (job.request.deferred && batchQueue && !data.selected && !manual) {
            const result = await batchQueue.enqueue({ correlationId: fingerprintId("reflectunit", `${job.id}:${job.ordinal}:${job.manifest.id}`),
              task: "reflection", systemPrompt: REFLECT_SYSTEM, userPrompt: job.manifest.prompt,
              metadata: { cluster: JSON.stringify(job.manifest.cluster), project: job.project!, sourceFingerprint: job.manifest.id } });
            if (!result.queued) throw new ReflectWaiting("waiting_index", "reflect_batch_not_accepted");
            job.status = "waiting_batch"; job.nextAttemptAt = Date.now() + 15 * 60000;
            await save(job);
            return { success: true, queued: true, jobId: job.id, status: job.status };
          }
          let timer: ReturnType<typeof setTimeout> | undefined;
          let timedOut = false;
          try {
            const options = {};
            if (data.selected) {
              job.claim = { id: generateId("reflectclaim"), expiresAt: Date.now() + 120000 };
              job.dispatchPending = false; job.dispatchedAt = Date.now(); await save(job);
            }
            job.generation = { state: "intended", effectKey };
            await save(job);
            const operation = async (target: MemoryProvider) => {
              const response = await target.summarize(REFLECT_SYSTEM, manifest.prompt, options);
              const responseBytes = Buffer.byteLength(response);
              if (timedOut) {
                await withKeyedLock(`reflect-job:${job.id}`, async () => {
                  const current = await readJob( job.id);
                  if (!current || current.manifest?.id !== manifest.id || current.generation?.effectKey !== effectKey) return;
                  if (responseBytes > 16384) current.generation = { state: "overflow", effectKey, responseBytes,
                    responseFingerprint: fingerprintId("reflect-response", response) };
                  else { current.captured = { response, effectKey }; current.generation = undefined; }
                  await save(current);
                });
                return response;
              }
              if (responseBytes > 16384) job.generation = { state: "overflow", effectKey, responseBytes,
                responseFingerprint: fingerprintId("reflect-response", response) };
              else { job.captured = { response, effectKey }; job.generation = undefined; }
              await save(job);
              return response;
            };
            const call = () => llmRouter
              ? llmRouter.run("reflection", operation, () => true)
              : operation(provider);
            await Promise.race([call(), new Promise<never>((_, reject) => { timer = setTimeout(() => { timedOut = true; reject(new Error("reflect_provider_timeout")); }, 45000); })]);
            if (job.captured) {
              job.captured.count = Math.min(countReflectResult(job.captured.response), 50 - job.appliedInsights);
              job.captured.generationValid = true;
              await save(job);
            }
          } finally { if (timer) clearTimeout(timer); }
        }
        if (job.generation?.state === "overflow") {
          throw new ReflectWaiting("failed", "reflect_result_limit");
        }
        if (!job.captured) throw new ReflectWaiting("blocked", "reflect_generation_outcome_unknown");
        const captured = job.captured;
        if (captured.effectKey !== effectKey) throw new Error("reflect_effect_identity_mismatch");
        if (!effectAcknowledged) {
        if (isBackgroundRecoveryPaused() && !data.selected && !manual && !foreground) return { success: true, paused: true };
          if (!await currentReflectManifest(kv, job)) throw new ReflectWaiting("waiting_index", "reflect_source_changed");
          captured.count = Math.min(countReflectResult(captured.response), 50 - job.appliedInsights);
          captured.generationValid = true;
          await save(job);
          const verdict = await judge(job, captured, manifest, replayOnly, disposition);
          if (verdict.status === "unsupported" && !replayOnly) {
            await rejectCandidate(job, manifest, "reflect_publication_unsupported", batchEffectKey(captured.response));
            return { success: true, jobId: job.id, status: job.status, skipped: true, reason: verdict.reason };
          }
          if (verdict.status !== "supported") {
            const status = verdict.status === "pending" ? "blocked" : "failed";
            throw new ReflectWaiting(status, `reflect_publication_${verdict.status}`);
          }
          if (!await currentReflectManifest(kv, job)) throw new ReflectWaiting("waiting_index", "reflect_source_changed");
          captured.evidenceRequestId = verdict.requestId;
          await save(job);
        }
        if (!effectAcknowledged && isBackgroundRecoveryPaused() && !data.selected && !manual && !foreground) return { success: true, paused: true };
        const applied = await runBatchCallback(kv, "reflect", captured.effectKey, async (resuming, admit) => {
          if (!resuming && !await currentReflectManifest(kv, job)) return { success: true, stale: true };
          await admit();
          const result = await apply(captured.response, job.manifest!.cluster, job.legacy ? undefined : job.project, Math.min(5, 50 - job.appliedInsights), captured.effectKey);
          captured.count = result.count;
          captured.insightIds = result.insightIds;
          await save(job);
          return { success: true, count: result.count, insightIds: result.insightIds };
        });
        if (applied.stale) throw new ReflectWaiting("waiting_index", "reflect_source_changed");
        if (countReflectResult(captured.response) > (captured.count ?? 5)) job.deferredFamilies = [...new Set([...job.deferredFamilies, "insight_budget"])];
        await recordAudit(kv, "reflect", "mem::reflect-unit", [], { jobId: job.id, ordinal: job.ordinal,
          count: captured.count ?? countReflectResult(captured.response) }, undefined, undefined, captured.effectKey);
        const completedManifest = job.manifest;
        if (!completedManifest) throw new Error("reflect_manifest_missing");
        await kv.set(REFLECT_CONTROL, resultKey, { manifestId: completedManifest.id,
          effectKey: captured.effectKey, cluster: completedManifest.cluster, nextCursor: completedManifest.nextCursor,
          responseFingerprint: batchEffectKey(captured.response), insightIds: captured.insightIds ?? [],
          appliedCount: captured.count ?? countReflectResult(captured.response), legacyWorkId: job.legacy?.workId,
          ...(captured.evidenceRequestId ? { evidenceRequestId: captured.evidenceRequestId } : {}) });
        await advanceManifest(job, completedManifest, captured.effectKey,
          { appliedCount: captured.count ?? countReflectResult(captured.response), replayOnly });
      } catch (error) {
        if (error instanceof ReflectNoEvidence) {
          await advanceEmptyEvidence(job, error);
          return { success: true, jobId: job.id, status: job.status, reason: job.error };
        }
        if (error instanceof IndexedRetrievalError) error = new ReflectWaiting(
          /GRAPH/.test(error.code) ? "waiting_graph" : "waiting_index", error.code);
        if (error instanceof ReflectWaiting) {
          job.status = error.status; job.error = error.message; job.waits = (job.waits ?? 0) + 1;
          if (error.message === "reflect_source_changed" && job.legacy) { job.status = "failed"; job.error = "legacy_reflect_sources_changed"; }
          else if (error.message === "reflect_source_changed") {
            job.status = "failed";
          }
        } else if (error instanceof Error && error.message === "reflect_generation_invalid") {
          if (job.captured) job.captured.generationValid = false;
          job.status = "failed"; job.error = "reflect_generation_invalid";
        } else if (job.generation?.state === "intended" && !job.captured) {
          job.status = "blocked"; job.error = "reflect_generation_outcome_unknown";
        } else {
          job.attempts++;
          const receipt = job.captured && await kv.get<{ state: string }>(KV.batchCallbacks, `reflect:${job.captured.effectKey}`);
          job.status = receipt?.state === "started" ? "waiting_index" : job.attempts >= 3 ? "failed" : "pending";
          job.error = receipt?.state === "started" ? "reflect_admitted_effect_pending" : "reflect_unit_failed";
        }
        job.nextAttemptAt = Date.now() + Math.min(900000, 120000 * 2 ** Math.min(job.attempts + job.refreshes + (job.waits ?? 0), 7));
        if (replayOnly && !terminal(job)) job.status = "blocked";
        job.dispatchPending = !terminal(job) && !job.executionOrigin;
        await save(job);
        if (terminal(job)) { await kv.delete(REFLECT_PENDING, job.id); await releaseAdmission(job); }
      } finally { job.claim = undefined; await save(job); }
      if (!data.selected && !foreground && job.dispatchPending && job.nextAttemptAt <= Date.now()) { try { await publish(job); } catch { /* Durable intent remains. */ } }
      return { success: true, jobId: job.id, status: job.status, reason: job.error };
    });
  };
  sdk.registerFunction("mem::reflect-unit", runUnit);
  sdk.registerTrigger({ type: "durable:subscriber", function_id: "mem::reflect-unit",
    config: { topic: REFLECT_TOPIC, queue_config: { type: "fifo", concurrency: 1, maxRetries: 3, backoffDelayMs: 30000 } } });
  sdk.registerFunction("mem::reflect-status", async (data: { jobId: string; publication?: unknown }) => {
    if (!validText(data?.jobId)) return { success: false, error: "invalid_reflect_job" };
    const job = await readJob( data.jobId);
    let publication: unknown;
    if (data.publication !== undefined) {
      if (!validReflectPublicationReplay(data.publication) || !job?.manifest || !job.captured
        || job.project !== data.publication.project || job.request.project !== data.publication.project
        || job.request.requestId !== data.publication.requestId || job.manifest.id !== data.publication.manifestId
        || (job.unitSequence ?? 0) !== data.publication.unitSequence || job.captured.effectKey !== data.publication.effectKey)
        return { success: false, error: "reflect_observation_identity_mismatch" };
      publication = await observePublicationEvidence(kv, evidenceInput(job, job.captured, job.manifest));
    }
    return job ? { success: true, jobId: job.id, status: job.status, processedClusters: job.processedClusters,
      clustersSkipped: job.clustersSkipped ?? 0, appliedInsights: job.appliedInsights, reservedInsights: job.reservedInsights,
      deferredFamilies: job.deferredFamilies, error: job.error,
      ...(publication !== undefined ? { publication } : {}),
      coverage: job.legacy ? { ...scopedCoverage(job), legacyWorkId: job.legacy.workId, correlationId: job.legacy.correlationId,
        sourceFingerprint: job.legacy.sourceFingerprint, projectedSources: job.legacy.offset, requiredSources: job.legacy.refs.length,
        completedUnits: job.unitSequence ?? 0, provenancePages: job.legacy.provenanceSequence,
        provenanceFingerprint: job.legacy.provenanceFingerprint,
        complete: job.status === "completed" && !job.manifest && !job.sourcePage && job.legacy.offset === job.legacy.refs.length } : scopedCoverage(job) } : { success: false, error: "reflect_job_not_found" };
  });
  sdk.registerFunction("mem::reflect-recover", async () => {
    if (isBackgroundRecoveryPaused()) return { success: true, paused: true, dispatched: 0 };
    const cursor = await kv.get<{ cursor?: string }>(REFLECT_CONTROL, "recovery-cursor");
    let dispatched = 0;
    try { for await (const page of kv.pages<ReflectPending & { request?: ReflectRequest; executionOrigin?: "selected" | "manual" }>(REFLECT_PENDING, { cursor: cursor?.cursor, limit: 16, maxBytes: 32768 })) {
      for (const pending of page.items) {
        if (pending.executionOrigin) continue;
        if (!validText(pending.id)) continue;
        const job = await readJob( pending.id);
        if (job?.executionOrigin) continue;
        if (!job && pending.request) { await admit(pending.request); dispatched++; continue; }
        if (!job || terminal(job)) { await kv.delete(REFLECT_PENDING, pending.id); if (job) await releaseAdmission(job); continue; }
        if (job.claim && job.claim.expiresAt > Date.now() || job.nextAttemptAt > Date.now()) continue;
        await publish(job); dispatched++;
      }
      await kv.set(REFLECT_CONTROL, "recovery-cursor", { cursor: page.next_cursor ?? undefined });
      break;
    } } catch (error) {
      if (!(error instanceof StatePageError) || error.code !== "STATE_PAGE_CURSOR_STALE") throw error;
      await kv.delete(REFLECT_CONTROL, "recovery-cursor");
      return { success: true, dispatched, cursorReset: true };
    }
    return { success: true, dispatched };
  });
  return { accept, acceptBatch };
}
