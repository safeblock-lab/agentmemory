import { observeWorkflowTerminal, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import { getAgentId } from "../config.js";
import type { WorkflowTerminalReceipt } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { KV, fingerprintId } from "../state/schema.js";
import type { ConsolidationFragment, ConsolidationManifest } from "./consolidation-partitions.js";
import { ConsolidationPending, ConsolidationRequestBlocked, replaceConsolidationRequest, boundedPartitions, iterateSourceFragments, type ConsolidationRequests } from "./consolidation-partitions.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import { SEMANTIC_FRAGMENT_SYSTEM, PROCEDURAL_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt } from "../prompts/consolidation.js";
import { captureSourceCut, qualityCut, verifySourceCut, verifySourceProjects, verifyCapturedSource } from "./consolidation-capture.js";
import type { PublicationEvidenceResult } from "./publication-evidence.js";
import { extractCut, reductionCut, aggregateCut, candidatePage, aggregateCandidateKey, publicationCandidate, recoverLegacyFinalCut, type AggregateCandidate } from "./consolidation-reduction.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { consolidationRetrievalCoverage, type ConsolidationRetrievalCoverage } from "./consolidation-selection.js";

export const CONSOLIDATION_PAGE_BYTES = 65_536;
export const CONSOLIDATION_SOURCE_BYTES = 16 * 1024 * 1024;
export type RetrievalCoverageReason = ConsolidationRetrievalCoverage;
export interface PagedSource {
  id: string; sessionId: string; scope: string; rowVersion?: string;
  kind: "summary" | "observation" | "memory"; project: string;
  retrievalCoverage?: RetrievalCoverageReason[];
}
export interface SourceHeader extends PagedSource {
  rowVersion: string; fingerprint: string; bytes: number; frequency: number; eligible: boolean;
  createdAt?: string; conceptCount: number;
  typesafe?: { protected: boolean; deterministicDrop: boolean; state: string; gateProtected: boolean; gateState: string; updatedAt?: string };
  legacy?: { observationFingerprint?: string; contentFingerprint?: string; domainVersion?: number; updatedAt?: string };
}
export interface ConsolidationInvocationPolicy {
  deferred: boolean;
  force: boolean;
  compaction: boolean;
  pipelineGate: boolean;
}
export interface ConsolidationTypeSafeDecision {
  selectedSourceIndexes: number[];
  droppedSourceIndexes: number[];
  compactionFallbackReasons: string[];
  pipelineGate: "run" | "skip" | "fail-open" | "not-applicable";
}
export interface PagedCandidate {
  id: string; key: string; sourceSessionIds: string[]; sourceObservationIds?: string[]; children: string[];
  fact?: string; confidence?: number; name?: string; trigger?: string; steps?: string[];
  memoryXml?: string; memoryTitle?: string; originalMemoryTitle?: string; originalProcedureName?: string;
  legacyCandidateIds?: string[];
}
export interface CandidatePage {
  id: string; candidates: PagedCandidate[]; examined: number; rejected: number;
}
export interface PagedConsolidation {
  workflowTerminal?: WorkflowTerminalReceipt;
  schema: 2; id: string; kind: "semantic" | "procedural" | "memory"; project: string;
  refs: PagedSource[]; createdAt: string; deferred: boolean;
  phase: "legacy" | "capture" | "quality" | "extract" | "reduce" | "verify" | "aggregate" | "publish" | "completed" | "retained";
  capture: { source: number; offset: number; ordinal: number; concepts: number };
  fragmentPages: number; fragmentCount: number; extract: { page: number; offset: number };
  candidatePages: number; candidateCount: number;
  reduction: { round: number; inputPages: number; inputCount: number; page: number; offset: number; outputPages: number; outputCount: number };
  finalRound: number; finalPages: number; verify: number; aggregate: { page: number; offset: number };
  publish: { page: number; offset: number };
  counters: { examined: number; rejected: number; created: number; newlyCreated: number; applied: number; replayed: number; published: number };
  checkpoint?: { processedThrough: string; processedSessionIdsAtThrough: string[] };
  checkpointKey?: string;
  legacy?: { originalId: string; source: number; fragment: number; imported: boolean; verified?: boolean; effectKey?: string; cohort?: string; fingerprint?: string;
    generatedSource?: number; generatedOffset?: number; round?: number; scanComplete?: boolean; recoveredFinal?: boolean;
    recoveredRound?: number; roundOffset?: number; seedComplete?: boolean; completed?: boolean };
  minObservations?: number; error?: string; result?: Record<string, unknown>;
  retrievalCoverage: RetrievalCoverageReason[];
  quality: { source: number; page: number }; eligibleSources: number[];
  baselineCheckpoint?: { processedThrough: string; processedSessionIdsAtThrough: string[] }; minimumNewSummaries?: number; emptyReason?: string;
  historicalBaselineCheckpoint?: { processedThrough: string; processedSessionIdsAtThrough: string[] };
  callerPolicy?: ConsolidationInvocationPolicy;
  typesafeDecision?: ConsolidationTypeSafeDecision;
  typesafeValidation?: { sourceIndexes: number[]; nextSource: number; complete: boolean };
}
export function boundedBytes(value: unknown, maximum = CONSOLIDATION_PAGE_BYTES): void {
  if (Buffer.byteLength(JSON.stringify(value), "utf8") > maximum) throw new Error("consolidation_page_budget_exceeded");
}
export function pageKey(id: string, kind: string, index: number | string): string {
  return `conpage_${createHash("sha256").update(JSON.stringify([id, kind, index])).digest("hex")}`;
}
export function semanticProjectCheckpointKey(project: string): string {
  return pageKey(project, "semantic-checkpoint", 0);
}
export async function immutablePage<T>(kv: StateKV, key: string, value: T): Promise<void> {
  boundedBytes(value);
  const previous = await kv.get<T>(KV.state, key);
  if (previous !== null) {
    if (!isDeepStrictEqual(previous, value)) throw new Error("consolidation_immutable_page_conflict");
    return;
  }
  await kv.set(KV.state, key, value);
}
export async function saveRoot(kv: StateKV, root: PagedConsolidation): Promise<void> {
  if (root.kind === "memory" && root.phase === "completed" && root.result?.skipped !== true) {
    root.workflowTerminal ??= { executionId: root.id, agentId: getAgentId(), occurredAt: new Date().toISOString(),
      outcome: root.result?.partial === true ? "partial" : "completed" };
  }
  boundedBytes(root);
  await kv.set(KV.state, pageKey(root.id, "root", 0), root);
  if (root.workflowTerminal) await observeWorkflowTerminal(kv, { ...root.workflowTerminal,
    workflow: "consolidation", producer: "consolidate-memory", executionLevel: "workflow" });
}
export async function loadRoot(kv: StateKV, id: string): Promise<PagedConsolidation | null> {
  const root = await kv.get<PagedConsolidation>(KV.state, pageKey(id, "root", 0));
  if (root) {
    if (root.kind === "memory" && root.phase === "completed" && root.result?.skipped !== true) {
      if (root.workflowTerminal) await observeWorkflowTerminal(kv, { ...root.workflowTerminal,
        workflow: "consolidation", producer: "consolidate-memory", executionLevel: "workflow" });
      else await markWorkflowStatisticsGap(kv, "consolidation");
    }
    boundedBytes(root);
    if (root.schema !== 2 || root.id !== id || !Array.isArray(root.refs) || root.refs.length > 12) throw new Error("consolidation_root_identity_invalid");
  }
  return root;
}
export function assertRootIdentity(root: PagedConsolidation, kind: PagedConsolidation["kind"], project: string, refs: PagedSource[],
  callerPolicy?: ConsolidationInvocationPolicy): void {
  if (root.kind !== kind || root.project !== project || root.refs.length !== refs.length || refs.some((ref, index) => {
    const captured = root.refs[index];
    return ref.id !== captured.id || ref.sessionId !== captured.sessionId || ref.scope !== captured.scope || ref.kind !== captured.kind
      || ref.project !== captured.project || ref.rowVersion !== undefined && ref.rowVersion !== captured.rowVersion;
  }) || callerPolicy !== undefined && !isDeepStrictEqual(root.callerPolicy, callerPolicy)) throw new Error("consolidation_capture_identity_conflict");
}
export function newRoot(id: string, kind: PagedConsolidation["kind"], project: string, refs: PagedSource[], deferred = false,
  callerPolicy?: ConsolidationInvocationPolicy): PagedConsolidation {
  if (!project.trim() || refs.length > 12 || refs.some(ref => ref.project !== project || !ref.id || !ref.sessionId || !ref.scope
    || [ref.id, ref.sessionId, ref.scope].some(value => Buffer.byteLength(value) > 256))) throw new Error("consolidation_source_reference_invalid");
  const root: PagedConsolidation = { schema: 2, id, kind, project, refs, deferred, createdAt: new Date().toISOString(), phase: "capture",
    capture: { source: 0, offset: 0, ordinal: 0, concepts: 0 }, fragmentPages: 0, fragmentCount: 0,
    extract: { page: 0, offset: 0 }, candidatePages: 0, candidateCount: 0,
    reduction: { round: 0, inputPages: 0, inputCount: 0, page: 0, offset: 0, outputPages: 0, outputCount: 0 },
    finalRound: 0, finalPages: 0, verify: 0, aggregate: { page: 0, offset: 0 }, publish: { page: 0, offset: 0 },
    counters: { examined: 0, rejected: 0, created: 0, newlyCreated: 0, applied: 0, replayed: 0, published: 0 }, quality: { source: 0, page: 0 }, eligibleSources: [],
    retrievalCoverage: consolidationRetrievalCoverage(refs), ...(callerPolicy ? { callerPolicy } : {}) };
  boundedBytes(root); return root;
}
export type FragmentPage = { fragments: ConsolidationFragment[]; source: number };
export type LegacyManifest = ConsolidationManifest<Record<string, unknown>>;

export function consolidationIsPending(error: unknown): boolean {
  return error instanceof ConsolidationPending || Boolean(error && typeof error === "object" && "code" in error
    && ["STATE_CANONICAL_PREPARING", "STATE_TX_CONFLICT", "STATE_INDEX_NOT_READY"].includes(String(error.code)));
}
export function sourceRetrievalLimited(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && "code" in error && error.code === "STATE_RETRIEVAL_RESOURCE_LIMIT");
}
export interface PagedPublisherResult { applied: boolean; created: boolean; id: string; rowVersion: string }
export async function runPagedCut(kv: StateKV, id: string, requests: (root: PagedConsolidation) => ConsolidationRequests,
  publish: (root: PagedConsolidation, candidate: AggregateCandidate) => Promise<PagedPublisherResult>,
  beforeExtract?: (root: PagedConsolidation) => Promise<boolean | void>,
  publicationEvidence?: (root: PagedConsolidation, candidate: AggregateCandidate) => Promise<PublicationEvidenceResult>,
  recoverPublication?: (root: PagedConsolidation, candidate: AggregateCandidate) => Promise<PagedPublisherResult | null>): Promise<Record<string, unknown>> {
  return withKeyedLock(`consolidation-paged:${id}`, async () => {
    const root = await loadRoot(kv, id);
    if (!root) throw new Error("consolidation_manifest_missing");
    if (root.phase === "completed") return { ...root.result, completed: true, manifestId: id, captureId: id, replayed: true,
      newFacts: 0, newProcedures: 0, appliedEffects: 0 };
    try {
      if (root.phase === "legacy") await importLegacyCut(kv, root);
      else if (root.phase === "capture") await captureSourceCut(kv, root);
      else if (root.phase === "quality") await qualityCut(kv, root);
      else if (root.phase === "extract") {
        if (beforeExtract && root.callerPolicy && !root.legacy
          && (!root.typesafeDecision || root.typesafeDecision.pipelineGate === "skip" && root.typesafeValidation?.complete !== true)) {
          const ready = await beforeExtract(root);
          if (ready === false) return { completed: false, queued: true, manifestId: id, captureId: id, phase: root.phase,
            continuation: root.kind === "memory" ? { consolidationCaptureId: id } : { consolidationManifestId: id } };
        }
        if (!publicationEvidence && root.callerPolicy && !root.legacy && root.typesafeDecision?.pipelineGate === "skip") {
          root.phase = "completed";
          root.result = { completed: true, skipped: true, reason: "TypeSafe pipeline gate", manifestId: root.id,
            captureId: root.id, sourceCount: root.refs.length,
            selectedSourceIds: root.typesafeDecision.selectedSourceIndexes.map(index => root.refs[index]!.id),
            selectedSources: root.typesafeDecision.selectedSourceIndexes.map(index => ({ id: root.refs[index]!.id,
              sessionId: root.refs[index]!.sessionId, project: root.refs[index]!.project, rowVersion: root.refs[index]!.rowVersion })),
            compactedSourceIds: root.typesafeDecision.droppedSourceIndexes.map(index => root.refs[index]!.id),
            compactedSourceCount: root.typesafeDecision.droppedSourceIndexes.length, appliedEffects: 0, replayedEffects: 0,
            newFacts: 0, newProcedures: 0, typeSafeDecision: root.typesafeDecision };
          await saveRoot(kv, root);
          return root.result;
        }
        if (root.phase === "extract") await extractCut(kv, root, requests(root));
      }
      else if (root.phase === "reduce") await reductionCut(kv, root, requests(root));
      else if (root.phase === "verify") await verifySourceCut(kv, root);
      else if (root.phase === "aggregate") await aggregateCut(kv, root);
      else if (root.phase === "publish") {
        if (root.publish.page < root.finalPages) {
          const page = await candidatePage(kv, root, root.publish.page);
          for (const candidate of page.candidates.slice(root.publish.offset, root.publish.offset + (publicationEvidence ? 1 : 12))) {
            const position = `${root.publish.page}:${root.publish.offset}`;
            const aggregate = await kv.get<AggregateCandidate>(KV.state, aggregateCandidateKey(root, candidate));
            if (!aggregate) throw new Error("consolidation_aggregate_missing");
            if (aggregate.anchor === position) {
              const receiptKey = pageKey(root.id, "published", position);
              let effect = await kv.get<{ rejected?: boolean; applied: boolean; created: boolean; id?: string; rowVersion?: string }>(KV.state, receiptKey);
              if (!effect && recoverPublication) {
                effect = await recoverPublication(root, await publicationCandidate(kv, root, aggregate));
                if (effect) await immutablePage(kv, receiptKey, effect);
              }
              if (!effect) {
                await verifySourceProjects(kv, root);
                const candidate = await publicationCandidate(kv, root, aggregate);
                const evidence = publicationEvidence ? await publicationEvidence(root, candidate) : undefined;
                if (evidence && evidence.status !== "supported" && evidence.status !== "unsupported") {
                  return { completed: false, queued: false, partial: true, retained: true,
                    reason: `consolidation_publication_${evidence.status}`, evidence, manifestId: id,
                    continuation: { consolidationManifestId: id } };
                }
                const rejected = evidence ? evidence.status === "unsupported"
                  : root.kind === "semantic" && !root.legacy && aggregate.sourceSessionIds.length < 2 && (aggregate.confidence ?? 0) < 0.9;
                if (!rejected && publicationEvidence) {
                  await verifySourceProjects(kv, root);
                  for (let index = 0; index < root.refs.length; index++) await verifyCapturedSource(kv, root, index);
                }
                effect = rejected ? { rejected: true, applied: false, created: false } : await publish(root, candidate);
                await immutablePage(kv, receiptKey, effect);
              }
              root.counters.examined++;
              if (effect.rejected) root.counters.rejected++;
              else { root.counters.published++; if (effect.created) root.counters.created++;
                if (effect.applied) { root.counters.applied++; if (effect.created) root.counters.newlyCreated++; } else root.counters.replayed++; }
            }
            root.publish.offset++;
          }
          if (root.publish.offset >= page.candidates.length) root.publish = { page: root.publish.page + 1, offset: 0 };
          await saveRoot(kv, root);
        }
        if (root.publish.page >= root.finalPages) {
          root.phase = "completed";
          root.result = { completed: true, manifestId: root.id, captureId: root.id, sourceCount: root.refs.length,
            selectedSourceIds: root.refs.map(ref => ref.id), fragmentCount: root.fragmentCount, candidateCount: root.candidateCount,
            finalPages: root.finalPages, examined: root.counters.examined, rejected: root.counters.rejected,
            retrievalCoverage: root.retrievalCoverage,
            appliedEffects: root.counters.applied, replayedEffects: root.counters.replayed,
            newFacts: root.kind === "semantic" ? root.counters.newlyCreated : 0, createdFacts: root.kind === "semantic" ? root.counters.created : 0,
            newProcedures: root.kind === "procedural" ? root.counters.newlyCreated : 0, createdProcedures: root.kind === "procedural" ? root.counters.created : 0,
            consolidated: root.kind === "memory" ? root.counters.published : 0,
            coverage: { capturedSources: root.capture.source, verifiedSources: root.verify, extractedPages: root.extract.page,
              candidatePages: root.candidatePages, finalPages: root.finalPages, publishedPages: root.publish.page },
            ...(root.counters.published ? {} : { reason: root.emptyReason ?? (root.eligibleSources.length ? "no_supported_candidates"
              : root.kind === "procedural" ? "no_nonempty_procedural_evidence" : "insufficient_repeated_concept_evidence") }) };
          if (root.typesafeDecision) Object.assign(root.result, {
            selectedSourceIds: root.typesafeDecision.selectedSourceIndexes.map(index => root.refs[index]!.id),
            selectedSources: root.typesafeDecision.selectedSourceIndexes.map(index => ({ id: root.refs[index]!.id,
              sessionId: root.refs[index]!.sessionId, project: root.refs[index]!.project, rowVersion: root.refs[index]!.rowVersion })),
            compactedSourceIds: root.typesafeDecision.droppedSourceIndexes.map(index => root.refs[index]!.id),
            compactedSourceCount: root.typesafeDecision.droppedSourceIndexes.length,
            typeSafeDecision: root.typesafeDecision,
          });
          await saveRoot(kv, root); return root.result;
        }
      } else if (root.phase === "retained") return { completed: false, partial: true, retained: true, error: root.error, manifestId: id };
      return { completed: false, queued: true, manifestId: id, captureId: id, phase: root.phase,
        continuation: root.kind === "memory" ? { consolidationCaptureId: id } : { consolidationManifestId: id } };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (error instanceof ConsolidationRequestBlocked) return { success: false, completed: false, queued: false, partial: true, retained: true,
        error: message, errorCode: error.code, diagnostics: { manifestId: id, requestId: error.requestId, workItemId: error.workItemId, state: error.state,
          ...(error.dispatched === false ? { dispatched: false } : {}) },
        continuation: root.kind === "memory" ? { consolidationCaptureId: id } : { consolidationManifestId: id } };
      if (root.legacy && error && typeof error === "object" && "code" in error && error.code === "CONSOLIDATION_LEGACY_CAPTURE_MISSING") {
        root.phase = "retained"; root.error = message; await saveRoot(kv, root);
        return { completed: false, partial: true, retained: true, error: message, manifestId: id };
      }
      // Keep the last durable cursor. A write may have committed before its caller observed a failure.
      return { completed: false, manifestId: id, captureId: id, queued: consolidationIsPending(error), partial: !consolidationIsPending(error),
        stale: message.includes("source_changed") || message.includes("scope_mismatch"),
        ...(consolidationIsPending(error) ? { reason: "consolidation_continuation" } : { error: message }),
        continuation: root.kind === "memory" ? { consolidationCaptureId: id } : { consolidationManifestId: id } };
    }
  });
}

export async function replacePagedConsolidationRequest(kv: StateKV, queue: FireworksBatchQueue, canReplace: (id: string) => Promise<boolean>,
  input: { manifestId: string; partitionId: string; workItemId: string }): Promise<Record<string, unknown>> {
  if (Object.values(input).some(value => typeof value !== "string" || !value || Buffer.byteLength(value) > 256)) throw new Error("consolidation_replacement_identity_invalid");
  try { return await withKeyedLock(`consolidation-paged:${input.manifestId}`, async () => {
    const root = await loadRoot(kv, input.manifestId);
    if (!root || root.kind === "memory") throw new Error("consolidation_replacement_manifest_mismatch");
    const result = await replaceConsolidationRequest(kv, queue, canReplace, input, root.phase !== "completed" && root.phase !== "retained" && !root.legacy?.completed);
    return { ...result, manifestId: input.manifestId, partitionId: input.partitionId, continuation: { consolidationManifestId: input.manifestId } };
  }); } catch (error) {
    if (!(error instanceof ConsolidationRequestBlocked)) throw error;
    return { success: false, completed: false, queued: false, partial: true, retained: true, error: error.message, errorCode: error.code,
      diagnostics: { manifestId: input.manifestId, requestId: error.requestId, workItemId: error.workItemId, state: error.state,
        ...(error.dispatched === false ? { dispatched: false } : {}) },
      continuation: { consolidationManifestId: input.manifestId } };
  }
}

async function importLegacyCut(kv: StateKV, root: PagedConsolidation): Promise<void> {
  const progress = root.legacy;
  if (!progress) throw new Error("consolidation_legacy_identity_missing");
  if (progress.imported) {
    await recoverLegacyFinalCut(kv, root);
    if (progress.scanComplete) { root.capture.source = root.refs.length; root.phase = "verify"; await saveRoot(kv, root); }
    return;
  }
  const old = await kv.get<LegacyManifest>(KV.state, progress.originalId);
  if (!old || !Array.isArray(old.sources) || !Array.isArray(old.fragments)) throw new Error("consolidation_legacy_capture_missing");
  boundedBytes(old, CONSOLIDATION_SOURCE_BYTES);
  if (progress.source < old.sources.length) {
    const source = old.sources[progress.source]; const ref = root.refs[progress.source];
    const json = JSON.stringify(source);
    const header: SourceHeader = { ...ref, rowVersion: "0", fingerprint: fingerprintId("consource", json), bytes: Buffer.byteLength(json),
      frequency: Number(source.frequency) || 1, eligible: true, conceptCount: 0,
      ...(typeof source.createdAt === "string" ? { createdAt: source.createdAt } : {}),
      ...(ref.kind !== "summary" ? { legacy: { ...(typeof source.fingerprint === "string" ? { observationFingerprint: source.fingerprint } : {}),
        contentFingerprint: fingerprintId("concontent", String(source.content)), domainVersion: Number(source.version), updatedAt: String(source.updatedAt) } } : {}) };
    await immutablePage(kv, pageKey(root.id, "source", progress.source), header);
    progress.source++; await saveRoot(kv, root); return;
  }
  if (progress.fragment < old.fragments.length) {
    const window = old.fragments.slice(progress.fragment, progress.fragment + 64);
    const prompt = (fragments: ConsolidationFragment[]) => buildConsolidationFragmentPrompt(root.kind === "procedural"
      ? fragments.map(fragment => ({ ...fragment, frequency: old.sources.find(source => source.id === fragment.sourceId)?.frequency })) : fragments);
    const fragments = boundedPartitions(window, prompt, root.kind === "semantic" ? SEMANTIC_FRAGMENT_SYSTEM : PROCEDURAL_FRAGMENT_SYSTEM)[0];
    const source = root.refs.findIndex(ref => (ref.kind === "summary" ? ref.sessionId : ref.id) === fragments[0].sourceId);
    if (source < 0) throw new Error("consolidation_legacy_fragment_source_unknown");
    await immutablePage(kv, pageKey(root.id, "fragments", root.fragmentPages), { fragments, source });
    progress.fragment += fragments.length; root.fragmentPages++; root.fragmentCount += fragments.length;
    await saveRoot(kv, root); return;
  }
  if (!old.fragments.length && (progress.generatedSource ?? 0) < old.sources.length) {
    const index = progress.generatedSource ?? 0; const source = old.sources[index]; const offset = progress.generatedOffset ?? 0;
    const fields: Record<string, string> = root.kind === "semantic" ? { title: String(source.title), narrative: String(source.narrative),
      ...Object.fromEntries((source.concepts as string[] ?? []).map((text, position) => [`concepts[${position}]`, text])),
      ...Object.fromEntries((source.keyDecisions as string[] ?? []).map((text, position) => [`keyDecisions[${position}]`, text])),
      ...Object.fromEntries((source.filesModified as string[] ?? []).map((text, position) => [`filesModified[${position}]`, text])) } : { content: String(source.content) };
    const fragments: ConsolidationFragment[] = []; let seen = 0; let done = true;
    for (const fragment of iterateSourceFragments({ id: root.refs[index].id, version: root.kind === "semantic" ? String(source.createdAt) : `${source.version}:${source.updatedAt}`, fields },
      root.kind === "semantic" ? SEMANTIC_FRAGMENT_SYSTEM : PROCEDURAL_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt)) {
      if (seen++ < offset) continue;
      if (fragments.length >= 6) { done = false; break; } fragments.push(fragment);
    }
    if (fragments.length) { await immutablePage(kv, pageKey(root.id, "fragments", root.fragmentPages), { fragments, source: index }); root.fragmentPages++; root.fragmentCount += fragments.length; }
    progress.generatedOffset = done ? 0 : offset + fragments.length; progress.generatedSource = done ? index + 1 : index;
    await saveRoot(kv, root); return;
  }
  progress.imported = true; await saveRoot(kv, root);
}
