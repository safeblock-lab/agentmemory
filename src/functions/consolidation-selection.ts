import type { StateKV } from "../state/kv.js";
import { fingerprintId, KV } from "../state/schema.js";
import type { BatchEffectMetadata, CompressedObservation, EmbeddingProvider, HybridSearchResult, SessionSummary, Session } from "../types.js";
import { getIndexedVector, configuredSemanticProbes } from "../state/indexed-vector.js";
import { IndexedRetrievalError, retrievalRequest } from "../state/indexed-retrieval.js";
import { rerank } from "../state/reranker.js";
import { isStateCounter } from "../state/state-transactions.js";
import type { CanonicalLookup, CanonicalWriteResult } from "../state/canonical-memory.js";
const retrievalFailureActions = new WeakMap<object, string>();
export function consolidationRetrievalFailureAction(error: unknown): string | undefined {
  return error !== null && typeof error === "object" ? retrievalFailureActions.get(error) : undefined;
}
async function selectedRetrieval<T>(kv: StateKV, payload: Record<string, unknown>): Promise<T> {
  try { return await retrievalRequest<T>(kv, payload); }
  catch (error) {
    if (error !== null && typeof error === "object" && typeof payload.action === "string")
      retrievalFailureActions.set(error, payload.action);
    throw error;
  }
}
export type { CanonicalWriteResult } from "../state/canonical-memory.js";

export type ConsolidationRetrievalCoverage = "bounded_candidate_budget" | "bounded_query_budget";
const COVERAGE_ORDER: ConsolidationRetrievalCoverage[] = ["bounded_candidate_budget", "bounded_query_budget"];
export interface ConsolidationSelectedReference { id: string; sessionId: string; rowVersion: string; scope?: string; retrievalCoverage?: ConsolidationRetrievalCoverage[] }
export type ConsolidationSelection = ConsolidationSelectedReference | HybridSearchResult;
export type ConsolidationSelector = (project: string, sessionIds?: string[]) => Promise<ConsolidationSelection[]>;
export interface ConsolidationSummaryReference { sourceKind: "summary"; id: string; sessionId: string; scope: string; project: string;
  rowVersion?: string; sessionVersion?: string; retrievalCoverage?: ConsolidationRetrievalCoverage[] }
export interface ProceduralObservationReference extends ConsolidationSelectedReference { sourceKind: "observation"; project: string }

export function consolidationRetrievalCoverage(sources: readonly unknown[]): ConsolidationRetrievalCoverage[] {
  const labels = new Set<ConsolidationRetrievalCoverage>();
  for (const source of sources) {
    if (!record(source)) throw new Error("consolidation_retrieval_coverage_invalid");
    const value = source.retrievalCoverage;
    if (value === undefined) continue;
    if (!Array.isArray(value) || value.length > 2 || new Set(value).size !== value.length
      || value.some(label => label !== "bounded_candidate_budget" && label !== "bounded_query_budget")) {
      throw new Error("consolidation_retrieval_coverage_invalid");
    }
    for (const label of value) labels.add(label);
  }
  return COVERAGE_ORDER.filter(label => labels.has(label));
}

type ScopedSemanticQueryDiagnostic = { subpath: "scope_resolution" | "small_project" | "small_project_prepare_sql" | "small_project_candidate_read"
  | "vector_prepare_sql" | "scoring" | "enrichment" | "ann_prepare_sql" | "ann_candidate_read";
  small_project_ms: number; prepare_sql_ms: number; candidate_read_ms: number; window: number; page: number;
  visited: number; unique: number; duplicate: number; id_bytes: number;
  fallback_reason: "small_project_disabled" | "small_project_bound" | "small_project_candidate_invalid"
    | "small_project_source_invalid" | "small_project_id_bytes" | "small_project_vector_missing" | null };
type ScopedKeywordQueryDiagnostic = { subpath: "pre_enrichment" | "projection_lookup" | "proof_lookup"
  | "literal_validation" | "decode_validation" | "complete";
  pre_enrichment_ms: number; enrichment_ms: number; projection_lookup_ms: number; proof_lookup_ms: number;
  literal_validation_ms: number; decode_validation_ms: number; hits_started: number; hits_completed: number;
  literal_started: number; recipe1_started: number; full_bytes_validated: number };
type ScopedQueryDiagnostic = { action: "keyword_search" | "semantic_search";
  phase: "candidate_selection" | "scoring" | "enrichment"; elapsedMs: number;
  queryDiagnostics?: ScopedSemanticQueryDiagnostic; keywordDiagnostics?: ScopedKeywordQueryDiagnostic };

const SEMANTIC_QUERY_DIAGNOSTIC_SUBPATHS = new Set<string>(["scope_resolution", "small_project", "small_project_prepare_sql",
  "small_project_candidate_read", "vector_prepare_sql", "scoring", "enrichment", "ann_prepare_sql", "ann_candidate_read"]);
const SEMANTIC_QUERY_DIAGNOSTIC_FALLBACKS = new Set<string>(["small_project_disabled", "small_project_bound", "small_project_candidate_invalid",
  "small_project_source_invalid", "small_project_id_bytes", "small_project_vector_missing"]);
const SEMANTIC_QUERY_DIAGNOSTIC_FIELDS = new Set([
  "subpath", "small_project_ms", "prepare_sql_ms", "candidate_read_ms", "window", "page", "visited", "unique",
  "duplicate", "id_bytes", "fallback_reason",
]);
const KEYWORD_QUERY_DIAGNOSTIC_SUBPATHS = new Set<string>([
  "pre_enrichment", "projection_lookup", "proof_lookup", "literal_validation", "decode_validation", "complete",
]);
const KEYWORD_QUERY_DIAGNOSTIC_FIELDS = new Set([
  "subpath", "pre_enrichment_ms", "enrichment_ms", "projection_lookup_ms", "proof_lookup_ms",
  "literal_validation_ms", "decode_validation_ms", "hits_started", "hits_completed", "literal_started",
  "recipe1_started", "full_bytes_validated",
]);

function isSemanticQueryDiagnosticSubpath(value: unknown): value is ScopedSemanticQueryDiagnostic["subpath"] {
  return typeof value === "string" && SEMANTIC_QUERY_DIAGNOSTIC_SUBPATHS.has(value);
}

function isSemanticQueryDiagnosticFallback(value: unknown): value is Exclude<ScopedSemanticQueryDiagnostic["fallback_reason"], null> {
  return typeof value === "string" && SEMANTIC_QUERY_DIAGNOSTIC_FALLBACKS.has(value);
}

function isKeywordQueryDiagnosticSubpath(value: unknown): value is ScopedKeywordQueryDiagnostic["subpath"] {
  return typeof value === "string" && KEYWORD_QUERY_DIAGNOSTIC_SUBPATHS.has(value);
}

function isNonnegativeSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function scopedSemanticQueryDiagnostic(value: unknown): ScopedSemanticQueryDiagnostic | undefined {
  if (!record(value)) return undefined;
  const fields = Object.keys(value);
  if (fields.length !== SEMANTIC_QUERY_DIAGNOSTIC_FIELDS.size || fields.some(field => !SEMANTIC_QUERY_DIAGNOSTIC_FIELDS.has(field))) {
    return undefined;
  }
  if (!isSemanticQueryDiagnosticSubpath(value.subpath)
    || (value.fallback_reason !== null && !isSemanticQueryDiagnosticFallback(value.fallback_reason))
    || !isNonnegativeSafeInteger(value.small_project_ms) || !isNonnegativeSafeInteger(value.prepare_sql_ms)
    || !isNonnegativeSafeInteger(value.candidate_read_ms) || !isNonnegativeSafeInteger(value.window)
    || !isNonnegativeSafeInteger(value.page) || !isNonnegativeSafeInteger(value.visited)
    || !isNonnegativeSafeInteger(value.unique) || !isNonnegativeSafeInteger(value.duplicate)
    || !isNonnegativeSafeInteger(value.id_bytes)) {
    return undefined;
  }
  return {
    subpath: value.subpath, small_project_ms: value.small_project_ms, prepare_sql_ms: value.prepare_sql_ms,
    candidate_read_ms: value.candidate_read_ms, window: value.window, page: value.page, visited: value.visited,
    unique: value.unique, duplicate: value.duplicate, id_bytes: value.id_bytes, fallback_reason: value.fallback_reason,
  };
}

function scopedKeywordQueryDiagnostic(value: unknown): ScopedKeywordQueryDiagnostic | undefined {
  if (!record(value)) return undefined;
  const fields = Object.keys(value);
  if (fields.length !== KEYWORD_QUERY_DIAGNOSTIC_FIELDS.size || fields.some(field => !KEYWORD_QUERY_DIAGNOSTIC_FIELDS.has(field))) {
    return undefined;
  }
  if (!isKeywordQueryDiagnosticSubpath(value.subpath)
    || !isNonnegativeSafeInteger(value.pre_enrichment_ms) || !isNonnegativeSafeInteger(value.enrichment_ms)
    || !isNonnegativeSafeInteger(value.projection_lookup_ms) || !isNonnegativeSafeInteger(value.proof_lookup_ms)
    || !isNonnegativeSafeInteger(value.literal_validation_ms) || !isNonnegativeSafeInteger(value.decode_validation_ms)
    || !isNonnegativeSafeInteger(value.hits_started) || !isNonnegativeSafeInteger(value.hits_completed)
    || !isNonnegativeSafeInteger(value.literal_started) || !isNonnegativeSafeInteger(value.recipe1_started)
    || !isNonnegativeSafeInteger(value.full_bytes_validated)) {
    return undefined;
  }
  return {
    subpath: value.subpath, pre_enrichment_ms: value.pre_enrichment_ms, enrichment_ms: value.enrichment_ms,
    projection_lookup_ms: value.projection_lookup_ms, proof_lookup_ms: value.proof_lookup_ms,
    literal_validation_ms: value.literal_validation_ms, decode_validation_ms: value.decode_validation_ms,
    hits_started: value.hits_started, hits_completed: value.hits_completed, literal_started: value.literal_started,
    recipe1_started: value.recipe1_started, full_bytes_validated: value.full_bytes_validated,
  };
}

export class ScopedObservationBudgetError extends IndexedRetrievalError {
  readonly diagnostics: { reason: "query_work_budget" | "candidate_budget"; project: string; sessionId?: string; sourceKind: "observation"; limit: 12; queries?: ScopedQueryDiagnostic[] };
  constructor(reason: "query_work_budget" | "candidate_budget", project: string, sessionId?: string, queries?: ScopedQueryDiagnostic[]) {
    super("STATE_RETRIEVAL_RESOURCE_LIMIT", "Scoped observation query exhausted its work budget; selected input remains available for retry.");
    this.diagnostics = { reason, project, ...(sessionId ? { sessionId } : {}), sourceKind: "observation", limit: 12, ...(queries?.length ? { queries } : {}) };
  }
}

export interface ObservationProceduralSource {
  sourceKind: "observation";
  id: string;
  sessionId: string;
  project: string;
  fingerprint: string;
  content: string;
  frequency: number;
  updatedAt: string;
  version: number;
}

export async function selectProceduralObservations(
  _kv: StateKV, project: string, summaries: Array<{ sessionId: string }>, selected: ConsolidationSelection[],
): Promise<ProceduralObservationReference[]> {
  if (!project.trim()) throw new Error("consolidation_project_required");
  if (summaries.length > 12 || selected.length > 12) throw new Error("consolidation_source_partition_required");
  const sessions = new Set(summaries.map(summary => summary.sessionId));
  const observations: ProceduralObservationReference[] = [];
  for (const hit of selected) {
    if (!sessions.has(hit.sessionId)) continue;
    if ("observation" in hit) throw new Error("consolidation_legacy_selection_requires_version");
    if (!hit.id || Buffer.byteLength(hit.id) > 256 || !isStateCounter(hit.rowVersion)
      || (hit.scope !== undefined && hit.scope !== KV.observations(hit.sessionId))) throw new Error("consolidation_observation_reference_invalid");
    if (observations.some(observation => observation.id === hit.id && observation.sessionId === hit.sessionId)) continue;
    if (observations.length >= 12) throw new Error("consolidation_source_partition_required");
    const retrievalCoverage = consolidationRetrievalCoverage([hit]);
    observations.push({ ...hit, sourceKind: "observation", project, scope: KV.observations(hit.sessionId),
      ...(retrievalCoverage.length ? { retrievalCoverage } : {}) });
  }
  return observations;
}

export interface ConsolidationCanonicalMatch { status: "ready"; canonicalId: string | null; version: string; token?: CanonicalLookup }
export type ConsolidationCanonicalLookup = (
  scope: "semantic" | "procedural", normalizedKey: string,
) => Promise<ConsolidationCanonicalMatch
  | { status: "preparing"; continuation: string }>;

export type ConsolidationCanonicalWriter = <T extends BatchEffectMetadata>(
  scope: "semantic" | "procedural", normalizedKey: string, match: ConsolidationCanonicalMatch,
  newId: string, effectKey: string, update: (current: T | null) => T,
) => Promise<CanonicalWriteResult>;

export async function canonicalConsolidationId(
  lookup: ConsolidationCanonicalLookup | undefined, scope: "semantic" | "procedural", key: string,
): Promise<ConsolidationCanonicalMatch> {
  if (!lookup) throw new Error("consolidation_canonical_index_unavailable");
  const result = await lookup(scope, key.toLowerCase());
  if (result.status !== "ready") throw new Error("consolidation_canonical_index_preparing");
  if (!result.version || (result.canonicalId !== null && (!result.canonicalId || Buffer.byteLength(result.canonicalId) > 256))) {
    throw new Error("consolidation_canonical_index_invalid");
  }
  return result;
}

export async function selectConsolidationSummaries(
  _kv: StateKV, project: string, sourceIds: string[] | undefined, select?: ConsolidationSelector,
): Promise<ConsolidationSummaryReference[]> {
  if (!project.trim()) throw new Error("consolidation_project_required");
  const explicit = sourceIds !== undefined;
  const selected = explicit ? [] : await select?.(project) ?? [];
  if (selected.length > 12) throw new Error("consolidation_source_partition_required");
  const ids = sourceIds ?? [...new Set(selected.map(item => item.sessionId))];
  if (!ids.length) throw new Error(explicit ? "selected_consolidation_source_identity_mismatch"
    : select ? "consolidation_no_relevant_sources" : "consolidation_selector_unavailable");
  if (ids.length > 12 || ids.some(id => typeof id !== "string" || !id.trim() || Buffer.byteLength(id) > 256)) {
    throw new Error("consolidation_source_partition_required");
  }
  if (new Set(ids).size !== ids.length) throw new Error("selected_consolidation_source_identity_mismatch");
  const references = await Promise.all(ids.map(async id => {
    const retrievalCoverage = consolidationRetrievalCoverage(selected.filter(item => item.sessionId === id));
    if (!explicit) return { sourceKind: "summary" as const, id, sessionId: id, scope: KV.summaries, project,
      ...(retrievalCoverage.length ? { retrievalCoverage } : {}) };
    if (typeof _kv.getVersioned !== "function") throw new Error("consolidation_versioned_source_required");
    const [session, summary] = await Promise.all([
      _kv.getVersioned<Session>(KV.sessions, id),
      _kv.getVersioned<SessionSummary>(KV.summaries, id),
    ]);
    if (!session.exists || !session.value || !isStateCounter(session.version) || session.version === "0"
      || session.value.id !== id || session.value.project !== project
      || !summary.exists || !summary.value || !isStateCounter(summary.version) || summary.version === "0"
      || summary.value.sessionId !== id || summary.value.project !== project) {
      throw new Error("selected_consolidation_source_identity_mismatch");
    }
    return { sourceKind: "summary" as const, id, sessionId: id, scope: KV.summaries, project,
      rowVersion: summary.version, sessionVersion: session.version };
  }));
  if (Buffer.byteLength(JSON.stringify(references)) > 65_536) throw new Error("consolidation_source_partition_required");
  return references;
}

export async function fingerprintConsolidationSummaries(
  kv: StateKV, project: string, references: readonly ConsolidationSummaryReference[],
): Promise<string> {
  if (typeof kv.getVersioned !== "function") throw new Error("consolidation_versioned_source_required");
  let bytes = 0;
  const sources = await Promise.all(references.map(async reference => {
    const [session, summary] = await Promise.all([
      kv.getVersioned<Session>(KV.sessions, reference.sessionId),
      kv.getVersioned<SessionSummary>(KV.summaries, reference.sessionId),
    ]);
    if (!session.exists || !session.value || session.value.id !== reference.sessionId || session.value.project !== project
      || !isStateCounter(session.version) || session.version === "0") {
      throw new Error("selected_consolidation_source_identity_mismatch");
    }
    if (!summary.exists || !summary.value || summary.value.sessionId !== reference.sessionId || summary.value.project !== project
      || !isStateCounter(summary.version) || summary.version === "0"
      || reference.rowVersion !== undefined && summary.version !== reference.rowVersion
      || reference.sessionVersion !== undefined && session.version !== reference.sessionVersion) {
      throw new Error("selected_consolidation_summary_source_changed");
    }
    const source = { sessionId: reference.sessionId, project, sessionVersion: session.version,
      summaryVersion: summary.version, summary: summary.value };
    bytes += Buffer.byteLength(JSON.stringify(source));
    if (bytes > 65_536) throw new Error("consolidation_source_partition_required");
    return source;
  }));
  return fingerprintId("consummary", JSON.stringify(sources));
}

export function createProjectConsolidationSelector(kv: StateKV, provider: EmbeddingProvider | null): ConsolidationSelector {
  return async (project, sessionIds) => {
    if (!project.trim()) throw new Error("consolidation_project_required");
    const query = `Reusable implementation decisions, workflows, recurring patterns, errors and solutions for ${project}`.slice(0, 256);
    const selected = sessionIds === undefined
      ? await selectIndexedObservations(kv, provider, project, query)
      : await selectIndexedObservationsForSessions(kv, provider, project, query, sessionIds);
    for (const { ref } of selected) {
      if ((await kv.get<Session>(KV.sessions, ref.sessionId))?.project !== project) throw new Error("consolidation_source_scope_mismatch");
    }
    return selected.map(({ ref }) => ({ ...ref, scope: KV.observations(ref.sessionId) }));
  };
}

export function createSkillObservationSelector(kv: StateKV, provider: EmbeddingProvider | null): (project: string, sessionId: string) => Promise<ConsolidationSelectedReference[]> {
  return async (project, sessionId) => {
    if (!provider || !project.trim() || !sessionId.trim()) throw new Error("skill_scoped_retrieval_unavailable");
    const [session, summary] = await Promise.all([kv.get<Session>(KV.sessions, sessionId), kv.get<SessionSummary>(KV.summaries, sessionId)]);
    if (session?.project !== project || summary?.project !== project || summary.sessionId !== sessionId) throw new Error("skill_source_scope_mismatch");
    const query = `${summary.title} ${summary.narrative} reusable workflow and recurring implementation procedure`.slice(0, 256);
    const selected = await selectIndexedObservations(kv, provider, project, query, sessionId);
    if ((await kv.get<Session>(KV.sessions, sessionId))?.project !== project) throw new Error("skill_source_scope_changed");
    return selected.map(item => item.ref);
  };
}

function record(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function boundedLexicalQuery(query: string): string | undefined {
  if (Buffer.byteLength(query) > 65_536) return undefined;
  const terms = [...new Set((query.match(/[\p{L}\p{N}_]+/gu) ?? []).map(term => term.toLowerCase()))].sort();
  if (terms.some(term => Buffer.byteLength(term) > 256)) return undefined;
  const bounded: string[] = [];
  for (const term of terms) {
    if (bounded.length >= 32) break;
    const candidate = [...bounded, term].join(" ");
    if (Buffer.byteLength(candidate) > 256) break;
    bounded.push(term);
  }
  return bounded.length ? bounded.join(" ") : undefined;
}

async function validateSelectedObservationIndex(kv: StateKV,
  identity: { index_id: string; model: string; dimensions: number; generation: string }): Promise<void> {
  const [status, marker] = await Promise.all([
    selectedRetrieval<unknown>(kv, { action: "index_status", index_id: identity.index_id, metadata_only: true }),
    kv.get<unknown>(KV.config, "indexed-corpus"),
  ]);
  if (!record(status) || status.version !== 1 || status.metadata_only !== true || status.index_id !== identity.index_id
    || !Array.isArray(status.semantic) || status.semantic.length !== 1
    || !record(status.semantic[0]) || status.semantic[0].index_id !== identity.index_id) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_RESPONSE", "Invalid selected observation index metadata response.");
  }
  const selected = status.semantic[0];
  if (!Array.isArray(status.capabilities) || !status.capabilities.includes("state::semantic_lsh_v1")
    || !record(selected) || selected.status !== "ready" || selected.lexical_ready !== true || selected.source_kind !== "agentmemory"
    || !record(marker) || !["ready", "pending"].includes(String(marker.status))
    || [selected, marker].some(item => item.model !== identity.model || item.dimensions !== identity.dimensions || item.generation !== identity.generation)) {
    throw new IndexedRetrievalError("STATE_INDEX_NOT_READY", "Selected observation index configuration or embedding identity is not ready.");
  }
}

export class SelectedObservationPreparationPending extends IndexedRetrievalError {
  constructor(readonly diagnostics: { project: string; reason: string; cursor: string; coverageEpoch: string; projectGeneration: string; processed: number }) {
    super("STATE_INDEX_NOT_READY", "Selected observation preparation awaits its native source dependency.");
  }
}

export async function prepareSelectedObservations(kv: StateKV,
  identity: { index_id: string; model: string; dimensions: number; generation: string }, project: string): Promise<void> {
  await validateSelectedObservationIndex(kv, identity);
  const prepared = await selectedRetrieval<unknown>(kv, { action: "project_vectors_prepare", index_id: identity.index_id,
    project, source_kind: "observation", max_rows: 16, max_bytes: 65_536 });
  if (!record(prepared) || !["ready", "pending", "partial"].includes(String(prepared.status)) || prepared.index_id !== identity.index_id
    || prepared.project !== project || prepared.source_kind !== "observation" || typeof prepared.has_more !== "boolean"
    || prepared.has_more !== (prepared.status !== "ready") || !isStateCounter(prepared.coverage_epoch) || !isStateCounter(prepared.project_generation)
    || typeof prepared.cursor !== "string" || Buffer.byteLength(prepared.cursor) > 2048
    || !Number.isSafeInteger(prepared.processed) || Number(prepared.processed) < 0 || Number(prepared.processed) > 16
    || !Number.isSafeInteger(prepared.session_visits) || Number(prepared.session_visits) < 0 || Number(prepared.session_visits) > 16
    || !Number.isSafeInteger(prepared.text_bytes) || Number(prepared.text_bytes) < 0 || Number(prepared.text_bytes) > 1_048_576
    || (prepared.status === "ready" ? prepared.reason !== null
      : !["catalog_not_ready", "project_source_not_ready", "project_projection_pending"].includes(String(prepared.reason)))) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_RESPONSE", "Invalid selected observation preparation response.");
  }
  if (prepared.status !== "ready") throw new SelectedObservationPreparationPending({ project, reason: String(prepared.reason),
    cursor: prepared.cursor, coverageEpoch: prepared.coverage_epoch, projectGeneration: prepared.project_generation, processed: Number(prepared.processed) });
}

async function searchIndexedObservations(kv: StateKV, identity: { index_id: string; model: string; dimensions: number; generation: string },
  project: string, query: string, embedding: string, sessionId?: string, contextCoverage?: { partial: boolean }) {
    const base = { ...identity, project, source_kind: "observation", include_text: true, limit: 12,
      ...(sessionId ? { session_id: sessionId, source_scope: KV.observations(sessionId) } : {}) };
    const lexicalQuery = boundedLexicalQuery(query);
    const streams = await Promise.all([
      ...(lexicalQuery ? [{ kind: "lexical" as const,
        request: selectedRetrieval<unknown>(kv, { action: "keyword_search", ...base, query: lexicalQuery }) }] : []),
      { kind: "semantic" as const,
        request: selectedRetrieval<unknown>(kv, { action: "semantic_search", ...base, embedding, max_candidates: 4096, probes: configuredSemanticProbes() }) },
    ].map(async stream => ({ kind: stream.kind, response: await stream.request })));
    const hits = new Map<string, { bm25Score: number; vectorScore: number; lexicalRank: number; vectorRank: number;
      text: string; rowVersion: string; sessionId: string }>();
    const coverage = new Set<ConsolidationRetrievalCoverage>();
    const queryDiagnostics: ScopedQueryDiagnostic[] = [];
    let queryBudgetAction: string | undefined;
    let candidateBudgetAction: string | undefined;
    for (const { kind, response } of streams) {
      if (!response || typeof response !== "object") throw new Error("skill_scoped_retrieval_invalid");
      const raw = response as Record<string, unknown>;
      if (raw.model !== identity.model || raw.dimensions !== identity.dimensions || raw.generation !== identity.generation
        || !Array.isArray(raw.items) || raw.items.length > 12 || Buffer.byteLength(JSON.stringify(raw)) > 65_536) throw new Error("skill_scoped_retrieval_invalid");
      if (kind === "semantic" && (typeof raw.approximate !== "boolean" || typeof raw.budget_exhausted !== "boolean")) throw new Error("skill_scoped_retrieval_invalid");
      if (raw.status !== "ready" && raw.status !== "partial") throw new Error("skill_scoped_retrieval_invalid");
      if (raw.status === "partial") {
        if (contextCoverage) contextCoverage.partial = true;
        if ((raw.reason === "query_work_budget" || raw.reason === "candidate_budget") && raw.budget_exhausted === true) {
          coverage.add(raw.reason === "candidate_budget" ? "bounded_candidate_budget" : "bounded_query_budget");
          const action = kind === "lexical" ? "keyword_search" : "semantic_search";
          if (raw.reason === "query_work_budget" && raw.query_phase !== undefined) {
            if (!["candidate_selection", "scoring", "enrichment"].includes(String(raw.query_phase))
              || !Number.isSafeInteger(raw.query_elapsed_ms) || Number(raw.query_elapsed_ms) < 0
              || Number(raw.query_elapsed_ms) > 300_000) throw new Error("skill_scoped_retrieval_invalid");
            const semanticDiagnostic = kind === "semantic" ? scopedSemanticQueryDiagnostic(raw.query_diagnostics) : undefined;
            const keywordDiagnostic = kind === "lexical" && raw.keyword_diagnostics !== undefined
              ? scopedKeywordQueryDiagnostic(raw.keyword_diagnostics) : undefined;
            queryDiagnostics.push({ action, phase: raw.query_phase as ScopedQueryDiagnostic["phase"], elapsedMs: Number(raw.query_elapsed_ms),
              ...(semanticDiagnostic ? { queryDiagnostics: semanticDiagnostic } : {}),
              ...(keywordDiagnostic ? { keywordDiagnostics: keywordDiagnostic } : {}) });
          }
          if (raw.reason === "query_work_budget") queryBudgetAction = queryBudgetAction ? `${queryBudgetAction},${action}` : action;
          else candidateBudgetAction = candidateBudgetAction ? `${candidateBudgetAction},${action}` : action;
        } else if (contextCoverage && ["catalog_not_ready", "project_mapping_not_ready", "project_source_not_ready",
          "project_projection_pending", "project_index_not_ready"].includes(String(raw.reason))) {
          // Scoped native hits still validate current source versions while coverage is incomplete.
        } else throw new IndexedRetrievalError("STATE_INDEX_NOT_READY", "Scoped observation index coverage is incomplete; selection remains pending.");
      }
      const streamIds = new Set<string>();
      for (const [rank, item] of raw.items.entries()) {
        if (!item || typeof item !== "object" || typeof item.obsId !== "string" || !item.obsId
          || typeof item.sessionId !== "string" || !item.sessionId || (sessionId && item.sessionId !== sessionId)
          || !Number.isFinite(item.score) || typeof item.text !== "string" || Buffer.byteLength(item.text) > 2048
          || item.source_scope !== KV.observations(item.sessionId) || !isStateCounter(item.source_version)) throw new Error("skill_scoped_retrieval_invalid");
        if (streamIds.has(item.obsId)) throw new Error("skill_scoped_retrieval_invalid");
        streamIds.add(item.obsId);
        const previous = hits.get(item.obsId) ?? { bm25Score: 0, vectorScore: 0, lexicalRank: 0, vectorRank: 0,
          text: item.text, rowVersion: item.source_version, sessionId: item.sessionId };
        if (previous.rowVersion !== item.source_version || previous.text !== item.text || previous.sessionId !== item.sessionId) throw new Error("skill_source_changed");
        const normalizedRank = (raw.items.length - rank) / raw.items.length;
        if (kind === "lexical") { previous.bm25Score = item.score; previous.lexicalRank = normalizedRank; }
        else { previous.vectorScore = item.score; previous.vectorRank = normalizedRank; }
        hits.set(item.obsId, previous);
      }
    }
    if (!hits.size && coverage.size && !contextCoverage) {
      const error = new ScopedObservationBudgetError(
        coverage.has("bounded_query_budget") ? "query_work_budget" : "candidate_budget", project, sessionId, queryDiagnostics);
      const action = queryBudgetAction ?? candidateBudgetAction;
      if (action) retrievalFailureActions.set(error, action);
      throw error;
    }
    const candidates: HybridSearchResult[] = [];
    let totalBytes = 0;
    for (const [id, scores] of hits) {
      const observation: CompressedObservation = { id, sessionId: scores.sessionId, timestamp: "", title: "", narrative: scores.text,
        type: "other", facts: [], concepts: [], files: [], importance: 0 };
      totalBytes += Buffer.byteLength(JSON.stringify(observation));
      if (totalBytes > 65_536) throw new Error("skill_source_selection_too_large");
      const overlap = scores.lexicalRank > 0 && scores.vectorRank > 0;
      candidates.push({ observation, ...scores, graphScore: 0,
        combinedScore: (scores.lexicalRank + scores.vectorRank) * (overlap ? 1.05 : 1) });
    }
    if (candidates.some(candidate => !Number.isFinite(candidate.combinedScore))) throw new Error("skill_scoped_retrieval_invalid");
    candidates.sort((left, right) => right.combinedScore - left.combinedScore
      || (left.observation.id < right.observation.id ? -1 : left.observation.id > right.observation.id ? 1 : 0));
    const ranked = await rerank(query, candidates, candidates.length);
    if (ranked.length !== candidates.length || new Set(ranked.map(hit => hit.observation.id)).size !== candidates.length
      || ranked.some(hit => !hits.has(hit.observation.id) || hits.get(hit.observation.id)!.sessionId !== hit.sessionId || !Number.isFinite(hit.combinedScore))) throw new Error("skill_rerank_invalid");
    const retrievalCoverage = COVERAGE_ORDER.filter(label => coverage.has(label));
    return ranked.slice(0, 12).map(result => ({ result,
      ref: { id: result.observation.id, sessionId: result.sessionId, rowVersion: hits.get(result.observation.id)!.rowVersion,
        ...(retrievalCoverage.length ? { retrievalCoverage } : {}) } }));
}

async function prepareObservationEmbedding(provider: EmbeddingProvider, query: string): Promise<string> {
  const embedding = await provider.embed(query);
  if (embedding.length !== provider.dimensions || embedding.some(value => !Number.isFinite(value))) throw new Error("skill_embedding_invalid");
  const bytes = Buffer.alloc(embedding.length * 4);
  embedding.forEach((value, index) => bytes.writeFloatLE(value, index * 4));
  return bytes.toString("base64");
}

export async function selectIndexedObservations(kv: StateKV, provider: EmbeddingProvider | null, project: string, query: string, sessionId?: string) {
  if (!provider || !project.trim() || Buffer.byteLength(project) > 256) throw new Error("scoped_observation_retrieval_unavailable");
  const vector = getIndexedVector(kv, provider);
  await prepareSelectedObservations(kv, vector.identity, project);
  const embedding = await prepareObservationEmbedding(provider, query);
  return searchIndexedObservations(kv, vector.identity, project, query, embedding, sessionId);
}

export async function selectIndexedContextObservations(kv: StateKV, provider: EmbeddingProvider | null, project: string, query: string) {
  if (!provider || !project.trim() || Buffer.byteLength(project) > 256) throw new Error("scoped_observation_retrieval_unavailable");
  const vector = getIndexedVector(kv, provider);
  await validateSelectedObservationIndex(kv, vector.identity);
  const embedding = await prepareObservationEmbedding(provider, query);
  const coverage = { partial: false };
  const observations = await searchIndexedObservations(kv, vector.identity, project, query, embedding, undefined, coverage);
  return { observations, partial: coverage.partial };
}

export async function selectIndexedObservationsForSessions(kv: StateKV, provider: EmbeddingProvider | null, project: string,
  query: string, sessionIds: string[]) {
  if (!provider || !project.trim() || Buffer.byteLength(project) > 256 || !Array.isArray(sessionIds)
    || sessionIds.length < 1 || sessionIds.length > 12 || new Set(sessionIds).size !== sessionIds.length
    || sessionIds.some(id => typeof id !== "string" || !id.trim() || Buffer.byteLength(id) > 256)
    || Buffer.byteLength(JSON.stringify(sessionIds)) > 65_536) throw new Error("scoped_observation_retrieval_unavailable");
  const vector = getIndexedVector(kv, provider);
  await prepareSelectedObservations(kv, vector.identity, project);
  const embedding = await prepareObservationEmbedding(provider, query);
  const perSession: Array<Array<{ result: HybridSearchResult; ref: ConsolidationSelectedReference }>> = [];
  for (const sessionId of sessionIds) {
    perSession.push(await searchIndexedObservations(kv, vector.identity, project, query, embedding, sessionId));
  }
  const selected: Array<{ result: HybridSearchResult; ref: ConsolidationSelectedReference }> = [];
  for (let rank = 0; rank < 12 && selected.length < 12; rank++) {
    for (const items of perSession) {
      if (items[rank]) selected.push(items[rank]!);
      if (selected.length >= 12) break;
    }
  }
  if (Buffer.byteLength(JSON.stringify(selected.map(item => item.ref))) > 65_536) throw new Error("skill_source_selection_too_large");
  return selected;
}
