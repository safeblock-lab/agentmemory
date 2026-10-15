import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import type { EmbeddingProvider, SessionSummary, CompressedObservation, Memory, HybridSearchResult } from "../types.js";
import { readNativeIndexStatus, retrievalRequest, IndexedRetrievalError } from "../state/indexed-retrieval.js";
import { rerank } from "../state/reranker.js";
import { memoryToObservation } from "../state/memory-utils.js";
import { REFLECT_SYSTEM } from "../prompts/reflect.js";
import { stripPrivateData } from "./privacy.js";
import { REFLECT_MAX_SOURCES, type ReflectJob, type ReflectManifest, type ReflectSource, type ConceptCluster, type ReflectEmptyEvidence } from "./reflect-queue-types.js";
import { REFLECT_CONTROL } from "./reflect-queue-types.js";
import { createNextGraphWorkUnit } from "./graph-work-groups.js";

export class ReflectWaiting extends Error {
  constructor(readonly status: "waiting_index" | "waiting_graph" | "waiting_scope" | "waiting_budget" | "blocked" | "failed", code: string) { super(code); }
}
export class ReflectNoEvidence extends ReflectWaiting {
  constructor(readonly binding: ReflectEmptyEvidence) { super("waiting_index", "reflect_no_evidence"); }
}
export const reflectFingerprint = (value: unknown): string => fingerprintId("reflectsource", JSON.stringify(value));
type SourceValue = Record<string, unknown>;
interface ProjectedSource { scope: string; key: string; version: string; fingerprint: string; value: SourceValue;
  provenance?: Array<{ session_id: string; project: string }>; next_cursor?: string }
async function projectedSource(kv: StateKV, scope: string, key: string, project: string, expectedVersion?: string, maxBytes = 65536): Promise<ProjectedSource | null> {
  try {
    const result = await retrievalRequest<{ items: ProjectedSource[]; deferred: Array<{ scope: string; key: string; reason: string }> }>(kv, {
      action: "reflect_sources", project, sources: [{ scope, key, ...(expectedVersion ? { expected_version: expectedVersion } : {}) }], max_items: 1, max_bytes: maxBytes,
    });
    if (!Array.isArray(result.items) || result.items.length > 1 || !Array.isArray(result.deferred) ||
      Buffer.byteLength(JSON.stringify(result)) > 65536) throw new Error("invalid_reflect_projection");
    const deferred = result.deferred.find(item => item.scope === scope && item.key === key);
    if (deferred?.reason === "oversized") throw new ReflectWaiting("waiting_budget", "source_requires_partition");
    const item = result.items[0];
    if (!item) return null;
    if (item.scope !== scope || item.key !== key || typeof item.version !== "string" || typeof item.fingerprint !== "string" ||
      !item.value || typeof item.value !== "object" || expectedVersion && item.version !== expectedVersion) throw new Error("invalid_reflect_projection");
    return item;
  } catch (error) {
    if (error instanceof ReflectWaiting) throw error;
    if (error instanceof IndexedRetrievalError) throw new ReflectWaiting("waiting_index", error.code);
    if (error && typeof error === "object" && "code" in error) throw new ReflectWaiting("waiting_index", "bounded_source_projection_unavailable");
    throw error;
  }
}

export async function legacyReflectEvidence(kv: StateKV, cluster: ConceptCluster, project?: string) {
  const ids = [...cluster.factIds, ...cluster.lessonIds, ...cluster.crystalIds];
  if (!project || ids.length < 2 || ids.length > 12 || new Set(ids).size !== ids.length
    || cluster.memoryIds?.length || cluster.observationIds?.length) return undefined;
  const groups: SourceValue[][] = [];
  const rows: ProjectedSource[] = [];
  for (const [scope, keys] of [[KV.semantic, cluster.factIds], [KV.lessons, cluster.lessonIds], [KV.crystals, cluster.crystalIds]] as const) {
    const values: SourceValue[] = [];
    for (const id of keys) {
      const row = await projectedSource(kv, scope, id, project);
      if (!row || row.value.id !== id || row.value.deleted || row.value.project !== undefined && row.value.project !== project) return undefined;
      rows.push(row); values.push(row.value);
      if (Buffer.byteLength(JSON.stringify(rows)) > 65536) return undefined;
    }
    groups.push(values);
  }
  const fingerprint = fingerprintId("fwbreflect", JSON.stringify({ concepts: cluster.concepts,
    facts: groups[0].map(fact => [fact.id, fact.fact, fact.confidence, fact.updatedAt]).sort(),
    lessons: groups[1].map(lesson => [lesson.id, lesson.content, lesson.confidence, lesson.updatedAt]).sort(),
    crystals: groups[2].map(crystal => [crystal.id, crystal.narrative, crystal.createdAt]).sort() }));
  return { fingerprint, rows };
}
export async function legacyReflectSourceFingerprint(kv: StateKV, cluster: ConceptCluster, project?: string): Promise<string | undefined> {
  return (await legacyReflectEvidence(kv, cluster, project))?.fingerprint;
}
export async function currentLegacyReflectEvidence(kv: StateKV, evidence: NonNullable<Awaited<ReturnType<typeof legacyReflectEvidence>>>, project: string) {
  for (const source of evidence.rows) {
    const row = await projectedSource(kv, source.scope, source.key, project, source.version);
    if (!row || row.version !== source.version || row.fingerprint !== source.fingerprint) return false;
  }
  return true;
}

export async function freezeReflectSeeds(kv: StateKV, job: ReflectJob): Promise<void> {
  if (job.legacy) { job.seeds = [job.legacy.concepts.join(", ").slice(0, 256) || "Related memories"]; return; }
  if (job.seeds.length) {
    if (!job.project) throw new ReflectWaiting("waiting_scope", "project_required_for_reflection");
    return;
  }
  const summaryId = job.request.summaryId ?? job.request.sessionId;
  if (!job.project) throw new ReflectWaiting("waiting_scope", "project_required_for_reflection");
  if (summaryId) {
    const row = await projectedSource(kv, KV.summaries, summaryId, job.project);
    if (!row) throw new ReflectWaiting("waiting_index", "summary_not_ready");
    const summary = row.value as unknown as SessionSummary;
    if (summary.project !== job.project) throw new ReflectWaiting("waiting_scope", "summary_project_mismatch");
    job.seedBinding = { scope: KV.summaries, id: summaryId, version: row.version, fingerprint: row.fingerprint };
    job.seeds = [...new Set(summary.concepts)].slice(0, job.request.maxClusters ?? 4).map(seed => seed.slice(0, 256));
    if (!job.seeds.length) job.seeds = [summary.title.slice(0, 256)];
  } else if (job.request.query) {
    job.seeds = [job.request.query];
  } else {
    const result = await retrievalRequest<{ status: string; items: Array<{ value?: { type?: string; name?: string; stale?: boolean }; type?: string; name?: string; stale?: boolean }> }>(kv,
      { action: "graph_read", project: job.project, mode: "concept_seeds", top_n: Math.min(16, job.request.maxClusters ?? 4), limit: 16, max_bytes: 65536 });
    if (result.status !== "ready" || !Array.isArray(result.items) || result.items.length > 16 || Buffer.byteLength(JSON.stringify(result)) > 65536) throw new ReflectWaiting("waiting_graph", "graph_seeds_not_ready");
    const nodes = result.items.map(item => item.value ?? item);
    if (nodes.some(node => node.type !== "concept" || node.stale !== false || typeof node.name !== "string" || !node.name.trim()))
      throw new Error("invalid_graph_concept_seeds");
    job.seeds = nodes.map(node => node.name!.slice(0, 256));

  }
  if (!job.project) throw new ReflectWaiting("waiting_scope", "project_required_for_reflection");
}

function convertSource(row: ProjectedSource, kind: ReflectSource["kind"]): ReflectSource | null {
  const { key: id, scope } = row;
  const value = row.value;
  let observation: CompressedObservation;
  if (kind === "memory") observation = memoryToObservation(value as unknown as Memory);
  else if (kind === "observation") observation = value as unknown as CompressedObservation;
  else {
    const text = kind === "fact" ? value.fact : kind === "lesson" ? value.content : value.narrative;
    if (typeof text !== "string") return null;
    observation = { id, sessionId: "reflection", timestamp: String(value.updatedAt ?? value.createdAt), type: "decision",
      title: text.slice(0, 120), narrative: text, facts: [text], concepts: [], files: [], importance: Number(value.confidence ?? 0.5) };
  }
  if (typeof observation.title !== "string" || typeof observation.narrative !== "string") return null;
  return { id, scope, kind, version: row.version, fingerprint: row.fingerprint, observation,
    order: 0, score: 0, bm25Score: 0, vectorScore: 0 };
}

async function source(kv: StateKV, scope: string, id: string, kind: ReflectSource["kind"], project: string, maxBytes = 65536): Promise<ReflectSource | null> {
  const row = await projectedSource(kv, scope, id, project, undefined, maxBytes);
  return row ? convertSource(row, kind) : null;
}

export async function prepareReflectGroup(kv: StateKV, job: ReflectJob): Promise<boolean> {
  if (job.legacy) {
    if (job.sourceCursor) return true;
    if (job.sourcePage?.length && (job.sourcePage.length >= 12 || job.legacy.offset >= job.legacy.refs.length)) return true;
    const ref = job.legacy.refs[job.legacy.offset];
    if (!ref) return true;
    const result = await retrievalRequest<{ items: ProjectedSource[]; deferred: Array<{ reason: string }> }>(kv, {
      action: "shared_source_projection", sources: [{ scope: ref.scope, key: ref.key,
        ...(job.legacy.provenanceCursor ? { provenance_cursor: job.legacy.provenanceCursor, expected_version: job.legacy.provenanceVersion } : {}) }], max_items: 1, max_bytes: 65536,
    });
    if (!Array.isArray(result.items) || result.items.length > 1 || Buffer.byteLength(JSON.stringify(result)) > 65536) throw new Error("invalid_shared_projection");
    const row = result.items[0];
    if (!row || row.scope !== ref.scope || row.key !== ref.key) throw new ReflectWaiting("waiting_scope", result.deferred?.[0]?.reason ?? "shared_source_unavailable");
    if (job.legacy.provenanceCursor && row.version !== job.legacy.provenanceVersion) throw new ReflectWaiting("waiting_index", "reflect_source_changed");
    if (!Array.isArray(row.provenance) || row.provenance.length > 12 || row.provenance.some(p => !p.session_id || !p.project)) throw new ReflectWaiting("waiting_scope", "shared_provenance_unavailable");
    const receipt = `${job.id}:provenance:${job.legacy.offset}:${job.legacy.provenanceSequence}`;
    await kv.set(REFLECT_CONTROL, receipt, { scope: row.scope, key: row.key, version: row.version,
      fingerprint: row.fingerprint, provenance: row.provenance, previous: job.legacy.provenanceFingerprint });
    job.legacy.provenanceFingerprint = reflectFingerprint([job.legacy.provenanceFingerprint, receipt, row.fingerprint, row.provenance]);
    job.legacy.provenanceSequence++;
    if (row.next_cursor) { job.legacy.provenanceCursor = row.next_cursor; job.legacy.provenanceVersion = row.version; return false; }
    const item = convertSource(row, ref.kind);
    if (!item) throw new ReflectWaiting("waiting_scope", "shared_source_invalid");
    item.provenanceReceipt = receipt;
    const page = [...(job.sourcePage ?? []), item];
    if (Buffer.byteLength(JSON.stringify(page)) > 65536) {
      if (!job.sourcePage?.length) throw new ReflectWaiting("waiting_budget", "source_requires_partition");
      return true;
    }
    job.sourcePage = page; job.legacy.offset++; job.legacy.provenanceCursor = undefined; job.legacy.provenanceVersion = undefined;
    return page.length >= 12 || job.legacy.offset >= job.legacy.refs.length;
  }
  if (job.project && !job.global) return true;
  job.global ??= { projects: [], ordinal: 0, hasMore: true, partial: false, initialized: false };
  const global = job.global;
  if (job.project) return true;
  if (global.ordinal < global.projects.length) {
    const project = global.projects[global.ordinal];
    if (await kv.get(REFLECT_CONTROL, `${job.id}:project:${reflectFingerprint(project)}`)) { global.ordinal++; return false; }
    job.project = project; return true;
  }
  if (global.initialized && !global.hasMore) {
    if (global.partial) {
      global.initialized = false; global.cursor = undefined; global.projects = []; global.ordinal = 0; global.partial = false;
      throw new ReflectWaiting("waiting_index", "project_catalog_incomplete");
    }
    return true;
  }
  const page = await retrievalRequest<{ status: string; items: Array<{ project: string }>; next_cursor?: string; has_more: boolean }>(kv,
    { action: "project_catalog_page", cursor: global.cursor, limit: 16, max_bytes: 65536 });
  if (!Array.isArray(page.items) || page.items.length > 16 || page.items.some(item => !item.project) || Buffer.byteLength(JSON.stringify(page)) > 65536) throw new Error("invalid_project_catalog_page");
  global.projects = page.items.map(item => item.project); global.ordinal = 0; global.cursor = page.next_cursor;
  global.hasMore = page.has_more; global.partial ||= page.status !== "ready"; global.initialized = true;
  if (!global.projects.length && global.partial) throw new ReflectWaiting("waiting_index", "project_catalog_incomplete");
  return false;
}

function groupedManifest(job: ReflectJob, sources: ReflectSource[], identity: ReflectManifest["indexIdentity"], graphGeneration: string, deferredFamilies: string[]): ReflectManifest {
  const next = createNextGraphWorkUnit({ audience: job.legacy ? { scope: "global-orchestration" } : { scope: "project", project: job.project! },
    sources: sources.map(item => ({ id: item.id, version: item.version,
      content: stripPrivateData(item.observation.narrative) })), systemPrompt: REFLECT_SYSTEM,
    envelopePrefix: `Synthesize insights for ${job.seeds[job.ordinal] ?? job.seeds[0]}. Source fragments retain exact source identity and byte ranges.\n`, envelopeSuffix: "", cursor: job.sourceCursor });
  if (!next.unit) throw new ReflectWaiting("waiting_index", "insufficient_indexed_evidence");
  const selected = sources.filter(source => next.unit!.fragments.some(fragment => fragment.sourceId === source.id));
  const cluster: ConceptCluster = { concepts: [job.seeds[job.ordinal] ?? job.seeds[0]], facts: [], lessons: [], crystalNarratives: [],
    factIds: [], lessonIds: [], crystalIds: [], memoryIds: [], observationIds: [], jobId: job.id, unitId: next.unit.id };
  for (const item of selected) {
    if (item.kind === "fact") cluster.factIds.push(item.id);
    else if (item.kind === "lesson") cluster.lessonIds.push(item.id);
    else if (item.kind === "crystal") cluster.crystalIds.push(item.id);
    else if (item.kind === "memory") cluster.memoryIds!.push(item.id);
    else cluster.observationIds!.push(item.id);
  }
  return { id: reflectFingerprint([next.unit.fingerprint, sources.map(s => [s.scope, s.id, s.version, s.fingerprint, s.provenanceReceipt]), identity, graphGeneration]),
    cluster, sources: selected, indexIdentity: identity, graphGeneration, deferredFamilies, prompt: next.unit.userPrompt,
    nextCursor: next.cursor, grouped: true };
}

export async function retrieveReflectManifest(kv: StateKV, job: ReflectJob, embedding?: EmbeddingProvider): Promise<ReflectManifest> {
  if (job.sourcePage?.length) return groupedManifest(job, job.sourcePage,
    job.sourcePageBinding?.identity ?? { index_id: "direct", model: "direct", dimensions: 0, generation: "direct" },
    job.sourcePageBinding?.graphGeneration ?? "direct", job.sourcePageBinding?.deferredFamilies ?? []);
  if (!embedding) throw new ReflectWaiting("waiting_index", "local_embedding_required");
  const ready = await readNativeIndexStatus(kv, embedding);
  if (!ready?.graphReady) throw new ReflectWaiting("waiting_graph", "graph_index_not_ready");
  if (!ready.ready) throw new ReflectWaiting("waiting_index", "semantic_index_not_ready");
  const identity = { index_id: ready.identity.indexId, model: ready.identity.model, dimensions: ready.identity.dimensions, generation: ready.identity.generation };
  const query = job.seeds[job.ordinal];
  const vector = await embedding.embed(query);
  if (vector.length !== identity.dimensions || Array.from(vector).some(value => !Number.isFinite(value))) throw new Error("invalid_reflect_embedding");
  const bytes = Buffer.alloc(vector.length * 4);
  vector.forEach((value, index) => bytes.writeFloatLE(value, index * 4));
  type Candidate = { obsId: string; sessionId: string; score: number };
  type Candidates = { items: Candidate[]; model: string; dimensions: number; generation: string;
    status?: string; partial?: boolean; has_more?: boolean; next_cursor?: string; reason?: string;
    budget_exhausted?: boolean };
  const [lexical, semantic] = await Promise.all([
    retrievalRequest<Candidates>(kv, { action: "keyword_search", ...identity, project: job.project, query, limit: 12 }),
    retrievalRequest<Candidates>(kv, { action: "semantic_search", ...identity, project: job.project, embedding: bytes.toString("base64"), limit: 12, max_candidates: 512, probes: 32 }),
  ]);
  for (const result of [lexical, semantic]) {
    if (!Array.isArray(result.items) || result.items.length > 12 || result.model !== identity.model || result.dimensions !== identity.dimensions || result.generation !== identity.generation ||
      result.items.some(item => typeof item.obsId !== "string" || !Number.isFinite(item.score))) throw new Error("invalid_reflect_candidates");
    if (result.status === "failed" || result.status === "error") throw new Error("reflect_retrieval_failed");
  }
  if (!lexical.items.length && !semantic.items.length &&
    ![job.request.factIds, job.request.lessonIds, job.request.crystalIds].some(ids => ids?.length)) {
    job.sourcePage = [];
    job.sourcePageBinding = undefined;
    const authoritative = [lexical, semantic].every(result => result.status === "ready" &&
      result.partial !== true && result.has_more !== true && !result.next_cursor && !result.reason && result.budget_exhausted !== true);
    if (authoritative) throw new ReflectNoEvidence({ project: job.project!, seed: query, ordinal: job.ordinal,
      seedBinding: job.seedBinding, indexIdentity: identity, graphGeneration: JSON.stringify(ready.graph) });
    throw new ReflectWaiting("waiting_index", "reflect_preparation_pending");
  }
  const candidates = new Map<string, ReflectSource>();
  let retainedBytes = 2;
  const direct = [[job.request.factIds, KV.semantic, "fact"], [job.request.lessonIds, KV.lessons, "lesson"],
    [job.request.crystalIds, KV.crystals, "crystal"]] as const;
  for (const [ids, scope, kind] of direct) for (const id of ids ?? []) {
    if (retainedBytes > 64512) throw new ReflectWaiting("waiting_budget", "direct_sources_require_partition");
    const item = await source(kv, scope, id, kind, job.project!, 65536 - retainedBytes);
    if (!item) throw new ReflectWaiting("waiting_index", "requested_source_unavailable");
    retainedBytes += Buffer.byteLength(JSON.stringify(item)) + 1;
    if (retainedBytes > 65536) throw new ReflectWaiting("waiting_budget", "direct_sources_require_partition");
    item.score = 2; candidates.set(id, item);
  }
  for (const id of new Set([...lexical.items, ...semantic.items].map(item => item.obsId))) {
    if (candidates.size >= 12 || retainedBytes > 64512) break;
    const ref = await kv.get<{ sourceScope?: string; sourceVersion?: string }>(KV.semanticVectors(identity.index_id), id);
    const scope = ref?.sourceScope;
    if (!scope || (scope !== KV.memories && !scope.startsWith("mem:obs:"))) continue;
    const item = await source(kv, scope, id, scope === KV.memories ? "memory" : "observation", job.project!, 65536 - retainedBytes);
    if (item && item.version === ref.sourceVersion) {
      const itemBytes = Buffer.byteLength(JSON.stringify(item)) + 1;
      if (retainedBytes + itemBytes > 65536) break;
      candidates.set(id, item); retainedBytes += itemBytes;
    }
  }
  // Recompute ranks after authority filtering; foreign candidates never reach the reranker.
  for (const [stream, field] of [[lexical.items, "bm25Score"], [semantic.items, "vectorScore"]] as const) {
    let rank = 0;
    for (const candidate of stream) {
      const item = candidates.get(candidate.obsId);
      if (item) { item[field] = candidate.score; item.score += 1 / (60 + ++rank); }
    }
  }
  const ordered = [...candidates.values()].sort((a, b) => b.score - a.score || a.id.localeCompare(b.id));
  // An oversized selected source is retained as pending, never silently truncated.
  const ranked = await rerank(query, ordered.slice(0, REFLECT_MAX_SOURCES).map(item => ({ observation: item.observation,
    sessionId: item.observation.sessionId, bm25Score: item.bm25Score, vectorScore: item.vectorScore, graphScore: 0, combinedScore: item.score } satisfies HybridSearchResult)), REFLECT_MAX_SOURCES, AbortSignal.timeout(30_000));
  const sources = ranked.map((result, order) => ({ ...candidates.get(result.observation.id)!, order, score: result.combinedScore }));
  const deferredFamilies: string[] = [];
  const graphGeneration = JSON.stringify(ready.graph);
  job.sourcePage = sources;
  job.sourcePageBinding = { identity, graphGeneration, deferredFamilies };
  return groupedManifest(job, sources, identity, graphGeneration, deferredFamilies);
}

export async function currentReflectManifest(kv: StateKV, job: ReflectJob): Promise<boolean> {
  const manifest = job.manifest;
  if (!manifest || !job.project && !job.legacy) return false;
  if (job.seedBinding && job.project) {
    const row = await projectedSource(kv, job.seedBinding.scope, job.seedBinding.id, job.project, job.seedBinding.version);
    if (!row || row.fingerprint !== job.seedBinding.fingerprint) return false;
  }
  for (const item of manifest.sources) {
    if (job.legacy) {
      const result = await retrievalRequest<{ items: ProjectedSource[] }>(kv, { action: "shared_source_projection", sources: [{ scope: item.scope, key: item.id, expected_version: item.version }], max_items: 1, max_bytes: 65536 });
      const row = result.items?.[0];
      if (!row || row.version !== item.version || row.fingerprint !== item.fingerprint) return false;
      continue;
    }
    const row = await projectedSource(kv, item.scope, item.id, job.project!, item.version);
    if (!row || row.fingerprint !== item.fingerprint) return false;
  }
  if (job.legacy) return true;
  try {
    const status = await retrievalRequest<{ graph: unknown; semantic: Array<Record<string, unknown>> }>(kv, { action: "index_status" });
    const index = status.semantic.find(item => item.index_id === manifest.indexIdentity.index_id);
    const graph = [KV.graphNodes, KV.graphEdges].map(scope => {
      const item = Array.isArray(status.graph) ? status.graph.find((item: { scope: string }) => item.scope === scope) : undefined;
      return { scope, status: item?.status ?? null, revision: item?.revision ?? null };
    });
    return JSON.stringify(graph) === manifest.graphGeneration && !!index &&
      Object.entries(manifest.indexIdentity).every(([key, value]) => index[key] === value) && index.status === "ready" && index.coverage_ready === true && String(index.dirty_count) === "0";
  } catch (error) { if (error instanceof IndexedRetrievalError) return false; throw error; }
}
