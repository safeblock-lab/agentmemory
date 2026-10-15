import { InvocationError } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { IndexedRetrievalError, retrievalRequest } from "../state/indexed-retrieval.js";

export const VIEWER_PAGE_ROWS = 16;
export const VIEWER_PAGE_BYTES = 65_536;
const VIEWER_RECENT_PREPARATION_TIMEOUT_MS = 70_000;
const VIEWER_RECENT_PREPARATION_MAX_ROWS = 16_384;
const VIEWER_RECENT_PREPARATION_MAX_BYTES = 128 * 1024 * 1024;
const VIEWER_RECENT_PREPARATION_MAX_MS = 300_000;
const VIEWER_RECENT_SOURCE_MAX_BYTES = 16 * 1024 * 1024;
type Query = Record<string, unknown>;
type Row = Record<string, unknown>;
type RecentPreparationStatus = {
  target: "viewer_recent";
  status: "preparing" | "partial" | "ready";
  checkpoint: string;
  generation: string;
  scanned: boolean;
  dirty: number;
  issues: number;
  ready: boolean;
  has_more: boolean;
};
type RecentPreparationResult = {
  status: "preparing" | "partial" | "ready";
  processed: number;
  continue: boolean;
  cursor: { phase: "sessions" | "observations"; scanned: boolean; generation: string };
  reason: string | null;
  source_bytes: number;
  decoded_bytes: number;
  required_bytes: number | null;
  elapsed_ms: number;
  batch_limits: { rows: number; decoded_bytes: number; safety_ms: number };
  preparation: RecentPreparationStatus;
};
type RecentPageItem = { scope: string; key: string; version: string; value: Row; projection?: unknown };
type RecentPageResult = {
  status: "preparing" | "partial" | "ready";
  items: RecentPageItem[];
  has_more: boolean;
  next_cursor: string | null;
  ordering: string;
  reason?: string | null;
};
type RecentPreparationReconciliation =
  | ({ state: "reconciled" } & RecentPreparationStatus)
  | { state: "unknown"; reason: string };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSafeIntegerAtLeast(value: unknown, minimum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;
}

function hasBoundedJsonBytes(value: unknown, maxBytes: number): boolean {
  try {
    const serialized = JSON.stringify(value);
    return typeof serialized === "string" && Buffer.byteLength(serialized) <= maxBytes;
  } catch {
    return false;
  }
}

function memoryTimestamp(row: Row): number | null {
  const updatedAt = typeof row.updatedAt === "string" && row.updatedAt.trim() ? Date.parse(row.updatedAt) : Number.NaN;
  if (Number.isFinite(updatedAt)) return updatedAt;
  const createdAt = typeof row.createdAt === "string" && row.createdAt.trim() ? Date.parse(row.createdAt) : Number.NaN;
  return Number.isFinite(createdAt) ? createdAt : null;
}

function newestFirst(rows: Row[]): Row[] {
  return [...rows].sort((left, right) => {
    const leftTimestamp = memoryTimestamp(left);
    const rightTimestamp = memoryTimestamp(right);
    if (leftTimestamp !== rightTimestamp) {
      if (leftTimestamp === null) return 1;
      if (rightTimestamp === null) return -1;
      return leftTimestamp > rightTimestamp ? -1 : 1;
    }
    const leftId = typeof left.id === "string" ? left.id : "";
    const rightId = typeof right.id === "string" ? right.id : "";
    return leftId < rightId ? -1 : leftId > rightId ? 1 : 0;
  });
}

export function viewerPageOptions(query: Query) {
  const rawLimit = query.limit;
  const requested = rawLimit === undefined ? VIEWER_PAGE_ROWS : Number(rawLimit);
  if (!Number.isSafeInteger(requested) || requested < 1) throw new Error("viewer_invalid_limit");
  if (query.offset !== undefined && String(query.offset) !== "0") throw new Error("viewer_cursor_required");
  const cursor = query.cursor;
  if (cursor !== undefined && (typeof cursor !== "string" || !cursor || Buffer.byteLength(cursor) > 4096)) throw new Error("viewer_invalid_cursor");
  return { limit: Math.min(requested, VIEWER_PAGE_ROWS), maxBytes: VIEWER_PAGE_BYTES,
    ...(typeof cursor === "string" ? { cursor } : {}) };
}

export async function viewerPage(kv: StateKV, scope: string, query: Query, fields: string[]) {
  const options = viewerPageOptions(query);
  for await (const page of kv.pages<Row>(scope, { ...options, fields })) {
    return { items: page.items, nextCursor: page.next_cursor, partial: page.next_cursor !== null || query.cursor !== undefined,
      limit: options.limit, scanned: page.items.length };
  }
  throw new Error("viewer_missing_page");
}

export async function viewerMemories(kv: StateKV, query: Query) {
  if (typeof query.project === "string" && query.project.trim()) return viewerProjectMemories(kv, query.project.trim(), query);
  const page = await viewerPage(kv, KV.memories, query,
    ["id", "title", "content", "type", "strength", "version", "isLatest", "createdAt", "updatedAt", "concepts", "files", "project", "agentId", "origin", "parentId", "sessionIds"]);
  for (const item of page.items) if (typeof item.id !== "string" || !item.id) throw new Error("viewer_invalid_memory_id");
  const memories = newestFirst(page.items.filter(item => query.latest !== "true" || item.isLatest === true));
  const partial = page.partial;
  const metadata = { nextCursor: page.nextCursor, partial, scanned: page.scanned,
    returnedCount: memories.length, total: partial ? null : memories.length, limit: page.limit,
    countScope: "page" };
  return query.count === "true" ? { ...metadata, latestCount: partial ? null : memories.filter(item => item.isLatest).length }
    : { ...metadata, memories, offset: 0 };
}

async function viewerProjectMemories(kv: StateKV, project: string, query: Query) {
  viewerPageOptions(query);
  if (query.latest !== "true") throw new Error("viewer_project_history_requires_native_selector");
  let sessionCursor: string | undefined, memoryCursor: string | undefined;
  if (typeof query.cursor === "string") {
    let saved: unknown;
    try { saved = JSON.parse(Buffer.from(query.cursor, "base64url").toString("utf8")); } catch { throw new Error("viewer_invalid_cursor"); }
    if (!saved || typeof saved !== "object" || !("project" in saved) || saved.project !== project
      || !("sessionCursor" in saved) || !("memoryCursor" in saved)
      || !(saved.sessionCursor === null || typeof saved.sessionCursor === "string")
      || !(saved.memoryCursor === null || typeof saved.memoryCursor === "string")) throw new Error("viewer_invalid_cursor");
    sessionCursor = saved.sessionCursor ?? undefined; memoryCursor = saved.memoryCursor ?? undefined;
  }
  const sessions = await viewerProjectSessions(kv, project, sessionCursor);
  const base = { total: null, partial: true, nextCursor: null as string | null, limit: 12, countScope: "page" };
  if (sessions.status === "not_ready" || !sessions.items.length) return { ...base, memories: [], returnedCount: 0,
    reason: sessions.partial || sessions.status === "not_ready" ? "project_sessions_incomplete" : "no_sessions",
    total: sessions.partial || sessions.status === "not_ready" ? null : 0, partial: sessions.partial || sessions.status === "not_ready" };
  const page = await retrievalRequest<{ status: string; items: Array<{ id: string; row_version: string }>; next_cursor: string | null }>(kv,
    { action: "canonical_source_page", scope: KV.memories, source_project: project,
      source_session_ids: sessions.items.map(item => item.session_id), ...(memoryCursor ? { cursor: memoryCursor } : {}), limit: 12, max_bytes: VIEWER_PAGE_BYTES });
  if (page.status !== "ready") return { ...base, memories: [], returnedCount: 0, reason: "memory_sources_not_ready" };
  if (!Array.isArray(page.items) || page.items.length > 12 || Buffer.byteLength(JSON.stringify(page)) > VIEWER_PAGE_BYTES
    || !(page.next_cursor === null || typeof page.next_cursor === "string")
    || page.items.some(item => typeof item.id !== "string" || typeof item.row_version !== "string")) throw new Error("viewer_invalid_memory_sources");
  const projected = page.items.length ? await retrievalRequest<{ items: Array<{ key: string; scope: string; value: Row }>; deferred: unknown[] }>(kv,
    { action: "reflect_sources", project, sources: page.items.map(item => ({ scope: KV.memories, key: item.id, expected_version: item.row_version })), max_items: 12, max_bytes: VIEWER_PAGE_BYTES }) : { items: [], deferred: [] };
  if (!Array.isArray(projected.items) || !Array.isArray(projected.deferred) || projected.items.length > page.items.length
    || Buffer.byteLength(JSON.stringify(projected)) > VIEWER_PAGE_BYTES
    || projected.items.some(item => item.scope !== KV.memories || !page.items.some(ref => ref.id === item.key) || item.value?.id !== item.key)) throw new Error("viewer_invalid_memory_projection");
  const next = page.next_cursor || sessions.next_cursor;
  const nextCursor = next ? Buffer.from(JSON.stringify({ project,
    sessionCursor: page.next_cursor ? sessionCursor ?? null : sessions.next_cursor,
    memoryCursor: page.next_cursor })).toString("base64url") : null;
  if (nextCursor && Buffer.byteLength(nextCursor) > 4096) throw new Error("viewer_cursor_budget_exceeded");
  const memories = newestFirst(projected.items.map(item => item.value).filter(item => query.latest !== "true" || item.isLatest === true));
  const partial = sessions.partial || nextCursor !== null || projected.deferred.length > 0 || query.cursor !== undefined;
  return query.count === "true" ? { ...base, nextCursor, partial, total: partial ? null : memories.length, returnedCount: memories.length, latestCount: partial ? null : memories.length }
    : { ...base, memories, nextCursor, partial, returnedCount: memories.length, total: partial ? null : memories.length };
}

function isRecentPreparationStatus(value: unknown): value is RecentPreparationStatus {
  if (!isRecord(value) || value.target !== "viewer_recent"
    || !["preparing", "partial", "ready"].includes(String(value.status))
    || typeof value.checkpoint !== "string" || !/^[a-f\d]{64}$/i.test(value.checkpoint)
    || typeof value.generation !== "string" || !/^\d+$/.test(value.generation)
    || typeof value.scanned !== "boolean" || !isSafeIntegerAtLeast(value.dirty, 0)
    || !isSafeIntegerAtLeast(value.issues, 0) || typeof value.ready !== "boolean"
    || typeof value.has_more !== "boolean") return false;
  if (value.status === "ready") return value.ready && value.scanned && value.dirty === 0 && value.issues === 0 && !value.has_more;
  if (value.status === "partial") return !value.ready && value.issues > 0;
  return !value.ready && value.has_more;
}

function isRecentPreparationResult(value: unknown): value is RecentPreparationResult {
  if (!isRecord(value) || !["preparing", "partial", "ready"].includes(String(value.status))
    || !isSafeIntegerAtLeast(value.processed, 0) || typeof value.continue !== "boolean"
    || !isRecord(value.cursor) || !(value.cursor.phase === "sessions" || value.cursor.phase === "observations")
    || typeof value.cursor.scanned !== "boolean" || typeof value.cursor.generation !== "string"
    || !/^\d+$/.test(value.cursor.generation) || !(value.reason === null || typeof value.reason === "string")
    || !isSafeIntegerAtLeast(value.source_bytes, 0) || !isSafeIntegerAtLeast(value.decoded_bytes, 0)
    || !(value.required_bytes === null || isSafeIntegerAtLeast(value.required_bytes, 1)
      && value.required_bytes <= VIEWER_RECENT_SOURCE_MAX_BYTES)
    || !isSafeIntegerAtLeast(value.elapsed_ms, 0) || value.elapsed_ms > VIEWER_RECENT_PREPARATION_MAX_MS
    || !isRecord(value.batch_limits) || !isSafeIntegerAtLeast(value.batch_limits.rows, 1)
    || value.batch_limits.rows > VIEWER_RECENT_PREPARATION_MAX_ROWS
    || !isSafeIntegerAtLeast(value.batch_limits.decoded_bytes, 1024)
    || value.batch_limits.decoded_bytes > VIEWER_RECENT_PREPARATION_MAX_BYTES
    || !isSafeIntegerAtLeast(value.batch_limits.safety_ms, 1)
    || value.batch_limits.safety_ms > VIEWER_RECENT_PREPARATION_MAX_MS
    || !isRecentPreparationStatus(value.preparation)) return false;
  const status = value.status;
  const reason = value.reason;
  const cursor = value.cursor;
  const limits = value.batch_limits;
  const readiness = value.preparation;
  return value.processed <= limits.rows && value.source_bytes <= limits.decoded_bytes
    && value.decoded_bytes <= limits.decoded_bytes
    && cursor.generation === readiness.generation && cursor.scanned === readiness.scanned
    && (value.continue || cursor.scanned && readiness.dirty === 0)
    && (!value.continue || !cursor.scanned || readiness.dirty !== 0 || reason === "decode_budget_exceeded")
    && status === readiness.status
    && (status !== "partial" || readiness.issues > 0)
    && (status !== "ready" || readiness.ready && !value.continue)
    && (value.required_bytes === null || reason === "decode_budget_exceeded")
    && (reason !== "decode_budget_exceeded" || value.required_bytes !== null);
}

function isRecentPageItem(value: unknown, entity: "sessions" | "observations", sessionId?: string): value is RecentPageItem {
  if (!isRecord(value) || typeof value.key !== "string" || !value.key
    || typeof value.version !== "string" || !value.version || !isRecord(value.value) || value.value.id !== value.key) return false;
  if (entity === "sessions") return value.scope === KV.sessions;
  return typeof value.scope === "string" && value.scope.startsWith("mem:obs:") && value.scope.length > "mem:obs:".length
    && (sessionId === undefined || value.scope === KV.observations(sessionId) && value.value.sessionId === sessionId);
}

function isRecentPageResult(value: unknown, entity: "sessions" | "observations", sessionId: string | undefined, limit: number): value is RecentPageResult {
  return isRecord(value) && ["ready", "preparing", "partial"].includes(String(value.status))
    && Array.isArray(value.items) && value.items.length <= limit
    && typeof value.has_more === "boolean"
    && (value.next_cursor === null || typeof value.next_cursor === "string" && Buffer.byteLength(value.next_cursor) <= 4096)
    && value.has_more === (value.next_cursor !== null)
    && typeof value.ordering === "string" && value.ordering.length <= 128
    && (value.reason === undefined || value.reason === null || typeof value.reason === "string")
    && value.items.every(item => isRecentPageItem(item, entity, sessionId));
}

async function requestRecentPreparation(kv: StateKV): Promise<RecentPreparationResult> {
  let response: unknown;
  try {
    response = await kv.retrieval<unknown>({ action: "viewer_recent_prepare" }, {
      timeoutMs: VIEWER_RECENT_PREPARATION_TIMEOUT_MS,
    });
  } catch (error) {
    if (error instanceof InvocationError && ["STATE_RECENT_CURSOR_STALE", "STATE_TX_INVALID_REQUEST"].includes(error.code)) {
      throw new IndexedRetrievalError(error.code, "Native retrieval cursor or request is no longer valid.");
    }
    if (error instanceof InvocationError && error.code === "STATE_RETRIEVAL_RESOURCE_LIMIT") {
      throw new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "Native indexed retrieval exceeded its response resource limit.");
    }
    throw error;
  }
  if (response === null || typeof response !== "object") {
    throw new IndexedRetrievalError("STATE_INDEX_NOT_READY", "Native indexed retrieval is unavailable. Prepare the compatible engine indexes explicitly.");
  }
  if (Array.isArray(response)) throw new Error("viewer_invalid_recent_preparation");
  if (!isRecord(response)) throw new Error("viewer_invalid_recent_preparation");
  if (typeof response.error === "string" || typeof response.code === "string") {
    throw new IndexedRetrievalError(String(response.code ?? response.error), "Native indexed retrieval rejected the request.");
  }
  if (!hasBoundedJsonBytes(response, VIEWER_PAGE_BYTES) || !isRecentPreparationResult(response)) {
    throw new Error("viewer_invalid_recent_preparation");
  }
  return response;
}

async function reconcileRecentPreparation(kv: StateKV): Promise<RecentPreparationReconciliation> {
  try {
    const response = await retrievalRequest<unknown>(kv, { action: "preparation_status", target: "viewer_recent" });
    if (!isRecentPreparationStatus(response)) return { state: "unknown", reason: "invalid_status_response" };
    return { state: "reconciled", ...response };
  } catch (error) {
    const reason = error instanceof IndexedRetrievalError || error instanceof InvocationError ? error.code : "status_reconciliation_failed";
    return { state: "unknown", reason };
  }
}

async function requestRecentPage(kv: StateKV, entity: "sessions" | "observations", sessionId: string | undefined,
  options: ReturnType<typeof viewerPageOptions>, fields: string[]): Promise<RecentPageResult> {
  const response = await retrievalRequest<unknown>(kv, {
    action: "viewer_recent_page", entity, fields, limit: options.limit, max_bytes: options.maxBytes,
    ...(options.cursor ? { cursor: options.cursor } : {}), ...(sessionId ? { session_id: sessionId } : {}),
  });
  if (!isRecentPageResult(response, entity, sessionId, options.limit) || !hasBoundedJsonBytes(response, VIEWER_PAGE_BYTES)) {
    throw new Error("viewer_invalid_recent_page");
  }
  return response;
}

export const VIEWER_SESSION_FIELDS = ["id", "project", "agentId", "startedAt", "endedAt", "status", "observationCount", "title", "cwd", "agent", "metadata", "firstPrompt", "model"];
export const VIEWER_OBSERVATION_FIELDS = ["id", "sessionId", "agentId", "timestamp", "type", "title", "subtitle", "narrative", "facts", "concepts", "files", "importance", "tokens", "toolName", "hookType", "userPrompt"];

export async function viewerRecentPage(kv: StateKV, entity: "sessions" | "observations", query: Query, sessionId?: string) {
  const options = viewerPageOptions(query);
  if (query.latest !== "true") throw new Error("viewer_latest_required");
  const fields = entity === "sessions" ? VIEWER_SESSION_FIELDS.filter(field => field !== "metadata")
    : [...VIEWER_OBSERVATION_FIELDS, "compressionMethod"];
  let result = await requestRecentPage(kv, entity, sessionId, options, fields);
  let preparation: RecentPreparationResult | undefined;
  let preparationReconciliation: RecentPreparationReconciliation | undefined;
  if (result.status === "preparing") {
    try {
      preparation = await requestRecentPreparation(kv);
    } catch (error) {
      if (!(error instanceof InvocationError) || error.code !== "TIMEOUT") throw error;
      preparationReconciliation = await reconcileRecentPreparation(kv);
    }
    const exhausted = preparation
      ? !preparation.continue
      : preparationReconciliation?.state === "reconciled"
        && preparationReconciliation.scanned && preparationReconciliation.dirty === 0;
    if (options.cursor === undefined && exhausted) {
      result = await requestRecentPage(kv, entity, sessionId, options, fields);
    }
  }
  const partial = result.status !== "ready" || result.has_more || options.cursor !== undefined;
  return { items: result.items.map(item => item.value), status: result.status, partial, nextCursor: result.next_cursor,
    total: partial ? null : result.items.length, ordering: result.ordering, reason: result.reason,
    projections: result.items.map(item => ({ id: item.key, projection: item.projection })), limit: options.limit,
    ...(preparation ? { preparation } : {}), ...(preparationReconciliation ? { preparationReconciliation } : {}) };
}

export interface ViewerProjectSessions {
  items: Array<{ session_id: string; source_key: string; row_version: string; session: { id: string; project: string; startedAt?: string } }>;
  next_cursor: string | null;
  partial: boolean;
  status: "ready" | "partial" | "not_ready";
  revision: number;
  epoch: number;
}
export async function viewerProjectSessions(kv: StateKV, project: string, cursor?: string): Promise<ViewerProjectSessions> {
  const result = await retrievalRequest<ViewerProjectSessions>(kv, { action: "project_sessions_page", project,
    ...(cursor ? { cursor } : {}), limit: 12, max_bytes: VIEWER_PAGE_BYTES });
  if (!Array.isArray(result.items) || result.items.length > 12 || !["ready", "partial", "not_ready"].includes(result.status)
    || typeof result.partial !== "boolean" || !(result.next_cursor === null || typeof result.next_cursor === "string")
    || !Number.isSafeInteger(result.revision) || !Number.isSafeInteger(result.epoch)
    || Buffer.byteLength(JSON.stringify(result)) > VIEWER_PAGE_BYTES
    || result.items.some(item => !item.session_id || item.source_key !== item.session_id || !item.row_version
      || item.session?.id !== item.session_id || item.session.project !== project)) throw new Error("viewer_invalid_project_sessions");
  return result;
}
