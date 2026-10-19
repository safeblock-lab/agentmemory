import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { IndexedRetrievalError, retrievalRequest } from "../state/indexed-retrieval.js";

export const VIEWER_PAGE_ROWS = 16;
export const VIEWER_PAGE_BYTES = 65_536;
type Query = Record<string, unknown>;
type Row = Record<string, unknown>;
type RecentPageItem = { scope: string; key: string; version: string; value: Row; projection?: unknown };
type SessionCount = { total: number | null; scope: "all_sessions"; coverage: "complete" | "incomplete"; revision: string; generation: string };
type RecentPageResult = {
  status: "preparing" | "partial" | "ready";
  items: RecentPageItem[];
  has_more: boolean;
  next_cursor: string | null;
  ordering: string;
  reason?: string | null;
  count?: SessionCount;
};
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
    && (value.count === undefined || entity === "sessions" && isSessionCount(value.count))
    && value.items.every(item => isRecentPageItem(item, entity, sessionId));
}

function isSessionCount(value: unknown): value is SessionCount {
  return isRecord(value) && value.scope === "all_sessions"
    && (value.coverage === "complete" ? typeof value.total === "number" && Number.isSafeInteger(value.total) && value.total >= 0
      : value.coverage === "incomplete" && value.total === null)
    && typeof value.revision === "string" && /^\d{1,20}$/.test(value.revision)
    && typeof value.generation === "string" && /^\d{1,20}$/.test(value.generation);
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
  const result = await requestRecentPage(kv, entity, sessionId, options, fields);
  const partial = result.status !== "ready" || result.has_more || options.cursor !== undefined;
  return { items: result.items.map(item => item.value), status: result.status, partial, nextCursor: result.next_cursor,
    total: entity === "sessions" ? result.count?.total ?? null : partial ? null : result.items.length,
    countScope: entity === "sessions" ? "all_sessions" : "page", returnedCount: result.items.length,
    ...(entity === "sessions" ? { countCoverage: result.count?.coverage ?? "unavailable", countRevision: result.count?.revision ?? null,
      countGeneration: result.count?.generation ?? null } : {}), ordering: result.ordering, reason: result.reason,
    projections: result.items.map(item => ({ id: item.key, projection: item.projection })), limit: options.limit };
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
