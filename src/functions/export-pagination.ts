import { Buffer } from "node:buffer";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { SAFE_PAYLOAD_BYTES } from "../state/frame-guard.js";
import type { ExportCollection } from "../types.js";

const PAGE_DATA_BUDGET = SAFE_PAYLOAD_BYTES - 64 * 1024;
const KEY_PAGE_LIMIT = 16;
const READ_CONCURRENCY = 8;
const MAX_CURSOR_BYTES = 32 * 1024;
const MAX_OBSERVATION_SCOPES_PER_PAGE = 16;

interface ScopeScan {
  cursor: string | null;
  done: boolean;
  pendingKeys: string[];
}

interface ExportCursor {
  version: 1;
  collection: ExportCollection;
  collectionRevision: string;
  emitted: number;
  scan?: ScopeScan;
  sessions?: ScopeScan;
  sessionId?: string | null;
  observations?: ScopeScan | null;
}

export interface BoundedExportPage {
  records: unknown[];
  collectionRevision: string;
  nextCursor?: string;
  total?: number;
  hasMore: boolean;
}

const COLLECTION_SCOPES: Record<Exclude<ExportCollection, "observations">, string> = {
  sessions: KV.sessions,
  memories: KV.memories,
  summaries: KV.summaries,
  profiles: KV.profiles,
  graphNodes: KV.graphNodes,
  graphEdges: KV.graphEdges,
  semanticMemories: KV.semantic,
  proceduralMemories: KV.procedural,
  actions: KV.actions,
  actionEdges: KV.actionEdges,
  routines: KV.routines,
  signals: KV.signals,
  checkpoints: KV.checkpoints,
  sentinels: KV.sentinels,
  sketches: KV.sketches,
  crystals: KV.crystals,
  facets: KV.facets,
  lessons: KV.lessons,
  insights: KV.insights,
  accessLogs: KV.accessLog,
};

const COLLECTION_KEY_FIELDS: Record<ExportCollection, string> = {
  sessions: "id",
  observations: "id",
  memories: "id",
  summaries: "sessionId",
  profiles: "project",
  graphNodes: "id",
  graphEdges: "id",
  semanticMemories: "id",
  proceduralMemories: "id",
  actions: "id",
  actionEdges: "id",
  routines: "id",
  signals: "id",
  checkpoints: "id",
  sentinels: "id",
  sketches: "id",
  crystals: "id",
  facets: "id",
  lessons: "id",
  insights: "id",
  accessLogs: "memoryId",
};

function emptyScan(): ScopeScan {
  return { cursor: null, done: false, pendingKeys: [] };
}

function cursorError(code: string): Error {
  return new Error(code);
}

function decodeCursor(value: string | undefined, collection: ExportCollection): ExportCursor | undefined {
  if (value === undefined) return undefined;
  if (!value || Buffer.byteLength(value, "utf8") > MAX_CURSOR_BYTES) throw cursorError("STATE_EXPORT_CURSOR_INVALID");
  try {
    const decoded = JSON.parse(Buffer.from(value, "base64url").toString("utf8")) as Partial<ExportCursor>;
    if (decoded.version !== 1 || decoded.collection !== collection || typeof decoded.collectionRevision !== "string" ||
        !Number.isSafeInteger(decoded.emitted) || (decoded.emitted ?? -1) < 0) {
      throw cursorError("STATE_EXPORT_CURSOR_INVALID");
    }
    if (collection === "observations") {
      if (!validScan(decoded.sessions) || (decoded.sessionId !== null && typeof decoded.sessionId !== "string") ||
          (decoded.observations !== null && decoded.observations !== undefined && !validScan(decoded.observations))) {
        throw cursorError("STATE_EXPORT_CURSOR_INVALID");
      }
    } else if (!validScan(decoded.scan)) {
      throw cursorError("STATE_EXPORT_CURSOR_INVALID");
    }
    return decoded as ExportCursor;
  } catch (error) {
    if (error instanceof Error && error.message === "STATE_EXPORT_CURSOR_INVALID") throw error;
    throw cursorError("STATE_EXPORT_CURSOR_INVALID");
  }
}

function validScan(value: unknown): value is ScopeScan {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const scan = value as Partial<ScopeScan>;
  return (scan.cursor === null || typeof scan.cursor === "string") && typeof scan.done === "boolean" &&
    Array.isArray(scan.pendingKeys) && scan.pendingKeys.length <= KEY_PAGE_LIMIT &&
    scan.pendingKeys.every((key) => typeof key === "string" && key.length > 0 && Buffer.byteLength(key, "utf8") <= 512);
}

function encodeCursor(cursor: ExportCursor): string {
  const value = Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
  if (Buffer.byteLength(value, "utf8") > MAX_CURSOR_BYTES) throw cursorError("STATE_EXPORT_CURSOR_TOO_LARGE");
  return value;
}

async function collectionRevision(kv: StateKV, collection: ExportCollection): Promise<string> {
  if (collection !== "observations") {
    const revision = await kv.scopeRevision(COLLECTION_SCOPES[collection]);
    return JSON.stringify([revision.generation, revision.revision]);
  }
  const [sessions, observations] = await Promise.all([
    kv.scopeRevision(KV.sessions),
    kv.scopeRevision(KV.observations(""), true),
  ]);
  return JSON.stringify([sessions.generation, sessions.revision, observations.generation, observations.revision]);
}

async function fillProjectedKeys(kv: StateKV, scope: string, field: string, scan: ScopeScan): Promise<void> {
  if (scan.pendingKeys.length > 0 || scan.done) return;
  const options = { fields: [field], limit: KEY_PAGE_LIMIT, ...(scan.cursor === null ? {} : { cursor: scan.cursor }) };
  for await (const page of kv.pages<Record<string, unknown>>(scope, options)) {
    scan.cursor = page.next_cursor;
    scan.done = page.next_cursor === null;
    scan.pendingKeys = page.items.map((item) => {
      const key = item[field];
      if (typeof key !== "string" || key.length === 0 || Buffer.byteLength(key, "utf8") > 512) {
        throw cursorError("STATE_EXPORT_RECORD_IDENTITY_INVALID");
      }
      return key;
    });
    return;
  }
  throw cursorError("STATE_PAGE_INVALID_RESPONSE");
}

async function appendProjectedRecords(
  kv: StateKV,
  scope: string,
  field: string,
  scan: ScopeScan,
  records: unknown[],
  limit: number,
  wrap: (record: unknown) => unknown = (record) => record,
): Promise<number> {
  let bytes = records.reduce<number>((total, record) => total + Buffer.byteLength(JSON.stringify(record) ?? "", "utf8"), 0);
  while (records.length < limit) {
    await fillProjectedKeys(kv, scope, field, scan);
    if (scan.pendingKeys.length === 0) return bytes;
    const keys = scan.pendingKeys.slice(0, Math.min(READ_CONCURRENCY, limit - records.length));
    const values = await Promise.all(keys.map((key) => kv.get<Record<string, unknown>>(scope, key)));
    for (let index = 0; index < keys.length; index++) {
      const value = values[index];
      if (!value) throw cursorError("STATE_EXPORT_RECORD_MISSING");
      if (value[field] !== keys[index]) throw cursorError("STATE_EXPORT_RECORD_ID_MISMATCH");
      const record = wrap(value);
      const recordBytes = Buffer.byteLength(JSON.stringify(record) ?? "", "utf8");
      if (records.length > 0 && bytes + recordBytes > PAGE_DATA_BUDGET) return bytes;
      records.push(record);
      scan.pendingKeys.shift();
      bytes += recordBytes;
      if (records.length >= limit) return bytes;
    }
  }
  return bytes;
}

function scanHasMore(scan: ScopeScan): boolean {
  return scan.pendingKeys.length > 0 || !scan.done;
}

async function pageSimpleCollection(
  kv: StateKV,
  collection: Exclude<ExportCollection, "observations">,
  cursor: ExportCursor,
  limit: number,
): Promise<{ records: unknown[]; hasMore: boolean }> {
  const scan = cursor.scan!;
  const records: unknown[] = [];
  await appendProjectedRecords(kv, COLLECTION_SCOPES[collection], COLLECTION_KEY_FIELDS[collection], scan, records, limit);
  return { records, hasMore: scanHasMore(scan) };
}

async function pageObservations(
  kv: StateKV,
  cursor: ExportCursor,
  limit: number,
): Promise<{ records: unknown[]; hasMore: boolean }> {
  const sessions = cursor.sessions!;
  const records: unknown[] = [];
  let visitedScopes = 0;
  while (records.length < limit && visitedScopes < MAX_OBSERVATION_SCOPES_PER_PAGE) {
    if (cursor.sessionId === null || cursor.sessionId === undefined) {
      await fillProjectedKeys(kv, KV.sessions, "id", sessions);
      if (sessions.pendingKeys.length === 0) break;
      cursor.sessionId = sessions.pendingKeys.shift()!;
      cursor.observations = emptyScan();
      visitedScopes++;
    }
    const observations = cursor.observations!;
    await appendProjectedRecords(
      kv,
      KV.observations(cursor.sessionId),
      COLLECTION_KEY_FIELDS.observations,
      observations,
      records,
      limit,
      (observation) => ({ sessionId: cursor.sessionId, observation }),
    );
    if (observations.pendingKeys.length === 0 && observations.done) {
      cursor.sessionId = null;
      cursor.observations = null;
    }
    if (records.length >= limit) break;
    if (cursor.sessionId !== null && cursor.sessionId !== undefined) {
      if (observations.pendingKeys.length > 0) break;
      continue;
    }
  }
  const hasMore = (cursor.sessionId !== null && cursor.sessionId !== undefined) || scanHasMore(sessions);
  return { records, hasMore };
}

export async function pageExportCollection(
  kv: StateKV,
  collection: ExportCollection,
  cursorValue: string | undefined,
  limit: number,
): Promise<BoundedExportPage> {
  const cursor = decodeCursor(cursorValue, collection);
  const revision = await collectionRevision(kv, collection);
  if (cursor && cursor.collectionRevision !== revision) throw cursorError("STATE_EXPORT_COLLECTION_CHANGED");
  const state: ExportCursor = cursor ?? {
    version: 1,
    collection,
    collectionRevision: revision,
    emitted: 0,
    ...(collection === "observations" ? { sessions: emptyScan(), sessionId: null, observations: null } : { scan: emptyScan() }),
  };
  const page = collection === "observations"
    ? await pageObservations(kv, state, limit)
    : await pageSimpleCollection(kv, collection, state, limit);
  if (await collectionRevision(kv, collection) !== revision) throw cursorError("STATE_EXPORT_COLLECTION_CHANGED");
  state.emitted += page.records.length;
  return {
    records: page.records,
    collectionRevision: revision,
    hasMore: page.hasMore,
    ...(page.hasMore ? { nextCursor: encodeCursor(state) } : { total: state.emitted }),
  };
}
