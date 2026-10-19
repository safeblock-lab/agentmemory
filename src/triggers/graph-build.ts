import { Buffer } from "node:buffer";
import type { IIIClient } from "iii-sdk";
import type { CompressedObservation } from "../types.js";
import { viewerProjectSessions } from "../functions/viewer-memory-page.js";
import { fingerprintId, KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { StatePageError } from "../state/state-pages.js";
import { logger } from "../logger.js";
import type { GraphAutomaticRecoveryView } from "../functions/graph-automatic-workflow.js";
import { readGraphStoppedTerminal } from "../functions/graph-jobs.js";
import type { GraphTerminalRecoveryView } from "../functions/graph-automatic-terminal.js";
import { graphAutomaticAvailability, type GraphAutomaticAvailability } from "../functions/graph-automatic-authority.js";

export const MAX_GRAPH_BUILD_CONTINUATION_BYTES = 16_384;
export const MAX_GRAPH_BUILD_OBSERVATIONS = 25;
export const MAX_GRAPH_BUILD_PAGE_BYTES = 1_048_576;

type GraphBuildStatus = "completed" | "partial" | "queued" | "failed";
type GraphBuildError = "invalid_continuation" | "continuation_project_mismatch" | "project_session_changed"
  | "project_catalog_not_ready" | "project_catalog_unavailable" | "observation_page_unavailable"
  | "observation_changed" | "graph_extraction_failed";

export interface GraphBuildResponse {
  graphAutomaticAvailability: GraphAutomaticAvailability;
  status: GraphBuildStatus;
  project: string;
  sessionsProcessed: number;
  observationsProcessed: number;
  nodesAdded: number;
  edgesAdded: number;
  continuation: string | null;
  graphJobId?: string;
  error?: GraphBuildError;
  automaticRecovery?: GraphAutomaticRecoveryView;
  automaticTerminalRecovery?: GraphTerminalRecoveryView;
  action?: { endpoint: "/agentmemory/graph/build"; project: string; continuation: string } | null;
}

interface GraphBuildCursor {
  version: 1;
  project: string;
  sessionCursor: string | null;
  sessionIndex: number;
  sessionId: string | null;
  observationCursor: string | null;
  expectedGraphJobId: string | null;
}

interface ApiSdk extends Pick<IIIClient, "trigger"> {}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value as Record<string, unknown>).filter(([, item]) => item !== undefined)
      .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
      .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  }
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new Error("invalid_graph_build_value");
  return serialized;
}

function decodeCursor(raw: string | undefined, project: string): GraphBuildCursor {
  if (raw === undefined) return {
    version: 1, project, sessionCursor: null, sessionIndex: 0, sessionId: null,
    observationCursor: null, expectedGraphJobId: null,
  };
  if (!raw || Buffer.byteLength(raw, "utf8") > MAX_GRAPH_BUILD_CONTINUATION_BYTES) throw new Error("invalid_continuation");
  let value: unknown;
  try { value = JSON.parse(Buffer.from(raw, "base64url").toString("utf8")); }
  catch { throw new Error("invalid_continuation"); }
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid_continuation");
  const cursor = value as Record<string, unknown>;
  const keys = ["version", "project", "sessionCursor", "sessionIndex", "sessionId", "observationCursor", "expectedGraphJobId"];
  if (Object.keys(cursor).length !== keys.length || keys.some(key => !Object.hasOwn(cursor, key))
    || cursor.version !== 1 || typeof cursor.project !== "string"
    || !(cursor.sessionCursor === null || typeof cursor.sessionCursor === "string" && cursor.sessionCursor.length > 0)
    || !Number.isSafeInteger(cursor.sessionIndex) || (cursor.sessionIndex as number) < 0 || (cursor.sessionIndex as number) >= 12
    || !(cursor.sessionId === null || typeof cursor.sessionId === "string" && cursor.sessionId.length > 0)
    || !(cursor.observationCursor === null || typeof cursor.observationCursor === "string" && cursor.observationCursor.length > 0)
    || !(cursor.expectedGraphJobId === null || typeof cursor.expectedGraphJobId === "string"
      && /^graphbuild_[a-f0-9]{16}$/.test(cursor.expectedGraphJobId))
    || [cursor.sessionCursor, cursor.observationCursor, cursor.sessionId].some(item => typeof item === "string" && Buffer.byteLength(item, "utf8") > 4_096)) {
    throw new Error("invalid_continuation");
  }
  if (cursor.project !== project) throw new Error("continuation_project_mismatch");
  return cursor as GraphBuildCursor;
}

function encodeCursor(cursor: GraphBuildCursor): string {
  const encoded = Buffer.from(JSON.stringify(cursor), "utf8").toString("base64url");
  if (Buffer.byteLength(encoded, "utf8") > MAX_GRAPH_BUILD_CONTINUATION_BYTES) throw new Error("invalid_continuation");
  return encoded;
}

function errorCode(error: unknown): string {
  if (error instanceof StatePageError) return error.code;
  if (error && typeof error === "object" && "code" in error && typeof error.code === "string"
    && /^[A-Z][A-Z0-9_]{0,63}$/.test(error.code)) return error.code;
  if (error instanceof Error && /^[A-Z][A-Z0-9_]{0,63}$/.test(error.message)) return error.message;
  return error instanceof Error ? error.name : "unknown";
}

function response(
  project: string,
  status: GraphBuildStatus,
  continuation: string | null,
  fields: Partial<Pick<GraphBuildResponse, "sessionsProcessed" | "observationsProcessed" | "nodesAdded" | "edgesAdded" | "graphJobId" | "error" | "automaticRecovery" | "automaticTerminalRecovery">> = {},
): GraphBuildResponse {
  return {
    graphAutomaticAvailability: graphAutomaticAvailability(),
    status, project, sessionsProcessed: 0, observationsProcessed: 0, nodesAdded: 0, edgesAdded: 0,
    continuation, ...fields,
    ...(fields.automaticRecovery || fields.automaticTerminalRecovery ? { action: graphAutomaticAvailability().enabled && continuation
      && (fields.automaticTerminalRecovery?.action ?? fields.automaticRecovery?.action) !== "none"
      ? { endpoint: "/agentmemory/graph/build" as const, project, continuation } : null } : {}),
  };
}

function nextCursor(
  project: string,
  sessionCursor: string | null,
  sessionIndex: number,
  sessionId: string | null,
  observationCursor: string | null,
  expectedGraphJobId: string | null = null,
): string | null {
  return encodeCursor({ version: 1, project, sessionCursor, sessionIndex, sessionId, observationCursor, expectedGraphJobId });
}

async function continuationAfterPage(
  kv: StateKV,
  project: string,
  sessions: Awaited<ReturnType<typeof viewerProjectSessions>>,
  cursor: GraphBuildCursor,
  sessionId: string,
  observationCursor: string | null,
): Promise<string | null> {
  if (observationCursor) {
    return nextCursor(project, cursor.sessionCursor, cursor.sessionIndex, sessionId, observationCursor);
  }
  const nextIndex = cursor.sessionIndex + 1;
  const nextSession = sessions.items[nextIndex];
  if (nextSession) return nextCursor(project, cursor.sessionCursor, nextIndex, nextSession.session_id, null);
  if (!sessions.next_cursor) return null;
  const nextPage = await viewerProjectSessions(kv, project, sessions.next_cursor);
  return nextCursor(project, sessions.next_cursor, 0, nextPage.items[0]?.session_id ?? null, null);
}

function compressedObservations(items: unknown[], sessionId: string): CompressedObservation[] {
  const result: CompressedObservation[] = [];
  for (const item of items) {
    if (!item || typeof item !== "object" || Array.isArray(item)) throw new Error("invalid_observation_page");
    const observation = item as Record<string, unknown>;
    if (observation.title === undefined || observation.title === null || observation.title === "") continue;
    if (typeof observation.title !== "string" || typeof observation.id !== "string" || !observation.id
      || observation.sessionId !== sessionId) throw new Error("invalid_observation_page");
    result.push(observation as unknown as CompressedObservation);
  }
  return result;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function count(value: unknown): number {
  return Number.isSafeInteger(value) && (value as number) >= 0 ? value as number : 0;
}

export async function buildScopedGraph(
  sdk: ApiSdk,
  kv: StateKV,
  project: string,
  continuation?: string,
): Promise<GraphBuildResponse> {
  let cursor: GraphBuildCursor;
  try { cursor = decodeCursor(continuation, project); }
  catch (error) {
    const code = error instanceof Error ? error.message : "invalid_continuation";
    const publicCode = code === "continuation_project_mismatch" ? "continuation_project_mismatch" : "invalid_continuation";
    return response(project, "failed", continuation ?? null, { error: publicCode });
  }

  let sessions: Awaited<ReturnType<typeof viewerProjectSessions>>;
  try { sessions = await viewerProjectSessions(kv, project, cursor.sessionCursor ?? undefined); }
  catch (error) {
    logger.warn("graph-build project page failed", { project, code: errorCode(error) });
    return response(project, "failed", continuation ?? encodeCursor(cursor), { error: "project_catalog_unavailable" });
  }
  if (sessions.status === "not_ready") {
    return response(project, "partial", continuation ?? encodeCursor(cursor), { error: "project_catalog_not_ready" });
  }
  if (cursor.sessionIndex >= sessions.items.length) {
    if (cursor.sessionId !== null || cursor.expectedGraphJobId !== null) {
      return response(project, "failed", null, { error: "project_session_changed" });
    }
    if (sessions.items.length === 0 && sessions.next_cursor === null && sessions.status === "ready"
      && cursor.sessionIndex === 0) {
      return response(project, "completed", null);
    }
    if (sessions.next_cursor !== null) {
      return response(project, "partial", nextCursor(project, sessions.next_cursor, 0, null, null));
    }
    if (sessions.status === "ready") {
      return response(project, "failed", null, { error: "project_session_changed" });
    }
    return response(project, "partial", continuation ?? encodeCursor(cursor), { error: "project_catalog_not_ready" });
  }

  const selectedSession = sessions.items[cursor.sessionIndex];
  if (cursor.sessionId !== null && selectedSession.session_id !== cursor.sessionId) {
    return response(project, "failed", continuation ?? null, { error: "project_session_changed" });
  }
  const sessionId = selectedSession.session_id;
  const retryCursor = nextCursor(project, cursor.sessionCursor, cursor.sessionIndex, sessionId,
    cursor.observationCursor, cursor.expectedGraphJobId);
  let currentSession: { id?: string; project?: string } | null;
  try { currentSession = await kv.get<{ id?: string; project?: string }>(KV.sessions, sessionId); }
  catch (error) {
    logger.warn("graph-build session validation failed", { project, code: errorCode(error) });
    return response(project, "failed", retryCursor, { error: "project_catalog_unavailable" });
  }
  if (currentSession?.id !== sessionId || currentSession.project !== project) {
    return response(project, "failed", retryCursor, { error: "project_session_changed" });
  }

  let page: IteratorResult<{ items: CompressedObservation[]; next_cursor: string | null }>;
  try {
    page = await kv.pages<CompressedObservation>(KV.observations(sessionId), {
      ...(cursor.observationCursor ? { cursor: cursor.observationCursor } : {}),
      limit: MAX_GRAPH_BUILD_OBSERVATIONS,
      maxBytes: MAX_GRAPH_BUILD_PAGE_BYTES,
    }).next();
  } catch (error) {
    logger.warn("graph-build observation page failed", { project, code: errorCode(error) });
    return response(project, "failed", retryCursor, { error: "observation_page_unavailable" });
  }
  if (page.done) {
    logger.warn("graph-build observation page was empty", { project });
    return response(project, "failed", retryCursor, { error: "observation_page_unavailable" });
  }

  let observations: CompressedObservation[];
  try { observations = compressedObservations(page.value.items, sessionId); }
  catch {
    return response(project, "failed", retryCursor, { error: "observation_page_unavailable" });
  }
  let following: string | null;
  try { following = await continuationAfterPage(kv, project, sessions, cursor, sessionId, page.value.next_cursor); }
  catch (error) {
    logger.warn("graph-build continuation failed", { project, code: errorCode(error) });
    return response(project, "failed", retryCursor, { error: "project_catalog_unavailable" });
  }
  if (observations.length === 0) {
    if (cursor.expectedGraphJobId !== null) {
      return response(project, "failed", null, { error: "observation_changed" });
    }
    return response(project, following ? "partial" : sessions.status === "ready" ? "completed" : "partial", following,
      { sessionsProcessed: 1 });
  }

  try {
    const current = await Promise.all(observations.map(observation => kv.get<unknown>(KV.observations(sessionId), observation.id)));
    if (current.some((value, index) => value === null || canonical(value) !== canonical(observations[index]))) {
      return response(project, "failed", retryCursor, { error: "observation_changed" });
    }
  } catch (error) {
    logger.warn("graph-build observation validation failed", { project, code: errorCode(error) });
    return response(project, "failed", retryCursor, { error: "observation_page_unavailable" });
  }

  const graphJobId = fingerprintId("graphbuild", canonical({
    version: 1, project, sessionId, observations,
    graphProvenanceVersion: 2, graphWorkGroupsVersion: 2,
  }));
  if (cursor.expectedGraphJobId !== null && cursor.expectedGraphJobId !== graphJobId) {
    return response(project, "failed", null, { graphJobId, error: "observation_changed" });
  }
  const replayCursor = nextCursor(project, cursor.sessionCursor, cursor.sessionIndex, sessionId,
    cursor.observationCursor, graphJobId);
  let extracted: unknown;
  try {
    extracted = await sdk.trigger<Record<string, unknown>, unknown>({
      function_id: "mem::graph-extract",
      payload: {
        observations, sessionId, graphJobId,
        graphProvenanceVersion: 2, graphWorkGroupsVersion: 2,
      },
    });
  } catch (error) {
    logger.warn("graph-build extraction failed", { project, sessionId, graphJobId, code: errorCode(error) });
    return response(project, "failed", replayCursor, { graphJobId, error: "graph_extraction_failed" });
  }
  if (!isRecord(extracted)) {
    return response(project, "failed", replayCursor, { graphJobId, error: "graph_extraction_failed" });
  }
  let durableJob = await kv.get<{ automaticRecovery?: GraphAutomaticRecoveryView }>(KV.graphJobs, graphJobId);
  const terminalRecovery = await readGraphStoppedTerminal(kv, graphJobId);
  if (terminalRecovery?.link) {
    durableJob = await kv.get<typeof durableJob>(KV.graphJobs, terminalRecovery.link.jobId)
      ?? (terminalRecovery.recovery ? { automaticRecovery: terminalRecovery.recovery } : null);
  } else if (!durableJob?.automaticRecovery && terminalRecovery?.recovery) {
    durableJob = { automaticRecovery: terminalRecovery.recovery };
  }
  if (durableJob?.automaticRecovery?.replacementJobId) {
    durableJob = await kv.get<typeof durableJob>(KV.graphJobs, durableJob.automaticRecovery.replacementJobId) ?? durableJob;
  }
  let automaticRecovery = (isRecord(extracted.automaticRecovery) && extracted.automaticRecovery.version === 1
    ? extracted.automaticRecovery as unknown as GraphAutomaticRecoveryView : durableJob?.automaticRecovery);
  if (automaticRecovery?.captured && extracted.graphContinuation === true && automaticRecovery.state === "completed") {
    automaticRecovery = { ...automaticRecovery, action: "continue", nextCheckAt: null };
  }
  const counts = { sessionsProcessed: 1, observationsProcessed: observations.length,
    nodesAdded: count(extracted.nodesAdded), edgesAdded: count(extracted.edgesAdded), graphJobId,
    ...(isRecord(extracted.automaticTerminalRecovery) && extracted.automaticTerminalRecovery.version === 1
      ? { automaticTerminalRecovery: extracted.automaticTerminalRecovery as unknown as GraphTerminalRecoveryView }
      : terminalRecovery ? { automaticTerminalRecovery: terminalRecovery.outcome } : {}),
    ...(automaticRecovery ? { automaticRecovery } : {}) };
  if (extracted.queued === true || extracted.deferred === true || extracted.graphContinuation === true
    || extracted.paused === true || extracted.reason === "selected_foreground_required") {
    return response(project, "queued", replayCursor, counts);
  }
  if (extracted.success !== true || extracted.failed === true || extracted.incomplete === true) {
    return response(project, "failed", replayCursor, { ...counts, error: "graph_extraction_failed" });
  }
  return response(project, following || sessions.status !== "ready" ? "partial" : "completed", following, counts);
}

export function graphBuildHttpStatus(result: GraphBuildResponse): number {
  if (result.status !== "failed") return 200;
  if (result.error === "invalid_continuation") return 400;
  if (result.error === "continuation_project_mismatch" || result.error === "project_session_changed"
    || result.error === "observation_changed") return 409;
  if (result.error === "project_catalog_unavailable" || result.error === "observation_page_unavailable") return 503;
  return 502;
}
