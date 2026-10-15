import { beforeEach, describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation } from "../src/types.js";

const secret = "scoped-graph-build-test-secret";
const project = "D:/repo/agentmemory";
const sessionId = "session-a";
const observations: CompressedObservation[] = [
  { id: "obs-a", sessionId, title: "compressed A", type: "tool_use", narrative: "A", facts: [], concepts: [], files: [], importance: 1 },
  { id: "obs-b", sessionId, title: "compressed B", type: "tool_use", narrative: "B", facts: [], concepts: [], files: [], importance: 1 },
];

type ApiResponse = { status_code: number; body: Record<string, unknown> };
type ApiHandler = (request: { headers?: Record<string, string>; body?: unknown }) => Promise<ApiResponse>;
type SessionPage = {
  items: Array<{ session_id: string; source_key: string; row_version: string; session: { id: string; project: string } }>;
  next_cursor: string | null;
  partial: boolean;
  status: "ready" | "partial" | "not_ready";
  revision: number;
  epoch: number;
};

function projectSessions(rows: SessionPage["items"] = [{
  session_id: sessionId, source_key: sessionId, row_version: "1", session: { id: sessionId, project },
}], status: SessionPage["status"] = "ready"): SessionPage {
  return { items: rows, next_cursor: null, partial: false, status, revision: 1, epoch: 1 };
}

function cursor(overrides: Record<string, unknown> = {}): string {
  return Buffer.from(JSON.stringify({
    version: 1, project, sessionCursor: null, sessionIndex: 0, sessionId, observationCursor: null,
    expectedGraphJobId: null, ...overrides,
  })).toString("base64url");
}

function apiBoundary(options: {
  sessions?: SessionPage;
  sessionPages?: Map<string, SessionPage>;
  pages?: Map<string, { items: unknown[]; next_cursor: string | null }>;
  currentSessionProject?: string;
  sourceObservations?: CompressedObservation[];
  extractResult?: unknown;
  extractError?: Error;
} = {}) {
  const handlers = new Map<string, ApiHandler>();
  const sessions = options.sessions ?? projectSessions();
  const sourceObservations = options.sourceObservations ?? observations;
  const pages = options.pages ?? new Map([["", { items: sourceObservations, next_cursor: null }]]);
  const retrieve = vi.fn(async (request?: { cursor?: string }) =>
    options.sessionPages?.get(request?.cursor ?? "") ?? sessions);
  const statePages = vi.fn((scope: string, pageOptions: { cursor?: string }) => {
    if (scope !== KV.observations(sessionId)) throw new Error("unexpected observation scope");
    const selectedPage = pages.get(pageOptions.cursor ?? "") ?? { items: [], next_cursor: null };
    return (async function* () { yield selectedPage as { items: CompressedObservation[]; next_cursor: string | null }; })();
  });
  const get = vi.fn(async (scope: string, key: string) => {
    if (scope === KV.sessions && key === sessionId) return { id: sessionId, project: options.currentSessionProject ?? project };
    if (scope === KV.observations(sessionId)) return sourceObservations.find(item => item.id === key) ?? null;
    return null;
  });
  const trigger = vi.fn(async () => {
    if (options.extractError) throw options.extractError;
    return options.extractResult ?? { success: true, nodesAdded: 2, edgesAdded: 1 };
  });
  const sdk = {
    registerFunction: (id: string, handler: ApiHandler) => handlers.set(id, handler),
    registerTrigger: vi.fn(),
    trigger,
  };
  registerApiTriggers(sdk as never, { retrieval: retrieve, pages: statePages, get } as never, secret);
  return {
    handle: handlers.get("api::graph-build")!, retrieve, pages: statePages, get, trigger,
  };
}

function authorized(body: unknown) {
  return { headers: { authorization: `Bearer ${secret}` }, body };
}

describe("project-scoped graph rebuild endpoint", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("authenticates before any project or graph read", async () => {
    const boundary = apiBoundary();
    const result = await boundary.handle({ headers: { authorization: "Bearer wrong" }, body: { project } });

    expect(result).toEqual({ status_code: 401, body: { error: "unauthorized" } });
    expect(boundary.retrieve).not.toHaveBeenCalled();
    expect(boundary.pages).not.toHaveBeenCalled();
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it.each([
    ["missing project", {}],
    ["blank project", { project: "  " }],
    ["wrong project type", { project: 7 }],
    ["unrecognized control", { project, selected: { providerCalls: 1 } }],
    ["invalid continuation type", { project, continuation: { sessionId } }],
  ])("rejects %s before reading project data", async (_label, body) => {
    const boundary = apiBoundary();
    const result = await boundary.handle(authorized(body));

    expect(result).toEqual({ status_code: 400, body: { error: "invalid_graph_build_request" } });
    expect(boundary.retrieve).not.toHaveBeenCalled();
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("extracts one exact current session page and returns its continuation", async () => {
    const rows = [
      { id: "raw", sessionId, type: "tool_use" },
      ...observations,
    ];
    const boundary = apiBoundary({ pages: new Map([["", { items: rows, next_cursor: "obs-page-2" }]]) });
    const result = await boundary.handle(authorized({ project }));

    expect(result.status_code).toBe(200);
    expect(result.body).toMatchObject({ status: "partial", project, sessionsProcessed: 1,
      observationsProcessed: 2, nodesAdded: 2, edgesAdded: 1 });
    expect(typeof result.body.continuation).toBe("string");
    expect(boundary.retrieve).toHaveBeenCalledExactlyOnceWith({
      action: "project_sessions_page", project, limit: 12, max_bytes: 65_536,
    });
    expect(boundary.pages).toHaveBeenCalledExactlyOnceWith(KV.observations(sessionId), {
      limit: 25, maxBytes: 1_048_576,
    });
    expect(boundary.get).toHaveBeenCalledWith(KV.sessions, sessionId);
    expect(boundary.get).toHaveBeenCalledWith(KV.observations(sessionId), "obs-a");
    expect(boundary.get).toHaveBeenCalledWith(KV.observations(sessionId), "obs-b");
    expect(boundary.trigger).toHaveBeenCalledExactlyOnceWith({
      function_id: "mem::graph-extract",
      payload: expect.objectContaining({
        sessionId, observations, graphProvenanceVersion: 2, graphWorkGroupsVersion: 2,
      }),
    });
    expect((boundary.trigger.mock.calls[0][0] as { payload: Record<string, unknown> }).payload)
      .not.toHaveProperty("selected");
  });

  it("binds a cursor to its project and selected session", async () => {
    const boundary = apiBoundary();
    const foreignProject = await boundary.handle(authorized({
      project, continuation: cursor({ project: "D:/repo/other" }),
    }));
    expect(foreignProject.status_code).toBe(409);
    expect(foreignProject.body.error).toBe("continuation_project_mismatch");
    expect(boundary.retrieve).not.toHaveBeenCalled();

    const foreignSession = await boundary.handle(authorized({
      project, continuation: cursor({ sessionId: "session-from-another-project" }),
    }));
    expect(foreignSession.status_code).toBe(409);
    expect(foreignSession.body.error).toBe("project_session_changed");
    expect(boundary.pages).not.toHaveBeenCalled();
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("does not extract if the selected session changed projects", async () => {
    const boundary = apiBoundary({ currentSessionProject: "D:/repo/other" });
    const result = await boundary.handle(authorized({ project }));

    expect(result.status_code).toBe(409);
    expect(result.body).toMatchObject({ status: "failed", error: "project_session_changed" });
    expect(boundary.pages).not.toHaveBeenCalled();
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("advances empty pages once and never extracts uncompressed observations", async () => {
    const boundary = apiBoundary({ pages: new Map([
      ["", { items: [{ id: "raw", sessionId, type: "tool_use" }], next_cursor: "obs-page-2" }],
      ["obs-page-2", { items: observations.slice(0, 1), next_cursor: null }],
    ]) });
    const first = await boundary.handle(authorized({ project }));
    expect(first.body.status).toBe("partial");
    expect(first.body.observationsProcessed).toBe(0);
    expect(boundary.trigger).not.toHaveBeenCalled();

    const second = await boundary.handle(authorized({ project, continuation: first.body.continuation }));
    expect(second.body.status).toBe("completed");
    expect(second.body.observationsProcessed).toBe(1);
    expect(boundary.trigger).toHaveBeenCalledTimes(1);
  });

  it("advances past an empty partial project-session page without looping", async () => {
    const firstPage: SessionPage = { ...projectSessions(), next_cursor: "session-page-2", status: "partial" };
    const emptyMiddlePage: SessionPage = {
      items: [], next_cursor: "session-page-3", partial: true, status: "partial", revision: 1, epoch: 1,
    };
    const boundary = apiBoundary({
      sessions: firstPage,
      sessionPages: new Map([
        ["", firstPage],
        ["session-page-2", emptyMiddlePage],
      ]),
    });

    const first = await boundary.handle(authorized({ project }));
    expect(first.body.status).toBe("partial");
    const middle = await boundary.handle(authorized({ project, continuation: first.body.continuation }));

    expect(middle.body).toMatchObject({ status: "partial", continuation: expect.any(String) });
    const nextCursor = JSON.parse(Buffer.from(middle.body.continuation as string, "base64url").toString("utf8"));
    expect(nextCursor.sessionCursor).toBe("session-page-3");
    expect(nextCursor.sessionIndex).toBe(0);
    expect(boundary.retrieve).toHaveBeenCalledTimes(3);
    expect(boundary.trigger).toHaveBeenCalledTimes(1);
  });

  it("keeps queued and failed work on the same replay-safe source unit", async () => {
    const queued = apiBoundary({ extractResult: { success: true, queued: true } });
    const initial = await queued.handle(authorized({ project }));
    expect(initial.body.status).toBe("queued");
    expect(typeof initial.body.continuation).toBe("string");
    const retry = await queued.handle(authorized({ project, continuation: initial.body.continuation }));
    const firstPayload = (queued.trigger.mock.calls[0][0] as { payload: Record<string, unknown> }).payload;
    const retryPayload = (queued.trigger.mock.calls[1][0] as { payload: Record<string, unknown> }).payload;
    expect(retry.body.status).toBe("queued");
    expect(retry.body.continuation).toBe(initial.body.continuation);
    expect(retryPayload.graphJobId).toBe(firstPayload.graphJobId);
    expect(queued.trigger).toHaveBeenCalledTimes(2);

    const failed = apiBoundary({ extractError: new Error("provider detail must stay internal") });
    const failedResponse = await failed.handle(authorized({ project }));
    expect(failedResponse.status_code).toBe(502);
    expect(failedResponse.body).toMatchObject({ status: "failed", error: "graph_extraction_failed" });
    expect(failedResponse.body.continuation).toBeTruthy();
    expect(JSON.stringify(failedResponse.body)).not.toContain("provider detail");
    const failedRetry = await failed.handle(authorized({ project, continuation: failedResponse.body.continuation }));
    expect(failedRetry.body.status).toBe("failed");
    expect(failedRetry.body.graphJobId).toBe(failedResponse.body.graphJobId);
    expect(failedRetry.body.continuation).toBe(failedResponse.body.continuation);
    expect(failed.trigger).toHaveBeenCalledTimes(2);
  });

  it("rejects a queued replay when its source content changed", async () => {
    const sourceObservations = observations.map(observation => ({ ...observation }));
    const boundary = apiBoundary({ extractResult: { success: true, queued: true }, sourceObservations });
    const queued = await boundary.handle(authorized({ project }));
    expect(queued.body.status).toBe("queued");
    expect(typeof queued.body.continuation).toBe("string");
    const originalJobId = queued.body.graphJobId;

    sourceObservations[0].narrative = "changed after queued extraction";
    const replay = await boundary.handle(authorized({ project, continuation: queued.body.continuation }));

    expect(replay.status_code).toBe(409);
    expect(replay.body).toMatchObject({ status: "failed", error: "observation_changed", continuation: null });
    expect(replay.body.graphJobId).not.toBe(originalJobId);
    expect(boundary.trigger).toHaveBeenCalledTimes(1);
  });

  it("does not report a queued unit completed after its session leaves the project", async () => {
    const queued = apiBoundary({ extractResult: { success: true, queued: true } });
    const first = await queued.handle(authorized({ project }));
    const removed = apiBoundary({ sessions: projectSessions([]) });

    const replay = await removed.handle(authorized({ project, continuation: first.body.continuation }));

    expect(replay.status_code).toBe(409);
    expect(replay.body).toMatchObject({ status: "failed", error: "project_session_changed", continuation: null });
    expect(replay.body.status).not.toBe("completed");
    expect(removed.trigger).not.toHaveBeenCalled();
  });

  it("reports an empty ready project as completed without invoking extraction", async () => {
    const boundary = apiBoundary({ sessions: projectSessions([]) });
    const result = await boundary.handle(authorized({ project }));

    expect(result).toEqual({ status_code: 200, body: {
      status: "completed", project, sessionsProcessed: 0, observationsProcessed: 0,
      nodesAdded: 0, edgesAdded: 0, continuation: null,
    } });
    expect(boundary.trigger).not.toHaveBeenCalled();
  });
});
