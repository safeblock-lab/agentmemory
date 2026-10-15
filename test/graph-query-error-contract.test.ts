import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../src/config.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/config.js")>();
  return { ...actual, isGraphExtractionEnabled: vi.fn(() => true) };
});

import { isGraphExtractionEnabled } from "../src/config.js";
import { registerApiTriggers } from "../src/triggers/api.js";
import { InvocationError } from "iii-sdk";

const secret = "graph-query-error-test-secret";
type Request = { headers?: Record<string, string>; body?: unknown; query_params?: Record<string, string> };
type ApiResponse = { status_code: number; body: unknown };
type Handler = (request: Request) => Promise<ApiResponse>;

function apiBoundary(triggerImpl: (request: unknown) => Promise<unknown>) {
  const handlers = new Map<string, Handler>();
  const trigger = vi.fn(triggerImpl);
  const sdk = {
    registerFunction: (id: string, handler: Handler) => handlers.set(id, handler),
    registerTrigger: vi.fn(),
    trigger,
  };
  registerApiTriggers(sdk as never, { get: vi.fn(), list: vi.fn() } as never, secret);
  return { handlers, trigger };
}

function authorized(request: Request = {}): Request {
  return { ...request, headers: { authorization: `Bearer ${secret}`, ...request.headers } };
}

describe("graph query HTTP error contract", () => {
  beforeEach(() => vi.mocked(isGraphExtractionEnabled).mockReturnValue(true));
  afterEach(() => vi.restoreAllMocks());

  it("whitelists opt-in display browsing without forwarding unrelated fields", async () => {
    const boundary = apiBoundary(async () => ({ display: true, nodes: [], edges: [] }));
    const result = await boundary.handlers.get("api::graph-query")!(authorized({ body: {
      project: " project-a ", display: true, cursor: "opaque", limit: 128, secret: "discard",
    } }));
    expect(result.status_code).toBe(200);
    expect(boundary.trigger).toHaveBeenCalledWith({ function_id: "mem::graph-query", payload: expect.objectContaining({ project: "project-a", display: true, cursor: "opaque", limit: 128 }) });
    expect(JSON.stringify(boundary.trigger.mock.calls)).not.toContain("discard");
  });

  it.each([
    { display: true }, { project: "a", display: "true" },
    { project: "a", cursor: "opaque" }, { project: "a", display: true, cursor: "x".repeat(4097) },
    { project: "a", display: true, offset: 0 }, { project: "a", display: true, query: "x" },
    { project: "a", display: true, startNodeId: "x" }, { project: "a", display: true, limit: 129 },
    { project: "a", prepare: true }, { project: "a", display: false, prepare: false },
    { project: "a", display: true, prepare: "true" },
    { project: "a", display: true, prepare: true, cursor: "opaque" },
    { project: "a", display: true, prepare: true, limit: 1 },
    { project: "a", display: true, prepare: true, nodeType: "" },
    { project: "a", display: true, prepare: true, maxDepth: 1 },
    { project: "a", display: true, cursor: "😀".repeat(1025) },
  ])("rejects ambiguous or invalid display selectors before dispatch: %j", async body => {
    const boundary = apiBoundary(async () => { throw new Error("must not dispatch"); });
    expect((await boundary.handlers.get("api::graph-query")!(authorized({ body }))).status_code).toBe(400);
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it.each([
    { status: "preparing", phase: "memberships", processed: 16, revision: "1:1", continue: true },
    { status: "preparing", phase: "prerequisites", processed: 0, revision: null, continue: false, reason: "index_not_ready" },
    { status: "error", phase: "display", processed: 0, revision: "1:1", continue: false, reason: "index_record_invalid" },
  ])("whitelists preparation and preserves its distinct envelope: %j", async preparation => {
    const envelope = { display: true, project: "project-a", preparation };
    const boundary = apiBoundary(async () => envelope);
    const result = await boundary.handlers.get("api::graph-query")!(authorized({ body: {
      project: " project-a ", display: true, prepare: true, scope: "arbitrary", max_rows: 99999,
    } }));
    expect(result).toEqual({ status_code: 200, body: envelope });
    expect(boundary.trigger).toHaveBeenCalledTimes(1);
    expect(boundary.trigger).toHaveBeenCalledWith({ function_id: "mem::graph-query", payload: expect.objectContaining({ project: "project-a", display: true, prepare: true }) });
    expect(JSON.stringify(boundary.trigger.mock.calls)).not.toMatch(/arbitrary|max_rows/);
    expect(result.body).not.toHaveProperty("nodes");
  });

  it("authenticates preparation before dispatching the state-changing batch", async () => {
    const boundary = apiBoundary(async () => { throw new Error("must not dispatch"); });
    const result = await boundary.handlers.get("api::graph-query")!({ body: { display: true, prepare: true, project: "a" } });
    expect(result).toEqual({ status_code: 401, body: { error: "unauthorized" } });
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("preserves explicit ready-empty versus preparing read pages", async () => {
    for (const status of ["ready", "preparing", "error"]) {
      const page = { display: true, status, project: "a", nodes: [], edges: [], hasMore: false };
      const boundary = apiBoundary(async () => page);
      const result = await boundary.handlers.get("api::graph-query")!(authorized({ body: { display: true, prepare: false, project: "a" } }));
      expect(result).toEqual({ status_code: 200, body: page });
    }
  });

  it("authenticates before graph reads", async () => {
    const boundary = apiBoundary(async () => { throw new Error("must not be called"); });

    const result = await boundary.handlers.get("api::graph-query")!({ body: { project: "project-a" } });

    expect(result).toEqual({ status_code: 401, body: { error: "unauthorized" } });
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("keeps the disabled hint when the graph feature is actually disabled", async () => {
    vi.mocked(isGraphExtractionEnabled).mockReturnValue(false);
    const boundary = apiBoundary(async () => { throw new Error("internal engine message"); });

    const result = await boundary.handlers.get("api::graph-query")!(authorized({ body: { project: "project-a" } }));

    expect(result.status_code).toBe(503);
    expect(result.body).toMatchObject({ error: "Knowledge graph not enabled", flag: "GRAPH_EXTRACTION_ENABLED" });
    expect(JSON.stringify(result.body)).not.toContain("internal engine message");
  });

  it.each([
    ["api::graph-query", { body: { project: "project-a" } }, "graph_query_resource_limit"],
    ["api::graph-stats", { query_params: { project: "project-a" } }, "graph_stats_resource_limit"],
  ])("returns a sanitized 503 for native resource exhaustion in %s", async (id, request, error) => {
    const nativeError = Object.assign(new Error("internal state path and SQL details"), {
      code: "STATE_RETRIEVAL_RESOURCE_LIMIT",
    });
    const boundary = apiBoundary(async () => { throw nativeError; });

    const result = await boundary.handlers.get(id as string)!(authorized(request as Request));

    expect(result).toEqual({ status_code: 503, body: { error, code: "STATE_RETRIEVAL_RESOURCE_LIMIT" } });
    expect(JSON.stringify(result.body)).not.toContain("internal state path");
  });

  it("maps the SDK InvocationError resource-code prefix without returning its message", async () => {
    const sdkError = new InvocationError({
      code: "invocation_failed",
      message: "STATE_RETRIEVAL_RESOURCE_LIMIT: Native indexed retrieval exceeded its response resource limit.",
      function_id: "mem::graph-query",
    });
    const boundary = apiBoundary(async () => { throw sdkError; });

    const result = await boundary.handlers.get("api::graph-query")!(authorized({ body: { project: "project-a" } }));

    expect(result).toEqual({ status_code: 503, body: {
      error: "graph_query_resource_limit", code: "STATE_RETRIEVAL_RESOURCE_LIMIT",
    } });
    expect(JSON.stringify(result.body)).not.toContain("Native indexed retrieval");
  });

  it.each([
    ["api::graph-query", { body: { project: "project-a" } }, "graph_query_failed"],
    ["api::graph-stats", { query_params: { project: "project-a" } }, "graph_stats_failed"],
  ])("returns a non-success sanitized 502 for other enabled graph errors in %s", async (id, request, error) => {
    const boundary = apiBoundary(async () => { throw new Error("provider token and internal stack details"); });

    const result = await boundary.handlers.get(id as string)!(authorized(request as Request));

    expect(result).toEqual({ status_code: 502, body: { error } });
    expect(JSON.stringify(result.body)).not.toContain("provider token");
    expect(JSON.stringify(result.body)).not.toContain("internal stack");
  });
});
