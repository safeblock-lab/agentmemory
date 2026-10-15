import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { viewerPage } from "../src/functions/viewer-memory-page.js";
import { viewerRecentPage } from "../src/functions/viewer-memory-page.js";
import { StatePageError } from "../src/state/state-pages.js";
import { StateKV } from "../src/state/kv.js";
import { InvocationError } from "iii-sdk";
import { registerApiTriggers } from "../src/triggers/api.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));

const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
const projectSource = html.slice(html.indexOf("    async function loadGraphProjects("), html.indexOf("    async function selectGraphProject("));

function projectFixture() {
  const graph = { projects: ["retained"], selectedProject: "retained", projectsLoaded: true,
    sessionNextCursor: "stale", projectInput: "" } as Record<string, unknown>;
  const elements = new Map<string, { innerHTML: string; value: string; addEventListener: ReturnType<typeof vi.fn> }>();
  const element = (id: string) => {
    if (!elements.has(id)) elements.set(id, { innerHTML: "", value: "", addEventListener: vi.fn() });
    return elements.get(id)!;
  };
  const api = vi.fn();
  const selectGraphProject = vi.fn();
  const context = vm.createContext({ state: { graph }, api, encodeURIComponent, esc: String,
    document: { getElementById: element }, selectGraphProject, liveSubscriptionReady: Promise.resolve() });
  vm.runInContext(projectSource, context);
  return { graph, api, context, element, selectGraphProject };
}

describe("viewer session cursor recovery", () => {
  it.each(["STATE_PAGE_CURSOR_STALE", "STATE_PAGE_CURSOR_INVALID"] as const)("preserves strict state error %s without retry in the viewer reader", async code => {
    const pages = vi.fn(async function* () { throw new StatePageError(code); });
    await expect(viewerPage({ pages } as never, "mem:sessions", { cursor: "old" }, ["id"]))
      .rejects.toMatchObject({ code });
    expect(pages).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["STATE_PAGE_CURSOR_STALE", 409, "viewer_cursor_stale"],
    ["STATE_PAGE_CURSOR_INVALID", 400, "invalid_viewer_cursor"],
  ] as const)("maps %s to an explicit session REST response", async (code, status, error) => {
    const pages = vi.fn(async function* () { throw new StatePageError(code); });
    const handlers = new Map<string, (request: unknown) => Promise<unknown>>();
    registerApiTriggers({ registerFunction: (id: string, fn: (request: unknown) => Promise<unknown>) => handlers.set(id, fn),
      registerTrigger: vi.fn() } as never, { pages } as never, "test-secret");
    const response = await handlers.get("api::sessions")!({ headers: { authorization: "Bearer test-secret" },
      query_params: { cursor: "old", agentId: "*" } });
    expect(response).toMatchObject({ status_code: status, body: { error } });
    if (status === 409) expect(response).toMatchObject({ body: { restartRequired: true } });
    expect(pages).toHaveBeenCalledTimes(1);
  });

  it("does not convert storage failures into stale recovery or empty success", async () => {
    const pages = vi.fn(async function* () { throw new StatePageError("STATE_PAGE_FAILED"); });
    const handlers = new Map<string, (request: unknown) => Promise<unknown>>();
    registerApiTriggers({ registerFunction: (id: string, fn: (request: unknown) => Promise<unknown>) => handlers.set(id, fn),
      registerTrigger: vi.fn() } as never, { pages } as never, "test-secret");
    await expect(handlers.get("api::sessions")!({ headers: { authorization: "Bearer test-secret" }, query_params: { agentId: "*" } }))
      .rejects.toMatchObject({ code: "STATE_PAGE_FAILED" });
  });

  it("restarts once on stale and retains projects and selection across later pages", async () => {
    const { graph, api, context } = projectFixture();
    api.mockResolvedValueOnce({ error: "viewer_cursor_stale", restartRequired: true })
      .mockResolvedValueOnce({ sessions: [{ project: "first" }], nextCursor: "fresh" });
    await vm.runInContext("loadGraphProjects('stale')", context);
    expect(api.mock.calls.map(call => call[0])).toEqual(["sessions?latest=true&limit=16&cursor=stale", "sessions?latest=true&limit=16"]);
    expect(graph).toMatchObject({ projects: ["first", "retained"], selectedProject: "retained", sessionNextCursor: "fresh", projectsLoading: false });
    expect(graph.projectNotice).toContain("browsing restarted");
    api.mockResolvedValueOnce({ sessions: [{ project: "second" }, { project: "first" }], nextCursor: null });
    await vm.runInContext("loadGraphProjects('fresh')", context);
    expect(graph.projects).toEqual(["first", "retained", "second"]);
    expect(graph.selectedProject).toBe("retained");
    expect(graph.sessionNextCursor).toBeNull();
  });

  it("stops after a failed restart, clears stale continuation, and keeps known projects usable", async () => {
    const { graph, api, context, element } = projectFixture();
    api.mockResolvedValue({ error: "viewer_cursor_stale", restartRequired: true });
    await vm.runInContext("loadGraphProjects('stale')", context);
    expect(api).toHaveBeenCalledTimes(2);
    expect(graph).toMatchObject({ projects: ["retained"], selectedProject: "retained", sessionNextCursor: null, projectsLoading: false });
    expect(element("graph-project-toolbar").innerHTML).toContain("Retry projects");
    expect(element("graph-project-toolbar").innerHTML).not.toContain('data-action="graph-source-next"');
  });

  it("does not restart transport/auth/invalid-cursor failures", async () => {
    for (const result of [null, { error: "unauthorized" }, { error: "invalid_viewer_cursor" }]) {
      const { api, context, graph } = projectFixture();
      api.mockResolvedValue(result);
      await vm.runInContext("loadGraphProjects('stale')", context);
      expect(api).toHaveBeenCalledTimes(1);
      expect(graph.projectError).toContain("could not be loaded");
      expect(graph.projects).toEqual(["retained"]);
    }
  });

  it("prevents duplicate discovery requests while showing loading state", async () => {
    const { graph, api, context, element } = projectFixture();
    let resolve: (result: unknown) => void = () => {};
    api.mockImplementation(() => new Promise(done => { resolve = done; }));
    const pending = vm.runInContext("loadGraphProjects('stale')", context);
    await vm.runInContext("loadGraphProjects('stale')", context);
    expect(api).toHaveBeenCalledTimes(1);
    expect(element("graph-project-toolbar").innerHTML).toContain('disabled>Browse more projects');
    expect(element("graph-project-toolbar").innerHTML).toContain("Loading projects...");
    resolve({ sessions: [], nextCursor: null });
    await pending;
    expect(graph.projectsLoading).toBe(false);
  });

  it("opens an exact undiscovered project, including keyboard activation, without any session scan", async () => {
    const { graph, api, context, element, selectGraphProject } = projectFixture();
    vm.runInContext("renderGraphProjectPicker()", context);
    element("graph-project-name").value = "  proofproject  ";
    const keyHandler = element("graph-project-name").addEventListener.mock.calls.find(call => call[0] === "keydown")![1];
    const preventDefault = vi.fn();
    keyHandler({ key: "Enter", preventDefault });
    expect(preventDefault).toHaveBeenCalledOnce();
    expect(selectGraphProject).toHaveBeenCalledWith("proofproject");
    expect(graph.projects).toEqual(["proofproject", "retained"]);
    expect(graph.projectInput).toBe("proofproject");
    expect(api).not.toHaveBeenCalled();
  });

  it("uses labelled responsive controls and announces incomplete/restarted discovery", () => {
    const { context, element } = projectFixture();
    vm.runInContext("renderGraphProjectPicker()", context);
    const toolbar = element("graph-project-toolbar").innerHTML;
    expect(toolbar).toContain('label for="graph-project-name"');
    expect(toolbar).toContain('max-width:100%;min-width:0;');
    expect(toolbar).toContain('role="status"');
    expect(toolbar).toContain("list may be incomplete");
    expect(html).toContain("if (action === 'graph-project-open') { openGraphProject(); return; }");
  });

  it("opens the typed project through scoped graph reads without a graph build", async () => {
    const { context, element, api, graph } = projectFixture();
    Object.assign(context, { GRAPH_INITIAL_LIMIT: 500, graphSim: { raf: null },
      renderGraphSidebar: vi.fn(), initGraph: vi.fn() });
    api.mockResolvedValue({ status: "ready", nodes: [], edges: [], hasMore: false });
    const source = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    vm.runInContext(source, context);
    vm.runInContext("state.activeTab = 'graph'", context);
    element("graph-project-name").value = "proofproject";
    vm.runInContext("openGraphProject()", context);
    await vi.waitFor(() => expect(graph.browseStatus).toBe("ready"));
    expect(api).toHaveBeenCalledExactlyOnceWith("graph/query", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ project: "proofproject", display: true, limit: 500 }), readErrorBody: true });
  });
});

type RecentRequest = {
  function_id: string;
  payload: Record<string, unknown>;
  timeoutMs?: number;
};

function recentKV(handler: (request: RecentRequest) => unknown | Promise<unknown>) {
  const trigger = vi.fn((request: RecentRequest) => Promise.resolve(handler(request)));
  return { kv: new StateKV({ trigger } as never), trigger };
}

function recentItem(entity: "sessions" | "observations", sessionId = "session-a") {
  const value = entity === "sessions" ? { id: sessionId } : { id: "observation-a", sessionId };
  return { scope: entity === "sessions" ? "mem:sessions" : `mem:obs:${sessionId}`,
    key: value.id, version: "4", value, projection: { selected: true } };
}

function recentPage(status: "preparing" | "partial" | "ready", items: unknown[] = []) {
  return { status, items, has_more: false, next_cursor: null, ordering: "latest_first",
    reason: status === "partial" ? "source_issues" : null };
}

type RecentPreparationFixture = {
  status: "preparing" | "partial" | "ready";
  processed: number;
  continue: boolean;
  cursor: { phase: "observations"; scanned: boolean; generation: string };
  reason: string | null;
  source_bytes: number;
  decoded_bytes: number;
  required_bytes: number | null;
  elapsed_ms: number;
  batch_limits: { rows: number; decoded_bytes: number; safety_ms: number };
  preparation: { target: "viewer_recent"; status: "preparing" | "partial" | "ready"; checkpoint: string;
    generation: string; scanned: boolean; dirty: number; issues: number; ready: boolean; has_more: boolean };
};

function recentPreparation(status: "preparing" | "partial" | "ready"): RecentPreparationFixture {
  const scanned = status !== "preparing";
  const ready = status === "ready";
  const issues = status === "partial" ? 1 : 0;
  return { status, processed: ready ? 4096 : 0, continue: !scanned,
    cursor: { phase: "observations", scanned, generation: "3" }, reason: null,
    source_bytes: 0, decoded_bytes: 0, required_bytes: null, elapsed_ms: 12,
    batch_limits: { rows: 4096, decoded_bytes: 33_554_432, safety_ms: 60_000 },
    preparation: { target: "viewer_recent", status, checkpoint: "a".repeat(64), generation: "3",
      scanned, dirty: 0, issues, ready, has_more: !ready } };
}

describe("viewer recent preparation caller", () => {
  it("uses native preparation defaults, the typed SDK timeout, and accepts larger bounded cuts", async () => {
    let pages = 0;
    const { kv, trigger } = recentKV(({ payload }) => {
      if (payload.action === "viewer_recent_page") {
        pages++;
        return pages === 1 ? recentPage("preparing")
          : recentPage("ready", [recentItem("sessions")]);
      }
      if (payload.action === "viewer_recent_prepare") return recentPreparation("ready");
      throw new Error(`unexpected retrieval action: ${String(payload.action)}`);
    });

    const result = await viewerRecentPage(kv, "sessions", { latest: "true" });

    expect(result).toMatchObject({ status: "ready", partial: false, items: [{ id: "session-a" }] });
    const preparationRequest = trigger.mock.calls[1]![0];
    expect(preparationRequest.payload).toEqual({ action: "viewer_recent_prepare" });
    expect(preparationRequest.timeoutMs).toBe(70_000);
    expect(trigger.mock.calls[0]![0]).not.toHaveProperty("timeoutMs");
    expect(result.preparation?.processed).toBe(4096);
  });

  it("surfaces a zero-progress decode cut without issuing a repeated preparation", async () => {
    const blocked = recentPreparation("preparing");
    blocked.reason = "decode_budget_exceeded";
    blocked.required_bytes = 8_192;
    blocked.batch_limits.decoded_bytes = 1_024;
    const { kv, trigger } = recentKV(({ payload }) => payload.action === "viewer_recent_page"
      ? recentPage("preparing") : blocked);

    const result = await viewerRecentPage(kv, "sessions", { latest: "true" });

    expect(result).toMatchObject({ status: "preparing", partial: true, total: null,
      preparation: { processed: 0, reason: "decode_budget_exceeded", required_bytes: 8_192 } });
    expect(trigger.mock.calls.map(([request]) => request.payload.action))
      .toEqual(["viewer_recent_page", "viewer_recent_prepare"]);
  });

  it("accepts required-byte metadata when a decode cut made positive row progress", async () => {
    const bounded = recentPreparation("preparing");
    bounded.processed = 2;
    bounded.reason = "decode_budget_exceeded";
    bounded.source_bytes = 32_000_000;
    bounded.decoded_bytes = 32_000_000;
    bounded.required_bytes = 4_194_304;
    const { kv, trigger } = recentKV(({ payload }) => payload.action === "viewer_recent_page"
      ? recentPage("preparing") : bounded);

    const result = await viewerRecentPage(kv, "sessions", { latest: "true" });

    expect(result).toMatchObject({ status: "preparing", partial: true,
      preparation: { processed: 2, reason: "decode_budget_exceeded", required_bytes: 4_194_304 } });
    expect(trigger.mock.calls.map(([request]) => request.payload.action))
      .toEqual(["viewer_recent_page", "viewer_recent_prepare"]);
  });

  it("reconciles a preparation timeout once and never retries an unknown mutation", async () => {
    const { kv, trigger } = recentKV(({ payload }) => {
      if (payload.action === "viewer_recent_page") return recentPage("preparing");
      if (payload.action === "viewer_recent_prepare") throw new InvocationError({ code: "TIMEOUT", message: "late ACK" });
      if (payload.action === "preparation_status") throw new InvocationError({ code: "TIMEOUT", message: "status unavailable" });
      throw new Error(`unexpected retrieval action: ${String(payload.action)}`);
    });

    const result = await viewerRecentPage(kv, "sessions", { latest: "true" });

    expect(result).toMatchObject({ status: "preparing", partial: true,
      preparationReconciliation: { state: "unknown", reason: "TIMEOUT" } });
    expect(trigger.mock.calls.map(([request]) => request.payload.action))
      .toEqual(["viewer_recent_page", "viewer_recent_prepare", "preparation_status"]);
    expect(trigger.mock.calls.filter(([request]) => request.payload.action === "viewer_recent_prepare")).toHaveLength(1);
    expect(trigger.mock.calls[2]![0]).not.toHaveProperty("timeoutMs");
  });

  it("uses a completed read-only reconciliation to refresh partial data after a timed-out cut", async () => {
    let pages = 0;
    const { kv, trigger } = recentKV(({ payload }) => {
      if (payload.action === "viewer_recent_page") {
        pages++;
        return pages === 1 ? recentPage("preparing")
          : recentPage("partial", [recentItem("sessions")]);
      }
      if (payload.action === "viewer_recent_prepare") throw new InvocationError({ code: "TIMEOUT", message: "late ACK" });
      if (payload.action === "preparation_status") return recentPreparation("partial").preparation;
      throw new Error(`unexpected retrieval action: ${String(payload.action)}`);
    });

    const result = await viewerRecentPage(kv, "sessions", { latest: "true" });

    expect(result).toMatchObject({ status: "partial", partial: true, total: null,
      items: [{ id: "session-a" }], preparationReconciliation: { state: "reconciled", status: "partial", has_more: true } });
    expect(trigger.mock.calls.map(([request]) => request.payload.action))
      .toEqual(["viewer_recent_page", "viewer_recent_prepare", "preparation_status", "viewer_recent_page"]);
    expect(trigger.mock.calls.filter(([request]) => request.payload.action === "viewer_recent_prepare")).toHaveLength(1);
  });

  it("returns useful partial data after exhausted preparation without inferring readiness", async () => {
    let pages = 0;
    const { kv, trigger } = recentKV(({ payload }) => {
      if (payload.action === "viewer_recent_page") {
        pages++;
        return pages === 1 ? recentPage("preparing")
          : recentPage("partial", [recentItem("observations")]);
      }
      return recentPreparation("partial");
    });

    const result = await viewerRecentPage(kv, "observations", { latest: "true" }, "session-a");

    expect(result).toMatchObject({ status: "partial", partial: true, total: null,
      items: [{ id: "observation-a", sessionId: "session-a" }], preparation: { status: "partial", preparation: { has_more: true } } });
    expect(trigger.mock.calls.map(([request]) => request.payload.action))
      .toEqual(["viewer_recent_page", "viewer_recent_prepare", "viewer_recent_page"]);
  });

  it("rejects native preparation metadata beyond the finite response caps", async () => {
    const invalid = recentPreparation("ready");
    invalid.batch_limits.rows = 16_385;
    const { kv } = recentKV(({ payload }) => payload.action === "viewer_recent_page"
      ? recentPage("preparing") : invalid);

    await expect(viewerRecentPage(kv, "sessions", { latest: "true" }))
      .rejects.toThrow("viewer_invalid_recent_preparation");
  });

  it("rejects a recent page row returned from the wrong scope", async () => {
    const invalidItem = { ...recentItem("sessions"), scope: "mem:obs:session-a" };
    const { kv, trigger } = recentKV(() => recentPage("ready", [invalidItem]));

    await expect(viewerRecentPage(kv, "sessions", { latest: "true" }))
      .rejects.toThrow("viewer_invalid_recent_page");
    expect(trigger).toHaveBeenCalledTimes(1);
  });
});
