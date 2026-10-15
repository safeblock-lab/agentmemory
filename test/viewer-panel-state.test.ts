import { readFileSync } from "node:fs";
import vm from "node:vm";
import { describe, expect, it, vi } from "vitest";

const html = readFileSync("src/viewer/index.html", "utf8");
function source(start: string, end: string) {
  return html.slice(html.indexOf(start), html.indexOf(end));
}
function sandbox(state: Record<string, unknown>, api: (path: string, options?: unknown) => Promise<unknown>, code: string) {
  const element = { innerHTML: "" };
  const context = vm.createContext({ state, api, encodeURIComponent,
    liveSubscriptionReady: Promise.resolve(), document: { getElementById: () => element },
    renderMemories: vi.fn(), renderCrystals: vi.fn(), renderProfile: vi.fn(),
    renderProfileToolbar: vi.fn(), loadProfileData: vi.fn() });
  vm.runInContext(code, context);
  return context;
}
describe("viewer scoped panel state", () => {
  function dashboardHarness(loaded = false) {
    const element = { innerHTML: "" };
    const dashboard = { loaded, stale: false, sessions: [{ id: "retained" }], sessionsNotice: "" };
    const api = vi.fn(async (): Promise<unknown> => ({ status: "healthy" }));
    const recentViewerPage = vi.fn(async (): Promise<unknown> => null);
    const retry = vi.fn();
    const context = vm.createContext({ state: { dashboard }, document: { getElementById: () => element },
      api, apiGet: async () => ({}), recentViewerPage, loadViewerStatus: async () => ({}),
      recentHistoryStates: { sessions: "preparing" }, liveSessionSeq: 0, memoryCountRequest: 0,
      dashboardLoading: false, dashboardReloadPending: false, scheduleStaleRetry: retry,
      scheduleDashboardReload: vi.fn(), replaySessionEvents: (rows: unknown) => rows,
      listOr: (previous: unknown) => previous, renderDashboard: vi.fn(), esc: (text: string) => text, console });
    vm.runInContext(source("    function dashboardRecentNotice(", "    var LAYER_STATE_BADGES"), context);
    return { context, dashboard, element, api, recentViewerPage, retry };
  }

  it("shows reachable preparation and retries on initial dashboard load", async () => {
    const { context, dashboard, element, retry } = dashboardHarness();
    await context.loadDashboard();
    expect(element.innerHTML).toContain("API is reachable");
    expect(element.innerHTML).toContain("still preparing");
    expect(element.innerHTML).toContain('role="status"');
    expect(element.innerHTML).toContain('data-action="refresh-dashboard"');
    expect(element.innerHTML).not.toContain("server may be restarting");
    expect(dashboard.loaded).toBe(false);
    expect(dashboard.stale).toBe(true);
    expect(retry).toHaveBeenCalledOnce();
  });

  it("keeps loaded sessions during preparation and recovers on the next ready response", async () => {
    const { context, dashboard, recentViewerPage } = dashboardHarness(true);
    await context.loadDashboard();
    expect(dashboard.sessions).toEqual([{ id: "retained" }]);
    expect(dashboard.sessionsNotice).toContain("still preparing");
    recentViewerPage.mockResolvedValueOnce({ status: "ready", sessions: [{ id: "fresh" }] });
    await context.loadDashboard();
    expect(dashboard.sessions).toEqual([{ id: "fresh" }]);
    expect(dashboard.sessionsNotice).toBe("");
    expect(dashboard.stale).toBe(false);
  });

  it("reserves API unreachable wording for failed transport with no reachable health", async () => {
    const { context, api, element } = dashboardHarness();
    api.mockResolvedValueOnce(null);
    context.recentHistoryStates.sessions = "transport";
    await context.loadDashboard();
    expect(element.innerHTML).toContain("Could not reach the agentmemory API");
    expect(element.innerHTML).not.toContain("still preparing");
  });

  it("retains fetch readiness state without presenting preparing sessions as an empty page", async () => {
    const api = vi.fn(async () => ({ status: "preparing", reason: "recent_index_not_ready", sessions: [] }));
    const context = vm.createContext({ api, liveSubscriptionReady: Promise.resolve(), AbortSignal,
      observationSeq: 0, liveSessionSeq: 0, LIVE_BUFFER_MAX: 200, LIVE_SESSION_LOG_MAX: 500,
      recentHistoryReasons: {}, recentHistoryStates: {} });
    vm.runInContext(source("    async function fetchRecentViewerPage(", "    function looksLikeObservation("), context);
    expect(await context.fetchRecentViewerPage("sessions")).toBe(null);
    expect(context.recentHistoryStates.sessions).toBe("preparing");
    expect(context.recentHistoryReasons.sessions).toBe("recent_index_not_ready");
  });

  const memories = source("    async function loadMemories(", "    function renderMemories(");
  it("retains memory rows when the page read fails", async () => {
    const state = { memories: { loaded: true, items: [{ id: "retained" }], error: "" } };
    const context = sandbox(state, async () => null, memories);
    await context.loadMemories();
    expect(state.memories.items).toEqual([{ id: "retained" }]);
    expect(state.memories.error).toContain("failed");
  });
  it("binds memory requests to scope and rejects a late prior scope", async () => {
    let finish: (value: unknown) => void = () => {};
    const state = { memories: { loaded: true, project: "old", items: [] as { id: string }[] } };
    const api = vi.fn(async (path: string) => path.includes("project=old")
      ? new Promise(resolve => { finish = resolve; })
      : { memories: [{ id: "new" }], partial: false, nextCursor: null });
    const context = sandbox(state, api, memories);
    const pending = context.loadMemories();
    state.memories.project = "new";
    await context.loadMemories();
    finish({ memories: [{ id: "old" }] }); await pending;
    expect(state.memories.items).toEqual([{ id: "new" }]);
    expect(api.mock.calls[1][0]).toContain("project=new");
  });
  it("keeps discovered projects and selection across pages", async () => {
    const state = { profile: { loaded: true, projects: ["earlier"], selectedProject: "earlier" } };
    const context = sandbox(state, async () => ({ status: "ready", sessions: [{ project: "later" }], nextCursor: "next" }),
      source("    async function loadProfile(", "    function renderProfileToolbar("));
    await context.loadProfile("cursor");
    expect(Array.from(state.profile.projects)).toEqual(["earlier", "later"]);
    expect(state.profile.selectedProject).toBe("earlier");
  });
  it("rejects late profile content after project switch", async () => {
    let finish: (value: unknown) => void = () => {};
    const state = { profile: { selectedProject: "old", data: null as null | { marker: string } } };
    const context = sandbox(state, async (path: string) => path.includes("old")
      ? new Promise(resolve => { finish = resolve; }) : { profile: { marker: "new" } },
      source("    async function loadProfileData(", "    function renderProfile("));
    const pending = context.loadProfileData(); state.profile.selectedProject = "new";
    await context.loadProfileData(); finish({ profile: { marker: "old" } }); await pending;
    expect(state.profile.data?.marker).toBe("new");
  });
  it("retains crystals and reports an unsupported unbounded envelope", async () => {
    const state = { crystals: { loaded: true, items: [{ id: "retained" }], error: "" } };
    const context = sandbox(state, async () => ({ crystals: [] }),
      source("    async function loadCrystals(", "    function renderCrystals("));
    await context.loadCrystals();
    expect(state.crystals.items).toEqual([{ id: "retained" }]);
    expect(state.crystals.error).toContain("unavailable");
  });
  it("keeps rows beyond the first rendered20 accessible within a200-row bound", () => {
    const context = vm.createContext({});
    vm.runInContext(source("    function latestTimelineObservations(", "    var tlTypeFilter"), context);
    const rows = Array.from({ length: 230 }, (_, i) => ({ id: String(i), timestamp: new Date(1e9 + i).toISOString() }));
    const retained = context.latestTimelineObservations(rows);
    expect(retained).toHaveLength(200);
    expect(retained[199].id).toBe("30");
    expect(rows).toHaveLength(230);
  });
});
