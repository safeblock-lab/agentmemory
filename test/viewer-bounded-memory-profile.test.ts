import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import { viewerMemories, viewerPage, viewerPageOptions, viewerProjectSessions } from "../src/functions/viewer-memory-page.js";
import { registerProfileFunction } from "../src/functions/profile.js";
import { registerApiTriggers } from "../src/triggers/api.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
const sessionPage = { status: "ready", items: [{ session_id: "s", source_key: "s", row_version: "1",
  session: { id: "s", project: "p", startedAt: "2026-10-10T00:00:00Z" } }], next_cursor: null, partial: false, revision: 1, epoch: 1 };
function fixture(rows: Record<string, unknown>[], next: string | null = null) {
  const calls: Array<{ scope: string; options: unknown }> = [];
  const retrieval = vi.fn(async (payload: Record<string, unknown>) => {
    if (payload.action === "shared_source_projection" || payload.action === "reflect_sources") {
      if (!Number.isInteger(payload.max_items) || Number(payload.max_items) < 1 || Number(payload.max_items) > 12) throw new Error("STATE_TX_INVALID_REQUEST");
    }
    if (payload.action === "project_sessions_page") return sessionPage;
    return { items: [{ scope: "mem:memories", key: "m", value: { id: "m", title: "Useful", content: "Actual content", isLatest: true } }], deferred: [] };
  });
  const kv = { retrieval, list: vi.fn(() => { throw new Error("unbounded list forbidden"); }),
    pages: async function* (scope: string, options: unknown) { calls.push({ scope, options }); yield { items: rows, next_cursor: next }; throw new Error("second page forbidden"); },
    get: vi.fn(async () => null), set: vi.fn(async (_scope: string, _key: string, value: unknown) => value) };
  return { kv, calls, retrieval };
}
describe("bounded viewer reads", () => {
  it.each(["sessions", "observations"])("keeps %s last continuation total unknown through REST", async kind => {
    const { kv, calls } = fixture([{ id: "s", sessionId: "s", firstPrompt: "Initial task", userPrompt: "Follow-up task" }]);
    const handlers = new Map<string, (request: unknown) => Promise<{ status_code: number; body: unknown }>>();
    registerApiTriggers({ registerFunction: (id: string, fn: typeof handlers extends Map<string, infer H> ? H : never) => handlers.set(id, fn), registerTrigger: vi.fn() } as never, kv as never, "test-secret");
    const result = await handlers.get(`api::${kind}`)!({ headers: { authorization: "Bearer test-secret" },
      query_params: { cursor: "last", sessionId: "s", agentId: "*" } });
    expect(result).toMatchObject({ status_code: 200, body: { partial: true, total: null, nextCursor: null } });
    expect(calls).toHaveLength(1);
    expect(calls[0].options).toMatchObject({ fields: expect.arrayContaining([kind === "sessions" ? "firstPrompt" : "userPrompt"]) });
  });
  it("returns useful global memory content with fixed metadata and payload budgets", async () => {
    const { kv, calls, retrieval } = fixture([{ id: "m", isLatest: true, title: "Useful", content: "Actual content" }], "next");
    const result = await viewerMemories(kv as never, { latest: "true", limit: "2000" });
    expect(result).toMatchObject({ memories: [{ title: "Useful", content: "Actual content" }], total: null, partial: true, nextCursor: "next" });
    expect(calls[0].options).toMatchObject({ limit: 16, maxBytes: 65536 });
    expect(calls[0].options).toMatchObject({ fields: expect.arrayContaining(["title", "content", "version"]) });
    expect(retrieval).not.toHaveBeenCalled();
    expect(kv.list).not.toHaveBeenCalled();
  });
  it("does not invent empty corpus totals from a page without latest memories", async () => {
    const { kv, retrieval } = fixture([{ id: "old", isLatest: false }], "next");
    expect(await viewerMemories(kv as never, { latest: "true" })).toMatchObject({ memories: [], partial: true, total: null });
    expect(retrieval).not.toHaveBeenCalled();
  });
  it("keeps a final continuation page total unknown", async () => {
    const { kv } = fixture([{ id: "m", isLatest: true }]);
    expect(await viewerMemories(kv as never, { cursor: "prior" })).toMatchObject({ partial: true, total: null, nextCursor: null });
  });
  it("preserves audience-unknown legacy and superseded local history without a sharing projection", async () => {
    const { kv, retrieval } = fixture([{ id: "legacy", title: "Legacy content", content: "Saved before project tracking", isLatest: true },
      { id: "old", title: "Superseded content", content: "Prior version", isLatest: false }]);
    expect(await viewerMemories(kv as never, {})).toMatchObject({ memories: [{ id: "legacy" }, { id: "old" }], total: 2 });
    expect(await viewerMemories(kv as never, { latest: "true" })).toMatchObject({ memories: [{ id: "legacy", content: "Saved before project tracking" }], total: 1 });
    expect(retrieval).not.toHaveBeenCalled();
  });
  it.each([{ limit: "0" }, { limit: "nan" }, { offset: "1" }, { cursor: "x".repeat(4097) }])("rejects invalid request %j before reads", query => {
    expect(() => viewerPageOptions(query)).toThrow();
  });
  it("reads only the requested session observation scope once", async () => {
    const { kv, calls } = fixture([{ id: "o", sessionId: "s", title: "Observation" }], "next");
    const page = await viewerPage(kv as never, "mem:obs:s", { limit: "1" }, ["id", "sessionId", "title"]);
    expect(page).toMatchObject({ items: [{ title: "Observation" }], partial: true, nextCursor: "next" });
    expect(calls).toHaveLength(1);
    expect(calls[0].scope).toBe("mem:obs:s");
  });
  it("rejects foreign project session responses before observation reads", async () => {
    const { kv, calls, retrieval } = fixture([]);
    retrieval.mockResolvedValueOnce({ ...sessionPage, items: [{ ...sessionPage.items[0], session: { ...sessionPage.items[0].session, project: "foreign" } }] } as never);
    await expect(viewerProjectSessions(kv as never, "p")).rejects.toThrow("viewer_invalid_project_sessions");
    expect(calls).toHaveLength(0);
  });
  it("propagates oversized or stale state pages instead of empty success", async () => {
    const { kv } = fixture([{ id: "m", isLatest: true }]);
    kv.pages = async function* () { throw new Error("STATE_RECORD_TOO_LARGE"); };
    await expect(viewerMemories(kv as never, {})).rejects.toThrow("STATE_RECORD_TOO_LARGE");
  });
  it("records incomplete pages as partial, with no fake count", async () => {
    const { kv } = fixture([{ id: "m", isLatest: true }], "more");
    expect(await viewerMemories(kv as never, { count: "true" })).toMatchObject({ total: null, latestCount: null, partial: true });
  });
  it("enforces actual native projection max_items=12 while global viewer avoids that primitive", async () => {
    const { kv, retrieval } = fixture(Array.from({ length: 16 }, (_, index) => ({ id: `m${index}`, isLatest: true, content: "Visible" })));
    await expect(retrieval({ action: "shared_source_projection", max_items: 16 })).rejects.toThrow("STATE_TX_INVALID_REQUEST");
    retrieval.mockClear();
    const result = await viewerMemories(kv as never, {});
    expect("memories" in result && result.memories).toHaveLength(16);
    expect(retrieval).not.toHaveBeenCalled();
  });
  it("uses exact project sessions before versioned canonical memory selection", async () => {
    const { kv, retrieval, calls } = fixture([]);
    retrieval.mockResolvedValueOnce(sessionPage as never);
    retrieval.mockResolvedValueOnce({ status: "ready", items: [{ id: "m", row_version: "3" }], next_cursor: null } as never);
    retrieval.mockResolvedValueOnce({ items: [{ scope: "mem:memories", key: "m", value: { id: "m", title: "Project memory", isLatest: true } }], deferred: [] } as never);
    expect(await viewerMemories(kv as never, { project: "p", latest: "true" })).toMatchObject({ memories: [{ title: "Project memory" }], total: 1, partial: false });
    expect(retrieval.mock.calls[1][0]).toMatchObject({ action: "canonical_source_page", source_project: "p", source_session_ids: ["s"] });
    expect(retrieval.mock.calls[2][0]).toMatchObject({ action: "reflect_sources", project: "p", sources: [{ expected_version: "3" }], max_items: 12 });
    expect(calls).toHaveLength(0);
  });
  it("does not hydrate when the project catalog is incomplete", async () => {
    const { kv, retrieval } = fixture([]);
    retrieval.mockResolvedValueOnce({ ...sessionPage, status: "not_ready", items: [], partial: true } as never);
    expect(await viewerMemories(kv as never, { project: "p", latest: "true" })).toMatchObject({ memories: [], total: null, partial: true });
    expect(retrieval).toHaveBeenCalledTimes(1);
  });
  it("rejects project historical listing before selecting current-only canonical sources", async () => {
    const { kv, retrieval } = fixture([]);
    await expect(viewerMemories(kv as never, { project: "p" })).rejects.toThrow("viewer_project_history_requires_native_selector");
    expect(retrieval).not.toHaveBeenCalled();
  });
  it("generates useful bounded profile and preserves partial count scope", async () => {
    const { kv, calls } = fixture([{ id: "o", sessionId: "s", title: "Useful", narrative: "Details", timestamp: "2026-10-10", importance: 8,
      concepts: ["sqlite"], files: ["src/cache.ts"], type: "decision" }], "more");
    let handler: ((data: unknown) => Promise<unknown>) | undefined;
    registerProfileFunction({ registerFunction: (_id: string, fn: typeof handler) => { handler = fn; } } as never, kv as never);
    const result = await handler!({ project: "p" });
    expect(result).toMatchObject({ cached: false, partial: true, profile: { project: "p", sessionCount: 1, totalObservations: 1,
      countScope: "bounded_sources", topConcepts: [{ concept: "sqlite", frequency: 1 }] } });
    expect(calls).toHaveLength(1);
    expect(calls[0].scope).toBe("mem:obs:s");
    expect(kv.list).not.toHaveBeenCalled();
  });
  it("compiles the viewer script with partial status and native next-page controls", () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const script = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(match => match[1]).join("\n");
    expect(() => new vm.Script(script)).not.toThrow();
    expect(html).toContain('role="status"');
    expect(html).toContain("Next memory page");
    expect(html).toContain("timeline-source-next");
  });
  it("retains graph readiness and unknown totals without automatic graph mutation", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    const apiPost = vi.fn(async () => ({ nodes: [], edges: [], truncated: true, totalNodes: 0, totalEdges: 0, warning: "index_not_ready" }));
    const state = { graph: { projectsLoaded: true, projects: ["p"], selectedProject: "p" } as Record<string, unknown> };
    const context = vm.createContext({ state, GRAPH_INITIAL_LIMIT: 500, esc: String, document: { getElementById: () => ({ innerHTML: "", addEventListener: vi.fn() }) },
      apiPost, api: async () => ({ totalNodes: 0, totalEdges: 0 }), renderGraphSidebar: vi.fn(), initGraph: vi.fn() });
    vm.runInContext(source, context);
    await vm.runInContext("loadGraph()", context);
    expect(state.graph).toMatchObject({ queryPartial: true, queryWarning: "index_not_ready", totalNodes: null, totalEdges: null });
    expect(apiPost.mock.calls).toHaveLength(1);
  });
  it("draws index readiness instead of the extraction flag hint on an incomplete graph", () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    function renderGraph()"), html.indexOf("    async function loadMemories"));
    const fillText = vi.fn();
    const ctx = new Proxy({ fillText }, { get: (target, name) => name in target ? target[name as keyof typeof target] : () => undefined });
    const context = vm.createContext({ state: { graph: { filters: {}, queryPartial: true, queryWarning: "index_not_ready" } },
      graphSim: { ctx, canvas: { width: 320, height: 200, getBoundingClientRect: () => ({ left: 0, top: 0 }) }, nodes: [], edges: [], zoom: 1 },
      window: { devicePixelRatio: 1 }, graphSearchTerm: "", isDarkMode: () => false });
    vm.runInContext(source, context);
    vm.runInContext("renderGraph()", context);
    expect(fillText.mock.calls[0][0]).toBe("Graph index not ready.");
    expect(fillText.mock.calls.flat().join(" ")).not.toContain("GRAPH_EXTRACTION_ENABLED");
  });
  it("opens a ready empty graph without requesting an automatic rebuild", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    const apiPost = vi.fn(async () => ({ nodes: [], edges: [], truncated: false, totalNodes: 0, totalEdges: 0 }));
    const context = vm.createContext({ state: { graph: { projectsLoaded: true, projects: ["p"], selectedProject: "p" } }, GRAPH_INITIAL_LIMIT: 500, esc: String,
      document: { getElementById: () => ({ innerHTML: "", addEventListener: vi.fn() }) }, apiPost,
      api: async () => ({ totalNodes: 0, totalEdges: 0 }), renderGraphSidebar: vi.fn(), initGraph: vi.fn() });
    vm.runInContext(source, context);
    await vm.runInContext("loadGraph()", context);
    expect(apiPost.mock.calls).toHaveLength(1);
    expect(source).not.toContain("graph/build");
    expect(html).toContain('data-action="rebuild-graph"');
  });
  it("renders explicit partial profile and sampled count labels", () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    function renderProfile()"), html.indexOf("    var wsReconnectTimer"));
    const content = { innerHTML: "" };
    const context = vm.createContext({ document: { getElementById: () => content },
      state: { profile: { data: { partial: true, sessionCount: 12, totalObservations: 192 } } }, esc: (value: unknown) => String(value), formatTime: () => "now" });
    vm.runInContext(source, context);
    vm.runInContext("renderProfile()", context);
    expect(content.innerHTML).toContain("Partial profile.");
    expect(content.innerHTML).toContain("Sessions sampled");
    expect(content.innerHTML).toContain("Observations sampled");
    expect(content.innerHTML).not.toContain("Total Obs");
  });
  it("discovers one bounded project page and scopes initial graph and statistics reads", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    const elements = new Map<string, { innerHTML: string; addEventListener: ReturnType<typeof vi.fn> }>();
    const apiPost = vi.fn(async () => ({ nodes: [{ id: "n", type: "file" }], edges: [] }));
    const api = vi.fn(async (path: string) => path.startsWith("sessions?")
      ? { sessions: [{ project: "agentmemory" }], nextCursor: "next" } : {});
    const state = { graph: {} as Record<string, unknown> };
    const context = vm.createContext({ state, GRAPH_INITIAL_LIMIT: 500, apiPost, api, esc: String,
      encodeURIComponent, renderGraphSidebar: vi.fn(), initGraph: vi.fn(), document: { getElementById: (id: string) => {
        if (!elements.has(id)) elements.set(id, { innerHTML: "", addEventListener: vi.fn() });
        return elements.get(id);
      } } });
    vm.runInContext(source, context);
    await vm.runInContext("loadGraph()", context);
    expect(api).toHaveBeenNthCalledWith(1, "sessions?limit=16", { readErrorBody: true });
    expect(apiPost).toHaveBeenCalledExactlyOnceWith("graph/query", { project: "agentmemory", limit: 500 });
    expect(api).toHaveBeenNthCalledWith(2, "graph/stats?project=agentmemory", { readErrorBody: true });
    expect(api).toHaveBeenCalledTimes(2);
    expect(elements.get("graph-project-toolbar")?.innerHTML).toContain('for="graph-project"');
    expect(elements.get("graph-project-toolbar")?.innerHTML).toContain("Browse more projects");
    expect(state.graph).toMatchObject({ selectedProject: "agentmemory", nodes: [{ id: "n" }] });
  });
  it("does not query a global graph when the bounded project page is empty", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    const apiPost = vi.fn();
    const content = { innerHTML: "", addEventListener: vi.fn() };
    const api = vi.fn(async () => ({ sessions: [], nextCursor: "next" }));
    const context = vm.createContext({ state: { graph: {} }, api, apiPost, esc: String,
      document: { getElementById: () => content } });
    vm.runInContext(source, context);
    await vm.runInContext("loadGraph()", context);
    expect(api).toHaveBeenCalledExactlyOnceWith("sessions?limit=16", { readErrorBody: true });
    expect(apiPost).not.toHaveBeenCalled();
    expect(content.innerHTML).toContain("Choose a project");
  });
  it("resets graph selection and rejects stale initial and expansion responses after switching projects", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const initial = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    const expansion = html.slice(html.indexOf("    async function expandNode("), html.indexOf("    function runSimulation("));
    let finishInitial: (value: unknown) => void = () => undefined;
    let finishExpansion: (value: unknown) => void = () => undefined;
    const apiPost = vi.fn((_: string, body: { project: string; startNodeId?: string }) => {
      if (body.startNodeId) return new Promise(resolve => { finishExpansion = resolve; });
      if (body.project === "old") return new Promise(resolve => { finishInitial = resolve; });
      return Promise.resolve({ nodes: [{ id: "new-node", type: "file" }], edges: [] });
    });
    const state = { graph: { projectsLoaded: true, projects: ["old", "new"], selectedProject: "old", requestId: 0,
      nodes: [{ id: "old-node" }], edges: [{ id: "old-edge" }], selectedNode: { id: "old-node" }, filters: { old: true } } };
    const graphSim = { raf: 1, nodes: [{ id: "old-node" }], edges: [{ id: "old-edge" }], dragNode: {} };
    const context = vm.createContext({ state, graphSim, graphSearchTerm: "old search", GRAPH_INITIAL_LIMIT: 500,
      apiPost, api: async () => ({}), esc: String, encodeURIComponent, cancelAnimationFrame: vi.fn(),
      renderGraphSidebar: vi.fn(), initGraph: vi.fn(), document: { getElementById: () => ({ innerHTML: "", addEventListener: vi.fn() }) } });
    vm.runInContext(initial + expansion, context);
    const oldInitial = vm.runInContext("loadGraph()", context);
    const oldExpansion = vm.runInContext("expandNode('old-node')", context);
    await vm.runInContext("selectGraphProject('new')", context);
    finishInitial({ nodes: [{ id: "stale-initial" }], edges: [] });
    finishExpansion({ nodes: [{ id: "stale-expanded" }], edges: [] });
    await Promise.all([oldInitial, oldExpansion]);
    expect(apiPost.mock.calls[1][1]).toEqual({ project: "old", startNodeId: "old-node", maxDepth: 1 });
    expect(state.graph).toMatchObject({ selectedProject: "new", nodes: [{ id: "new-node" }], edges: [], selectedNode: null });
    expect(graphSim).toMatchObject({ nodes: [], edges: [], dragNode: null, raf: null });
    expect(vm.runInContext("graphSearchTerm", context)).toBe("");
  });
  it("adds selected project neighbors and retains their partial status", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    async function expandNode("), html.indexOf("    function runSimulation("));
    const state = { graph: { selectedProject: "agentmemory", requestId: 1, nodes: [{ id: "parent" }], edges: [], filters: {} } };
    const graphSim = { nodes: [{ id: "parent", x: 1, y: 2 }], edges: [] };
    const apiPost = vi.fn(async () => ({ nodes: [{ id: "neighbor", type: "file" }], edges: [{ id: "edge" }], partial: true, warning: "project_projection_incomplete" }));
    const context = vm.createContext({ state, graphSim, apiPost, renderGraphSidebar: vi.fn() });
    vm.runInContext(source, context);
    await vm.runInContext("expandNode('parent')", context);
    expect(apiPost).toHaveBeenCalledExactlyOnceWith("graph/query", { project: "agentmemory", startNodeId: "parent", maxDepth: 1 });
    expect(state.graph.nodes.map(node => node.id)).toEqual(["parent", "neighbor"]);
    expect(graphSim.nodes.map(node => node.id)).toEqual(["parent", "neighbor"]);
    expect(state.graph).toMatchObject({ queryPartial: true, totalNodes: null, totalEdges: null, queryWarning: "project_projection_incomplete" });
  });
  it("browses one additional project page while retaining the selected project", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    async function loadGraph()"), html.indexOf("    var NODE_SHAPES"));
    const state = { graph: { selectedProject: "agentmemory", projects: ["earlier"] } };
    const api = vi.fn(async () => ({ sessions: [{ project: "other" }], nextCursor: "next-2" }));
    const context = vm.createContext({ state, api, encodeURIComponent, esc: String,
      document: { getElementById: () => ({ innerHTML: "", addEventListener: vi.fn() }) } });
    vm.runInContext(source, context);
    await vm.runInContext("loadGraphProjects('cursor+1')", context);
    expect(api).toHaveBeenCalledExactlyOnceWith("sessions?limit=16&cursor=cursor%2B1", { readErrorBody: true });
    expect(state.graph).toMatchObject({ selectedProject: "agentmemory", projects: ["agentmemory", "earlier", "other"], sessionNextCursor: "next-2" });
  });
  it.each(["completed", "partial", "queued", "failed"])("rebuilds exactly one selected unit and reports %s honestly", async status => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    var graphRebuilding = false;"), html.indexOf("    function drawNodeShape("));
    const continuation = status === "completed" ? null : "next-unit";
    const result = { status, project: "agentmemory", observationsProcessed: 3, continuation, graphJobId: status === "queued" ? "job-1" : undefined };
    const api = vi.fn(async (_path: string, _options: { body: string }) => result);
    const loadGraph = vi.fn();
    const confirm = vi.fn((_message: string) => true);
    const state = { graph: { selectedProject: "agentmemory", requestId: 1 } };
    const context = vm.createContext({ state, api, loadGraph, esc: String, renderGraphSidebar: vi.fn(), window: { confirm } });
    vm.runInContext(source, context);
    await vm.runInContext("rebuildGraph('previous-unit')", context);
    expect(api).toHaveBeenCalledTimes(1);
    expect(api.mock.calls[0][0]).toBe("graph/build");
    expect(JSON.parse(api.mock.calls[0][1].body)).toEqual({ project: "agentmemory", continuation: "previous-unit" });
    expect(confirm.mock.calls[0][0]).toContain("spends provider tokens");
    expect(loadGraph).toHaveBeenCalledTimes(1);
    expect(state.graph).toMatchObject({ rebuildResult: result });
    const rendered = vm.runInContext("graphBuildStatusHtml()", context);
    expect(rendered).toContain(status === "completed" ? "Project rebuild completed." : status === "partial" ? "more work remains" : status === "queued" ? "completion has not been confirmed" : "unit failed");
    expect(rendered.includes('data-action="continue-graph-build"')).toBe(status !== "completed");
  });
  it("does not retry failed or stale rebuild requests automatically", async () => {
    const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
    const source = html.slice(html.indexOf("    var graphRebuilding = false;"), html.indexOf("    function drawNodeShape("));
    let finish: (value: unknown) => void = () => undefined;
    const api = vi.fn(() => new Promise(resolve => { finish = resolve; }));
    const loadGraph = vi.fn();
    const state = { graph: { selectedProject: "old", requestId: 1, rebuildResult: null } };
    const context = vm.createContext({ state, api, loadGraph, esc: String, renderGraphSidebar: vi.fn(), window: { confirm: () => true } });
    vm.runInContext(source, context);
    const pending = vm.runInContext("rebuildGraph()", context);
    await vm.runInContext("rebuildGraph()", context);
    state.graph.selectedProject = "new";
    state.graph.requestId = 2;
    finish({ status: "completed", project: "old", continuation: null });
    await pending;
    expect(api).toHaveBeenCalledTimes(1);
    expect(loadGraph).not.toHaveBeenCalled();
    expect(state.graph.rebuildResult).toBeNull();
  });
});
