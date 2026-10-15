import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";

// viewer graph kept bouncing on >1000 nodes because damping
// alone could not bleed off the per-frame force pile-up. Cool-down
// adds tick-decayed damping, a per-node velocity cap, and parks the
// raf loop when total kinetic energy drops below an epsilon.
describe("viewer graph cool-down", () => {
  const viewer = readFileSync("src/viewer/index.html", "utf-8");

  it("discards a display page when project selection changes before it resolves", async () => {
    const source = viewer.slice(viewer.indexOf("    async function loadGraph()"), viewer.indexOf("    async function loadGraphProjects("));
    let resolveQuery: (value: unknown) => void = () => {};
    const query = new Promise(resolve => { resolveQuery = resolve; });
    const state = { activeTab: "graph", graph: { requestId: 0, selectedProject: "old", projectsLoaded: true, nodes: [{ id: "keep" }], pageCursors: [null], pageIndex: 0 } };
    const elements = { "view-graph": { innerHTML: "" }, "graph-sidebar": { innerHTML: "" } };
    const load = runInNewContext(source + ";loadGraph", {
      state, GRAPH_INITIAL_LIMIT: 128, document: { getElementById: (id: keyof typeof elements) => elements[id] },
      renderGraphProjectPicker() {}, api: (path: string) => path === "graph/query" ? query : Promise.resolve({}),
      renderGraphSidebar() { throw new Error("stale page rendered"); }, initGraph() { throw new Error("stale graph drawn"); },
    });
    const pending = load();
    state.graph.selectedProject = "new";
    resolveQuery({ display: true, status: "ready", nodes: [{ id: "stale" }], edges: [] });
    await pending;
    expect(state.graph.nodes).toEqual([]);
  });

  function browseHarness(replies: unknown[]) {
    const source = viewer.slice(viewer.indexOf("    function cancelGraphBrowse()"), viewer.indexOf("    async function loadGraphProjects("));
    const state = { activeTab: "graph", graph: {
      requestId: 0, selectedProject: "project", projectsLoaded: true,
      nodes: [], edges: [], pageCursors: [null] as (string | null)[], pageIndex: 0,
      browseStatus: "", queryError: null, nextCursor: null,
    } };
    const calls: Record<string, unknown>[] = [];
    const statuses: string[] = [];
    const yields: (() => void)[] = [];
    const context = {
      state, GRAPH_INITIAL_LIMIT: 128,
      document: { getElementById: () => ({ innerHTML: "" }) },
      renderGraphProjectPicker() {}, initGraph() {},
      renderGraphSidebar() { statuses.push(state.graph.browseStatus); },
      setTimeout(resolve: () => void) { yields.push(resolve); },
      api(_path: string, options: { body: string }) {
        calls.push(JSON.parse(options.body));
        const response = replies.shift();
        if (response === undefined) throw new Error("Unexpected graph request");
        return Promise.resolve(response);
      },
    };
    const functions = runInNewContext(source + ";({loadGraph,cancelGraphBrowse})", context);
    async function flush() { for (let index = 0; index < 12; index++) await Promise.resolve(); }
    async function yieldBatch() { const next = yields.shift(); expect(next).toBeDefined(); next!(); await flush(); }
    return { state, calls, statuses, yields, flush, yieldBatch, ...functions };
  }

  const preparing = { display: true, status: "preparing", nodes: [], edges: [], progress: { phase: "membership", processed: 2 } };
  const ready = { display: true, status: "ready", nodes: [], edges: [], edgeScope: "page", crossPageEdges: "not_loaded", edgesTruncated: false };

  it("reads first, yields between single preparation batches, then reads the current empty graph", async () => {
    const h = browseHarness([preparing,
      { preparation: { status: "preparing", phase: "membership", processed: 16, continue: true } },
      { preparation: { status: "ready", phase: "ready", processed: 3, continue: false } }, ready]);
    const pending = h.loadGraph();
    await h.flush();
    expect(h.calls).toEqual([{ project: "project", display: true, limit: 128 }]);
    expect(h.state.graph.browseStatus).toBe("preparing");
    await h.yieldBatch();
    expect(h.calls[1]).toEqual({ project: "project", display: true, prepare: true });
    expect(h.calls).toHaveLength(2);
    await h.yieldBatch();
    await pending;
    expect(h.calls).toHaveLength(4);
    expect(h.calls[3]).toEqual({ project: "project", display: true, limit: 128 });
    expect(h.state.graph.browseStatus).toBe("ready");
    expect(h.statuses.slice(0, -1)).not.toContain("ready");
  });

  it.each(["cancel", "leave", "project"])("stops continuation after %s during the render yield", async (action) => {
    const h = browseHarness([preparing]);
    const pending = h.loadGraph();
    await h.flush();
    if (action === "cancel") h.cancelGraphBrowse();
    if (action === "leave") h.state.activeTab = "sessions";
    if (action === "project") h.state.graph.selectedProject = "other";
    await h.yieldBatch();
    await pending;
    expect(h.calls).toHaveLength(1);
  });

  it("stops unavailable preparation with its reason and never renders ready empty", async () => {
    const h = browseHarness([preparing, { preparation: { status: "preparing", phase: "prerequisites", processed: 0, continue: false, reason: "index_not_ready" } }]);
    const pending = h.loadGraph(); await h.flush(); await h.yieldBatch(); await pending;
    expect(h.state.graph.queryError).toBe("index_not_ready");
    expect(h.state.graph.browseStatus).toBe("error");
    expect(h.yields).toHaveLength(0);
  });

  it("discards a late preparation response after cancellation", async () => {
    let resolveBatch: (value: unknown) => void = () => {};
    const batch = new Promise(resolve => { resolveBatch = resolve; });
    const h = browseHarness([preparing, batch]);
    const pending = h.loadGraph();
    await h.flush();
    await h.yieldBatch();
    expect(h.calls).toHaveLength(2);
    h.cancelGraphBrowse();
    resolveBatch({ preparation: { status: "ready", phase: "ready", processed: 16, continue: false } });
    await pending;
    expect(h.calls).toHaveLength(2);
    expect(h.state.graph.browseStatus).toBe("preparing");
    expect(h.yields).toHaveLength(0);
  });

  it("surfaces a failed preparation batch instead of reading an empty page", async () => {
    const h = browseHarness([preparing, { preparation: { status: "error", reason: "invalid_record", continue: false } }]);
    const pending = h.loadGraph();
    await h.flush(); await h.yieldBatch(); await pending;
    expect(h.state.graph.queryError).toBe("invalid_record");
    expect(h.state.graph.browseStatus).toBe("error");
    expect(h.calls).toHaveLength(2);
  });

  it("clears stale page history and reads fresh without preparing a ready membership", async () => {
    const h = browseHarness([{ ...preparing, reason: "stale_graph_revision" }, ready]);
    h.state.graph.pageCursors = [null, "old-cursor"];
    h.state.graph.pageIndex = 1;
    const pending = h.loadGraph(); await h.flush(); await h.yieldBatch(); await pending;
    expect(h.calls[0].cursor).toBe("old-cursor");
    expect(h.calls[1].cursor).toBeUndefined();
    expect(h.calls.every(call => !call.prepare)).toBe(true);
    expect(h.state.graph.pageCursors).toEqual([null]);
    expect(h.state.graph.pageIndex).toBe(0);
  });

  it("retains opaque pagination and honest page edge metadata", async () => {
    const h = browseHarness([{ ...ready, nodes: [{ id: "node", type: "concept", name: "Node" }], hasMore: true, nextCursor: "next", edgesTruncated: true }]);
    await h.loadGraph();
    expect(h.state.graph.nextCursor).toBe("next");
    expect(h.state.graph).toMatchObject({ edgeScope: "page", crossPageEdges: "not_loaded", edgesTruncated: true });
    const sidebar = viewer.slice(viewer.indexOf("    function renderGraphSidebar()"), viewer.indexOf("    function initGraph()"));
    expect(sidebar).not.toMatch(/gs\.totalNodes|gs\.totalEdges|Historical graph snapshot/);
    expect(sidebar).toContain('aria-live="polite"');
    expect(sidebar).toContain('flex-wrap:wrap');
    expect(sidebar).toContain("Connections to nodes on other pages are not loaded.");
    expect(sidebar).toContain("No graph data in this project. The current graph is empty.");
  });

  it("tracks a tickCount that grows each simulation step", () => {
    expect(viewer).toMatch(/graphSim\.tickCount\s*=\s*\(graphSim\.tickCount\s*\|\|\s*0\)\s*\+\s*1/);
  });

  it("adds tick-decay to damping (coolBoost scales with tickCount)", () => {
    expect(viewer).toMatch(/coolBoost\s*=\s*Math\.min\(0\.4,\s*graphSim\.tickCount\s*\/\s*1500\)/);
    expect(viewer).toMatch(/damping\s*=\s*0\.9\s*-\s*coolBoost/);
  });

  it("caps per-node velocity by node-count band", () => {
    expect(viewer).toMatch(
      /velocityCap\s*=\s*nodeCount\s*>\s*1000\s*\?\s*6/,
    );
    expect(viewer).toMatch(/nvx\s*>\s*velocityCap/);
    expect(viewer).toMatch(/nvy\s*>\s*velocityCap/);
  });

  it("parks the raf loop once the layout is quiet for 30 ticks", () => {
    expect(viewer).toMatch(/rmsVelocity/);
    expect(viewer).toMatch(/quietTicks/);
    expect(viewer).toMatch(/if\s*\(graphSim\.quietTicks\s*>\s*30\)/);
  });

  it("wakes the parked loop on mousedown so drag still responds", () => {
    expect(viewer).toMatch(/graphSim\.quietTicks\s*=\s*0/);
    expect(viewer).toMatch(/if\s*\(graphSim\.running\s*&&\s*!graphSim\.raf\)/);
  });
});
