import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { readProjectGraphStatistics } from "../src/functions/graph-statistics.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { logger } from "../src/logger.js";

vi.mock("iii-sdk", () => ({ InvocationError: class InvocationError extends Error {} }));
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));

function snapshot(project = "other-project") {
  return {
    version: 1, project, updatedAt: "2026-10-10T12:00:00Z",
    stats: { totalNodes: 90, totalEdges: 120, nodesByType: { file: 90 }, edgesByType: { uses: 120 } },
  };
}

function state(value: unknown = snapshot()) {
  return {
    pages: vi.fn(async function* () {
      yield { items: value === null ? [] : [value] };
    }),
  };
}

function sample() {
  return {
    nodes: [{ id: "n1", type: "file" }, { id: "n2", type: "concept" }],
    edges: [{ id: "e1", type: "uses" }],
    totalNodes: 999, totalEdges: 999,
    truncated: true, warning: "graph_node_too_large",
  };
}

describe("bounded project graph sample statistics", () => {
  it("retains unknown totals and aggregate maps while exposing useful partial evidence", async () => {
    const query = vi.fn(async () => sample());
    const result = await readProjectGraphStatistics(state() as never, "selected", query);
    expect(query).toHaveBeenCalledExactlyOnceWith("selected");
    expect(result).toMatchObject({
      totalNodes: null, totalEdges: null, nodesByType: {}, edgesByType: {},
      countsAreCurrent: false, partial: true, provenance: "unavailable", project: "selected",
      sampleNodes: 2, sampleEdges: 1,
      sampleNodesByType: { file: 1, concept: 1 }, sampleEdgesByType: { uses: 1 },
      coverage: {
        project: "selected", requestedLimit: 12, returnedNodes: 2, returnedEdges: 1,
        truncated: true, warning: "graph_node_too_large",
      },
    });
  });

  it("counts each returned ID once and safely handles arbitrary type keys", async () => {
    const result = await readProjectGraphStatistics(state() as never, "selected", async () => ({
      nodes: [{ id: "n1", type: "__proto__" }, { id: "n1", type: "file" }],
      edges: [{ id: "e1", type: "uses" }, { id: "e1", type: "uses" }],
    }));
    expect(result).toMatchObject({ sampleNodes: 1, sampleEdges: 1, sampleEdgesByType: { uses: 1 } });
    expect("sampleNodesByType" in result && Object.entries(result.sampleNodesByType)).toEqual([["__proto__", 1]]);
  });

  it("does not reinterpret an untruncated empty sample as exact totals", async () => {
    const result = await readProjectGraphStatistics(state(null) as never, "selected", async () => ({ nodes: [], edges: [], truncated: false }));
    expect(result).toMatchObject({ totalNodes: null, totalEdges: null, sampleNodes: 0, sampleEdges: 0, partial: true, countsAreCurrent: false });
  });

  it("preserves a complete historical snapshot without querying", async () => {
    const query = vi.fn();
    const result = await readProjectGraphStatistics(state(snapshot("selected")) as never, "selected", query);
    expect(query).not.toHaveBeenCalled();
    expect(result).toMatchObject({ totalNodes: 90, totalEdges: 120, provenance: "historical_snapshot", countsAreCurrent: false, partial: true });
    expect(result).not.toHaveProperty("sampleNodes");
  });

  it("does not perform a global query for projectless stats", async () => {
    const query = vi.fn();
    const result = await readProjectGraphStatistics(state(null) as never, undefined, query);
    expect(query).not.toHaveBeenCalled();
    expect(result).toMatchObject({ totalNodes: null, totalEdges: null });
    expect(result).not.toHaveProperty("coverage");
  });

  it.each(["throw", "malformed"])("keeps query failure unknown (%s)", async (failure) => {
    vi.mocked(logger.warn).mockClear();
    const result = await readProjectGraphStatistics(state() as never, "selected", async () => {
      if (failure === "throw") throw new Error("private transport details");
      return { nodes: [{ type: "file" }], edges: [] };
    });
    expect(result).toMatchObject({
      totalNodes: null, totalEdges: null, sampleNodes: null, sampleEdges: null, partial: true,
      coverage: { returnedNodes: null, returnedEdges: null, warning: "graph_sample_query_failed" },
    });
    expect(JSON.stringify(result)).not.toContain("private transport details");
    expect(logger.warn).toHaveBeenCalledExactlyOnceWith("Project graph sample unavailable", {
      project: "selected", code: "graph_sample_query_failed",
    });
    expect(JSON.stringify(vi.mocked(logger.warn).mock.calls)).not.toContain("private transport details");
  });

  it("wires exactly one registered bounded project query with no search or start node", async () => {
    const handlers = new Map<string, (data?: unknown) => Promise<unknown>>();
    const trigger = vi.fn(async () => sample());
    const sdk = {
      registerFunction: (id: string, handler: (data?: unknown) => Promise<unknown>) => handlers.set(id, handler),
      registerTrigger: vi.fn(), trigger,
    };
    registerGraphFunction(sdk as never, state() as never, { name: "noop" } as never);
    const result = await handlers.get("mem::graph-stats")!({ project: "  selected  " });
    expect(trigger).toHaveBeenCalledExactlyOnceWith({
      function_id: "mem::graph-query", payload: { project: "selected", maxDepth: 1, limit: 12 },
    });
    expect(result).toMatchObject({ project: "selected", sampleNodes: 2, totalNodes: null });
  });
});

const viewer = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
const renderers = viewer.slice(viewer.indexOf("    function graphCountAvailable("), viewer.indexOf("    function renderDashboard()"));
const escapeHtml = (value: unknown) => String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
const render = new Function("esc", "formatTime", `${renderers}; return { renderGraphStat, renderGraphSampleStats };`)(escapeHtml, String) as {
  renderGraphStat: (graph: unknown, error?: boolean) => string;
  renderGraphSampleStats: (graph: unknown) => string;
};

describe("graph sample labels", () => {
  it("labels samples separately and escapes type and warning text", () => {
    const html = render.renderGraphStat({
      totalNodes: null, totalEdges: null, countsAreCurrent: false,
      sampleNodes: 6, sampleEdges: 11, sampleNodesByType: { "<script>": 6 }, sampleEdgesByType: { uses: 11 },
      coverage: { project: "selected", truncated: true, warning: "<unsafe>" },
    });
    expect(html).toContain('class="value">Unavailable');
    expect(html).toContain("Sample nodes: 6 · Sample edges: 11");
    expect(html).toContain("Sample only; full totals unavailable.");
    expect(html).toContain("Query returned partial results.");
    expect(html).toContain("Sample node types: &lt;script&gt;: 6");
    expect(html).toContain("&lt;unsafe&gt;");
    expect(html).not.toContain("Graph snapshot nodes");
    expect(html).toContain('role="status"');
    expect(html).toContain("overflow-wrap:anywhere");
  });

  it("shows unknown samples as unavailable and real zero samples as zero", () => {
    const unknown = render.renderGraphSampleStats({ sampleNodes: null, sampleEdges: null, coverage: {} });
    expect(unknown).toContain("Sample nodes: Unavailable · Sample edges: Unavailable");
    expect(render.renderGraphSampleStats({ sampleNodes: 0, sampleEdges: 0, coverage: {} })).toContain("Sample nodes: 0 · Sample edges: 0");
  });

  it("retains historical total labels and sidebar unknown-total guards", () => {
    const html = render.renderGraphStat({ totalNodes: 90, totalEdges: 120, fromSnapshot: true, countsAreCurrent: false });
    expect(html).toContain("Graph snapshot nodes");
    expect(html).toContain("historical counts");
    expect(html).not.toContain("Bounded project sample");
    const sidebar = viewer.slice(viewer.indexOf("    function renderGraphSidebar()"), viewer.indexOf("    function renderGraphSidebar()") + 7500);
    expect(sidebar).toContain("graphCountAvailable(gs.totalNodes) ? gs.totalNodes : 'Unavailable'");
    expect(sidebar).toContain("graphCountAvailable(gs.totalEdges) ? gs.totalEdges : 'Unavailable'");
    expect(sidebar).toContain("renderGraphSampleStats(gs)");
  });
});
