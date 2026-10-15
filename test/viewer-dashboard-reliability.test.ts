import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";

const viewer = readFileSync("src/viewer/index.html", "utf-8");

function extractFunction(name: string): string {
  const start = viewer.indexOf(`function ${name}(`);
  if (start < 0) throw new Error(`function ${name} not found in viewer`);
  let depth = 0;
  for (let i = viewer.indexOf("{", start); i < viewer.length; i++) {
    if (viewer[i] === "{") depth++;
    if (viewer[i] === "}") {
      depth--;
      if (depth === 0) return viewer.slice(start, i + 1);
    }
  }
  throw new Error(`function ${name} is not balanced`);
}

function load<T>(name: string): T {
  return new Function(`${extractFunction(name)}\nreturn ${name};`)() as T;
}

describe("viewer dashboard reliability", () => {
  function graphStat(graph: Record<string, unknown>, error = false): string {
    return new Function("esc", "formatTime", ["graphCountAvailable", "renderGraphSampleStats", "renderGraphStat"].map(extractFunction).join("\n") + "\nreturn renderGraphStat;")(
      (value: string) => value.replaceAll("<", "&lt;"),
      (value: string) => value,
    )(graph, error) as string;
  }

  function savingsStats(report: unknown): string {
    return metricRenderer("renderSavingsStats")(report);
  }

  function metricRenderer(name: string): (data: unknown) => string {
    const deps = ["savingsNumber", "measuredNumber", "metricDate", "metricRow", "cacheMetric", "usagePercent", "usageMoney", "pricingSource", name];
    return new Function("esc", deps.map(extractFunction).join("\n") + `\nreturn ${name};`)(
      (value: string) => value.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll('"', "&quot;"),
    ) as (data: unknown) => string;
  }

  it("removes the full efficiency details and restores top Crystals while moving telemetry below", () => {
    expect(viewer).not.toContain("Memory efficiency · measured usage and assumptions");
    expect(viewer).not.toContain("renderSavingsDetails");
    expect(extractFunction("renderDashboard")).toContain('data-tab="crystals"');
    const dashboard = extractFunction("renderDashboard");
    expect(dashboard.indexOf("renderSavingsStats(savingsReport)")).toBeGreaterThan(dashboard.indexOf("var savingsReport"));
    expect(dashboard).toContain('<dl class="metric-rows">');
    expect(viewer).toContain('<button data-tab="crystals">Crystals</button>');
  });

  it("renders each client's cache counts and the supplied weighted combined ratio", () => {
    const html = savingsStats({ agentUsage: { clients: {
      codex: { requests: 1, inputTokens: 100, cachedInputTokens: 50, cacheHitRatio: .5 },
      claude: { requests: 2, inputTokens: 900, cachedInputTokens: 90, cacheHitRatio: .1 },
    }, inputTokens: 1000, cachedInputTokens: 140, cacheHitRatio: .14, partial: true } });
    expect(html).toContain("Codex input cache hits");
    expect(html).toContain("Claude input cache hits");
    expect(html).toContain("Combined input cache hits");
    expect(html).toContain("14.00%");
    expect(html).not.toContain("30.00%");
    expect(html).toContain("partial sample");
    const absent = savingsStats({ agentUsage: { clients: { claude: { requests: 0, unavailable: false } } } });
    expect(absent).toContain("No local usage");
  });

  it("renders operational zeros, UTC sample dates and actual automatic compaction state", () => {
    const html = metricRenderer("renderOperationalStats")({
      failures24h: { complete: true, count: 0, intervalStart: "2026-10-08T12:00:00Z", intervalEnd: "2026-10-09T12:00:00Z" },
      graphStorage: {
        storedBytes: 2000000000, logicalBytes: 1900000000, rows: 12000,
        sampleAt: "2026-10-09T11:00:00Z", coverage: "all seven durable graph scopes",
        complete: true, source: "native_scoped_metadata",
      },
      engineMemory: { source: "iii.exe", residentBytes: 0, sampledAt: "2026-10-09T11:59:00Z" },
      lastSuccessfulCompactionAt: "2026-10-09T10:00:00Z", automaticCompaction: { enabled: false, state: "paused", autoVacuumMode: 0 },
    });
    expect(html).toContain('class="value">0');
    expect(html).toContain("2.000 GB");
    expect(html).toContain("Stored durable graph payload");
    expect(html).toContain("2026-10-09T11:00:00.000Z");
    expect(html).toContain("0.0 MiB");
    expect(html).toContain("2026-10-09T10:00:00.000Z");
    expect(html).toContain("Paused");
    expect(html).toContain("Configured: disabled");
  });

  it("never substitutes Node RAM or partial day counts and keeps invalid metrics unavailable", () => {
    const html = metricRenderer("renderOperationalStats")({
      failures24h: { complete: false, count: 99, coverageSince: "2026-10-09T10:00:00Z" },
      graphStorage: { storedBytes: NaN, sampleAt: "invalid", coverage: '<script>', complete: true, source: "native_scoped_metadata" },
      engineMemory: { source: "node", residentBytes: 104857600 },
      automaticCompaction: { enabled: null, state: "__proto__" },
    });
    expect(html).not.toContain('class="value">99');
    expect(html).not.toContain("100.0 MiB");
    expect(html).toContain("incomplete coverage");
    expect(html).not.toContain("<script>");
    expect(html).toContain("No complete native measurement for graph scopes.");
    expect(html).not.toMatch(/NaN|Invalid Date|\[object Object\]/);
    expect(metricRenderer("renderOperationalStats")(null)).toContain("No successful compaction recorded");
  });

  it("requires a native graph source and escapes coverage in a measured result", () => {
    const render = metricRenderer("renderOperationalStats");
    const nativeSample = {
      storedBytes: 2000000000, sampleAt: "2026-10-09T11:00:00Z",
      coverage: "<script>", complete: true, source: "native_scoped_metadata",
    };
    const measured = render({ graphStorage: nativeSample });
    expect(measured).toContain("2.000 GB");
    expect(measured).toContain("&lt;script>");
    expect(measured).not.toContain("<script>");

    const legacySources = [
      { ...nativeSample, source: "state_rows_categories" },
      { storedBytes: 2000000000, complete: true, sampleAt: nativeSample.sampleAt, coverage: nativeSample.coverage },
    ];
    for (const graphStorage of legacySources) {
      const html = render({ graphStorage });
      expect(html).not.toContain("2.000 GB");
      expect(html).toContain('class="value">Unavailable');
    }

    const unavailable = render({ graphStorage: {
      ...nativeSample, source: "unavailable", reason: "scoped_metadata_unavailable",
    } });
    expect(unavailable).not.toContain("2.000 GB");
    expect(unavailable).toContain("Native metadata for graph scopes is unavailable; total state storage is not graph size.");
  });

  it("renders observed model shares and each measured category rate/cost with tariff attribution", () => {
    const html = metricRenderer("renderModelPricing")({ partial: true, modelUsage: [{
      client: "claude", model: "test-model", requests: 3, requestShare: .6, inputTokenShare: .7, outputTokenShare: .8, totalTokenShare: .75,
      pricing: { source: "https://platform.claude.com/docs/en/about-claude/pricing", checkedAt: "2026-10-09", scenarioRequests: 3,
        weightedRatesUsdPerMillion: { input: 2, cacheRead: .1, cacheWrite: null, output: 10 },
        costsUsd: { input: 1, cacheRead: .05, cacheWrite: null, output: .2, total: null }, unpricedRequests: 1 },
    }], pricing: { pricedSubtotalUsd: 1.25, costsUsd: { total: null } } });
    expect(html).toContain("Requests 60.0%");
    expect(html).toContain("Total tokens 75.0%");
    expect(html).toContain("Cache hit $0.1000");
    expect(html).toContain("Cache write Unpriced");
    expect(html).toContain("Output $10.0000");
    expect(html).toContain("checked 2026-10-09");
    expect(html).toContain("partly/unpriced");
    expect(html).toContain("not an invoice");
    expect(html).toContain("standard direct API assumptions");
    expect(html).toContain("Priced subtotal $1.2500 · total Unpriced");
    expect(html).not.toMatch(/6\.1|medium/);
    const money = new Function(`${extractFunction("measuredNumber")}\n${extractFunction("usageMoney")}\nreturn usageMoney;`)() as (value: number) => string;
    expect(money(.000025)).toBe("$0.00002500");
  });

  it("escapes unknown model metadata and rejects unsafe tariff links", () => {
    const html = metricRenderer("renderModelPricing")({ modelUsage: [{ model: '<img src=x>', client: '<script>', pricing: { source: 'javascript:alert(1)', checkedAt: '<x>' } }] });
    expect(html).toContain("&lt;img src=x>");
    expect(html).not.toContain("javascript:");
    expect(html).toContain("Tariff unavailable");
    expect(metricRenderer("renderModelPricing")({ modelUsage: [] })).toContain("No model usage available");
    expect(viewer).toContain('aria-label="Observed model pricing" tabindex="0"');
    expect(viewer).toContain('.metric-row { grid-template-columns: minmax(0, 1fr);');
    expect(viewer).toContain('.pricing-scroll { overflow-x: auto; }');
  });

  it("renders real current graph counts including zero without calling them historical snapshots", () => {
    for (const [nodes, edges] of [[1234, 5678], [0, 0]]) {
      const html = graphStat({ totalNodes: nodes, totalEdges: edges, countsAreCurrent: true });
      expect(html).toContain('class="label">Graph nodes');
      expect(html).toContain(`class="value">${nodes}`);
      expect(html).toContain(`${edges} edges`);
      expect(html).not.toMatch(/snapshot|historical|unknown|Unavailable/);
      expect(html).toContain('data-action="goto-tab" data-tab="graph" role="link" tabindex="0"');
    }
  });

  it("identifies historical snapshot counts and keeps them visible on refresh failure", () => {
    const html = graphStat({ totalNodes: 42, totalEdges: 65, countsAreCurrent: false, fromSnapshot: true, updatedAt: "2026-10-09T00:00:00Z" }, true);
    expect(html).toContain("Graph snapshot nodes");
    expect(html).toContain('class="value">42');
    expect(html).toContain("65 snapshot edges · historical counts · 2026-10-09T00:00:00Z · refresh failed; retrying");
  });

  it("keeps absent and malformed counts distinct from a known zero", () => {
    for (const graph of [{}, { totalNodes: null, totalEdges: null }, { totalNodes: -1, totalEdges: NaN }]) {
      const html = graphStat(graph, true);
      expect(html).toContain('class="value">Unavailable');
      expect(html).toContain("Graph counts unavailable · refresh failed; retrying");
      expect(html).not.toContain('class="value">0');
      expect(html).not.toContain("historical counts");
    }
    expect(graphStat({ totalNodes: 0 })).toContain('class="value">0');
    expect(graphStat({ totalNodes: 0 })).toContain("Edge count unavailable");
  });

  it("explains missing telemetry independently of stored memories and never implies collection is running", () => {
    const html = savingsStats({ context: { emissions: 0, pairedEmissions: 0 }, codex: {} });
    expect(html).toContain("No hook telemetry");
    expect(html).toContain("stored memories are separate");
    expect(html).toContain("No paired telemetry");
    expect(html).toContain("stored memories do not measure savings");
    expect(html).not.toMatch(/Collecting|NaN|0\.0%/);
    expect(savingsStats(null)).toContain("Usage data could not load. Refresh to retry.");
  });

  it("renders measured zero emissions and selection reduction without inventing net savings", () => {
    const html = savingsStats({ context: { emissions: 2, meanTokensPerObservedSession: 0, candidateTokens: 100, pairedEmissions: 2, selectionReductionTokens: 0 }, codex: { cacheHitRatio: 0 } });
    expect(html).toContain('class="value">~0');
    expect(html).toContain('class="value">0.0%');
    expect(html).toContain('class="value">0.00%');
    expect(html).toContain("not net savings");
    expect(html).not.toMatch(/No hook telemetry|No paired telemetry|Collecting/);
  });

  it("observations never reload the dashboard; pushed session and memory events update it in place", () => {
    const router = extractFunction("routeWsMessage");
    expect(router).not.toMatch(/loadDashboard\(\)|scheduleDashboardReload\(\)/);
    const live = extractFunction("handleLiveEvent");
    expect(live).not.toMatch(/loadDashboard\(\)/);
    expect(live).toMatch(/type === 'session\.updated'/);
    expect(live).toMatch(/type === 'session\.activity'/);
    expect(live).toMatch(/type === 'session\.deleted'/);
    expect(live).toMatch(/type === 'memory\.updated' \|\| type === 'memory\.deleted'/);
    expect(live).toMatch(/scheduleLiveRender\(\);\s*\}$/);
    expect(viewer).toMatch(/var LIVE_RENDER_THROTTLE_MS = 250;/);
    expect(extractFunction("handleStreamEvent")).toMatch(
      /evt\.type === 'event' && evt\.event && evt\.event\.type\) \{\s*handleLiveEvent\(evt\.event\.type, evt\.event\.data \|\| \{\}\);/,
    );
  });

  it("no timer re-fetches the dashboard and no refresh path blanks a loaded view", () => {
    expect(viewer).not.toMatch(/startDashboardAutoRefresh|dashboardTimer|Auto-refresh 30s/);
    expect(extractFunction("refreshDashboard")).not.toMatch(/loaded = false/);
    expect(extractFunction("startPolling")).not.toMatch(/loaded = false/);
    expect(extractFunction("endSession")).not.toMatch(/loaded = false/);
    expect(extractFunction("confirmDeleteMemory")).not.toMatch(/loaded = false/);
  });

  it("session events upsert, patch and remove sessions by id", () => {
    const deps = ["sessionKey", "upsertSessionIn", "removeSessionFrom", "patchSessionCount"]
      .map(extractFunction)
      .join("\n");
    const api = new Function(`${deps}\nreturn { upsertSessionIn, removeSessionFrom, patchSessionCount };`)() as {
      upsertSessionIn: (l: Array<Record<string, unknown>>, s: Record<string, unknown>) => Array<Record<string, unknown>>;
      removeSessionFrom: (l: Array<Record<string, unknown>>, k: string) => Array<Record<string, unknown>>;
      patchSessionCount: (l: Array<Record<string, unknown>>, k: string, c: number) => void;
    };
    const list: Array<Record<string, unknown>> = [{ id: "a", status: "active", observationCount: 1 }];
    api.upsertSessionIn(list, { id: "a", status: "completed" });
    expect(list).toEqual([{ id: "a", status: "completed", observationCount: 1 }]);
    api.upsertSessionIn(list, { id: "b", status: "active" });
    expect(list.map((s) => s.id)).toEqual(["b", "a"]);
    api.patchSessionCount(list, "a", 7);
    expect(list[1].observationCount).toBe(7);
    expect(api.removeSessionFrom(list, "b").map((s) => s.id)).toEqual(["a"]);
  });

  it("re-rendering identical HTML leaves the DOM alone, but a placeholder in between forces a render", () => {
    const setViewHtml = load<(el: Record<string, unknown>, html: string) => void>("setViewHtml");
    let writes = 0;
    const el: Record<string, unknown> & { firstElementChild?: object } = {};
    Object.defineProperty(el, "innerHTML", {
      set() {
        writes++;
        el.firstElementChild = {};
      },
    });
    setViewHtml(el, "<div>a</div>");
    setViewHtml(el, "<div>a</div>");
    expect(writes).toBe(1);
    setViewHtml(el, "<div>b</div>");
    expect(writes).toBe(2);
    el.firstElementChild = {};
    setViewHtml(el, "<div>b</div>");
    expect(writes).toBe(3);
  });

  it("connects with the join mode first and flips mode after two failures of either", () => {
    expect(viewer).toMatch(/var directFailed = true;/);
    expect(viewer).toMatch(/if \(directFailures >= DIRECT_FAILURE_THRESHOLD\) \{\s*directFailed = ws\.__direct;/);
  });

  it("loadDashboard never runs twice at once and replays a request that arrived mid-flight", () => {
    const loader = extractFunction("loadDashboard");
    expect(loader).toMatch(/if \(dashboardLoading\) \{\s*dashboardReloadPending = true;\s*return;/);
    expect(loader).toMatch(/finally \{\s*dashboardLoading = false;/);
  });

  it("a failed refresh keeps the last data instead of showing the new-install hero", () => {
    const loader = extractFunction("loadDashboard");
    expect(loader).toMatch(/if \(sessionsOk\) d\.sessions = replaySessionEvents\(results\[1\]\.sessions, seqAtStart\);/);
    expect(loader).toMatch(/if \(!sessionsOk && !d\.loaded\)/);
    const listOr = load<(p: unknown[], r: unknown, k: string, f?: string) => unknown[]>("listOr");
    expect(listOr([1, 2], null, "items")).toEqual([1, 2]);
    expect(listOr([1, 2], { items: [3] }, "items")).toEqual([3]);
    expect(listOr([1], { other: [4] }, "items", "other")).toEqual([4]);
    expect(listOr([1], {}, "items")).toEqual([]);
  });

  it("selection reduction uses paired measured tokens without inferring savings from session count", () => {
    const html = savingsStats({ context: { emissions: 2, meanTokensPerObservedSession: 50, candidateTokens: 200, pairedEmissions: 2, selectionReductionTokens: 100 } });
    expect(html).toContain('class="value">50.0%');
    expect(html).toContain("not net savings");
    const missingPair = savingsStats({ context: { emissions: 2, meanTokensPerObservedSession: 50, candidateTokens: 200, pairedEmissions: 0, selectionReductionTokens: 100 } });
    expect(missingPair).toContain("No paired telemetry");
    expect(missingPair).not.toContain("50.0%");
  });

  it("session summaries stored as objects render as text (#1229)", () => {
    const summaryText = load<(v: unknown) => string>("summaryText");
    expect(summaryText({ title: "Fix auth", narrative: "long" })).toBe("Fix auth");
    expect(summaryText({ narrative: "Only narrative" })).toBe("Only narrative");
    expect(summaryText({ keyDecisions: ["a", "b"] })).toBe("a; b");
    expect(summaryText("plain")).toBe("plain");
    expect(summaryText(undefined)).toBe("");
    expect(viewer).not.toMatch(/s\.firstPrompt \|\| s\.summary \|\|/);
  });

  it("tags stored as a CSV string no longer break a tab (#906)", () => {
    const asTags = load<(v: unknown) => string[]>("asTags");
    expect(asTags("analysis, run,streamed")).toEqual(["analysis", "run", "streamed"]);
    expect(asTags(["feat", "prompts"])).toEqual(["feat", "prompts"]);
    expect(asTags(undefined)).toEqual([]);
    expect(viewer).not.toMatch(/\(a\.tags \|\| \[\]\)/);
    expect(viewer).not.toMatch(/\(l\.tags \|\| \[\]\)/);
  });

  it("live buffers are capped so a large sync backlog cannot freeze the tab (#609)", () => {
    expect(viewer).toMatch(/var LIVE_BUFFER_MAX = 200;/);
    expect(extractFunction("handleStreamEvent")).toMatch(/evt\.data\.slice\(-LIVE_BUFFER_MAX\)/);
    expect(extractFunction("routeWsMessage")).toMatch(/observations\.length > LIVE_BUFFER_MAX/);
  });

  it("Rebuild Graph asks first, ignores repeat clicks, and reports the bounded result (#1383)", async () => {
    const state = { graph: { selectedProject: "api", requestId: 1, rebuildResult: null as unknown } };
    const confirm = vi.fn(() => true);
    let finish!: (value: unknown) => void;
    const api = vi.fn((_path: string, _options: { body: string; readErrorBody: boolean }) =>
      new Promise(resolve => { finish = resolve; }));
    const refresh = vi.fn(async () => undefined);
    const rebuild = new Function("state", "window", "api", "loadGraph", "renderGraphSidebar",
      `var graphRebuilding = false; async ${extractFunction("rebuildGraph")}\nreturn rebuildGraph;`)(
      state, { confirm }, api, refresh, vi.fn(),
    ) as (continuation?: string) => Promise<void>;
    const first = rebuild("next-unit");
    await rebuild("duplicate");
    expect(confirm).toHaveBeenCalledTimes(1);
    expect(api).toHaveBeenCalledTimes(1);
    expect(JSON.parse(api.mock.calls[0][1].body)).toEqual({ project: "api", continuation: "next-unit" });
    expect(api.mock.calls[0][1].readErrorBody).toBe(true);
    finish({ project: "api", status: "partial", continuation: "last-unit" });
    await first;
    expect(state.graph.rebuildResult).toEqual({ project: "api", status: "partial", continuation: "last-unit" });
    expect(refresh).toHaveBeenCalledTimes(1);
    confirm.mockReturnValue(false);
    await rebuild();
    expect(confirm).toHaveBeenCalledTimes(2);
    expect(api).toHaveBeenCalledTimes(1);
  });

  it("a session list fetched while live events arrive replays them so deletes and updates stick", () => {
    const deps = ["sessionKey", "upsertSessionIn", "removeSessionFrom", "patchSessionCount", "applySessionEvent", "replaySessionEvents"]
      .map(extractFunction)
      .join("\n");
    const replay = new Function(
      "liveSessionLog",
      `${deps}\nreturn replaySessionEvents;`,
    )([
      { seq: 1, type: "session.deleted", data: { sessionId: "old" } },
      { seq: 2, type: "session.deleted", data: { sessionId: "a" } },
      { seq: 3, type: "session.updated", data: { session: { id: "c", status: "active" } } },
      { seq: 4, type: "session.activity", data: { sessionId: "b", observationCount: 9 } },
    ]) as (l: Array<Record<string, unknown>>, since: number) => Array<Record<string, unknown>>;
    const fetched = [
      { id: "a", observationCount: 1 },
      { id: "b", observationCount: 2 },
      { id: "old", observationCount: 0 },
    ];
    expect(replay(fetched, 1)).toEqual([
      { id: "c", status: "active" },
      { id: "b", observationCount: 9 },
      { id: "old", observationCount: 0 },
    ]);
    expect(extractFunction("loadSessions")).toMatch(/replaySessionEvents\(result\.sessions, seqAtStart\)/);
  });

  it("count updates rerender the session list without refetching or blanking the open detail", () => {
    const render = extractFunction("renderSessions");
    expect(render).toMatch(/cached\.id === state\.sessions\.selectedId && !state\.sessions\.detailStale/);
    const detail = extractFunction("renderSessionDetail");
    expect(detail).toMatch(/if \(!cached \|\| cached\.id !== id\) \{\s*panel\.innerHTML = '<div class="detail-panel"><h3>Loading/);
    expect(detail).toMatch(/if \(request !== sessionDetailRequest \|\| state\.sessions\.selectedId !== id\) return;/);
    expect(extractFunction("handleLiveEvent")).toMatch(
      /if \(type !== 'session\.activity' && touched === state\.sessions\.selectedId\) \{\s*state\.sessions\.detailStale = true;/,
    );
  });

  it("memory events coalesce list reloads and only the newest count response wins", () => {
    const reload = extractFunction("scheduleMemoriesReload");
    expect(reload).toMatch(/if \(memoriesReloading\) \{\s*memoriesReloadPending = true;\s*return;/);
    expect(reload).toMatch(/finally \{\s*memoriesReloading = false;/);
    expect(extractFunction("handleLiveEvent")).not.toMatch(/loadMemories\(\)/);
    expect(extractFunction("refreshMemoryCount")).toMatch(
      /var request = \+\+memoryCountRequest;[\s\S]*if \(request !== memoryCountRequest\) return;/,
    );
  });

  it("a reconnect resyncs loaded views, and only a delivered message clears the failure count", () => {
    expect(viewer).toMatch(/if \(wsHasConnected\) resyncLiveViews\(\);\s*wsHasConnected = true;/);
    const resync = extractFunction("resyncLiveViews");
    expect(resync).toMatch(/if \(state\.dashboard\.loaded\) loadDashboard\(\);/);
    expect(resync).toMatch(/if \(state\.sessions\.loaded\) loadSessions\(\);/);
    expect(viewer).toMatch(/if \(!ws\.__usable\) \{\s*ws\.__usable = true;\s*directFailures = 0;/);
    const onopen = viewer.slice(viewer.indexOf("ws.onopen = function"), viewer.indexOf("ws.onmessage = function"));
    expect(onopen).toMatch(/wsRetries = 0;/);
    expect(onopen).not.toMatch(/directFailures = 0;/);
  });

  it("a stale dashboard retries on its own slower timer, once at a time", () => {
    expect(viewer).toMatch(/var STALE_RETRY_MS = 15000;/);
    const retry = extractFunction("scheduleStaleRetry");
    expect(retry).toMatch(/if \(staleRetryTimer\) return;/);
    expect(retry).toMatch(/state\.dashboard\.stale\) loadDashboard\(\);/);
    expect(extractFunction("loadDashboard")).toMatch(/if \(d\.stale\) scheduleStaleRetry\(\);/);
  });

  it("the dashboard applies its memory total only when no newer count request started", () => {
    const loader = extractFunction("loadDashboard");
    expect(loader).toMatch(/var countToken = \+\+memoryCountRequest;/);
    expect(loader).toMatch(/if \(countToken === memoryCountRequest && results\[2\]/);
  });

  it("a rebuild whose graph refresh fails renders a separate query failure and retry", () => {
    const sidebar = { innerHTML: "", querySelectorAll: () => [] };
    const state = { graph: { selectedProject: "api", stats: {}, filters: {}, nodes: [], edges: [],
      queryError: "graph/query failed", rebuildResult: { status: "completed" } } };
    const deps = ["graphCountAvailable", "renderGraphSampleStats", "graphBuildStatusHtml", "renderGraphSidebar"].map(extractFunction).join("\n");
    new Function("state", "document", "esc", "captureSearchFocus", "restoreSearchFocus",
      `var graphRebuilding=false, NODE_COLORS={}, NODE_SHAPES={}; ${deps}\nrenderGraphSidebar();`)(
      state, { getElementById: (id: string) => id === "graph-sidebar" ? sidebar : null },
      (value: string) => value.replaceAll("<", "&lt;"), () => null, () => undefined,
    );
    expect(sidebar.innerHTML).toContain("Project rebuild completed.");
    expect(sidebar.innerHTML).toContain("Graph query failed");
    expect(sidebar.innerHTML).toContain('data-action="reload-graph"');
    expect(sidebar.innerHTML).toContain("Retry query");
    expect(sidebar.innerHTML).not.toContain('>0</span>');
    expect(viewer).toMatch(/consolidated ' \+ esc\(formatTime\(status\.lastRunAt\)\)/);
  });

  it("returning to the tab reconnects a dropped live stream (#1370)", () => {
    expect(viewer).toMatch(/addEventListener\('visibilitychange'/);
    expect(viewer).toMatch(/visibilityState !== 'visible'[\s\S]{0,400}connectWs\(\);/);
  });
});
