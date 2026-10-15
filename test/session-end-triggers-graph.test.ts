import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";

// #666: api::session::end must publish the session-stopped lifecycle so
// summary enqueue + slot-reflect + graph extraction actually fire. Before this
// fix the `event::session::stopped` handler in events.ts was a dead
// subscriber — no code published `agentmemory.session.stopped`, so graph
// nodes / lessons / crystals never materialized despite the handler
// existing. The HTTP response waits for the enqueue acknowledgment, while
// the LLM work continues in the background.
describe("api::session::end → event::session::stopped (#666)", () => {
  const api = readFileSync("src/triggers/api.ts", "utf-8");

  it("api::session::end fires event::session::stopped after kv.update", () => {
    expect(api).toMatch(
      /api::session::end[\s\S]*?kv\.update\(KV\.sessions[\s\S]*?function_id:\s*"event::session::stopped"/,
    );
  });

  it("event::session::stopped trigger payload includes sessionId", () => {
    expect(api).toMatch(
      /function_id:\s*"event::session::stopped",\s*payload:\s*\{\s*sessionId\s*\}/,
    );
  });

  it("waits for the enqueue acknowledgment and reports failures", () => {
    const handler = api.slice(
      api.indexOf('registerFunction("api::session::end"'),
      api.indexOf('function_id: "api::session::end"'),
    );
    expect(handler).toMatch(/await sdk\.trigger\(\{\s*function_id:\s*"event::session::stopped"/);
    expect(handler).not.toContain("TriggerAction.Void()");
    expect(handler).toContain('error: "summary_enqueue_failed"');
  });
});

// #666: viewer's manual rebuild operates on its selected project in bounded
// pages so one click cannot trigger a whole-corpus extraction.
describe("api::graph-build endpoint (#666)", () => {
  const api = readFileSync("src/triggers/api.ts", "utf-8");
  const builder = readFileSync("src/triggers/graph-build.ts", "utf-8");

  it("registers api::graph-build function", () => {
    expect(api).toMatch(/registerFunction\("api::graph-build"/);
  });

  it("registers HTTP trigger at /agentmemory/graph/build", () => {
    expect(api).toMatch(
      /api_path:\s*"\/agentmemory\/graph\/build",\s*http_method:\s*"POST"/,
    );
  });

  it("uses the selected project page and never enumerates all KV sessions", () => {
    expect(api).toMatch(/buildScopedGraph\(sdk, kv, input\.project, input\.continuation/);
    expect(builder).toMatch(/viewerProjectSessions\(kv, project, cursor\.sessionCursor/);
    expect(builder).toContain("sdk.trigger<Record<string, unknown>, unknown>");
    expect(builder).toContain('function_id: "mem::graph-extract"');
    expect(builder).not.toMatch(/kv\.list/);
  });

  it("extracts only observations with compressed titles", () => {
    expect(builder).toMatch(/compressedObservations\(page\.value\.items, sessionId\)/);
    expect(builder).toMatch(/observation\.title === undefined \|\| observation\.title === null/);
  });

  it("caps a rebuild unit at 25 observations and 1 MiB before extraction", () => {
    expect(builder).toMatch(/MAX_GRAPH_BUILD_OBSERVATIONS = 25/);
    expect(builder).toMatch(/MAX_GRAPH_BUILD_PAGE_BYTES = 1_048_576/);
    expect(builder).toMatch(/limit: MAX_GRAPH_BUILD_OBSERVATIONS,[\s\S]*?maxBytes: MAX_GRAPH_BUILD_PAGE_BYTES/);
  });

  it("reports honest bounded progress and replay continuation states", () => {
    expect(builder).toMatch(/type GraphBuildStatus = "completed" \| "partial" \| "queued" \| "failed"/);
    expect(builder).toMatch(/continuation: string \| null/);
    expect(builder).toMatch(/expectedGraphJobId/);
  });
});

// #666: `agentmemory status` showed Memories/Observations as 0 because it
// fetched /agentmemory/export which times out on iii-engine's file-based
// KV under concurrent kv.list() pressure. Switch to /memories for the
// memory count and derive observation count from sessions[].observationCount.
describe("agentmemory status no longer depends on /export (#666)", () => {
  const cli = readFileSync("src/cli.ts", "utf-8");

  it("status uses count-only memories endpoint instead of export", () => {
    expect(cli).toMatch(/apiFetch<any>\(base,\s*"memories\?count=true"\)/);
    expect(cli).not.toMatch(/apiFetch<any>\(base,\s*"export"\)/);
  });

  it("status derives obsCount from sessions[].observationCount", () => {
    expect(cli).toMatch(
      /sessionList\.reduce\([\s\S]*?observationCount/,
    );
  });

  it("status reads memCount from memoriesRes.latestCount (count endpoint)", () => {
    expect(cli).toMatch(/memoriesRes\?\.latestCount\s*\?\?\s*memoriesRes\?\.total/);
  });
});
