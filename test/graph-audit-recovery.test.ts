import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CompressedObservation, MemoryProvider } from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { registerGraphFunction } from "../src/functions/graph.js";
import { registerGraphJobRecovery } from "../src/functions/graph-jobs.js";
import { StateTransactionError } from "../src/state/state-transactions.js";
import { graphStateHarness } from "./helpers/graph-state-harness.js";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

const response = `<entities>
<entity type="file" name="src/a.ts"/>
<entity type="function" name="main"/>
</entities>
<relationships>
<relationship type="uses" source="src/a.ts" target="main" weight="0.9"/>
</relationships>`;

const observation = (id: string, overrides: Partial<CompressedObservation> = {}): CompressedObservation => ({
  id,
  sessionId: "session-audit",
  timestamp: "2026-02-01T10:00:00Z",
  type: "file_edit",
  title: "Edit source file",
  facts: ["Updated main"],
  narrative: "Updated a source file",
  concepts: [],
  files: [],
  importance: 7,
  ...overrides,
});

describe("graph extraction without persistent audit history", () => {
  const originalGraphFlag = process.env["GRAPH_EXTRACTION_ENABLED"];
  const originalInputTarget = process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"];

  beforeEach(() => {
    process.env["GRAPH_EXTRACTION_ENABLED"] = "true";
  });

  afterEach(() => {
    vi.useRealTimers();
    if (originalGraphFlag === undefined) delete process.env["GRAPH_EXTRACTION_ENABLED"];
    else process.env["GRAPH_EXTRACTION_ENABLED"] = originalGraphFlag;
    if (originalInputTarget === undefined) delete process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"];
    else process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"] = originalInputTarget;
  });

  it("replays a completed provider job without duplicating graph effects", async () => {
    const harness = graphStateHarness();
    const compress = vi.fn().mockResolvedValue(response);
    const provider = { name: "frozen-test", compress, summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);

    const request = { observations: [observation("provider-replay-source")], graphJobId: "provider-replay-job" };
    await expect(harness.sdk.trigger({ function_id: "mem::graph-extract", payload: request }))
      .resolves.toMatchObject({ success: true });
    const firstNodes = await harness.kv.list(KV.graphNodes);
    const firstEdges = await harness.kv.list(KV.graphEdges);
    const firstSnapshot = await harness.kv.get<{ stats: { totalNodes: number; totalEdges: number } }>(KV.graphSnapshot, "current");

    await expect(harness.sdk.trigger({ function_id: "mem::graph-extract", payload: request }))
      .resolves.toMatchObject({ success: true });

    expect(compress).toHaveBeenCalledTimes(1);
    expect(await harness.kv.list(KV.graphNodes)).toEqual(firstNodes);
    expect(await harness.kv.list(KV.graphEdges)).toEqual(firstEdges);
    expect(firstSnapshot?.stats).toMatchObject({ totalNodes: 2, totalEdges: 1 });
    expect(await harness.sdk.trigger({ function_id: "mem::graph-recover", payload: {} }))
      .toMatchObject({ success: true, recovered: false });
    expect(await harness.kv.list(KV.audit)).toHaveLength(0);
  });

  it("replays heuristic graph effects without an audit marker", async () => {
    process.env["GRAPH_EXTRACTION_ENABLED"] = "false";
    const harness = graphStateHarness();
    const provider = { name: "frozen-test", compress: vi.fn(), summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);

    const request = {
      observations: [observation("heuristic-replay-source", { files: ["src/a.ts", "src/b.ts"], concepts: ["recovery"] })],
      graphJobId: "heuristic-replay-job",
    };
    await expect(harness.sdk.trigger({ function_id: "mem::graph-extract", payload: request }))
      .resolves.toMatchObject({ success: true });
    const firstNodes = await harness.kv.list(KV.graphNodes);
    const firstEdges = await harness.kv.list(KV.graphEdges);

    await expect(harness.sdk.trigger({ function_id: "mem::graph-extract", payload: request }))
      .resolves.toMatchObject({ success: true });

    expect(provider.compress).not.toHaveBeenCalled();
    expect(firstNodes).toHaveLength(3);
    expect(firstEdges).toHaveLength(3);
    expect(await harness.kv.list(KV.graphNodes)).toEqual(firstNodes);
    expect(await harness.kv.list(KV.graphEdges)).toEqual(firstEdges);
    expect(await harness.kv.list(KV.audit)).toHaveLength(0);
  });

  it("keeps independent provider jobs functional without saving per-group audit rows", async () => {
    process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"] = "4000";
    const harness = graphStateHarness();
    const compress = vi.fn().mockResolvedValue(response);
    const provider = { name: "frozen-test", compress, summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);

    const observations = [
      observation("group-a", { type: "error", narrative: "a".repeat(4500) }),
      observation("group-b", { type: "error", narrative: "b".repeat(4500) }),
    ];
    for (const graphJobId of ["provider-job-a", "provider-job-b"]) {
      await expect(harness.sdk.trigger({
        function_id: "mem::graph-extract",
        payload: { observations, graphJobId },
      })).resolves.toMatchObject({ success: true });
    }

    expect(compress).toHaveBeenCalledTimes(4);
    expect(await harness.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await harness.kv.list(KV.graphEdges)).toHaveLength(1);
    expect(await harness.kv.list(KV.audit)).toHaveLength(0);
  });

  it("propagates graph transaction failures without writing diagnostics", async () => {
    const harness = graphStateHarness();
    const provider = {
      name: "frozen-test",
      compress: vi.fn().mockRejectedValue(new StateTransactionError("STATE_TX_FAILED")),
      summarize: vi.fn(),
    } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);

    await expect(harness.sdk.trigger({
      function_id: "mem::graph-extract",
      payload: { observations: [observation("typed-failure-source")], graphJobId: "typed-failure-job" },
    })).rejects.toMatchObject({ code: "STATE_TX_FAILED" });
    expect(await harness.kv.list(KV.audit)).toHaveLength(0);
  });
});
