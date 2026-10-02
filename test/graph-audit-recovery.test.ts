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

describe("graph extraction audit replay", () => {
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

  it("reuses the exact provider audit after persistence succeeds but job finalization fails", async () => {
    const harness = graphStateHarness();
    const compress = vi.fn().mockResolvedValue(response);
    const provider = { name: "frozen-test", compress, summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);

    const originalSet = harness.kv.set.bind(harness.kv);
    const originalCommitBatch = harness.kv.commitBatch.bind(harness.kv);
    let providerAuditPersisted = false;
    let failedFinalization = false;
    Object.assign(harness.kv, {
      set: async (scope: string, key: string, value: unknown) => {
        const saved = await originalSet(scope, key, value);
        const details = (value as { details?: Record<string, unknown> } | null)?.details;
        if (
          scope === KV.audit &&
          (value as { functionId?: string } | null)?.functionId === "mem::graph-extract" &&
          details?.structural !== true &&
          typeof details?.nodesExtracted === "number"
        ) {
          providerAuditPersisted = true;
        }
        return saved;
      },
      commitBatch: async (...args: Parameters<typeof harness.kv.commitBatch>) => {
        if (providerAuditPersisted && !failedFinalization) {
          failedFinalization = true;
          throw new Error("simulated process interruption after provider audit persistence");
        }
        return originalCommitBatch(...args);
      },
    });

    const request = { observations: [observation("audit-replay-source")], graphJobId: "audit-replay-job" };
    await expect(harness.sdk.trigger({ function_id: "mem::graph-extract", payload: request }))
      .rejects.toThrow("simulated process interruption after provider audit persistence");
    expect(providerAuditPersisted).toBe(true);
    expect(failedFinalization).toBe(true);

    const auditsAfterCrash = await harness.kv.list(KV.audit) as Array<Record<string, unknown>>;
    expect(auditsAfterCrash).toHaveLength(1);
    const persistedAudit = structuredClone(auditsAfterCrash[0]);
    const persistedAuditJson = JSON.stringify(auditsAfterCrash);
    const nodesAfterCrash = await harness.kv.list(KV.graphNodes);
    const edgesAfterCrash = await harness.kv.list(KV.graphEdges);
    expect(nodesAfterCrash).toHaveLength(2);
    expect(edgesAfterCrash).toHaveLength(1);

    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2030-01-01T00:00:00.000Z"));
    await expect(harness.sdk.trigger({ function_id: "mem::graph-recover", payload: {} }))
      .resolves.toMatchObject({ success: true, recovered: true, jobId: "audit-replay-job" });

    expect(JSON.stringify(await harness.kv.list(KV.audit))).toBe(persistedAuditJson);
    expect(await harness.kv.list(KV.audit)).toEqual([persistedAudit]);
    expect(await harness.kv.list(KV.graphNodes)).toEqual(nodesAfterCrash);
    expect(await harness.kv.list(KV.graphEdges)).toEqual(edgesAfterCrash);
    expect(compress).toHaveBeenCalledTimes(1);
  });

  it("reuses the heuristic audit and graph effects after an interrupted job with a later retry clock", async () => {
    process.env["GRAPH_EXTRACTION_ENABLED"] = "false";
    const harness = graphStateHarness();
    const provider = { name: "frozen-test", compress: vi.fn(), summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);
    registerGraphJobRecovery(harness.sdk as never, harness.kv as never);

    const originalSet = harness.kv.set.bind(harness.kv);
    const originalCommitBatch = harness.kv.commitBatch.bind(harness.kv);
    let heuristicAuditPersisted = false;
    let failedFinalization = false;
    Object.assign(harness.kv, {
      set: async (scope: string, key: string, value: unknown) => {
        const saved = await originalSet(scope, key, value);
        const details = (value as { details?: Record<string, unknown> } | null)?.details;
        if (
          scope === KV.audit &&
          (value as { functionId?: string } | null)?.functionId === "mem::graph-extract" &&
          details?.structural === true
        ) {
          heuristicAuditPersisted = true;
        }
        return saved;
      },
      commitBatch: async (...args: Parameters<typeof harness.kv.commitBatch>) => {
        if (heuristicAuditPersisted && !failedFinalization) {
          failedFinalization = true;
          throw new Error("simulated process interruption after heuristic audit persistence");
        }
        return originalCommitBatch(...args);
      },
    });

    const request = {
      observations: [observation("heuristic-audit-source", { files: ["src/a.ts", "src/b.ts"], concepts: ["recovery"] })],
      graphJobId: "heuristic-audit-replay-job",
    };
    await expect(harness.sdk.trigger({ function_id: "mem::graph-extract", payload: request }))
      .rejects.toThrow("simulated process interruption after heuristic audit persistence");
    expect(heuristicAuditPersisted).toBe(true);
    expect(failedFinalization).toBe(true);
    expect(provider.compress).not.toHaveBeenCalled();

    const auditsAfterCrash = await harness.kv.list(KV.audit) as Array<Record<string, unknown>>;
    expect(auditsAfterCrash).toHaveLength(1);
    if (JSON.stringify(auditsAfterCrash[0].details) !== JSON.stringify({
      nodesExtracted: 3, edgesExtracted: 3, structural: true,
    })) throw new Error(`Unexpected heuristic audit details: ${JSON.stringify(auditsAfterCrash[0].details)}`);
    const persistedAuditJson = JSON.stringify(auditsAfterCrash);
    const nodesAfterCrash = await harness.kv.list(KV.graphNodes);
    const edgesAfterCrash = await harness.kv.list(KV.graphEdges);
    expect(nodesAfterCrash).toHaveLength(3);
    expect(edgesAfterCrash).toHaveLength(3);

    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(new Date("2035-01-01T00:00:00.000Z"));
    await expect(harness.sdk.trigger({ function_id: "mem::graph-recover", payload: {} }))
      .resolves.toMatchObject({ success: true, recovered: true, jobId: "heuristic-audit-replay-job" });

    expect(JSON.stringify(await harness.kv.list(KV.audit))).toBe(persistedAuditJson);
    expect(await harness.kv.list(KV.graphNodes)).toEqual(nodesAfterCrash);
    expect(await harness.kv.list(KV.graphEdges)).toEqual(edgesAfterCrash);
    expect(provider.compress).not.toHaveBeenCalled();
  });

  it("keeps provider groups and independent durable jobs as separate audit effects", async () => {
    process.env["AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS"] = "4000";
    const harness = graphStateHarness();
    const compress = vi.fn().mockResolvedValue(response);
    const provider = { name: "frozen-test", compress, summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);

    const observations = [
      observation("group-a", { type: "error", narrative: "a".repeat(4500) }),
      observation("group-b", { type: "error", narrative: "b".repeat(4500) }),
    ];
    for (const graphJobId of ["audit-job-a", "audit-job-b"]) {
      await expect(harness.sdk.trigger({
        function_id: "mem::graph-extract",
        payload: { observations, graphJobId },
      })).resolves.toMatchObject({ success: true });
    }

    const audits = await harness.kv.list(KV.audit) as Array<{
      id: string;
      targetIds: string[];
      details: Record<string, unknown>;
    }>;
    expect(compress).toHaveBeenCalledTimes(4);
    expect(audits).toHaveLength(4);
    expect(new Set(audits.map((entry) => entry.id)).size).toBe(4);
    expect(audits.map((entry) => entry.targetIds).sort((a, b) => a[0].localeCompare(b[0])))
      .toEqual([["group-a"], ["group-a"], ["group-b"], ["group-b"]]);
    expect(audits.every((entry) => entry.details.nodesExtracted === 2 && entry.details.edgesExtracted === 1)).toBe(true);
    expect(await harness.kv.list(KV.graphNodes)).toHaveLength(2);
    expect(await harness.kv.list(KV.graphEdges)).toHaveLength(1);
  });

  it("keeps heuristic and provider audit effects distinct within one durable job", async () => {
    const harness = graphStateHarness();
    const compress = vi.fn().mockResolvedValue(response);
    const provider = { name: "frozen-test", compress, summarize: vi.fn() } as unknown as MemoryProvider;
    registerGraphFunction(harness.sdk as never, harness.kv as never, provider);

    await expect(harness.sdk.trigger({
      function_id: "mem::graph-extract",
      payload: {
        observations: [observation("mixed-audit-source", { files: ["src/a.ts", "src/b.ts"], concepts: ["resilience"] })],
        graphJobId: "mixed-audit-job",
      },
    })).resolves.toMatchObject({ success: true });

    const audits = await harness.kv.list(KV.audit) as Array<{
      id: string;
      targetIds: string[];
      details: Record<string, unknown>;
    }>;
    expect(compress).toHaveBeenCalledTimes(1);
    expect(audits).toHaveLength(2);
    expect(new Set(audits.map((entry) => entry.id)).size).toBe(2);
    expect(audits.map((entry) => entry.targetIds)).toEqual([["mixed-audit-source"], ["mixed-audit-source"]]);
    expect(audits.map((entry) => entry.details.structural)).toEqual([true, undefined]);
  });

  it("propagates typed graph transaction failures from provider execution", async () => {
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
