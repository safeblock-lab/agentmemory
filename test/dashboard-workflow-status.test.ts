import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { buildWorkflowStatus, type WorkflowStatusInputs } from "../src/functions/dashboard-operational-metrics.js";
import { registerApiTriggers } from "../src/triggers/api.js";
import { KV } from "../src/state/schema.js";

function input(overrides: Partial<WorkflowStatusInputs> = {}): WorkflowStatusInputs {
  return {
    backgroundRecoveryPaused: false, providerAvailable: true, providerCircuitOpen: false, nativeAvailable: true,
    configured: { graph: true, consolidation: true, compression: true, reflection: true, snapshots: true, maintenance: true },
    activity: { source: "native-operation-feed", limit: 30, items: [] }, ...overrides,
  };
}
function state(value: WorkflowStatusInputs, key: string) {
  return buildWorkflowStatus(value).items.find(item => item.key === key)!;
}

describe("workflow availability and observed results", () => {
  it("shows all enabled without claiming unobserved execution succeeded", () => {
    const result = buildWorkflowStatus(input());
    expect(result.backgroundRecoveryPaused).toBe(false);
    expect(result.items).toHaveLength(8);
    expect(result.items.every(item => item.state === "enabled" && item.lastOutcome === null)).toBe(true);
  });
  it("distinguishes configured off from paused background recovery", () => {
    const value = input({ backgroundRecoveryPaused: true });
    value.configured.graph = false;
    expect(state(value, "graph")).toMatchObject({ state: "disabled", configured: false });
    expect(state(value, "summaries")).toMatchObject({ state: "paused", configured: true });
    expect(state(value, "snapshots").state).toBe("enabled");
  });
  it("reports missing dependencies and missing evidence", () => {
    expect(state(input({ providerAvailable: false }), "skills").reason).toBe("provider_unavailable");
    expect(state(input({ providerCircuitOpen: true }), "summaries").reason).toBe("provider_circuit_open");
    expect(state(input({ nativeAvailable: false }), "graph").reason).toBe("native_state_unavailable");
    expect(state(input({ nativeAvailable: false }), "maintenance").state).toBe("dependency_failure");
    expect(state(input({ activity: null }), "skills").state).toBe("unavailable");
    expect(state(input({ slotsAvailable: false }), "reflection").reason).toBe("slots_disabled");
  });
  it("does not hide a failed extraction behind a newer queued delivery", () => {
    const value = input();
    value.activity!.items = [
      { id: "op_queue", timestamp: "2026-10-13T18:45:18Z", functionId: "mem::graph-dispatch-deliver", name: "Queued graph extraction", outcome: "queued", counts: { unitsQueued: 1 } },
      { id: "op_failure", timestamp: "2026-10-13T18:45:17Z", functionId: "mem::graph-extract", name: "Graph extraction", outcome: "failed", counts: {}, error: { code: "graph_extraction_failed", message: "Graph extraction failed." } },
    ];
    expect(state(value, "graph")).toMatchObject({ state: "failed", lastOutcome: "failed", lastObservedAt: "2026-10-13T18:45:17Z" });
    expect(JSON.stringify(buildWorkflowStatus(value))).not.toContain("op_failure");
  });
  it("assembles authenticated status repeatedly without executing workflows or writing events", async () => {
    const handlers = new Map<string, (request: unknown) => Promise<{ status_code: number; body: unknown }>>();
    const trigger = vi.fn(async () => null);
    const update = vi.fn(async () => ({}));
    const kv = {
      indexedRetrieval: true,
      get: vi.fn(async (scope: string) => scope === KV.dashboardFailureMinutes
        ? { schemaVersion: 1, coverageSince: "2026-10-01T00:00:00Z" }
        : scope === KV.recentOperations ? { items: [] } : null),
      update, graphStorageMetadata: async () => null,
      retrieval: async () => { throw new Error("native unavailable"); },
    };
    registerApiTriggers({ registerFunction: (id: string, handler: never) => handlers.set(id, handler), registerTrigger: () => {}, trigger } as never, kv as never, "secret");
    const handler = handlers.get("api::status")!;
    expect((await handler({ headers: {} })).status_code).toBe(401);
    expect(trigger).not.toHaveBeenCalled();
    for (let i = 0; i < 2; i++) {
      const response = await handler({ headers: { authorization: "Bearer secret" } });
      expect(response.status_code).toBe(200);
      expect(response.body).toHaveProperty("operationalMetrics.workflows.backgroundRecoveryPaused");
      const workflows = (response.body as { operationalMetrics: { workflows: ReturnType<typeof buildWorkflowStatus> } }).operationalMetrics.workflows;
      expect(workflows.items.find(item => item.key === "skills")).toMatchObject({ state: "dependency_failure", reason: "provider_unavailable" });
    }
    expect(trigger.mock.calls.every(call => (call as unknown as [{ function_id: string }])[0].function_id === "mem::graph-stats")).toBe(true);
    expect(trigger).toHaveBeenCalledTimes(2);
    expect(update).not.toHaveBeenCalled();
  }, 30_000);
  it("renders enabled, paused and failed states with escaped content and no controls that trigger work", () => {
    const source = readFileSync("src/viewer/index.html", "utf8");
    const body = source.slice(source.indexOf("    function renderWorkflowStatus("), source.indexOf("    function usagePercent("));
    const render = new Function("metricRow", "metricDate", body + ";return renderWorkflowStatus;")(
      (label: string, status: string, detail: string) => `${label}:${status}:${detail}`.replaceAll("<", "&lt;"), (value: string) => value,
    );
    const value = buildWorkflowStatus(input({ backgroundRecoveryPaused: true }));
    value.items[0]!.label = "<script>";
    expect(render({ workflows: value })).toContain("Paused");
    expect(render({ workflows: value })).toContain("&lt;script>");
    expect(render({ workflows: value })).not.toContain("<script>");
    const enabled = buildWorkflowStatus(input());
    enabled.items[2]!.state = "failed";
    enabled.items[2]!.lastOutcome = "failed";
    enabled.items[2]!.lastObservedAt = "2026-10-13T18:45:17Z";
    expect(render({ workflows: enabled })).toContain("Graph extraction:Failed");
    expect(render({ workflows: enabled })).toContain("Enabled means eligible");
    expect(render({ workflows: enabled })).toContain("Eligible to run; no success inferred");
    expect(render(null)).toContain("Workflow status unavailable");
    expect(body).not.toMatch(/apiPost|setTimeout|fetch\(/);
  });
});
