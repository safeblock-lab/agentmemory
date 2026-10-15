import { AsyncLocalStorage } from "node:async_hooks";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StateKV } from "../src/state/kv.js";
import { WorkflowAttemptAccounting, runWorkflowAttempt, loadWorkflowAttemptAccounting, type WorkflowAttemptIdentity } from "../src/functions/workflow-attempt-accounting.js";
import { fingerprintId } from "../src/state/schema.js";

const transport = vi.hoisted(() => ({ claims: new Set<string>(), grants: new Map<string, { windowId: string; ceiling: number; context?: Record<string, string> }>(),
  used: new Map<string, number>(), entries: vi.fn(), storage: undefined as unknown }));
vi.mock("../src/providers/provider-attempt-budget.js", () => ({
  withProviderAttemptBudget: async (binding: { operationId: string; windowId: string; ceiling: number; context: Record<string, string> }, run: () => Promise<unknown>) => {
    transport.entries(binding);
    const grant = transport.grants.get(binding.operationId);
    if (!grant || grant.windowId !== binding.windowId || grant.ceiling !== binding.ceiling ||
        JSON.stringify(grant.context) !== JSON.stringify(binding.context)) throw new Error("binding_mismatch");
    if (transport.claims.has(binding.operationId)) throw new Error("operation_already_claimed");
    transport.claims.add(binding.operationId);
    return (transport.storage as AsyncLocalStorage<typeof binding>).run(binding, run);
  },
}));
const identity: WorkflowAttemptIdentity = { workflow: "summary", functionId: "mem::summary-unit", project: "p",
  sourceId: "s", sourceFingerprint: "snapshot-v1", jobId: "job", unitId: "map-1", phase: "generation", continuation: "[0,map]" };
function store() {
  const rows = new Map<string, unknown>();
  return { rows, kv: { get: async (_scope: string, key: string) => rows.get(key) ?? null,
    set: async (_scope: string, key: string, value: unknown) => { rows.set(key, structuredClone(value)); } } as unknown as StateKV };
}
const binding = (value = identity, operationId = "op-1") => ({ ...value, operationId, windowId: "window", ceiling: 2 });
function approve(operationId = "op-1", ceiling = 2, value = identity) {
  transport.grants.set(operationId, { windowId: "window", ceiling, context: {
    workflow: value.workflow, functionId: value.functionId, sourceId: value.sourceId, jobId: value.jobId,
    unitId: value.unitId, phase: value.phase, continuationId: fingerprintId("workflow-continuation", value.continuation),
  } });
}
async function fetchMock() {
  const active = (transport.storage as AsyncLocalStorage<ReturnType<typeof binding>>).getStore();
  if (!active) throw new Error("scope_missing");
  const used = transport.used.get(active.operationId) ?? 0;
  if (used >= active.ceiling) throw new Error("operation_exhausted");
  transport.used.set(active.operationId, used + 1);
  return "captured";
}
beforeEach(() => { transport.claims.clear(); transport.grants.clear(); transport.used.clear(); transport.entries.mockClear();
  transport.storage = new AsyncLocalStorage(); approve(); });

describe("daemon startup workflow configuration", () => {
  const configuration = () => ({ version: 1,
    scopes: [{ workflow: identity.workflow, project: identity.project, sourceId: identity.sourceId }], bindings: [binding()] });
  it("preserves absent configuration and ordinary unscoped execution", () => {
    expect(loadWorkflowAttemptAccounting(undefined)).toBeUndefined();
  });
  it("loads exact private binding into actual provider owner", async () => {
    const accounting = loadWorkflowAttemptAccounting(JSON.stringify(configuration()));
    expect(await runWorkflowAttempt(accounting, store().kv, identity, fetchMock)).toBe("captured");
    expect(transport.entries).toHaveBeenCalledOnce();
  });
  it.each(["", "{", "x".repeat(65537), '{"version":1,"scopes":[],"bindings":[],"authority":true}'])("refuses malformed or oversized input", raw => {
    expect(() => loadWorkflowAttemptAccounting(raw)).toThrow("workflow_binding_configuration_invalid");
  });
  it("refuses duplicate scopes and unapproved binding fields", () => {
    const duplicate = configuration(); duplicate.scopes.push(duplicate.scopes[0]);
    expect(() => loadWorkflowAttemptAccounting(JSON.stringify(duplicate))).toThrow("workflow_binding_configuration_invalid");
    expect(() => loadWorkflowAttemptAccounting(JSON.stringify({ ...configuration(), bindings: [{ ...binding(), grantPath: "injected" }] })))
      .toThrow("workflow_binding_configuration_invalid");
  });
  it("cannot authorize a binding absent from native private grants", async () => {
    transport.grants.clear();
    const accounting = loadWorkflowAttemptAccounting(JSON.stringify(configuration()));
    await expect(runWorkflowAttempt(accounting, store().kv, identity, fetchMock)).rejects.toThrow("binding_mismatch");
    expect(transport.used.size).toBe(0);
  });
  it("loads graph identity through the same private startup configuration", async () => {
    const graph = { ...identity, workflow: "graph" as const, functionId: "mem::graph-external-value",
      continuation: '["1","delta:2",2,"provider-response","intent-id"]' };
    approve("graph-op", 2, graph);
    const accounting = loadWorkflowAttemptAccounting(JSON.stringify({ version: 1,
      scopes: [{ workflow: graph.workflow, project: graph.project, sourceId: graph.sourceId }],
      bindings: [binding(graph, "graph-op")] }));
    expect(await runWorkflowAttempt(accounting, store().kv, graph, fetchMock)).toBe("captured");
  });
});

describe("private daemon bindings against mocked future State admission", () => {
  it("enters scope at actual dispatch and retains assertions across rehydration", async () => {
    const { kv, rows } = store(); const configuration = binding();
    expect(await new WorkflowAttemptAccounting([identity], [configuration]).run(kv, identity, fetchMock)).toBe("captured");
    expect(rows.size).toBe(2);
    await expect(new WorkflowAttemptAccounting([identity], [configuration]).run(kv, identity, fetchMock)).rejects.toThrow("operation_already_claimed");
    expect(transport.used.get("op-1")).toBe(1);
  });
  it.each(["functionId", "sourceFingerprint", "jobId", "unitId", "phase", "continuation"] as const)("rejects wrong %s before admission", async field => {
    const { kv } = store(); const changed = { ...identity, [field]: "wrong" } as WorkflowAttemptIdentity;
    await expect(new WorkflowAttemptAccounting([identity], [binding()]).run(kv, changed, fetchMock)).rejects.toThrow("workflow_binding_identity_mismatch");
    expect(transport.entries).not.toHaveBeenCalled();
  });
  it.each(["windowId", "ceiling"] as const)("binding %s assertion grants no authority", async field => {
    const { kv } = store(); const forged = { ...binding(), [field]: field === "ceiling" ? 99 : "wrong" };
    await expect(new WorkflowAttemptAccounting([identity], [forged]).run(kv, identity, fetchMock)).rejects.toThrow("binding_mismatch");
    expect(transport.used.size).toBe(0);
  });
  it("rejects changed durable assertion after configuration mutation", async () => {
    const { kv } = store(); await new WorkflowAttemptAccounting([identity], [binding()]).run(kv, identity, fetchMock);
    await expect(new WorkflowAttemptAccounting([identity], [{ ...binding(), ceiling: 3 }]).run(kv, identity, fetchMock)).rejects.toThrow("workflow_binding_assertion_mismatch");
  });
  it("rejects removal of private scope after rehydration", async () => {
    const { kv } = store(); await new WorkflowAttemptAccounting([identity], [binding()]).run(kv, identity, fetchMock);
    await expect(runWorkflowAttempt(undefined, kv, identity, fetchMock)).rejects.toThrow("workflow_binding_configuration_missing");
    await expect(new WorkflowAttemptAccounting([], []).run(kv, identity, fetchMock)).rejects.toThrow("workflow_binding_configuration_missing");
  });
  it("separates generation and publication operation approvals", async () => {
    const { kv } = store(); const publication = { ...identity, phase: "publication" as const, unitId: "judge-1" };
    approve("op-judge", 2, publication); const runner = new WorkflowAttemptAccounting([identity], [binding(), binding(publication, "op-judge")]);
    await runner.run(kv, identity, fetchMock); await runner.run(kv, publication, fetchMock);
    expect([...transport.claims]).toEqual(["op-1", "op-judge"]);
  });
  it("concurrent duplicates cannot dispatch twice", async () => {
    const { kv } = store(); const runner = new WorkflowAttemptAccounting([identity], [binding()]);
    const results = await Promise.allSettled([runner.run(kv, identity, fetchMock), runner.run(kv, identity, fetchMock)]);
    expect(results.filter(result => result.status === "fulfilled")).toHaveLength(1);
    expect(transport.used.get("op-1")).toBe(1);
  });
  it("retries and fallback consume same scope, exhaustion precedes mock fetch", async () => {
    const { kv } = store(); let effects = 0;
    await expect(new WorkflowAttemptAccounting([identity], [binding()]).run(kv, identity, async () => {
      await fetchMock(); effects++; await fetchMock(); effects++; await fetchMock(); effects++;
    })).rejects.toThrow("operation_exhausted");
    expect(effects).toBe(2); expect(transport.entries).toHaveBeenCalledTimes(1);
  });
  it("lost admission ACK and unknown operation cannot create fresh identity", async () => {
    const { kv } = store(); transport.claims.add("op-1");
    const runner = new WorkflowAttemptAccounting([identity], [binding()]);
    await expect(runner.run(kv, identity, fetchMock)).rejects.toThrow("operation_already_claimed");
    await expect(runner.run(kv, { ...identity, unitId: "fresh" }, fetchMock)).rejects.toThrow("workflow_binding_identity_mismatch");
    expect(transport.used.size).toBe(0);
  });
  it("captured replay outside dispatcher makes zero claims", async () => {
    const { kv } = store(); const capture = "saved";
    const replay = capture ?? await new WorkflowAttemptAccounting([identity], [binding()]).run(kv, identity, fetchMock);
    expect(replay).toBe("saved"); expect(transport.entries).not.toHaveBeenCalled();
  });
  it("normal enabled unscoped automation keeps behavior", async () => {
    const { kv } = store(); const run = vi.fn(async () => "automatic");
    expect(await new WorkflowAttemptAccounting([identity], [binding()]).run(kv, { ...identity, sourceId: "normal-session" }, run)).toBe("automatic");
    expect(await runWorkflowAttempt(undefined, kv, identity, run)).toBe("automatic");
    expect(run).toHaveBeenCalledTimes(2); expect(transport.entries).not.toHaveBeenCalled();
  });
  it("distinct logical dispatches cannot share a durable operation", () => {
    expect(() => new WorkflowAttemptAccounting([identity], [binding(), binding({ ...identity, unitId: "map-2" })])).toThrow("workflow_binding_configuration_invalid");
  });
  it.each(["missing", "extra"])("native private context equality rejects %s authority assertions", async mode => {
    const { kv } = store(); const grant = transport.grants.get("op-1")!;
    transport.grants.set("op-1", { ...grant, context: mode === "missing" ? undefined : { ...grant.context, extra: "forged" } });
    await expect(new WorkflowAttemptAccounting([identity], [binding()]).run(kv, identity, fetchMock)).rejects.toThrow("binding_mismatch");
    expect(transport.used.size).toBe(0);
  });
});
