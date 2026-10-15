import * as audit from "../src/functions/audit.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { registerSkillLifecycleFunctions } from "../src/functions/skill-lifecycle.js";
import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { summarySourceFingerprint } from "../src/functions/summary-queue.js";
import { KV } from "../src/state/schema.js";

vi.mock("../src/config.js", async original => ({ ...await original<typeof import("../src/config.js")>(),
  isBackgroundRecoveryPaused: () => true, isConsolidationEnabled: () => false, detectLlmProviderKind: () => "llm" }));

function fixture() {
  const rows = new Map<string, any>();
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const put = (scope: string, id: string, value: unknown) => rows.set(key(scope, id), structuredClone(value));
  const get = (scope: string, id: string) => rows.get(key(scope, id));
  const kv = {
    get: vi.fn(async (scope: string, id: string) => structuredClone(get(scope, id) ?? null)),
    set: vi.fn(async (scope: string, id: string, value: unknown) => { put(scope, id, value); return value; }),
    list: vi.fn(async (scope: string) => [...rows].filter(([id]) => id.startsWith(`${scope}\0`)).map(([, value]) => structuredClone(value))),
    pages: vi.fn(async function* (scope: string) {
      yield { items: [...rows].filter(([id]) => id.startsWith(`${scope}\0`)).map(([, value]) => structuredClone(value)), next_cursor: null };
    }),
  };
  const handlers: Record<string, Function> = {};
  const sdk = { registerFunction: (id: string, handler: Function) => { handlers[id] = handler; }, registerTrigger: vi.fn(),
    trigger: vi.fn(async (request: any) => handlers[request.function_id]?.(request.payload) ?? { success: true }) };
  put(KV.sessions, "s", { id: "s", project: "p", status: "completed" });
  put(KV.summaries, "s", { sessionId: "s", project: "p", title: "Setup", narrative: "Install then validate",
    keyDecisions: [], filesModified: [], concepts: [] });
  const observations = [0, 1, 2].map(index => ({ id: `o${index}`, sessionId: "s", timestamp: "2026-10-11",
    type: "file_edit", title: "Setup", narrative: "Install then validate", importance: 7, concepts: [], facts: [], files: [] }));
  observations.forEach(observation => put(KV.observations("s"), observation.id, observation));
  const provider = { name: "test", compress: vi.fn(),
    summarize: vi.fn(async (system: string) => system.includes("evaluate publication evidence")
      ? JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)?.[1], verdict: "supported", reason: "Supported" })
      : "<skill><trigger>New project</trigger><title>Setup</title><steps><step>Install</step><step>Validate</step></steps></skill>") };
  registerSkillLifecycleFunctions(sdk as any, kv as any);
  registerSkillExtractFunctions(sdk as any, kv as any, provider, undefined, undefined,
    async () => observations.map(observation => ({ id: observation.id, sessionId: "s", timestamp: observation.timestamp })));
  return { kv, rows, get, put, provider, sdk, handlers };
}

afterEach(() => { vi.restoreAllMocks(); });

describe("skill applied effect receipt ACK", () => {
  it.each(["deleted", "changed", "missing_receipt", "wrong_receipt"])("recovers only an exact applied receipt after audit cut: %s", async change => {
    const h = fixture();
    const fingerprint = await summarySourceFingerprint(h.kv as any, "s", "p");
    h.put(KV.summaryQueueCompleted, "s", { sessionId: "s", fingerprint, completedAt: "2026-10-11" });
    const control = { project: "p", sessionId: "s", sourceFingerprint: fingerprint };
    const admitted = await h.handlers["mem::skill-lifecycle-session-ended"]({ sessionId: "s", selected: control });
    const intentId = admitted.intentId;
    const dispatch = () => h.handlers["mem::skill-lifecycle-dispatch"]({ intentId, foreground: true,
      selected: { ...control, intentId } });
    vi.spyOn(audit, "recordAudit").mockRejectedValueOnce(new Error("audit ACK cut"));
    const cut = await dispatch();
    expect(cut).toMatchObject({ queued: true, failureCode: "skill_extraction_failed" });
    const intent = h.get(KV.skillLifecycleIntents, intentId);
    const id = intent.effectPlan.skillId;
    const applied = structuredClone(h.get(KV.procedural, id));
    expect(applied.appliedBatchEffects).toContain(intent.effectId);
    if (change === "changed") h.put(KV.observations("s"), "o0", { ...h.get(KV.observations("s"), "o0"), narrative: "Source changed" });
    else h.rows.delete(`${KV.observations("s")}\0o0`);
    if (change === "missing_receipt") h.put(KV.procedural, id, { ...applied, appliedBatchEffects: [] });
    if (change === "wrong_receipt") h.put(KV.procedural, id, { ...applied, appliedBatchEffects: ["c".repeat(64)] });
    const result = await dispatch();
    if (change === "missing_receipt" || change === "wrong_receipt") {
      expect(result).toMatchObject({ stale: true });
      expect(h.get(KV.skillLifecycleIntents, intentId).status).toBe("stale");
    } else {
      expect(result).toMatchObject({ completed: true, effectReplay: true });
      expect(h.get(KV.procedural, id)).toEqual(applied);
      expect(h.get(KV.skillLifecycleIntents, intentId)).toMatchObject({ status: "completed", skillId: id });
      const writes = h.kv.set.mock.calls.length;
      await dispatch();
      expect(h.kv.set.mock.calls.length).toBe(writes);
    }
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
  it("acknowledges a lost completion write without a new effect or evaluator", async () => {
    const h = fixture();
    const fingerprint = await summarySourceFingerprint(h.kv as any, "s", "p");
    h.put(KV.summaryQueueCompleted, "s", { sessionId: "s", fingerprint, completedAt: "2026-10-11" });
    const control = { project: "p", sessionId: "s", sourceFingerprint: fingerprint };
    const admitted = await h.handlers["mem::skill-lifecycle-session-ended"]({ sessionId: "s", selected: control });
    const intentId = admitted.intentId;
    const dispatch = () => h.handlers["mem::skill-lifecycle-dispatch"]({ intentId, foreground: true,
      selected: { ...control, intentId } });
    const set = h.kv.set.getMockImplementation()!;
    let cut = true;
    h.kv.set.mockImplementation(async (scope, id, value: any) => {
      if (cut && scope === KV.skillLifecycleIntents && id === intentId && value.status === "completed") { cut = false; throw new Error("lost completion ACK"); }
      return set(scope, id, value);
    });
    expect(await dispatch()).toMatchObject({ queued: true });
    const id = h.get(KV.skillLifecycleIntents, intentId).effectPlan.skillId;
    const applied = structuredClone(h.get(KV.procedural, id));
    h.rows.delete(`${KV.observations("s")}\0o0`);
    expect(await dispatch()).toMatchObject({ completed: true, effectReplay: true });
    expect(h.get(KV.procedural, id)).toEqual(applied);
    expect(h.provider.summarize).toHaveBeenCalledTimes(2);
  });
});
