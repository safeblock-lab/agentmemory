import { beforeEach, describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import { registerSkillLifecycleFunctions, type SkillLifecycleIntent } from "../src/functions/skill-lifecycle.js";
import { registerSkillExtractFunctions, type SkillObservationSelector } from "../src/functions/skill-extract.js";
import { IndexedRetrievalError } from "../src/state/indexed-retrieval.js";
import { recordAudit } from "../src/functions/audit.js";

const config = vi.hoisted(() => ({ paused: false }));
vi.mock("../src/config.js", async (original) => ({
  ...await original<typeof import("../src/config.js")>(),
  isBackgroundRecoveryPaused: () => config.paused,
  isConsolidationEnabled: () => true,
  detectLlmProviderKind: () => "llm",
}));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));

describe("durable skill lifecycle", () => {
  let rows: Map<string, unknown>;
  let handlers: Record<string, (data: any) => Promise<any>>;
  let published: Array<{ intentId: string; deliveryId: string }>;
  let kv: StateKV;
  let provider: { name: string; summarize: ReturnType<typeof vi.fn>; compress: ReturnType<typeof vi.fn> };
  let failCompletion: boolean;
  let preparationDeliveries: number;
  let resourceLimitDeliveries: number;
  let includeRetrievalCoverage: boolean;
  let selectorCalls: number;
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const put = (scope: string, id: string, value: unknown) => rows.set(key(scope, id), structuredClone(value));
  const intent = () => [...rows.values()].find((row: any) => row.kind === "intent") as SkillLifecycleIntent;
  const call = (name: string, data: unknown = {}) => handlers[`mem::skill-lifecycle-${name}`](data);

  beforeEach(() => {
    config.paused = false;
    failCompletion = false;
    preparationDeliveries = 0;
    resourceLimitDeliveries = 0;
    includeRetrievalCoverage = false;
    selectorCalls = 0;
    rows = new Map(); handlers = {}; published = [];
    kv = {
      get: vi.fn(async (scope, id) => structuredClone(rows.get(key(scope, id)) ?? null)),
      set: vi.fn(async (scope, id, value) => {
        if (failCompletion && value.status === "completed" && value.kind === "intent") {
          failCompletion = false; throw new Error("simulated completion write interruption");
        }
        put(scope, id, value); return value;
      }),
      list: vi.fn(async (scope) => [...rows].filter(([id]) => id.startsWith(`${scope}\0`)).map(([, row]) => structuredClone(row))),
      values: vi.fn(async function* () {}),
      pages: vi.fn(async function* () { yield { items: [], next_cursor: null }; }),
    } as unknown as StateKV;
    const sdk = {
      registerFunction: (id: string, handler: typeof handlers[string]) => { handlers[id] = handler; },
      registerTrigger: vi.fn(),
      trigger: vi.fn(async ({ function_id, payload }) => {
        if (function_id === "iii::durable::publish") { published.push(payload.data); return {}; }
        return handlers[function_id](payload);
      }),
    } as unknown as IIIClient;
    provider = { name: "mock", compress: vi.fn(), summarize: vi.fn(async (system: string) => {
      if (system.startsWith("You evaluate publication evidence.")) return JSON.stringify({
        requestId: /requestId must be ([^.]+)\./.exec(system)?.[1], verdict: "supported",
        reason: "The fixture describes the same repair workflow as its original sources.",
      });
      return "<skill><trigger>When fixing a failure</trigger><title>Repair</title><steps><step>Inspect</step><step>Repair</step></steps><expected_outcome>Success</expected_outcome><tags>repair</tags></skill>";
    }) };
    const selectObservations: SkillObservationSelector = async (project, sessionId) => {
      selectorCalls++;
      if (project !== "p" || sessionId !== "s1") return [];
      if (preparationDeliveries > 0) {
        preparationDeliveries--;
        throw new IndexedRetrievalError("STATE_INDEX_NOT_READY", "coverage pending");
      }
      if (resourceLimitDeliveries > 0) {
        resourceLimitDeliveries--;
        throw new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "native query work budget exhausted");
      }
      return ["o0", "o1", "o2"].map((id, index) => ({ id, sessionId, timestamp: "2026-10-09T00:00:00Z",
        ...(includeRetrievalCoverage ? { retrievalCoverage: [index === 0 ? "bounded_query_budget" : "bounded_candidate_budget"] } : {}) })) as any;
    };
    registerSkillExtractFunctions(sdk, kv, provider, undefined, undefined, selectObservations);
    registerSkillLifecycleFunctions(sdk, kv);
    put(KV.sessions, "s1", { id: "s1", project: "p", status: "completed" });
    put(KV.summaries, "s1", { sessionId: "s1", project: "p", title: "Repair failure", narrative: "Fixed",
      keyDecisions: [], filesModified: [], concepts: ["repair"] });
    for (let i = 0; i < 3; i++) put(KV.observations("s1"), `o${i}`, {
      id: `o${i}`, sessionId: "s1", title: "Repair", narrative: "Step", importance: 7, type: "file_edit",
      timestamp: "2026-10-09T00:00:00Z", concepts: [], facts: [],
    });
  });

  async function ready() {
    await call("session-stopped", { sessionId: "s1" });
    put(KV.summaryQueueCompleted, "s1", { sessionId: "s1", fingerprint: "fp", completedAt: "2026-10-09" });
    return call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
  }

  it("does not dispatch before the finalized matching summary marker", async () => {
    await call("session-stopped", { sessionId: "s1" });
    expect(await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" })).toMatchObject({ queued: false });
    put(KV.summaryQueueCompleted, "s1", { fingerprint: "other" });
    await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
    expect(published).toHaveLength(0);
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("rejects superseded and foreign deliveries before provider calls", async () => {
    await ready();
    expect(await call("dispatch", { ...published[0], deliveryId: "foreign" })).toMatchObject({ stale: true });
    put(KV.summaryQueueCompleted, "s1", { fingerprint: "new" });
    expect(await call("dispatch", published[0])).toMatchObject({ stale: true });
    expect(intent().status).toBe("stale");
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("honors pause for delivery and marker recovery", async () => {
    await ready(); config.paused = true;
    expect(await call("dispatch", published[0])).toMatchObject({ paused: true });
    expect(await call("replay")).toMatchObject({ paused: true, inspected: 0 });
    expect(provider.summarize).not.toHaveBeenCalled();
    expect(kv.pages).not.toHaveBeenCalled();
  });

  it("retains captured provider result across interruption after the skill effect", async () => {
    includeRetrievalCoverage = true;
    vi.mocked(recordAudit).mockClear();
    await ready(); failCompletion = true;
    expect(await call("dispatch", published[0])).toMatchObject({ queued: true });
    expect(intent().capturedResult).toMatchObject({ kind: "skill",
      retrievalCoverage: ["bounded_candidate_budget", "bounded_query_budget"] });
    expect(intent().retrievalCoverage).toEqual(["bounded_candidate_budget", "bounded_query_budget"]);
    expect(selectorCalls).toBe(1);
    const retry = intent();
    put(KV.skillLifecycleIntents, retry.id, { ...retry, nextAttemptAt: undefined });
    await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
    const completed = await call("dispatch", published.at(-1));
    expect(completed).toMatchObject({ completed: true,
      retrievalCoverage: ["bounded_candidate_budget", "bounded_query_budget"] });
    expect(provider.summarize).toHaveBeenCalledTimes(2);
    expect(provider.summarize.mock.calls.filter(([system]) => String(system).startsWith("You evaluate publication evidence."))).toHaveLength(1);
    expect(selectorCalls).toBe(1);
    const skills = [...rows].filter(([id]) => id.startsWith(`${KV.procedural}\0`)).map(([, row]) => row as any);
    expect(skills).toHaveLength(1);
    expect(skills[0]).toMatchObject({ frequency: 1, sourceSessionIds: ["s1"] });
    expect(intent().capturedResult?.kind).toBe("skill");
    await call("dispatch", published.at(-1));
    expect(provider.summarize).toHaveBeenCalledTimes(2);
    expect(selectorCalls).toBe(1);
  });

  it("persists bounded coverage with a queued fragment continuation", async () => {
    await ready();
    const current = intent();
    const retrievalCoverage = ["bounded_candidate_budget", "bounded_query_budget"];
    handlers["mem::skill-extract"] = async () => ({ success: true, queued: true, completed: false,
      reason: "skill_source_fragmentation", jobId: "skillparts_coverage", retrievalCoverage,
      continuation: { sessionId: current.sessionId, intentId: current.id,
        snapshotFingerprint: current.summaryFingerprint, retrievalCoverage } });

    const result = await call("dispatch", published[0]);

    expect(result).toMatchObject({ success: true, queued: true, reason: "skill_source_fragmentation",
      retrievalCoverage, continuation: { retrievalCoverage } });
    expect(intent()).toMatchObject({ status: "queued", attempts: 0, retrievalCoverage });
    expect(intent().nextAttemptAt).toBeUndefined();
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("retries resource-limited source retrieval and terminalizes on actual attempt six", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-09T00:00:00.000Z"));
    vi.mocked(recordAudit).mockClear();
    try {
      await ready();
      resourceLimitDeliveries = 6;
      for (let attempt = 1; attempt < 6; attempt++) {
        const result = await call("dispatch", published.at(-1));
        expect(result).toMatchObject({ success: true, queued: true,
          failureCode: "source_retrieval_resource_limit", reason: "source_retrieval_resource_limit" });
        expect(intent()).toMatchObject({ status: "queued", attempts: attempt,
          sessionId: "s1", summaryFingerprint: "fp",
          lastFailure: { code: "source_retrieval_resource_limit" } });
        expect(intent().capturedResult).toBeUndefined();
        expect(intent().auditReceipt).toBeUndefined();

        const publishedCount = published.length;
        vi.setSystemTime(new Date(Date.parse(intent().nextAttemptAt!) + 1));
        await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
        expect(published).toHaveLength(publishedCount + 1);
      }

      const terminal = await call("dispatch", published.at(-1));
      expect(terminal).toMatchObject({ success: false, terminal: true,
        failureCode: "source_retrieval_resource_limit", reason: "source_retrieval_resource_limit" });
      expect(intent()).toMatchObject({ status: "failed", attempts: 6,
        terminalReason: "source_retrieval_resource_limit",
        lastFailure: { code: "source_retrieval_resource_limit" } });
      expect(provider.summarize).not.toHaveBeenCalled();
      expect([...rows.keys()].some(id => id.startsWith(`${KV.procedural}\0`))).toBe(false);
      expect(recordAudit).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("preserves a persisted cancellation when source retrieval fails", async () => {
    vi.mocked(recordAudit).mockClear();
    await ready();
    resourceLimitDeliveries = 1;
    const originalExtract = handlers["mem::skill-extract"];
    handlers["mem::skill-extract"] = async data => {
      const result = await originalExtract(data);
      const running = intent();
      put(KV.skillLifecycleIntents, running.id, { ...running, status: "skipped", terminalReason: "skip_consolidation",
        deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined, nextAttemptAt: undefined,
        auditReceipt: { id: `${running.id}:skip`, outcome: "skipped", recordedAt: new Date().toISOString() } });
      return result;
    };

    expect(await call("dispatch", published[0])).toMatchObject({ success: true, status: "skipped", terminal: true });
    expect(intent()).toMatchObject({ status: "skipped", terminalReason: "skip_consolidation", attempts: 1,
      auditReceipt: { outcome: "skipped" } });
    expect(intent().lastFailure).toBeUndefined();
    expect(intent().nextAttemptAt).toBeUndefined();
    expect(provider.summarize).not.toHaveBeenCalled();
    expect(recordAudit).not.toHaveBeenCalled();
  });

  it("reports exhausted provider attempts as failed with sanitized context", async () => {
    await ready();
    put(KV.skillLifecycleIntents, intent().id, { ...intent(), attempts: 5 });
    provider.summarize.mockRejectedValue(new Error("secret provider payload"));
    expect(await call("dispatch", published[0])).toMatchObject({ success: false, terminal: true });
    expect(intent()).toMatchObject({ status: "failed", attempts: 6, terminalReason: "skill_extraction_failed" });
    expect(JSON.stringify(intent())).not.toContain("secret provider payload");
  });

  it("preserves captured no-skill evidence when terminalizing", async () => {
    await ready(); provider.summarize.mockResolvedValue("<no-skill/>");
    expect(await call("dispatch", published[0])).toMatchObject({ skipped: true, reason: "no_skill" });
    expect(intent()).toMatchObject({ status: "skipped", capturedResult: { kind: "skip", reason: "no_skill" } });
    await call("dispatch", published[0]);
    expect(provider.summarize).toHaveBeenCalledTimes(1);
  });

  it("keeps more than the failure budget of successful cuts recoverable with stable identity", async () => {
    await ready();
    const originalExtract = handlers["mem::skill-extract"];
    let cuts = 0;
    const identities: unknown[] = [];
    handlers["mem::skill-extract"] = async (data) => {
      identities.push(data);
      if (++cuts > 8) return originalExtract(data);
      const current = intent();
      put(KV.skillLifecycleIntents, current.id, { ...current,
        effectPlan: { skillId: "captured-plan", created: true, reinforced: false } });
      return { success: true, queued: true, completed: false, reason: "skill_source_fragmentation",
        jobId: "skillparts_stable", continuation: data };
    };
    for (let i = 0; i < 8; i++) {
      const delivery = published.at(-1)!;
      expect(await call("dispatch", delivery)).toMatchObject({ queued: true, completed: false });
      expect(intent()).toMatchObject({ status: "queued", attempts: 0,
        effectPlan: { skillId: "captured-plan" } });
      expect(intent().deliveryId).toBeUndefined();
      expect(intent().leaseExpiresAt).toBeUndefined();
      expect(intent().auditReceipt).toBeUndefined();
      expect(await call("dispatch", delivery)).toMatchObject({ stale: true });
      await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
    }
    // The extract fixture creates its own effect plan; remove the simulated progress field.
    put(KV.skillLifecycleIntents, intent().id, { ...intent(), effectPlan: undefined });
    expect(await call("dispatch", published.at(-1))).toMatchObject({ completed: true });
    expect(provider.summarize).toHaveBeenCalledTimes(2);
    expect(provider.summarize.mock.calls.filter(([system]) => String(system).startsWith("You evaluate publication evidence."))).toHaveLength(1);
    expect(identities).toHaveLength(9);
    expect(identities.every((identity) => JSON.stringify(identity) === JSON.stringify(identities[0]))).toBe(true);
    expect([...rows.keys()].filter((id) => id.startsWith(`${KV.procedural}\0`))).toHaveLength(1);
  });

  it("counts real failures across successful cuts and rejects foreign continuation identities", async () => {
    await ready();
    put(KV.skillLifecycleIntents, intent().id, { ...intent(), attempts: 4 });
    handlers["mem::skill-extract"] = async (data) => ({ success: true, queued: true, completed: false,
      reason: "skill_source_fragmentation", jobId: "skillparts_stable", continuation: data });
    await call("dispatch", published.at(-1));
    expect(intent().attempts).toBe(4);
    await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
    handlers["mem::skill-extract"] = async () => ({ success: true, queued: true, completed: false,
      reason: "skill_source_fragmentation", jobId: "skillparts_stable", continuation: { sessionId: "foreign" } });
    expect(await call("dispatch", published.at(-1))).toMatchObject({ queued: true, retryAt: expect.any(String) });
    expect(intent()).toMatchObject({ attempts: 5, status: "queued", lastFailure: { code: "skill_extraction_failed" } });
    put(KV.skillLifecycleIntents, intent().id, { ...intent(), nextAttemptAt: undefined });
    await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
    expect(await call("dispatch", published.at(-1))).toMatchObject({ success: false, terminal: true });
    expect(intent()).toMatchObject({ attempts: 6, status: "failed" });
  });

  it("backs off through eight incomplete index deliveries then extracts once when coverage is ready", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-09T00:00:00.000Z"));
    try {
      await ready();
      preparationDeliveries = 8;
      const originalExtract = handlers["mem::skill-extract"];
      const identities: unknown[] = [];
      handlers["mem::skill-extract"] = async (data) => { identities.push(data); return originalExtract(data); };

      for (let deliveryIndex = 1; deliveryIndex <= 8; deliveryIndex++) {
        const delivery = published.at(-1)!;
        const result = await call("dispatch", delivery);
        expect(result).toMatchObject({ success: true, queued: true, completed: false, reason: "skill_index_preparing",
          continuation: { sessionId: "s1", intentId: intent().id, snapshotFingerprint: "fp" } });
        expect(result.jobId).toBeUndefined();
        expect(intent()).toMatchObject({ status: "queued", attempts: 0, indexPreparationBackoffLevel: Math.min(deliveryIndex, 8) });
        expect(intent().lastFailure).toBeUndefined();
        expect(intent().capturedResult).toBeUndefined();
        expect(intent().nextAttemptAt).toBeDefined();
        expect(provider.summarize).not.toHaveBeenCalled();

        const publishedCount = published.length;
        await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
        expect(published).toHaveLength(publishedCount);
        vi.setSystemTime(new Date(Date.parse(intent().nextAttemptAt!) + 1));
        await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
        expect(published).toHaveLength(publishedCount + 1);
      }

      expect(identities).toHaveLength(8);
      expect(identities.every((identity) => JSON.stringify(identity) === JSON.stringify(identities[0]))).toBe(true);
      vi.setSystemTime(new Date(Date.parse(intent().nextAttemptAt!) + 1));
      await call("summary-ready", { sessionId: "s1", snapshotFingerprint: "fp" });
      expect(await call("dispatch", published.at(-1))).toMatchObject({ success: true, completed: true });
      expect(provider.summarize).toHaveBeenCalledTimes(2);
      expect(provider.summarize.mock.calls.filter(([system]) => String(system).startsWith("You evaluate publication evidence."))).toHaveLength(1);
      expect(intent()).toMatchObject({ status: "completed", attempts: 1 });
      expect([...rows.keys()].filter((id) => id.startsWith(`${KV.procedural}\0`))).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it.each([
    { continuation: { sessionId: "s1", intentId: "wrong", snapshotFingerprint: "fp" } },
    { continuation: { sessionId: "s1", intentId: "placeholder" } },
    { jobId: "fake-fragment", continuation: { sessionId: "s1", intentId: "placeholder", snapshotFingerprint: "fp" } },
  ])("counts malformed or foreign index continuations as actual failures: %j", async (partial) => {
    await ready();
    const selected = intent();
    const continuation = { ...partial.continuation, intentId: partial.continuation.intentId === "placeholder" ? selected.id : partial.continuation.intentId };
    handlers["mem::skill-extract"] = async () => ({ success: true, queued: true, completed: false,
      reason: "skill_index_preparing", ...partial, continuation });

    expect(await call("dispatch", published[0])).toMatchObject({ success: true, queued: true, retryAt: expect.any(String) });
    expect(intent()).toMatchObject({ status: "queued", attempts: 1,
      lastFailure: { code: "skill_extraction_failed" } });
    expect(intent().indexPreparationBackoffLevel).toBeUndefined();
    expect(intent().auditReceipt).toBeUndefined();
  });

  it("admits one exact foreground cut while paused without publishing or recovering other intents", async () => {
    config.paused = true;
    await ready();
    const selectedId = intent().id;
    expect(published).toHaveLength(0);
    const extract = vi.fn(async () => ({ success: true, queued: true, completed: false,
      reason: "skill_source_fragmentation", jobId: "skillparts_selected",
      continuation: { sessionId: "s1", intentId: selectedId, snapshotFingerprint: "fp" } }));
    handlers["mem::skill-extract"] = extract;
    expect(await call("dispatch", { intentId: selectedId })).toMatchObject({ success: false });
    expect(await call("dispatch", { intentId: selectedId, foreground: true })).toMatchObject({ queued: true, completed: false });
    expect(extract).toHaveBeenCalledTimes(1);
    expect(intent()).toMatchObject({ status: "queued", attempts: 0 });
    expect(intent().deliveryId).toBeUndefined();
    expect(published).toHaveLength(0);
    expect(kv.pages).not.toHaveBeenCalled();
    expect(await call("replay")).toMatchObject({ paused: true });
    put(KV.skillLifecycleIntents, selectedId, { ...intent(), nextAttemptAt: new Date(Date.now() + 60_000).toISOString() });
    expect(await call("dispatch", { intentId: selectedId, foreground: true })).toMatchObject({ deferred: true });
    expect(extract).toHaveBeenCalledTimes(1);
  });

  it("serializes duplicate selected foreground delivery and rejects stale delivery replay", async () => {
    await ready(); config.paused = true;
    const delivery = { ...published[0], foreground: true };
    handlers["mem::skill-extract"] = vi.fn(async (data) => ({ success: true, queued: true, completed: false,
      reason: "skill_source_fragmentation", jobId: "skillparts_selected", continuation: data }));
    const results = await Promise.all([call("dispatch", delivery), call("dispatch", delivery)]);
    expect(results).toEqual(expect.arrayContaining([
      expect.objectContaining({ queued: true, completed: false }), expect.objectContaining({ stale: true }),
    ]));
    expect(handlers["mem::skill-extract"]).toHaveBeenCalledTimes(1);
    expect(await call("dispatch", delivery)).toMatchObject({ stale: true });
    expect(published).toHaveLength(1);
  });

  it("does not resurrect a continuation cancelled while its cut was running", async () => {
    await ready();
    handlers["mem::skill-extract"] = async (data) => {
      await call("session-stopped", { sessionId: "s1", skipConsolidation: true });
      return { success: true, queued: true, completed: false, reason: "skill_source_fragmentation",
        jobId: "skillparts_cancelled", continuation: data };
    };
    expect(await call("dispatch", published[0])).toMatchObject({ terminal: true, status: "skipped" });
    expect(intent()).toMatchObject({ status: "skipped", terminalReason: "skip_consolidation" });
    expect(intent().deliveryId).toBeUndefined();
  });

  it("preserves cancellation and captured evidence when final extraction returns", async () => {
    await ready();
    handlers["mem::skill-extract"] = async () => {
      const current = intent();
      put(KV.skillLifecycleIntents, current.id, { ...current,
        capturedResult: { kind: "skip", reason: "no_skill" } });
      await call("session-stopped", { sessionId: "s1", skipConsolidation: true });
      return { success: true, extracted: true, created: true, skillId: "late-skill" };
    };
    expect(await call("dispatch", published[0])).toMatchObject({ terminal: true, status: "skipped" });
    expect(intent()).toMatchObject({ status: "skipped", terminalReason: "skip_consolidation",
      capturedResult: { kind: "skip", reason: "no_skill" }, auditReceipt: { outcome: "skipped" } });
    expect(intent().skillId).toBeUndefined();
    expect(intent().deliveryId).toBeUndefined();
    expect(provider.summarize).not.toHaveBeenCalled();
  });

  it("does not retry or replace cancellation with failure when extraction throws afterwards", async () => {
    await ready();
    put(KV.skillLifecycleIntents, intent().id, { ...intent(), attempts: 5 });
    handlers["mem::skill-extract"] = async () => {
      await call("session-stopped", { sessionId: "s1", skipConsolidation: true });
      throw new Error("provider response failed after cancellation");
    };
    expect(await call("dispatch", published[0])).toMatchObject({ success: true, terminal: true, status: "skipped" });
    expect(intent()).toMatchObject({ status: "skipped", terminalReason: "skip_consolidation",
      auditReceipt: { outcome: "skipped" } });
    expect(intent().lastFailure).toBeUndefined();
    expect(intent().nextAttemptAt).toBeUndefined();
    expect(intent().deliveryId).toBeUndefined();
    expect(published).toHaveLength(1);
  });

  it.each([undefined, "", "   ", 123])("retries extracted response with invalid skillId %s", async (skillId) => {
    await ready();
    handlers["mem::skill-extract"] = async () => ({ success: true, extracted: true, skillId });
    expect(await call("dispatch", published[0])).toMatchObject({ queued: true, retryAt: expect.any(String) });
    expect(intent()).toMatchObject({ status: "queued", attempts: 1,
      lastFailure: { code: "skill_extraction_failed" } });
    expect(intent().auditReceipt).toBeUndefined();
    expect(intent().skillId).toBeUndefined();
  });

  it.each([{ success: true }, { success: true, extracted: false },
    { success: true, reason: "unclear" }])("retries ambiguous extraction result %j", async (result) => {
    await ready();
    handlers["mem::skill-extract"] = async () => result;
    expect(await call("dispatch", published[0])).toMatchObject({ queued: true, retryAt: expect.any(String) });
    expect(intent()).toMatchObject({ status: "queued", attempts: 1 });
    expect(intent().terminalReason).toBeUndefined();
    expect(intent().auditReceipt).toBeUndefined();
  });

  it.each(["typesafe_gate", "no_skill", "too_few_observations", "protected_session"])(
    "preserves explicit source skip reason %s", async (reason) => {
      await ready();
      handlers["mem::skill-extract"] = async () => ({ success: true, extracted: false, skipped: true, reason });
      expect(await call("dispatch", published[0])).toMatchObject({ skipped: true, reason });
      expect(intent()).toMatchObject({ status: "skipped", terminalReason: reason });
    },
  );

  it("persists one marker page and resumes its cursor", async () => {
    vi.mocked(kv.pages).mockImplementation(async function* () { yield { items: [], next_cursor: "next", has_more: true } as any; });
    await call("replay", { limit: 2 });
    await call("replay", { limit: 2 });
    expect(kv.pages).toHaveBeenLastCalledWith(KV.summaryQueueCompleted, { cursor: "next", limit: 2, maxBytes: 262144 });
    expect(await call("replay", { limit: 33 })).toMatchObject({ success: false });
  });
});
