import { afterEach, describe, expect, it, vi } from "vitest";
import { registerSummaryQueueFunctions, summarySourceFingerprint } from "../src/functions/summary-queue.js";
import { registerSummarizeFunction } from "../src/functions/summarize.js";
import { registerApiTriggers } from "../src/triggers/api.js";
import { captureSummaryEvidence } from "../src/functions/summary-source-grounding.js";
import { SUMMARY_RECOVERY_FIDELITY_VERSION } from "../src/functions/summary-queue-prompts.js";
import { summaryConstructionKey, zeroSpendSummaryIdentity } from "../src/functions/summary-zero-spend-recovery.js";
import { KV, fingerprintId } from "../src/state/schema.js";
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
vi.mock("../src/functions/dashboard-activity.js", async original => ({
  ...await original<typeof import("../src/functions/dashboard-activity.js")>(), recordOperation: vi.fn() }));
vi.mock("../src/config.js", async original => ({ ...await original<typeof import("../src/config.js")>(),
  isBackgroundRecoveryPaused: () => true,
  getSummaryBudgetConfig: () => ({ contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
    maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 }) }));
const raw = "<summary><title>Inspection</title><narrative>Inspected existing behavior.</narrative><decisions></decisions><files></files><concepts></concepts></summary>";
afterEach(() => { vi.useRealTimers(); });

async function harness(seedChild = true) {
  const rows = new Map<string, Map<string, any>>();
  const revisions = new Map<string, number>();
  const kv = {
    get: async (scope: string, key: string) => structuredClone(rows.get(scope)?.get(key) ?? null),
    set: vi.fn(async (scope: string, key: string, value: unknown) => {
      if (!rows.has(scope)) rows.set(scope, new Map()); rows.get(scope)!.set(key, structuredClone(value));
      revisions.set(scope, (revisions.get(scope) ?? 0) + 1); return value;
    }),
    delete: async (scope: string, key: string) => { rows.get(scope)?.delete(key); },
    list: async (scope: string) => structuredClone([...(rows.get(scope)?.values() ?? [])]),
    scopeRevision: async (scope: string) => ({ generation: "test", revision: String(revisions.get(scope) ?? 0) }),
    getVersioned: async (scope: string, key: string) => ({ exists: rows.get(scope)?.has(key) ?? false,
      value: structuredClone(rows.get(scope)?.get(key) ?? null), version: JSON.stringify(rows.get(scope)?.get(key)) }),
    pages: async function* (scope: string) { yield { items: structuredClone([...(rows.get(scope)?.values() ?? [])]), next_cursor: null }; },
  };
  await kv.set(KV.sessions, "s", { id: "s", project: "p", status: "active", startedAt: "2026-10-01" });
  const observation = { id: "o", sessionId: "s", timestamp: "2026-10-01T00:00:00Z", type: "conversation",
    title: "Inspection", narrative: "Inspected existing behavior.", facts: [], files: [], concepts: [] };
  await kv.set(KV.observations("s"), "o", observation);
  const fingerprint = await summarySourceFingerprint(kv as never, "s", "p");
  const root: any = { id: "root-spent", sessionId: "s", project: "p", snapshotFingerprint: fingerprint,
    observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
      maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 }, createdAt: "2026-10-01", updatedAt: "2026-10-01",
    status: "failed", stage: "map", round: 0, unitIds: ["spent-root-unit"], sourceProgressSize: 200,
    failure: "summary_publication_invalid_response", executionOrigin: "manual" };
  await kv.set(KV.summaryQueueJobs, root.id, root);
  await kv.set(KV.state, `${root.id}:raw:spent-root-unit`, { state: "captured", raw: "old spent candidate" });
  await captureSummaryEvidence(kv as never, root, [observation] as never);
  const requestId = fingerprintId("summary-recovery", JSON.stringify([SUMMARY_RECOVERY_FIDELITY_VERSION,
    "full-originals-v2", root.id, root.project, root.sessionId, fingerprint]));
  const childId = fingerprintId("sqj", requestId);
  const child = { ...root, id: childId, failure: "manual_summary_limit_exceeded", recoveryMode: "full-originals-v1",
    recoveryGeneration: "full-originals-v2", recoveryOfJobId: root.id, unitIds: ["zero-unit"] };
  const unit = { id: "zero-unit", jobId: child.id, stage: "map", round: 0, attempts: 0,
    items: [{ text: "", obsRangeStart: 1, obsRangeEnd: 1, sourcePaths: [], modifiedPaths: [] }],
    deliveryId: "zero-delivery", dispatchedAt: "2026-10-01T00:00:00Z" };
  const continuation = { operationId: `${child.id}:manual`, jobId: child.id, sessionId: "s", project: "p",
    sourceFingerprint: fingerprint, step: 0, round: 0, unitId: unit.id, deliveryId: unit.deliveryId };
  const operation = { id: continuation.operationId, jobId: child.id, sessionId: "s", project: "p",
    sourceFingerprint: fingerprint, step: 0, reservedCalls: 0, maxCalls: 2, maxRequests: 2,
    deadline: 0, continuation };
  const markerKey = `s:failed-summary:${fingerprint}`;
  const oldAckKey = `manual-summary-recovery:${requestId}`;
  if (seedChild) {
    await kv.set(KV.summaryQueueJobs, child.id, child);
    await kv.set(KV.summaryQueueUnits(child.id), unit.id, unit);
    await kv.set(KV.state, operation.id, operation);
    await kv.set(KV.state, oldAckKey, { identity: "old immutable ACK", response: { continuation } });
  }
  await kv.set(KV.state, markerKey, seedChild ? { jobId: child.id, reason: child.failure } : { jobId: root.id, reason: root.failure });
  const handlers = new Map<string, (data: any) => Promise<any>>();
  const sdk = { registerFunction: (id: string, fn: any) => handlers.set(id, fn), registerTrigger: vi.fn(),
    trigger: vi.fn(async ({ function_id, payload }: any) => {
      if (!handlers.has(function_id)) throw new Error(`Unexpected effect: ${function_id}`);
      return handlers.get(function_id)!(payload);
    }) };
  const provider = { name: "test", compress: vi.fn(),
    prepareRequest: vi.fn((_operation: unknown, system: string, prompt: string) => ({ endpoint: "POST https://offline.invalid/summary", body: JSON.stringify([system, prompt]) })),
    summarize: vi.fn(async (system: string) => system.startsWith("You evaluate publication evidence")
      ? JSON.stringify({ verdict: "supported", reason: "Original source supports inspection" }) : raw) };
  const restart = () => { registerSummaryQueueFunctions(sdk as never, kv as never, provider as never);
    registerSummarizeFunction(sdk as never, kv as never, provider as never); };
  restart();
  const invoke = (data: any) => handlers.get("mem::summarize")!({ sessionId: "s", manual: true, ...data });
  return { kv, rows, sdk, handlers, provider, restart, invoke, root, child, unit, continuation, operation,
    markerKey, oldAckKey, recover: () => invoke({ zeroSpendRecoveryOfJobId: child.id }) };
}

describe("persistent summary zero-spend successor", () => {
  it("creates one deterministic child concurrently, preserves spent root and predecessor, and replays immutable creation ACK", async () => {
    const h = await harness(); const before = structuredClone(h.rows);
    const [first, second] = await Promise.all([h.recover(), h.recover()]);
    expect(first).toEqual(second); expect(first).toMatchObject({ success: true, completed: false });
    expect(first.deadline).toBeUndefined(); expect(first.continuation.recoveryGeneration).toBeUndefined();
    const id = fingerprintId("sqj", zeroSpendSummaryIdentity(h.child, h.root));
    expect(first.jobId).toBe(id);
    expect(await h.kv.get(KV.summaryQueueJobs, id)).toMatchObject({ recoveryOfJobId: h.root.id,
      zeroSpendRecoveryOfJobId: h.child.id, recoveryMode: "full-originals-v1", recoveryGeneration: "full-originals-v2" });
    for (const [scope, values] of before) for (const [key, value] of values)
      expect(await h.kv.get(scope, key)).toEqual(value);
    h.restart(); expect(await h.recover()).toEqual(first);
    const operation = await h.kv.get(KV.state, first.continuation.operationId);
    await h.kv.set(KV.state, operation.id, { ...operation, step: 1, reservedCalls: 1 });
    expect(await h.recover()).toEqual(first);
    expect(await h.kv.get(KV.state, operation.id)).toMatchObject({ step: 1, reservedCalls: 1 });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });

  it.each(["step", "reservedCalls", "maxCalls", "maxRequests"])("fails closed on absent, malformed or spent %s", async field => {
    for (const value of [undefined, "0", -1, 1]) {
      const h = await harness(); const operation: any = { ...h.operation, [field]: value };
      if (value === undefined) delete operation[field];
      await h.kv.set(KV.state, h.operation.id, operation);
      expect(await h.recover()).toMatchObject({ success: false, error: "summary_zero_spend_recovery_not_available" });
      expect((await h.kv.list(KV.summaryQueueJobs)).length).toBe(2); expect(h.provider.summarize).not.toHaveBeenCalled();
    }
  });

  it.each(["raw-intended", "raw-captured", "raw-malformed", "raw-unknown", "validated", "effect", "completion", "request", "attempt", "output", "frozen", "source", "active"])("rejects %s evidence before successor construction", async kind => {
    const h = await harness();
    if (kind.startsWith("raw-")) await h.kv.set(KV.state, `${h.child.id}:raw:${h.unit.id}`, kind === "raw-malformed" ? {} : { state: kind.slice(4), raw: "saved" });
    if (kind === "validated") await h.kv.set(KV.state, `${h.child.id}:validated:${h.unit.id}`, {});
    if (kind === "effect") await h.kv.set(KV.state, `${h.child.id}:publication-effect`, {});
    if (kind === "completion") await h.kv.set(KV.summaryQueueCompleted, "s", { jobId: h.child.id });
    if (kind === "request") await h.kv.set(KV.state, `${h.operation.id}:request:0`, { request: h.continuation });
    if (kind === "attempt" || kind === "output") await h.kv.set(KV.summaryQueueUnits(h.child.id), h.unit.id,
      { ...h.unit, ...(kind === "attempt" ? { attempts: 1 } : { output: {} }) });
    if (kind === "frozen") await h.kv.set(KV.state, h.operation.id, { ...h.operation, continuation: { ...h.continuation, deliveryId: "changed" } });
    if (kind === "source") { const observation = await h.kv.get(KV.observations("s"), "o"); await h.kv.set(KV.observations("s"), "o", { ...observation, narrative: "changed" }); }
    if (kind === "active") { await h.kv.set(KV.summaryQueueJobs, "competing", { ...h.root, id: "competing", status: "pending" }); await h.kv.set(KV.summaryQueueActive, "s", { jobId: "competing" }); }
    expect(await h.recover()).toMatchObject({ success: false });
    expect(await h.kv.get(KV.state, summaryConstructionKey(fingerprintId("sqj", zeroSpendSummaryIdentity(h.child, h.root))))).toBeNull();
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });

  it.each([{ records: 8193, bytesPerRecord: 0 }, { records: 9, bytesPerRecord: 1_048_576 }])(
    "creates an eligible successor despite $records unrelated state rows of $bytesPerRecord bytes", async ({ records, bytesPerRecord }) => {
      const h = await harness(); const value = { unrelated: "x".repeat(bytesPerRecord) };
      for (let index = 0; index < records; index++) await h.kv.set(KV.state, `unrelated:${index}`, value);
      const created = await h.recover();
      expect(created).toMatchObject({ success: true, completed: false,
        jobId: fingerprintId("sqj", zeroSpendSummaryIdentity(h.child, h.root)) });
      expect(await h.recover()).toEqual(created);
      expect(await h.kv.get(KV.state, "unrelated:0")).toEqual(value);
      expect(await h.kv.get(KV.state, `unrelated:${records - 1}`)).toEqual(value);
      expect(h.provider.summarize).not.toHaveBeenCalled();
    });

  it.each(["zero-spend", "original-root"].flatMap(mode => [1, 2, 3, 4, 5, 6].map(crashAt => ({ mode, crashAt }))))(
    "repairs $mode crash after construction write $crashAt with no early dispatch", async ({ mode, crashAt }) => {
    const h = await harness(mode === "zero-spend");
    const discovered = mode === "original-root" ? await h.invoke({}) : undefined;
    const recover = mode === "zero-spend" ? h.recover : () => h.invoke({ continuation: discovered.continuation });
    const set = h.kv.set.getMockImplementation()!; let writes = 0; let crashed = false;
    h.kv.set.mockImplementation(async (...args) => {
      const result = await set(...args);
      if (!crashed && ++writes === crashAt) { crashed = true; throw new Error("injected crash"); }
      return result;
    });
    await recover().catch(() => undefined); expect(crashed).toBe(true); expect(h.provider.summarize).not.toHaveBeenCalled();
    const id = mode === "zero-spend" ? fingerprintId("sqj", zeroSpendSummaryIdentity(h.child, h.root)) : h.child.id;
    const journal = await h.kv.get(KV.state, summaryConstructionKey(id));
    if (journal.state === "creating" && await h.kv.get(KV.summaryQueueJobs, id)) {
      expect(await h.handlers.get("mem::summary-unit")!({ jobId: id, unitId: journal.unit.id,
        manual: { jobId: id, unitId: journal.unit.id, project: "p", sessionId: "s", sourceFingerprint: h.root.snapshotFingerprint, round: 0 } }))
        .toMatchObject({ success: false, error: "summary_recovery_construction_pending" });
    }
    h.kv.set.mockImplementation(set); h.restart(); const repaired = await recover();
    expect(repaired).toMatchObject({ success: true, completed: false }); expect(repaired.jobId).toBe(id);
    expect((await h.kv.list(KV.summaryQueueUnits(id))).length).toBe(1);
    expect(await h.kv.get(KV.state, summaryConstructionKey(id))).toMatchObject({ state: "ready" });
    expect(await recover()).toEqual(repaired); expect(h.provider.summarize).not.toHaveBeenCalled();
  });

  it("retains conflicting partial rows and refuses repair", async () => {
    const h = await harness(); const set = h.kv.set.getMockImplementation()!;
    h.kv.set.mockImplementation(async (...args) => { await set(...args); throw new Error("crash"); });
    await expect(h.recover()).rejects.toThrow("crash"); h.kv.set.mockImplementation(set);
    const id = fingerprintId("sqj", zeroSpendSummaryIdentity(h.child, h.root)); const journal = await h.kv.get(KV.state, summaryConstructionKey(id));
    const conflict = { ...journal.unit, attempts: 1 }; await h.kv.set(KV.summaryQueueUnits(id), journal.unit.id, conflict);
    expect(await h.recover()).toMatchObject({ success: false, error: "summary_recovery_identity_mismatch" });
    expect(await h.kv.get(KV.summaryQueueUnits(id), journal.unit.id)).toEqual(conflict);
    expect(await h.kv.get(KV.summaryQueueJobs, id)).toBeNull(); expect(h.provider.summarize).not.toHaveBeenCalled();
  });

  it("whitelists the additive REST field and rejects mixed controls at every public entrypoint", async () => {
    const h = await harness(); registerApiTriggers(h.sdk as never, {} as never, undefined as never, undefined as never);
    const api = h.handlers.get("api::summarize")!;
    for (const fields of [{ continuation: {} }, { providerCalls: 0 }, { prepare: false }, { selected: {} }, { manual: false }])
      expect(await api({ body: { sessionId: "s", zeroSpendRecoveryOfJobId: h.child.id, ...fields } })).toMatchObject({ status_code: 400 });
    for (const id of ["", " padded ", "x".repeat(129), 3])
      expect(await h.invoke({ zeroSpendRecoveryOfJobId: id })).toMatchObject({ success: false, error: "invalid_manual_summary_request" });
    expect(await h.handlers.get("mem::summarize")!({ sessionId: "s", zeroSpendRecoveryOfJobId: h.child.id })).toMatchObject({ success: false });
    expect(await api({ body: { sessionId: "s", zeroSpendRecoveryOfJobId: h.child.id, ignored: true } })).toMatchObject({ status_code: 200, body: { success: true } });
    expect(h.sdk.trigger).toHaveBeenCalledWith(expect.objectContaining({ function_id: "mem::summarize", payload: { sessionId: "s", manual: true, zeroSpendRecoveryOfJobId: h.child.id } }));
  });
});
