import { afterEach, describe, expect, it, vi } from "vitest";
import type { CompressedObservation, MemoryProvider, Session, SessionSummary, SummaryQueueJob } from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { registerSummaryQueueFunctions, summarySourceFingerprint } from "../src/functions/summary-queue.js";
import { formatSummaryPartial } from "../src/prompts/summary.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
vi.mock("../src/functions/dashboard-activity.js", () => ({ recordOperation: vi.fn() }));
vi.mock("../src/config.js", () => ({
  isBackgroundRecoveryPaused: () => false,
  getSummaryBudgetConfig: () => ({ contextTokens: 4096, outputTokens: 512,
    safetyMarginTokens: 256, maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 }),
}));

type Row = Record<string, unknown>;
type Handler = (data: unknown) => Promise<Row>;

function harness() {
  const rows = new Map<string, Map<string, unknown>>();
  const versions = new Map<string, number>();
  const get = vi.fn(async <T>(scope: string, key: string) => rows.get(scope)?.get(key) as T | undefined ?? null);
  const set = vi.fn(async (scope: string, key: string, value: unknown) => {
    if (!rows.has(scope)) rows.set(scope, new Map());
    rows.get(scope)!.set(key, structuredClone(value));
    versions.set(`${scope}\0${key}`, (versions.get(`${scope}\0${key}`) ?? 0) + 1);
    return value;
  });
  const remove = vi.fn(async (scope: string, key: string) => { rows.get(scope)?.delete(key); });
  const list = vi.fn(async <T>(scope: string) => [...(rows.get(scope)?.values() ?? [])] as T[]);
  const pages = async function* <T>(scope: string): AsyncGenerator<{ items: T[]; next_cursor: string | null }> {
    yield { items: [...(rows.get(scope)?.values() ?? [])] as T[], next_cursor: null };
  };
  const getVersioned = async <T>(scope: string, key: string) => ({
    exists: rows.get(scope)?.has(key) ?? false, value: structuredClone(await get<T>(scope, key)),
    version: String(versions.get(`${scope}\0${key}`) ?? 0),
  });
  const kv = { get, getVersioned, set, delete: remove, list, pages };
  const handlers = new Map<string, Handler>();
  const sdk = {
    registerFunction: (id: string, handler: Handler) => handlers.set(id, handler),
    registerTrigger: vi.fn(),
    trigger: vi.fn(async () => ({ accepted: true })),
  };
  const provider: MemoryProvider = {
    name: "offline-test",
    compress: vi.fn(async () => ""),
    summarize: vi.fn(async (system: string) => {
      if (!system.startsWith("You evaluate publication evidence.")) throw new Error("unexpected generation");
      return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "The captured fixture matches its original source." });
    }),
    prepareRequest: (_operation, system, prompt, options) => ({ endpoint: "POST https://provider.example/v1/chat/completions",
      body: JSON.stringify({ system, prompt, options }) }),
  };
  registerSummaryQueueFunctions(sdk as never, kv as never, provider);
  const invoke = (data: unknown) => handlers.get("mem::summary-enqueue")!(data);
  return { kv, provider, invoke, handlers };
}

const completedAt = "2026-10-11T04:00:00.000Z";

async function seed(h: ReturnType<typeof harness>, options: {
  selectedOrigin?: boolean;
  markerJobId?: string;
  intentOrigin?: "selected" | "ordinary";
} = {}) {
  const session: Session = {
    id: "session", project: "project", cwd: ".", startedAt: "2026-10-11T03:00:00.000Z",
    status: "completed", observationCount: 1,
  };
  const observation: CompressedObservation = {
    id: "observation", sessionId: "session", timestamp: "2026-10-11T03:30:00.000Z",
    type: "conversation", title: "Retained source", narrative: "Persisted work for a terminal summary replay.",
    facts: [], files: [], concepts: [], importance: 5,
  };
  await h.kv.set(KV.sessions, session.id, session);
  await h.kv.set(KV.observations(session.id), observation.id, observation);
  const sourceFingerprint = await summarySourceFingerprint(h.kv as never, session.id, session.project);
  const job: SummaryQueueJob & { executionOrigin?: "selected" } = {
    id: "sqj_retained_1", sessionId: session.id, project: session.project,
    snapshotFingerprint: sourceFingerprint, observationCount: 1,
    config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
      maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
    createdAt: session.startedAt, updatedAt: completedAt, completedAt,
    status: "completed", stage: "map", round: 0, unitIds: ["squ_retained_1"],
    sourceProgressSize: 1,
    ...(options.selectedOrigin !== false && { executionOrigin: "selected" as const }),
  };
  const summary: SessionSummary = {
    sessionId: session.id, project: session.project, createdAt: completedAt,
    title: "Retained summary", narrative: "The exact source was summarized and remains unchanged.",
    keyDecisions: [], filesModified: [], concepts: [], observationCount: 1,
  };
  await h.kv.set(KV.summaryQueueJobs, job.id, job);
  await h.kv.set(KV.summaries, session.id, summary);
  await h.kv.set(KV.summaryQueueCompleted, session.id, {
    sessionId: session.id, fingerprint: sourceFingerprint, completedAt,
    ...(options.markerJobId !== undefined && { jobId: options.markerJobId }),
  });
  if (options.intentOrigin) {
    await h.kv.set(KV.summaryQueueIntents, session.id, {
      sessionId: session.id, createdAt: session.startedAt,
      ...(options.intentOrigin === "selected" && { executionOrigin: "selected" }),
    });
  }
  return { session, observation, job, sourceFingerprint };
}

function selected(session: Session, sourceFingerprint: string, jobId?: string) {
  return { project: session.project, sessionId: session.id, sourceFingerprint, ...(jobId && { jobId }) };
}

afterEach(() => vi.clearAllMocks());

describe("selected summary terminal replay identity", () => {
  it("replays only the exact selected job linked by a new completion marker", async () => {
    const h = harness();
    const fixture = await seed(h, { markerJobId: "sqj_retained_1", intentOrigin: "ordinary" });
    const before = [
      await h.kv.get(KV.summaryQueueJobs, fixture.job.id),
      await h.kv.get(KV.summaries, fixture.session.id),
      await h.kv.get(KV.summaryQueueCompleted, fixture.session.id),
    ];

    expect(await h.invoke({ sessionId: fixture.session.id, selected: selected(fixture.session, fixture.sourceFingerprint) }))
      .toEqual({ success: true, queued: false, completed: true });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
    expect(await h.kv.get(KV.summaryQueueJobs, fixture.job.id)).toEqual(before[0]);
    expect(await h.kv.get(KV.summaries, fixture.session.id)).toEqual(before[1]);
    expect(await h.kv.get(KV.summaryQueueCompleted, fixture.session.id)).toEqual(before[2]);
    expect(await h.kv.get(KV.summaryQueueIntents, fixture.session.id)).toBeNull();
  });

  it("requires the caller's exact retained job ID when a legacy marker has no job ID", async () => {
    const h = harness();
    const fixture = await seed(h, { intentOrigin: "ordinary" });

    expect(await h.invoke({ sessionId: fixture.session.id,
      selected: selected(fixture.session, fixture.sourceFingerprint, fixture.job.id) }))
      .toEqual({ success: true, queued: false, completed: true });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("rejects legacy replay without either exact job ID and never scans the jobs scope", async () => {
    const h = harness();
    const fixture = await seed(h, { intentOrigin: "ordinary" });

    expect(await h.invoke({ sessionId: fixture.session.id,
      selected: selected(fixture.session, fixture.sourceFingerprint) }))
      .toMatchObject({ success: false, error: "selection_origin_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
    expect(await h.kv.get(KV.summaryQueueJobs, fixture.job.id)).toMatchObject({ status: "completed" });

    const withoutIntent = harness();
    const unindexed = await seed(withoutIntent);
    expect(await withoutIntent.invoke({ sessionId: unindexed.session.id,
      selected: selected(unindexed.session, unindexed.sourceFingerprint) }))
      .toMatchObject({ success: false, error: "selection_origin_mismatch" });
    expect(withoutIntent.provider.summarize).not.toHaveBeenCalled();
    expect(withoutIntent.kv.list).not.toHaveBeenCalled();
  });

  it("rejects a foreign job pin and stale source before any provider work", async () => {
    const h = harness();
    const fixture = await seed(h, { markerJobId: "sqj_retained_1", intentOrigin: "ordinary" });

    expect(await h.invoke({ sessionId: fixture.session.id,
      selected: { ...selected(fixture.session, fixture.sourceFingerprint), project: "foreign-project" } }))
      .toMatchObject({ success: false, error: "selection_identity_mismatch" });
    expect(await h.invoke({ sessionId: fixture.session.id,
      selected: selected(fixture.session, fixture.sourceFingerprint, "sqj_foreign") }))
      .toMatchObject({ success: false, error: "selection_identity_mismatch" });
    const changed = { ...fixture.observation, narrative: "The source changed after the retained summary." };
    await h.kv.set(KV.observations(fixture.session.id), changed.id, changed);
    expect(await h.invoke({ sessionId: fixture.session.id,
      selected: selected(fixture.session, fixture.sourceFingerprint, fixture.job.id) }))
      .toMatchObject({ success: false, error: "selection_source_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("does not treat a completed ordinary job as selected replay evidence", async () => {
    const h = harness();
    const fixture = await seed(h, { selectedOrigin: false, intentOrigin: "ordinary" });

    expect(await h.invoke({ sessionId: fixture.session.id,
      selected: selected(fixture.session, fixture.sourceFingerprint, fixture.job.id) }))
      .toMatchObject({ success: false, error: "selection_identity_mismatch" });
    expect(h.provider.summarize).not.toHaveBeenCalled();
    expect(h.kv.list).not.toHaveBeenCalled();
  });

  it("preserves ordinary completed-summary compatibility without selected provenance", async () => {
    const h = harness();
    const fixture = await seed(h, { selectedOrigin: false, markerJobId: "sqj_retained_1", intentOrigin: "ordinary" });

    expect(await h.invoke({ sessionId: fixture.session.id }))
      .toMatchObject({ success: true, queued: false, completed: true });
    expect(h.provider.summarize).not.toHaveBeenCalled();
  });

  it("writes the genuine completed job ID into new selected completion markers", async () => {
    const h = harness();
    const session: Session = {
      id: "session", project: "project", cwd: ".", startedAt: "2026-10-11T03:00:00.000Z",
      status: "completed", observationCount: 1,
    };
    const observation: CompressedObservation = {
      id: "observation", sessionId: session.id, timestamp: "2026-10-11T03:30:00.000Z",
      type: "conversation", title: "Retained source", narrative: "Persist selected work for a marker test.",
      facts: [], files: [], concepts: [], importance: 5,
    };
    await h.kv.set(KV.sessions, session.id, session);
    await h.kv.set(KV.observations(session.id), observation.id, observation);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, session.id, session.project);
    const baseSelection = selected(session, sourceFingerprint);
    const admission = await h.invoke({ sessionId: session.id, selected: baseSelection });
    const jobId = admission.jobId as string;
    const job = await h.kv.get<SummaryQueueJob & { executionOrigin?: "selected" }>(KV.summaryQueueJobs, jobId);
    const unitId = job!.unitIds[0];
    const unitScope = KV.summaryQueueUnits(jobId);
    const unit = await h.kv.get<Row>(unitScope, unitId);
    const summary: SessionSummary = {
      sessionId: session.id, project: session.project, createdAt: completedAt,
      title: "Captured summary", narrative: "The selected source summary is already captured.",
      keyDecisions: [], filesModified: [], concepts: [], observationCount: 1,
    };
    await h.kv.set(unitScope, unitId, { ...unit, summary,
      output: { text: formatSummaryPartial(summary), obsRangeStart: 1, obsRangeEnd: 1 } });

    const unitHandler = h.handlers.get("mem::summary-unit")!;
    const selection = { ...baseSelection, jobId, unitId, round: job!.round };
    expect(await unitHandler({ jobId, unitId, selected: selection }))
      .toMatchObject({ success: true, completed: true });
    expect(await h.kv.get(KV.summaryQueueCompleted, session.id)).toMatchObject({ jobId, fingerprint: sourceFingerprint });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect(vi.mocked(h.provider.summarize).mock.calls[0][0]).toMatch(/^You evaluate publication evidence\./);
  });
});
