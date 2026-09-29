import { describe, expect, it, vi } from "vitest";
import type {
  CompressedObservation, MemoryProvider, Session, SessionSummary, SummaryQueueJob,
} from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { registerSummaryQueueFunctions } from "../src/functions/summary-queue.js";
import { MAX_SUMMARY_DEPTH, summaryProgressSize } from "../src/functions/summary-budget.js";
import { formatSummaryPartial, type SummaryPromptItem } from "../src/prompts/summary.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/config.js", async importOriginal => ({
  ...await importOriginal<typeof import("../src/config.js")>(),
  getSummaryBudgetConfig: () => ({
    contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
    maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2,
  }),
}));

type Handler = (payload: never) => Promise<unknown>;
type Queued = { function_id: string; payload: never };

function store() {
  const rows = new Map<string, Map<string, unknown>>();
  return {
    rows,
    get: async <T>(scope: string, key: string): Promise<T | null> => (rows.get(scope)?.get(key) as T) ?? null,
    set: async <T>(scope: string, key: string, value: T): Promise<T> => {
      if (!rows.has(scope)) rows.set(scope, new Map());
      rows.get(scope)!.set(key, value);
      return value;
    },
    delete: async (scope: string, key: string) => { rows.get(scope)?.delete(key); },
    list: async <T>(scope: string): Promise<T[]> => [...(rows.get(scope)?.values() ?? [])] as T[],
  };
}

function observation(index: number, narrative = "source"): CompressedObservation {
  return {
    id: `obs-${index}`, sessionId: "session", timestamp: `2026-09-28T00:00:${String(index).padStart(2, "0")}Z`,
    type: "conversation", title: `Observation ${index}`, narrative, facts: ["fact"],
    files: ["src/file.ts"], concepts: ["queue"], importance: 5,
  };
}

function xmlWithLists(
  title: string, narrative: string, decisions: string[], files: string[], concepts: string[],
): string {
  return `<summary><title>${title}</title><narrative>${narrative}</narrative>` +
    `<decisions>${decisions.map(value => `<decision>${value}</decision>`).join("")}</decisions>` +
    `<files>${files.map(value => `<file>${value}</file>`).join("")}</files>` +
    `<concepts>${concepts.map(value => `<concept>${value}</concept>`).join("")}</concepts></summary>`;
}

const xml = (title = "Finished", narrative = "Completed the requested work and persisted the resulting summary.") =>
  xmlWithLists(title, narrative, ["persist"], ["src/file.ts"], ["queue"]);

function partialOutput(
  title: string, narrative: string, obsRangeStart: number,
  keyDecisions = ["persist"], filesModified = ["src/file.ts"], concepts = ["queue"],
): SummaryPromptItem {
  return {
    text: formatSummaryPartial({ title, narrative, keyDecisions, filesModified, concepts }),
    obsRangeStart, obsRangeEnd: obsRangeStart,
  };
}

function reduceXmlFromPrompt(prompt: string): string {
  const values = (pattern: RegExp) => [...new Set([...prompt.matchAll(pattern)].map(match => match[0]))];
  const decisions = values(/decision-\d+/g);
  const files = values(/src\/file-\d+\.ts/g);
  const concepts = values(/concept-\d+/g);
  return `<summary><title>Complete session summary</title><narrative>Preserved all decisions, files and concepts through every reduction round.</narrative>` +
    `<decisions>${decisions.map(value => `<decision>${value}</decision>`).join("")}</decisions>` +
    `<files>${files.map(value => `<file>${value}</file>`).join("")}</files>` +
    `<concepts>${concepts.map(value => `<concept>${value}</concept>`).join("")}</concepts></summary>`;
}

async function seedReduceJob(
  kv: ReturnType<typeof store>, sourceProgressSize: number, completedOutputs: SummaryPromptItem[],
  pendingInput?: SummaryPromptItem, round = 1,
): Promise<{ jobId: string; pendingUnitId: string }> {
  const jobId = "job-reduce-progress";
  const pendingUnitId = "unit-reduce-pending";
  const completedUnitIds = completedOutputs.map((_, index) => `unit-reduce-completed-${index + 1}`);
  const now = "2026-09-28T00:00:00.000Z";
  const config = {
    contextTokens: 8192, outputTokens: 1024, safetyMarginTokens: 512,
    maxCallInputBytes: 7500, chunkSize: 400, concurrency: 12,
  };
  const job: SummaryQueueJob = {
    id: jobId, sessionId: "session", project: "project", snapshotFingerprint: "snapshot",
    observationCount: completedOutputs.length + 1, config, createdAt: now, updatedAt: now, status: "pending", stage: "reduce",
    round, unitIds: [...completedUnitIds, pendingUnitId], sourceProgressSize,
  };
  await kv.set(KV.summaryQueueJobs, jobId, job);
  await kv.set(KV.summaryQueueActive, "session", { jobId });
  for (let index = 0; index < completedOutputs.length; index++) {
    const id = completedUnitIds[index];
    const output = completedOutputs[index];
    const chunk = output.obsRangeStart;
    const completedSummary: SessionSummary = {
      sessionId: "session", project: "project", createdAt: now, title: `Chunk ${chunk}`,
      narrative: `Partial summary for chunk ${chunk} describing its completed work.`,
      keyDecisions: [`decision-${chunk}`, "shared"],
      filesModified: [`src/file-${chunk}.ts`, "src/shared.ts"],
      concepts: [`concept-${chunk}`, "shared-concept"], observationCount: 2,
    };
    await kv.set(KV.summaryQueueUnits(jobId), id, {
      id, jobId, stage: "reduce", round, items: [output], attempts: 0,
      output, summary: completedSummary,
    });
  }
  await kv.set(KV.summaryQueueUnits(jobId), pendingUnitId, {
    id: pendingUnitId, jobId, stage: "reduce", round,
    items: [pendingInput ?? {
      text: "Previous partial summary", obsRangeStart: completedOutputs.length + 1,
      obsRangeEnd: completedOutputs.length + 1,
    }], attempts: 0,
  });
  return { jobId, pendingUnitId };
}

function harness(kv = store(), run: (call: number, prompt: string) => Promise<string> | string = () => xml()) {
  const handlers = new Map<string, Handler>();
  const subscribers = new Map<string, string>();
  const messages: Queued[] = [];
  let calls = 0;
  const provider: MemoryProvider = {
    name: "test", compress: async () => "",
    summarize: async (_systemPrompt, userPrompt) => run(++calls, userPrompt),
  };
  const sdk = {
    registerFunction: (id: string, handler: Handler) => handlers.set(id, handler),
    registerTrigger: ({ type, function_id, config }: { type: string; function_id: string; config: { topic: string } }) => {
      if (type === "durable:subscriber") subscribers.set(config.topic, function_id);
    },
    trigger: async ({ function_id, payload }: { function_id: string; payload: never }) => {
      if (function_id === "engine::queue::topic_stats") {
        return { depth: messages.filter(message => message.function_id === "mem::summary-unit").length, dlq_depth: 0 };
      }
      if (function_id === "iii::durable::publish") {
        const message = payload as { topic: string; data: never };
        const subscriber = subscribers.get(message.topic);
        if (!subscriber) throw new Error(`Missing subscriber: ${message.topic}`);
        messages.push({ function_id: subscriber, payload: message.data });
        return { accepted: true };
      }
      const handler = handlers.get(function_id);
      if (!handler) throw new Error(`Missing handler: ${function_id}`);
      return handler(payload);
    },
  };
  registerSummaryQueueFunctions(sdk as never, kv as never, provider);
  const invoke = (id: string, payload: unknown = {}) => handlers.get(id)!(payload as never);
  const drain = async () => {
    let deliveries = 0;
    while (messages.length) {
      if (++deliveries > 500) throw new Error("queue did not drain");
      const message = messages.shift()!;
      try { await invoke(message.function_id, message.payload); }
      catch { messages.push(message); }
    }
  };
  return { kv, messages, invoke, drain, get calls() { return calls; } };
}

async function seed(kv: ReturnType<typeof store>, observations: CompressedObservation[]) {
  await kv.set(KV.sessions, "session", {
    id: "session", project: "project", cwd: ".", startedAt: "2026-09-28T00:00:00Z",
    status: "completed", observationCount: observations.length,
  } satisfies Session);
  for (const item of observations) await kv.set(KV.observations("session"), item.id, item);
}

describe("durable summary queue", () => {
  it("deduplicates Stop deliveries and clears intermediates after the summary is saved", async () => {
    const h = harness();
    await seed(h.kv, [observation(1)]);
    const first = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    expect(await h.invoke("mem::summary-enqueue", { sessionId: "session" }))
      .toMatchObject({ success: true, queued: true, jobId: first.jobId, deduplicated: true });
    await h.drain();
    expect(h.calls).toBe(1);
    expect((await h.kv.list<SummaryQueueJob>(KV.summaryQueueJobs)).map(job => [job.status, job.failure, job.round]))
      .toEqual([["completed", undefined, 0]]);
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished", observationCount: 1 });
    expect(await h.kv.list(KV.summaryQueueUnits(first.jobId))).toHaveLength(0);
    expect(await h.invoke("mem::summary-enqueue", { sessionId: "session" }))
      .toMatchObject({ success: true, queued: false, completed: true });
  });

  it("recovers a persisted job after the process and its in-memory messages disappear", async () => {
    const kv = store();
    await seed(kv, [observation(1)]);
    const original = harness(kv);
    await original.invoke("mem::summary-enqueue", { sessionId: "session" });
    const restarted = harness(kv);
    expect(await restarted.invoke("mem::summary-recover"))
      .toMatchObject({ success: true, recovered: 1 });
    await restarted.drain();
    expect(await kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
  });

  it("replays a durable API intent only after the session is completed", async () => {
    const h = harness();
    await seed(h.kv, [observation(1)]);
    await h.kv.set(KV.summaryQueueIntents, "session", {
      sessionId: "session", createdAt: "2026-09-28T00:00:00Z",
    });
    await h.kv.set(KV.sessions, "session", {
      ...(await h.kv.get<Session>(KV.sessions, "session"))!, status: "active",
    });
    expect(await h.invoke("mem::summary-recover")).toMatchObject({ recovered: 0 });
    expect(h.messages).toHaveLength(0);
    await h.kv.set(KV.sessions, "session", {
      ...(await h.kv.get<Session>(KV.sessions, "session"))!, status: "completed",
    });
    expect(await h.invoke("mem::summary-recover")).toMatchObject({ recovered: 1 });
    await h.drain();
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
    expect(await h.kv.get(KV.summaryQueueIntents, "session")).toBeNull();
  });

  it("throws a transient provider failure for queue retry, preserving prior unit work", async () => {
    const h = harness(undefined, call => {
      if (call === 1) throw new Error("fetch failed");
      return xml();
    });
    await seed(h.kv, [observation(1)]);
    const { jobId } = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const delivery = h.messages.shift()!;
    await expect(h.invoke(delivery.function_id, delivery.payload)).rejects.toThrow("fetch failed");
    expect(await h.invoke(delivery.function_id, delivery.payload)).toMatchObject({ skipped: true });
    expect(h.calls).toBe(1);
    const unitId = (delivery.payload as { unitId: string }).unitId;
    const scope = KV.summaryQueueUnits(jobId);
    const unit = await h.kv.get<{ lastAttemptAt: string }>(scope, unitId);
    await h.kv.set(scope, unitId, { ...unit!, lastAttemptAt: "2026-01-01T00:00:00Z" });
    await h.invoke(delivery.function_id, delivery.payload);
    expect(h.calls).toBe(2);
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
  });

  it("retries malformed XML immediately within one delivery", async () => {
    const h = harness(undefined, call => call === 1 ? "not XML" : xml());
    await seed(h.kv, [observation(1)]);
    await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const unit = h.messages.shift()!;
    expect(await h.invoke(unit.function_id, unit.payload)).toMatchObject({ success: true, completed: true });
    expect(h.calls).toBe(2);
    expect(h.messages).toHaveLength(0);
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
  });

  it("retries a schema-invalid final summary immediately", async () => {
    const h = harness(undefined, call => call === 1
      ? xml().replace("Completed the requested work and persisted the resulting summary.", "Short")
      : xml());
    await seed(h.kv, [observation(1)]);
    await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const unit = h.messages.shift()!;
    expect(await h.invoke(unit.function_id, unit.payload)).toMatchObject({ success: true, completed: true });
    expect(h.calls).toBe(2);
    expect(h.messages).toHaveLength(0);
  });

  it("marks repeated malformed XML terminal after two immediate calls", async () => {
    const h = harness(undefined, () => "not XML private-token");
    await seed(h.kv, [observation(1)]);
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const unit = h.messages.shift()!;
    expect(await h.invoke(unit.function_id, unit.payload))
      .toMatchObject({ success: false, error: "summary_parse_failed", terminal: true });
    expect(h.calls).toBe(2);
    expect(h.messages).toHaveLength(0);
    expect(await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, result.jobId))
      .toMatchObject({ status: "failed", failure: "summary_parse_failed" });
  });

  it("runs map and reduction units through the same persisted queue", async () => {
    const h = harness();
    await seed(h.kv, Array.from({ length: 20 }, (_, i) => observation(i, "detail ".repeat(110))));
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const job = await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, result.jobId);
    expect(job?.unitIds.length).toBeGreaterThan(1);
    await h.drain();
    expect(h.calls).toBeGreaterThan(job!.unitIds.length);
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ observationCount: 20 });
    expect(await h.kv.list(KV.summaryQueueUnits(result.jobId))).toHaveLength(0);
  });

  it("continues reduction when packed unit count falls despite larger aggregate text", async () => {
    const kv = store();
    const existingOutput = partialOutput("Expanded", "previous detail ".repeat(25), 1);
    const { jobId, pendingUnitId } = await seedReduceJob(kv, 1, [existingOutput]);
    const h = harness(kv, call => call === 1
      ? xml("Expanded", "preserved decision detail ".repeat(15))
      : xml());

    expect(await h.invoke("mem::summary-unit", { jobId, unitId: pendingUnitId }))
      .toMatchObject({ success: true, completed: false });
    const nextRound = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
    expect(nextRound).toMatchObject({ status: "pending", stage: "reduce", round: 2 });
    expect(nextRound?.unitIds).toHaveLength(1);
    expect(nextRound!.sourceProgressSize).toBeGreaterThan(1);

    await h.drain();
    expect(h.calls).toBe(2);
    expect(await kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished", observationCount: 2 });
    expect(await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId)).toMatchObject({ status: "completed" });
  });

  it("reduces five partials to fewer groups and one final summary without dropping lists", async () => {
    const kv = store();
    const completedOutputs = Array.from({ length: 4 }, (_, index) => partialOutput(
      `Chunk ${index + 1}`, "source chunk detail ".repeat(80), index + 1,
      [`decision-${index + 1}`], [`src/file-${index + 1}.ts`], [`concept-${index + 1}`],
    ));
    const pendingInput = partialOutput(
      "Chunk 5", "source chunk detail ".repeat(80), 5,
      ["decision-5"], ["src/file-5.ts"], ["concept-5"],
    );
    const { jobId, pendingUnitId } = await seedReduceJob(kv, 1, completedOutputs, pendingInput);
    const h = harness(kv, (_call, prompt) => reduceXmlFromPrompt(prompt));

    expect(await h.invoke("mem::summary-unit", { jobId, unitId: pendingUnitId }))
      .toMatchObject({ success: true, completed: false });
    const firstReducedRound = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
    expect(firstReducedRound?.round).toBe(2);
    expect(firstReducedRound?.unitIds).toHaveLength(3);

    let deliveries = 0;
    while ((await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId))?.round === 2 && h.messages.length) {
      if (++deliveries > 50) throw new Error("reduction did not reach round 3");
      const message = h.messages.shift()!;
      await h.invoke(message.function_id, message.payload);
    }
    const finalReducedRound = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
    expect(finalReducedRound).toMatchObject({ status: "pending", stage: "reduce", round: 3 });
    expect(finalReducedRound?.unitIds).toHaveLength(1);

    await h.drain();
    const summary = await kv.get<SessionSummary>(KV.summaries, "session");
    expect(summary?.keyDecisions).toEqual(Array.from({ length: 5 }, (_, index) => `decision-${index + 1}`));
    expect(summary?.filesModified).toEqual(Array.from({ length: 5 }, (_, index) => `src/file-${index + 1}.ts`));
    expect(summary?.concepts).toEqual(Array.from({ length: 5 }, (_, index) => `concept-${index + 1}`));
    expect(await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId)).toMatchObject({ status: "completed" });
  });

  it("completes stalled reduction deterministically and preserves ordered, deduplicated lists", async () => {
    const kv = store();
    const firstNarrative = `The session began with initial planning. ${"preserved decision detail ".repeat(400)}`;
    const lastNarrative = `The session ended after final verification. ${"preserved decision detail ".repeat(400)}`;
    const firstOutput = partialOutput("First chunk", firstNarrative, 1,
      ["decision-1", "shared"], ["src/file-1.ts", "src/shared.ts"], ["concept-1", "shared-concept"]);
    const lastOutput = partialOutput("Last chunk", lastNarrative, 2,
      ["decision-2", "shared"], ["src/file-2.ts", "src/shared.ts"], ["concept-2", "shared-concept"]);
    const { jobId, pendingUnitId } = await seedReduceJob(
      kv, summaryProgressSize([lastOutput, firstOutput]), [lastOutput], firstOutput,
    );
    const scope = KV.summaryQueueUnits(jobId);
    const seeded = await kv.get<{ summary: SessionSummary }>(scope, "unit-reduce-completed-1");
    await kv.set(scope, "unit-reduce-completed-1", {
      ...seeded!, summary: {
        ...seeded!.summary, title: "Last chunk", narrative: lastNarrative,
        keyDecisions: ["decision-2", "shared"],
        filesModified: ["src/file-2.ts", "src/shared.ts"],
        concepts: ["concept-2", "shared-concept"],
      },
    });
    const pending = await kv.get<{ items: SummaryPromptItem[] }>(scope, pendingUnitId);
    await kv.set(scope, pendingUnitId, {
      ...pending!, output: firstOutput,
      summary: {
        sessionId: "session", project: "project", createdAt: "2026-09-28T00:00:00.000Z",
        title: "First chunk", narrative: firstNarrative,
        keyDecisions: ["decision-1", "shared"],
        filesModified: ["src/file-1.ts", "src/shared.ts"],
        concepts: ["concept-1", "shared-concept"], observationCount: 1,
      },
    });
    const h = harness(kv, () => { throw new Error("unexpected provider call"); });

    expect(await h.invoke("mem::summary-unit", { jobId, unitId: pendingUnitId }))
      .toMatchObject({ success: true, completed: true, method: "deterministic_fallback" });
    expect(h.calls).toBe(0);
    const summary = await kv.get<SessionSummary>(KV.summaries, "session");
    expect(summary).toMatchObject({
      observationCount: 2,
      keyDecisions: ["decision-1", "shared", "decision-2"],
      filesModified: ["src/file-1.ts", "src/shared.ts", "src/file-2.ts"],
      concepts: ["concept-1", "shared-concept", "concept-2"],
    });
    expect(summary?.narrative).toContain("Beginning: The session began with initial planning.");
    expect(summary?.narrative).toContain("Ending: The session ended after final verification.");
    expect(await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId))
      .toMatchObject({ status: "completed", round: 1 });
    const audit = await kv.list<{ details: Record<string, unknown> }>(KV.audit);
    expect(audit[0]?.details).toMatchObject({ method: "deterministic_fallback", observationCount: 2 });
    expect(audit[0]?.details).not.toHaveProperty("title");
    expect(h.messages).toHaveLength(0);
  });

  it("uses deterministic fallback at maximum depth without another provider call or round", async () => {
    const kv = store();
    const first = partialOutput("Chunk 1", "The first chunk records the initial work in the session.", 1,
      ["decision-1"], ["src/file-1.ts"], ["concept-1"]);
    const last = partialOutput("Chunk 2", "The last chunk records the completed work in the session.", 2,
      ["decision-2"], ["src/file-2.ts"], ["concept-2"]);
    const { jobId, pendingUnitId } = await seedReduceJob(
      kv, summaryProgressSize([first, last]), [first], last, MAX_SUMMARY_DEPTH,
    );
    const scope = KV.summaryQueueUnits(jobId);
    const pending = await kv.get<{ items: SummaryPromptItem[] }>(scope, pendingUnitId);
    await kv.set(scope, pendingUnitId, {
      ...pending!, items: [last], output: last,
      summary: {
        sessionId: "session", project: "project", createdAt: "2026-09-28T00:00:00.000Z",
        title: "Chunk 2", narrative: "The last chunk records the completed work in the session.",
        keyDecisions: ["decision-2"], filesModified: ["src/file-2.ts"], concepts: ["concept-2"],
        observationCount: 1,
      },
    });
    const h = harness(kv, () => { throw new Error("unexpected provider call"); });

    expect(await h.invoke("mem::summary-unit", { jobId, unitId: pendingUnitId }))
      .toMatchObject({ success: true, completed: true, method: "deterministic_fallback" });
    expect(h.calls).toBe(0);
    expect(await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId))
      .toMatchObject({ status: "completed", round: MAX_SUMMARY_DEPTH });
    expect(h.messages).toHaveLength(0);
    expect(await kv.get<SessionSummary>(KV.summaries, "session"))
      .toMatchObject({ keyDecisions: ["decision-1", "shared", "decision-2"] });
  });

  it("fails deterministic fallback when any partial summary fails schema validation", async () => {
    const kv = store();
    const first = partialOutput("Chunk 1", "The first chunk records the initial work in the session.", 1);
    const last = partialOutput("Chunk 2", "The last chunk records the completed work in the session.", 2);
    const { jobId, pendingUnitId } = await seedReduceJob(
      kv, summaryProgressSize([first, last]), [first], last, MAX_SUMMARY_DEPTH,
    );
    const scope = KV.summaryQueueUnits(jobId);
    const pending = await kv.get<{ items: SummaryPromptItem[] }>(scope, pendingUnitId);
    await kv.set(scope, pendingUnitId, {
      ...pending!, items: [last], output: last,
      summary: {
        sessionId: "session", project: "project", createdAt: "2026-09-28T00:00:00.000Z",
        title: "Chunk 2", narrative: "too short", keyDecisions: ["decision-2"],
        filesModified: ["src/file-2.ts"], concepts: ["concept-2"], observationCount: 1,
      },
    });
    const h = harness(kv, () => { throw new Error("unexpected provider call"); });

    expect(await h.invoke("mem::summary-unit", { jobId, unitId: pendingUnitId }))
      .toMatchObject({ success: false, error: "summary_validation_failed", terminal: true });
    expect(h.calls).toBe(0);
    expect(await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId))
      .toMatchObject({ status: "failed", failure: "summary_validation_failed" });
    expect(await kv.get(KV.summaries, "session")).toBeNull();
  });

  it("keeps a 12-unit window and refills it as each map unit finishes", async () => {
    const h = harness();
    await seed(h.kv, Array.from({ length: 30 }, (_, i) => observation(i, "detail ".repeat(110))));
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const job = await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, result.jobId);
    expect(job!.unitIds.length).toBeGreaterThan(12);
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    expect(h.messages).toHaveLength(12);
    expect(h.messages.every(message => message.function_id === "mem::summary-unit")).toBe(true);
    const first = h.messages.shift()!;
    await h.invoke(first.function_id, first.payload);
    expect(h.messages).toHaveLength(12);
    await h.drain();
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ observationCount: 30 });
  });

  it("requeues a lost undelivered unit after the topic is empty and continues reduction", async () => {
    const h = harness();
    await seed(h.kv, Array.from({ length: 20 }, (_, i) => observation(i, "detail ".repeat(110))));
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const lost = h.messages.shift()!;
    const unitId = (lost.payload as { unitId: string }).unitId;
    await h.drain();
    expect(h.messages).toHaveLength(0);
    expect(await h.kv.get(KV.summaries, "session")).toBeNull();
    const scope = KV.summaryQueueUnits(result.jobId);
    const unit = await h.kv.get<{ attempts: number; dispatchedAt: string; startedAt?: string }>(scope, unitId);
    expect(unit).toMatchObject({ attempts: 0 });
    expect(unit?.startedAt).toBeUndefined();
    await h.kv.set(scope, unitId, {
      ...unit!, dispatchedAt: "2026-01-01T00:00:00Z", startedAt: new Date().toISOString(),
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ success: true, replayed: 0 });
    expect(h.messages).toHaveLength(0);
    await h.kv.set(scope, unitId, {
      ...unit!, dispatchedAt: "2026-01-01T00:00:00Z", startedAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 1, recovered: 1 });
    expect(h.messages).toHaveLength(1);
    await h.drain();
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ observationCount: 20 });
  });

  it("replays a stale initial delivery while the topic is busy and fences the old delivery", async () => {
    const h = harness();
    await seed(h.kv, [observation(1)]);
    const { jobId } = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const oldDelivery = h.messages.shift()!;
    const unitId = (oldDelivery.payload as { unitId: string }).unitId;
    const scope = KV.summaryQueueUnits(jobId);
    const unit = await h.kv.get<{ dispatchedAt: string; deliveryId: string }>(scope, unitId);
    h.messages.push(oldDelivery, {
      function_id: "mem::summary-unit", payload: { jobId: "other", unitId: "other" } as never,
    });
    const recent = new Date(Date.now() - 2 * 60_000).toISOString();
    await h.kv.set(scope, unitId, { ...unit!, dispatchedAt: recent, startedAt: recent });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 0 });

    await h.kv.set(scope, unitId, {
      ...unit!, dispatchedAt: "2026-01-01T00:00:00Z", startedAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 1, recovered: 1 });
    const newDelivery = h.messages.find(message =>
      (message.payload as { unitId?: string }).unitId === unitId && message !== oldDelivery);
    expect(newDelivery).toBeDefined();
    expect((newDelivery!.payload as { deliveryId: string }).deliveryId).not.toBe(unit!.deliveryId);
    expect(await h.invoke(oldDelivery.function_id, oldDelivery.payload)).toMatchObject({ skipped: true });
    expect(h.calls).toBe(0);
    await h.invoke(newDelivery!.function_id, newDelivery!.payload);
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
  });

  it("retries a failed unit after 30 seconds", async () => {
    const h = harness(undefined, call => {
      if (call === 1) throw new Error("fetch failed");
      return xml();
    });
    await seed(h.kv, [observation(1)]);
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const message = h.messages.shift()!;
    await expect(h.invoke(message.function_id, message.payload)).rejects.toThrow("fetch failed");
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 0 });
    expect(h.messages).toHaveLength(0);
    const unitId = (message.payload as { unitId: string }).unitId;
    const scope = KV.summaryQueueUnits(result.jobId);
    const unit = await h.kv.get<{ attempts: number; dispatchedAt: string; lastAttemptAt: string }>(scope, unitId);
    expect(unit?.attempts).toBe(1);
    await h.kv.set(scope, unitId, {
      ...unit!, lastAttemptAt: new Date(Date.now() - 29_000).toISOString(),
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 0 });
    await h.kv.set(scope, unitId, {
      ...unit!, lastAttemptAt: new Date(Date.now() - 31_000).toISOString(),
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 1 });
    await h.drain();
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
  });

  it("replays an eligible failed unit while other deliveries occupy the topic", async () => {
    const h = harness(undefined, call => {
      if (call === 1) throw new Error("fetch failed");
      return xml();
    });
    await seed(h.kv, [observation(1)]);
    const { jobId } = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const failedDelivery = h.messages.shift()!;
    h.messages.push({ function_id: "mem::summary-unit", payload: { jobId: "other", unitId: "other" } as never });
    const failedId = (failedDelivery.payload as { unitId: string }).unitId;
    const scope = KV.summaryQueueUnits(jobId);
    const initialUnit = await h.kv.get<{ dispatchedAt: string; deliveryId: string }>(scope, failedId);
    await h.kv.set(scope, failedId, {
      ...initialUnit!, dispatchedAt: new Date(Date.now() - 2 * 60_000).toISOString(),
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 0 });
    expect(await h.kv.get(scope, failedId)).toMatchObject({ deliveryId: initialUnit!.deliveryId });
    await h.kv.set(scope, failedId, initialUnit!);
    await expect(h.invoke(failedDelivery.function_id, failedDelivery.payload)).rejects.toThrow("fetch failed");

    const failedUnit = await h.kv.get<{ dispatchedAt: string; lastAttemptAt: string }>(scope, failedId);
    await h.kv.set(scope, failedId, {
      ...failedUnit!, dispatchedAt: "2026-01-01T00:00:00Z", lastAttemptAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 1 });
    const replay = h.messages.find(message =>
      (message.payload as { unitId?: string }).unitId === failedId &&
      (message.payload as { deliveryId?: string }).deliveryId !==
        (failedDelivery.payload as { deliveryId?: string }).deliveryId);
    expect(replay).toBeDefined();
    expect(await h.invoke(failedDelivery.function_id, failedDelivery.payload))
      .toMatchObject({ skipped: true });
    expect(h.calls).toBe(1);
    await h.invoke(replay!.function_id, replay!.payload);
    expect(h.calls).toBe(2);
  });

  it("does not replay an active provider call even after its persisted start looks stale", async () => {
    let release!: (value: string) => void;
    const providerResult = new Promise<string>(resolve => { release = resolve; });
    const h = harness(undefined, () => providerResult);
    await seed(h.kv, [observation(1)]);
    const { jobId } = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const delivery = h.messages.shift()!;
    const inFlight = h.invoke(delivery.function_id, delivery.payload);
    await vi.waitFor(() => expect(h.calls).toBe(1));
    const unitId = (delivery.payload as { unitId: string }).unitId;
    const scope = KV.summaryQueueUnits(jobId);
    const unit = await h.kv.get<{ dispatchedAt: string; startedAt: string }>(scope, unitId);
    await h.kv.set(scope, unitId, {
      ...unit!, dispatchedAt: "2026-01-01T00:00:00Z", startedAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 0 });
    expect(h.messages).toHaveLength(0);
    release(xml());
    await inFlight;
    expect(h.calls).toBe(1);
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
  });

  it("releases a unit when its provider call never settles", async () => {
    vi.useFakeTimers();
    try {
      const h = harness(undefined, () => new Promise<string>(() => {}));
      await seed(h.kv, [observation(1)]);
      const { jobId } = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
      const dispatch = h.messages.shift()!;
      await h.invoke(dispatch.function_id, dispatch.payload);
      const delivery = h.messages.shift()!;
      const inFlight = h.invoke(delivery.function_id, delivery.payload);
      await vi.advanceTimersByTimeAsync(0);
      expect(h.calls).toBe(1);

      const failed = expect(inFlight).rejects.toThrow("Summary provider timed out");
      await vi.advanceTimersByTimeAsync(150_000);
      await failed;
      const unitId = (delivery.payload as { unitId: string }).unitId;
      expect(await h.kv.get(KV.summaryQueueUnits(jobId), unitId))
        .toMatchObject({ attempts: 1, startedAt: undefined, lastError: "timeout" });
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps only a compact failed diagnosis for 30 days, then cleans it", async () => {
    const h = harness(undefined, () => "invalid response private-token");
    await seed(h.kv, [observation(1)]);
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    await h.drain();
    const job = await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, result.jobId);
    expect(job).toMatchObject({ status: "failed", failure: "summary_parse_failed" });
    expect(JSON.stringify(job)).not.toContain("private-token");
    expect(await h.kv.list(KV.summaryQueueUnits(result.jobId))).toHaveLength(0);
    await h.kv.set(KV.summaryQueueJobs, result.jobId, {
      ...job!, failedAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-recover")).toMatchObject({ cleaned: 1 });
    expect(await h.kv.get(KV.summaryQueueJobs, result.jobId)).toBeNull();
    expect(await h.kv.list(KV.observations("session"))).toHaveLength(1);
  });
});
