import { describe, expect, it, vi } from "vitest";
import type { CompressedObservation, MemoryProvider, Session, SummaryQueueJob } from "../src/types.js";
import { KV } from "../src/state/schema.js";
import { registerSummaryQueueFunctions } from "../src/functions/summary-queue.js";

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

const xml = (title = "Finished") => `<summary><title>${title}</title><narrative>Completed the requested work and persisted the resulting summary.</narrative><decisions><decision>persist</decision></decisions><files><file>src/file.ts</file></files><concepts><concept>queue</concept></concepts></summary>`;

function harness(kv = store(), run: (call: number) => Promise<string> | string = () => xml()) {
  const handlers = new Map<string, Handler>();
  const subscribers = new Map<string, string>();
  const messages: Queued[] = [];
  let calls = 0;
  const provider: MemoryProvider = {
    name: "test", compress: async () => "",
    summarize: async () => run(++calls),
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
    await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    await h.drain();
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
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 0 });
    expect(h.messages).toHaveLength(0);
    await h.kv.set(scope, unitId, {
      ...unit!, dispatchedAt: "2026-01-01T00:00:00Z", startedAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 1, recovered: 1 });
    expect(h.messages).toHaveLength(1);
    await h.drain();
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ observationCount: 20 });
  });

  it("does not bypass the 15-minute retry delay for a failed unit", async () => {
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
      ...unit!, dispatchedAt: "2026-01-01T00:00:00Z", lastAttemptAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-reconcile")).toMatchObject({ replayed: 1 });
    await h.drain();
    expect(await h.kv.get(KV.summaries, "session")).toMatchObject({ title: "Finished" });
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
