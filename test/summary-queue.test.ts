import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  CompressedObservation, MemoryProvider, Session, SessionSummary, SummaryQueueJob,
} from "../src/types.js";
import { fingerprintId, KV } from "../src/state/schema.js";
import { registerSummaryQueueFunctions, summarySourceFingerprint, validManualSummaryContinuation,
  type ManualSummaryContinuation } from "../src/functions/summary-queue.js";
import { MAX_SUMMARY_DEPTH, summaryProgressSize } from "../src/functions/summary-budget.js";
import { formatSummaryPartial, type SummaryPromptItem } from "../src/prompts/summary.js";
import {
  buildReduceItemsPrompt, buildSummaryItemsPrompt, REDUCE_SYSTEM, SUMMARY_RECOVERY_FIDELITY_VERSION, SUMMARY_SYSTEM,
} from "../src/functions/summary-queue-prompts.js";
import {
  buildReduceItemsPrompt as buildBaseReduceItemsPrompt,
  buildSummaryItemsPrompt as buildBaseSummaryItemsPrompt,
  REDUCE_SYSTEM as BASE_REDUCE_SYSTEM,
  SUMMARY_SYSTEM as BASE_SUMMARY_SYSTEM,
} from "../src/prompts/summary.js";
import { captureSummaryEvidence } from "../src/functions/summary-source-grounding.js";
import { WorkflowAttemptAccounting } from "../src/functions/workflow-attempt-accounting.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
const semantic = vi.hoisted(() => ({ real: false, verdict: "supported" }));
vi.mock("../src/functions/publication-evidence.js", async importOriginal => {
  const actual = await importOriginal<typeof import("../src/functions/publication-evidence.js")>();
  return { ...actual, evaluatePublicationEvidence: (...args: Parameters<typeof actual.evaluatePublicationEvidence>) =>
    semantic.real ? actual.evaluatePublicationEvidence(...args) : Promise.resolve({ status: "supported", requestId: "mock", reason: "mock", replayed: false }) };
});
afterEach(() => { semantic.real = false; semantic.verdict = "supported"; });
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
  const revisions = new Map<string, number>();
  const pageControl: {
    bounded?: boolean;
    failOnPage?: number;
    afterPage?: (page: number, scope: string) => void | Promise<void>;
  } = {};
  const pageResults: Array<{ scope: string; itemCount: number; bytes: number; limit: number; maxBytes: number }> = [];
  return {
    rows,
    pageControl,
    pageResults,
    get: async <T>(scope: string, key: string): Promise<T | null> => (rows.get(scope)?.get(key) as T) ?? null,
    getVersioned: async <T>(scope: string, key: string) => ({ exists: rows.get(scope)?.has(key) ?? false,
      value: rows.get(scope)?.get(key) as T ?? null, version: fingerprintId("version", JSON.stringify(rows.get(scope)?.get(key))) }),
    set: async <T>(scope: string, key: string, value: T): Promise<T> => {
      if (!rows.has(scope)) rows.set(scope, new Map());
      rows.get(scope)!.set(key, value);
      revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
      return value;
    },
    delete: async (scope: string, key: string) => {
      if (rows.get(scope)?.delete(key)) revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
    },
    list: async <T>(scope: string): Promise<T[]> => [...(rows.get(scope)?.values() ?? [])] as T[],
    scopeRevision: async (scope: string) => ({ generation: "test", revision: String(revisions.get(scope) ?? 0) }),
    pages: async function* <T>(scope: string, options?: { limit?: number; maxBytes?: number }): AsyncGenerator<{ items: T[]; next_cursor: string | null }> {
      const values = [...(rows.get(scope)?.values() ?? [])];
      if (!pageControl.bounded) {
        yield { items: values as T[], next_cursor: null };
        return;
      }
      const limit = options?.limit ?? 256;
      const maxBytes = options?.maxBytes ?? 1_048_576;
      let offset = 0;
      let pageNumber = 0;
      do {
        pageNumber++;
        if (pageControl.failOnPage === pageNumber) throw Object.assign(new Error("page failed"), { code: "STATE_PAGE_FAILED" });
        const items: T[] = [];
        while (offset < values.length && items.length < limit) {
          const candidate = [...items, values[offset] as T];
          const cursor = offset + 1 < values.length ? `cursor-${offset + 1}` : null;
          if (Buffer.byteLength(JSON.stringify({ items: candidate, next_cursor: cursor }), "utf8") > maxBytes) {
            if (items.length === 0) throw Object.assign(new Error("record too large"), { code: "STATE_RECORD_TOO_LARGE" });
            break;
          }
          items.push(values[offset] as T);
          offset++;
        }
        const next_cursor = offset < values.length ? `cursor-${offset}` : null;
        const bytes = Buffer.byteLength(JSON.stringify({ items, next_cursor }), "utf8");
        pageResults.push({ scope, itemCount: items.length, bytes, limit, maxBytes });
        yield { items, next_cursor };
        await pageControl.afterPage?.(pageNumber, scope);
        if (next_cursor === null) return;
      } while (true);
    },
  };
}

function observation(index: number, narrative = "source"): CompressedObservation {
  return {
    id: `obs-${index}`, sessionId: "session", timestamp: `2026-09-28T00:00:${String(index).padStart(2, "0")}Z`,
    type: "conversation", title: `Observation ${index}`, narrative, facts: ["fact"],
    files: ["src/file.ts"], concepts: ["queue"], importance: 5,
  };
}

function copiedSourceObservation(index: number, sourceSessionId: string, narrative: string,
  filesModified: string[] = [], files: string[] = []): CompressedObservation {
  const value = observation(index, JSON.stringify({
    provenance: { originalSession: sourceSessionId },
    completeOriginalSource: { sessionId: sourceSessionId, title: `Source ${index}`, narrative, files, filesModified },
  }));
  value.files = [];
  value.facts = [];
  return value;
}

function observationWithSerializedBytes(index: number, targetBytes: number): CompressedObservation {
  const value = observation(index, "");
  value.raw = { prompt: `raw prompt ${index}`, narrative: `raw narrative ${index}` };
  value.userPrompt = `user prompt ${index}`;
  value.assistantResponse = `assistant response ${index}`;
  const padding = targetBytes - Buffer.byteLength(JSON.stringify(value), "utf8");
  if (padding < 0) throw new Error("target observation size is too small");
  value.narrative = "x".repeat(padding);
  if (Buffer.byteLength(JSON.stringify(value), "utf8") !== targetBytes) throw new Error("observation size mismatch");
  return value;
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
    sourcePaths: [...filesModified], modifiedPaths: [...filesModified],
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
  await seed(kv, Array.from({ length: job.observationCount }, (_, index) => observation(index)));
  job.snapshotFingerprint = await summarySourceFingerprint(kv as never, job.sessionId, job.project);
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
      sourcePaths: ["src/file.ts"], modifiedPaths: ["src/file.ts"],
    }], attempts: 0,
  });
  return { jobId, pendingUnitId };
}

function harness(kv = store(), run: (call: number, prompt: string) => Promise<string> | string = () => xml(), accounting?: WorkflowAttemptAccounting) {
  const handlers = new Map<string, Handler>();
  const subscribers = new Map<string, string>();
  const messages: Queued[] = [];
  let calls = 0;
  let judges = 0;
  const judgeInputs: string[] = [];
  const provider: MemoryProvider = {
    name: "test", compress: async () => "",
    summarize: async (systemPrompt, userPrompt) => {
      if (systemPrompt.startsWith("You evaluate publication evidence.")) {
        judges++; judgeInputs.push(userPrompt);
        return JSON.stringify({ verdict: semantic.verdict, reason: "mock semantic decision" });
      }
      return run(++calls, userPrompt);
    },
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
  registerSummaryQueueFunctions(sdk as never, kv as never, provider, undefined, accounting);
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
  return { kv, messages, invoke, drain, judgeInputs, get calls() { return calls; }, get judges() { return judges; } };
}

async function seed(kv: ReturnType<typeof store>, observations: CompressedObservation[]) {
  await kv.set(KV.sessions, "session", {
    id: "session", project: "project", cwd: ".", startedAt: "2026-09-28T00:00:00Z",
    status: "completed", observationCount: observations.length,
  } satisfies Session);
  for (const item of observations) await kv.set(KV.observations("session"), item.id, item);
}

describe("durable summary queue", () => {
  it("manual continuation enters actual generation scope, enqueue enters none", async () => {
    const accounting = new WorkflowAttemptAccounting([], []); const spy = vi.spyOn(accounting, "run");
    const h = harness(store(), () => xml(), accounting);
    await seed(h.kv, [observation(0, "source")]);
    const first = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as { continuation: unknown };
    expect(spy).not.toHaveBeenCalled();
    await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session", continuation: first.continuation });
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][1]).toMatchObject({ sourceId: "session", phase: "generation", functionId: "mem::summary-unit" });
  });
  it("revalidates actual queued generation after enqueue and process rehydration", async () => {
    const kv = store(); const first = harness(kv);
    await seed(kv, [observation(0, "source")]);
    await first.invoke("mem::summary-enqueue", { sessionId: "session" });
    expect(first.calls).toBe(0);
    const accounting = new WorkflowAttemptAccounting([{ workflow: "summary", project: "project", sourceId: "session" }], []);
    const restarted = harness(kv, () => xml(), accounting);
    const spy = vi.spyOn(accounting, "run");
    for (const message of first.messages) await restarted.invoke(message.function_id, message.payload);
    await restarted.drain();
    expect(restarted.calls).toBe(0);
    expect(spy).toHaveBeenCalled();
    expect(spy.mock.calls[0][1]).toMatchObject({ functionId: "mem::summary-unit", sourceId: "session", phase: "generation" });
  });
  it("keeps the original 4096-observation admission capacity with paged source versions", async () => {
    const h = harness();
    await seed(h.kv, Array.from({ length: 4096 }, (_, index) => observation(index, "source")));
    const admitted = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const root = await h.kv.get<{ count: number; pages: number; mode: string }>(KV.state, `${admitted.jobId}:source-root`);
    expect(root).toMatchObject({ count: 4096, pages: 342, mode: "hierarchical" });
    for (let page = 0; page < root!.pages; page++) {
      const references = await h.kv.get<unknown[]>(KV.state, `${admitted.jobId}:source-page:${page}`);
      expect(references!.length).toBeLessThanOrEqual(12);
      expect(Buffer.byteLength(JSON.stringify(references))).toBeLessThan(16384);
    }
    expect(h.calls).toBe(0); expect(h.judges).toBe(0);
  });

  it("captures normal manual sources across bounded pages and preserves exact source evidence", async () => {
    const h = harness();
    h.kv.pageControl.bounded = true;
    const observations = [
      observationWithSerializedBytes(0, 7_435),
      observationWithSerializedBytes(1, 5_224),
      observationWithSerializedBytes(2, 4_003),
    ];
    expect(Buffer.byteLength(JSON.stringify(observations), "utf8")).toBe(16_666);
    await seed(h.kv, observations);

    const queued = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      success: boolean; continuation: unknown;
    };
    expect(queued).toMatchObject({ success: true, queued: true, completed: false });
    const jobId = (queued.continuation as { jobId: string }).jobId;
    const job = await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
    expect(job).toMatchObject({ status: "pending", executionOrigin: "manual", observationCount: 3 });
    const root = await h.kv.get<{ count: number }>(KV.state, `${jobId}:source-root`);
    const references = await h.kv.get<Array<{ id: string; version: string; hash: string; ordinal: number }>>(
      KV.state, `${jobId}:source-page:0`);
    expect(root?.count).toBe(3);
    expect(references?.map(reference => reference.id)).toEqual(observations.map(item => item.id));
    for (const [index, reference] of (references ?? []).entries()) {
      const current = await h.kv.getVersioned<CompressedObservation>(KV.observations("session"), reference.id);
      expect(reference).toMatchObject({ ordinal: index + 1, version: current.version,
        hash: fingerprintId("summary-source", JSON.stringify(observations[index])) });
      expect(current.value).toEqual(observations[index]);
    }
    const sourcePages = h.kv.pageResults.filter(page => page.scope === KV.observations("session"));
    expect(sourcePages.length).toBeGreaterThan(1);
    expect(sourcePages.every(page => page.limit === 13 && page.maxBytes === 16_384 && page.bytes <= page.maxBytes)).toBe(true);
    expect(h.calls).toBe(0);

    await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: queued.continuation,
    });
    expect(h.calls).toBeGreaterThan(0);
  });

  it("keeps referenced paths as context and does not promote paths to current-session edits", async () => {
    const h = harness();
    const historical = copiedSourceObservation(0, "prior-session",
      "A prior session updated the environment and startup configuration.",
      ["C:/Users/harus/.agentmemory/.env", "C:/Users/harus/.agentmemory/Start-AgentMemory.ps1"]);
    const ideTabs = copiedSourceObservation(1, "older-session",
      "# Context from my IDE setup:\n\n## Open tabs:\n- notification.mjs: plugin/scripts/notification.mjs\n- post-commit.mjs: plugin/scripts/post-commit.mjs",
      [], []);
    const current = copiedSourceObservation(2, "session", "The current session explicitly changed src/current.ts.", ["src/current.ts"]);
    await seed(h.kv, [historical, ideTabs, current]);
    const queued = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const units = await h.kv.list<{ items: SummaryPromptItem[] }>(KV.summaryQueueUnits(queued.jobId));
    const items = units.flatMap(unit => unit.items);
    const priorItem = items.find(item => item.sourcePaths?.includes("C:/Users/harus/.agentmemory/Start-AgentMemory.ps1"))!;
    const tabsItem = items.find(item => item.text.includes("plugin/scripts/notification.mjs"))!;
    const currentItem = items.find(item => item.text.includes("src/current.ts"))!;
    expect(priorItem.sourcePaths).toContain("C:/Users/harus/.agentmemory/Start-AgentMemory.ps1");
    expect(priorItem.modifiedPaths).toEqual([]);
    expect(tabsItem.sourcePaths).toContain("plugin/scripts/notification.mjs");
    expect(tabsItem.modifiedPaths).toEqual([]);
    expect(currentItem.sourcePaths).toContain("src/current.ts");
    expect(currentItem.modifiedPaths).toEqual([]);
    const prompt = buildSummaryItemsPrompt(items);
    expect(items.map(item => item.text).join("\n")).toContain("C:/Users/harus/.agentmemory/.env");
    expect(prompt).toContain("C:/Users/harus/.agentmemory/.env");
    expect(prompt).toContain("prior-session");
    expect(prompt).not.toContain("Source paths supported as modified files");
    expect(SUMMARY_SYSTEM).toContain("the enclosing session ID and capture time describe collection, not authorship");
    expect(SUMMARY_SYSTEM).toMatch(/qualify missing or conflicting provenance/i);
    expect(SUMMARY_SYSTEM).toContain("files supported as changed in the session being summarized");
    expect(SUMMARY_SYSTEM).toContain("configuration, script propagation, provider selection, and outcomes as distinct");
    expect(SUMMARY_SYSTEM).toContain("IDE-tab path does not prove a file changed");
    expect(REDUCE_SYSTEM).toContain("preserve distinct origins among full originals");
    expect(h.calls).toBe(0);
  });

  it("keeps fidelity protocol fixed-prompt overhead within the existing summary budget", () => {
    const items: SummaryPromptItem[] = [{ text: "sparse source", obsRangeStart: 1, obsRangeEnd: 1,
      fragment: true, sourcePaths: ["src/open.ts"], modifiedPaths: ["src/open.ts"] }];
    const context = items.map(item => ({ ...item, modifiedPaths: [] }));
    expect(Buffer.byteLength(SUMMARY_SYSTEM, "utf8")).toBeLessThanOrEqual(Buffer.byteLength(BASE_SUMMARY_SYSTEM, "utf8"));
    expect(Buffer.byteLength(REDUCE_SYSTEM, "utf8")).toBeLessThanOrEqual(Buffer.byteLength(BASE_REDUCE_SYSTEM, "utf8"));
    expect(Buffer.byteLength(buildSummaryItemsPrompt(items), "utf8"))
      .toBeLessThanOrEqual(Buffer.byteLength(buildBaseSummaryItemsPrompt(context), "utf8"));
    expect(Buffer.byteLength(buildReduceItemsPrompt(items), "utf8"))
      .toBeLessThanOrEqual(Buffer.byteLength(buildBaseReduceItemsPrompt(context), "utf8"));
  });

  it("returns an explicit bounded error for one observation larger than a page", async () => {
    const h = harness();
    h.kv.pageControl.bounded = true;
    await seed(h.kv, [observationWithSerializedBytes(0, 16_385)]);
    expect(await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }))
      .toMatchObject({ success: false, error: "summary_source_grounding_snapshot_limit_exceeded" });
    expect(await h.kv.list(KV.summaryQueueJobs)).toHaveLength(0);
    expect(h.calls).toBe(0);
  });

  it("regenerates an explicitly resumed failed publication into a new job and retains old history", async () => {
    semantic.real = true;
    const prompts: string[] = [];
    const h = harness(undefined, (_call, prompt) => { prompts.push(prompt); return xml(); });
    const source = observation(0, "The original observations remain available for faithful regeneration.");
    await seed(h.kv, [source]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const oldJob = {
      id: "failed-publication", sessionId: "session", project: "project", snapshotFingerprint: sourceFingerprint,
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["old-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual", dispatchPending: true,
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, oldJob.id, oldJob);
    const staleCandidate = { id: "old-final-unit", jobId: oldJob.id, stage: "reduce", round: 1,
      items: [{ text: "STALE BAD CANDIDATE: claim the current session edited src/invented.ts.",
        obsRangeStart: 1, obsRangeEnd: 1 }], attempts: 1 };
    await h.kv.set(KV.summaryQueueUnits(oldJob.id), staleCandidate.id, staleCandidate);
    await captureSummaryEvidence(h.kv as never, oldJob, [source]);
    const sourceRoot = await h.kv.get(KV.state, `${oldJob.id}:source-root`);
    const sourcePage = await h.kv.get(KV.state, `${oldJob.id}:source-page:0`);
    const legacyOperationId = fingerprintId("summary-recovery", JSON.stringify([SUMMARY_RECOVERY_FIDELITY_VERSION,
      "full-originals-v1", oldJob.id, oldJob.project, oldJob.sessionId, sourceFingerprint]));
    const legacyJobId = fingerprintId("sqj", legacyOperationId);
    const legacyChild = { ...oldJob, id: legacyJobId, status: "pending" as const, recoveryMode: "full-originals-v1" as const,
      recoveryOfJobId: oldJob.id, dispatchPending: true };
    const legacyUnit = { ...staleCandidate, jobId: legacyJobId };
    const legacyContinuation: ManualSummaryContinuation = { operationId: legacyOperationId, step: 0,
      jobId: oldJob.id, project: oldJob.project, sessionId: oldJob.sessionId, sourceFingerprint,
      round: oldJob.round, unitId: "old-final-unit", deliveryId: legacyOperationId };
    const legacyResponse = { success: true, queued: true, completed: false, jobId: legacyJobId,
      regeneratedFrom: oldJob.id, continuation: legacyContinuation };
    const legacyIdentity = JSON.stringify([legacyOperationId, 0, oldJob.id, oldJob.project, oldJob.sessionId,
      sourceFingerprint, oldJob.round, "old-final-unit", legacyOperationId]);
    await h.kv.set(KV.summaryQueueJobs, legacyJobId, legacyChild);
    await h.kv.set(KV.summaryQueueUnits(legacyJobId), legacyUnit.id, legacyUnit);
    await h.kv.set(KV.state, `manual-summary-recovery:${legacyOperationId}`, { identity: legacyIdentity, response: legacyResponse });
    expect(await h.kv.get(KV.summaryQueueActive, "session")).toBeNull();
    const markerKey = `session:failed-summary:${sourceFingerprint}`;
    const failedMarker = { jobId: oldJob.id, reason: oldJob.failure };
    await h.kv.set(KV.state, markerKey, failedMarker);

    const retained = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      success: boolean; continuation: ManualSummaryContinuation;
    };
    expect(retained.success).toBe(false);
    expect(retained.continuation).toMatchObject({ jobId: oldJob.id, sourceFingerprint, round: 1,
      recoveryGeneration: "full-originals-v2" });
    const continuation = retained.continuation;
    const resumed = await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation,
    }) as Record<string, unknown>;
    expect(resumed).toMatchObject({ success: true, queued: true, regeneratedFrom: oldJob.id });
    const messagesAfterRecovery = h.messages.length;
    const callsAfterRecovery = h.calls;
    const reorderedContinuation: ManualSummaryContinuation = {
      deliveryId: continuation.deliveryId, unitId: continuation.unitId, round: continuation.round,
      sourceFingerprint: continuation.sourceFingerprint, sessionId: continuation.sessionId,
      project: continuation.project, jobId: continuation.jobId, step: continuation.step,
      operationId: continuation.operationId, recoveryGeneration: continuation.recoveryGeneration,
    };
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: reorderedContinuation,
    })).toEqual(resumed);
    expect(h.messages).toHaveLength(messagesAfterRecovery);
    expect(h.calls).toBe(callsAfterRecovery);
    const recoveredJobId = fingerprintId("sqj", continuation.operationId);
    expect(recoveredJobId).not.toBe(oldJob.id);
    expect(recoveredJobId).not.toBe(legacyJobId);
    const recoveredJob = await h.kv.get<SummaryQueueJob & { executionOrigin: string }>(KV.summaryQueueJobs, recoveredJobId);
    expect(recoveredJob).toMatchObject({ id: recoveredJobId, sessionId: "session", project: "project",
      snapshotFingerprint: sourceFingerprint, status: "pending", stage: "map", executionOrigin: "manual",
      recoveryMode: "full-originals-v1", recoveryGeneration: "full-originals-v2", recoveryOfJobId: oldJob.id });
    const callsBeforeLegacyReplay = h.calls;
    const messagesBeforeLegacyReplay = h.messages.length;
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: legacyContinuation,
    })).toEqual(legacyResponse);
    expect(h.calls).toBe(callsBeforeLegacyReplay);
    expect(h.messages).toHaveLength(messagesBeforeLegacyReplay);
    expect(await h.kv.get(KV.summaryQueueJobs, legacyJobId)).toEqual(legacyChild);
    expect(await h.kv.get(KV.summaryQueueUnits(legacyJobId), legacyUnit.id)).toEqual(legacyUnit);
    expect(await h.kv.get(KV.state, `manual-summary-recovery:${legacyOperationId}`))
      .toEqual({ identity: legacyIdentity, response: legacyResponse });
    expect(await h.kv.get(KV.state, `${oldJob.id}:source-root`)).toEqual(sourceRoot);
    expect(await h.kv.get(KV.state, `${oldJob.id}:source-page:0`)).toEqual(sourcePage);
    const regeneratedUnits = await h.kv.list<{ items: SummaryPromptItem[] }>(KV.summaryQueueUnits(recoveredJobId));
    expect(regeneratedUnits).toHaveLength(1);
    expect(regeneratedUnits.flatMap(unit => unit.items).map(item => item.text)).toEqual([""]);
    expect(h.calls).toBe(0);
    expect(await h.kv.get(KV.summaryQueueJobs, oldJob.id)).toEqual(oldJob);
    expect(await h.kv.get(KV.summaryQueueUnits(oldJob.id), staleCandidate.id)).toEqual(staleCandidate);
    expect(await h.kv.get(KV.state, markerKey)).toEqual(failedMarker);
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation,
    })).toEqual(resumed);
    const followup = resumed.continuation as ManualSummaryContinuation;
    const completed = await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: followup,
    }) as Record<string, unknown>;
    expect(completed).toMatchObject({ success: true, completed: true });
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: followup,
    })).toEqual(completed);
    expect(h.calls).toBe(1);
    expect(h.judges).toBe(1);
    expect(prompts).toHaveLength(1);
    expect(prompts[0]).toContain(source.narrative);
    expect(prompts[0]).not.toContain("STALE BAD CANDIDATE");
    expect(prompts[0]).not.toContain("src/invented.ts");
    expect(await h.kv.get(KV.summaryQueueJobs, recoveredJobId)).toMatchObject({ status: "completed",
      recoveryMode: "full-originals-v1", recoveryOfJobId: oldJob.id });
  });

  it("rejects recovery without a complete failed-job source root before writing recovery state", async () => {
    const h = harness();
    const source = observation(0, "Original source remains available but the failed job has no captured evidence root.");
    await seed(h.kv, [source]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const oldJob = {
      id: "failed-publication-without-root", sessionId: "session", project: "project", snapshotFingerprint: sourceFingerprint,
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["old-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual",
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, oldJob.id, oldJob);
    const markerKey = `session:failed-summary:${sourceFingerprint}`;
    await h.kv.set(KV.state, markerKey, { jobId: oldJob.id, reason: oldJob.failure });
    const retained = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      continuation: ManualSummaryContinuation;
    };
    const beforeJobs = await h.kv.list(KV.summaryQueueJobs);
    const result = await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: retained.continuation,
    });
    const recoveryJobId = fingerprintId("sqj", retained.continuation.operationId);
    expect(result).toMatchObject({ success: false, error: "summary_recovery_evidence_incomplete" });
    expect(await h.kv.list(KV.summaryQueueJobs)).toEqual(beforeJobs);
    expect(await h.kv.get(KV.state, `${oldJob.id}:source-root`)).toBeNull();
    expect(await h.kv.get(KV.state, `${oldJob.id}:source-page:0`)).toBeNull();
    expect(await h.kv.get(KV.state, `${recoveryJobId}:manual`)).toBeNull();
    expect(await h.kv.get(KV.state, `manual-summary-recovery:${retained.continuation.operationId}`)).toBeNull();
    expect(await h.kv.get(KV.state, markerKey)).toEqual({ jobId: oldJob.id, reason: oldJob.failure });
    expect(h.calls).toBe(0);
    expect(h.judges).toBe(0);
    expect(h.messages).toHaveLength(0);
  });

  it("rejects invalid generations, active recovery conflicts, and changed current source before new work", async () => {
    const h = harness();
    const original = observation(0, "The original source is still available for full recovery.");
    await seed(h.kv, [original]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const failedJob = {
      id: "failed-publication-source-change", sessionId: "session", project: "project",
      snapshotFingerprint: sourceFingerprint, observationCount: 1,
      config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["old-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual",
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, failedJob.id, failedJob);
    await captureSummaryEvidence(h.kv as never, failedJob, [original]);
    await h.kv.set(KV.state, `session:failed-summary:${sourceFingerprint}`, {
      jobId: failedJob.id, reason: failedJob.failure,
    });

    const retained = await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session",
    }) as { continuation: ManualSummaryContinuation };
    const invalidGeneration = { ...retained.continuation, recoveryGeneration: "full-originals-v3" };
    expect(validManualSummaryContinuation(invalidGeneration)).toBe(false);
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: invalidGeneration,
    })).toMatchObject({ success: false, error: "invalid_manual_summary_request" });

    const activeLegacyChild = { ...failedJob, id: "pending-legacy-recovery", status: "pending" as const,
      recoveryMode: "full-originals-v1" as const, recoveryOfJobId: failedJob.id, dispatchPending: true };
    await h.kv.set(KV.summaryQueueJobs, activeLegacyChild.id, activeLegacyChild);
    await h.kv.set(KV.summaryQueueActive, "session", { jobId: activeLegacyChild.id });
    const recoveryJobId = fingerprintId("sqj", retained.continuation.operationId);
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: retained.continuation,
    })).toMatchObject({ success: false, error: "summary_recovery_conflict" });
    expect(await h.kv.get(KV.summaryQueueJobs, recoveryJobId)).toBeNull();
    expect(await h.kv.get(KV.summaryQueueActive, "session")).toEqual({ jobId: activeLegacyChild.id });
    await h.kv.delete(KV.summaryQueueActive, "session");
    await h.kv.delete(KV.summaryQueueJobs, activeLegacyChild.id);

    await h.kv.set(KV.observations("session"), original.id, { ...original, narrative: "Changed after the failed source." });
    const result = await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: retained.continuation,
    });
    expect(result).toMatchObject({ success: false, error: "summary_source_changed" });
    expect(await h.kv.list(KV.summaryQueueJobs)).toEqual([failedJob]);
    expect(await h.kv.get(KV.state, `${failedJob.id}:source-root`)).not.toBeNull();
    expect(await h.kv.get(KV.state, `manual-summary-recovery:${retained.continuation.operationId}`)).toBeNull();
    expect(h.calls).toBe(0);
    expect(h.judges).toBe(0);
    expect(h.messages).toHaveLength(0);
  });

  it("drains only an exhausted one-parent v1 child, preserves its evidence and replays both cached cuts", async () => {
    const h = harness();
    const original = observation(0, "The original source remains available for exact recovery.");
    await seed(h.kv, [original]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const source = {
      id: "failed-publication-drain", sessionId: "session", project: "project", snapshotFingerprint: sourceFingerprint,
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["source-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual",
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, source.id, source);
    await captureSummaryEvidence(h.kv as never, source, [original]);

    const legacyOperationId = fingerprintId("summary-recovery", JSON.stringify([SUMMARY_RECOVERY_FIDELITY_VERSION,
      "full-originals-v1", source.id, source.project, source.sessionId, sourceFingerprint]));
    const legacyJobId = fingerprintId("sqj", legacyOperationId);
    const legacyUnitId = "legacy-recovery-unit";
    const legacyChild = { ...source, id: legacyJobId, status: "pending" as const, failure: undefined,
      stage: "map" as const, round: 0, unitIds: [legacyUnitId], recoveryMode: "full-originals-v1" as const,
      recoveryOfJobId: source.id, dispatchPending: true };
    const legacyContinuation: ManualSummaryContinuation = { operationId: `${legacyJobId}:manual`, step: 0,
      jobId: legacyJobId, project: "project", sessionId: "session", sourceFingerprint, round: 0,
      unitId: legacyUnitId, deliveryId: "legacy-delivery" };
    const legacyUnit = { id: legacyUnitId, jobId: legacyJobId, stage: "map" as const, round: 0,
      items: [{ text: "", obsRangeStart: 1, obsRangeEnd: 1 }], attempts: 1, deliveryId: "legacy-delivery" };
    const legacyOperation = { id: `${legacyJobId}:manual`, sessionId: "session", jobId: legacyJobId,
      project: "project", sourceFingerprint, step: 1, maxRequests: 1,
      deadline: Date.now() + 60_000, continuation: legacyContinuation };
    const oldCutResponse = { success: false, completed: false, error: "timeout" };
    const oldRecoveryResponse = { success: true, queued: true, completed: false, jobId: legacyJobId,
      regeneratedFrom: source.id, continuation: legacyContinuation };
    const oldRecoveryIdentity = JSON.stringify([legacyOperationId, 0, source.id, source.project, source.sessionId,
      sourceFingerprint, source.round, source.unitIds[0], legacyOperationId]);
    const markerKey = `session:failed-summary:${sourceFingerprint}`;
    const rawKey = `${legacyJobId}:raw:${legacyUnitId}`;
    const rawIntent = { state: "intended" as const };
    const [sourceRoot, sourcePage] = await Promise.all([
      h.kv.get(KV.state, `${source.id}:source-root`), h.kv.get(KV.state, `${source.id}:source-page:0`),
    ]);
    await h.kv.set(KV.summaryQueueJobs, legacyJobId, legacyChild);
    await h.kv.set(KV.summaryQueueUnits(legacyJobId), legacyUnitId, legacyUnit);
    await h.kv.set(KV.summaryQueueActive, "session", { jobId: legacyJobId });
    await h.kv.set(KV.state, `${legacyJobId}:manual`, legacyOperation);
    await h.kv.set(KV.state, rawKey, rawIntent);
    await h.kv.set(KV.state, `${legacyOperation.id}:request:0`, { request: legacyContinuation, response: oldCutResponse });
    await h.kv.set(KV.state, `manual-summary-recovery:${legacyOperationId}`, {
      identity: oldRecoveryIdentity, response: oldRecoveryResponse,
    });
    await h.kv.set(KV.state, markerKey, { jobId: source.id, reason: source.failure });

    const retained = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      continuation: ManualSummaryContinuation;
    };
    expect(retained.continuation).toMatchObject({ jobId: source.id, recoveryGeneration: "full-originals-v2" });
    const recoveryRequest = { action: "manual", sessionId: "session", continuation: retained.continuation };
    const [first, duplicate] = await Promise.all([
      h.invoke("mem::summary-enqueue", recoveryRequest), h.invoke("mem::summary-enqueue", recoveryRequest),
    ]);
    expect(duplicate).toEqual(first);
    expect(first).toMatchObject({ success: true, queued: true, regeneratedFrom: source.id });

    const recoveredJobId = fingerprintId("sqj", retained.continuation.operationId);
    const recoveredJob = await h.kv.get<SummaryQueueJob & { recoveryGeneration?: string }>(KV.summaryQueueJobs, recoveredJobId);
    expect(recoveredJob).toMatchObject({ status: "pending", recoveryGeneration: "full-originals-v2",
      recoveryOfJobId: source.id, executionOrigin: "manual" });
    expect(await h.kv.get(KV.summaryQueueJobs, legacyJobId)).toMatchObject({ status: "failed",
      failure: "manual_summary_limit_exceeded" });
    expect(await h.kv.get(KV.summaryQueueActive, "session")).toEqual({ jobId: recoveredJobId });
    expect(await h.kv.get(KV.state, markerKey)).toEqual({ jobId: legacyJobId, reason: "manual_summary_limit_exceeded" });
    expect(await h.kv.get(KV.state, rawKey)).toEqual(rawIntent);
    expect(await h.kv.get(KV.summaryQueueUnits(legacyJobId), legacyUnitId)).toEqual(legacyUnit);
    expect(await h.kv.get(KV.state, `${legacyJobId}:manual`)).toEqual(legacyOperation);
    expect(await h.kv.get(KV.state, `${legacyOperation.id}:request:0`))
      .toEqual({ request: legacyContinuation, response: oldCutResponse });
    expect(await h.kv.get(KV.state, `manual-summary-recovery:${legacyOperationId}`))
      .toEqual({ identity: oldRecoveryIdentity, response: oldRecoveryResponse });
    expect(await h.kv.get(KV.state, `${source.id}:source-root`)).toEqual(sourceRoot);
    expect(await h.kv.get(KV.state, `${source.id}:source-page:0`)).toEqual(sourcePage);

    expect(await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }))
      .toMatchObject({ retained: true, continuation: retained.continuation });
    expect(await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session", continuation: legacyContinuation }))
      .toEqual(oldCutResponse);
    expect(await h.invoke("mem::summary-enqueue", recoveryRequest)).toEqual(first);
    expect(h.calls).toBe(0);
    expect(h.judges).toBe(0);
  });

  it("does not drain a pending v2 judge merely because its reserved-call count is full", async () => {
    const h = harness();
    const original = observation(0, "The original source remains available for exact recovery.");
    await seed(h.kv, [original]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const source = {
      id: "failed-publication-live-v2", sessionId: "session", project: "project", snapshotFingerprint: sourceFingerprint,
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["source-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual",
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, source.id, source);
    await captureSummaryEvidence(h.kv as never, source, [original]);
    await h.kv.set(KV.state, `session:failed-summary:${sourceFingerprint}`, {
      jobId: source.id, reason: source.failure,
    });
    const rootRequestId = fingerprintId("summary-recovery", JSON.stringify([SUMMARY_RECOVERY_FIDELITY_VERSION,
      "full-originals-v2", source.id, source.project, source.sessionId, sourceFingerprint]));
    const recoveryJobId = fingerprintId("sqj", rootRequestId);
    const unitId = "live-v2-unit";
    const childContinuation: ManualSummaryContinuation = { operationId: `${recoveryJobId}:manual`, step: 1,
      jobId: recoveryJobId, project: "project", sessionId: "session", sourceFingerprint, round: 0,
      unitId, deliveryId: "live-v2-delivery" };
    const child = { ...source, id: recoveryJobId, status: "pending" as const, failure: undefined,
      stage: "map" as const, round: 0, unitIds: [unitId], recoveryMode: "full-originals-v1" as const,
      recoveryGeneration: "full-originals-v2" as const, recoveryOfJobId: source.id, dispatchPending: true };
    await h.kv.set(KV.summaryQueueJobs, recoveryJobId, child);
    await h.kv.set(KV.summaryQueueUnits(recoveryJobId), unitId, { id: unitId, jobId: recoveryJobId,
      stage: "map", round: 0, items: [{ text: "", obsRangeStart: 1, obsRangeEnd: 1 }], attempts: 0,
      deliveryId: childContinuation.deliveryId });
    await h.kv.set(KV.summaryQueueActive, "session", { jobId: recoveryJobId });
    await h.kv.set(KV.state, `${recoveryJobId}:manual`, { id: `${recoveryJobId}:manual`, sessionId: "session",
      jobId: recoveryJobId, project: "project", sourceFingerprint, step: 1,
      maxRequests: 2, deadline: Date.now() + 60_000, continuation: childContinuation });

    const retained = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      continuation: ManualSummaryContinuation;
    };
    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation: retained.continuation,
    })).toMatchObject({ success: true, queued: true, continuation: childContinuation });
    expect(await h.kv.get(KV.summaryQueueJobs, recoveryJobId)).toMatchObject({ status: "pending" });
    expect(await h.kv.get(KV.summaryQueueActive, "session")).toEqual({ jobId: recoveryJobId });
    expect(h.calls).toBe(0);
    expect(h.judges).toBe(0);
  });

  it("retains the active v1 child and all evidence when a newer canonical value conflicts with its receipt", async () => {
    const h = harness();
    const original = observation(0, "The original source remains available for exact recovery.");
    await seed(h.kv, [original]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const source = {
      id: "failed-publication-conflict", sessionId: "session", project: "project", snapshotFingerprint: sourceFingerprint,
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["source-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual",
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, source.id, source);
    await captureSummaryEvidence(h.kv as never, source, [original]);
    const markerKey = `session:failed-summary:${sourceFingerprint}`;
    await h.kv.set(KV.state, markerKey, { jobId: source.id, reason: source.failure });
    const recoveryRequestId = fingerprintId("summary-recovery", JSON.stringify([SUMMARY_RECOVERY_FIDELITY_VERSION,
      "full-originals-v1", source.id, source.project, source.sessionId, sourceFingerprint]));
    const childId = fingerprintId("sqj", recoveryRequestId);
    const unitId = "conflicting-legacy-unit";
    const continuation: ManualSummaryContinuation = { operationId: `${childId}:manual`, step: 0,
      jobId: childId, project: "project", sessionId: "session", sourceFingerprint, round: 0,
      unitId, deliveryId: "legacy-delivery" };
    const child = { ...source, id: childId, status: "pending" as const, failure: undefined,
      stage: "map" as const, round: 0, unitIds: [unitId], recoveryMode: "full-originals-v1" as const,
      recoveryOfJobId: source.id, dispatchPending: true };
    const unit = { id: unitId, jobId: childId, stage: "map" as const, round: 0,
      items: [{ text: "", obsRangeStart: 1, obsRangeEnd: 1 }], attempts: 1, deliveryId: continuation.deliveryId };
    const operation = { id: continuation.operationId, sessionId: "session", jobId: childId,
      project: "project", sourceFingerprint, step: 1, maxRequests: 1,
      deadline: Date.now() + 60_000, continuation };
    const oldCutResponse = { success: false, completed: false, error: "timeout" };
    const rawKey = `${childId}:raw:${unitId}`;
    const rawCapture = { state: "captured" as const,
      raw: "<summary><title>Recovered</title><narrative>Captured response remains available.</narrative></summary>" };
    const [sourceRoot, sourcePage] = await Promise.all([
      h.kv.get(KV.state, `${source.id}:source-root`), h.kv.get(KV.state, `${source.id}:source-page:0`),
    ]);
    const effectSummary: SessionSummary = { sessionId: "session", project: "project",
      createdAt: "2026-10-11T00:00:00.000Z", title: "Effect summary",
      narrative: "This is the already committed publication effect.", keyDecisions: [], filesModified: [],
      concepts: [], observationCount: 1 };
    const unrelatedCanonical: SessionSummary = { ...effectSummary, title: "Newer unrelated canonical" };
    const receipt = { summary: effectSummary, completedAt: effectSummary.createdAt, method: "llm" as const,
      previousCanonicalIdentity: null };
    await h.kv.set(KV.summaryQueueJobs, childId, child);
    await h.kv.set(KV.summaryQueueUnits(childId), unitId, unit);
    await h.kv.set(KV.summaryQueueActive, "session", { jobId: childId });
    await h.kv.set(KV.state, operation.id, operation);
    await h.kv.set(KV.state, `${operation.id}:request:0`, { request: continuation, response: oldCutResponse });
    await h.kv.set(KV.state, rawKey, rawCapture);
    await h.kv.set(KV.state, `${childId}:publication-effect`, receipt);
    await h.kv.set(KV.summaries, "session", unrelatedCanonical);
    const beforeChild = await h.kv.get(KV.summaryQueueJobs, childId);
    const beforeUnit = await h.kv.get(KV.summaryQueueUnits(childId), unitId);

    const retained = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      continuation: ManualSummaryContinuation;
    };
    const request = { action: "manual", sessionId: "session", continuation: retained.continuation };
    expect(await h.invoke("mem::summary-enqueue", request)).toMatchObject({
      success: false, error: "summary_recovery_conflict",
    });
    expect(await h.kv.get(KV.summaryQueueJobs, childId)).toEqual(beforeChild);
    expect(await h.kv.get(KV.summaryQueueUnits(childId), unitId)).toEqual(beforeUnit);
    expect(await h.kv.get(KV.summaryQueueActive, "session")).toEqual({ jobId: childId });
    expect(await h.kv.get(KV.state, markerKey)).toEqual({ jobId: source.id, reason: source.failure });
    expect(await h.kv.get(KV.state, rawKey)).toEqual(rawCapture);
    expect(await h.kv.get(KV.state, `${childId}:publication-effect`)).toEqual(receipt);
    expect(await h.kv.get(KV.summaries, "session")).toEqual(unrelatedCanonical);
    expect(await h.kv.get(KV.state, `${source.id}:source-root`)).toEqual(sourceRoot);
    expect(await h.kv.get(KV.state, `${source.id}:source-page:0`)).toEqual(sourcePage);
    expect(await h.kv.get(KV.summaryQueueJobs, fingerprintId("sqj", retained.continuation.operationId))).toBeNull();
    expect(await h.kv.get(KV.state, `manual-summary-recovery:${retained.continuation.operationId}`)).toBeNull();
    expect(await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session", continuation }))
      .toEqual(oldCutResponse);
    expect(h.calls).toBe(0);
    expect(h.judges).toBe(0);
  });

  it("does not accept a completed recovery whose publication receipt differs from the canonical summary", async () => {
    const h = harness();
    await seed(h.kv, [observation(0, "The original source remains available.")]);
    const sourceFingerprint = await summarySourceFingerprint(h.kv as never, "session", "project");
    const oldJob = {
      id: "failed-publication", sessionId: "session", project: "project", snapshotFingerprint: sourceFingerprint,
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
      status: "failed", stage: "reduce", round: 1, unitIds: ["old-final-unit"], sourceProgressSize: 100,
      failure: "summary_publication_invalid_response", executionOrigin: "manual",
    } as SummaryQueueJob & { failure: string; executionOrigin: "manual" };
    await h.kv.set(KV.summaryQueueJobs, oldJob.id, oldJob);
    await h.kv.set(KV.state, `session:failed-summary:${sourceFingerprint}`, {
      jobId: oldJob.id, reason: oldJob.failure,
    });
    const retained = await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }) as {
      continuation: ManualSummaryContinuation;
    };
    const continuation = retained.continuation;
    const recoveredJobId = fingerprintId("sqj", continuation.operationId);
    const completedAt = "2026-10-11T00:00:00.000Z";
    const receiptSummary: SessionSummary = {
      sessionId: "session", project: "project", createdAt: completedAt, title: "Receipt summary",
      narrative: "Supported receipt narrative.", keyDecisions: [], filesModified: [], concepts: [], observationCount: 1,
    };
    const canonicalSummary = { ...receiptSummary, title: "Different canonical summary" };
    await h.kv.set(KV.summaryQueueJobs, recoveredJobId, {
      ...oldJob, id: recoveredJobId, status: "completed", completedAt, executionOrigin: "manual",
    } as SummaryQueueJob & { executionOrigin: "manual" });
    await h.kv.set(KV.summaries, "session", canonicalSummary);
    await h.kv.set(KV.state, `${recoveredJobId}:publication-effect`, { summary: receiptSummary, completedAt, method: "llm" });
    await h.kv.set(KV.summaryQueueJobs, recoveredJobId, {
      ...(await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, recoveredJobId)),
      recoveryMode: "full-originals-v1", recoveryGeneration: "full-originals-v2", recoveryOfJobId: oldJob.id,
    });

    expect(await h.invoke("mem::summary-enqueue", {
      action: "manual", sessionId: "session", continuation,
    })).toMatchObject({ success: false, error: "summary_publication_effect_mismatch" });
    expect(h.calls).toBe(0);
    expect(h.messages).toHaveLength(0);
  });

  it("rejects a normal manual source scan when its observation scope changes", async () => {
    const h = harness();
    h.kv.pageControl.bounded = true;
    await seed(h.kv, Array.from({ length: 14 }, (_, index) => observation(index)));
    h.kv.pageControl.afterPage = async (page, scope) => {
      if (page === 1) {
        const current = await h.kv.get<CompressedObservation>(scope, "obs-13");
        await h.kv.set(scope, "obs-13", { ...current!, narrative: "changed during page capture" });
      }
    };
    expect(await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }))
      .toMatchObject({ success: false, error: "summary_source_changed" });
    expect(await h.kv.list(KV.summaryQueueJobs)).toHaveLength(0);
    expect(h.calls).toBe(0);
  });

  it("returns an explicit unavailable error when a later source page fails", async () => {
    const h = harness();
    h.kv.pageControl.bounded = true;
    await seed(h.kv, Array.from({ length: 14 }, (_, index) => observation(index)));
    h.kv.pageControl.failOnPage = 2;
    expect(await h.invoke("mem::summary-enqueue", { action: "manual", sessionId: "session" }))
      .toMatchObject({ success: false, error: "summary_source_grounding_snapshot_unavailable" });
    expect(await h.kv.list(KV.summaryQueueJobs)).toHaveLength(0);
    expect(h.calls).toBe(0);
  });

  it("validates a large session once per generated unit with durable lineage and no duplicate root judge", async () => {
    const h = harness(); semantic.real = true;
    await seed(h.kv, Array.from({ length: 129 }, (_, index) => observation(index, "source")));
    const admitted = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    await h.drain();
    expect(await h.kv.get(KV.summaryQueueJobs, admitted.jobId)).toMatchObject({ status: "completed" });
    expect(h.judges).toBe(h.calls);
    expect(h.judges).toBeGreaterThan(1);
    const receipts = await h.kv.list<Record<string, unknown>>(KV.state);
    expect(receipts.filter(row => typeof row.requestId === "string" && "outputHash" in row)).toHaveLength(h.calls);
    const calls = [h.calls, h.judges];
    await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    expect([h.calls, h.judges]).toEqual(calls);
    expect(h.judgeInputs.every(prompt => Buffer.byteLength(prompt) <= 262144)).toBe(true);
  });

  it("uses one final semantic call for a small multistage session", async () => {
    const h = harness(); semantic.real = true;
    await seed(h.kv, Array.from({ length: 4 }, (_, index) => observation(index, "details ".repeat(110))));
    await h.invoke("mem::summary-enqueue", { sessionId: "session" }); await h.drain();
    expect(h.calls).toBeGreaterThan(1); expect(h.judges).toBe(1);
  });
  it("validates generated outputs for an oversized original split into exact contiguous fragments", async () => {
    const h = harness(); semantic.real = true;
    await seed(h.kv, [observation(1, "repeated source detail ".repeat(3300))]);
    const admitted = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    expect(await h.kv.get(KV.state, `${admitted.jobId}:source-root`)).toMatchObject({ mode: "hierarchical" });
    const initial = await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, admitted.jobId);
    const fragments: { fragmentStart: number; fragmentEnd: number }[] = [];
    for (const unitId of initial!.unitIds) {
      const lineage = await h.kv.get<{ sources: { content: string }[] }>(KV.state, `${admitted.jobId}:lineage:${unitId}`);
      fragments.push(...lineage!.sources.map(source => JSON.parse(source.content).provenance));
    }
    expect(fragments[0].fragmentStart).toBe(0);
    for (let index = 1; index < fragments.length; index++) expect(fragments[index].fragmentStart).toBe(fragments[index - 1].fragmentEnd);
    await h.drain();
    expect(await h.kv.get(KV.summaryQueueJobs, admitted.jobId)).toMatchObject({ status: "completed" });
    expect(h.judges).toBe(h.calls);
  });
  it("retains a rejected large-session output and never regenerates the same snapshot", async () => {
    const h = harness(); semantic.real = true; semantic.verdict = "unsupported";
    await seed(h.kv, Array.from({ length: 129 }, (_, index) => observation(index)));
    const admitted = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    await h.drain();
    expect(await h.kv.get(KV.summaryQueueJobs, admitted.jobId)).toMatchObject({ status: "failed", failure: "summary_publication_unsupported" });
    expect(await h.invoke("mem::summary-enqueue", { sessionId: "session" })).toMatchObject({ retained: true });
    await h.invoke("mem::summary-recover");
    expect(h.calls).toBe(1); expect(h.judges).toBe(1);
    expect(await h.kv.get(KV.summaries, "session")).toBeNull();
  });
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

  it("keeps a successor active when the old completion release races its enqueue", async () => {
    const h = harness();
    await seed(h.kv, [observation(0)]);
    const first = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };

    let resumeRelease!: () => void;
    const releaseGate = new Promise<void>(resolve => { resumeRelease = resolve; });
    let markReleasePaused!: () => void;
    const releasePaused = new Promise<void>(resolve => { markReleasePaused = resolve; });
    let releaseIsPaused = false;
    let pauseRelease = true;
    let successorStarted = false;
    let successorJobReads = 0;
    let markStaleCheckReached!: () => void;
    const staleCheckReached = new Promise<void>(resolve => { markStaleCheckReached = resolve; });
    let successorActiveSet = false;

    const originalGet = h.kv.get.bind(h.kv);
    h.kv.get = async <T>(scope: string, key: string): Promise<T | null> => {
      const value = await originalGet<T>(scope, key);
      if (pauseRelease && scope === KV.summaryQueueActive && key === "session" &&
          (value as { jobId?: string } | null)?.jobId === first.jobId) {
        const job = await originalGet<SummaryQueueJob>(KV.summaryQueueJobs, first.jobId);
        if (job?.status === "completed") {
          pauseRelease = false;
          releaseIsPaused = true;
          markReleasePaused();
          await releaseGate;
        }
      }
      if (successorStarted && scope === KV.summaryQueueJobs && key === first.jobId &&
          (value as SummaryQueueJob | null)?.status === "completed" && ++successorJobReads === 2) {
        markStaleCheckReached();
      }
      return value;
    };
    const originalSet = h.kv.set.bind(h.kv);
    h.kv.set = async <T>(scope: string, key: string, value: T): Promise<T> => {
      const stored = await originalSet(scope, key, value);
      if (successorStarted && scope === KV.summaryQueueActive && key === "session" &&
          (value as { jobId?: string } | null)?.jobId !== first.jobId) successorActiveSet = true;
      return stored;
    };

    let oldCompletion: Promise<unknown> | undefined;
    while (!releaseIsPaused) {
      const message = h.messages.shift();
      if (!message) throw new Error("summary completion did not reach its active release");
      const delivery = h.invoke(message.function_id, message.payload);
      const outcome = await Promise.race([
        delivery.then(() => false),
        releasePaused.then(() => true),
      ]);
      if (outcome) oldCompletion = delivery;
    }

    const successorObservation = observation(2);
    await h.kv.set(KV.observations("session"), successorObservation.id, successorObservation);
    successorStarted = true;
    const successorPromise = h.invoke("mem::summary-enqueue", { sessionId: "session" });
    await staleCheckReached;
    await new Promise<void>(resolve => setImmediate(resolve));
    expect(successorActiveSet).toBe(false);

    resumeRelease();
    await oldCompletion;
    const successor = await successorPromise as { success: boolean; queued: boolean; jobId: string };
    expect(successor).toMatchObject({ success: true, queued: true });
    expect(successor.jobId).not.toBe(first.jobId);
    expect(successorActiveSet).toBe(true);
    expect(await h.kv.get(KV.summaryQueueActive, "session")).toEqual({ jobId: successor.jobId });
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
    expect(h.calls).toBe(1);
    expect(await h.kv.get(KV.summaries, "session")).toBeNull();
  });

  it("retains malformed XML without regenerating", async () => {
    const h = harness(undefined, call => call === 1 ? "not XML" : xml());
    await seed(h.kv, [observation(1)]);
    await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const unit = h.messages.shift()!;
    expect(await h.invoke(unit.function_id, unit.payload)).toMatchObject({ success: false, terminal: true });
    expect(h.calls).toBe(1);
    expect(h.messages).toHaveLength(0);
    expect(await h.kv.get(KV.summaries, "session")).toBeNull();
  });

  it("retains a schema-invalid final summary without regenerating", async () => {
    const h = harness(undefined, call => call === 1
      ? xml().replace("Completed the requested work and persisted the resulting summary.", "Short")
      : xml());
    await seed(h.kv, [observation(1)]);
    await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const unit = h.messages.shift()!;
    expect(await h.invoke(unit.function_id, unit.payload)).toMatchObject({ success: false, terminal: true });
    expect(h.calls).toBe(1);
    expect(h.messages).toHaveLength(0);
  });

  it("marks malformed XML terminal after one captured call", async () => {
    const h = harness(undefined, () => "not XML private-token");
    await seed(h.kv, [observation(1)]);
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    const dispatch = h.messages.shift()!;
    await h.invoke(dispatch.function_id, dispatch.payload);
    const unit = h.messages.shift()!;
    expect(await h.invoke(unit.function_id, unit.payload))
      .toMatchObject({ success: false, error: "summary_parse_failed", terminal: true });
    expect(h.calls).toBe(1);
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
    expect(await kv.list(KV.audit)).toHaveLength(0);
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
  it.each([MAX_SUMMARY_DEPTH - 1, MAX_SUMMARY_DEPTH])("bounds an oversized fallback at depth %s and retains its candidate", async depth => {
    const kv = store();
    const first = partialOutput("First", "The first retained output describes its original source work.", 1);
    const decisions = Array.from({ length: 80 }, (_, index) => `decision-${index}-${"detail".repeat(90)}`);
    const last = partialOutput("Last", "The last retained output describes its original source work.", 2, decisions);
    const { jobId, pendingUnitId } = await seedReduceJob(kv, 1, [first], last, depth);
    const scope = KV.summaryQueueUnits(jobId);
    const pending = await kv.get<Record<string, unknown>>(scope, pendingUnitId);
    await kv.set(scope, pendingUnitId, { ...pending, output: last, summary: {
      sessionId: "session", project: "project", createdAt: "2026-09-28T00:00:00.000Z", title: "Last",
      narrative: "The last retained output describes its original source work.", keyDecisions: decisions,
      filesModified: ["src/file.ts"], concepts: ["queue"], observationCount: 1,
    } });
    const h = harness(kv);
    const result = await h.invoke("mem::summary-unit", { jobId, unitId: pendingUnitId });
    expect(await kv.get(KV.state, `${jobId}:fallback:${depth}`)).not.toBeNull();
    expect(await kv.get(KV.summaries, "session")).toBeNull();
    expect(h.calls).toBe(0); expect(h.judges).toBe(0);
    if (depth < MAX_SUMMARY_DEPTH) {
      expect(result).toMatchObject({ completed: false });
      expect(await kv.get(KV.summaryQueueJobs, jobId)).toMatchObject({ status: "pending", round: MAX_SUMMARY_DEPTH });
    } else {
      expect(result).toMatchObject({ terminal: true, error: "summary_publication_evidence_limit_exceeded" });
      expect(await kv.get(KV.summaryQueueJobs, jobId)).toMatchObject({ status: "failed" });
      expect(await kv.get(scope, pendingUnitId)).not.toBeNull();
    }
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

  it("keeps an ambiguous dispatch closed after backoff", async () => {
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
    expect(await h.kv.get(KV.summaries, "session")).toBeNull();
    expect(h.calls).toBe(1);
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
    expect(h.calls).toBe(1);
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

  it("keeps a compact diagnosis and private capture until explicit retention cleanup", async () => {
    const h = harness(undefined, () => "invalid response private-token");
    await seed(h.kv, [observation(1)]);
    const result = await h.invoke("mem::summary-enqueue", { sessionId: "session" }) as { jobId: string };
    await h.drain();
    const job = await h.kv.get<SummaryQueueJob>(KV.summaryQueueJobs, result.jobId);
    expect(job).toMatchObject({ status: "failed", failure: "summary_parse_failed" });
    expect(JSON.stringify(job)).not.toContain("private-token");
    expect(await h.kv.list(KV.summaryQueueUnits(result.jobId))).toHaveLength(1);
    const repeated = await h.invoke("mem::summary-enqueue", { sessionId: "session" });
    expect(repeated).toMatchObject({ retained: true, queued: false });
    expect(h.calls).toBe(1);
    await h.kv.set(KV.summaryQueueJobs, result.jobId, {
      ...job!, failedAt: "2026-01-01T00:00:00Z",
    });
    expect(await h.invoke("mem::summary-recover")).toMatchObject({ cleaned: 1 });
    expect(await h.kv.get(KV.summaryQueueJobs, result.jobId)).toBeNull();
    expect(await h.kv.list(KV.observations("session"))).toHaveLength(1);
  });
});
