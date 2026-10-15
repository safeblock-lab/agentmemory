import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../src/state/reranker.js", () => ({ rerank: async (_query: string, candidates: unknown[]) => candidates }));

import { registerContextFunction } from "../src/functions/context.js";
import { selectContextLessons } from "../src/functions/lessons.js";
import { setEmbeddingProvider } from "../src/functions/search.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, EmbeddingProvider, Lesson, Session, SessionSummary } from "../src/types.js";

interface Hit { id: string; sessionId: string; rowVersion: string }

function makeLesson(id: string, project: string | undefined, confidence: number, deleted = false): Lesson {
  return {
    id,
    content: `${id} content`,
    context: "",
    confidence,
    reinforcements: 1,
    source: "manual",
    sourceIds: [],
    project,
    tags: [],
    createdAt: "2026-10-01T00:00:00.000Z",
    updatedAt: "2026-10-01T00:00:00.000Z",
    decayRate: 0.05,
    ...(deleted ? { deleted: true } : {}),
  };
}

function makeSession(id: string, project: string, agentId: string): Session {
  return {
    id,
    project,
    agentId,
    cwd: "/workspace",
    startedAt: "2026-10-01T00:00:00.000Z",
    status: "completed",
    observationCount: 1,
  };
}

function makeObservation(id: string, sessionId: string, narrative: string): CompressedObservation {
  return {
    id,
    sessionId,
    timestamp: "2026-10-01T00:00:00.000Z",
    type: "decision",
    title: `Observation ${id}`,
    facts: [],
    narrative,
    concepts: [],
    files: [],
    importance: 5,
  };
}

function provider(): EmbeddingProvider {
  return {
    name: "context-test",
    dimensions: 2,
    embed: async () => new Float32Array([1, 0]),
    embedBatch: async (texts) => texts.map(() => new Float32Array([1, 0])),
  };
}

function createSdk() {
  const handlers = new Map<string, Function>();
  return {
    registerFunction: (id: string, handler: Function) => handlers.set(id, handler),
    trigger: vi.fn(),
    handler: (id: string) => handlers.get(id)!,
  };
}

function createIndexedKv(
  hits: Hit[],
  sessions: Session[],
  summaries: SessionSummary[],
  observations: CompressedObservation[],
  lessons: Lesson[],
  lessonPages = 1,
  coverage?: { reason: string },
) {
  const sessionsById = new Map(sessions.map((session) => [session.id, session]));
  const summariesById = new Map(summaries.map((summary) => [summary.sessionId, summary]));
  const observationsByKey = new Map(observations.map((observation) => [
    `${KV.observations(observation.sessionId)}\0${observation.id}`,
    observation,
  ]));
  const retrieval = vi.fn(async (payload: Record<string, unknown>) => {
    if (payload.action === "index_status") {
      return {
        version: 1,
        metadata_only: true,
        index_id: "observations",
        capabilities: ["state::semantic_lsh_v1"],
        semantic: [{
          index_id: "observations",
          model: "context-test",
          dimensions: 2,
          generation: "local-v1",
          status: "ready",
          lexical_ready: true,
          source_kind: "agentmemory",
        }],
      };
    }
    if (payload.action === "project_vectors_prepare") {
      throw new Error("context must never prepare vectors");
    }
    if (payload.action === "keyword_search" || payload.action === "semantic_search") {
      return {
        status: coverage ? "partial" : "ready",
        ...(coverage ? { reason: coverage.reason } : {}),
        model: "context-test",
        dimensions: 2,
        generation: "local-v1",
        items: hits.map((hit) => ({
          obsId: hit.id,
          sessionId: hit.sessionId,
          score: 1,
          text: "selected observation",
          source_scope: KV.observations(hit.sessionId),
          source_version: hit.rowVersion,
        })),
        ...(payload.action === "semantic_search" ? { approximate: true, budget_exhausted: false } : {}),
      };
    }
    throw new Error(`Unexpected retrieval action: ${String(payload.action)}`);
  });
  const pages = vi.fn((_scope: string, _options: unknown) => (async function* () {
    for (let page = 0; page < lessonPages; page++) {
      yield { items: lessons, next_cursor: page + 1 < lessonPages || lessonPages === 16 ? `cursor-${page + 1}` : null };
    }
  })());
  const kv = {
    indexedRetrieval: true,
    get: vi.fn(async (scope: string, key: string) => {
      if (scope === KV.config && key === "indexed-corpus") {
        return { status: "ready", model: "context-test", dimensions: 2, generation: "local-v1" };
      }
      if (scope === KV.profiles) return null;
      if (scope === KV.sessions) return sessionsById.get(key) ?? null;
      if (scope === KV.summaries) return summariesById.get(key) ?? null;
      return null;
    }),
    getVersioned: vi.fn(async (scope: string, key: string) => ({
      exists: observationsByKey.has(`${scope}\0${key}`),
      value: observationsByKey.get(`${scope}\0${key}`) ?? null,
      version: hits.find((hit) => hit.id === key)?.rowVersion ?? "1",
    })),
    set: vi.fn(async (_scope: string, _key: string, value: unknown) => value),
    retrieval,
    pages,
    list: vi.fn(async () => { throw new Error("unbounded list forbidden"); }),
  };
  return { kv, pages, retrieval };
}

describe("bounded indexed context sources", () => {
  const originalSlots = process.env["AGENTMEMORY_SLOTS"];

  afterEach(() => {
    setEmbeddingProvider(null);
    if (originalSlots === undefined) delete process.env["AGENTMEMORY_SLOTS"];
    else process.env["AGENTMEMORY_SLOTS"] = originalSlots;
  });

  it("uses project observation candidates and canonical point reads without scope scans", async () => {
    delete process.env["AGENTMEMORY_SLOTS"];
    const hits: Hit[] = [
      { id: "summary-observation", sessionId: "summary-session", rowVersion: "1" },
      { id: "fallback-observation", sessionId: "fallback-session", rowVersion: "2" },
      { id: "foreign-observation", sessionId: "foreign-session", rowVersion: "1" },
      { id: "wrong-agent-observation", sessionId: "wrong-agent-session", rowVersion: "1" },
      { id: "caller-observation", sessionId: "caller", rowVersion: "1" },
    ];
    const summary: SessionSummary = {
      sessionId: "summary-session",
      project: "api",
      createdAt: "2026-10-01T00:00:00.000Z",
      title: "Allowed summary",
      narrative: "allowed summary narrative",
      keyDecisions: ["keep bounded"],
      filesModified: ["src/context.ts"],
      concepts: [],
      observationCount: 1,
    };
    const { kv, pages, retrieval } = createIndexedKv(hits, [
      makeSession("caller", "api", "agent-a"),
      makeSession("summary-session", "api", "agent-a"),
      makeSession("fallback-session", "api", "agent-a"),
      makeSession("foreign-session", "web", "agent-a"),
      makeSession("wrong-agent-session", "api", "agent-b"),
    ], [summary], [
      makeObservation("fallback-observation", "fallback-session", "allowed fallback observation"),
      makeObservation("foreign-observation", "foreign-session", "foreign project narrative"),
      makeObservation("wrong-agent-observation", "wrong-agent-session", "foreign agent narrative"),
      makeObservation("caller-observation", "caller", "caller narrative"),
    ], [
      makeLesson("project-lesson", "api", 0.7),
      makeLesson("global-lesson", undefined, 0.9),
      makeLesson("foreign-lesson", "web", 1),
      makeLesson("deleted-lesson", "api", 1, true),
    ]);
    setEmbeddingProvider(provider());
    const sdk = createSdk();
    registerContextFunction(sdk as never, kv as never, 4000);

    const rawResult = await sdk.handler("mem::context")({ sessionId: "caller", project: "api", agentId: "agent-a" });
    const result = rawResult as { context: string; blocks: number; tokens: number };

    expect(result.context).toContain("allowed summary narrative");
    expect(result.context).toContain("allowed fallback observation");
    expect(result.context).toContain("project-lesson content");
    expect(result.context).toContain("global-lesson content");
    expect(result.context).not.toContain("foreign project narrative");
    expect(result.context).not.toContain("foreign agent narrative");
    expect(result.context).not.toContain("caller narrative");
    expect(result.context).not.toContain("foreign-lesson content");
    expect(result.context).not.toContain("deleted-lesson content");
    expect(result.context.indexOf("project-lesson content")).toBeLessThan(result.context.indexOf("global-lesson content"));
    expect(kv.list).not.toHaveBeenCalled();
    expect(retrieval.mock.calls.map(([payload]) => payload.action)).not.toContain("project_vectors_prepare");
    expect(pages).toHaveBeenCalledWith(KV.lessons, { limit: 12, maxBytes: 16_384 });
    const summaryReads = kv.get.mock.calls.filter(([scope]) => scope === KV.summaries).map(([, id]) => id);
    expect(summaryReads).toHaveLength(2);
    expect(summaryReads).toContain("summary-session");
    expect(summaryReads).toContain("fallback-session");
  });

  it("stops lesson streaming at the page cap and reports incomplete coverage", async () => {
    let yielded = 0;
    const kv = {
      pages: (_scope: string, options: { limit?: number; maxBytes?: number }) => (async function* () {
        for (let page = 0; page < 20; page++) {
          yielded++;
          expect(options).toEqual({ limit: 12, maxBytes: 16_384 });
          yield {
            items: page === 0
              ? [
                  makeLesson("lesson-0-0", "api", 0.5),
                  { ...makeLesson("invalid-lesson", "api", 1), content: 42 } as unknown as Lesson,
                  ...Array.from({ length: 10 }, (_, index) => makeLesson(`lesson-0-${index + 1}`, "api", 0.5)),
                ]
              : Array.from({ length: 12 }, (_, index) => makeLesson(`lesson-${page}-${index}`, "api", 0.5)),
            next_cursor: `cursor-${page + 1}`,
          };
        }
      })(),
      list: vi.fn(async () => { throw new Error("unbounded list forbidden"); }),
    };

    const result = await selectContextLessons(kv as never, "api");

    expect(yielded).toBe(16);
    expect(result.lessons).toHaveLength(10);
    expect(result.lessons.some((lesson) => lesson.id === "invalid-lesson")).toBe(false);
    expect(result.partial).toBe(true);
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("surfaces incomplete coverage when bounded sources yield no context", async () => {
    delete process.env["AGENTMEMORY_SLOTS"];
    const { kv } = createIndexedKv([], [], [], [], [], 16);
    setEmbeddingProvider(provider());
    const sdk = createSdk();
    registerContextFunction(sdk as never, kv as never, 4000);

    const rawResult = await sdk.handler("mem::context")({ sessionId: "caller", project: "api" });
    const result = rawResult as { context: string; partial: boolean };

    expect(result.context).toBe("");
    expect(result.partial).toBe(true);
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("returns available context with truthful partial coverage without preparing vectors", async () => {
    delete process.env["AGENTMEMORY_SLOTS"];
    const { kv, retrieval } = createIndexedKv([{ id: "o", sessionId: "s", rowVersion: "1" }],
      [makeSession("s", "api", "agent-a")], [], [makeObservation("o", "s", "available indexed observation")],
      [], 1, { reason: "project_projection_pending" });
    setEmbeddingProvider(provider());
    const sdk = createSdk();
    registerContextFunction(sdk as never, kv as never, 4000);
    const result = await sdk.handler("mem::context")({ sessionId: "caller", project: "api", agentId: "agent-a" });
    expect(result.context).toContain("available indexed observation");
    expect(result.partial).toBe(true);
    expect(retrieval.mock.calls.map(([payload]) => payload.action)).toEqual([
      "index_status", "keyword_search", "semantic_search",
    ]);
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("reports native incomplete coverage even when it yields no observation hits", async () => {
    delete process.env["AGENTMEMORY_SLOTS"];
    const { kv, retrieval } = createIndexedKv([], [], [], [], [], 1, { reason: "project_index_not_ready" });
    setEmbeddingProvider(provider());
    const sdk = createSdk();
    registerContextFunction(sdk as never, kv as never, 4000);
    const result = await sdk.handler("mem::context")({ sessionId: "caller", project: "api" });
    expect(result.context).toBe("");
    expect(result.partial).toBe(true);
    expect(retrieval.mock.calls.map(([payload]) => payload.action)).not.toContain("project_vectors_prepare");
  });
});
