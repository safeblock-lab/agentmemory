import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/state/reranker.js", () => ({ rerank: async (_query: string, candidates: unknown[]) => candidates }));

import { registerEnrichFunction } from "../src/functions/enrich.js";
import { registerFileIndexFunction } from "../src/functions/file-index.js";
import { setEmbeddingProvider } from "../src/functions/search.js";
import { KV } from "../src/state/schema.js";
import type { CompressedObservation, EmbeddingProvider, Memory, Session } from "../src/types.js";

interface IndexedHit {
  id: string;
  sessionId: string;
  rowVersion?: string;
}

function createSdk() {
  const functions = new Map<string, Function>();
  const overrides = new Map<string, Function>();
  return {
    registerFunction: (id: string, handler: Function) => functions.set(id, handler),
    registerTrigger: () => undefined,
    overrideTrigger: (id: string, handler: Function) => overrides.set(id, handler),
    trigger: async (input: string | { function_id: string; payload: unknown }, data?: unknown) => {
      const id = typeof input === "string" ? input : input.function_id;
      const payload = typeof input === "string" ? data : input.payload;
      const override = overrides.get(id);
      if (override) return override(payload);
      const handler = functions.get(id);
      if (!handler) throw new Error(`No function registered: ${id}`);
      return handler(payload);
    },
    handler: (id: string) => functions.get(id)!,
  };
}

function createIndexedKv(
  hits: IndexedHit[],
  sessions: Session[],
  observations: CompressedObservation[],
  currentVersions: Record<string, string> = {},
) {
  const sessionMap = new Map(sessions.map((session) => [session.id, session]));
  const observationMap = new Map(observations.map((observation) => [
    `${KV.observations(observation.sessionId)}\0${observation.id}`,
    observation,
  ]));
  const retrievalCalls: Array<Record<string, unknown>> = [];
  const kv = {
    indexedRetrieval: true,
    get: vi.fn(async (scope: string, key: string) => {
      if (scope === KV.config && key === "indexed-corpus") {
        return { status: "ready", model: "test-embed", dimensions: 2, generation: "local-v1" };
      }
      if (scope === KV.sessions) return sessionMap.get(key) ?? null;
      return null;
    }),
    getVersioned: vi.fn(async (scope: string, key: string) => {
      const value = observationMap.get(`${scope}\0${key}`) ?? null;
      const hit = hits.find((candidate) => candidate.id === key && scope === KV.observations(candidate.sessionId));
      return {
        exists: value !== null,
        value,
        version: currentVersions[key] ?? hit?.rowVersion ?? "1",
      };
    }),
    list: vi.fn(async () => { throw new Error("unbounded list forbidden"); }),
    set: vi.fn(async (_scope: string, _key: string, value: unknown) => value),
    retrieval: vi.fn(async (payload: Record<string, unknown>) => {
      retrievalCalls.push(payload);
      if (payload.action === "index_status") {
        return {
          version: 1,
          metadata_only: true,
          index_id: "observations",
          capabilities: ["state::semantic_lsh_v1"],
          semantic: [{
            index_id: "observations",
            model: "test-embed",
            dimensions: 2,
            generation: "local-v1",
            status: "ready",
            lexical_ready: true,
            source_kind: "agentmemory",
          }],
        };
      }
      if (payload.action === "project_vectors_prepare") {
        return {
          status: "ready",
          index_id: "observations",
          project: payload.project,
          source_kind: "observation",
          has_more: false,
          coverage_epoch: "1",
          project_generation: "1",
          cursor: "",
          processed: 1,
          session_visits: 1,
          text_bytes: 64,
          reason: null,
        };
      }
      if (payload.action === "keyword_search" || payload.action === "semantic_search") {
        const items = hits.map((hit) => ({
          obsId: hit.id,
          sessionId: hit.sessionId,
          score: 0.5,
          text: "indexed observation text",
          source_scope: KV.observations(hit.sessionId),
          source_version: hit.rowVersion ?? "1",
        }));
        return {
          status: "ready",
          model: "test-embed",
          dimensions: 2,
          generation: "local-v1",
          items,
          ...(payload.action === "semantic_search" ? { approximate: true, budget_exhausted: false } : {}),
        };
      }
      throw new Error(`Unexpected indexed retrieval action: ${String(payload.action)}`);
    }),
    retrievalCalls,
  };
  return kv;
}

function session(id: string, project: string): Session {
  return { id, project, cwd: "/workspace", startedAt: "2026-10-01T00:00:00.000Z", status: "completed", observationCount: 1 };
}

function observation(id: string, sessionId: string, files: string[], narrative = "safe history"): CompressedObservation {
  return {
    id,
    sessionId,
    timestamp: "2026-10-01T00:00:00.000Z",
    type: "file_read",
    title: `History ${id}`,
    facts: [],
    narrative,
    concepts: [],
    files,
    importance: 5,
  };
}

function provider(): EmbeddingProvider {
  return {
    name: "test-embed",
    dimensions: 2,
    embed: async () => new Float32Array([1, 0]),
    embedBatch: async (texts) => texts.map(() => new Float32Array([1, 0])),
  };
}

describe("bounded enrichment context", () => {
  afterEach(() => setEmbeddingProvider(null));

  it("uses indexed candidates and point reads to enforce project, session, version, and exact file scope", async () => {
    const hits: IndexedHit[] = [
      { id: "allowed", sessionId: "session-api", rowVersion: "2" },
      { id: "caller", sessionId: "caller", rowVersion: "2" },
      { id: "foreign", sessionId: "session-web", rowVersion: "2" },
      { id: "near-match", sessionId: "session-api", rowVersion: "2" },
      { id: "stale", sessionId: "session-api", rowVersion: "1" },
    ];
    const kv = createIndexedKv(hits, [session("caller", "api"), session("session-api", "api"), session("session-web", "web")], [
      observation("allowed", "session-api", ["src\\handler.ts"], "Allowed <history>"),
      observation("caller", "caller", ["src/handler.ts"]),
      observation("foreign", "session-web", ["src/handler.ts"]),
      observation("near-match", "session-api", ["src/handler.tsx"]),
      observation("stale", "session-api", ["src/handler.ts"]),
    ], { stale: "2" });
    const sdk = createSdk();
    setEmbeddingProvider(provider());
    registerFileIndexFunction(sdk as never, kv as never);

    const result = await sdk.handler("mem::file-context")({
      sessionId: "caller",
      files: ["./src/handler.ts"],
    }) as { context: string };

    expect(result.context).toContain("Allowed &lt;history&gt;");
    expect(result.context).not.toContain("caller");
    expect(result.context).not.toContain("foreign");
    expect(result.context).not.toContain("near-match");
    expect(result.context).not.toContain("stale");
    expect(kv.retrievalCalls.find((call) => call.action === "keyword_search")?.project).toBe("api");
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("returns empty file context for an empty file request without reading scopes", async () => {
    const kv = createIndexedKv([], [session("caller", "api")], []);
    const sdk = createSdk();
    setEmbeddingProvider(provider());
    registerFileIndexFunction(sdk as never, kv as never);

    const result = await sdk.handler("mem::file-context")({ sessionId: "caller", files: [] }) as { context: string };

    expect(result.context).toBe("");
    expect(kv.retrieval).not.toHaveBeenCalled();
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("keeps indexed observation context when default search cannot cover its global catalog", async () => {
    const hits: IndexedHit[] = [{ id: "relevant", sessionId: "session-api", rowVersion: "2" }];
    const kv = createIndexedKv(hits, [session("caller", "api"), session("session-api", "api")], [
      observation("relevant", "session-api", ["src/handler.ts"], "Canonical relevant implementation detail"),
    ]);
    const sdk = createSdk();
    setEmbeddingProvider(provider());
    registerFileIndexFunction(sdk as never, kv as never);
    registerEnrichFunction(sdk as never, kv as never);
    sdk.overrideTrigger("mem::search", async () => {
      throw new Error("STATE_INDEX_NOT_READY: global catalog is partial");
    });

    const result = await sdk.handler("mem::enrich")({
      sessionId: "caller",
      files: ["src/handler.ts"],
    }) as { context: string };

    expect(result.context).toContain("agentmemory-relevant-context");
    expect(result.context).toContain("Canonical relevant implementation detail");
    expect(result.context).not.toContain("STATE_INDEX_NOT_READY");
    expect(kv.list).not.toHaveBeenCalled();
  });

  it("forwards caller project and reads at most bounded bug candidates by ID", async () => {
    const caller = session("caller", "api");
    const sdk = createSdk();
    const candidateIds = ["bug-newest", "bug-two", "bug-three", "bug-oldest", "bug-fixed", "bug-foreign", "bug-other-file", "pattern", "bug-unscoped"];
    const memories = new Map<string, Memory>([
      ["bug-newest", makeMemory("bug-newest", "api", "2026-10-09T00:00:00.000Z")],
      ["bug-two", makeMemory("bug-two", "api", "2026-10-08T00:00:00.000Z")],
      ["bug-three", makeMemory("bug-three", "api", "2026-10-07T00:00:00.000Z")],
      ["bug-oldest", makeMemory("bug-oldest", "api", "2026-10-01T00:00:00.000Z")],
      ["bug-fixed", makeMemory("bug-fixed", "api", "2026-10-10T00:00:00.000Z", false)],
      ["bug-foreign", makeMemory("bug-foreign", "web", "2026-10-10T00:00:00.000Z")],
      ["bug-other-file", makeMemory("bug-other-file", "api", "2026-10-10T00:00:00.000Z", true, ["src/other.ts"])],
      ["pattern", makeMemory("pattern", "api", "2026-10-10T00:00:00.000Z", true, ["src/handler.ts"], "pattern")],
      ["bug-unscoped", makeMemory("bug-unscoped", undefined, "2026-10-10T12:00:00.000Z")],
    ]);
    const list = vi.fn(async () => { throw new Error("unbounded list forbidden"); });
    const get = vi.fn(async (scope: string, key: string) => {
      if (scope === KV.sessions && key === "caller") return caller;
      if (scope === KV.memories) return memories.get(key) ?? null;
      return null;
    });
    const kv = { get, list, set: vi.fn(async (_scope: string, _key: string, value: unknown) => value) };
    registerEnrichFunction(sdk as never, kv as never);
    let fileContextPayload: Record<string, unknown> = {};
    const searchPayloads: Array<Record<string, unknown>> = [];
    sdk.overrideTrigger("mem::file-context", async (payload: unknown) => {
      fileContextPayload = payload as Record<string, unknown>;
      return { context: "" };
    });
    sdk.overrideTrigger("mem::search", async (payload: unknown) => {
      searchPayloads.push(payload as Record<string, unknown>);
      return { results: candidateIds.map((id) => ({ observation: { id, narrative: "indexed candidate" } })) };
    });

    const result = await sdk.handler("mem::enrich")({
      sessionId: "caller",
      files: ["src/handler.ts"],
    }) as { context: string };

    expect(fileContextPayload.project).toBe("api");
    expect(searchPayloads.some((payload) => payload.project === "api" && payload.limit === 12)).toBe(true);
    expect(searchPayloads.some((payload) => payload.project === undefined && payload.limit === 12)).toBe(true);
    expect(result.context).toContain("bug-unscoped");
    expect(result.context).toContain("bug-newest");
    expect(result.context).toContain("bug-two");
    expect(result.context).not.toContain("bug-three");
    expect(result.context).not.toContain("bug-foreign");
    expect(result.context).not.toContain("bug-fixed");
    expect(result.context).not.toContain("bug-other-file");
    expect(result.context).not.toContain("pattern");
    expect(get.mock.calls.filter(([scope]) => scope === KV.memories)).toHaveLength(candidateIds.length);
    expect(list).not.toHaveBeenCalled();
  });
});

function makeMemory(
  id: string,
  project: string | undefined,
  updatedAt: string,
  isLatest = true,
  files = ["src/handler.ts"],
  type: Memory["type"] = "bug",
): Memory {
  return {
    id,
    createdAt: updatedAt,
    updatedAt,
    type,
    title: id,
    content: `${id} content`,
    concepts: [],
    files,
    sessionIds: ["caller"],
    strength: 5,
    version: 1,
    isLatest,
    ...(project ? { project } : {}),
  };
}
