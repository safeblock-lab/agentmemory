import { describe, it, expect, beforeEach, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { registerMcpEndpoints } from "../src/mcp/server.js";
import { SAFE_PAYLOAD_BYTES } from "../src/state/frame-guard.js";
import { KV } from "../src/state/schema.js";
import type { Session, SessionSummary, Memory } from "../src/types.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      return (store.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    delete: async (scope: string, key: string): Promise<void> => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      const entries = store.get(scope);
      return entries ? (Array.from(entries.values()) as T[]) : [];
    },
    pages: async function* <T>(scope: string, options: { cursor?: string; limit?: number; fields?: string[] } = {}) {
      const rows = Array.from(store.get(scope)?.values() ?? []);
      const limit = options.limit ?? 256;
      let start = options.cursor === undefined ? 0 : Number(options.cursor);
      while (start < rows.length || start === 0) {
        const end = Math.min(rows.length, start + limit);
        const items = rows.slice(start, end).map((row) => {
          if (!options.fields) return row as T;
          const source = row && typeof row === "object" ? row as Record<string, unknown> : {};
          return Object.fromEntries(options.fields.flatMap((field) =>
            Object.hasOwn(source, field) ? [[field, source[field]]] : [],
          )) as T;
        });
        const next_cursor = end < rows.length ? String(end) : null;
        yield { items, next_cursor };
        if (next_cursor === null) return;
        start = end;
      }
    },
  };
}

function mockSdk() {
  const functions = new Map<string, Function>();
  const triggerOverrides = new Map<string, Function>();
  return {
    registerFunction: (idOrOpts: string | { id: string }, handler: Function) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      functions.set(id, handler);
    },
    registerTrigger: () => {},
    trigger: async (
      idOrInput: string | { function_id: string; payload: unknown },
      data?: unknown,
    ) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      if (triggerOverrides.has(id)) {
        return triggerOverrides.get(id)!(payload);
      }
      const fn = functions.get(id);
      if (!fn) throw new Error(`No function: ${id}`);
      return fn(payload);
    },
    overrideTrigger: (id: string, handler: Function) => {
      triggerOverrides.set(id, handler);
    },
    getFunction: (id: string) => functions.get(id),
  };
}

function makeReq(body?: unknown, headers?: Record<string, string>) {
  return {
    body,
    headers: headers || {},
    query_params: {},
  };
}

describe("MCP Resources", () => {
  let sdk: ReturnType<typeof mockSdk>;
  let kv: ReturnType<typeof mockKV>;

  beforeEach(() => {
    sdk = mockSdk();
    kv = mockKV();
    registerMcpEndpoints(sdk as never, kv as never);
  });

  it("lists 6 resources", async () => {
    const fn = sdk.getFunction("mcp::resources::list")!;
    const result = (await fn(makeReq())) as {
      status_code: number;
      body: { resources: unknown[] };
    };

    expect(result.status_code).toBe(200);
    expect(result.body.resources).toHaveLength(6);
  });

  it("reads agentmemory://status", async () => {
    const session: Session = {
      id: "ses_1",
      project: "/test",
      cwd: "/test",
      startedAt: new Date().toISOString(),
      status: "active",
      observationCount: 5,
    };
    await kv.set("mem:sessions", "ses_1", session);

    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(makeReq({ uri: "agentmemory://status" }))) as {
      status_code: number;
      body: { contents: Array<{ text: string }> };
    };

    expect(result.status_code).toBe(200);
    const data = JSON.parse(result.body.contents[0].text);
    expect(data.sessionCount).toBe(1);
  });

  it("reads agentmemory://project/{name}/profile", async () => {
    sdk.overrideTrigger("mem::profile", async () => ({
      project: "/myapp",
      topConcepts: [{ concept: "auth", frequency: 5 }],
    }));

    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(
      makeReq({ uri: "agentmemory://project/myapp/profile" }),
    )) as {
      status_code: number;
      body: { contents: Array<{ text: string }> };
    };

    expect(result.status_code).toBe(200);
    const data = JSON.parse(result.body.contents[0].text);
    expect(data.project).toBe("/myapp");
  });

  it("reads agentmemory://project/{name}/recent with sorted summaries", async () => {
    const summaries: SessionSummary[] = [
      {
        sessionId: "ses_1",
        project: "myapp",
        createdAt: "2026-01-01T00:00:00Z",
        title: "Old session",
        narrative: "old",
        keyDecisions: [],
        filesModified: [],
        concepts: [],
        observationCount: 1,
      },
      {
        sessionId: "ses_2",
        project: "myapp",
        createdAt: "2026-02-01T00:00:00Z",
        title: "New session",
        narrative: "new",
        keyDecisions: [],
        filesModified: [],
        concepts: [],
        observationCount: 2,
      },
      {
        sessionId: "ses_3",
        project: "other",
        createdAt: "2026-02-15T00:00:00Z",
        title: "Other project",
        narrative: "other",
        keyDecisions: [],
        filesModified: [],
        concepts: [],
        observationCount: 3,
      },
    ];
    for (const s of summaries) {
      await kv.set("mem:summaries", s.sessionId, s);
    }

    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(
      makeReq({ uri: "agentmemory://project/myapp/recent" }),
    )) as {
      status_code: number;
      body: { contents: Array<{ text: string }> };
    };

    expect(result.status_code).toBe(200);
    const data = JSON.parse(result.body.contents[0].text);
    expect(data).toHaveLength(2);
    expect(data[0].sessionId).toBe("ses_2");
  });

  it("reads agentmemory://memories/latest", async () => {
    const memories: Memory[] = [
      {
        id: "mem_1",
        createdAt: "2026-01-01T00:00:00Z",
        updatedAt: "2026-02-01T00:00:00Z",
        type: "pattern",
        title: "Latest pattern",
        content: "content",
        concepts: [],
        files: [],
        sessionIds: [],
        strength: 5,
        version: 1,
        isLatest: true,
      },
      {
        id: "mem_2",
        createdAt: "2026-01-01T00:00:00Z",
        updatedAt: "2026-01-15T00:00:00Z",
        type: "bug",
        title: "Old bug",
        content: "content",
        concepts: [],
        files: [],
        sessionIds: [],
        strength: 3,
        version: 2,
        isLatest: false,
      },
    ];
    for (const m of memories) {
      await kv.set("mem:memories", m.id, m);
    }

    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(
      makeReq({ uri: "agentmemory://memories/latest" }),
    )) as {
      status_code: number;
      body: { contents: Array<{ text: string }> };
    };

    expect(result.status_code).toBe(200);
    const data = JSON.parse(result.body.contents[0].text);
    expect(data).toHaveLength(1);
    expect(data[0].id).toBe("mem_1");
    expect(data[0].title).toBe("Latest pattern");
  });

  it("counts graph statistics from projected IDs and reports read failures", async () => {
    await kv.set(KV.graphNodes, "large-node", {
      id: "large-node",
      type: "concept",
      name: "Large node",
      properties: { payload: "x".repeat(1_320_550) },
      sourceObservationIds: [],
      createdAt: new Date().toISOString(),
    });
    await kv.set(KV.graphEdges, "edge-a", {
      id: "edge-a",
      type: "related_to",
      sourceNodeId: "large-node",
      targetNodeId: "large-node",
      weight: 1,
      sourceObservationIds: [],
      createdAt: new Date().toISOString(),
    });
    const list = vi.spyOn(kv, "list").mockImplementation(async (scope) => {
      if (scope === KV.graphNodes || scope === KV.graphEdges) throw new Error("STATE_RECORD_TOO_LARGE");
      return [];
    });
    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = await fn(makeReq({ uri: "agentmemory://graph/stats" })) as {
      status_code: number;
      body: { contents: Array<{ text: string }> };
    };
    expect(result.status_code).toBe(200);
    expect(JSON.parse(result.body.contents[0].text)).toMatchObject({
      totalNodes: 1,
      totalEdges: 1,
      nodesByType: { concept: 1 },
      edgesByType: { related_to: 1 },
    });
    expect(list).not.toHaveBeenCalledWith(KV.graphNodes);
    expect(list).not.toHaveBeenCalledWith(KV.graphEdges);

    const unavailable = { pages: async function* () { throw new Error("down"); } };
    const failedSdk = mockSdk();
    registerMcpEndpoints(failedSdk as never, unavailable as never);
    const failed = await failedSdk.getFunction("mcp::resources::read")!(makeReq({ uri: "agentmemory://graph/stats" })) as {
      status_code: number;
      body: { error: string };
    };
    expect(failed.status_code).toBe(500);
    expect(failed.body.error).toContain("unavailable");
  });

  it("forwards bounded export page arguments and rejects page arguments without a collection", async () => {
    let forwarded: unknown;
    sdk.overrideTrigger("mem::export", async (payload: unknown) => {
      forwarded = payload;
      return { version: "0.9.82", sessions: [], observations: {}, memories: [], summaries: [] };
    });
    const fn = sdk.getFunction("mcp::tools::call")!;
    const result = await fn(makeReq({
      name: "memory_export",
      arguments: { collection: "graphNodes", offset: 4, limit: 2 },
    })) as { status_code: number };
    expect(result.status_code).toBe(200);
    expect(forwarded).toEqual({ collection: "graphNodes", offset: 4, limit: 2 });

    const invalid = await fn(makeReq({
      name: "memory_export",
      arguments: { offset: 4 },
    })) as { status_code: number; body: { error: string } };
    expect(invalid.status_code).toBe(400);
    expect(invalid.body.error).toContain("collection is required");

    sdk.overrideTrigger("mem::export", async () => ({ payload: "x".repeat(SAFE_PAYLOAD_BYTES + 1) }));
    const oversized = await fn(makeReq({
      name: "memory_export",
      arguments: { collection: "graphNodes", limit: 1 },
    })) as { status_code: number; body: { content: Array<{ text: string }> } };
    expect(oversized.status_code).toBe(200);
    expect(JSON.parse(oversized.body.content[0].text)).toMatchObject({
      success: false,
      oversized: true,
    });
  });

  it("returns 404 for unknown URI", async () => {
    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(
      makeReq({ uri: "agentmemory://nonexistent" }),
    )) as { status_code: number };

    expect(result.status_code).toBe(404);
  });

  it("returns 401 when auth fails", async () => {
    const authedSdk = mockSdk();
    const authedKv = mockKV();
    registerMcpEndpoints(authedSdk as never, authedKv as never, "test-secret");

    const fn = authedSdk.getFunction("mcp::resources::list")!;
    const result = (await fn(makeReq())) as { status_code: number };
    expect(result.status_code).toBe(401);

    const authedResult = (await fn(
      makeReq(undefined, { authorization: "Bearer test-secret" }),
    )) as { status_code: number };
    expect(authedResult.status_code).toBe(200);
  });

  it("handles URI with special characters via decodeURIComponent", async () => {
    sdk.overrideTrigger("mem::profile", async (data: any) => ({
      project: data.project,
      topConcepts: [],
    }));

    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(
      makeReq({
        uri: "agentmemory://project/my%20app%2Fsubdir/profile",
      }),
    )) as {
      status_code: number;
      body: { contents: Array<{ text: string }> };
    };

    expect(result.status_code).toBe(200);
    const data = JSON.parse(result.body.contents[0].text);
    expect(data.project).toBe("my app/subdir");
  });

  it("returns 400 for malformed percent-encoding in URI", async () => {
    const fn = sdk.getFunction("mcp::resources::read")!;
    const result = (await fn(
      makeReq({
        uri: "agentmemory://project/bad%E0encoding/profile",
      }),
    )) as { status_code: number; body: { error: string } };

    expect(result.status_code).toBe(400);
    expect(result.body.error).toContain("percent-encoding");
  });
});
