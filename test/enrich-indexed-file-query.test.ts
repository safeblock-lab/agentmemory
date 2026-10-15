import { describe, expect, it } from "vitest";
import { registerEnrichFunction } from "../src/functions/enrich.js";
import { KV } from "../src/state/schema.js";
import type { Memory } from "../src/types.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  const memoryReads: string[] = [];
  return {
    memoryReads,
    get: async <T>(scope: string, key: string): Promise<T | null> => {
      if (scope === KV.memories) memoryReads.push(key);
      return (store.get(scope)?.get(key) as T) ?? null;
    },
    set: async <T>(scope: string, key: string, value: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, value);
      return value;
    },
  };
}

function mockSdk() {
  const functions = new Map<string, Function>();
  const overrides = new Map<string, Function>();
  return {
    registerFunction: (id: string, handler: Function) => functions.set(id, handler),
    trigger: async (input: string | { function_id: string; payload: unknown }, data?: unknown) => {
      const id = typeof input === "string" ? input : input.function_id;
      const payload = typeof input === "string" ? data : input.payload;
      const override = overrides.get(id);
      if (override) return override(payload);
      const handler = functions.get(id);
      if (!handler) throw new Error(`No function registered: ${id}`);
      return handler(payload);
    },
    overrideTrigger: (id: string, handler: Function) => overrides.set(id, handler),
  };
}

function makeBugMemory(overrides: Partial<Memory> = {}): Memory {
  return {
    id: "mem_auth_bug",
    createdAt: "2026-10-11T00:00:00.000Z",
    updatedAt: "2026-10-11T00:00:00.000Z",
    type: "bug",
    title: "Authorization <header> mismatch",
    content: "A & B use different auth headers",
    concepts: ["auth"],
    files: ["src/middleware/auth.ts"],
    sessionIds: ["session-auth"],
    strength: 8,
    version: 1,
    isLatest: true,
    ...overrides,
  };
}

async function setupSearch(memory?: Memory) {
  const sdk = mockSdk();
  const kv = mockKV();
  registerEnrichFunction(sdk as never, kv as never);
  sdk.overrideTrigger("mem::file-context", async () => ({ context: "" }));
  if (memory) await kv.set(KV.memories, memory.id, memory);
  const searches: Array<{ query: string; limit: number; project?: string }> = [];
  sdk.overrideTrigger("mem::search", async (raw: unknown) => {
    const payload = raw as { query: string; limit: number; project?: string };
    searches.push(payload);
    const candidate = memory && payload.query.includes("src/middleware/auth.ts")
      ? [{ observation: { id: memory.id, narrative: "" }, sessionId: "session-auth" }]
      : [];
    return { results: payload.project ? [] : candidate };
  });
  return { sdk, kv, searches };
}

describe("mem::enrich indexed file query", () => {
  it("queries the normalized full indexed path and returns the same-file error", async () => {
    const { sdk, searches } = await setupSearch(makeBugMemory({ project: "api" }));
    const result = await sdk.trigger("mem::enrich", {
      sessionId: "session-current",
      project: "api",
      files: [".\\src\\middleware\\auth.ts"],
    }) as { context: string };

    expect(searches.some(({ query }) => query.includes("src/middleware/auth.ts"))).toBe(true);
    expect(searches.every(({ limit }) => limit === 12)).toBe(true);
    expect(result.context).toContain("agentmemory-past-errors");
    expect(result.context).toContain("Authorization &lt;header&gt;");
    expect(result.context).toContain("A &amp; B");
  });

  it("rejects a matching file memory from a different project", async () => {
    const { sdk } = await setupSearch(makeBugMemory({ project: "web" }));
    const result = await sdk.trigger("mem::enrich", {
      sessionId: "session-current",
      project: "api",
      files: ["src/middleware/auth.ts"],
    }) as { context: string };

    expect(result.context).not.toContain("agentmemory-past-errors");
    expect(result.context).not.toContain("Authorization");
  });

  it("keeps projectless legacy bug memories visible for the exact file", async () => {
    const { sdk } = await setupSearch(makeBugMemory({ project: undefined }));
    const result = await sdk.trigger("mem::enrich", {
      sessionId: "session-current",
      files: ["src/middleware/auth.ts"],
    }) as { context: string };

    expect(result.context).toContain("agentmemory-past-errors");
    expect(result.context).toContain("Authorization");
  });

  it("keeps the search and candidate bounds while truncating context at 4000 characters", async () => {
    const { sdk, kv, searches } = await setupSearch();
    const ids = Array.from({ length: 13 }, (_, index) => `mem_bug_${index + 1}`);
    for (const [index, id] of ids.entries()) {
      await kv.set(KV.memories, id, makeBugMemory({
        id,
        title: `Bug ${id}`,
        content: "x".repeat(index < 3 ? 1300 : 1),
        files: ["src/middleware/auth.ts"],
      }));
    }
    sdk.overrideTrigger("mem::search", async (raw: unknown) => {
      searches.push(raw as { query: string; limit: number });
      return { results: ids.map((id) => ({ observation: { id, narrative: "" } })) };
    });

    const result = await sdk.trigger("mem::enrich", {
      sessionId: "session-current",
      files: ["src/middleware/auth.ts"],
      terms: ["x".repeat(300)],
    }) as { context: string; truncated: boolean };

    expect(searches.every(({ query }) => query.length <= 256)).toBe(true);
    expect(searches.every(({ limit }) => limit === 12)).toBe(true);
    expect(kv.memoryReads).toHaveLength(12);
    expect(kv.memoryReads).not.toContain("mem_bug_13");
    expect(result.context).toHaveLength(4000);
    expect(result.truncated).toBe(true);
  });
});
