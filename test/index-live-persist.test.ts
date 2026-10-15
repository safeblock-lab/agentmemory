import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

import { getSearchIndex, setIndexPersistence } from "../src/functions/search.js";
import { registerObserveFunction } from "../src/functions/observe.js";
import { registerRememberFunction } from "../src/functions/remember.js";

function mockKV() {
  const store = new Map<string, Map<string, unknown>>();
  return {
    store,
    get: async <T>(scope: string, key: string): Promise<T | null> =>
      (store.get(scope)?.get(key) as T) ?? null,
    set: async <T>(scope: string, key: string, data: T): Promise<T> => {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, data);
      return data;
    },
    update: async (
      scope: string,
      key: string,
      updates: Array<{ path: string; value: unknown }>,
    ) => {
      const m = store.get(scope);
      if (!m) return;
      const v = (m.get(key) as Record<string, unknown>) ?? {};
      for (const u of updates) v[u.path] = u.value;
      m.set(key, v);
    },
    delete: async (scope: string, key: string) => {
      store.get(scope)?.delete(key);
    },
    list: async <T>(scope: string): Promise<T[]> => {
      const m = store.get(scope);
      return m ? (Array.from(m.values()) as T[]) : [];
    },
    async *pages<T>(scope: string, options: { limit: number; fields?: string[] }) {
      const rows = Array.from(store.get(scope)?.entries() ?? [], ([id, value]) =>
        options.fields?.length ? { id, ...Object.fromEntries(options.fields.filter((field) => field !== "id").map((field) => [field, (value as Record<string, unknown>)[field]])) } : value,
      );
      for (let offset = 0; offset < rows.length; offset += options.limit) {
        yield { items: rows.slice(offset, offset + options.limit) as T[] };
      }
    },
  };
}

function mockSdk() {
  const fns = new Map<string, Function>();
  return {
    registerFunction: (idOrOpts: string | { id: string }, fn: Function) => {
      fns.set(typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id, fn);
    },
    registerTrigger: () => {},
    trigger: async (input: { function_id: string; payload: unknown }) => {
      const fn = fns.get(input.function_id);
      return fn ? fn(input.payload) : null;
    },
  };
}

function registerObservationAdmission(sdk: ReturnType<typeof mockSdk>, kv: ReturnType<typeof mockKV>) {
  sdk.registerFunction("state::observation_admit_v1", async (input: unknown) => {
    const request = input as Record<string, any>;
    const sessionId = request.session_id as string;
    const identityScope = `mem:obs-identity:${sessionId}`;
    const eventScope = `mem:obs-events:${sessionId}`;
    const observationScope = `mem:obs:${sessionId}`;
    const identity = await kv.get<{ observationId: string }>(identityScope, request.identity_digest);
    const observationId = identity?.observationId ?? request.observation.id;
    const current = await kv.get<Record<string, any>>(observationScope, observationId);
    const event = await kv.get(eventScope, request.event_id);
    if (event && current) {
      return { created: false, duplicate_event: true, observation_id: observationId, observation: current, version: "1", occurrence_count: current.occurrenceCount ?? 1 };
    }
    if (identity && current) {
      const count = (current.occurrenceCount ?? 1) + 1;
      const updated = { ...current, occurrenceCount: count, lastSeenAt: request.occurred_at };
      await kv.set(observationScope, observationId, updated);
      await kv.set(eventScope, request.event_id, { observationId, identityDigest: request.identity_digest, occurredAt: request.occurred_at });
      return { created: false, duplicate_event: false, observation_id: observationId, observation: updated, version: String(count), occurrence_count: count };
    }
    const session = await kv.get<Record<string, any>>("mem:sessions", sessionId) ?? request.session;
    if (request.max_observations && session && (session.observationCount ?? 0) >= request.max_observations) {
      throw new Error("STATE_OBSERVATION_LIMIT");
    }
    const observation = { ...request.observation, identityDigest: request.identity_digest, occurrenceCount: 1, firstSeenAt: request.occurred_at, lastSeenAt: request.occurred_at, provenance: [{ ...request.provenance, count: 1, firstSeenAt: request.occurred_at, lastSeenAt: request.occurred_at }] };
    await kv.set(observationScope, observationId, observation);
    await kv.set(identityScope, request.identity_digest, { observationId, identityDigest: request.identity_digest });
    await kv.set(identityScope, "__ready__", { version: 1, ready: true });
    await kv.set(eventScope, request.event_id, { observationId, identityDigest: request.identity_digest, occurredAt: request.occurred_at });
    if (session) await kv.set("mem:sessions", sessionId, { ...session, observationCount: (session.observationCount ?? 0) + 1, updatedAt: request.occurred_at });
    return { created: true, duplicate_event: false, observation_id: observationId, observation, version: "1", occurrence_count: 1 };
  });
}

describe("live index writes schedule the persisted snapshot", () => {
  const persistence = { scheduleSave: vi.fn(), save: vi.fn(async () => {}) };

  beforeEach(() => {
    getSearchIndex().clear();
    persistence.scheduleSave.mockClear();
    setIndexPersistence(persistence);
  });

  afterEach(() => {
    setIndexPersistence(null);
  });

  it("after a synthetic observation", async () => {
    const sdk = mockSdk();
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never);
    registerObservationAdmission(sdk, kv);

    const result = (await sdk.trigger({
      function_id: "mem::observe",
      payload: {
        sessionId: "ses_live",
        project: "agentmemory",
        cwd: "/tmp/agentmemory",
        hookType: "post_tool_use",
        timestamp: new Date().toISOString(),
        data: {
          tool_name: "Bash",
          tool_input: { command: "echo persist-probe" },
          tool_output: "persist-probe output",
        },
      },
    })) as { observationId: string };

    expect(result.observationId).toBeTruthy();
    expect(getSearchIndex().has(result.observationId)).toBe(true);
    expect(persistence.scheduleSave).toHaveBeenCalled();
  });

  it("after a memory save", async () => {
    const sdk = mockSdk();
    const kv = mockKV();
    registerRememberFunction(sdk as never, kv as never);

    const result = (await sdk.trigger({
      function_id: "mem::remember",
      payload: { content: "persisted snapshot follows every live add", type: "fact" },
    })) as { success: boolean; memory: { id: string } };

    expect(result.success).toBe(true);
    expect(getSearchIndex().has(result.memory.id)).toBe(true);
    expect(persistence.scheduleSave).toHaveBeenCalled();
  });
});
