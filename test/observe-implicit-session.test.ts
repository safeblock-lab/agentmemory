import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

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
    update: async (scope: string, key: string, updates: Array<{ path: string; value: unknown }>) => {
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
    fns,
    registerFunction: (
      idOrOpts: string | { id: string },
      fn: Function,
    ) => {
      const id = typeof idOrOpts === "string" ? idOrOpts : idOrOpts.id;
      fns.set(id, fn);
    },
    trigger: async (
      idOrInput: string | { function_id: string; payload: unknown; action?: unknown },
      data?: unknown,
    ) => {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      const fn = fns.get(id);
      if (fn) return fn(payload);
      return null;
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
    if (event && current) return { created: false, duplicate_event: true, observation_id: observationId, observation: current, version: "1", occurrence_count: current.occurrenceCount ?? 1 };
    if (identity && current) {
      const count = (current.occurrenceCount ?? 1) + 1;
      const updated = { ...current, occurrenceCount: count, lastSeenAt: request.occurred_at };
      await kv.set(observationScope, observationId, updated);
      await kv.set(eventScope, request.event_id, { observationId, identityDigest: request.identity_digest, occurredAt: request.occurred_at });
      return { created: false, duplicate_event: false, observation_id: observationId, observation: updated, version: String(count), occurrence_count: count };
    }
    const session = await kv.get<Record<string, any>>("mem:sessions", sessionId) ?? request.session;
    if (request.max_observations && session && (session.observationCount ?? 0) >= request.max_observations) throw new Error("STATE_OBSERVATION_LIMIT");
    const observation = { ...request.observation, identityDigest: request.identity_digest, occurrenceCount: 1, firstSeenAt: request.occurred_at, lastSeenAt: request.occurred_at, provenance: [{ ...request.provenance, count: 1, firstSeenAt: request.occurred_at, lastSeenAt: request.occurred_at }] };
    await kv.set(observationScope, observationId, observation);
    await kv.set(identityScope, request.identity_digest, { observationId, identityDigest: request.identity_digest });
    await kv.set(identityScope, "__ready__", { version: 1, ready: true });
    await kv.set(eventScope, request.event_id, { observationId, identityDigest: request.identity_digest, occurredAt: request.occurred_at });
    if (session) await kv.set("mem:sessions", sessionId, { ...session, observationCount: (session.observationCount ?? 0) + 1, updatedAt: request.occurred_at });
    return { created: true, duplicate_event: false, observation_id: observationId, observation, version: "1", occurrence_count: 1 };
  });
}

describe("observe implicit session create (#638)", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("creates the session on first observe when project+cwd present and session record missing", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const sdk = mockSdk();
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never);
    registerObservationAdmission(sdk, kv);

    const result = (await sdk.trigger("mem::observe", {
      sessionId: "ses_opencode_abc",
      project: "/home/user/myrepo",
      cwd: "/home/user/myrepo",
      hookType: "prompt_submit",
      timestamp: new Date().toISOString(),
      data: { prompt: "ship the helm chart" },
    })) as { observationId: string };

    expect(result.observationId).toBeTruthy();

    const sessionScope = kv.store.get("mem:sessions");
    expect(sessionScope).toBeTruthy();
    const session = sessionScope!.get("ses_opencode_abc") as Record<string, unknown>;
    expect(session).toBeTruthy();
    expect(session.id).toBe("ses_opencode_abc");
    expect(session.project).toBe("/home/user/myrepo");
    expect(session.cwd).toBe("/home/user/myrepo");
    expect(session.status).toBe("active");
    expect(session.observationCount).toBe(1);
    expect(session.firstPrompt).toBe("ship the helm chart");
  });

  it("does not implicit-create when project+cwd missing (test-payload back-compat)", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const sdk = mockSdk();
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never);
    registerObservationAdmission(sdk, kv);

    await sdk.trigger("mem::observe", {
      sessionId: "ses_no_project",
      hookType: "post_tool_use",
      timestamp: new Date().toISOString(),
      data: { tool_name: "Read", tool_input: { file_path: "x.ts" } },
    });

    const sessionScope = kv.store.get("mem:sessions");
    // Either no scope at all, or no entry for this session
    expect(sessionScope?.get("ses_no_project")).toBeUndefined();
  });

  it("does not overwrite an existing session when one already exists", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const sdk = mockSdk();
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never);
    registerObservationAdmission(sdk, kv);

    await kv.set("mem:sessions", "ses_existing", {
      id: "ses_existing",
      project: "/orig/project",
      cwd: "/orig/cwd",
      startedAt: "2026-01-01T00:00:00Z",
      status: "active",
      observationCount: 7,
      firstPrompt: "original first prompt",
    });

    await sdk.trigger("mem::observe", {
      sessionId: "ses_existing",
      project: "/different/project",
      cwd: "/different/cwd",
      hookType: "post_tool_use",
      timestamp: new Date().toISOString(),
      data: { tool_name: "Read" },
    });

    const session = kv.store.get("mem:sessions")!.get("ses_existing") as Record<string, unknown>;
    // Original project + firstPrompt preserved
    expect(session.project).toBe("/orig/project");
    expect(session.firstPrompt).toBe("original first prompt");
    // Counter bumped, updatedAt refreshed
    expect(session.observationCount).toBe(8);
    expect(session.updatedAt).toBeTruthy();
  });
});
