import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  isTypeSafeObservationTool,
  registerObserveFunction,
} from "../src/functions/observe.js";
import { mockKV } from "./helpers/mocks.js";

type Handler = (payload: unknown) => Promise<unknown>;

function harness() {
  const handlers = new Map<string, Handler>();
  const triggered: Array<{ id: string; payload: unknown }> = [];
  const sdk = {
    registerFunction(idOrOptions: string | { id: string }, handler: Handler) {
      handlers.set(typeof idOrOptions === "string" ? idOrOptions : idOrOptions.id, handler);
    },
    async trigger(idOrInput: string | { function_id: string; payload: unknown }, data?: unknown) {
      const id = typeof idOrInput === "string" ? idOrInput : idOrInput.function_id;
      const payload = typeof idOrInput === "string" ? data : idOrInput.payload;
      triggered.push({ id, payload });
      return handlers.get(id)?.(payload) ?? null;
    },
  };
  const kv = mockKV();
  (kv as any).pages = async function* (scope: string, options: { limit: number; fields?: string[] }) {
    const rows = Array.from(kv.store.get(scope)?.entries() ?? [], ([id, value]) => options.fields?.length ? { id } : value);
    for (let offset = 0; offset < rows.length; offset += options.limit) yield { items: rows.slice(offset, offset + options.limit) };
  };
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
  return { sdk, kv, triggered };
}

function observationPayload() {
  return {
    sessionId: "session-typesafe-compression",
    project: "agentmemory",
    cwd: "D:/agentmemory",
    hookType: "post_tool_use",
    timestamp: new Date().toISOString(),
    data: {
      tool_name: "mcp__codebase_memory_mcp__search_graph",
      tool_input: { query: "compression registration" },
      tool_output: "Symbols found: registerObserveFunction and registerCompressFunction.",
    },
  };
}

beforeEach(() => {
  vi.stubEnv("AGENTMEMORY_AUTO_COMPRESS", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_SCORING_ENABLED", "true");
});

afterEach(() => vi.unstubAllEnvs());

describe("TypeSafe observation compression preflight", () => {
  it("recognizes qualified read tools while excluding mutating and shell tools", () => {
    expect(isTypeSafeObservationTool("Read")).toBe(true);
    expect(isTypeSafeObservationTool("read_mcp_resource")).toBe(true);
    expect(isTypeSafeObservationTool("mcp__codebase_memory_mcp__search_graph")).toBe(true);
    expect(isTypeSafeObservationTool("functions.exec_command")).toBe(false);
    expect(isTypeSafeObservationTool("apply_patch")).toBe(false);
    expect(isTypeSafeObservationTool("memory_delete")).toBe(false);
  });

  it("passes the TypeSafe score into the real compression trigger", async () => {
    const { sdk, kv, triggered } = harness();
    const typeSafe = {
      evaluate: vi.fn(async () => ({
        admission: {
          type: "choice",
          choice: "keep",
          probabilities: { keep: 0.98, discard: 0.02 },
          confidence: 0.96,
        },
        importance: {
          type: "score",
          score: 6,
          legend: {},
          probabilities: {},
          confidence: 0.91,
        },
      })),
    };
    registerObserveFunction(sdk as never, kv as never, undefined, undefined, typeSafe as never);
    sdk.registerFunction("mem::compress", async () => ({ success: true }));

    await sdk.trigger("mem::observe", observationPayload());

    expect(typeSafe.evaluate).toHaveBeenCalledOnce();
    const compression = triggered.find((entry) => entry.id === "mem::compress");
    expect(compression?.payload).toMatchObject({ importanceOverride: 7 });
  });

  it("rejects low-value input before spending on LLM compression", async () => {
    const { sdk, kv, triggered } = harness();
    const typeSafe = {
      evaluate: vi.fn(async () => ({
        admission: {
          type: "choice",
          choice: "discard",
          probabilities: { keep: 0.02, discard: 0.98 },
          confidence: 0.96,
        },
      })),
    };
    registerObserveFunction(sdk as never, kv as never, undefined, undefined, typeSafe as never);
    sdk.registerFunction("mem::compress", async () => ({ success: true }));

    const result = await sdk.trigger("mem::observe", observationPayload());

    expect(result).toMatchObject({ success: true, skipped: true, reason: "TypeSafe admission" });
    expect(triggered.some((entry) => entry.id === "mem::compress")).toBe(false);
  });
});
