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
  return { sdk, kv: mockKV(), triggered };
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
