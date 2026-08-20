import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";

const AUX_KEYS = [
  "AGENTMEMORY_AUX_LLM_BASE_URL",
  "AGENTMEMORY_AUX_LLM_API_KEY",
  "AGENTMEMORY_AUX_LLM_MODEL",
  "AGENTMEMORY_AUX_LLM_TIMEOUT_MS",
  "AGENTMEMORY_AUX_LLM_MAX_TOKENS",
  "AGENTMEMORY_AUX_LLM_MAX_INPUT_CHARS",
  "AGENTMEMORY_AUX_LLM_REASONING_EFFORT",
  "AGENTMEMORY_AUX_LLM_NOTHINK",
  "AGENTMEMORY_AUX_LLM_KEEP_ALIVE",
] as const;
const ROUTE_KEYS = [
  "AGENTMEMORY_GRAPH_LLM",
  "AGENTMEMORY_REFLECTION_LLM",
] as const;
const original = new Map<string, string | undefined>();

beforeEach(() => {
  for (const key of [...AUX_KEYS, ...ROUTE_KEYS]) {
    original.set(key, process.env[key]);
    process.env[key] = "";
  }
});

afterEach(() => {
  for (const [key, value] of original) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  original.clear();
});

describe("auxiliary LLM configuration", () => {
  it("keeps auxiliary optional and preserves role defaults", () => {
    const config = loadConfig();
    expect(config.auxiliaryProvider).toBeUndefined();
    expect(config.llmRouting.routes.graph_extraction).toBe("aux");
    expect(config.llmRouting.routes.reflection).toBe("primary");
  });

  it("parses independent auxiliary OpenAI-compatible settings", () => {
    process.env["AGENTMEMORY_AUX_LLM_BASE_URL"] = "http://127.0.0.1:11434/v1";
    process.env["AGENTMEMORY_AUX_LLM_API_KEY"] = "ollama";
    process.env["AGENTMEMORY_AUX_LLM_MODEL"] = "local-model";
    process.env["AGENTMEMORY_AUX_LLM_MAX_INPUT_CHARS"] = "9000";
    process.env["AGENTMEMORY_AUX_LLM_NOTHINK"] = "true";
    process.env["AGENTMEMORY_GRAPH_LLM"] = "primary";
    const config = loadConfig();
    expect(config.auxiliaryProvider).toMatchObject({
      baseURL: "http://127.0.0.1:11434/v1",
      apiKey: "ollama",
      model: "local-model",
      maxInputChars: 9000,
      noThink: true,
      reasoningEffort: "none",
    });
    expect(config.llmRouting.routes.graph_extraction).toBe("primary");
    expect(config.llmRouting.explicitRoutes.graph_extraction).toBe("primary");
  });

  it("rejects partial auxiliary configuration without changing primary routing", () => {
    process.env["AGENTMEMORY_AUX_LLM_MODEL"] = "local-model";
    const config = loadConfig();
    expect(config.auxiliaryProvider).toBeUndefined();
    expect(config.llmRouting.routes.reflection).toBe("primary");
    expect(config.llmRouting.warnings.join(" ")).toContain("configuration ignored");
  });
});
