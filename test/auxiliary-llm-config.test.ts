import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { loadConfig } from "../src/config.js";

const AUX_KEYS = [
  "AGENTMEMORY_AUX_LLM_PROVIDER",
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
const THINKING_KEYS = [
  "AGENTMEMORY_GRAPH_LLM_THINKING",
  "AGENTMEMORY_TEMPORAL_GRAPH_LLM_THINKING",
  "AGENTMEMORY_CONSOLIDATION_LLM_THINKING",
  "AGENTMEMORY_COMPRESSION_LLM_THINKING",
  "AGENTMEMORY_SUMMARY_LLM_THINKING",
  "AGENTMEMORY_ENTITY_EXTRACTION_LLM_THINKING",
  "AGENTMEMORY_CLASSIFICATION_LLM_THINKING",
  "AGENTMEMORY_REFLECTION_LLM_THINKING",
  "AGENTMEMORY_CONFLICT_RESOLUTION_LLM_THINKING",
  "AGENTMEMORY_SKILL_EXTRACTION_LLM_THINKING",
  "AGENTMEMORY_QUERY_EXPANSION_LLM_THINKING",
  "AGENTMEMORY_FLOW_COMPRESSION_LLM_THINKING",
] as const;
const BATCH_KEYS = [
  "AGENTMEMORY_FIREWORKS_BATCH_ENABLED",
  "AGENTMEMORY_FIREWORKS_BATCH_ACCOUNT_ID",
  "AGENTMEMORY_FIREWORKS_BATCH_API_KEY",
  "AGENTMEMORY_FIREWORKS_BATCH_MODEL",
  "FIREWORKS_ACCOUNT_ID",
  "FIREWORKS_API_KEY",
  "FIREWORKS_MODEL",
  "OPENAI_API_KEY",
  "OPENAI_BASE_URL",
  "OPENAI_MODEL",
  "AGENTMEMORY_FIREWORKS_BATCH_POLL_DEADLINE_MS",
] as const;
const original = new Map<string, string | undefined>();

beforeEach(() => {
  for (const key of [...AUX_KEYS, ...ROUTE_KEYS, ...THINKING_KEYS, ...BATCH_KEYS]) {
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
    expect(config.llmRouting.thinking).toEqual({});
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
      provider: "ollama",
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

  it("parses true and false per-task thinking overrides without changing routes", () => {
    process.env["AGENTMEMORY_GRAPH_LLM_THINKING"] = "true";
    process.env["AGENTMEMORY_TEMPORAL_GRAPH_LLM_THINKING"] = "1";
    process.env["AGENTMEMORY_CONSOLIDATION_LLM_THINKING"] = "0";
    process.env["AGENTMEMORY_REFLECTION_LLM_THINKING"] = "false";
    const config = loadConfig();
    expect(config.llmRouting.thinking).toEqual({
      graph_extraction: true,
      temporal_graph_extraction: true,
      consolidation: false,
      reflection: false,
    });
    expect(config.llmRouting.routes.graph_extraction).toBe("aux");
    expect(config.llmRouting.routes.reflection).toBe("primary");
  });

  it("ignores invalid per-task thinking overrides with a warning", () => {
    process.env["AGENTMEMORY_GRAPH_LLM_THINKING"] = "sometimes";
    const config = loadConfig();
    expect(config.llmRouting.thinking).toEqual({});
    expect(config.llmRouting.warnings.join(" ")).toContain(
      "AGENTMEMORY_GRAPH_LLM_THINKING must be true, false, 1, or 0; ignoring thinking override.",
    );
  });

  it("rejects native Ollama on a non-local endpoint", () => {
    process.env["AGENTMEMORY_AUX_LLM_PROVIDER"] = "ollama";
    process.env["AGENTMEMORY_AUX_LLM_BASE_URL"] = "https://example.test/v1";
    process.env["AGENTMEMORY_AUX_LLM_MODEL"] = "local-model";
    const config = loadConfig();
    expect(config.auxiliaryProvider).toBeUndefined();
    expect(config.llmRouting.warnings.join(" ")).toContain("requires a local");
  });

  it("rejects partial auxiliary configuration without changing primary routing", () => {
    process.env["AGENTMEMORY_AUX_LLM_MODEL"] = "local-model";
    const config = loadConfig();
    expect(config.auxiliaryProvider).toBeUndefined();
    expect(config.llmRouting.routes.reflection).toBe("primary");
    expect(config.llmRouting.warnings.join(" ")).toContain("configuration ignored");
  });

  it("keeps Fireworks Batch disabled unless explicitly configured", () => {
    const config = loadConfig();
    expect(config.fireworksBatch.enabled).toBe(false);
  });

  it("reuses the primary Fireworks key and model without changing aux", () => {
    process.env["AGENTMEMORY_AUX_LLM_BASE_URL"] = "http://127.0.0.1:11434/v1";
    process.env["AGENTMEMORY_AUX_LLM_API_KEY"] = "ollama";
    process.env["AGENTMEMORY_AUX_LLM_MODEL"] = "qwen3.5:4b";
    process.env["AGENTMEMORY_FIREWORKS_BATCH_ENABLED"] = "true";
    process.env["FIREWORKS_ACCOUNT_ID"] = "test-account";
    process.env["FIREWORKS_API_KEY"] = "test-key";
    process.env["OPENAI_BASE_URL"] = "https://api.fireworks.ai/inference/v1";
    process.env["OPENAI_MODEL"] = "accounts/test/models/test";
    const config = loadConfig();
    expect(config.fireworksBatch).toMatchObject({
      enabled: true,
      accountId: "test-account",
      model: "accounts/test/models/test",
      pollDeadlineMs: 24 * 60 * 60_000,
    });
    expect(config.auxiliaryProvider?.model).toBe("qwen3.5:4b");
  });

  it("bounds the Fireworks polling deadline to 24 hours", () => {
    process.env["AGENTMEMORY_FIREWORKS_BATCH_ENABLED"] = "true";
    process.env["FIREWORKS_ACCOUNT_ID"] = "test-account";
    process.env["FIREWORKS_API_KEY"] = "test-key";
    process.env["FIREWORKS_MODEL"] = "accounts/test/models/test";
    process.env["AGENTMEMORY_FIREWORKS_BATCH_POLL_DEADLINE_MS"] = "999999999";

    const config = loadConfig();

    expect(config.fireworksBatch.pollDeadlineMs).toBe(24 * 60 * 60_000);
    expect(config.llmRouting.warnings.join(" ")).toContain(
      "AGENTMEMORY_FIREWORKS_BATCH_POLL_DEADLINE_MS must be between 1 and 86400000",
    );
  });

  it("does not enable Batch from a non-Fireworks auxiliary key", () => {
    process.env["AGENTMEMORY_AUX_LLM_BASE_URL"] = "https://example.test/v1";
    process.env["AGENTMEMORY_AUX_LLM_API_KEY"] = "test-key";
    process.env["AGENTMEMORY_AUX_LLM_MODEL"] = "test-model";
    process.env["AGENTMEMORY_FIREWORKS_BATCH_ENABLED"] = "true";
    process.env["AGENTMEMORY_FIREWORKS_BATCH_ACCOUNT_ID"] = "test-account";
    const config = loadConfig();
    expect(config.fireworksBatch.enabled).toBe(false);
    expect(config.llmRouting.warnings.join(" ")).toContain("Fireworks credentials/model");
  });
});
