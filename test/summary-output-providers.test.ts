import { afterEach, describe, expect, it, vi } from "vitest";
import { OpenAIProvider } from "../src/providers/openai.js";
import { OpenRouterProvider, OpenRouterProviderError } from "../src/providers/openrouter.js";
import { AnthropicProvider } from "../src/providers/anthropic.js";
import { MinimaxProvider } from "../src/providers/minimax.js";
import { OllamaProvider } from "../src/providers/ollama.js";
import { LlmTaskRouter } from "../src/providers/task-router.js";
import { FallbackChainProvider } from "../src/providers/fallback-chain.js";
import { ResilientProvider } from "../src/providers/resilient.js";
import { OpenRouterKeyPoolProvider } from "../src/providers/openrouter-key-pool.js";
import { GeminiAccountPoolProvider } from "../src/providers/gemini-account-pool.js";
import { taskOutputTokens, summaryOutputTokens } from "../src/providers/task-output-limits.js";
import type { LlmCallOptions, LlmRoutingConfig, MemoryProvider } from "../src/types.js";

const mocks = vi.hoisted(() => ({ anthropicCreate: vi.fn() }));
vi.mock("@anthropic-ai/sdk", () => ({ default: class { messages = { create: mocks.anthropicCreate }; } }));
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); mocks.anthropicCreate.mockReset(); });
const summaryOptions: LlmCallOptions = { task: "summary", outputTokens: 8192 };
const factories: Array<[string, () => MemoryProvider, number]> = [
  ["OpenAI", () => new OpenAIProvider("test-key", "model", 4096, "https://example.test"), 768],
  ["DeepSeek", () => new OpenAIProvider("test-key", "model", 4096, "https://api.deepseek.com"), 768],
  ["Azure", () => new OpenAIProvider("test-key", "model", 4096, "https://sample.openai.azure.com"), 768],
  ["OpenRouter", () => new OpenRouterProvider("test-key", "model", 4096, "https://openrouter.ai/api/v1/chat/completions"), 4096],
  ["Gemini compatibility", () => new OpenRouterProvider("test-key", "model", 4096, "https://example.test/gemini"), 4096],
  ["MiniMax", () => new MinimaxProvider("test-key", "model", 4096), 4096],
  ["Ollama", () => new OllamaProvider({ provider: "ollama", baseURL: "http://localhost:11434", apiKey: "", model: "local", maxTokens: 4096, timeoutMs: 5000, noThink: true, maxInputChars: 120000 }), 768],
];
function stubResponses(rejectFirst = false) {
  const bodies: Array<Record<string, unknown>> = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
    bodies.push(JSON.parse(String(init?.body)));
    if (rejectFirst && bodies.length === 1) return new Response("context_length_exceeded", { status: 400 });
    return new Response(JSON.stringify({ choices: [{ message: { content: "ok" } }], content: [{ type: "text", text: "ok" }], message: { content: JSON.stringify({ output: "ok" }) }, usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 } }), { status: 200 });
  });
  return bodies;
}
function outputLimit(body: Record<string, unknown>): unknown {
  const options = body.options;
  return options && typeof options === "object" && "num_predict" in options ? options.num_predict : body.max_tokens;
}
const routing: LlmRoutingConfig = {
  routes: { graph_extraction: "aux", temporal_graph_extraction: "aux", consolidation: "aux", compression: "aux", summary: "aux", entity_extraction: "aux", classification: "aux", reflection: "primary", conflict_resolution: "primary", skill_extraction: "aux", query_expansion: "aux", flow_compression: "aux" },
  explicitRoutes: {}, thinking: { summary: false }, warnings: [],
};

describe("summary output budgets at provider boundaries", () => {
  it.each(factories)("%s uses per-call summary cap, preserves global and other task limits", async (_name, factory, legacySummaryMaximum) => {
    const bodies = stubResponses();
    const selected = factory();
    await selected.summarize("system", "user", summaryOptions);
    await selected.summarize("system", "user", { task: "summary" });
    await selected.compress("system", "user", { task: "compression", outputTokens: 8192 });
    await selected.summarize("system", "user");
    expect(outputLimit(bodies[0])).toBe(8192);
    expect(outputLimit(bodies[1])).toBe(legacySummaryMaximum);
    expect(outputLimit(bodies[2])).toBe(legacySummaryMaximum);
    expect(outputLimit(bodies[3])).toBe(4096);
  });
  it("Anthropic SDK request carries the summary cap; compression keeps MAX_TOKENS", async () => {
    mocks.anthropicCreate.mockResolvedValue({ content: [{ type: "text", text: "ok" }] });
    const selected = new AnthropicProvider("test-key", "model", 4096);
    await selected.summarize("system", "user", summaryOptions);
    await selected.compress("system", "user", { task: "compression", outputTokens: 8192 });
    expect(mocks.anthropicCreate.mock.calls[0][0]).toMatchObject({ max_tokens: 8192 });
    expect(mocks.anthropicCreate.mock.calls[1][0]).toMatchObject({ max_tokens: 4096 });
  });
  it.each([false, true])("Ollama summary cap is independent of thinking=%s", async thinking => {
    const bodies = stubResponses();
    await factories[6][1]().summarize("system", "user", { ...summaryOptions, thinking });
    expect(outputLimit(bodies[0])).toBe(8192);
    expect(bodies[0].think).toBe(thinking);
  });
  it.each(["primary", "aux", "fallback"])("router preserves the cap for %s and records usage", async role => {
    const bodies = stubResponses(role === "fallback");
    const primary = new OpenAIProvider("test-key", "primary-model", 4096, "https://primary.test");
    const auxiliary = new OpenAIProvider("test-key", "aux-model", 4096, "https://aux.test");
    const usage = vi.fn();
    const router = new LlmTaskRouter({ primary: { provider: primary, model: "primary-model" }, auxiliary: { provider: auxiliary, model: "aux-model" }, routing: { ...routing, routes: { ...routing.routes, summary: role === "primary" ? "primary" : "aux" } }, onUsage: usage });
    await router.run("summary", selected => selected.summarize("system", "user", summaryOptions), candidate => candidate === "ok");
    expect(bodies).toHaveLength(role === "fallback" ? 2 : 1);
    expect(bodies.map(outputLimit)).toEqual(role === "fallback" ? [8192, 8192] : [8192]);
    expect(usage).toHaveBeenCalledWith(expect.objectContaining({ task: "summary", inputTokens: 2, outputTokens: 3 }));
  });
  it("fallback and resilience wrappers preserve cap and circuit behavior", async () => {
    const bodies = stubResponses(true);
    const failed = new ResilientProvider(factories[0][1]());
    const fallback = new FallbackChainProvider([failed, new ResilientProvider(factories[3][1]())]);
    await expect(fallback.summarize("system", "user", summaryOptions)).resolves.toBe("ok");
    expect(bodies.map(outputLimit)).toEqual([8192, 8192]);
    expect(failed.circuitState.failures).toBe(1);
  });
  it.each(["openrouter", "gemini"])("%s account pool preserves cap into its terminal fallback", async pool => {
    const bodies = stubResponses(pool === "openrouter");
    const unavailable = vi.fn(async () => { throw new OpenRouterProviderError("gemini", 503, "unavailable"); });
    const account = pool === "openrouter" ? factories[3][1]() : { name: "gemini", compress: unavailable, summarize: unavailable };
    const terminal = factories[0][1]();
    const selected = pool === "openrouter" ? new OpenRouterKeyPoolProvider([account], terminal, 1, () => 0)
      : new GeminiAccountPoolProvider([account], terminal, { minimumRequestIntervalMs: 0, unavailableRetryDelaysMs: [] });
    await selected.summarize("system", "user", summaryOptions);
    expect(bodies.map(outputLimit)).toEqual(pool === "openrouter" ? [8192, 8192] : [8192]);
    if (pool === "gemini") expect(unavailable).toHaveBeenCalledWith("system", "user", summaryOptions);
  });
  it("rejects invalid per-call budgets and leaves unrelated task limits intact", () => {
    expect(() => summaryOutputTokens({ task: "summary", outputTokens: Infinity }, 4096)).toThrow("invalid_summary_budget");
    expect(taskOutputTokens("graph_extraction", 4096, 8192)).toBe(512);
    expect(taskOutputTokens("summary", 4096, 8192)).toBe(8192);
    expect(taskOutputTokens("summary", 4096)).toBe(768);
  });
});
