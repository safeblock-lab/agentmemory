import { afterEach, describe, expect, it, vi } from "vitest";
import { OllamaProvider } from "../src/providers/ollama.js";

describe("OllamaProvider", () => {
  afterEach(() => vi.restoreAllMocks());

  it("uses the native chat API with deterministic no-thinking task limits", async () => {
    let requestUrl = "";
    let body: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
      requestUrl = String(url);
      body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({ message: { content: "<classification>bug</classification>" } }), { status: 200 });
    });

    const provider = new OllamaProvider({
      provider: "ollama",
      model: "qwen3:4b",
      maxTokens: 4096,
      baseURL: "http://127.0.0.1:11434/v1",
      apiKey: "unused",
      timeoutMs: 5_000,
      noThink: true,
      keepAlive: "10m",
      maxInputChars: 1_000,
    });
    await expect(provider.compress("system", "user", { task: "classification" })).resolves.toContain("bug");

    expect(requestUrl).toBe("http://127.0.0.1:11434/api/chat");
    expect(body).toMatchObject({
      model: "qwen3:4b",
      stream: false,
      think: false,
      keep_alive: "10m",
      options: { num_predict: 384, temperature: 0 },
    });
  });

  it("rejects reasoning-only native responses", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      message: { thinking: "hidden reasoning" },
    }), { status: 200 }));
    const provider = new OllamaProvider({
      provider: "ollama",
      model: "qwen3:4b",
      maxTokens: 512,
      baseURL: "http://localhost:11434/v1",
      apiKey: "",
      timeoutMs: 5_000,
      noThink: true,
      maxInputChars: 1_000,
    });
    await expect(provider.summarize("system", "user")).rejects.toThrow("did not contain assistant content");
  });

  it("rejects non-local native endpoints", () => {
    expect(() => new OllamaProvider({
      provider: "ollama",
      model: "qwen3:4b",
      maxTokens: 512,
      baseURL: "https://example.test/v1",
      apiKey: "",
      timeoutMs: 5_000,
      noThink: true,
      maxInputChars: 1_000,
    })).toThrow("requires a local");
  });
});
