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
      return new Response(JSON.stringify({
        message: { content: JSON.stringify({ output: "<classification>bug</classification>" }) },
      }), { status: 200 });
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
    expect(body?.format).toMatchObject({
      type: "object",
      required: ["output"],
    });
    expect((body?.messages as Array<{ content: string }>)[0]?.content).toContain("field named output");
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

  it("serializes permanent keep-alive as Ollama's numeric sentinel", async () => {
    let body: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      body = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({ message: { content: "ok" } }), { status: 200 });
    });
    const provider = new OllamaProvider({
      provider: "ollama", model: "qwen3:4b", maxTokens: 512,
      baseURL: "http://127.0.0.1:11434/v1", apiKey: "", timeoutMs: 5_000,
      noThink: true, keepAlive: "-1", maxInputChars: 1_000,
    });
    await provider.compress("system", "user");
    expect(body).toMatchObject({ keep_alive: -1 });
  });

  it("removes a legacy Qwen thinking trace when no-thinking is requested", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      message: { content: "private reasoning</think>\n{\"facts\":[\"qwen3:4b\"]}" },
    }), { status: 200 }));
    const provider = new OllamaProvider({
      provider: "ollama", model: "qwen3:4b", maxTokens: 512,
      baseURL: "http://127.0.0.1:11434/v1", apiKey: "", timeoutMs: 5_000,
      noThink: true, maxInputChars: 1_000,
    });
    await expect(provider.compress("system", "user")).resolves.toBe('{"facts":["qwen3:4b"]}');
  });

  it("unwraps a valid structured response surrounded by conversational text", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      message: { content: 'Done.\n{"output":"<summary>kept</summary>"}\nThanks.' },
    }), { status: 200 }));
    const provider = new OllamaProvider({
      provider: "ollama", model: "qwen3:4b", maxTokens: 512,
      baseURL: "http://127.0.0.1:11434/v1", apiKey: "", timeoutMs: 5_000,
      noThink: true, maxInputChars: 1_000,
    });
    await expect(provider.summarize("system", "user")).resolves.toBe("<summary>kept</summary>");
  });

  it("repairs an invalid escape inside a structured no-thinking response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(JSON.stringify({
      message: { content: JSON.stringify({ output: '{"facts":["cache_mode\\=write-through"]}' }) },
    }), { status: 200 }));
    const provider = new OllamaProvider({
      provider: "ollama", model: "qwen3:4b", maxTokens: 512,
      baseURL: "http://127.0.0.1:11434/v1", apiKey: "", timeoutMs: 5_000,
      noThink: true, maxInputChars: 1_000,
    });
    await expect(provider.compress("system", "user")).resolves.toBe('{"facts":["cache_mode=write-through"]}');
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
