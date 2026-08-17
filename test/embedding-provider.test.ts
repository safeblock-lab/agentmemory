import { describe, it, expect, vi, beforeAll, afterAll, beforeEach, afterEach } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { EmbeddingProvider } from "../src/types.js";

const originalEnv = { ...process.env };
let sandboxHome: string;
let createEmbeddingProvider: typeof import("../src/providers/embedding/index.js").createEmbeddingProvider;
let withDimensionGuard: typeof import("../src/providers/embedding/index.js").withDimensionGuard;
let GeminiEmbeddingProvider: typeof import("../src/providers/embedding/gemini.js").GeminiEmbeddingProvider;
let OpenAIEmbeddingProvider: typeof import("../src/providers/embedding/openai.js").OpenAIEmbeddingProvider;

function restoreOriginalEnv(): void {
  for (const key of Object.keys(process.env)) {
    if (!(key in originalEnv)) delete process.env[key];
  }
  Object.assign(process.env, originalEnv);
}

function resetTestEnv(): void {
  restoreOriginalEnv();
  process.env["HOME"] = sandboxHome;
  process.env["USERPROFILE"] = sandboxHome;
}

beforeAll(async () => {
  sandboxHome = mkdtempSync(join(tmpdir(), "agentmemory-embedding-"));
  resetTestEnv();
  vi.resetModules();
  ({ createEmbeddingProvider, withDimensionGuard } = await import("../src/providers/embedding/index.js"));
  ({ GeminiEmbeddingProvider } = await import("../src/providers/embedding/gemini.js"));
  ({ OpenAIEmbeddingProvider } = await import("../src/providers/embedding/openai.js"));
});

afterAll(() => {
  restoreOriginalEnv();
  rmSync(sandboxHome, { recursive: true, force: true });
  vi.resetModules();
});

describe("createEmbeddingProvider", () => {
  beforeEach(() => {
    resetTestEnv();
    delete process.env["GEMINI_API_KEY"];
    delete process.env["OPENAI_API_KEY"];
    delete process.env["OPENAI_EMBEDDING_API_KEY"];
    delete process.env["OPENAI_BASE_URL"];
    delete process.env["OPENAI_EMBEDDING_BASE_URL"];
    delete process.env["VOYAGE_API_KEY"];
    delete process.env["COHERE_API_KEY"];
    delete process.env["OPENROUTER_API_KEY"];
    delete process.env["EMBEDDING_PROVIDER"];
  });

  afterEach(() => {
    resetTestEnv();
  });

  it("returns null when no API keys are set", () => {
    const provider = createEmbeddingProvider();
    expect(provider).toBeNull();
  });

  it("returns GeminiEmbeddingProvider when GEMINI_API_KEY is set", () => {
    process.env["GEMINI_API_KEY"] = "test-key-123";
    const provider = createEmbeddingProvider();
    expect(provider).toBeInstanceOf(GeminiEmbeddingProvider);
    expect(provider!.name).toBe("gemini");
  });

  it("returns OpenAIEmbeddingProvider when OPENAI_API_KEY is set", () => {
    process.env["OPENAI_API_KEY"] = "test-key-456";
    const provider = createEmbeddingProvider();
    expect(provider).toBeInstanceOf(OpenAIEmbeddingProvider);
    expect(provider!.name).toBe("openai");
  });

  it("keeps embedding credentials and endpoint separate from OpenAI LLM settings", async () => {
    process.env["OPENAI_API_KEY"] = "llm-key";
    process.env["OPENAI_BASE_URL"] = "https://chat.example.test/v1";
    process.env["OPENAI_EMBEDDING_API_KEY"] = "embedding-key";
    process.env["OPENAI_EMBEDDING_BASE_URL"] = "https://embeddings.example.test/v1";
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ data: [{ embedding: Array(1536).fill(0.1) }] }), {
        status: 200,
      }),
    );

    const provider = createEmbeddingProvider();
    await provider!.embed("hello");

    expect(fetchSpy).toHaveBeenCalledWith(
      "https://embeddings.example.test/v1/embeddings",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Authorization: "Bearer embedding-key",
        }),
      }),
    );
    const [, request] = fetchSpy.mock.calls[0];
    expect((request?.headers as Record<string, string>)["Authorization"]).not.toBe(
      "Bearer llm-key",
    );
  });

  it("EMBEDDING_PROVIDER override takes precedence", () => {
    process.env["GEMINI_API_KEY"] = "test-key-123";
    process.env["OPENAI_API_KEY"] = "test-key-456";
    process.env["EMBEDDING_PROVIDER"] = "openai";
    const provider = createEmbeddingProvider();
    expect(provider).toBeInstanceOf(OpenAIEmbeddingProvider);
  });
});

describe("OpenAIEmbeddingProvider", () => {
  beforeEach(() => {
    resetTestEnv();
    delete process.env["OPENAI_BASE_URL"];
    delete process.env["OPENAI_EMBEDDING_BASE_URL"];
    delete process.env["OPENAI_EMBEDDING_API_KEY"];
    delete process.env["OPENAI_EMBEDDING_MODEL"];
    delete process.env["OPENAI_EMBEDDING_DIMENSIONS"];
  });

  afterEach(() => {
    resetTestEnv();
  });

  it("uses default base URL and model when env vars are not set", () => {
    const provider = new OpenAIEmbeddingProvider("test-key");
    expect(provider.name).toBe("openai");
    expect(provider.dimensions).toBe(1536);
  });

  it("throws when no API key is provided", () => {
    delete process.env["OPENAI_API_KEY"];
    delete process.env["OPENAI_EMBEDDING_API_KEY"];
    expect(() => new OpenAIEmbeddingProvider()).toThrow(/API key is required.*OPENAI_EMBEDDING_API_KEY.*OPENAI_API_KEY/);
  });

  it("respects OPENAI_BASE_URL env var", async () => {
    process.env["OPENAI_BASE_URL"] = "https://my-proxy.example.com";
    const provider = new OpenAIEmbeddingProvider("test-key");

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ data: [{ embedding: [0.1, 0.2, 0.3] }] }), { status: 200 }),
    );

    await provider.embed("hello");
    expect(fetchSpy).toHaveBeenCalledWith(
      "https://my-proxy.example.com/v1/embeddings",
      expect.any(Object),
    );

    fetchSpy.mockRestore();
  });

  it("respects OPENAI_EMBEDDING_MODEL env var", async () => {
    process.env["OPENAI_EMBEDDING_MODEL"] = "text-embedding-3-large";
    const provider = new OpenAIEmbeddingProvider("test-key");

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ data: [{ embedding: [0.1, 0.2, 0.3] }] }), { status: 200 }),
    );

    await provider.embed("hello");
    const body = JSON.parse((fetchSpy.mock.calls[0][1] as RequestInit).body as string);
    expect(body.model).toBe("text-embedding-3-large");

    fetchSpy.mockRestore();
  });

  it("derives dimensions from model in the known-models table", () => {
    process.env["OPENAI_EMBEDDING_MODEL"] = "text-embedding-3-large";
    const large = new OpenAIEmbeddingProvider("test-key");
    expect(large.dimensions).toBe(3072);

    process.env["OPENAI_EMBEDDING_MODEL"] = "text-embedding-ada-002";
    const ada = new OpenAIEmbeddingProvider("test-key");
    expect(ada.dimensions).toBe(1536);

    process.env["OPENAI_EMBEDDING_MODEL"] = "text-embedding-3-small";
    const small = new OpenAIEmbeddingProvider("test-key");
    expect(small.dimensions).toBe(1536);
  });

  it("OPENAI_EMBEDDING_DIMENSIONS overrides the model-derived dimensions", () => {
    process.env["OPENAI_EMBEDDING_MODEL"] = "text-embedding-3-large";
    process.env["OPENAI_EMBEDDING_DIMENSIONS"] = "768";
    const provider = new OpenAIEmbeddingProvider("test-key");
    expect(provider.dimensions).toBe(768);
  });

  it("falls back to 1536 for unknown custom models", () => {
    process.env["OPENAI_EMBEDDING_MODEL"] = "mystery-self-hosted-model";
    const provider = new OpenAIEmbeddingProvider("test-key");
    expect(provider.dimensions).toBe(1536);
  });

  it("rejects invalid OPENAI_EMBEDDING_DIMENSIONS values", () => {
    process.env["OPENAI_EMBEDDING_DIMENSIONS"] = "not-a-number";
    expect(() => new OpenAIEmbeddingProvider("test-key")).toThrow(
      /OPENAI_EMBEDDING_DIMENSIONS must be a positive integer/,
    );

    process.env["OPENAI_EMBEDDING_DIMENSIONS"] = "-5";
    expect(() => new OpenAIEmbeddingProvider("test-key")).toThrow(
      /OPENAI_EMBEDDING_DIMENSIONS must be a positive integer/,
    );

    process.env["OPENAI_EMBEDDING_DIMENSIONS"] = "0";
    expect(() => new OpenAIEmbeddingProvider("test-key")).toThrow(
      /OPENAI_EMBEDDING_DIMENSIONS must be a positive integer/,
    );
  });
});

describe("withDimensionGuard", () => {
  function fakeProvider(opts: {
    dimensions: number;
    embed: () => Float32Array;
    batch?: () => Float32Array[];
    image?: () => Float32Array;
  }): EmbeddingProvider {
    const provider: EmbeddingProvider = {
      name: "fake",
      dimensions: opts.dimensions,
      embed: async () => opts.embed(),
      embedBatch: async () => opts.batch?.() ?? [opts.embed()],
    };
    if (opts.image) provider.embedImage = async () => opts.image!();
    return provider;
  }

  it("preserves the wrapped provider's prototype so instanceof keeps working", async () => {
    class FakeProvider implements EmbeddingProvider {
      readonly name = "fake-class";
      readonly dimensions = 4;
      async embed(): Promise<Float32Array> {
        return new Float32Array([1, 2, 3, 4]);
      }
      async embedBatch(): Promise<Float32Array[]> {
        return [new Float32Array([1, 2, 3, 4])];
      }
    }
    const guarded = withDimensionGuard(new FakeProvider());
    expect(guarded).toBeInstanceOf(FakeProvider);
    expect(guarded.name).toBe("fake-class");
    expect(guarded.dimensions).toBe(4);
  });

  it("passes through vectors that match the declared dimensions", async () => {
    const guarded = withDimensionGuard(
      fakeProvider({
        dimensions: 4,
        embed: () => new Float32Array([1, 2, 3, 4]),
        batch: () => [new Float32Array([1, 2, 3, 4]), new Float32Array([5, 6, 7, 8])],
      }),
    );
    await expect(guarded.embed("x")).resolves.toEqual(new Float32Array([1, 2, 3, 4]));
    await expect(guarded.embedBatch(["a", "b"])).resolves.toHaveLength(2);
  });

  it("throws when embed() returns the wrong dimension", async () => {
    const guarded = withDimensionGuard(
      fakeProvider({
        dimensions: 4,
        embed: () => new Float32Array([1, 2, 3]),
      }),
    );
    await expect(guarded.embed("x")).rejects.toThrow(
      /dimension mismatch in fake\.embed: expected 4, got 3/,
    );
  });

  it("throws when any vector in embedBatch() returns the wrong dimension", async () => {
    const guarded = withDimensionGuard(
      fakeProvider({
        dimensions: 4,
        embed: () => new Float32Array([1, 2, 3, 4]),
        batch: () => [new Float32Array([1, 2, 3, 4]), new Float32Array([1, 2])],
      }),
    );
    await expect(guarded.embedBatch(["a", "b"])).rejects.toThrow(
      /dimension mismatch in fake\.embedBatch\[1\]: expected 4, got 2/,
    );
  });

  it("guards embedImage when present and omits it when absent", async () => {
    const withImage = withDimensionGuard(
      fakeProvider({
        dimensions: 4,
        embed: () => new Float32Array([1, 2, 3, 4]),
        image: () => new Float32Array([1, 2]),
      }),
    );
    expect(withImage.embedImage).toBeDefined();
    await expect(withImage.embedImage!("/tmp/x")).rejects.toThrow(
      /dimension mismatch in fake\.embedImage: expected 4, got 2/,
    );

    const withoutImage = withDimensionGuard(
      fakeProvider({
        dimensions: 4,
        embed: () => new Float32Array([1, 2, 3, 4]),
      }),
    );
    expect(withoutImage.embedImage).toBeUndefined();
  });
});
