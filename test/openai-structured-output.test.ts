import { afterEach, describe, expect, it, vi } from "vitest";
import type { LlmCallOptions, LlmResponseFormat } from "../src/types.js";
import { OpenAIProvider } from "../src/providers/openai.js";

const schemaFormat: LlmResponseFormat = {
  type: "json_schema",
  json_schema: {
    name: "publication_judgment",
    strict: true,
    schema: {
      type: "object",
      properties: {
        verdict: { type: "string", enum: ["supported", "unsupported", "uncertain"] },
        reason: { type: "string" },
      },
      required: ["verdict", "reason"],
      additionalProperties: false,
    },
  },
};

describe("OpenAIProvider structured response formats", () => {
  afterEach(() => vi.restoreAllMocks());

  it.each<LlmResponseFormat>([
    { type: "json_object" },
    schemaFormat,
  ])("sends the explicit format identically in prepared and live requests: %s", async responseFormat => {
    const sent: Array<{ body: string; headers?: HeadersInit }> = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      sent.push({ body: String(init?.body), headers: init?.headers });
      return new Response(JSON.stringify({ choices: [{ message: { content: "ok" } }] }), { status: 200 });
    });

    const provider = new OpenAIProvider("test-key", "nemotron", 256, "https://llm.example/v1", {
      noThink: true,
    });
    const options: LlmCallOptions = { task: "summary", thinking: false, outputTokens: 64, responseFormat };
    const prepared = provider.prepareRequest("summarize", "system", "user", options);

    await expect(provider.summarize("system", "user", options)).resolves.toBe("ok");

    expect(sent).toHaveLength(1);
    expect(sent[0]?.body).toBe(prepared.body);
    expect(JSON.parse(sent[0]!.body)).toMatchObject({
      response_format: responseFormat,
      max_tokens: 64,
      reasoning_effort: "none",
      think: false,
    });
    expect(sent[0]?.headers).toMatchObject({ Authorization: "Bearer test-key" });
  });

  it("omits response_format when callers leave it unset", async () => {
    let sentBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      sentBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({ choices: [{ message: { content: "<summary>ok</summary>" } }] }), {
        status: 200,
      });
    });

    const provider = new OpenAIProvider("test-key", "nemotron", 256, "https://llm.example/v1");
    await expect(provider.summarize("Return XML.", "<input />")).resolves.toBe("<summary>ok</summary>");

    expect(sentBody).not.toHaveProperty("response_format");
    expect(sentBody?.messages).toEqual([
      { role: "system", content: "Return XML." },
      { role: "user", content: "<input />" },
    ]);
  });

  it.each<unknown>([
    { type: "json_schema", json_schema: { name: "", strict: true, schema: {} } },
    { type: "json_schema", json_schema: { name: "invalid", strict: true, schema: [] } },
    { type: "json_object", extra: true },
  ])("rejects malformed formats before dispatch: %s", async responseFormat => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const provider = new OpenAIProvider("test-key", "nemotron", 256, "https://llm.example/v1");
    const options = { responseFormat } as unknown as LlmCallOptions;

    await expect(provider.summarize("system", "user", options)).rejects.toThrow(/Invalid OpenAI/);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("rejects non-JSON schema values during preparation", () => {
    const responseFormat = {
      type: "json_schema",
      json_schema: { name: "invalid", strict: true, schema: { invalid: undefined } },
    };
    const provider = new OpenAIProvider("test-key", "nemotron", 256, "https://llm.example/v1");

    expect(() => provider.prepareRequest("summarize", "system", "user", {
      responseFormat: responseFormat as unknown as LlmResponseFormat,
    })).toThrow("Invalid OpenAI JSON schema response format.");
  });
});
