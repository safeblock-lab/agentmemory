import { afterEach, describe, expect, it, vi } from "vitest";
import { OpenAIProvider } from "../src/providers/openai.js";

describe("OpenAIProvider with local Ollama", () => {
  afterEach(() => vi.restoreAllMocks());

  it("uses Ollama's think flag without unsupported reasoning_effort", async () => {
    let sentBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      sentBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({
        choices: [{ message: { content: "local reply" } }],
      }), { status: 200 });
    });

    const provider = new OpenAIProvider(
      "",
      "qwen3:4b",
      128,
      "http://127.0.0.1:11434/v1",
      { noThink: true },
    );
    await expect(provider.compress("system", "user")).resolves.toBe("local reply");

    expect(sentBody).toMatchObject({ think: false });
    expect(sentBody).not.toHaveProperty("reasoning_effort");
  });

  it("gives task thinking overrides priority over the provider default", async () => {
    const bodies: Record<string, unknown>[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      bodies.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
      return new Response(JSON.stringify({
        choices: [{ message: { content: "local reply" } }],
      }), { status: 200 });
    });

    const provider = new OpenAIProvider(
      "",
      "qwen3:4b",
      128,
      "http://127.0.0.1:11434/v1",
      { noThink: true },
    );
    await provider.compress("system", "user", { task: "summary", thinking: true });
    await provider.compress("system", "user", { task: "summary", thinking: false });

    expect(bodies[0]).toMatchObject({ think: true });
    expect(bodies[0]).not.toHaveProperty("reasoning_effort");
    expect(bodies[1]).toMatchObject({ think: false });
    expect(bodies[1]).not.toHaveProperty("reasoning_effort");
  });
});
