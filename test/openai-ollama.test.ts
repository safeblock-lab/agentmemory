import { describe, expect, it, vi } from "vitest";
import { OpenAIProvider } from "../src/providers/openai.js";

describe("OpenAIProvider with local Ollama", () => {
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
});
