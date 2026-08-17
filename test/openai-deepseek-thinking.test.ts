import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const originalHome = process.env["HOME"];
const originalUserProfile = process.env["USERPROFILE"];
let sandboxHome: string;
const loggerInfo = vi.fn();

async function loadProvider() {
  vi.resetModules();
  vi.doMock("../src/logger.js", () => ({
    logger: { info: loggerInfo, warn: vi.fn(), error: vi.fn() },
  }));
  return import("../src/providers/openai.js");
}

function writeAgentMemoryEnv(contents: string): void {
  const dir = join(sandboxHome, ".agentmemory");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, ".env"), contents);
}

describe("OpenAIProvider direct DeepSeek thinking control and telemetry", () => {
  beforeEach(() => {
    sandboxHome = mkdtempSync(join(tmpdir(), "agentmemory-deepseek-"));
    process.env["HOME"] = sandboxHome;
    process.env["USERPROFILE"] = sandboxHome;
    process.env["AGENTMEMORY_LLM_LOGGING"] = "true";
    loggerInfo.mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.doUnmock("../src/logger.js");
    delete process.env["AGENTMEMORY_LLM_LOGGING"];
    if (originalHome === undefined) delete process.env["HOME"];
    else process.env["HOME"] = originalHome;
    if (originalUserProfile === undefined) delete process.env["USERPROFILE"];
    else process.env["USERPROFILE"] = originalUserProfile;
    rmSync(sandboxHome, { recursive: true, force: true });
  });

  it("disables DeepSeek thinking by default and logs safe completion metadata", async () => {
    let sentBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      sentBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({
        choices: [{ message: { content: "private reply" } }],
        usage: { prompt_tokens: 11, completion_tokens: 7, total_tokens: 18 },
      }), { status: 200 });
    });

    const { OpenAIProvider } = await loadProvider();
    const provider = new OpenAIProvider("private-key", "deepseek-v4-pro", 128, "https://api.deepseek.com/v1");
    await expect(provider.compress("private system prompt", "private user prompt")).resolves.toBe("private reply");

    expect(sentBody?.thinking).toEqual({ type: "disabled" });
    expect(loggerInfo).toHaveBeenCalledWith("LLM call started", expect.objectContaining({
      provider: "deepseek",
      thinkingRequest: "disabled",
    }));
    expect(loggerInfo).toHaveBeenCalledWith("LLM call completed", expect.objectContaining({
      usage: { inputTokens: 11, outputTokens: 7, totalTokens: 18 },
      responseChars: 13,
    }));
    const logs = JSON.stringify(loggerInfo.mock.calls);
    expect(logs).not.toContain("private-key");
    expect(logs).not.toContain("private system prompt");
    expect(logs).not.toContain("private user prompt");
    expect(logs).not.toContain("private reply");
  });

  it("enables DeepSeek thinking only when the AgentMemory env file says true", async () => {
    writeAgentMemoryEnv("deepseek_thinking=true");
    let sentBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      sentBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({ choices: [{ message: { content: "ok" } }] }), { status: 200 });
    });

    const { OpenAIProvider } = await loadProvider();
    const provider = new OpenAIProvider("key", "deepseek-v4-pro", 128, "https://api.deepseek.com/v1");
    await provider.compress("system", "user");

    expect(sentBody?.thinking).toEqual({ type: "enabled" });
  });

  it("applies the disabled default to DeepSeek subdomains", async () => {
    let sentBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      sentBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({ choices: [{ message: { content: "ok" } }] }), { status: 200 });
    });

    const { OpenAIProvider } = await loadProvider();
    const provider = new OpenAIProvider("key", "deepseek-v4-pro", 128, "https://regional.deepseek.com/v1");
    await provider.compress("system", "user");

    expect(sentBody?.thinking).toEqual({ type: "disabled" });
  });

  it("sends the Fireworks DeepSeek model unchanged without thinking", async () => {
    writeAgentMemoryEnv("OPENAI_REASONING_EFFORT=none");
    let requestUrl: string | undefined;
    let sentBody: Record<string, unknown> | undefined;
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
      requestUrl = String(url);
      sentBody = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response(JSON.stringify({ choices: [{ message: { content: "ok" } }] }), {
        status: 200,
      });
    });

    const { OpenAIProvider } = await loadProvider();
    const provider = new OpenAIProvider(
      "test-fireworks-key",
      "accounts/fireworks/models/deepseek-v4-flash-0731",
      128,
      "https://api.fireworks.ai/inference/v1",
    );
    await provider.compress("system", "user");

    expect(requestUrl).toBe("https://api.fireworks.ai/inference/v1/chat/completions");
    expect(sentBody).toMatchObject({
      model: "accounts/fireworks/models/deepseek-v4-flash-0731",
      reasoning_effort: "none",
    });
    expect(sentBody).not.toHaveProperty("thinking");
  });
});
