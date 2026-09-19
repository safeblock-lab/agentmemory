import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";
import {
  GeminiAccountPoolProvider,
  loadGeminiAccounts,
} from "../src/providers/gemini-account-pool.js";
import { createProvider } from "../src/providers/index.js";
import { OpenRouterProviderError } from "../src/providers/openrouter.js";
import type { MemoryProvider } from "../src/types.js";

const temporaryDirectories: string[] = [];
const immediatePoolOptions = {
  minimumRequestIntervalMs: 0,
  unavailableRetryDelaysMs: [],
} as const;
const managedEnvironmentKeys = [
  "AGENTMEMORY_GEMINI_ACCOUNTS_DIR",
  "FIREWORKS_API_KEY",
  "FIREWORKS_MODEL",
  "GEMINI_MODEL",
  "OPENAI_API_KEY",
  "OPENAI_BASE_URL",
  "OPENAI_MODEL",
] as const;
const originalEnvironment = Object.fromEntries(
  managedEnvironmentKeys.map((key) => [key, process.env[key]]),
) as Record<(typeof managedEnvironmentKeys)[number], string | undefined>;

function temporaryDirectory(): string {
  const directory = mkdtempSync(join(tmpdir(), "agentmemory-gemini-"));
  temporaryDirectories.push(directory);
  return directory;
}

function provider(
  name: string,
  operation: () => Promise<string>,
): MemoryProvider {
  return {
    name,
    compress: operation,
    summarize: operation,
  };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
  for (const key of managedEnvironmentKeys) {
    const originalValue = originalEnvironment[key];
    if (originalValue === undefined) delete process.env[key];
    else process.env[key] = originalValue;
  }
});

describe("loadGeminiAccounts", () => {
  it("shuffles account files after loading and applies the default model", () => {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "02-second.json"), JSON.stringify({
      apiKey: "second-key",
      project: "projects/2",
      model: "gemini-2.5-pro",
    }));
    writeFileSync(join(directory, "01-first.json"), JSON.stringify({
      apiKey: "first-key",
      project: "projects/1",
    }));

    const randomIndex = vi.fn().mockReturnValue(0);
    const accounts = loadGeminiAccounts(
      directory,
      "gemini-3.6-flash",
      randomIndex,
    );

    expect(accounts).toEqual([
      {
        apiKey: "second-key",
        project: "projects/2",
        model: "gemini-2.5-pro",
      },
      {
        apiKey: "first-key",
        project: "projects/1",
        model: "gemini-3.6-flash",
      },
    ]);
    expect(randomIndex).toHaveBeenCalledWith(2);
  });

  it("rejects malformed account files without exposing their contents", () => {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "broken.json"), "not-json-with-secret");

    expect(() => loadGeminiAccounts(directory, "gemini-3.6-flash"))
      .toThrow("Gemini account broken.json: invalid JSON.");
  });
});

describe("GeminiAccountPoolProvider", () => {
  it("moves to the next account after quota exhaustion", async () => {
    const first = vi.fn().mockRejectedValue(
      new OpenRouterProviderError("gemini", 429, "quota exhausted"),
    );
    const second = vi.fn().mockResolvedValue("second-account");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new GeminiAccountPoolProvider(
      [provider("first", first), provider("second", second)],
      provider("fireworks", fallback),
      immediatePoolOptions,
    );

    await expect(pool.compress("system", "user")).resolves.toBe("second-account");
    await expect(pool.compress("system", "user")).resolves.toBe("second-account");
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(2);
    expect(fallback).not.toHaveBeenCalled();
  });

  it("uses Fireworks for all later calls after every account is exhausted", async () => {
    const exhausted = () => Promise.reject(
      new OpenRouterProviderError("gemini", 429, "quota exhausted"),
    );
    const first = vi.fn(exhausted);
    const second = vi.fn(exhausted);
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new GeminiAccountPoolProvider(
      [provider("first", first), provider("second", second)],
      provider("fireworks", fallback),
      immediatePoolOptions,
    );

    await expect(pool.summarize("system", "user")).resolves.toBe("fireworks");
    await expect(pool.summarize("system", "user")).resolves.toBe("fireworks");
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
    expect(fallback).toHaveBeenCalledTimes(2);
  });

  it("does not hide non-quota Gemini failures behind another account", async () => {
    const unauthorized = vi.fn().mockRejectedValue(
      new OpenRouterProviderError("gemini", 401, "invalid key"),
    );
    const second = vi.fn().mockResolvedValue("second-account");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new GeminiAccountPoolProvider(
      [provider("first", unauthorized), provider("second", second)],
      provider("fireworks", fallback),
      immediatePoolOptions,
    );

    await expect(pool.compress("system", "user")).rejects.toMatchObject({ status: 401 });
    expect(second).not.toHaveBeenCalled();
    expect(fallback).not.toHaveBeenCalled();
  });

  it("serializes concurrent Gemini calls and spaces their start times", async () => {
    vi.useFakeTimers();
    const gemini = vi.fn()
      .mockResolvedValueOnce("first-result")
      .mockResolvedValueOnce("second-result");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new GeminiAccountPoolProvider(
      [provider("gemini", gemini)],
      provider("fireworks", fallback),
      { minimumRequestIntervalMs: 1_000, unavailableRetryDelaysMs: [] },
    );

    const firstResult = pool.compress("system", "first");
    const secondResult = pool.compress("system", "second");
    await vi.advanceTimersByTimeAsync(0);

    expect(gemini).toHaveBeenCalledTimes(1);
    await expect(firstResult).resolves.toBe("first-result");

    await vi.advanceTimersByTimeAsync(999);
    expect(gemini).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    await expect(secondResult).resolves.toBe("second-result");
    expect(gemini).toHaveBeenCalledTimes(2);
    expect(fallback).not.toHaveBeenCalled();
  });

  it("retries temporary Gemini unavailability without using Fireworks", async () => {
    const unavailable = new OpenRouterProviderError("gemini", 503, "unavailable");
    const gemini = vi.fn()
      .mockRejectedValueOnce(unavailable)
      .mockRejectedValueOnce(unavailable)
      .mockRejectedValueOnce(unavailable)
      .mockRejectedValueOnce(unavailable)
      .mockResolvedValue("google-response");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new GeminiAccountPoolProvider(
      [provider("gemini", gemini)],
      provider("fireworks", fallback),
      { minimumRequestIntervalMs: 0, unavailableRetryDelaysMs: [0, 0, 0, 0] },
    );

    await expect(pool.compress("system", "user")).resolves.toBe("google-response");
    expect(gemini).toHaveBeenCalledTimes(5);
    expect(fallback).not.toHaveBeenCalled();
  });

  it("uses the terminal fallback without rotating accounts after bounded 503 retries", async () => {
    const unavailable = vi.fn().mockRejectedValue(
      new OpenRouterProviderError("gemini", 503, "unavailable"),
    );
    const second = vi.fn().mockResolvedValue("second-account");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new GeminiAccountPoolProvider(
      [provider("first", unavailable), provider("second", second)],
      provider("fireworks", fallback),
      { minimumRequestIntervalMs: 0, unavailableRetryDelaysMs: [0, 0, 0, 0] },
    );

    await expect(pool.compress("system", "user")).resolves.toBe("fireworks");
    expect(unavailable).toHaveBeenCalledTimes(5);
    expect(second).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });
});

describe("Gemini account directory configuration", () => {
  it("takes precedence over an OpenAI-compatible Fireworks key", () => {
    process.env["AGENTMEMORY_GEMINI_ACCOUNTS_DIR"] = "C:\\gemini-accounts";
    process.env["OPENAI_API_KEY"] = "fireworks-key";
    process.env["OPENAI_BASE_URL"] = "https://api.fireworks.ai/inference/v1";
    process.env["OPENAI_MODEL"] = "accounts/example/models/example";
    process.env["GEMINI_MODEL"] = "gemini-3.6-flash";

    expect(loadConfig().provider).toMatchObject({
      provider: "gemini",
      model: "gemini-3.6-flash",
      geminiAccountsDir: "C:\\gemini-accounts",
    });
  });

  it("constructs the account pool with the configured Fireworks fallback", () => {
    const directory = temporaryDirectory();
    writeFileSync(join(directory, "account.json"), JSON.stringify({
      apiKey: "gemini-key",
      project: "projects/1",
    }));
    process.env["FIREWORKS_API_KEY"] = "fireworks-key";
    process.env["FIREWORKS_MODEL"] = "accounts/example/models/example";

    const providerInstance = createProvider({
      provider: "gemini",
      model: "gemini-3.6-flash",
      maxTokens: 4_096,
      geminiAccountsDir: directory,
    });

    expect(providerInstance.name).toBe("resilient(gemini-pool(1) -> openai)");
  });

  it("inserts the two-key OpenRouter free pool before Fireworks", () => {
    const directory = temporaryDirectory();
    const keysFile = join(temporaryDirectory(), "openrouter-keys.json");
    writeFileSync(join(directory, "account.json"), JSON.stringify({
      apiKey: "gemini-key",
      project: "projects/1",
    }));
    writeFileSync(keysFile, JSON.stringify([
      "sk-or-v1-first",
      "sk-or-v1-second",
      "sk-or-v1-third",
    ]));
    process.env["FIREWORKS_API_KEY"] = "fireworks-key";
    process.env["FIREWORKS_MODEL"] = "accounts/example/models/example";

    const providerInstance = createProvider({
      provider: "gemini",
      model: "gemini-3.6-flash",
      maxTokens: 4_096,
      geminiAccountsDir: directory,
      openRouterKeysFile: keysFile,
    });

    expect(providerInstance.name).toBe(
      "resilient(gemini-pool(1) -> openrouter-pool(3, sample=2) -> openai)",
    );
  });
});
