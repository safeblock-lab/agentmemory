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
    );

    await expect(pool.compress("system", "user")).rejects.toMatchObject({ status: 401 });
    expect(second).not.toHaveBeenCalled();
    expect(fallback).not.toHaveBeenCalled();
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
});
