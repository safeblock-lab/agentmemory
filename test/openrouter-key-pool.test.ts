import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  loadOpenRouterKeys,
  OpenRouterKeyPoolProvider,
} from "../src/providers/openrouter-key-pool.js";
import { OpenRouterProviderError } from "../src/providers/openrouter.js";
import type { MemoryProvider } from "../src/types.js";

const temporaryDirectories: string[] = [];

function temporaryDirectory(): string {
  const directory = mkdtempSync(join(tmpdir(), "agentmemory-openrouter-"));
  temporaryDirectories.push(directory);
  return directory;
}

function provider(name: string, operation: () => Promise<string>): MemoryProvider {
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
});

describe("loadOpenRouterKeys", () => {
  it("loads a JSON array without exposing keys", () => {
    const filePath = join(temporaryDirectory(), "openrouter-keys.json");
    writeFileSync(filePath, JSON.stringify([
      "sk-or-v1-first",
      "sk-or-v1-second",
    ]));

    expect(loadOpenRouterKeys(filePath)).toEqual([
      "sk-or-v1-first",
      "sk-or-v1-second",
    ]);
  });

  it("rejects malformed files without exposing their contents", () => {
    const filePath = join(temporaryDirectory(), "openrouter-keys.json");
    writeFileSync(filePath, "not-json-with-secret");

    expect(() => loadOpenRouterKeys(filePath))
      .toThrow("OpenRouter key file openrouter-keys.json contains invalid JSON.");
  });

  it("rejects duplicate keys", () => {
    const filePath = join(temporaryDirectory(), "openrouter-keys.json");
    writeFileSync(filePath, JSON.stringify([
      "sk-or-v1-duplicate",
      "sk-or-v1-duplicate",
    ]));

    expect(() => loadOpenRouterKeys(filePath))
      .toThrow("OpenRouter key file openrouter-keys.json contains duplicate keys.");
  });
});

describe("OpenRouterKeyPoolProvider", () => {
  it("tries two distinct randomly sampled accounts", async () => {
    const first = vi.fn().mockResolvedValue("first");
    const second = vi.fn().mockResolvedValue("second");
    const third = vi.fn().mockRejectedValue(
      new OpenRouterProviderError("openrouter", 429, "quota"),
    );
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const randomIndex = vi.fn()
      .mockReturnValueOnce(2)
      .mockReturnValueOnce(0);
    const pool = new OpenRouterKeyPoolProvider(
      [provider("first", first), provider("second", second), provider("third", third)],
      provider("fireworks", fallback),
      2,
      randomIndex,
    );

    await expect(pool.compress("system", "user")).resolves.toBe("first");
    expect(third).toHaveBeenCalledTimes(1);
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).not.toHaveBeenCalled();
    expect(fallback).not.toHaveBeenCalled();
    expect(randomIndex).toHaveBeenNthCalledWith(1, 3);
    expect(randomIndex).toHaveBeenNthCalledWith(2, 2);
  });

  it("uses Fireworks only after both sampled accounts fail", async () => {
    const unavailable = () => Promise.reject(
      new OpenRouterProviderError("openrouter", 429, "quota"),
    );
    const first = vi.fn(unavailable);
    const second = vi.fn(unavailable);
    const third = vi.fn().mockResolvedValue("not-selected");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const pool = new OpenRouterKeyPoolProvider(
      [provider("first", first), provider("second", second), provider("third", third)],
      provider("fireworks", fallback),
      2,
      vi.fn().mockReturnValue(0),
    );

    await expect(pool.summarize("system", "user")).resolves.toBe("fireworks");
    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
    expect(third).not.toHaveBeenCalled();
    expect(fallback).toHaveBeenCalledTimes(1);
  });

  it("samples again for each operation", async () => {
    const first = vi.fn().mockResolvedValue("first");
    const second = vi.fn().mockResolvedValue("second");
    const fallback = vi.fn().mockResolvedValue("fireworks");
    const randomIndex = vi.fn()
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(1)
      .mockReturnValueOnce(0);
    const pool = new OpenRouterKeyPoolProvider(
      [provider("first", first), provider("second", second)],
      provider("fireworks", fallback),
      2,
      randomIndex,
    );

    await expect(pool.compress("system", "first")).resolves.toBe("first");
    await expect(pool.compress("system", "second")).resolves.toBe("second");
    expect(randomIndex).toHaveBeenCalledTimes(4);
    expect(fallback).not.toHaveBeenCalled();
  });
});
