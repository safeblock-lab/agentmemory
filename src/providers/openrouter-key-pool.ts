import { randomInt } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { basename } from "node:path";
import { logger } from "../logger.js";
import type { LlmCallOptions, MemoryProvider } from "../types.js";
import { OpenRouterProviderError } from "./openrouter.js";

const MAX_KEY_FILE_BYTES = 128 * 1024;
const MAX_KEYS = 128;
const MAX_KEY_LENGTH = 512;

type RandomIndex = (exclusiveMaximum: number) => number;

export function loadOpenRouterKeys(filePath: string): string[] {
  const fileName = basename(filePath);
  if (statSync(filePath).size > MAX_KEY_FILE_BYTES) {
    throw new Error(`OpenRouter key file ${fileName} exceeds ${MAX_KEY_FILE_BYTES} bytes.`);
  }

  let value: unknown;
  try {
    value = JSON.parse(readFileSync(filePath, "utf8"));
  } catch {
    throw new Error(`OpenRouter key file ${fileName} contains invalid JSON.`);
  }
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(`OpenRouter key file ${fileName} must contain a non-empty JSON array.`);
  }
  if (value.length > MAX_KEYS) {
    throw new Error(`OpenRouter key file ${fileName} contains more than ${MAX_KEYS} keys.`);
  }

  const keys = value.map((key, index) => {
    if (
      typeof key !== "string"
      || key.length > MAX_KEY_LENGTH
      || !/^sk-or-v1-[A-Za-z0-9_-]+$/.test(key)
    ) {
      throw new Error(`OpenRouter key file ${fileName} has an invalid key at index ${index}.`);
    }
    return key;
  });
  if (new Set(keys).size !== keys.length) {
    throw new Error(`OpenRouter key file ${fileName} contains duplicate keys.`);
  }
  return keys;
}

export class OpenRouterKeyPoolProvider implements MemoryProvider {
  readonly name: string;
  private readonly sampleSize: number;

  constructor(
    private readonly accounts: readonly MemoryProvider[],
    private readonly terminalFallback: MemoryProvider,
    sampleSize = 2,
    private readonly randomIndex: RandomIndex = (exclusiveMaximum) => randomInt(exclusiveMaximum),
  ) {
    if (accounts.length === 0) {
      throw new Error("At least one OpenRouter account provider is required.");
    }
    if (!Number.isInteger(sampleSize) || sampleSize < 1) {
      throw new Error("OpenRouter account sample size must be a positive integer.");
    }
    this.sampleSize = Math.min(accounts.length, sampleSize);
    this.name = `openrouter-pool(${accounts.length}, sample=${this.sampleSize}) -> ${terminalFallback.name}`;
  }

  compress(
    systemPrompt: string,
    userPrompt: string,
    options?: LlmCallOptions,
  ): Promise<string> {
    return this.run((provider) => provider.compress(systemPrompt, userPrompt, options));
  }

  summarize(
    systemPrompt: string,
    userPrompt: string,
    options?: LlmCallOptions,
  ): Promise<string> {
    return this.run((provider) => provider.summarize(systemPrompt, userPrompt, options));
  }

  private async run(operation: (provider: MemoryProvider) => Promise<string>): Promise<string> {
    const sampledAccounts = this.sampleAccounts();
    for (let attempt = 0; attempt < sampledAccounts.length; attempt += 1) {
      try {
        return await operation(sampledAccounts[attempt]!);
      } catch (error) {
        logger.warn("OpenRouter free account failed; trying the next sampled account", {
          attempt: attempt + 1,
          sampleSize: sampledAccounts.length,
          ...(error instanceof OpenRouterProviderError ? { status: error.status } : {}),
        });
      }
    }

    logger.warn("OpenRouter free sample exhausted; using Fireworks", {
      sampleSize: sampledAccounts.length,
    });
    return operation(this.terminalFallback);
  }

  private sampleAccounts(): MemoryProvider[] {
    const available = [...this.accounts];
    const sampled: MemoryProvider[] = [];
    while (sampled.length < this.sampleSize) {
      const selectedIndex = this.randomIndex(available.length);
      sampled.push(available.splice(selectedIndex, 1)[0]!);
    }
    return sampled;
  }
}
