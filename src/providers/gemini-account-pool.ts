import { randomInt } from "node:crypto";
import {
  readdirSync,
  readFileSync,
  statSync,
} from "node:fs";
import { join, resolve } from "node:path";
import { logger } from "../logger.js";
import type { LlmCallOptions, MemoryProvider } from "../types.js";
import { OpenRouterProviderError } from "./openrouter.js";

const MAX_ACCOUNT_FILES = 64;
const MAX_ACCOUNT_FILE_BYTES = 64 * 1024;
const MAX_FIELD_LENGTH = 512;
const DEFAULT_MINIMUM_REQUEST_INTERVAL_MS = 1_000;
const DEFAULT_UNAVAILABLE_RETRY_DELAYS_MS = [2_000, 4_000, 8_000, 16_000] as const;

export interface GeminiAccountConfig {
  apiKey: string;
  name?: string;
  project?: string;
  model: string;
}

export interface GeminiAccountPoolOptions {
  minimumRequestIntervalMs?: number;
  unavailableRetryDelaysMs?: readonly number[];
}

type RandomIndex = (exclusiveMaximum: number) => number;

function shuffleAccounts(
  accounts: readonly GeminiAccountConfig[],
  randomIndex: RandomIndex,
): GeminiAccountConfig[] {
  const shuffled = [...accounts];
  for (let currentIndex = shuffled.length - 1; currentIndex > 0; currentIndex -= 1) {
    const swapIndex = randomIndex(currentIndex + 1);
    const currentAccount = shuffled[currentIndex]!;
    shuffled[currentIndex] = shuffled[swapIndex]!;
    shuffled[swapIndex] = currentAccount;
  }
  return shuffled;
}

function optionalString(
  value: unknown,
  field: string,
  fileName: string,
): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`Gemini account ${fileName}: ${field} must be a non-empty string.`);
  }
  const normalized = value.trim();
  if (normalized.length > MAX_FIELD_LENGTH) {
    throw new Error(`Gemini account ${fileName}: ${field} is too long.`);
  }
  return normalized;
}

function parseAccount(
  text: string,
  fileName: string,
  defaultModel: string,
): GeminiAccountConfig {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch {
    throw new Error(`Gemini account ${fileName}: invalid JSON.`);
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`Gemini account ${fileName}: expected a JSON object.`);
  }
  const record = value as Record<string, unknown>;
  const apiKey = optionalString(record["apiKey"], "apiKey", fileName);
  if (!apiKey) {
    throw new Error(`Gemini account ${fileName}: apiKey is required.`);
  }
  if (apiKey.length > 2_048) {
    throw new Error(`Gemini account ${fileName}: apiKey is too long.`);
  }
  return {
    apiKey,
    name: optionalString(record["name"], "name", fileName),
    project: optionalString(record["project"], "project", fileName),
    model: optionalString(record["model"], "model", fileName) ?? defaultModel,
  };
}

export function loadGeminiAccounts(
  directory: string,
  defaultModel: string,
  randomIndex: RandomIndex = (exclusiveMaximum) => randomInt(exclusiveMaximum),
): GeminiAccountConfig[] {
  const resolvedDirectory = resolve(directory);
  const entries = readdirSync(resolvedDirectory, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".json"))
    .sort((left, right) => left.name.localeCompare(right.name));

  if (entries.length === 0) {
    throw new Error(`No Gemini account JSON files found in ${resolvedDirectory}.`);
  }
  if (entries.length > MAX_ACCOUNT_FILES) {
    throw new Error(`Gemini account directory contains more than ${MAX_ACCOUNT_FILES} JSON files.`);
  }

  const seenKeys = new Set<string>();
  const accounts = entries.map((entry) => {
    const filePath = join(resolvedDirectory, entry.name);
    if (statSync(filePath).size > MAX_ACCOUNT_FILE_BYTES) {
      throw new Error(`Gemini account ${entry.name}: file exceeds ${MAX_ACCOUNT_FILE_BYTES} bytes.`);
    }
    const account = parseAccount(readFileSync(filePath, "utf8"), entry.name, defaultModel);
    if (seenKeys.has(account.apiKey)) {
      throw new Error(`Gemini account ${entry.name}: duplicate apiKey.`);
    }
    seenKeys.add(account.apiKey);
    return account;
  });
  return shuffleAccounts(accounts, randomIndex);
}

export class GeminiAccountPoolProvider implements MemoryProvider {
  readonly name: string;
  private activeAccountIndex = 0;
  private fallbackOnly = false;
  private operationTail: Promise<void> = Promise.resolve();
  private nextGeminiRequestAt = 0;
  private readonly minimumRequestIntervalMs: number;
  private readonly unavailableRetryDelaysMs: readonly number[];

  constructor(
    private readonly accounts: readonly MemoryProvider[],
    private readonly terminalFallback: MemoryProvider,
    options: GeminiAccountPoolOptions = {},
  ) {
    if (accounts.length === 0) {
      throw new Error("At least one Gemini account provider is required.");
    }
    this.name = `gemini-pool(${accounts.length}) -> ${terminalFallback.name}`;
    this.minimumRequestIntervalMs = normalizeDelay(
      options.minimumRequestIntervalMs ?? DEFAULT_MINIMUM_REQUEST_INTERVAL_MS,
    );
    this.unavailableRetryDelaysMs = (
      options.unavailableRetryDelaysMs ?? DEFAULT_UNAVAILABLE_RETRY_DELAYS_MS
    ).map(normalizeDelay);
  }

  compress(
    systemPrompt: string,
    userPrompt: string,
    options?: LlmCallOptions,
  ): Promise<string> {
    return this.enqueue((provider) => provider.compress(systemPrompt, userPrompt, options));
  }

  summarize(
    systemPrompt: string,
    userPrompt: string,
    options?: LlmCallOptions,
  ): Promise<string> {
    return this.enqueue((provider) => provider.summarize(systemPrompt, userPrompt, options));
  }

  private enqueue(operation: (provider: MemoryProvider) => Promise<string>): Promise<string> {
    const result = this.operationTail.then(() => this.run(operation));
    this.operationTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async run(operation: (provider: MemoryProvider) => Promise<string>): Promise<string> {
    while (!this.fallbackOnly && this.activeAccountIndex < this.accounts.length) {
      const accountIndex = this.activeAccountIndex;
      let unavailableRetryIndex = 0;

      while (true) {
        await this.waitForGeminiSlot();
        try {
          return await operation(this.accounts[accountIndex]);
        } catch (error) {
          if (
            error instanceof OpenRouterProviderError
            && error.status === 503
          ) {
            if (unavailableRetryIndex < this.unavailableRetryDelaysMs.length) {
              const retryDelayMs = this.unavailableRetryDelaysMs[unavailableRetryIndex]!;
              unavailableRetryIndex += 1;
              logger.warn("Gemini temporarily unavailable; retrying current account", {
                accountIndex: accountIndex + 1,
                accountCount: this.accounts.length,
                retryAttempt: unavailableRetryIndex,
                retryDelayMs,
              });
              await delay(retryDelayMs);
              continue;
            }
            logger.warn("Gemini remains unavailable; using terminal fallback for this request", {
              accountIndex: accountIndex + 1,
              accountCount: this.accounts.length,
              attempts: unavailableRetryIndex + 1,
            });
            return operation(this.terminalFallback);
          }
          if (!(error instanceof OpenRouterProviderError) || error.status !== 429) {
            throw error;
          }
          if (this.activeAccountIndex === accountIndex) {
            this.activeAccountIndex += 1;
            logger.warn("Gemini account quota exhausted; advancing account pool", {
              accountIndex: accountIndex + 1,
              accountCount: this.accounts.length,
            });
          }
          break;
        }
      }
    }

    if (!this.fallbackOnly) {
      logger.warn("Gemini account pool exhausted; switching permanently to terminal fallback", {
        accountCount: this.accounts.length,
      });
    }
    this.fallbackOnly = true;
    return operation(this.terminalFallback);
  }

  private async waitForGeminiSlot(): Promise<void> {
    const waitMs = Math.max(0, this.nextGeminiRequestAt - Date.now());
    await delay(waitMs);
    this.nextGeminiRequestAt = Date.now() + this.minimumRequestIntervalMs;
  }
}

function normalizeDelay(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.floor(value));
}

function delay(milliseconds: number): Promise<void> {
  if (milliseconds === 0) return Promise.resolve();
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}
