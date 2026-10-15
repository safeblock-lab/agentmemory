import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MemoryProvider } from "../src/types.js";
import { FallbackChainProvider } from "../src/providers/fallback-chain.js";
import { fetchWithTimeout } from "../src/providers/_fetch.js";
import { getProviderAttemptBudgetReceipt, withProviderAttemptBudget } from "../src/providers/provider-attempt-budget.js";

const artifactRoot = join(process.cwd(), ".codex-task", "full-closure", "normal-provider-budget-integration");
const budgetEnv = "AGENTMEMORY_PROVIDER_ATTEMPT_BUDGET_CONFIG";
const primaryUrl = "https://primary.test/v1/chat";
const auxiliaryUrl = "https://auxiliary.test/v1/chat";
let tempDir: string;

function createBudget(operationCeiling: number, windowCeiling: number): string {
  mkdirSync(artifactRoot, { recursive: true, mode: 0o700 });
  tempDir = mkdtempSync(join(artifactRoot, "fetch-test-"));
  const ledgerPath = join(tempDir, "attempts.jsonl");
  const now = Date.now();
  const startsAt = new Date(now - 60_000).toISOString();
  const expiresAt = new Date(now + 60_000).toISOString();
  writeFileSync(
    ledgerPath,
    `${JSON.stringify({
      type: "window_opened",
      version: 1,
      windowId: "fetch-window",
      startsAt,
      expiresAt,
      windowCeiling,
      at: new Date().toISOString(),
    })}\n`,
    { mode: 0o600 },
  );
  const configPath = join(tempDir, "budget.json");
  writeFileSync(
    configPath,
    JSON.stringify({
      version: 1,
      window: { id: "fetch-window", startsAt, expiresAt, ceiling: windowCeiling },
      ledgerPath,
      allowedEndpoints: [`POST ${primaryUrl}`, `POST ${auxiliaryUrl}`],
      operations: [{ id: "fetch-attempt", ceiling: operationCeiling }],
    }),
    { mode: 0o600 },
  );
  vi.stubEnv(budgetEnv, configPath);
  return ledgerPath;
}

function binding(ceiling: number) {
  return { operationId: "fetch-attempt", windowId: "fetch-window", ceiling };
}

function removeTestDirectory(): void {
  if (!tempDir) return;
  const root = `${resolve(artifactRoot)}${sep}`;
  const target = resolve(tempDir);
  if (!target.startsWith(root)) throw new Error("provider budget test path escaped its artifact directory");
  rmSync(target, { recursive: true, force: true });
  tempDir = "";
}

function fetchProvider(name: string, url: string): MemoryProvider {
  const call = async (): Promise<string> => {
    const response = await fetchWithTimeout(url, {
      method: "POST",
      headers: { Authorization: "Bearer provider-secret" },
      body: "raw user prompt secret",
    }, 5000);
    if (!response.ok) throw new Error(`${name} unavailable`);
    return `${name} result`;
  };
  return { name, compress: call, summarize: call };
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  removeTestDirectory();
});

describe("provider budget at raw fetch", () => {
  it("counts every retry and the auxiliary fallback fetch while keeping payloads out of the ledger", async () => {
    const ledgerPath = createBudget(4, 4);
    const calls: Array<{ url: string; redirect?: RequestRedirect; body?: BodyInit | null }> = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      calls.push({ url, redirect: init?.redirect, body: init?.body });
      return url === primaryUrl
        ? new Response(null, { status: 503, headers: { "Retry-After": "0" } })
        : new Response(null, { status: 200 });
    }));
    const chain = new FallbackChainProvider([
      fetchProvider("primary", primaryUrl),
      fetchProvider("auxiliary", auxiliaryUrl),
    ]);

    const result = await withProviderAttemptBudget(binding(4), () => chain.compress("private system prompt", "private user prompt"));

    expect(result).toBe("auxiliary result");
    expect(calls.map((call) => call.url)).toEqual([primaryUrl, primaryUrl, primaryUrl, auxiliaryUrl]);
    expect(calls.every((call) => call.redirect === "manual")).toBe(true);
    expect(calls.every((call) => call.body === "raw user prompt secret")).toBe(true);
    expect(getProviderAttemptBudgetReceipt("fetch-attempt")).toMatchObject({ state: "completed", consumed: 4, windowConsumed: 4 });
    expect(readFileSync(ledgerPath, "utf8")).not.toMatch(/private system prompt|private user prompt|raw user prompt|provider-secret|Authorization/i);
  }, 30_000);

  it("denies an auxiliary fetch when retries have consumed the remaining ceiling", async () => {
    const ledgerPath = createBudget(3, 3);
    const urls: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      urls.push(url);
      return new Response(null, { status: url === primaryUrl ? 503 : 200, headers: { "Retry-After": "0" } });
    }));
    const chain = new FallbackChainProvider([
      fetchProvider("primary", primaryUrl),
      fetchProvider("auxiliary", auxiliaryUrl),
    ]);

    await expect(withProviderAttemptBudget(binding(3), () => chain.compress("prompt", "prompt")))
      .rejects.toMatchObject({ code: "operation_exhausted" });

    expect(urls).toEqual([primaryUrl, primaryUrl, primaryUrl]);
    expect(getProviderAttemptBudgetReceipt("fetch-attempt")).toMatchObject({ state: "unknown", consumed: 3, remaining: 0 });
    expect(readFileSync(ledgerPath, "utf8")).not.toMatch(/prompt|provider-secret|Authorization/i);
  }, 30_000);

  it("does not consume a slot for a request already aborted before dispatch", async () => {
    createBudget(3, 3);
    const dispatch = vi.fn();
    vi.stubGlobal("fetch", dispatch);
    const controller = new AbortController();
    controller.abort();

    await expect(withProviderAttemptBudget(binding(3), () => fetchWithTimeout(primaryUrl, {
      method: "POST",
      signal: controller.signal,
    }, 1000))).rejects.toMatchObject({ name: "AbortError" });

    expect(dispatch).not.toHaveBeenCalled();
    expect(getProviderAttemptBudgetReceipt("fetch-attempt")).toMatchObject({ state: "unknown", consumed: 0 });
  }, 30_000);

  it("preserves retry behavior for ordinary unscoped provider calls", async () => {
    const dispatch = vi.fn()
      .mockResolvedValueOnce(new Response(null, { status: 429, headers: { "Retry-After": "0" } }))
      .mockResolvedValueOnce(new Response(null, { status: 200 }));
    vi.stubGlobal("fetch", dispatch);

    const response = await fetchWithTimeout(primaryUrl, { method: "POST" }, 3000);

    expect(response.status).toBe(200);
    expect(dispatch).toHaveBeenCalledTimes(2);
    expect(dispatch.mock.calls[0]?.[1]?.redirect).toBeUndefined();
  }, 30_000);
});
