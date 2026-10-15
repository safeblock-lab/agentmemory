import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  consumeProviderAttempt,
  getProviderAttemptBudgetReceipt,
  ProviderAttemptBudgetError,
  withProviderAttemptBudget,
} from "../src/providers/provider-attempt-budget.js";
import { fetchWithTimeout } from "../src/providers/_fetch.js";

const artifactRoot = join(process.cwd(), ".codex-task", "full-closure", "normal-provider-budget-integration");
const budgetEnv = "AGENTMEMORY_PROVIDER_ATTEMPT_BUDGET_CONFIG";
const endpoint = "https://provider.test/v1/run";
let tempDir: string;
let ledgerPath: string;

function createBudget(options: {
  windowId?: string;
  windowCeiling?: number;
  operations?: Array<{ id: string; ceiling: number }>;
  endpoints?: string[];
} = {}): string {
  mkdirSync(artifactRoot, { recursive: true, mode: 0o700 });
  tempDir = mkdtempSync(join(artifactRoot, "budget-test-"));
  ledgerPath = join(tempDir, "attempts.jsonl");
  const now = Date.now();
  const startsAt = new Date(now - 60_000).toISOString();
  const expiresAt = new Date(now + 60_000).toISOString();
  const windowId = options.windowId ?? "test-window";
  const windowCeiling = options.windowCeiling ?? 4;
  writeFileSync(
    ledgerPath,
    `${JSON.stringify({
      type: "window_opened",
      version: 1,
      windowId,
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
      window: { id: windowId, startsAt, expiresAt, ceiling: windowCeiling },
      ledgerPath,
      allowedEndpoints: options.endpoints ?? [`POST ${endpoint}`],
      operations: options.operations ?? [{ id: "attempt-a", ceiling: 2 }],
    }),
    { mode: 0o600 },
  );
  vi.stubEnv(budgetEnv, configPath);
  return configPath;
}

function binding(operationId = "attempt-a", ceiling = 2, windowId = "test-window") {
  return { operationId, windowId, ceiling };
}

function removeTestDirectory(): void {
  if (!tempDir) return;
  const root = `${resolve(artifactRoot)}${sep}`;
  const target = resolve(tempDir);
  if (!target.startsWith(root)) throw new Error("provider budget test path escaped its artifact directory");
  rmSync(target, { recursive: true, force: true });
  tempDir = "";
}

function runChild(script: string, env: NodeJS.ProcessEnv = process.env): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e", script], {
      cwd: process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => child.kill(), 20_000);
    child.stdout.setEncoding("utf8").on("data", (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk: string) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code) => {
      clearTimeout(timeout);
      resolve({ code, stdout, stderr });
    });
  });
}

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  removeTestDirectory();
});

describe("withProviderAttemptBudget", () => {
  it("uses exact operator binding, consumes each attempt once, and never reopens an operation", async () => {
    const configPath = createBudget({
      windowCeiling: 4,
      operations: [
        { id: "attempt-a", ceiling: 2 },
        { id: "attempt-b", ceiling: 2 },
      ],
    });
    const mismatchedRun = vi.fn(async () => "must not run");
    await expect(withProviderAttemptBudget(binding("attempt-a", 1), mismatchedRun))
      .rejects.toMatchObject({ code: "binding_mismatch" });
    expect(mismatchedRun).not.toHaveBeenCalled();

    const mutableBinding = binding();
    const value = await withProviderAttemptBudget(mutableBinding, async () => {
      mutableBinding.operationId = "spoofed-operation";
      vi.stubEnv(budgetEnv, join(tempDir, "unavailable-config.json"));
      expect(consumeProviderAttempt(endpoint, "POST")).toBe(true);
      expect(consumeProviderAttempt(endpoint, "POST")).toBe(true);
      return "done";
    });
    vi.stubEnv(budgetEnv, configPath);
    expect(value).toBe("done");
    const receipt = getProviderAttemptBudgetReceipt("attempt-a");
    expect(receipt).toMatchObject({ state: "completed", consumed: 2, remaining: 0, windowConsumed: 2 });

    const replay = vi.fn(async () => "replayed");
    await expect(withProviderAttemptBudget(binding(), replay)).rejects.toMatchObject({ code: "operation_already_claimed" });
    expect(replay).not.toHaveBeenCalled();
    expect(getProviderAttemptBudgetReceipt("attempt-a")).toMatchObject({ consumed: 2, windowConsumed: 2 });

    await expect(withProviderAttemptBudget(binding("attempt-b"), async () => {
      consumeProviderAttempt(endpoint, "POST");
      throw new Error("provider outcome unknown");
    })).rejects.toThrow("provider outcome unknown");
    expect(getProviderAttemptBudgetReceipt("attempt-b")).toMatchObject({ state: "unknown", consumed: 1, remaining: 1 });
    expect(readFileSync(ledgerPath, "utf8")).not.toMatch(/provider outcome unknown|prompt|secret|authorization/i);
  }, 30_000);

  it("fails closed on an unapproved endpoint before dispatch or spend", async () => {
    createBudget();
    const dispatch = vi.fn();
    await expect(withProviderAttemptBudget(binding(), async () => {
      vi.stubGlobal("fetch", dispatch);
      await fetchWithTimeout("https://other.test/v1/run", { method: "POST" }, 1000);
    })).rejects.toBeInstanceOf(ProviderAttemptBudgetError);
    expect(dispatch).not.toHaveBeenCalled();
    expect(getProviderAttemptBudgetReceipt("attempt-a")).toMatchObject({ state: "unknown", consumed: 0 });
  }, 30_000);

  it("keeps a durably consumed slot after a process exits before recording completion", async () => {
    createBudget();
    const script = `
      import { withProviderAttemptBudget, consumeProviderAttempt } from "./src/providers/provider-attempt-budget.ts";
      await withProviderAttemptBudget({ operationId: "attempt-a", windowId: "test-window", ceiling: 2 }, async () => {
        consumeProviderAttempt(${JSON.stringify(endpoint)}, "POST");
        process.exit(0);
      });
    `;
    const result = await runChild(script);
    expect(result.code).toBe(0);
    expect(result.stderr).toBe("");
    expect(getProviderAttemptBudgetReceipt("attempt-a")).toMatchObject({ state: "started", consumed: 1, remaining: 1 });
    await expect(withProviderAttemptBudget(binding(), async () => "must not run"))
      .rejects.toMatchObject({ code: "operation_already_claimed" });
    expect(getProviderAttemptBudgetReceipt("attempt-a")).toMatchObject({ consumed: 1, windowConsumed: 1 });
  }, 30_000);

  it("admits only the bounded number of actual dispatches across separate processes", async () => {
    createBudget({
      windowCeiling: 1,
      operations: [
        { id: "attempt-a", ceiling: 1 },
        { id: "attempt-b", ceiling: 1 },
      ],
    });
    const readyA = join(tempDir, "ready-a");
    const readyB = join(tempDir, "ready-b");
    const dispatches = join(tempDir, "dispatches");
    const childScript = (operationId: string, ownReady: string, peerReady: string) => `
      import { existsSync, appendFileSync, writeFileSync } from "node:fs";
      import { withProviderAttemptBudget, ProviderAttemptBudgetError } from "./src/providers/provider-attempt-budget.ts";
      import { fetchWithTimeout } from "./src/providers/_fetch.ts";
      globalThis.fetch = async () => {
        appendFileSync(${JSON.stringify(dispatches)}, ${JSON.stringify(`${operationId}\n`)});
        return new Response(null, { status: 200 });
      };
      try {
        await withProviderAttemptBudget({ operationId: ${JSON.stringify(operationId)}, windowId: "test-window", ceiling: 1 }, async () => {
          writeFileSync(${JSON.stringify(ownReady)}, "ready");
          const deadline = Date.now() + 10000;
          while (!existsSync(${JSON.stringify(peerReady)})) {
            if (Date.now() > deadline) throw new Error("barrier timeout");
            await new Promise((resolve) => setTimeout(resolve, 5));
          }
          await fetchWithTimeout(${JSON.stringify(endpoint)}, { method: "POST" }, 3000);
        });
        process.stdout.write("dispatched");
      } catch (error) {
        process.stdout.write(error instanceof ProviderAttemptBudgetError ? "denied:" + error.code : "unexpected");
        if (!(error instanceof ProviderAttemptBudgetError)) process.exitCode = 2;
      }
    `;

    const childA = runChild(childScript("attempt-a", readyA, readyB));
    const readyDeadline = Date.now() + 10_000;
    while (!existsSync(readyA) && Date.now() < readyDeadline) await new Promise((resolve) => setTimeout(resolve, 5));
    expect(existsSync(readyA)).toBe(true);
    const childB = runChild(childScript("attempt-b", readyB, readyA));
    const [a, b] = await Promise.all([childA, childB]);
    expect([a.code, b.code]).toEqual([0, 0]);
    expect([a.stdout, b.stdout].filter((value) => value === "dispatched")).toHaveLength(1);
    expect([a.stdout, b.stdout].filter((value) => value.startsWith("denied:")).length).toBe(1);
    expect([a.stdout, b.stdout]).not.toContain("unexpected");
    const actualDispatches = readFileSync(dispatches, "utf8").trim().split("\n").filter(Boolean);
    expect(actualDispatches).toHaveLength(1);
    expect(getProviderAttemptBudgetReceipt("attempt-a")?.windowConsumed).toBe(1);
    const receiptA = getProviderAttemptBudgetReceipt("attempt-a");
    const receiptB = getProviderAttemptBudgetReceipt("attempt-b");
    expect((receiptA?.consumed ?? 0) + (receiptB?.consumed ?? 0)).toBe(1);
  }, 30_000);
});
