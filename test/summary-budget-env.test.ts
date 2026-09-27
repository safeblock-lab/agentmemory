import { afterEach, beforeEach, expect, it, vi } from "vitest";

vi.mock("node:os", async importOriginal => ({ ...await importOriginal<typeof import("node:os")>(), homedir: () => "D:/mock-summary-home" }));
vi.mock("node:fs", async importOriginal => {
  const original = await importOriginal<typeof import("node:fs")>();
  return {
    ...original,
    existsSync: (path: string) => String(path).replaceAll("\\", "/") === "D:/mock-summary-home/.agentmemory/.env" || original.existsSync(path),
    readFileSync: (path: string, options: unknown) => String(path).replaceAll("\\", "/") === "D:/mock-summary-home/.agentmemory/.env"
      ? "AGENTMEMORY_SUMMARY_CONTEXT_TOKENS=4096\nAGENTMEMORY_SUMMARY_OUTPUT_TOKENS=1024\nAGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS=256\nSUMMARIZE_CHUNK_SIZE=7\nSUMMARIZE_CHUNK_CONCURRENCY=2"
      : Reflect.apply(original.readFileSync, original, [path, options]),
  };
});
import { __resetEnvFileCache, getSummaryBudgetConfig } from "../src/config.js";
beforeEach(() => {
  __resetEnvFileCache();
  for (const key of ["AGENTMEMORY_SUMMARY_CONTEXT_TOKENS", "AGENTMEMORY_SUMMARY_OUTPUT_TOKENS", "AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS", "SUMMARIZE_CHUNK_SIZE", "SUMMARIZE_CHUNK_CONCURRENCY"]) vi.stubEnv(key, undefined);
});
afterEach(() => vi.unstubAllEnvs());
it("honors home .env without copying settings into process.env; process wins", () => {
  expect(getSummaryBudgetConfig()).toEqual({ contextTokens: 4096, outputTokens: 1024, safetyMarginTokens: 256, chunkSize: 7, concurrency: 2 });
  vi.stubEnv("AGENTMEMORY_SUMMARY_CONTEXT_TOKENS", "8192");
  expect(getSummaryBudgetConfig().contextTokens).toBe(8192);
});
