import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";

const source = readFileSync("src/cli.ts", "utf8").replace(/\r\n/g, "\n");
const start = source.indexOf("async function waitForAgentmemoryReady(timeoutMs: number): Promise<boolean> {");
const end = source.indexOf("\n}", start) + 2;
const readinessFunction = source
  .slice(start, end)
  .replace(
    "async function waitForAgentmemoryReady(timeoutMs: number): Promise<boolean>",
    "async function waitForAgentmemoryReady(timeoutMs)",
  );

function createReadinessWait(probe: () => Promise<boolean>) {
  return new Function(
    "isAgentmemoryReady",
    `${readinessFunction}; return waitForAgentmemoryReady;`,
  )(probe) as (timeoutMs: number) => Promise<boolean>;
}

describe("CLI worker readiness wait", () => {
  it("keeps startup alive when readiness takes longer than the old 15-second limit", async () => {
    vi.useFakeTimers();
    try {
      const startedAt = Date.now();
      const probe = vi.fn(async () => Date.now() - startedAt >= 16_000);
      const waitForReady = createReadinessWait(probe);
      const result = waitForReady(15 * 60 * 1000);

      await vi.advanceTimersByTimeAsync(16_000);

      await expect(result).resolves.toBe(true);
      expect(probe).toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("returns false at the configured timeout when readiness never arrives", async () => {
    vi.useFakeTimers();
    try {
      const probe = vi.fn(async () => false);
      const waitForReady = createReadinessWait(probe);
      const timeoutMs = 15 * 60 * 1000;
      const result = waitForReady(timeoutMs);

      await vi.advanceTimersByTimeAsync(timeoutMs);

      await expect(result).resolves.toBe(false);
      expect(probe).toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("uses a 15-minute bound for every worker startup path", () => {
    expect(source).toContain("const WORKER_READINESS_TIMEOUT_MS = 15 * 60 * 1000;");
    const boundedCalls = source.match(/waitForAgentmemoryReady\(WORKER_READINESS_TIMEOUT_MS\)/g) ?? [];
    const readinessOccurrences = source.match(/waitForAgentmemoryReady\(/g) ?? [];
    expect(boundedCalls.length).toBeGreaterThanOrEqual(4);
    expect(boundedCalls).toHaveLength(readinessOccurrences.length - 1);
    expect(source).toContain("agentmemory worker did not become ready within 15 minutes.");
  });
});
