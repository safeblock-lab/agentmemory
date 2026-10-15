import { afterEach, describe, expect, it, vi } from "vitest";
import { usageSnapshot } from "../src/functions/codex-usage.js";
import { combineAgentUsage } from "../src/functions/agent-usage.js";

describe("combined local usage", () => {
  it("weights total cache reads by total input, never averages percentages", () => {
    const codex = { ...usageSnapshot("codex"), requests: 1, inputTokens: 100, cachedInputTokens: 90 };
    const claude = { ...usageSnapshot("claude"), requests: 1, inputTokens: 900, cachedInputTokens: 90 };
    expect(combineAgentUsage(codex, claude)).toMatchObject({ requests: 2, inputTokens: 1000, cacheHitRatio: .18,
      inputUsdPerMillion: null });
  });
  it("uses either client alone; absence of the other is normal, retained internally", () => {
    for (const client of ["codex", "claude"] as const) {
      const codex = usageSnapshot("codex"), claude = usageSnapshot("claude");
      const active = client === "codex" ? codex : claude;
      const absent = client === "codex" ? claude : codex;
      Object.assign(active, { requests: 2, inputTokens: 1000, cachedInputTokens: 250 });
      Object.assign(absent, { unavailable: true, status: "unavailable" });
      expect(combineAgentUsage(codex, claude)).toMatchObject({ requests: 2, cacheHitRatio: .25, partial: false, unavailable: false });
    }
  });
  it("does not invent a zero ratio for empty usage and retains partial coverage", () => {
    expect(combineAgentUsage(usageSnapshot("codex"), usageSnapshot("claude"))).toMatchObject({ cacheHitRatio: null });
    expect(combineAgentUsage({ ...usageSnapshot("codex"), partial: true }, usageSnapshot("claude"))).toMatchObject({ partial: true });
  });
  it("marks unsafe combined totals partial and suppresses ratio and valuation", () => {
    const codex = { ...usageSnapshot("codex"), requests: 1, inputTokens: Number.MAX_SAFE_INTEGER,
      inputApiEquivalentUsd: 10 };
    const claude = { ...usageSnapshot("claude"), requests: 1, inputTokens: 1, inputApiEquivalentUsd: 1 };
    expect(combineAgentUsage(codex, claude)).toMatchObject({ inputTokens: null, cacheHitRatio: null,
      inputUsdPerMillion: null, inputApiEquivalentUsd: null, partial: true });
    codex.inputTokens = 100; claude.inputTokens = 100;
    codex.outputTokens = Number.MAX_SAFE_INTEGER; claude.outputTokens = 1;
    expect(combineAgentUsage(codex, claude)).toMatchObject({ outputTokens: null, cacheHitRatio: null, partial: true });
  });
});

afterEach(() => { vi.doUnmock("../src/functions/codex-usage.js"); vi.doUnmock("../src/functions/claude-usage.js"); vi.resetModules(); });
it("shares one concurrent collection and caches both clients over the same exact window", async () => {
  const codex = vi.fn(async ({ now }: { now: Date }) => usageSnapshot("codex", now));
  const claude = vi.fn(async ({ now }: { now: Date }) => usageSnapshot("claude", now));
  vi.doMock("../src/functions/codex-usage.js", () => ({ collectCodexUsage: codex }));
  vi.doMock("../src/functions/claude-usage.js", () => ({ collectClaudeUsage: claude }));
  vi.resetModules();
  const { readAgentUsage } = await import("../src/functions/agent-usage.js");
  const [first, second] = await Promise.all([readAgentUsage(), readAgentUsage()]);
  expect(first).toBe(second); expect(await readAgentUsage()).toBe(first);
  expect(codex).toHaveBeenCalledTimes(1); expect(claude).toHaveBeenCalledTimes(1);
  expect(codex.mock.calls[0][0].now).toBe(claude.mock.calls[0][0].now);
  expect(first.clients.codex.since).toBe(first.clients.claude.since);
  expect(first.clients.codex.until).toBe(first.clients.claude.until);
});
