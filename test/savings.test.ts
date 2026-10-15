import { expect, it, vi } from "vitest";
import type { StateKV } from "../src/state/kv.js";
vi.mock("../src/functions/context-usage.js", () => ({ readContextUsage: async () => ({ selectionReductionTokens: 1000 }) }));
vi.mock("../src/functions/agent-usage.js", () => ({ readAgentUsage: async () => ({ inputUsdPerMillion: 1.25, clients: { codex: {} } }) }));
vi.mock("../src/functions/processing-usage.js", () => ({ readProcessingUsage: async () => ({ limit: 1000, calls: 2,
  unreportedCalls: 0, missingCacheCalls: 0, solEquivalentUsdRange: [100, 200] }) }));
vi.mock("../src/config.js", () => ({ isAgentScopeIsolated: () => false, isContextInjectionEnabled: () => true }));
import { readSavingsReport } from "../src/functions/savings.js";
it("uses measured model mix only for hypothetical input exposure and exposes no fixed processing valuation", async () => {
  const report = await readSavingsReport({} as StateKV, []);
  expect(report.pricing).toMatchObject({ model: null, effort: null, isInvoice: false });
  expect(report.selectionScenario).toMatchObject({ cachedShortContextUsd: null, uncachedShortContextUsd: null,
    profileCacheWeightedUsd: .00125 });
  expect(report.processing).toMatchObject({ costUsd: null });
  expect(JSON.stringify(report)).not.toMatch(/solEquivalentUsdRange|gpt-6\.1-sol|"medium"/);
  expect(report.netSavingsUsd).toBeNull();
});
