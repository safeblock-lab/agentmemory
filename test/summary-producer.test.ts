import { describe, expect, it, vi } from "vitest";
import type { CompressedObservation, LlmCallOptions, MemoryProvider, SummaryBudgetConfig } from "../src/types.js";
import { parseSummaryBudgetConfig } from "../src/config.js";
import { createSummaryProducer } from "../src/functions/summary-producer.js";
import {
  estimateSummaryTokens, summaryInputLimit, summaryChunkInputLimit,
  summaryReduceInputLimit, MAX_SUMMARY_CALLS, MAX_SUMMARY_DEPTH,
} from "../src/functions/summary-budget.js";
import { SUMMARY_SYSTEM, buildSummaryItemsPrompt, formatSummaryObservation } from "../src/prompts/summary.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
const xml = (narrative = "Done.") => `<summary><title>Summary</title><narrative>${narrative}</narrative><decisions><decision>decision</decision></decisions><files><file>a.ts</file></files><concepts><concept>code</concept></concepts></summary>`;
const observation = (index: number, narrative = "source") : CompressedObservation => ({
  id: `obs${index}`, sessionId: "session", timestamp: "2026-09-27", type: "conversation", title: `Title ${index}`,
  facts: ["fact"], narrative, files: ["a.ts"], concepts: ["concept"], importance: 5,
});
function provider(run: (system: string, prompt: string, index: number) => Promise<string> | string) {
  const calls: Array<{ system: string; prompt: string; options?: LlmCallOptions }> = [];
  const selected: MemoryProvider = {
    name: "test", compress: async () => "",
    summarize: async (system, prompt, options) => {
      calls.push({ system, prompt, options });
      return run(system, prompt, calls.length);
    },
  };
  return { selected, calls };
}
const small: SummaryBudgetConfig = { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256, maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 };
const produce = (selected: MemoryProvider, config = small) => createSummaryProducer(selected, undefined, config, "session", "project");

describe("bounded summary producer", () => {
  it("sends the summary output budget on every call", async () => {
    const mock = provider(() => xml());
    await produce(mock.selected, parseSummaryBudgetConfig({}))([observation(1)]);
    expect(mock.calls).toHaveLength(1);
    expect(mock.calls[0].options).toEqual({ task: "summary", outputTokens: 8192 });
  });
  it("bounds single, map and reduce calls independently of a large context window", async () => {
    const cap = 3000;
    const config = parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_MAX_CALL_INPUT_BYTES: String(cap) });
    const mock = provider((system, prompt) => system.includes("merging")
      ? xml("merged")
      : xml(`partial-${prompt.match(/\[(\d+)\] conversation/)?.[1] ?? "fragment"} ${"x".repeat(900)}`));
    await produce(mock.selected, config)([observation(1)]);
    expect(mock.calls).toHaveLength(1);
    expect(estimateSummaryTokens(mock.calls[0].system, mock.calls[0].prompt)).toBeLessThanOrEqual(cap);

    mock.calls.length = 0;
    const result = await produce(mock.selected, config)(Array.from({ length: 20 }, (_, index) => observation(index, "x".repeat(1000))));
    expect(result.mode).toBe("chunked");
    expect(mock.calls.filter(call => !call.system.includes("merging")).length).toBeGreaterThan(1);
    const reduction = mock.calls.filter(call => call.system.includes("merging"));
    const firstRound = reduction.filter(call => call.prompt.includes("partial-"));
    expect(firstRound.length).toBeGreaterThan(1);
    expect(reduction.some(call => !call.prompt.includes("partial-") && call.prompt.includes("merged"))).toBe(true);
    const sourceIndices = [...firstRound.map(call => call.prompt).join("\n").matchAll(/partial-(\d+)/g)]
      .map(match => Number(match[1]));
    expect(sourceIndices.length).toBeGreaterThan(1);
    expect(sourceIndices).toEqual([...sourceIndices].sort((left, right) => left - right));
    for (const call of mock.calls) expect(estimateSummaryTokens(call.system, call.prompt)).toBeLessThanOrEqual(cap);
    expect(config.contextTokens - config.outputTokens - config.safetyMarginTokens).toBeGreaterThan(cap);
  });
  it("keeps a small session in one request and balances a formerly single large request", async () => {
    const smallMock = provider(() => xml());
    const smallResult = await produce(smallMock.selected)([observation(1)]);
    expect(smallResult).toMatchObject({ mode: "single", chunks: 1 });
    expect(smallMock.calls.filter(call => !call.system.includes("merging"))).toHaveLength(1);

    const medium: SummaryBudgetConfig = {
      contextTokens: 8192, outputTokens: 1024, safetyMarginTokens: 512, maxCallInputBytes: 6656, chunkSize: 400, concurrency: 2,
    };
    const source = observation(0, "x".repeat(3000));
    const oneItem = {
      text: formatSummaryObservation(source, 1), obsRangeStart: 1, obsRangeEnd: 1,
    };
    expect(estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([oneItem])))
      .toBeLessThan(summaryInputLimit(medium));
    expect(estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([oneItem])))
      .toBeGreaterThan(summaryChunkInputLimit(medium));

    const largeMock = provider(() => xml("brief"));
    const result = await produce(largeMock.selected, medium)([source]);
    const mapCalls = largeMock.calls.filter(call => !call.system.includes("merging"));
    const sizes = mapCalls.map(call => estimateSummaryTokens(call.system, call.prompt));
    expect(result.mode).toBe("chunked");
    expect(mapCalls.length).toBeGreaterThan(1);
    expect(sizes.every(size => size <= summaryChunkInputLimit(medium))).toBe(true);
    expect(Math.max(...sizes) - Math.min(...sizes)).toBeLessThan(Math.max(...sizes) * 0.25);
  });
  it("runs independent reduce chunks concurrently and preserves their source order", async () => {
    const config: SummaryBudgetConfig = {
      contextTokens: 20_000, outputTokens: 6000, safetyMarginTokens: 2000, maxCallInputBytes: 12000, chunkSize: 1, concurrency: 2,
    };
    let reduceInFlight = 0;
    let maxReduceInFlight = 0;
    const mock = provider(async (system, prompt) => {
      const reducing = system.includes("merging");
      if (reducing) {
        reduceInFlight++;
        maxReduceInFlight = Math.max(maxReduceInFlight, reduceInFlight);
      }
      await new Promise(resolve => setTimeout(resolve, 5));
      try {
        if (reducing) return xml("merged");
        const index = prompt.match(/\[(\d+)\] conversation/)?.[1] ?? "?";
        return xml(`obs-${index} ${"x".repeat(1500)}`);
      } finally {
        if (reducing) reduceInFlight--;
      }
    });
    const result = await produce(mock.selected, config)(Array.from({ length: 12 }, (_, index) => observation(index)));
    const reduceCalls = mock.calls.filter(call => call.system.includes("merging"));
    const firstRound = reduceCalls.filter(call => /obs-(?:[1-9]|1[0-2])\b/.test(call.prompt));
    const firstRoundText = firstRound.map(call => call.prompt).join("\n");
    const firstPositions = Array.from({ length: 12 }, (_, index) => firstRoundText.indexOf(`obs-${index + 1}`));

    expect(result.response).toContain("Summary");
    expect(maxReduceInFlight).toBeGreaterThan(1);
    expect(maxReduceInFlight).toBeLessThanOrEqual(config.concurrency);
    expect(firstRound.length).toBeGreaterThan(1);
    expect(firstPositions.every(position => position >= 0)).toBe(true);
    expect(firstPositions).toEqual([...firstPositions].sort((left, right) => left - right));
  });
  it("fits map and multi-level reduce prompts for giant records and many partials", async () => {
    const mock = provider(() => xml("brief"));
    const result = await produce(mock.selected)(Array.from({ length: 100 }, (_, index) => observation(index, "漢字🧠code".repeat(40))));
    expect(result.mode).toBe("chunked");
    expect(mock.calls.filter(call => call.system.includes("merging")).length).toBeGreaterThan(1);
    for (const call of mock.calls) expect(estimateSummaryTokens(call.system, call.prompt) + small.outputTokens + small.safetyMarginTokens).toBeLessThanOrEqual(small.contextTokens);
  });
  it("fragments huge observations with original provenance", async () => {
    const mock = provider(() => xml());
    const result = await produce(mock.selected)([observation(0, "🧠漢字".repeat(1000)), observation(1)]);
    expect(result.mode).toBe("chunked");
    const maps = mock.calls.filter(call => !call.system.includes("merging"));
    expect(maps.length).toBeGreaterThan(2);
    expect(maps[0].prompt).toContain("Observation 1 fragment");
    expect(maps[maps.length - 1].prompt).toContain("[2]");
    expect(mock.calls.some(call => call.system.includes("merging") && call.prompt.includes("obs 1-1"))).toBe(true);
  });
  it("handles oversized provider partials without truncating their reduction inputs", async () => {
    const giant = "漢字🧠".repeat(1000);
    const mock = provider((system) => system.includes("merging") ? xml("brief") : xml(giant));
    const result = await produce(mock.selected, { ...small, chunkSize: 1 })([observation(0), observation(1)]);
    expect(result.response).toContain("brief");
    const reduction = mock.calls.filter(call => call.system.includes("merging"));
    expect(reduction.length).toBeGreaterThan(2);
    expect(reduction[0].prompt).toContain("fragment in source order");
    const firstRound = reduction.filter(call => call.prompt.includes("fragment in source order"));
    const combined = firstRound.map(call => call.prompt).join("");
    expect(combined.match(/漢字🧠/g)?.length).toBeGreaterThan(1900);
    for (const call of reduction) expect(estimateSummaryTokens(call.system, call.prompt)).toBeLessThanOrEqual(3328);
  });
  it("subdivides explicit context failures and never repeats the rejected request", async () => {
    const mock = provider((_system, _prompt, index) => {
      if (index === 1) throw new Error("context_length_exceeded");
      return xml();
    });
    const result = await produce(mock.selected)([observation(0, "x".repeat(1800))]);
    expect(result.mode).toBe("chunked");
    const rejectedPrompt = mock.calls[0].prompt;
    expect(mock.calls.filter(call => call.prompt === rejectedPrompt)).toHaveLength(1);
    expect(mock.calls.slice(1).some(call => call.prompt.length < rejectedPrompt.length)).toBe(true);
  });
  it("bounds repeated explicit size failures and does not return success", async () => {
    const mock = provider(() => { throw new Error("maximum context length exceeded"); });
    await expect(produce(mock.selected)([observation(0, "x".repeat(1800))])).rejects.toThrow(/summary_context_limit_exhausted|summary_fragment_cannot_fit|summary_depth_limit/);
    expect(mock.calls.length).toBeLessThanOrEqual(1 + small.concurrency * (MAX_SUMMARY_DEPTH + 1));
  });
  it("keeps ambiguous rejection on the existing retry/skip contract", async () => {
    const mock = provider((_system, _prompt, index) => {
      if (index <= 2) throw new Error("invalid_content: empty or too large");
      return xml();
    });
    const result = await produce(mock.selected, { ...small, chunkSize: 1, concurrency: 1 })([observation(0), observation(1), observation(2)]);
    expect(result.skipped).toBe(1);
    expect(mock.calls).toHaveLength(5);
    expect(mock.calls[0].prompt).toBe(mock.calls[1].prompt);
  });
  it("adapts explicit reducer failures without losing earlier partials", async () => {
    let rejected: string | undefined;
    const mock = provider((system, prompt) => {
      if (system.includes("merging") && !rejected) {
        rejected = prompt;
        throw new Error("too many tokens");
      }
      return xml();
    });
    const result = await produce(mock.selected, { ...small, chunkSize: 1 })(Array.from({ length: 10 }, (_, i) => observation(i)));
    expect(result.response).toContain("Summary");
    expect(mock.calls.filter(call => call.prompt === rejected)).toHaveLength(1);
    expect(mock.calls.filter(call => call.system.includes("merging")).length).toBeGreaterThan(2);
  });
  it("stops reducers whose outputs do not shrink", async () => {
    const mock = provider(() => xml("x".repeat(1400)));
    await expect(produce(mock.selected, { ...small, chunkSize: 1 })(Array.from({ length: 4 }, (_, i) => observation(i)))).rejects.toThrow("summary_reduce_no_progress");
  });
  it("bounds reducer depth even when each round makes tiny progress", async () => {
    const mock = provider((system, prompt) => {
      const narrative = system.includes("merging") ? (prompt.match(/Narrative: (x+)/)?.[1].slice(1) ?? "x".repeat(1400)) : "x".repeat(1400);
      return xml(narrative);
    });
    await expect(produce(mock.selected, { ...small, chunkSize: 1 })(Array.from({ length: 4 }, (_, i) => observation(i)))).rejects.toThrow("summary_depth_limit_exceeded");
    expect(mock.calls.length).toBeLessThanOrEqual(4 + 4 * MAX_SUMMARY_DEPTH);
  });
  it("caps total selected-provider calls across the invocation", async () => {
    const mock = provider(() => xml());
    await expect(produce(mock.selected, { ...parseSummaryBudgetConfig({}), chunkSize: 1 })(Array.from({ length: MAX_SUMMARY_CALLS }, (_, i) => observation(i)))).rejects.toThrow("summary_call_limit_exceeded");
    expect(mock.calls).toHaveLength(MAX_SUMMARY_CALLS);
  });
});
