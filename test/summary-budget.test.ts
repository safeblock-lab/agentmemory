import { describe, expect, it, vi, afterEach } from "vitest";
import { getSummaryBudgetConfig, parseSummaryBudgetConfig } from "../src/config.js";
import {
  estimateSummaryTokens, packSummaryItems, summaryInputLimit, isExplicitSummarySizeError,
  MAX_SUMMARY_ITEMS,
} from "../src/functions/summary-budget.js";
import {
  SUMMARY_SYSTEM, REDUCE_SYSTEM, buildSummaryItemsPrompt, buildReduceItemsPrompt,
  formatSummaryObservation, formatSummaryPartial, type SummaryPromptItem,
} from "../src/prompts/summary.js";

afterEach(() => vi.unstubAllEnvs());
const item = (text: string, index = 1): SummaryPromptItem => ({ text, obsRangeStart: index, obsRangeEnd: index });

describe("summary budget configuration", () => {
  it("reserves the correct model output and safety margin", () => {
    const config = parseSummaryBudgetConfig({});
    expect(config).toEqual({ contextTokens: 131072, outputTokens: 8192, safetyMarginTokens: 4096, chunkSize: 400, concurrency: 6 });
    expect(summaryInputLimit(config)).toBe(118784);
  });
  it.each(["", "0", "-1", "Infinity", "NaN", "1.5", "4096junk", "9007199254740992"])("rejects invalid integers: %j", raw => {
    expect(() => parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_CONTEXT_TOKENS: raw })).toThrow("positive finite integer");
  });
  it("resolves process overrides through normal merged configuration", () => {
    vi.stubEnv("AGENTMEMORY_SUMMARY_CONTEXT_TOKENS", "8192");
    vi.stubEnv("AGENTMEMORY_SUMMARY_OUTPUT_TOKENS", "1024");
    vi.stubEnv("AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS", "512");
    expect(getSummaryBudgetConfig().contextTokens).toBe(8192);
    expect(summaryInputLimit(getSummaryBudgetConfig())).toBe(6656);
  });
  it("rejects impossible reserves and fixed prompt overhead", () => {
    expect(() => parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_CONTEXT_TOKENS: "12288" })).toThrow("context must exceed");
    expect(() => summaryInputLimit({ ...parseSummaryBudgetConfig({}), contextTokens: 13000 })).toThrow("fixed prompts");
    expect(() => parseSummaryBudgetConfig({ SUMMARIZE_CHUNK_CONCURRENCY: "33" })).toThrow("at most 32");
  });
});

describe("summary prompt packing", () => {
  it("includes system, formatting, JSON escaping and envelope in exact boundary checks", () => {
    const entry = item("code = \"x\";\n".repeat(100));
    const limit = estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([entry]));
    expect(packSummaryItems([entry], SUMMARY_SYSTEM, buildSummaryItemsPrompt, limit)).toEqual([[entry]]);
    const chunks = packSummaryItems([item(entry.text + "x")], SUMMARY_SYSTEM, buildSummaryItemsPrompt, limit);
    expect(chunks.length).toBeGreaterThan(1);
    for (const chunk of chunks) expect(estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt(chunk))).toBeLessThanOrEqual(limit);
  });
  it.each(["漢字🧠é", "const value = \"quoted\";\n\t\u0000", "abcdefghijklmnop"])("splits huge source records losslessly and in order: %j", content => {
    const source = formatSummaryObservation({ type: "conversation", title: content.repeat(100), narrative: content.repeat(200), facts: [content.repeat(100)], files: [content.repeat(100)], concepts: [content.repeat(100)] }, 17);
    const groups = packSummaryItems([item(source, 17)], SUMMARY_SYSTEM, buildSummaryItemsPrompt, 2600);
    expect(groups.length).toBeGreaterThan(1);
    const fragments = groups.flat();
    expect(fragments.map(part => part.text).join("")).toBe(source);
    for (const fragment of fragments) {
      expect(fragment).toMatchObject({ obsRangeStart: 17, obsRangeEnd: 17, fragment: true });
      expect(fragment.text.isWellFormed()).toBe(true);
    }
    for (const group of groups) expect(estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt(group))).toBeLessThanOrEqual(2600);
  });
  it("splits huge partials without losing fields or source ranges", () => {
    const source = formatSummaryPartial({ title: "long".repeat(900), narrative: "🧠".repeat(600), keyDecisions: ["漢字".repeat(900)], filesModified: ["path".repeat(900)], concepts: ["tag".repeat(900)] });
    const groups = packSummaryItems([{ text: source, obsRangeStart: 3, obsRangeEnd: 19 }], REDUCE_SYSTEM, buildReduceItemsPrompt, 3000);
    expect(groups.flat().map(part => part.text).join("")).toBe(source);
    expect(groups.flat().every(part => part.obsRangeStart === 3 && part.obsRangeEnd === 19)).toBe(true);
    for (const group of groups) expect(estimateSummaryTokens(REDUCE_SYSTEM, buildReduceItemsPrompt(group))).toBeLessThanOrEqual(3000);
  });
  it("bounds items and rejects a context that cannot hold a character", () => {
    expect(packSummaryItems([], SUMMARY_SYSTEM, buildSummaryItemsPrompt, 3000)).toEqual([]);
    expect(() => packSummaryItems(Array.from({ length: MAX_SUMMARY_ITEMS + 1 }, () => item("a")), SUMMARY_SYSTEM, buildSummaryItemsPrompt, 3000)).toThrow("summary_item_limit");
    expect(() => packSummaryItems([item("🧠")], SUMMARY_SYSTEM, buildSummaryItemsPrompt, 1)).toThrow("cannot_fit");
  });
  it("adapts only explicit context and token limit failures", () => {
    expect(isExplicitSummarySizeError(new Error("maximum context length exceeded"))).toBe(true);
    expect(isExplicitSummarySizeError(new Error("context_length_exceeded"))).toBe(true);
    expect(isExplicitSummarySizeError(new Error("input tokens exceed model limit"))).toBe(true);
    expect(isExplicitSummarySizeError(new Error("invalid_content: empty or too large"))).toBe(false);
    expect(isExplicitSummarySizeError(new Error("content rejected"))).toBe(false);
  });
});
