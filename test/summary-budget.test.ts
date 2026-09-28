import { describe, expect, it, vi, afterEach } from "vitest";
import { getSummaryBudgetConfig, parseSummaryBudgetConfig } from "../src/config.js";
import {
  estimateSummaryTokens, summaryOutputTokenBudget, packSummaryItems, summaryInputLimit, summaryCallInputLimit, summaryChunkInputLimit,
  summaryReduceInputLimit, isExplicitSummarySizeError, MAX_SUMMARY_ITEMS,
  MIN_SUMMARY_CHUNK_CONTENT_TOKENS, MIN_SUMMARY_REDUCE_CONTENT_TOKENS,
} from "../src/functions/summary-budget.js";
import {
  SUMMARY_SYSTEM, REDUCE_SYSTEM, buildSummaryItemsPrompt, buildReduceItemsPrompt,
  formatSummaryObservation, formatSummaryPartial, type SummaryPromptItem,
} from "../src/prompts/summary.js";

afterEach(() => vi.unstubAllEnvs());
const item = (text: string, index = 1): SummaryPromptItem => ({ text, obsRangeStart: index, obsRangeEnd: index });

describe("summary budget configuration", () => {
  it("derives the input budget from the dynamic output ceiling and safety margin", () => {
    const config = parseSummaryBudgetConfig({});
    expect(config).toEqual({ contextTokens: 131072, safetyMarginTokens: 4096, maxCallInputBytes: 7500, chunkSize: 400, concurrency: 12 });
    expect(summaryInputLimit(config)).toBe(63488);
    expect(summaryCallInputLimit(config)).toBe(7500);
    expect(summaryChunkInputLimit(config)).toBeLessThanOrEqual(summaryCallInputLimit(config));
    expect(summaryChunkInputLimit(config)).toBeGreaterThanOrEqual(500);
    expect(summaryReduceInputLimit(config)).toBe(7500);
    expect(summaryReduceInputLimit(config)).toBeLessThan(summaryInputLimit(config));
    expect(summaryInputLimit({ ...config, outputTokens: 8192 })).toBe(118784);
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
  it("derives a positive output ceiling from each prompt and honors only an explicit admin cap", () => {
    const config = parseSummaryBudgetConfig({});
    const short = summaryOutputTokenBudget(config, SUMMARY_SYSTEM, "short summary input");
    const larger = summaryOutputTokenBudget(config, SUMMARY_SYSTEM, "source detail ".repeat(120));
    const unicode = summaryOutputTokenBudget(config, SUMMARY_SYSTEM, "漢字🧠".repeat(120));

    expect(short).toBeGreaterThan(768);
    expect(larger).toBeGreaterThan(short);
    expect(unicode).toBeGreaterThan(short);
    expect(unicode).toBe(estimateSummaryTokens(SUMMARY_SYSTEM, "漢字🧠".repeat(120)));
    for (const [prompt, output] of [
      ["short summary input", short], ["source detail ".repeat(120), larger], ["漢字🧠".repeat(120), unicode],
    ] as const) {
      const input = estimateSummaryTokens(SUMMARY_SYSTEM, prompt);
      expect(input + output + config.safetyMarginTokens).toBeLessThanOrEqual(config.contextTokens);
      expect(output).toBeLessThanOrEqual(input);
    }
    expect(summaryOutputTokenBudget({ ...config, outputTokens: 256 }, SUMMARY_SYSTEM, "x".repeat(1000))).toBe(256);
  });
  it("clamps an administrator ceiling that exceeds the configured context", () => {
    const config = parseSummaryBudgetConfig({
      AGENTMEMORY_SUMMARY_CONTEXT_TOKENS: "8192",
      AGENTMEMORY_SUMMARY_OUTPUT_TOKENS: "16384",
      AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS: "512",
    });
    const prompt = "small request";
    const input = estimateSummaryTokens(SUMMARY_SYSTEM, prompt);
    const output = summaryOutputTokenBudget(config, SUMMARY_SYSTEM, prompt);
    expect(output).toBe(input);
    expect(input + output + config.safetyMarginTokens).toBeLessThanOrEqual(config.contextTokens);
  });
  it("rejects impossible reserves and fixed prompt overhead", () => {
    expect(() => parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_CONTEXT_TOKENS: "4096", AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS: "4096" })).toThrow("context must exceed safety margin");
    expect(() => summaryInputLimit({ ...parseSummaryBudgetConfig({}), contextTokens: 4096 })).toThrow("fixed prompts");
    expect(() => parseSummaryBudgetConfig({ SUMMARIZE_CHUNK_CONCURRENCY: "33" })).toThrow("at most 32");
    expect(() => parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_MAX_CALL_INPUT_BYTES: "0" })).toThrow("positive finite integer");
    const fixed = Math.max(
      estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([{ ...item(""), fragment: true }])),
      estimateSummaryTokens(REDUCE_SYSTEM, buildReduceItemsPrompt([{ ...item(""), fragment: true }])),
    );
    expect(() => parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_MAX_CALL_INPUT_BYTES: String(fixed + 499) })).toThrow("500 bytes of content");
    expect(summaryCallInputLimit(parseSummaryBudgetConfig({ AGENTMEMORY_SUMMARY_MAX_CALL_INPUT_BYTES: String(fixed + 500) }))).toBe(fixed + 500);
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
  it("splits a formerly single near-limit item into balanced contiguous chunks", () => {
    const source = item("x".repeat(6000), 17);
    const inputLimit = 5000;
    const groups = packSummaryItems([source], SUMMARY_SYSTEM, buildSummaryItemsPrompt, inputLimit);
    const sizes = groups.map(group => estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt(group)));
    expect(groups.length).toBeGreaterThan(1);
    expect(groups.flat().map(part => part.text).join("")).toBe(source.text);
    expect(groups.flat().every(part => part.obsRangeStart === 17 && part.obsRangeEnd === 17)).toBe(true);
    expect(Math.max(...sizes) - Math.min(...sizes), JSON.stringify({ sizes, lengths: groups.map(group => group.map(part => part.text.length)) })).toBeLessThan(Math.max(...sizes) * 0.2);
    expect(sizes.every(size => size <= inputLimit)).toBe(true);
  });
  it("keeps the minimum target based on content tokens, not fixed prompt overhead", () => {
    const empty = item("");
    const fixed = estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([empty]));
    const small = item("x".repeat(400));
    const content = estimateSummaryTokens(SUMMARY_SYSTEM, buildSummaryItemsPrompt([small])) - fixed;
    expect(MIN_SUMMARY_CHUNK_CONTENT_TOKENS).toBe(500);
    expect(MIN_SUMMARY_REDUCE_CONTENT_TOKENS).toBe(2000);
    expect(content).toBeLessThan(MIN_SUMMARY_CHUNK_CONTENT_TOKENS);
    expect(packSummaryItems([small], SUMMARY_SYSTEM, buildSummaryItemsPrompt, fixed + 550)).toHaveLength(1);
    expect(packSummaryItems([item("x".repeat(1000))], SUMMARY_SYSTEM, buildSummaryItemsPrompt, fixed + 600))
      .toHaveLength(2);
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
