import { describe, expect, it, vi } from "vitest";
import type { ContextUsageEvent } from "../src/types.js";
import { countContextTokens } from "../src/utils/context-tokens.js";
import { readContextUsage, recordContextUsage } from "../src/functions/context-usage.js";

vi.mock("../src/config.js", () => ({ getAgentId: () => undefined }));

function fixture() {
  const items: ContextUsageEvent[] = [];
  return {
    items,
    async get() { return { items }; },
    async update(_scope: string, _key: string, ops: Array<{ value?: unknown }>) {
      const { item, limit } = ops[0]!.value as { item: ContextUsageEvent; limit: number };
      items.push(item);
      if (items.length > limit) items.splice(0, items.length - limit);
      return { errors: [] };
    },
  };
}

const eventId = "c6dd9ad0-e8d7-4d0c-9954-69e5a2e9e33c";
const text = "Source-grounded context with one reusable implementation decision.";
const metadata = { eventId, source: "pre-compact", text };

describe("retained context emission telemetry", () => {
  it("pairs exact emitted text with a valid eligible count and deduplicates retained replay IDs", async () => {
    const kv = fixture();
    const emitted = countContextTokens(text);
    await recordContextUsage(kv as never, "session", { ...metadata, candidateTokens: emitted + 40 });
    await recordContextUsage(kv as never, "session", { ...metadata, candidateTokens: emitted + 40 });
    expect(await readContextUsage(kv as never)).toMatchObject({ emissions: 1, pairedEmissions: 1,
      textTokens: emitted, candidateTokens: emitted + 40, pairedEmittedTokens: emitted, selectionReductionTokens: 40 });
    expect(JSON.stringify(kv.items)).not.toContain(text);
  });

  it.each([undefined, null, -1, 0, 0.5, 1_000_001, NaN, Infinity])(
    "retains the actual emission but excludes an invalid eligible count %s", async candidateTokens => {
      const kv = fixture();
      await recordContextUsage(kv as never, "session", { ...metadata, candidateTokens });
      expect(kv.items[0]).not.toHaveProperty("candidateTokens");
      expect(await readContextUsage(kv as never)).toMatchObject({ emissions: 1, pairedEmissions: 0,
        textTokens: countContextTokens(text), candidateTokens: 0, pairedEmittedTokens: 0, selectionReductionTokens: 0 });
    });

  it("keeps an explicit equal baseline as a measured zero reduction", async () => {
    const kv = fixture();
    await recordContextUsage(kv as never, "session", { ...metadata, candidateTokens: countContextTokens(text) });
    expect(await readContextUsage(kv as never)).toMatchObject({ pairedEmissions: 1, selectionReductionTokens: 0 });
  });

  it.each([-1, 0, 0.5, 1_000_001, NaN, Infinity])(
    "excludes malformed historical candidate %s from paired totals", async candidateTokens => {
      const kv = fixture();
      await recordContextUsage(kv as never, "session", metadata);
      kv.items[0]!.candidateTokens = candidateTokens;
      expect(await readContextUsage(kv as never)).toMatchObject({ emissions: 1, pairedEmissions: 0,
        candidateTokens: 0, pairedEmittedTokens: 0, selectionReductionTokens: 0 });
    });
});
