import { describe, expect, it } from "vitest";
import {
  GRAPH_WORK_LIMITS,
  GraphWorkEnvelopeTooLargeError,
  GraphWorkPageTooLargeError,
  GraphWorkReferenceTooLargeError,
  GraphWorkStaleCursorError,
  appendGraphWorkResultReference,
  combineGraphWorkProgress,
  createGraphWorkReferenceAccumulator,
  createNextGraphWorkUnit,
  flushGraphWorkResultReferences,
  type CreateGraphWorkUnitInput,
  type GraphWorkCursor,
} from "../src/functions/graph-work-groups.js";

const baseInput = (sources: CreateGraphWorkUnitInput["sources"]): CreateGraphWorkUnitInput => ({
  audience: { scope: "project", project: "project-a" },
  sources,
  systemPrompt: "Resume fuentes acotadas. 🧠",
  envelopePrefix: "BEGIN 🌐\n",
  envelopeSuffix: "\nEND",
});

describe("graph work grouping", () => {
  it("measures the actual UTF-8 system and user envelope and emits one bounded unit", () => {
    const input = baseInput([{ id: "obs-1", version: 7, content: "á漢🙂".repeat(600) }]);
    const result = createNextGraphWorkUnit(input);
    expect(result.unit).not.toBeNull();
    expect(result.unit!.inputBytes).toBe(Buffer.byteLength(JSON.stringify({
      systemPrompt: result.unit!.systemPrompt,
      userPrompt: result.unit!.userPrompt,
    }), "utf8"));
    expect(result.unit!.inputBytes).toBeLessThanOrEqual(GRAPH_WORK_LIMITS.maxInputBytes);
    expect(result.unit!.maxResponseBytes).toBe(GRAPH_WORK_LIMITS.maxResponseBytes);
    expect(result.unit!.fragments.length).toBeLessThanOrEqual(GRAPH_WORK_LIMITS.maxPageSources);
    expect(result.unit!.fragments[0]).toMatchObject({ sourceId: "obs-1", sourceVersion: 7, startByte: 0 });
    expect(createNextGraphWorkUnit(input).unit?.id).toBe(result.unit!.id);
    expect(createNextGraphWorkUnit(input).cursor).toEqual(result.cursor);
  });

  it("continues exact UTF-8 ranges without skipping or duplicating content", () => {
    const sources = [
      { id: "small", version: "v1", content: "primero: café 🧠" },
      { id: "large", version: 12, content: "á漢🙂e\u0301".repeat(5_000) },
      { id: "last", version: "opaque:3", content: "fin" },
    ];
    const input = baseInput(sources);
    const seen = new Map<string, { end: number; content: string }>();
    let cursor: GraphWorkCursor | null = null;
    let units = 0;
    do {
      const result = createNextGraphWorkUnit({ ...input, cursor });
      expect(result.unit).not.toBeNull();
      expect(result.unit!.inputBytes).toBeLessThanOrEqual(8 * 1024);
      for (const fragment of result.unit!.fragments) {
        const previous = seen.get(fragment.sourceId) ?? { end: 0, content: "" };
        expect(fragment.startByte).toBe(previous.end);
        expect(Buffer.byteLength(fragment.content, "utf8")).toBe(fragment.endByte - fragment.startByte);
        seen.set(fragment.sourceId, { end: fragment.endByte, content: previous.content + fragment.content });
      }
      cursor = result.cursor;
      units++;
      expect(units).toBeLessThan(20);
    } while (cursor);

    expect(units).toBeGreaterThan(1);
    for (const source of sources) {
      expect(seen.get(source.id)).toEqual({ end: Buffer.byteLength(source.content, "utf8"), content: source.content });
    }
  });

  it("rejects oversized pages, envelopes, and changed-page cursors without mutating sources", () => {
    const tooLarge = [{ id: "large", version: 1, content: "x".repeat(65 * 1024) }];
    expect(() => createNextGraphWorkUnit(baseInput(tooLarge))).toThrow(GraphWorkPageTooLargeError);
    expect(tooLarge[0]!.content.length).toBe(65 * 1024);

    const envelope = { ...baseInput([{ id: "x", version: 1, content: "source" }]), systemPrompt: "x".repeat(9 * 1024) };
    expect(() => createNextGraphWorkUnit(envelope)).toThrow(GraphWorkEnvelopeTooLargeError);
    expect(() => createNextGraphWorkUnit(baseInput([{ id: "x".repeat(9 * 1024), version: 1, content: "" }]))).toThrow(GraphWorkEnvelopeTooLargeError);

    const input = baseInput([{ id: "x", version: 1, content: "first".repeat(2_000) }]);
    const first = createNextGraphWorkUnit(input);
    expect(first.cursor).not.toBeNull();
    const changed = baseInput([{ id: "x", version: 2, content: "first".repeat(2_000) }]);
    expect(() => createNextGraphWorkUnit({ ...changed, cursor: first.cursor })).toThrow(GraphWorkStaleCursorError);
  });

  it("keeps result references bounded and resumes at the first reference after flush", () => {
    let accumulator = createGraphWorkReferenceAccumulator();
    for (let sequence = 0; sequence < GRAPH_WORK_LIMITS.maxResultReferences; sequence++) {
      const result = appendGraphWorkResultReference(accumulator, sequence, {
        id: `result-${sequence}`,
        fingerprint: `fp-${sequence}`,
      });
      accumulator = result.accumulator;
      if (sequence < GRAPH_WORK_LIMITS.maxResultReferences - 1) expect(result.flushRequired).toBe(false);
      else expect(result.flushRequired).toBe(true);
    }
    expect(accumulator.references).toHaveLength(GRAPH_WORK_LIMITS.maxResultReferences);
    const overflow = appendGraphWorkResultReference(accumulator, 12, { id: "result-12", fingerprint: "fp-12" });
    expect(overflow.flushRequired).toBe(true);
    expect(overflow.continuation.nextSequence).toBe(12);
    expect(overflow.accumulator).toBe(accumulator);

    const flushed = flushGraphWorkResultReferences(accumulator);
    expect(flushed.fingerprint).toBeTruthy();
    expect(flushed.accumulator).toMatchObject({ batchSequence: 1, nextSequence: 12, references: [] });
    const resumed = appendGraphWorkResultReference(flushed.accumulator, 12, { id: "result-12", fingerprint: "fp-12" });
    expect(resumed.accumulator.references).toEqual([{ id: "result-12", fingerprint: "fp-12" }]);
    expect(resumed.accumulator.referenceBytes).toBeLessThanOrEqual(GRAPH_WORK_LIMITS.maxResultReferenceBytes);
    let byteBounded = createGraphWorkReferenceAccumulator();
    byteBounded = appendGraphWorkResultReference(byteBounded, 0, {
      id: "\n".repeat(10_000), fingerprint: "large-but-bounded",
    }).accumulator;
    const byteOverflow = appendGraphWorkResultReference(byteBounded, 1, {
      id: "\n".repeat(23_000), fingerprint: "requires-flush",
    });
    expect(byteOverflow.flushRequired).toBe(true);
    expect(byteOverflow.continuation.nextSequence).toBe(1);
    expect(byteOverflow.accumulator.references).toHaveLength(1);
    expect(byteOverflow.accumulator.referenceBytes).toBeLessThanOrEqual(GRAPH_WORK_LIMITS.maxResultReferenceBytes);
    expect(() => appendGraphWorkResultReference(createGraphWorkReferenceAccumulator(), 0, {
      id: "x".repeat(65 * 1024), fingerprint: "too-large",
    })).toThrow(GraphWorkReferenceTooLargeError);
  });

  it("combines durable result counters and references without retaining source content", () => {
    const initial = {
      completedUnits: 0,
      completedFragments: 0,
      resultReferenceCount: 0,
      resultBytes: 0,
      resultFingerprint: "seed",
      lastResultId: null,
    };
    const progress = combineGraphWorkProgress(initial, {
      unitId: "unit-1",
      fragmentCount: 3,
      result: { id: "stored-result-1", fingerprint: "result-fp" },
      resultBytes: 4096,
    });
    expect(progress).toMatchObject({
      completedUnits: 1,
      completedFragments: 3,
      resultReferenceCount: 1,
      resultBytes: 4096,
      lastResultId: "stored-result-1",
    });
    expect(JSON.stringify(progress)).not.toContain("content");
  });
});
