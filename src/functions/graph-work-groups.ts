import { fingerprintId } from "../state/schema.js";

export const GRAPH_WORK_LIMITS = Object.freeze({
  maxPageSources: 12,
  maxPageBytes: 64 * 1024,
  maxInputBytes: 8 * 1024,
  maxResponseBytes: 16 * 1024,
  maxResultReferences: 12,
  maxResultReferenceBytes: 64 * 1024,
});

export type GraphWorkAudience =
  | { scope: "project"; project: string }
  | { scope: "global-orchestration" };

export type GraphWorkSourceVersion = string | number;

export interface GraphWorkSource {
  id: string;
  version: GraphWorkSourceVersion;
  content: string;
}

export interface GraphWorkFragment {
  sourceId: string;
  sourceVersion: GraphWorkSourceVersion;
  sourceFingerprint: string;
  startByte: number;
  endByte: number;
  content: string;
}

export interface GraphWorkCursor {
  pageFingerprint: string;
  sourceIndex: number;
  byteOffset: number;
}

export interface CreateGraphWorkUnitInput {
  audience: GraphWorkAudience;
  sources: readonly GraphWorkSource[];
  systemPrompt: string;
  envelopePrefix: string;
  envelopeSuffix: string;
  cursor?: GraphWorkCursor | null;
}

export interface GraphWorkUnit {
  id: string;
  fingerprint: string;
  audience: GraphWorkAudience;
  systemPrompt: string;
  userPrompt: string;
  inputBytes: number;
  maxResponseBytes: number;
  fragments: readonly GraphWorkFragment[];
}

export interface NextGraphWorkUnit {
  pageFingerprint: string;
  unit: GraphWorkUnit | null;
  cursor: GraphWorkCursor | null;
  complete: boolean;
}

export type GraphWorkErrorCode =
  | "GRAPH_WORK_PAGE_TOO_LARGE"
  | "GRAPH_WORK_PAGE_INVALID"
  | "GRAPH_WORK_ENVELOPE_TOO_LARGE"
  | "GRAPH_WORK_CURSOR_STALE"
  | "GRAPH_WORK_REFERENCE_TOO_LARGE"
  | "GRAPH_WORK_REFERENCE_SEQUENCE_INVALID";

export class GraphWorkError extends Error {
  constructor(readonly code: GraphWorkErrorCode, message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class GraphWorkPageTooLargeError extends GraphWorkError {
  constructor() { super("GRAPH_WORK_PAGE_TOO_LARGE", "Graph work source page exceeds its bounded source count or byte budget"); }
}

export class GraphWorkEnvelopeTooLargeError extends GraphWorkError {
  constructor() { super("GRAPH_WORK_ENVELOPE_TOO_LARGE", "Graph work envelope or indivisible source metadata exceeds the UTF-8 input budget"); }
}

export class GraphWorkStaleCursorError extends GraphWorkError {
  constructor() { super("GRAPH_WORK_CURSOR_STALE", "Graph work cursor does not match this source page and rendered envelope"); }
}

export class GraphWorkReferenceTooLargeError extends GraphWorkError {
  constructor() { super("GRAPH_WORK_REFERENCE_TOO_LARGE", "A graph work result reference exceeds the bounded reference batch"); }
}

export class GraphWorkReferenceSequenceError extends GraphWorkError {
  constructor() { super("GRAPH_WORK_REFERENCE_SEQUENCE_INVALID", "Graph work result references must be appended in cursor order"); }
}

function byteLength(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

function isWellFormedUtf16(value: string): boolean {
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index++;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function validateInput(input: CreateGraphWorkUnitInput): { pageBytes: number; fingerprints: string[]; pageFingerprint: string } {
  if (!input || !input.audience ||
    (input.audience.scope !== "project" && input.audience.scope !== "global-orchestration") ||
    typeof input.systemPrompt !== "string" || typeof input.envelopePrefix !== "string" || typeof input.envelopeSuffix !== "string") {
    throw new GraphWorkError("GRAPH_WORK_PAGE_INVALID", "Graph work audience and prompt envelope are required");
  }
  if (!Array.isArray(input.sources) || input.sources.length > GRAPH_WORK_LIMITS.maxPageSources) throw new GraphWorkPageTooLargeError();
  if (input.audience.scope === "project" && (typeof input.audience.project !== "string" || !input.audience.project.trim())) {
    throw new GraphWorkError("GRAPH_WORK_PAGE_INVALID", "Project audience requires an explicit project id");
  }
  const fixedBytes = byteLength(input.systemPrompt) + byteLength(input.envelopePrefix) + byteLength(input.envelopeSuffix) +
    byteLength(input.audience.scope === "project" ? input.audience.project : "");
  if (fixedBytes > GRAPH_WORK_LIMITS.maxInputBytes) throw new GraphWorkEnvelopeTooLargeError();
  if (requestBytes(input.systemPrompt, renderUserPrompt(input, [])) > GRAPH_WORK_LIMITS.maxInputBytes) throw new GraphWorkEnvelopeTooLargeError();
  let pageBytes = 0;
  for (const source of input.sources) {
    if (!source || typeof source.id !== "string" || !source.id || typeof source.content !== "string" ||
      (typeof source.version !== "string" && typeof source.version !== "number") ||
      (typeof source.version === "number" && !Number.isSafeInteger(source.version)) ||
      (typeof source.version === "string" && (!source.version.length || !isWellFormedUtf16(source.version))) ||
      !isWellFormedUtf16(source.id) || !isWellFormedUtf16(source.content)) {
      throw new GraphWorkError("GRAPH_WORK_PAGE_INVALID", "Graph work source must have a stable id, version and valid UTF-8 content");
    }
    pageBytes += byteLength(source.id) + byteLength(String(source.version)) + byteLength(source.content);
    if (pageBytes > GRAPH_WORK_LIMITS.maxPageBytes) throw new GraphWorkPageTooLargeError();
  }
  const seen = new Set<string>();
  const fingerprints: string[] = [];
  for (const source of input.sources) {
    const sourceKey = JSON.stringify([source.id, source.version]);
    if (seen.has(sourceKey)) throw new GraphWorkError("GRAPH_WORK_PAGE_INVALID", "Graph work source page contains a duplicate id and version");
    seen.add(sourceKey);
    fingerprints.push(fingerprintId("gws", JSON.stringify([source.id, source.version, source.content])));
  }
  const pageFingerprint = fingerprintId("gwpage", JSON.stringify([
    input.audience,
    input.systemPrompt,
    input.envelopePrefix,
    input.envelopeSuffix,
    input.sources.map((source, index) => [source.id, source.version, fingerprints[index]]),
  ]));
  return { pageBytes, fingerprints, pageFingerprint };
}

function renderUserPrompt(input: CreateGraphWorkUnitInput, fragments: readonly GraphWorkFragment[]): string {
  return `${input.envelopePrefix}${JSON.stringify({ audience: input.audience, fragments })}${input.envelopeSuffix}`;
}

function requestBytes(systemPrompt: string, userPrompt: string): number {
  return byteLength(JSON.stringify({ systemPrompt, userPrompt }));
}

function sourceCharOffset(content: string, byteOffset: number): number {
  return Buffer.from(content, "utf8").subarray(0, byteOffset).toString("utf8").length;
}

function isUtf8Boundary(content: string, byteOffset: number): boolean {
  if (byteOffset === 0 || byteOffset === byteLength(content)) return true;
  const bytes = Buffer.from(content, "utf8");
  return byteOffset > 0 && byteOffset < bytes.length && (bytes[byteOffset]! & 0xc0) !== 0x80;
}

function safeEndChar(content: string, end: number): number {
  if (end > 0 && end < content.length) {
    const previous = content.charCodeAt(end - 1);
    const next = content.charCodeAt(end);
    if (previous >= 0xd800 && previous <= 0xdbff && next >= 0xdc00 && next <= 0xdfff) return end - 1;
  }
  return end;
}

function largestFittingEnd(
  input: CreateGraphWorkUnitInput,
  current: readonly GraphWorkFragment[],
  source: GraphWorkSource,
  sourceFingerprint: string,
  startByte: number,
): number {
  const startChar = sourceCharOffset(source.content, startByte);
  const empty: GraphWorkFragment = {
    sourceId: source.id,
    sourceVersion: source.version,
    sourceFingerprint,
    startByte,
    endByte: startByte,
    content: "",
  };
  const fits = (fragment: GraphWorkFragment): boolean =>
    requestBytes(input.systemPrompt, renderUserPrompt(input, [...current, fragment])) <= GRAPH_WORK_LIMITS.maxInputBytes;
  if (!fits(empty)) return startByte;
  let low = startChar;
  let high = source.content.length;
  let best = startChar;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const candidateChar = safeEndChar(source.content, middle);
    if (candidateChar <= startChar) {
      low = middle + 1;
      continue;
    }
    const content = source.content.slice(startChar, candidateChar);
    const endByte = startByte + byteLength(content);
    if (fits({ ...empty, endByte, content })) {
      best = candidateChar;
      low = middle + 1;
    } else {
      high = middle - 1;
    }
  }
  return startByte + byteLength(source.content.slice(startChar, best));
}

export function createNextGraphWorkUnit(input: CreateGraphWorkUnitInput): NextGraphWorkUnit {
  const { fingerprints, pageFingerprint } = validateInput(input);
  const cursor = input.cursor ?? null;
  let sourceIndex = cursor?.sourceIndex ?? 0;
  let byteOffset = cursor?.byteOffset ?? 0;
  if (cursor && (cursor.pageFingerprint !== pageFingerprint || !Number.isInteger(sourceIndex) ||
    sourceIndex < 0 || sourceIndex >= input.sources.length || !Number.isInteger(byteOffset) || byteOffset < 0 ||
    byteOffset >= byteLength(input.sources[sourceIndex]?.content ?? "") ||
    !isUtf8Boundary(input.sources[sourceIndex]?.content ?? "", byteOffset))) throw new GraphWorkStaleCursorError();
  if (sourceIndex === input.sources.length || input.sources.length === 0) {
    if (cursor && sourceIndex !== input.sources.length) throw new GraphWorkStaleCursorError();
    return { pageFingerprint, unit: null, cursor: null, complete: true };
  }

  const fragments: GraphWorkFragment[] = [];
  while (sourceIndex < input.sources.length) {
    const source = input.sources[sourceIndex]!;
    const startByte = byteOffset;
    const contentBytes = byteLength(source.content);
    const endByte = largestFittingEnd(input, fragments, source, fingerprints[sourceIndex]!, startByte);
    if (endByte === startByte && contentBytes > startByte) {
      if (fragments.length > 0) break;
      throw new GraphWorkEnvelopeTooLargeError();
    }
    const startChar = sourceCharOffset(source.content, startByte);
    const content = source.content.slice(startChar, startChar + sourceCharOffset(source.content.slice(startChar), endByte - startByte));
    const fragment: GraphWorkFragment = {
      sourceId: source.id,
      sourceVersion: source.version,
      sourceFingerprint: fingerprints[sourceIndex]!,
      startByte,
      endByte,
      content,
    };
    if (requestBytes(input.systemPrompt, renderUserPrompt(input, [...fragments, fragment])) > GRAPH_WORK_LIMITS.maxInputBytes) {
      if (fragments.length > 0) break;
      throw new GraphWorkEnvelopeTooLargeError();
    }
    fragments.push(fragment);
    if (endByte < contentBytes) {
      byteOffset = endByte;
      break;
    }
    sourceIndex++;
    byteOffset = 0;
  }

  if (fragments.length === 0) throw new GraphWorkEnvelopeTooLargeError();
  const userPrompt = renderUserPrompt(input, fragments);
  const inputBytes = requestBytes(input.systemPrompt, userPrompt);
  if (inputBytes > GRAPH_WORK_LIMITS.maxInputBytes) throw new GraphWorkEnvelopeTooLargeError();
  const identity = JSON.stringify([
    pageFingerprint,
    input.audience,
    fragments.map(({ sourceId, sourceVersion, sourceFingerprint, startByte, endByte }) =>
      [sourceId, sourceVersion, sourceFingerprint, startByte, endByte]),
    input.systemPrompt,
    userPrompt,
  ]);
  const fingerprint = fingerprintId("gwunitfp", identity);
  const unit: GraphWorkUnit = {
    id: fingerprintId("gwunit", identity),
    fingerprint,
    audience: input.audience,
    systemPrompt: input.systemPrompt,
    userPrompt,
    inputBytes,
    maxResponseBytes: GRAPH_WORK_LIMITS.maxResponseBytes,
    fragments,
  };
  const nextCursor = sourceIndex >= input.sources.length
    ? null
    : { pageFingerprint, sourceIndex, byteOffset };
  return { pageFingerprint, unit, cursor: nextCursor, complete: nextCursor === null };
}

export interface GraphWorkResultReference {
  id: string;
  fingerprint: string;
}

export interface GraphWorkReferenceAccumulator {
  batchSequence: number;
  nextSequence: number;
  references: readonly GraphWorkResultReference[];
  referenceBytes: number;
}

export interface GraphWorkReferenceContinuation {
  batchSequence: number;
  nextSequence: number;
  previousBatchFingerprint: string | null;
}

export function createGraphWorkReferenceAccumulator(batchSequence = 0, nextSequence = 0): GraphWorkReferenceAccumulator {
  return { batchSequence, nextSequence, references: [], referenceBytes: byteLength("[]") };
}

function validateReferenceAccumulator(accumulator: GraphWorkReferenceAccumulator): void {
  if (!Number.isSafeInteger(accumulator.batchSequence) || accumulator.batchSequence < 0 ||
    !Number.isSafeInteger(accumulator.nextSequence) || accumulator.nextSequence < 0 ||
    !Array.isArray(accumulator.references) || accumulator.references.length > GRAPH_WORK_LIMITS.maxResultReferences) {
    throw new GraphWorkReferenceSequenceError();
  }
  let rawReferenceBytes = 0;
  for (const reference of accumulator.references) {
    if (!reference || typeof reference.id !== "string" || typeof reference.fingerprint !== "string" ||
      byteLength(reference.id) + byteLength(reference.fingerprint) > GRAPH_WORK_LIMITS.maxResultReferenceBytes) {
      throw new GraphWorkReferenceTooLargeError();
    }
    rawReferenceBytes += byteLength(reference.id) + byteLength(reference.fingerprint);
    if (rawReferenceBytes > GRAPH_WORK_LIMITS.maxResultReferenceBytes) throw new GraphWorkReferenceTooLargeError();
  }
  const encodedBytes = byteLength(JSON.stringify(accumulator.references));
  if (encodedBytes > GRAPH_WORK_LIMITS.maxResultReferenceBytes || encodedBytes !== accumulator.referenceBytes) {
    throw new GraphWorkReferenceTooLargeError();
  }
}

export function appendGraphWorkResultReference(
  accumulator: GraphWorkReferenceAccumulator,
  sequence: number,
  reference: GraphWorkResultReference,
): { accumulator: GraphWorkReferenceAccumulator; flushRequired: boolean; continuation: GraphWorkReferenceContinuation } {
  validateReferenceAccumulator(accumulator);
  if (!Number.isSafeInteger(sequence) || sequence !== accumulator.nextSequence || !reference ||
    typeof reference.id !== "string" || !reference.id || typeof reference.fingerprint !== "string" || !reference.fingerprint) {
    throw new GraphWorkReferenceSequenceError();
  }
  if (byteLength(reference.id) + byteLength(reference.fingerprint) > GRAPH_WORK_LIMITS.maxResultReferenceBytes) {
    if (accumulator.references.length === 0) throw new GraphWorkReferenceTooLargeError();
    const previousBatchFingerprint = fingerprintId("gwrefs", JSON.stringify(accumulator.references));
    return {
      accumulator,
      flushRequired: true,
      continuation: { batchSequence: accumulator.batchSequence, nextSequence: sequence, previousBatchFingerprint },
    };
  }
  const candidate = [...accumulator.references, reference];
  const referenceBytes = byteLength(JSON.stringify(candidate));
  if (candidate.length > GRAPH_WORK_LIMITS.maxResultReferences || referenceBytes > GRAPH_WORK_LIMITS.maxResultReferenceBytes) {
    if (accumulator.references.length === 0) throw new GraphWorkReferenceTooLargeError();
    const previousBatchFingerprint = fingerprintId("gwrefs", JSON.stringify(accumulator.references));
    return {
      accumulator,
      flushRequired: true,
      continuation: { batchSequence: accumulator.batchSequence, nextSequence: sequence, previousBatchFingerprint },
    };
  }
  const next = { ...accumulator, references: candidate, nextSequence: sequence + 1, referenceBytes };
  const full = candidate.length === GRAPH_WORK_LIMITS.maxResultReferences || referenceBytes === GRAPH_WORK_LIMITS.maxResultReferenceBytes;
  return {
    accumulator: next,
    flushRequired: full,
    continuation: {
      batchSequence: accumulator.batchSequence,
      nextSequence: sequence + 1,
      previousBatchFingerprint: full ? fingerprintId("gwrefs", JSON.stringify(candidate)) : null,
    },
  };
}

export function flushGraphWorkResultReferences(accumulator: GraphWorkReferenceAccumulator): {
  fingerprint: string;
  accumulator: GraphWorkReferenceAccumulator;
} {
  validateReferenceAccumulator(accumulator);
  if (accumulator.references.length === 0) throw new GraphWorkReferenceTooLargeError();
  return {
    fingerprint: fingerprintId("gwrefs", JSON.stringify(accumulator.references)),
    accumulator: createGraphWorkReferenceAccumulator(accumulator.batchSequence + 1, accumulator.nextSequence),
  };
}

export interface GraphWorkProgress {
  completedUnits: number;
  completedFragments: number;
  resultReferenceCount: number;
  resultBytes: number;
  resultFingerprint: string;
  lastResultId: string | null;
}

export function combineGraphWorkProgress(
  progress: GraphWorkProgress,
  completion: { unitId: string; fragmentCount: number; result: GraphWorkResultReference; resultBytes: number },
): GraphWorkProgress {
  if (!Number.isSafeInteger(progress.completedUnits) || progress.completedUnits < 0 ||
    !Number.isSafeInteger(progress.completedFragments) || progress.completedFragments < 0 ||
    !Number.isSafeInteger(progress.resultReferenceCount) || progress.resultReferenceCount < 0 ||
    !Number.isSafeInteger(progress.resultBytes) || progress.resultBytes < 0 ||
    completion.fragmentCount < 0 || !Number.isSafeInteger(completion.fragmentCount) ||
    completion.resultBytes < 0 || !Number.isSafeInteger(completion.resultBytes) || !completion.unitId ||
    byteLength(completion.unitId) + byteLength(completion.result.id) + byteLength(completion.result.fingerprint) > GRAPH_WORK_LIMITS.maxResultReferenceBytes) {
    throw new GraphWorkError("GRAPH_WORK_PAGE_INVALID", "Graph work completion counters are invalid");
  }
  const nextIdentity = JSON.stringify([progress.resultFingerprint, completion.unitId, completion.result]);
  return {
    completedUnits: progress.completedUnits + 1,
    completedFragments: progress.completedFragments + completion.fragmentCount,
    resultReferenceCount: progress.resultReferenceCount + 1,
    resultBytes: progress.resultBytes + completion.resultBytes,
    resultFingerprint: fingerprintId("gwprogress", nextIdentity),
    lastResultId: completion.result.id,
  };
}
