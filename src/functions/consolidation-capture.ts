import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import { isStateCounter } from "../state/state-transactions.js";
import { requestInputBytes, MAX_CONSOLIDATION_INPUT_BYTES, type ConsolidationFragment } from "./consolidation-partitions.js";
import { SEMANTIC_FRAGMENT_SYSTEM, PROCEDURAL_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt } from "../prompts/consolidation.js";
import { MEMORY_SYSTEM } from "./consolidation-reduction.js";
import { stripPrivateData } from "./privacy.js";
import { CONSOLIDATION_SOURCE_BYTES, immutablePage, pageKey, saveRoot,
  type FragmentPage, type PagedConsolidation, type PagedSource, type SourceHeader } from "./consolidation-manifest.js";

interface CapturedSource { value: Record<string, unknown>; version: string; json: string }
export interface ConceptPosting { concept: string; sourceIds: string[]; sessionIds: string[] }
const PROTECTED_PIPELINE_SIGNAL = /\b(?:error|failed|failure|decision|instruction|prompt|security|secret|token|password|credential|api[-_ ]?key|auth|permission|mutat(?:e|ion)|write|edit|patch|delete|remove|deploy|install|bug|fix|shell|terminal|bash|powershell|environment|env|(?:AGENTS|CLAUDE|GEMINI|COPILOT)\.md)\b/i;

export function hasProtectedPipelineSignal(text: string): boolean {
  return PROTECTED_PIPELINE_SIGNAL.test(text) || stripPrivateData(text) !== text;
}

function shortState(value: unknown, limit: number): string {
  return stripPrivateData(typeof value === "string" ? value : "").slice(0, limit);
}

function typeSafeHeader(ref: PagedSource, value: Record<string, unknown>, createdAt: string | undefined,
  rootCreatedAt: string): NonNullable<SourceHeader["typesafe"]> {
  if (ref.kind === "summary") {
    const title = shortState(value.title, 56);
    const narrative = shortState(value.narrative, 96);
    const concepts = Array.isArray(value.concepts) ? value.concepts.filter((item): item is string => typeof item === "string") : [];
    const keyDecisions = Array.isArray(value.keyDecisions) ? value.keyDecisions : [];
    const filesModified = Array.isArray(value.filesModified) ? value.filesModified : [];
    const signalText = `${value.title ?? ""} ${value.narrative ?? ""} ${keyDecisions.join(" ")}`;
    const protectedItem = keyDecisions.length > 0 || filesModified.length > 0 || hasProtectedPipelineSignal(signalText);
    const deterministicDrop = value.observationCount === 0 && !String(value.title ?? "").trim()
      && !String(value.narrative ?? "").trim() && concepts.length === 0 && keyDecisions.length === 0 && filesModified.length === 0;
    const state = stripPrivateData(JSON.stringify({ title, narrative,
      concepts: concepts.slice(0, 3).map(item => shortState(item, 24)), observationCount: value.observationCount, createdAt })).slice(0, 512);
    const gateState = stripPrivateData(JSON.stringify({ title: shortState(value.title, 48), conceptCount: concepts.length,
      concepts: concepts.slice(0, 3).map(item => shortState(item, 24)), preview: shortState(value.narrative, 96) })).slice(0, 512);
    return { protected: protectedItem, deterministicDrop, state, gateProtected: protectedItem, gateState, ...(createdAt ? { updatedAt: createdAt } : {}) };
  }
  const title = shortState(value.title, 64);
  const narrative = shortState(value.narrative ?? value.content, 192);
  const facts = Array.isArray(value.facts) ? value.facts.filter((item): item is string => typeof item === "string") : [];
  const content = `${title} ${narrative} ${facts.slice(0, 3).map(item => shortState(item, 48)).join(" ")}`.trim();
  const updatedAt = typeof value.updatedAt === "string" ? value.updatedAt : createdAt;
  const timestamp = Date.parse(updatedAt ?? "");
  const rootTimestamp = Date.parse(rootCreatedAt);
  const ageDays = Number.isFinite(timestamp) && Number.isFinite(rootTimestamp)
    ? Math.max(0, Math.floor((rootTimestamp - timestamp) / (24 * 60 * 60 * 1000))) : undefined;
  const protectedItem = !Number.isFinite(timestamp) || ageDays! <= 7 || hasProtectedPipelineSignal(content);
  const frequency = Number(value.frequency ?? 1);
  const deterministicDrop = (ref.kind === "memory" && frequency < 2) || !content.trim();
  const state = stripPrivateData(JSON.stringify({ frequency, preview: content.slice(0, 192), ageDays })).slice(0, 512);
  const gateState = stripPrivateData(JSON.stringify({ frequency, preview: content.slice(0, 72) })).slice(0, 512);
  return { protected: protectedItem, deterministicDrop, state, gateProtected: frequency >= 3 || hasProtectedPipelineSignal(content), gateState,
    ...(updatedAt ? { updatedAt } : {}) };
}
export async function readSource(kv: StateKV, ref: PagedSource): Promise<CapturedSource> {
  if (typeof kv.getVersioned !== "function") throw new Error("consolidation_versioned_source_required");
  const session = await kv.get<{ project: string }>(KV.sessions, ref.sessionId);
  if (session?.project !== ref.project) throw new Error("consolidation_source_scope_mismatch");
  const result = await kv.getVersioned<Record<string, unknown>>(ref.scope, ref.id);
  if (!result.exists || !result.value || !isStateCounter(result.version)
    || (ref.rowVersion !== undefined && result.version !== ref.rowVersion)) throw new Error("consolidation_source_changed");
  const value = result.value;
  if (ref.kind === "summary" && (value.sessionId !== ref.sessionId || value.project !== ref.project)
    || ref.kind === "observation" && (value.id !== ref.id || value.sessionId !== ref.sessionId)
    || ref.kind === "memory" && (value.id !== ref.id || value.project !== ref.project || value.isLatest !== true)) {
    throw new Error("consolidation_source_scope_mismatch");
  }
  const json = JSON.stringify(value);
  if (Buffer.byteLength(json) > CONSOLIDATION_SOURCE_BYTES) throw new Error("consolidation_source_codec_limit");
  return { value, version: result.version, json };
}
function endOfFragment(text: string, start: number, fits: (text: string, end: number) => boolean, maximum = 2048): number {
  let low = start; let high = Math.min(text.length, start + maximum);
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (Buffer.byteLength(text.slice(start, middle)) <= maximum && fits(text.slice(start, middle), middle)) low = middle;
    else high = middle - 1;
  }
  if (low < text.length && low > start && /[\uD800-\uDBFF]/.test(text[low - 1]) && /[\uDC00-\uDFFF]/.test(text[low])) low--;
  if (low === start && start < text.length) throw new Error("consolidation_fragment_cannot_progress");
  return low;
}
function headerFor(ref: PagedSource, source: CapturedSource, rootCreatedAt: string, kind: PagedConsolidation["kind"]): SourceHeader {
  const { value } = source;
  const concepts = Array.isArray(value.concepts) ? value.concepts : [];
  const sessionIds = Array.isArray(value.sessionIds) ? value.sessionIds : [];
  const hasContent = [value.narrative, value.content].some(text => typeof text === "string" && text.trim().length > 0)
    || Array.isArray(value.facts) && value.facts.some(text => typeof text === "string" && text.trim().length > 0);
  const eligible = kind === "procedural" ? hasContent && (ref.kind === "observation" || ref.kind === "memory" && value.type === "pattern")
    : ref.kind === "summary" || ref.kind === "memory" && value.type === "pattern" && sessionIds.length >= 2
    || ref.kind === "observation" && typeof value.title === "string" && !!value.title.trim() && Number(value.importance) >= 5;
  return { ...ref, rowVersion: source.version, fingerprint: fingerprintId("consource", source.json), bytes: Buffer.byteLength(source.json),
    frequency: ref.kind === "memory" ? new Set(sessionIds).size : 1, eligible, conceptCount: concepts.length,
    ...(typeof value.createdAt === "string" ? { createdAt: value.createdAt }
      : typeof value.timestamp === "string" ? { createdAt: value.timestamp } : {}),
    typesafe: typeSafeHeader(ref, value, typeof value.updatedAt === "string" ? value.updatedAt
      : typeof value.createdAt === "string" ? value.createdAt : typeof value.timestamp === "string" ? value.timestamp : undefined, rootCreatedAt) };
}
export async function captureSourceCut(kv: StateKV, root: PagedConsolidation): Promise<void> {
  if (root.capture.source >= root.refs.length) { root.phase = "quality"; await saveRoot(kv, root); return; }
  const index = root.capture.source; const ref = root.refs[index];
  const source = await readSource(kv, ref);
  const header = headerFor(ref, source, root.createdAt, root.kind);
  await immutablePage(kv, pageKey(root.id, "source", index), header);
  ref.rowVersion = header.rowVersion;
  const concepts = Array.isArray(source.value.concepts) ? source.value.concepts : [];
  let processed = 0;
  const conceptPage: string[] = [];
  const conceptStart = root.capture.concepts;
  while (root.capture.concepts < concepts.length && processed++ < 12) {
    const concept = concepts[root.capture.concepts];
    if (header.eligible && typeof concept === "string" && concept.trim() && Buffer.byteLength(concept) <= 256) {
      conceptPage.push(concept.toLowerCase());
      const key = pageKey(root.id, "concept", concept.toLowerCase());
      const old = await kv.get<ConceptPosting>(KV.state, key);
      const posting: ConceptPosting = { concept: concept.toLowerCase(),
        sourceIds: [...new Set([...(old?.sourceIds ?? []), ref.id])], sessionIds: [...new Set([...(old?.sessionIds ?? []), ref.sessionId])] };
      await kv.set(KV.state, key, posting);
    }
    root.capture.concepts++;
  }
  if (root.capture.concepts > conceptStart) await immutablePage(kv, pageKey(root.id, `source-concepts:${index}`, conceptStart), conceptPage);
  if (root.capture.concepts < concepts.length) { await saveRoot(kv, root); return; }
  const fragments: ConsolidationFragment[] = [];
  let offset = root.capture.offset; let ordinal = root.capture.ordinal;
  const system = root.kind === "semantic" ? SEMANTIC_FRAGMENT_SYSTEM : root.kind === "procedural" ? PROCEDURAL_FRAGMENT_SYSTEM : MEMORY_SYSTEM;
  while (offset < source.json.length && fragments.length < 12) {
    const fragment = (text: string, end: number): ConsolidationFragment => ({ sourceId: ref.kind === "summary" ? ref.sessionId : ref.id,
      version: header.rowVersion, fingerprint: header.fingerprint, field: "$json", start: offset, end, ordinal: ordinal + 1, count: 0, text });
    const end = endOfFragment(source.json, offset, (text, end) => requestInputBytes(system, `Consolidation manifest: ${root.id}\n\n${buildConsolidationFragmentPrompt([
      { ...fragment(text, end), ...(root.kind === "procedural" ? { frequency: Number.MAX_SAFE_INTEGER } : {}) }])}`) <= MAX_CONSOLIDATION_INPUT_BYTES - 128);
    fragments.push(fragment(source.json.slice(offset, end), end)); ordinal++;
    offset = end;
  }
  if (fragments.length) {
    await immutablePage<FragmentPage>(kv, pageKey(root.id, "fragments", root.fragmentPages), { fragments, source: index });
    root.fragmentPages++; root.fragmentCount += fragments.length;
  }
  root.capture.offset = offset; root.capture.ordinal = ordinal;
  if (offset === source.json.length) {
    await immutablePage(kv, pageKey(root.id, "capture-complete", index), { fingerprint: header.fingerprint, length: offset, fragmentCount: ordinal });
    root.capture = { source: index + 1, offset: 0, ordinal: 0, concepts: 0 };
    if (root.capture.source >= root.refs.length) root.phase = "quality";
  }
  await saveRoot(kv, root);
}
export async function verifySourceCut(kv: StateKV, root: PagedConsolidation): Promise<void> {
  if (root.verify >= root.refs.length) { root.phase = "aggregate"; await saveRoot(kv, root); return; }
  const { source, header } = await verifyCapturedSource(kv, root, root.verify);
  if (root.legacy && !root.legacy.verified) {
    root.refs[root.verify].rowVersion = source.version;
    await kv.set(KV.state, pageKey(root.id, "source", root.verify), { ...header, rowVersion: source.version });
  }
  root.verify++;
  if (root.verify >= root.refs.length) {
    if (root.legacy && !root.legacy.verified) {
      root.legacy.verified = true; root.eligibleSources = root.refs.map((_, index) => index);
      if (root.legacy.recoveredFinal) root.phase = "aggregate";
      else if (root.legacy.seedComplete) { root.verify = 0; root.phase = "reduce"; }
      else { root.verify = 0; root.phase = "extract"; }
    } else root.phase = "aggregate";
  }
  await saveRoot(kv, root);
}
export async function verifyCapturedSource(kv: StateKV, root: PagedConsolidation, index: number) {
  const source = await readSource(kv, root.refs[index]);
  const header = await kv.get<SourceHeader>(KV.state, pageKey(root.id, "source", index));
  const matches = header?.legacy ? header.kind === "observation"
    ? header.legacy.observationFingerprint === fingerprintId("conobs", source.json)
    : header.legacy.domainVersion === source.value.version && header.legacy.updatedAt === source.value.updatedAt
      && header.legacy.contentFingerprint === fingerprintId("concontent", String(source.value.content))
    : header?.fingerprint === fingerprintId("consource", source.json);
  if (!header || !matches || (header.rowVersion !== "0" && source.version !== header.rowVersion)) {
    throw new Error("consolidation_source_changed_before_effects");
  }
  return { source, header };
}
export async function verifySourceProjects(kv: StateKV, root: PagedConsolidation): Promise<void> {
  const sessions = await Promise.all(root.refs.map(ref => kv.get<{ project: string }>(KV.sessions, ref.sessionId)));
  if (sessions.some((session, index) => session?.project !== root.refs[index].project)) throw new Error("consolidation_source_scope_mismatch");
}
export async function proceduralFrequency(kv: StateKV, root: PagedConsolidation, source: number): Promise<number> {
  const header = await kv.get<SourceHeader>(KV.state, pageKey(root.id, "source", source));
  if (!header?.eligible) return 0;
  if (root.legacy || header.kind === "memory") return header.frequency;
  return (await kv.get<{ frequency: number }>(KV.state, pageKey(root.id, "quality", source)))?.frequency ?? 0;
}

export async function qualityCut(kv: StateKV, root: PagedConsolidation): Promise<void> {
  const index = root.quality.source;
  if (index >= root.refs.length) { root.phase = "extract"; await saveRoot(kv, root); return; }
  const header = await kv.get<SourceHeader>(KV.state, pageKey(root.id, "source", index));
  if (!header) throw new Error("consolidation_source_header_missing");
  let frequency = (await kv.get<{ frequency: number }>(KV.state, pageKey(root.id, "quality", index)))?.frequency ?? (header.kind === "memory" ? header.frequency : 0);
  if (root.kind === "procedural") frequency = header.frequency;
  const concepts = await kv.get<string[]>(KV.state, pageKey(root.id, `source-concepts:${index}`, root.quality.page)) ?? [];
  for (const concept of root.kind === "procedural" ? [] : concepts) {
    const posting = await kv.get<ConceptPosting>(KV.state, pageKey(root.id, "concept", concept));
    frequency = Math.max(frequency, posting?.sourceIds.length ?? 0);
  }
  await kv.set(KV.state, pageKey(root.id, "quality", index), { frequency });
  root.quality.page += 12;
  if (root.quality.page >= header.conceptCount) {
    if (header.eligible && (root.kind === "semantic" || root.kind === "procedural" || frequency >= 3)) root.eligibleSources.push(index);
    root.quality = { source: index + 1, page: 0 };
    if (root.quality.source >= root.refs.length) {
      if (root.kind === "memory" && root.eligibleSources.length < (root.minObservations ?? 10)) root.eligibleSources = [];
      if (root.kind === "semantic" && root.baselineCheckpoint) {
        const headers = await Promise.all(root.refs.map((_, position) => kv.get<SourceHeader>(KV.state, pageKey(root.id, "source", position))));
        const baseline = root.baselineCheckpoint;
        const fresh = headers.filter(value => value?.createdAt && (value.createdAt > baseline.processedThrough
          || value.createdAt === baseline.processedThrough && !baseline.processedSessionIdsAtThrough.includes(value.sessionId)));
        if (fresh.length < (root.minimumNewSummaries ?? 5)) {
          root.eligibleSources = []; root.checkpoint = baseline; root.emptyReason = "fewer than required new summaries";
        }
      }
      root.phase = "extract";
    }
  }
  await saveRoot(kv, root);
}
