import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import type { FireworksBatchWorkItem } from "../types.js";
import { createHash } from "node:crypto";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { boundedBytes, immutablePage, pageKey } from "./consolidation-manifest.js";

export const MAX_CONSOLIDATION_INPUT_BYTES = 8 * 1024;
const PARTITION_INPUT_BYTES = MAX_CONSOLIDATION_INPUT_BYTES - 128;
const MAX_FRAGMENTS = 16384;

export interface ConsolidationFragment {
  sourceId: string;
  version: string;
  fingerprint: string;
  field: string;
  start: number;
  end: number;
  ordinal: number;
  count: number;
  text: string;
}

export function requestInputBytes(systemPrompt: string, userPrompt: string): number {
  return Buffer.byteLength(JSON.stringify({ systemPrompt, userPrompt }), "utf8");
}

export function boundedPartitions<T>(items: T[], prompt: (items: T[]) => string, system: string): T[][] {
  const groups: T[][] = [];
  let current: T[] = [];
  for (const item of items) {
    if (requestInputBytes(system, prompt([item])) > PARTITION_INPUT_BYTES) {
      throw new Error("Consolidation metadata or indivisible candidate exceeds 8 KiB; original source retained locally");
    }
    if (current.length && requestInputBytes(system, prompt([...current, item])) > PARTITION_INPUT_BYTES) {
      groups.push(current);
      current = [];
    }
    current.push(item);
  }
  if (current.length) groups.push(current);
  return groups;
}

export function* iterateSourceFragments(
  source: { id: string; version: string; fields: Record<string, string> },
  system: string, prompt: (fragments: ConsolidationFragment[]) => string,
): Generator<ConsolidationFragment> {
  const fingerprint = fingerprintId("consource", JSON.stringify(source));
  function* fields(): Generator<ConsolidationFragment> {
    for (const [field, text] of Object.entries(source.fields)) {
      let start = 0;
      do {
        const base = { sourceId: source.id, version: source.version, fingerprint, field, start,
          end: start, ordinal: MAX_FRAGMENTS, count: MAX_FRAGMENTS, text: "" };
        if (requestInputBytes(system, prompt([base])) > PARTITION_INPUT_BYTES) throw new Error("Consolidation source metadata exceeds 8 KiB before content; original source retained locally");
        let low = start; let high = Math.min(text.length, start + PARTITION_INPUT_BYTES);
        while (low < high) {
          const middle = Math.ceil((low + high) / 2);
          if (requestInputBytes(system, prompt([{ ...base, end: middle, text: text.slice(start, middle) }])) <= PARTITION_INPUT_BYTES) low = middle;
          else high = middle - 1;
        }
        let end = low;
        if (end < text.length && end > start && /[\uD800-\uDBFF]/.test(text[end - 1]) && /[\uDC00-\uDFFF]/.test(text[end])) end--;
        if (end === start && start < text.length) throw new Error("Consolidation source metadata leaves no UTF-8 content budget; original source retained locally");
        yield { ...base, end, text: text.slice(start, end) }; start = end;
      } while (start < text.length);
    }
  }
  let count = 0;
  for (const _fragment of fields()) if (++count > MAX_FRAGMENTS) throw new Error("Consolidation fragment capacity exhausted; original source retained locally");
  let ordinal = 0;
  for (const fragment of fields()) yield { ...fragment, ordinal: ++ordinal, count };
}

export function fragmentSources(
  sources: Array<{ id: string; version: string; fields: Record<string, string> }>,
  system: string, prompt: (fragments: ConsolidationFragment[]) => string,
): ConsolidationFragment[] {
  const fragments: ConsolidationFragment[] = [];
  for (const source of sources) for (const fragment of iterateSourceFragments(source, system, prompt)) {
    if (fragments.length >= MAX_FRAGMENTS) throw new Error("Consolidation fragment capacity exhausted; original source retained locally");
    fragments.push(fragment);
  }
  return fragments;
}

export interface ConsolidationManifest<T = unknown> {
  id: string;
  tier: "semantic" | "procedural";
  sources: T[];
  checkpoint?: { processedThrough: string; processedSessionIdsAtThrough: string[] };
  fragments: ConsolidationFragment[];
  createdAt: string;
  deferred: boolean;
  status: "pending" | "completed" | "failed";
  error?: string;
  completedAt?: string;
  legacyCohort?: string;
  result?: Record<string, unknown>;
}

export interface RequestCheckpoint {
  id: string;
  manifestId: string;
  systemPrompt: string;
  userPrompt: string;
  status: "pending" | "completed";
  dispatchMode?: "direct" | "queue";
  providerAttempt?: { state: "started" | "rejected"; attempt: number };
  workItemId?: string;
  response?: string;
  responsePages?: { count: number; sha256: string; bytes: number };
  createdAt: string;
  completedAt?: string;
}

export class ConsolidationPending extends Error {
  constructor(readonly manifestId: string) { super("Consolidation provider work queued"); }
}

export interface ConsolidationCallBudget { used: number; maximum: number }
export class ConsolidationRequestBlocked extends Error {
  constructor(readonly requestId: string, readonly workItemId: string | undefined, readonly state: string) {
    super(`consolidation_request_retained:${state}`);
  }
}
type RequestLookup = { found: false } | { found: true; response: string | undefined }
interface ResponsePage { text: string; index: number; count: number; sha256: string; bytes: number }
async function capturedResponse(kv: StateKV, record: RequestCheckpoint): Promise<string | undefined> {
  if (record.status === "completed" && typeof record.response === "string") return record.response;
  const first = await kv.get<ResponsePage>(KV.state, pageKey(record.manifestId, `response:${record.id}`, 0));
  if (!first) {
    if (record.responsePages || record.status === "completed") throw new ConsolidationRequestBlocked(record.id, record.workItemId, "capture_missing");
    return undefined;
  }
  if (record.responsePages && (record.responsePages.count !== first.count || record.responsePages.sha256 !== first.sha256 || record.responsePages.bytes !== first.bytes)) throw new ConsolidationRequestBlocked(record.id, record.workItemId, "capture_invalid");
  if (!Number.isSafeInteger(first.count) || first.count < 1 || first.count > 8 || first.index !== 0 || first.bytes > 65_536) throw new ConsolidationRequestBlocked(record.id, record.workItemId, "capture_invalid");
  let response = "";
  for (let index = 0; index < first.count; index++) {
    const page = index === 0 ? first : await kv.get<ResponsePage>(KV.state, pageKey(record.manifestId, `response:${record.id}`, index));
    if (!page) throw new ConsolidationRequestBlocked(record.id, record.workItemId, "capture_incomplete");
    boundedBytes(page);
    if (page.index !== index || page.count !== first.count || page.sha256 !== first.sha256 || page.bytes !== first.bytes || typeof page.text !== "string") throw new ConsolidationRequestBlocked(record.id, record.workItemId, "capture_invalid");
    response += page.text;
  }
  if (Buffer.byteLength(response) !== first.bytes || createHash("sha256").update(response).digest("hex") !== first.sha256) throw new ConsolidationRequestBlocked(record.id, record.workItemId, "capture_invalid");
  return response;
}
async function captureResponse(kv: StateKV, record: RequestCheckpoint, response: string): Promise<void> {
  if (typeof response !== "string" || Buffer.byteLength(response) > 65_536) throw new Error("Consolidation response exceeds bounded page; request retained locally");
  const texts: string[] = [];
  for (let offset = 0; offset < response.length;) {
    let end = Math.min(offset + 8192, response.length);
    if (end < response.length && /[\uD800-\uDBFF]/.test(response[end - 1]) && /[\uDC00-\uDFFF]/.test(response[end])) end--;
    texts.push(response.slice(offset, end)); offset = end;
  }
  if (!texts.length) texts.push("");
  const metadata = { count: texts.length, sha256: createHash("sha256").update(response).digest("hex"), bytes: Buffer.byteLength(response) };
  for (let index = 0; index < texts.length; index++) await immutablePage(kv, pageKey(record.manifestId, `response:${record.id}`, index), { ...metadata, index, text: texts[index] });
  const completed = { ...record, status: "completed" as const, responsePages: metadata, completedAt: new Date().toISOString() };
  boundedBytes(completed); await kv.set(KV.state, record.id, completed);
}
interface ReplacementLink { manifestId: string; requestId: string; oldWorkItemId: string; newWorkItemId: string; depth: number }
async function requestWork(kv: StateKV, record: RequestCheckpoint): Promise<{ id: string; depth: number; chain: string[] }> {
  let id = record.workItemId!; const seen = new Set([id]);
  for (let depth = 0; depth < 12; depth++) {
    const link = await kv.get<ReplacementLink>(KV.state, pageKey(record.manifestId, `replacement:${record.id}`, id));
    if (!link) return { id, depth, chain: [...seen] };
    boundedBytes(link);
    if (link.manifestId !== record.manifestId || link.requestId !== record.id || link.oldWorkItemId !== id || !link.newWorkItemId || seen.has(link.newWorkItemId) || link.depth !== depth + 1) throw new ConsolidationRequestBlocked(record.id, id, "replacement_identity_invalid");
    id = link.newWorkItemId; seen.add(id);
  }
  return { id, depth: 12, chain: [...seen] };
}
function exactRequestWork(record: RequestCheckpoint, item: FireworksBatchWorkItem | null, id: string): item is FireworksBatchWorkItem {
  return Boolean(item && item.id === id && item.task === "consolidation" && item.correlationId === record.id && item.customId === record.id
    && item.systemPrompt === record.systemPrompt && item.userPrompt === record.userPrompt && item.metadata?.consolidationManifestId === record.manifestId
    && item.metadata?.consolidationPartitionId === record.id && item.metadata?.sourceFingerprint === record.id);
}
function definitiveProviderRejection(error: unknown): boolean {
  if (typeof error !== "object" || error === null || !("status" in error)) return false;
  return typeof error.status === "number" && [400, 401, 403, 404, 413, 422, 429].includes(error.status);
}

export class ConsolidationRequests {
  pending = false;
  constructor(
    private kv: StateKV,
    readonly manifestId: string,
    private summarize: (system: string, prompt: string, request: Readonly<Pick<RequestCheckpoint, "id" | "manifestId">>) => Promise<string>,
    private queue?: FireworksBatchQueue,
    private budget: ConsolidationCallBudget = { used: 0, maximum: 2 },
    private capturedOnly = false,
  ) {}

  async checkpointRound(round: number, candidates: readonly unknown[]): Promise<void> {
    const id = fingerprintId("conround", `${this.manifestId}:${round}`);
    const existing = await this.kv.get<{ candidates: unknown[] }>(KV.state, id);
    if (existing) {
      if (JSON.stringify(existing.candidates) !== JSON.stringify(candidates)) throw new Error("Consolidation reduction round changed during recovery; originals retained locally");
      return;
    }
    await this.kv.set(KV.state, id, { id, manifestId: this.manifestId, round, candidates, createdAt: new Date().toISOString() });
  }

  async hasRecordedRequest(systemPrompt: string, userPrompt: string, compatibleSystemPrompts: readonly string[] = []): Promise<boolean> {
    userPrompt = `Consolidation manifest: ${this.manifestId}\n\n${userPrompt}`;
    const visited = new Set<string>();
    for (const [index, prompt] of [systemPrompt, ...compatibleSystemPrompts].entries()) {
      if (visited.has(prompt)) continue;
      visited.add(prompt);
      if (index > 0 && requestInputBytes(prompt, userPrompt) > MAX_CONSOLIDATION_INPUT_BYTES) continue;
      const id = fingerprintId("conrequest", JSON.stringify([this.manifestId, prompt, userPrompt]));
      const previous = await withKeyedLock(`consolidation-request:${id}`, () => this.kv.get<RequestCheckpoint>(KV.state, id));
      if (!previous) continue;
      if (previous.id !== id || previous.manifestId !== this.manifestId || previous.systemPrompt !== prompt || previous.userPrompt !== userPrompt) {
        throw new ConsolidationRequestBlocked(id, previous.workItemId, "request_identity_invalid");
      }
      return true;
    }
    return false;
  }

  async request(systemPrompt: string, userPrompt: string, compatibleSystemPrompts: readonly string[] = []): Promise<string | undefined> {
    userPrompt = `Consolidation manifest: ${this.manifestId}\n\n${userPrompt}`;
    const visited = new Set([systemPrompt]);
    for (const compatibleSystemPrompt of compatibleSystemPrompts) {
      if (visited.has(compatibleSystemPrompt)) continue;
      visited.add(compatibleSystemPrompt);
      if (requestInputBytes(compatibleSystemPrompt, userPrompt) > MAX_CONSOLIDATION_INPUT_BYTES) continue;
      const compatible = await this.requestExact(compatibleSystemPrompt, userPrompt, false);
      if (compatible.found) return compatible.response;
    }
    const current = await this.requestExact(systemPrompt, userPrompt, true);
    if (!current.found) throw new Error("consolidation_request_lookup_invalid");
    return current.response;
  }

  private async requestExact(systemPrompt: string, userPrompt: string, allowDispatch: boolean): Promise<RequestLookup> {
    if (requestInputBytes(systemPrompt, userPrompt) > MAX_CONSOLIDATION_INPUT_BYTES) throw new Error("Consolidation request exceeds 8 KiB; originals retained locally");
    const id = fingerprintId("conrequest", JSON.stringify([this.manifestId, systemPrompt, userPrompt]));
    return withKeyedLock(`consolidation-request:${id}`, async () => {
    const previous = await this.kv.get<RequestCheckpoint>(KV.state, id);
    if (!previous && !allowDispatch) return { found: false };
    if (previous && (previous.id !== id || previous.manifestId !== this.manifestId || previous.systemPrompt !== systemPrompt || previous.userPrompt !== userPrompt)) throw new ConsolidationRequestBlocked(id, previous.workItemId, "request_identity_invalid");
    if (previous) { const captured = await capturedResponse(this.kv, previous); if (captured !== undefined) return { found: true, response: captured }; }
    if (this.capturedOnly) throw Object.assign(new Error("consolidation_legacy_completed_response_missing"), { code: "CONSOLIDATION_LEGACY_CAPTURE_MISSING" });
    if (previous?.workItemId) {
      const work = await requestWork(this.kv, previous);
      const item = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, work.id);
      if (!exactRequestWork(previous, item, work.id)) throw new ConsolidationRequestBlocked(id, work.id, item ? "work_identity_invalid" : "work_missing");
      if (item.state === "completed" && item.result?.customId === id && typeof item.result.content === "string") {
        await captureResponse(this.kv, previous, item.result.content); return { found: true, response: item.result.content };
      }
      if (["completed", "stale", "failed", "dead-letter"].includes(item.state)) throw new ConsolidationRequestBlocked(id, work.id, item.state);
      if (!["queued", "submitted", "polling"].includes(item.state)) throw new ConsolidationRequestBlocked(id, work.id, "work_state_invalid");
      this.pending = true; return { found: true, response: undefined };
    }
    if (previous?.providerAttempt?.state === "started") throw new ConsolidationRequestBlocked(id, undefined, "provider_dispatch_uncertain");
    if (previous && previous.dispatchMode !== "queue" && previous.providerAttempt?.state !== "rejected") throw new ConsolidationRequestBlocked(id, undefined, "legacy_provider_dispatch_unknown");
    if (previous?.dispatchMode === "queue" && !this.queue) throw new ConsolidationRequestBlocked(id, undefined, "queue_dispatch_unresolved");
    if (!allowDispatch) throw new ConsolidationRequestBlocked(id, previous?.workItemId, "compatible_request_provider_retry_disabled");
    if (this.budget.used >= this.budget.maximum) throw new ConsolidationPending(this.manifestId);
    const record: RequestCheckpoint = previous ?? { id, manifestId: this.manifestId, systemPrompt, userPrompt, status: "pending", createdAt: new Date().toISOString() };
    if (this.queue) {
      const queuedRecord = { ...record, dispatchMode: "queue" as const };
      await this.kv.set(KV.state, id, queuedRecord);
      this.budget.used++;
      const result = await this.queue.enqueue({
        task: "consolidation", correlationId: id, systemPrompt, userPrompt,
        metadata: { tier: "durable", sourceFingerprint: id, consolidationManifestId: this.manifestId, consolidationPartitionId: id },
      });
      if (!result.queued || !result.workItemId) throw new Error(result.reason ?? "Consolidation queue rejected durable request");
      await this.kv.set(KV.state, id, { ...queuedRecord, workItemId: result.workItemId });
      this.pending = true;
      return { found: true, response: undefined };
    }
    const attempt = (record.providerAttempt?.attempt ?? 0) + 1;
    if (!Number.isSafeInteger(attempt) || attempt < 1) throw new ConsolidationRequestBlocked(id, undefined, "provider_attempt_invalid");
    const dispatched: RequestCheckpoint = { ...record, dispatchMode: "direct", providerAttempt: { state: "started", attempt } };
    await this.kv.set(KV.state, id, dispatched);
    this.budget.used++;
    let response: string;
    try { response = await this.summarize(systemPrompt, userPrompt, { id, manifestId: this.manifestId }); }
    catch (error) {
      if (definitiveProviderRejection(error)) await this.kv.set(KV.state, id, { ...dispatched, providerAttempt: { state: "rejected", attempt } });
      throw error;
    }
    await captureResponse(this.kv, dispatched, response);
    return { found: true, response };
    });
  }

  assertReady(): void { if (this.pending) throw new ConsolidationPending(this.manifestId); }
}

export async function acceptConsolidationResponse(kv: StateKV, manifestId: string, partitionId: string, response: string): Promise<void> {
  if (typeof response !== "string" || Buffer.byteLength(response, "utf8") > 65_536) throw new Error("Consolidation response exceeds bounded page; request retained locally");
  return withKeyedLock(`consolidation-request:${partitionId}`, async () => {
  const request = await kv.get<RequestCheckpoint>(KV.state, partitionId);
  if (!request || request.manifestId !== manifestId || request.id !== partitionId) throw new Error("Unknown consolidation partition; originals retained locally");
  if (request.status === "completed") {
    if (await capturedResponse(kv, request) !== response) throw new Error("Conflicting consolidation provider result; original result retained locally");
    return;
  }
  await captureResponse(kv, request, response);
  });
}

export async function replaceConsolidationRequest(kv: StateKV, queue: FireworksBatchQueue, canReplace: (id: string) => Promise<boolean>,
  input: { manifestId: string; partitionId: string; workItemId: string }, allowReplacement = true): Promise<Record<string, unknown>> {
  return withKeyedLock(`consolidation-request:${input.partitionId}`, async () => {
    const request = await kv.get<RequestCheckpoint>(KV.state, input.partitionId);
    if (!request || request.id !== input.partitionId || request.manifestId !== input.manifestId) throw new Error("consolidation_replacement_request_mismatch");
    if (!request.workItemId) throw new Error("consolidation_replacement_work_missing");
    const work = await requestWork(kv, request);
    if (!work.chain.includes(input.workItemId)) throw new Error("consolidation_replacement_work_mismatch");
    const original = await kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, input.workItemId);
    if (!exactRequestWork(request, original, input.workItemId)) throw new Error("consolidation_replacement_work_identity_invalid");
    if (await capturedResponse(kv, request) !== undefined) return { success: true, queued: false, replayed: true };
    if (!allowReplacement) throw new ConsolidationRequestBlocked(request.id, input.workItemId, "manifest_terminal");
    if (work.id !== input.workItemId) {
      if (!work.chain.includes(input.workItemId)) throw new Error("consolidation_replacement_work_mismatch");
      return { success: true, queued: true, replayed: true, workItemId: work.id };
    }
    if (work.depth >= 12) throw new ConsolidationRequestBlocked(request.id, work.id, "replacement_limit");
    const item = await kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, work.id);
    if (!exactRequestWork(request, item, work.id)) throw new Error("consolidation_replacement_work_identity_invalid");
    if (!await canReplace(work.id)) throw new ConsolidationRequestBlocked(request.id, work.id, "replacement_ineligible");
    const result = await queue.enqueue({ task: "consolidation", replacementOf: work.id, correlationId: request.id,
      systemPrompt: request.systemPrompt, userPrompt: request.userPrompt, metadata: { tier: "durable", sourceFingerprint: request.id,
        consolidationManifestId: request.manifestId, consolidationPartitionId: request.id } });
    if (!result.queued || !result.workItemId || result.workItemId === work.id) throw new Error("consolidation_replacement_enqueue_rejected");
    const latest = await kv.get<RequestCheckpoint>(KV.state, request.id);
    if (!latest || latest.manifestId !== request.manifestId || latest.systemPrompt !== request.systemPrompt || latest.userPrompt !== request.userPrompt) throw new Error("consolidation_replacement_request_changed");
    if (await capturedResponse(kv, latest) !== undefined) return { success: true, queued: false, replayed: true, workItemId: result.workItemId };
    await immutablePage(kv, pageKey(request.manifestId, `replacement:${request.id}`, work.id), {
      manifestId: request.manifestId, requestId: request.id, oldWorkItemId: work.id, newWorkItemId: result.workItemId, depth: work.depth + 1 });
    return { success: true, queued: true, workItemId: result.workItemId };
  });
}
