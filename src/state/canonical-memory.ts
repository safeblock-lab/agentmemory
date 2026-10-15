import { createHash } from "node:crypto";
import type { StateKV } from "./kv.js";
import { KV } from "./schema.js";
import { isStateCounter } from "./state-transactions.js";
import { retrievalRequest } from "./indexed-retrieval.js";
import type { BatchEffectMetadata, Memory } from "../types.js";

export type CanonicalScope = "semantic" | "procedural" | "memories";
type OperationKind = "upsert" | "evolve" | "decay";
export const MAX_CANONICAL_RECORD_BYTES = 16 * 1024 * 1024 - 65_536 - 4096;
export interface CanonicalLookup {
  status: "ready"; scope: CanonicalScope; partition: string; normalizedKey: string;
  canonicalId: string | null; rowVersion: string | null; scopeRevision: string;
  coverageEpoch: string; normalizationVersion: "js-lower-v1";
}
export interface CanonicalReceipt {
  operationKind: OperationKind; effectKey: string; operationFingerprint: string;
  id: string; rowVersion: string; created: boolean; resultFingerprint: string;
}
export interface CanonicalWriteResult {
  applied: boolean; created: boolean; id: string; rowVersion: string; receipt: CanonicalReceipt;
}
export interface CanonicalSourceRef { id: string; rowVersion: string }
export interface CanonicalSourcePage {
  items: CanonicalSourceRef[]; nextCursor: string | null; membershipGeneration: string; coverageEpoch: string;
}
export class CanonicalMemoryPending extends Error {
  readonly code = "STATE_CANONICAL_PREPARING";
  constructor(readonly continuation: string, readonly partial = false) {
    super(partial ? "Canonical index has incomplete records; continuation retained" : "Canonical index preparation continues");
  }
}
export class CanonicalMemoryConflict extends Error {
  readonly code = "STATE_TX_CONFLICT";
  constructor() { super("Canonical state changed; captured operation remains pending"); }
}
function invalid(): never { throw new Error("STATE_TX_INVALID_RESPONSE: Invalid canonical memory response"); }
function object(value: unknown): value is Record<string, unknown> { return !!value && typeof value === "object" && !Array.isArray(value); }
function text(value: unknown, max = 256): value is string { return typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= max; }
function hash(value: unknown): string { return createHash("sha256").update(JSON.stringify(value)).digest("hex"); }
function digest(value: unknown): value is string { return typeof value === "string" && /^[a-f0-9]{64}$/.test(value); }
function count(value: unknown): value is number { return Number.isSafeInteger(value) && Number(value) >= 0; }
function conflict(error: unknown): boolean {
  return object(error) && error.code === "STATE_TX_CONFLICT" || error instanceof Error && error.message.includes("STATE_TX_CONFLICT");
}
function identity(scope: CanonicalScope, key: string, partition: string): void {
  if (!["semantic", "procedural", "memories"].includes(scope) || !text(key, 1_048_576) || key !== key.toLowerCase()
    || (scope === "memories" ? !text(partition) : partition !== "")) throw new Error("canonical_identity_invalid");
}
function sessions(ids: string[]): void {
  if (!Array.isArray(ids) || !ids.length || ids.length > 12 || new Set(ids).size !== ids.length || ids.some(id => !text(id))) {
    throw new Error("canonical_source_sessions_invalid");
  }
}

export class CanonicalMemoryIndex {
  constructor(private readonly kv: StateKV) {}

  async lookup(scope: CanonicalScope, normalizedKey: string, partition = ""): Promise<CanonicalLookup> {
    identity(scope, normalizedKey, partition);
    for (let round = 0; round < 2; round++) {
      const raw = await retrievalRequest<unknown>(this.kv, { action: "canonical_lookup", scope: KV[scope], partition, normalized_key: normalizedKey });
      if (!object(raw)) invalid();
      if (raw.status === "preparing" || raw.status === "partial") {
        if (!text(raw.continuation, 4096) || !count(raw.invalid_records)) invalid();
        if (round === 0 && raw.status === "preparing") {
          await this.prepare(scope);
          continue;
        }
        throw new CanonicalMemoryPending(raw.continuation, raw.status === "partial");
      }
      if (raw.status !== "ready" || !(raw.canonical_id === null || text(raw.canonical_id))
        || !(raw.row_version === null || isStateCounter(raw.row_version)) || (raw.canonical_id === null) !== (raw.row_version === null)
        || !isStateCounter(raw.scope_revision) || !isStateCounter(raw.coverage_epoch) || raw.normalization_version !== "js-lower-v1") invalid();
      return { status: "ready", scope, partition, normalizedKey, canonicalId: raw.canonical_id, rowVersion: raw.row_version,
        scopeRevision: raw.scope_revision, coverageEpoch: raw.coverage_epoch, normalizationVersion: raw.normalization_version };
    }
    throw new CanonicalMemoryConflict();
  }

  async prepare(scope: CanonicalScope): Promise<void> {
    const raw = await retrievalRequest<unknown>(this.kv, { action: "canonical_prepare", scope: KV[scope], max_rows: 16, max_bytes: 65_536 });
    if (!object(raw) || !["pending", "ready", "partial"].includes(String(raw.status))
      || !Number.isSafeInteger(raw.processed) || Number(raw.processed) < 0 || Number(raw.processed) > 16
      || !isStateCounter(raw.scope_revision) || !isStateCounter(raw.coverage_epoch) || !count(raw.invalid_records)
      || !(raw.continuation === null || text(raw.continuation, 4096)) || typeof raw.cursor !== "string") invalid();
    if (raw.status !== "ready") throw new CanonicalMemoryPending(String(raw.continuation ?? raw.cursor), raw.status === "partial");
  }

  private async current<T>(match: CanonicalLookup): Promise<T | null> {
    if (!match.canonicalId) return null;
    const row = await this.kv.getVersioned<T>(KV[match.scope], match.canonicalId);
    if (!row.exists || !row.value || row.version !== match.rowVersion) throw new CanonicalMemoryConflict();
    if (Buffer.byteLength(JSON.stringify(row.value)) > 16 * 1024 * 1024) throw new Error("canonical_record_too_large");
    const value: unknown = row.value;
    if (!object(value) || value.id !== match.canonicalId) invalid();
    if (match.scope !== "memories") {
      const key = match.scope === "semantic" ? value.fact : value.name;
      if (typeof key !== "string" || key.toLowerCase() !== match.normalizedKey || !Array.isArray(value.sourceSessionIds)
        || value.sourceSessionIds.some(id => !text(id)) || !Number.isFinite(value.strength)
        || !Number.isFinite(match.scope === "semantic" ? value.accessCount : value.frequency)) invalid();
    } else if (value.project !== match.partition || typeof value.title !== "string" || value.title.toLowerCase() !== match.normalizedKey
      || value.isLatest !== true || !Number.isSafeInteger(value.version) || Number(value.version) < 1) {
      invalid();
    }
    return row.value;
  }

  async upsert<T extends BatchEffectMetadata>(scope: "semantic" | "procedural", key: string, initial: CanonicalLookup,
    newId: string, effectKey: string, update: (current: T | null) => T): Promise<CanonicalWriteResult> {
    const candidate = update(null);
    const operationFingerprint = hash(["upsert", scope, key, effectKey, newId, candidate]);
    const replay = await this.probe(scope, "", "upsert", newId, effectKey, operationFingerprint, key);
    if (replay) return replay;
    return this.retry(scope, key, "", initial, async match => {
      const current = await this.current<T>(match);
      const value = update(current);
      const targetId = match.canonicalId ?? newId;
      const target = await this.kv.getVersioned(KV[scope], targetId);
      return this.apply(match, "upsert", targetId, target.version, effectKey, operationFingerprint, value);
    });
  }

  async memoryLookup(project: string, title: string): Promise<{ current: Memory | null; token: CanonicalLookup }> {
    for (let round = 0; round < 3; round++) {
      const token = await this.lookup("memories", title, project);
      try {
        const current = await this.current<Memory>(token);
        if (current && (current.project !== project || current.title.toLowerCase() !== title || current.isLatest !== true
          || current.id !== token.canonicalId || !Number.isSafeInteger(current.version) || current.version < 1)) invalid();
        return { current, token };
      } catch (error) { if (!conflict(error)) throw error; }
    }
    throw new CanonicalMemoryConflict();
  }

  async evolve(project: string, title: string, initial: CanonicalLookup, next: Memory, effectKey: string,
    expectedPredecessor?: { id: string; version: number; rowVersion: string } | null): Promise<CanonicalWriteResult> {
    const { parentId: _parent, supersedes: _supersedes, version: _version, ...captured } = next;
    const operationFingerprint = hash(["evolve", project, title, effectKey, captured]);
    const replay = await this.probe("memories", project, "evolve", next.id, effectKey, operationFingerprint, title);
    if (replay) return replay;
    return this.retry("memories", title, project, initial, async match => {
      const current = await this.current<Memory>(match);
      if (expectedPredecessor === null && current) throw new CanonicalMemoryConflict();
      if (expectedPredecessor && (!current || current.id !== expectedPredecessor.id || current.version !== expectedPredecessor.version
        || match.rowVersion !== expectedPredecessor.rowVersion)) {
        throw new CanonicalMemoryConflict();
      }
      const value: Memory = { ...captured, version: (current?.version ?? 0) + 1,
        ...(current ? { parentId: current.id, supersedes: [current.id, ...(current.supersedes ?? [])] } : {}) };
      const target = await this.kv.getVersioned(KV.memories, next.id);
      return this.apply(match, "evolve", next.id, target.version, effectKey, operationFingerprint, value);
    });
  }

  private async retry(scope: CanonicalScope, key: string, partition: string, initial: CanonicalLookup,
    operation: (match: CanonicalLookup) => Promise<CanonicalWriteResult>): Promise<CanonicalWriteResult> {
    identity(scope, key, partition);
    if (!initial || initial.scope !== scope || initial.partition !== partition || initial.normalizedKey !== key) throw new Error("canonical_token_mismatch");
    let match = initial;
    for (let round = 0; round < 3; round++) {
      try { return await operation(match); }
      catch (error) {
        if (!conflict(error)) throw error;
        if (round < 2) match = await this.lookup(scope, key, partition);
      }
    }
    throw new CanonicalMemoryConflict();
  }

  async sourcePage(scope: "semantic" | "procedural", sourceSessionIds: string[], cursor: string | null, project: string): Promise<CanonicalSourcePage> {
    sessions(sourceSessionIds);
    if (!text(project)) throw new Error("canonical_source_project_required");
    const raw = await retrievalRequest<unknown>(this.kv, { action: "canonical_source_page", scope: KV[scope],
      source_session_ids: sourceSessionIds, source_project: project, ...(cursor ? { cursor } : {}), limit: 12, max_bytes: 65_536 });
    if (!object(raw)) invalid();
    if (raw.status === "preparing" || raw.status === "partial") {
      if (!text(raw.continuation, 4096)) invalid();
      if (raw.status === "preparing") await this.prepare(scope);
      throw new CanonicalMemoryPending(raw.continuation, raw.status === "partial");
    }
    if (raw.status !== "ready" || !Array.isArray(raw.items) || raw.items.length > 12 || Buffer.byteLength(JSON.stringify(raw)) > 65_536
      || !isStateCounter(raw.membership_generation) || !isStateCounter(raw.coverage_epoch) || typeof raw.has_more !== "boolean"
      || !(raw.next_cursor === null || text(raw.next_cursor, 4096)) || raw.has_more !== (raw.next_cursor !== null)) invalid();
    const items = raw.items.map(item => {
      if (!object(item) || !text(item.id) || !isStateCounter(item.row_version)) invalid();
      return { id: item.id, rowVersion: item.row_version };
    });
    if (new Set(items.map(item => item.id)).size !== items.length || raw.next_cursor === cursor && raw.next_cursor !== null) invalid();
    return { items, nextCursor: raw.next_cursor, membershipGeneration: raw.membership_generation, coverageEpoch: raw.coverage_epoch };
  }

  async decay<T>(scope: "semantic" | "procedural", source: CanonicalSourceRef, sourceSessionIds: string[],
    project: string, effectKey: string, capturedOperation: unknown, update: (current: T) => T): Promise<CanonicalWriteResult> {
    sessions(sourceSessionIds);
    const operationFingerprint = hash(["decay", scope, source.id, effectKey, capturedOperation]);
    const replay = await this.probe(scope, "", "decay", source.id, effectKey, operationFingerprint);
    if (replay) return replay;
    let version = source.rowVersion;
    for (let round = 0; round < 3; round++) {
      const row = await this.kv.getVersioned<T & { fact?: string; name?: string; sourceSessionIds: string[] }>(KV[scope], source.id);
      if (!row.exists || !row.value) throw new CanonicalMemoryConflict();
      if (row.version !== version) { version = row.version; if (round === 2) break; continue; }
      if (!Array.isArray(row.value.sourceSessionIds) || !row.value.sourceSessionIds.some(id => sourceSessionIds.includes(id))) throw new CanonicalMemoryConflict();
      const key = scope === "semantic" ? row.value.fact : row.value.name;
      if (typeof key !== "string") invalid();
      const match = await this.lookup(scope, key.toLowerCase());
      try {
        return await this.apply({ ...match, canonicalId: source.id, rowVersion: version }, "decay", source.id, version,
          effectKey, operationFingerprint, update(row.value), sourceSessionIds, project);
      } catch (error) { if (!conflict(error)) throw error; }
    }
    throw new CanonicalMemoryConflict();
  }

  private async probe(scope: CanonicalScope, partition: string, kind: OperationKind, targetId: string,
    effectKey: string, operationFingerprint: string, normalizedKey?: string): Promise<CanonicalWriteResult | null> {
    if (!digest(effectKey) || !digest(operationFingerprint) || !text(targetId)) throw new Error("canonical_receipt_probe_invalid");
    const raw = await retrievalRequest<unknown>(this.kv, { action: "canonical_apply", receipt_only: true,
      scope: KV[scope], partition, operation_kind: kind, target_id: targetId, effect_key: effectKey,
      operation_fingerprint: operationFingerprint, ...(normalizedKey === undefined ? {} : { normalized_key: normalizedKey }) });
    if (object(raw) && raw.status === "not_applied" && Object.keys(raw).length === 1) return null;
    const result = this.result(raw, kind, effectKey, operationFingerprint);
    if (result.applied) invalid();
    return result;
  }

  private async apply(match: CanonicalLookup, kind: OperationKind, targetId: string, targetVersion: string,
    effectKey: string, operationFingerprint: string, value: unknown, sourceSessionIds?: string[], sourceProject?: string): Promise<CanonicalWriteResult> {
    if (!text(targetId) || !isStateCounter(targetVersion) || !digest(effectKey) || !digest(operationFingerprint) || !object(value)) throw new Error("canonical_apply_invalid");
    if (Buffer.byteLength(JSON.stringify(value)) > MAX_CANONICAL_RECORD_BYTES) throw new CanonicalMemoryPending(`record:${KV[match.scope]}:${targetId}`, true);
    const payload = { action: "canonical_apply", scope: KV[match.scope], partition: match.partition,
      normalized_key: match.normalizedKey, operation_kind: kind, canonical_id: match.canonicalId,
      expected_row_version: match.rowVersion ?? "0", expected_scope_revision: match.scopeRevision,
      coverage_epoch: match.coverageEpoch, normalization_version: match.normalizationVersion,
      target_id: targetId, expected_target_version: targetVersion, effect_key: effectKey, operation_fingerprint: operationFingerprint,
      value, ...(sourceSessionIds ? { source_session_ids: sourceSessionIds, source_project: sourceProject } : {}) };
    if (Buffer.byteLength(JSON.stringify(payload)) > MAX_CANONICAL_RECORD_BYTES + 4096) throw new CanonicalMemoryPending(`record:${KV[match.scope]}:${targetId}`, true);
    const raw = await retrievalRequest<unknown>(this.kv, payload);
    return this.result(raw, kind, effectKey, operationFingerprint);
  }

  private result(raw: unknown, kind: OperationKind, effectKey: string, operationFingerprint: string): CanonicalWriteResult {
    if (!object(raw) || !["applied", "already_applied"].includes(String(raw.status)) || typeof raw.applied !== "boolean"
      || raw.applied !== (raw.status === "applied") || typeof raw.created !== "boolean" || !text(raw.canonical_id)
      || !isStateCounter(raw.row_version) || !digest(raw.result_fingerprint) || !object(raw.receipt)) invalid();
    const receipt = raw.receipt;
    if (receipt.operation_kind !== kind || receipt.effect_key !== effectKey || receipt.operation_fingerprint !== operationFingerprint
      || receipt.canonical_id !== raw.canonical_id || receipt.row_version !== raw.row_version || receipt.created !== raw.created
      || receipt.result_fingerprint !== raw.result_fingerprint) invalid();
    return { applied: raw.applied, created: raw.created, id: raw.canonical_id, rowVersion: raw.row_version,
      receipt: { operationKind: kind, effectKey, operationFingerprint, id: raw.canonical_id, rowVersion: raw.row_version,
        created: raw.created, resultFingerprint: raw.result_fingerprint } };
  }
}
