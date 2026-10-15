import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import { batchEffectKey } from "../state/batch-effects.js";
import { CanonicalMemoryConflict, CanonicalMemoryPending, type CanonicalMemoryIndex,
  type CanonicalSourcePage, type CanonicalSourceRef, type CanonicalWriteResult } from "../state/canonical-memory.js";
import type { SemanticMemory, ProceduralMemory, SessionSummary } from "../types.js";
import { isStateCounter } from "../state/state-transactions.js";

type DecayScope = "semantic" | "procedural";
type DecayRecord = (SemanticMemory | ProceduralMemory) & { lastDecayAt?: string };
export interface ConsolidationDecaySelector {
  (scope: DecayScope, sourceSessionIds: string[], cursor: string | null, project: string): Promise<CanonicalSourcePage>;
  apply(scope: DecayScope, ref: CanonicalSourceRef, sessionIds: string[], project: string, effectKey: string,
    operation: unknown, update: (current: DecayRecord) => DecayRecord): Promise<CanonicalWriteResult>;
}
export function createConsolidationDecaySelector(index: CanonicalMemoryIndex): ConsolidationDecaySelector {
  return Object.assign(
    (scope: DecayScope, sessions: string[], cursor: string | null, project: string) => index.sourcePage(scope, sessions, cursor, project),
    { apply: (scope: DecayScope, ref: CanonicalSourceRef, sessions: string[], project: string, effect: string, operation: unknown,
      update: (current: DecayRecord) => DecayRecord) => index.decay(scope, ref, sessions, project, effect, operation, update) });
}
interface DecayManifest {
  kind: "consolidation-decay"; id: string; project: string; sessionIds: string[]; at: string;
  scope: DecayScope; cursor: string | null; pending?: CanonicalSourcePage & { done: string[] };
  processed: number; scopeProcessed: number; completed: boolean; decayDays: number;
  recordIds?: Partial<Record<DecayScope, string[]>>; matchedRecordIds?: Partial<Record<DecayScope, string[]>>;
}
function pendingError(error: unknown): boolean {
  return error instanceof CanonicalMemoryPending || error instanceof CanonicalMemoryConflict
    || (error instanceof Error && /STATE_TX_CONFLICT|STATE_CANONICAL_PREPARING/.test(error.message));
}
function validManifest(value: DecayManifest, id: string): boolean {
  if (value.pending && (!Array.isArray(value.pending.items) || value.pending.items.length > 12
    || !Array.isArray(value.pending.done) || new Set(value.pending.done).size !== value.pending.done.length
    || new Set(value.pending.items.map(ref => ref.id)).size !== value.pending.items.length
    || value.pending.items.some(ref => typeof ref.id !== "string" || !ref.id || Buffer.byteLength(ref.id) > 256 || !isStateCounter(ref.rowVersion))
    || !isStateCounter(value.pending.membershipGeneration) || !isStateCounter(value.pending.coverageEpoch)
    || value.pending.done.some(done => !value.pending!.items.some(ref => ref.id === done))
    || (value.pending.nextCursor !== null && (typeof value.pending.nextCursor !== "string" || !value.pending.nextCursor || Buffer.byteLength(value.pending.nextCursor) > 4096)))) return false;
  return value.kind === "consolidation-decay" && value.id === id && !!value.project && Array.isArray(value.sessionIds)
    && value.sessionIds.length > 0 && value.sessionIds.length <= 12 && new Set(value.sessionIds).size === value.sessionIds.length
    && value.sessionIds.every(session => typeof session === "string" && session.length > 0 && Buffer.byteLength(session) <= 256)
    && (value.recordIds === undefined || Object.keys(value.recordIds).every(scope => ["semantic", "procedural"].includes(scope))
      && (["semantic", "procedural"] as const).every(scope => value.recordIds?.[scope] === undefined
        || Array.isArray(value.recordIds[scope]) && value.recordIds[scope]!.length <= 12
          && new Set(value.recordIds[scope]).size === value.recordIds[scope]!.length
          && value.recordIds[scope]!.every(id => typeof id === "string" && id.length > 0 && Buffer.byteLength(id) <= 256)))
    && (value.matchedRecordIds === undefined || Object.keys(value.matchedRecordIds).every(scope => ["semantic", "procedural"].includes(scope))
      && (["semantic", "procedural"] as const).every(scope => value.matchedRecordIds?.[scope] === undefined
        || Array.isArray(value.matchedRecordIds[scope]) && new Set(value.matchedRecordIds[scope]).size === value.matchedRecordIds[scope]!.length
          && value.matchedRecordIds[scope]!.every(id => typeof id === "string" && id.length > 0 && Buffer.byteLength(id) <= 256)))
    && ["semantic", "procedural"].includes(value.scope) && (value.cursor === null
      || typeof value.cursor === "string" && value.cursor.length > 0 && Buffer.byteLength(value.cursor) <= 4096) && Number.isFinite(Date.parse(value.at))
    && Number.isFinite(value.decayDays) && value.decayDays >= 0 && Number.isSafeInteger(value.processed) && value.processed >= 0
    && Number.isSafeInteger(value.scopeProcessed) && value.scopeProcessed >= 0 && typeof value.completed === "boolean";
}
export async function runSelectedConsolidationDecay(kv: StateKV, selector: ConsolidationDecaySelector | undefined, decayDays: number,
  input: { project: string; sessionIds: string[]; sourceVersion: string; effectTimestamp?: string; recordIds?: Partial<Record<DecayScope, string[]>> } | { manifestId: string }): Promise<Record<string, unknown>> {
  if (!selector || typeof selector.apply !== "function") throw new Error("consolidation_decay_atomic_index_unavailable");
  const requestedAt = "manifestId" in input || input.effectTimestamp === undefined
    ? new Date() : new Date(input.effectTimestamp);
  if (!("manifestId" in input) && !Number.isFinite(requestedAt.getTime())) throw new Error("consolidation_decay_effect_timestamp_invalid");
  const epoch = decayDays > 0 && Number.isFinite(decayDays) ? Math.floor(requestedAt.getTime() / (decayDays * 86_400_000)) : null;
  const id = "manifestId" in input ? input.manifestId
    : fingerprintId("condecay", JSON.stringify([input.project, input.sessionIds, input.sourceVersion, decayDays, epoch, input.recordIds ?? null]));
  let manifest = await kv.get<DecayManifest>(KV.state, id);
  if (!manifest) {
    if ("manifestId" in input) throw new Error("consolidation_decay_manifest_missing");
    manifest = { kind: "consolidation-decay", id, project: input.project, sessionIds: input.sessionIds,
      at: requestedAt.toISOString(), scope: "semantic", cursor: null, processed: 0, scopeProcessed: 0,
      completed: decayDays === 0, decayDays, ...(input.recordIds ? { recordIds: structuredClone(input.recordIds), matchedRecordIds: { semantic: [], procedural: [] } } : {}) };
    if (!validManifest(manifest, id)) throw new Error("consolidation_decay_sources_invalid");
    await kv.set(KV.state, id, manifest);
  }
  if (!validManifest(manifest, id)) throw new Error("consolidation_decay_manifest_invalid");
  if (manifest.completed) return { completed: true, manifestId: id, processed: manifest.processed,
    ...(manifest.decayDays === 0 ? { reason: "decay_disabled" } : {}) };
  const saved = manifest;
  const result = () => ({ completed: saved.completed, queued: !saved.completed, manifestId: id,
    processed: saved.processed + saved.scopeProcessed, selectionCoverage: saved.recordIds ? "selected_canonical_records" : "selected_source_sessions",
    ...(!saved.completed ? { continuation: { decayManifestId: id } } : {}) });
  try {
    for (const sessionId of saved.sessionIds) {
      if ((await kv.get<SessionSummary>(KV.summaries, sessionId))?.project !== saved.project) throw new CanonicalMemoryConflict();
    }
    if (saved.recordIds && saved.recordIds[saved.scope]?.length === 0) {
      saved.processed += saved.scopeProcessed; saved.scopeProcessed = 0; saved.cursor = null;
      if (saved.scope === "semantic") saved.scope = "procedural"; else saved.completed = true;
      await kv.set(KV.state, id, saved);
      return result();
    }
    if (!saved.pending) {
      const page = await selector(saved.scope, saved.sessionIds, saved.cursor, saved.project);
      if (!Array.isArray(page.items) || page.items.length > 12 || new Set(page.items.map(item => item.id)).size !== page.items.length
        || page.items.some(item => !item.id || Buffer.byteLength(item.id) > 256 || !isStateCounter(item.rowVersion))
        || !isStateCounter(page.membershipGeneration) || !isStateCounter(page.coverageEpoch)
        || (page.nextCursor !== null && (!page.nextCursor || page.nextCursor === saved.cursor || Buffer.byteLength(page.nextCursor) > 4096))) {
        throw new Error("consolidation_decay_page_invalid");
      }
      const wanted = saved.recordIds?.[saved.scope];
      const items = wanted ? page.items.filter(ref => wanted.includes(ref.id)) : page.items;
      saved.pending = { ...page, items, done: [] };
      await kv.set(KV.state, id, saved);
    }
    for (const ref of saved.pending.items) {
      if (saved.pending.done.includes(ref.id)) continue;
      const effect = batchEffectKey(`${id}:${saved.scope}:${ref.id}`);
      await selector.apply(saved.scope, ref, saved.sessionIds, saved.project, effect,
        { project: saved.project, sessionIds: saved.sessionIds, at: saved.at, decayDays: saved.decayDays }, current => {
          if (!current || !current.sourceSessionIds.some(sessionId => saved.sessionIds.includes(sessionId))) throw new CanonicalMemoryConflict();
          const anchor = Math.max(Date.parse(current.lastDecayAt ?? current.updatedAt), Date.parse(current.updatedAt),
            Date.parse("lastAccessedAt" in current ? current.lastAccessedAt : current.updatedAt));
          if (!Number.isFinite(anchor) || !Number.isFinite(current.strength)) throw new Error("consolidation_decay_source_invalid");
          const periods = saved.decayDays > 0 ? Math.floor((Date.parse(saved.at) - anchor) / (saved.decayDays * 86_400_000)) : 0;
          return periods > 0 ? { ...current, strength: Math.max(0.1, current.strength * Math.pow(0.9, periods)), lastDecayAt: saved.at } : current;
        });
      saved.pending.done.push(ref.id);
      if (saved.matchedRecordIds) (saved.matchedRecordIds[saved.scope] ??= []).push(ref.id);
      await kv.set(KV.state, id, saved);
    }
    saved.scopeProcessed += saved.pending.items.length;
    saved.cursor = saved.pending.nextCursor;
    delete saved.pending;
    if (!saved.cursor) {
      const expected = saved.recordIds?.[saved.scope];
      if (expected && expected.some(recordId => !saved.matchedRecordIds?.[saved.scope]?.includes(recordId))) {
        throw new Error("consolidation_decay_record_scope_mismatch");
      }
      saved.processed += saved.scopeProcessed;
      saved.scopeProcessed = 0;
      if (saved.scope === "semantic") saved.scope = "procedural";
      else saved.completed = true;
    }
    await kv.set(KV.state, id, saved);
    return result();
  } catch (error) {
    if (!pendingError(error)) throw error;
    // Membership changes restart selection; independent receipts retain every applied effect.
    if (!(error instanceof CanonicalMemoryPending)) { delete saved.pending; saved.cursor = null; saved.scopeProcessed = 0; }
    await kv.set(KV.state, id, saved);
    return { ...result(), reason: error instanceof CanonicalMemoryPending ? "canonical_index_preparing" : "canonical_state_conflict" };
  }
}
