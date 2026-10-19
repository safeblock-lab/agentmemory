import { randomUUID } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import type { StorageGrowthSample } from "../types.js";
import type { GraphWorkTurn } from "../state/graph-work-queue.js";
import { measuredFootprint, type ComparableFootprint } from "./periodic-maintenance-state.js";

type Family = "root_layout" | "text_layout" | "search_packed";
type Guard = { owner_id: string; generation: string; fence: string };
type Request = { operation_id: string; job_id: string; expected_ordinal: number; step: string;
  max_ms: number; max_bytes: number; limit: number; owner_id: string; fence: number; guard: Guard; admission_id?: string };
type Status = { id: string; phase: string; ordinal: number; gc_complete: boolean; admission?: string;
  cursor?: number; gc_cursor?: number; gc_stage?: number };
type Pending = { kind: "apply"; family: Family; request: Request; before: Status | null }
  | { kind: "graph_lease"; action: "acquire" | "release"; payload: Record<string, unknown> };
type NotApplied = { family: Family; request: Request; receiptStatus: "not_found"; observedStatus: Status; verifiedAt: string;
  currentLease: { ownerId: string; fence: number } };
export interface LayoutAdmission { acceptedByMain: boolean; format: 4; allFamiliesAccepted: boolean;
  admissionId: string; measuredCombinedNetBytes: number; rollbackAccounted: boolean }
export interface LayoutCycle { baseline: ComparableFootprint | null; active: boolean; familyIndex: number;
  jobs: Partial<Record<Family, string>>; pending: Pending | null; pendingRaw: unknown | null;
  notApplied: NotApplied[]; recoveryRequired: boolean; disposition: string;
  contextRecoveryRequired?: boolean;
  historyUnknown?: { observedCount: number | null; evidenceReference: string | null; reason: string } | null;
  continuationProof?: { graphGeneration: string; admissionId: string; evidenceReference: string;
    jobs: Partial<Record<Family, string>> } | null;
  lastLeaseResolution?: { outcome: "released" | "guard_fenced"; payload: Record<string, unknown>; observedFence: string } }
const FAMILIES: Family[] = ["root_layout", "text_layout", "search_packed"];
const MAX_NOT_APPLIED = 30;
const graphTurns = new Map<string, { fence: number; turn: GraphWorkTurn; consumed: boolean }>();
export function bindLayoutGraphTurn(lease: { ownerId: string; fence: number }, turn: GraphWorkTurn): () => void {
  if (!turn.granted || graphTurns.has(lease.ownerId)) throw Error("STATE_LAYOUT_GRAPH_TURN_INVALID");
  const binding = { fence: lease.fence, turn, consumed: false };
  graphTurns.set(lease.ownerId, binding);
  return () => {
    if (graphTurns.get(lease.ownerId) === binding) graphTurns.delete(lease.ownerId);
    turn.release();
  };
}
export function releaseUnusedLayoutGraphTurn(lease: { ownerId: string; fence: number }): void {
  const binding = graphTurns.get(lease.ownerId);
  if (binding?.fence === lease.fence && !binding.consumed) binding.turn.release();
}
export const LAYOUT_LEASE_CONTEXT_RECOVERY_VERSION = 1;
const record = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
export const emptyLayoutCycle = (): LayoutCycle => ({ baseline: null, active: false, familyIndex: 0,
  jobs: {}, pending: null, pendingRaw: null, notApplied: [], recoveryRequired: false, disposition: "admission_pending" });
function savedStatus(value: unknown): Status | null {
  if (!record(value) || typeof value.id !== "string" || typeof value.phase !== "string"
    || !Number.isSafeInteger(value.ordinal) || Number(value.ordinal) < 0
    || (value.gc_complete !== undefined && typeof value.gc_complete !== "boolean")) return null;
  return { id: value.id, phase: value.phase, ordinal: Number(value.ordinal), gc_complete: value.gc_complete === true,
    ...statusCursors(value) };
}
function statusCursors(value: Record<string, unknown>): Pick<Status, "cursor" | "gc_cursor" | "gc_stage"> {
  return {
    ...(Number.isSafeInteger(value.cursor) && Number(value.cursor) >= 0 ? { cursor: Number(value.cursor) } : {}),
    ...(Number.isSafeInteger(value.gc_cursor) && Number(value.gc_cursor) >= 0 ? { gc_cursor: Number(value.gc_cursor) } : {}),
    ...(Number.isSafeInteger(value.gc_stage) && Number(value.gc_stage) >= 0 ? { gc_stage: Number(value.gc_stage) } : {}),
  };
}
function layoutAdvanced(before: Status, after: Status): boolean {
  return before.id !== after.id || before.phase !== after.phase || before.gc_complete !== after.gc_complete
    || (["cursor", "gc_cursor", "gc_stage"] as const).some(key => before[key] !== undefined
      && after[key] !== undefined && before[key] !== after[key]);
}
function parsePending(value: unknown): Pending | null {
  if (!record(value)) return null;
  if (value.kind === "graph_lease") {
    const payload = value.payload;
    if (!record(payload) || (value.action !== "acquire" && value.action !== "release") || payload.action !== value.action
      || Object.keys(value).some(key => !["kind", "action", "payload"].includes(key))
      || typeof payload.owner_id !== "string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(payload.owner_id)
      || typeof payload.generation !== "string" || !/^[1-9][0-9]{0,19}$/.test(payload.generation)
      || BigInt(payload.generation) > 18_446_744_073_709_551_615n) return null;
    const allowed = value.action === "acquire" ? ["action", "owner_id", "generation", "ttl_ms"] : ["action", "owner_id", "generation", "fence"];
    if (Object.keys(payload).some(key => !allowed.includes(key))) return null;
    if (value.action === "acquire") {
      if (!Number.isSafeInteger(payload.ttl_ms) || Number(payload.ttl_ms) < 1 || Number(payload.ttl_ms) > 300000) return null;
    } else if (typeof payload.fence !== "string" || !/^[1-9][0-9]{0,19}$/.test(payload.fence)
      || BigInt(payload.fence) > 18_446_744_073_709_551_615n) return null;
    return { kind: "graph_lease", action: value.action, payload: { ...payload } };
  }
  const family = FAMILIES.find(item => item === value.family), r = value.request;
  if (value.kind !== "apply" || !family || !record(r) || !record(r.guard)) return null;
  const before = value.before == null ? null : savedStatus(value.before);
  if (value.before != null && before === null) return null;
  const allowed = ["operation_id", "job_id", "expected_ordinal", "step", "max_ms", "max_bytes", "limit", "owner_id", "fence", "guard", "admission_id"];
  if (Object.keys(r).some(key => !allowed.includes(key)) || Object.keys(r.guard).some(key => !["owner_id", "generation", "fence"].includes(key))) return null;
  if (typeof r.operation_id !== "string" || !r.operation_id || r.operation_id.length > 128
    || typeof r.job_id !== "string" || !r.job_id || r.job_id.length > 96 || typeof r.step !== "string"
    || !["begin", "copy", "verify", "cut", "retire", "collect"].includes(r.step)
    || !Number.isSafeInteger(r.expected_ordinal) || Number(r.expected_ordinal) < 0
    || !Number.isSafeInteger(r.limit) || Number(r.limit) < 1 || Number(r.limit) > 100000
    || !Number.isSafeInteger(r.max_bytes) || Number(r.max_bytes) < 16384 || Number(r.max_bytes) > 4194304
    || !Number.isSafeInteger(r.max_ms) || Number(r.max_ms) < 1 || Number(r.max_ms) > 2500
    || typeof r.owner_id !== "string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(r.owner_id)
    || !Number.isSafeInteger(r.fence) || Number(r.fence) < 1 || r.guard.owner_id !== r.owner_id
    || typeof r.guard.generation !== "string" || !/^[1-9][0-9]{0,19}$/.test(r.guard.generation)
    || typeof r.guard.fence !== "string" || !/^[1-9][0-9]{0,19}$/.test(r.guard.fence)
    || (family !== "search_packed" && (typeof r.admission_id !== "string" || !r.admission_id || r.admission_id.length > 128))) return null;
  return { kind: "apply", family, request: { operation_id: r.operation_id, job_id: r.job_id,
    expected_ordinal: Number(r.expected_ordinal), step: r.step, max_ms: Number(r.max_ms), max_bytes: Number(r.max_bytes),
    limit: Number(r.limit), owner_id: r.owner_id, fence: Number(r.fence),
    guard: { owner_id: r.owner_id, generation: r.guard.generation, fence: r.guard.fence },
    ...(typeof r.admission_id === "string" ? { admission_id: r.admission_id } : {}) }, before };
}
function parseNotApplied(value: unknown): NotApplied | null {
  if (!record(value) || !FAMILIES.includes(value.family as Family) || value.receiptStatus !== "not_found"
    || typeof value.verifiedAt !== "string" || !Number.isFinite(Date.parse(value.verifiedAt)) || !record(value.currentLease)
    || typeof value.currentLease.ownerId !== "string" || !Number.isSafeInteger(value.currentLease.fence)
    || Number(value.currentLease.fence) < 1) return null;
  const pending = parsePending({ kind: "apply", family: value.family, request: value.request });
  const observedStatus = savedStatus(value.observedStatus);
  if (!pending || pending.kind !== "apply" || !observedStatus) return null;
  return { family: pending.family, request: pending.request, receiptStatus: "not_found", observedStatus, verifiedAt: value.verifiedAt,
    currentLease: { ownerId: value.currentLease.ownerId, fence: Number(value.currentLease.fence) } };
}
export function normalizeLayoutCycle(value: unknown): LayoutCycle {
  if (!record(value)) return emptyLayoutCycle();
  const fresh = emptyLayoutCycle();
  const original = value;
  if (record(value.pendingRaw) && record(value.pendingRaw.jobs)) value = value.pendingRaw;
  const cycle: LayoutCycle = { ...fresh };
  const preservedRaw = record(original.pendingRaw) && record(original.pendingRaw.jobs) ? original.pendingRaw : original;
  const blocked = (): LayoutCycle => ({ ...cycle, pendingRaw: preservedRaw, recoveryRequired: true,
    historyUnknown: cycle.historyUnknown ?? { observedCount: null, evidenceReference: null, reason: "unparsed_cycle_context" },
    disposition: "manual_reconciliation_required" });
  if (typeof value.active !== "boolean" || !Number.isInteger(value.familyIndex)
    || Number(value.familyIndex) < 0 || Number(value.familyIndex) > 3 || !record(value.jobs)) return blocked();
  const jobs: Partial<Record<Family, string>> = {};
  for (const family of FAMILIES) { const id = value.jobs[family]; if (id !== undefined) {
    if (typeof id !== "string" || !id || id.length > 96) return blocked(); jobs[family] = id;
  } }
  cycle.active = value.active; cycle.familyIndex = Number(value.familyIndex); cycle.jobs = jobs;
  if (value.notApplied !== undefined && (!Array.isArray(value.notApplied) || value.notApplied.length > MAX_NOT_APPLIED)) return blocked();
  const notApplied: NotApplied[] = [];
  for (const item of Array.isArray(value.notApplied) ? value.notApplied : []) {
    const parsed = parseNotApplied(item); if (!parsed) return blocked(); notApplied.push(parsed);
  }
  cycle.notApplied = notApplied;
  const raw = value.baseline;
  let baseline: ComparableFootprint | null = null;
  if (raw != null) {
    if (!record(raw) || typeof raw.sampledAt !== "string" || !Number.isFinite(Date.parse(raw.sampledAt))
      || !Number.isSafeInteger(raw.categoryVersion) || typeof raw.coverage !== "string"
      || !Number.isSafeInteger(raw.databaseBytes) || !Number.isSafeInteger(raw.walBytes)
      || !Number.isSafeInteger(raw.observedBytes) || Number(raw.observedBytes) !== Number(raw.databaseBytes) + Number(raw.walBytes)) return blocked();
    baseline = { sampledAt: raw.sampledAt, categoryVersion: Number(raw.categoryVersion), coverage: raw.coverage,
      databaseBytes: Number(raw.databaseBytes), walBytes: Number(raw.walBytes), observedBytes: Number(raw.observedBytes) };
  }
  cycle.baseline = baseline;
  const proof = value.continuationProof;
  if (proof != null) {
    if (!record(proof) || typeof proof.graphGeneration !== "string" || !/^[1-9][0-9]{0,19}$/.test(proof.graphGeneration)
      || typeof proof.admissionId !== "string" || !proof.admissionId || proof.admissionId.length > 128
      || typeof proof.evidenceReference !== "string" || proof.evidenceReference.length > 4096 || !record(proof.jobs)) return blocked();
    const proofJobs: Partial<Record<Family, string>> = {};
    for (const family of FAMILIES) if (proof.jobs[family] !== undefined) {
      const id = proof.jobs[family];
      if (typeof id !== "string" || !id || id.length > 96 || id !== jobs[family]) return blocked();
      proofJobs[family] = id;
    }
    if (!proofJobs.root_layout || !proofJobs.text_layout) return blocked();
    cycle.continuationProof = { graphGeneration: proof.graphGeneration, admissionId: proof.admissionId,
      evidenceReference: proof.evidenceReference, jobs: proofJobs };
  }
  const unknown = value.historyUnknown;
  if (unknown != null) {
    if (!record(unknown) || (unknown.observedCount !== null && (!Number.isSafeInteger(unknown.observedCount) || Number(unknown.observedCount) < 0))
      || (unknown.evidenceReference !== null && (typeof unknown.evidenceReference !== "string" || unknown.evidenceReference.length > 4096))
      || typeof unknown.reason !== "string" || unknown.reason.length > 512) return blocked();
    cycle.historyUnknown = { observedCount: unknown.observedCount === null ? null : Number(unknown.observedCount),
      evidenceReference: unknown.evidenceReference as string | null, reason: unknown.reason };
  }
  const resolution = value.lastLeaseResolution;
  if (resolution != null) {
    if (!record(resolution) || !["released", "guard_fenced"].includes(String(resolution.outcome))
      || typeof resolution.observedFence !== "string" || !/^[1-9][0-9]{0,19}$/.test(resolution.observedFence)) return blocked();
    const resolvedPending = parsePending({ kind: "graph_lease", action: "release", payload: resolution.payload });
    if (!resolvedPending || resolvedPending.kind !== "graph_lease") return blocked();
    cycle.lastLeaseResolution = { outcome: resolution.outcome as "released" | "guard_fenced",
      payload: resolvedPending.payload, observedFence: resolution.observedFence };
  }
  if (value.pendingRaw != null && !(record(value.pendingRaw) && value.pendingRaw.kind)) return blocked();
  const rawPending = value.pending ?? (record(value.pendingRaw) && value.pendingRaw.kind ? value.pendingRaw : null);
  const pending = rawPending == null ? null : parsePending(rawPending);
  if (rawPending != null && pending === null) return blocked();
  const lostContext = value.contextRecoveryRequired === true || (original.recoveryRequired === true
    && pending?.kind === "graph_lease" && !cycle.active && cycle.familyIndex === 0 && Object.keys(jobs).length === 0 && baseline === null);
  cycle.contextRecoveryRequired = lostContext;
  if (lostContext) cycle.historyUnknown ??= { observedCount: null, evidenceReference: null, reason: "normalization_discarded_cycle_context" };
  cycle.pending = pending;
  cycle.pendingRaw = null;
  cycle.recoveryRequired = lostContext || (original.recoveryRequired === true && pending === null);
  cycle.disposition = lostContext ? "layout_context_missing" : typeof value.disposition === "string" ? value.disposition : "resumed";
  return cycle;
}
function status(value: unknown): Status {
  if (!record(value) || typeof value.id !== "string" || typeof value.phase !== "string"
    || !Number.isSafeInteger(value.ordinal) || Number(value.ordinal) < 0) throw Error("STATE_LAYOUT_STATUS_INVALID");
  return { id: value.id, phase: value.phase, ordinal: Number(value.ordinal), gc_complete: value.gc_complete === true,
    ...(typeof value.admission === "string" ? { admission: value.admission } : {}), ...statusCursors(value) };
}
function sameStatus(left: Status, right: Status): boolean {
  return left.id === right.id && left.phase === right.phase && left.ordinal === right.ordinal
    && left.gc_complete === right.gc_complete;
}
function unchangedLegacyRequestStatus(current: Status, pending: Extract<Pending, { kind: "apply" }>): boolean {
  const request = pending.request;
  if (current.ordinal !== request.expected_ordinal || (request.step !== "begin" && current.id !== request.job_id)) return false;
  if (request.step === "begin") return current.phase === "retired" && current.gc_complete;
  if (request.step === "copy") return current.phase === "copy";
  if (request.step === "verify") return current.phase.startsWith("verify");
  if (request.step === "cut") return current.phase === "ready" || current.phase === "cutting";
  if (request.step === "retire") return current.phase === "cut" || current.phase === "retiring";
  if (request.step === "collect") return current.phase === "retired" || current.phase === "collecting";
  return false;
}
export function isLayoutAdmissionAccepted(value: LayoutAdmission | undefined): value is LayoutAdmission {
  return value?.acceptedByMain === true && value.format === 4 && value.allFamiliesAccepted === true
    && typeof value.admissionId === "string" && value.admissionId.length > 0 && value.admissionId.length <= 128
    && Number.isSafeInteger(value.measuredCombinedNetBytes) && value.measuredCombinedNetBytes >= 285_552_525
    && value.rollbackAccounted === true;
}
export async function runPeriodicLayouts(input: { sdk: IIIClient; cycle: LayoutCycle; admission?: LayoutAdmission;
  sample: StorageGrowthSample; lease: { ownerId: string; fence: number }; refresh: () => Promise<void>;
  persist: () => Promise<void>; isCancelled?: () => boolean }): Promise<{ preserveLease: boolean; disposition: string;
    turnComplete?: boolean; progressMade?: boolean }> {
  const { sdk, cycle } = input;
  const invoke = (function_id: string, payload: unknown): Promise<unknown> => sdk.trigger({ function_id, payload });
  const storage = (payload: unknown) => invoke("state::storage", payload);
  const graphTurn = graphTurns.get(input.lease.ownerId);
  let progressMade = false;
  const finish = async (disposition: string, preserveLease = false) => { cycle.disposition = disposition; await input.persist();
    return { preserveLease, disposition, turnComplete: graphTurn?.consumed === true, progressMade }; };
  if (!graphTurn || graphTurn.fence !== input.lease.fence || !graphTurn.turn.granted || graphTurn.consumed)
    return finish("graph_turn_pending");
  graphTurn.consumed = true;
  try {
  if (cycle.pending?.kind === "graph_lease") {
    const pending = cycle.pending;
    const control = await invoke("state::get_versioned", { scope: "mem:graph:control", key: "current" });
    if (!record(control) || control.exists !== true || !record(control.value) || control.value.recovery != null
      || control.value.generation !== pending.payload.generation || typeof control.value.fence !== "string"
      || !/^[1-9][0-9]{0,19}$/.test(control.value.fence)) return finish("graph_lease_recovery_pending", true);
    if (pending.action !== "release") {
      const nativeLease = control.value.lease;
      if (!record(nativeLease) || nativeLease.owner_id !== pending.payload.owner_id
        || nativeLease.generation !== pending.payload.generation || nativeLease.fence !== control.value.fence)
        return finish("graph_acquire_acknowledgement_unknown", true);
      cycle.pending = { kind: "graph_lease", action: "release", payload: { action: "release",
        owner_id: nativeLease.owner_id, generation: nativeLease.generation, fence: nativeLease.fence } };
      await input.persist();
      return finish("graph_release_reconciliation_pending", true);
    }
    const oldFence = BigInt(pending.payload.fence as string), observedFence = BigInt(control.value.fence);
    if (observedFence < oldFence) return finish("graph_lease_recovery_pending", true);
    if (observedFence > oldFence) {
      cycle.lastLeaseResolution = { outcome: "guard_fenced", payload: { ...pending.payload }, observedFence: control.value.fence };
      cycle.pending = null; cycle.pendingRaw = null; await input.persist();
    } else {
      let released: unknown;
      try { released = await invoke("state::lease", pending.payload); }
      catch (error) {
        const code = record(error) && typeof error.code === "string" ? error.code : error instanceof Error ? error.message.match(/\bSTATE_TX_[A-Z_]+\b/)?.[0] : null;
        if (code !== "STATE_TX_FENCED") throw error;
        await input.refresh();
        let acquired: unknown;
        try { acquired = await invoke("state::lease", { action: "acquire", owner_id: input.lease.ownerId,
          generation: pending.payload.generation, ttl_ms: 300000 }); }
        catch (acquireError) {
          const acquireCode = record(acquireError) && typeof acquireError.code === "string" ? acquireError.code : acquireError instanceof Error ? acquireError.message.match(/\bSTATE_TX_[A-Z_]+\b/)?.[0] : null;
          if (acquireCode === "STATE_TX_LEASE_BUSY" || acquireCode === "STATE_TX_GENERATION_STALE" || acquireCode === "STATE_TX_FENCED") return finish("graph_lease_recovery_pending", true);
          throw acquireError;
        }
        if (!record(acquired) || acquired.owner_id !== input.lease.ownerId || acquired.generation !== pending.payload.generation
          || typeof acquired.fence !== "string" || !/^[1-9][0-9]{0,19}$/.test(acquired.fence)
          || BigInt(acquired.fence) <= oldFence) return finish("graph_lease_recovery_pending", true);
        cycle.lastLeaseResolution = { outcome: "guard_fenced", payload: { ...pending.payload }, observedFence: acquired.fence };
        const release = { action: "release", owner_id: input.lease.ownerId, generation: acquired.generation, fence: acquired.fence };
        cycle.pending = { kind: "graph_lease", action: "release", payload: release }; await input.persist();
        const freshReleased = await invoke("state::lease", release);
        if (!record(freshReleased) || freshReleased.released !== true) return finish("lease_acknowledgement_unknown", true);
        cycle.pending = null; cycle.pendingRaw = null; await input.persist();
      }
      if (released !== undefined) {
        if (!record(released) || released.released !== true) return finish("lease_acknowledgement_unknown", true);
        cycle.lastLeaseResolution = { outcome: "released", payload: { ...pending.payload }, observedFence: control.value.fence };
        cycle.pending = null; cycle.pendingRaw = null; await input.persist();
      }
    }
  }
  if (cycle.contextRecoveryRequired) return finish("layout_context_missing", true);
  if (cycle.recoveryRequired) return finish("manual_reconciliation_required", true);
  if (cycle.pending) {
    if (cycle.pending.kind !== "apply") return finish("lease_acknowledgement_unknown", true);
    const pending = cycle.pending;
    await input.refresh();
    const receipt = await storage({ action: `${pending.family}_receipt`, request: pending.request });
    if (record(receipt) && receipt.status === "applied") {
      if (!record(receipt.result)) throw Error("STATE_LAYOUT_RECEIPT_INVALID");
      status(receipt.result.job); cycle.pending = null; await input.persist();
    } else {
      if (!record(receipt) || receipt.status !== "not_found" || cycle.notApplied.length >= MAX_NOT_APPLIED)
        return finish("exact_receipt_pending", true);
      const current = status(await storage({ action: `${pending.family}_status` }));
      await input.refresh();
      const unchanged = pending.before ? sameStatus(current, pending.before) : unchangedLegacyRequestStatus(current, pending);
      const oldLeaseFenced = pending.request.owner_id !== input.lease.ownerId
        && input.lease.fence > pending.request.fence;
      if (!unchanged || !oldLeaseFenced) return finish("exact_receipt_pending", true);
      cycle.notApplied.push({ family: pending.family, request: pending.request, receiptStatus: "not_found",
        observedStatus: current, verifiedAt: new Date().toISOString(),
        currentLease: { ownerId: input.lease.ownerId, fence: input.lease.fence } });
      cycle.pending = null; cycle.disposition = "previous_request_not_applied"; await input.persist();
    }
  }
  if (input.isCancelled?.()) return finish("window_yielded");
  if (!isLayoutAdmissionAccepted(input.admission)) return finish("admission_pending");
  const current = measuredFootprint(input.sample);
  if (!current) return finish("sample_incomparable");
  if (!cycle.baseline) { cycle.baseline = current; return finish("baseline_established"); }
  if (!cycle.active) {
    if (current.categoryVersion !== cycle.baseline.categoryVersion || current.coverage !== cycle.baseline.coverage
      || Date.parse(current.sampledAt) <= Date.parse(cycle.baseline.sampledAt)) return finish("sample_incomparable");
    if (current.observedBytes - cycle.baseline.observedBytes < 1_000_000_000) return finish("growth_below_threshold");
    cycle.active = true; cycle.familyIndex = 0; cycle.jobs = {}; await input.persist();
  }
  await input.refresh();
  const control = await invoke("state::get_versioned", { scope: "mem:graph:control", key: "current" });
  if (!record(control) || control.exists !== true || !record(control.value) || control.value.recovery != null
    || typeof control.value.generation !== "string") return finish("graph_recovery_pending");
  const continuation = cycle.continuationProof;
  if (continuation && (control.value.generation !== continuation.graphGeneration
    || input.admission.admissionId !== continuation.admissionId
    || FAMILIES.some(family => continuation.jobs[family] !== undefined
      && cycle.jobs[family] !== continuation.jobs[family]))) return finish("continuation_proof_conflict", true);
  const acquire = { action: "acquire", owner_id: input.lease.ownerId, generation: control.value.generation, ttl_ms: 300000 };
  cycle.pending = { kind: "graph_lease", action: "acquire", payload: acquire }; await input.persist();
  let acquired: unknown;
  try { acquired = await invoke("state::lease", acquire); }
  catch (error) {
    const code = record(error) && typeof error.code === "string" ? error.code : error instanceof Error ? error.message.match(/\bSTATE_TX_LEASE_BUSY\b/)?.[0] : null;
    if (code !== "STATE_TX_LEASE_BUSY") throw error;
    cycle.pending = null; return finish("graph_lease_busy");
  }
  if (!record(acquired) || acquired.owner_id !== input.lease.ownerId || typeof acquired.generation !== "string"
    || typeof acquired.fence !== "string") throw Error("STATE_LAYOUT_GRAPH_LEASE_INVALID");
  const guard: Guard = { owner_id: input.lease.ownerId, generation: acquired.generation, fence: acquired.fence };
  cycle.pending = null; await input.persist();
  try {
    const started = Date.now();
    for (let batch = 0; batch < 16 && Date.now() - started < 5000 && !input.isCancelled?.()
      && cycle.familyIndex < FAMILIES.length; batch++) {
      const family = FAMILIES[cycle.familyIndex]!;
      const now = status(await storage({ action: `${family}_status` }));
      let step: string;
      let job = cycle.jobs[family];
      if (!job) {
        if (cycle.continuationProof && family !== "search_packed") return finish("continuation_context_missing", true);
        if (now.phase !== "retired" || !now.gc_complete) return finish("native_initial_acceptance_pending");
        job = family === "search_packed" ? now.id : randomUUID(); cycle.jobs[family] = job;
        step = family === "search_packed" ? "collect" : "begin";
      } else {
        if (now.id !== job) return finish("native_job_conflict", true);
        if (family !== "search_packed" && now.admission !== input.admission.admissionId)
          return finish("native_admission_conflict", true);
        if (now.phase === "retired" && now.gc_complete) { cycle.familyIndex++; await input.persist(); progressMade = true; continue; }
        if (now.phase === "copy") step = "copy";
        else if (now.phase.startsWith("verify")) step = "verify";
        else if (now.phase === "ready" || now.phase === "cutting") step = "cut";
        else if (now.phase === "cut" || now.phase === "retiring") step = "retire";
        else if (now.phase === "retired" || now.phase === "collecting") step = "collect";
        else return finish("native_phase_requires_main", true);
      }
      const request: Request = { operation_id: randomUUID(), job_id: job, expected_ordinal: now.ordinal, step,
        limit: 10000, max_bytes: 4194304, max_ms: 2500, owner_id: input.lease.ownerId, fence: input.lease.fence, guard,
        ...(family === "search_packed" ? {} : { admission_id: input.admission.admissionId }) };
      await input.refresh();
      if (input.isCancelled?.()) break;
      cycle.pending = { kind: "apply", family, request, before: now }; await input.persist();
      if (input.isCancelled?.()) { cycle.pending = null; await input.persist(); break; }
      const result = await storage({ action: `${family}_apply`, request });
      if (!record(result)) throw Error("STATE_LAYOUT_RESULT_INVALID");
      const appliedStatus = status(result.job);
      cycle.pending = null; await input.persist();
      progressMade ||= layoutAdvanced(now, appliedStatus);
    }
    if (cycle.familyIndex === FAMILIES.length) { cycle.active = false; cycle.baseline = current; cycle.jobs = {}; cycle.continuationProof = null; }
  } finally {
    if (!cycle.pending) {
      const release = { action: "release", ...guard };
      cycle.pending = { kind: "graph_lease", action: "release", payload: release }; await input.persist();
      const released = await invoke("state::lease", release);
      if (!record(released) || released.released !== true) throw Error("STATE_LAYOUT_GRAPH_RELEASE_UNKNOWN");
      cycle.pending = null; await input.persist();
    }
  }
  return finish(cycle.active ? "bounded_cycle_progress" : "cycle_complete", cycle.pending !== null);
  } finally {
    graphTurn.turn.release();
  }
}
