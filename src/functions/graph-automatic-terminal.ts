import { GRAPH_AUTOMATIC_ADOPTION_REVISION, GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION, GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION } from "../state/graph-automatic-recovery.js";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { StatePageError } from "../state/state-pages.js";
import { isStateCounter, StateTransactionError } from "../state/state-transactions.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { graphAutomaticRecoveryView, type GraphAutomaticRecoveryView } from "./graph-automatic-workflow.js";

export const GRAPH_TERMINAL_SOURCE_PROOF_REVISION = "applied-receipt-postcommit-v1";
const TERMINAL_PROJECTION_REVISION = "terminal-staging-child-proof-v1";
const JOBS_PROJECTION_REVISION = "jobs-derived-child-source-v1";

type TerminalDiagnosticStep = "terminal_cache_read" | "terminal_cache_write" | "terminal_job_read"
  | "terminal_turn_wait" | "control_after_turn" | "terminal_lease_acquire" | "terminal_lease_release"
  | "guarded_proof" | "native_terminal" | "local_delivery";
type TerminalDiagnosticOperation = "get" | "get_versioned" | "set" | "lease_acquire" | "lease_release"
  | "terminal_recover" | "status" | "local_delivery" | "admission";
const terminalFailures = new WeakMap<object, { recoveryStep: TerminalDiagnosticStep; recoveryOperation: TerminalDiagnosticOperation }>();

export function graphTerminalFailureContext(error: unknown) {
  return typeof error === "object" && error !== null ? terminalFailures.get(error) : undefined;
}
export function recordGraphTerminalFailure(error: unknown, recoveryStep: TerminalDiagnosticStep,
  recoveryOperation: TerminalDiagnosticOperation): void {
  if (typeof error === "object" && error !== null && !terminalFailures.has(error))
    terminalFailures.set(error, { recoveryStep, recoveryOperation });
}
export async function withGraphTerminalStep<T>(step: TerminalDiagnosticStep, operation: TerminalDiagnosticOperation,
  run: () => Promise<T>): Promise<T> {
  try { return await run(); }
  catch (error) { recordGraphTerminalFailure(error, step, operation); throw error; }
}

export class GraphTerminalRecoveryDeferred extends StateTransactionError {
  constructor() { super("STATE_TX_LEASE_BUSY"); }
}

export interface GraphTerminalRecoveryLink {
  sourceJobId: string;
  slotId: string;
  jobId: string;
  regenerationId: string;
  mode: "provider" | "local_replay";
  adoption?: { revision: typeof GRAPH_AUTOMATIC_ADOPTION_REVISION; epoch: number; regenerationId: string };
  replayRef?: { scope: string; key: string; version: string };
  selection?: { project: string; sessionId: string; sourceFingerprint: string; intentId?: string; jobId?: string };
}
export interface GraphTerminalRecoveryView {
  version: 1;
  sourceJobId: string;
  resolution: "local_replay" | "regeneration" | "terminal";
  reason: string | null;
  nextCheckAt: number | null;
  replacementJobId: string | null;
  action: "continue" | "wait" | "none";
}
export interface GraphTerminalRecoveryRecord {
  version: 1;
  sourceJobId: string;
  sourceJobVersion: string;
  updatedAt: string;
  recovery: GraphAutomaticRecoveryView | null;
  outcome: GraphTerminalRecoveryView;
  link?: GraphTerminalRecoveryLink;
  dispatchIntentId?: string;
  localParseRepair?: { parser: string; unitId: string; checkpointVersion: string; responseVersion: string };
  sourceProofRevision?: string;
  adoptionRevision?: string;
  unspentChildRevision?: string;
  unspentChildProofRevision?: string;
  emptyOutputRepairEligibility?: "eligible" | "ineligible";
}
function expiredUnspentAdoption(record: GraphTerminalRecoveryRecord): boolean {
  const link = record.link;
  const reservation = record.recovery?.reservation;
  return record.adoptionRevision === GRAPH_AUTOMATIC_ADOPTION_REVISION
    && link?.adoption?.revision === GRAPH_AUTOMATIC_ADOPTION_REVISION
    && link.adoption.regenerationId === link.regenerationId
    && Number.isSafeInteger(link.adoption.epoch) && link.adoption.epoch > 0 && link.adoption.epoch < Number.MAX_SAFE_INTEGER
    && reservation?.attempt_ordinal === 1 && reservation.epoch === link.adoption.epoch
    && reservation.adoption_count === reservation.epoch && reservation.consumed === false
    && reservation.capture === null && reservation.claim_present === false
    && (reservation.state === "prepared" || reservation.state === "expired")
    && Number.isSafeInteger(reservation.deadline_ms) && reservation.deadline_ms <= Date.now()
    && ["none", "continue"].includes(record.outcome.action)
    && ["GRAPH_AUTOMATIC_SLOT_EXHAUSTED", "GRAPH_AUTOMATIC_REGENERATION_READY", "GRAPH_AUTOMATIC_REGENERATION_DEADLINE"].includes(record.outcome.reason ?? "");
}
export function graphTerminalCacheReconsideration(record: GraphTerminalRecoveryRecord): "source" | "adoption" | null {
  if (expiredUnspentAdoption(record)) return "adoption";
  if (["GRAPH_AUTOMATIC_ADOPTION_CHILD_PRESENT", "GRAPH_AUTOMATIC_UNSPENT_CHILD_UNPROVEN"].includes(record.outcome.reason ?? "")
    && record.unspentChildProofRevision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION
    && (record.outcome.nextCheckAt === null || record.outcome.nextCheckAt <= Date.now())) return "adoption";
  if (record.outcome.reason === "GRAPH_AUTOMATIC_ADOPTION_CHILD_PRESENT"
    && record.unspentChildRevision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION
    && (record.outcome.nextCheckAt === null || record.outcome.nextCheckAt <= Date.now())) return "adoption";
  if (record.outcome.action !== "none") return null;
  if (!record.link && record.outcome.reason === "GRAPH_AUTOMATIC_SOURCE_UNIT_UNPROVEN"
    && record.sourceProofRevision !== GRAPH_TERMINAL_SOURCE_PROOF_REVISION) return "source";
  if (record.outcome.reason === "GRAPH_AUTOMATIC_SLOT_EXHAUSTED"
    && record.adoptionRevision !== GRAPH_AUTOMATIC_ADOPTION_REVISION) return "adoption";
  return null;
}

type TerminalTickError = { at: string; candidateJobId: string | null; stage: "candidate_recovery" | "metadata";
  errorName: string; code: string; stateFunction: string | null };
const terminalTickCodes = new Set([
  "UNKNOWN", "deserialization_error", "invocation_failed", "invocation_timeout", "TIMEOUT",
  "STATE_TX_FENCED", "STATE_TX_GENERATION_STALE", "STATE_TX_LEASE_BUSY", "STATE_TX_LIMIT_EXCEEDED", "STATE_TX_INVALID_REQUEST",
  "STATE_GRAPH_RECOVERY_REQUIRED", "STATE_RECORD_TOO_LARGE", "STATE_RETRIEVAL_RESOURCE_LIMIT",
  "STATE_PAGE_CURSOR_INVALID", "STATE_PAGE_CURSOR_STALE", "STATE_PAGE_INVALID_RESPONSE", "STATE_PAGE_UNSUPPORTED",
  "STATE_PAGE_INVALID_REQUEST", "STATE_GRAPH_AUTOMATIC_INVALID_RESPONSE", "STATE_GRAPH_AUTOMATIC_ID_CONFLICT",
  "STATE_GRAPH_AUTOMATIC_ADOPTION_EPOCH_CONFLICT", "STATE_GRAPH_AUTOMATIC_ADOPTION_AUTHORITY_UNPROVEN",
  "STATE_GRAPH_AUTOMATIC_SOURCE_UNIT_UNPROVEN", "STATE_GRAPH_AUTOMATIC_BINDING_MISMATCH",
]);
const terminalTickNames = new Set(["Error", "TypeError", "RangeError", "SyntaxError", "InvocationError", "StateTransactionError", "StatePageError"]);
const terminalTickFunctions = new Set(["state::get", "state::get_versioned", "state::storage", "state::list_page", "state::lease", "state::commit"]);
function terminalTickError(error: unknown, stage: TerminalTickError["stage"], candidateJobId: string | null): TerminalTickError {
  let code = "UNKNOWN", errorName = "unknown", stateFunction: string | null = null;
  const seen = new Set<object>();
  const visit = (value: unknown, depth: number) => {
    if (!value || typeof value !== "object" || seen.has(value) || depth > 4) return;
    seen.add(value);
    const record = value as Record<string, unknown>;
    if (errorName === "unknown" && typeof record.name === "string" && terminalTickNames.has(record.name)) errorName = record.name;
    if (typeof record.code === "string" && terminalTickCodes.has(record.code)
      && (code === "UNKNOWN" || code === "invocation_failed")) code = record.code;
    for (const key of ["stateFunction", "function_id", "functionId"]) {
      const name = record[key];
      if (stateFunction === null && typeof name === "string" && terminalTickFunctions.has(name)) stateFunction = name;
    }
    if (typeof record.message === "string") {
      const message = record.message.slice(0, 4096);
      const nativeCode = message.match(/\b(?:deserialization_error|STATE_[A-Z0-9_]{1,96}|invocation_timeout)\b/g)?.find(value => terminalTickCodes.has(value));
      if (nativeCode && (code === "UNKNOWN" || code === "invocation_failed")) code = nativeCode;
      const functionId = message.match(/\bstate::(?:get_versioned|get|storage|list_page|lease|commit)\b/)?.[0];
      if (stateFunction === null && functionId && terminalTickFunctions.has(functionId)) stateFunction = functionId;
    }
    for (const key of ["cause", "error", "response"]) visit(record[key], depth + 1);
  };
  visit(error, 0);
  return { at: new Date().toISOString(), candidateJobId, stage, errorName, code, stateFunction };
}

type Candidate = { id: string; state: string; kind: string; recoveryStopped?: boolean; selectedOrigin?: unknown;
  generation?: string; automaticReplacementOf?: unknown };
export interface GraphTerminalLocalContinuation {
  jobId: string;
  generation: string;
  checkpointVersion: string;
  chunkOrdinal: number;
}
type Recover = (jobId: string, localContinuation?: GraphTerminalLocalContinuation) => Promise<GraphTerminalRecoveryRecord | null>;
type Dispatch = (input: unknown, link: GraphTerminalRecoveryLink) => Promise<unknown>;
const executors = new WeakMap<StateKV, Recover>();
const dispatchers = new WeakMap<StateKV, Dispatch>();
const recordKey = (jobId: string) => `graph-automatic:terminal:${jobId}`;
const SCAN_KEY = "graph-automatic:terminal:scan";

export function installGraphTerminalRecovery(kv: StateKV, recover: Recover): void { executors.set(kv, recover); }
export function installGraphTerminalDispatcher(kv: StateKV, dispatch: Dispatch): void { dispatchers.set(kv, dispatch); }
export function dispatchGraphTerminalRecovery(kv: StateKV, input: unknown, link: GraphTerminalRecoveryLink): Promise<unknown> {
  const dispatch = dispatchers.get(kv);
  if (!dispatch) throw new Error("STATE_GRAPH_AUTOMATIC_DISPATCH_UNAVAILABLE");
  return dispatch(input, link);
}
export function readGraphTerminalRecovery(kv: StateKV, jobId: string): Promise<GraphTerminalRecoveryRecord | null> {
  return withGraphTerminalStep("terminal_cache_read", "get", () => kv.get(KV.config, recordKey(jobId)));
}
export async function recoverStoppedGraphJob(kv: StateKV, jobId: string, localContinuation?: GraphTerminalLocalContinuation): Promise<GraphTerminalRecoveryRecord | null> {
  const recover = executors.get(kv);
  if (!recover) return readGraphTerminalRecovery(kv, jobId);
  const visited = new Set<string>();
  let generation: string | undefined;
  let resolved = false;
  for (let depth = 0; depth < 8; depth++) {
    if (visited.has(jobId)) return null;
    visited.add(jobId);
    const row = await withGraphTerminalStep("terminal_job_read", "get_versioned", () => kv.getVersioned<Candidate>(KV.graphJobs, jobId));
    const job = row.value;
    if (!job || job.id !== jobId || job.kind !== "extraction" || job.state !== "failed" || job.recoveryStopped !== true
      || !isStateCounter(job.generation) || generation !== undefined && job.generation !== generation) return null;
    generation ??= job.generation;
    if (job.automaticReplacementOf == null) { resolved = true; break; }
    if (localContinuation) throw new StateTransactionError("STATE_GRAPH_AUTOMATIC_SOURCE_UNIT_UNPROVEN");
    if (typeof job.automaticReplacementOf !== "string" || job.automaticReplacementOf.length > 128
      || !/^(?:graphjob_[A-Za-z0-9_-]+|graphbuild_[a-f0-9]{16})$/.test(job.automaticReplacementOf)) return null;
    jobId = job.automaticReplacementOf;
  }
  if (!resolved) return null;
  return withKeyedLock(`graph-terminal-recovery:${jobId}`, async () => {
    const previous = await readGraphTerminalRecovery(kv, jobId);
    let confirmed = previous;
    let reconsideration = previous ? graphTerminalCacheReconsideration(previous) : null;
    const needsReservationProof = previous ? expiredUnspentAdoption(previous) : false;
    if (previous && (previous.outcome.action === "none" && !reconsideration
      || previous.outcome.nextCheckAt !== null && previous.outcome.nextCheckAt > Date.now())) return previous;
    if (localContinuation && previous?.link && (!reconsideration || needsReservationProof)) return previous;
    if (previous?.link && !localContinuation && (!reconsideration || needsReservationProof)) {
      const snapshot = await withGraphTerminalStep("native_terminal", "status",
        () => kv.graphAutomaticRecovery({ command: "status", slot_id: previous.link!.slotId }));
      if (snapshot.new_job_id !== previous.link.jobId || snapshot.regeneration_id !== previous.link.regenerationId
        || snapshot.anchor.job_id !== previous.link.sourceJobId
        || previous.link.adoption && (snapshot.adoption_revision !== previous.link.adoption.revision
          || snapshot.reservation?.epoch !== previous.link.adoption.epoch)
        || needsReservationProof && (snapshot.root_id !== previous.recovery?.rootId
          || snapshot.reservation?.attempt_id !== previous.recovery?.reservation?.attempt_id)) throw new Error("STATE_GRAPH_AUTOMATIC_ID_CONFLICT");
      const view = graphAutomaticRecoveryView(snapshot);
      const refreshed: GraphTerminalRecoveryRecord = { ...previous, updatedAt: new Date().toISOString(), recovery: view,
        outcome: { ...previous.outcome, ...(view.action === "none" ? { resolution: "terminal" as const } : {}),
          reason: snapshot.reason, nextCheckAt: view.nextCheckAt, action: view.action } };
      await withGraphTerminalStep("terminal_cache_write", "set", () => kv.set(KV.config, recordKey(jobId), refreshed));
      confirmed = refreshed;
      if (needsReservationProof) {
        reconsideration = expiredUnspentAdoption(refreshed) ? "adoption" : null;
        if (!reconsideration) return refreshed;
      }
      if (view.action === "none" && !reconsideration) return refreshed;
    }
    const job = await withGraphTerminalStep("terminal_job_read", "get_versioned", () => kv.getVersioned<Candidate>(KV.graphJobs, jobId));
    if (!job.value || job.value.kind !== "extraction" || job.value.state !== "failed" || job.value.recoveryStopped !== true) return null;
    const result = await recover(jobId, localContinuation);
    if (result) await withGraphTerminalStep("terminal_cache_write", "set", () => kv.set(KV.config, recordKey(jobId), result));
    return result ?? confirmed;
  });
}

export async function discoverStoppedGraphJobs(kv: StateKV): Promise<{ scanned: number; linked: number }> {
  if (!executors.has(kv)) return { scanned: 0, linked: 0 };
  return withKeyedLock("graph-terminal-recovery:scan", async () => {
    type Source = "jobs" | "terminal";
    type PendingPage = { source?: Source; items: unknown[]; nextCursor: string | null };
    const lastTickAt = new Date().toISOString();
    const deadline = Date.now() + 2_500;
    const raw = await kv.get<{ cursor?: string | null; terminalCursor?: string | null; historyIndex?: number; deferredJobId?: string;
      consistency?: "keyset_live"; page?: PendingPage; firstSource?: Source; nextHead?: "deferred" | "history";
      totalRowsVisited?: number; terminalProjectionRevision?: string; jobsProjectionRevision?: string; lastTickError?: TerminalTickError }>(KV.config, SCAN_KEY);
    const validId = (value: unknown): value is string => typeof value === "string" && Buffer.byteLength(value) <= 128
      && /^(?:graphjob_[A-Za-z0-9_-]+|graphbuild_[a-f0-9]{16})$/.test(value);
    const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
    const validCursor = (value: unknown): value is string | null => value === null
      || typeof value === "string" && Buffer.byteLength(value) <= 4096;
    const trustedCursor = (value: unknown) => raw?.consistency === "keyset_live" && validCursor(value) ? value : null;
    const cursors: Record<Source, string | null> = { jobs: trustedCursor(raw?.cursor), terminal: trustedCursor(raw?.terminalCursor) };
    const jobsProjectionCompatible = raw?.jobsProjectionRevision === JOBS_PROJECTION_REVISION;
    if (!jobsProjectionCompatible) cursors.jobs = null;
    const terminalProjectionCompatible = raw?.terminalProjectionRevision === TERMINAL_PROJECTION_REVISION;
    if (!terminalProjectionCompatible) cursors.terminal = null;
    let pendingPage = raw?.consistency === "keyset_live" ? raw.page : undefined;
    if (pendingPage && (!Array.isArray(pendingPage.items) || pendingPage.items.length > 3 || !validCursor(pendingPage.nextCursor)
      || pendingPage.source !== undefined && pendingPage.source !== "jobs" && pendingPage.source !== "terminal"
      || Buffer.byteLength(JSON.stringify(pendingPage.items)) > 32 * 1024)) throw new StatePageError("STATE_PAGE_INVALID_RESPONSE");
    if (pendingPage) pendingPage = { ...pendingPage, source: pendingPage.source ?? "jobs" };
    if (!jobsProjectionCompatible && pendingPage?.source === "jobs") pendingPage = undefined;
    if (!terminalProjectionCompatible && pendingPage?.source === "terminal") pendingPage = undefined;
    let rowsVisited = 0, candidatesVisited = 0, headVisits = 0, linked = 0, metadataBytes = 0;
    let historyIndex = Number.isSafeInteger(raw?.historyIndex) && raw!.historyIndex! >= 0 ? raw!.historyIndex! : 0;
    let deferredJobId = validId(raw?.deferredJobId) ? raw.deferredJobId : undefined;
    const firstSource: Source = pendingPage?.source ?? (raw?.firstSource === "jobs" ? "jobs" : "terminal");
    const nextFirstSource: Source = firstSource === "jobs" ? "terminal" : "jobs";
    let nextHead = raw?.nextHead === "history" ? "history" as const : "deferred" as const;
    let cycleComplete = false;
    let deferredReason: string | null = null;
    const storedError = raw?.lastTickError;
    let lastTickError: TerminalTickError | undefined = storedError && typeof storedError.at === "string" && storedError.at.length <= 40
      && /^\d{4}-\d\d-\d\dT[\d:.]+Z$/.test(storedError.at)
      && (storedError.candidateJobId === null || validId(storedError.candidateJobId))
      && ["candidate_recovery", "metadata"].includes(storedError.stage)
      && (terminalTickNames.has(storedError.errorName) || storedError.errorName === "unknown")
      && terminalTickCodes.has(storedError.code)
      && (storedError.stateFunction === null || terminalTickFunctions.has(storedError.stateFunction))
      ? { at: storedError.at, candidateJobId: storedError.candidateJobId, stage: storedError.stage,
        errorName: storedError.errorName, code: storedError.code, stateFunction: storedError.stateFunction } : undefined;
    let thisTickError: TerminalTickError | undefined;

    const completedSources = new Set<Source>();
    const attempted = new Set<string>();
    const staleCache = (value: unknown, jobId: string): value is GraphTerminalRecoveryRecord => object(value) && value.version === 1
      && value.sourceJobId === jobId && isStateCounter(value.sourceJobVersion) && object(value.outcome)
      && value.outcome.version === 1 && value.outcome.sourceJobId === jobId
      && ["terminal", "regeneration", "local_replay"].includes(String(value.outcome.resolution))
      && ["none", "continue"].includes(String(value.outcome.action)) && value.outcome.nextCheckAt == null
      && (value.sourceProofRevision == null || typeof value.sourceProofRevision === "string" && value.sourceProofRevision.length <= 128)
      && (value.adoptionRevision == null || typeof value.adoptionRevision === "string" && value.adoptionRevision.length <= 128)
      && graphTerminalCacheReconsideration(value as unknown as GraphTerminalRecoveryRecord) !== null;
    const candidateId = (value: unknown, source: Source): string | null => {
      if (!object(value)) return null;
      if (source === "jobs") return validId(value.id) && value.kind === "extraction" && value.state === "failed"
        && value.recoveryStopped === true ? value.id : null;
      return value.version === 1 && validId(value.sourceJobId) && isStateCounter(value.sourceJobVersion)
        && (value.sourceProofRevision == null || typeof value.sourceProofRevision === "string" && value.sourceProofRevision.length <= 128)
        && (value.sourceProofRevision !== GRAPH_TERMINAL_SOURCE_PROOF_REVISION
          || value.adoptionRevision !== GRAPH_AUTOMATIC_ADOPTION_REVISION
          || object(value.outcome) && value.outcome.reason === "GRAPH_AUTOMATIC_ADOPTION_CHILD_PRESENT"
            && value.unspentChildRevision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION
          || object(value.outcome) && ["GRAPH_AUTOMATIC_ADOPTION_CHILD_PRESENT", "GRAPH_AUTOMATIC_UNSPENT_CHILD_UNPROVEN"].includes(String(value.outcome.reason))
            && value.unspentChildProofRevision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION
          || object(value.outcome) && ["none", "continue"].includes(String(value.outcome.action))
            && ["GRAPH_AUTOMATIC_SLOT_EXHAUSTED", "GRAPH_AUTOMATIC_REGENERATION_READY", "GRAPH_AUTOMATIC_REGENERATION_DEADLINE"].includes(String(value.outcome.reason))) ? value.sourceJobId : null;
    };
    const budgetAvailable = () => {
      if (linked >= 1) { deferredReason ??= "link_budget"; return false; }
      if (candidatesVisited >= 4) { deferredReason ??= "candidate_budget"; return false; }
      if (Date.now() >= deadline) { deferredReason ??= "metadata_time_budget"; return false; }
      return true;
    };
    const save = () => withGraphTerminalStep("terminal_cache_write", "set", () => kv.set(KV.config, SCAN_KEY, {
      cursor: cursors.jobs, terminalCursor: cursors.terminal, terminalProjectionRevision: TERMINAL_PROJECTION_REVISION,
      jobsProjectionRevision: JOBS_PROJECTION_REVISION,
      historyIndex, consistency: "keyset_live",
      firstSource: nextFirstSource, nextHead, ...(pendingPage ? { page: pendingPage } : {}),
      ...(deferredJobId ? { deferredJobId } : {}),
      lastTickAt, rowsVisited, candidatesVisited, cycleComplete, deferredReason,
      ...(lastTickError ? { lastTickError } : {}),
      cursorPresent: cursors.jobs !== null || cursors.terminal !== null || !!pendingPage,
      totalRowsVisited: Math.min(Number.MAX_SAFE_INTEGER,
        (Number.isSafeInteger(raw?.totalRowsVisited) && raw!.totalRowsVisited! >= 0 ? raw!.totalRowsVisited! : 0) + rowsVisited),
    }));
    const attempt = async (jobId: string, staleOnly = false): Promise<boolean> => {
      if (attempted.has(jobId)) return true;
      if (!budgetAvailable()) return false;
      attempted.add(jobId);
      candidatesVisited++;
      try {
      const previous = await readGraphTerminalRecovery(kv, jobId);
      if (staleOnly && !staleCache(previous, jobId)) return true;
      let result: GraphTerminalRecoveryRecord | null;
      try { result = await recoverStoppedGraphJob(kv, jobId); }
      catch (error) {
        if (!(error instanceof GraphTerminalRecoveryDeferred)) throw error;
        deferredJobId ??= jobId;
        deferredReason ??= "graph_lease_busy";
        return true;
      }
      if (result?.sourceJobId) attempted.add(result.sourceJobId);
      if (deferredJobId === jobId) deferredJobId = undefined;
      if (result?.outcome.action === "wait" && !result.link) { deferredJobId ??= jobId; deferredReason ??= "native_cooldown"; }
      if (result?.link && (!previous?.link || result.link.regenerationId !== previous.link.regenerationId)) linked++;
      return true;
      } catch (error) {
        thisTickError = terminalTickError(error, "candidate_recovery", jobId);
        throw error;
      }
    };
    const visitPage = async (source: Source): Promise<boolean> => {
      if (!budgetAvailable() || completedSources.has(source)) return false;
      if (rowsVisited >= 32) { deferredReason ??= "metadata_row_budget"; return false; }
      if (pendingPage && pendingPage.source !== source) return false;
      if (!pendingPage) {
        const remainingBytes = 32 * 1024 - metadataBytes;
        if (remainingBytes < 128) { deferredReason ??= "metadata_byte_budget"; return false; }
        try {
          const page = await kv.pages<unknown>(source === "jobs" ? KV.graphJobs : KV.config, {
            ...(cursors[source] ? { cursor: cursors[source]! } : {}), consistency: "keyset_live", limit: 3, maxBytes: remainingBytes,
            fields: source === "jobs" ? ["id", "kind", "state", "recoveryStopped", "generation", "automaticReplacementOf"]
              : ["version", "sourceJobId", "sourceJobVersion", "sourceProofRevision", "adoptionRevision", "unspentChildRevision", "unspentChildProofRevision", "outcome"],
          }).next();
          if (page.done) { cursors[source] = null; completedSources.add(source); if (source === "jobs") cycleComplete = true; return false; }
          metadataBytes += Buffer.byteLength(JSON.stringify(page.value.items));
          pendingPage = { source, items: page.value.items, nextCursor: page.value.next_cursor };
        } catch (error) {
          if (!(error instanceof StatePageError)) throw error;
          if (error.code === "STATE_PAGE_CURSOR_STALE" || error.code === "STATE_PAGE_CURSOR_INVALID") {
            cursors[source] = null;
            pendingPage = undefined;
            completedSources.add(source);
            deferredReason ??= error.code === "STATE_PAGE_CURSOR_INVALID" ? "page_cursor_invalid" : "page_cursor_stale";
            return false;
          }
          if (error.code === "STATE_RECORD_TOO_LARGE") {
            completedSources.add(source); deferredReason ??= remainingBytes < 32 * 1024 ? "metadata_byte_budget" : "metadata_row_too_large"; return false;
          }
          throw error;
        }
      }
      while (pendingPage.items.length && rowsVisited < 32) {
        if (!budgetAvailable()) return false;
        const id = candidateId(pendingPage.items[0], source);
        if (id && !await attempt(id, source === "terminal")) return false;
        pendingPage.items.shift();
        rowsVisited++;
      }
      if (pendingPage.items.length) { deferredReason ??= "metadata_row_budget"; return false; }
      cursors[source] = pendingPage.nextCursor;
      pendingPage = undefined;
      if (cursors[source] === null) { completedSources.add(source); if (source === "jobs") cycleComplete = true; }
      return true;
    };
    const visitHead = async () => {
      if (!budgetAvailable()) return;
      if (nextHead === "deferred" && deferredJobId) {
        nextHead = "history"; headVisits++; await attempt(deferredJobId); return;
      }
      const histories = await Promise.all(["workflow-invocations:graph:v1:all:failures", "workflow-invocations:graph:v1:all"]
        .map(key => kv.get<{ items?: Array<{ jobId?: unknown; outcome?: unknown; occurredAt?: unknown }> }>(KV.state, key)));
      const historyIds = [...new Set(histories.flatMap(history => Array.isArray(history?.items) ? history.items : [])
        .filter(item => item?.outcome === "failed" && validId(item.jobId))
        .sort((left, right) => Date.parse(String(right.occurredAt)) - Date.parse(String(left.occurredAt)))
        .map(item => item.jobId as string))];
      nextHead = "deferred";
      if (historyIds.length) {
        const jobId = historyIds[historyIndex % historyIds.length]!;
        headVisits++;
        if (await attempt(jobId)) historyIndex = (historyIndex + 1) % historyIds.length;
        else nextHead = "history";
      } else if (deferredJobId) { headVisits++; await attempt(deferredJobId); }
    };
    try {
      await visitPage(firstSource);
      await visitHead();
      const secondSource: Source = firstSource === "jobs" ? "terminal" : "jobs";
      await visitPage(secondSource);
      let source = firstSource;
      while (budgetAvailable() && rowsVisited < 32 && completedSources.size < 2) {
        const before = rowsVisited;
        const beforeCompleted = completedSources.size;
        let selected = pendingPage?.source ?? source;
        if (completedSources.has(selected)) selected = selected === "jobs" ? "terminal" : "jobs";
        await visitPage(selected);
        if (rowsVisited === before && completedSources.size === beforeCompleted) break;
        source = selected === "jobs" ? "terminal" : "jobs";
      }
      if (rowsVisited >= 32 && completedSources.size < 2) deferredReason ??= "metadata_row_budget";
    } catch (error) {
      deferredReason = "candidate_or_metadata_error";
      lastTickError = thisTickError ?? terminalTickError(error, "metadata", null);
      throw error;
    } finally { await save(); }
    return { scanned: rowsVisited + headVisits, linked };
  });
}
