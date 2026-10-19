import { GraphWorkQueueDrainingError, peekGraphWorkQueue } from "../state/graph-work-queue.js";
import { randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import type { IIIClient } from "iii-sdk";
import type { CompressedObservation } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { StateTransactionError } from "../state/state-transactions.js";
import { StatePageError } from "../state/state-pages.js";
import { isBackgroundRecoveryPaused } from "../config.js";
import { graphRecordDigest } from "./graph-job-storage.js";
import { graphDispatchBlocker, graphFailureDiagnostic, graphResult, recoverGraphStoppedTerminal, withGraphAutomaticRegeneration, withLocalGraphRecovery, withSelectedGraphOrigin, GraphTerminalLocalDeliveryStopped } from "./graph-jobs.js";
import { graphAutomaticRecoveryView } from "./graph-automatic-workflow.js";
import { logger } from "../logger.js";
import { readGraphTerminalRecovery, type GraphTerminalRecoveryLink } from "./graph-automatic-terminal.js";
import { GRAPH_AUTOMATIC_ADOPTION_REVISION, GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION, isGraphUnspentPrepareWaitResult } from "../state/graph-automatic-recovery.js";
import type { GraphAutomaticRecoveryView } from "./graph-automatic-workflow.js";
import { readGraphSessionCapture } from "./graph-session-capture.js";

export interface GraphDispatchRequest {
  observations?: CompressedObservation[]; sessionId?: string; batchResponse?: string;
  deferred?: boolean; force?: boolean; batchEffectKey?: string; replacementOf?: string;
  graphJobId?: string; graphProvenanceVersion?: 2; graphWorkGroupsVersion?: 2;
  selected?: SelectedGraphControl;
}
export interface SelectedGraphControl {
  project: string; sessionId: string; sourceFingerprint: string;
  intentId?: string; jobId?: string; deliveryId?: string; unitId?: string; checkpoint?: string;
}
const selectedGeneration = new AsyncLocalStorage<SelectedGraphControl | undefined>();
export const selectedGraphGeneration = () => selectedGeneration.getStore();
export function withSelectedGraphGeneration<T>(control: SelectedGraphControl | undefined, run: () => Promise<T>): Promise<T> {
  return selectedGeneration.run(control ? Object.freeze({ ...control }) : undefined, run);
}
export function validateSelectedGraphControl(value: unknown): asserts value is SelectedGraphControl {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  const data = value as Record<string, unknown>;
  const allowed = new Set(["project", "sessionId", "sourceFingerprint", "intentId", "jobId", "deliveryId", "unitId", "checkpoint"]);
  if (Object.keys(data).some(key => !allowed.has(key)) || ["project", "sessionId", "sourceFingerprint"].some(key => typeof data[key] !== "string" || !(data[key] as string).trim() || (data[key] as string).length > 4096)
    || ["intentId", "jobId", "deliveryId", "unitId", "checkpoint"].some(key => data[key] !== undefined && (typeof data[key] !== "string" || !data[key] || (data[key] as string).length > 4096))) {
    throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  }
}
export async function validateSelectedGraphSources(kv: StateKV, selected: SelectedGraphControl, observations: Array<{ id: string; sessionId?: string }>) {
  for (const observation of observations) {
    if (observation.sessionId !== selected.sessionId) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    const current = await kv.get<Record<string, unknown>>(KV.observations(selected.sessionId), observation.id);
    if (!current || Object.entries(observation).some(([field, value]) => canonical({ value: current[field] }) !== canonical({ value }))) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  }
}
export interface GraphDispatchIntent {
  automaticTerminalLink?: GraphTerminalRecoveryLink;
  version: 1; id: string; jobId: string; digest: string; captureParts: number;
  createdAt: string; updatedAt: string; state: "pending" | "completed" | "failed";
  attempts: number; retryAfter?: string; blockingJobId?: string;
  deliveryId?: string; claimedAt?: string; providerRequired?: boolean; sourceRequired?: boolean; failureCode?: string;
  claimUntil?: string;
  groupedWorkProgress?: { workUnits: number; nodesAdded: number; edgesAdded: number };
  executionOrigin?: "selected";
}
const TOPIC = "agentmemory.graph.dispatch";
const FRAGMENT_CHARS = 128 * 1024;
const MAX_CAPTURE_BYTES = 64 * 1024 * 1024;
const MAX_SCAN_PAGES = 32;
const CLAIM_MS = 10 * 60_000;
const RETRY_MS = 30_000;
type Execute = (request: GraphDispatchRequest, jobId: string) => Promise<unknown>;
type Delivery = { intentId: string; deliveryId: string; localOnly: boolean; selected?: SelectedGraphControl };

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") return `{${Object.entries(value)
    .filter(([, item]) => item !== undefined).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
    .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(",")}}`;
  const text = JSON.stringify(value);
  if (text === undefined) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  return text;
}
function freezeGraphDispatchRequest(request: GraphDispatchRequest): GraphDispatchRequest {
  return JSON.parse(JSON.stringify({
    observations: request.observations,
    sessionId: request.sessionId,
    batchResponse: request.batchResponse,
    deferred: request.deferred,
    force: request.force,
    batchEffectKey: request.batchEffectKey,
    replacementOf: request.replacementOf,
    graphJobId: request.graphJobId,
    graphProvenanceVersion: request.graphProvenanceVersion,
    graphWorkGroupsVersion: request.graphWorkGroupsVersion,
  })) as GraphDispatchRequest;
}
function code(error: unknown): string {
  if (error instanceof StateTransactionError) return error.code;
  return "GRAPH_DISPATCH_FAILED";
}
function queued(intent: GraphDispatchIntent, extra: Record<string, unknown> = {}) {
  return { success: true, queued: true, deferred: true, intentId: intent.id, jobId: intent.jobId,
    retryAfter: intent.retryAfter ?? new Date(Date.now() + RETRY_MS).toISOString(),
    ...(intent.blockingJobId ? { blockingJobId: intent.blockingJobId } : {}),
    ...(intent.providerRequired ? { providerRequired: true } : {}),
    ...(intent.sourceRequired ? { sourceRequired: true } : {}), ...extra };
}

function groupedContinuation(value: unknown): { workUnits: number; nodesAdded: number; edgesAdded: number } | undefined {
  if (typeof value !== "object" || value === null || (value as { graphContinuation?: unknown }).graphContinuation !== true) return undefined;
  const progress = value as { workUnits?: unknown; nodesAdded?: unknown; edgesAdded?: unknown };
  if (![progress.workUnits, progress.nodesAdded, progress.edgesAdded].every((count) => Number.isSafeInteger(count) && (count as number) >= 0)) {
    throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
  }
  return { workUnits: progress.workUnits as number, nodesAdded: progress.nodesAdded as number, edgesAdded: progress.edgesAdded as number };
}

export function registerGraphDispatch(sdk: IIIClient, kv: StateKV, execute: Execute): (request: GraphDispatchRequest, terminalLink?: GraphTerminalRecoveryLink) => Promise<unknown> {
  const lock = <T>(id: string, run: () => Promise<T>) => withKeyedLock(`graph-dispatch:${id}`, run);
  const read = (id: string) => kv.get<GraphDispatchIntent>(KV.graphDispatchIntents, id);
  const write = (intent: GraphDispatchIntent) => kv.set(KV.graphDispatchIntents, intent.id, intent);
  async function terminalSnapshot(link: GraphTerminalRecoveryLink) {
    const native = await kv.graphAutomaticRecovery({ command: "status", slot_id: link.slotId });
    if (native.anchor.job_id !== link.sourceJobId || native.new_job_id !== link.jobId || native.regeneration_id !== link.regenerationId) {
      throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    }
    if (link.adoption && (native.adoption_revision !== link.adoption.revision
      || native.reservation?.epoch !== link.adoption.epoch || link.adoption.regenerationId !== link.regenerationId))
      throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    return native;
  }

  async function retireSuperseded(intent: GraphDispatchIntent, lockHeld: boolean): Promise<unknown | undefined> {
    const old = intent.automaticTerminalLink;
    if (intent.state !== "pending" || !old) return undefined;
    const saved = await readGraphTerminalRecovery(kv, old.sourceJobId);
    const next = saved?.link;
    const epoch = old.adoption?.epoch ?? 0;
    const newerEpoch = next?.adoption?.epoch;
    const reservation = saved?.recovery?.reservation;
    if (!saved || saved.version !== 1 || saved.sourceJobId !== old.sourceJobId
      || typeof saved.sourceJobVersion !== "string" || !/^(?:0|[1-9][0-9]*)$/.test(saved.sourceJobVersion)
      || !next || next.sourceJobId !== old.sourceJobId || next.jobId !== old.jobId || next.slotId !== old.slotId
      || intent.jobId !== old.jobId || next.mode !== old.mode || next.mode !== "provider"
      || saved.adoptionRevision !== GRAPH_AUTOMATIC_ADOPTION_REVISION
      || saved.recovery?.adoptionRevision !== GRAPH_AUTOMATIC_ADOPTION_REVISION
      || saved.recovery.slotId !== old.slotId || saved.outcome.sourceJobId !== old.sourceJobId
      || saved.outcome.replacementJobId !== old.jobId
      || !next.adoption || next.adoption.revision !== GRAPH_AUTOMATIC_ADOPTION_REVISION
      || typeof old.regenerationId !== "string" || !old.regenerationId
      || typeof next.regenerationId !== "string" || !next.regenerationId
      || next.adoption.regenerationId !== next.regenerationId || next.regenerationId === old.regenerationId
      || !Number.isSafeInteger(epoch) || epoch < 0 || epoch >= Number.MAX_SAFE_INTEGER
      || old.adoption && (old.adoption.revision !== GRAPH_AUTOMATIC_ADOPTION_REVISION
        || old.adoption.regenerationId !== old.regenerationId || epoch < 1)
      || !Number.isSafeInteger(newerEpoch) || newerEpoch! <= epoch || newerEpoch! >= Number.MAX_SAFE_INTEGER
      || !reservation || reservation.epoch !== newerEpoch
      || reservation.attempt_ordinal !== 1 || reservation.adoption_count !== newerEpoch
      || canonical({ selection: next.selection }) !== canonical({ selection: old.selection })
      || typeof saved.dispatchIntentId !== "string" || !/^graphintent_[a-f0-9]{64}$/.test(saved.dispatchIntentId)
      || saved.dispatchIntentId === intent.id) return undefined;
    const successor = await read(saved.dispatchIntentId);
    if (!successor || successor.id !== saved.dispatchIntentId || successor.jobId !== intent.jobId
      || successor.digest !== intent.digest || successor.captureParts !== intent.captureParts
      || canonical({ link: successor.automaticTerminalLink }) !== canonical({ link: next })) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    const native = await terminalSnapshot(next);
    if (native.root_id !== saved.recovery.rootId || native.slot_id !== old.slotId
      || native.reservation?.attempt_ordinal !== 1 || native.reservation.epoch !== newerEpoch
      || native.reservation.adoption_count !== newerEpoch
      || native.reservation.attempt_id !== reservation.attempt_id)
      throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    const retire = async () => {
      const latest = await read(intent.id);
      if (!latest || latest.state !== "pending" || latest.id !== intent.id || latest.jobId !== intent.jobId
        || latest.digest !== intent.digest || canonical({ link: latest.automaticTerminalLink }) !== canonical({ link: old }))
        return { success: true, stale: true, jobId: intent.jobId };
      await write({ ...latest, state: "failed", failureCode: "GRAPH_DISPATCH_SUPERSEDED", updatedAt: new Date().toISOString() });
      return { success: false, failed: true, stopped: true, superseded: true, jobId: intent.jobId };
    };
    return lockHeld ? retire() : lock(intent.id, retire);
  }
  async function reconcile(intent: GraphDispatchIntent, lockHeld = false): Promise<unknown | undefined> {
    let job = await kv.get<{ state: string; recoveryStopped?: boolean; failureCode?: string; continuationState?: { version?: unknown }; captureDigest?: string; result?: Parameters<typeof graphResult>[0]; automaticRecovery?: GraphAutomaticRecoveryView }>(KV.graphJobs, intent.jobId);
    if (!job && intent.automaticTerminalLink) {
      const superseded = await retireSuperseded(intent, lockHeld);
      if (superseded !== undefined) return superseded;
      const current = graphAutomaticRecoveryView(await terminalSnapshot(intent.automaticTerminalLink));
      if (current.action === "none") {
        if (intent.state === "pending") await write({ ...intent, state: "failed", failureCode: current.reason ?? "GRAPH_AUTOMATIC_TERMINAL", updatedAt: new Date().toISOString() });
        return { success: false, failed: true, stopped: true, jobId: intent.jobId, automaticRecovery: current };
      }
    }
    if (job?.state === "failed" && job.recoveryStopped === true && !job.automaticRecovery?.replacementJobId) {
      if (job.captureDigest !== intent.digest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const terminal = await recoverGraphStoppedTerminal(kv, intent.jobId);
      if (terminal) {
        if (intent.state === "pending") await write({ ...intent, state: "failed", updatedAt: new Date().toISOString() });
        if (terminal.link) {
          const linked = await kv.get<typeof job>(KV.graphJobs, terminal.link.jobId);
          if (linked?.state === "completed") return { ...graphResult<Record<string, unknown>>(linked.result),
            automaticTerminalRecovery: { ...terminal.outcome, action: "none", nextCheckAt: null } };
          if (terminal.outcome.action === "none" && !linked) return { success: false, failed: true, stopped: true,
            jobId: intent.jobId, automaticRecovery: terminal.recovery, automaticTerminalRecovery: terminal.outcome };
          if (linked?.state === "failed" && linked.recoveryStopped) return { success: false, failed: true, stopped: true,
            jobId: terminal.link.jobId, automaticRecovery: linked.automaticRecovery ?? terminal.recovery,
            automaticTerminalRecovery: { ...terminal.outcome, resolution: "terminal", action: "none", nextCheckAt: null,
              reason: linked.automaticRecovery?.reason ?? linked.failureCode ?? terminal.outcome.reason }, failureCode: linked.failureCode };
          const childRecovery = linked?.automaticRecovery;
          return { success: true, queued: true, deferred: true, jobId: terminal.link.jobId,
            ...(childRecovery || terminal.recovery ? { automaticRecovery: childRecovery ?? terminal.recovery } : {}),
            automaticTerminalRecovery: childRecovery ? { ...terminal.outcome, action: childRecovery.action === "none" ? "continue" : childRecovery.action,
              nextCheckAt: childRecovery.nextCheckAt } : terminal.outcome };
        }
        return { success: false, stopped: terminal.outcome.action === "none", queued: terminal.outcome.action !== "none",
          jobId: intent.jobId, ...(terminal.recovery ? { automaticRecovery: terminal.recovery } : {}), automaticTerminalRecovery: terminal.outcome };
      }
    }
    if (job?.automaticRecovery?.replacementJobId) {
      if (job.captureDigest !== intent.digest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const replacement = await kv.get<typeof job>(KV.graphJobs, job.automaticRecovery.replacementJobId);
      if (!replacement || replacement.state !== "completed" && !(replacement.state === "failed" && replacement.recoveryStopped === true)) return queued(intent, {
        jobId: job.automaticRecovery.replacementJobId,
        ...(replacement?.automaticRecovery ? { automaticRecovery: replacement.automaticRecovery } : {}),
      });
      job = { ...replacement, captureDigest: intent.digest };
    }
    const terminalFailure = job?.state === "failed" && job.recoveryStopped === true;
    if (!job || job.state !== "completed" && !terminalFailure) return undefined;
    if (job.captureDigest !== intent.digest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    if (intent.state === "pending") await write({ ...intent, state: terminalFailure ? "failed" : "completed", updatedAt: new Date().toISOString() });
    return terminalFailure ? { success: false, failed: true, stopped: true, jobId: intent.jobId,
      failureCode: job.failureCode ?? "STATE_GRAPH_RECOVERY_REQUIRED", ...graphResult<Record<string, unknown>>(job.result) } : graphResult(job.result);
  }
  async function defer(intent: GraphDispatchIntent, details: Partial<GraphDispatchIntent> = {}) {
    const latest = await read(intent.id);
    if (!latest || latest.state !== "pending" || latest.deliveryId !== intent.deliveryId) return queued(intent);
    const next: GraphDispatchIntent = { ...latest, ...details,
      deliveryId: latest.automaticTerminalLink ? latest.deliveryId : undefined,
      claimedAt: latest.automaticTerminalLink ? latest.claimedAt : undefined,
      claimUntil: latest.automaticTerminalLink ? latest.claimUntil : undefined,
      retryAfter: details.retryAfter ?? new Date(Date.now() + Math.min(RETRY_MS * 2 ** Math.min(latest.attempts, 5), 15 * 60_000)).toISOString(),
      updatedAt: new Date().toISOString() };
    await write(next);
    return queued(next);
  }
  async function load(intent: GraphDispatchIntent): Promise<GraphDispatchRequest> {
    if (!Number.isSafeInteger(intent.captureParts) || intent.captureParts < 1 || intent.captureParts > 512) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    let text = "", bytes = 0;
    for (let part = 0; part < intent.captureParts; part++) {
      const fragment = await kv.get<string>(KV.graphDispatchInputs(intent.id), `part:${part}`);
      if (typeof fragment !== "string") throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      bytes += Buffer.byteLength(fragment, "utf8");
      if (bytes > MAX_CAPTURE_BYTES) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
      text += fragment;
    }
    if (graphRecordDigest(text) !== intent.digest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    return JSON.parse(text) as GraphDispatchRequest;
  }
  async function claim(intent: GraphDispatchIntent, localOnly: boolean, deadline?: number): Promise<Delivery | null> {
    return lock(intent.id, async () => {
      const latest = await read(intent.id);
      if (!latest || latest.state !== "pending" || latest.retryAfter && Date.parse(latest.retryAfter) > Date.now()
        || latest.claimedAt && (latest.claimUntil ? Date.parse(latest.claimUntil) : Date.parse(latest.claimedAt) + CLAIM_MS) > Date.now()) return null;
      if (deadline !== undefined && deadline <= Date.now()) return null;
      const deliveryId = randomUUID(), now = new Date().toISOString();
      const claimUntil = new Date(Math.min(Date.parse(now) + CLAIM_MS, deadline ?? Infinity)).toISOString();
      await write({ ...latest, deliveryId, claimedAt: now, claimUntil, updatedAt: now });
      return { intentId: latest.id, deliveryId, localOnly };
    });
  }
  async function selectedRequest(intent: GraphDispatchIntent, selected: SelectedGraphControl) {
    const request = await load(intent);
    const session = await kv.get<{ project?: string }>(KV.sessions, selected.sessionId);
    if (session?.project !== selected.project || request.sessionId !== selected.sessionId || request.graphWorkGroupsVersion !== 2
      || request.observations?.some(observation => observation.sessionId !== selected.sessionId)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    await validateSelectedGraphSources(kv, selected, request.observations ?? []);
    return request;
  }
  async function deliver(data: Delivery): Promise<unknown> {
    if (!data || typeof data.intentId !== "string" || typeof data.deliveryId !== "string" || typeof data.localOnly !== "boolean") throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    if (data.selected !== undefined) validateSelectedGraphControl(data.selected);
    return lock(data.intentId, async () => {
      const intent = await read(data.intentId);
      if (!intent || intent.deliveryId !== data.deliveryId) return { success: true, stale: true };
      if (data.selected && (data.localOnly !== true || data.selected.intentId !== intent.id || data.selected.jobId !== intent.jobId
        || data.selected.sourceFingerprint !== intent.digest || data.selected.deliveryId !== data.deliveryId)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      if (data.selected) await selectedRequest(intent, data.selected);
      if (data.selected && intent.state === "pending" && intent.executionOrigin !== "selected") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const completed = await reconcile(intent, true);
      if (completed !== undefined) return completed;
      if (intent.state !== "pending") return { success: true, completed: intent.state === "completed", failed: intent.state === "failed", jobId: intent.jobId };
      if (!data.selected && isBackgroundRecoveryPaused()) return defer(intent, { failureCode: "GRAPH_RECOVERY_PAUSED" });
      let groupedWork = false;
      try {
        const blocker = await withSelectedGraphOrigin(data.selected, () => graphDispatchBlocker(kv));
        const original = await kv.get<{ state?: string; recoveryStopped?: boolean; automaticRegenerationId?: string;
          automaticReplacementOf?: string; automaticRegenerationClaimedSlot?: string;
          automaticRecovery?: { replacementJobId: string | null } }>(KV.graphJobs, intent.jobId);
        if (blocker && blocker.id !== intent.jobId && blocker.id !== original?.automaticRecovery?.replacementJobId) return defer(intent, { blockingJobId: blocker.id,
          ...(blocker.recoveryAfter ? { retryAfter: blocker.recoveryAfter } : {}) });
        const request = await load(intent);
        if (data.selected) {
          const job = await kv.get<{ recoveryStopped?: boolean; recoveryAfter?: string | null }>(KV.graphJobs, intent.jobId);
          if (job?.recoveryStopped || job?.recoveryAfter && Date.parse(job.recoveryAfter) > Date.now()) return defer(intent, { failureCode: "STATE_GRAPH_RECOVERY_REQUIRED" });
        }
        groupedWork = request.graphWorkGroupsVersion === 2;
        if (peekGraphWorkQueue(kv).draining) throw new GraphWorkQueueDrainingError();
        const active = { ...intent, attempts: intent.attempts + (groupedWork ? 0 : 1), updatedAt: new Date().toISOString() };
        await write(active);
        const executeRequest = () => withSelectedGraphOrigin(data.selected, () => withSelectedGraphGeneration(data.selected,
          () => withLocalGraphRecovery(data.localOnly, async () => execute(request, active.jobId))));
        if (intent.automaticTerminalLink) {
          const link = intent.automaticTerminalLink;
          const native = await terminalSnapshot(link);
          const current = graphAutomaticRecoveryView(native);
          const reservation = native.reservation;
          const canReachNativePreparation = link.mode === "provider"
            && native.unspent_child_revision === GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION
            && native.root_id === link.slotId && native.slot_id === link.slotId
            && native.new_job_id === intent.jobId && native.regeneration_id === link.regenerationId
            && ["GRAPH_AUTOMATIC_SLOT_EXHAUSTED", "GRAPH_AUTOMATIC_REGENERATION_DEADLINE", "GRAPH_AUTOMATIC_PREPARED_DEADLINE"].includes(native.reason ?? "")
            && reservation?.attempt_ordinal === 1 && reservation.state === "expired"
            && reservation.consumed === false && reservation.capture === null && reservation.claim_present === false
            && original?.state === "staging" && original.recoveryStopped !== true
            && original.automaticReplacementOf === link.sourceJobId && original.automaticRegenerationId === link.regenerationId
            && original.automaticRegenerationClaimedSlot === undefined;
          if (current.action === "none" && !canReachNativePreparation) {
            await write({ ...active, state: "failed", failureCode: current.reason ?? "GRAPH_AUTOMATIC_TERMINAL", updatedAt: new Date().toISOString() });
            return { success: false, failed: true, stopped: true, jobId: active.jobId, automaticRecovery: current };
          }
          if (link.selection) {
            const source = await kv.get<{ selectedOrigin?: typeof link.selection; automaticSelectedSource?: typeof link.selection; captureDigest?: string }>(KV.graphJobs, link.sourceJobId);
            if (canonical(source?.selectedOrigin ?? source?.automaticSelectedSource) !== canonical(link.selection)
              || source?.selectedOrigin && source.captureDigest !== link.selection.sourceFingerprint) {
              throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
            }
          }
        }
        const result = intent.automaticTerminalLink
          ? await withGraphAutomaticRegeneration(intent.automaticTerminalLink.regenerationId, intent.automaticTerminalLink.sourceJobId, executeRequest,
            intent.automaticTerminalLink.mode === "local_replay" ? intent.automaticTerminalLink.replayRef : undefined, intent.automaticTerminalLink.selection)
          : await executeRequest();
        if (isGraphUnspentPrepareWaitResult(result)) {
          if (result.jobId !== active.jobId) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
          return defer(active, { attempts: intent.attempts, retryAfter: result.retryAfter });
        }
        const progress = groupedContinuation(result);
        if (progress) {
          const latest = await read(active.id);
          if (!latest || latest.deliveryId !== data.deliveryId || latest.state !== "pending") return { success: true, stale: true };
          const now = new Date().toISOString();
          const continuation: GraphDispatchIntent = {
            ...latest, groupedWorkProgress: progress, deliveryId: data.selected ? undefined : randomUUID(), claimedAt: data.selected ? undefined : now,
            retryAfter: undefined, updatedAt: now,
          };
          await write(continuation);
          if (data.selected) return queued(continuation, { progress, continuation: true, selected: { ...data.selected, deliveryId: undefined } });
          try {
            await sdk.trigger({ function_id: "iii::durable::publish", payload: { topic: TOPIC,
              data: { intentId: continuation.id, deliveryId: continuation.deliveryId, localOnly: data.localOnly } } });
          } catch {
            // Keep the durable claim until lease expiry; publication may have succeeded.
          }
          return queued(continuation, { progress });
        }
        if (await reconcile(active, true) === undefined) await write({ ...active, state: "completed", updatedAt: new Date().toISOString() });
        return result;
      } catch (error) {
        if (error instanceof GraphWorkQueueDrainingError) {
          if (!groupedWork) {
            try {
              const latest = await read(intent.id);
              if (latest?.state === "pending" && latest.id === intent.id && latest.jobId === intent.jobId
                && latest.deliveryId === data.deliveryId && latest.deliveryId === intent.deliveryId
                && latest.claimedAt === intent.claimedAt && latest.claimUntil === intent.claimUntil
                && latest.digest === intent.digest && latest.captureParts === intent.captureParts
                && latest.attempts === intent.attempts + 1
                && canonical({ link: latest.automaticTerminalLink }) === canonical({ link: intent.automaticTerminalLink }))
                await write({ ...latest, attempts: intent.attempts });
            } catch {
              logger.warn("Graph drain counter compensation outcome unknown", { code: "GRAPH_DRAIN_COMPENSATION_UNKNOWN" });
              return queued(intent, { reason: "graph_work_queue_draining", counterCompensation: "unknown" });
            }
          }
          return queued(intent, { reason: "graph_work_queue_draining" });
        }
        if (error instanceof GraphTerminalLocalDeliveryStopped) return queued((await read(intent.id)) ?? intent, {
          localOnly: true, reason: "captured_local_delivery_already_materialized",
        });
        logger.warn("Graph dispatch delivery deferred", { intentId: intent.id, jobId: intent.jobId, ...graphFailureDiagnostic(error) });
        if (data.selected && error instanceof Error && error.name === "GraphProviderRecoveryRequired") {
          const latest = await read(intent.id);
          if (latest?.deliveryId !== data.deliveryId) return { success: true, stale: true };
          await write({ ...latest, attempts: intent.attempts, deliveryId: undefined, claimedAt: undefined, retryAfter: undefined,
            providerRequired: true, updatedAt: new Date().toISOString() });
          return queued(latest, { providerRequired: true });
        }
        return defer((await read(intent.id)) ?? intent, { failureCode: code(error),
          ...(groupedWork ? { attempts: intent.attempts + 1 } : {}),
          ...(error instanceof Error && error.name === "GraphProviderRecoveryRequired" ? { providerRequired: true } : {}),
          ...(error instanceof Error && error.name === "GraphSourceRecoveryRequired" ? { sourceRequired: true } : {}) });
      }
    });
  }
  sdk.registerFunction("mem::graph-dispatch-deliver", deliver);
  sdk.registerTrigger({ type: "durable:subscriber", function_id: "mem::graph-dispatch-deliver",
    config: { topic: TOPIC, max_retries: 5 } });
  sdk.registerFunction("mem::graph-dispatch", async (request?: { localOnly?: boolean; selected?: SelectedGraphControl }) => {
    if (request?.localOnly !== undefined && typeof request.localOnly !== "boolean") throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    if (request?.selected !== undefined) {
      const selected = request.selected;
      validateSelectedGraphControl(selected);
      if (!selected.intentId || !selected.jobId || request.localOnly === false) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
      const intent = await read(selected.intentId);
      if (!intent || intent.jobId !== selected.jobId || intent.digest !== selected.sourceFingerprint) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      await selectedRequest(intent, selected);
      if (intent.state === "pending" && intent.executionOrigin !== "selected") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const completed = await reconcile(intent);
      if (completed !== undefined) return completed;
      if (intent.state !== "pending") return { success: true, completed: intent.state === "completed", failed: intent.state === "failed", jobId: intent.jobId };
      const delivery = selected.deliveryId ? { intentId: intent.id, deliveryId: selected.deliveryId, localOnly: true } : await claim(intent, true);
      if (!delivery) return queued(intent);
      return deliver({ ...delivery, selected: { ...selected, deliveryId: delivery.deliveryId } });
    }
    const localOnly = request?.localOnly !== false;
    if (isBackgroundRecoveryPaused()) return { success: true, queued: 0, paused: true };
    let recovery: Record<string, unknown>;
    try { recovery = await sdk.trigger<unknown, Record<string, unknown>>({ function_id: "mem::graph-recover", payload: { localOnly } }); }
    catch (error) {
      const blocker = await graphDispatchBlocker(kv);
      return { success: true, queued: 0, deferred: true, failureCode: code(error),
        ...(blocker ? { blockingJobId: blocker.id } : {}), retryAfter: new Date(Date.now() + RETRY_MS).toISOString() };
    }
    if (recovery.providerRequired || recovery.sourceRequired || recovery.stopped || recovery.retryAfter || recovery.captureUnavailable) return { ...recovery, success: true, queued: 0, deferred: true, blockingJobId: recovery.jobId,
      retryAfter: recovery.retryAfter ?? new Date(Date.now() + RETRY_MS).toISOString() };
    const blocker = await graphDispatchBlocker(kv);
    if (blocker) return { success: true, queued: 0, deferred: true, blockingJobId: blocker.id, retryAfter: blocker.recoveryAfter ?? new Date(Date.now() + RETRY_MS).toISOString() };
    let scanned = 0, published = 0, pageCount = 0;
    const startedAt = Date.now();
    const saved = await kv.get<{ cursor: string | null }>(KV.config, "graph-dispatch:scan");
    try {
      for await (const page of kv.pages<GraphDispatchIntent>(KV.graphDispatchIntents, { ...(saved?.cursor ? { cursor: saved.cursor } : {}), limit: 4, maxBytes: 128 * 1024 })) {
        pageCount++;
        for (const intent of page.items) {
          scanned++;
          if (intent.state !== "pending" || intent.executionOrigin === "selected") continue;
          if (await reconcile(intent) !== undefined) continue;
          const delivery = await claim(intent, localOnly);
          if (!delivery) continue;
          try {
            await sdk.trigger({ function_id: "iii::durable::publish", payload: { topic: TOPIC, data: delivery } });
            published++;
          } catch (error) {
            // Retain the claim until its lease expires: publish may have succeeded.
            return { success: true, queued: published, deferred: true, failureCode: code(error) };
          }
        }
        await kv.set(KV.config, "graph-dispatch:scan", { cursor: page.next_cursor });
        if (published >= 4 || pageCount >= MAX_SCAN_PAGES || Date.now() - startedAt > 5_000) break;
      }
    } catch (error) {
      if (!(error instanceof StatePageError) || error.code !== "STATE_PAGE_CURSOR_STALE") throw error;
      await kv.set(KV.config, "graph-dispatch:scan", { cursor: null });
      return { success: true, queued: published, deferred: true, failureCode: error.code };
    }
    return { success: true, queued: published, scanned };
  });

  return async (request: GraphDispatchRequest, terminalLink?: GraphTerminalRecoveryLink) => {
    if (terminalLink && (request.graphJobId !== terminalLink.jobId || request.replacementOf !== terminalLink.sourceJobId)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    if (request?.selected !== undefined) validateSelectedGraphControl(request.selected);
    if (!request || typeof request !== "object" || Array.isArray(request)
      || request.sessionId !== undefined && typeof request.sessionId !== "string"
      || request.observations !== undefined && (!Array.isArray(request.observations) || request.observations.length > 100_000)
      || [request.deferred, request.force].some(value => value !== undefined && typeof value !== "boolean")
      || [request.batchResponse, request.batchEffectKey, request.replacementOf, request.graphJobId].some(value => value !== undefined && typeof value !== "string")
      || request.graphProvenanceVersion !== undefined && request.graphProvenanceVersion !== 2
      || request.graphWorkGroupsVersion !== undefined && request.graphWorkGroupsVersion !== 2) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const frozen = freezeGraphDispatchRequest(request);
    if (!frozen.observations && frozen.sessionId) frozen.observations = (await readGraphSessionCapture(kv, frozen.sessionId)).observations;
    if (!frozen.graphJobId && !frozen.batchEffectKey) frozen.graphProvenanceVersion = 2;
    const text = canonical(frozen);
    if (Buffer.byteLength(text, "utf8") > MAX_CAPTURE_BYTES || (frozen.observations?.length ?? 0) > 100_000) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
    const digest = graphRecordDigest(text);
    const adoption = terminalLink?.adoption;
    if (adoption) {
      if (adoption.revision !== GRAPH_AUTOMATIC_ADOPTION_REVISION || !Number.isSafeInteger(adoption.epoch)
        || adoption.epoch < 1 || adoption.epoch >= Number.MAX_SAFE_INTEGER || adoption.regenerationId !== terminalLink!.regenerationId)
        throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const confirmed = await readGraphTerminalRecovery(kv, terminalLink!.sourceJobId);
      const saved = confirmed?.link;
      if (!saved || saved.sourceJobId !== terminalLink!.sourceJobId || saved.slotId !== terminalLink!.slotId
        || saved.jobId !== terminalLink!.jobId || saved.regenerationId !== terminalLink!.regenerationId
        || saved.adoption?.epoch !== adoption.epoch || saved.adoption.revision !== adoption.revision
        || confirmed.adoptionRevision !== adoption.revision || confirmed.recovery?.reservation?.epoch !== adoption.epoch)
        throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    }
    const id = adoption ? `graphintent_${graphRecordDigest(`${digest}:native-adoption:${adoption.epoch}`)}` : `graphintent_${digest}`;
    const jobId = frozen.graphJobId ?? (frozen.batchEffectKey ? `graphcallback:${frozen.batchEffectKey}` : `graphjob_${digest}`);
    const selected = request.selected ? { ...request.selected, intentId: id, jobId } : undefined;
    if (selected) {
      if (selected.sourceFingerprint !== digest || request.selected?.intentId !== undefined && request.selected.intentId !== id
        || request.selected?.jobId !== undefined && request.selected.jobId !== jobId || request.selected?.deliveryId !== undefined) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const session = await kv.get<{ project?: string }>(KV.sessions, selected.sessionId);
      if (session?.project !== selected.project || frozen.sessionId !== selected.sessionId || frozen.graphWorkGroupsVersion !== 2
        || frozen.observations?.some(observation => observation.sessionId !== selected.sessionId)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      await validateSelectedGraphSources(kv, selected, frozen.observations ?? []);
    }
    const intent = await lock(id, async () => {
      const existing = await read(id);
      if (existing) {
        if (selected && existing.state === "pending" && existing.executionOrigin !== "selected") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
        return existing;
      }
      if (adoption) {
        const native = await terminalSnapshot(terminalLink!);
        if (!native.reservation || native.reservation.consumed || native.reservation.capture !== null
          || native.reservation.state !== "prepared" || native.reservation.deadline_ms <= Date.now())
          throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      }
      let captureParts = 0;
      for (let start = 0; start < text.length;) {
        let end = Math.min(start + FRAGMENT_CHARS, text.length);
        if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end--;
        await kv.set(KV.graphDispatchInputs(id), `part:${captureParts++}`, text.slice(start, end));
        start = end;
      }
      const now = new Date().toISOString();
      return write({ version: 1, id, jobId,
        digest, captureParts, createdAt: now, updatedAt: now, state: "pending", attempts: 0,
        ...(terminalLink ? { automaticTerminalLink: terminalLink } : {}),
        ...(selected && { executionOrigin: "selected" as const }) });
    });
    if (intent.executionOrigin === "selected" && !selected) return { success: true, reason: "selected_foreground_required" };
    const completed = await reconcile(intent);
    if (intent.state !== "pending" || completed !== undefined) return completed ?? { success: true, completed: intent.state === "completed", failed: intent.state === "failed", jobId: intent.jobId };
    let delivery: Delivery | null;
    if (terminalLink) {
      const native = await terminalSnapshot(terminalLink);
      const current = graphAutomaticRecoveryView(native);
      if (current.action === "none") {
        if (intent.state === "pending") await write({ ...intent, state: "failed", failureCode: current.reason ?? "GRAPH_AUTOMATIC_TERMINAL", updatedAt: new Date().toISOString() });
        return { success: false, failed: true, stopped: true, jobId: intent.jobId, intentId: intent.id, automaticRecovery: current };
      }
      if (intent.retryAfter && Date.parse(intent.retryAfter) > Date.now()) return queued(intent);
      const deadline = native.reason === "GRAPH_AUTOMATIC_REGENERATION_READY" ? native.deadline_ms ?? undefined : undefined;
      delivery = intent.deliveryId ? { intentId: intent.id, deliveryId: intent.deliveryId, localOnly: Boolean(selected) }
        : await claim(intent, Boolean(selected), deadline);
    } else delivery = await claim(intent, Boolean(selected));
    if (delivery && terminalLink) {
      const delivered = await deliver(delivery);
      return delivered && typeof delivered === "object" ? { ...delivered, intentId: intent.id } : { result: delivered, intentId: intent.id };
    }
    return delivery ? deliver({ ...delivery, ...(selected ? { selected: { ...selected, deliveryId: delivery.deliveryId } } : {}) }) : queued(intent);
  };
}
