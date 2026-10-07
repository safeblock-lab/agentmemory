import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import type { StateKV, StatePage } from "../state/kv.js";
import { generateId, isGuardedGraphRecord, KV } from "../state/schema.js";
import { logger } from "../logger.js";
import type { CompressedObservation, GraphControlState, GraphExtractionJob } from "../types.js";
import { StateTransactionError, type StateGraphLease, type StateJsonValue } from "../state/state-transactions.js";
import { StatePageError } from "../state/state-pages.js";
import { GraphJobStorage, graphJson, graphRecordDigest, type GraphDeltaPreparation } from "./graph-job-storage.js";

const LEASE_TTL_MS = 120_000;
const CAPTURE_FRAGMENT_CHARS = 128 * 1024;
const MAX_GRAPH_EXTRACTION_STAGING_CHUNKS = 100_000;
const MAX_GRAPH_EXTRACTION_RECOVERY_FAILURES = 3;
const GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS = 30_000;
const MAX_GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS = 15 * 60_000;
interface DurableGraphJob extends Omit<GraphExtractionJob, "logicalDeltaCount"> {
  logicalDeltaCount?: number;
  captureParts: number;
  captureDigest: string;
  captureComplete: boolean;
  result?: StateJsonValue;
  recoveryDeltaId?: string;
  recoveryAttempts?: number;
  recoveryAfter?: string | null;
  recoveryStopped?: boolean;
}
interface GraphExecution {
  storage: GraphJobStorage;
  job: DurableGraphJob;
  cursor: number;
  delta?: GraphDeltaPreparation;
}
type GraphHandler = (data: unknown, jobId: string) => Promise<unknown>;
type CaptureReader = Pick<GraphJobStorage, "get">;
const execution = new AsyncLocalStorage<GraphExecution>();
const originals = new WeakMap<StateKV, StateKV>();
const handlers = new WeakMap<StateKV, Map<GraphExtractionJob["kind"], GraphHandler>>();
const graphJobQueues = new WeakMap<StateKV, Promise<void>>();
const baseKV = (kv: StateKV): StateKV => originals.get(kv) ?? kv;

async function withGraphJobQueue<T>(kv: StateKV, run: () => Promise<T>): Promise<T> {
  const previous = graphJobQueues.get(kv);
  let release!: () => void;
  const current = new Promise<void>((resolve) => { release = resolve; });
  graphJobQueues.set(kv, current);
  if (previous) await previous;
  try {
    return await run();
  } finally {
    release();
    if (graphJobQueues.get(kv) === current) graphJobQueues.delete(kv);
  }
}

export function graphCapturedAt(): string { return execution.getStore()?.delta?.capturedAt ?? new Date().toISOString(); }
export function graphJobId(): string | undefined { return execution.getStore()?.job.id; }
export function hasGraphDelta(): boolean { return execution.getStore()?.delta !== undefined; }
export function graphResult<T>(result: StateJsonValue | undefined): T {
  return structuredClone((result as { value?: T } | undefined)?.value) as T;
}

export type GraphJobReadOnlyStorage = Pick<GraphJobStorage, "get">;
export type GraphJobPreflight<T> = (storage: GraphJobReadOnlyStorage) => Promise<T | null>;

export function graphTransactionFailure(error: unknown): boolean {
  if (error instanceof StateTransactionError) return true;
  if (typeof error !== "object" || error === null) return false;
  const record = error as { code?: unknown; message?: unknown; cause?: unknown };
  return (typeof record.code === "string" && /^STATE_(?:TX_|GRAPH_|RECORD_TOO_LARGE|PAGE_)/.test(record.code))
    || (typeof record.message === "string" && /STATE_(?:TX_|GRAPH_|RECORD_TOO_LARGE|PAGE_)/.test(record.message))
    || (record.cause !== undefined && graphTransactionFailure(record.cause));
}

export function graphKV(kv: StateKV): StateKV {
  if (originals.has(kv)) return kv;
  const proxy = new Proxy(kv, {
    get(target, property) {
      const value: unknown = Reflect.get(target, property, target);
      if (typeof value !== "function") return value;
      if (["get", "set", "delete", "update"].includes(String(property))) return (...args: unknown[]) => {
        const context = execution.getStore();
        const scope = String(args[0]), key = String(args[1]);
        if (context?.storage.kv === target && isGuardedGraphRecord(scope, key)) {
          if (context.delta) {
            const staged = context.storage.facade(context.delta);
            const method = Reflect.get(staged, property, staged) as (...values: unknown[]) => unknown;
            return method(...args);
          }
          if (property === "get") return context.storage.get(scope, key);
          throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
        }
        return Reflect.apply(value, target, args);
      };
      return value.bind(target);
    },
  });
  originals.set(proxy, kv);
  return proxy;
}

function nextDelta(ordinal: number): GraphDeltaPreparation {
  return { id: `delta:${ordinal}`, ordinal, capturedAt: new Date().toISOString(), attempt: randomUUID(), phase: "preparing", shadowCount: 0 };
}

export async function withGraphDelta<T>(kv: StateKV, run: () => Promise<T>, advanceGeneration = false): Promise<T> {
  const context = execution.getStore();
  if (!context || context.storage.kv !== baseKV(kv)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  if (context.delta) return run();
  const ordinal = context.cursor++;
  const scope = KV.graphDeltas(context.job.id);
  const saved = await context.storage.get<GraphDeltaPreparation>(scope, `manifest:${ordinal}`);
  const checkpoint = context.storage.checkpoint;
  if (saved && checkpoint && (checkpoint.delta_ordinal > ordinal || (checkpoint.delta_ordinal === ordinal && checkpoint.visibility === "complete"))) return graphResult<T>(saved.result);
  let delta = saved ?? nextDelta(ordinal);
  if (saved?.phase === "freezing" || saved?.phase === "prepared") {
    await context.storage.finishFreeze(delta);
    await context.storage.apply(delta);
    return graphResult<T>(delta.result);
  }
  // No live rows have changed during preparation. Discard an interrupted
  // shadow attempt, retaining the frozen manifest's original timestamp.
  delta = { ...delta, attempt: randomUUID(), shadowCount: 0, phase: "preparing", advanceGeneration };
  if (checkpoint?.logical_delta_id !== delta.id) context.storage.checkpoint = null;
  await context.storage.stage(scope, `manifest:${ordinal}`, delta, delta);
  const result = await execution.run({ ...context, delta }, run);
  delta.advanceGeneration = advanceGeneration && !(typeof result === "object" && result !== null && "success" in result && result.success === false);
  if (delta.advanceGeneration) {
    await context.storage.facade(delta).set(KV.graphJobs, context.job.id, {
      ...context.job, state: "completed", logicalDeltaCount: ordinal + 1,
      updatedAt: delta.capturedAt, result: graphJson({ value: result }),
    });
  }
  await context.storage.freeze(delta, result);
  await context.storage.apply(delta);
  return result;
}

export async function freezeGraphValue<T>(kv: StateKV, name: string, create: () => Promise<T> | T): Promise<T> {
  return freezeValue(kv, name, create, false);
}

export async function freezeGraphHandledValue<T>(kv: StateKV, name: string, create: () => Promise<T> | T): Promise<T> {
  return freezeValue(kv, name, create, true);
}

async function freezeValue<T>(kv: StateKV, name: string, create: () => Promise<T> | T, handleFailure: boolean): Promise<T> {
  const context = execution.getStore();
  if (!context) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  const reference = await withGraphDelta(kv, async () => {
    const current = execution.getStore();
    if (!current?.delta) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const scope = KV.graphProviderResults(context.job.id), key = `value:${current.delta.ordinal}:${name}`;
    const saved = await context.storage.get<FrozenValue>(scope, key);
    if (saved) return { scope, key, ...saved };
    let value: T | FrozenFailure;
    let failure = false;
    try { value = await create(); }
    catch (error) {
      if (!handleFailure || graphTransactionFailure(error)) throw error;
      failure = true;
      value = { name: error instanceof Error ? error.name : "unknown", message: error instanceof Error ? error.message : String(error) };
    }
    return { scope, key, ...await writeFrozenValue(context.storage, scope, key, value, current.delta, failure) };
  });
  // Top-level calls complete their delta before the caller handles failure;
  // nested calls retain their enclosing delta's original completion boundary.
  if (reference.failure) {
    const failure = await readFrozenValue<FrozenFailure>(context.storage, reference.scope, reference.key, reference);
    if (typeof failure?.name !== "string" || typeof failure.message !== "string") throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    const error = new Error(failure.message);
    error.name = failure.name;
    throw error;
  }
  return readFrozenValue<T>(context.storage, reference.scope, reference.key, reference);
}

interface FrozenValue { parts: number; digest: string; failure?: true }
interface FrozenFailure { name: string; message: string }

async function writeFrozenValue(storage: GraphJobStorage, scope: string, key: string, value: unknown, delta: GraphDeltaPreparation, failure = false): Promise<FrozenValue> {
  const text = JSON.stringify({ value });
  let parts = 0;
  for (let start = 0; start < text.length;) {
    let end = Math.min(text.length, start + CAPTURE_FRAGMENT_CHARS);
    if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end--;
    const partKey = `${key}:part:${parts++}`, part = text.slice(start, end);
    const previous = await storage.get<string>(scope, partKey);
    if (previous !== null && previous !== part) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    if (previous === null) await storage.stage(scope, partKey, part, delta);
    start = end;
  }
  const reference: FrozenValue = { parts, digest: graphRecordDigest(text), ...(failure ? { failure: true } : {}) };
  await storage.stage(scope, key, reference, delta);
  return reference;
}

async function readFrozenValue<T>(storage: CaptureReader, scope: string, key: string, reference: FrozenValue): Promise<T> {
  let text = "";
  for (let ordinal = 0; ordinal < reference.parts; ordinal++) {
    const part = await storage.get<string>(scope, `${key}:part:${ordinal}`);
    if (part === null) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    text += part;
  }
  if (graphRecordDigest(text) !== reference.digest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  return (JSON.parse(text) as { value: T }).value;
}

export async function freezeGraphSessionObservations(kv: StateKV, sessionId: string): Promise<CompressedObservation[]> {
  const context = execution.getStore();
  if (!context) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  const scope = KV.graphInputs(context.job.id);
  const reference = await withGraphDelta(kv, async () => {
    const delta = execution.getStore()?.delta;
    if (!delta) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const key = `session:${delta.ordinal}`;
    let source = await context.storage.get<{ count: number; pageOrdinal: number; cursor?: string; complete: boolean }>(scope, key);
    if (!source) {
      // The first cursor pins the source revision before any admitted row is
      // materialized. Recovery uses it and fails visibly if the source moved.
      source = { count: 0, pageOrdinal: 0, complete: false };
    }
    while (!source.complete) {
        const pageKey: string = `${key}:page:${source.pageOrdinal}`;
        const pageReference = await context.storage.get<FrozenValue>(scope, pageKey);
        let page: StatePage<CompressedObservation>;
        if (pageReference) page = await readFrozenValue<StatePage<CompressedObservation>>(context.storage, scope, pageKey, pageReference);
        else {
          const pages: AsyncGenerator<StatePage<CompressedObservation>> = context.storage.kv.pages<CompressedObservation>(KV.observations(sessionId), { ...(source.cursor ? { cursor: source.cursor } : {}), limit: 1 });
          try {
            const next: IteratorResult<StatePage<CompressedObservation>> = await pages.next();
            if (next.done) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
            page = next.value;
            await writeFrozenValue(context.storage, scope, pageKey, page, delta);
          } finally { await pages.return(undefined); }
        }
        for (const observation of page.items) {
          if (observation.title) {
            const rowKey = `${key}:row:${source.count}`;
            const previous = await context.storage.get<FrozenValue>(scope, rowKey);
            if (previous) {
              const frozen = await readFrozenValue<CompressedObservation>(context.storage, scope, rowKey, previous);
              if (graphRecordDigest(JSON.stringify(frozen)) !== graphRecordDigest(JSON.stringify(observation))) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
            } else await writeFrozenValue(context.storage, scope, rowKey, observation, delta);
            source.count++;
          }
        }
        source = { count: source.count, pageOrdinal: source.pageOrdinal + 1, complete: page.next_cursor === null, ...(page.next_cursor ? { cursor: page.next_cursor } : {}) };
        await context.storage.stage(scope, key, source, delta);
    }
    return { key, count: source.count };
  });
  const observations: CompressedObservation[] = [];
  for (let ordinal = 0; ordinal < reference.count; ordinal++) {
    const key = `${reference.key}:row:${ordinal}`;
    const row = await context.storage.get<FrozenValue>(scope, key);
    if (!row) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    observations.push(await readFrozenValue<CompressedObservation>(context.storage, scope, key, row));
  }
  return observations;
}

async function capture(storage: GraphJobStorage, job: DurableGraphJob, data: unknown): Promise<void> {
  const text = JSON.stringify(data);
  if (text === undefined) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  const delta: GraphDeltaPreparation = { id: "capture", ordinal: 0, capturedAt: job.createdAt, attempt: "capture", phase: "preparing" };
  job.captureDigest = graphRecordDigest(text);
  await storage.stage(KV.graphJobs, job.id, job, delta);
  for (let start = 0, ordinal = 0; start < text.length; ordinal++) {
    let end = Math.min(text.length, start + CAPTURE_FRAGMENT_CHARS);
    if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end--;
    await storage.stage(KV.graphInputs(job.id), `capture:${ordinal}`, text.slice(start, end), delta);
    job.captureParts = ordinal + 1;
    start = end;
  }
  job.captureComplete = true;
  await storage.stage(KV.graphJobs, job.id, job, delta);
  await storage.completeStaging(delta);
}

async function readCapture(storage: CaptureReader, job: DurableGraphJob): Promise<unknown> {
  if (!job.captureComplete) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  if (!Number.isSafeInteger(job.captureParts) || job.captureParts < 1 || typeof job.captureDigest !== "string" || !/^[a-f0-9]{64}$/.test(job.captureDigest)) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
  let text = "";
  for (let ordinal = 0; ordinal < job.captureParts; ordinal++) {
    const part = await storage.get<string>(KV.graphInputs(job.id), `capture:${ordinal}`);
    if (part === null) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    if (typeof part !== "string") throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    text += part;
  }
  if (graphRecordDigest(text) !== job.captureDigest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  return JSON.parse(text) as unknown;
}

async function pendingJob(kv: StateKV, generation?: string): Promise<DurableGraphJob | null> {
  for await (const job of kv.values<DurableGraphJob>(KV.graphJobs)) {
    if ((!generation || job.generation === generation) && job.state !== "completed" && job.state !== "invalidated" && job.recoveryStopped !== true) return job;
  }
  return null;
}

function terminalPayloadFailureCode(error: unknown, seen = new Set<object>()): string | undefined {
  if (error instanceof StateTransactionError && ["STATE_RECORD_TOO_LARGE", "STATE_TX_LIMIT_EXCEEDED"].includes(error.code)) return error.code;
  if (typeof error !== "object" || error === null || seen.has(error)) return undefined;
  seen.add(error);
  const record = error as { code?: unknown; status?: unknown; statusCode?: unknown; status_code?: unknown; cause?: unknown; response?: unknown };
  if ([record.status, record.statusCode, record.status_code].some((status) => status === 413 || status === "413")) return "HTTP_413";
  if (typeof record.code === "string" && /(?:^|[_-])413(?:$|[_-])/.test(record.code)) return record.code;
  return terminalPayloadFailureCode(record.cause, seen) ?? terminalPayloadFailureCode(record.response, seen);
}

async function recordStagingFailure(storage: GraphJobStorage, error: unknown): Promise<"retry-scheduled" | "stopped" | false> {
  const permanentFailureCode = terminalPayloadFailureCode(error);
  const checkpoint = storage.checkpoint;
  if (!checkpoint || checkpoint.visibility !== "staging" || checkpoint.logical_delta_id !== `delta:${checkpoint.delta_ordinal}`) return false;
  const job = await storage.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
  if (!job || job.kind !== "extraction" || !job.captureComplete || job.generation !== checkpoint.generation || job.state === "completed" || job.state === "invalidated") return false;

  const recoveryAttempts = job.recoveryDeltaId === checkpoint.logical_delta_id
    && Number.isSafeInteger(job.recoveryAttempts) && (job.recoveryAttempts ?? 0) >= 0
    ? job.recoveryAttempts! + 1
    : 1;
  const stopped = permanentFailureCode !== undefined || recoveryAttempts >= MAX_GRAPH_EXTRACTION_RECOVERY_FAILURES;
  const failureCode = permanentFailureCode
    ?? (error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED");
  const terminalDelta: GraphDeltaPreparation = {
    id: checkpoint.logical_delta_id,
    ordinal: checkpoint.delta_ordinal,
    capturedAt: job.createdAt,
    attempt: "terminal-payload-failure",
    phase: "preparing",
  };
  await storage.stage(KV.graphJobs, job.id, {
    ...job,
    state: "failed",
    updatedAt: new Date().toISOString(),
    failureCode,
    recoveryDeltaId: checkpoint.logical_delta_id,
    recoveryAttempts,
    recoveryAfter: stopped ? null : new Date(Date.now() + Math.min(
      GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS * (2 ** (recoveryAttempts - 1)),
      MAX_GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS,
    )).toISOString(),
    recoveryStopped: stopped,
  }, terminalDelta);
  if (stopped) {
    await storage.completeStaging(terminalDelta);
    return "stopped";
  }
  return "retry-scheduled";
}

async function leasedControl(storage: GraphJobStorage): Promise<GraphControlState> {
  const control = await storage.get<GraphControlState>(KV.graphControl, "current");
  if (!control || control.generation !== storage.guard.generation) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
  if (!control.lease || control.lease.owner_id !== storage.guard.owner_id || control.lease.fence !== storage.guard.fence || control.lease.expires_at_ms <= Date.now()) throw new StateTransactionError("STATE_TX_FENCED");
  return control;
}

async function discoverJob(storage: GraphJobStorage, control: GraphControlState): Promise<DurableGraphJob | null> {
  if (!control.recovery) return pendingJob(storage.kv, control.generation);
  const checkpoint = control.recovery;
  if (checkpoint.generation !== control.generation || !checkpoint.job_id) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
  const job = await storage.get<DurableGraphJob>(KV.graphJobs, checkpoint.job_id);
  if (!job || job.version !== 1 || job.id !== checkpoint.job_id || !["queued", "staging", "applying", "failed"].includes(job.state) || typeof job.kind !== "string" || typeof job.createdAt !== "string" || typeof job.captureComplete !== "boolean") throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  if (job.generation !== control.generation) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
  const saved = await storage.get<GraphControlState["recovery"]>(KV.graphCheckpoints, job.id);
  if (!saved || saved.generation !== checkpoint.generation || saved.job_id !== checkpoint.job_id || saved.logical_delta_id !== checkpoint.logical_delta_id || saved.delta_ordinal !== checkpoint.delta_ordinal || saved.next_chunk_ordinal !== checkpoint.next_chunk_ordinal || saved.visibility !== checkpoint.visibility) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  return job;
}

export async function runGraphJob<T>(kv: StateKV, kind: GraphExtractionJob["kind"], input: unknown, run: (frozen: unknown) => Promise<T>, durableId?: string, preflight?: GraphJobPreflight<T>): Promise<T> {
  const base = baseKV(kv), nested = execution.getStore();
  if (nested?.storage.kv === base) return run(input);
  return withGraphJobQueue(base, async () => {
    let control = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
    if (!control) throw new StateTransactionError("STATE_TX_UNSUPPORTED");
    const lease = await base.lease({ action: "acquire", owner_id: randomUUID(), generation: control.generation, ttl_ms: LEASE_TTL_MS }) as StateGraphLease;
    const storage = new GraphJobStorage(base, { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }, durableId ?? generateId("graphjob"));
    const heartbeat = setInterval(() => {
      const renewing = { ...storage.guard };
      void base.lease({ action: "renew", ...renewing, ttl_ms: LEASE_TTL_MS }).catch((error) => { storage.failLease(error, renewing); logger.error("Graph job lease renewal failed", { jobId: storage.jobId, code: error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED" }); });
    }, LEASE_TTL_MS / 3);
    heartbeat.unref();
    let recoveryDeferred = false;
    try {
      control = await leasedControl(storage);
      const pending = await discoverJob(storage, control);
      if (pending && pending.id !== storage.jobId) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      const existing = await storage.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
      if (existing && existing.kind !== kind) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      if (existing?.state === "invalidated") throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      if (existing?.recoveryStopped) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      if (existing?.recoveryAfter && Date.parse(existing.recoveryAfter) > Date.now()) {
        recoveryDeferred = true;
        throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      }
      if (existing && graphRecordDigest(JSON.stringify(input)) !== existing.captureDigest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      if (existing?.state === "completed") return graphResult<T>(existing.result);
      if (existing && existing.generation !== control.generation) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
      if (control.recovery && control.recovery.job_id !== storage.jobId) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      if (preflight && !existing) {
        const result = await preflight(storage);
        if (result !== null) return result;
      }
      await storage.restore();
      const now = new Date().toISOString();
      const job: DurableGraphJob = existing ?? { version: 1, id: storage.jobId, generation: control.generation, kind, state: "staging", createdAt: now, updatedAt: now, inputCount: 1, captureParts: 0, captureDigest: "", captureComplete: false };
      if (!job.captureComplete) {
        if (existing && graphRecordDigest(JSON.stringify(input)) !== existing.captureDigest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
        await capture(storage, job, input);
      }
      if (storage.checkpoint?.logical_delta_id === "capture" && storage.checkpoint.visibility !== "complete") {
        await storage.completeStaging({ id: "capture", ordinal: 0, capturedAt: job.createdAt, attempt: "capture", phase: "preparing" });
      }
      const frozen = await readCapture(storage, job);
      const context: GraphExecution = { storage, job, cursor: 1 };
      const result = await execution.run(context, () => run(frozen));
      const latestControl = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
      if (latestControl?.generation !== job.generation) return result;
      storage.assertLease();
      await execution.run(context, () => withGraphDelta(kv, async () => {
        const active = execution.getStore();
        if (!active?.delta) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
        await storage.facade(active.delta).set(KV.graphJobs, job.id, { ...job, state: "completed", logicalDeltaCount: context.cursor, updatedAt: active.delta.capturedAt, result: graphJson({ value: result }) });
        return result;
      }));
      return result;
    } catch (error) {
      let recoveryOutcome: "retry-scheduled" | "stopped" | false = false;
      if (!recoveryDeferred) {
        try {
          recoveryOutcome = await recordStagingFailure(storage, error);
        } catch (terminalizeError) {
          logger.error("Graph job permanent failure could not be terminalized", {
            jobId: storage.jobId,
            code: terminalizeError instanceof StateTransactionError ? terminalizeError.code : "STATE_TX_FAILED",
          });
        }
      }
      const checkpoint = storage.checkpoint;
      const descriptor = typeof input === "object" && input !== null ? input as { sessionId?: unknown; observations?: unknown } : null;
      if (error instanceof StatePageError && error.code === "STATE_PAGE_CURSOR_STALE" && kind === "extraction" && typeof descriptor?.sessionId === "string" && descriptor.observations === undefined && checkpoint?.logical_delta_id === "delta:1" && checkpoint.delta_ordinal === 1 && checkpoint.visibility === "staging") {
        const current = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
        const admitted = await storage.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
        if (!current?.recovery && admitted) {
          // Cursor epochs change on native restart. An unfinished initial source
          // capture has no graph effects and cannot adopt a changed source.
          const delta: GraphDeltaPreparation = { id: checkpoint.logical_delta_id, ordinal: checkpoint.delta_ordinal, capturedAt: admitted.createdAt, attempt: "capture-abort", phase: "preparing" };
          await storage.stage(KV.graphJobs, storage.jobId, { ...admitted, state: "invalidated", failureCode: error.code }, delta);
          await storage.completeStaging(delta);
          logger.warn("Initial graph source capture invalidated; retry with a fresh complete request", { jobId: storage.jobId, code: error.code });
        }
      }
      if (recoveryOutcome === "stopped") logger.error("Graph job stopped after repeated or permanent staging failure", { jobId: storage.jobId, code: terminalPayloadFailureCode(error) ?? (error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED") });
      else if (recoveryOutcome === "retry-scheduled") logger.warn("Graph job retry scheduled with bounded backoff", { jobId: storage.jobId });
      else logger.error("Graph job paused for recovery", { jobId: storage.jobId, code: error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED" });
      throw error;
    } finally {
      clearInterval(heartbeat);
      await base.lease({ action: "release", ...storage.guard }).catch((error) => {
        if (!(error instanceof StateTransactionError) || !["STATE_TX_FENCED", "STATE_TX_GENERATION_STALE"].includes(error.code)) logger.warn("Graph job lease release failed", { jobId: storage.jobId });
      });
    }
  });
}

export function registerGraphJobHandler(kv: StateKV, kind: GraphExtractionJob["kind"], handler: GraphHandler): void {
  const base = baseKV(kv), registry = handlers.get(base) ?? new Map();
  registry.set(kind, handler); handlers.set(base, registry);
}

export function registerGraphJobRecovery(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::graph-recover", async () => {
    const base = baseKV(kv);
    const initial = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
    if (!initial) {
      if (await pendingJob(base)) throw new StateTransactionError("STATE_TX_UNSUPPORTED");
      return { success: true, recovered: false };
    }
    const lease = await base.lease({ action: "acquire", owner_id: randomUUID(), generation: initial.generation, ttl_ms: LEASE_TTL_MS }) as StateGraphLease;
    const storage = new GraphJobStorage(base, { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }, initial.recovery?.job_id ?? "recovery-discovery");
    let job: DurableGraphJob | null = null;
    let input: unknown;
    let handler: GraphHandler | undefined;
    try {
      const control = await leasedControl(storage);
      job = await discoverJob(storage, control);
      if (!job) return { success: true, recovered: false };
      const jobStorage = new GraphJobStorage(base, storage.guard, job.id);
      await jobStorage.restore();
      const jobCheckpoint = jobStorage.checkpoint;
      if (job.recoveryStopped) {
        if (jobCheckpoint?.visibility === "staging" && jobCheckpoint.logical_delta_id === `delta:${jobCheckpoint.delta_ordinal}`) {
          await jobStorage.completeStaging({
            id: jobCheckpoint.logical_delta_id,
            ordinal: jobCheckpoint.delta_ordinal,
            capturedAt: job.createdAt,
            attempt: "terminal-failure",
            phase: "preparing",
          });
        }
        return { success: true, recovered: false, jobId: job.id, stopped: true };
      }
      if (job.recoveryAfter && Date.parse(job.recoveryAfter) > Date.now()) {
        return { success: true, recovered: false, jobId: job.id, retryAfter: job.recoveryAfter };
      }
      if (
        job.kind === "extraction" && job.captureComplete && jobCheckpoint?.visibility === "staging"
        && jobCheckpoint.logical_delta_id === `delta:${jobCheckpoint.delta_ordinal}`
        && jobCheckpoint.next_chunk_ordinal >= MAX_GRAPH_EXTRACTION_STAGING_CHUNKS
      ) {
        const outcome = await recordStagingFailure(jobStorage, new StateTransactionError("STATE_TX_LIMIT_EXCEEDED"));
        if (outcome === "stopped") {
          logger.error("Graph extraction stopped at the staging checkpoint limit", {
            jobId: job.id,
            chunkOrdinal: jobCheckpoint.next_chunk_ordinal,
          });
          return { success: true, recovered: false, jobId: job.id, stopped: true };
        }
      }
      if (!job.captureComplete) {
        if (control.recovery) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const captureStorage = new GraphJobStorage(base, storage.guard, job.id);
        await captureStorage.restore();
        const delta: GraphDeltaPreparation = { id: "capture", ordinal: 0, capturedAt: job.createdAt, attempt: "capture", phase: "preparing" };
        await captureStorage.stage(KV.graphJobs, job.id, { ...job, state: "invalidated", failureCode: "STATE_GRAPH_RECOVERY_REQUIRED" }, delta);
        await captureStorage.completeStaging(delta);
        return { success: false, recovered: false, jobId: job.id, error: "Incomplete graph capture aborted before effects; retry with the complete source request" };
      }
      handler = handlers.get(base)?.get(job.kind);
      if (!handler) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      input = await readCapture(storage, job);
    } finally {
      // Reconstruction must finish releasing its fence before the handler
      // acquires a new lease and independently validates the durable request.
      await base.lease({ action: "release", ...storage.guard });
    }
    await handler(input, job.id);
    return { success: true, recovered: true, jobId: job.id };
  });
}

export async function withCompletedGraphRead<T>(kv: StateKV, read: () => Promise<T>): Promise<T> {
  const base = baseKV(kv);
  const before = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
  const context = execution.getStore();
  if (context?.storage.kv === base) return read();
  if (before?.recovery || (before?.lease && before.lease.expires_at_ms > Date.now())) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  const result = await read();
  const after = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
  if (after?.recovery || (after?.lease && after.lease.expires_at_ms > Date.now()) || before?.generation !== after?.generation || before?.fence !== after?.fence) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  return result;
}
