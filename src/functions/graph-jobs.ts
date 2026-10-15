import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { generateId, isGuardedGraphRecord, KV } from "../state/schema.js";
import { logger } from "../logger.js";
import type { CompressedObservation, GraphControlState, GraphExtractionJob } from "../types.js";
import { isStateCounter, StateTransactionError, type StateGraphGuard, type StateGraphLease, type StateJsonValue } from "../state/state-transactions.js";
import { StatePageError } from "../state/state-pages.js";
import { withGraphLeaseAdmission } from "../state/batch-effects.js";
import { GraphJobStorage, graphJson, graphRecordDigest, type GraphDeltaPreparation } from "./graph-job-storage.js";
import { readGraphSessionCapture } from "./graph-session-capture.js";

const LEASE_TTL_MS = 120_000;
const CAPTURE_FRAGMENT_CHARS = 128 * 1024;
const MAX_GRAPH_EXTRACTION_STAGING_CHUNKS = 100_000;
const MAX_GRAPH_EXTRACTION_RECOVERY_FAILURES = 3;
const GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS = 30_000;
const MAX_GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS = 15 * 60_000;
interface DurableGraphJob extends Omit<GraphExtractionJob, "logicalDeltaCount"> {
  selectedOrigin?: SelectedGraphOrigin;
  logicalDeltaCount?: number;
  captureParts: number;
  captureDigest: string;
  captureComplete: boolean;
  result?: StateJsonValue;
  recoveryDeltaId?: string;
  recoveryAttempts?: number;
  recoveryAfter?: string | null;
  recoveryStopped?: boolean;
  continuationFromOrdinal?: number;
  continuationState?: StateJsonValue;
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
const localRecovery = new AsyncLocalStorage<boolean>();
const providerContinuation = new AsyncLocalStorage<{ jobId: string; ordinal: number }>();
const EXTERNAL_FROZEN_VALUES = new Set(["compaction", "input-units", "pipeline-gate", "provider-response"]);
const isExternalFrozenValue = (name: string) => EXTERNAL_FROZEN_VALUES.has(name)
  || name.startsWith("project-work-response:v3:") || name.startsWith("queued:");
class GraphProviderOutcomeUnknown extends StateTransactionError {
  constructor() { super("STATE_GRAPH_RECOVERY_REQUIRED"); this.name = "GraphProviderOutcomeUnknown"; }
}
interface UnknownCompactionRepeat {
  jobId: string; jobVersion: string; generation: string; checkpointVersion: string;
  chunkOrdinal: number; deltaOrdinal: 3; valueName: "compaction"; acknowledgeUnknown: true;
  effectId: string; maxAttempts: 1;
  provider: { endpoint: "https://api.typesafe.ai/v1/systemone"; model: "jev-latest"; timeoutMs: number; maxStateChars: number };
}
interface UnknownCompactionReceipt {
  version: 1; state: "intended" | "completed"; effectId: string; acknowledgedUnknown: true;
  generation: string; checkpointVersion: string; chunkOrdinal: number; jobVersion: string;
  maxAttempts: 1; provider: UnknownCompactionRepeat["provider"]; reference?: FrozenValue; consumed?: number;
}
type UnknownCompactionFetchPermit = (provider: { endpoint: string; model: string; timeoutMs: number; maxStateChars: number }) => void;
const unknownCompactionRepeat = new AsyncLocalStorage<{ request: UnknownCompactionRepeat; admitted: boolean; permit?: UnknownCompactionFetchPermit }>();
interface ProviderResponseAdmission {
  jobId: string; jobVersion: string; generation: string; checkpointVersion: string; chunkOrdinal: number;
  deltaOrdinal: number; valueName: "provider-response"; intentVersion: "1"; intentId: string; effectId: string; maxAttempts: 1;
  provider: { endpoint: string; model: string; timeoutMs: number; maxRequestBytes: number; maxResponseBytes: number; maxOutputTokens: number };
}
interface ProviderResponseReceipt { version: 1; state: "intended" | "completed"; effectId: string; intentId: string; reference?: FrozenValue; consumed?: number }
const providerResponseAdmission = new AsyncLocalStorage<{ request: ProviderResponseAdmission; admitted: boolean; admit?: () => Promise<void> }>();
class GraphProviderResponseStopped extends GraphProviderOutcomeUnknown {
  constructor(readonly effectId: string, readonly outcome: "completed" | "unknown" | "rejected", readonly causeDiagnostic?: { code: string; errorClass: string }) { super(); }
}
export function graphStrictProviderResponse(): { provider: ProviderResponseAdmission["provider"]; admit: () => Promise<void> } | undefined {
  const scope = providerResponseAdmission.getStore();
  if (!scope?.admitted) return undefined;
  if (!scope.admit) throw new GraphProviderOutcomeUnknown();
  return { provider: scope.request.provider, admit: scope.admit };
}
function validateProviderResponseAdmission(request: ProviderResponseAdmission): void {
  if (!request || Object.keys(request).sort().join() !== "checkpointVersion,chunkOrdinal,deltaOrdinal,effectId,generation,intentId,intentVersion,jobId,jobVersion,maxAttempts,provider,valueName"
    || request.jobId !== "graphjob_ffac2cbe8d51901d0314683be6f04dacc9163834e06726df4fdfebccdad50e80"
    || request.jobVersion !== "2" || request.generation !== "1" || !isStateCounter(request.checkpointVersion)
    || !Number.isSafeInteger(request.chunkOrdinal) || request.chunkOrdinal < 1 || ![5, 8, 11, 14, 17].includes(request.deltaOrdinal) || request.valueName !== "provider-response"
    || request.intentVersion !== "1" || request.maxAttempts !== 1
    || ![request.intentId, request.effectId].every(value => typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value))
    || (request.deltaOrdinal === 5 && (request.intentId !== "0c552e8c-f311-47f1-afaa-21bbff8bf84e" || request.effectId !== "2e00cd32-7c8a-4447-bf65-6f0c5550e816"))
    || !request.provider || Object.keys(request.provider).sort().join() !== "endpoint,maxOutputTokens,maxRequestBytes,maxResponseBytes,model,timeoutMs"
    || request.provider.endpoint !== "https://llm.safeblocklab.com/v1/chat/completions" || request.provider.model !== "nemotron-3.5-lightning-30b-a3b"
    || request.provider.timeoutMs !== 30000 || request.provider.maxRequestBytes !== 98304 || request.provider.maxResponseBytes !== 65536 || request.provider.maxOutputTokens !== 512) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
}
async function assertProviderResponseSnapshot(storage: GraphJobStorage, job: DurableGraphJob, request: ProviderResponseAdmission): Promise<void> {
  const row = await storage.kv.getVersioned<DurableGraphJob>(KV.graphJobs, job.id, storage.guard), checkpoint = storage.checkpoint;
  if (job.id !== request.jobId || row.version !== request.jobVersion || job.generation !== request.generation || storage.guard.generation !== request.generation
    || job.kind !== "extraction" || job.state !== "staging" || !job.captureComplete || job.recoveryStopped
    || storage.checkpointVersion !== request.checkpointVersion || checkpoint?.visibility !== "staging" || checkpoint.logical_delta_id !== `delta:${request.deltaOrdinal}`
    || checkpoint.delta_ordinal !== request.deltaOrdinal || checkpoint.next_chunk_ordinal !== request.chunkOrdinal) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  const scope = KV.graphProviderResults(job.id);
  const intent = await storage.kv.getVersioned<{ version: number; state: string; id: string }>(scope, `provider-intent:${request.deltaOrdinal}:provider-response`, storage.guard);
  const value = await storage.kv.getVersioned(scope, `value:${request.deltaOrdinal}:provider-response`, storage.guard);
  const slot = await storage.kv.getVersioned(scope, `provider-response-admission:${request.deltaOrdinal}`, storage.guard);
  const compaction = await storage.get<UnknownCompactionReceipt>(scope, "repeat-intent:3:compaction"), captured = await storage.get<FrozenValue>(scope, "value:3:compaction");
  if (intent.version !== request.intentVersion || intent.value?.version !== 1 || intent.value.state !== "not-dispatched" || intent.value.id !== request.intentId
    || value.exists || value.version !== "0" || slot.exists || slot.version !== "0" || compaction?.state !== "completed" || compaction.consumed !== 1
    || compaction.effectId !== "ea0c2c5d-2749-440a-9e6b-abf37eeef0e6" || !captured || JSON.stringify(compaction.reference) !== JSON.stringify(captured)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  const units = await storage.kv.getVersioned<FrozenValue>(scope, "value:4:input-units", storage.guard);
  const unitIndex = (request.deltaOrdinal - 5) / 3;
  if (!units.exists || units.version !== "1" || units.value?.parts !== 2 || units.value.failure || !Number.isSafeInteger(unitIndex) || unitIndex < 1 || unitIndex > 4) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  const inputUnits = await readFrozenValue<unknown[]>(storage, scope, "value:4:input-units", units.value);
  if (!Array.isArray(inputUnits) || inputUnits.length !== 5) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  const first = await storage.get<ProviderResponseReceipt>(scope, "provider-response-admission:5"), firstValue = await storage.get<FrozenValue>(scope, "value:5:provider-response");
  if (first?.state !== "completed" || first.effectId !== "2e00cd32-7c8a-4447-bf65-6f0c5550e816" || first.consumed !== 1 || !firstValue || JSON.stringify(first.reference) !== JSON.stringify(firstValue)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
}
class GraphUnknownCompactionStopped extends GraphProviderOutcomeUnknown {
  constructor(readonly effectId: string, readonly outcome: "completed" | "unknown" | "rejected", readonly causeDiagnostic?: { code: string; errorClass: string }) { super(); }
}
export function graphUnknownCompactionRepeatProvider(actual?: { typeSafe: boolean; compaction: boolean; runtimeEnabled: boolean; timeoutMs: number; maxStateChars: number; runtimeMaxStateChars: number }): UnknownCompactionRepeat["provider"] | undefined {
  const repeat = unknownCompactionRepeat.getStore();
  if (repeat?.admitted && actual && (!actual.typeSafe || !actual.compaction || !actual.runtimeEnabled
    || actual.timeoutMs !== repeat.request.provider.timeoutMs || actual.maxStateChars !== repeat.request.provider.maxStateChars
    || actual.runtimeMaxStateChars !== repeat.request.provider.maxStateChars)) {
    throw new GraphUnknownCompactionStopped(repeat.request.effectId, "rejected", { code: "STATE_TX_REPLAY_CONFLICT", errorClass: "StateTransactionError" });
  }
  return repeat?.admitted ? repeat.request.provider : undefined;
}
export function graphUnknownCompactionRepeatFetchPermit(): UnknownCompactionFetchPermit | undefined {
  return unknownCompactionRepeat.getStore()?.permit;
}
function validateUnknownCompactionRepeat(value: UnknownCompactionRepeat): void {
  const keys = ["jobId", "jobVersion", "generation", "checkpointVersion", "chunkOrdinal", "deltaOrdinal", "valueName", "acknowledgeUnknown", "effectId", "maxAttempts", "provider"];
  if (!value || Object.keys(value).sort().join() !== keys.sort().join() || typeof value.jobId !== "string" || !/^graphjob_[a-z0-9_]+$/.test(value.jobId)
    || ![value.jobVersion, value.generation, value.checkpointVersion].every(isStateCounter) || !Number.isSafeInteger(value.chunkOrdinal) || value.chunkOrdinal < 1
    || value.deltaOrdinal !== 3 || value.valueName !== "compaction" || value.acknowledgeUnknown !== true
    || typeof value.effectId !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.effectId) || value.maxAttempts !== 1
    || !value.provider || Object.keys(value.provider).sort().join() !== "endpoint,maxStateChars,model,timeoutMs"
    || value.provider.endpoint !== "https://api.typesafe.ai/v1/systemone" || value.provider.model !== "jev-latest"
    || !Number.isSafeInteger(value.provider.timeoutMs) || value.provider.timeoutMs < 1 || value.provider.timeoutMs > 30_000
    || !Number.isSafeInteger(value.provider.maxStateChars) || value.provider.maxStateChars < 256 || value.provider.maxStateChars > 64_000) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
}
async function assertUnknownCompactionSnapshot(storage: GraphJobStorage, job: DurableGraphJob, request: UnknownCompactionRepeat): Promise<void> {
  const row = await storage.kv.getVersioned<DurableGraphJob>(KV.graphJobs, job.id, storage.guard);
  const checkpoint = storage.checkpoint;
  if (request.jobId !== job.id || !row.exists || row.version !== request.jobVersion || job.kind !== "extraction" || !job.captureComplete || job.state !== "staging"
    || job.recoveryStopped || job.generation !== request.generation || storage.guard.generation !== request.generation
    || storage.checkpointVersion !== request.checkpointVersion || checkpoint?.visibility !== "staging"
    || checkpoint.logical_delta_id !== "delta:3" || checkpoint.delta_ordinal !== 3 || checkpoint.next_chunk_ordinal !== request.chunkOrdinal) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  for (const key of ["value:3:compaction", "provider-intent:3:compaction", "repeat-intent:3:compaction"]) {
    const saved = await storage.kv.getVersioned(KV.graphProviderResults(job.id), key, storage.guard);
    if (saved.exists || saved.version !== "0") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  }
}
export interface SelectedGraphOrigin {
  project: string; sessionId: string; sourceFingerprint: string; intentId?: string; jobId?: string;
}
const selectedOrigin = new AsyncLocalStorage<SelectedGraphOrigin | undefined>();
export function withSelectedGraphOrigin<T>(control: SelectedGraphOrigin | undefined, run: () => Promise<T>): Promise<T> {
  if (!control) return selectedOrigin.run(undefined, run);
  const origin: SelectedGraphOrigin = { project: control.project, sessionId: control.sessionId, sourceFingerprint: control.sourceFingerprint,
    ...(control.intentId !== undefined ? { intentId: control.intentId } : {}), ...(control.jobId !== undefined ? { jobId: control.jobId } : {}) };
  if (Object.values(origin).some(value => typeof value !== "string" || !value.trim() || value.length > 4096)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  return selectedOrigin.run(Object.freeze(origin), run);
}
class GraphSelectedForegroundRequired extends StateTransactionError {
  constructor() { super("STATE_GRAPH_RECOVERY_REQUIRED"); this.name = "GraphSelectedForegroundRequired"; }
}
function assertGraphOrigin(job: Pick<DurableGraphJob, "selectedOrigin" | "state">): void {
  const current = selectedOrigin.getStore();
  if (job.selectedOrigin) {
    if (!current) throw new GraphSelectedForegroundRequired();
    if ((["project", "sessionId", "sourceFingerprint", "intentId", "jobId"] as const).some(key =>
      job.selectedOrigin?.[key] !== current[key])) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  } else if (current && job.state !== "completed" && job.state !== "invalidated") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
}
const localRecoveryBudget = new AsyncLocalStorage<{ deadline: number; deltas: number }>();
class GraphProviderRecoveryRequired extends Error {
  constructor() { super("Graph recovery requires an uncaptured provider operation"); this.name = "GraphProviderRecoveryRequired"; }
}
class GraphLocalRecoveryDeferred extends Error {
  constructor() { super("Local graph recovery reached its bounded delta budget"); this.name = "GraphLocalRecoveryDeferred"; }
}
class GraphSourceRecoveryRequired extends Error {
  constructor() { super("Graph recovery requires an unavailable captured source"); this.name = "GraphSourceRecoveryRequired"; }
}
class GraphDiscoveryDeferred extends Error {}
interface GraphActivePointer { jobId: string | null; generation: string; fence: string; baselineComplete: boolean; revision?: string; revisionGeneration?: string }
interface GraphDiscoveryCursor { generation: string; revision: string; cursor: string | null }
const ACTIVE_JOB_KEY = "graph-dispatch:active";
const DISCOVERY_KEY = "graph-dispatch:discovery";
class GraphCaptureUnavailableError extends StateTransactionError {
  constructor(readonly jobId: string) {
    super("STATE_GRAPH_RECOVERY_REQUIRED");
    this.name = "GraphCaptureUnavailableError";
  }
}
const LOCAL_FROZEN_VALUES = new Set(["options", "heuristic", "parsed-batch", "parsed-response"]);
const LOCAL_FROZEN_VALUE_PREFIXES = ["project-work-parsed:v3:", "temporal-repair-parsed:v1"];
const originals = new WeakMap<StateKV, StateKV>();
const handlers = new WeakMap<StateKV, Map<GraphExtractionJob["kind"], GraphHandler>>();
const graphJobQueues = new WeakMap<StateKV, Promise<void>>();
const baseKV = (kv: StateKV): StateKV => originals.get(kv) ?? kv;

async function acquireGraphJobQueue(kv: StateKV): Promise<() => void> {
  const previous = graphJobQueues.get(kv);
  let release!: () => void;
  const current = new Promise<void>((resolve) => { release = resolve; });
  graphJobQueues.set(kv, current);
  if (previous) await previous;
  return () => {
    release();
    if (graphJobQueues.get(kv) === current) graphJobQueues.delete(kv);
  };
}

export function graphCapturedAt(): string { return execution.getStore()?.delta?.capturedAt ?? new Date().toISOString(); }
export function graphJobId(): string | undefined { return execution.getStore()?.job.id; }
export interface GraphJobYield<T> { readonly __graphJobYield: "continue"; readonly progress: T }
export function yieldGraphJob<T>(progress: T): GraphJobYield<T> { return { __graphJobYield: "continue", progress }; }
export function isGraphJobYield(value: unknown): value is GraphJobYield<unknown> {
  return typeof value === "object" && value !== null && (value as { __graphJobYield?: unknown }).__graphJobYield === "continue";
}
function isTerminalGraphWorkFailure(value: unknown): boolean {
  return typeof value === "object" && value !== null && (value as { terminalFailure?: unknown }).terminalFailure === true;
}
function isVersion2GroupedGraphInput(value: unknown): boolean {
  if (typeof value !== "object" || value === null) return false;
  const input = value as { graphProvenanceVersion?: unknown; graphWorkGroupsVersion?: unknown };
  return input.graphProvenanceVersion === 2 && input.graphWorkGroupsVersion === 2;
}
export async function readGraphJobContinuation<T>(kv: StateKV): Promise<T | undefined> {
  const context = execution.getStore();
  if (!context || context.storage.kv !== baseKV(kv)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  const job = await context.storage.get<DurableGraphJob>(KV.graphJobs, context.job.id);
  return job?.continuationState === undefined ? undefined : structuredClone(job.continuationState as T);
}
export async function writeGraphJobContinuation<T>(kv: StateKV, value: T): Promise<number>;
export async function writeGraphJobContinuation<T>(kv: StateKV, value: undefined, forNextDelta: (nextDeltaOrdinal: number) => T): Promise<number>;
export async function writeGraphJobContinuation<T>(kv: StateKV, value: T | undefined, forNextDelta?: (nextDeltaOrdinal: number) => T): Promise<number> {
  const context = execution.getStore();
  if (!context || context.storage.kv !== baseKV(kv)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  let nextDeltaOrdinal = 0;
  await withGraphDelta(kv, async () => {
    const active = execution.getStore();
    if (!active?.delta) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const job = await active.storage.get<DurableGraphJob>(KV.graphJobs, active.job.id);
    if (!job || job.id !== active.job.id || job.generation !== active.job.generation) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    nextDeltaOrdinal = graphJobNextDeltaOrdinal(kv);
    const continuationState = graphJson(forNextDelta ? forNextDelta(nextDeltaOrdinal) : value);
    await active.storage.facade(active.delta).set(KV.graphJobs, job.id, {
      ...job, state: "staging", continuationState, updatedAt: active.delta.capturedAt,
    });
  });
  return nextDeltaOrdinal;
}
export function graphJobNextDeltaOrdinal(kv: StateKV): number {
  const context = execution.getStore();
  if (!context || context.storage.kv !== baseKV(kv)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  return nativeDeltaOrdinal(context.job, context.cursor);
}
export function hasGraphDelta(): boolean { return execution.getStore()?.delta !== undefined; }
export function withLocalGraphRecovery<T>(localOnly: boolean, run: () => Promise<T>): Promise<T> {
  return localRecovery.run(localOnly, () => localOnly
    ? localRecoveryBudget.run({ deadline: Date.now() + 30_000, deltas: 256 }, run)
    : run());
}
function assertLocalRecoveryDeadline(): void {
  const budget = localRecoveryBudget.getStore();
  if (budget && Date.now() >= budget.deadline) throw new GraphLocalRecoveryDeferred();
}
export function graphResult<T>(result: StateJsonValue | undefined): T {
  return structuredClone((result as { value?: T } | undefined)?.value) as T;
}

export type GraphJobReadOnlyStorage = Pick<GraphJobStorage, "get">;
export type GraphJobPreflight<T> = (storage: GraphJobReadOnlyStorage) => Promise<T | null>;

export interface CompletedGraphJobReplayRequest {
  jobId: string;
  values: readonly { ordinal: number; name: string }[];
  deltas: readonly number[];
  callbackKeys?: readonly string[];
}

export interface CompletedGraphJobReplay {
  job: { id: string; kind: GraphExtractionJob["kind"]; generation: string; logicalDeltaCount: number; result: unknown };
  input: unknown;
  values: Array<{ ordinal: number; name: string; value: unknown }>;
  deltas: Array<{ ordinal: number; manifest: GraphDeltaPreparation; result: unknown }>;
  callbacks: Array<{ key: string; receipt: unknown }>;
}

const MAX_COMPLETED_REPLAY_VALUES = 4;
const MAX_COMPLETED_REPLAY_DELTAS = 2;
const MAX_COMPLETED_REPLAY_CALLBACKS = 2;

function validateCompletedReplayRequest(request: CompletedGraphJobReplayRequest): void {
  if (!request || typeof request.jobId !== "string" || !/^graphjob_[a-z0-9_]+$/.test(request.jobId)
    || !Array.isArray(request.values) || request.values.length > MAX_COMPLETED_REPLAY_VALUES
    || !Array.isArray(request.deltas) || request.deltas.length > MAX_COMPLETED_REPLAY_DELTAS
    || request.callbackKeys !== undefined && (!Array.isArray(request.callbackKeys) || request.callbackKeys.length > MAX_COMPLETED_REPLAY_CALLBACKS)) {
    throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  }
  const valueKeys = new Set<string>();
  for (const value of request.values) {
    if (!value || !Number.isSafeInteger(value.ordinal) || value.ordinal < 1 || value.ordinal > 1_000_000
      || typeof value.name !== "string" || !/^[a-zA-Z0-9:_-]{1,128}$/.test(value.name)) {
      throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    }
    const key = `${value.ordinal}:${value.name}`;
    if (valueKeys.has(key)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    valueKeys.add(key);
  }
  if (new Set(request.deltas).size !== request.deltas.length
    || request.deltas.some((ordinal) => !Number.isSafeInteger(ordinal) || ordinal < 1 || ordinal > 1_000_000)) {
    throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  }
  const callbackKeys = request.callbackKeys ?? [];
  if (new Set(callbackKeys).size !== callbackKeys.length || callbackKeys.some((key) => !/^[a-f0-9]{64}$/.test(key))) {
    throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  }
}

function isFrozenValue(value: unknown): value is FrozenValue {
  if (!value || typeof value !== "object") return false;
  const record = value as Partial<FrozenValue>;
  return Number.isSafeInteger(record.parts) && (record.parts ?? 0) > 0 && (record.parts ?? 0) <= 512
    && typeof record.digest === "string" && /^[a-f0-9]{64}$/.test(record.digest) && record.failure !== true;
}

/** Read only immutable values from a completed graph job under a graph lease. */
export async function readCompletedGraphJobReplay(
  kv: StateKV,
  request: CompletedGraphJobReplayRequest,
): Promise<CompletedGraphJobReplay> {
  validateCompletedReplayRequest(request);
  const base = baseKV(kv);
  return withGraphLeaseAdmission(async () => {
    const initial = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
    if (!initial || initial.recovery) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    const lease = await base.lease({ action: "acquire", owner_id: randomUUID(), generation: initial.generation, ttl_ms: LEASE_TTL_MS }) as StateGraphLease;
    const storage = new GraphJobStorage(base, { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }, request.jobId);
    try {
      const control = await leasedControl(storage);
      if (control.recovery) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      await storage.restore();
      const job = await storage.get<DurableGraphJob>(KV.graphJobs, request.jobId);
      const logicalDeltaCount = job?.logicalDeltaCount;
      if (!job || job.version !== 1 || job.id !== request.jobId || job.state !== "completed"
        || job.generation !== control.generation || job.kind !== "temporal"
        || typeof logicalDeltaCount !== "number" || !Number.isSafeInteger(logicalDeltaCount) || logicalDeltaCount < 1) {
        throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      }
      assertGraphOrigin(job);
      const input = await readCapture(storage, job);
      const checkpoint = storage.checkpoint;
      if (!checkpoint || checkpoint.generation !== job.generation || checkpoint.job_id !== job.id
        || checkpoint.visibility !== "complete" || checkpoint.delta_ordinal + 1 !== logicalDeltaCount) {
        throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      }

      const values: CompletedGraphJobReplay["values"] = [];
      for (const value of request.values) {
        if (value.ordinal >= logicalDeltaCount) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const scope = KV.graphProviderResults(job.id);
        const key = `value:${value.ordinal}:${value.name}`;
        const reference = await storage.get<FrozenValue>(scope, key);
        if (!isFrozenValue(reference)) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        values.push({ ...value, value: await readFrozenValue(storage, scope, key, reference) });
      }

      const deltas: CompletedGraphJobReplay["deltas"] = [];
      for (const ordinal of request.deltas) {
        if (ordinal >= logicalDeltaCount) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const manifest = await storage.get<GraphDeltaPreparation>(KV.graphDeltas(job.id), `manifest:${ordinal}`);
        if (!manifest || manifest.id !== `delta:${ordinal}` || manifest.ordinal !== ordinal
          || !["freezing", "prepared", "completed"].includes(manifest.phase)
          || !Number.isSafeInteger(manifest.templateCount) || (manifest.templateCount ?? 0) < 1
          || !Number.isSafeInteger(manifest.applyOrdinal) || (manifest.applyOrdinal ?? 0) < 1
          || manifest.result === undefined) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const applied = checkpoint.delta_ordinal > ordinal
          || checkpoint.delta_ordinal === ordinal && checkpoint.visibility === "complete"
            && checkpoint.next_chunk_ordinal >= manifest.applyOrdinal! + manifest.templateCount!;
        if (!applied) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        deltas.push({ ordinal, manifest, result: graphResult(manifest.result) });
      }

      const callbacks: CompletedGraphJobReplay["callbacks"] = [];
      for (const key of request.callbackKeys ?? []) {
        callbacks.push({ key, receipt: await storage.get(KV.batchCallbacks, `graph:${key}`) });
      }
      const after = await leasedControl(storage);
      if (after.recovery || after.generation !== control.generation || after.fence !== control.fence) {
        throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      }
      return {
        job: { id: job.id, kind: job.kind, generation: job.generation, logicalDeltaCount, result: graphResult(job.result) },
        input, values, deltas, callbacks,
      };
    } finally {
      await base.lease({ action: "release", ...storage.guard });
    }
  });
}

export function graphTransactionFailure(error: unknown): boolean {
  if (error instanceof GraphProviderOutcomeUnknown || error instanceof GraphProviderRecoveryRequired || error instanceof GraphLocalRecoveryDeferred || error instanceof GraphSourceRecoveryRequired) return true;
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

function nativeDeltaOrdinal(job: DurableGraphJob, logicalOrdinal: number): number {
  return logicalOrdinal + (job.continuationFromOrdinal !== undefined && logicalOrdinal >= job.continuationFromOrdinal ? 1 : 0);
}

export async function withGraphDelta<T>(kv: StateKV, run: () => Promise<T>, advanceGeneration = false): Promise<T> {
  const context = execution.getStore();
  if (!context || context.storage.kv !== baseKV(kv)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  if (context.delta) return run();
  const budget = localRecoveryBudget.getStore();
  if (budget && Date.now() >= budget.deadline) throw new GraphLocalRecoveryDeferred();
  const ordinal = nativeDeltaOrdinal(context.job, context.cursor++);
  const scope = KV.graphDeltas(context.job.id);
  const saved = await context.storage.get<GraphDeltaPreparation>(scope, `manifest:${ordinal}`);
  const checkpoint = context.storage.checkpoint;
  if (saved && checkpoint && (checkpoint.delta_ordinal > ordinal || (checkpoint.delta_ordinal === ordinal && checkpoint.visibility === "complete"))) return graphResult<T>(saved.result);
  if (budget && budget.deltas-- <= 0) throw new GraphLocalRecoveryDeferred();
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

export async function freezeGraphInputUnits<T>(kv: StateKV, capturedLocalCompactor: boolean, create: () => Promise<T> | T): Promise<T> {
  return freezeValue(kv, "input-units", create, false, capturedLocalCompactor === false);
}

export async function freezeGraphHandledValue<T>(kv: StateKV, name: string, create: () => Promise<T> | T): Promise<T> {
  return freezeValue(kv, name, create, true);
}

async function freezeValue<T>(kv: StateKV, name: string, create: () => Promise<T> | T, handleFailure: boolean, localInputUnits = false): Promise<T> {
  const context = execution.getStore();
  if (!context) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  const priorCheckpoint = context.storage.checkpoint;
  const reference = await withGraphDelta(kv, async () => {
    const current = execution.getStore();
    if (!current?.delta) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const scope = KV.graphProviderResults(context.job.id), key = `value:${current.delta.ordinal}:${name}`;
    const saved = await context.storage.get<FrozenValue>(scope, key);
    if (saved) return { scope, key, ...saved };
    const responseAdmission = providerResponseAdmission.getStore();
    if (name === "provider-response" && responseAdmission?.admitted && current.delta.ordinal === responseAdmission.request.deltaOrdinal) {
      await captureAdmittedProviderResponse(context.storage, current.delta, responseAdmission.request, create);
    }
    const repeat = unknownCompactionRepeat.getStore();
    if (name === "compaction" && current.delta.ordinal === 3) {
      const priorRepeat = await context.storage.get<UnknownCompactionReceipt>(scope, "repeat-intent:3:compaction");
      if (priorRepeat) throw new GraphUnknownCompactionStopped(priorRepeat.effectId, "unknown");
      if (repeat?.admitted && repeat.request.jobId === context.job.id) {
        await repeatUnknownCompaction(context.storage, current.delta, repeat.request, create);
      }
    }
    const localInput = name === "input-units" && localInputUnits;
    const external = isExternalFrozenValue(name);
    const intentKey = `provider-intent:${current.delta.ordinal}:${name}`;
    const intent = external ? await context.storage.get<{ version: number; state: string; id: string }>(scope, intentKey) : null;
    const proof = providerContinuation.getStore();
    const provenUndispatched = proof?.jobId === context.job.id && proof.ordinal === current.delta.ordinal;
    const legacyPending = priorCheckpoint?.logical_delta_id === current.delta.id && priorCheckpoint.visibility === "staging";
    if (intent && (intent.version !== 1 || typeof intent.id !== "string" || !["not-dispatched", "intended"].includes(intent.state))) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    if (external && (intent?.state === "intended" || !intent && legacyPending && !provenUndispatched && !localInput)) throw new GraphProviderOutcomeUnknown();
    if (localRecovery.getStore() && !localInput && !LOCAL_FROZEN_VALUES.has(name)
      && !LOCAL_FROZEN_VALUE_PREFIXES.some((prefix) => name.startsWith(prefix))) {
      if (external && !intent) await context.storage.stage(scope, intentKey, { version: 1, state: "not-dispatched", id: randomUUID() }, current.delta);
      throw new GraphProviderRecoveryRequired();
    }
    if (external && !localInput) await context.storage.stage(scope, intentKey, {
      version: 1, state: "intended", id: intent?.id ?? randomUUID(),
    }, current.delta);
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

async function captureAdmittedProviderResponse<T>(storage: GraphJobStorage, delta: GraphDeltaPreparation, request: ProviderResponseAdmission, create: () => Promise<T> | T): Promise<never> {
  const scope = KV.graphProviderResults(storage.jobId), key = `value:${request.deltaOrdinal}:provider-response`, slotKey = `provider-response-admission:${request.deltaOrdinal}`, intentKey = `provider-intent:${request.deltaOrdinal}:provider-response`;
  const active = providerResponseAdmission.getStore();
  if (!active?.admitted || active.request !== request || storage.jobId !== request.jobId) throw new GraphProviderOutcomeUnknown();
  let admissionAttempted = false, consumed = false, closed = false;
  const receipt: ProviderResponseReceipt = { version: 1, state: "intended", effectId: request.effectId, intentId: request.intentId };
  active.admit = async () => {
    storage.assertLease();
    if (closed || admissionAttempted) throw new GraphProviderOutcomeUnknown();
    const intent = await storage.kv.getVersioned<{ version: number; state: string; id: string }>(scope, intentKey, storage.guard);
    const slot = await storage.kv.getVersioned(scope, slotKey, storage.guard), value = await storage.kv.getVersioned(scope, key, storage.guard);
    if (intent.version !== request.intentVersion || intent.value?.version !== 1 || intent.value.state !== "not-dispatched" || intent.value.id !== request.intentId
      || slot.exists || slot.version !== "0" || value.exists || value.version !== "0") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    const prepared = storage.prepared([
      { type: "check", scope: KV.graphJobs, key: storage.jobId, expected_version: request.jobVersion },
      { type: "check", scope, key, expected_version: "0" },
      { type: "check", scope, key: "value:4:input-units", expected_version: "1" },
      { type: "set", scope, key: intentKey, expected_version: request.intentVersion, value: graphJson({ ...intent.value, state: "intended" }) },
      { type: "set", scope, key: slotKey, expected_version: "0", value: graphJson(receipt) },
    ], delta, "staging");
    admissionAttempted = true;
    const deadline = Date.now() + 3000, ack = await storage.kv.commitBatch(storage.guard, prepared);
    if (closed || Date.now() >= deadline) throw new GraphProviderOutcomeUnknown();
    storage.checkpointVersion = ack.checkpoint_version;
    storage.checkpoint = JSON.parse(prepared.payload_json).checkpoint;
    storage.assertLease();
    consumed = true;
  };
  try {
    let value: T;
    try { value = await create(); }
    finally { closed = true; active.admit = undefined; }
    if (!consumed) throw new GraphProviderOutcomeUnknown();
    await writeFrozenValue(storage, scope, key, value, delta, false, async (reference) => {
      const slot = await storage.kv.getVersioned<ProviderResponseReceipt>(scope, slotKey, storage.guard), frozen = await storage.kv.getVersioned(scope, key, storage.guard);
      if (slot.value?.effectId !== request.effectId || slot.value.state !== "intended" || frozen.exists) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const prepared = storage.prepared([
        { type: "check", scope: KV.graphJobs, key: storage.jobId, expected_version: request.jobVersion },
        { type: "set", scope, key, expected_version: frozen.version, value: graphJson(reference) },
        { type: "set", scope, key: slotKey, expected_version: slot.version, value: graphJson({ ...receipt, state: "completed", reference, consumed: 1 }) },
      ], delta, "staging");
      await storage.kv.commitBatch(storage.guard, prepared);
    });
  } catch (error) {
    throw new GraphProviderResponseStopped(request.effectId, admissionAttempted ? "unknown" : "rejected", graphFailureDiagnostic(error));
  } finally { closed = true; active.admit = undefined; }
  throw new GraphProviderResponseStopped(request.effectId, "completed");
}

async function repeatUnknownCompaction<T>(storage: GraphJobStorage, delta: GraphDeltaPreparation, request: UnknownCompactionRepeat, create: () => Promise<T> | T): Promise<never> {
  const scope = KV.graphProviderResults(storage.jobId), key = "value:3:compaction", receiptKey = "repeat-intent:3:compaction";
  const receipt: UnknownCompactionReceipt = { version: 1, state: "intended", effectId: request.effectId, acknowledgedUnknown: true,
    generation: request.generation, checkpointVersion: request.checkpointVersion, chunkOrdinal: request.chunkOrdinal,
    jobVersion: request.jobVersion, maxAttempts: 1, provider: request.provider };
  const repeat = unknownCompactionRepeat.getStore();
  if (!repeat?.admitted || repeat.request !== request) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  let consumed = 0, closed = false;
  try {
    const slot = await storage.kv.getVersioned(scope, receiptKey, storage.guard);
    if (slot.exists || slot.version !== "0") throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
    const intent = storage.prepared([
      { type: "check", scope: KV.graphJobs, key: storage.jobId, expected_version: request.jobVersion },
      { type: "set", scope, key: receiptKey, expected_version: slot.version, value: graphJson(receipt) },
    ], delta, "staging");
    const ackDeadline = Date.now() + 3000;
    // Only this first commit ACK admits a fetch; storage.stage retries and reconciliation cannot mint permission.
    const ack = await storage.kv.commitBatch(storage.guard, intent);
    if (Date.now() >= ackDeadline) throw new GraphProviderOutcomeUnknown();
    storage.checkpointVersion = ack.checkpoint_version;
    storage.checkpoint = JSON.parse(intent.payload_json).checkpoint;
    const deadline = Date.now() + request.provider.timeoutMs;
    const approvedProvider = Object.freeze({ ...request.provider });
    repeat.permit = (provider) => {
      storage.assertLease();
      if (closed || consumed !== 0 || Date.now() >= deadline
        || provider.endpoint !== approvedProvider.endpoint || provider.model !== approvedProvider.model
        || provider.timeoutMs !== approvedProvider.timeoutMs || provider.maxStateChars !== approvedProvider.maxStateChars) throw new GraphProviderOutcomeUnknown();
      consumed = 1;
    };
    let value: T;
    try { value = await create(); }
    finally { closed = true; repeat.permit = undefined; }
    await writeFrozenValue(storage, scope, key, value, delta, false, async (reference) => {
      const slot = await storage.kv.getVersioned<UnknownCompactionReceipt>(scope, receiptKey, storage.guard);
      const frozen = await storage.kv.getVersioned(scope, key, storage.guard);
      if (slot.value?.effectId !== request.effectId || slot.value.state !== "intended" || frozen.exists) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const prepared = storage.prepared([
        { type: "check", scope: KV.graphJobs, key: storage.jobId, expected_version: request.jobVersion },
        { type: "set", scope, key, expected_version: frozen.version, value: graphJson(reference) },
        { type: "set", scope, key: receiptKey, expected_version: slot.version, value: graphJson({ ...receipt, state: "completed", reference, consumed }) },
      ], delta, "staging");
      await storage.kv.commitBatch(storage.guard, prepared);
    });
  } catch (error) {
    // No recovery write/retry can turn an uncertain admission, provider or commit ACK into permission to dispatch again.
    const { code, errorClass } = graphFailureDiagnostic(error);
    throw new GraphUnknownCompactionStopped(request.effectId, "unknown", { code, errorClass });
  } finally {
    closed = true;
    repeat.permit = undefined;
  }
  throw new GraphUnknownCompactionStopped(request.effectId, "completed");
}

async function writeFrozenValue(storage: GraphJobStorage, scope: string, key: string, value: unknown, delta: GraphDeltaPreparation, failure = false, complete?: (reference: FrozenValue) => Promise<void>): Promise<FrozenValue> {
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
  if (complete) await complete(reference);
  else await storage.stage(scope, key, reference, delta);
  return reference;
}

async function readFrozenValue<T>(storage: CaptureReader, scope: string, key: string, reference: FrozenValue): Promise<T> {
  if (!Number.isSafeInteger(reference.parts) || reference.parts < 1 || reference.parts > 512) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
  let text = "", bytes = 0;
  for (let ordinal = 0; ordinal < reference.parts; ordinal++) {
    assertLocalRecoveryDeadline();
    const part = await storage.get<string>(scope, `${key}:part:${ordinal}`);
    if (typeof part !== "string") throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    bytes += Buffer.byteLength(part, "utf8");
    if (bytes > 64 * 1024 * 1024) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
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
    const source = await context.storage.get<{ count: number; complete: boolean; rowPrefix?: string }>(scope, key);
    if (source?.complete) return { key, count: source.count, ...(source.rowPrefix ? { rowPrefix: source.rowPrefix } : {}) };

    if (localRecovery.getStore()) throw new GraphSourceRecoveryRequired();

    const { observations, ...captureMetrics } = await readGraphSessionCapture(context.storage.kv, sessionId);
    logger.info("Graph session source captured consistently", { jobId: context.job.id, ...captureMetrics });
    // Interrupted attempts have their own namespace. A complete reference is
    // published only after every row, so recovery never joins source revisions.
    const rowPrefix = `${key}:attempt:${randomUUID()}`;
    for (let ordinal = 0; ordinal < observations.length; ordinal++) {
      await writeFrozenValue(context.storage, scope, `${rowPrefix}:row:${ordinal}`, observations[ordinal], delta);
    }
    await context.storage.stage(scope, key, { count: observations.length, complete: true, rowPrefix }, delta);
    return { key, count: observations.length, rowPrefix };
  });
  const observations: CompressedObservation[] = [];
  if (!Number.isSafeInteger(reference.count) || reference.count < 0 || reference.count > 100_000) throw new GraphSourceRecoveryRequired();
  let bytes = 0;
  for (let ordinal = 0; ordinal < reference.count; ordinal++) {
    assertLocalRecoveryDeadline();
    const key = `${reference.rowPrefix ?? reference.key}:row:${ordinal}`;
    const row = await context.storage.get<FrozenValue>(scope, key);
    if (!row) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    const observation = await readFrozenValue<CompressedObservation>(context.storage, scope, key, row);
    bytes += Buffer.byteLength(JSON.stringify(observation), "utf8");
    if (bytes > 64 * 1024 * 1024) throw new GraphSourceRecoveryRequired();
    observations.push(observation);
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
  if (!Number.isSafeInteger(job.captureParts) || job.captureParts < 1 || job.captureParts > 512 || typeof job.captureDigest !== "string" || !/^[a-f0-9]{64}$/.test(job.captureDigest)) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
  let text = "", bytes = 0;
  for (let ordinal = 0; ordinal < job.captureParts; ordinal++) {
    assertLocalRecoveryDeadline();
    const part = await storage.get<string>(KV.graphInputs(job.id), `capture:${ordinal}`);
    if (part === null) throw new GraphCaptureUnavailableError(job.id);
    if (typeof part !== "string") throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    bytes += Buffer.byteLength(part, "utf8");
    if (bytes > 64 * 1024 * 1024) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
    text += part;
  }
  if (graphRecordDigest(text) !== job.captureDigest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
  return JSON.parse(text) as unknown;
}

async function pendingJob(kv: StateKV, generation?: string, migrate = false, fence = "0"): Promise<DurableGraphJob | null> {
  const pointer = await kv.get<GraphActivePointer>(KV.config, ACTIVE_JOB_KEY);
  if (pointer?.jobId) {
    const job = await kv.get<DurableGraphJob>(KV.graphJobs, pointer.jobId);
    if (!job) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    if (job.state !== "completed" && job.state !== "invalidated" && job.recoveryStopped !== true) {
      assertGraphOrigin(job);
      if (generation && job.generation !== generation) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
      return job;
    }
  }
  if (pointer?.baselineComplete) {
    const current = await kv.scopeRevision(KV.graphJobs);
    if (pointer.revision === current.revision && pointer.revisionGeneration === current.generation) return null;
  }
  if (!migrate || !generation) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  const revision = await kv.scopeRevision(KV.graphJobs);
  const saved = await kv.get<GraphDiscoveryCursor>(KV.config, DISCOVERY_KEY);
  const cursor = saved?.generation === revision.generation && saved.revision === revision.revision ? saved.cursor : null;
  const pages = kv.pages<Pick<DurableGraphJob, "id" | "state" | "generation" | "recoveryStopped">>(KV.graphJobs, {
    ...(cursor ? { cursor } : {}), limit: 128, maxBytes: 128 * 1024, fields: ["id", "state", "generation", "recoveryStopped"],
  });
  try {
    const next = await pages.next();
    if (next.done) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    const after = await kv.scopeRevision(KV.graphJobs);
    if (revision.generation !== after.generation || revision.revision !== after.revision) {
      await kv.set(KV.config, DISCOVERY_KEY, { ...after, cursor: null });
      throw new GraphDiscoveryDeferred();
    }
    for (const candidate of next.value.items) {
      if (candidate.generation !== generation || candidate.state === "completed" || candidate.state === "invalidated" || candidate.recoveryStopped === true) continue;
      const job = await kv.get<DurableGraphJob>(KV.graphJobs, candidate.id);
      if (!job) throw new GraphDiscoveryDeferred();
      assertGraphOrigin(job);
      await kv.set(KV.config, ACTIVE_JOB_KEY, { jobId: job.id, generation, fence, baselineComplete: false } satisfies GraphActivePointer);
      return job;
    }
    await kv.set(KV.config, DISCOVERY_KEY, { ...revision, cursor: next.value.next_cursor });
    if (next.value.next_cursor) throw new GraphDiscoveryDeferred();
    await kv.set(KV.config, ACTIVE_JOB_KEY, { jobId: null, generation, fence, baselineComplete: true,
      revision: revision.revision, revisionGeneration: revision.generation } satisfies GraphActivePointer);
    return null;
  } catch (error) {
    if (!(error instanceof StatePageError) || error.code !== "STATE_PAGE_CURSOR_STALE") throw error;
    await kv.set(KV.config, DISCOVERY_KEY, { ...revision, cursor: null });
    throw new GraphDiscoveryDeferred();
  } finally { await pages.return(undefined); }
}

export async function graphDispatchBlocker(kv: StateKV): Promise<DurableGraphJob | null> {
  const base = baseKV(kv);
  const control = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
  if (!control) throw new StateTransactionError("STATE_TX_UNSUPPORTED");
  if (control.recovery) {
    const job = await base.get<DurableGraphJob>(KV.graphJobs, control.recovery.job_id);
    if (!job) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
    return job;
  }
  if (selectedOrigin.getStore()) return null;
  return pendingJob(base, control.generation);
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

function graphFailureDiagnostic(error: unknown): { code: string; errorClass: string; errorCategory?: string; sourceFrames: string[] } {
  const names = ["Error", "TypeError", "RangeError", "ReferenceError", "SyntaxError", "DOMException", "StateTransactionError", "StatePageError", "IndexedRetrievalError", "InvocationError", "GraphProviderRecoveryRequired"];
  const candidate = error instanceof Error ? error.constructor.name : "unknown";
  const errorClass = names.includes(candidate) ? candidate : "unknown";
  const record = typeof error === "object" && error !== null ? error as { code?: unknown } : null;
  const knownCodes = ["TIMEOUT", "invocation_failed", "function_not_found", "STATE_INDEX_NOT_READY", "STATE_RETRIEVAL_RESOURCE_LIMIT", "STATE_GRAPH_RECOVERY_REQUIRED", "STATE_TX_INVALID_REQUEST"];
  const code = terminalPayloadFailureCode(error)
    ?? (error instanceof StateTransactionError || error instanceof StatePageError ? error.code
      : typeof record?.code === "string" && knownCodes.includes(record.code) ? record.code : "GRAPH_EXECUTION_FAILED");
  const categories: Record<string, string> = {
    "Graph snapshot read failed; refusing to continue": "snapshot-read",
    "Graph snapshot is malformed; refusing to continue": "snapshot-invalid",
    "Invalid batch effect receipt metadata": "batch-receipt-invalid",
    "Batch effect receipt capacity exhausted": "batch-receipt-limit",
    "A batch graph application must be recovered first": "batch-graph-recovery",
    "A batch callback must be recovered first": "batch-callback-recovery",
    "Ambiguous batch callback admission state": "batch-admission-invalid",
    "Ambiguous batch callback receipt state": "batch-receipt-state-invalid",
    "Graph job configuration changed; restore the captured provider/compaction configuration before recovery": "job-configuration-changed",
  };
  const message = error instanceof Error ? error.message : "";
  const errorCategory = Object.hasOwn(categories, message) ? categories[message]
    : /^Graph snapshot projection is \d{1,16} bytes, over the \d{1,16}-byte cache limit$/.test(message) ? "snapshot-size-limit" : undefined;
  const sourceFrames = error instanceof Error && typeof error.stack === "string"
    ? [...error.stack.matchAll(/[\\/](graph(?:-[a-z-]+)?|state-transactions|state-pages|indexed-retrieval|index|(?:src|schema|keyed-mutex)-[A-Za-z0-9_-]{8})\.(?:m?js|ts):(\d+):(\d+)/g)]
      .slice(0, 4).map((match) => `${match[1]}:${match[2]}:${match[3]}`)
    : [];
  return { code, errorClass, ...(errorCategory ? { errorCategory } : {}), sourceFrames };
}

async function recordStagingFailure(storage: GraphJobStorage, error: unknown): Promise<"retry-scheduled" | "stopped" | false> {
  const permanentFailureCode = terminalPayloadFailureCode(error);
  const checkpoint = storage.checkpoint;
  if (!checkpoint || checkpoint.visibility !== "staging" || checkpoint.logical_delta_id !== `delta:${checkpoint.delta_ordinal}`) return false;
  const job = await storage.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
  if (!job || job.kind !== "extraction" || !job.captureComplete || job.generation !== checkpoint.generation || job.state === "completed" || job.state === "invalidated") return false;
  const continuationState = job.continuationState as { version?: unknown } | undefined;
  if (continuationState?.version === 2) {
    const manifest = await storage.get<GraphDeltaPreparation>(KV.graphDeltas(job.id), `manifest:${checkpoint.delta_ordinal}`);
    if (manifest && (manifest.phase !== "preparing" || manifest.templateCount !== undefined || manifest.applyOrdinal !== undefined)) return false;
  }

  const recoveryAttempts = job.recoveryDeltaId === checkpoint.logical_delta_id
    && Number.isSafeInteger(job.recoveryAttempts) && (job.recoveryAttempts ?? 0) >= 0
    ? job.recoveryAttempts! + 1
    : 1;
  const stopped = permanentFailureCode !== undefined || recoveryAttempts >= MAX_GRAPH_EXTRACTION_RECOVERY_FAILURES;
  const failureCode = graphFailureDiagnostic(error).code;
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

async function discoverJob(storage: GraphJobStorage, control: GraphControlState, migrate = false): Promise<DurableGraphJob | null> {
  if (!control.recovery) {
    if (selectedOrigin.getStore()) return null;
    return pendingJob(storage.kv, control.generation, migrate, storage.guard.fence);
  }
  const checkpoint = control.recovery;
  if (checkpoint.generation !== control.generation || !checkpoint.job_id) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
  const job = await storage.get<DurableGraphJob>(KV.graphJobs, checkpoint.job_id);
  if (!job || job.version !== 1 || job.id !== checkpoint.job_id || !["queued", "staging", "applying", "failed"].includes(job.state) || typeof job.kind !== "string" || typeof job.createdAt !== "string" || typeof job.captureComplete !== "boolean") throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  if (job.generation !== control.generation) throw new StateTransactionError("STATE_TX_GENERATION_STALE");
  assertGraphOrigin(job);
  const saved = await storage.get<GraphControlState["recovery"]>(KV.graphCheckpoints, job.id);
  if (!saved || saved.generation !== checkpoint.generation || saved.job_id !== checkpoint.job_id || saved.logical_delta_id !== checkpoint.logical_delta_id || saved.delta_ordinal !== checkpoint.delta_ordinal || saved.next_chunk_ordinal !== checkpoint.next_chunk_ordinal || saved.visibility !== checkpoint.visibility) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  return job;
}

export async function runGraphJob<T>(kv: StateKV, kind: GraphExtractionJob["kind"], input: unknown, run: (frozen: unknown) => Promise<T>, durableId?: string, preflight?: GraphJobPreflight<T>): Promise<T> {
  const base = baseKV(kv), nested = execution.getStore(), origin = selectedOrigin.getStore();
  if (nested?.storage.kv === base) return run(input);
  // Admit before queueing so maintenance cannot own a lock needed by queued work.
  const admitted = await withGraphLeaseAdmission(async () => {
    const releaseQueue = await acquireGraphJobQueue(base);
    try {
      const control = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
      if (!control) throw new StateTransactionError("STATE_TX_UNSUPPORTED");
      const lease = await base.lease({ action: "acquire", owner_id: randomUUID(), generation: control.generation, ttl_ms: LEASE_TTL_MS }) as StateGraphLease;
      return { control, lease, releaseQueue };
    } catch (error) {
      releaseQueue();
      throw error;
    }
  });
  try {
    let { control } = admitted;
    const { lease } = admitted;
    const storage = new GraphJobStorage(base, { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }, durableId ?? generateId("graphjob"));
    let renewal: Promise<void> | undefined;
    let renewalFailed = false;
    let renewalFailure: unknown;
    let renewalGuard: StateGraphGuard | undefined;
    let expectedFinalizedRenewalFailure = false;
    const renewLease = (): Promise<void> => {
      if (renewal) return renewal;
      if (renewalFailed) return Promise.reject(renewalFailure);
      const renewing = { ...storage.guard };
      renewalGuard = renewing;
      const pending: Promise<void> = Promise.resolve()
        .then(() => base.lease({ action: "renew", ...renewing, ttl_ms: LEASE_TTL_MS }))
        .then(() => undefined)
        .catch((error) => {
          const current = storage.guard;
          if (current.owner_id !== renewing.owner_id || current.generation !== renewing.generation || current.fence !== renewing.fence) {
            logger.warn("Graph job lease renewal failed after guard replacement", { jobId: storage.jobId, code: error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED" });
            return;
          }
          renewalFailed = true;
          renewalFailure = error;
          storage.failLease(error, renewing);
          logger.error("Graph job lease renewal failed", { jobId: storage.jobId, code: error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED" });
          throw error;
        });
      renewal = pending;
      void pending.then(
        () => { if (renewal === pending) renewal = undefined; },
        () => { if (renewal === pending) renewal = undefined; },
      );
      return pending;
    };
    const heartbeat = setInterval(() => {
      void renewLease().catch(() => undefined);
    }, LEASE_TTL_MS / 3);
    heartbeat.unref();
    let recoveryDeferred = false;
    let operationFailed = false;
    let operationFailure: unknown;
    try {
      control = await leasedControl(storage);
      const pending = await discoverJob(storage, control, true);
      if (pending && pending.id !== storage.jobId) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      const existing = await storage.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
      if (existing) assertGraphOrigin(existing);
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
      const repeat = unknownCompactionRepeat.getStore();
      const response = providerResponseAdmission.getStore();
      const repeatHold = await storage.get<UnknownCompactionReceipt>(KV.graphProviderResults(storage.jobId), "repeat-intent:3:compaction");
      if (repeatHold && !repeat && !localRecovery.getStore()) throw new GraphProviderOutcomeUnknown();
      if (repeat) {
        try {
          if (!existing) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
          await assertUnknownCompactionSnapshot(storage, existing, repeat.request);
        }
        catch (error) {
          const { code, errorClass } = graphFailureDiagnostic(error);
          throw new GraphUnknownCompactionStopped(repeat.request.effectId, "rejected", { code, errorClass });
        }
        repeat.admitted = true;
      }
      if (response) {
        try {
          if (!existing) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
          await assertProviderResponseSnapshot(storage, existing, response.request);
        } catch (error) { throw new GraphProviderResponseStopped(response.request.effectId, "rejected", graphFailureDiagnostic(error)); }
        response.admitted = true;
      }
      if (!origin) {
        const pointer = await base.get<GraphActivePointer>(KV.config, ACTIVE_JOB_KEY);
        await base.set(KV.config, ACTIVE_JOB_KEY, { jobId: storage.jobId, generation: control.generation,
          fence: storage.guard.fence, baselineComplete: pointer?.baselineComplete === true } satisfies GraphActivePointer);
      }
      const now = new Date().toISOString();
      const job: DurableGraphJob = existing ?? { version: 1, id: storage.jobId, generation: control.generation, kind, state: "staging", createdAt: now, updatedAt: now, inputCount: 1, captureParts: 0, captureDigest: "", captureComplete: false,
        ...(selectedOrigin.getStore() ? { selectedOrigin: selectedOrigin.getStore() } : {}) };
      if (!job.captureComplete) {
        if (existing && graphRecordDigest(JSON.stringify(input)) !== existing.captureDigest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
        await capture(storage, job, input);
      }
      if (storage.checkpoint?.logical_delta_id === "capture" && storage.checkpoint.visibility !== "complete") {
        await storage.completeStaging({ id: "capture", ordinal: 0, capturedAt: job.createdAt, attempt: "capture", phase: "preparing" });
      }
      const frozen = await readCapture(storage, job);
      const continuation = existing?.continuationState as { version?: unknown; activeUnit?: { resultOrdinal?: unknown } } | undefined;
      const resumeOrdinal = continuation?.version === 2
        ? Number.isSafeInteger(continuation.activeUnit?.resultOrdinal)
          ? continuation.activeUnit!.resultOrdinal as number
          : Math.max(existing?.logicalDeltaCount ?? 1, (storage.checkpoint?.delta_ordinal ?? 0) + 1)
        : 1;
      const context: GraphExecution = { storage, job, cursor: resumeOrdinal };
      const executed = await execution.run(context, () => run(frozen));
      const yielded = isGraphJobYield(executed);
      const result = yielded ? executed.progress : executed;
      const terminalWorkFailure = !yielded && isTerminalGraphWorkFailure(result) && kind === "extraction"
        && isVersion2GroupedGraphInput(frozen);
      clearInterval(heartbeat);
      const latestControl = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
      const generationAdvanced = latestControl !== null && latestControl.generation !== job.generation;
      const expectedStaleRenewal = (error: unknown): boolean => generationAdvanced
        && renewalGuard?.owner_id === lease.owner_id
        && renewalGuard.generation === lease.generation
        && renewalGuard.fence === lease.fence
        && error instanceof StateTransactionError
        && ["STATE_TX_FENCED", "STATE_TX_GENERATION_STALE"].includes(error.code);
      if (renewal) {
        try { await renewal; }
        catch (error) { if (!expectedStaleRenewal(error)) throw error; }
      }
      if (renewalFailed) {
        if (!expectedStaleRenewal(renewalFailure)) throw renewalFailure;
        expectedFinalizedRenewalFailure = true;
      }
      if (generationAdvanced) return executed;
      storage.assertLease();
      await execution.run(context, () => withGraphDelta(kv, async () => {
        const active = execution.getStore();
        if (!active?.delta) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
        const latest = await storage.get<DurableGraphJob>(KV.graphJobs, job.id);
        if (!latest || latest.generation !== job.generation) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const groupedContinuation = latest.continuationState as { version?: unknown } | undefined;
        const terminalGroupedWorkFailure = terminalWorkFailure && groupedContinuation?.version === 2;
        await storage.facade(active.delta).set(KV.graphJobs, job.id, {
          ...latest, state: yielded ? "staging" : terminalGroupedWorkFailure ? "failed" : "completed",
          ...(terminalGroupedWorkFailure ? { recoveryStopped: true, recoveryAfter: null, failureCode: "GRAPH_WORK_UNIT_FAILED" } : {}),
          logicalDeltaCount: nativeDeltaOrdinal(latest, context.cursor), updatedAt: active.delta.capturedAt,
          result: graphJson({ value: result }),
        });
        return result;
      }));
      return (yielded ? executed : result) as T;
    } catch (error) {
      operationFailed = true;
      operationFailure = error;
      storage.discardPendingShadows();
      const failedCheckpoint = storage.checkpoint;
      let recoveryOutcome: "retry-scheduled" | "stopped" | "invalidated" | false = false;
      const checkpoint = storage.checkpoint;
      const descriptor = typeof input === "object" && input !== null ? input as { sessionId?: unknown; observations?: unknown } : null;
      if (error instanceof StatePageError && error.code === "STATE_PAGE_CURSOR_STALE" && kind === "extraction" && typeof descriptor?.sessionId === "string" && descriptor.observations === undefined && checkpoint?.logical_delta_id === "delta:1" && checkpoint.delta_ordinal === 1 && checkpoint.visibility === "staging") {
        const current = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
        const admitted = await storage.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
        if (!current?.recovery && admitted) {
          // Cursor epochs change on native restart. An unfinished initial source
          // capture has no graph effects and cannot adopt a changed source.
          const delta: GraphDeltaPreparation = { id: checkpoint.logical_delta_id, ordinal: checkpoint.delta_ordinal, capturedAt: admitted.createdAt, attempt: "capture-abort", phase: "preparing" };
          await storage.stage(KV.graphJobs, storage.jobId, { ...admitted, state: "invalidated", failureCode: error.code, recoveryAfter: null, recoveryStopped: true, updatedAt: new Date().toISOString() }, delta);
          await storage.completeStaging(delta);
          recoveryOutcome = "invalidated";
        }
      }
      if (!recoveryOutcome && !recoveryDeferred && !(error instanceof GraphProviderRecoveryRequired) && !(error instanceof GraphLocalRecoveryDeferred) && !(error instanceof GraphSourceRecoveryRequired) && !(error instanceof GraphProviderOutcomeUnknown)) {
        try {
          recoveryOutcome = await recordStagingFailure(storage, error);
        } catch (terminalizeError) {
          logger.error("Graph job permanent failure could not be terminalized", {
            jobId: storage.jobId,
            code: terminalizeError instanceof StateTransactionError ? terminalizeError.code : "STATE_TX_FAILED",
          });
        }
      }
      const diagnostic = { jobId: storage.jobId, ...graphFailureDiagnostic(error), ...(error instanceof StateTransactionError ? error.diagnostic : undefined), deltaOrdinal: failedCheckpoint?.delta_ordinal, chunkOrdinal: failedCheckpoint?.next_chunk_ordinal, visibility: failedCheckpoint?.visibility };
      if (error instanceof GraphProviderRecoveryRequired) logger.info("Local graph recovery paused before provider operation", diagnostic);
      else if (recoveryOutcome === "invalidated") logger.warn("Initial graph source capture invalidated; retry with a fresh complete request", diagnostic);
      else if (recoveryOutcome === "stopped") logger.error("Graph job stopped after repeated or permanent staging failure", diagnostic);
      else if (recoveryOutcome === "retry-scheduled") logger.warn("Graph job retry scheduled with bounded backoff", diagnostic);
      else logger.error("Graph job paused for recovery", diagnostic);
      throw error;
    } finally {
      clearInterval(heartbeat);
      let finalizationFailed = false;
      let finalizationError: unknown;
      if (renewal) {
        try { await renewal; }
        catch (error) { finalizationFailed = true; finalizationError = error; }
      }
      if (!origin) {
        try {
          const pointer = await base.get<GraphActivePointer>(KV.config, ACTIVE_JOB_KEY);
          if (pointer?.jobId === storage.jobId && pointer.fence === storage.guard.fence && pointer.baselineComplete) {
            const job = await base.get<DurableGraphJob>(KV.graphJobs, storage.jobId);
            if (job?.state === "completed") {
              const revision = await base.scopeRevision(KV.graphJobs);
              const current = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
              if (current?.lease?.fence === storage.guard.fence && current.generation === storage.guard.generation && current.lease.expires_at_ms > Date.now()) {
                await base.set(KV.config, ACTIVE_JOB_KEY, { ...pointer, revision: revision.revision, revisionGeneration: revision.generation });
              }
            }
          }
        } catch (error) {
          logger.warn("Graph active-job certification deferred", { jobId: storage.jobId, code: graphFailureDiagnostic(error).code });
        }
      }
      try { await base.lease({ action: "release", ...storage.guard }); }
      catch (error) {
        if (!(error instanceof StateTransactionError) || !["STATE_TX_FENCED", "STATE_TX_GENERATION_STALE"].includes(error.code)) {
          logger.warn("Graph job lease release failed", { jobId: storage.jobId, code: error instanceof StateTransactionError ? error.code : "STATE_TX_FAILED" });
          if (!finalizationFailed) { finalizationFailed = true; finalizationError = error; }
        }
      }
      if (renewalFailed && !expectedFinalizedRenewalFailure) {
        if (!operationFailed) throw renewalFailure;
        if (operationFailure !== renewalFailure) logger.error("Graph job lease renewal also failed during finalization", { jobId: storage.jobId, code: renewalFailure instanceof StateTransactionError ? renewalFailure.code : "STATE_TX_FAILED" });
      }
      if (finalizationFailed) {
        if (!operationFailed) throw finalizationError;
        logger.error("Graph job lease finalization also failed", { jobId: storage.jobId, code: finalizationError instanceof StateTransactionError ? finalizationError.code : "STATE_TX_FAILED" });
      }
    }
  } finally {
    admitted.releaseQueue();
  }
}

export function registerGraphJobHandler(kv: StateKV, kind: GraphExtractionJob["kind"], handler: GraphHandler): void {
  const base = baseKV(kv), registry = handlers.get(base) ?? new Map();
  registry.set(kind, handler); handlers.set(base, registry);
}

interface StoppedGraphContinuation {
  jobId: string;
  generation: string;
  checkpointVersion: string;
  chunkOrdinal: number;
}

async function continueStoppedJob(base: StateKV, discovery: GraphJobStorage, control: GraphControlState, expected: StoppedGraphContinuation): Promise<DurableGraphJob> {
  if (control.recovery || control.generation !== expected.generation || await pendingJob(base, control.generation)) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  const storage = new GraphJobStorage(base, discovery.guard, expected.jobId);
  await storage.restore();
  const checkpoint = storage.checkpoint;
  const job = await storage.get<DurableGraphJob>(KV.graphJobs, expected.jobId);
  if (job) assertGraphOrigin(job);
  if (!job || job.id !== expected.jobId || job.generation !== expected.generation || job.kind !== "extraction" || job.state !== "failed" || job.recoveryStopped !== true || job.captureComplete !== true || job.continuationFromOrdinal !== undefined
    || !checkpoint || checkpoint.visibility !== "complete" || checkpoint.generation !== expected.generation || checkpoint.logical_delta_id !== `delta:${checkpoint.delta_ordinal}` || checkpoint.next_chunk_ordinal !== expected.chunkOrdinal || storage.checkpointVersion !== expected.checkpointVersion || checkpoint.delta_ordinal < 1) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  await readCapture(storage, job);
  const manifest = await storage.get<GraphDeltaPreparation>(KV.graphDeltas(job.id), `manifest:${checkpoint.delta_ordinal}`);
  if (!manifest || manifest.id !== checkpoint.logical_delta_id || manifest.ordinal !== checkpoint.delta_ordinal || manifest.phase !== "preparing" || manifest.advanceGeneration !== false || manifest.templateCount !== undefined || manifest.applyOrdinal !== undefined || manifest.result !== undefined) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  const continued: DurableGraphJob = { ...job, state: "staging", recoveryStopped: false, recoveryAfter: null, continuationFromOrdinal: checkpoint.delta_ordinal };
  const next: GraphDeltaPreparation = { ...nextDelta(checkpoint.delta_ordinal + 1), capturedAt: manifest.capturedAt, advanceGeneration: false };
  // Terminal delta stays immutable; its original logical operation continues
  // at the next native ordinal while its captured prefix is reused.
  storage.checkpoint = null;
  await storage.stage(KV.graphJobs, job.id, continued, next);
  await storage.stage(KV.graphDeltas(job.id), `manifest:${next.ordinal}`, next, next);
  return continued;
}

async function stopMissingCapture(storage: GraphJobStorage, job: DurableGraphJob): Promise<void> {
  const control = await leasedControl(storage);
  const checkpoint = storage.checkpoint;
  if (control.recovery || !checkpoint || checkpoint.visibility !== "staging" || checkpoint.generation !== job.generation || checkpoint.logical_delta_id !== `delta:${checkpoint.delta_ordinal}` || job.generation !== storage.guard.generation) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  const manifest = await storage.get<GraphDeltaPreparation>(KV.graphDeltas(job.id), `manifest:${checkpoint.delta_ordinal}`);
  if (!manifest || manifest.id !== checkpoint.logical_delta_id || manifest.ordinal !== checkpoint.delta_ordinal || manifest.phase !== "preparing" || manifest.advanceGeneration !== false || manifest.templateCount !== undefined || manifest.applyOrdinal !== undefined || manifest.applyCheckpointVersion !== undefined || manifest.result !== undefined) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
  const delta: GraphDeltaPreparation = { ...manifest, attempt: "capture-unavailable" };
  await storage.stage(KV.graphJobs, job.id, {
    ...job, state: "failed", recoveryStopped: true, recoveryAfter: null,
    captureComplete: false, failureCode: "GRAPH_CAPTURE_UNAVAILABLE", updatedAt: new Date().toISOString(),
  }, delta);
  await storage.completeStaging(delta);
}

export function registerGraphJobRecovery(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::graph-recover", async (request?: { localOnly?: boolean; continuation?: StoppedGraphContinuation; providerContinuation?: StoppedGraphContinuation; repeatUnknown?: UnknownCompactionRepeat; providerResponse?: ProviderResponseAdmission }) => {
    let recoveryPhase = "validation";
    try {
    if (request?.localOnly !== undefined && typeof request.localOnly !== "boolean") throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const expected = request?.continuation;
    const providerProof = request?.providerContinuation;
    const repeat = request?.repeatUnknown === undefined ? undefined : structuredClone(request.repeatUnknown);
    const response = request?.providerResponse === undefined ? undefined : structuredClone(request.providerResponse);
    if (response) {
      validateProviderResponseAdmission(response);
      if (request?.localOnly !== true || expected !== undefined || providerProof !== undefined || repeat !== undefined) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
      Object.freeze(response.provider); Object.freeze(response);
    }
    if (repeat !== undefined) {
      validateUnknownCompactionRepeat(repeat);
      if (request?.localOnly !== true || expected !== undefined || providerProof !== undefined) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
      Object.freeze(repeat.provider);
      Object.freeze(repeat);
    }
    if (providerProof !== undefined && (request?.localOnly !== false || typeof providerProof !== "object" || providerProof === null
      || Object.keys(providerProof).some(key => !["jobId", "generation", "checkpointVersion", "chunkOrdinal"].includes(key))
      || typeof providerProof.jobId !== "string" || !isStateCounter(providerProof.generation) || !isStateCounter(providerProof.checkpointVersion)
      || !Number.isSafeInteger(providerProof.chunkOrdinal) || providerProof.chunkOrdinal < 1)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    if (expected !== undefined && (request?.localOnly !== true || typeof expected !== "object" || expected === null || Object.keys(expected).some(key => !["jobId", "generation", "checkpointVersion", "chunkOrdinal"].includes(key)) || typeof expected.jobId !== "string" || !/^graphjob_[a-z0-9_]+$/.test(expected.jobId) || !isStateCounter(expected.generation) || !isStateCounter(expected.checkpointVersion) || !Number.isSafeInteger(expected.chunkOrdinal) || expected.chunkOrdinal < 1)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const base = baseKV(kv);
    recoveryPhase = "admission";
    const initial = (await base.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
    if (!initial) {
      if (await pendingJob(base)) throw new StateTransactionError("STATE_TX_UNSUPPORTED");
      return { success: true, recovered: false };
    }
    const lease = await withGraphLeaseAdmission(() => base.lease({ action: "acquire", owner_id: randomUUID(), generation: initial.generation, ttl_ms: LEASE_TTL_MS })) as StateGraphLease;
    const storage = new GraphJobStorage(base, { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence }, initial.recovery?.job_id ?? "recovery-discovery");
    let job: DurableGraphJob | null = null;
    let input: unknown;
    let handler: GraphHandler | undefined;
    let providerProofOrdinal: number | undefined;
    try {
      const control = await leasedControl(storage);
      recoveryPhase = "discovery";
      job = expected ? await continueStoppedJob(base, storage, control, expected) : await discoverJob(storage, control, true);
      if (!job) return { success: true, recovered: false };
      const jobStorage = new GraphJobStorage(base, storage.guard, job.id);
      await jobStorage.restore();
      const jobCheckpoint = jobStorage.checkpoint;
      if (response) {
        if (response.jobId !== job.id) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
        const slot = await jobStorage.get<ProviderResponseReceipt>(KV.graphProviderResults(job.id), `provider-response-admission:${response.deltaOrdinal}`);
        if (slot) {
          if (slot.version !== 1 || slot.effectId !== response.effectId || !["intended", "completed"].includes(slot.state)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
          const value = await jobStorage.get<FrozenValue>(KV.graphProviderResults(job.id), `value:${response.deltaOrdinal}:provider-response`);
          const completed = slot.state === "completed" && value && JSON.stringify(value) === JSON.stringify(slot.reference);
          return { success: true, recovered: false, jobId: job.id, providerResponse: { effectId: slot.effectId, outcome: completed ? "completed" : "unknown" } };
        }
        await assertProviderResponseSnapshot(jobStorage, job, response);
      }
      if (repeat) {
        if (repeat.jobId !== job.id) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
        const receipt = await jobStorage.get<UnknownCompactionReceipt>(KV.graphProviderResults(job.id), "repeat-intent:3:compaction");
        if (receipt) {
          if (receipt.version !== 1 || receipt.effectId !== repeat.effectId || !["intended", "completed"].includes(receipt.state)) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
          const reference = await jobStorage.get<FrozenValue>(KV.graphProviderResults(job.id), "value:3:compaction");
          const completed = receipt.state === "completed" && reference && JSON.stringify(reference) === JSON.stringify(receipt.reference);
          return { success: true, recovered: false, jobId: job.id, repeatUnknown: { effectId: receipt.effectId, outcome: completed ? "completed" : "unknown" } };
        }
        await assertUnknownCompactionSnapshot(jobStorage, job, repeat);
      }
      if (providerProof && (providerProof.jobId !== job.id || providerProof.generation !== job.generation
        || providerProof.checkpointVersion !== jobStorage.checkpointVersion || providerProof.chunkOrdinal !== jobCheckpoint?.next_chunk_ordinal
        || jobCheckpoint.visibility !== "staging")) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      if (providerProof) providerProofOrdinal = jobCheckpoint!.delta_ordinal;
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
        return { success: true, recovered: false, jobId: job.id, captureUnavailable: true };
      }
      handler = handlers.get(base)?.get(job.kind);
      if (!handler) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      try { input = await readCapture(storage, job); }
      catch (error) {
        if (!(error instanceof GraphCaptureUnavailableError)) throw error;
        await stopMissingCapture(jobStorage, job);
        throw error;
      }
    } finally {
      // Reconstruction must finish releasing its fence before the handler
      // acquires a new lease and independently validates the durable request.
      await base.lease({ action: "release", ...storage.guard });
    }
    try {
      recoveryPhase = "execution";
      const recover = () => providerContinuation.run(providerProofOrdinal !== undefined ? { jobId: job!.id, ordinal: providerProofOrdinal } : undefined,
        () => withLocalGraphRecovery(request?.localOnly === true, () => handler!(input, job!.id)));
      if (repeat) await unknownCompactionRepeat.run({ request: repeat, admitted: false }, recover);
      else if (response) await providerResponseAdmission.run({ request: response, admitted: false }, recover);
      else await recover();
    } catch (error) {
      if (error instanceof GraphProviderResponseStopped) return { success: true, recovered: false, jobId: job.id,
        providerResponse: { effectId: error.effectId, outcome: error.outcome, ...(error.causeDiagnostic ? { diagnostic: error.causeDiagnostic } : {}) },
        ...(error.outcome === "unknown" ? { providerOutcomeUnknown: true } : {}) };
      if (error instanceof GraphUnknownCompactionStopped) return { success: true, recovered: false, jobId: job.id,
        repeatUnknown: { effectId: error.effectId, outcome: error.outcome, ...(error.causeDiagnostic ? { diagnostic: error.causeDiagnostic } : {}) },
        ...(error.outcome === "unknown" ? { providerOutcomeUnknown: true } : {}) };
      if (error instanceof GraphProviderOutcomeUnknown) return { success: true, recovered: false, jobId: job.id, providerRequired: true, providerOutcomeUnknown: true };
      if (error instanceof GraphProviderRecoveryRequired && request?.localOnly === true) return { success: true, recovered: false, jobId: job.id, localOnly: true, providerRequired: true };
      if (error instanceof GraphSourceRecoveryRequired && request?.localOnly === true) return { success: true, recovered: false, jobId: job.id, localOnly: true, sourceRequired: true };
      if (error instanceof GraphLocalRecoveryDeferred && request?.localOnly === true) return { success: true, recovered: false, jobId: job.id, localOnly: true, deferred: true, retryAfter: new Date(Date.now() + GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS).toISOString() };
      throw error;
    }
    return { success: true, recovered: true, jobId: job.id };
    } catch (error) {
      if (error instanceof GraphSelectedForegroundRequired) return { success: true, recovered: false, deferred: true, reason: "selected_foreground_required" };
      if (error instanceof GraphDiscoveryDeferred) return { success: true, recovered: false, deferred: true, discoveryPending: true,
        retryAfter: new Date(Date.now() + GRAPH_EXTRACTION_RECOVERY_BACKOFF_MS).toISOString() };
      if (error instanceof GraphCaptureUnavailableError) {
        logger.warn("Graph captured input is unavailable; operation cannot be resumed", { jobId: error.jobId, recoveryPhase });
        return { success: true, recovered: false, jobId: error.jobId, stopped: true, captureUnavailable: true };
      }
      logger.error("Graph recovery failed", {
        recoveryPhase,
        ...graphFailureDiagnostic(error),
        ...(error instanceof StateTransactionError ? error.diagnostic : undefined),
      });
      throw error;
    }
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


