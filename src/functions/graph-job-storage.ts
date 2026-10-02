import { Buffer } from "node:buffer";
import { createHash, randomUUID } from "node:crypto";
import type { StateKV } from "../state/kv.js";
import { isGuardedGraphRecord, KV } from "../state/schema.js";
import type { GraphControlState, GraphWorkingSnapshot, GraphSnapshot } from "../types.js";
import {
  MAX_STATE_COMMIT_OPERATIONS, prepareStateCommitBatch, STATE_COMMIT_TARGET_BYTES,
  StateTransactionError, type StateCommitOperation, type StateCounter,
  type StateGraphCheckpoint, type StateGraphGuard, type StateJsonValue,
  type StatePreparedBatch, type StateVersioned,
} from "../state/state-transactions.js";

const NATIVE_ENVELOPE_BYTES = 16 * 1024 * 1024 - 64 * 1024;
const TEMPLATE_TARGET_BYTES = 768 * 1024;
const PREPARED_FRAGMENT_CHARS = 128 * 1024;
const encodeKey = (value: unknown): string => Buffer.from(JSON.stringify(value)).toString("base64url");
const successor = (value: StateCounter, count = 1): StateCounter => {
  const next = BigInt(value) + BigInt(count);
  if (next > 18_446_744_073_709_551_615n) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
  return next.toString();
};

export interface GraphDeltaPreparation {
  id: string;
  ordinal: number;
  capturedAt: string;
  attempt: string;
  phase: "preparing" | "freezing" | "prepared" | "completed";
  templateCount?: number;
  applyOrdinal?: number;
  applyCheckpointVersion?: StateCounter;
  result?: StateJsonValue;
  advanceGeneration?: boolean;
  shadowCount?: number;
}

interface ShadowRow {
  operation: StateCommitOperation;
  exists: boolean;
  value?: StateJsonValue;
}

const shadowValue = (row: ShadowRow): StateJsonValue => row.operation.type === "set" ? row.operation.value : row.value ?? null;
interface PreparedReference { parts: number; digest: string }
function* fragments(text: string): Generator<string> {
  for (let start = 0; start < text.length;) {
    let end = Math.min(text.length, start + PREPARED_FRAGMENT_CHARS);
    if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end--;
    yield text.slice(start, end);
    start = end;
  }
}

export function graphJson(value: unknown): StateJsonValue {
  // Domain records historically contain optional undefined fields. Normalize
  // them at the persistence boundary, as the old JSON state transport did.
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
  return JSON.parse(encoded) as StateJsonValue;
}

export class GraphJobStorage {
  checkpointVersion: StateCounter = "0";
  checkpoint: StateGraphCheckpoint | null = null;
  private serial: Promise<unknown> = Promise.resolve();
  private leaseFailure: unknown;
  private accessSerial: Promise<unknown> = Promise.resolve();

  constructor(readonly kv: StateKV, readonly guard: StateGraphGuard, readonly jobId: string) {}

  failLease(error: unknown, guard: StateGraphGuard = this.guard): void {
    if (guard.owner_id === this.guard.owner_id && guard.generation === this.guard.generation && guard.fence === this.guard.fence) this.leaseFailure = error;
  }
  assertLease(): void { if (this.leaseFailure) throw this.leaseFailure; }

  private async refreshGuard(): Promise<void> {
    const control = (await this.kv.getVersioned<GraphControlState>(KV.graphControl, "current")).value;
    if (!control) throw new StateTransactionError("STATE_TX_UNSUPPORTED");
    const lease = await this.kv.lease({ action: "acquire", owner_id: randomUUID(), generation: control.generation, ttl_ms: 120_000 });
    if ("released" in lease) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    Object.assign(this.guard, { owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence });
    this.leaseFailure = undefined;
  }

  async restore(): Promise<void> {
    const saved = await this.kv.getVersioned<StateGraphCheckpoint>(KV.graphCheckpoints, this.jobId, this.guard);
    this.checkpoint = saved.value;
    this.checkpointVersion = saved.version;
  }

  private async commit(prepared: StatePreparedBatch): Promise<void> {
    this.assertLease();
    const bytes = Buffer.byteLength(JSON.stringify({ guard: this.guard, ...prepared }));
    if (bytes > NATIVE_ENVELOPE_BYTES) throw new StateTransactionError("STATE_RECORD_TOO_LARGE");
    // A lost acknowledgement is resolved only by replaying these exact bytes.
    // Never regenerate the payload after a transport failure.
    let receipt;
    try { receipt = await this.kv.commitBatch(this.guard, prepared); }
    catch (error) {
      if (!(error instanceof StateTransactionError) || error.code !== "STATE_TX_FAILED") throw error;
      try { receipt = await this.kv.commitBatch(this.guard, prepared); }
      catch (replayError) {
        if (!(replayError instanceof StateTransactionError) || !["STATE_TX_FENCED", "STATE_TX_GENERATION_STALE"].includes(replayError.code)) throw replayError;
        await this.refreshGuard();
        this.assertLease();
        // Native validates the current guard, then checks the historical exact
        // receipt. An unseen old-generation body still fails before effects.
        receipt = await this.kv.commitBatch(this.guard, prepared);
      }
    }
    this.checkpointVersion = receipt.checkpoint_version;
    this.checkpoint = JSON.parse(prepared.payload_json).checkpoint as StateGraphCheckpoint;
  }

  prepared(operations: StateCommitOperation[], delta: GraphDeltaPreparation, visibility: StateGraphCheckpoint["visibility"], ordinal = this.checkpoint?.next_chunk_ordinal ?? 0, version = this.checkpointVersion): StatePreparedBatch {
    const checkpoint: StateGraphCheckpoint = {
      generation: this.guard.generation, job_id: this.jobId, logical_delta_id: delta.id,
      delta_ordinal: delta.ordinal, next_chunk_ordinal: ordinal + 1, visibility,
    };
    const input = {
      identity: { generation: this.guard.generation, job_id: this.jobId, logical_delta_id: delta.id, chunk_ordinal: ordinal },
      expected_checkpoint_version: version, checkpoint, operations,
      ...(visibility === "complete" && delta.advanceGeneration ? { advance_generation: true as const } : {}),
    };
    let prepared = prepareStateCommitBatch(input);
    if (Buffer.byteLength(JSON.stringify({ guard: this.guard, ...prepared })) > STATE_COMMIT_TARGET_BYTES) {
      if (operations.filter((op) => op.type !== "check").length !== 1 || !operations.some((op) => op.type === "set")) {
        throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
      }
      prepared = prepareStateCommitBatch({ ...input, allow_oversized_record: true });
    }
    if (Buffer.byteLength(JSON.stringify({ guard: this.guard, ...prepared })) > NATIVE_ENVELOPE_BYTES) throw new StateTransactionError("STATE_RECORD_TOO_LARGE");
    return prepared;
  }

  stage<T>(scope: string, key: string, value: T, delta: GraphDeltaPreparation): Promise<void> {
    const run = this.serial.then(async () => {
      this.assertLease();
      const row = await this.kv.getVersioned(scope, key, this.guard);
      await this.commit(this.prepared([{ type: "set", scope, key, expected_version: row.version, value: graphJson(value) }], delta, "staging"));
    });
    this.serial = run.catch(() => undefined);
    return run;
  }

  async get<T>(scope: string, key: string): Promise<T | null> {
    this.assertLease();
    return (await this.kv.getVersioned<T>(scope, key, this.guard)).value;
  }

  private shadowKey(delta: GraphDeltaPreparation, scope: string, key: string): string {
    return `row:${delta.ordinal}:${delta.attempt}:${encodeKey([scope, key])}`;
  }

  async row(delta: GraphDeltaPreparation, scope: string, key: string): Promise<ShadowRow> {
    const saved = await this.get<ShadowRow>(KV.graphDeltas(this.jobId), this.shadowKey(delta, scope, key));
    if (saved) return saved;
    const original = await this.kv.getVersioned<StateJsonValue>(scope, key, this.guard);
    return { operation: { type: "check", scope, key, expected_version: original.version }, exists: original.exists, value: original.value };
  }

  private async remember(delta: GraphDeltaPreparation, scope: string, key: string, row: ShadowRow): Promise<void> {
    const shadowKey = this.shadowKey(delta, scope, key);
    if (!(await this.get<ShadowRow>(KV.graphDeltas(this.jobId), shadowKey))) {
      const ordinal = delta.shadowCount ?? 0;
      delta.shadowCount = ordinal + 1;
      await this.stage(KV.graphRemaps(this.jobId), `row-order:${delta.ordinal}:${delta.attempt}:${ordinal}`, shadowKey, delta);
    }
    await this.stage(KV.graphDeltas(this.jobId), shadowKey, row, delta);
  }

  private access<T>(run: () => Promise<T>): Promise<T> {
    const pending = this.accessSerial.then(run);
    this.accessSerial = pending.catch(() => undefined);
    return pending;
  }

  facade(delta: GraphDeltaPreparation): StateKV {
    const storage = this;
    return new Proxy(this.kv, {
      get(target, property) {
        if (property === "get") return (scope: string, key: string) => storage.access(async () => {
          if (!isGuardedGraphRecord(scope, key)) return target.get(scope, key);
          const row = await storage.row(delta, scope, key);
          await storage.remember(delta, scope, key, row);
          return structuredClone(shadowValue(row));
        });
        if (property === "set" || property === "delete") return (scope: string, key: string, value?: unknown) => storage.access(async () => {
          if (!isGuardedGraphRecord(scope, key)) return property === "set" ? target.set(scope, key, value) : target.delete(scope, key);
          const previous = await storage.row(delta, scope, key);
          const next = property === "set" ? graphJson(value) : null;
          await storage.remember(delta, scope, key, {
            operation: property === "set" ? { ...previous.operation, type: "set", value: next } : { type: "delete", scope, key, expected_version: previous.operation.expected_version },
            exists: property === "set",
          });
          return structuredClone(next);
        });
        if (property === "update") return async (scope: string, key: string, operations: unknown[]) => {
          if (isGuardedGraphRecord(scope, key)) throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
          return target.update(scope, key, operations as Parameters<StateKV["update"]>[2]);
        };
        const value: unknown = Reflect.get(target, property, target);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
  }

  async workingSnapshot(delta: GraphDeltaPreparation): Promise<StateCommitOperation> {
    const row = await this.row(delta, KV.graphSnapshot, "current");
    if (row.operation.type === "delete") throw new StateTransactionError("STATE_TX_INVALID_REQUEST");
    const wrapper: GraphWorkingSnapshot = {
      version: 1, generation: this.guard.generation, jobId: this.jobId,
      logicalDeltaId: delta.id, snapshotExists: row.exists, snapshot: shadowValue(row) as GraphSnapshot | null,
    };
    await this.stage(KV.graphWorkingSnapshots, this.jobId, wrapper, delta);
    return row.operation;
  }

  private templateKey(delta: GraphDeltaPreparation, ordinal: number): string { return `template:${delta.ordinal}:${delta.attempt}:${ordinal}`; }
  private preparedKey(delta: GraphDeltaPreparation, ordinal: number): string { return `prepared:${delta.ordinal}:${delta.attempt}:${ordinal}`; }

  async freeze(delta: GraphDeltaPreparation, result: unknown): Promise<void> {
    const snapshot = await this.workingSnapshot(delta);
    let operations: StateCommitOperation[] = [];
    let count = 0;
    const save = async () => {
      await this.stage(KV.graphRemaps(this.jobId), this.templateKey(delta, count++), operations, delta);
      operations = [];
    };
    const completion: StateCommitOperation[] = [];
    for (let ordinal = 0; ordinal < (delta.shadowCount ?? 0); ordinal++) {
        const key = await this.get<string>(KV.graphRemaps(this.jobId), `row-order:${delta.ordinal}:${delta.attempt}:${ordinal}`);
        const row = key ? await this.get<ShadowRow>(KV.graphDeltas(this.jobId), key) : null;
        if (!row) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const op = row.operation;
        if (op.scope === KV.graphSnapshot) continue;
        if (op.scope === KV.batchCallbacks || op.scope === KV.graphJobs) { completion.push(op); continue; }
        const candidate = [...operations, op];
        if (operations.length && (candidate.length > MAX_STATE_COMMIT_OPERATIONS || Buffer.byteLength(JSON.stringify(candidate)) > TEMPLATE_TARGET_BYTES)) await save();
        operations.push(op);
        if (Buffer.byteLength(JSON.stringify(operations)) > TEMPLATE_TARGET_BYTES) await save();
    }
    if (operations.length) await save();
    // Snapshot publication and callback receipt completion share the final
    // application transaction. Snapshot operations never appear earlier.
    await this.stage(KV.graphRemaps(this.jobId), this.templateKey(delta, count++), [...completion, snapshot], delta);
    delta.phase = "freezing";
    delta.templateCount = count;
    delta.result = graphJson({ value: result });
    // Fragment count depends on decimal ordinal widths. Solve that bounded
    // fixed point before committing the manifest or any application bytes.
    const nextOrdinal = this.checkpoint?.next_chunk_ordinal ?? 0;
    let stagingCount = count;
    let stable = false;
    for (let pass = 0; pass < 32; pass++) {
      delta.applyOrdinal = nextOrdinal + 1 + stagingCount;
      delta.applyCheckpointVersion = successor(this.checkpointVersion, 1 + stagingCount);
      let predicted = 0;
      for (let ordinal = 0; ordinal < count; ordinal++) {
        const template = await this.get<StateCommitOperation[]>(KV.graphRemaps(this.jobId), this.templateKey(delta, ordinal));
        if (!template) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        const prepared = this.prepared(template, delta, ordinal === count - 1 ? "complete" : "applying", delta.applyOrdinal + ordinal, successor(delta.applyCheckpointVersion, ordinal));
        predicted += 1;
        for (const _part of fragments(prepared.payload_json)) predicted++;
      }
      if (predicted === stagingCount) { stable = true; break; }
      stagingCount = predicted;
    }
    if (!stable) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
    await this.stage(KV.graphDeltas(this.jobId), `manifest:${delta.ordinal}`, delta, delta);
    await this.finishFreeze(delta);
  }

  async finishFreeze(delta: GraphDeltaPreparation): Promise<void> {
    if (delta.templateCount === undefined || delta.applyOrdinal === undefined || delta.applyCheckpointVersion === undefined) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    for (let ordinal = 0; ordinal < delta.templateCount; ordinal++) {
      const key = this.preparedKey(delta, ordinal);
      if (await this.get<PreparedReference>(KV.graphPrepared(this.jobId), key)) continue;
      const operations = await this.get<StateCommitOperation[]>(KV.graphRemaps(this.jobId), this.templateKey(delta, ordinal));
      if (!operations) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      const final = ordinal === delta.templateCount - 1;
      const prepared = this.prepared(operations, delta, final ? "complete" : "applying", delta.applyOrdinal + ordinal, successor(delta.applyCheckpointVersion, ordinal));
      let parts = 0;
      for (const part of fragments(prepared.payload_json)) {
        const partKey = `${key}:part:${parts++}`;
        const saved = await this.get<string>(KV.graphPrepared(this.jobId), partKey);
        if (saved !== null && saved !== part) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
        if (saved === null) await this.stage(KV.graphPrepared(this.jobId), partKey, part, delta);
      }
      await this.stage(KV.graphPrepared(this.jobId), key, { parts, digest: prepared.payload_digest }, delta);
    }
    delta.phase = "prepared";
  }

  async apply(delta: GraphDeltaPreparation): Promise<void> {
    if (delta.templateCount === undefined || delta.applyOrdinal === undefined) throw new StateTransactionError("STATE_TX_INVALID_RESPONSE");
    for (let ordinal = 0; ordinal < delta.templateCount; ordinal++) {
      const key = this.preparedKey(delta, ordinal);
      const reference = await this.get<PreparedReference>(KV.graphPrepared(this.jobId), key);
      if (!reference) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
      let payload_json = "";
      for (let part = 0; part < reference.parts; part++) {
        const text = await this.get<string>(KV.graphPrepared(this.jobId), `${key}:part:${part}`);
        if (text === null) throw new StateTransactionError("STATE_GRAPH_RECOVERY_REQUIRED");
        payload_json += text;
      }
      if (graphRecordDigest(payload_json) !== reference.digest) throw new StateTransactionError("STATE_TX_REPLAY_CONFLICT");
      const prepared: StatePreparedBatch = { payload_json, payload_digest: reference.digest };
      const expected = delta.applyOrdinal + ordinal;
      if (this.checkpoint?.logical_delta_id === delta.id && this.checkpoint.next_chunk_ordinal > expected) continue;
      await this.commit(prepared);
    }
    delta.phase = "completed";
  }

  async completeStaging(delta: GraphDeltaPreparation): Promise<void> {
    await this.commit(this.prepared([await this.workingSnapshot(delta)], delta, "complete"));
  }
}

export function graphRecordDigest(value: string): string { return createHash("sha256").update(value).digest("hex"); }
export type GraphVersionedRow = StateVersioned<StateJsonValue>;
