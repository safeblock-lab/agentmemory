import { createHash } from "node:crypto";
import { StateKV } from "../../src/state/kv.js";
import { isGuardedGraphRecord, KV } from "../../src/state/schema.js";
import {
  prepareStateCommitBatch,
  type StateBatchReceipt,
  type StateCommitBatchInput,
  type StateCommitBatchRequest,
  type StateCounter,
  type StateGraphGuard,
  type StateGraphLease,
  type StateJsonValue,
  type StateLeaseRequest,
  type StatePreparedBatch,
  type StateVersioned,
} from "../../src/state/state-transactions.js";

type Handler = (value: unknown) => Promise<unknown>;
type KvLike = {
  get<T = unknown>(scope: string, key: string): Promise<T | null>;
  set<T = unknown>(scope: string, key: string, value: T): Promise<T>;
  delete(scope: string, key: string): Promise<void>;
  list<T = unknown>(scope: string): Promise<T[]>;
  getVersioned<T = unknown>(scope: string, key: string, guard?: StateGraphGuard): Promise<StateVersioned<T>>;
  lease(request: StateLeaseRequest): Promise<StateGraphLease | { released: true }>;
  commitBatch(guard: StateGraphGuard, prepared: StatePreparedBatch): Promise<StateBatchReceipt>;
  values<T = unknown>(scope: string): AsyncGenerator<T>;
  pages<T = unknown>(scope: string, options?: { cursor?: string; limit?: number; fields?: string[] }): AsyncGenerator<{ items: T[]; next_cursor: string | null }>;
  scopeRevision(scope: string, prefix?: boolean): Promise<{ generation: string; revision: string }>;
  [key: string]: unknown;
};
type SdkLike = {
  registerFunction(id: string | { id: string }, handler: Handler): void;
  trigger: (...args: unknown[]) => Promise<unknown>;
  [key: string]: unknown;
};
type Row<T = unknown> = StateVersioned<T>;
const address = (scope: string, key: string) => JSON.stringify([scope, key]);
const next = (counter: StateCounter): StateCounter => (BigInt(counter) + 1n).toString();
const copy = <T>(value: T): T => structuredClone(value);

export interface GraphStateHarness {
  sdk: SdkLike;
  kv: KvLike;
  rows: Map<string, Row>;
  receipts: Map<string, StateBatchReceipt>;
  scopedCommitFailures: number;
  seed(scope: string, key: string, value: StateJsonValue | null, exists?: boolean, version?: StateCounter): void;
  failCommitBeforeApply(): void;
  loseAcknowledgment(): void;
  failCommitForScope(scope: string, afterApply: boolean, matchingCommitOrdinal?: number): void;
}

export function installGraphStateWire(sdk: SdkLike | undefined, kv: KvLike): GraphStateHarness {
  const handlers = new Map<string, Handler>();
  const attachedSdk: SdkLike = sdk ?? {
    registerFunction(id, handler) { handlers.set(typeof id === "string" ? id : id.id, handler); },
    async trigger(call: unknown, payload?: unknown) {
      const id = typeof call === "string" ? call : (call as { function_id: string }).function_id;
      const data = typeof call === "string" ? payload : (call as { payload: unknown }).payload;
      const handler = handlers.get(id);
      if (!handler) throw new Error(`No function: ${id}`);
      return handler(data);
    },
  };
  const originalTrigger = attachedSdk.trigger.bind(attachedSdk);
  const rawGet = kv.get.bind(kv);
  const rawSet = kv.set.bind(kv);
  const rawDelete = kv.delete.bind(kv);
  const rawList = kv.list.bind(kv);
  const rows = new Map<string, Row>();
  const receipts = new Map<string, StateBatchReceipt>();
  const checkpoints = new Map<string, { version: StateCounter; value: StateCommitBatchInput["checkpoint"] | null }>();
  let generation: StateCounter = "1";
  let fence: StateCounter = "0";
  let lease: StateGraphLease | null = null;
  let now = Date.now();
  let recovery: StateCommitBatchInput["checkpoint"] | null = null;
  let failBeforeApply = false;
  let loseAck = false;
  let scopedCommitFailures = 0;
  let scopedCommitFailure: { scope: string; afterApply: boolean; remaining: number } | undefined;

  async function row(scope: string, key: string): Promise<Row> {
    const id = address(scope, key);
    const known = rows.get(id);
    if (known) return known;
    let value = await rawGet(scope, key);
    if (scope === KV.graphControl && key === "current" && value === null) {
      value = { version: 1, generation, fence, lease: null, recovery: null };
      await rawSet(scope, key, value);
    }
    const exists = value !== null;
    const initial: Row = { exists, value: exists ? copy(value) : null, version: exists ? "1" : "0" };
    rows.set(id, initial);
    return initial;
  }

  function controlValue(value: StateJsonValue | null): Record<string, unknown> {
    return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
  }
  async function saveControl(changes: Record<string, unknown>): Promise<void> {
    const current = await row(KV.graphControl, "current");
    const value = { ...controlValue(current.value as StateJsonValue | null), ...changes, generation, fence, lease: lease ? { ...lease } : null, recovery: recovery ? { ...recovery } : null };
    const updated = { exists: true, value, version: next(current.version) };
    rows.set(address(KV.graphControl, "current"), updated);
    await rawSet(KV.graphControl, "current", copy(value));
  }
  function fail(code: string): never { throw Object.assign(new Error(code), { code }); }
  function authorize(guard: StateGraphGuard): void {
    if (guard.generation !== generation) fail("STATE_TX_GENERATION_STALE");
    if (!lease || lease.expires_at_ms <= now || guard.owner_id !== lease.owner_id || guard.fence !== lease.fence) fail("STATE_TX_FENCED");
  }
  async function acquire(request: StateLeaseRequest): Promise<unknown> {
    if (request.generation !== generation) fail("STATE_TX_GENERATION_STALE");
    now = Math.max(now, Date.now());
    if (request.action === "acquire") {
      if (lease && lease.expires_at_ms > now) {
        if (lease.owner_id !== request.owner_id) fail("STATE_TX_LEASE_BUSY");
        return { ...lease };
      }
      fence = next(fence);
      lease = { owner_id: request.owner_id, generation, fence, expires_at_ms: now + request.ttl_ms };
      await saveControl({});
      return { ...lease };
    }
    authorize(request);
    if (request.action === "release") { lease = null; await saveControl({}); return { released: true }; }
    lease = { owner_id: request.owner_id, generation, fence: request.fence, expires_at_ms: now + request.ttl_ms };
    await saveControl({});
    return { ...lease };
  }
  async function commit(request: StateCommitBatchRequest): Promise<StateBatchReceipt> {
    authorize(request.guard);
    if (createHash("sha256").update(request.payload_json).digest("hex") !== request.payload_digest) fail("STATE_TX_INVALID_REQUEST");
    const body = JSON.parse(request.payload_json) as StateCommitBatchInput;
    const canonical = prepareStateCommitBatch(body);
    if (canonical.payload_json !== request.payload_json || canonical.payload_digest !== request.payload_digest) fail("STATE_TX_INVALID_REQUEST");
    const id = body.identity;
    const receiptId = JSON.stringify([id.generation, id.job_id, id.logical_delta_id, id.chunk_ordinal]);
    const prior = receipts.get(receiptId);
    if (prior) {
      if (prior.payload_digest !== request.payload_digest) fail("STATE_TX_REPLAY_CONFLICT");
      return copy(prior);
    }
    let failThisCommit = false;
    let failAfterApply = false;
    if (scopedCommitFailure && body.operations.some((operation) => operation.scope === scopedCommitFailure!.scope)) {
      scopedCommitFailure.remaining--;
      if (scopedCommitFailure.remaining === 0) {
        failThisCommit = true;
        failAfterApply = scopedCommitFailure.afterApply;
        scopedCommitFailure = undefined;
        scopedCommitFailures++;
      }
    }
    if (id.generation !== generation) fail("STATE_TX_GENERATION_STALE");
    if (recovery && (recovery.job_id !== id.job_id || recovery.logical_delta_id !== id.logical_delta_id)) fail("STATE_GRAPH_RECOVERY_REQUIRED");
    const saved = checkpoints.get(id.job_id) ?? { version: "0", value: null };
    if (saved.version !== body.expected_checkpoint_version) fail("STATE_TX_CHECKPOINT_CONFLICT");
    if (saved.value) {
      if (body.checkpoint.delta_ordinal === saved.value.delta_ordinal) {
        if (id.logical_delta_id !== saved.value.logical_delta_id || saved.value.visibility === "complete" || id.chunk_ordinal !== saved.value.next_chunk_ordinal) fail("STATE_TX_CHECKPOINT_CONFLICT");
      } else if (saved.value.visibility !== "complete" || body.checkpoint.delta_ordinal !== saved.value.delta_ordinal + 1 || id.chunk_ordinal !== 0) fail("STATE_TX_CHECKPOINT_CONFLICT");
    } else if (id.chunk_ordinal !== 0 || body.checkpoint.delta_ordinal !== 0) fail("STATE_TX_CHECKPOINT_CONFLICT");

    const before: Array<{ op: StateCommitBatchInput["operations"][number]; row: Row }> = [];
    for (const op of body.operations) {
      const current = await row(op.scope, op.key);
      if (current.version !== op.expected_version) fail("STATE_TX_CONFLICT");
      before.push({ op, row: current });
    }
    const rowVersions = body.operations.map((op) => ({ scope: op.scope, key: op.key, version: op.type === "check" ? op.expected_version : next(op.expected_version) }));
    const response: StateBatchReceipt = {
      identity: id,
      payload_digest: request.payload_digest,
      generation: body.advance_generation ? next(id.generation) : id.generation,
      checkpoint_version: next(body.expected_checkpoint_version),
      row_versions: rowVersions,
    };
    if (failBeforeApply || (failThisCommit && !failAfterApply)) {
      failBeforeApply = false;
      fail("STATE_TX_FAILED");
    }
    const applied: Array<{ scope: string; key: string; row: Row }> = [];
    try {
      for (const [index, { op, row: current }] of before.entries()) {
        if (op.type === "check") continue;
        const updated: Row = { exists: op.type === "set", value: op.type === "set" ? copy(op.value) : null, version: rowVersions[index].version };
        if (updated.exists) await rawSet(op.scope, op.key, copy(updated.value));
        else await rawDelete(op.scope, op.key);
        rows.set(address(op.scope, op.key), updated);
        applied.push({ scope: op.scope, key: op.key, row: current });
      }
    } catch (error) {
      for (const item of applied.reverse()) {
        if (item.row.exists) await rawSet(item.scope, item.key, copy(item.row.value));
        else await rawDelete(item.scope, item.key);
        rows.set(address(item.scope, item.key), item.row);
      }
      throw error;
    }
    checkpoints.set(id.job_id, { version: response.checkpoint_version, value: copy(body.checkpoint) });
    const checkpointRow = { exists: true, value: copy(body.checkpoint), version: response.checkpoint_version };
    rows.set(address(KV.graphCheckpoints, id.job_id), checkpointRow);
    await rawSet(KV.graphCheckpoints, id.job_id, copy(body.checkpoint));
    receipts.set(receiptId, response);
    if (body.checkpoint.visibility === "applying") recovery = body.checkpoint;
    if (body.checkpoint.visibility === "complete") recovery = null;
    if (body.advance_generation) {
      generation = response.generation;
      lease = null;
    }
    await saveControl({});
    if (loseAck || (failThisCommit && failAfterApply)) {
      loseAck = false;
      throw new Error("lost commit acknowledgement");
    }
    return copy(response);
  }

  const nativeTrigger = async (call: unknown): Promise<unknown> => {
    const request = call as { function_id: string; payload: unknown };
    if (request.function_id === "state::get_versioned") {
      const query = request.payload as { scope: string; key: string; guard?: StateGraphGuard };
      if (query.guard) authorize(query.guard);
      else if (recovery && isGuardedGraphRecord(query.scope, query.key)) fail("STATE_GRAPH_RECOVERY_REQUIRED");
      return copy(await row(query.scope, query.key));
    }
    if (request.function_id === "state::lease") return acquire(request.payload as StateLeaseRequest);
    if (request.function_id === "state::commit_batch") return commit(request.payload as StateCommitBatchRequest);
    return originalTrigger(call);
  };
  const adapter = new StateKV({ trigger: nativeTrigger } as never);
  const sdkTrigger = async (...args: unknown[]): Promise<unknown> => {
    const call = args[0];
    const id = typeof call === "string" ? call : (call as { function_id?: string } | null)?.function_id;
    if (id === "state::get_versioned" || id === "state::lease" || id === "state::commit_batch") {
      return nativeTrigger({ function_id: id, payload: typeof call === "string" ? args[1] : (call as { payload?: unknown }).payload });
    }
    if (id === "state::set" || id === "state::delete") {
      const payload = (typeof call === "string" ? args[1] : (call as { payload?: unknown }).payload) as { scope: string; key: string };
      if (isGuardedGraphRecord(payload.scope, payload.key)) fail("STATE_TX_FENCED");
    }
    return originalTrigger(...args);
  };
  attachedSdk.trigger = sdkTrigger;
  Object.assign(kv, {
    getVersioned: adapter.getVersioned.bind(adapter),
    lease: adapter.lease.bind(adapter),
    commitBatch: adapter.commitBatch.bind(adapter),
    async *values(scope: string) { for (const value of await rawList(scope)) yield value; },
    async *pages<T>(scope: string, options: { cursor?: string; limit?: number; fields?: string[] } = {}) {
      const rows = await rawList(scope);
      const limit = options.limit ?? 256;
      let start = options.cursor === undefined ? 0 : Number(options.cursor);
      for (;;) {
        const end = Math.min(rows.length, start + limit);
        const items = rows.slice(start, end).map((row) => {
          if (!options.fields) return copy(row) as T;
          const source = row && typeof row === "object" ? row as Record<string, unknown> : {};
          return Object.fromEntries(options.fields.flatMap((field) =>
            Object.hasOwn(source, field) ? [[field, source[field]]] : [],
          )) as T;
        });
        const next_cursor = end < rows.length ? String(end) : null;
        yield { items, next_cursor };
        if (next_cursor === null) return;
        start = end;
      }
    },
  });
  return {
    sdk: attachedSdk, kv, rows, receipts,
    get scopedCommitFailures() { return scopedCommitFailures; },
    seed(scope, key, value, exists = true, version = "1") {
      rows.set(address(scope, key), { exists, value: exists ? copy(value) : null, version });
      if (exists) void rawSet(scope, key, copy(value)); else void rawDelete(scope, key);
      if (scope === KV.graphCheckpoints) {
        checkpoints.set(key, {
          version,
          value: exists ? copy(value) as StateCommitBatchInput["checkpoint"] : null,
        });
      }
      if (scope === KV.graphControl && key === "current") {
        const control = controlValue(value);
        generation = String(control.generation ?? "1");
        fence = String(control.fence ?? "0");
        recovery = control.recovery && typeof control.recovery === "object"
          ? copy(control.recovery) as StateCommitBatchInput["checkpoint"]
          : null;
      }
    },
    failCommitBeforeApply() { failBeforeApply = true; },
    loseAcknowledgment() { loseAck = true; },
    failCommitForScope(scope, afterApply, matchingCommitOrdinal = 1) {
      if (!scope || !Number.isSafeInteger(matchingCommitOrdinal) || matchingCommitOrdinal < 1) throw new Error("Invalid scoped commit failure");
      scopedCommitFailure = { scope, afterApply, remaining: matchingCommitOrdinal };
    },
  };
}

export function graphStateHarness(): GraphStateHarness {
  const handlers = new Map<string, Handler>();
  const store = new Map<string, Map<string, unknown>>();
  const revisions = new Map<string, bigint>();
  const bumpRevision = (scope: string) => revisions.set(scope, (revisions.get(scope) ?? 0n) + 1n);
  const kv: KvLike = {
    async get<T>(scope: string, key: string) { return (store.get(scope)?.get(key) as T) ?? null; },
    async set<T>(scope: string, key: string, value: T) {
      if (!store.has(scope)) store.set(scope, new Map());
      store.get(scope)!.set(key, copy(value)); bumpRevision(scope); return value;
    },
    async delete(scope: string, key: string) {
      if (store.get(scope)?.has(key)) { store.get(scope)!.delete(key); bumpRevision(scope); }
    },
    async list<T>(scope: string) { return (Array.from(store.get(scope)?.values() ?? []) as T[]).map(copy); },
    async scopeRevision(scope: string, prefix = false) {
      const scopes = prefix ? [...revisions.keys()].filter((candidate) => candidate.startsWith(scope)) : [scope];
      const total = scopes.reduce((sum, candidate) => sum + (revisions.get(candidate) ?? 0n), 0n);
      return { generation: "1", revision: prefix ? `${scopes.length}:${total}` : String(revisions.get(scope) ?? 0n) };
    },
  };
  const sdk: SdkLike = {
    registerFunction(id, handler) { handlers.set(typeof id === "string" ? id : id.id, handler); },
    async trigger(call: unknown, payload?: unknown) {
      const id = typeof call === "string" ? call : (call as { function_id: string }).function_id;
      const data = typeof call === "string" ? payload : (call as { payload: unknown }).payload;
      const handler = handlers.get(id);
      if (!handler) throw new Error(`No function: ${id}`);
      return handler(data);
    },
  };
  return installGraphStateWire(sdk, kv);
}
