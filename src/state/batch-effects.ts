import { createHash } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import type { BatchEffectMetadata, BatchCallbackReceipt } from "../types.js";
import type { StateKV } from "./kv.js";
import { KV } from "./schema.js";
import { withKeyedLock } from "./keyed-mutex.js";
import { withGraphDelta, type GraphJobPreflight } from "../functions/graph-jobs.js";

const MAX_EFFECTS_PER_RECORD = 4096;
const destinations = ["consolidation", "crystallize", "graph", "lessons", "reflect"];
interface Admission { family: string; startedAt: number; active: boolean }
const activeAdmissions = new Set<Admission>();
const admissionContext = new AsyncLocalStorage<Admission>();
const maintenanceContext = new AsyncLocalStorage<{ active: boolean }>();
let maintenance = false;
let onQuiescent: (() => void) | undefined;
let admissionWaiters: Array<() => void> = [];

// Only descendants of a still-admitted local callback can join its drain.
// Expired contexts must not let detached work enter an exclusive operation.
async function withAdmission<T>(family: string, run: () => Promise<T>): Promise<T> {
  while (maintenance && !admissionContext.getStore()?.active) {
    if (admissionWaiters.length >= 1024) throw new Error("Batch maintenance admission capacity exhausted");
    await new Promise<void>((resolve) => admissionWaiters.push(resolve));
  }
  const admission: Admission = { family, startedAt: Date.now(), active: true };
  activeAdmissions.add(admission);
  try { return await admissionContext.run(admission, run); }
  finally {
    admission.active = false;
    activeAdmissions.delete(admission);
    if (activeAdmissions.size === 0) onQuiescent?.();
  }
}

export function withGraphLeaseAdmission<T>(run: () => Promise<T>): Promise<T> {
  // The exclusive maintenance owner may import graph state under its own lock.
  if (maintenanceContext.getStore()?.active) return run();
  return withAdmission("graph-lease", run);
}

export function withBatchRecordLocks<T>(records: ReadonlyArray<readonly [string, string]>, run: () => Promise<T>): Promise<T> {
  const keys = [...new Set(records.map(([scope, id]) => `batch-record:${scope}:${id}`))].sort();
  return keys.reduceRight<() => Promise<T>>((next, key) => () => withKeyedLock(key, next), run)();
}

async function assertRecovered(kv: StateKV, families: string[]): Promise<void> {
  if (families.includes("graph") && (await kv.get<{ batchInProgress?: string }>(KV.graphSnapshot, "current"))?.batchInProgress) throw new Error("A batch graph application must be recovered first");
  for (const destination of families) {
    const active = await kv.get<BatchCallbackReceipt>(KV.batchCallbacks, `active:${destination}`);
    if (!active) continue;
    if (!active.activeKey) throw new Error("Ambiguous batch callback admission state");
    const receipt = await kv.get<BatchCallbackReceipt>(KV.batchCallbacks, `${destination}:${active.activeKey}`);
    if (receipt?.state !== "completed" && receipt?.state !== "stale") throw new Error("A batch callback must be recovered before importing or mutating shared state");
  }
}

export function withBatchWriterLocks<T>(kv: StateKV, families: string[], run: () => Promise<T>): Promise<T> {
  return withAdmission([...new Set(families)].sort().join("+"), () => [...new Set(families)].sort().reduceRight<() => Promise<T>>(
    (next, family) => () => withKeyedLock(`batch-callback:${family}`, next),
    async () => { await assertRecovered(kv, families); return run(); },
  )());
}

export function batchEffectKey(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

type BatchCallbackReader = { get<T>(scope: string, key: string): Promise<T | null> };

interface BatchCallbackAdmission {
  receipt: BatchCallbackReceipt | null;
  activeKeyToClear?: string;
  terminal?: "completed" | "stale";
}

export function assertBatchCallbackKey(destination: string, key: string | undefined): void {
  if (destination === "graph" && key !== undefined && !/^[a-f0-9]{64}$/.test(key)) throw new Error("Invalid batch effect key");
}

async function inspectBatchCallbackAdmission(reader: BatchCallbackReader, destination: string, key: string | undefined): Promise<BatchCallbackAdmission> {
  if (destination === "graph" && !key) return { receipt: null };
  const activeId = `active:${destination}`;
  const active = await reader.get<BatchCallbackReceipt>(KV.batchCallbacks, activeId);
  if (active && !active.activeKey) throw new Error("Ambiguous batch callback admission state");
  let activeKeyToClear: string | undefined;
  if (active?.activeKey) {
    const previous = await reader.get<BatchCallbackReceipt>(KV.batchCallbacks, `${destination}:${active.activeKey}`);
    if (previous?.state === "completed" || previous?.state === "stale") activeKeyToClear = active.activeKey;
    else if (active.activeKey !== key) throw new Error("A batch callback must be recovered first");
  }
  if (!key) return { receipt: null, activeKeyToClear };
  const receipt = await reader.get<BatchCallbackReceipt>(KV.batchCallbacks, `${destination}:${key}`);
  if (receipt && !["started", "completed", "stale"].includes(receipt.state)) throw new Error("Ambiguous batch callback receipt state");
  return {
    receipt,
    activeKeyToClear,
    ...(receipt?.state === "completed" || receipt?.state === "stale" ? { terminal: receipt.state } : {}),
  };
}

export function createGraphBatchCallbackPreflight<T>(
  key: string,
  resultForTerminal: (state: "completed" | "stale") => T,
): GraphJobPreflight<T> {
  assertBatchCallbackKey("graph", key);
  return async (reader) => {
    const admission = await inspectBatchCallbackAdmission(reader, "graph", key);
    return admission.terminal ? resultForTerminal(admission.terminal) : null;
  };
}

export function effectMetadata(value: BatchEffectMetadata | null, key?: string): BatchEffectMetadata {
  const effects = value?.appliedBatchEffects === undefined ? [] : value.appliedBatchEffects;
  if (!Array.isArray(effects) || effects.length > MAX_EFFECTS_PER_RECORD || effects.some((effect) => typeof effect !== "string" || !/^[a-f0-9]{64}$/.test(effect)) || new Set(effects).size !== effects.length) {
    throw new Error("Invalid batch effect receipt metadata");
  }
  if (!key) return value?.appliedBatchEffects ? { appliedBatchEffects: effects } : {};
  if (!/^[a-f0-9]{64}$/.test(key)) throw new Error("Invalid batch effect key");
  if (effects.includes(key)) return { appliedBatchEffects: effects };
  // Receipts cannot be evicted while old work may still be retried.
  if (effects.length >= MAX_EFFECTS_PER_RECORD) throw new Error("Batch effect receipt capacity exhausted");
  return { appliedBatchEffects: [...effects, key] };
}

export function preserveBatchProvenance<T extends object>(current: T | null, incoming: T): T {
  const previous = current as (T & BatchEffectMetadata) | null;
  const candidate = incoming as T & BatchEffectMetadata;
  effectMetadata(previous);
  effectMetadata(candidate);
  const effects = [...new Set([...(previous?.appliedBatchEffects ?? []), ...(candidate.appliedBatchEffects ?? [])])];
  const result = { ...incoming, ...(effects.length ? { appliedBatchEffects: effects } : {}) };
  effectMetadata(result);
  const target = result as Record<string, unknown>;
  const old = current as Record<string, unknown> | null;
  for (const field of ["sourceObservationIds", "sourceSessionIds", "sourceMemoryIds", "sourceActionIds", "sourceIds", "aliases"]) {
    const values = [...(Array.isArray(old?.[field]) ? old[field] : []), ...(Array.isArray(target[field]) ? target[field] : [])];
    if (values.length) target[field] = [...new Set(values)];
  }
  // Replication must not remove locally acknowledged additive contributions.
  if (previous?.appliedBatchEffects?.length) {
    for (const field of ["frequency", "reinforcements", "reinforcementCount", "accessCount", "strength", "confidence"]) {
      if (typeof old?.[field] === "number") target[field] = typeof target[field] === "number" ? Math.max(old[field], target[field]) : old[field];
    }
  }
  if (typeof old?.supersededBy === "string") {
    target.supersededBy = old.supersededBy;
    target.isLatest = false;
    if (old.tvalidEnd !== undefined) target.tvalidEnd = old.tvalidEnd;
  }
  if (typeof old?.version === "number" && typeof target.version === "number") target.version = Math.max(old.version, target.version);
  return result;
}

export async function applyBatchEffect<T extends BatchEffectMetadata>(
  kv: StateKV, scope: string, id: string, key: string | undefined,
  change: (current: T | null) => T,
): Promise<T> {
  return withBatchRecordLocks([[scope, id]], async () => {
    const current = await kv.get<T>(scope, id);
    effectMetadata(current);
    if (key && current?.appliedBatchEffects?.includes(key)) return current;
    const metadata = effectMetadata(current, key);
    const next = { ...change(current ? structuredClone(current) : null), ...metadata };
    await kv.set(scope, id, next);
    return next;
  });
}

export async function runBatchCallback<T extends { success: boolean; stale?: boolean }>(
  kv: StateKV, destination: string, key: string | undefined,
  run: (resuming: boolean, admit: (metadata?: Pick<BatchCallbackReceipt, "semanticSourceIds" | "semanticCheckpoint" | "resultHash" | "effectTimestamp">) => Promise<void>, receipt?: BatchCallbackReceipt | null) => Promise<T>,
  repairAudit?: () => Promise<unknown>,
): Promise<T | { success: true; stale?: boolean }> {
  assertBatchCallbackKey(destination, key);
  const execute = () => withKeyedLock(`batch-callback:${destination}`, async () => {
    const activeId = `active:${destination}`;
    if (destination === "graph" && !key) return run(false, async () => {});
    const admission = await inspectBatchCallbackAdmission(kv, destination, key);
    if (admission.activeKeyToClear) await kv.delete(KV.batchCallbacks, activeId);
    if (!key) return run(false, async () => { });
    if (admission.terminal === "completed") return { success: true as const };
    if (admission.terminal === "stale") return { success: true as const, stale: true };
    const receiptId = `${destination}:${key}`;
    const receipt = admission.receipt;
    let admittedReceipt = receipt;
    // Admission precedes effects; retry must bypass source-staleness checks
    // because an earlier partial application may itself have changed the source.
    const result = await run(Boolean(receipt), async (metadata) => {
      if (!admittedReceipt || metadata) {
        const next = { ...admittedReceipt, state: "started" as const, ...metadata } satisfies BatchCallbackReceipt;
        await kv.set(KV.batchCallbacks, receiptId, next);
        admittedReceipt = next;
      }
      await kv.set(KV.batchCallbacks, activeId, { state: "started", activeKey: key } satisfies BatchCallbackReceipt);
    }, receipt);
    if (result.success) await kv.set(KV.batchCallbacks, receiptId, {
      state: result.stale ? "stale" : "completed",
    } satisfies BatchCallbackReceipt);
    if (result.success) await kv.delete(KV.batchCallbacks, activeId);
    return result;
  }).then(async (result) => {
    if (result.success && key && repairAudit) await repairAudit().catch(() => { });
    return result;
  });
  return withAdmission(destination, () => destination === "graph" && key ? withGraphDelta(kv, execute) : execute());
}

export class BatchMaintenanceBusyError extends Error {
  readonly code = "BATCH_MAINTENANCE_BUSY";
  readonly details: { waitedMs: number; activeAdmissions: number; waitingAdmissions: number; families: Array<{ family: string; count: number; oldestMs: number }> };

  constructor(startedAt: number) {
    const now = Date.now();
    const families = new Map<string, { family: string; count: number; oldestMs: number }>();
    for (const admission of activeAdmissions) {
      const entry = families.get(admission.family) ?? { family: admission.family, count: 0, oldestMs: 0 };
      entry.count++;
      entry.oldestMs = Math.max(entry.oldestMs, now - admission.startedAt);
      families.set(admission.family, entry);
    }
    super(`Batch maintenance quiescence timed out (${activeAdmissions.size} active admissions; ${[...families.keys()].join(", ")})`);
    this.name = "BatchMaintenanceBusyError";
    this.details = { waitedMs: now - startedAt, activeAdmissions: activeAdmissions.size, waitingAdmissions: admissionWaiters.length, families: [...families.values()] };
  }
}

export function withBatchMutationLocks<T>(kv: StateKV, run: () => Promise<T>): Promise<T> {
  if (admissionContext.getStore()?.active) return Promise.reject(new Error("Batch maintenance cannot run inside an admitted callback"));
  return withKeyedLock("batch-maintenance", async () => {
    const exclusive = { active: true };
    maintenance = true;
    try {
      if (activeAdmissions.size > 0) await new Promise<void>((resolve, reject) => {
        const startedAt = Date.now();
        const timer = setTimeout(() => { onQuiescent = undefined; reject(new BatchMaintenanceBusyError(startedAt)); }, 30000);
        onQuiescent = () => { clearTimeout(timer); resolve(); };
      });
      onQuiescent = undefined;
      return await maintenanceContext.run(exclusive, () => destinations.reduceRight<() => Promise<T>>((next, destination) => () =>
        withKeyedLock(`batch-callback:${destination}`, next), async () => {
          await assertRecovered(kv, destinations);
          return run();
        })());
    } finally {
      exclusive.active = false;
      onQuiescent = undefined;
      maintenance = false;
      const waiters = admissionWaiters;
      admissionWaiters = [];
      for (const resume of waiters) resume();
    }
  });
}
