import { createHash } from "node:crypto";
import type { BatchEffectMetadata, BatchCallbackReceipt } from "../types.js";
import type { StateKV } from "./kv.js";
import { KV } from "./schema.js";
import { withKeyedLock } from "./keyed-mutex.js";

const MAX_EFFECTS_PER_RECORD = 4096;
const destinations = ["consolidation", "crystallize", "graph", "lessons", "reflect"];
let activeMutations = 0;
let maintenance = false;
let onQuiescent: (() => void) | undefined;
let admissionWaiters: Array<() => void> = [];

// One worker owns a state store. Readers may nest through iii function calls;
// waiting maintenance must not block a child needed by an admitted callback.
async function withAdmission<T>(run: () => Promise<T>): Promise<T> {
  while (maintenance) {
    if (admissionWaiters.length >= 1024) throw new Error("Batch maintenance admission capacity exhausted");
    await new Promise<void>((resolve) => admissionWaiters.push(resolve));
  }
  activeMutations++;
  try { return await run(); }
  finally {
    activeMutations--;
    if (activeMutations === 0) onQuiescent?.();
  }
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
  return withAdmission(() => [...new Set(families)].sort().reduceRight<() => Promise<T>>(
    (next, family) => () => withKeyedLock(`batch-callback:${family}`, next),
    async () => { await assertRecovered(kv, families); return run(); },
  )());
}

export function batchEffectKey(value: string): string {
  return createHash("sha256").update(value).digest("hex");
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
  return withAdmission(() => withKeyedLock(`batch-callback:${destination}`, async () => {
    const activeId = `active:${destination}`;
    const active = await kv.get<BatchCallbackReceipt>(KV.batchCallbacks, activeId);
    if (active && !active.activeKey) throw new Error("Ambiguous batch callback admission state");
    if (active?.activeKey) {
      const previous = await kv.get<BatchCallbackReceipt>(KV.batchCallbacks, `${destination}:${active.activeKey}`);
      if (previous?.state === "completed" || previous?.state === "stale") await kv.delete(KV.batchCallbacks, activeId);
      else if (active.activeKey !== key) throw new Error("A batch callback must be recovered first");
    }
    if (!key) return run(false, async () => { });
    if (!/^[a-f0-9]{64}$/.test(key)) throw new Error("Invalid batch effect key");
    const receiptId = `${destination}:${key}`;
    const receipt = await kv.get<BatchCallbackReceipt>(KV.batchCallbacks, receiptId);
    if (receipt && !["started", "completed", "stale"].includes(receipt.state)) throw new Error("Ambiguous batch callback receipt state");
    if (receipt?.state === "completed") return { success: true as const };
    if (receipt?.state === "stale") return { success: true as const, stale: true };
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
  })).then(async (result) => {
    if (result.success && key && repairAudit) await repairAudit().catch(() => { });
    return result;
  });
}

export function withBatchMutationLocks<T>(kv: StateKV, run: () => Promise<T>): Promise<T> {
  return withKeyedLock("batch-maintenance", async () => {
    if (activeMutations > 0) await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => { onQuiescent = undefined; reject(new Error("Batch maintenance quiescence timed out")); }, 30000);
      onQuiescent = () => { clearTimeout(timer); maintenance = true; resolve(); };
    });
    else maintenance = true;
    onQuiescent = undefined;
    try {
      return await destinations.reduceRight<() => Promise<T>>((next, destination) => () =>
        withKeyedLock(`batch-callback:${destination}`, next), async () => {
          await assertRecovered(kv, destinations);
          return run();
        })();
    } finally {
      maintenance = false;
      const waiters = admissionWaiters;
      admissionWaiters = [];
      for (const resume of waiters) resume();
    }
  });
}
