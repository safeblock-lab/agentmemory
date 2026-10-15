import type { IIIClient } from "iii-sdk";
import type { Memory } from "../types.js";
import { KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { recordAudit } from "./audit.js";
import { deleteAccessLog } from "./access-tracker.js";
import { getSearchIndex, vectorIndexRemove, flushIndexSave } from "./search.js";
import { hasRetentionProtection, cleanupAuthorized } from "./storage-retention.js";
import { withBatchMutationLocks } from "../state/batch-effects.js";
import { StatePageError } from "../state/state-pages.js";

interface AutoForgetResult {
  ttlExpired: string[];
  contradictions: Array<{ memoryA: string; memoryB: string; similarity: number }>;
  lowValueObs: string[];
  dryRun: boolean;
  cleanupAuthorized: boolean;
  pending: boolean;
}
export function registerAutoForgetFunction(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::auto-forget", async (data: { dryRun?: boolean }): Promise<AutoForgetResult> => {
    const authorized = await cleanupAuthorized(kv);
    const dryRun = data?.dryRun !== false || !authorized;
    const result: AutoForgetResult = { ttlExpired: [], contradictions: [], lowValueObs: [], dryRun, cleanupAuthorized: authorized, pending: false };
    const now = Date.now();
    const cursorKey = "auto-forget-retention-cursor-v1";
    const saved = dryRun ? null : await kv.get<{ cursor: string | null }>(KV.config, cursorKey);
    let page;
    try {
      page = (await kv.pages<Pick<Memory, "id" | "forgetAfter">>(KV.memories, {
        limit: 128, maxBytes: 65_536, fields: ["id", "forgetAfter"], ...(saved?.cursor ? { cursor: saved.cursor } : {}),
      }).next()).value;
    } catch (error) {
      if (!(error instanceof StatePageError) || !["STATE_PAGE_CURSOR_STALE", "STATE_PAGE_CURSOR_INVALID"].includes(error.code)) throw error;
      if (!dryRun) await kv.set(KV.config, cursorKey, { cursor: null });
      result.pending = true;
      return result;
    }
    result.pending = page?.next_cursor !== null && page?.next_cursor !== undefined;
    const apply = async () => {
      const { decrementImageRef } = await import("./image-refs.js");
      for (const memory of page?.items ?? []) {
        if (!memory.forgetAfter) continue;
        const expires = Date.parse(memory.forgetAfter);
        if (!Number.isFinite(expires) || expires > now) continue;
        const current = await kv.get<Memory>(KV.memories, memory.id);
        if (!current || current.forgetAfter !== memory.forgetAfter || hasRetentionProtection({ ...current })) continue;
        result.ttlExpired.push(memory.id);
        if (dryRun) continue;
        // Explicit TTL never implies deletion of shared source observations or their replay identities.
        await kv.delete(KV.memories, memory.id);
        if (current.imageRef) await decrementImageRef(kv, sdk, current.imageRef);
        await recordAudit(kv, "delete", "mem::auto-forget", [memory.id], {
          resource: "memory", reason: "explicit memory TTL", timestamp: current.forgetAfter,
        });
        await deleteAccessLog(kv, memory.id);
        getSearchIndex().remove(memory.id);
        vectorIndexRemove(memory.id);
      }
      if (!dryRun && result.ttlExpired.length) await flushIndexSave();
      if (!dryRun) await kv.set(KV.config, cursorKey, { cursor: result.ttlExpired.length ? null : page?.next_cursor ?? null });
    };
    if (dryRun) await apply();
    else await withBatchMutationLocks(kv, apply);
    return result;
  });
}
