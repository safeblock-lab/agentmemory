import { Buffer } from "node:buffer";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { MAX_STALE_LIST_RESTARTS, StatePageError, type StateScopeRevision } from "../state/state-pages.js";
import { StateTransactionError } from "../state/state-transactions.js";
import type { CompressedObservation } from "../types.js";

const MAX_CAPTURE_BYTES = 64 * 1024 * 1024;
const MAX_CAPTURE_ROWS = 100_000;
const MAX_CAPTURE_MS = 30_000;

interface GraphSessionCapture {
  observations: CompressedObservation[];
  attempts: number;
  pages: number;
  rows: number;
  bytes: number;
  elapsedMs: number;
}

function validateRevision(value: StateScopeRevision): StateScopeRevision {
  if (!value || typeof value.generation !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value.generation)
    || typeof value.revision !== "string" || !/^\d+$/.test(value.revision)) {
    throw new StatePageError("STATE_PAGE_INVALID_RESPONSE");
  }
  return value;
}

async function beforeDeadline<T>(read: () => Promise<T>, deadline: number): Promise<T> {
  const remaining = deadline - Date.now();
  if (remaining <= 0) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      read(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new StateTransactionError("STATE_TX_LIMIT_EXCEEDED")), remaining);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/** Keep the revision-pinned read separate from all durable graph writes. */
export async function readGraphSessionCapture(kv: StateKV, sessionId: string): Promise<GraphSessionCapture> {
  const scope = KV.observations(sessionId);
  const deadline = Date.now() + MAX_CAPTURE_MS;
  for (let attempt = 0; ; attempt++) {
    const observations: CompressedObservation[] = [];
    const pages = kv.pages<CompressedObservation>(scope, { limit: 256, maxBytes: 1_048_576 });
    try {
      const before = validateRevision(await beforeDeadline(() => kv.scopeRevision(scope), deadline));
      let bytes = 0, rows = 0, pageCount = 0;
      for (;;) {
        const next = await beforeDeadline(() => pages.next(), deadline);
        if (next.done) break;
        pageCount++;
        bytes += Buffer.byteLength(JSON.stringify(next.value.items), "utf8");
        rows += next.value.items.length;
        if (bytes > MAX_CAPTURE_BYTES || rows > MAX_CAPTURE_ROWS) throw new StateTransactionError("STATE_TX_LIMIT_EXCEEDED");
        for (const observation of next.value.items) {
          if (observation.title) observations.push(observation);
        }
      }
      const after = validateRevision(await beforeDeadline(() => kv.scopeRevision(scope), deadline));
      if (before.generation !== after.generation || before.revision !== after.revision) throw new StatePageError("STATE_PAGE_CURSOR_STALE");
      return { observations, attempts: attempt + 1, pages: pageCount, rows, bytes, elapsedMs: MAX_CAPTURE_MS - (deadline - Date.now()) };
    } catch (error) {
      if (!(error instanceof StatePageError) || error.code !== "STATE_PAGE_CURSOR_STALE" || attempt >= MAX_STALE_LIST_RESTARTS) throw error;
    } finally {
      // A timed-out native read may still settle, but it cannot publish or resume
      // this attempt; closing the iterator never launches another page request.
      void pages.return(undefined).catch(() => {});
    }
  }
}
