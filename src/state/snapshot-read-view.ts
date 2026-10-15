import { randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";

export const SNAPSHOT_CAPTURE_MS = 300_000;
export const SNAPSHOT_EXPORT_MS = 600_000;
export const SNAPSHOT_CALL_MS = 10_000;
export const SNAPSHOT_ROW_BYTES = 16 * 1024 * 1024 - 128 * 1024;
export const SNAPSHOT_PAGE_BYTES = 1024 * 1024;
export const SNAPSHOT_PUBLICATION_BYTES = 4 * 1024 ** 3;
export const SNAPSHOT_PUBLICATION_HEADROOM = 16 * 1024 ** 2;
const MAX_PAGES = 100_000;
export const SNAPSHOT_MAX_ROWS = 10_000_000;

export type SnapshotReadRequest =
  | { action: "snapshot_open"; owner_id: string; request_id: string }
  | { action: "snapshot_cancel"; owner_id: string; request_id: string }
  | { action: "snapshot_keys"; owner_id: string; request_id: string; handle: string; generation: string; scope: string; cursor: string | null }
  | { action: "snapshot_get"; owner_id: string; request_id: string; handle: string; generation: string; scope: string; key: string };
export type SnapshotTransport = (request: SnapshotReadRequest, timeoutMs: number) => Promise<unknown>;
type Opened = { handle: string; generation: string; cut_at: string; expires_at: string };
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function token(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f-]{36}$/.test(value);
}
function timestamp(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

export class SnapshotReadView {
  private closed = false;
  private pagesRead = 0;
  private rowsRead = 0;
  private readonly deadline: number;
  constructor(private readonly transport: SnapshotTransport,
    private readonly owner_id: string, private readonly request_id: string,
    readonly cut: Opened, private readonly signal?: AbortSignal) {
    this.deadline = Math.min(Date.now() + SNAPSHOT_EXPORT_MS, Date.parse(cut.expires_at));
  }
  check(): void {
    if (this.closed) throw new Error("STATE_SNAPSHOT_CLOSED");
    if (this.signal?.aborted) throw new Error("STATE_SNAPSHOT_CANCELLED");
    if (Date.now() >= this.deadline) throw new Error("STATE_SNAPSHOT_EXPIRED");
  }
  private async call(request: SnapshotReadRequest): Promise<unknown> {
    this.check();
    const value = await this.transport(request, Math.max(1, Math.min(SNAPSHOT_CALL_MS, this.deadline - Date.now())));
    this.check();
    return value;
  }
  private identity() {
    return { owner_id: this.owner_id, request_id: this.request_id, handle: this.cut.handle, generation: this.cut.generation };
  }
  async *keys(scope: string): AsyncGenerator<string> {
    let cursor: string | null = null;
    do {
      if (++this.pagesRead > MAX_PAGES) throw new Error("STATE_SNAPSHOT_RESOURCE_LIMIT");
      const page = await this.call({ action: "snapshot_keys", ...this.identity(), scope, cursor });
      if (!record(page) || !Array.isArray(page.keys) || page.keys.length > 256
        || !(page.next_cursor === null || token(page.next_cursor))
        || (page.next_cursor !== null && (page.keys.length === 0 || page.next_cursor === cursor))
        || Buffer.byteLength(JSON.stringify(page)) > SNAPSHOT_PAGE_BYTES) throw new Error("STATE_SNAPSHOT_INVALID_RESPONSE");
      for (const key of page.keys) {
        if (typeof key !== "string" || !key || Buffer.byteLength(key) > 4096) throw new Error("STATE_SNAPSHOT_INVALID_RESPONSE");
        if (++this.rowsRead > SNAPSHOT_MAX_ROWS) throw new Error("STATE_SNAPSHOT_RESOURCE_LIMIT");
        yield key;
      }
      cursor = page.next_cursor;
    } while (cursor !== null);
  }
  async get<T>(scope: string, key: string): Promise<T> {
    const result = await this.call({ action: "snapshot_get", ...this.identity(), scope, key });
    if (!record(result) || !record(result.value)
      || Buffer.byteLength(JSON.stringify(result.value)) > SNAPSHOT_ROW_BYTES) throw new Error("STATE_SNAPSHOT_INVALID_RESPONSE");
    return result.value as T;
  }
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    const result = await this.transport({ action: "snapshot_cancel", owner_id: this.owner_id, request_id: this.request_id }, SNAPSHOT_CALL_MS);
    if (!record(result) || result.closed !== true) throw new Error("STATE_SNAPSHOT_CLEANUP_FAILED");
  }
}

export async function withSnapshotReadView<T>(transport: SnapshotTransport,
  run: (view: SnapshotReadView) => Promise<T>, signal?: AbortSignal): Promise<T> {
  const owner_id = randomUUID(), request_id = randomUUID();
  let view: SnapshotReadView | undefined;
  let result: T | undefined;
  let failure: unknown;
  try {
    if (signal?.aborted) throw new Error("STATE_SNAPSHOT_CANCELLED");
    const cut = await transport({ action: "snapshot_open", owner_id, request_id }, SNAPSHOT_CAPTURE_MS);
    if (!record(cut) || !token(cut.handle) || !token(cut.generation) || !timestamp(cut.cut_at) || !timestamp(cut.expires_at)
      || Date.parse(cut.expires_at) <= Date.parse(cut.cut_at)
      || Date.parse(cut.expires_at) - Date.parse(cut.cut_at) > SNAPSHOT_EXPORT_MS) throw new Error("STATE_SNAPSHOT_INVALID_RESPONSE");
    view = new SnapshotReadView(transport, owner_id, request_id, cut as Opened, signal);
    view.check();
    result = await run(view);
  } catch (error) { failure = error; }
  try {
    if (view) await view.close();
    else {
      const closed = await transport({ action: "snapshot_cancel", owner_id, request_id }, SNAPSHOT_CALL_MS);
      if (!record(closed) || closed.closed !== true) throw new Error("STATE_SNAPSHOT_CLEANUP_FAILED");
    }
  } catch (cleanup) {
    if (failure !== undefined) throw new AggregateError([failure, cleanup], "STATE_SNAPSHOT_FAILED_WITH_CLEANUP_PENDING");
    throw cleanup;
  }
  if (failure !== undefined) throw failure;
  return result as T;
}
