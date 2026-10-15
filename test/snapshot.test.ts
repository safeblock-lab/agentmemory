import { beforeEach, describe, expect, it, vi } from "vitest";
import { mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { join } from "node:path";
import type { SnapshotReadRequest, SnapshotTransport } from "../src/state/snapshot-read-view.js";

const controls = vi.hoisted(() => ({ disk: true, stream: false, closeFailure: false, closeEvents: [] as string[], priorBytes: null as number | null, git: [] as string[][] }));
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/state/schema.js", () => ({
  KV: { sessions: "mem:sessions", memories: "mem:memories", graphNodes: "mem:graph:nodes", accessLog: "mem:access", observations: (id: string) => `mem:obs:${id}` },
  generateId: (prefix: string) => `${prefix}_${randomUUID()}`,
}));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn() }));
vi.mock("../src/functions/graph-jobs.js", () => ({
  graphKV: (value: unknown) => value, registerGraphJobHandler: vi.fn(),
  runGraphJob: async (_kv: unknown, _kind: unknown, input: unknown, run: (value: unknown) => unknown) => run(input),
  withGraphDelta: async (_kv: unknown, run: () => unknown) => run(),
}));
vi.mock("../src/state/batch-effects.js", () => ({
  BatchMaintenanceBusyError: class extends Error {}, withBatchMutationLocks: async (_kv: unknown, run: () => unknown) => run(),
  preserveBatchProvenance: (_old: unknown, value: unknown) => value,
}));
vi.mock("node:child_process", () => ({ execFile: vi.fn() }));
vi.mock("node:util", async () => ({ ...await vi.importActual("node:util"),
  promisify: () => async (_cmd: string, args: string[]) => { controls.git.push(args); return { stdout: "abc1234\n" }; },
}));
vi.mock("node:fs/promises", async () => ({ ...await vi.importActual("node:fs/promises"),
  stat: async (path: string) => controls.priorBytes === null
    ? (await vi.importActual<typeof import("node:fs/promises")>("node:fs/promises")).stat(path) : { size: controls.priorBytes },
  statfs: async () => ({ bavail: controls.disk ? 64n * 1024n ** 3n : 1n, bsize: 1n }),
}));
vi.mock("node:fs", async () => {
  const actual = await vi.importActual<typeof import("node:fs")>("node:fs");
  return { ...actual, createWriteStream: (...args: Parameters<typeof actual.createWriteStream>) => {
    if (controls.closeFailure) {
      const options = typeof args[1] === "object" ? args[1] : {};
      const stream = actual.createWriteStream(args[0], { ...options, fs: { ...actual,
        close: (fd, callback) => actual.close(fd, error => callback(error ?? Object.assign(new Error("CLOSE_FAILED"), { code: "EIO" }))),
      } });
      stream.on("error", () => controls.closeEvents.push("error"));
      stream.once("close", () => controls.closeEvents.push("close"));
      return stream;
    }
    if (!controls.stream) return actual.createWriteStream(...args);
    const stream = new Writable({ write(_chunk, _encoding, callback) { callback(new Error("STREAM_FAILED")); } });
    queueMicrotask(() => stream.emit("open",0));
    return stream;
  } };
});
import { registerSnapshotFunction } from "../src/functions/snapshot.js";
import { withSnapshotReadView, SnapshotReadView, SNAPSHOT_ROW_BYTES } from "../src/state/snapshot-read-view.js";
import { triggerSnapshotStorage } from "../src/state/storage.js";
import { logger } from "../src/logger.js";
type Corpus = Map<string, Map<string, Record<string, unknown>>>;
const root = "D:/agentmemory/.codex-task/full-closure/snapshot-readview-source/candidate/.test-files";
let folder: string;
function fixture() {
  const live: Corpus = new Map(); const generation = randomUUID(); const handle = randomUUID();
  let cut: Corpus | undefined; let owner = "", request = "", closed = false, calls = 0;
  const cursors = new Map<string, { scope: string; offset: number }>(); const requests: SnapshotReadRequest[] = [];
  const hooks: { beforeCut?: () => void; read?: (call: number) => void; fail?: string; cleanupFail?: boolean; lostAck?: boolean; expired?: boolean } = {};
  const set = (scope: string, key: string, value: Record<string, unknown>) => { if (!live.has(scope)) live.set(scope, new Map()); live.get(scope)!.set(key, value); };
  const transport: SnapshotTransport = async data => {
    requests.push(data);
    if (data.action === "snapshot_open") {
      hooks.beforeCut?.(); owner = data.owner_id; request = data.request_id; cut = structuredClone(live); closed = false;
      if (hooks.lostAck) throw new Error("OPEN_ACK_LOST");
      const now = Date.now() - (hooks.expired ? 700_000 : 0);
      return { handle, generation, cut_at: new Date(now).toISOString(), expires_at: new Date(now + 600_000).toISOString() };
    }
    if (data.action === "snapshot_cancel") {
      if (hooks.cleanupFail) throw new Error("CLEANUP_FAILED");
      if (data.owner_id === owner && data.request_id === request) { closed = true; cut = undefined; }
      return { closed: true };
    }
    if (data.generation !== generation) throw new Error("STATE_SNAPSHOT_STALE");
    if (closed || !cut) throw new Error("STATE_SNAPSHOT_EXPIRED");
    if (data.owner_id !== owner || data.request_id !== request || data.handle !== handle) throw new Error("STATE_SNAPSHOT_FOREIGN");
    hooks.read?.(++calls); if (hooks.fail) throw new Error(hooks.fail);
    if (data.action === "snapshot_get") {
      const value = cut.get(data.scope)?.get(data.key); if (!value) throw new Error("STATE_SNAPSHOT_RECORD_MISSING");
      if (Buffer.byteLength(JSON.stringify(value)) > SNAPSHOT_ROW_BYTES) throw new Error("STATE_RECORD_TOO_LARGE"); return { value };
    }
    const prior = data.cursor ? cursors.get(data.cursor) : { scope: data.scope, offset: 0 };
    if (!prior || prior.scope !== data.scope) throw new Error("STATE_SNAPSHOT_CURSOR_INVALID");
    const keys = [...cut.get(data.scope)?.keys() ?? []].sort().slice(prior.offset, prior.offset + 2);
    if (keys.some(key => Buffer.byteLength(key) > 4096)) throw new Error("STATE_RECORD_TOO_LARGE");
    const more = (cut.get(data.scope)?.size ?? 0) > prior.offset + keys.length;
    if (data.cursor) cursors.delete(data.cursor); const next = more ? randomUUID() : null;
    if (next) cursors.set(next, { scope: data.scope, offset: prior.offset + keys.length }); return { keys, next_cursor: next };
  };
  const functions = new Map<string, (data?: unknown) => Promise<unknown>>();
  const sdk = { registerFunction: (name: string, run: (data?: unknown) => Promise<unknown>) => functions.set(name, run) };
  const kv = { snapshotTransport: transport, get: vi.fn(), set: vi.fn() };
  registerSnapshotFunction(sdk as never, kv as never, folder);
  return { live, set, hooks, requests, transport, generation, handle, kv, isClosed: () => closed,
    create: () => functions.get("mem::snapshot-create")!({ message: "fixture" }), run: (name: string, data?: unknown) => functions.get(name)!(data) };
}
beforeEach(async () => {
  controls.disk = true; controls.stream = false; controls.closeFailure = false; controls.closeEvents = []; controls.priorBytes = null; controls.git = [];
  vi.mocked(logger.warn).mockClear();
  folder = join(root, randomUUID()); await mkdir(join(folder, ".git"), { recursive: true });
  await writeFile(join(folder, ".git", "HEAD"), "OLD_HEAD"); await writeFile(join(folder, "state.json"), "OLD_STATE");
});
async function unchanged() {
  expect(await readFile(join(folder, "state.json"), "utf8")).toBe("OLD_STATE");
  expect(await readFile(join(folder, ".git", "HEAD"), "utf8")).toBe("OLD_HEAD");
  expect((await readdir(folder)).filter(name => name.endsWith(".tmp"))).toEqual([]); expect(controls.git).toEqual([]);
}
describe("coherent snapshot candidate using mocked future State transport", () => {
  it("preserves one completion cut across pages/collections while create/update/delete continue", async () => {
    const f = fixture();
    for (let i = 0; i < 7; i++) {
      f.set("mem:sessions", `s${i}`, { id: `s${i}`, original: i }); f.set("mem:memories", `m${i}`, { id: `m${i}`, original: i, provenance: ["original"] });
      f.set(`mem:obs:s${i}`, `o${i}`, { id: `o${i}`, raw: `original-${i}` }); f.set("mem:graph:nodes", `g${i}`, { id: `g${i}`, sourceObservationIds: [`o${i}`] });
    }
    f.set("mem:access", "m1", { memoryId: "m1", count: 7, recent: ["a"] });
    f.hooks.beforeCut = () => f.set("mem:memories", "at-completion", { id: "at-completion", content: "completed cut" });
    f.hooks.read = () => {
      f.set("mem:sessions", "late", { id: "late" }); f.live.get("mem:sessions")!.delete("s3");
      f.set("mem:memories", "m0", { id: "m0", original: "updated" }); f.live.get("mem:memories")!.delete("m4");
      f.set("mem:graph:nodes", "g0", { id: "g0", sourceObservationIds: ["late"] });
      f.live.get("mem:obs:s2")!.delete("o2"); f.set("mem:access", "m1", { memoryId: "m1", count: 99 });
    };
    expect(await f.create()).toMatchObject({ success: true, snapshot: { stats: { sessions: 7, memories: 8, observations: 7, graphNodes: 7 } } });
    const state = JSON.parse(await readFile(join(folder, "state.json"), "utf8"));
    expect(state.sessions).toHaveLength(7); expect(state.sessions).toContainEqual({ id: "s3", original: 3 });
    expect(state.memories).toContainEqual({ id: "m0", original: 0, provenance: ["original"] }); expect(state.memories).toContainEqual({ id: "at-completion", content: "completed cut" });
    expect(state.observations.s2).toEqual([{ id: "o2", raw: "original-2" }]); expect(state.graphNodes[0].sourceObservationIds).toEqual(["o0"]); expect(state.accessLogs[0].count).toBe(7);
    expect(f.kv.get).not.toHaveBeenCalled(); expect(f.isClosed()).toBe(true);
    expect(new Set(f.requests.filter(r => "handle" in r).map(r => "handle" in r && r.handle)).size).toBe(1);
  });
  it("preserves complete fifteen MiB original/provenance/JSON/counters", async () => {
    const f = fixture(); const row = { id: "o", raw: "x".repeat(15 * 1024 * 1024), provenance: [{ source: "full", revisions: [1,2,3] }], metadata: { original: true } };
    f.set("mem:sessions", "s", { id: "s", extra: { full: true } }); f.set("mem:obs:s", "o", row);
    expect(await f.create()).toMatchObject({ success: true, snapshot: { stats: { observations: 1, sessions: 1 } } });
    expect(JSON.parse(await readFile(join(folder, "state.json"), "utf8")).observations.s).toEqual([row]);
  });
  it.each(["STATE_RECORD_TOO_LARGE", "STATE_SNAPSHOT_DISK_LIMIT", "STATE_SNAPSHOT_EXPIRED", "STATE_SNAPSHOT_STALE", "STATE_SNAPSHOT_FOREIGN", "STATE_GRAPH_RECOVERY_REQUIRED", "STATE_SNAPSHOT_RECORD_MISSING", "READ_FAILED"])("preserves old file/Git on %s", async failure => {
    const f = fixture(); f.hooks.fail = failure; expect(await f.create()).toMatchObject({ success: false, error: failure }); await unchanged(); expect(f.isClosed()).toBe(true);
  });
  it("refuses oversized full rows", async () => {
    const f = fixture(); f.set("mem:memories", "m", { id: "m", raw: "x".repeat(SNAPSHOT_ROW_BYTES) });
    expect(await f.create()).toMatchObject({ success: false, error: "STATE_RECORD_TOO_LARGE" }); await unchanged();
  });
  it("refuses oversized identities", async () => {
    const f = fixture(); const key = "k".repeat(4097); f.set("mem:memories", key, { id: key });
    expect(await f.create()).toMatchObject({ success: false, error: "STATE_RECORD_TOO_LARGE" }); await unchanged();
  });
  it("refuses mismatched row identities", async () => {
    const f = fixture(); f.set("mem:memories", "key", { id: "other" }); expect(await f.create()).toMatchObject({ success: false, error: "SNAPSHOT_RECORD_KEY_MISMATCH" }); await unchanged();
  });
  it("preflights publication capacity before view open", async () => {
    controls.disk = false; const f = fixture(); expect(await f.create()).toMatchObject({ success: false, error: "STATE_SNAPSHOT_DISK_LIMIT" }); expect(f.requests).toEqual([]); await unchanged();
  });
  it("enforces the combined old/new/Git publication ceiling while streaming", async () => {
    controls.priorBytes = 4 * 1024 ** 3 - 16 * 1024 ** 2 - 2048;
    const f = fixture(); f.set("mem:memories", "m", { id: "m", content: "x".repeat(4096) });
    expect(await f.create()).toMatchObject({ success: false, error: "STATE_SNAPSHOT_DISK_LIMIT" }); await unchanged(); expect(f.isClosed()).toBe(true);
  });
  it("refuses pre-existing crash artifacts without deleting another publisher's file", async () => {
    await writeFile(join(folder,".state-snapshot.tmp"),"ORPHAN_OR_ACTIVE"); const f = fixture();
    expect(await f.create()).toMatchObject({ success: false, error: "STATE_SNAPSHOT_PUBLICATION_RECOVERY_REQUIRED" });
    expect(await readFile(join(folder,".state-snapshot.tmp"),"utf8")).toBe("ORPHAN_OR_ACTIVE");
    expect(await readFile(join(folder,"state.json"),"utf8")).toBe("OLD_STATE"); expect(controls.git).toEqual([]); expect(f.requests).toEqual([]);
  });
  it("cleans the view/output after stream failure", async () => {
    controls.stream = true; const f = fixture(); expect(await f.create()).toMatchObject({ success: false, error: "STREAM_FAILED" }); await unchanged(); expect(f.isClosed()).toBe(true);
  });
  it("cancels the exact open request after lost ACK", async () => {
    const f = fixture(); f.hooks.lostAck = true; expect(await f.create()).toMatchObject({ success: false, error: "OPEN_ACK_LOST" }); await unchanged(); expect(f.isClosed()).toBe(true);
    const [open,cancel] = f.requests; expect(cancel).toMatchObject({ owner_id: open.owner_id, request_id: open.request_id, action: "snapshot_cancel" });
  });
  it("attempts owned unlink after a real WriteStream emits a close error and preserves the primary failure", async () => {
    controls.closeFailure = true;
    const f = fixture(); f.hooks.lostAck = true;
    expect(await f.create()).toMatchObject({ success: false, error: "OPEN_ACK_LOST" });
    expect(controls.closeEvents).toEqual(["error", "close"]);
    expect(logger.warn).toHaveBeenCalledWith("Snapshot temporary file cleanup failed", { stage: "stream_close", error: "CLOSE_FAILED" });
    expect(f.isClosed()).toBe(true);
    await unchanged();
  });
  it("requires cleanup ACK before rename/Git", async () => {
    const f = fixture(); f.hooks.cleanupFail = true; expect(await f.create()).toMatchObject({ success: false, error: "CLEANUP_FAILED" }); await unchanged();
  });
  it("rejects expired completed views before reads", async () => {
    const f = fixture(); f.hooks.expired = true; expect(await f.create()).toMatchObject({ success: false, error: "STATE_SNAPSHOT_EXPIRED" }); await unchanged(); expect(f.isClosed()).toBe(true);
  });
  it("closes on cancellation", async () => {
    const f = fixture(); const abort = new AbortController();
    await expect(withSnapshotReadView(f.transport, async view => { abort.abort(); await view.get("mem:memories", "x"); },abort.signal)).rejects.toThrow("STATE_SNAPSHOT_CANCELLED");
    expect(f.isClosed()).toBe(true); await unchanged();
  });
  it("rejects stale/foreign/expired mock-native handles", async () => {
    const f = fixture(); const owner_id = randomUUID(), request_id = randomUUID();
    const cut = await f.transport({ action: "snapshot_open", owner_id, request_id },1000) as { handle: string; generation: string; cut_at: string; expires_at: string };
    await expect(new SnapshotReadView(f.transport,owner_id,request_id,{...cut,generation:randomUUID()}).get("mem:memories","x")).rejects.toThrow("STALE");
    await expect(new SnapshotReadView(f.transport,randomUUID(),request_id,cut).get("mem:memories","x")).rejects.toThrow("FOREIGN");
    await f.transport({ action: "snapshot_cancel",owner_id,request_id },1000);
    await expect(new SnapshotReadView(f.transport,owner_id,request_id,cut).get("mem:memories","x")).rejects.toThrow("EXPIRED");
  });
  it("preserves reentrancy and releases the guard", async () => {
    const f = fixture(); const original = f.kv.snapshotTransport; let release!: () => void; const gate = new Promise<void>(resolve => { release = resolve; });
    f.kv.snapshotTransport = async (request, timeout) => { if (request.action === "snapshot_open") await gate; return original(request, timeout); };
    const first = f.create(); expect(await f.create()).toEqual({ success: true, message: "Snapshot already in progress" }); release();
    expect(await first).toMatchObject({ success: true }); expect(await f.create()).toMatchObject({ success: true });
  });
  it("preserves restore validation", async () => {
    const f = fixture(); expect(await f.run("mem::snapshot-restore",{})).toMatchObject({ success: false, error: "commitHash is required" });
    expect(await f.run("mem::snapshot-restore",{ commitHash: "../invalid" })).toMatchObject({ success: false, error: "Invalid commitHash format" });
  });
  it("uses State transport with explicit deadlines", async () => {
    const trigger = vi.fn().mockResolvedValue({ closed: true }); const request: SnapshotReadRequest = { action: "snapshot_cancel", owner_id: randomUUID(), request_id: randomUUID() };
    await triggerSnapshotStorage({ trigger } as never,request,10_000); expect(trigger).toHaveBeenCalledWith({ function_id: "state::storage",payload:request,timeoutMs:10_000 });
    await expect(triggerSnapshotStorage({ trigger } as never,request,0)).rejects.toThrow("INVALID_REQUEST");
  });
});
