import { beforeEach, describe, expect, it, vi } from "vitest";
import { lookup } from "node:dns/promises";
import { request } from "node:https";
import { effectHarness } from "./batch-effects-harness.js";
import { KV } from "../src/state/schema.js";
import { batchEffectKey, runBatchCallback } from "../src/state/batch-effects.js";
import { registerMeshFunction } from "../src/functions/mesh.js";

vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("node:dns/promises", () => ({ lookup: vi.fn() }));
const transport = vi.hoisted(() => ({ status: 200, bytes: Buffer.from('{"accepted":1}'), options: undefined as unknown }));
vi.mock("node:https", async () => {
  const { EventEmitter } = await import("node:events");
  const { Readable } = await import("node:stream");
  return { request: vi.fn((_url, options, callback) => {
    transport.options = options;
    const req = new EventEmitter();
    return Object.assign(req, { end: () => callback(Object.assign(Readable.from([transport.bytes]), { statusCode: transport.status })) });
  }) };
});

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(lookup).mockResolvedValue([{ address: "93.184.216.34", family: 4 }] as never);
  transport.status = 200; transport.bytes = Buffer.from('{"accepted":1}');
});
function setup() { const h = effectHarness(); registerMeshFunction(h.sdk as never, h.kv, "test-secret"); return h; }
async function sync() {
  const h = setup();
  await h.kv.set(KV.mesh, "peer", { id: "peer", url: "https://peer.example", sharedScopes: ["procedural"] });
  return h.call<{ results: Array<{ errors: string[] }> }>("mem::mesh-sync", { direction: "push" });
}

describe("mesh destination boundary", () => {
  it.each(["http://public.example", "https://127.1", "https://2130706433", "https://0x7f000001", "https://169.254.1.1", "https://100.64.0.1", "https://192.0.2.1", "https://224.0.0.1", "https://[::1]", "https://[::ffff:127.0.0.1]", "https://[fe80::2]", "https://[fc01::1]", "https://[2001:db8::1]", "https://[2002:7f00:1::]", "https://user:secret@public.example", "https://host.local"]) ("blocks %s without opening a socket", async (url) => {
    const h = setup();
    expect(await h.call("mem::mesh-register", { url, name: "peer" })).toMatchObject({ success: false });
    expect(request).not.toHaveBeenCalled();
  });
  it.each([[], [{ address: "93.184.216.34", family: 4 }, { address: "10.0.0.1", family: 4 }]])("rejects empty or mixed DNS results", async (addresses) => {
    vi.mocked(lookup).mockResolvedValue(addresses as never);
    expect(await setup().call("mem::mesh-register", { url: "https://peer.example", name: "peer" })).toMatchObject({ success: false });
  });
  it("fails closed on DNS errors", async () => {
    vi.mocked(lookup).mockRejectedValue(new Error("DNS unavailable"));
    expect(await setup().call("mem::mesh-register", { url: "https://peer.example", name: "peer" })).toMatchObject({ success: false });
    expect(request).not.toHaveBeenCalled();
  });
  it("pins the final resolution while retaining TLS and Host identity", async () => {
    expect((await sync()).results[0].errors).toEqual([]);
    expect(transport.options).toMatchObject({ hostname: "93.184.216.34", servername: "peer.example", agent: false, headers: { Host: "peer.example", Authorization: "Bearer test-secret" } });
    expect(lookup).toHaveBeenCalledTimes(2);
  });
  it("rejects a private DNS result at request time after public preflight", async () => {
    vi.mocked(lookup).mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }] as never).mockResolvedValueOnce([{ address: "10.0.0.1", family: 4 }] as never);
    expect((await sync()).results[0].errors.join()).toContain("blocked");
    expect(request).not.toHaveBeenCalled();
  });
  it("does not follow redirects", async () => {
    transport.status = 302;
    expect((await sync()).results[0].errors.join()).toContain("redirects blocked");
    expect(request).toHaveBeenCalledTimes(1);
  });
  it("stops oversized responses before JSON parsing", async () => {
    transport.bytes = Buffer.alloc(8 * 1024 * 1024 + 1);
    expect((await sync()).results[0].errors.join()).toContain("too large");
  });
});

describe("mesh callback writers", () => {
  it("rereads after a concurrent callback and preserves receipts and source IDs", async () => {
    const h = setup(); const key = batchEffectKey("local");
    await h.kv.set(KV.procedural, "p", { id: "p", updatedAt: "2024-01-01", sourceSessionIds: ["old"], frequency: 1 });
    let release!: () => void; let admitted!: () => void;
    const entered = new Promise<void>((resolve) => { admitted = resolve; });
    const pending = new Promise<void>((resolve) => { release = resolve; });
    const callback = runBatchCallback(h.kv, "consolidation", key, async (_, admit) => {
      await admit(); admitted(); await pending;
      await h.kv.set(KV.procedural, "p", { id: "p", updatedAt: "2025-01-01", sourceSessionIds: ["old", "batch"], frequency: 2, appliedBatchEffects: [key] });
      return { success: true };
    });
    await entered;
    const merge = h.call("mem::mesh-receive", { procedural: [{ id: "p", updatedAt: "2026-01-01", sourceSessionIds: ["remote"], frequency: 1 }] });
    release(); await Promise.all([callback, merge]);
    expect(await h.kv.get(KV.procedural, "p")).toMatchObject({ sourceSessionIds: ["old", "batch", "remote"], frequency: 2, appliedBatchEffects: [key] });
  });
  it("does not merge over an incomplete callback", async () => {
    const h = setup();
    await h.kv.set(KV.batchCallbacks, "active:consolidation", { state: "started", activeKey: batchEffectKey("pending") });
    await expect(h.call("mem::mesh-receive", { procedural: [{ id: "p", updatedAt: "2026-01-01" }] })).rejects.toThrow("recovered");
    expect(await h.kv.list(KV.procedural)).toEqual([]);
  });
});
