import { describe, expect, it, vi } from "vitest";
import { ConsolidationRequests } from "../src/functions/consolidation-partitions.js";
import type { StateKV } from "../src/state/kv.js";
function store() {
  const rows = new Map<string, unknown>();
  return { get: async (_scope: string, key: string) => structuredClone(rows.get(key) ?? null),
    set: async (_scope: string, key: string, value: unknown) => { rows.set(key, structuredClone(value)); } } as unknown as StateKV;
}
describe("durable request identity supplied at actual generation callback", () => {
  it("supplies exact request and manifest, captured rehydration calls no callback", async () => {
    const kv = store(); const call = vi.fn(async () => "captured");
    const requests = new ConsolidationRequests(kv, "manifest", call);
    expect(await requests.request("system", "source")).toBe("captured");
    expect(call.mock.calls[0]).toEqual(["system", "Consolidation manifest: manifest\n\nsource",
      { id: expect.stringMatching(/^conrequest_/), manifestId: "manifest" }]);
    const restarted = new ConsolidationRequests(kv, "manifest", call);
    expect(await restarted.request("system", "source")).toBe("captured");
    expect(call).toHaveBeenCalledTimes(1);
  });
  it("unknown dispatch outcome is reconciliation-only across rehydration", async () => {
    const kv = store(); const call = vi.fn(async () => { throw new Error("lost ACK"); });
    await expect(new ConsolidationRequests(kv, "manifest", call).request("system", "source")).rejects.toThrow("lost ACK");
    await expect(new ConsolidationRequests(kv, "manifest", call).request("system", "source")).rejects.toThrow("consolidation_request_retained");
    expect(call).toHaveBeenCalledTimes(1);
  });
  it("preserves existing two-argument callback compatibility", async () => {
    expect(await new ConsolidationRequests(store(), "manifest", async (system, prompt) => `${system}:${prompt}`).request("system", "source"))
      .toBe("system:Consolidation manifest: manifest\n\nsource");
  });
});
