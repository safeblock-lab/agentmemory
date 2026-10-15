import { describe, expect, it, vi } from "vitest";
import { StateKV } from "../src/state/kv.js";
import { prepareGraphBrowse, readScopedGraphPage } from "../src/functions/graph-retrieval-read.js";
import { retrievalRequest } from "../src/state/indexed-retrieval.js";
import type { IIIClient } from "iii-sdk";

function fixture(result: unknown) {
  const trigger = vi.fn().mockResolvedValue(result);
  const sdk = { trigger } as unknown as IIIClient;
  return { kv: new StateKV(sdk), trigger };
}
const preparing = { scope: "mem:graph:browse", project: "p", status: "preparing",
  phase: "membership", processed: 256, revision: "1", continue: true, reason: null };

describe("graph preparation budget", () => {
  it("uses a useful work batch and leaves ten seconds for ACK delivery", async () => {
    const { kv, trigger } = fixture(preparing);
    const result = await prepareGraphBrowse(kv, "p");
    expect(trigger).toHaveBeenCalledExactlyOnceWith({ function_id: "state::retrieval",
      payload: { action: "index_prepare", scope: "mem:graph:browse", project: "p",
        max_rows: 256, max_bytes: 1048576 }, timeoutMs: 310000 });
    expect(result).toEqual({ status: "preparing", phase: "membership", processed: 256,
      revision: "1", continue: true });
  });
  it("preserves bounded errors without claiming continuation", async () => {
    const { kv } = fixture({ ...preparing, status: "error", processed: 0,
      continue: false, reason: "preparation_resource_limit", required_bytes: 2000000 });
    await expect(prepareGraphBrowse(kv, "p")).resolves.toMatchObject({
      status: "error", processed: 0, continue: false, reason: "preparation_resource_limit", required_bytes: 2000000 });
  });
  it("rejects progress exceeding admitted row work", async () => {
    const { kv } = fixture({ ...preparing, processed: 257 });
    await expect(prepareGraphBrowse(kv, "p")).rejects.toMatchObject({ code: "STATE_TX_INVALID_RESPONSE" });
  });
  it("admits an explicit large-source budget without changing display response limits", async () => {
    const { kv, trigger } = fixture(preparing);
    await prepareGraphBrowse(kv, "p", { maxWorkBytes: 160 * 1024 * 1024 });
    expect(trigger.mock.calls[0][0]).toMatchObject({ payload: { max_bytes: 167772160 }, timeoutMs: 310000 });
  });
  it.each([1023, 167772161, 1.5, Number.NaN])("rejects invalid work budget %s before dispatch", async maxWorkBytes => {
    const { kv, trigger } = fixture(preparing);
    await expect(prepareGraphBrowse(kv, "p", { maxWorkBytes })).rejects.toMatchObject({ code: "STATE_TX_INVALID_REQUEST" });
    expect(trigger).not.toHaveBeenCalled();
  });
  it("propagates delivery failure without another preparation attempt", async () => {
    const { kv, trigger } = fixture(preparing);
    trigger.mockRejectedValueOnce(new Error("lost acknowledgement"));
    await expect(prepareGraphBrowse(kv, "p")).rejects.toThrow("lost acknowledgement");
    expect(trigger).toHaveBeenCalledTimes(1);
  });
  it("keeps ordinary retrieval timeout and public read byte bounds", async () => {
    const { kv, trigger } = fixture({ ok: true });
    await retrievalRequest(kv, { action: "index_status" });
    expect(trigger).toHaveBeenCalledExactlyOnceWith({ function_id: "state::retrieval",
      payload: { action: "index_status" } });
    await expect(readScopedGraphPage(kv, { project: "p", mode: "browse", max_bytes: 65537 }))
      .rejects.toMatchObject({ code: "STATE_TX_INVALID_REQUEST" });
    expect(trigger).toHaveBeenCalledTimes(1);
  });
});
