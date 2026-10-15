import { describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import { triggerStateStorage, type StateStorageRequest } from "../src/state/storage.js";

const expiry = "2026-10-13T19:02:00.000Z";
function boundary(response: unknown) {
  const trigger = vi.fn(async () => response);
  return { trigger, call: (request: StateStorageRequest) => triggerStateStorage({ trigger } as unknown as IIIClient, request) };
}
const acquire = { action: "maintenance_lease_acquire", owner_id: "owner:1", lease_ms: 120000 } as const;
const refresh = { action: "maintenance_lease_refresh", owner_id: "owner:1", fence: 7, lease_ms: 120000 } as const;
const release = { action: "maintenance_lease_release", owner_id: "owner:1", fence: 7 } as const;
const value = { version: 1, updatedAt: expiry, compaction: {}, totals: {}, diagnostics: {}, observationIndex: {}, technical: {}, maintenanceHistory: [] };

describe("state storage maintenance boundary", () => {
  it.each([
    [acquire, { status: "acquired", fence: 7, expires_at: expiry, recovered_expired: false }],
    [acquire, { status: "busy", fence: 7, expires_at: expiry }],
    [refresh, { status: "refreshed", fence: 7, expires_at: expiry }],
    [refresh, { status: "stale", fence: 8, expires_at: expiry }],
    [release, { status: "stale", fence: null, expires_at: null }],
    [release, { status: "released", fence: 7 }],
  ] as const)("validates and forwards action %j", async (request, response) => {
    const harness = boundary(response);
    expect(await harness.call(request)).toEqual(response);
    expect(harness.trigger).toHaveBeenCalledExactlyOnceWith({ function_id: "state::storage", payload: request });
  });

  it.each([
    { ...acquire, owner_id: "invalid owner" }, { ...acquire, lease_ms: 999 }, { ...acquire, lease_ms: 300001 },
    { ...refresh, fence: 0 }, { ...release, fence: Number.MAX_SAFE_INTEGER + 1 },
    { action: "reclaim", max_pages: 1, max_ms: 25 },
    { action: "reclaim", max_pages: 17, max_ms: 25, lease_owner_id: "a", lease_fence: 1 },
    { action: "reclaim", max_pages: 1, max_ms: 26, lease_owner_id: "a", lease_fence: 1 },
    { action: "maintenance_state_save", owner_id: "a", fence: 1, value: { ...value, maintenanceHistory: Array(31).fill({}) }, release: false },
  ])("rejects invalid request before dispatch %j", async (request) => {
    const harness = boundary({});
    await expect(harness.call(request as StateStorageRequest)).rejects.toThrow("STATE_STORAGE_INVALID_REQUEST");
    expect(harness.trigger).not.toHaveBeenCalled();
  });

  it.each([
    [acquire, { status: "acquired", fence: 7, expires_at: expiry }],
    [acquire, { status: "released", fence: 7 }],
    [acquire, { status: "busy", fence: 0, expires_at: expiry }],
    [refresh, { status: "refreshed", fence: 8, expires_at: expiry }],
    [refresh, { status: "refreshed", fence: 7, expires_at: "bad" }],
    [release, { status: "released", fence: 8 }],
  ] as const)("rejects invalid acknowledgement %j", async (request, response) => {
    await expect(boundary(response).call(request)).rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
  });

  it("validates state-save acknowledgement identity and release atomically", async () => {
    const request = { action: "maintenance_state_save", owner_id: "a", fence: 7, value, release: true } as unknown as StateStorageRequest;
    const response = { status: "saved", fence: 7, version: "2", released: true };
    expect(await boundary(response).call(request)).toEqual(response);
    await expect(boundary({ ...response, released: false }).call(request)).rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
    await expect(boundary({ ...response, fence: 8 }).call(request)).rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
    await expect(boundary({ ...response, version: "0" }).call(request)).rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
  });

  it("propagates unknown transport outcomes without repeating dispatch", async () => {
    const trigger = vi.fn(async () => { throw new Error("lost acknowledgement"); });
    await expect(triggerStateStorage({ trigger } as unknown as IIIClient, acquire)).rejects.toThrow("lost acknowledgement");
    expect(trigger).toHaveBeenCalledTimes(1);
  });
});
