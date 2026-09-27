import { afterEach, describe, expect, it, vi } from "vitest";
import { registerWorker } from "iii-sdk";
import { registerHealthMonitor } from "../src/health/monitor.js";
import { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";

vi.mock("iii-sdk", () => ({
  registerWorker: vi.fn(() => ({ trigger: vi.fn() })),
}));

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("health monitor engine connectivity", () => {
  it.each([true, false])("reports engine probe success=%s", async (connected) => {
    vi.useFakeTimers();
    const sdk = registerWorker("ws://unused.test");
    const trigger = vi.spyOn(sdk, "trigger");
    if (connected) trigger.mockResolvedValue({ workers: [] });
    else trigger.mockRejectedValue(new Error("engine unavailable"));
    const kv = new StateKV(sdk);
    vi.spyOn(kv, "get").mockResolvedValue(null);
    const set = vi.spyOn(kv, "set").mockResolvedValue(null);

    const monitor = registerHealthMonitor(sdk, kv);
    try {
      await vi.advanceTimersByTimeAsync(1);
      expect(trigger).toHaveBeenCalledWith({
        function_id: "engine::workers::list",
        payload: {},
        timeoutMs: 5000,
      });
      expect(set).toHaveBeenCalledWith(
        KV.health,
        "latest",
        expect.objectContaining({
          connectionState: connected ? "connected" : "disconnected",
          status: connected ? "healthy" : "critical",
        }),
      );
    } finally {
      monitor.stop();
    }
  });

  it("records disconnected health after the bounded engine probe times out", async () => {
    vi.useFakeTimers();
    const sdk = registerWorker("ws://unused.test");
    const trigger = vi.spyOn(sdk, "trigger").mockImplementation((request) =>
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error("engine probe timeout")), request.timeoutMs);
      }),
    );
    const kv = new StateKV(sdk);
    vi.spyOn(kv, "get").mockResolvedValue(null);
    const set = vi.spyOn(kv, "set").mockResolvedValue(null);
    const monitor = registerHealthMonitor(sdk, kv);
    try {
      await vi.advanceTimersByTimeAsync(4999);
      expect(set).not.toHaveBeenCalledWith(KV.health, "latest", expect.anything());
      await vi.advanceTimersByTimeAsync(2);
      expect(trigger).toHaveBeenCalledTimes(1);
      expect(set).toHaveBeenCalledWith(KV.health, "latest", expect.objectContaining({
        connectionState: "disconnected",
        status: "critical",
        alerts: expect.arrayContaining(["connection_disconnected"]),
      }));
    } finally {
      monitor.stop();
    }
  });
});
