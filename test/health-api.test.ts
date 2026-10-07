import { afterEach, describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";

function registerHealth(
  healthRead: Promise<unknown>,
  metricsRead: Promise<unknown[]>,
) {
  const handlers = new Map<string, (request?: unknown) => Promise<unknown>>();
  const sdk = {
    registerFunction: (id: string, handler: (request?: unknown) => Promise<unknown>) =>
      handlers.set(id, handler),
    registerTrigger: () => {},
    trigger: async () => null,
  };
  const kv = { get: () => healthRead, list: async () => [] };
  const metricsStore = { getAll: () => metricsRead };

  registerApiTriggers(sdk as never, kv as never, "", metricsStore as never);
  return handlers.get("api::health")!;
}

afterEach(() => {
  vi.useRealTimers();
});

describe("api::health bounded state reads", () => {
  it("returns unavailable within the probe deadline when health state stalls", async () => {
    vi.useFakeTimers();
    const handle = registerHealth(new Promise(() => {}), Promise.resolve([]));

    const pending = handle();
    await vi.advanceTimersByTimeAsync(5000);
    const response = (await pending) as {
      status_code: number;
      body: {
        status: string;
        health: unknown;
        unavailableComponents: string[];
      };
    };

    expect(response).toMatchObject({
      status_code: 503,
      body: {
        status: "unavailable",
        health: null,
        unavailableComponents: ["health"],
      },
    });
  });

  it("keeps the measured health status when metrics state stalls", async () => {
    vi.useFakeTimers();
    const health = { status: "critical", alerts: ["connection_disconnected"] };
    const handle = registerHealth(
      Promise.resolve(health),
      new Promise(() => {}),
    );

    const pending = handle();
    await vi.advanceTimersByTimeAsync(5000);
    const response = (await pending) as {
      status_code: number;
      body: {
        status: string;
        health: { status: string };
        unavailableComponents: string[];
      };
    };

    expect(response).toMatchObject({
      status_code: 503,
      body: {
        status: "unavailable",
        health: { status: "critical" },
        unavailableComponents: ["functionMetrics"],
      },
    });
  });

  it("returns the current health status when both reads complete", async () => {
    const handle = registerHealth(
      Promise.resolve({ status: "healthy", alerts: [] }),
      Promise.resolve([]),
    );

    const response = (await handle()) as {
      status_code: number;
      body: { status: string; health: { status: string } };
    };

    expect(response).toMatchObject({
      status_code: 200,
      body: { status: "healthy", health: { status: "healthy" } },
    });
  });
});
