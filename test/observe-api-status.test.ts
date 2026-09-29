import { describe, expect, it } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";

function registerObserve(result: unknown) {
  const handlers = new Map<string, (request: unknown) => Promise<unknown>>();
  const sdk = {
    registerFunction: (id: string, handler: (request: unknown) => Promise<unknown>) => handlers.set(id, handler),
    registerTrigger: () => {},
    trigger: async () => result,
  };
  const kv = { list: async () => [], get: async () => null };
  registerApiTriggers(sdk as never, kv as never, "");
  return handlers.get("api::observe")!;
}

const request = {
  body: {
    hookType: "post_tool_use",
    sessionId: "ses_test",
    project: "agentmemory",
    cwd: "D:\\agentmemory",
    timestamp: "2026-09-29T00:00:00.000Z",
    data: {},
  },
  headers: {},
};

describe("api::observe status", () => {
  it("returns 409 when the configured session cap rejects an observation", async () => {
    const handle = registerObserve({ success: false, code: "SESSION_OBSERVATION_LIMIT", error: "Session observation limit reached (1)" });
    const response = await handle(request) as { status_code: number; body: { code: string } };
    expect(response.status_code).toBe(409);
    expect(response.body.code).toBe("SESSION_OBSERVATION_LIMIT");
  });

  it("returns 201 only for an accepted observation", async () => {
    const handle = registerObserve({ observationId: "obs_1" });
    const response = await handle(request) as { status_code: number };
    expect(response.status_code).toBe(201);
  });

  it("does not report success when the observation function returns no result", async () => {
    const handle = registerObserve(null);
    const response = await handle(request) as { status_code: number };
    expect(response.status_code).toBe(500);
  });
});
