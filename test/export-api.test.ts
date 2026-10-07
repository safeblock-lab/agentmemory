import { describe, expect, it, vi } from "vitest";

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));

import { registerApiTriggers } from "../src/triggers/api.js";

function mockSdk() {
  const functions = new Map<string, Function>();
  const calls: Array<{ function_id: string; payload: unknown }> = [];
  let nextResult: unknown;
  return {
    registerFunction(id: string, handler: Function) { functions.set(id, handler); },
    registerTrigger() {},
    async trigger(input: { function_id: string; payload: unknown }) {
      calls.push(input);
      return nextResult ?? input.payload;
    },
    setNextResult(result: unknown) { nextResult = result; },
    functions,
    calls,
  };
}

describe("api::export collection pagination", () => {
  it("forwards bounded collection pages and rejects invalid collection bounds", async () => {
    const sdk = mockSdk();
    registerApiTriggers(sdk as never, {} as never, "");
    const handler = sdk.functions.get("api::export")!;
    const response = await handler({
      headers: {},
      query_params: { collection: "graphNodes", offset: "0", limit: "2" },
    }) as { status_code: number; body: unknown };
    expect(response.status_code).toBe(200);
    expect(sdk.calls.at(-1)).toMatchObject({
      function_id: "mem::export",
      payload: { collection: "graphNodes", limit: 2 },
    });

    const continuation = await handler({
      headers: {},
      query_params: { collection: "graphNodes", cursor: "opaque-cursor", limit: "2" },
    }) as { status_code: number; body: unknown };
    expect(continuation.status_code).toBe(200);
    expect(sdk.calls.at(-1)).toMatchObject({
      function_id: "mem::export",
      payload: { collection: "graphNodes", cursor: "opaque-cursor", limit: 2 },
    });

    sdk.setNextResult({ success: false, oversized: true, error: "too large" });
    const oversized = await handler({
      headers: {},
      query_params: { collection: "graphNodes", offset: "0", limit: "2" },
    }) as { status_code: number; body: { oversized: boolean } };
    expect(oversized.status_code).toBe(413);
    expect(oversized.body.oversized).toBe(true);
    sdk.setNextResult(undefined);

    const beforeOffset = sdk.calls.length;
    const offset = await handler({
      headers: {},
      query_params: { collection: "graphNodes", offset: "4", limit: "2" },
    }) as { status_code: number; body: { error: string } };
    expect(offset.status_code).toBe(400);
    expect(offset.body.error).toContain("cursor");
    expect(sdk.calls).toHaveLength(beforeOffset);

    const beforeInvalid = sdk.calls.length;
    const invalid = await handler({
      headers: {},
      query_params: { collection: "unknown", offset: "0", limit: "1" },
    }) as { status_code: number; body: { error: string } };
    expect(invalid.status_code).toBe(400);
    expect(invalid.body.error).toContain("collection");
    expect(sdk.calls).toHaveLength(beforeInvalid);
  });

  it("preserves legacy maxSessions/offset forwarding", async () => {
    const sdk = mockSdk();
    registerApiTriggers(sdk as never, {} as never, "");
    await sdk.functions.get("api::export")!({
      headers: {},
      query_params: { maxSessions: "5", offset: "10" },
    });
    expect(sdk.calls.at(-1)?.payload).toEqual({ maxSessions: 5, offset: 10 });
  });
});
