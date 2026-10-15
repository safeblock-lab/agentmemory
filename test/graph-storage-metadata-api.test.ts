import { describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";
import { StateKV } from "../src/state/kv.js";
import type { GraphStorageMetadataPrepareResult } from "../src/state/storage.js";

const secret = "graph-metadata-test-secret";
const completeResult: GraphStorageMetadataPrepareResult = {
  status: "complete",
  complete: true,
  rows: 42,
  logical_bytes: 12_345,
  stored_bytes: 8_765,
  sampled_at: "2026-10-10T12:00:00.000Z",
  scope_count: 7,
  scopes_completed: 7,
  rows_scanned: 42,
};

type ApiResponse = { status_code: number; body: unknown };
type ApiRequest = { headers?: Record<string, string>; body?: unknown };
type ApiHandler = (request: ApiRequest) => Promise<ApiResponse>;

function apiBoundary(configuredSecret: string | undefined = secret) {
  const handlers = new Map<string, ApiHandler>();
  const prepareGraphStorageMetadata = vi.fn(async () => completeResult);
  const sdk = {
    registerFunction: (id: string, handler: ApiHandler) => handlers.set(id, handler),
    registerTrigger: vi.fn(),
    trigger: vi.fn(async () => null),
  };
  registerApiTriggers(sdk as never, { prepareGraphStorageMetadata } as never, configuredSecret);
  return {
    handle: handlers.get("api::storage-maintenance")!,
    prepareGraphStorageMetadata,
    trigger: sdk.trigger,
  };
}

function authorized(body: unknown): ApiRequest {
  return { headers: { authorization: `Bearer ${secret}` }, body };
}

describe("graph storage metadata preparation REST boundary", () => {
  it("rejects unauthorized requests before invoking the native preparation method", async () => {
    const boundary = apiBoundary();

    const response = await boundary.handle({
      headers: { authorization: "Bearer wrong-secret" },
      body: { action: "graph_metadata_prepare" },
    });

    expect(response).toEqual({ status_code: 401, body: { error: "unauthorized" } });
    expect(boundary.prepareGraphStorageMetadata).not.toHaveBeenCalled();
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("requires a configured secret before allowing preparation", async () => {
    const boundary = apiBoundary("");

    const response = await boundary.handle({ body: { action: "graph_metadata_prepare" } });

    expect(response.status_code).toBe(503);
    expect(response.body).toEqual({ error: "Storage maintenance requires AGENTMEMORY_SECRET" });
    expect(boundary.prepareGraphStorageMetadata).not.toHaveBeenCalled();
  });

  it("forwards only the bounded preparation fields and returns the result", async () => {
    const boundary = apiBoundary();
    const response = await boundary.handle(authorized({
      action: "graph_metadata_prepare",
      limit: 1,
      max_bytes: 8192,
      max_ms: 1,
      extra: "discard this",
      scope: "caller-controlled",
    }));

    expect(response).toEqual({ status_code: 200, body: completeResult });
    expect(boundary.prepareGraphStorageMetadata).toHaveBeenCalledExactlyOnceWith({
      limit: 1,
      max_bytes: 8192,
      max_ms: 1,
    });
    expect(boundary.trigger).not.toHaveBeenCalled();
  });

  it("accepts the largest documented preparation limits", async () => {
    const boundary = apiBoundary();
    await boundary.handle(authorized({
      action: "graph_metadata_prepare",
      limit: 1024,
      max_bytes: 1_048_576,
      max_ms: 3000,
    }));

    expect(boundary.prepareGraphStorageMetadata).toHaveBeenCalledExactlyOnceWith({
      limit: 1024,
      max_bytes: 1_048_576,
      max_ms: 3000,
    });
  });

  it.each([
    ["limit below minimum", { limit: 0 }],
    ["limit above maximum", { limit: 1025 }],
    ["fractional limit", { limit: 1.5 }],
    ["string limit", { limit: "1" }],
    ["NaN limit", { limit: Number.NaN }],
    ["infinite limit", { limit: Number.POSITIVE_INFINITY }],
    ["max_bytes below minimum", { max_bytes: 8191 }],
    ["max_bytes above maximum", { max_bytes: 1_048_577 }],
    ["fractional max_bytes", { max_bytes: 8192.5 }],
    ["string max_bytes", { max_bytes: "8192" }],
    ["infinite max_bytes", { max_bytes: Number.POSITIVE_INFINITY }],
    ["max_ms below minimum", { max_ms: 0 }],
    ["max_ms above maximum", { max_ms: 3001 }],
    ["fractional max_ms", { max_ms: 1.25 }],
    ["string max_ms", { max_ms: "1" }],
    ["NaN max_ms", { max_ms: Number.NaN }],
  ])("rejects %s before any preparation", async (_label, invalidField) => {
    const boundary = apiBoundary();
    const response = await boundary.handle(authorized({
      action: "graph_metadata_prepare",
      limit: 256,
      max_bytes: 262_144,
      max_ms: 3000,
      ...(invalidField as Record<string, unknown>),
    }));

    expect(response.status_code).toBe(400);
    expect(response.body).toMatchObject({ error: expect.stringContaining("graph_metadata_prepare requires") });
    expect(boundary.prepareGraphStorageMetadata).not.toHaveBeenCalled();
  });

  it("defaults omitted bounds to the safe preparation request", async () => {
    const boundary = apiBoundary();
    await boundary.handle(authorized({ action: "graph_metadata_prepare", unrelated: true }));

    expect(boundary.prepareGraphStorageMetadata).toHaveBeenCalledExactlyOnceWith({
      limit: 256,
      max_bytes: 262_144,
      max_ms: 3000,
    });
  });

  it("returns native unavailability without presenting it as a completed scan", async () => {
    const unavailable: GraphStorageMetadataPrepareResult = {
      status: "unavailable",
      complete: false,
      rows: null,
      logical_bytes: null,
      stored_bytes: null,
      sampled_at: null,
      scope_count: 7,
      scopes_completed: 0,
      rows_scanned: 1,
      reason: "metadata_limit_exceeded",
    };
    const boundary = apiBoundary();
    boundary.prepareGraphStorageMetadata.mockResolvedValue(unavailable);

    const response = await boundary.handle(authorized({ action: "graph_metadata_prepare" }));

    expect(response.status_code).toBe(200);
    expect(response.body).toEqual(unavailable);
  });

  it("does not hide native preparation errors", async () => {
    const boundary = apiBoundary();
    const error = new Error("native preparation failed");
    boundary.prepareGraphStorageMetadata.mockRejectedValue(error);

    await expect(boundary.handle(authorized({ action: "graph_metadata_prepare" }))).rejects.toBe(error);
  });
});

describe("StateKV graph metadata preparation contract", () => {
  it("sends the exact state storage action and returns a valid native result", async () => {
    const trigger = vi.fn(async () => completeResult);
    const kv = new StateKV({ trigger } as never);

    const result = await kv.prepareGraphStorageMetadata({ limit: 64, max_bytes: 32_768, max_ms: 275 });

    expect(trigger).toHaveBeenCalledExactlyOnceWith({
      function_id: "state::storage",
      payload: { action: "graph_metadata_prepare", limit: 64, max_bytes: 32_768, max_ms: 275 },
    });
    expect(result).toEqual(completeResult);
  });

  it("rejects malformed or over-limit native results", async () => {
    const trigger = vi.fn(async () => ({ ...completeResult, rows_scanned: 18 }));
    const kv = new StateKV({ trigger } as never);

    await expect(kv.prepareGraphStorageMetadata({ limit: 17, max_bytes: 32_768, max_ms: 275 }))
      .rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
  });

  it("propagates state storage transport errors", async () => {
    const error = new Error("state storage unavailable");
    const trigger = vi.fn(async () => { throw error; });
    const kv = new StateKV({ trigger } as never);

    await expect(kv.prepareGraphStorageMetadata({ limit: 17, max_bytes: 32_768, max_ms: 275 }))
      .rejects.toBe(error);
  });
});
