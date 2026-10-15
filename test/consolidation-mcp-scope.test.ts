import { beforeEach, describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../src/state/kv.js";
import { getAllTools } from "../src/mcp/tools-registry.js";

const recordContextUsageMock = vi.hoisted(() => vi.fn(async () => undefined));
const warnContextUsageFailureMock = vi.hoisted(() => vi.fn());

vi.mock("../src/functions/context-usage.js", () => ({
  recordContextUsage: recordContextUsageMock,
  warnContextUsageFailure: warnContextUsageFailureMock,
}));

import { registerMcpEndpoints } from "../src/mcp/server.js";

type ToolResponse = {
  status_code: number;
  body: unknown;
};

type TestHandler = (request: unknown) => Promise<ToolResponse>;

function createHarness() {
  const handlers = new Map<string, TestHandler>();
  const trigger = vi.fn(async () => ({ accepted: true }));
  const sdk = {
    registerFunction: vi.fn((id: string, handler: unknown) => {
      handlers.set(id, handler as TestHandler);
    }),
    registerTrigger: vi.fn(),
    trigger,
  } as unknown as IIIClient;

  registerMcpEndpoints(sdk, {} as StateKV);
  const call = handlers.get("mcp::tools::call");
  if (!call) throw new Error("MCP call handler was not registered");

  return {
    trigger,
    call: (args: unknown) => call({
      body: { name: "memory_consolidate", arguments: args },
    }),
  };
}

function responseBody(response: ToolResponse): {
  content?: Array<{ type: string; text: string }>;
  isError?: boolean;
} {
  return response.body as {
    content?: Array<{ type: string; text: string }>;
    isError?: boolean;
  };
}

describe("memory_consolidate MCP scope and continuation", () => {
  beforeEach(() => {
    recordContextUsageMock.mockClear();
    warnContextUsageFailureMock.mockClear();
  });

  it("advertises project and bounded returned continuation fields without changing tool count", () => {
    const tools = getAllTools();
    const tool = tools.find((item) => item.name === "memory_consolidate");

    expect(tools).toHaveLength(54);
    expect(tool).toBeDefined();
    expect(tool?.inputSchema.properties).toMatchObject({
      project: { type: "string", maxLength: 256 },
      batchSourceIds: {
        type: "array",
        minItems: 1,
        maxItems: 12,
        items: { type: "string", maxLength: 256 },
      },
      consolidationSelectionId: { type: "string", maxLength: 256 },
      consolidationManifestId: { type: "string", maxLength: 256 },
      decayManifestId: { type: "string", maxLength: 256 },
    });
    expect(tool?.inputSchema.properties).not.toHaveProperty("batchResponse");
    expect(tool?.inputSchema.properties).not.toHaveProperty("consolidationPartitionId");
  });

  it("preserves the exact project and pre-capture continuation values while whitelisting the payload", async () => {
    const harness = createHarness();
    const args = {
      tier: "semantic",
      project: " project-one ",
      force: false,
      deferred: true,
      batchSourceIds: [" source-one ", "source-two"],
      consolidationSelectionId: " selection-id ",
      consolidationCatalogProject: false,
      batchResponse: "untrusted provider callback",
      consolidationPartitionId: "untrusted-partition",
      batchEffectKey: "a".repeat(64),
    };

    const response = await harness.call(args);

    expect(response.status_code).toBe(200);
    expect(harness.trigger).toHaveBeenCalledOnce();
    expect(harness.trigger).toHaveBeenCalledWith({
      function_id: "mem::consolidate-pipeline",
      payload: {
        tier: "semantic",
        project: " project-one ",
        force: false,
        deferred: true,
        batchSourceIds: [" source-one ", "source-two"],
        consolidationSelectionId: " selection-id ",
        consolidationCatalogProject: false,
      },
    });
    expect(recordContextUsageMock).toHaveBeenCalledOnce();
  });

  it.each([
    ["consolidation manifest", { consolidationManifestId: " manifest-id " }],
    ["decay manifest", { decayManifestId: " decay-id " }],
  ])("forwards the exact %s continuation identity", async (_label, continuation) => {
    const harness = createHarness();

    await harness.call({ tier: "semantic", project: "project-one", ...continuation });

    expect(harness.trigger).toHaveBeenCalledWith({
      function_id: "mem::consolidate-pipeline",
      payload: { tier: "semantic", project: "project-one", ...continuation },
    });
  });

  it.each<[string, Record<string, unknown>]>([
    ["non-string project", { project: 42 }],
    ["empty project", { project: "   " }],
    ["oversized project", { project: "p".repeat(257) }],
    ["oversized continuation ID", { consolidationSelectionId: "i".repeat(257) }],
    ["invalid boolean", { force: "true" }],
    ["too many source IDs", { batchSourceIds: Array.from({ length: 13 }, (_, i) => `source-${i}`) }],
    ["non-string source ID", { batchSourceIds: ["source-one", 2] }],
    ["multiple continuation modes", { consolidationManifestId: "manifest-one", decayManifestId: "decay-one" }],
  ])("rejects %s before the pipeline is invoked", async (_label, invalid) => {
    const harness = createHarness();

    const response = await harness.call({ tier: "semantic", ...invalid });

    expect(response.status_code).toBe(200);
    expect(responseBody(response).isError).toBe(true);
    expect(harness.trigger).not.toHaveBeenCalled();
  });

  it.each([null, [], "invalid"])("rejects non-object arguments (%s)", async (args) => {
    const harness = createHarness();

    const response = await harness.call(args);

    expect(responseBody(response).isError).toBe(true);
    expect(harness.trigger).not.toHaveBeenCalled();
  });
});
