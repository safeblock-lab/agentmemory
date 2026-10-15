import { afterEach, describe, expect, it, vi } from "vitest";
vi.mock("../src/state/reranker.js", () => ({ rerank: vi.fn(async (_query: string, results: unknown[]) => results) }));
import { reflectHarness } from "./helpers/reflect-harness.js";
import { KV } from "../src/state/schema.js";
import { REFLECT_CONTROL, REFLECT_JOBS } from "../src/functions/reflect-queue-types.js";

const fiveUnsupportedInsights = `<insights>
<insight confidence="0.9" title="Speed">This drastically reduces time.</insight>
<insight confidence="0.9" title="Type safety">This immediately eliminates runtime type-errors.</insight>
<insight confidence="0.9" title="Environments">This eliminates environment-specific failures.</insight>
<insight confidence="0.9" title="Capture">This guarantees continuous capture.</insight>
<insight confidence="0.9" title="Storage">This reduces orphaned records.</insight>
</insights>`;
const positiveInsight = '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>';
function configureProvider(h: ReturnType<typeof reflectHarness>, response: () => string, verdict: (candidate: string, index: number) => string = () => "supported") {
  let judgments = 0;
  const preparedCalls: Array<{ system: string; user: string; options?: unknown }> = [];
  const provider = h.provider as typeof h.provider & {
    prepareRequest?: (operation: string, system: string, user: string, options?: unknown) => { endpoint: string; body: string };
  };
  provider.prepareRequest = (_operation, system, user, options) => {
    preparedCalls.push({ system, user, options });
    return { endpoint: "POST https://reflect.test/summarize", body: JSON.stringify({ system, user, options }) };
  };
  h.provider.summarize.mockImplementation(async (systemPrompt: string, userPrompt: string) => {
    if (!systemPrompt.includes("You evaluate publication evidence")) return response();
    const payload = JSON.parse(userPrompt);
    const candidate = JSON.parse(payload.candidate);
    const index = judgments++;
    return JSON.stringify({ verdict: verdict(candidate.generatedResponse, index), reason: "The complete evidence was evaluated." });
  });
  return preparedCalls;
}

afterEach(() => {
  vi.unstubAllEnvs(); vi.useRealTimers();
});

describe("reflection publication evidence", () => {
  it("rejects all five unsupported benefit claims and continues the remaining source fragments", async () => {
    vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
    const h = reflectHarness();
    for (const id of ["one", "two"]) {
      const source = await h.kv.get<Record<string, unknown>>(KV.memories, id);
      await h.kv.set(KV.memories, id, { ...source, content: `Boundary validation ${id}. ${"Repeated input checks. ".repeat(650)}` });
    }
    configureProvider(h, (() => { let generations = 0; return () => generations++ === 0 ? fiveUnsupportedInsights : positiveInsight; })(),
      (candidate, index) => index === 0 ? "unsupported" : "supported");
    const { jobId } = await h.call("mem::reflect", { project: "/app", query: "boundaries" });

    const rejected = await h.call("mem::reflect-unit", { jobId });
    const afterRejected = await h.kv.get<any>(REFLECT_JOBS, jobId);
    const firstReceipt = await h.kv.get<any>(REFLECT_CONTROL, `${jobId}:result:0`);
    expect(rejected).toMatchObject({ skipped: true, status: "pending" });
    expect(afterRejected).toMatchObject({ unitSequence: 1, processedClusters: 1, appliedInsights: 0,
      deferredFamilies: expect.arrayContaining(["reflect_publication_unsupported"]) });
    expect(afterRejected.sourceCursor).toBeTruthy();
    expect(afterRejected.sourcePage.length).toBeGreaterThan(0);
    expect(firstReceipt).toMatchObject({ kind: "publication_rejected", reason: "reflect_publication_unsupported", appliedCount: 0 });
    expect(await h.kv.list(KV.insights)).toHaveLength(0);

    const advanced = await h.call("mem::reflect-unit", { jobId });
    const afterAdvanced = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(advanced.status).toBe("pending");
    expect(afterAdvanced.unitSequence).toBe(2);
    expect(afterAdvanced.appliedInsights).toBe(1);
    expect(await h.kv.list(KV.insights)).toMatchObject([expect.objectContaining({ content: "Validate input at boundaries." })]);
    expect(h.provider.summarize).toHaveBeenCalledTimes(4);
    const evidenceCalls = h.provider.summarize.mock.calls.filter(([system]) => String(system).includes("You evaluate publication evidence"));
    const candidate = JSON.parse(JSON.parse(String(evidenceCalls[0]![1])).candidate);
    expect(candidate.generatedResponse).toContain("drastically reduces time");
    expect(candidate.generatedResponse).toContain("eliminates environment-specific failures");
    expect(candidate.generationPrompt).toContain('"startByte":0');
    expect(candidate.sourceLineage).toEqual(expect.arrayContaining([expect.objectContaining({ id: "one", fingerprint: expect.any(String) })]));
  });

  it("retains an invalid generated response without retrying generation", async () => {
    vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
    const h = reflectHarness();
    h.provider.summarize.mockResolvedValue("not an insights document");
    const { jobId } = await h.call("mem::reflect", { project: "/app", query: "boundaries" });

    const first = await h.call("mem::reflect-unit", { jobId });
    const captured = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(first).toMatchObject({ status: "failed", reason: "reflect_generation_invalid" });
    expect(captured.captured.response).toBe("not an insights document");
    expect(await h.kv.list(KV.insights)).toHaveLength(0);

    captured.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, captured);
    const replay = await h.call("mem::reflect-unit", { jobId });
    expect(replay).toMatchObject({ skipped: true });
    expect(h.provider.summarize).toHaveBeenCalledTimes(1);
    expect((await h.kv.get<any>(REFLECT_JOBS, jobId)).captured.response).toBe("not an insights document");
  });

  it.each([
    ["invalid_response", "not JSON"],
    ["uncertain", "uncertain"],
    ["unknown", "hang"],
  ])("retains %s judge outcome without retrying generation or judge", async (mode, response) => {
    vi.stubEnv("AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", "0");
    const h = reflectHarness();
    let generationCalls = 0;
    let judgeCalls = 0;
    h.provider.summarize.mockImplementation(async (systemPrompt: string, userPrompt: string) => {
      if (!systemPrompt.includes("You evaluate publication evidence")) { generationCalls++; return positiveInsight; }
      judgeCalls++;
      if (response === "hang") return new Promise<string>(() => {});
      if (response === "not JSON") return response;
      return JSON.stringify({ verdict: "uncertain", reason: "The source evidence is ambiguous." });
    });
    const { jobId } = await h.call("mem::reflect", { project: "/app", query: "boundaries" });
    const first = h.call("mem::reflect-unit", { jobId });
    if (response === "hang") {
      vi.useFakeTimers();
      await vi.advanceTimersByTimeAsync(60_001);
    }
    await first;
    const original = await h.kv.get<any>(REFLECT_JOBS, jobId);
    expect(original.captured.response).toBe(positiveInsight);
    original.nextAttemptAt = 0;
    await h.kv.set(REFLECT_JOBS, jobId, original);
    await h.call("mem::reflect-unit", { jobId });
    expect(generationCalls).toBe(1);
    expect(judgeCalls).toBe(1);
    expect(await h.kv.list(KV.insights)).toHaveLength(0);
    expect((await h.kv.get<any>(REFLECT_JOBS, jobId)).captured.response).toBe(positiveInsight);
  });
});
