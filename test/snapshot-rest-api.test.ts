import { describe, expect, it, vi } from "vitest";
import { registerApiTriggers } from "../src/triggers/api.js";
import { registerSnapshotFunction } from "../src/functions/snapshot.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { semanticProjectCheckpointKey } from "../src/functions/consolidation-manifest.js";
import { effectHarness } from "./batch-effects-harness.js";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { KV } from "../src/state/schema.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import type { ConsolidationCanonicalLookup, ConsolidationCanonicalWriter } from "../src/functions/consolidation-selection.js";
import type { SessionSummary } from "../src/types.js";

type Response = { status_code: number; body: unknown };

function semanticFixtureResponse(prompt: string): string {
  const candidatesMarker = "Candidate facts with source evidence:\n\n";
  if (prompt.includes(candidatesMarker)) {
    const candidates = JSON.parse(prompt.slice(prompt.indexOf(candidatesMarker) + candidatesMarker.length)) as Array<{ candidateIds: string[] }>;
    const candidateIds = [...new Set(candidates.flatMap((candidate) => candidate.candidateIds))];
    return `<facts><fact confidence="0.9" candidateIds="${candidateIds.join(",")}">Preserve source evidence</fact></facts>`;
  }
  const episodesMarker = "Extract factual candidates from these episodes and identify their supporting session IDs:\n\n";
  if (prompt.includes(episodesMarker)) {
    const sourceIds = [...new Set([...prompt.matchAll(/^Session ID: ([^\r\n]+)$/gm)].map((match) => match[1]))];
    return `<facts><fact confidence="0.9" sourceIds="${sourceIds.join(",")}">Preserve source evidence</fact></facts>`;
  }
  const fragmentsMarker = "Analyze these original source fragments with their provenance:\n\n";
  const fragments = JSON.parse(prompt.slice(prompt.indexOf(fragmentsMarker) + fragmentsMarker.length)) as Array<{ sourceId: string }>;
  const sourceIds = [...new Set(fragments.map((fragment) => fragment.sourceId))];
  return `<facts><fact confidence="0.9" sourceIds="${sourceIds.join(",")}">Preserve source evidence</fact></facts>`;
}

function registerSnapshot(result: unknown, secret = "") {
  const handlers = new Map<string, (request: unknown) => Promise<Response>>();
  const trigger = vi.fn(async () => result);
  const sdk = {
    registerFunction: (id: string, handler: (request: unknown) => Promise<Response>) => handlers.set(id, handler),
    registerTrigger: () => {},
    trigger,
  };
  registerApiTriggers(sdk as never, { list: async () => [], get: async () => null } as never, secret);
  return { handle: handlers.get("api::snapshot-create")!, trigger };
}

describe("api::snapshot-create status", () => {
  it("returns 201 with the created snapshot", async () => {
    const result = { success: true, snapshot: { commitHash: "abc1234" } };
    const { handle, trigger } = registerSnapshot(result);
    expect(await handle({ body: { message: "manual" }, headers: {} })).toEqual({ status_code: 201, body: result });
    expect(trigger).toHaveBeenCalledWith({ function_id: "mem::snapshot-create", payload: { message: "manual" }, timeoutMs: 600000 });
  });

  it.each([
    { success: false, deferred: true, retryable: true, code: "BATCH_MAINTENANCE_BUSY", details: { activeAdmissions: 1 } },
    { success: false, code: "BATCH_MAINTENANCE_BUSY" },
  ])("returns 503 without hiding busy details: %j", async (result) => {
    const { handle } = registerSnapshot(result);
    expect(await handle({ headers: {} })).toEqual({ status_code: 503, body: result });
  });

  it.each([{ success: false, error: "Snapshot failed" }, null, undefined])("returns 500 for unsuccessful results: %j", async (result) => {
    const { handle } = registerSnapshot(result);
    expect(await handle({ headers: {} })).toEqual({ status_code: 500, body: result });
  });

  it("preserves the disabled endpoint response", async () => {
    const { handle, trigger } = registerSnapshot(null);
    trigger.mockRejectedValueOnce(new Error("Function not registered"));
    expect(await handle({ headers: {} })).toEqual({ status_code: 404, body: { error: "Snapshots not enabled" } });
  });

  it("rejects unauthorized callers before creating a snapshot", async () => {
    const { handle, trigger } = registerSnapshot({ success: true }, "test-secret");
    expect(await handle({ headers: {} })).toEqual({ status_code: 401, body: { error: "unauthorized" } });
    expect(trigger).not.toHaveBeenCalled();
    expect((await handle({ headers: { authorization: "Bearer test-secret" } })).status_code).toBe(201);
  });

  it("returns 503 while a real semantic callback awaits its provider, then preserves its completed facts", async () => {
    vi.useFakeTimers();
    const h = effectHarness();
    const sdk = { ...h.sdk, registerTrigger: () => {} };
    const snapshotDir = resolve(".native-pagination-build/background-health-20261004/verification",
      `snapshot-busy-test-${process.pid}-${Date.now()}`);
    let entered!: () => void, finish!: () => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const providerResult = new Promise<void>((resolve) => { finish = resolve; });
    const provider = {
      compress: vi.fn(),
      summarize: vi.fn(async (system: string, prompt: string) => {
        if (system.startsWith("You evaluate publication evidence.")) {
          const evidence = JSON.parse(prompt);
          expect(evidence.sources).toHaveLength(5);
          return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "The fixture candidate preserves the five source records." });
        }
        entered(); await providerResult;
        return semanticFixtureResponse(prompt);
      }),
    };
    registerApiTriggers(sdk as never, h.kv, "");
    registerSnapshotFunction(sdk as never, h.kv, snapshotDir);
    Object.assign(h.kv, { indexedRetrieval: true });
    for (let index = 0; index < 5; index++) {
      const summary = {
        sessionId: `ses_${index}`, project: "test-project", title: "Source evidence", narrative: "Retain original records",
        concepts: ["evidence"], createdAt: new Date(2026, 0, index + 1).toISOString(),
      };
      h.seed(KV.summaries, `ses_${index}`, summary);
      h.seed(KV.sessions, `ses_${index}`, {
        id: `ses_${index}`, project: "test-project", cwd: "D:/test", startedAt: summary.createdAt,
        status: "completed", observationCount: 1,
      });
    }
    const selectSources = async (project: string) => (await h.kv.list<SessionSummary>(KV.summaries))
      .filter((summary) => summary.project === project)
      .map((summary) => ({ id: summary.sessionId, sessionId: summary.sessionId, rowVersion: "1" }));
    const lookup: ConsolidationCanonicalLookup = async () => ({ status: "ready", version: "1", canonicalId: null });
    const write: ConsolidationCanonicalWriter = async (scope, key, match, proposedId, effect, update) => {
      const id = match.canonicalId ?? proposedId;
      const old = await h.kv.get<Record<string, unknown>>(KV[scope], id);
      const applied = await applyBatchEffect(h.kv, KV[scope], id, effect, update as never);
      return {
        applied: !(old?.appliedBatchEffects as string[] | undefined)?.includes(effect),
        created: !old,
        id,
        rowVersion: "1",
        receipt: {
          operationKind: "upsert",
          effectKey: effect,
          operationFingerprint: batchEffectKey(key),
          id,
          rowVersion: "1",
          created: !old,
          resultFingerprint: batchEffectKey(JSON.stringify(applied)),
        },
      };
    };
    registerConsolidationPipelineFunction(sdk as never, h.kv, provider as never, undefined, undefined, undefined,
      undefined, selectSources, lookup, undefined, write);
    const consolidation = (async () => {
      let response = await h.call<Response>("api::consolidate-pipeline", {
        headers: {}, body: { tier: "semantic", force: true, project: "test-project" },
      });
      let manifestId = (response.body as { results?: { semantic?: { continuation?: { consolidationManifestId?: string } } } })
        .results?.semantic?.continuation?.consolidationManifestId;
      for (let step = 0; manifestId && step < 40; step += 1) {
        response = await h.call("api::consolidate-pipeline", { headers: {}, body: { consolidationManifestId: manifestId } });
        manifestId = (response.body as { continuation?: { consolidationManifestId?: string } }).continuation?.consolidationManifestId;
      }
      if (manifestId) throw new Error("Consolidation continuation limit exceeded before completion");
      return response;
    })();
    const delivery = consolidation.then(() => h.drainDeliveries());
    try {
      await Promise.race([started, consolidation.then(() => { throw new Error("Consolidation completed before provider invocation"); })]);
      expect(provider.summarize).toHaveBeenCalledTimes(1);
      const snapshot = h.call<Response>("api::snapshot-create", { headers: {}, body: {} });
      await vi.advanceTimersByTimeAsync(30001);
      expect(await snapshot).toMatchObject({ status_code: 503, body: {
        success: false, deferred: true, retryable: true, code: "BATCH_MAINTENANCE_BUSY",
        details: { families: [{ family: "consolidation", count: 1 }] },
      } });
      expect(existsSync(snapshotDir)).toBe(false);
      finish();
      expect(await consolidation).toMatchObject({ status_code: 200, body: { success: true } });
      await delivery;
      expect(await h.kv.list<Record<string, unknown>>(KV.semantic)).toMatchObject([{
        sourceSessionIds: expect.arrayContaining(["ses_0", "ses_1", "ses_2", "ses_3", "ses_4"]),
      }]);
      expect(await h.kv.get(KV.state, semanticProjectCheckpointKey("test-project"))).toBeTruthy();
      expect(provider.summarize).toHaveBeenCalled();
    } finally { finish(); await consolidation; await delivery; vi.useRealTimers(); }
  });
});
