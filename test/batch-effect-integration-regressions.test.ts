import { describe, expect, it, vi } from "vitest";
import { effectHarness } from "./batch-effects-harness.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import type { ConsolidationCanonicalLookup, ConsolidationCanonicalWriter } from "../src/functions/consolidation-selection.js";
import type { ConsolidationDecaySelector } from "../src/functions/consolidation-decay.js";
import type { SessionSummary } from "../src/types.js";

function registerPipeline(h: ReturnType<typeof effectHarness>): void {
  const lookup: ConsolidationCanonicalLookup = async () => ({ status: "ready", canonicalId: null, version: "1" });
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
  const decay: ConsolidationDecaySelector = Object.assign(
    async (scope: "semantic" | "procedural", sessions: string[], _cursor: string | null, project: string) => ({
      items: (await h.kv.list<Record<string, unknown>>(KV[scope]))
        .filter(row => row.project === project && (row.sourceSessionIds as string[] | undefined)?.some(id => sessions.includes(id)))
        .map(row => ({ id: String(row.id), rowVersion: "1" })),
      nextCursor: null,
      membershipGeneration: "1",
      coverageEpoch: "1",
    }),
    {
      apply: async (scope, ref, sessions, project, effect, _operation, update) => {
        const old = await h.kv.get<Record<string, unknown>>(KV[scope], ref.id);
        if (!old || old.project !== project || !(old.sourceSessionIds as string[] | undefined)?.some(id => sessions.includes(id))) {
          throw new Error("decay fixture scope changed");
        }
        const applied = await applyBatchEffect(h.kv, KV[scope], ref.id, effect, update as never);
        return {
          applied: !(old.appliedBatchEffects as string[] | undefined)?.includes(effect),
          created: false,
          id: ref.id,
          rowVersion: "1",
          receipt: {
            operationKind: "decay",
            effectKey: effect,
            operationFingerprint: effect,
            id: ref.id,
            rowVersion: "1",
            created: false,
            resultFingerprint: batchEffectKey(JSON.stringify(applied)),
          },
        };
      },
    },
  );
  const provider = { summarize: vi.fn(async (system: string, prompt: string) => {
    expect(system).toMatch(/^You evaluate publication evidence\./);
    const evidence = JSON.parse(prompt);
    expect(evidence.workflow).toBe("consolidation");
    for (const source of evidence.sources) {
      expect(source.content).toBe(JSON.stringify(await h.kv.get(source.scope, source.key)));
      expect(source.project).toBe("test-project");
    }
    return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "Original fixture records support the callback candidate." });
  }) };
  registerConsolidationPipelineFunction(h.sdk as never, h.kv, provider as never, undefined, undefined, undefined, undefined, undefined, lookup, decay, write);
}

function summaries(): SessionSummary[] {
  return Array.from({ length: 5 }, (_, index) => ({
    sessionId: `session-${index}`,
    project: "test-project",
    title: `Summary ${index}`,
    narrative: `Durable change ${index}`,
    concepts: [],
    createdAt: `2026-01-0${index + 1}T00:00:00.000Z`,
  }));
}

function semanticFingerprint(items: SessionSummary[]): string {
  return fingerprintId("fwbconsem", JSON.stringify(items.map(summary => [
    summary.sessionId,
    summary.title,
    summary.narrative,
    summary.concepts,
    summary.createdAt,
  ])));
}

describe("legacy consolidation callback replay", () => {
  it("persists and replays the exact semantic stage selected by a tier-all fingerprint", async () => {
    const h = effectHarness();
    registerPipeline(h);
    const selected = summaries();
    for (const summary of selected) {
      await h.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: summary.project });
      await h.kv.set(KV.summaries, summary.sessionId, summary);
    }
    const reflect = vi.fn(async () => ({ success: true, completed: true }));
    h.sdk.registerFunction("mem::reflect", async (payload: never) => reflect(payload));
    const key = batchEffectKey("legacy-all-semantic-stage");
    const payload = {
      tier: "all",
      project: "test-project",
      batchSourceIds: selected.map(summary => summary.sessionId),
      batchSourceFingerprint: semanticFingerprint(selected),
      force: true,
      batchResponse: '<fact confidence="0.7">Persist this semantic fact</fact>',
      batchEffectKey: key,
    };

    h.crash(KV.semantic, true);
    await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow("lost write acknowledgement");
    expect(await h.kv.get(KV.batchCallbacks, `consolidation:${key}`)).toMatchObject({
      state: "started",
      semanticSourceIds: selected.map(summary => summary.sessionId),
      resultHash: expect.stringMatching(/^concallback_[a-f0-9]{16}$/),
    });

    const result = await h.call<Record<string, unknown>>("mem::consolidate-pipeline", payload);
    expect(result).toMatchObject({ success: true, completed: true, results: { semantic: { newFacts: 0 } } });
    expect(await h.kv.list(KV.semantic)).toHaveLength(1);
    expect(reflect).toHaveBeenCalledTimes(1);
    expect(result.results).not.toHaveProperty("procedural");
    expect(result.results).toHaveProperty("decay");
  });

  it.each([
    { sourceFingerprint: undefined, batchResponse: "<result/>", reason: "legacy_consolidation_stage_identity_required" },
    { sourceFingerprint: fingerprintId("foreign", "captured"), batchResponse: "<facts/>", reason: "legacy_consolidation_stage_identity_invalid" },
    { sourceFingerprint: fingerprintId("fwbconsem", "foreign-capture"), batchResponse: '<fact confidence="0.8">Foreign capture</fact>', reason: "legacy_consolidation_stage_identity_mismatch" },
  ])("rejects ambiguous or foreign tier-all callback identity before effects ($reason)", async ({ sourceFingerprint, batchResponse, reason }) => {
    const h = effectHarness();
    registerPipeline(h);
    const selected = summaries();
    for (const summary of selected) {
      await h.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: summary.project });
      await h.kv.set(KV.summaries, summary.sessionId, summary);
    }
    const key = batchEffectKey(`legacy-all-invalid:${reason}`);
    const result = await h.call<Record<string, unknown>>("mem::consolidate-pipeline", {
      tier: "all",
      project: "test-project",
      batchSourceIds: selected.map(summary => summary.sessionId),
      batchResponse,
      batchSourceFingerprint: sourceFingerprint,
      batchEffectKey: key,
      force: true,
    });

    expect(result).toMatchObject({ success: false, completed: false, retained: true, reason });
    expect(await h.kv.get(KV.batchCallbacks, `consolidation:${key}`)).toBeNull();
    expect(await h.kv.list(KV.semantic)).toHaveLength(0);
    expect(await h.kv.list(KV.procedural)).toHaveLength(0);
  });

  it("routes a tier-all procedural callback only to its captured procedural stage", async () => {
    const h = effectHarness();
    registerPipeline(h);
    const patternSources = ["pattern-a", "pattern-b"].map((id, index) => ({
      id,
      project: "test-project",
      content: `Repeated workflow ${index}`,
      type: "pattern",
      isLatest: true,
      sessionIds: ["session-a", "session-b"],
      updatedAt: "2026-01-01T00:00:00.000Z",
    }));
    for (const source of patternSources) {
      for (const id of source.sessionIds) await h.kv.set(KV.sessions, id, { id, project: source.project });
      await h.kv.set(KV.memories, source.id, source);
    }
    const reflect = vi.fn(async () => ({ success: true, completed: true }));
    h.sdk.registerFunction("mem::reflect", async (payload: never) => reflect(payload));
    const patterns = patternSources.map(source => ({ content: source.content, frequency: source.sessionIds.length }));
    const key = batchEffectKey("legacy-all-procedural-stage");
    const result = await h.call<Record<string, unknown>>("mem::consolidate-pipeline", {
      tier: "all",
      project: "test-project",
      batchSourceIds: patternSources.map(source => source.id),
      batchSourceFingerprint: fingerprintId("fwbconproc", JSON.stringify(patterns)),
      batchResponse: "<procedures><procedure name=\"Safe retry\" trigger=\"A callback loses its acknowledgement\"><step>Replay the captured operation</step><step>Check its durable effect receipt</step></procedure></procedures>",
      batchEffectKey: key,
      force: true,
    });

    expect(result).toMatchObject({ success: true, completed: true, results: { procedural: { newProcedures: 1 } } });
    expect(await h.kv.list(KV.procedural)).toHaveLength(1);
    expect(reflect).not.toHaveBeenCalled();
    expect(result.results).not.toHaveProperty("semantic");
    expect(result.results).not.toHaveProperty("decay");
  });

  it("reuses the captured decay timestamp after a lost effect acknowledgement while allowing a new scheduled period", async () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    try {
      vi.setSystemTime(new Date("2026-04-01T00:00:00.000Z"));
      const h = effectHarness();
      registerPipeline(h);
      const source = "decay-source";
      await h.kv.set(KV.sessions, source, { id: source, project: "test-project" });
      const start = "2026-03-01T00:00:00.000Z";
      await h.kv.set(KV.summaries, source, { sessionId: source, project: "test-project", title: "Summary", narrative: "Durable", concepts: [], createdAt: start });
      await h.kv.set(KV.semantic, "decay-semantic", {
        id: "decay-semantic",
        project: "test-project",
        sourceSessionIds: [source],
        strength: 1,
        updatedAt: start,
        lastAccessedAt: start,
      });
      const key = batchEffectKey("lost-ack-captured-decay");
      const payload = {
        tier: "decay",
        project: "test-project",
        batchSourceIds: [source],
        force: true,
        batchResponse: "<facts/>",
        batchEffectKey: key,
      };

      h.crash(KV.semantic, true);
      await expect(h.call("mem::consolidate-pipeline", payload)).rejects.toThrow("lost write acknowledgement");
      const receipt = await h.kv.get<Record<string, unknown>>(KV.batchCallbacks, `consolidation:${key}`);
      expect(receipt).toMatchObject({ state: "started", effectTimestamp: "2026-04-01T00:00:00.000Z" });
      const firstManifest = (await h.kv.list<Record<string, unknown>>(KV.state))
        .find(value => value.kind === "consolidation-decay");
      expect(firstManifest).toMatchObject({ at: "2026-04-01T00:00:00.000Z" });

      vi.setSystemTime(new Date("2026-06-01T00:00:00.000Z"));
      await h.call("mem::consolidate-pipeline", payload);
      const replayedManifest = (await h.kv.list<Record<string, unknown>>(KV.state))
        .filter(value => value.kind === "consolidation-decay");
      expect(replayedManifest).toHaveLength(1);
      expect(replayedManifest[0]).toMatchObject({ id: firstManifest!.id, at: firstManifest!.at });
      expect(await h.kv.get(KV.semantic, "decay-semantic")).toMatchObject({ strength: 0.9 });

      const next = await h.call<Record<string, unknown>>("mem::consolidate-pipeline", {
        tier: "decay", project: "test-project", batchSourceIds: [source], force: true,
      });
      expect(next).toMatchObject({ success: true, completed: false, queued: true });
      const manifests = (await h.kv.list<Record<string, unknown>>(KV.state))
        .filter(value => value.kind === "consolidation-decay");
      expect(manifests).toHaveLength(2);
      expect(manifests.some(value => value.id !== firstManifest!.id)).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });
});
