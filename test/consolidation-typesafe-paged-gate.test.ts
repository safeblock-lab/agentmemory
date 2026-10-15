import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KV } from "../src/state/schema.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { loadRoot, pageKey, semanticProjectCheckpointKey } from "../src/functions/consolidation-manifest.js";
import type { TypeSafeDecisionProvider } from "../src/providers/typesafe.js";
import { effectHarness } from "./batch-effects-harness.js";
import { applyBatchEffect, batchEffectKey } from "../src/state/batch-effects.js";
import type { SessionSummary } from "../src/types.js";

function choice(choice: "keep" | "drop" | "skip" | "run", confidence = 0.99) {
  return { type: "choice" as const, choice, confidence, probabilities: { [choice]: confidence, other: 1 - confidence } };
}

function factsForPrompt(prompt: string): string {
  const body = prompt.replace(/^Consolidation manifest: [^\n]+\n\n/, "");
  const json = body.slice(body.indexOf("\n\n") + 2);
  const input = JSON.parse(json) as Array<Record<string, unknown>>;
  if (input.every(item => typeof item.sourceId === "string")) {
    const sourceIds = [...new Set(input.map(item => String(item.sourceId)))];
    return `<facts><fact confidence="0.95" sourceIds="${sourceIds.join(",")}">Stable setup decision</fact></facts>`;
  }
  const candidateIds = [...new Set(input.flatMap(item => Array.isArray(item.candidateIds) ? item.candidateIds.map(String) : []))];
  return `<facts><fact confidence="0.95" candidateIds="${candidateIds.join(",")}">Stable setup decision</fact></facts>`;
}

function routineTypeSafe(gateChoice: "skip" | "run" = "skip", dropFirst = true) {
  return {
    evaluate: vi.fn(async (_feature: string, _state: unknown, questions: Record<string, unknown>) =>
      Object.fromEntries(Object.keys(questions).map((id, index) => [id, choice(dropFirst && index === 0 ? "drop" : "keep")]))),
    evaluateChoice: vi.fn(async () => choice(gateChoice)),
  };
}

async function fixture(count = 5) {
  const h = effectHarness();
  const { kv, sdk } = h;
  const summaries: SessionSummary[] = Array.from({ length: count }, (_, i) => ({ sessionId: `s${i}`, project: "p",
    createdAt: `2026-10-${String(i + 1).padStart(2, "0")}T00:00:00.000Z`, title: `Decision ${i}`,
    narrative: "Grounded stable decision", concepts: ["setup"], keyDecisions: [], filesModified: [], observationCount: 5 }));
  for (const summary of summaries) {
    await kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    await kv.set(KV.summaries, summary.sessionId, summary);
  }
  const select = vi.fn(async () => summaries.map(summary => ({ id: `o-${summary.sessionId}`, sessionId: summary.sessionId, rowVersion: "1" })));
  const lookup = vi.fn(async () => ({ status: "ready" as const, canonicalId: null, version: "1" }));
  let applied = 0;
  const write = vi.fn(async (scope: "semantic" | "procedural", key: string, _match: unknown, id: string, effect: string, update: (current: any) => any) => {
    const old = await kv.get<Record<string, unknown>>(KV[scope], id);
    const value = await applyBatchEffect(kv as never, KV[scope], id, effect, update as never);
    const wasApplied = !old?.appliedBatchEffects?.includes(effect);
    if (wasApplied) applied++;
    const rowVersion = (await kv.getVersioned(KV[scope], id)).version;
    return { applied: wasApplied, created: !old, id, rowVersion,
      receipt: { operationKind: "upsert" as const, effectKey: effect, operationFingerprint: batchEffectKey(key), id, rowVersion,
        created: !old, resultFingerprint: batchEffectKey(JSON.stringify(value)) } };
  });
  const provider = { summarize: vi.fn(async (system: string, prompt: string) => {
    if (system.startsWith("You evaluate publication evidence.")) {
      const evidence = JSON.parse(prompt);
      for (const source of evidence.sources) {
        expect(source.content).toBe(JSON.stringify(await kv.get(source.scope, source.key)));
        expect(source.version).toBe((await kv.getVersioned(source.scope, source.key)).version);
      }
      return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict: "supported", reason: "Original fixture records support the complete candidate." });
    }
    if (system.includes("procedur")) return '<procedures><procedure name="Safe setup" trigger="Setting up the project"><step>Read the project configuration</step><step>Apply the documented setup</step></procedure></procedures>';
    return factsForPrompt(prompt);
  }) };
  const register = (typeSafe?: TypeSafeDecisionProvider) => registerConsolidationPipelineFunction(sdk as never, kv as never,
    provider as never, undefined, undefined, undefined, typeSafe, select, lookup, undefined, write as never);
  register();
  async function drive(input: Record<string, unknown>, maximum = 3000) {
    let result = await sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: input }) as any;
    for (let cut = 0; result.queued && result.continuation && cut < maximum; cut++) {
      result = await sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: result.continuation }) as any;
    }
    if (result.queued && result.continuation) throw new Error("fixture cut budget exhausted");
    return result;
  }
  return { ...h, kv, sdk, summaries, select, lookup, write, provider, register, drive, applied: () => applied };
}

type Fixture = Awaited<ReturnType<typeof fixture>>;

function registerWithTypeSafe(f: Fixture, typeSafe: TypeSafeDecisionProvider) {
  registerConsolidationPipelineFunction(f.sdk as never, f.kv as never, f.provider as never,
    undefined, undefined, undefined, typeSafe, f.select as never, f.lookup as never, undefined, f.write as never);
}

async function makeRoutineSummaries(f: Fixture) {
  for (const summary of f.summaries) await f.kv.set(KV.summaries, summary.sessionId, { ...summary,
    title: `Routine source ${summary.sessionId}`, narrative: "A repetitive lookup with no reusable result.",
    keyDecisions: [], filesModified: [], observationCount: 1 });
}

function installFaithfulSummaryVersions(f: Fixture) {
  const versions = new Map(f.summaries.map(summary => [summary.sessionId, 1]));
  const originalGetVersioned = f.kv.getVersioned.bind(f.kv);
  const originalSet = f.kv.set.bind(f.kv);
  f.kv.getVersioned = vi.fn(async <T>(scope: string, key: string) => {
    if (scope !== KV.summaries) return originalGetVersioned<T>(scope, key);
    const value = await f.kv.get<T>(scope, key);
    return { exists: value !== null, value, version: String(versions.get(key) ?? 0) };
  });
  f.kv.set = vi.fn(async (scope, key, value: any) => {
    const saved = await originalSet(scope, key, value);
    if (scope === KV.summaries) versions.set(key, (versions.get(key) ?? 0) + 1);
    return saved;
  });
}

async function advanceToExtract(f: Fixture, typeSafe: TypeSafeDecisionProvider): Promise<string> {
  registerWithTypeSafe(f, typeSafe);
  const initial = await f.sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: { tier: "semantic", project: "p", deferred: true } }) as any;
  const manifestId = initial.results?.semantic?.manifestId;
  if (typeof manifestId !== "string") throw new Error(`semantic manifest missing: ${JSON.stringify(initial)}`);
  let root = await loadRoot(f.kv as never, manifestId);
  for (let cut = 0; root?.phase !== "extract" && cut < 40; cut++) {
    await f.sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: { consolidationManifestId: manifestId } });
    root = await loadRoot(f.kv as never, manifestId);
  }
  if (root?.phase !== "extract") throw new Error(`extract phase not reached: ${root?.phase ?? "missing"}`);
  return manifestId;
}

beforeEach(() => {
  vi.stubEnv("CONSOLIDATION_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED", "true");
  vi.stubEnv("AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED", "true");
});
afterEach(() => vi.unstubAllEnvs());

describe("semantic publication replaces TypeSafe consolidation decisions", () => {
  function noTypeSafe(typeSafe: ReturnType<typeof routineTypeSafe>) {
    expect(typeSafe.evaluate).not.toHaveBeenCalled();
    expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
  }
  const judges = (f: Fixture) => f.provider.summarize.mock.calls.filter(([system]) => system.startsWith("You evaluate publication evidence."));

  it("retains every routine original and its provenance for semantic evaluation without TypeSafe compaction", async () => {
    const f = await fixture(8); await makeRoutineSummaries(f);
    const typeSafe = routineTypeSafe(); registerWithTypeSafe(f, typeSafe as never);
    const result = await f.drive({ tier: "semantic", project: "p", deferred: true });
    expect(result).toMatchObject({ success: true, completed: true }); noTypeSafe(typeSafe);
    expect(judges(f)).toHaveLength(1);
    const evidence = JSON.parse(judges(f)[0][1]);
    expect(evidence.sources.map((source: { key: string }) => source.key).sort()).toEqual(f.summaries.map(s => s.sessionId).sort());
    expect(result.results.semantic).toMatchObject({ selectedSourceIds: f.summaries.map(s => s.sessionId), newFacts: 1 });
    expect((await loadRoot(f.kv as never, result.results.semantic.manifestId))?.eligibleSources).toHaveLength(8);
    expect((await f.kv.list<{ sourceSessionIds: string[] }>(KV.semantic))[0].sourceSessionIds.sort()).toEqual(f.summaries.map(s => s.sessionId).sort());
    expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("p"))).not.toBeNull();
  });

  it("includes protected decisions and routine originals together in the complete judge evidence", async () => {
    const f = await fixture(5); await makeRoutineSummaries(f);
    const first = await f.kv.get<Record<string, unknown>>(KV.summaries, "s0");
    await f.kv.set(KV.summaries, "s0", { ...first, keyDecisions: ["The project decision remains important"] });
    const typeSafe = routineTypeSafe(); registerWithTypeSafe(f, typeSafe as never);
    expect(await f.drive({ tier: "semantic", project: "p", deferred: true })).toMatchObject({ completed: true });
    noTypeSafe(typeSafe); expect(judges(f)).toHaveLength(1);
    expect(JSON.parse(judges(f)[0][1]).sources).toHaveLength(5);
    expect(judges(f)[0][1]).toContain("The project decision remains important");
    expect(f.applied()).toBe(1);
  });

  it("binds force into identity while both scheduled and forced runs use semantic publication", async () => {
    const f = await fixture(5); await makeRoutineSummaries(f);
    const typeSafe = routineTypeSafe(); registerWithTypeSafe(f, typeSafe as never);
    const batchSourceIds = f.summaries.map(source => source.sessionId);
    const scheduledStart = await f.sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: { tier: "semantic", project: "p", deferred: true, batchSourceIds } }) as any;
    const forcedStart = await f.sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: { tier: "semantic", project: "p", deferred: true, force: true, batchSourceIds } }) as any;
    const scheduledId = scheduledStart.results.semantic.manifestId, forcedId = forcedStart.results.semantic.manifestId;
    expect(forcedId).not.toBe(scheduledId);
    expect((await loadRoot(f.kv as never, scheduledId))?.callerPolicy?.force).toBe(false);
    expect((await loadRoot(f.kv as never, forcedId))?.callerPolicy?.force).toBe(true);
    const scheduled = await f.drive({ consolidationManifestId: scheduledId });
    const forced = await f.drive({ consolidationManifestId: forcedId });
    expect(scheduled).toMatchObject({ completed: true }); expect(forced).toMatchObject({ completed: true });
    expect(judges(f)).toHaveLength(2); noTypeSafe(typeSafe);
  });

  it("sends original procedural evidence to generation and its separate judge without frequency-based compaction", async () => {
    const f = await fixture(2);
    for (let index = 0; index < 2; index++) await f.kv.set(KV.observations(`s${index}`), `o-s${index}`, {
      id: `o-s${index}`, sessionId: `s${index}`, title: "Routine workflow", importance: 1,
      narrative: "Read the project configuration and apply the documented setup.", concepts: [], timestamp: "2026-09-01T00:00:00.000Z" });
    const typeSafe = routineTypeSafe("skip", false); registerWithTypeSafe(f, typeSafe as never);
    const result = await f.drive({ tier: "procedural", project: "p", deferred: true });
    expect(result).toMatchObject({ success: true, completed: true }); noTypeSafe(typeSafe);
    expect(result.results.procedural).toMatchObject({ selectedSourceIds: ["o-s0", "o-s1"], newProcedures: 1 });
    expect((await loadRoot(f.kv as never, result.results.procedural.manifestId))?.eligibleSources).toHaveLength(2);
    expect(judges(f)).toHaveLength(1); expect(JSON.parse(judges(f)[0][1]).sources).toHaveLength(2);
    expect(f.applied()).toBe(1);
  });

  it("recovers a lost completion acknowledgement from published effects without another generation or judge", async () => {
    const f = await fixture(5); const typeSafe = routineTypeSafe(); registerWithTypeSafe(f, typeSafe as never);
    const set = f.kv.set.bind(f.kv); let interrupted = false;
    f.kv.set = vi.fn(async (scope, key, value: any) => {
      if (!interrupted && scope === KV.state && value.phase === "completed" && value.kind === "semantic") {
        interrupted = true; throw new Error("completion acknowledgement lost");
      }
      return set(scope, key, value);
    });
    const initial = await f.drive({ tier: "semantic", project: "p", deferred: true });
    expect(interrupted).toBe(true); expect(initial).toMatchObject({ completed: false, partial: true });
    const calls = f.provider.summarize.mock.calls.length, writes = f.write.mock.calls.length;
    const resumed = await f.drive(initial.continuation);
    expect(resumed).toMatchObject({ completed: true });
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); expect(f.write).toHaveBeenCalledTimes(writes);
    expect(judges(f)).toHaveLength(1); noTypeSafe(typeSafe);
  });

  it.each(["unsupported", "uncertain", "invalid_response"] as const)("retains a %s semantic verdict without publication or paid replay", async verdict => {
    const f = await fixture(8); await makeRoutineSummaries(f);
    const typeSafe = routineTypeSafe(); registerWithTypeSafe(f, typeSafe as never);
    const summarize = f.provider.summarize.getMockImplementation()!;
    f.provider.summarize.mockImplementation(async (system, prompt) => system.startsWith("You evaluate publication evidence.")
      ? verdict === "invalid_response" ? '{"unexpected":true}' : JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict, reason: "The fixture candidate lacks complete support." })
      : summarize(system, prompt));
    const initial = await f.drive({ tier: "semantic", project: "p", deferred: true });
    expect(initial).toMatchObject(verdict === "unsupported" ? { completed: true } : { completed: false, partial: true });
    const calls = f.provider.summarize.mock.calls.length;
    const manifestId = initial.results?.semantic?.manifestId ?? initial.manifestId ?? initial.continuation?.consolidationManifestId;
    expect(typeof manifestId).toBe("string");
    const root = await loadRoot(f.kv as never, manifestId);
    if (verdict === "unsupported") expect(root?.counters).toMatchObject({ rejected: 1, applied: 0, published: 0 });
    await f.drive({ consolidationManifestId: manifestId });
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); noTypeSafe(typeSafe);
    expect(judges(f)).toHaveLength(1); expect(f.write).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
    expect(await f.kv.list(KV.semantic)).toHaveLength(0);
    if (verdict === "unsupported") expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("p"))).not.toBeNull();
    else expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("p"))).toBeNull();
    for (const source of f.summaries) expect(await f.kv.get(KV.summaries, source.sessionId)).not.toBeNull();
  });

  it("rejects changed source versions before publication and keeps the captured originals", async () => {
    const f = await fixture(8); installFaithfulSummaryVersions(f);
    const typeSafe = routineTypeSafe(); const manifestId = await advanceToExtract(f, typeSafe as never);
    const root = await loadRoot(f.kv as never, manifestId);
    const captured = await f.kv.get<{ rowVersion: string }>(KV.state, pageKey(manifestId, "source", 7));
    const before = await f.kv.getVersioned<SessionSummary>(KV.summaries, "s7");
    await f.kv.set(KV.summaries, "s7", { ...before.value!, narrative: "Changed after original capture" });
    expect(captured?.rowVersion).toBe(before.version); expect(root?.refs[7]?.rowVersion).toBe(before.version);
    expect((await f.kv.getVersioned(KV.summaries, "s7")).version).not.toBe(before.version);
    expect(await f.drive({ consolidationManifestId: manifestId })).toMatchObject({ completed: false, partial: true });
    noTypeSafe(typeSafe); expect(f.write).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
    expect(await f.kv.list(KV.semantic)).toHaveLength(0); expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("p"))).toBeNull();
    expect(await f.kv.get(KV.state, pageKey(manifestId, "source", 7))).toEqual(captured);
  });
});
