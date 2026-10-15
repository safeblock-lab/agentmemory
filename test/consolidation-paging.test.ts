import { describe, expect, it, vi } from "vitest";
import { createHash } from "node:crypto";
vi.mock("../src/logger.js", () => ({ logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../src/functions/audit.js", () => ({ recordAudit: vi.fn(async () => {}) }));
vi.mock("../src/providers/_llm-logging.js", () => ({ startLlmCallTelemetry: () => ({ failure: vi.fn(), success: vi.fn() }), extractLlmTokenUsage: () => ({}) }));
import { KV, fingerprintId } from "../src/state/schema.js";
import { registerConsolidationPipelineFunction } from "../src/functions/consolidation-pipeline.js";
import { requestInputBytes, fragmentSources, ConsolidationRequests, acceptConsolidationResponse } from "../src/functions/consolidation-partitions.js";
import { loadRoot, newRoot, saveRoot, pageKey, semanticProjectCheckpointKey, replacePagedConsolidationRequest } from "../src/functions/consolidation-manifest.js";
import { readSource } from "../src/functions/consolidation-capture.js";
import { SEMANTIC_FRAGMENT_SYSTEM, SEMANTIC_CANDIDATE_FINAL_SYSTEM, PROCEDURAL_FRAGMENT_SYSTEM, buildSemanticCandidatePrompt, buildConsolidationFragmentPrompt } from "../src/prompts/consolidation.js";
import { batchEffectKey } from "../src/state/batch-effects.js";
import { ScopedObservationBudgetError } from "../src/functions/consolidation-selection.js";
import { LlmTaskRouter } from "../src/providers/task-router.js";
import { OpenRouterProvider } from "../src/providers/openrouter.js";
import { OpenRouterKeyPoolProvider } from "../src/providers/openrouter-key-pool.js";
import type { LlmCallOptions } from "../src/types.js";
import type { ConsolidationDecaySelector } from "../src/functions/consolidation-decay.js";
function publicationVerdict(system: string, verdict = "supported") {
  return JSON.stringify({ requestId: /requestId must be ([^.]+)\./.exec(system)![1], verdict,
    reason: "Mock semantic assessment of complete original sources." });
}

export function pagingFixture(count = 5, narrative = "Grounded stable decision") {
  const values = new Map<string, unknown>(); const versions = new Map<string, number>(); const handlers = new Map<string, (data: any) => Promise<any>>();
  const key = (scope: string, id: string) => `${scope}\0${id}`;
  const kv = {
    indexedRetrieval: true,
    get: vi.fn(async <T>(scope: string, id: string): Promise<T | null> => structuredClone(values.get(key(scope, id)) as T ?? null)),
    set: vi.fn(async <T>(scope: string, id: string, value: T) => { values.set(key(scope, id), structuredClone(value)); versions.set(key(scope, id), (versions.get(key(scope, id)) ?? 0) + 1); return value; }),
    delete: vi.fn(async (scope: string, id: string) => { values.delete(key(scope, id)); }),
    getVersioned: vi.fn(async <T>(scope: string, id: string) => ({ exists: values.has(key(scope, id)), value: structuredClone(values.get(key(scope, id)) as T ?? null), version: String(versions.get(key(scope, id)) ?? 0) })),
    list: vi.fn(async () => { throw new Error("global list forbidden"); }),
    retrieval: vi.fn(async (request?: Record<string, unknown>) => {
      if (request?.action === "canonical_apply" && request.receipt_only === true) {
        const scope = request.scope === KV.semantic ? "semantic" : "procedural";
        const result = effects.get(`${scope}:${request.normalized_key}:${request.effect_key}`);
        if (!result) return { status: "not_applied" };
        if (result.receipt.operationFingerprint !== request.operation_fingerprint) throw new Error("STATE_TX_CONFLICT");
        return { status: "already_applied", applied: false, created: result.created, canonical_id: result.id,
          row_version: result.rowVersion, result_fingerprint: result.receipt.resultFingerprint,
          receipt: { operation_kind: "upsert", effect_key: request.effect_key, operation_fingerprint: request.operation_fingerprint,
            canonical_id: result.id, row_version: result.rowVersion, created: result.created, result_fingerprint: result.receipt.resultFingerprint } };
      }
      return { status: "ready", items: [{ project: "p" }], has_more: false };
    }),
  };
  const summaries = Array.from({ length: count }, (_, i) => ({ sessionId: `s${i}`, project: "p", createdAt: `2026-10-0${i + 1}T00:00:00Z`, title: `Decision ${i}`,
    narrative, concepts: ["setup"], keyDecisions: [], filesModified: [], observationCount: 5 }));
  for (const summary of summaries) { values.set(key(KV.summaries, summary.sessionId), structuredClone(summary)); versions.set(key(KV.summaries, summary.sessionId), 1);
    values.set(key(KV.sessions, summary.sessionId), { id: summary.sessionId, project: "p" }); }
  const sdk = { registerFunction: (id: string, fn: (data: any) => Promise<any>) => handlers.set(id, fn), trigger: async (id: string | { function_id: string; payload: unknown }, data?: unknown) => handlers.get(typeof id === "string" ? id : id.function_id)!(typeof id === "string" ? data : id.payload) };
  const select = vi.fn(async () => summaries.map(summary => ({ id: `o-${summary.sessionId}`, sessionId: summary.sessionId, rowVersion: "1" })));
  const effects = new Map<string, any>(); let applied = 0;
  const write = vi.fn(async (scope: "semantic" | "procedural", normalized: string, _match: unknown, id: string, effect: string, update: (old: any) => any) => {
    const effectId = `${scope}:${normalized}:${effect}`; const prior = effects.get(effectId);
    if (prior) return { ...prior, applied: false };
    const current = await kv.get<any>(KV[scope], id); const next = update(current); await kv.set(KV[scope], id, next); applied++;
    const operationFingerprint = createHash("sha256").update(JSON.stringify(["upsert", scope, normalized, effect, id, update(null)])).digest("hex");
    const result = { applied: true, created: !current, id, rowVersion: "2", receipt: { operationKind: "upsert", effectKey: effect, operationFingerprint, id, rowVersion: "2", created: !current, resultFingerprint: "d".repeat(64) } };
    effects.set(effectId, result); return result;
  });
  const lookup = vi.fn(async () => ({ status: "ready" as const, canonicalId: null, version: "1" }));
  const provider = { summarize: vi.fn(async (_system: string, prompt: string, _options?: LlmCallOptions) => {
    if (_system.startsWith("You evaluate publication evidence.")) {
      return publicationVerdict(_system);
    }
    const body = prompt.replace(/^Consolidation manifest: [^\n]+\n\n/, "");
    if (body.startsWith("Analyze")) { const fragments = JSON.parse(body.slice(body.indexOf("\n\n") + 2));
      return `<facts><fact confidence="0.95" sourceIds="${[...new Set(fragments.map((f: any) => f.sourceId))].join(",")}">Stable setup decision</fact></facts>`; }
    const candidates = JSON.parse(body.slice(body.indexOf("\n\n") + 2));
    return `<facts><fact confidence="0.95" candidateIds="${candidates.flatMap((c: any) => c.candidateIds).join(",")}">Stable setup decision</fact></facts>`;
  }) };
  const register = (queue?: any, router?: LlmTaskRouter, decay?: ConsolidationDecaySelector,
    typeSafe?: Parameters<typeof registerConsolidationPipelineFunction>[6]) => registerConsolidationPipelineFunction(sdk as never, kv as never, provider as never, router, undefined, queue, typeSafe, select, lookup, decay, write as never);
  register();
  async function drive(input: any, maximum = 3000) {
    let result = await sdk.trigger("mem::consolidate-pipeline", input);
    for (let cut = 0; result.queued && result.continuation && cut < maximum; cut++) result = await sdk.trigger("mem::consolidate-pipeline", result.continuation);
    if (result.queued && result.continuation) throw new Error("fixture cut budget exhausted"); return result;
  }
  return { kv, sdk, summaries, select, write, lookup, provider, values, key, drive, register, applied: () => applied };
}

function nativeJsonOrder<T>(value: T): T {
  if (Array.isArray(value)) return value.map(nativeJsonOrder) as T;
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value)
    .sort(([left], [right]) => left.localeCompare(right)).map(([key, nested]) => [key, nativeJsonOrder(nested)])) as T;
  return value;
}

async function selectedReplayFixture() {
  const f = pagingFixture(1); const summary = f.summaries[0]!;
  const ref = { id: summary.sessionId, sessionId: summary.sessionId, kind: "summary" as const, project: "p", scope: KV.summaries };
  const source = await readSource(f.kv as never, ref);
  const refs = [{ ...ref, rowVersion: source.version }];
  const callerPolicy = { deferred: false, force: true, compaction: false, pipelineGate: false };
  const id = fingerprintId("conmanifest", JSON.stringify(["semantic", refs, null, callerPolicy]));
  const root = newRoot(id, "semantic", "p", refs, false, callerPolicy);
  root.phase = "completed";
  root.result = { completed: true, manifestId: id, appliedEffects: 0, newFacts: 0, newProcedures: 0 };
  await saveRoot(f.kv as never, nativeJsonOrder(root));
  await f.kv.set(KV.state, pageKey(id, "source", 0), { fingerprint: fingerprintId("consource", source.json), rowVersion: source.version });
  await f.kv.set(KV.state, `selected-consolidation:${id}`, { id, project: "p" });
  return { f, id };
}

describe("paged consolidation", () => {
  async function queuedFixture() {
    const f = pagingFixture(); const requests: any[] = []; const dedup = new Map<string, string>(); let beforeReturn: ((request: any) => Promise<void>) | undefined;
    const queue = { enqueue: vi.fn(async (request: any) => {
      requests.push(request); const identity = `${request.replacementOf ?? "original"}:${request.correlationId}`;
      let id = dedup.get(identity);
      if (!id) { id = `fwbwork_${dedup.size}`; dedup.set(identity, id); await f.kv.set(KV.fireworksBatchWorkItems, id, { ...request, id, customId: request.correlationId, state: "queued" }); }
      if (beforeReturn) await beforeReturn(request);
      return { queued: true, workItemId: id };
    }) };
    f.register(queue);
    let result = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true, deferred: true });
    for (let cut = 0; !requests.length && cut < 100; cut++) result = await f.sdk.trigger("mem::consolidate-pipeline", result.continuation);
    const request = requests[0]; const workId = dedup.get(`original:${request.correlationId}`)!;
    const input = { manifestId: request.metadata.consolidationManifestId, partitionId: request.correlationId, workItemId: workId };
    const work = await f.kv.get<any>(KV.fireworksBatchWorkItems, workId); await f.kv.set(KV.fireworksBatchWorkItems, workId, { ...work, state: "dead-letter" });
    const response = '<facts><fact confidence="0.95" sourceIds="s0">Captured original fact</fact></facts>';
    return { ...f, queue, input, request, response, uniqueWork: () => dedup.size, hook: (callback: typeof beforeReturn) => { beforeReturn = callback; } };
  }
  it("replays a completed selected manifest after native JSON property reordering without new calls or effects", async () => {
    const { f, id } = await selectedReplayFixture();
    const root = (await loadRoot(f.kv as never, id))!;
    expect(Object.keys(root.refs[0]!)).toEqual(["id", "kind", "project", "rowVersion", "scope", "sessionId"]);
    expect(Object.keys(root.callerPolicy!)).toEqual(["compaction", "deferred", "force", "pipelineGate"]);

    const result = await f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: id });

    expect(result).toMatchObject({ success: true, completed: true, replayed: true, manifestId: id,
      newFacts: 0, newProcedures: 0, appliedEffects: 0 });
    expect(f.provider.summarize).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
  });
  it("rejects changed selected replay policy and version or source bindings", async () => {
    const policyCase = await selectedReplayFixture();
    const policyRoot = (await loadRoot(policyCase.f.kv as never, policyCase.id))!;
    policyRoot.callerPolicy = { ...policyRoot.callerPolicy!, force: false };
    await saveRoot(policyCase.f.kv as never, nativeJsonOrder(policyRoot));
    expect(await policyCase.f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: policyCase.id }))
      .toMatchObject({ success: false, completed: false, error: "selected_consolidation_replay_identity_mismatch" });

    const versionCase = await selectedReplayFixture();
    const versionRoot = (await loadRoot(versionCase.f.kv as never, versionCase.id))!;
    versionRoot.refs[0]!.rowVersion = "2";
    await saveRoot(versionCase.f.kv as never, nativeJsonOrder(versionRoot));
    expect(await versionCase.f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: versionCase.id }))
      .toMatchObject({ success: false, completed: false, error: "selected_consolidation_replay_identity_mismatch" });

    const sourceCase = await selectedReplayFixture();
    const summary = sourceCase.f.summaries[0]!;
    await sourceCase.f.kv.set(KV.summaries, summary.sessionId, { ...summary, narrative: "Changed after capture" });
    await expect(sourceCase.f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: sourceCase.id }))
      .rejects.toThrow("consolidation_source_changed");
    for (const replayCase of [policyCase, versionCase, sourceCase]) {
      expect(replayCase.f.provider.summarize).not.toHaveBeenCalled(); expect(replayCase.f.applied()).toBe(0);
    }
  });
  it("still requires canonical publication acknowledgement when replay identity matches", async () => {
    const { f, id } = await selectedReplayFixture();
    const root = (await loadRoot(f.kv as never, id))!;
    const candidate = { id: "candidate-0", key: "stable setup decision", sourceSessionIds: ["s0"], children: [],
      fact: "Stable setup decision", confidence: 0.95 };
    await f.kv.set(KV.state, pageKey(id, "candidates", 0), { id: pageKey(id, "candidates", 0), candidates: [candidate], examined: 1, rejected: 0 });
    await f.kv.set(KV.state, pageKey(id, "aggregate", candidate.key), { ...candidate, anchor: "0:0" });
    root.finalPages = 1; root.counters.published = 1;
    await saveRoot(f.kv as never, nativeJsonOrder(root));

    await expect(f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: id }))
      .rejects.toThrow("consolidation_canonical_receipt_missing");
    expect(f.provider.summarize).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
  });
  it("retains an exact terminal queue failure and recovers enqueue-before-link with deterministic replacement identity", async () => {
    const f = await queuedFixture();
    const terminal = await f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: f.input.manifestId });
    expect(terminal).toMatchObject({ completed: false, queued: false, partial: true, retained: true,
      diagnostics: { requestId: f.input.partitionId, workItemId: f.input.workItemId, state: "dead-letter" } });
    const guard = vi.fn(async () => true); const set = f.kv.set; let cut = false;
    f.kv.set = vi.fn(async (scope, id, value: any) => {
      if (!cut && value.oldWorkItemId === f.input.workItemId) { cut = true; throw new Error("cut after enqueue before link"); }
      return set(scope, id, value);
    });
    await expect(replacePagedConsolidationRequest(f.kv as never, f.queue, guard, f.input)).rejects.toThrow("cut after enqueue before link");
    const replacement = await replacePagedConsolidationRequest(f.kv as never, f.queue, guard, f.input);
    expect(replacement).toMatchObject({ success: true, queued: true, workItemId: "fwbwork_1" }); expect(f.uniqueWork()).toBe(2);
    expect(await f.kv.get<any>(KV.state, f.input.partitionId)).toMatchObject({ workItemId: f.input.workItemId, status: "pending" });
    expect(f.queue.enqueue.mock.calls.slice(1).every(([request]) => request.replacementOf === f.input.workItemId
      && request.correlationId === f.input.partitionId && request.systemPrompt === f.request.systemPrompt && request.userPrompt === f.request.userPrompt)).toBe(true);
    await acceptConsolidationResponse(f.kv as never, f.input.manifestId, f.input.partitionId, f.response);
    const before = f.queue.enqueue.mock.calls.length;
    expect(await replacePagedConsolidationRequest(f.kv as never, f.queue, guard, f.input)).toMatchObject({ success: true, replayed: true, queued: false });
    expect(f.queue.enqueue).toHaveBeenCalledTimes(before); expect(f.provider.summarize).not.toHaveBeenCalled();
  });
  it("never overwrites an interleaved captured callback with a replacement pointer", async () => {
    const f = await queuedFixture(); let callback: Promise<void> | undefined;
    f.hook(async request => { if (request.replacementOf) callback = acceptConsolidationResponse(f.kv as never, f.input.manifestId, f.input.partitionId, f.response); });
    await replacePagedConsolidationRequest(f.kv as never, f.queue, async () => true, f.input); await callback;
    const request = await f.kv.get<any>(KV.state, f.input.partitionId);
    expect(request).toMatchObject({ status: "completed", workItemId: f.input.workItemId });
    const noProvider = vi.fn(async () => { throw new Error("provider must not repeat"); });
    const requests = new ConsolidationRequests(f.kv as never, f.input.manifestId, noProvider, f.queue);
    expect(await requests.request(f.request.systemPrompt, f.request.userPrompt.replace(/^Consolidation manifest: [^\n]+\n\n/, ""))).toBe(f.response);
    expect(noProvider).not.toHaveBeenCalled(); expect(f.uniqueWork()).toBe(2);
  });
  it("recovers an exact completed queue result and honors an ineligible replacement guard", async () => {
    const f = await queuedFixture(); const guard = vi.fn(async () => false);
    expect(await replacePagedConsolidationRequest(f.kv as never, f.queue, guard, f.input)).toMatchObject({ success: false, retained: true, queued: false, diagnostics: { state: "replacement_ineligible" } });
    expect(f.uniqueWork()).toBe(1); expect(guard).toHaveBeenCalledWith(f.input.workItemId);
    const item = await f.kv.get<any>(KV.fireworksBatchWorkItems, f.input.workItemId);
    await f.kv.set(KV.fireworksBatchWorkItems, f.input.workItemId, { ...item, state: "completed", result: {
      customId: f.input.partitionId, content: f.response, receivedAt: "2026-10-01T00:00:00Z" } });
    const result = await f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: f.input.manifestId });
    expect(result).toMatchObject({ queued: true, completed: false }); expect(f.uniqueWork()).toBe(1);
    expect(await f.kv.get(KV.state, f.input.partitionId)).toMatchObject({ status: "completed", responsePages: { bytes: Buffer.byteLength(f.response) } });
    expect(f.provider.summarize).not.toHaveBeenCalled();
  });
  it("validates request and work identity even when the manifest is completed or already captured", async () => {
    const f = await queuedFixture(); await acceptConsolidationResponse(f.kv as never, f.input.manifestId, f.input.partitionId, f.response);
    const root = (await loadRoot(f.kv as never, f.input.manifestId))!; root.phase = "completed"; await saveRoot(f.kv as never, root);
    const guard = vi.fn(async () => true);
    await expect(replacePagedConsolidationRequest(f.kv as never, f.queue, guard, { ...f.input, partitionId: "foreign-partition" })).rejects.toThrow("replacement_request_mismatch");
    await expect(replacePagedConsolidationRequest(f.kv as never, f.queue, guard, { ...f.input, workItemId: "foreign-work" })).rejects.toThrow("replacement_work_mismatch");
    expect(await replacePagedConsolidationRequest(f.kv as never, f.queue, guard, f.input)).toMatchObject({ success: true, replayed: true, queued: false });
    expect(guard).not.toHaveBeenCalled(); expect(f.uniqueWork()).toBe(1);
  });
  it("rejects foreign replacement links and stops explicit replacement at the twelve-link bound", async () => {
    const f = await queuedFixture();
    const key = pageKey(f.input.manifestId, `replacement:${f.input.partitionId}`, f.input.workItemId);
    await f.kv.set(KV.state, key, { manifestId: "foreign", requestId: f.input.partitionId, oldWorkItemId: f.input.workItemId, newWorkItemId: "foreign-work", depth: 1 });
    expect(await replacePagedConsolidationRequest(f.kv as never, f.queue, async () => true, f.input)).toMatchObject({ success: false, retained: true, queued: false, diagnostics: { state: "replacement_identity_invalid" } });
    await f.kv.delete(KV.state, key);
    let old = f.input.workItemId;
    for (let depth = 1; depth <= 12; depth++) {
      const next = `fwbwork_chain${depth}`;
      await f.kv.set(KV.state, pageKey(f.input.manifestId, `replacement:${f.input.partitionId}`, old), { manifestId: f.input.manifestId,
        requestId: f.input.partitionId, oldWorkItemId: old, newWorkItemId: next, depth }); old = next;
    }
    await f.kv.set(KV.fireworksBatchWorkItems, old, { ...f.request, id: old, customId: f.input.partitionId, state: "dead-letter" });
    expect(await replacePagedConsolidationRequest(f.kv as never, f.queue, async () => true, { ...f.input, workItemId: old })).toMatchObject({ success: false, retained: true, queued: false, diagnostics: { state: "replacement_limit" } });
    expect(f.uniqueWork()).toBe(1);
  });
  it("captures heavily escaped response pages within 64KiB and recovers partial capture without another provider call", async () => {
    const f = pagingFixture(); const response = '"\\\u0000😀'.repeat(6000); const provider = vi.fn(async () => response);
    const requests = new ConsolidationRequests(f.kv as never, "response-pages", provider); const set = f.kv.set; let cut = false;
    f.kv.set = vi.fn(async (scope, id, value: any) => { await set(scope, id, value);
      if (!cut && value.index === 0 && typeof value.text === "string") { cut = true; throw new Error("cut during response pages"); } return value; });
    await expect(requests.request("system", "prompt")).rejects.toThrow("cut during response pages");
    await expect(requests.request("system", "prompt")).rejects.toThrow("capture_incomplete"); expect(provider).toHaveBeenCalledOnce();
    const request = [...f.values.values()].find((value: any) => value.manifestId === "response-pages" && value.id?.startsWith("conrequest_")) as any;
    const firstKey = pageKey("response-pages", `response:${request.id}`, 0);
    const first = await f.kv.get<any>(KV.state, firstKey);
    await f.kv.set(KV.state, firstKey, Object.fromEntries(Object.entries(first).reverse()));
    await acceptConsolidationResponse(f.kv as never, "response-pages", request.id, response);
    expect(await requests.request("system", "prompt")).toBe(response); expect(provider).toHaveBeenCalledOnce();
    for (const [key, value] of f.values) if (key.startsWith(`${KV.state}\0`)) expect(Buffer.byteLength(JSON.stringify(value))).toBeLessThanOrEqual(65_536);
  });
  it.each(["transport", "unavailable", "invalid-json", "empty-output"])("limits real router/pool/HTTP wrappers to one attempt for %s and retains uncertainty", async point => {
    const f = pagingFixture();
    const endpoint = (name: string) => new OpenRouterProvider("fixture-key", name, 128, `https://provider.invalid/${name}`);
    const primary = endpoint("primary"); const fallback = endpoint("fallback");
    const pool = new OpenRouterKeyPoolProvider([endpoint("account-a"), endpoint("account-b")], fallback, 2, () => 0);
    const router = new LlmTaskRouter({ primary: { provider: primary, model: "primary" }, auxiliary: { provider: pool, model: "pool" },
      routing: { routes: { consolidation: "aux", conflict_resolution: "aux", graph_extraction: "primary", temporal_graph_extraction: "primary",
        compression: "primary", summary: "primary", entity_extraction: "primary", classification: "primary", reflection: "primary",
        skill_extraction: "primary", query_expansion: "primary", flow_compression: "primary" }, explicitRoutes: {}, warnings: [] } });
    const transport = vi.spyOn(globalThis, "fetch").mockImplementation(async () => {
      if (point === "transport") throw new Error("transport acknowledgement lost");
      if (point === "unavailable") return new Response("unavailable", { status: 503, headers: { "retry-after": "0" } });
      return new Response(point === "invalid-json" ? "{" : JSON.stringify({ choices: [{ message: { content: " " } }] }), { status: 200 });
    });
    try {
      f.register(undefined, router);
      const first = await f.drive({ tier: "semantic", project: "p", force: true });
      expect(first).toMatchObject({ success: false, completed: false, partial: true }); expect(transport).toHaveBeenCalledOnce();
      expect(String(transport.mock.calls[0][0])).toBe("https://provider.invalid/account-a");
      const retained = await f.sdk.trigger("mem::consolidate-pipeline", first.continuation);
      expect(retained).toMatchObject({ success: false, completed: false, queued: false, retained: true, diagnostics: { state: "provider_dispatch_uncertain" } });
      expect(transport).toHaveBeenCalledOnce(); expect(f.applied()).toBe(0); expect(f.provider.summarize).not.toHaveBeenCalled();
    } finally { transport.mockRestore(); }
  });
  it("preserves retained diagnostics in the all-stage envelope without reporting completed or queued", async () => {
    const f = pagingFixture(1);
    await f.kv.set(KV.sessions, "s0", { id: "s0", project: "p" });
    const failure = { success: false, completed: false, partial: true, retained: true, queued: false, error: "retained reflection fixture",
      errorCode: "REFLECT_RETAINED_FIXTURE", diagnostics: { state: "fixture_retained" } };
    const reflect = vi.fn(async () => failure);
    f.sdk.registerFunction("mem::reflect", reflect);
    const decay = Object.assign(vi.fn(async () => ({ items: [], nextCursor: null, membershipGeneration: "1", coverageEpoch: "1" })), {
      apply: vi.fn(async () => { throw new Error("empty decay cannot publish"); }) });
    f.register(undefined, undefined, decay);
    const result = await f.drive({ tier: "all", project: "p", force: true });
    expect(result).toMatchObject(failure); expect(result.results.reflect).toEqual(failure);
    expect(reflect).toHaveBeenCalledOnce(); expect(result.results).not.toHaveProperty("decay");
    expect(f.provider.summarize).not.toHaveBeenCalled(); expect(decay).not.toHaveBeenCalled(); expect(decay.apply).not.toHaveBeenCalled();
  });
  it.each(["transport", "response-before", "invalid-200", "unstructured-rejection"])("retains uncertain direct dispatch %s and recovers only the original response", async point => {
    const f = pagingFixture(); const manifestId = `uncertain-${point}`; const response = "Original provider result";
    const id = fingerprintId("conrequest", JSON.stringify([manifestId, "system", `Consolidation manifest: ${manifestId}\n\nprompt`]));
    const provider = vi.fn(async () => {
      expect(await f.kv.get(KV.state, id)).toMatchObject({ dispatchMode: "direct", providerAttempt: { state: "started", attempt: 1 } });
      if (point === "transport") throw new Error("connection lost after dispatch");
      if (point === "invalid-200") throw Object.assign(new Error("invalid JSON after successful response"), { status: 200 });
      if (point === "unstructured-rejection") throw new Error("HTTP 413 text is not a definitive status");
      return response;
    });
    const set = f.kv.set; let cut = false;
    f.kv.set = vi.fn(async (scope, key, value: any) => {
      if (point === "response-before" && !cut && value.index === 0 && value.text === response) { cut = true; throw new Error("response capture acknowledgement lost"); }
      return set(scope, key, value);
    });
    const requests = new ConsolidationRequests(f.kv as never, manifestId, provider);
    await expect(requests.request("system", "prompt")).rejects.toThrow();
    const reopened = new ConsolidationRequests(f.kv as never, manifestId, provider);
    await expect(reopened.request("system", "prompt")).rejects.toMatchObject({ code: "CONSOLIDATION_REQUEST_RETAINED", requestId: id, state: "provider_dispatch_uncertain" });
    expect(provider).toHaveBeenCalledOnce();
    expect(await f.kv.get(KV.state, id)).toMatchObject({ status: "pending", providerAttempt: { state: "started", attempt: 1 } });
    await acceptConsolidationResponse(f.kv as never, manifestId, id, response);
    expect(await reopened.request("system", "prompt")).toBe(response); expect(provider).toHaveBeenCalledOnce();
  });
  it("retains legacy direct dispatch with unknown intent instead of manufacturing another attempt", async () => {
    const f = pagingFixture(); const manifestId = "legacy-unknown-dispatch";
    const userPrompt = `Consolidation manifest: ${manifestId}\n\nprompt`;
    const id = fingerprintId("conrequest", JSON.stringify([manifestId, "system", userPrompt]));
    const original = { id, manifestId, systemPrompt: "system", userPrompt, status: "pending", createdAt: "2026-10-01T00:00:00Z" };
    await f.kv.set(KV.state, id, original); const provider = vi.fn(async () => "must not dispatch");
    await expect(new ConsolidationRequests(f.kv as never, manifestId, provider).request("system", "prompt")).rejects.toMatchObject({ state: "legacy_provider_dispatch_unknown" });
    expect(await f.kv.get(KV.state, id)).toEqual(original); expect(provider).not.toHaveBeenCalled();
  });
  it("recovers a queue enqueue-before-pointer cut through its durable mode and deterministic enqueue identity", async () => {
    const f = pagingFixture(); const provider = vi.fn(async () => "must not dispatch"); let uniqueWork = 0;
    const dedup = new Map<string, string>(); const queue = { enqueue: vi.fn(async (input: any) => {
      if (!dedup.has(input.correlationId)) dedup.set(input.correlationId, `queue-work-${++uniqueWork}`);
      return { queued: true, workItemId: dedup.get(input.correlationId) };
    }) };
    const set = f.kv.set; let cut = false;
    f.kv.set = vi.fn(async (scope, id, value: any) => {
      if (!cut && value.workItemId) { cut = true; throw new Error("cut before work pointer"); } return set(scope, id, value);
    });
    await expect(new ConsolidationRequests(f.kv as never, "queue-mode", provider, queue).request("system", "prompt")).rejects.toThrow("cut before work pointer");
    expect(await new ConsolidationRequests(f.kv as never, "queue-mode", provider, queue).request("system", "prompt")).toBeUndefined();
    expect(uniqueWork).toBe(1); expect(queue.enqueue).toHaveBeenCalledTimes(2); expect(provider).not.toHaveBeenCalled();
  });
  it.each(['é😀"\\\n'.repeat(6500), "\\".repeat(40000)])("captures >64KiB Unicode and escaped sources exactly with bounded cuts: %#", async narrative => {
    const f = pagingFixture(5, narrative);
    let result = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true }); let id = result.results.semantic.manifestId;
    for (let cut = 0; result.queued && cut < 2000; cut++) {
      f.kv.getVersioned.mockClear(); const before = f.provider.summarize.mock.calls.length;
      result = await f.sdk.trigger("mem::consolidate-pipeline", result.continuation);
      expect(f.kv.getVersioned.mock.calls.length).toBeLessThanOrEqual(12);
      expect(f.provider.summarize.mock.calls.length - before).toBeLessThanOrEqual(2);
    }
    expect(result).toMatchObject({ completed: false, retained: true, reason: "consolidation_publication_incomplete_evidence" });
    const root = (await loadRoot(f.kv as never, id))!;
    for (let source = 0; source < 5; source++) {
      const fragments = [];
      for (let page = 0; page < root.fragmentPages; page++) {
        const value = await f.kv.get<any>(KV.state, pageKey(id, "fragments", page)); if (value.source === source) fragments.push(...value.fragments);
      }
      expect(JSON.parse(fragments.map(fragment => fragment.text).join(""))).toEqual(f.summaries[source]);
      let offset = 0;
      for (const fragment of fragments) { expect(fragment.start).toBe(offset); offset = fragment.end; expect(fragment.text).not.toMatch(/^[\uDC00-\uDFFF]|[\uD800-\uDBFF]$/); }
    }
    for (const [key, value] of f.values) if (key.startsWith(`${KV.state}\0conpage_`)) expect(Buffer.byteLength(JSON.stringify(value))).toBeLessThanOrEqual(65_536);
    for (const [system, prompt] of f.provider.summarize.mock.calls) {
      expect(requestInputBytes(system, prompt)).toBeLessThanOrEqual(8192);
    }
    expect(f.kv.list).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
  });
  it("does not turn many fragments of one original episode into repeated semantic support", async () => {
    const f = pagingFixture();
    await f.kv.set(KV.summaries, "s0", { ...f.summaries[0], narrative: "Single original episode. ".repeat(2000) });
    f.provider.summarize.mockImplementation(async (_system, prompt) => {
      if (_system.startsWith("You evaluate publication evidence.")) return publicationVerdict(_system, "unsupported");
      const body = prompt.replace(/^Consolidation manifest: [^\n]+\n\n/, ""); const input = JSON.parse(body.slice(body.indexOf("\n\n") + 2));
      if (body.startsWith("Analyze")) return input[0].sourceId === "s0" ? '<facts><fact confidence="0.6" sourceIds="s0">Single original detail</fact></facts>' : "<facts/>";
      return `<facts><fact confidence="0.6" candidateIds="${[...new Set(input.flatMap((candidate: any) => candidate.candidateIds))].join(",")}">Single original detail</fact></facts>`;
    });
    const result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(result.completed).toBe(true); expect(result.results.semantic).toMatchObject({ examined: 1, rejected: 1, newFacts: 0 });
    expect(result.results.semantic.candidateCount).toBeGreaterThan(1); expect(f.applied()).toBe(0);
  });
  it("finishes thousands of distinct candidates in multiple final pages, with exact coverage", async () => {
    const f = pagingFixture(); let count = 0;
    f.provider.summarize.mockImplementation(async (_system, prompt) => {
      if (_system.startsWith("You evaluate publication evidence.")) return publicationVerdict(_system);
      const body = prompt.replace(/^Consolidation manifest: [^\n]+\n\n/, ""); const data = JSON.parse(body.slice(body.indexOf("\n\n") + 2));
      if (body.startsWith("Analyze")) return `<facts>${Array.from({ length: 220 }, () => `<fact confidence="0.95" sourceIds="${data[0].sourceId}">Distinct stable decision ${count++}</fact>`).join("")}</facts>`;
      return `<facts>${data.map((candidate: any) => `<fact confidence="${candidate.confidence}" candidateIds="${candidate.candidateIds.join(",")}">${candidate.fact}</fact>`).join("")}</facts>`;
    });
    const result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(result.completed).toBe(true); expect(f.applied()).toBe(1100); expect(result.results.semantic.finalPages).toBeGreaterThan(1);
    expect(result.results.semantic.examined).toBe(1100); expect(result.results.semantic.rejected).toBe(0);
    const root = (await loadRoot(f.kv as never, result.results.semantic.manifestId))!;
    expect(root.candidateCount).toBe(1100); expect(root.counters.published).toBe(1100); expect(root.reduction.round).toBe(0);
    const extracted = new Set<string>();
    for (let page = 0; page < root.candidatePages; page++) {
      const value = await f.kv.get<any>(KV.state, pageKey(root.id, "candidates", page));
      value.candidates.forEach((candidate: any) => extracted.add(candidate.id));
    }
    const provenance = [...f.values.values()].filter((value: any) => value.candidateId && value.aggregateKey) as any[];
    expect(provenance).toHaveLength(1100);
    expect(new Set(provenance.flatMap(value => value.children))).toEqual(extracted);
  });
  it.each(["fragment-page-before", "fragment-page", "response", "published", "root-completed-before", "root-completed"])("recovers a cut around durable %s without duplicate provider captures or effects", async point => {
    const f = pagingFixture(); const set = f.kv.set; let interrupted = false;
    f.kv.set = vi.fn(async (scope, id, value: any) => {
      const matches = point === "response" ? id.startsWith("conrequest_") && value.status === "completed"
        : point.startsWith("fragment-page") ? value.fragments?.length : point === "published" ? value.receipt : value.schema === 2 && value.phase === "completed";
      if (!interrupted && matches && point.endsWith("before")) { interrupted = true; throw new Error("interruption before durable write"); }
      await set(scope, id, value);
      if (!interrupted && matches) { interrupted = true; throw new Error("interruption after durable write"); }
      return value;
    });
    let result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(interrupted).toBe(true);
    if (!result.completed) result = await f.drive(result.continuation);
    expect(result).toMatchObject({ completed: true }); expect(f.applied()).toBe(1);
    const requests = [...f.values.entries()].filter(([key, value]: any) => key.includes("conrequest_") && value.status === "completed");
    const evidence = [...f.values.values()].filter((value: any) => value.version === "publication-evidence-v1");
    expect(f.provider.summarize.mock.calls.length).toBe(requests.length + evidence.length);
  });
  it("rejects project and rowVersion changes before effects", async () => {
    const f = pagingFixture(); const first = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true });
    await f.kv.set(KV.summaries, "s0", { ...f.summaries[0], narrative: "Changed source" });
    const result = await f.drive(first.continuation);
    expect(result.completed).toBe(false); expect(result.partial).toBe(true); expect(f.applied()).toBe(0);
    expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("p"))).toBeNull();
  });
  it("rejects a changed session project before provider generation", async () => {
    const f = pagingFixture(); const first = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true });
    await f.kv.set(KV.sessions, "s0", { id: "s0", project: "foreign" });
    const result = await f.drive(first.continuation);
    expect(result).toMatchObject({ completed: false, partial: true }); expect(f.applied()).toBe(0);
    expect(f.provider.summarize).not.toHaveBeenCalled();
  });
  it.each([false, true])("lets the LLM judge procedural evidence without literal concept recurrence: repeated=%s", async repeated => {
    const f = pagingFixture(2);
    for (let index = 0; index < 2; index++) await f.kv.set(KV.observations(`s${index}`), `o-s${index}`, {
      id: `o-s${index}`, sessionId: `s${index}`, title: "Repeatable setup", importance: 8,
      narrative: "Long original evidence. ".repeat(index === 0 ? 2000 : 1), facts: [], files: [], concepts: [repeated || index === 0 ? "setup" : "different"] });
    f.provider.summarize.mockImplementation(async (_system, prompt) => {
      if (_system.startsWith("You evaluate publication evidence.")) return publicationVerdict(_system);
      const fragments = JSON.parse(prompt.slice(prompt.indexOf("[")));
      expect(fragments.every((fragment: any) => fragment.frequency === 1)).toBe(true);
      return '<procedures><procedure name="Setup" trigger="New environment"><step>Install dependency</step><step>Validate configuration</step></procedure></procedures>';
    });
    const result = await f.drive({ tier: "procedural", project: "p", force: true });
    expect(result.completed).toBe(true); expect(f.applied()).toBe(1);
    expect(f.provider.summarize.mock.calls.length).toBeGreaterThan(2);
  });
  it("preserves distinct same-name procedures and exact duplicates with stable order and replay", async () => {
    async function run(reverse: boolean) {
      const f = pagingFixture(3);
      for (let index = 0; index < 3; index++) await f.kv.set(KV.observations(`s${index}`), `o-s${index}`, {
        id: `o-s${index}`, sessionId: `s${index}`, title: "Setup workflow", importance: 8, narrative: "Grounded setup evidence", facts: [], files: [], concepts: ["setup"] });
      if (reverse) f.select.mockImplementation(async () => [...f.summaries].reverse().map(summary => ({ id: `o-${summary.sessionId}`, sessionId: summary.sessionId, rowVersion: "1" })));
      f.provider.summarize.mockImplementation(async (_system, prompt) => {
        if (_system.startsWith("You evaluate publication evidence.")) return publicationVerdict(_system);
        const fragments = JSON.parse(prompt.slice(prompt.indexOf("[")));
        return fragments[0].sourceId === "o-s1"
          ? '<procedures><procedure name="Setup" trigger="Rotate signing keys"><step>Generate replacement key</step><step>Publish the key ID</step></procedure></procedures>'
          : '<procedures><procedure name="Setup" trigger="Install a dependency"><step>Pin the dependency version</step><step>Validate the lockfile</step></procedure></procedures>';
      });
      const result = await f.drive({ tier: "procedural", project: "p", force: true });
      expect(result.completed).toBe(true); expect(result.results.procedural).toMatchObject({ candidateCount: 3, examined: 2, newProcedures: 2 });
      expect(f.applied()).toBe(2); expect(f.provider.summarize).toHaveBeenCalledTimes(5);
      const procedures = [...f.values.entries()].filter(([key]) => key.startsWith(`${KV.procedural}\0`)).map(([, value]) => value as any);
      expect(procedures.map(procedure => procedure.steps).sort()).toEqual([["Generate replacement key", "Publish the key ID"], ["Pin the dependency version", "Validate the lockfile"]]);
      for (const call of f.write.mock.calls) { const next = procedures.find(procedure => procedure.id === call[3]); expect(call[1]).toBe(next.name.toLowerCase()); }
      for (const procedure of procedures) expect(procedure.sourceSessionIds).toEqual(procedure.triggerCondition === "Rotate signing keys" ? ["s1"] : ["s0", "s2"]);
      await f.drive({ consolidationManifestId: result.results.procedural.manifestId });
      expect(f.applied()).toBe(2); expect(f.provider.summarize).toHaveBeenCalledTimes(5); expect(f.write).toHaveBeenCalledTimes(2);
      return procedures.map(procedure => procedure.name).sort();
    }
    expect(await run(false)).toEqual(await run(true));
  });
  it.each(["pending", "completed", "completed-missing"])("retains ambiguous same-name legacy procedures before applying any replacement identity: %s", async state => {
    const f = pagingFixture(2); const id = "conmanifest_legacy_procedure"; const sources = [];
    const status = state === "pending" ? "pending" : "completed";
    for (let index = 0; index < 2; index++) {
      const observation = { id: `o-s${index}`, sessionId: `s${index}`, title: "Setup", importance: 8, narrative: "Original setup evidence", concepts: ["setup"] };
      await f.kv.set(KV.observations(observation.sessionId), observation.id, observation);
      sources.push({ ...observation, project: "p", sourceKind: "observation", content: observation.narrative, frequency: 2,
        fingerprint: fingerprintId("conobs", JSON.stringify(observation)), version: 1, updatedAt: "2026-10-01T00:00:00Z" });
    }
    const fragments = fragmentSources(sources.map(source => ({ id: source.id, version: `${source.version}:${source.updatedAt}`, fields: { content: source.content } })), "", buildConsolidationFragmentPrompt);
    await f.kv.set(KV.state, id, { id, tier: "procedural", sources, fragments, deferred: false, status, createdAt: "2026-10-01T00:00:00Z" });
    const response = '<procedures><procedure name="Setup" trigger="Install dependency"><step>Pin version</step><step>Validate lockfile</step></procedure><procedure name="Setup" trigger="Rotate key"><step>Generate key</step><step>Publish key ID</step></procedure></procedures>';
    const userPrompt = `Consolidation manifest: ${id}\n\n${buildConsolidationFragmentPrompt(fragments.map(fragment => ({ ...fragment, frequency: 2 })))}`;
    const requestId = fingerprintId("conrequest", JSON.stringify([id, PROCEDURAL_FRAGMENT_SYSTEM, userPrompt]));
    if (state !== "completed-missing") await f.kv.set(KV.state, requestId, { id: requestId, manifestId: id, systemPrompt: PROCEDURAL_FRAGMENT_SYSTEM, userPrompt,
      status: "completed", response, createdAt: "2026-10-01T00:00:00Z" });
    const result = await f.drive({ consolidationManifestId: id });
    expect(result).toMatchObject({ completed: false, partial: true, retained: true, error: state === "completed-missing"
      ? "consolidation_legacy_completed_response_missing" : "consolidation_legacy_distinct_same_name_candidates_require_recovery" });
    expect(f.applied()).toBe(0); expect(await f.kv.get(KV.state, id)).toMatchObject({ id, sources, status });
    expect(f.provider.summarize).not.toHaveBeenCalled();
  });
  it("returns premanifest readiness continuation without invoking the provider", async () => {
    const f = pagingFixture(); f.select.mockRejectedValue(Object.assign(new Error("index partial"), { code: "STATE_INDEX_NOT_READY" }));
    const result = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true });
    expect(result).toMatchObject({ queued: true, completed: false, reason: "source_index_pending" }); expect(result.continuation.project).toBe("p");
    expect(f.provider.summarize).not.toHaveBeenCalled();
  });
  it("keeps semantic checkpoints per exact project when a newer project precedes an older one", async () => {
    const f = pagingFixture(); const a = f.summaries.map(source => ({ ...source, project: "A", createdAt: "2026-11-01T00:00:00Z" }));
    const b = f.summaries.map(source => ({ ...source, sessionId: `b-${source.sessionId}`, project: "B", createdAt: "2020-01-01T00:00:00Z" }));
    for (const source of [...a, ...b]) { await f.kv.set(KV.summaries, source.sessionId, source); await f.kv.set(KV.sessions, source.sessionId, { id: source.sessionId, project: source.project }); }
    f.select.mockImplementation(async (...args: unknown[]) => (args[0] === "B" ? b : a).map(source => ({ id: `o-${source.sessionId}`, sessionId: source.sessionId, rowVersion: "1" })));
    const global = { processedThrough: "2029-01-01T00:00:00Z", processedSessionIdsAtThrough: ["historical-unknown-project"] };
    await f.kv.set(KV.state, "semantic-consolidation", global);
    const first = await f.drive({ tier: "semantic", project: "A", force: true }); const calls = f.provider.summarize.mock.calls.length;
    const second = await f.drive({ tier: "semantic", project: "B", force: true });
    expect(first.completed).toBe(true); expect(second.completed).toBe(true); expect(f.provider.summarize.mock.calls.length).toBeGreaterThan(calls); expect(f.applied()).toBe(2);
    expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("A"))).toMatchObject({ processedThrough: a[0].createdAt });
    expect(await f.kv.get(KV.state, semanticProjectCheckpointKey("B"))).toMatchObject({ processedThrough: b[0].createdAt });
    expect(await f.kv.get(KV.state, "semantic-consolidation")).toEqual(global);
    const after = f.provider.summarize.mock.calls.length; await f.drive({ tier: "semantic", project: "A", force: true });
    expect(f.provider.summarize).toHaveBeenCalledTimes(after); expect(f.applied()).toBe(2);
  });
  it("preserves an older v2 capture identity and historical baseline without adopting a global project watermark", async () => {
    const f = pagingFixture(); const first = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true });
    const id = first.results.semantic.manifestId; const root = (await loadRoot(f.kv as never, id))!;
    const historical = { processedThrough: "2029-01-01T00:00:00Z", processedSessionIdsAtThrough: ["foreign"] };
    delete root.checkpointKey; root.baselineCheckpoint = historical; await saveRoot(f.kv as never, root);
    const result = await f.drive(first.continuation); expect(result.completed).toBe(true); expect(f.applied()).toBe(1);
    const updated = (await loadRoot(f.kv as never, id))!;
    expect(updated.id).toBe(id); expect(updated.historicalBaselineCheckpoint).toEqual(historical);
    expect(updated.checkpointKey).toBe(semanticProjectCheckpointKey("p")); expect(updated.baselineCheckpoint).toBeUndefined();
  });
  it("distinguishes an empty query work limit from preparation and preserves nonempty bounded coverage", async () => {
    const f = pagingFixture(); const coverage = ["bounded_candidate_budget", "bounded_query_budget"] as const;
    f.select.mockImplementation(async () => f.summaries.map(summary => ({ id: `o-${summary.sessionId}`, sessionId: summary.sessionId, rowVersion: "1", retrievalCoverage: [...coverage] })));
    f.select.mockRejectedValueOnce(new ScopedObservationBudgetError("query_work_budget", "p"));
    const first = await f.sdk.trigger("mem::consolidate-pipeline", { tier: "semantic", project: "p", force: true });
    expect(first).toMatchObject({ success: false, completed: false, partial: true, queued: false, retryable: true,
      errorCode: "STATE_RETRIEVAL_RESOURCE_LIMIT", reason: "source_retrieval_resource_limit",
      diagnostics: { project: "p", tier: "semantic", reason: "query_work_budget", sourceKind: "observation", limit: 12 } });
    expect(f.provider.summarize).not.toHaveBeenCalled(); expect(first.continuation).toMatchObject({ project: "p", tier: "semantic", force: true });
    const result = await f.drive(first.continuation);
    expect(result.completed).toBe(true); expect(result.retrievalCoverage).toEqual(coverage); expect(result.results.semantic.retrievalCoverage).toEqual(coverage);
    const root = (await loadRoot(f.kv as never, result.results.semantic.manifestId))!;
    expect(root.retrievalCoverage).toEqual(coverage); expect(root.refs.every(ref => JSON.stringify(ref.retrievalCoverage) === JSON.stringify(coverage))).toBe(true);
  });
  it("imports a legacy captured request with its original ID and effect identity", async () => {
    const f = pagingFixture(1); const summary = f.summaries[0]; const id = "conmanifest_legacy";
    const fragments = fragmentSources([{ id: summary.sessionId, version: summary.createdAt, fields: { title: summary.title, narrative: summary.narrative,
      concepts0: summary.concepts[0] } }], SEMANTIC_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt);
    await f.kv.set(KV.state, id, { id, tier: "semantic", sources: [summary], fragments, deferred: false, status: "pending", createdAt: "2026-10-01T00:00:00Z" });
    const prompt = `Consolidation manifest: ${id}\n\n${buildConsolidationFragmentPrompt(fragments)}`;
    const requestId = fingerprintId("conrequest", JSON.stringify([id, SEMANTIC_FRAGMENT_SYSTEM, prompt]));
    await f.kv.set(KV.state, requestId, { id: requestId, manifestId: id, systemPrompt: SEMANTIC_FRAGMENT_SYSTEM, userPrompt: prompt, status: "completed", response: '<facts><fact confidence="0.7" sourceIds="s0">Legacy fact</fact></facts>' });
    const result = await f.drive({ consolidationManifestId: id });
    expect(result.completed).toBe(true); expect(f.provider.summarize).toHaveBeenCalledOnce();
    expect(f.provider.summarize.mock.calls[0][0]).toContain("You evaluate publication evidence."); expect(f.write.mock.calls[0][4]).toBe(batchEffectKey(id));
    expect(await f.kv.get(KV.state, id)).toMatchObject({ id, sources: [summary], status: "pending" });
  });
  it("recovers an applied legacy final capture from its exact canonical receipt without generation or a new effect", async () => {
    const f = pagingFixture(2); const id = "conmanifest_partially_applied";
    const candidates = f.summaries.map((source, index) => ({ fact: `Grounded original ${index}`, confidence: 0.9, sourceSessionIds: [source.sessionId], candidateIds: [`old-candidate-${index}`] }));
    const fragments = fragmentSources(f.summaries.map(summary => ({ id: summary.sessionId, version: summary.createdAt, fields: { title: summary.title, narrative: summary.narrative } })), SEMANTIC_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt);
    await f.kv.set(KV.state, id, { id, tier: "semantic", sources: f.summaries, fragments, deferred: false, status: "failed", createdAt: "2026-10-01T00:00:00Z" });
    await f.kv.set(KV.state, fingerprintId("conround", `${id}:0`), { candidates });
    const prompt = `Consolidation manifest: ${id}\n\n${buildSemanticCandidatePrompt(candidates)}`;
    const requestId = fingerprintId("conrequest", JSON.stringify([id, SEMANTIC_CANDIDATE_FINAL_SYSTEM, prompt]));
    await f.kv.set(KV.state, requestId, { manifestId: id, status: "completed", response: '<facts><fact confidence="0.95" candidateIds="old-candidate-0,old-candidate-1">Captured merged fact</fact></facts>' });
    const canonicalId = fingerprintId("sem", "captured merged fact");
    await f.write("semantic", "captured merged fact", null, canonicalId, batchEffectKey(id), () => ({ id: canonicalId, fact: "Captured merged fact", confidence: 0.95,
      sourceSessionIds: ["s0", "s1"], sourceMemoryIds: [], accessCount: 1, lastAccessedAt: "2026-10-01T00:00:00Z", strength: 0.95,
      createdAt: "2026-10-01T00:00:00Z", updatedAt: "2026-10-01T00:00:00Z" }));
    const result = await f.drive({ consolidationManifestId: id });
    expect(result.completed).toBe(true); expect(result.newFacts).toBe(0); expect(result.createdFacts).toBe(1); expect(result.replayedEffects).toBe(1);
    expect(f.applied()).toBe(1); expect(f.provider.summarize).not.toHaveBeenCalled();
    expect(await f.kv.get<any>(KV.semantic, canonicalId)).toMatchObject({ accessCount: 1, sourceSessionIds: ["s0", "s1"] });
  });
  it("migrates a legacy source captured before fragment generation without an aggregate size rejection", async () => {
    const f = pagingFixture(1, "é😀".repeat(18000)); const id = "conmanifest_before_fragments";
    await f.kv.set(KV.state, id, { id, tier: "semantic", sources: f.summaries, fragments: [], status: "pending", deferred: false, createdAt: "2026-10-01T00:00:00Z" });
    const result = await f.drive({ consolidationManifestId: id });
    expect(result.error).toBeUndefined();
    expect(result).toMatchObject({ completed: false, retained: true, reason: "consolidation_publication_incomplete_evidence" }); expect(f.applied()).toBe(0);
    expect((await loadRoot(f.kv as never, id))!.fragmentCount).toBeGreaterThan(10);
  });
  it("retains a legacy manifest whose project identity is unavailable", async () => {
    const f = pagingFixture(1); const id = "conmanifest_unknown_identity"; const { project: _project, ...source } = f.summaries[0];
    const original = { id, tier: "semantic", sources: [source], fragments: [], status: "pending", deferred: true, createdAt: "2026-10-01T00:00:00Z" };
    await f.kv.set(KV.state, id, original);
    const result = await f.sdk.trigger("mem::consolidate-pipeline", { consolidationManifestId: id });
    expect(result).toMatchObject({ completed: false, retained: true, partial: true }); expect(f.provider.summarize).not.toHaveBeenCalled();
    expect(await f.kv.get(KV.state, id)).toEqual(original); expect(await loadRoot(f.kv as never, id)).toBeNull();
  });
});

describe("consolidation semantic publication evidence", () => {
  it("continues a partial legacy effect plan with available sources without another semantic judge", async () => {
    const f = pagingFixture(); for (const summary of f.summaries) await f.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    const input = { tier: "semantic", force: true, project: "p", batchEffectKey: "8".repeat(64),
      batchSourceIds: f.summaries.map(summary => summary.sessionId),
      batchSourceFingerprint: fingerprintId("fwbconsem", JSON.stringify(f.summaries.map(summary => [summary.sessionId, summary.title, summary.narrative, summary.concepts, summary.createdAt]))),
      batchResponse: '<facts><fact confidence="1" sourceIds="s0">First claim</fact><fact confidence="1" sourceIds="s1">Second claim</fact></facts>' };
    const write = f.write.getMockImplementation()!; let cut = false;
    f.write.mockImplementation(async (...args) => { const result = await write(...args); if (!cut) { cut = true; throw new Error("partial ACK lost"); } return result; });
    await expect(f.sdk.trigger("mem::consolidate-pipeline", input)).rejects.toThrow("partial ACK lost");
    expect(await f.sdk.trigger("mem::consolidate-pipeline", input)).toMatchObject({ success: true, completed: true, results: { semantic: { newFacts: 1 } } });
    expect(f.applied()).toBe(2); expect(f.provider.summarize).toHaveBeenCalledOnce();
    expect(await f.kv.get(KV.batchCallbacks, `consolidation:${input.batchEffectKey}`)).toMatchObject({ state: "completed" });
  });
  it("preserves semantic results and pending all-tier obligations when recovering a committed callback", async () => {
    const f = pagingFixture(); for (const summary of f.summaries) await f.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    const input = { tier: "all", force: true, project: "p", batchEffectKey: "9".repeat(64),
      batchSourceIds: f.summaries.map(summary => summary.sessionId),
      batchSourceFingerprint: fingerprintId("fwbconsem", JSON.stringify(f.summaries.map(summary => [summary.sessionId, summary.title, summary.narrative, summary.concepts, summary.createdAt]))),
      batchResponse: '<facts><fact confidence="1" sourceIds="s0">Committed claim</fact></facts>' };
    const write = f.write.getMockImplementation()!; let cut = false;
    f.write.mockImplementation(async (...args) => { const result = await write(...args); if (!cut) { cut = true; throw new Error("ACK lost"); } return result; });
    await expect(f.sdk.trigger("mem::consolidate-pipeline", input)).rejects.toThrow("ACK lost");
    await f.kv.delete(KV.summaries, "s0");
    expect(await f.sdk.trigger("mem::consolidate-pipeline", input)).toMatchObject({ success: false, partial: true, completed: false,
      canonicalAckRecovered: true, results: { semantic: { newFacts: 0, totalSummaries: f.summaries.length }, reflect: { completed: false } } });
    expect(f.applied()).toBe(1); expect(f.write).toHaveBeenCalledOnce(); expect(f.provider.summarize).toHaveBeenCalledOnce();
    expect(await f.kv.get(KV.batchCallbacks, `consolidation:${input.batchEffectKey}`)).toMatchObject({ state: "started" });
  });
  it("does not acknowledge an entire legacy callback when only the first of two planned canonical effects committed", async () => {
    const f = pagingFixture(); for (const summary of f.summaries) await f.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    const input = { tier: "semantic", force: true, project: "p", batchEffectKey: "7".repeat(64),
      batchSourceIds: f.summaries.map(summary => summary.sessionId),
      batchSourceFingerprint: fingerprintId("fwbconsem", JSON.stringify(f.summaries.map(summary => [summary.sessionId, summary.title, summary.narrative, summary.concepts, summary.createdAt]))),
      batchResponse: '<facts><fact confidence="1" sourceIds="s0">First claim</fact><fact confidence="1" sourceIds="s1">Second claim</fact></facts>' };
    const write = f.write.getMockImplementation()!; let cut = false;
    f.write.mockImplementation(async (...args) => { const result = await write(...args); if (!cut) { cut = true; throw new Error("partial ACK lost"); } return result; });
    await expect(f.sdk.trigger("mem::consolidate-pipeline", input)).rejects.toThrow("partial ACK lost");
    await f.kv.delete(KV.summaries, "s0");
    await expect(f.sdk.trigger("mem::consolidate-pipeline", input)).rejects.toThrow("source_identity_mismatch");
    expect(f.applied()).toBe(1); expect(f.write).toHaveBeenCalledOnce(); expect(f.provider.summarize).toHaveBeenCalledOnce();
    expect(await f.kv.get(KV.batchCallbacks, `consolidation:${input.batchEffectKey}`)).toMatchObject({ state: "started" });
  });
  it.each(["semantic", "procedural"])("legacy %s recovers committed canonical effects after source deletion before callback ACK", async kind => {
    const f = pagingFixture();
    for (const summary of f.summaries) await f.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    if (kind === "procedural") for (let index = 0; index < 2; index++) await f.kv.set(KV.memories, `m${index}`, {
      id: `m${index}`, project: "p", isLatest: true, type: "pattern", content: "Pin the dependency then verify the lockfile.",
      sessionIds: [`s${index}`], version: 1, updatedAt: "2026-10-01T00:00:00Z" });
    const input = { tier: kind, force: true, project: "p", batchEffectKey: (kind === "semantic" ? "e" : "f").repeat(64),
      batchSourceFingerprint: kind === "semantic" ? fingerprintId("fwbconsem", JSON.stringify(f.summaries.map(summary =>
        [summary.sessionId, summary.title, summary.narrative, summary.concepts, summary.createdAt])))
        : fingerprintId("fwbconproc", JSON.stringify([0, 1].map(() => ({ content: "Pin the dependency then verify the lockfile.", frequency: 1 })))),
      batchSourceIds: kind === "semantic" ? f.summaries.map(summary => summary.sessionId) : ["m0", "m1"],
      batchResponse: kind === "semantic" ? '<facts><fact confidence="0.3" sourceIds="s0">Committed fact</fact></facts>'
        : '<procedures><procedure name="Install" trigger="Dependency update"><step>Pin version</step><step>Verify lockfile</step></procedure></procedures>' };
    const write = f.write.getMockImplementation()!; let cut = false;
    f.write.mockImplementation(async (...args) => { const result = await write(...args);
      if (!cut) { cut = true; throw new Error("ACK lost after canonical commit"); } return result; });
    await expect(f.sdk.trigger("mem::consolidate-pipeline", input)).rejects.toThrow("ACK lost");
    expect(f.applied()).toBe(1); const calls = f.provider.summarize.mock.calls.length; const writes = f.write.mock.calls.length;
    await f.kv.delete(kind === "semantic" ? KV.summaries : KV.memories, kind === "semantic" ? "s0" : "m0");
    expect(await f.sdk.trigger("mem::consolidate-pipeline", input)).toMatchObject({ success: true, completed: true, canonicalAckRecovered: true, appliedEffects: 0,
      results: { [kind]: { [kind === "semantic" ? "newFacts" : "newProcedures"]: 0, replayedEffects: 1 } } });
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); expect(f.write).toHaveBeenCalledTimes(writes); expect(f.applied()).toBe(1);
    expect(await f.sdk.trigger("mem::consolidate-pipeline", input)).toEqual({ success: true });
  });
  it.each(["version", "project", "deleted"])("recovers canonical commit before the publication receipt after source %s without another judge or writer", async change => {
    const f = pagingFixture(); const set = f.kv.set; let cut = false;
    f.kv.set = vi.fn(async (scope, id, value: any) => {
      if (!cut && value.receipt && value.applied === true) { cut = true; throw new Error("cut before published receipt"); }
      return set(scope, id, value);
    });
    const first = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(cut).toBe(true); expect(first.completed).toBe(false); expect(f.applied()).toBe(1);
    const root = (await loadRoot(f.kv as never, first.results.semantic.manifestId))!;
    expect(await f.kv.get(KV.state, pageKey(root.id, "published", "0:0"))).toBeNull();
    const calls = f.provider.summarize.mock.calls.length; const writes = f.write.mock.calls.length;
    if (change === "version") await f.kv.set(KV.summaries, "s0", { ...f.summaries[0], narrative: "changed after canonical commit" });
    else if (change === "project") await f.kv.set(KV.sessions, "s0", { id: "s0", project: "foreign" });
    else await f.kv.delete(KV.summaries, "s0");
    const replay = await f.drive(first.continuation);
    expect(replay).toMatchObject({ completed: true, results: { semantic: { replayedEffects: 1 } } });
    expect(f.applied()).toBe(1); expect(f.write).toHaveBeenCalledTimes(writes);
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls);
    expect(await f.kv.get(KV.state, pageKey(root.id, "published", "0:0"))).toMatchObject({ applied: false, created: true });
  });
  it("uses only the LLM publication decision even when a TypeSafe provider is injected", async () => {
    const f = pagingFixture(); const typeSafe = { evaluate: vi.fn(async () => { throw new Error("TypeSafe must not run"); }),
      evaluateChoice: vi.fn(async () => { throw new Error("TypeSafe must not run"); }) };
    f.register(undefined, undefined, undefined, typeSafe as never);
    vi.stubEnv("CONSOLIDATION_ENABLED", "true");
    try {
      const result = await f.drive({ tier: "semantic", project: "p", deferred: true });
      expect(result.completed).toBe(true); expect(f.applied()).toBe(1);
      expect(typeSafe.evaluate).not.toHaveBeenCalled(); expect(typeSafe.evaluateChoice).not.toHaveBeenCalled();
      expect(f.provider.summarize.mock.calls.some(([system]) => system.startsWith("You evaluate publication evidence."))).toBe(true);
    } finally { vi.unstubAllEnvs(); }
  });
  it.each(["version", "project"])("acknowledges a durable effect receipt after source %s changed without new decisions or effects", async change => {
    const f = pagingFixture(); const set = f.kv.set; let cut = false;
    f.kv.set = vi.fn(async (scope, id, value: any) => {
      await set(scope, id, value);
      if (!cut && value.receipt && value.applied === true) { cut = true; throw new Error("cut after published receipt"); }
      return value;
    });
    const first = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(cut).toBe(true); expect(first.completed).toBe(false); expect(f.applied()).toBe(1);
    const calls = f.provider.summarize.mock.calls.length;
    if (change === "version") await f.kv.set(KV.summaries, "s0", { ...f.summaries[0], narrative: "changed after committed receipt" });
    else await f.kv.set(KV.sessions, "s0", { id: "s0", project: "foreign" });
    const replay = await f.drive(first.continuation);
    expect(replay).toMatchObject({ completed: true }); expect(f.applied()).toBe(1);
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); expect(f.write).toHaveBeenCalledOnce();
  });
  it.each(["semantic", "procedural"])("gates legacy %s callbacks once and completed receipt replay has no calls or effects", async kind => {
    const f = pagingFixture();
    for (const summary of f.summaries) await f.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    if (kind === "procedural") for (let index = 0; index < 2; index++) await f.kv.set(KV.memories, `m${index}`, {
      id: `m${index}`, project: "p", isLatest: true, type: "pattern", content: "Pin the dependency then verify the lockfile.",
      sessionIds: [`s${index}`], version: 1, updatedAt: "2026-10-01T00:00:00Z" });
    const input = { tier: kind, force: true, project: "p", batchEffectKey: (kind === "semantic" ? "a" : "b").repeat(64),
      batchSourceFingerprint: kind === "semantic" ? fingerprintId("fwbconsem", JSON.stringify(f.summaries.map(summary =>
        [summary.sessionId, summary.title, summary.narrative, summary.concepts, summary.createdAt])))
        : fingerprintId("fwbconproc", JSON.stringify([0, 1].map(() => ({ content: "Pin the dependency then verify the lockfile.", frequency: 1 })))),
      batchSourceIds: kind === "semantic" ? f.summaries.map(summary => summary.sessionId) : ["m0", "m1"],
      batchResponse: kind === "semantic" ? '<facts><fact confidence="0.3" sourceIds="s0">A supported durable fact</fact></facts>'
        : '<procedures><procedure name="Install" trigger="Dependency update"><step>Pin version</step><step>Verify lockfile</step></procedure></procedures>' };
    expect(await f.sdk.trigger("mem::consolidate-pipeline", input)).toMatchObject({ success: true, completed: true });
    expect(f.provider.summarize).toHaveBeenCalledOnce(); expect(f.applied()).toBe(1);
    const canonical = [...f.values.entries()].filter(([key]) => key.startsWith(`${KV[kind]}\0`));
    expect(await f.sdk.trigger("mem::consolidate-pipeline", input)).toEqual({ success: true });
    expect(f.provider.summarize).toHaveBeenCalledOnce(); expect(f.applied()).toBe(1);
    expect([...f.values.entries()].filter(([key]) => key.startsWith(`${KV[kind]}\0`))).toEqual(canonical);
  });
  it.each(["unsupported", "uncertain", "changed"])("legacy callback %s never writes a canonical fact", async state => {
    const f = pagingFixture();
    for (const summary of f.summaries) await f.kv.set(KV.sessions, summary.sessionId, { id: summary.sessionId, project: "p" });
    f.provider.summarize.mockImplementation(async (system) => {
      if (state === "changed") await f.kv.set(KV.summaries, "s0", { ...f.summaries[0], narrative: "changed after dispatch" });
      return publicationVerdict(system, state === "changed" ? "supported" : state);
    });
    const input = { tier: "all", force: true, project: "p", batchEffectKey: "c".repeat(64), batchSourceIds: f.summaries.map(summary => summary.sessionId),
      batchResponse: '<facts><fact confidence="1" sourceIds="s0">Claim to assess</fact></facts>' };
    if (state === "changed") await expect(f.sdk.trigger("mem::consolidate-pipeline", input)).rejects.toThrow("source_changed");
    else {
      const first = await f.sdk.trigger("mem::consolidate-pipeline", input);
      expect(first).toMatchObject({ completed: state === "unsupported", reason: state === "unsupported" ? "no_supported_candidates" : "consolidation_publication_uncertain" });
      await f.sdk.trigger("mem::consolidate-pipeline", input); expect(f.provider.summarize).toHaveBeenCalledOnce();
    }
    expect(f.write).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
  });
  it("publishes two final facts and terminal replay makes zero calls, effects or canonical changes", async () => {
    const f = pagingFixture();
    f.provider.summarize.mockImplementation(async (system, prompt) => {
      if (system.startsWith("You evaluate publication evidence.")) return publicationVerdict(system);
      const body = prompt.replace(/^Consolidation manifest: [^\n]+\n\n/, ""); const input = JSON.parse(body.slice(body.indexOf("\n\n") + 2));
      if (body.startsWith("Analyze")) return `<facts>${["First grounded fact", "Second grounded fact"].map(fact => `<fact confidence="0.95" sourceIds="${input[0].sourceId}">${fact}</fact>`).join("")}</facts>`;
      return `<facts>${["First grounded fact", "Second grounded fact"].map(fact => `<fact confidence="0.95" candidateIds="${input.filter((candidate: any) => candidate.fact === fact).flatMap((candidate: any) => candidate.candidateIds).join(",")}">${fact}</fact>`).join("")}</facts>`;
    });
    const result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(result.completed).toBe(true); expect(f.applied()).toBe(2);
    expect(f.provider.summarize.mock.calls.filter(([system]) => system.startsWith("You evaluate publication evidence."))).toHaveLength(2);
    const canonical = [...f.values.entries()].filter(([key]) => key.startsWith(`${KV.semantic}\0`));
    const calls = f.provider.summarize.mock.calls.length; const writes = f.write.mock.calls.length;
    const replay = await f.drive({ consolidationManifestId: result.results.semantic.manifestId });
    expect(replay).toMatchObject({ completed: true, replayed: true, appliedEffects: 0, newFacts: 0 });
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); expect(f.write).toHaveBeenCalledTimes(writes);
    expect([...f.values.entries()].filter(([key]) => key.startsWith(`${KV.semantic}\0`))).toEqual(canonical);
  });
  it.each(["unsupported", "uncertain", "invalid"])("retains original captures and replays %s without another call or effect", async verdict => {
    const f = pagingFixture(); const generate = f.provider.summarize.getMockImplementation()!;
    f.provider.summarize.mockImplementation(async (system, prompt, options) => system.startsWith("You evaluate publication evidence.")
      ? verdict === "invalid" ? "{invalid" : publicationVerdict(system, verdict) : generate(system, prompt, options));
    const result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(f.applied()).toBe(0);
    if (verdict === "unsupported") expect(result).toMatchObject({ completed: true, results: { semantic: { reason: "no_supported_candidates", rejected: 1 } } });
    else expect(result).toMatchObject({ completed: false, retained: true, reason: `consolidation_publication_${verdict === "invalid" ? "invalid_response" : verdict}` });
    const captures = [...f.values.entries()].filter(([key]) => key.includes("conrequest_") || key.includes("publication-evidence_"));
    const calls = f.provider.summarize.mock.calls.length;
    await f.drive({ consolidationManifestId: result.results.semantic.manifestId });
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); expect(f.write).not.toHaveBeenCalled();
    expect([...f.values.entries()].filter(([key]) => key.includes("conrequest_") || key.includes("publication-evidence_"))).toEqual(captures);
  });
  it.each(["version", "project"])("checks %s again after the supported LLM verdict and before the first effect", async change => {
    const f = pagingFixture(); const generate = f.provider.summarize.getMockImplementation()!;
    f.provider.summarize.mockImplementation(async (system, prompt, options) => {
      if (!system.startsWith("You evaluate publication evidence.")) return generate(system, prompt, options);
      const request = JSON.parse(prompt);
      expect(request.sources).toHaveLength(5);
      expect(request.sources.every((source: any) => source.content === JSON.stringify(f.summaries.find(item => item.sessionId === source.sessionId)))).toBe(true);
      if (change === "version") await f.kv.set(KV.summaries, "s0", { ...f.summaries[0], narrative: "changed while awaiting the LLM" });
      else await f.kv.set(KV.sessions, "s0", { id: "s0", project: "foreign" });
      return publicationVerdict(system);
    });
    const result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(result).toMatchObject({ completed: false, partial: true });
    expect(f.write).not.toHaveBeenCalled(); expect(f.applied()).toBe(0);
  });
  it("captures inside the router operation before outer router failure, then publishes from the captured verdict", async () => {
    const f = pagingFixture(); const generate = f.provider.summarize.getMockImplementation()!;
    const router = { run: vi.fn(async (_task, operation) => {
      const output = await operation(f.provider);
      const last = f.provider.summarize.mock.calls.at(-1)!;
      if (last[0].startsWith("You evaluate publication evidence.")) {
        expect([...f.values.values()].some((value: any) => value.version === "publication-evidence-v1" && value.raw === output)).toBe(true);
        throw new Error("router validation after captured response");
      }
      return output;
    }) };
    f.register(undefined, router as never);
    const result = await f.drive({ tier: "semantic", project: "p", force: true });
    expect(result.completed).toBe(true); expect(f.applied()).toBe(1);
    const calls = f.provider.summarize.mock.calls.length;
    await f.drive({ consolidationManifestId: result.results.semantic.manifestId });
    expect(f.provider.summarize).toHaveBeenCalledTimes(calls); expect(f.write).toHaveBeenCalledOnce();
    expect(generate).toBeDefined();
  });
});
