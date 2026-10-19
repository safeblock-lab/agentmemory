import { isDeepStrictEqual } from "node:util";
import type { IIIClient } from "iii-sdk";
import type { MemoryProvider } from "../types.js";
import type { StateKV } from "../state/kv.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { withProviderAttemptBudget } from "../providers/provider-attempt-budget.js";
import { validBudgetId } from "../state/provider-attempt-budget.js";
import { isStateCounter } from "../state/state-transactions.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { KV, fingerprintId } from "../state/schema.js";
import { boundedBytes, pageKey, type PagedConsolidation, type CandidatePage } from "./consolidation-manifest.js";
import { aggregateCandidateKey, candidatePage, publicationCandidate, consolidationPublicationEvidence,
  type AggregateCandidate } from "./consolidation-reduction.js";
import { verifyCapturedSource, verifySourceProjects } from "./consolidation-capture.js";
import { publicationExecutor } from "./publication-call-options.js";
import { preparePublicationJudgeDisposition, supersedePublicationJudge, validPublicationJudgeDisposition,
  type PublicationEvidenceResult, type PublicationJudgeDisposition } from "./publication-evidence.js";

const VERSION = "consolidation-publication-recovery-v1";
interface Capture {
  manifestId: string; project: string; rootVersion: string; page: number; offset: number;
  fingerprint: string;
}
interface Admission {
  version: typeof VERSION; capture: Capture; disposition: PublicationJudgeDisposition; attemptId: string;
}
interface Journal { admission: Admission; result?: PublicationEvidenceResult; priorResults?: PublicationEvidenceResult[] }
const object = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const closed = (value: Record<string, unknown>, fields: string[]): boolean =>
  Object.keys(value).length === fields.length && fields.every(field => Object.hasOwn(value, field));
const fail = (code: string): never => { throw new Error(code); };
const natural = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
function invocationPayload(value: unknown): unknown {
  if (!object(value) || !Object.hasOwn(value, "_caller_worker_id")) return value;
  const { _caller_worker_id: caller, ...payload } = value;
  if (typeof caller !== "string" || caller.length < 1 || caller.length > 128)
    return fail("invalid_consolidation_publication_caller");
  return payload;
}
function validCapture(value: unknown): value is Capture {
  return object(value) && closed(value, ["manifestId", "project", "rootVersion", "page", "offset", "fingerprint"])
    && validBudgetId(value.manifestId) && validBudgetId(value.fingerprint) && isStateCounter(value.rootVersion)
    && typeof value.project === "string" && !!value.project.trim() && Buffer.byteLength(value.project) <= 512
    && natural(value.page) && natural(value.offset);
}
function validAdmission(value: unknown): value is Admission {
  return object(value) && closed(value, ["version", "capture", "disposition", "attemptId"])
    && value.version === VERSION && validCapture(value.capture) && validBudgetId(value.attemptId)
    && validPublicationJudgeDisposition(value.disposition) && !!value.disposition.captured && !value.disposition.migration
    && validBudgetId(value.disposition.dispositionId) && validBudgetId(value.disposition.budget.windowId)
    && value.disposition.budget.ceiling === 1;
}

async function loadCapture(kv: StateKV, manifestId: string) {
  const row = await kv.getVersioned<PagedConsolidation>(KV.state, pageKey(manifestId, "root", 0));
  const root = row.value;
  if (!row.exists || !root || !isStateCounter(row.version)) return fail("consolidation_publication_root_unavailable");
  boundedBytes(root);
  if (root.schema !== 2 || root.id !== manifestId || root.phase !== "publish"
    || !["semantic", "procedural"].includes(root.kind) || typeof root.project !== "string" || !root.project.trim()
    || Buffer.byteLength(root.project) > 512 || !Array.isArray(root.refs) || !root.refs.length || root.refs.length > 12
    || root.refs.some(ref => !ref || ref.project !== root.project || !validBudgetId(ref.id) || !validBudgetId(ref.sessionId)
      || typeof ref.scope !== "string" || !ref.scope || !["summary", "observation", "memory"].includes(ref.kind))
    || !object(root.publish) || !closed(root.publish, ["page", "offset"]) || !natural(root.publish.page)
    || !natural(root.publish.offset) || !natural(root.finalRound) || !natural(root.finalPages)
    || root.publish.page >= root.finalPages) return fail("consolidation_publication_root_invalid");
  const position = `${root.publish.page}:${root.publish.offset}`;
  if (await kv.get(KV.state, pageKey(root.id, "published", position)))
    return fail("consolidation_publication_effect_already_present");
  const page = await candidatePage(kv, root, root.publish.page);
  const pageId = pageKey(root.id, root.finalRound === 0 ? "candidates" : `reduction:${root.finalRound}`, root.publish.page);
  const pageRow = await kv.getVersioned<CandidatePage>(KV.state, pageId);
  if (!pageRow.exists || !isStateCounter(pageRow.version) || page.id !== pageId || !isDeepStrictEqual(pageRow.value, page)
    || !Array.isArray(page.candidates) || root.publish.offset >= page.candidates.length)
    return fail("consolidation_publication_candidate_changed");
  const key = aggregateCandidateKey(root, page.candidates[root.publish.offset]);
  const aggregateRow = await kv.getVersioned<AggregateCandidate>(KV.state, key);
  const aggregate = aggregateRow.value;
  if (!aggregateRow.exists || !aggregate || !isStateCounter(aggregateRow.version) || aggregate.anchor !== position)
    return fail("consolidation_publication_aggregate_changed");
  boundedBytes(aggregate);
  await verifySourceProjects(kv, root);
  const sourceIdentities = [];
  for (let index = 0; index < root.refs.length; index++) {
    const { source, header } = await verifyCapturedSource(kv, root, index);
    const headerRow = await kv.getVersioned(KV.state, pageKey(root.id, "source", index));
    const sessionRow = await kv.getVersioned<{ project: string }>(KV.sessions, root.refs[index].sessionId);
    if (!headerRow.exists || !isDeepStrictEqual(headerRow.value, header) || !isStateCounter(headerRow.version)
      || !sessionRow.exists || !isStateCounter(sessionRow.version) || sessionRow.value?.project !== root.project)
      return fail("consolidation_publication_source_changed");
    sourceIdentities.push([source.version, source.json, headerRow.version, header, sessionRow.version, sessionRow.value]);
  }
  const candidate = await publicationCandidate(kv, root, aggregate);
  const input = await consolidationPublicationEvidence(kv, root, candidate);
  const capture: Capture = { manifestId, project: root.project, rootVersion: row.version,
    page: root.publish.page, offset: root.publish.offset,
    fingerprint: fingerprintId("consolidation-publication-capture", JSON.stringify([
      row.version, root, pageRow.version, page, aggregateRow.version, aggregate, candidate, sourceIdentities, input])) };
  const latestRoot = await kv.getVersioned(KV.state, pageKey(manifestId, "root", 0));
  if (latestRoot.version !== row.version || !isDeepStrictEqual(latestRoot.value, root))
    return fail("consolidation_publication_capture_changed");
  return { capture, input };
}

export function registerConsolidationPublicationRecovery(
  sdk: IIIClient, kv: StateKV, provider: MemoryProvider, router?: LlmTaskRouter,
): void {
  sdk.registerFunction("mem::consolidation-publication-prepare", async (raw: unknown) => {
    const data = invocationPayload(raw);
    if (!object(data) || !closed(data, ["manifestId", "dispositionId", "budget"])
      || !validBudgetId(data.manifestId) || !validBudgetId(data.dispositionId) || !object(data.budget)
      || !closed(data.budget, ["windowId", "ceiling"]) || !validBudgetId(data.budget.windowId) || data.budget.ceiling !== 1)
      return fail("invalid_consolidation_publication_preparation");
    const manifestId = data.manifestId, dispositionId = data.dispositionId, windowId = data.budget.windowId;
    return withKeyedLock(`consolidation-paged:${manifestId}`, async () => {
      const stored = await loadCapture(kv, manifestId);
      const executor = publicationExecutor(provider, router, "consolidation", {});
      const prepare = executor.prepare;
      if (!prepare) return fail("publication_output_capability_unavailable");
      let attemptId = "";
      executor.prepare = request => { attemptId = request.requestId; return prepare(request); };
      const disposition = await preparePublicationJudgeDisposition(kv, stored.input, dispositionId,
        { windowId, ceiling: 1 }, executor);
      const admission: Admission = { version: VERSION, capture: stored.capture, disposition, attemptId };
      return { success: true, admission, providerCalled: false, stateMutationExecuted: false };
    });
  });
  sdk.registerFunction("mem::consolidation-publication-admit", async (raw: unknown) => {
    const data = invocationPayload(raw);
    if (!validAdmission(data)) return fail("invalid_consolidation_publication_admission");
    const admission: Admission = structuredClone(data);
    return withKeyedLock(`consolidation-paged:${admission.capture.manifestId}`, async () => {
      const key = pageKey(admission.capture.manifestId, "publication-admission", `${admission.capture.page}:${admission.capture.offset}`);
      const journal = await kv.get<Journal>(KV.state, key);
      if (journal && !isDeepStrictEqual(journal.admission, admission))
        return fail("consolidation_publication_admission_already_selected");
      const response = (result: PublicationEvidenceResult) => ({ success: ["supported", "unsupported"].includes(result.status),
        admitted: true, completed: false, continuationRequired: ["supported", "unsupported"].includes(result.status), result });
      const pending = journal?.result;
      const reconcilePending = pending?.requestId === admission.attemptId && pending.status === "pending"
        && pending.reason === "dispatch_outcome_unknown";
      const replayCapturedInvalid = pending?.requestId === admission.attemptId && pending.status === "invalid_response"
        && pending.reason === "closed_verdict_required" && !!admission.disposition.captured;
      if (pending && !reconcilePending && !replayCapturedInvalid) return response({ ...pending, replayed: true });
      const stored = await loadCapture(kv, admission.capture.manifestId);
      if (!isDeepStrictEqual(stored.capture, admission.capture)) return fail("consolidation_publication_capture_changed");
      if (!journal) await kv.set(KV.state, key, { admission } satisfies Journal);
      const executor = publicationExecutor(provider, router, "consolidation", {}, async (request, dispatch) => {
        if (replayCapturedInvalid) return fail("consolidation_publication_captured_replay_dispatch_forbidden");
        if (request.requestId !== admission.attemptId) return fail("consolidation_publication_attempt_mismatch");
        const current = await loadCapture(kv, admission.capture.manifestId);
        if (!isDeepStrictEqual(current.capture, admission.capture)) return fail("consolidation_publication_capture_changed");
        return withProviderAttemptBudget({ operationId: request.requestId, ...admission.disposition.budget,
          context: { workflow: "consolidation", functionId: "mem::consolidation-publication-admit",
            sourceId: admission.capture.manifestId, jobId: admission.capture.manifestId, unitId: request.requestId,
            phase: "publication", continuationId: admission.capture.fingerprint } }, dispatch);
      });
      const prepare = executor.prepare;
      if (!prepare) return fail("publication_output_capability_unavailable");
      executor.prepare = request => {
        if (request.requestId !== admission.attemptId) return fail("consolidation_publication_attempt_mismatch");
        return prepare(request);
      };
      const result = await supersedePublicationJudge(kv, stored.input, admission.disposition, executor, undefined,
        { operationId: admission.attemptId, ...admission.disposition.budget });
      if (result.requestId !== admission.attemptId && result.status !== "invalid_input")
        return fail("consolidation_publication_attempt_mismatch");
      await kv.set(KV.state, key, { ...journal, admission, result,
        ...(pending && !isDeepStrictEqual(pending, result)
          ? { priorResults: [...(journal?.priorResults ?? []), pending] } : {}) } satisfies Journal);
      return response(result);
    });
  });
}
