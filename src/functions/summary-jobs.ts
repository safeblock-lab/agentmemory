import { isDeepStrictEqual } from "node:util";
import type { IIIClient } from "iii-sdk";
import type { MemoryProvider, SessionSummary, SummaryQueueJob } from "../types.js";
import type { StateKV } from "../state/kv.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { withProviderAttemptBudget } from "../providers/provider-attempt-budget.js";
import { validBudgetId } from "../state/provider-attempt-budget.js";
import { KV, fingerprintId } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import type { SummaryQueueUnit } from "./summary-queue.js";
import { getSummaryEvidenceRoot, verifySummaryEvidence } from "./summary-source-grounding.js";
import { publicationExecutor } from "./publication-call-options.js";
import { preparePublicationJudgeMigration, supersedePublicationJudge, validPublicationJudgeDisposition,
  type PublicationEvidenceInput, type PublicationEvidenceResult, type PublicationJudgeDisposition } from "./publication-evidence.js";

const VERSION = "summary-publication-recovery-v1";
interface CaptureIdentity {
  jobId: string; unitId: string; project: string; sessionId: string; sourceFingerprint: string;
  round: number; stage: "map" | "reduce"; captureFingerprint: string;
}
interface Admission {
  version: typeof VERSION; capture: CaptureIdentity; disposition: PublicationJudgeDisposition;
  attemptId: string;
}
interface Journal { admission: Admission; summary: SessionSummary; result?: PublicationEvidenceResult }
const object = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value);
const closed = (value: Record<string, unknown>, fields: string[]): boolean =>
  Object.keys(value).length === fields.length && fields.every(field => Object.hasOwn(value, field));
function validCapture(value: unknown): value is CaptureIdentity {
  return object(value) && closed(value, ["jobId", "unitId", "project", "sessionId", "sourceFingerprint", "round", "stage", "captureFingerprint"])
    && ["jobId", "unitId", "sessionId", "sourceFingerprint", "captureFingerprint"].every(field => validBudgetId(value[field]))
    && typeof value.project === "string" && value.project.length > 0 && value.project.length <= 512
    && Number.isSafeInteger(value.round) && Number(value.round) >= 0 && ["map", "reduce"].includes(String(value.stage));
}
function validAdmission(value: unknown): value is Admission {
  return object(value) && closed(value, ["version", "capture", "disposition", "attemptId"])
    && value.version === VERSION && validCapture(value.capture) && validBudgetId(value.attemptId)
    && validPublicationJudgeDisposition(value.disposition) && !!value.disposition.migration
    && validBudgetId(value.disposition.dispositionId) && validBudgetId(value.disposition.budget.windowId)
    && value.disposition.budget.ceiling === 1;
}
const fail = (code: string): never => { throw new Error(code); };
function invocationPayload(value: unknown): unknown {
  if (!object(value) || !Object.hasOwn(value, "_caller_worker_id")) return value;
  const { _caller_worker_id: caller, ...payload } = value;
  if (typeof caller !== "string" || caller.length < 1 || caller.length > 128)
    return fail("invalid_summary_publication_caller");
  return payload;
}

export function registerSummaryPublicationRecovery(
  sdk: IIIClient, kv: StateKV, provider: MemoryProvider, router: LlmTaskRouter | undefined,
  fields: (summary: SessionSummary) => object,
  complete: (job: SummaryQueueJob, summary: SessionSummary) => Promise<void>,
): void {
  const load = async (jobId: string, unitId: string, allowEffect = false) => {
    const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, jobId);
    const unit = await kv.get<SummaryQueueUnit>(KV.summaryQueueUnits(jobId), unitId);
    if (!job || job.id !== jobId || !(job.status === "pending" || job.status === "failed"
      && job.failure === "summary_publication_pending") || job.recoveryMode || job.recoveryOfJobId
      || job.unitIds.length !== 1 || job.unitIds[0] !== unitId || !unit || unit.id !== unitId
      || unit.jobId !== job.id || unit.round !== job.round || unit.stage !== job.stage
      || !unit.summary || !unit.output || unit.summary.sessionId !== job.sessionId
      || unit.summary.project !== job.project || unit.summary.observationCount !== job.observationCount
      || unit.outputInputHash !== fingerprintId("summary-input", JSON.stringify(unit.items)))
      return fail("summary_publication_capture_unavailable");
    const active = await kv.get<{ jobId: string }>(KV.summaryQueueActive, job.sessionId);
    if (active && active.jobId !== job.id || !allowEffect && await kv.get(KV.state, `${job.id}:publication-effect`))
      return fail("summary_publication_recovery_conflict");
    const root = await getSummaryEvidenceRoot(kv, job);
    if (!root || root.mode !== "complete") return fail("summary_publication_complete_capture_required");
    const sources = await verifySummaryEvidence(kv, job, root);
    const input: PublicationEvidenceInput = { workflow: "summary", operationId: job.id, project: job.project,
      candidate: JSON.stringify(fields(unit.summary)), sources, evidenceComplete: true };
    const capture: CaptureIdentity = { jobId, unitId, project: job.project, sessionId: job.sessionId,
      sourceFingerprint: job.snapshotFingerprint, round: job.round, stage: job.stage,
      captureFingerprint: fingerprintId("summary-publication-capture", JSON.stringify([
        job.id, job.sessionId, job.project, job.snapshotFingerprint, job.round, job.stage, job.observationCount,
        unit.id, unit.outputInputHash, unit.summary, unit.output, input])) };
    return { job, unit, input, capture };
  };
  sdk.registerFunction("mem::summary-publication-prepare", async (raw: unknown) => {
    const data = invocationPayload(raw);
    if (!object(data) || !closed(data, ["jobId", "unitId", "dispositionId", "budget"])
      || !validBudgetId(data.jobId) || !validBudgetId(data.unitId) || !validBudgetId(data.dispositionId)
      || !object(data.budget) || !closed(data.budget, ["windowId", "ceiling"])
      || !validBudgetId(data.budget.windowId) || data.budget.ceiling !== 1)
      return fail("invalid_summary_publication_preparation");
    const stored = await load(data.jobId, data.unitId);
    const executor = publicationExecutor(provider, router, "summary", { task: "summary", outputTokens: 1024 });
    const prepare = executor.prepare;
    if (!prepare) return fail("publication_output_capability_unavailable");
    let attemptId = "";
    executor.prepare = request => { attemptId = request.requestId; return prepare(request); };
    const disposition = await preparePublicationJudgeMigration(kv, stored.input, data.dispositionId,
      { windowId: data.budget.windowId, ceiling: 1 }, executor);
    const admission: Admission = { version: VERSION, capture: stored.capture, disposition, attemptId };
    return { success: true, admission, providerCalled: false, stateMutationExecuted: false };
  });
  sdk.registerFunction("mem::summary-publication-admit", async (raw: unknown) => {
    const data = invocationPayload(raw);
    if (!validAdmission(data)) return fail("invalid_summary_publication_admission");
    // Snapshot the validated payload before awaiting engine or provider calls.
    const admission: Admission = structuredClone(data);
    return withKeyedLock(`summary-session:${admission.capture.sessionId}`, () =>
      withKeyedLock(`summary-unit:${admission.capture.unitId}`, () =>
        withKeyedLock(`summary-job:${admission.capture.jobId}`, async () => {
          const key = `summary-publication-admission:${admission.capture.jobId}`;
          const journal = await kv.get<Journal>(KV.state, key);
          if (journal && !isDeepStrictEqual(journal.admission, admission))
            return fail("summary_publication_admission_already_selected");
          const job = await kv.get<SummaryQueueJob>(KV.summaryQueueJobs, admission.capture.jobId);
          if (job?.status === "completed" && journal?.result?.status === "supported") {
            const effect = await kv.get<{ summary: SessionSummary; completedAt: string }>(KV.state, `${job.id}:publication-effect`);
            const canonical = await kv.get<SessionSummary>(KV.summaries, job.sessionId);
            if (job.sessionId !== admission.capture.sessionId || job.project !== admission.capture.project
              || job.snapshotFingerprint !== admission.capture.sourceFingerprint || !effect
              || effect.completedAt !== job.completedAt || !isDeepStrictEqual(effect.summary, journal.summary)
              || !isDeepStrictEqual(canonical, effect.summary)) return fail("summary_publication_effect_mismatch");
            return { success: true, completed: true, result: { ...journal.result, replayed: true } };
          }
          const stored = await load(admission.capture.jobId, admission.capture.unitId, journal?.result?.status === "supported");
          if (!isDeepStrictEqual(stored.capture, admission.capture)) return fail("summary_publication_capture_changed");
          if (journal && !isDeepStrictEqual(journal.summary, stored.unit.summary)) return fail("summary_publication_capture_changed");
          if (!journal) await kv.set(KV.state, key, { admission, summary: stored.unit.summary! } satisfies Journal);
          const executor = publicationExecutor(provider, router, "summary", { task: "summary", outputTokens: 1024 },
            (request, dispatch) => {
              if (request.requestId !== admission.attemptId) return fail("summary_publication_attempt_mismatch");
              return withProviderAttemptBudget({ operationId: request.requestId, ...admission.disposition.budget,
                context: { workflow: "summary", functionId: "mem::summary-publication-admit",
                  sourceId: stored.job.sessionId, jobId: stored.job.id, unitId: stored.unit.id,
                  phase: "publication", continuationId: admission.capture.captureFingerprint } }, dispatch);
            });
          const prepare = executor.prepare;
          if (!prepare) return fail("publication_output_capability_unavailable");
          executor.prepare = request => {
            if (request.requestId !== admission.attemptId) return fail("summary_publication_attempt_mismatch");
            return prepare(request);
          };
          const result = await supersedePublicationJudge(kv, stored.input, admission.disposition, executor);
          if (result.requestId !== admission.attemptId && result.status !== "invalid_input")
            return fail("summary_publication_attempt_mismatch");
          await kv.set(KV.state, key, { admission, summary: stored.unit.summary!, result } satisfies Journal);
          if (result.status !== "supported") return { success: false, completed: false, result };
          const current = await load(admission.capture.jobId, admission.capture.unitId, true);
          if (!isDeepStrictEqual(current.capture, admission.capture)) return fail("summary_publication_capture_changed");
          await complete(current.job, current.unit.summary!);
          return { success: true, completed: true, result };
        })));
  });
}
