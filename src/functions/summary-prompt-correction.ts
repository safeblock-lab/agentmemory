import type { StateKV } from "../state/kv.js";
import type { SummaryQueueJob } from "../types.js";
import { KV, fingerprintId } from "../state/schema.js";
import { SUMMARY_GENERATION_REVISION } from "./summary-queue-prompts.js";

export interface SummaryPromptCorrection {
  revision: typeof SUMMARY_GENERATION_REVISION;
  state: "creating" | "ready";
  jobId: string;
  predecessorJobId: string;
  sessionId: string;
  project: string;
  sourceFingerprint: string;
  publicationRequestId: string;
  createdAt: string;
}

export function summaryPromptCorrectionId(job: Pick<SummaryQueueJob, "sessionId" | "project" | "snapshotFingerprint">): string {
  return fingerprintId("sqj", JSON.stringify([
    SUMMARY_GENERATION_REVISION, job.project, job.sessionId, job.snapshotFingerprint,
  ]));
}

export const summaryPromptCorrectionKey = (jobId: string): string => `${jobId}:prompt-correction`;

export function eligibleSummaryPromptPredecessor(job: SummaryQueueJob): boolean {
  return job.status === "failed" && job.failure === "summary_publication_unsupported"
    && job.generationRevision === undefined && job.promptCorrectionOfJobId === undefined
    && job.promptCorrectionRevision === undefined && job.executionOrigin === undefined
    && job.recoveryMode === undefined && job.recoveryOfJobId === undefined
    && job.zeroSpendRecoveryOfJobId === undefined && job.completedAt === undefined
    && typeof job.failedAt === "string" && Number.isFinite(Date.parse(job.failedAt))
    && job.stage === "map" && job.round === 0 && job.unitIds.length === 1
    && job.observationCount === 1;
}

export async function claimSummaryPromptCorrection(
  kv: StateKV, predecessor: SummaryQueueJob, publicationRequestId: string, now: string,
): Promise<SummaryPromptCorrection | null> {
  const jobId = summaryPromptCorrectionId(predecessor);
  const key = summaryPromptCorrectionKey(jobId);
  if (await kv.get(KV.state, key) !== null || await kv.get(KV.summaryQueueJobs, jobId) !== null)
    return null;
  const claimKey = `${key}:admission`;
  if (await kv.get(KV.state, claimKey) !== null) return null;
  const claim = await kv.update<{ claims: number }>(KV.state, claimKey,
    [{ type: "increment", path: "claims", by: 1 }]);
  if (claim?.claims !== 1) return null;
  const journal: SummaryPromptCorrection = {
    revision: SUMMARY_GENERATION_REVISION, state: "creating", jobId,
    predecessorJobId: predecessor.id, sessionId: predecessor.sessionId, project: predecessor.project,
    sourceFingerprint: predecessor.snapshotFingerprint, publicationRequestId, createdAt: now,
  };
  await kv.set(KV.state, key, journal);
  return journal;
}

export async function summaryPromptCorrectionReady(kv: StateKV, job: SummaryQueueJob): Promise<boolean> {
  if (!job.promptCorrectionOfJobId && !job.promptCorrectionRevision) return true;
  if (!job.promptCorrectionOfJobId || job.promptCorrectionRevision !== SUMMARY_GENERATION_REVISION
    || job.generationRevision !== SUMMARY_GENERATION_REVISION || summaryPromptCorrectionId(job) !== job.id)
    return false;
  const journal = await kv.get<SummaryPromptCorrection>(KV.state, summaryPromptCorrectionKey(job.id));
  return journal?.state === "ready" && journal.revision === job.promptCorrectionRevision
    && journal.jobId === job.id && journal.predecessorJobId === job.promptCorrectionOfJobId
    && journal.sessionId === job.sessionId && journal.project === job.project
    && journal.sourceFingerprint === job.snapshotFingerprint;
}
