import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { batchEffectKey } from "../state/batch-effects.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { currentReflectManifest, retrieveReflectManifest } from "./reflect-retrieval.js";
import { evaluatePublicationEvidence, preparePublicationEvidence, type PublicationEvidenceInput } from "./publication-evidence.js";
import { REFLECT_CONTROL, REFLECT_JOBS, REFLECT_PENDING, validReflectText, type ReflectJob, type ReflectRequest } from "./reflect-queue-types.js";

export interface ReflectManualOperation { identity: string; jobId: string; request?: ReflectRequest; sessionId?: string }
interface RecoveryJournal {
  version: 1; state: "creating" | "ready"; requestId: string; jobId: string; identity: string;
  operation: string; frozenIdentity: string; initialJob: ReflectJob;
}
export class ReflectRecoveryError extends Error {}
const fail = (message = "invalid_reflect_recovery_source"): never => { throw new ReflectRecoveryError(message); };
const integer = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
const same = (left: unknown, right: unknown): boolean => JSON.stringify(left) === JSON.stringify(right);
const journalKey = (predecessor: string) => `recovery-successor:${predecessor}`;
const selection = (job: ReflectJob) => ({ seeds: job.seeds, seedBinding: job.seedBinding, ordinal: job.ordinal,
  manifest: job.manifest, sourcePage: job.sourcePage, sourceCursor: job.sourceCursor,
  sourcePageBinding: job.sourcePageBinding, deferredFamilies: job.deferredFamilies });
const lineageMatches = (job: ReflectJob, request: ReflectRequest, id: string): boolean => job.id === id
  && same(job.request, request) && job.executionOrigin === "manual" && job.recoveryOfJobId === request.recoveryOfJobId
  && job.manual !== undefined;

export async function reflectRecoveryReady(kv: StateKV, job: ReflectJob): Promise<boolean> {
  const predecessor = job.recoveryOfJobId ?? job.request.recoveryOfJobId;
  if (!predecessor) return true;
  const journal = await kv.get<RecoveryJournal>(REFLECT_CONTROL, journalKey(predecessor));
  return journal?.version === 1 && journal.state === "ready" && journal.jobId === job.id
    && journal.requestId === job.request.requestId && journal.identity === batchEffectKey(JSON.stringify(job.request));
}

export async function validateReflectRecoveryReplay(kv: StateKV, job: ReflectJob, request: ReflectRequest,
  operation: string, identity: string): Promise<void> {
  if (job.recoveryOfJobId !== request.recoveryOfJobId) fail("reflect_recovery_construction_conflict");
  const link = await kv.get<RecoveryJournal | { requestId: string; jobId: string }>(REFLECT_CONTROL, journalKey(request.recoveryOfJobId!));
  if (!link || link.requestId !== request.requestId || link.jobId !== job.id) fail("reflect_recovery_construction_conflict");
  if (!("version" in link)) {
    if (Object.keys(link).length !== 2) fail("reflect_recovery_construction_conflict");
    return;
  }
  if (link.version !== 1 || link.state !== "ready" || link.identity !== identity || link.operation !== operation
    || !link.initialJob || !lineageMatches(link.initialJob, request, job.id)
    || link.frozenIdentity !== batchEffectKey(JSON.stringify(selection(link.initialJob)))) fail("reflect_recovery_construction_conflict");
}

async function validFrozenSelection(kv: StateKV, job: ReflectJob): Promise<boolean> {
  const manifest = job.manifest;
  if (!manifest || !validReflectText(manifest.id) || !validReflectText(manifest.graphGeneration)
    || typeof manifest.prompt !== "string" || !manifest.prompt.trim() || !Array.isArray(manifest.sources) || !manifest.sources.length || manifest.sources.length > 12
    || !Array.isArray(job.seeds) || !job.seeds.length || !job.seeds.every(validReflectText)
    || !integer(job.ordinal) || job.ordinal >= job.seeds.length || !Array.isArray(job.deferredFamilies)) return false;
  const identity = manifest.indexIdentity;
  if (!identity || ![identity.index_id, identity.model, identity.generation].every(validReflectText) || !integer(identity.dimensions)) return false;
  const seedId = job.request.summaryId ?? job.request.sessionId;
  if (seedId && (!job.seedBinding || job.seedBinding.scope !== KV.summaries || job.seedBinding.id !== seedId
    || ![job.seedBinding.version, job.seedBinding.fingerprint].every(validReflectText))) return false;
  const page = job.sourcePage;
  if (manifest.grouped && (!Array.isArray(page) || !page.length || !job.sourcePageBinding)) return false;
  if (page && (!Array.isArray(page) || !page.length || page.length > 12 || !job.sourcePageBinding
    || !same(job.sourcePageBinding.identity, identity) || job.sourcePageBinding.graphGeneration !== manifest.graphGeneration)) return false;
  for (const cursor of [job.sourceCursor, manifest.nextCursor]) {
    if (cursor && (!validReflectText(cursor.pageFingerprint) || !integer(cursor.sourceIndex) || !integer(cursor.byteOffset))) return false;
  }
  const sources = [...manifest.sources, ...(page ?? [])];
  if (sources.some(source => !source || ![source.id, source.scope, source.version].every(validReflectText)
    || typeof source.fingerprint !== "string" || !source.fingerprint.length || !source.observation)) return false;
  if (manifest.grouped) {
    try { if (!same(manifest, await retrieveReflectManifest(kv, structuredClone(job)))) return false; }
    catch { return false; }
  }
  return currentReflectManifest(kv, { ...job, manifest: { ...manifest, sources } });
}

async function eligible(kv: StateKV, predecessor: ReflectJob, request: ReflectRequest,
  evidenceInput: (job: ReflectJob) => PublicationEvidenceInput): Promise<void> {
  if (!predecessor.request || typeof predecessor.request !== "object") fail();
  const manual = predecessor.manual;
  if (predecessor.id !== request.recoveryOfJobId || predecessor.request.requestId === request.requestId
    || predecessor.executionOrigin !== "manual" || predecessor.project !== request.project || predecessor.request.project !== request.project
    || !manual || predecessor.request.sessionId !== request.sessionId
    || predecessor.request.deferred || predecessor.legacy || predecessor.global || predecessor.continuation || predecessor.claim
    || predecessor.generation || predecessor.resultFingerprint || predecessor.gateDecision
    || !integer(manual.cuts) || manual.cuts >= 256 || !integer(manual.startedAt)
    || !integer(predecessor.attempts) || !integer(predecessor.refreshes)
    || !integer(predecessor.reservedInsights) || predecessor.reservedInsights > 50
    || [predecessor.processedClusters, predecessor.clustersSkipped, predecessor.appliedInsights, predecessor.unitSequence].some(value => value !== 0)) fail();
  for (const field of ["summaryId", "query", "maxClusters", "factIds", "lessonIds", "crystalIds", "force"] as const) {
    if (!same(request[field], predecessor.request[field])) fail();
  }
  const ageTerminal = predecessor.status === "partial" && predecessor.error === "manual_reflect_operation_limit";
  if (ageTerminal) {
    if (predecessor.captured) fail();
  } else if (!["blocked", "failed"].includes(predecessor.status) || !predecessor.captured
    || typeof predecessor.captured.response !== "string" || !predecessor.captured.response.length
    || Buffer.byteLength(predecessor.captured.response) > 16384
    || predecessor.captured.insightIds !== undefined || predecessor.captured.evidenceRequestId !== undefined
    || predecessor.captured.publication !== undefined) fail();
  if (!await validFrozenSelection(kv, predecessor)) fail();
  const manifest = predecessor.manifest!;
  const effectKey = batchEffectKey(`${predecessor.id}:${predecessor.ordinal}:${manifest.id}`);
  if (predecessor.captured && predecessor.captured.effectKey !== effectKey) fail();
  const [result, skip, callback, active] = await Promise.all([
    kv.get(REFLECT_CONTROL, `${predecessor.id}:result:0`), kv.get(REFLECT_CONTROL, `${predecessor.id}:skip:0:${manifest.id}`),
    kv.get(KV.batchCallbacks, `reflect:${effectKey}`), kv.get<{ activeKey?: string }>(KV.batchCallbacks, "active:reflect"),
  ]);
  if (result || skip || callback || active && (!validReflectText(active.activeKey) || active.activeKey === effectKey)) fail("reflect_recovery_effect_already_admitted");
  if (predecessor.captured) {
    const input = evidenceInput(predecessor);
    const prepared = preparePublicationEvidence(input);
    if ("status" in prepared || await kv.get(KV.state, prepared.requestId)) fail("reflect_recovery_publication_already_started");
    // Replay older evidence versions without changing their retained checkpoints.
    const existing = await evaluatePublicationEvidence({ get: kv.get.bind(kv), set: async (_scope, _key, value) => value }, input);
    if (existing.status !== "unavailable") fail("reflect_recovery_publication_already_started");
  }
}

export async function constructReflectRecovery(kv: StateKV, request: ReflectRequest, operation: string, identity: string,
  jobId: string, evidenceInput: (job: ReflectJob) => PublicationEvidenceInput): Promise<ReflectManualOperation> {
  const predecessorId = request.recoveryOfJobId!;
  return withKeyedLock(`reflect-job:${predecessorId}`, () => withKeyedLock(`reflect-job:${jobId}`, async () => {
    const key = journalKey(predecessorId);
    const link = await kv.get<RecoveryJournal | { requestId: string; jobId: string }>(REFLECT_CONTROL, key);
    if (link && (link.requestId !== request.requestId || link.jobId !== jobId)) fail("reflect_recovery_already_linked");
    let journal = link && "version" in link ? link : null;
    if (journal && (journal.version !== 1 || !["creating", "ready"].includes(journal.state)
      || journal.identity !== identity || journal.operation !== operation || !journal.initialJob
      || !lineageMatches(journal.initialJob, request, jobId)
      || journal.frozenIdentity !== batchEffectKey(JSON.stringify(selection(journal.initialJob))))) fail("reflect_recovery_construction_conflict");
    const operationRow: ReflectManualOperation = { identity, jobId, request, sessionId: request.sessionId };
    const existingJob = await kv.get<ReflectJob>(REFLECT_JOBS, jobId);
    const existingOperation = await kv.get<ReflectManualOperation>(REFLECT_CONTROL, operation);
    if (existingOperation && !same(existingOperation, operationRow)) fail("reflect_recovery_construction_conflict");
    if (journal?.state === "ready") {
      if (!existingJob || !existingOperation || !lineageMatches(existingJob, request, jobId)) fail("reflect_recovery_construction_conflict");
      return operationRow;
    }
    const predecessor = await kv.get<ReflectJob>(REFLECT_JOBS, predecessorId);
    if (!predecessor) fail();
    await eligible(kv, predecessor, request, evidenceInput);
    if (!journal) {
      const now = new Date().toISOString();
      const initialJob: ReflectJob = { id: jobId, request, project: request.project, ...structuredClone(selection(predecessor)),
        executionOrigin: "manual", recoveryOfJobId: predecessorId, status: "pending", reservedInsights: predecessor.reservedInsights,
        appliedInsights: 0, processedClusters: 0, clustersSkipped: 0, unitSequence: 0, attempts: 0, refreshes: 0,
        createdAt: existingJob?.createdAt ?? now, updatedAt: existingJob?.updatedAt ?? now, nextAttemptAt: 0, dispatchPending: false,
        manual: { cuts: 0, startedAt: existingJob?.manual?.startedAt ?? Date.now() },
        ...(predecessor.captured && { captured: { ...structuredClone(predecessor.captured),
          effectKey: batchEffectKey(`${jobId}:${predecessor.ordinal}:${predecessor.manifest!.id}`) } }) };
      if (existingJob && !same(existingJob, initialJob)) fail("reflect_recovery_construction_conflict");
      journal = { version: 1, state: "creating", requestId: request.requestId!, jobId, operation, identity, initialJob,
        frozenIdentity: batchEffectKey(JSON.stringify(selection(initialJob))) };
      await kv.set(REFLECT_CONTROL, key, journal);
    }
    if (!same(selection(predecessor), selection(journal.initialJob))
      || predecessor.captured?.response !== journal.initialJob.captured?.response) fail("reflect_recovery_construction_conflict");
    if (existingJob && !same(existingJob, journal.initialJob)) fail("reflect_recovery_construction_conflict");
    if (!existingJob) await kv.set(REFLECT_JOBS, jobId, journal.initialJob);
    const pending = { id: jobId, request, executionOrigin: "manual" };
    const existingPending = await kv.get(REFLECT_PENDING, jobId);
    if (existingPending && !same(existingPending, pending)) fail("reflect_recovery_construction_conflict");
    if (!existingPending) await kv.set(REFLECT_PENDING, jobId, pending);
    if (!existingOperation) await kv.set(REFLECT_CONTROL, operation, operationRow);
    const admission = `admission:${request.project ?? "unscoped"}`;
    await withKeyedLock(admission, async () => {
      const current = await kv.get<{ ids: string[] }>(REFLECT_CONTROL, admission) ?? { ids: [] };
      if (!Array.isArray(current.ids) || current.ids.some(id => !validReflectText(id))) fail("reflect_recovery_construction_conflict");
      if (!current.ids.includes(jobId)) {
        if (current.ids.length >= 8) fail("reflect_project_queue_capacity");
        await kv.set(REFLECT_CONTROL, admission, { ids: [...current.ids, jobId] });
      }
    });
    await kv.set(REFLECT_CONTROL, key, { ...journal, state: "ready" });
    return operationRow;
  }));
}
