import type { IIIClient } from "iii-sdk";
import type { CompressedObservation, Session, SessionSummary } from "../types.js";
import { detectLlmProviderKind, isBackgroundRecoveryPaused, isConsolidationEnabled } from "../config.js";
import { KV, fingerprintId, generateId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { StatePageError } from "../state/state-pages.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { batchEffectKey } from "../state/batch-effects.js";
import { isProtectedSkillSession, recoverAppliedSkillEffect } from "./skill-extract.js";
import { summarySourceFingerprint } from "./summary-queue.js";
import { consolidationRetrievalCoverage, type ConsolidationRetrievalCoverage } from "./consolidation-selection.js";

const SKILL_TOPIC = "agentmemory.skill-lifecycle.dispatch";
const RECOVERY_KEY = "summary-completed";
const PAGE_SIZE = 32;
const PAGE_BYTES = 256 * 1024;
const MAX_ATTEMPTS = 6;
const MAX_ENQUEUE_FAILURES = 6;
const DELIVERY_LEASE_MS = 120_000;
const BASE_BACKOFF_MS = 30_000;
const MAX_BACKOFF_MS = 60 * 60 * 1000;
const MAX_INDEX_PREPARATION_BACKOFF_LEVEL = 8;

export type SkillExtractionCapture =
  | { kind: "skill"; skill: { trigger: string; title: string; steps: string[]; expectedOutcome: string; tags: string[] };
      retrievalCoverage?: ConsolidationRetrievalCoverage[] }
  | { kind: "skip"; reason: "typesafe_gate" | "no_skill" | "too_few_observations";
      retrievalCoverage?: ConsolidationRetrievalCoverage[] };

export interface SkillLifecycleSession {
  kind: "session";
  sessionId: string;
  consolidationEnabled: boolean;
  skipConsolidation: boolean;
  providerEligible: boolean;
  capturedAt: string;
}

export interface SkillLifecycleIntent {
  kind: "intent";
  admission?: "selected";
  project?: string;
  summaryContentFingerprint?: string;
  id: string;
  effectId: string;
  sessionId: string;
  summaryFingerprint: string;
  consolidationEnabled: boolean;
  skipConsolidation: boolean;
  providerEligible: boolean;
  protectedSignal: boolean;
  status: "queued" | "running" | "completed" | "skipped" | "stale" | "failed";
  attempts: number;
  enqueueFailures: number;
  indexPreparationBackoffLevel?: number;
  createdAt: string;
  updatedAt: string;
  nextAttemptAt?: string;
  deliveryId?: string;
  deliveryExpiresAt?: string;
  leaseExpiresAt?: string;
  terminalReason?: string;
  lastFailure?: { code: string; errorType: string; recordedAt: string };
  auditReceipt?: { id: string; outcome: string; targetId?: string; recordedAt: string };
  effectPlan?: { skillId: string; created: boolean; reinforced: boolean };
  skillId?: string;
  retrievalCoverage?: ConsolidationRetrievalCoverage[];
  capturedResult?: SkillExtractionCapture;
}

interface CompletedSnapshot {
  sessionId: string;
  fingerprint: string;
  completedAt: string;
}

interface ActiveSummaryJob { jobId: string }
interface SummaryJobIdentity { snapshotFingerprint: string; status: "pending" | "completed" | "failed" }
interface RecoveryCursor { cursor: string | null; updatedAt: string }

interface SelectedSkill {
  project: string; sessionId: string; sourceFingerprint: string;
  intentId?: string; deliveryId?: string;
}

function selectedSkill(value: unknown): SelectedSkill | null {
  if (!isRecord(value) ||
      !["project", "sessionId", "sourceFingerprint"].every(key =>
        typeof value[key] === "string" && value[key].trim().length > 0 && value[key] === value[key].trim()) ||
      !Object.keys(value).every(key => ["project", "sessionId", "sourceFingerprint", "intentId", "deliveryId"].includes(key)) ||
      ["intentId", "deliveryId"].some(key => value[key] !== undefined &&
        (typeof value[key] !== "string" || !value[key].trim()))) return null;
  return value as unknown as SelectedSkill;
}

function summaryContentFingerprint(summary: SessionSummary): string {
  return fingerprintId("skill-summary", JSON.stringify(summary));
}

async function admitSelected(kv: StateKV, value: unknown, sessionId: string, skip = false) {
  const selected = selectedSkill(value);
  if (!selected || selected.sessionId !== sessionId || selected.deliveryId !== undefined) {
    return { success: false, error: "invalid selected skill identity" };
  }
  const [session, summary, completed, lifecycle, active] = await Promise.all([
    kv.get<Session>(KV.sessions, sessionId), kv.get<SessionSummary>(KV.summaries, sessionId),
    kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId),
    kv.get<SkillLifecycleSession>(KV.skillLifecycleIntents, sessionKey(sessionId)),
    kv.get<ActiveSummaryJob>(KV.summaryQueueActive, sessionId),
  ]);
  if (lifecycle?.skipConsolidation) return { success: true, skipped: true, reason: "skip_consolidation" };
  if (session?.status !== "completed" || session.id !== sessionId || session.project !== selected.project ||
      !summary || summary.sessionId !== sessionId || summary.project !== selected.project ||
      completed?.sessionId !== sessionId || completed.fingerprint !== selected.sourceFingerprint ||
      !Number.isFinite(Date.parse(completed.completedAt))) return { success: false, stale: true, reason: "selected_summary_unavailable" };
  if (active?.jobId) {
    const job = await kv.get<SummaryJobIdentity>(KV.summaryQueueJobs, active.jobId);
    if (job?.status === "pending") return { success: false, stale: true, reason: "summary_superseded" };
  }
  if (await summarySourceFingerprint(kv, sessionId, selected.project) !== selected.sourceFingerprint) {
    return { success: false, stale: true, reason: "source_changed" };
  }
  if (skip) {
    await kv.set(KV.skillLifecycleIntents, sessionKey(sessionId), {
      ...(lifecycle ?? { kind: "session", sessionId, consolidationEnabled: isConsolidationEnabled(),
        providerEligible: detectLlmProviderKind() === "llm", capturedAt: new Date().toISOString() }),
      skipConsolidation: true,
    } satisfies SkillLifecycleSession);
    return { success: true, skipped: true, reason: "skip_consolidation" };
  }
  const contentFingerprint = summaryContentFingerprint(summary);
  const id = `intent:${fingerprintId("skill-selected", JSON.stringify([sessionId, selected.project, selected.sourceFingerprint, contentFingerprint]))}`;
  if (selected.intentId !== undefined && selected.intentId !== id) return { success: false, stale: true, reason: "intent_mismatch" };
  return withKeyedLock(`skill-intent:${id}`, async () => {
    let intent = await readIntent(kv, id);
    if (!intent) {
      const now = new Date().toISOString();
      intent = { kind: "intent", admission: "selected", id,
        effectId: batchEffectKey(JSON.stringify(["skill-selected-effect", sessionId, selected.sourceFingerprint, contentFingerprint])),
        sessionId, project: selected.project, summaryFingerprint: selected.sourceFingerprint,
        summaryContentFingerprint: contentFingerprint, consolidationEnabled: false,
        skipConsolidation: false, providerEligible: false, protectedSignal: await protectedSignalFor(kv, sessionId, summary),
        status: "queued", attempts: 0, enqueueFailures: 0, createdAt: now, updatedAt: now };
      await writeIntent(kv, intent);
    }
    return { ...preservedIntentResult(intent), intentId: id, selected: true };
  });
}

function sessionKey(sessionId: string): string { return `session:${sessionId}`; }

function intentKey(sessionId: string, fingerprint: string): string {
  return `intent:${fingerprintId("skill-lifecycle", JSON.stringify([sessionId, fingerprint]))}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function coverageFields(...values: unknown[]): { retrievalCoverage?: ConsolidationRetrievalCoverage[] } {
  const retrievalCoverage = consolidationRetrievalCoverage(values.map(value => ({ retrievalCoverage: value })));
  return retrievalCoverage.length ? { retrievalCoverage } : {};
}

function validContinuation(result: Record<string, unknown>, intent: SkillLifecycleIntent): boolean {
  const continuation = result.continuation;
  return result.completed === false && result.extracted !== true && result.reason === "skill_source_fragmentation" &&
    typeof result.jobId === "string" && result.jobId.trim().length > 0 &&
    isRecord(continuation) && continuation.sessionId === intent.sessionId &&
    (continuation.intentId === undefined || continuation.intentId === intent.id) &&
    (continuation.snapshotFingerprint === undefined || continuation.snapshotFingerprint === intent.summaryFingerprint);
}

function validIndexPreparationContinuation(result: Record<string, unknown>, intent: SkillLifecycleIntent): boolean {
  const continuation = result.continuation;
  return result.completed === false && result.extracted !== true && result.reason === "skill_index_preparing" &&
    result.jobId === undefined && isRecord(continuation) &&
    continuation.sessionId === intent.sessionId && continuation.intentId === intent.id &&
    continuation.snapshotFingerprint === intent.summaryFingerprint && Object.keys(continuation).length === 3;
}

function preservedIntentResult(intent: SkillLifecycleIntent) {
  return { success: intent.status !== "failed", status: intent.status,
    terminal: ["completed", "skipped", "stale", "failed"].includes(intent.status),
    ...(intent.status === "queued" ? { queued: true } : {}),
    ...(intent.status === "failed" ? { failureCode: intent.terminalReason, reason: intent.terminalReason } : {}),
    ...coverageFields(intent.retrievalCoverage, intent.capturedResult?.retrievalCoverage) };
}

function backoffMs(attempt: number): number {
  return Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS * 2 ** Math.max(0, attempt - 1));
}

function failureCode(error: unknown): string {
  const message = error instanceof Error ? error.message.toLowerCase() : "";
  if (message.includes("source_retrieval_resource_limit")) return "source_retrieval_resource_limit";
  if (/timed out|timeout|aborterror/.test(message)) return "provider_timeout";
  if (/rate.?limit|\b429\b/.test(message)) return "provider_rate_limited";
  if (/fetch failed|econnreset|econnrefused|network/.test(message)) return "provider_unavailable";
  if (message.includes("skill_effect_plan_mismatch")) return "effect_plan_conflict";
  return "skill_extraction_failed";
}

function safeErrorType(error: unknown): string {
  const name = error instanceof Error ? error.name : "UnknownError";
  return /^[A-Za-z][A-Za-z0-9]{0,39}$/.test(name) ? name : "UnknownError";
}

function dueAt(value: string | undefined, now: number): boolean {
  if (!value) return true;
  const timestamp = Date.parse(value);
  return !Number.isFinite(timestamp) || timestamp <= now;
}

function lifecycleSkipReason(session: SkillLifecycleSession): string | undefined {
  if (!session.consolidationEnabled) return "consolidation_disabled";
  if (session.skipConsolidation) return "skip_consolidation";
  if (!session.providerEligible) return "provider_ineligible";
  return undefined;
}

async function readIntent(kv: StateKV, id: string): Promise<SkillLifecycleIntent | null> {
  const intent = await kv.get<SkillLifecycleIntent>(KV.skillLifecycleIntents, id);
  return intent?.kind === "intent" && intent.id === id ? intent : null;
}

async function writeIntent(kv: StateKV, intent: SkillLifecycleIntent): Promise<void> {
  await kv.set(KV.skillLifecycleIntents, intent.id, intent);
}

async function markTerminal(
  kv: StateKV, intent: SkillLifecycleIntent, status: "skipped" | "stale" | "failed", reason: string,
): Promise<SkillLifecycleIntent> {
  const now = new Date().toISOString();
  const persisted = await readIntent(kv, intent.id) ?? intent;
  const terminal = {
    ...persisted, status, terminalReason: reason,
    deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined,
    nextAttemptAt: undefined, updatedAt: now,
    auditReceipt: { id: `${intent.id}:${status}`, outcome: status, recordedAt: now },
  } satisfies SkillLifecycleIntent;
  await writeIntent(kv, terminal);
  return terminal;
}

async function publishIntent(sdk: IIIClient, kv: StateKV, intent: SkillLifecycleIntent): Promise<SkillLifecycleIntent> {
  if (intent.admission === "selected") return intent;
  if (isBackgroundRecoveryPaused()) return intent;
  const now = Date.now();
  if (!dueAt(intent.nextAttemptAt, now) ||
      intent.deliveryId && !dueAt(intent.deliveryExpiresAt, now)) return intent;
  const deliveryId = generateId("skd");
  const queued = {
    ...intent, status: "queued" as const, deliveryId,
    deliveryExpiresAt: new Date(now + DELIVERY_LEASE_MS).toISOString(),
    updatedAt: new Date(now).toISOString(),
  } satisfies SkillLifecycleIntent;
  await writeIntent(kv, queued);
  try {
    await sdk.trigger({ function_id: "iii::durable::publish", payload: {
      topic: SKILL_TOPIC, data: { intentId: queued.id, deliveryId },
    } });
    return queued;
  } catch (error) {
    const failures = queued.enqueueFailures + 1;
    const lastFailure = {
      code: "durable_queue_unavailable",
      errorType: safeErrorType(error),
      recordedAt: new Date().toISOString(),
    };
    const updated = {
      ...queued, enqueueFailures: failures, deliveryId: undefined, deliveryExpiresAt: undefined,
      nextAttemptAt: new Date(Date.now() + backoffMs(failures)).toISOString(),
      lastFailure, updatedAt: lastFailure.recordedAt,
    } satisfies SkillLifecycleIntent;
    await writeIntent(kv, updated);
    if (failures >= MAX_ENQUEUE_FAILURES) return markTerminal(kv, updated, "failed", lastFailure.code);
    throw error;
  }
}

async function ensureIntent(
  sdk: IIIClient, kv: StateKV, sessionId: string, fingerprint: string,
): Promise<SkillLifecycleIntent | null> {
  const [session, summary, completed, lifecycle, active] = await Promise.all([
    kv.get<Session>(KV.sessions, sessionId),
    kv.get<SessionSummary>(KV.summaries, sessionId),
    kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId),
    kv.get<SkillLifecycleSession>(KV.skillLifecycleIntents, sessionKey(sessionId)),
    kv.get<ActiveSummaryJob>(KV.summaryQueueActive, sessionId),
  ]);
  if (active?.jobId) {
    const pendingJob = await kv.get<SummaryJobIdentity>(KV.summaryQueueJobs, active.jobId);
    if (pendingJob?.status === "pending" && pendingJob.snapshotFingerprint !== fingerprint) {
      const superseded = await readIntent(kv, intentKey(sessionId, fingerprint));
      return superseded && !["completed", "skipped", "stale", "failed"].includes(superseded.status)
        ? markTerminal(kv, superseded, "stale", "summary_superseded")
        : superseded;
    }
  }
  const id = intentKey(sessionId, fingerprint);
  let existing = await readIntent(kv, id);
  if (completed?.fingerprint && completed.fingerprint !== fingerprint) {
    if (existing && !["completed", "skipped", "stale", "failed"].includes(existing.status)) {
      return markTerminal(kv, existing, "stale", "summary_superseded");
    }
    return existing;
  }
  if (!summary || summary.sessionId !== sessionId || completed?.fingerprint !== fingerprint ||
      lifecycle?.kind !== "session" || lifecycle.sessionId !== sessionId) return null;

  if (existing) {
    if (existing.status === "running" && dueAt(existing.leaseExpiresAt, Date.now())) {
      if (existing.attempts >= MAX_ATTEMPTS) return markTerminal(kv, existing, "failed", "retry_exhausted");
      existing = {
        ...existing, status: "queued", deliveryId: undefined, deliveryExpiresAt: undefined,
        leaseExpiresAt: undefined,
        nextAttemptAt: new Date(Date.now() + backoffMs(existing.attempts)).toISOString(),
        updatedAt: new Date().toISOString(),
      };
      await writeIntent(kv, existing);
    }
    if (existing.status === "queued" && session?.status === "completed") {
      return publishIntent(sdk, kv, existing);
    }
    return existing;
  }

  const now = new Date().toISOString();
  const effectId = batchEffectKey(JSON.stringify(["skill-effect", sessionId, fingerprint]));
  const reason = lifecycleSkipReason(lifecycle);
  let intent: SkillLifecycleIntent = {
    kind: "intent", id, effectId, sessionId, summaryFingerprint: fingerprint,
    consolidationEnabled: lifecycle.consolidationEnabled,
    skipConsolidation: lifecycle.skipConsolidation,
    providerEligible: lifecycle.providerEligible,
    protectedSignal: await protectedSignalFor(kv, sessionId, summary),
    status: reason ? "skipped" : "queued", attempts: 0, enqueueFailures: 0,
    createdAt: now, updatedAt: now,
    ...(reason ? {
      terminalReason: reason,
      auditReceipt: { id: `${id}:skip`, outcome: "skipped", recordedAt: now },
    } : {}),
  };
  await writeIntent(kv, intent);
  if (reason) return intent;
  if (session?.status === "completed") intent = await publishIntent(sdk, kv, intent);
  return intent;
}

async function validateFinalSnapshot(
  kv: StateKV, intent: SkillLifecycleIntent,
): Promise<"ready" | "waiting" | "stale"> {
  const [session, summary, completed, active] = await Promise.all([
    kv.get<Session>(KV.sessions, intent.sessionId),
    kv.get<SessionSummary>(KV.summaries, intent.sessionId),
    kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, intent.sessionId),
    kv.get<ActiveSummaryJob>(KV.summaryQueueActive, intent.sessionId),
  ]);
  if (completed && completed.fingerprint !== intent.summaryFingerprint) return "stale";
  if (!completed || !summary || summary.sessionId !== intent.sessionId || session?.status !== "completed") return "waiting";
  if (intent.admission === "selected" && active?.jobId) {
    const job = await kv.get<SummaryJobIdentity>(KV.summaryQueueJobs, active.jobId);
    if (job?.status === "pending") return "stale";
  }
  if (intent.admission === "selected" && (session.project !== intent.project || summary.project !== intent.project ||
      completed.sessionId !== intent.sessionId || !Number.isFinite(Date.parse(completed.completedAt)) ||
      summaryContentFingerprint(summary) !== intent.summaryContentFingerprint ||
      await summarySourceFingerprint(kv, intent.sessionId, session.project) !== intent.summaryFingerprint)) return "stale";
  return "ready";
}

async function protectedSignalFor(
  kv: StateKV, sessionId: string, summary: SessionSummary,
): Promise<boolean> {
  if (isProtectedSkillSession(summary, [])) return true;
  for await (const observation of kv.values<CompressedObservation>(KV.observations(sessionId), {
    limit: PAGE_SIZE, maxBytes: PAGE_BYTES,
  })) {
    if (isProtectedSkillSession(summary, [observation])) return true;
  }
  return false;
}

async function recoverMarkerPage(sdk: IIIClient, kv: StateKV, limit: number): Promise<{
  inspected: number; cursor: string | null; restarted: boolean;
}> {
  const recovery = await kv.get<RecoveryCursor>(KV.skillLifecycleRecovery, RECOVERY_KEY);
  const cursor = recovery?.cursor ?? undefined;
  const pages = kv.pages<CompletedSnapshot>(KV.summaryQueueCompleted, {
    ...(cursor ? { cursor } : {}), limit, maxBytes: PAGE_BYTES,
  });
  try {
    const next = await pages.next();
    if (next.done) return { inspected: 0, cursor: null, restarted: false };
    for (const marker of next.value.items) {
      if (typeof marker.sessionId !== "string" || typeof marker.fingerprint !== "string") continue;
      const intent = await ensureIntent(sdk, kv, marker.sessionId, marker.fingerprint);
      if (intent?.status === "queued") await maybeDispatch(sdk, kv, intent);
    }
    const nextCursor = next.value.next_cursor;
    await kv.set(KV.skillLifecycleRecovery, RECOVERY_KEY, {
      cursor: nextCursor, updatedAt: new Date().toISOString(),
    } satisfies RecoveryCursor);
    return { inspected: next.value.items.length, cursor: nextCursor, restarted: false };
  } catch (error) {
    if (!(error instanceof StatePageError) || error.code !== "STATE_PAGE_CURSOR_STALE") throw error;
    await kv.set(KV.skillLifecycleRecovery, RECOVERY_KEY, {
      cursor: null, updatedAt: new Date().toISOString(),
    } satisfies RecoveryCursor);
    return { inspected: 0, cursor: null, restarted: true };
  } finally {
    await pages.return(undefined);
  }
}

async function maybeDispatch(sdk: IIIClient, kv: StateKV, intent: SkillLifecycleIntent): Promise<void> {
  if (intent.status !== "queued" || isBackgroundRecoveryPaused() || !dueAt(intent.nextAttemptAt, Date.now())) return;
  if (intent.deliveryId && !dueAt(intent.deliveryExpiresAt, Date.now())) return;
  if (await validateFinalSnapshot(kv, intent) !== "ready") return;
  await publishIntent(sdk, kv, intent);
}

export function registerSkillLifecycleFunctions(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::skill-lifecycle-session-stopped", async (data: {
    sessionId?: string; skipConsolidation?: boolean; selected?: unknown;
  }) => {
    if (typeof data?.sessionId !== "string" || !data.sessionId.trim()) return { success: false, error: "sessionId is required" };
    const sessionId = data.sessionId.trim();
    if (data.selected !== undefined) return admitSelected(kv, data.selected, sessionId, data.skipConsolidation === true);
    const key = sessionKey(sessionId);
    const current = await kv.get<SkillLifecycleSession>(KV.skillLifecycleIntents, key);
    const captured = current ?? {
      kind: "session", sessionId, consolidationEnabled: isConsolidationEnabled(),
      skipConsolidation: data.skipConsolidation === true,
      providerEligible: detectLlmProviderKind() === "llm", capturedAt: new Date().toISOString(),
    } satisfies SkillLifecycleSession;
    const updated = data.skipConsolidation === true && !captured.skipConsolidation
      ? { ...captured, skipConsolidation: true }
      : captured;
    if (!current || updated !== current) await kv.set(KV.skillLifecycleIntents, key, updated);
    if (data.skipConsolidation === true) {
      const completed = await kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId);
      const intent = completed?.fingerprint
        ? await readIntent(kv, intentKey(sessionId, completed.fingerprint))
        : null;
      if (intent && !["completed", "skipped", "stale", "failed"].includes(intent.status)) {
        await markTerminal(kv, intent, "skipped", "skip_consolidation");
      }
    }
    return { success: true, captured: true };
  });

  sdk.registerFunction("mem::skill-lifecycle-summary-ready", async (data: {
    sessionId?: string; snapshotFingerprint?: string; selected?: unknown;
  }) => {
    if (typeof data?.sessionId !== "string" || !data.sessionId.trim() ||
        typeof data.snapshotFingerprint !== "string" || !data.snapshotFingerprint.trim()) {
      return { success: false, error: "invalid summary identity" };
    }
    if (data.selected !== undefined) {
      if (selectedSkill(data.selected)?.sourceFingerprint !== data.snapshotFingerprint) return { success: false, error: "invalid selected summary identity" };
      return admitSelected(kv, data.selected, data.sessionId.trim());
    }
    const intent = await ensureIntent(sdk, kv, data.sessionId.trim(), data.snapshotFingerprint);
    return { success: true, queued: intent?.status === "queued", failed: intent?.status === "failed",
      ...(intent?.status === "failed" ? { failureCode: intent.terminalReason } : {}), intentId: intent?.id };
  });

  sdk.registerFunction("mem::skill-lifecycle-session-ended", async (data: { sessionId?: string; selected?: unknown }) => {
    if (typeof data?.sessionId !== "string" || !data.sessionId.trim()) return { success: false, error: "sessionId is required" };
    const sessionId = data.sessionId.trim();
    if (data.selected !== undefined) return admitSelected(kv, data.selected, sessionId);
    const completed = await kv.get<CompletedSnapshot>(KV.summaryQueueCompleted, sessionId);
    if (completed?.fingerprint) {
      const intent = await ensureIntent(sdk, kv, sessionId, completed.fingerprint);
      return { success: true, queued: intent?.status === "queued", failed: intent?.status === "failed",
        ...(intent?.status === "failed" ? { failureCode: intent.terminalReason } : {}) };
    }
    return { success: true, queued: false };
  });

  sdk.registerFunction("mem::skill-lifecycle-summary-written", async (data: {
    key?: string; new_value?: CompletedSnapshot; selected?: unknown;
  }) => {
    const marker = data?.new_value;
    if (data?.selected !== undefined) {
      if (typeof data.key !== "string" || marker?.sessionId !== data.key ||
          marker.fingerprint !== selectedSkill(data.selected)?.sourceFingerprint) return { success: false, error: "invalid selected summary identity" };
      return admitSelected(kv, data.selected, data.key);
    }
    if (typeof data?.key !== "string" || typeof marker?.fingerprint !== "string") return { success: true, skipped: true };
    const intent = await ensureIntent(sdk, kv, data.key, marker.fingerprint);
    return { success: true, queued: intent?.status === "queued", failed: intent?.status === "failed",
      ...(intent?.status === "failed" ? { failureCode: intent.terminalReason } : {}) };
  });
  sdk.registerTrigger({
    type: "state", function_id: "mem::skill-lifecycle-summary-written",
    config: { scope: KV.summaryQueueCompleted },
  });

  sdk.registerFunction("mem::skill-lifecycle-dispatch", async (data: {
    intentId?: string; deliveryId?: string; foreground?: boolean; selected?: unknown;
  }) => {
    const foreground = data?.foreground === true;
    if (typeof data?.intentId !== "string" || !data.intentId ||
        (data.deliveryId !== undefined && typeof data.deliveryId !== "string") ||
        (!foreground && typeof data.deliveryId !== "string")) return { success: false, error: "invalid delivery" };
    const result = await withKeyedLock(`skill-intent:${data.intentId}`, async () => {
      let intent = await readIntent(kv, data.intentId!);
      if (data.selected !== undefined || intent?.admission === "selected") {
        const selected = selectedSkill(data.selected);
        if (!foreground || !selected || intent?.admission !== "selected" || selected.intentId !== intent.id ||
            selected.sessionId !== intent.sessionId || selected.project !== intent.project ||
            selected.sourceFingerprint !== intent.summaryFingerprint || selected.deliveryId !== data.deliveryId) {
          return { success: false, error: "invalid selected delivery" };
        }
      }
      if (!intent || (intent.deliveryId ? intent.deliveryId !== data.deliveryId
        : !foreground || data.deliveryId !== undefined)) return { success: true, stale: true };
      if (intent.status === "completed" || intent.status === "skipped" || intent.status === "stale") return { success: true, terminal: true };
      if (intent.status === "failed") return { success: false, terminal: true, error: "skill_extraction_failed",
        failureCode: intent.terminalReason, reason: intent.terminalReason,
        ...coverageFields(intent.retrievalCoverage, intent.capturedResult?.retrievalCoverage) };
      if (isBackgroundRecoveryPaused() && !foreground) return { success: true, queued: true, paused: true };
      if (intent.effectPlan && intent.capturedResult?.kind === "skill") {
        const session = await kv.get<Session>(KV.sessions, intent.sessionId);
        if (session?.id === intent.sessionId && session.status === "completed"
          && (intent.project === undefined || session.project === intent.project)) {
          const applied = await recoverAppliedSkillEffect(kv, intent);
          if (applied) {
            const now = new Date().toISOString();
            await writeIntent(kv, { ...intent, status: "completed", skillId: applied.skillId,
              deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined, nextAttemptAt: undefined,
              updatedAt: now, auditReceipt: { id: `${intent.id}:effect`, targetId: applied.skillId,
                outcome: applied.reinforced ? "reinforced" : applied.created ? "extracted" : "reused", recordedAt: now } });
            return { success: true, completed: true, effectReplay: true,
              ...coverageFields(intent.retrievalCoverage, intent.capturedResult.retrievalCoverage) };
          }
        }
      }
      if (intent.status === "running" && !dueAt(intent.leaseExpiresAt, Date.now())) return { success: true, queued: true, active: true };
      const snapshot = await validateFinalSnapshot(kv, intent);
      if (snapshot === "stale") {
        intent = await markTerminal(kv, intent, "stale", "summary_superseded");
        return { success: true, stale: true };
      }
      if (snapshot === "waiting") {
        await writeIntent(kv, {
          ...intent, status: "queued", deliveryId: undefined, deliveryExpiresAt: undefined,
          updatedAt: new Date().toISOString(),
        });
        return { success: true, queued: true, waitingForCompletion: true };
      }
      if (intent.admission === "selected") {
        const lifecycle = await kv.get<SkillLifecycleSession>(KV.skillLifecycleIntents, sessionKey(intent.sessionId));
        if (lifecycle?.skipConsolidation) {
          await markTerminal(kv, intent, "skipped", "skip_consolidation");
          return { success: true, skipped: true, reason: "skip_consolidation" };
        }
      }
      if (!dueAt(intent.nextAttemptAt, Date.now())) return { success: true, queued: true, deferred: true };
      intent = {
        ...intent, status: "running", attempts: intent.attempts + 1,
        ...(!intent.deliveryId ? { deliveryId: generateId("skd"),
          deliveryExpiresAt: new Date(Date.now() + DELIVERY_LEASE_MS).toISOString() } : {}),
        leaseExpiresAt: new Date(Date.now() + DELIVERY_LEASE_MS).toISOString(),
        nextAttemptAt: undefined, updatedAt: new Date().toISOString(),
      };
      await writeIntent(kv, intent);

      try {
        const result: unknown = await sdk.trigger({ function_id: "mem::skill-extract", payload: {
          sessionId: intent.sessionId, intentId: intent.id,
          snapshotFingerprint: intent.summaryFingerprint,
          ...(intent.admission === "selected" ? { selected: data.selected } : {}),
        } });
        let persisted = await readIntent(kv, intent.id);
        if (!persisted) throw new Error("skill_intent_missing");
        if (persisted.status !== "running") return preservedIntentResult(persisted);
        if (!isRecord(result)) throw new Error("skill_extraction_failed");
        if (result.success !== true) {
          if (result.errorCode === "STATE_RETRIEVAL_RESOURCE_LIMIT" &&
              result.failureCode === "source_retrieval_resource_limit") {
            throw new Error("source_retrieval_resource_limit");
          }
          throw new Error("skill_extraction_failed");
        }
        const continuationCoverage = isRecord(result.continuation)
          ? result.continuation.retrievalCoverage : undefined;
        const retrievalCoverage = consolidationRetrievalCoverage([
          { retrievalCoverage: persisted.retrievalCoverage },
          { retrievalCoverage: result.retrievalCoverage },
          { retrievalCoverage: continuationCoverage },
        ]);
        if (retrievalCoverage.length && JSON.stringify(retrievalCoverage) !== JSON.stringify(persisted.retrievalCoverage)) {
          persisted = { ...persisted, retrievalCoverage };
          await writeIntent(kv, persisted);
        }
        const resultCoverage = coverageFields(retrievalCoverage);
        if (result.stale === true) {
          await markTerminal(kv, intent, "stale", "summary_superseded");
          return { success: true, stale: true, ...resultCoverage };
        }
        if (result.queued === true) {
          if (result.reason === "skill_index_preparing") {
            if (!validIndexPreparationContinuation(result, intent)) throw new Error("invalid_skill_index_continuation");
            const level = Math.min(MAX_INDEX_PREPARATION_BACKOFF_LEVEL,
              (persisted.indexPreparationBackoffLevel ?? 0) + 1);
            const nextAttemptAt = new Date(Date.now() + backoffMs(level)).toISOString();
            await writeIntent(kv, {
              ...persisted, status: "queued", attempts: intent.attempts - 1,
              indexPreparationBackoffLevel: level,
              deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined,
              nextAttemptAt, updatedAt: new Date().toISOString(),
            });
            return { success: true, queued: true, completed: false, reason: result.reason,
              continuation: result.continuation, retryAt: nextAttemptAt, ...resultCoverage };
          }
          if (!validContinuation(result, intent)) throw new Error("invalid_skill_continuation");
          await writeIntent(kv, {
            ...persisted, status: "queued", attempts: intent.attempts - 1,
            deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined,
            nextAttemptAt: undefined, updatedAt: new Date().toISOString(),
          });
          return { success: true, queued: true, completed: false, reason: result.reason,
            jobId: result.jobId, continuation: result.continuation, ...resultCoverage };
        }
        if (result.extracted !== true) {
          const hasReason = typeof result.reason === "string" && result.reason.trim().length > 0;
          if (result.skipped !== true && !(result.extracted === false && hasReason)) {
            throw new Error("invalid_skill_extraction_result");
          }
          const reason = hasReason && typeof result.reason === "string"
            ? result.reason
            : "extraction_skipped";
          await markTerminal(kv, intent, "skipped", reason);
          return { success: true, skipped: true, reason, ...resultCoverage };
        }
        if (typeof result.skillId !== "string" || !result.skillId.trim()) {
          throw new Error("invalid_skill_extraction_result");
        }
        const now = new Date().toISOString();
        const completed = {
          ...persisted, status: "completed" as const,
          skillId: typeof result.skillId === "string" ? result.skillId : undefined,
          ...resultCoverage,
          deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined,
          updatedAt: now,
          auditReceipt: {
            id: `${intent.id}:effect`,
            outcome: result.reinforced === true ? "reinforced" : result.created === true ? "extracted" : "reused",
            ...(typeof result.skillId === "string" ? { targetId: result.skillId } : {}), recordedAt: now,
          },
        } satisfies SkillLifecycleIntent;
        await writeIntent(kv, completed);
        return { success: true, completed: true, ...resultCoverage };
      } catch (error) {
        let updated = await readIntent(kv, intent.id);
        if (!updated) return { success: false, error: "skill_intent_missing" };
        if (updated.status !== "running") return preservedIntentResult(updated);
        const code = failureCode(error);
        const lastFailure = { code, errorType: safeErrorType(error), recordedAt: new Date().toISOString() };
        if (updated.attempts >= MAX_ATTEMPTS) {
          const failed = {
            ...updated, status: "failed" as const, terminalReason: code, lastFailure,
            deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined,
            nextAttemptAt: undefined, updatedAt: lastFailure.recordedAt,
            auditReceipt: { id: `${updated.id}:failed`, outcome: "failed", recordedAt: lastFailure.recordedAt },
          } satisfies SkillLifecycleIntent;
          await writeIntent(kv, failed);
          return { success: false, terminal: true, error: "skill_extraction_failed", failureCode: code, reason: code,
            ...coverageFields(updated.retrievalCoverage, updated.capturedResult?.retrievalCoverage) };
        }
        const retry = {
          ...updated, status: "queued" as const,
          lastFailure,
          deliveryId: undefined, deliveryExpiresAt: undefined, leaseExpiresAt: undefined,
          nextAttemptAt: new Date(Date.now() + backoffMs(updated.attempts)).toISOString(),
          updatedAt: new Date().toISOString(),
        } satisfies SkillLifecycleIntent;
        await writeIntent(kv, retry);
        return { success: true, queued: true, retryAt: retry.nextAttemptAt, failureCode: code, reason: code,
          ...coverageFields(retry.retrievalCoverage, retry.capturedResult?.retrievalCoverage) };
      }
    });
    return result;
  });
  sdk.registerTrigger({
    type: "durable:subscriber", function_id: "mem::skill-lifecycle-dispatch",
    config: { topic: SKILL_TOPIC, queue_config: { type: "fifo", concurrency: 1, maxRetries: 3, backoffDelayMs: BASE_BACKOFF_MS } },
  });

  sdk.registerFunction("mem::skill-lifecycle-replay", async (data: { limit?: number } | undefined) => {
    if (isBackgroundRecoveryPaused()) return { success: true, paused: true, inspected: 0 };
    const requested = data?.limit ?? PAGE_SIZE;
    if (!Number.isInteger(requested) || requested < 1 || requested > PAGE_SIZE) return { success: false, error: "limit is outside supported bounds" };
    const page = await recoverMarkerPage(sdk, kv, requested);
    return { success: true, ...page };
  });
}
