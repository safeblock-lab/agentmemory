import { runWorkflowAttempt, type WorkflowAttemptAccounting } from "./workflow-attempt-accounting.js";
import type { IIIClient } from "iii-sdk";
import type { CompressedObservation, SessionSummary, MemoryProvider, Session, WorkflowTerminalReceipt } from "../types.js";
import { KV, fingerprintId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { SummaryOutputSchema } from "../eval/schemas.js";
import { validateOutput } from "../eval/validator.js";
import { scoreSummary } from "../eval/quality.js";
import type { MetricsStore } from "../eval/metrics-store.js";
import { recordAudit } from "./audit.js";
import { recordOperation, observeWorkflowTerminal, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { getAgentId, getSummaryBudgetConfig } from "../config.js";
import { parseSummaryXml } from "./summary-xml.js";
import { captureSummaryEvidence, getSummaryEvidenceRoot, verifySummaryEvidence,
  groundSummarySourcePaths, summarySourcePathLiterals } from "./summary-source-grounding.js";
import { formatSummaryObservation } from "../prompts/summary.js";
import { SUMMARY_SYSTEM, buildSummaryItemsPrompt } from "./summary-queue-prompts.js";
import { validManualSummaryContinuation, type ManualSummaryContinuation } from "./summary-queue.js";
import { packSummaryItems, summaryChunkInputLimit, summaryOutputTokenBudget } from "./summary-budget.js";
import { evaluatePublicationEvidence, preparePublicationEvidence } from "./publication-evidence.js";
import { publicationExecutor } from "./publication-call-options.js";

interface Selection { project: string; sessionId: string; sourceFingerprint: string }
interface DirectAttempt { workflowTerminal?: WorkflowTerminalReceipt; id: string; sessionId: string; project: string; sourceFingerprint: string;
  observationCount: number; raw?: string; intended?: boolean; overflow?: boolean;
  summary?: SessionSummary; effect?: SessionSummary; completed?: boolean; lastError?: string }
const fields = (summary: SessionSummary) => ({ title: summary.title, narrative: summary.narrative,
  keyDecisions: summary.keyDecisions, filesModified: summary.filesModified, concepts: summary.concepts });

export function registerSummarizeFunction(sdk: IIIClient, kv: StateKV, provider: MemoryProvider,
  metricsStore?: MetricsStore, llmRouter?: LlmTaskRouter, accounting?: WorkflowAttemptAccounting): void {
  sdk.registerFunction("mem::summarize", async (data: { sessionId: string; selected?: Selection; manual?: true;
    continuation?: ManualSummaryContinuation; zeroSpendRecoveryOfJobId?: string } | undefined) => {
    if (!data || typeof data.sessionId !== "string" || !data.sessionId.trim()) {
      await recordOperation(kv, "mem::summarize", "failed", {}, undefined, { executionLevel: "unit", durationMs: 0, errorCode: "invalid_input" });
      return { success: false, error: "sessionId is required" };
    }
    const sessionId = data.sessionId.trim();
    if (data.zeroSpendRecoveryOfJobId !== undefined && (!data.manual || data.selected !== undefined ||
        typeof data.zeroSpendRecoveryOfJobId !== "string" || !data.zeroSpendRecoveryOfJobId.trim() ||
        data.zeroSpendRecoveryOfJobId !== data.zeroSpendRecoveryOfJobId.trim() || data.zeroSpendRecoveryOfJobId.length > 128 ||
        Object.hasOwn(data, "continuation")))
      return { success: false, error: "invalid_manual_summary_request" };
    if (data.manual !== undefined && data.manual !== true || data.manual && data.selected ||
        data.continuation !== undefined && (!data.manual || !validManualSummaryContinuation(data.continuation) ||
          data.continuation.sessionId !== sessionId))
      return { success: false, error: "invalid_manual_summary_request" };
    if (data.manual) return sdk.trigger({ function_id: "mem::summary-enqueue",
      payload: { sessionId, action: "manual", ...(data.zeroSpendRecoveryOfJobId !== undefined && { zeroSpendRecoveryOfJobId: data.zeroSpendRecoveryOfJobId }), ...(data.continuation !== undefined && { continuation: data.continuation }),
      },
      timeoutMs: 360_000 });
    return withKeyedLock(`direct-summary:${sessionId}`, async () => {
      const started = Date.now();
      let agentId: string | undefined;
      let observationCount = 0;
      const record = (outcome: "failed" | "skipped" | "partial", errorCode: string, summariesCreated = 0) => recordOperation(kv,
        "mem::summarize", outcome, { observationsProcessed: observationCount, summariesCreated }, agentId,
        { executionLevel: "unit", durationMs: Date.now() - started, errorCode });
      try {
        const selected = data.selected;
        if (selected && (selected.sessionId !== sessionId || typeof selected.project !== "string" ||
            typeof selected.sourceFingerprint !== "string"))
          return { success: false, error: "selection_source_mismatch" };
        const pointerKey = `direct-summary:${sessionId}`;
        let attempt = await kv.get<DirectAttempt>(KV.state, pointerKey);
        const success = async (summary: SessionSummary) => {
          if (attempt?.workflowTerminal?.outcome === "completed") await observeWorkflowTerminal(kv, { ...attempt.workflowTerminal,
            workflow: "summaries", producer: "direct-summary", executionLevel: "workflow" });
          else await markWorkflowStatisticsGap(kv, "summaries");
          return { success: true, summary, qualityScore: scoreSummary(fields(summary)), ...(selected && { completed: true }) };
        };
        // Only an exact applied effect permits ACK recovery without re-reading mutable sources.
        if (attempt?.effect && (!attempt.completed || selected?.sourceFingerprint === attempt.sourceFingerprint) &&
            attempt.effect.sessionId === sessionId && attempt.effect.project === attempt.project &&
            (!selected || (selected.project === attempt.project && selected.sourceFingerprint === attempt.sourceFingerprint)) &&
            JSON.stringify(await kv.get(KV.summaries, sessionId)) === JSON.stringify(attempt.effect)) {
          if (!attempt.completed) {
            await recordAudit(kv, "compress", "mem::summarize", [sessionId], { title: attempt.effect.title,
              observationCount: attempt.observationCount }, undefined, undefined, `${attempt.id}:completed`);
            await kv.set(KV.state, pointerKey, { ...attempt, completed: true });
          }
          return success(attempt.effect);
        }
        const session = await kv.get<Session>(KV.sessions, sessionId);
        if (!session) { await record("failed", "session_not_found"); return { success: false, error: "session_not_found" }; }
        agentId = session.agentId;
        const observations = (await kv.list<CompressedObservation>(KV.observations(sessionId)))
          .filter(value => value.title).sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.id.localeCompare(b.id));
        observationCount = observations.length;
        if (!observations.length) { await record("skipped", "no_observations"); return { success: false, error: "no_observations" }; }
        if (provider.name === "noop") {
          observationCount = 0;
          await record("skipped", "no_provider");
          return { success: false, error: "no_provider",
          reason: "No LLM provider key set; Summarize is a no-op. Set ANTHROPIC_API_KEY (or GEMINI/OPENROUTER/MINIMAX) in ~/.agentmemory/.env to enable." };
        }
        const items = observations.map((value, index) => ({ text: formatSummaryObservation(value, index + 1),
          obsRangeStart: index + 1, obsRangeEnd: index + 1, sourcePaths: summarySourcePathLiterals([value]),
          modifiedPaths: [] }));
        const fingerprintItems = items.map(({ sourcePaths: _paths, modifiedPaths: _modified, ...value }) => value);
        const fingerprint = fingerprintId("sq", JSON.stringify([sessionId, session.project, fingerprintItems]));
        if (selected && (selected.project !== session.project || selected.sourceFingerprint !== fingerprint))
          return { success: false, error: "selection_source_mismatch" };
        if (attempt?.completed && attempt.sourceFingerprint === fingerprint && attempt.effect &&
            JSON.stringify(await kv.get(KV.summaries, sessionId)) === JSON.stringify(attempt.effect)) return success(attempt.effect);
        if (attempt && !attempt.completed && (attempt.sourceFingerprint !== fingerprint || attempt.project !== session.project)) {
          if (selected || (attempt.intended && attempt.raw === undefined && !attempt.summary && !attempt.overflow))
            return { success: false, error: "summary_source_changed" };
          await kv.set(KV.state, `${attempt.id}:direct-attempt`, attempt);
          attempt = null;
        }
        if (!attempt || attempt.completed) {
          if (attempt) await kv.set(KV.state, `${attempt.id}:direct-attempt`, attempt);
          const id = fingerprintId("direct-summary", JSON.stringify([sessionId, session.project, fingerprint]));
          attempt = await kv.get<DirectAttempt>(KV.state, `${id}:direct-attempt`) ?? {
            id, sessionId, project: session.project, sourceFingerprint: fingerprint, observationCount: observations.length };
          await kv.set(KV.state, pointerKey, attempt);
        }
        const root = await getSummaryEvidenceRoot(kv, attempt) ?? await captureSummaryEvidence(kv, attempt, observations);
        const config = getSummaryBudgetConfig();
        const groups = packSummaryItems(items, SUMMARY_SYSTEM, buildSummaryItemsPrompt,
          summaryChunkInputLimit(config), config.chunkSize);
        if (root.mode === "hierarchical" || groups.length !== 1) {
          const queued = await sdk.trigger({ function_id: "mem::summary-enqueue",
            payload: { sessionId, ...(selected && { selected }) } }) as Record<string, unknown>;
          if (queued.completed === true) {
            const summary = await kv.get<SessionSummary>(KV.summaries, sessionId);
            if (summary?.project === session.project) {
              await kv.set(KV.state, pointerKey, { ...attempt, effect: summary, completed: true });
              return success(summary);
            }
          }
          return { ...queued, completed: false };
        }
        if (!attempt.summary) {
          if (attempt.overflow) throw new Error("summary_response_limit_exceeded");
          if (attempt.intended && attempt.raw === undefined) throw new Error("summary_dispatch_outcome_unknown");
          if (attempt.raw === undefined) {
            const prompt = buildSummaryItemsPrompt(groups[0]);
            const outputTokens = summaryOutputTokenBudget(config, SUMMARY_SYSTEM, prompt);
            attempt = { ...attempt, intended: true };
            await kv.set(KV.state, pointerKey, attempt);
            const call = async (chosen: MemoryProvider) => {
              const raw = await chosen.summarize(SUMMARY_SYSTEM, prompt,
                { task: "summary", outputTokens });
              attempt = { ...attempt!, ...(Buffer.byteLength(raw) > 65536 ? { overflow: true } : { raw }) };
              await kv.set(KV.state, pointerKey, attempt);
              return raw;
            };
            const execute = () => llmRouter ? llmRouter.run("summary", call, () => true) : call(provider);
            await runWorkflowAttempt(accounting, kv, { workflow: "summary", functionId: "mem::summarize",
              project: session.project, sourceId: sessionId, sourceFingerprint: fingerprint, jobId: attempt.id,
              unitId: attempt.id, phase: "generation", continuation: "direct" }, execute);
          }
          if (attempt.overflow) throw new Error("summary_response_limit_exceeded");
          if (!attempt.raw?.trim()) throw new Error("empty_provider_response");
          const parsed = parseSummaryXml(attempt.raw, sessionId, session.project, observations.length);
          if (!parsed) throw new Error("parse_failed");
          const summary = groundSummarySourcePaths(parsed, summarySourcePathLiterals(observations));
          if (!validateOutput(SummaryOutputSchema, fields(summary), "mem::summarize").valid)
            throw new Error("validation_failed");
          attempt = { ...attempt, summary };
          await kv.set(KV.state, pointerKey, attempt);
        }
        const summary = attempt.summary!;
        const sources = await verifySummaryEvidence(kv, attempt, root);
        const input = { workflow: "summary" as const, operationId: attempt.id, project: session.project,
          candidate: JSON.stringify(fields(summary)), sources, evidenceComplete: true };
        let verdict = await evaluatePublicationEvidence(kv, input);
        if (verdict.status === "unavailable") {
          const request = preparePublicationEvidence(input);
          if ("status" in request) throw new Error(`summary_publication_${request.status}`);
          verdict = await evaluatePublicationEvidence(kv, input, publicationExecutor(provider, llmRouter, "summary",
            { task: "summary", outputTokens: 1024 }, (prepared, execute) =>
              runWorkflowAttempt(accounting, kv, { workflow: "summary", functionId: "mem::summarize",
                project: session.project, sourceId: sessionId, sourceFingerprint: fingerprint, jobId: attempt!.id,
                unitId: prepared.requestId, phase: "publication", continuation: "direct" }, execute)));
        }
        if (verdict.status !== "supported") throw new Error(`summary_publication_${verdict.status}`);
        await verifySummaryEvidence(kv, attempt, root);
        attempt = { ...attempt, effect: summary };
        await kv.set(KV.state, pointerKey, attempt);
        await withKeyedLock(`summary-canonical:${sessionId}`, async () => {
          const currentSession = await kv.get<Session>(KV.sessions, sessionId);
          if (!currentSession || currentSession.id !== session.id || currentSession.project !== session.project ||
              currentSession.startedAt !== session.startedAt)
            throw new Error("summary_source_changed");
          if (JSON.stringify(await kv.get(KV.summaries, sessionId)) !== JSON.stringify(summary))
            await kv.set(KV.summaries, sessionId, summary);
        });
        await recordAudit(kv, "compress", "mem::summarize", [sessionId], { title: summary.title,
          observationCount: observations.length }, undefined, undefined, `${attempt.id}:completed`);
        attempt = { ...attempt, completed: true, workflowTerminal: attempt.workflowTerminal ?? {
          executionId: attempt.id, agentId: session.agentId ?? getAgentId(), occurredAt: new Date().toISOString(), outcome: "completed" } };
        await kv.set(KV.state, pointerKey, attempt);
        await metricsStore?.record("mem::summarize", Date.now() - started, true, scoreSummary(fields(summary)));
        await recordOperation(kv, "mem::summarize", "completed", { observationsProcessed: observations.length, summariesCreated: 1 }, session.agentId, { executionLevel: "unit", durationMs: Date.now() - started });
        return success(summary);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const retained = await kv.get<DirectAttempt>(KV.state, `direct-summary:${sessionId}`);
        if (!retained || retained.lastError !== message) {
          const applied = retained?.effect && JSON.stringify(await kv.get(KV.summaries, sessionId)) === JSON.stringify(retained.effect);
          await record(applied ? "partial" : "failed", "summary_failed", applied ? 1 : 0);
          if (retained) {
            const outcome = applied ? "partial" : "failed";
            const workflowTerminal = retained.workflowTerminal ?? { executionId: retained.id,
              agentId: agentId ?? getAgentId(), occurredAt: new Date().toISOString(), outcome };
            await kv.set(KV.state, `direct-summary:${sessionId}`, { ...retained, lastError: message, workflowTerminal });
            if (workflowTerminal.outcome !== outcome) await markWorkflowStatisticsGap(kv, "summaries");
            else await observeWorkflowTerminal(kv, { ...workflowTerminal, workflow: "summaries", producer: "direct-summary", executionLevel: "workflow" });
          } else await markWorkflowStatisticsGap(kv, "summaries");
          await metricsStore?.record("mem::summarize", Date.now() - started, false);
        }
        return { success: false, error: error instanceof Error ? error.message : String(error),
          ...(data.selected && { completed: false }) };
      }
    });
  });
}
