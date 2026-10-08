import type { IIIClient } from "iii-sdk";
import type {
  CompressedObservation,
  SessionSummary,
  MemoryProvider,
  Session,
} from "../types.js";
import { KV } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { SummaryOutputSchema } from "../eval/schemas.js";
import { validateOutput } from "../eval/validator.js";
import { scoreSummary } from "../eval/quality.js";
import type { MetricsStore } from "../eval/metrics-store.js";
import { safeAudit } from "./audit.js";
import { recordOperation } from "./dashboard-activity.js";
import { logger } from "../logger.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { getSummaryBudgetConfig } from "../config.js";
import { createSummaryProducer } from "./summary-producer.js";
import { parseSummaryXml } from "./summary-xml.js";

export function registerSummarizeFunction(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  metricsStore?: MetricsStore,
  llmRouter?: LlmTaskRouter,
): void {
  sdk.registerFunction("mem::summarize", 
    async (data: { sessionId: string } | undefined) => {
      const startMs = Date.now();
      if (!data || typeof data.sessionId !== "string" || !data.sessionId.trim()) {
        await recordOperation(kv, "mem::summarize", "failed", { summariesCreated: 0 });
        return { success: false, error: "sessionId is required" };
      }
      const sessionId = data.sessionId.trim();

      const session = await kv.get<Session>(KV.sessions, sessionId);
      if (!session) {
        logger.warn("Session not found for summarize", {
          sessionId,
        });
        await recordOperation(kv, "mem::summarize", "failed", { summariesCreated: 0 });
        return { success: false, error: "session_not_found" };
      }

      const observations = await kv.list<CompressedObservation>(
        KV.observations(sessionId),
      );
      const compressed = observations.filter((o) => o.title);

      if (compressed.length === 0) {
        logger.info("No observations to summarize", {
          sessionId,
        });
        await recordOperation(kv, "mem::summarize", "skipped", { observationsProcessed: 0, summariesCreated: 0 }, session.agentId);
        return { success: false, error: "no_observations" };
      }

      if (provider.name === "noop") {
        await recordOperation(kv, "mem::summarize", "skipped", { observationsProcessed: 0, summariesCreated: 0 }, session.agentId);
        logger.info("Summarize skipped — no LLM provider configured", {
          sessionId,
        });
        return {
          success: false,
          error: "no_provider",
          reason:
            "No LLM provider key set; Summarize is a no-op. Set ANTHROPIC_API_KEY (or GEMINI/OPENROUTER/MINIMAX) in ~/.agentmemory/.env to enable.",
        };
      }

      try {
        const produceSummaryXml = createSummaryProducer(provider, llmRouter, getSummaryBudgetConfig(), sessionId, session.project);
        // #783: chunk-level produceSummaryXml retries internally, but
        // the final merge used to parse once and bail. Wrap the
        // produce-and-parse pair in the same 2-attempt loop so a
        // markdown-wrapped or otherwise wrapped response gets a
        // second roll-of-the-dice instead of dropping the summary.
        let summary: SessionSummary | null = null;
        let response = "";
        let mode = "single";
        let chunks = 1;
        for (let attempt = 1; attempt <= 2; attempt++) {
          const produced = await produceSummaryXml(compressed);
          response = produced.response;
          mode = produced.mode;
          chunks = produced.chunks;
          if (!response || !response.trim()) {
            logger.warn("Empty provider response on summarize", {
              sessionId,
              provider: provider.name,
              mode,
              chunks,
              observationCount: compressed.length,
              attempt,
            });
            continue;
          }
          summary = parseSummaryXml(
            response,
            sessionId,
            session.project,
            compressed.length,
          );
          if (summary) break;
          logger.warn("Failed to parse summary XML", { sessionId, attempt });
        }

        if (!response || !response.trim()) {
          const latencyMs = Date.now() - startMs;
          if (metricsStore) {
            await metricsStore.record("mem::summarize", latencyMs, false);
          }
          await recordOperation(kv, "mem::summarize", "failed", { observationsProcessed: compressed.length, summariesCreated: 0 }, session.agentId);
          return { success: false, error: "empty_provider_response" };
        }

        if (!summary) {
          const latencyMs = Date.now() - startMs;
          if (metricsStore) {
            await metricsStore.record("mem::summarize", latencyMs, false);
          }
          await recordOperation(kv, "mem::summarize", "failed", { observationsProcessed: compressed.length, summariesCreated: 0 }, session.agentId);
          return { success: false, error: "parse_failed" };
        }

        const summaryForValidation = {
          title: summary.title,
          narrative: summary.narrative,
          keyDecisions: summary.keyDecisions,
          filesModified: summary.filesModified,
          concepts: summary.concepts,
        };
        const validation = validateOutput(
          SummaryOutputSchema,
          summaryForValidation,
          "mem::summarize",
        );

        if (!validation.valid) {
          const latencyMs = Date.now() - startMs;
          if (metricsStore) {
            await metricsStore.record("mem::summarize", latencyMs, false);
          }
          logger.warn("Summary validation failed", {
            sessionId,
            errorCount: validation.result.errors.length,
          });
          await recordOperation(kv, "mem::summarize", "failed", { observationsProcessed: compressed.length, summariesCreated: 0 }, session.agentId);
          return { success: false, error: "validation_failed" };
        }

        const qualityScore = scoreSummary(summaryForValidation);

        await kv.set(KV.summaries, sessionId, summary);
        await safeAudit(kv, "compress", "mem::summarize", [sessionId], {
          title: summary.title,
          observationCount: compressed.length,
        });

        const latencyMs = Date.now() - startMs;
        if (metricsStore) {
          await metricsStore.record(
            "mem::summarize",
            latencyMs,
            true,
            qualityScore,
          );
        }

        logger.info("Session summarized", {
          sessionId,
          decisions: summary.keyDecisions.length,
          qualityScore,
          valid: validation.valid,
        });

        await recordOperation(kv, "mem::summarize", "completed", { observationsProcessed: compressed.length, summariesCreated: 1 }, session.agentId);
        return { success: true, summary, qualityScore };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const latencyMs = Date.now() - startMs;
        if (metricsStore) {
          await metricsStore.record("mem::summarize", latencyMs, false);
        }
        logger.error("Summarize failed", {
          sessionId,
          reason: "summary_failed",
        });
        await recordOperation(kv, "mem::summarize", "failed", { observationsProcessed: compressed.length }, session.agentId);
        return { success: false, error: msg };
      }
    },
  );
}
