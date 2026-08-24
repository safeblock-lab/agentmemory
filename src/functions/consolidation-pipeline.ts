import type { ISdk } from "iii-sdk";
import type {
  SemanticMemory,
  ProceduralMemory,
  SessionSummary,
  Memory,
  MemoryProvider,
} from "../types.js";
import { KV, fingerprintId, generateId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import {
  SEMANTIC_MERGE_SYSTEM,
  buildSemanticMergePrompt,
  PROCEDURAL_EXTRACTION_SYSTEM,
  buildProceduralExtractionPrompt,
} from "../prompts/consolidation.js";
import { recordAudit } from "./audit.js";
import {
  getConsolidationDecayDays,
  getConsolidationMinNewSummaries,
  isConsolidationEnabled,
} from "../config.js";
import { logger } from "../logger.js";
import { assessConsolidationComplexity } from "./consolidation-complexity.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";

const SEMANTIC_CHECKPOINT_KEY = "semantic-consolidation";
const SEMANTIC_ANCHOR_SUMMARIES = 5;
const SEMANTIC_NEW_SUMMARIES_PER_RUN = 15;

interface SemanticCheckpoint {
  processedThrough: string;
  processedSessionIdsAtThrough: string[];
}

interface SemanticInput {
  summaries: SessionSummary[];
  checkpoint?: SemanticCheckpoint;
}

function isAfterCheckpoint(summary: SessionSummary, checkpoint: SemanticCheckpoint): boolean {
  if (summary.createdAt > checkpoint.processedThrough) return true;
  return summary.createdAt === checkpoint.processedThrough &&
    !checkpoint.processedSessionIdsAtThrough.includes(summary.sessionId);
}

function checkpointFor(summary: SessionSummary): SemanticCheckpoint {
  return {
    processedThrough: summary.createdAt,
    processedSessionIdsAtThrough: [summary.sessionId],
  };
}

function selectSemanticInput(
  summaries: SessionSummary[],
  checkpoint: SemanticCheckpoint | null,
  minNewSummaries: number,
): SemanticInput {
  const newestFirst = [...summaries].sort((left, right) =>
    right.createdAt.localeCompare(left.createdAt),
  );
  if (!checkpoint) {
    const selected = newestFirst.slice(0, 20);
    const newest = newestFirst[0];
    return { summaries: selected, checkpoint: newest ? checkpointFor(newest) : undefined };
  }

  const newSummaries = newestFirst
    .filter((summary) => isAfterCheckpoint(summary, checkpoint))
    .sort((left, right) => left.createdAt.localeCompare(right.createdAt));
  if (newSummaries.length < minNewSummaries) return { summaries: [] };

  const selectedNew = newSummaries.slice(0, SEMANTIC_NEW_SUMMARIES_PER_RUN);
  const anchors = newestFirst
    .filter((summary) => !isAfterCheckpoint(summary, checkpoint))
    .slice(0, SEMANTIC_ANCHOR_SUMMARIES);
  return {
    summaries: [...anchors, ...selectedNew],
    checkpoint: checkpointFor(selectedNew[selectedNew.length - 1]),
  };
}

function applyDecay(
  items: Array<{
    strength: number;
    lastAccessedAt?: string;
    updatedAt: string;
  }>,
  decayDays: number,
): void {
  if (decayDays <= 0 || !Number.isFinite(decayDays)) return;
  const now = Date.now();
  for (const item of items) {
    const lastAccess = item.lastAccessedAt || item.updatedAt;
    const daysSince =
      (now - new Date(lastAccess).getTime()) / (1000 * 60 * 60 * 24);
    if (daysSince > decayDays) {
      const decayPeriods = Math.floor(daysSince / decayDays);
      item.strength = Math.max(
        0.1,
        item.strength * Math.pow(0.9, decayPeriods),
      );
    }
  }
}

export function registerConsolidationPipelineFunction(
  sdk: ISdk,
  kv: StateKV,
  provider: MemoryProvider,
  llmRouter?: LlmTaskRouter,
  auxiliaryMaxInputChars?: number,
  batchQueue?: FireworksBatchQueue,
): void {
  sdk.registerFunction("mem::consolidate-pipeline", 
    async (data?: {
      tier?: string;
      force?: boolean;
      project?: string;
      batchResponse?: string;
      batchSourceFingerprint?: string;
      deferred?: boolean;
    }) => {
      if (!data?.force && !isConsolidationEnabled()) {
        return { success: false, skipped: true, reason: "Consolidation disabled: set CONSOLIDATION_ENABLED=true or configure an LLM provider (ANTHROPIC_API_KEY / OPENAI_API_KEY / OPENROUTER_API_KEY / GEMINI_API_KEY / GOOGLE_API_KEY / MINIMAX_API_KEY / OPENAI_BASE_URL / AGENTMEMORY_PROVIDER=agent-sdk)" };
      }
      const tier = data?.tier || "all";
      const decayDays = getConsolidationDecayDays();
      const results: Record<string, unknown> = {};

      if (tier === "all" || tier === "semantic") {
        const summaries = await kv.list<SessionSummary>(KV.summaries);
        const existingSemantic = await kv.list<SemanticMemory>(KV.semantic);

        if (summaries.length >= 5) {
          const checkpoint = await kv.get<SemanticCheckpoint>(
            KV.state,
            SEMANTIC_CHECKPOINT_KEY,
          );
          const semanticInput = selectSemanticInput(
            summaries,
            checkpoint,
            getConsolidationMinNewSummaries(),
          );
          const recentSummaries = semanticInput.summaries;
          if (recentSummaries.length === 0) {
            results.semantic = {
              skipped: true,
              reason: `fewer than ${getConsolidationMinNewSummaries()} new summaries`,
            };
          } else {

          const prompt = buildSemanticMergePrompt(
            recentSummaries.map((s) => ({
              title: s.title,
              narrative: s.narrative,
              concepts: s.concepts,
            })),
          );
          const sourceFingerprint = fingerprintId("fwbconsem", JSON.stringify(
            recentSummaries.map((summary) => [
              summary.sessionId,
              summary.title,
              summary.narrative,
              summary.concepts,
              summary.createdAt,
            ]),
          ));

          if (data?.batchResponse && data.batchSourceFingerprint !== sourceFingerprint) {
            return { success: true, stale: true };
          }

          try {
            const complexity = assessConsolidationComplexity({
              prompt,
              maxAuxiliaryInputChars: auxiliaryMaxInputChars,
            });
            let queued = false;
            if (!data?.batchResponse && data?.deferred && batchQueue) {
              const enqueueResult = await batchQueue.enqueue({
                correlationId: generateId("fwbcon-sem"),
                task: "consolidation",
                systemPrompt: SEMANTIC_MERGE_SYSTEM,
                userPrompt: prompt,
                metadata: { tier: "semantic", sourceFingerprint },
              });
              if (enqueueResult.queued) {
                queued = true;
                results.semantic = { queued: true, workItemId: enqueueResult.workItemId, totalSummaries: summaries.length };
              }
            }
            if (!queued) {
            const response = data?.batchResponse ?? (llmRouter
              ? await llmRouter.run(
                complexity.complex ? "conflict_resolution" : "consolidation",
                (selectedProvider) => selectedProvider.summarize(
                  SEMANTIC_MERGE_SYSTEM,
                  prompt,
                ),
                (candidate) => /<fact\s+confidence="[^"]+">[^<]+<\/fact>/.test(candidate),
              )
              : await provider.summarize(SEMANTIC_MERGE_SYSTEM, prompt));

            const factRegex = /<fact\s+confidence="([^"]+)">([^<]+)<\/fact>/g;
            let match;
            let newFacts = 0;
            const now = new Date().toISOString();

            while ((match = factRegex.exec(response)) !== null) {
              const parsedConf = parseFloat(match[1]);
              const confidence = Number.isNaN(parsedConf) ? 0.5 : parsedConf;
              const fact = match[2].trim();

              const existing = existingSemantic.find(
                (s) => s.fact.toLowerCase() === fact.toLowerCase(),
              );
              if (existing) {
                existing.accessCount++;
                existing.lastAccessedAt = now;
                existing.updatedAt = now;
                existing.confidence = Math.max(existing.confidence, confidence);
                await kv.set(KV.semantic, existing.id, existing);
              } else {
                const sem: SemanticMemory = {
                  id: generateId("sem"),
                  fact,
                  confidence,
                  sourceSessionIds: recentSummaries.map((s) => s.sessionId),
                  sourceMemoryIds: [],
                  accessCount: 1,
                  lastAccessedAt: now,
                  strength: confidence,
                  createdAt: now,
                  updatedAt: now,
                };
                await kv.set(KV.semantic, sem.id, sem);
                newFacts++;
              }
            }
            results.semantic = { newFacts, totalSummaries: summaries.length };
            if (semanticInput.checkpoint) {
              await kv.set(KV.state, SEMANTIC_CHECKPOINT_KEY, semanticInput.checkpoint);
            }
            }
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error("Semantic consolidation failed", { error: msg });
            results.semantic = { error: msg };
          }
          }
        } else {
          results.semantic = {
            skipped: true,
            reason: "fewer than 5 summaries",
          };
        }
      }

      if (tier === "all" || tier === "reflect") {
        try {
          const reflectResult = await sdk.trigger({ function_id: "mem::reflect", payload: {
            maxClusters: 10,
            project: data?.project,
            deferred: data?.deferred,
          } });
          results.reflect = reflectResult;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Reflect tier failed", { error: msg });
          results.reflect = { error: msg };
        }
      }

      if (tier === "all" || tier === "procedural") {
        const memories = await kv.list<Memory>(KV.memories);
        const patterns = memories
          .filter((m) => m.isLatest && m.type === "pattern")
          .map((m) => ({
            content: m.content,
            frequency: m.sessionIds.length || 1,
          }))
          .filter((p) => p.frequency >= 2);

        if (patterns.length >= 2) {
          const prompt = buildProceduralExtractionPrompt(patterns);
          const sourceFingerprint = fingerprintId("fwbconproc", JSON.stringify(patterns));

          if (data?.batchResponse && data.batchSourceFingerprint !== sourceFingerprint) {
            return { success: true, stale: true };
          }

          try {
            const complexity = assessConsolidationComplexity({
              prompt,
              maxAuxiliaryInputChars: auxiliaryMaxInputChars,
            });
            let queued = false;
            if (!data?.batchResponse && data?.deferred && batchQueue) {
              const enqueueResult = await batchQueue.enqueue({
                correlationId: generateId("fwbcon-proc"),
                task: "consolidation",
                systemPrompt: PROCEDURAL_EXTRACTION_SYSTEM,
                userPrompt: prompt,
                metadata: { tier: "procedural", sourceFingerprint },
              });
              if (enqueueResult.queued) {
                queued = true;
                results.procedural = { queued: true, workItemId: enqueueResult.workItemId, patternsAnalyzed: patterns.length };
              }
            }
            if (!queued) {
            const response = data?.batchResponse ?? (llmRouter
              ? await llmRouter.run(
                complexity.complex ? "conflict_resolution" : "consolidation",
                (selectedProvider) => selectedProvider.summarize(
                  PROCEDURAL_EXTRACTION_SYSTEM,
                  prompt,
                ),
                (candidate) => /<procedure\s+name="[^"]+"\s+trigger="[^"]+">[\s\S]*?<\/procedure>/.test(candidate),
              )
              : await provider.summarize(PROCEDURAL_EXTRACTION_SYSTEM, prompt));

            const procRegex =
              /<procedure\s+name="([^"]+)"\s+trigger="([^"]+)">([\s\S]*?)<\/procedure>/g;
            let match;
            let newProcs = 0;
            const now = new Date().toISOString();
            const existingProcs = await kv.list<ProceduralMemory>(
              KV.procedural,
            );

            while ((match = procRegex.exec(response)) !== null) {
              const name = match[1];
              const trigger = match[2];
              const stepsBlock = match[3];
              const steps: string[] = [];

              const stepRegex = /<step>([^<]+)<\/step>/g;
              let stepMatch;
              while ((stepMatch = stepRegex.exec(stepsBlock)) !== null) {
                steps.push(stepMatch[1].trim());
              }

              const existing = existingProcs.find(
                (p) => p.name.toLowerCase() === name.toLowerCase(),
              );
              if (existing) {
                existing.frequency++;
                existing.updatedAt = now;
                existing.strength = Math.min(1, existing.strength + 0.1);
                await kv.set(KV.procedural, existing.id, existing);
              } else {
                const proc: ProceduralMemory = {
                  id: generateId("proc"),
                  name,
                  steps,
                  triggerCondition: trigger,
                  frequency: 1,
                  sourceSessionIds: [],
                  strength: 0.5,
                  createdAt: now,
                  updatedAt: now,
                };
                await kv.set(KV.procedural, proc.id, proc);
                newProcs++;
              }
            }
            results.procedural = {
              newProcedures: newProcs,
              patternsAnalyzed: patterns.length,
            };
            }
          } catch (err) {
            const msg = err instanceof Error ? err.message : String(err);
            logger.error("Procedural extraction failed", { error: msg });
            results.procedural = { error: msg };
          }
        } else {
          results.procedural = {
            skipped: true,
            reason: "fewer than 2 recurring patterns",
          };
        }
      }

      if (tier === "all" || tier === "decay") {
        const semantic = await kv.list<SemanticMemory>(KV.semantic);
        applyDecay(semantic, decayDays);
        for (const s of semantic) {
          await kv.set(KV.semantic, s.id, s);
        }

        const procedural = await kv.list<ProceduralMemory>(KV.procedural);
        applyDecay(procedural, decayDays);
        for (const p of procedural) {
          await kv.set(KV.procedural, p.id, p);
        }

        results.decay = {
          semantic: semantic.length,
          procedural: procedural.length,
        };
      }

      if (process.env["OBSIDIAN_AUTO_EXPORT"] === "true") {
        try {
          await sdk.trigger({ function_id: "mem::obsidian-export", payload: {} });
          results.obsidianExport = { success: true };
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Obsidian auto-export failed", { error: msg });
          results.obsidianExport = { success: false, error: msg };
        }
      }

      await recordAudit(kv, "consolidate", "mem::consolidate-pipeline", [], {
        tier,
        results,
      });

      logger.info("Consolidation pipeline complete", { tier, results });
      return { success: true, results };
    },
  );
}
