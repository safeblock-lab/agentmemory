import type { ISdk } from "iii-sdk";
import type {
  SemanticMemory,
  ProceduralMemory,
  SessionSummary,
  Memory,
  MemoryProvider,
  FireworksBatchWorkItem,
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
import { applyBatchEffect, batchEffectKey, runBatchCallback } from "../state/batch-effects.js";
import { withKeyedLock } from "../state/keyed-mutex.js";

const SEMANTIC_CHECKPOINT_KEY = "semantic-consolidation";
const SEMANTIC_ANCHOR_SUMMARIES = 5;
const SEMANTIC_NEW_SUMMARIES_PER_RUN = 15;

function boundedPartitions<T>(items: T[], prompt: (items: T[]) => string): T[][] {
  const groups: T[][] = [];
  let group: T[] = [];
  for (const item of items) {
    if (prompt([item]).length > 10000) throw new Error("Consolidation source exceeds bounded batch input; source retained locally");
    if (group.length && prompt([...group, item]).length > 10000) { groups.push(group); group = []; }
    group.push(item);
  }
  if (group.length) groups.push(group);
  return groups;
}

function semanticFingerprint(summaries: SessionSummary[]): string {
  return fingerprintId("fwbconsem", JSON.stringify(summaries.map((s) => [s.sessionId, s.title, s.narrative, s.concepts, s.createdAt])));
}

interface SemanticCheckpoint {
  processedThrough: string;
  processedSessionIdsAtThrough: string[];
}

interface SemanticInput {
  summaries: SessionSummary[];
  checkpoint?: SemanticCheckpoint;
}

// Call only while holding the cohort lock, after any current effect was admitted.
async function reconcileSemanticCohort(kv: StateKV, key: string, currentEffectKey?: string): Promise<void> {
  const cohort = await kv.get<{ workItemIds: string[]; checkpoint?: SemanticCheckpoint }>(KV.state, key);
  if (!cohort?.checkpoint || !cohort.workItemIds.length
    || (currentEffectKey && !cohort.workItemIds.some((id) => batchEffectKey(id) === currentEffectKey))) return;
  const members = await Promise.all(cohort.workItemIds.map((id) => kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)));
  if (!members.every((work, index) => work?.id === cohort.workItemIds[index]
    && (work.state === "completed" || batchEffectKey(work.id) === currentEffectKey))) return;
  const current = await kv.get<SemanticCheckpoint>(KV.state, SEMANTIC_CHECKPOINT_KEY);
  if (current && current.processedThrough > cohort.checkpoint.processedThrough) return;
  const checkpoint = current?.processedThrough === cohort.checkpoint.processedThrough ? {
    ...cohort.checkpoint,
    processedSessionIdsAtThrough: [...new Set([...current.processedSessionIdsAtThrough, ...cohort.checkpoint.processedSessionIdsAtThrough])],
  } : cohort.checkpoint;
  await kv.set(KV.state, SEMANTIC_CHECKPOINT_KEY, checkpoint);
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
  now = Date.now(),
): void {
  if (decayDays <= 0 || !Number.isFinite(decayDays)) return;
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
      batchEffectKey?: string;
      replacementOf?: string;
      batchSourceIds?: string[];
      batchCohort?: string;
    }) => runBatchCallback(kv, "consolidation", data?.batchEffectKey, async (resuming, admit, receipt) => {
      if (!data?.force && !isConsolidationEnabled()) {
        return { success: false, skipped: true, reason: "Consolidation disabled: set CONSOLIDATION_ENABLED=true or configure an LLM provider (ANTHROPIC_API_KEY / OPENAI_API_KEY / OPENROUTER_API_KEY / GEMINI_API_KEY / GOOGLE_API_KEY / MINIMAX_API_KEY / OPENAI_BASE_URL / AGENTMEMORY_PROVIDER=agent-sdk)" };
      }
      const tier = data?.tier || "all";
      const decayDays = getConsolidationDecayDays();
      const results: Record<string, unknown> = {};

      if (tier === "all" || tier === "semantic") {
        const summaries = await kv.list<SessionSummary>(KV.summaries);
        const existingSemantic = await kv.list<SemanticMemory>(KV.semantic);

        if (summaries.length >= 5 || data?.batchSourceIds?.length || (resuming && data?.batchResponse)) {
          const checkpoint = await kv.get<SemanticCheckpoint>(
            KV.state,
            SEMANTIC_CHECKPOINT_KEY,
          );
          let semanticInput = receipt?.semanticSourceIds ? {
            summaries: summaries.filter((summary) => receipt.semanticSourceIds!.includes(summary.sessionId)),
            checkpoint: receipt.semanticCheckpoint,
          } : selectSemanticInput(
            summaries,
            resuming ? null : checkpoint,
            getConsolidationMinNewSummaries(),
          );
          if (!resuming && (data?.deferred || data?.batchSourceIds)) {
            const candidates = data.batchSourceIds
              ? data.batchSourceIds.map((id) => summaries.find((summary) => summary.sessionId === id)).filter((summary): summary is SessionSummary => Boolean(summary))
              : summaries.filter((summary) => data?.replacementOf || !checkpoint || isAfterCheckpoint(summary, checkpoint)).sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.sessionId.localeCompare(b.sessionId));
            if (data.batchSourceIds && candidates.length !== data.batchSourceIds.length) return { success: true, stale: true };
            const selected = candidates;
            const last = selected[selected.length - 1];
            semanticInput = { summaries: selected, checkpoint: last ? {
              processedThrough: last.createdAt,
              processedSessionIdsAtThrough: [...new Set([
                ...(checkpoint?.processedThrough === last.createdAt ? checkpoint.processedSessionIdsAtThrough : []),
                ...selected.filter((summary) => summary.createdAt === last.createdAt).map((summary) => summary.sessionId),
              ])],
            } : undefined };
          }
          const recentSummaries = semanticInput.summaries;
          if (recentSummaries.length === 0 && !resuming) {
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

            if (!resuming && data?.batchResponse && data.batchSourceFingerprint !== sourceFingerprint) {
              return { success: true, stale: true };
            }

            try {
              const complexity = assessConsolidationComplexity({
                prompt,
                maxAuxiliaryInputChars: auxiliaryMaxInputChars,
              });
              let queued = false;
              if (!data?.batchResponse && data?.deferred && batchQueue) {
                const cohort = data.batchCohort ?? fingerprintId("fwbcohort", `${data.replacementOf ?? ""}:${sourceFingerprint}`);
                const failure = await withKeyedLock(`semantic-cohort:${cohort}`, async () => {
                  if (data.batchCohort && (!data.replacementOf || !/^fwbcohort_[a-f0-9]{16}$/.test(data.batchCohort)
                    || !await kv.get(KV.state, data.batchCohort))) {
                    return { success: false, error: "Replacement cohort is missing or invalid" };
                  }
                  const workItemIds: string[] = [];
                  for (const group of boundedPartitions(recentSummaries, buildSemanticMergePrompt)) {
                    const enqueueResult = await batchQueue.enqueue({
                      replacementOf: data.replacementOf,
                      correlationId: data.batchEffectKey ? fingerprintId("fwbcon-sem", data.batchEffectKey) : generateId("fwbcon-sem"),
                      task: "consolidation",
                      systemPrompt: SEMANTIC_MERGE_SYSTEM,
                      userPrompt: buildSemanticMergePrompt(group),
                      metadata: { tier: "semantic", sourceFingerprint: semanticFingerprint(group), sourceIds: JSON.stringify(group.map((summary) => summary.sessionId)), cohort, ...(data.batchEffectKey ? { batchEffectKey: data.batchEffectKey } : {}) },
                    });
                    if (!enqueueResult.queued || !enqueueResult.workItemId) {
                      return { success: false, error: enqueueResult.reason ?? "Consolidation queue rejected bounded input" };
                    }
                    queued = true;
                    workItemIds.push(enqueueResult.workItemId);
                    results.semantic = { queued: true, workItemId: workItemIds[0], workItemIds, totalSummaries: summaries.length };
                  }
                  if (!data.batchCohort) {
                    await kv.set(KV.state, cohort, { workItemIds, checkpoint: semanticInput.checkpoint });
                  } else {
                    const original = await kv.get<{ workItemIds: string[]; checkpoint?: SemanticCheckpoint }>(KV.state, cohort);
                    if (!original || !Array.isArray(original.workItemIds)) throw new Error("Replacement cohort is invalid");
                    if (original.workItemIds.includes(data.replacementOf!)) {
                      const members = original.workItemIds.flatMap((id) => id === data.replacementOf ? workItemIds : [id]);
                      await kv.set(KV.state, cohort, { ...original, workItemIds: [...new Set(members)] });
                    } else if (!workItemIds.every((id) => original.workItemIds.includes(id))) {
                      throw new Error("Old work is not a member of the replacement cohort");
                    }
                  }
                  await reconcileSemanticCohort(kv, cohort);
                });
                if (failure) return failure;
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
                await admit({ semanticSourceIds: recentSummaries.map((summary) => summary.sessionId), semanticCheckpoint: semanticInput.checkpoint });
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
                  if (data?.batchEffectKey) {
                    const id = existing?.id ?? fingerprintId("sem", `${data.batchEffectKey}:${fact.toLowerCase()}`);
                    await applyBatchEffect<SemanticMemory>(kv, KV.semantic, id, data.batchEffectKey, (current) => current ? {
                      ...current, accessCount: current.accessCount + 1, lastAccessedAt: now, updatedAt: now,
                      confidence: Math.max(current.confidence, confidence),
                    } : {
                      id, fact, confidence, sourceSessionIds: receipt?.semanticSourceIds ?? recentSummaries.map((s) => s.sessionId), sourceMemoryIds: [],
                      accessCount: 1, lastAccessedAt: now, strength: confidence, createdAt: now, updatedAt: now,
                    });
                    continue;
                  }
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
                if (data?.batchCohort) {
                  await withKeyedLock(`semantic-cohort:${data.batchCohort}`, () => reconcileSemanticCohort(kv, data.batchCohort!, data.batchEffectKey));
                } else if (semanticInput.checkpoint && (!checkpoint || semanticInput.checkpoint.processedThrough >= checkpoint.processedThrough)) {
                  await kv.set(KV.state, SEMANTIC_CHECKPOINT_KEY, semanticInput.checkpoint);
                }
              }
            } catch (err) {
              if (data?.batchEffectKey) throw err;
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
          const reflectResult = await sdk.trigger({
            function_id: "mem::reflect", payload: {
              maxClusters: 10,
              project: data?.project,
              deferred: data?.deferred,
              ...(data?.batchEffectKey ? { batchEffectKey: batchEffectKey(`${data.batchEffectKey}:reflect`) } : {}),
            }
          });
          if (reflectResult && typeof reflectResult === "object" && "success" in reflectResult && reflectResult.success === false) {
            throw new Error("Reflection tier reported failure");
          }
          results.reflect = reflectResult;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Reflect tier failed", { error: msg });
          throw err;
        }
      }

      if (tier === "all" || tier === "procedural") {
        const memories = await kv.list<Memory>(KV.memories);
        const patternSources = memories
          .filter((m) => m.isLatest && m.type === "pattern")
          .filter((m) => !data?.batchSourceIds || data.batchSourceIds.includes(m.id))
          .map((m) => ({
            id: m.id,
            content: m.content,
            frequency: m.sessionIds.length || 1,
          }))
          .filter((p) => p.frequency >= 2);
        if (data?.batchSourceIds && patternSources.length !== data.batchSourceIds.length) return { success: true, stale: true };
        const patterns = patternSources.map(({ content, frequency }) => ({ content, frequency }));

        if (patterns.length >= 2 || data?.batchSourceIds?.length || (resuming && data?.batchResponse)) {
          const prompt = buildProceduralExtractionPrompt(patterns);
          const sourceFingerprint = fingerprintId("fwbconproc", JSON.stringify(patterns));

          if (!resuming && data?.batchResponse && data.batchSourceFingerprint !== sourceFingerprint) {
            return { success: true, stale: true };
          }

          try {
            const complexity = assessConsolidationComplexity({
              prompt,
              maxAuxiliaryInputChars: auxiliaryMaxInputChars,
            });
            let queued = false;
            if (!data?.batchResponse && data?.deferred && batchQueue) {
              const workItemIds: string[] = [];
              for (const group of boundedPartitions(patternSources, buildProceduralExtractionPrompt)) {
              const groupPatterns = group.map(({ content, frequency }) => ({ content, frequency }));
              const enqueueResult = await batchQueue.enqueue({
                replacementOf: data.replacementOf,
                correlationId: data.batchEffectKey ? fingerprintId("fwbcon-proc", data.batchEffectKey) : generateId("fwbcon-proc"),
                task: "consolidation",
                systemPrompt: PROCEDURAL_EXTRACTION_SYSTEM,
                userPrompt: buildProceduralExtractionPrompt(groupPatterns),
                metadata: { tier: "procedural", sourceFingerprint: fingerprintId("fwbconproc", JSON.stringify(groupPatterns)), sourceIds: JSON.stringify(group.map((item) => item.id)), ...(data.batchEffectKey ? { batchEffectKey: data.batchEffectKey } : {}) },
              });
              if (enqueueResult.queued) {
                queued = true;
                if (enqueueResult.workItemId) workItemIds.push(enqueueResult.workItemId);
                results.procedural = { queued: true, workItemId: workItemIds[0], workItemIds, patternsAnalyzed: patterns.length };
              } else return { success: false, error: enqueueResult.reason ?? "Consolidation queue rejected bounded input" };
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
              await admit();
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
                if (data?.batchEffectKey) {
                  const id = existing?.id ?? fingerprintId("proc", `${data.batchEffectKey}:${name.toLowerCase()}`);
                  await applyBatchEffect<ProceduralMemory>(kv, KV.procedural, id, data.batchEffectKey, (current) => current ? {
                    ...current, frequency: current.frequency + 1, updatedAt: now, strength: Math.min(1, current.strength + 0.1),
                  } : { id, name, steps, triggerCondition: trigger, frequency: 1, sourceSessionIds: [], strength: 0.5, createdAt: now, updatedAt: now });
                  continue;
                }
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
            if (data?.batchEffectKey) throw err;
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
        const effectTimestamp = receipt?.effectTimestamp ?? new Date().toISOString();
        if (data?.batchEffectKey) await admit({ effectTimestamp });
        const decayKey = data?.batchEffectKey ? batchEffectKey(`${data.batchEffectKey}:decay`) : undefined;
        const semantic = await kv.list<SemanticMemory>(KV.semantic);
        for (const s of semantic) {
          if (decayKey) await applyBatchEffect<SemanticMemory>(kv, KV.semantic, s.id, decayKey, (current) => {
            if (!current) throw new Error("Semantic decay source disappeared during recovery");
            applyDecay([current], decayDays, Date.parse(effectTimestamp));
            return current;
          });
          else { applyDecay([s], decayDays); await kv.set(KV.semantic, s.id, s); }
        }

        const procedural = await kv.list<ProceduralMemory>(KV.procedural);
        for (const p of procedural) {
          if (decayKey) await applyBatchEffect<ProceduralMemory>(kv, KV.procedural, p.id, decayKey, (current) => {
            if (!current) throw new Error("Procedural decay source disappeared during recovery");
            applyDecay([current], decayDays, Date.parse(effectTimestamp));
            return current;
          });
          else { applyDecay([p], decayDays); await kv.set(KV.procedural, p.id, p); }
        }

        results.decay = {
          semantic: semantic.length,
          procedural: procedural.length,
        };
      }

      if (process.env["OBSIDIAN_AUTO_EXPORT"] === "true") {
        try {
          await sdk.trigger({ function_id: "mem::obsidian-export", payload: { batchEffectKey: data?.batchEffectKey } });
          results.obsidianExport = { success: true };
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Obsidian auto-export failed", { error: msg });
          results.obsidianExport = { success: false, error: msg };
        }
      }

      const audit = recordAudit(kv, "consolidate", "mem::consolidate-pipeline", [], {
        tier,
        results,
      }, undefined, undefined, data?.batchEffectKey);
      if (data?.batchEffectKey) await audit.catch(() => { });
      else await audit;

      logger.info("Consolidation pipeline complete", { tier, results });
      return { success: true, results };
    }, () => recordAudit(kv, "consolidate", "mem::consolidate-pipeline", [], {}, undefined, undefined, data?.batchEffectKey)),
  );
}
