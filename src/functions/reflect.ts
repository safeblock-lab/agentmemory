import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import type { Insight, MemoryProvider, EmbeddingProvider } from "../types.js";
import { isBackgroundRecoveryPaused } from "../config.js";
import { recordAudit } from "./audit.js";
import { applyBatchEffect, runBatchCallback, batchEffectKey } from "../state/batch-effects.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import { registerReflectQueue, type SelectedReflectControl } from "./reflect-queue.js";
import { legacyReflectEvidence, currentLegacyReflectEvidence } from "./reflect-retrieval.js";
import { evaluatePublicationEvidence, type PublicationEvidenceInput } from "./publication-evidence.js";
import { publicationExecutor } from "./publication-call-options.js";
import { stripPrivateData } from "./privacy.js";
import { parseReflectionResult } from "./reflect-result.js";
import { REFLECT_CONTROL, validReflectText, type ConceptCluster, type ReflectRequest } from "./reflect-queue-types.js";

async function applyInsights(
  kv: StateKV,
  response: string,
  cluster: ConceptCluster,
  project: string | undefined,
  maxInsights: number,
  effectKey: string,
): Promise<{ newInsights: number; reinforced: number; count: number; insightIds: string[] }> {
  let newInsights = 0;
  let reinforced = 0;
  let count = 0;
  const insightIds: string[] = [];

  for (const { title, content, confidence } of parseReflectionResult(response).slice(0, maxInsights)) {

    const legacyId = fingerprintId("ins", content.toLowerCase());
    const legacy = await kv.get<Insight>(KV.insights, legacyId);
    const id = legacy?.project === project ? legacyId : fingerprintId("ins", `${project ?? ""}:${content.toLowerCase()}`);
    await applyBatchEffect<Insight & { sourceObservationIds?: string[] }>(kv, KV.insights, id, effectKey, (existing) => {
        if (existing && existing.project !== project) throw new Error("reflect_insight_project_mismatch");
        if (existing && !existing.deleted) {
          reinforceInsight(existing);
          existing.sourceMemoryIds = [...new Set([...existing.sourceMemoryIds, ...cluster.factIds, ...(cluster.memoryIds ?? [])])];
          existing.sourceLessonIds = [...new Set([...existing.sourceLessonIds, ...cluster.lessonIds])];
          existing.sourceCrystalIds = [...new Set([...existing.sourceCrystalIds, ...cluster.crystalIds])];
          existing.sourceObservationIds = [...new Set([...(existing.sourceObservationIds ?? []), ...(cluster.observationIds ?? [])])];
          return existing;
        }
        const now = new Date().toISOString();
        return { id, title, content, confidence, reinforcements: 0, sourceConceptCluster: cluster.concepts,
          sourceMemoryIds: [...cluster.factIds, ...(cluster.memoryIds ?? [])], sourceObservationIds: cluster.observationIds ?? [], sourceLessonIds: cluster.lessonIds, sourceCrystalIds: cluster.crystalIds,
          project, tags: cluster.concepts, createdAt: now, updatedAt: now, decayRate: 0.05 };
      });
    if (legacy && !legacy.deleted && legacy.project === project) reinforced++;
    else newInsights++;
    if (!insightIds.includes(id)) insightIds.push(id);
    count++;
  }

  return { newInsights, reinforced, count, insightIds };
}

function reinforceInsight(insight: Insight): void {
  const now = new Date().toISOString();
  insight.reinforcements++;
  insight.confidence = Math.min(
    1.0,
    insight.confidence + 0.1 * (1 - insight.confidence),
  );
  insight.lastReinforcedAt = now;
  insight.updatedAt = now;
}

function validInsightReadText(value: unknown, maxBytes = 512): value is string {
  return typeof value === "string" && value.length > 0 && value.trim() === value && Buffer.byteLength(value) <= maxBytes;
}

function insightReadLimit(value: unknown, fallback: number): number | null {
  if (value === undefined || value === null) return fallback;
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

async function readSelectedInsights(
  kv: StateKV,
  value: unknown,
): Promise<Insight[] | { success: false; error: string }> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { success: false, error: "selected insight scope is invalid" };
  }
  const selected = value as { project?: unknown; insightIds?: unknown };
  if (!validInsightReadText(selected.project, 4096) || !Array.isArray(selected.insightIds)
    || selected.insightIds.length < 1 || selected.insightIds.length > 12
    || selected.insightIds.some((id: unknown) => !validInsightReadText(id))
    || new Set(selected.insightIds).size !== selected.insightIds.length) {
    return { success: false, error: "selected insight scope is invalid" };
  }

  const insightIds = selected.insightIds as string[];
  const records = await Promise.all(insightIds.map(id => kv.get<unknown>(KV.insights, id)));
  const insights: Insight[] = [];
  for (let index = 0; index < records.length; index++) {
    const record = records[index];
    if (record === null || record === undefined) return { success: false, error: "selected insight not found" };
    if (!record || typeof record !== "object" || Array.isArray(record)) {
      return { success: false, error: "selected insight record is invalid" };
    }
    const insight = record as Partial<Insight>;
    if (insight.id !== insightIds[index] || !validInsightReadText(insight.title)
      || !validInsightReadText(insight.content, 64 * 1024) || !Number.isFinite(insight.confidence)
      || !Array.isArray(insight.tags) || insight.tags.some(tag => !validInsightReadText(tag))) {
      return { success: false, error: "selected insight record is invalid" };
    }
    if (insight.project !== selected.project) return { success: false, error: "selected insight project scope mismatch" };
    insights.push(insight as Insight);
  }
  return insights;
}

export function registerReflectFunctions(
  sdk: IIIClient, kv: StateKV, provider: MemoryProvider, llmRouter?: LlmTaskRouter,
  batchQueue?: FireworksBatchQueue, _unusedProvider?: unknown, embedding?: EmbeddingProvider,
): void {
  const queue = registerReflectQueue(sdk, kv, provider,
    (response, cluster, project, limit, effect) => applyInsights(kv, response, cluster, project, limit, effect),
    llmRouter, batchQueue, embedding);
  sdk.registerFunction("mem::reflect", async (data: ReflectRequest & { selected?: SelectedReflectControl }) => {
    if (data?.batchResponse !== undefined || data?.batchCluster !== undefined) {
      if (typeof data.batchResponse !== "string" || Buffer.byteLength(data.batchResponse) > 16384 ||
          typeof data.batchCluster !== "string" || Buffer.byteLength(data.batchCluster) > 16384) {
        return { success: false, error: "Invalid batch reflection payload" };
      }
      let cluster: ConceptCluster;
      try {
        const value = JSON.parse(data.batchCluster);
        const names = ["concepts", "factIds", "lessonIds", "crystalIds"];
        if (!value || names.some(name => !Array.isArray(value[name]) || value[name].length > 12 ||
          value[name].some((item: unknown) => typeof item !== "string" || Buffer.byteLength(item) > 256)) ||
          value.factIds.length + value.lessonIds.length + value.crystalIds.length > 12) throw new Error("invalid_cluster");
        cluster = value;
      } catch { return { success: false, error: "Invalid batch reflection cluster" }; }
      if (cluster.jobId !== undefined || cluster.unitId !== undefined) {
        if (!validReflectText(cluster.jobId) || !validReflectText(cluster.unitId)) return { success: false, error: "Invalid batch reflection identity" };
        return queue.acceptBatch(data, cluster);
      }
      const effect = data.batchEffectKey && /^[a-f0-9]{64}$/.test(data.batchEffectKey) ? data.batchEffectKey :
        batchEffectKey(JSON.stringify([data.batchSourceFingerprint, data.batchCluster, data.batchResponse]));
      return withKeyedLock(`reflect-legacy:${effect}`, async () => {
      const captureId = `callback:${effect}`;
      const captured = await kv.get<{ response: string; cluster: ConceptCluster; project?: string; fingerprint?: string }>(REFLECT_CONTROL, captureId);
      if (captured && (captured.response !== data.batchResponse || JSON.stringify(captured.cluster) !== JSON.stringify(cluster)
        || captured.project !== data.project || captured.fingerprint !== data.batchSourceFingerprint)) {
        return { success: false, error: "reflect_callback_identity_mismatch" };
      }
      const result = captured ?? { response: data.batchResponse, cluster, project: data.project, fingerprint: data.batchSourceFingerprint };
      if (!captured) await kv.set(REFLECT_CONTROL, captureId, result);
      const receipt = await kv.get<{ state?: string }>(KV.batchCallbacks, `reflect:${effect}`);
      let evidence: Awaited<ReturnType<typeof legacyReflectEvidence>>;
      if (receipt?.state !== "started" && receipt?.state !== "completed" && receipt?.state !== "stale") {
          if (isBackgroundRecoveryPaused()) return { success: false, reason: "background_recovery_paused" };
          evidence = await legacyReflectEvidence(kv, result.cluster, result.project);
          if (!evidence || evidence.fingerprint !== result.fingerprint) return { success: true, stale: true };
          const input: PublicationEvidenceInput = { workflow: "reflection", operationId: `legacy-reflect:${effect}`,
            project: result.project!, evidenceComplete: true,
            candidate: JSON.stringify({ generatedResponse: result.response, cluster: result.cluster,
              sourceFingerprint: evidence.fingerprint, sourceLineage: evidence.rows.map(row => ({ scope: row.scope,
                id: row.key, version: row.version, fingerprint: row.fingerprint })) }),
            sources: evidence.rows.map(row => ({ id: JSON.stringify([row.scope, row.key]), scope: row.scope, key: row.key,
              version: row.version, project: result.project!, content: stripPrivateData(JSON.stringify(row.value)) })) };
          const verdict = await evaluatePublicationEvidence(kv, input,
            publicationExecutor(provider, llmRouter, "reflection"));
          if (!await currentLegacyReflectEvidence(kv, evidence, result.project!)) return { success: true, stale: true };
          if (verdict.status === "unsupported") return { success: true, rejected: true, reason: "reflect_publication_unsupported" };
          if (verdict.status !== "supported") return { success: false, reason: `reflect_publication_${verdict.status}` };
      }
      return runBatchCallback(kv, "reflect", effect, async (resuming, admit) => {
        if (!resuming && isBackgroundRecoveryPaused()) return { success: false, reason: "background_recovery_paused" };
        if (!resuming && (!evidence || !await currentLegacyReflectEvidence(kv, evidence, result.project!))) return { success: true, stale: true };
        await admit();
        const applied = await applyInsights(kv, result.response, result.cluster, result.project, 5, effect);
        await recordAudit(kv, "reflect", "mem::reflect", [], applied, undefined, undefined, effect);
        return { success: true, ...applied };
      });
      });
    }
    return queue.accept(data ?? {});
  });

  sdk.registerFunction("mem::insight-list", 
    async (data: {
      project?: unknown;
      minConfidence?: unknown;
      limit?: unknown;
      selected?: unknown;
    }) => {
      const limit = insightReadLimit(data?.limit, 50);
      const minConfidence = data?.minConfidence ?? 0;
      if (limit === null) return { success: false, error: "limit must be a non-negative safe integer" };
      if (typeof minConfidence !== "number" || !Number.isFinite(minConfidence) || minConfidence < 0 || minConfidence > 1) {
        return { success: false, error: "minConfidence must be between 0 and 1" };
      }
      if (data?.selected !== undefined && data.project !== undefined
        && (!validInsightReadText(data.project, 4096) || data.project !== (data.selected as { project?: unknown } | null)?.project)) {
        return { success: false, error: "selected insight project does not match project filter" };
      }
      let items = data?.selected === undefined
        ? await kv.list<Insight>(KV.insights)
        : await readSelectedInsights(kv, data.selected);
      if (!Array.isArray(items)) return items;

      items = items.filter(
        (i) => !i.deleted && i.confidence >= minConfidence,
      );

      if (data?.selected === undefined && data?.project) {
        items = items.filter((i) => i.project === data.project);
      }

      items.sort((a, b) => b.confidence - a.confidence || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));

      return { success: true, insights: items.slice(0, limit) };
    },
  );

  sdk.registerFunction("mem::insight-search", 
    async (data: {
      query?: unknown;
      project?: unknown;
      minConfidence?: unknown;
      limit?: unknown;
      selected?: unknown;
    }) => {
      if (typeof data?.query !== "string" || !data.query.trim()) {
        return { success: false, error: "query is required" };
      }

      const query = data.query.trim().toLowerCase();
      const minConfidence = data.minConfidence ?? 0.1;
      const limit = insightReadLimit(data.limit, 10);
      if (limit === null) return { success: false, error: "limit must be a non-negative safe integer" };
      if (typeof minConfidence !== "number" || !Number.isFinite(minConfidence) || minConfidence < 0 || minConfidence > 1) {
        return { success: false, error: "minConfidence must be between 0 and 1" };
      }
      if (data.selected !== undefined && data.project !== undefined
        && (!validInsightReadText(data.project, 4096) || data.project !== (data.selected as { project?: unknown } | null)?.project)) {
        return { success: false, error: "selected insight project does not match project filter" };
      }

      let items = data.selected === undefined
        ? await kv.list<Insight>(KV.insights)
        : await readSelectedInsights(kv, data.selected);
      if (!Array.isArray(items)) return items;
      items = items.filter(
        (i) => !i.deleted && i.confidence >= minConfidence,
      );

      if (data.selected === undefined && data.project) {
        items = items.filter((i) => i.project === data.project);
      }

      const terms = query.split(/\s+/).filter((t) => t.length > 1);
      const scored = items
        .map((i) => {
          const text =
            `${i.title} ${i.content} ${i.tags.join(" ")}`.toLowerCase();
          const matchCount = terms.filter((t) => text.includes(t)).length;
          if (matchCount === 0) return null;

          const relevance = matchCount / terms.length;
          const daysSince = i.lastReinforcedAt
            ? (Date.now() - new Date(i.lastReinforcedAt).getTime()) /
              (1000 * 60 * 60 * 24)
            : (Date.now() - new Date(i.createdAt).getTime()) /
              (1000 * 60 * 60 * 24);
          const recencyBoost = 1 / (1 + daysSince * 0.01);
          const score = i.confidence * relevance * recencyBoost;

          return { insight: i, score };
        })
        .filter(Boolean) as Array<{ insight: Insight; score: number }>;

      scored.sort((a, b) => b.score - a.score || (a.insight.id < b.insight.id ? -1 : a.insight.id > b.insight.id ? 1 : 0));

      try {
        await recordAudit(kv, "insight_search", "mem::insight-search", [], {
          query: data.query,
          resultCount: scored.length,
        });
      } catch {}

      return {
        success: true,
        insights: scored.slice(0, limit).map((s) => ({
          ...s.insight,
          score: Math.round(s.score * 1000) / 1000,
        })),
      };
    },
  );

  sdk.registerFunction("mem::insight-decay-sweep", 
    async () => withKeyedLock("batch-callback:reflect", async () => {
      const items = await kv.list<Insight>(KV.insights);
      let decayed = 0;
      let softDeleted = 0;
      const now = Date.now();
      const timestamp = new Date().toISOString();
      const dirty: Insight[] = [];

      for (const insight of items) {
        if (insight.deleted) continue;

        const baseline =
          insight.lastDecayedAt ||
          insight.lastReinforcedAt ||
          insight.createdAt;
        const weeksSince =
          (now - new Date(baseline).getTime()) / (1000 * 60 * 60 * 24 * 7);

        if (weeksSince < 1) continue;

        const decay = insight.decayRate * weeksSince;
        const newConfidence = Math.max(0.05, insight.confidence - decay);

        if (newConfidence !== insight.confidence) {
          insight.confidence = Math.round(newConfidence * 1000) / 1000;
          insight.lastDecayedAt = timestamp;
          insight.updatedAt = timestamp;

          if (insight.confidence <= 0.1 && insight.reinforcements === 0) {
            insight.deleted = true;
            softDeleted++;
          } else {
            decayed++;
          }

          dirty.push(insight);
        }
      }

      await Promise.all(dirty.map((i) => kv.set(KV.insights, i.id, i)));
      await recordAudit(kv, "reflect", "mem::insight-decay-sweep", dirty.map((i) => i.id), {
        event: "insight.decay",
        decayed,
        softDeleted,
        total: items.length,
        timestamp,
      });

      return { success: true, decayed, softDeleted, total: items.length };
    }),
  );
}
