import type { IIIClient } from "iii-sdk";
import { withBatchMutationLocks } from "../state/batch-effects.js";
import type {
  Memory,
  SemanticMemory,
  RetentionScore,
  DecayConfig,
} from "../types.js";
import { KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import type { AccessLog } from "./access-tracker.js";
import {
  emptyAccessLog,
  deleteAccessLog,
  normalizeAccessLog,
} from "./access-tracker.js";
import { recordAudit } from "./audit.js";
import { getSearchIndex, vectorIndexRemove, flushIndexSave } from "./search.js";
import { logger } from "../logger.js";
import { cleanupAuthorized, hasRetentionProtection } from "./storage-retention.js";

const DEFAULT_DECAY: DecayConfig = {
  lambda: 0.01,
  sigma: 0.3,
  tierThresholds: {
    hot: 0.7,
    warm: 0.4,
    cold: 0.15,
  },
};

function resolveDecayConfig(
  input?: Partial<DecayConfig>,
): { config: DecayConfig } | { error: string } {
  const tierThresholds = {
    ...DEFAULT_DECAY.tierThresholds,
    ...(input?.tierThresholds ?? {}),
  };
  const config: DecayConfig = {
    lambda:
      typeof input?.lambda === "number" ? input.lambda : DEFAULT_DECAY.lambda,
    sigma: typeof input?.sigma === "number" ? input.sigma : DEFAULT_DECAY.sigma,
    tierThresholds,
  };

  if (!Number.isFinite(config.lambda) || config.lambda <= 0) {
    return { error: "config.lambda must be a positive number" };
  }
  if (!Number.isFinite(config.sigma) || config.sigma < 0) {
    return { error: "config.sigma must be a non-negative number" };
  }
  const { hot, warm, cold } = config.tierThresholds;
  if (![hot, warm, cold].every((v) => Number.isFinite(v))) {
    return {
      error: "config.tierThresholds.hot/warm/cold must be finite numbers",
    };
  }
  if (!(hot >= warm && warm >= cold && cold >= 0)) {
    return {
      error:
        "config.tierThresholds must satisfy hot >= warm >= cold >= 0",
    };
  }
  return { config };
}

function computeReinforcementBoost(
  accessTimestamps: number[],
  sigma: number,
): number {
  const now = Date.now();
  let boost = 0;
  for (const tAccess of accessTimestamps) {
    if (!Number.isFinite(tAccess)) continue;
    const daysSinceAccess = (now - tAccess) / (1000 * 60 * 60 * 24);
    if (daysSinceAccess > 0) {
      boost += 1 / daysSinceAccess;
    }
  }
  return boost * sigma;
}

function computeSalience(
  memory: Memory | SemanticMemory,
  accessCount: number,
): number {
  let baseSalience = 0.5;

  if ("type" in memory) {
    const typeWeights: Record<string, number> = {
      architecture: 0.9,
      bug: 0.7,
      pattern: 0.8,
      preference: 0.85,
      workflow: 0.6,
      fact: 0.5,
    };
    baseSalience = typeWeights[(memory as Memory).type] || 0.5;
  }

  if ("confidence" in memory) {
    baseSalience = Math.max(baseSalience, (memory as SemanticMemory).confidence);
  }

  const accessBonus = Math.min(0.2, accessCount * 0.02);
  return Math.min(1, baseSalience + accessBonus);
}

export function registerRetentionFunctions(
  sdk: IIIClient,
  kv: StateKV,
): void {
  sdk.registerFunction("mem::retention-score",
    async (data: { config?: Partial<DecayConfig> }) => {
      const resolved = resolveDecayConfig(data?.config);
      if ("error" in resolved) {
        return { success: false, error: resolved.error };
      }
      const { config } = resolved;

      const [memories, semanticMems, allLogs] = await Promise.all([
        kv.list<Memory>(KV.memories),
        kv.list<SemanticMemory>(KV.semantic),
        kv.list<unknown>(KV.accessLog).catch(() => [] as unknown[]),
      ]);
      const logsById = new Map<string, AccessLog>();
      for (const raw of allLogs) {
        const log = normalizeAccessLog(raw);
        if (log.memoryId) logsById.set(log.memoryId, log);
      }

      const scores: RetentionScore[] = [];

      const computeDecay = (createdAt: string): number =>
        Math.exp(
          -config.lambda *
          ((Date.now() - new Date(createdAt).getTime()) /
            (1000 * 60 * 60 * 24)),
        );

      // Build all entries in memory first, then flush with Promise.all
      // so a full rescore is one batched KV write instead of N sequential
      // round-trips. Separate counts for the audit record at the end.
      const pendingWrites: Array<[string, RetentionScore]> = [];
      let episodicScored = 0;
      let semanticScored = 0;

      for (const mem of memories) {
        if (!mem.isLatest) continue;
        const log = logsById.get(mem.id) ?? emptyAccessLog(mem.id);
        const salience = computeSalience(mem, log.count);
        const temporalDecay = computeDecay(mem.createdAt);
        const reinforcementBoost = computeReinforcementBoost(
          log.recent,
          config.sigma,
        );
        const score = Math.min(
          1,
          salience * temporalDecay + reinforcementBoost,
        );

        const entry: RetentionScore = {
          memoryId: mem.id,
          source: "episodic",
          score,
          salience,
          temporalDecay,
          reinforcementBoost,
          lastAccessed: log.lastAt || mem.updatedAt,
          accessCount: log.count,
        };

        scores.push(entry);
        pendingWrites.push([mem.id, entry]);
        episodicScored++;
      }

      for (const sem of semanticMems) {
        const log = logsById.get(sem.id) ?? emptyAccessLog(sem.id);

        // Pre-0.8.3 fallback: use sem.lastAccessedAt only when mem:access is empty.
        let accessTimestamps: number[];
        let effectiveCount: number;
        if (log.recent.length > 0 || log.count > 0) {
          accessTimestamps = log.recent;
          effectiveCount = log.count;
        } else if (sem.lastAccessedAt) {
          const legacyTs = Date.parse(sem.lastAccessedAt);
          accessTimestamps = Number.isFinite(legacyTs) ? [legacyTs] : [];
          effectiveCount = sem.accessCount;
        } else {
          accessTimestamps = [];
          effectiveCount = sem.accessCount;
        }

        const salience = computeSalience(sem, effectiveCount);
        const temporalDecay = computeDecay(sem.createdAt);
        const reinforcementBoost = computeReinforcementBoost(
          accessTimestamps,
          config.sigma,
        );
        const score = Math.min(
          1,
          salience * temporalDecay + reinforcementBoost,
        );

        const entry: RetentionScore = {
          memoryId: sem.id,
          source: "semantic",
          score,
          salience,
          temporalDecay,
          reinforcementBoost,
          lastAccessed: log.lastAt || sem.lastAccessedAt,
          accessCount: effectiveCount,
        };

        scores.push(entry);
        pendingWrites.push([sem.id, entry]);
        semanticScored++;
      }

      // Flush all retention rows in parallel. N sequential writes was
      // making full rescores O(n) round-trips on stores with 1000+
      // memories; batching drops that to O(1) wall time on the KV
      // backends that can pipeline.
      await Promise.all(
        pendingWrites.map(([id, entry]) =>
          kv.set(KV.retentionScores, id, entry),
        ),
      );

      scores.sort((a, b) => b.score - a.score);

      const tiers = {
        hot: scores.filter((s) => s.score >= config.tierThresholds.hot)
          .length,
        warm: scores.filter(
          (s) =>
            s.score >= config.tierThresholds.warm &&
            s.score < config.tierThresholds.hot,
        ).length,
        cold: scores.filter(
          (s) =>
            s.score >= config.tierThresholds.cold &&
            s.score < config.tierThresholds.warm,
        ).length,
        evictable: scores.filter(
          (s) => s.score < config.tierThresholds.cold,
        ).length,
      };

      logger.info("Retention scores computed", {
        total: scores.length,
        ...tiers,
      });

      // Audit the rescore as a single batched event per sweep. We
      // intentionally pass an empty targetIds array — a mature store
      // can have 1000+ memory ids per rescore and flooding the audit
      // log with every memoryId on every cron tick is worse than
      // recording just the summary. The details payload has enough
      // context for observability (counts per source + per tier).
      if (scores.length > 0) {
        await recordAudit(kv, "retention_score", "mem::retention-score", [], {
          total: scores.length,
          episodic: episodicScored,
          semantic: semanticScored,
          tiers,
          config,
        });
      }

      return { success: true, total: scores.length, tiers, scores };
    },
  );

  sdk.registerFunction("mem::retention-evict", async (data?: { threshold?: number; dryRun?: boolean; maxEvict?: number }) => {
    const threshold = typeof data?.threshold === "number" && Number.isFinite(data.threshold)
      ? Math.max(0, Math.min(1, data.threshold)) : DEFAULT_DECAY.tierThresholds.cold;
    const maxEvict = typeof data?.maxEvict === "number" && Number.isInteger(data.maxEvict)
      ? Math.max(0, Math.min(1000, data.maxEvict)) : 50;
    const authorized = await cleanupAuthorized(kv);
    const dryRun = data?.dryRun !== false || !authorized;
    return withBatchMutationLocks(kv, async () => {
      const candidates: Array<{ score: RetentionScore; scope: string; memory: Pick<Memory, "id" | "forgetAfter">; source: "episodic" | "semantic" }> = [];
      const now = Date.now();
      for await (const score of kv.values<RetentionScore>(KV.retentionScores)) {
        if (!Number.isFinite(score.score) || score.score >= threshold) continue;
        const scopes = score.source === "semantic" ? [KV.semantic]
          : score.source === "episodic" ? [KV.memories] : [KV.memories, KV.semantic];
        for (const scope of scopes) {
          const memory = await kv.get<Memory>(scope, score.memoryId);
          if (!memory || !memory.forgetAfter || !Number.isFinite(Date.parse(memory.forgetAfter))
            || Date.parse(memory.forgetAfter) > now || hasRetentionProtection({ ...memory })) continue;
          candidates.push({ score, scope, memory: { id: memory.id, forgetAfter: memory.forgetAfter }, source: scope === KV.semantic ? "semantic" : "episodic" });
          candidates.sort((a, b) => a.score.score - b.score.score);
          if (candidates.length > maxEvict) candidates.pop();
          break;
        }
      }
      if (dryRun) return { success: true, dryRun, cleanupAuthorized: authorized, wouldEvict: candidates.length,
        candidates: candidates.map(candidate => ({ id: candidate.memory.id, score: candidate.score.score })) };
      const { decrementImageRef } = await import("./image-refs.js");
      const evictedIds: string[] = [];
      let evictedEpisodic = 0, evictedSemantic = 0;
      for (const candidate of candidates) {
        const current = await kv.get<Memory>(candidate.scope, candidate.memory.id);
        if (!current || current.forgetAfter !== candidate.memory.forgetAfter || hasRetentionProtection({ ...current })) continue;
        await kv.delete(candidate.scope, current.id);
        await kv.delete(KV.retentionScores, current.id);
        await deleteAccessLog(kv, current.id);
        if (current.imageRef) await decrementImageRef(kv, sdk, current.imageRef);
        getSearchIndex().remove(current.id);
        vectorIndexRemove(current.id);
        evictedIds.push(current.id);
        if (candidate.source === "semantic") evictedSemantic++; else evictedEpisodic++;
        await recordAudit(kv, "delete", "mem::retention-evict", [current.id], { reason: "explicit memory TTL", threshold });
      }
      if (evictedIds.length) await flushIndexSave();
      return { success: true, dryRun, evicted: evictedIds.length, evictedEpisodic, evictedSemantic };
    });
  });
}
