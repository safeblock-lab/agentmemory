import type { ISdk } from "iii-sdk";
import type {
  Session,
  CompressedObservation,
  Memory,
  SessionSummary,
  ProjectProfile,
  ExportData,
  GraphNode,
  GraphEdge,
  SemanticMemory,
  ProceduralMemory,
  Action,
  ActionEdge,
  Routine,
  Signal,
  Checkpoint,
  Sentinel,
  Sketch,
  Crystal,
  Facet,
  Lesson,
  Insight,
  AccessLogExport,
} from "../types.js";
import { normalizeAccessLog } from "./access-tracker.js";
import { KV } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { VERSION } from "../version.js";
import { recordAudit } from "./audit.js";
import { logger } from "../logger.js";
import { withBatchMutationLocks, preserveBatchProvenance, effectMetadata } from "../state/batch-effects.js";
import type { BatchEffectMetadata } from "../types.js";

export function registerExportImportFunction(sdk: ISdk, kv: StateKV): void {
  sdk.registerFunction("mem::export", 
    async (data?: { maxSessions?: number; offset?: number }) => {
      const rawMax = Number(data?.maxSessions);
      const maxSessions = Number.isFinite(rawMax) && rawMax > 0 ? Math.min(Math.floor(rawMax), 1000) : undefined;
      const rawOffset = Number(data?.offset);
      const offset = Number.isFinite(rawOffset) && rawOffset >= 0 ? Math.floor(rawOffset) : 0;

      const allSessions = await kv.list<Session>(KV.sessions);
      const paginatedSessions = maxSessions !== undefined
        ? allSessions.slice(offset, offset + maxSessions)
        : allSessions;
      const memories = await kv.list<Memory>(KV.memories);
      const summaries = await kv.list<SessionSummary>(KV.summaries);

      const observations: Record<string, CompressedObservation[]> = {};
      const obsResults = await Promise.all(
        paginatedSessions.map((session) =>
          kv
            .list<CompressedObservation>(KV.observations(session.id))
            .catch(() => [] as CompressedObservation[])
            .then((obs) => ({ sessionId: session.id, obs })),
        ),
      );
      for (const { sessionId, obs } of obsResults) {
        if (obs.length > 0) {
          observations[sessionId] = obs;
        }
      }

      const profiles: ProjectProfile[] = [];
      const uniqueProjects = [...new Set(paginatedSessions.map((s) => s.project))];
      const profileResults = await Promise.all(
        uniqueProjects.map((project) =>
          kv.get<ProjectProfile>(KV.profiles, project).catch(() => null),
        ),
      );
      for (const profile of profileResults) {
        if (profile) profiles.push(profile);
      }

      const [
        graphNodes,
        graphEdges,
        semanticMemories,
        proceduralMemories,
        actions,
        actionEdges,
        sentinels,
        sketches,
        crystals,
        facets,
        lessons,
        insights,
        routines,
        signals,
        checkpoints,
        accessLogs,
      ] = await Promise.all([
        kv.list<GraphNode>(KV.graphNodes).catch(() => []),
        kv.list<GraphEdge>(KV.graphEdges).catch(() => []),
        kv.list<SemanticMemory>(KV.semantic).catch(() => []),
        kv.list<ProceduralMemory>(KV.procedural).catch(() => []),
        kv.list<Action>(KV.actions).catch(() => []),
        kv.list<ActionEdge>(KV.actionEdges).catch(() => []),
        kv.list<Sentinel>(KV.sentinels).catch(() => []),
        kv.list<Sketch>(KV.sketches).catch(() => []),
        kv.list<Crystal>(KV.crystals).catch(() => []),
        kv.list<Facet>(KV.facets).catch(() => []),
        kv.list<Lesson>(KV.lessons).catch(() => []),
        kv.list<Insight>(KV.insights).catch(() => []),
        kv.list<Routine>(KV.routines).catch(() => []),
        kv.list<Signal>(KV.signals).catch(() => []),
        kv.list<Checkpoint>(KV.checkpoints).catch(() => []),
        kv.list<AccessLogExport>(KV.accessLog).catch(() => []),
      ]);

      const exportData: ExportData = {
        version: VERSION,
        exportedAt: new Date().toISOString(),
        sessions: paginatedSessions,
        observations,
        memories,
        summaries,
        profiles: profiles.length > 0 ? profiles : undefined,
        graphNodes: graphNodes.length > 0 ? graphNodes : undefined,
        graphEdges: graphEdges.length > 0 ? graphEdges : undefined,
        semanticMemories:
          semanticMemories.length > 0 ? semanticMemories : undefined,
        proceduralMemories:
          proceduralMemories.length > 0 ? proceduralMemories : undefined,
        actions: actions.length > 0 ? actions : undefined,
        actionEdges: actionEdges.length > 0 ? actionEdges : undefined,
        sentinels: sentinels.length > 0 ? sentinels : undefined,
        sketches: sketches.length > 0 ? sketches : undefined,
        crystals: crystals.length > 0 ? crystals : undefined,
        facets: facets.length > 0 ? facets : undefined,
        lessons: lessons.length > 0 ? lessons : undefined,
        insights: insights.length > 0 ? insights : undefined,
        routines: routines.length > 0 ? routines : undefined,
        signals: signals.length > 0 ? signals : undefined,
        checkpoints: checkpoints.length > 0 ? checkpoints : undefined,
        accessLogs: accessLogs.length > 0 ? accessLogs : undefined,
      };

      if (maxSessions !== undefined) {
        exportData.pagination = {
          offset,
          limit: maxSessions,
          total: allSessions.length,
          hasMore: offset + maxSessions < allSessions.length,
        };
      }

      const totalObs = Object.values(observations).reduce(
        (sum, arr) => sum + arr.length,
        0,
      );
      logger.info("Export complete", {
        sessions: paginatedSessions.length,
        totalSessions: allSessions.length,
        observations: totalObs,
        memories: memories.length,
        summaries: summaries.length,
      });

      return exportData;
    },
  );

  sdk.registerFunction("mem::import", 
    async (data: {
      exportData: ExportData;
      strategy?: "merge" | "replace" | "skip";
    }) => withBatchMutationLocks(kv, async () => {
      if (
        !data?.exportData ||
        typeof data.exportData !== "object" ||
        typeof (data.exportData as { version?: unknown }).version !== "string"
      ) {
        return { success: false, error: "exportData with string version is required" };
      }
      const strategy = data.strategy || "merge";
      if (!["merge", "replace", "skip"].includes(strategy)) return { success: false, error: "Invalid import strategy" };
      const importData = data.exportData;
      for (const records of [importData.lessons, importData.insights, importData.semanticMemories, importData.proceduralMemories]) {
        if (!Array.isArray(records)) continue;
        if (records.some((record) => !record || (record.appliedBatchEffects !== undefined && (
          !Array.isArray(record.appliedBatchEffects) || record.appliedBatchEffects.length > 4096 ||
          record.appliedBatchEffects.some((key) => typeof key !== "string" || !/^[a-f0-9]{64}$/.test(key))
        )))) return { success: false, error: "Invalid batch effect metadata" };
      }

      const supportedVersions = new Set(["0.3.0", "0.4.0", "0.5.0", "0.6.0", "0.6.1", "0.7.0", "0.7.2", "0.7.3", "0.7.4", "0.7.5", "0.7.6", "0.7.7", "0.7.9", "0.8.0", "0.8.1", "0.8.2", "0.8.3", "0.8.4", "0.8.5", "0.8.6", "0.8.7", "0.8.8", "0.8.9", "0.8.10", "0.8.11", "0.8.12", "0.8.13", "0.9.0", "0.9.1", "0.9.2", "0.9.3", "0.9.4", "0.9.5", "0.9.6", "0.9.7", "0.9.8", "0.9.9", "0.9.10", "0.9.11", "0.9.12", "0.9.13", "0.9.14", "0.9.15", "0.9.16", "0.9.17", "0.9.18", "0.9.19", "0.9.20", "0.9.21", "0.9.22", "0.9.23", "0.9.24", "0.9.25", "0.9.26", "0.9.27", "0.9.28", "0.9.29", "0.9.30", "0.9.31", "0.9.32", "0.9.33", "0.9.34", "0.9.35", "0.9.36", "0.9.37", "0.9.38", "0.9.39", "0.9.40", "0.9.41", "0.9.42", "0.9.43", "0.9.44", "0.9.45", "0.9.46", "0.9.47", "0.9.48", "0.9.49", "0.9.50", "0.9.51", "0.9.52", "0.9.53"]);
      if (!supportedVersions.has(importData.version)) {
        return {
          success: false,
          error: `Unsupported export version: ${importData.version}`,
        };
      }

      const MAX_SESSIONS = 10_000;
      const MAX_MEMORIES = 50_000;
      const MAX_SUMMARIES = 10_000;
      const MAX_OBS_PER_SESSION = 5_000;
      const MAX_TOTAL_OBSERVATIONS = 500_000;
      const MAX_ACCESS_LOGS = 50_000;

      if (!Array.isArray(importData.sessions)) {
        return { success: false, error: "sessions must be an array" };
      }
      if (!Array.isArray(importData.memories)) {
        return { success: false, error: "memories must be an array" };
      }
      if (!Array.isArray(importData.summaries)) {
        return { success: false, error: "summaries must be an array" };
      }
      if (
        typeof importData.observations !== "object" ||
        importData.observations === null ||
        Array.isArray(importData.observations)
      ) {
        return { success: false, error: "observations must be an object" };
      }

      if (importData.sessions.length > MAX_SESSIONS) {
        return {
          success: false,
          error: `Too many sessions (max ${MAX_SESSIONS})`,
        };
      }
      if (importData.memories.length > MAX_MEMORIES) {
        return {
          success: false,
          error: `Too many memories (max ${MAX_MEMORIES})`,
        };
      }
      if (importData.summaries.length > MAX_SUMMARIES) {
        return {
          success: false,
          error: `Too many summaries (max ${MAX_SUMMARIES})`,
        };
      }
      const MAX_OBS_BUCKETS = 10_000;
      const obsBuckets = Object.keys(importData.observations);
      if (obsBuckets.length > MAX_OBS_BUCKETS) {
        return {
          success: false,
          error: `Too many observation buckets (max ${MAX_OBS_BUCKETS})`,
        };
      }

      let totalObservations = 0;
      for (const [, obs] of Object.entries(importData.observations)) {
        if (!Array.isArray(obs)) {
          return { success: false, error: "observation values must be arrays" };
        }
        if (obs.length > MAX_OBS_PER_SESSION) {
          return {
            success: false,
            error: `Too many observations per session (max ${MAX_OBS_PER_SESSION})`,
          };
        }
        totalObservations += obs.length;
      }
      if (totalObservations > MAX_TOTAL_OBSERVATIONS) {
        return {
          success: false,
          error: `Too many total observations (max ${MAX_TOTAL_OBSERVATIONS})`,
        };
      }

      const sectionKeys = {
        sessions: "id", memories: "id", summaries: "sessionId", profiles: "project",
        graphNodes: "id", graphEdges: "id", semanticMemories: "id", proceduralMemories: "id",
        actions: "id", actionEdges: "id", routines: "id", signals: "id", checkpoints: "id",
        sentinels: "id", sketches: "id", crystals: "id", facets: "id", lessons: "id", insights: "id",
        accessLogs: "memoryId",
      } as const;
      function validRecord(value: unknown, key: string): boolean {
        if (!value || typeof value !== "object" || Array.isArray(value)) return false;
        const id = (value as Record<string, unknown>)[key];
        return typeof id === "string" && id.trim().length > 0;
      }
      // Validate every collection consumed below before replace can delete
      // anything. Legacy optional fields are normalized, not made mandatory.
      for (const [section, key] of Object.entries(sectionKeys)) {
        const records = importData[section as keyof typeof sectionKeys];
        if (records === undefined) continue;
        if (!Array.isArray(records)) return { success: false, error: `${section} must be an array` };
        const limit = section === "accessLogs" ? MAX_ACCESS_LOGS : MAX_MEMORIES;
        if (records.length > limit) return { success: false, error: `Too many ${section} (max ${limit})` };
        if (records.some((record) => !validRecord(record, key))) return { success: false, error: `${section} contains an invalid record` };
      }
      for (const [sessionId, observations] of Object.entries(importData.observations)) {
        if (!sessionId.trim() || observations.some((observation) => !validRecord(observation, "id"))) {
          return { success: false, error: "observations contains an invalid record or session key" };
        }
      }
      const normalizedAccessLogs = (importData.accessLogs ?? []).map(normalizeAccessLog);

      const protectedScopes = [KV.graphNodes, KV.graphEdges, KV.semantic, KV.procedural, KV.lessons, KV.insights];
      const prepared = new Map<string, Map<string, object>>();
      async function prepareRecords<T extends { id: string }>(scope: string, records?: T[]): Promise<void> {
        if (records === undefined) return;
        if (!Array.isArray(records) || records.length > MAX_MEMORIES) throw new Error("Invalid protected import collection");
        const merged = new Map<string, object>();
        for (const record of records) {
          if (!record || typeof record.id !== "string" || !record.id) throw new Error("Invalid protected import record");
          const current = (merged.get(record.id) as T | undefined) ?? await kv.get<T>(scope, record.id);
          if (strategy === "skip" && current) continue;
          merged.set(record.id, preserveBatchProvenance(strategy === "replace" ? null : current, record));
        }
        prepared.set(scope, merged);
      }
      function preparedRecord<T extends { id: string }>(scope: string, record: T): T {
        return (prepared.get(scope)?.get(record.id) as T | undefined) ?? record;
      }
      try {
        if (strategy === "replace") {
          // Callback receipts also protect deterministic graph/crystal effects
          // whose identities do not carry a same-record receipt array.
          if ((await kv.list(KV.batchCallbacks)).length) return { success: false, error: "Replace blocked: existing batch callback receipts must be preserved; use merge" };
          for (const scope of [...protectedScopes, KV.graphSnapshot]) {
            for (const record of await kv.list<BatchEffectMetadata>(scope)) {
              if (effectMetadata(record).appliedBatchEffects?.length) return { success: false, error: "Replace blocked: existing batch effect metadata must be preserved; use merge" };
            }
          }
        }
        // This preflight runs under maintenance, before any delete/write. A
        // union overflow must not leave an otherwise partially imported file.
        await prepareRecords(KV.graphNodes, importData.graphNodes);
        await prepareRecords(KV.graphEdges, importData.graphEdges);
        await prepareRecords(KV.semantic, importData.semanticMemories);
        await prepareRecords(KV.procedural, importData.proceduralMemories);
        await prepareRecords(KV.lessons, importData.lessons);
        await prepareRecords(KV.insights, importData.insights);
      } catch {
        return { success: false, error: "Import preflight failed: invalid or unavailable batch metadata, or receipt capacity exhausted" };
      }

      const stats = {
        sessions: 0,
        observations: 0,
        memories: 0,
        summaries: 0,
        skipped: 0,
      };

      if (strategy === "replace") {
        const existing = await kv.list<Session>(KV.sessions);
        for (const session of existing) {
          await kv.delete(KV.sessions, session.id);
          const obs = await kv
            .list<CompressedObservation>(KV.observations(session.id))
            .catch(() => []);
          for (const o of obs) {
            await kv.delete(KV.observations(session.id), o.id);
          }
        }
        const existingMem = await kv.list<Memory>(KV.memories);
        for (const m of existingMem) {
          await kv.delete(KV.memories, m.id);
        }
        const existingSummaries = await kv.list<SessionSummary>(KV.summaries);
        for (const s of existingSummaries) {
          await kv.delete(KV.summaries, s.sessionId);
        }
        for (const a of await kv.list<Action>(KV.actions).catch(() => [])) {
          await kv.delete(KV.actions, a.id);
        }
        for (const e of await kv.list<ActionEdge>(KV.actionEdges).catch(() => [])) {
          await kv.delete(KV.actionEdges, e.id);
        }
        for (const r of await kv.list<Routine>(KV.routines).catch(() => [])) {
          await kv.delete(KV.routines, r.id);
        }
        for (const s of await kv.list<Signal>(KV.signals).catch(() => [])) {
          await kv.delete(KV.signals, s.id);
        }
        for (const c of await kv.list<Checkpoint>(KV.checkpoints).catch(() => [])) {
          await kv.delete(KV.checkpoints, c.id);
        }
        for (const s of await kv.list<Sentinel>(KV.sentinels).catch(() => [])) {
          await kv.delete(KV.sentinels, s.id);
        }
        for (const s of await kv.list<Sketch>(KV.sketches).catch(() => [])) {
          await kv.delete(KV.sketches, s.id);
        }
        for (const c of await kv.list<Crystal>(KV.crystals).catch(() => [])) {
          await kv.delete(KV.crystals, c.id);
        }
        for (const f of await kv.list<Facet>(KV.facets).catch(() => [])) {
          await kv.delete(KV.facets, f.id);
        }
        for (const l of await kv.list<Lesson>(KV.lessons).catch(() => [])) {
          await kv.delete(KV.lessons, l.id);
        }
        for (const i of await kv.list<Insight>(KV.insights).catch(() => [])) {
          await kv.delete(KV.insights, i.id);
        }
        for (const n of await kv.list<{ id: string }>(KV.graphNodes).catch(() => [])) {
          await kv.delete(KV.graphNodes, n.id);
        }
        for (const e of await kv.list<{ id: string }>(KV.graphEdges).catch(() => [])) {
          await kv.delete(KV.graphEdges, e.id);
        }
        for (const s of await kv.list<{ id: string }>(KV.semantic).catch(() => [])) {
          await kv.delete(KV.semantic, s.id);
        }
        for (const p of await kv.list<{ id: string }>(KV.procedural).catch(() => [])) {
          await kv.delete(KV.procedural, p.id);
        }
        for (const profile of await kv.list<ProjectProfile>(KV.profiles).catch(() => [])) {
          await kv.delete(KV.profiles, profile.project);
        }
        for (const a of await kv.list<AccessLogExport>(KV.accessLog).catch(() => [])) {
          await kv.delete(KV.accessLog, a.memoryId);
        }
      }

      for (const session of importData.sessions) {
        if (strategy === "skip") {
          const existing = await kv
            .get<Session>(KV.sessions, session.id)
            .catch(() => null);
          if (existing) {
            stats.skipped++;
            continue;
          }
        }
        await kv.set(KV.sessions, session.id, session);
        stats.sessions++;
      }

      for (const [sessionId, obs] of Object.entries(importData.observations)) {
        for (const o of obs) {
          if (strategy === "skip") {
            const existing = await kv
              .get<CompressedObservation>(KV.observations(sessionId), o.id)
              .catch(() => null);
            if (existing) {
              stats.skipped++;
              continue;
            }
          }
          await kv.set(KV.observations(sessionId), o.id, o);
          stats.observations++;
        }
      }

      for (const memory of importData.memories) {
        if (strategy === "skip") {
          const existing = await kv
            .get<Memory>(KV.memories, memory.id)
            .catch(() => null);
          if (existing) {
            stats.skipped++;
            continue;
          }
        }
        // Older exports + hand-edited dumps can omit this field.
        if (!Array.isArray(memory.sessionIds)) {
          memory.sessionIds = [];
        }
        await kv.set(KV.memories, memory.id, memory);
        stats.memories++;
      }

      for (const summary of importData.summaries) {
        if (strategy === "skip") {
          const existing = await kv
            .get<SessionSummary>(KV.summaries, summary.sessionId)
            .catch(() => null);
          if (existing) {
            stats.skipped++;
            continue;
          }
        }
        await kv.set(KV.summaries, summary.sessionId, summary);
        stats.summaries++;
      }

      if (importData.graphNodes) {
        for (const node of importData.graphNodes) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.graphNodes, node.id);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.graphNodes, node.id, preparedRecord(KV.graphNodes, node));
        }
      }
      if (importData.graphEdges) {
        for (const edge of importData.graphEdges) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.graphEdges, edge.id);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.graphEdges, edge.id, preparedRecord(KV.graphEdges, edge));
        }
      }
      if (importData.semanticMemories) {
        for (const sem of importData.semanticMemories) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.semantic, sem.id);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.semantic, sem.id, preparedRecord(KV.semantic, sem));
        }
      }
      if (importData.proceduralMemories) {
        for (const proc of importData.proceduralMemories) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.procedural, proc.id);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.procedural, proc.id, preparedRecord(KV.procedural, proc));
        }
      }
      if (importData.profiles) {
        for (const profile of importData.profiles) {
          if (strategy === "skip") {
            const existing = await kv
              .get<ProjectProfile>(KV.profiles, profile.project)
              .catch(() => null);
            if (existing) {
              stats.skipped++;
              continue;
            }
          }
          await kv.set(KV.profiles, profile.project, profile);
        }
      }

      if (importData.actions) {
        for (const action of importData.actions) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.actions, action.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.actions, action.id, action);
        }
      }
      if (importData.actionEdges) {
        for (const edge of importData.actionEdges) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.actionEdges, edge.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.actionEdges, edge.id, edge);
        }
      }
      if (importData.routines) {
        for (const routine of importData.routines) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.routines, routine.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.routines, routine.id, routine);
        }
      }
      if (importData.signals) {
        for (const signal of importData.signals) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.signals, signal.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.signals, signal.id, signal);
        }
      }
      if (importData.checkpoints) {
        for (const checkpoint of importData.checkpoints) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.checkpoints, checkpoint.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.checkpoints, checkpoint.id, checkpoint);
        }
      }
      if (importData.sentinels) {
        for (const sentinel of importData.sentinels) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.sentinels, sentinel.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.sentinels, sentinel.id, sentinel);
        }
      }
      if (importData.sketches) {
        for (const sketch of importData.sketches) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.sketches, sketch.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.sketches, sketch.id, sketch);
        }
      }
      if (importData.crystals) {
        for (const crystal of importData.crystals) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.crystals, crystal.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.crystals, crystal.id, crystal);
        }
      }
      if (importData.facets) {
        for (const facet of importData.facets) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.facets, facet.id).catch(() => null);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.facets, facet.id, facet);
        }
      }
      if (importData.lessons) {
        for (const lesson of importData.lessons) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.lessons, lesson.id);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.lessons, lesson.id, preparedRecord(KV.lessons, lesson));
        }
      }
      if (importData.insights) {
        for (const insight of importData.insights) {
          if (strategy === "skip") {
            const existing = await kv.get(KV.insights, insight.id);
            if (existing) { stats.skipped++; continue; }
          }
          await kv.set(KV.insights, insight.id, preparedRecord(KV.insights, insight));
        }
      }
      if (normalizedAccessLogs.length) {
        const memoryIds = new Set<string>(
          importData.memories.map((m) => m.id),
        );
        for (const log of normalizedAccessLogs) {
          if (!log.memoryId || !memoryIds.has(log.memoryId)) continue;
          if (strategy === "skip") {
            const existing = await kv
              .get(KV.accessLog, log.memoryId)
              .catch(() => null);
            if (existing) {
              stats.skipped++;
              continue;
            }
          }
          await kv.set(KV.accessLog, log.memoryId, log);
        }
      }

      logger.info("Import complete", { strategy, ...stats });
      await recordAudit(kv, "import", "mem::import", [], {
        strategy,
        stats,
      });
      return { success: true, strategy, ...stats };
    }),
  );
}
