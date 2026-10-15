import type { IIIClient } from "iii-sdk";
import type {
  ProjectProfile,
} from "../types.js";
import { KV } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { recordAudit } from "./audit.js";
import { logger } from "../logger.js";
import { viewerProjectSessions, viewerPage, VIEWER_OBSERVATION_FIELDS } from "./viewer-memory-page.js";

type BoundedProfile = ProjectProfile & { partial: boolean; nextCursor: string | null; boundedVersion: 1; sourceRevision: number; sourceEpoch: number; countScope: "bounded_sources" };

export function registerProfileFunction(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::profile", 
    async (data: { project: string; refresh?: boolean } | undefined) => {
      if (!data || typeof data.project !== "string" || !data.project.trim()) {
        return { success: false, error: "project is required" };
      }
      const project = data.project.trim();

      const sourcePage = await viewerProjectSessions(kv, project);
      if (sourcePage.status === "not_ready") return { profile: null, partial: true, reason: "project_sessions_not_ready" };
      if (!data.refresh) {
        const cached = await kv
          .get<BoundedProfile>(KV.profiles, project);
        if (cached?.project === project && cached.boundedVersion === 1 && cached.sourceRevision === sourcePage.revision && cached.sourceEpoch === sourcePage.epoch) {
          const age = Date.now() - new Date(cached.updatedAt).getTime();
          if (age < 3600_000) {
            return { profile: cached, cached: true, partial: cached.partial };
          }
        }
      }

      const projectSessions = sourcePage.items.map(item => item.session);

      if (projectSessions.length === 0) {
        return { profile: null, partial: sourcePage.partial, reason: sourcePage.partial ? "project_sessions_incomplete" : "no_sessions" };
      }

      const conceptFreq = new Map<string, number>();
      const fileFreq = new Map<string, number>();
      const errors: string[] = [];
      const recentActivity: string[] = [];
      let totalObs = 0;

      const sortedSessions = projectSessions.sort(
        (a, b) =>
          new Date(b.startedAt ?? "").getTime() - new Date(a.startedAt ?? "").getTime(),
      );

      const top20Sessions = sortedSessions.slice(0, 20);
      const obsPerSession: Array<Array<{ title: string; type: string; importance: number; concepts: string[]; files: string[] }>> = [];
      let partial = sourcePage.partial || sourcePage.next_cursor !== null;
      for (const session of top20Sessions) {
        const page = await viewerPage(kv, KV.observations(session.id), {}, VIEWER_OBSERVATION_FIELDS);
        if (page.items.some(obs => obs.sessionId !== session.id)) throw new Error("profile_observation_session_mismatch");
        obsPerSession.push(page.items.map(obs => {
          const concepts = obs.concepts ?? [], files = obs.files ?? [];
          if (!Array.isArray(concepts) || concepts.some(value => typeof value !== "string")
            || !Array.isArray(files) || files.some(value => typeof value !== "string")) throw new Error("profile_observation_invalid");
          return { title: typeof obs.title === "string" ? obs.title : "", type: typeof obs.type === "string" ? obs.type : "",
            importance: typeof obs.importance === "number" ? obs.importance : 0,
            concepts: concepts.filter((value): value is string => typeof value === "string"),
            files: files.filter((value): value is string => typeof value === "string") };
        }));
        partial ||= page.partial;
      }

      for (let i = 0; i < top20Sessions.length; i++) {
        const session = top20Sessions[i];
        const observations = obsPerSession[i];
        totalObs += observations.length;

        for (const obs of observations) {
          for (const concept of obs.concepts || []) {
            conceptFreq.set(concept, (conceptFreq.get(concept) || 0) + 1);
          }
          for (const file of obs.files || []) {
            fileFreq.set(file, (fileFreq.get(file) || 0) + 1);
          }
          if (obs.type === "error") {
            errors.push(obs.title);
          }
        }

        const important = observations
          .filter((o) => o.importance >= 7)
          .sort((a, b) => b.importance - a.importance);
        if (important.length > 0) {
          recentActivity.push(
            `[${(session.startedAt ?? "").slice(0, 10)}] ${important[0].title}`,
          );
        }
      }

      const topConcepts = Array.from(conceptFreq.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15)
        .map(([concept, frequency]) => ({ concept, frequency }));

      const topFiles = Array.from(fileFreq.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15)
        .map(([file, frequency]) => ({ file, frequency }));

      const uniqueErrors = [...new Set(errors)].slice(0, 10);

      const profile: BoundedProfile = {
        project,
        updatedAt: new Date().toISOString(),
        topConcepts,
        topFiles,
        conventions: extractConventions(topConcepts, topFiles),
        commonErrors: uniqueErrors,
        recentActivity: recentActivity.slice(0, 10),
        sessionCount: projectSessions.length,
        totalObservations: totalObs,
        partial, nextCursor: sourcePage.next_cursor, boundedVersion: 1,
        sourceRevision: sourcePage.revision, sourceEpoch: sourcePage.epoch, countScope: "bounded_sources",
      };

      await kv.set(KV.profiles, project, profile);
      await recordAudit(kv, "share", "mem::profile", [project], {
        sessionCount: projectSessions.length,
        totalObservations: totalObs,
      });

      logger.info("Profile generated", {
        project,
        sessions: projectSessions.length,
        observations: totalObs,
      });
      return { profile, cached: false, partial };
    },
  );
}

function extractConventions(
  concepts: Array<{ concept: string; frequency: number }>,
  files: Array<{ file: string; frequency: number }>,
): string[] {
  const conventions: string[] = [];

  const tsFiles = files.filter((f) => f.file.endsWith(".ts")).length;
  const jsFiles = files.filter((f) => f.file.endsWith(".js")).length;
  if (tsFiles > jsFiles && tsFiles > 0) {
    conventions.push("TypeScript project");
  }

  const srcFiles = files.filter((f) => f.file.includes("/src/")).length;
  if (srcFiles > files.length * 0.5) {
    conventions.push("Standard src/ directory structure");
  }

  const testFiles = files.filter(
    (f) => f.file.includes("test") || f.file.includes("spec"),
  ).length;
  if (testFiles > 0) {
    conventions.push("Has test files");
  }

  for (const { concept, frequency } of concepts.slice(0, 5)) {
    if (frequency >= 3) {
      conventions.push(`Frequently uses: ${concept}`);
    }
  }

  return conventions;
}
