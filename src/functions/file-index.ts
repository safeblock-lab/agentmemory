import type { IIIClient } from "iii-sdk";
import type { CompressedObservation, Session } from "../types.js";
import { KV } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { recordAudit } from "./audit.js";
import { recordAccessBatch } from "./access-tracker.js";
import { logger } from "../logger.js";
import { getEmbeddingProvider } from "./search.js";
import { selectIndexedObservations } from "./consolidation-selection.js";

interface FileHistory {
  file: string;
  observations: Array<{
    sessionId: string;
    obsId: string;
    type: string;
    title: string;
    narrative: string;
    importance: number;
    timestamp: string;
  }>;
}

interface ObservationCandidate {
  id: string;
  sessionId: string;
  rowVersion?: string;
}

function normalizeProject(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function normalizeFilePath(file: string): string {
  return file.replace(/\\/g, "/").replace(/^(?:\.\/)+/, "").replace(/\/{2,}/g, "/");
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

export function registerFileIndexFunction(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::file-context", 
    async (
      data: { sessionId?: string; files?: string[]; project?: string } | undefined,
    ) => {
      const sessionId =
        data && typeof data.sessionId === "string" ? data.sessionId.trim() : "";
      const requestedProject = normalizeProject(data?.project);
      const files = Array.isArray(data?.files)
        ? data!.files
            .map((file) => (typeof file === "string" ? file.trim() : ""))
            .filter(Boolean)
        : [];
      if (files.length === 0) {
        await recordAudit(kv, "observe", "mem::file-context", [sessionId || "unknown"], {
          error: "invalid_payload",
          hasSessionId: !!sessionId,
          hasProject: !!requestedProject,
          fileCount: files.length,
        });
        return { context: "", files: [] };
      }
      let callerSession: Session | null = null;
      if (!requestedProject && sessionId) {
        callerSession = await kv.get<Session>(KV.sessions, sessionId).catch(() => null);
      }
      const project = requestedProject ?? normalizeProject(callerSession?.project);
      if (!project) return { context: "" };

      const query = files.map(normalizeFilePath).join(" ").slice(0, 256);
      let candidates: ObservationCandidate[];
      if (kv.indexedRetrieval) {
        const selected = await selectIndexedObservations(kv, getEmbeddingProvider(), project, query);
        candidates = selected.map(({ ref }) => ({ id: ref.id, sessionId: ref.sessionId, rowVersion: ref.rowVersion }));
      } else {
        const search = await sdk.trigger<
          { query: string; limit: number; project: string },
          { results: Array<{ observation?: { id?: string }; sessionId: string }> }
        >({ function_id: "mem::search", payload: { query, limit: 12, project } });
        candidates = search.results
          .slice(0, 12)
          .map((result) => ({ id: result.observation?.id ?? "", sessionId: result.sessionId }))
          .filter((candidate) => candidate.id.length > 0 && candidate.sessionId.length > 0);
      }

      const uniqueCandidates = [...new Map(candidates
        .filter((candidate) => candidate.sessionId !== sessionId)
        .map((candidate) => [`${candidate.sessionId}\0${candidate.id}`, candidate])).values()].slice(0, 12);
      const candidateSessions = new Map<string, Session | null>();
      await Promise.all([...new Set(uniqueCandidates.map((candidate) => candidate.sessionId))].map(async (id) => {
        const session = await kv.get<Session>(KV.sessions, id).catch(() => null);
        candidateSessions.set(id, session);
      }));

      const requestedPaths = new Set(files.map(normalizeFilePath));
      const observations = await Promise.all(uniqueCandidates.map(async (candidate) => {
        const session = candidateSessions.get(candidate.sessionId);
        if (!session || session.id !== candidate.sessionId || session.project !== project) return null;
        const row = await kv.getVersioned<CompressedObservation>(KV.observations(candidate.sessionId), candidate.id).catch(() => null);
        const observation = row?.value;
        if (!row?.exists || !observation || (candidate.rowVersion !== undefined && row.version !== candidate.rowVersion)
          || observation.id !== candidate.id || observation.sessionId !== candidate.sessionId
          || !Array.isArray(observation.files) || !observation.title || typeof observation.narrative !== "string"
          || observation.importance < 4 || !observation.files.some((file) => requestedPaths.has(normalizeFilePath(file)))) {
          return null;
        }
        return observation;
      }));

      const results: FileHistory[] = [];
      for (const file of files) {
        const normalizedFile = normalizeFilePath(file);
        const matches = observations
          .filter((observation): observation is CompressedObservation =>
            !!observation && observation.files.some((candidate) => normalizeFilePath(candidate) === normalizedFile),
          )
          .sort((a, b) => b.importance - a.importance)
          .slice(0, 5);
        if (matches.length > 0) {
          results.push({
            file,
            observations: matches.map((observation) => ({
              sessionId: observation.sessionId,
              obsId: observation.id,
              type: observation.type,
              title: observation.title,
              narrative: observation.narrative,
              importance: observation.importance,
              timestamp: observation.timestamp,
            })),
          });
        }
      }

      if (results.length === 0) {
        return { context: "" };
      }

      const lines: string[] = ["<agentmemory-file-context>"];
      for (const fh of results) {
        lines.push(`## ${escapeXml(fh.file)}`);
        for (const obs of fh.observations) {
          lines.push(`- [${escapeXml(obs.type)}] ${escapeXml(obs.title)}: ${escapeXml(obs.narrative)}`);
        }
      }
      lines.push("</agentmemory-file-context>");

      const accessedIds: string[] = [];
      for (const fh of results) for (const obs of fh.observations) accessedIds.push(obs.obsId);
      void recordAccessBatch(kv, accessedIds);

      const context = lines.join("\n");
      logger.info("File context generated", {
        files: files.length,
        results: results.length,
      });
      return { context };
    },
  );
}
