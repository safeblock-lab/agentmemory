import type { IIIClient } from "iii-sdk";
import type { CompressedObservation, Memory, Session } from "../types.js";
import { KV } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { logger } from "../logger.js";
import { countContextTokens } from "../utils/context-tokens.js";
import { getEmbeddingProvider } from "./search.js";
import { selectIndexedObservations } from "./consolidation-selection.js";

const MAX_CONTEXT_LENGTH = 4000;

function escapeXml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function normalizeProject(value: unknown): string | undefined {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : undefined;
}

function normalizeFilePath(value: string): string {
  return value.replace(/\\/g, "/").replace(/^(?:\.\/)+/, "").replace(/\/{2,}/g, "/");
}

export function registerEnrichFunction(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::enrich",
    async (data: {
      sessionId: string;
      files: string[];
      terms?: string[];
      toolName?: string;
      project?: string;
    }) => {
      const sessionId = typeof data.sessionId === "string" ? data.sessionId.trim() : "";
      const project = normalizeProject(data.project) ?? (sessionId
        ? normalizeProject((await kv.get<Session>(KV.sessions, sessionId).catch(() => null))?.project)
        : undefined);

      const parts: string[] = [];

      const fileContextPromise = sdk
        .trigger<{ sessionId: string; files: string[] }, { context: string }>({
          function_id: "mem::file-context",
          payload: {
            sessionId,
            files: data.files,
            ...(project !== undefined && { project }),
          },
        })
        .catch(() => ({ context: "" }));

      const normalizedFiles = data.files.map(normalizeFilePath);
      const searchQueries: string[] = [
        ...normalizedFiles,
        ...normalizedFiles.map((file) => file.split("/").pop() || file),
        ...(data.terms || []),
      ].filter((q) => q.length > 0);
      const searchQuery = searchQueries.join(" ").slice(0, 256);

      let partial = false;
      const scopedSearchPromise =
        !kv.indexedRetrieval && project && searchQueries.length > 0
          ? sdk
              .trigger<
                { query: string; limit: number; project?: string },
                { results: Array<{ observation: { id?: string; narrative: string }; sessionId?: string }> }
              >({
                function_id: "mem::search",
                payload: {
                  query: searchQuery,
                  limit: 12,
                  project,
                },
              })
              .catch((error) => {
                partial = true;
                logger.warn("Scoped enrichment search failed", {
                  error: error instanceof Error ? error.message : String(error),
                });
                return { results: [] };
              })
          : Promise.resolve({ results: [] });

      const bugCandidateSearchPromise = searchQueries.length > 0
        ? sdk.trigger<
            { query: string; limit: number },
            { results: Array<{ observation: { id?: string; narrative: string }; sessionId?: string }> }
          >({ function_id: "mem::search", payload: { query: searchQuery, limit: 12 } })
            .then((result) => {
              if (result.results.length >= 12) partial = true;
              return result;
            })
            .catch((error) => {
              partial = true;
              logger.warn("Bug memory candidate search failed", {
                error: error instanceof Error ? error.message : String(error),
              });
              return { results: [] };
            })
        : Promise.resolve({ results: [] });

      let indexedObservationPartial = false;
      const indexedObservationsPromise = kv.indexedRetrieval && project && searchQueries.length > 0
        ? selectIndexedObservations(kv, getEmbeddingProvider(), project, searchQuery)
            .then(async (selected) => {
              indexedObservationPartial = selected.some(({ ref }) => (ref.retrievalCoverage?.length ?? 0) > 0);
              const candidateSessions = new Map<string, Session | null>();
              await Promise.all([...new Set(selected.map(({ ref }) => ref.sessionId))].map(async (id) => {
                candidateSessions.set(id, await kv.get<Session>(KV.sessions, id).catch(() => null));
              }));
              const observations = await Promise.all(selected.map(async ({ ref }) => {
                const session = candidateSessions.get(ref.sessionId);
                if (session?.project !== project) return null;
                const row = await kv.getVersioned<CompressedObservation>(
                  KV.observations(ref.sessionId), ref.id,
                ).catch(() => null);
                const observation = row?.value;
                if (!row?.exists || !observation || row.version !== ref.rowVersion
                  || observation.id !== ref.id || observation.sessionId !== ref.sessionId) return null;
                return { observation: { id: observation.id, narrative: observation.narrative } };
              }));
              return { results: observations.filter((item): item is NonNullable<typeof item> => item !== null) };
            })
            .catch((error) => {
              indexedObservationPartial = true;
              logger.warn("Indexed enrichment observation selection failed", {
                error: error instanceof Error ? error.message : String(error),
              });
              return { results: [] as Array<{ observation: { id: string; narrative: string } }> };
            })
        : null;

      const [fileContext, scopedSearch, bugSearch, indexedObservations] = await Promise.all([
        fileContextPromise,
        scopedSearchPromise,
        bugCandidateSearchPromise,
        indexedObservationsPromise ?? Promise.resolve(null),
      ]);
      const relevantResults = indexedObservations
        ? indexedObservations.results
        : await (async () => {
            const candidates = project && !kv.indexedRetrieval ? scopedSearch.results.slice(0, 12) : [];
            const sessionIds = [...new Set(candidates
              .map((candidate) => candidate.sessionId)
              .filter((id): id is string => typeof id === "string" && id.length > 0))];
            const sessions = new Map<string, Session | null>();
            await Promise.all(sessionIds.map(async (id) => {
              sessions.set(id, await kv.get<Session>(KV.sessions, id).catch(() => null));
            }));
            return candidates.filter((candidate) =>
              !!candidate.sessionId && sessions.get(candidate.sessionId)?.project === project,
            );
          })();

      const requestedFiles = new Set(normalizedFiles);
      const candidateMemoryIds = [...new Set(bugSearch.results
        .slice(0, 12)
        .map((result) => result.observation?.id)
        .filter((id): id is string => typeof id === "string" && id.length > 0))];
      const bugMemories = (await Promise.all(candidateMemoryIds.map((id) =>
        kv.get<Memory>(KV.memories, id).catch(() => null),
      ))).filter((memory): memory is Memory => !!memory)
        .filter((memory) =>
          memory.type === "bug" &&
          memory.isLatest &&
          (!project || !normalizeProject(memory.project) || normalizeProject(memory.project) === project) &&
          Array.isArray(memory.files) && memory.files.some((file) => requestedFiles.has(normalizeFilePath(file))) &&
          typeof memory.title === "string" && typeof memory.content === "string",
        )
        .sort((a, b) =>
          new Date(b.updatedAt || b.createdAt).getTime() -
          new Date(a.updatedAt || a.createdAt).getTime(),
        );

      if (fileContext.context) {
        parts.push(fileContext.context);
      }

      if (relevantResults.length > 0) {
        const observations = relevantResults
          .map((r) => r.observation?.narrative)
          .filter(Boolean)
          .map((n) => escapeXml(n as string))
          .join("\n");
        if (observations) {
          parts.push(
            `<agentmemory-relevant-context>\n${observations}\n</agentmemory-relevant-context>`,
          );
        }
      }

      if (bugMemories.length > 0) {
        const bugs = bugMemories
          .slice(0, 3)
          .map((m) => `- ${escapeXml(m.title)}: ${escapeXml(m.content)}`)
          .join("\n");
        parts.push(
          `<agentmemory-past-errors>\n${bugs}\n</agentmemory-past-errors>`,
        );
      }

      let context = parts.join("\n\n");
      const candidateTokens = countContextTokens(context);
      let truncated = false;
      if (context.length > MAX_CONTEXT_LENGTH) {
        context = context.slice(0, MAX_CONTEXT_LENGTH);
        truncated = true;
      }

      logger.info("Enrichment completed", {
        sessionId: data.sessionId,
        project,
        fileCount: data.files.length,
        contextLength: context.length,
        truncated,
        partial: partial || indexedObservationPartial,
      });

      return { context, truncated, candidateTokens, tokens: countContextTokens(context), tokenizer: "o200k_base" };
    },
  );
}
