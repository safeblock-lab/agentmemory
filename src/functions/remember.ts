import { TriggerAction, type IIIClient } from "iii-sdk";
import type { Memory } from "../types.js";
import { KV, generateId, jaccardSimilarity } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { memoryToObservation } from "../state/memory-utils.js";
import { deleteAccessLog } from "./access-tracker.js";
import { recordAudit } from "./audit.js";
import { getSearchIndex, scheduleIndexSave, vectorIndexAddGuarded, vectorIndexRemove, flushIndexSave } from "./search.js";
import { getAgentId } from "../config.js";
import { logger } from "../logger.js";
import { createHash } from "node:crypto";
import { stableObservationJson } from "./observation-identity.js";
import { decideMemoryAdmission, sameMemoryScope, recordAdmissionSkip, type AdmissionRequest, type AttributedMemory, type MemorySourceKind } from "./memory-admission.js";

// Slicing by UTF-16 code unit can cut an astral character (emoji, some CJK
// extensions) mid surrogate pair, leaving a lone high surrogate that renders
// as a replacement glyph. Drop a dangling trailing high surrogate so the
// title stays valid.
function safeSlice(text: string, length: number): string {
  const sliced = text.slice(0, length);
  return /[\uD800-\uDBFF]$/.test(sliced) ? sliced.slice(0, -1) : sliced;
}

export function registerRememberFunction(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction("mem::remember", 
    async (data: {
      content: string;
      type?: string;
      concepts?: string[];
      files?: string[];
      ttlDays?: number;
      sourceObservationIds?: string[];
      agentId?: string;
      project?: string;
      action?: "store" | "update" | "merge";
      targetMemoryId?: string;
      expectedVersion?: number;
      sourceKind?: MemorySourceKind;
    }) => {
      if (
        !data?.content ||
        typeof data.content !== "string" ||
        !data.content.trim()
      ) {
        if (typeof data?.content === "string") await recordAdmissionSkip(kv, "empty_content", Buffer.byteLength(data.content));
        return { success: false, error: "content is required" };
      }
      for (const field of ["files", "concepts", "sourceObservationIds"] as const) {
        if (data[field] !== undefined && (!Array.isArray(data[field]) || data[field].some(value => typeof value !== "string" || !value.trim()))) {
          return { success: false, error: `${field} must be an array of non-empty strings` };
        }
      }
      if (data.action !== undefined && !["store", "update", "merge"].includes(data.action)) return { success: false, error: "Invalid admission action" };
      if (data.sourceKind !== undefined && !["unknown", "human_source", "ai_output", "proposal", "observed_result"].includes(data.sourceKind)) return { success: false, error: "Invalid source kind" };
      if (data.targetMemoryId !== undefined && (typeof data.targetMemoryId !== "string" || !data.targetMemoryId.trim()
        || !Number.isSafeInteger(data.expectedVersion) || Number(data.expectedVersion) < 1)) return { success: false, error: "Target requires expectedVersion" };
      if (data.targetMemoryId !== undefined && data.action !== "update" && data.action !== "merge") return { success: false, error: "Target requires update or merge action" };
      if (data.expectedVersion !== undefined && data.targetMemoryId === undefined) return { success: false, error: "expectedVersion requires targetMemoryId" };
      const validTypes = new Set([
        "pattern",
        "preference",
        "architecture",
        "bug",
        "workflow",
        "fact",
      ]);
      const memType = validTypes.has(data.type || "")
        ? (data.type as Memory["type"])
        : "fact";

      const now = new Date().toISOString();
      // Normalize project early so every subsequent comparison and storage
      // operation uses the same cleaned value. Raw data.project must not be
      // referenced below this point.
      const project =
        typeof data.project === "string" && data.project.trim().length > 0
          ? data.project.trim()
          : undefined;

      return withKeyedLock("mem:remember", async () => {
        const callAgentId = typeof data.agentId === "string" && data.agentId.trim()
          ? data.agentId.trim().slice(0, 128) : getAgentId();
        const request: AdmissionRequest = {
          content: data.content, type: memType, project, agentId: callAgentId,
          action: data.action, targetMemoryId: data.targetMemoryId, expectedVersion: data.expectedVersion,
          concepts: [...new Set(data.concepts ?? [])], files: [...new Set(data.files ?? [])],
          sourceObservationIds: [...new Set(data.sourceObservationIds ?? [])],
          attribution: { sourceKind: data.sourceKind ?? "unknown", evidenceObservationIds: [...new Set(data.sourceObservationIds ?? [])] },
        };
        // Exact admission must cover the corpus; a ranked top-k cannot prove absence.
        const candidates = await kv.list<AttributedMemory>(KV.memories);
        let decision: ReturnType<typeof decideMemoryAdmission>;
        try { decision = decideMemoryAdmission(request, candidates); }
        catch (error) { return { success: false, error: error instanceof Error ? error.message : "memory_admission_failed" }; }
        if (decision.action === "skip" && decision.current) {
          await recordAdmissionSkip(kv, "exact_duplicate", Buffer.byteLength(request.content));
          return { success: true, memory: decision.current, admission: { action: decision.action, reason: decision.reason }, providerCalls: 0 };
        }
        const supersededMemory = decision.action === "update" || decision.action === "merge" ? decision.current : undefined;
        const supersededId = supersededMemory?.id;
        const nearMatch = candidates.filter(existing => existing.isLatest !== false && sameMemoryScope(existing, request))
          .map(existing => ({ id: existing.id, title: existing.title, similarity: jaccardSimilarity(data.content.toLowerCase(), existing.content.toLowerCase()) }))
          .filter(match => match.similarity > 0.4).sort((left, right) => right.similarity - left.similarity || left.id.localeCompare(right.id))[0];

        const memory: AttributedMemory = {
          id: generateId("mem"),
          createdAt: now,
          updatedAt: now,
          type: memType,
          title: supersededMemory?.title ?? safeSlice(data.content, 80),
          content: data.content,
          concepts: [...new Set([...(supersededMemory?.concepts ?? []), ...request.concepts])],
          files: [...new Set([...(supersededMemory?.files ?? []), ...request.files])],
          sessionIds: supersededMemory?.sessionIds ?? [],
          strength: supersededMemory?.strength ?? 7,
          version: (supersededMemory?.version ?? 0) + 1,
          parentId: supersededId,
          supersedes: supersededId ? [supersededId, ...(supersededMemory?.supersedes ?? [])] : [],
          ...(supersededMemory?.relatedIds ? { relatedIds: supersededMemory.relatedIds } : {}),
          sourceObservationIds: [...new Set([...(supersededMemory?.sourceObservationIds ?? []), ...request.sourceObservationIds])],
          attribution: { ...request.attribution, evidenceObservationIds: [...new Set([...(supersededMemory?.attribution?.evidenceObservationIds ?? []), ...request.attribution.evidenceObservationIds])] },
          isLatest: true,
          origin: supersededMemory?.origin ?? { channel: "agent", capturedAt: now },
          ...(supersededMemory?.forgetAfter ? { forgetAfter: supersededMemory.forgetAfter } : {}),
          ...(callAgentId ? { agentId: callAgentId } : {}),
          ...(project !== undefined && { project }),
        };

        if (data.ttlDays && typeof data.ttlDays === "number" && data.ttlDays > 0) {
          memory.forgetAfter = new Date(Date.now() + data.ttlDays * 86400000).toISOString();
        }

        if (supersededMemory) {
          if (!project) return { success: false, pending: true, code: "MEMORY_CANONICAL_SCOPE_REQUIRED", error: "Versioned updates and merges require a canonical project target" };
          const { current, token } = await kv.canonicalMemory.memoryLookup(project, supersededMemory.title.toLowerCase());
          if (!current || current.id !== supersededMemory.id || current.version !== supersededMemory.version
            || stableObservationJson(current) !== stableObservationJson(supersededMemory) || !token.rowVersion) {
            return { success: false, code: "MEMORY_ADMISSION_CONFLICT", error: "Canonical predecessor changed" };
          }
          if (decision.action === "merge") memory.embeddingReuse = { sourceMemoryId: current.id, sourceVersion: token.rowVersion };
          const effectKey = createHash("sha256").update(JSON.stringify(["remember-admission-v1", supersededMemory.id, supersededMemory.version, memory])).digest("hex");
          await kv.canonicalMemory.evolve(project, supersededMemory.title.toLowerCase(), token, memory, effectKey,
            { id: supersededMemory.id, version: supersededMemory.version, rowVersion: token.rowVersion });
        } else if (project) {
          const { current, token } = await kv.canonicalMemory.memoryLookup(project, memory.title.toLowerCase());
          if (current) return { success: false, code: "MEMORY_CANONICAL_COLLISION", error: "Canonical title occupied; no fact superseded",
            retainedInput: { ...request, ttlDays: data.ttlDays }, existingMemoryId: current.id };
          const effectKey = createHash("sha256").update(JSON.stringify(["remember-create-v1", memory])).digest("hex");
          await kv.canonicalMemory.evolve(project, memory.title.toLowerCase(), token, memory, effectKey, null);
        } else {
          await kv.set(KV.memories, memory.id, memory);
        }
        if (supersededMemory) {
          // The superseded version stays in KV (the viewer's version
          // chain reads it there) but leaves both search indexes:
          // recall returning an outdated fact as if current is worse
          // than returning nothing.
          try {
            getSearchIndex().remove(supersededMemory.id);
          } catch {}
          vectorIndexRemove(supersededMemory.id);
        }
        await recordAudit(kv, "remember", "mem::remember", [memory.id], { admission: decision.action, reason: decision.reason, supersededId });

        // Without this, mem::remember persists the row but the BM25
        // index never sees it, so memory_smart_search and memory_recall
        // return empty even seconds after save (#257). Use try/catch so
        // an indexing failure doesn't block the save itself — the
        // restart-time rebuild will pick the memory up either way.
        try {
          getSearchIndex().add(memoryToObservation(memory));
          scheduleIndexSave();
        } catch (err) {
          logger.warn("Failed to index saved memory into BM25", {
            memId: memory.id,
            error: err instanceof Error ? err.message : String(err),
          });
        }
        if (decision.action !== "merge") await vectorIndexAddGuarded(
          memory.id,
          memory.sessionIds?.[0] ?? "memory",
          memory.title + " " + memory.content,
          { kind: "memory", logId: memory.id },
        );

        if (supersededId && decision.action === "update") {
          await sdk.trigger({
            function_id: "mem::cascade-update",
            payload: {
              supersededMemoryId: supersededId,
            },
            action: TriggerAction.Void(),
          });
        }

        logger.info("Memory saved", {
          memId: memory.id,
          type: memory.type,
          project: memory.project,
        });
        // similarTo is advisory only: a close-but-not-superseding match
        // the caller may want to consolidate via memory_update/forget.
        return {
          success: true,
          memory,
          admission: { action: decision.action, reason: decision.reason },
          ...(nearMatch && !supersededId
            ? {
                similarTo: {
                  ...nearMatch,
                  similarity: Math.round(nearMatch.similarity * 100) / 100,
                },
              }
            : {}),
        };
      }).catch((error: unknown) => {
        const code = error instanceof Error ? error.message.match(/STATE_[A-Z0-9_]+/)?.[0]
          ?? ("code" in error && typeof error.code === "string" ? error.code : undefined) : undefined;
        if (!code) throw error;
        logger.warn("Memory admission awaits canonical publication", { code });
        return { success: false, pending: true, code,
          error: "Canonical publication outcome requires retry with the same input",
          retainedInput: { content: data.content, type: data.type, concepts: data.concepts, files: data.files,
            sourceObservationIds: data.sourceObservationIds, sourceKind: data.sourceKind, project, agentId: data.agentId,
            action: data.action, targetMemoryId: data.targetMemoryId, expectedVersion: data.expectedVersion, ttlDays: data.ttlDays },
        };
      });
    },
  );

  sdk.registerFunction("mem::forget",
    async (data: {
      sessionId?: string;
      observationIds?: string[];
      memoryId?: string;
    }) => {
      let deleted = 0;
      const deletedMemoryIds: string[] = [];
      const deletedObservationIds: string[] = [];
      let deletedSession = false;
      const { decrementImageRef } = await import("./image-refs.js");

      if (data.memoryId) {
        const mem = await kv.get<Memory>(KV.memories, data.memoryId);
        if (mem) {
          await kv.delete(KV.memories, data.memoryId);
          if (mem.imageRef) {
            await decrementImageRef(kv, sdk, mem.imageRef);
          }
          await deleteAccessLog(kv, data.memoryId);
          getSearchIndex().remove(data.memoryId);
          vectorIndexRemove(data.memoryId);
          deletedMemoryIds.push(data.memoryId);
          deleted++;
        }
      }

      if (
        data.sessionId &&
        data.observationIds &&
        data.observationIds.length > 0
      ) {
        for (const obsId of data.observationIds) {
          const obs = await kv.get<{ imageData?: string; imageRef?: string }>(
            KV.observations(data.sessionId),
            obsId,
          );
          await kv.delete(KV.observations(data.sessionId), obsId);
          await deleteAccessLog(kv, obsId);
          if (obs?.imageData) await decrementImageRef(kv, sdk, obs.imageData);
          if (obs?.imageRef && obs.imageRef !== obs.imageData) {
            await decrementImageRef(kv, sdk, obs.imageRef);
          }
          getSearchIndex().remove(obsId);
          vectorIndexRemove(obsId);
          deletedObservationIds.push(obsId);
          deleted++;
        }
      }

      if (
        data.sessionId &&
        (!data.observationIds || data.observationIds.length === 0) &&
        !data.memoryId
      ) {
        const sessionId = data.sessionId;
        const observations = await kv.list<{ id: string; imageData?: string; imageRef?: string }>(
          KV.observations(sessionId),
        );
        for (const obs of observations) {
          await kv.delete(KV.observations(sessionId), obs.id);
          await deleteAccessLog(kv, obs.id);
          if (obs.imageData) await decrementImageRef(kv, sdk, obs.imageData);
          if (obs.imageRef && obs.imageRef !== obs.imageData) {
            await decrementImageRef(kv, sdk, obs.imageRef);
          }
          getSearchIndex().remove(obs.id);
          vectorIndexRemove(obs.id);
          deletedObservationIds.push(obs.id);
          deleted++;
        }
        await withKeyedLock(`summary-canonical:${sessionId}`, async () => {
          await kv.delete(KV.sessions, sessionId);
          await kv.delete(KV.summaries, sessionId);
        });
        deletedSession = true;
        deleted += 2;
      }

      if (deleted > 0) {
        await flushIndexSave();
        await recordAudit(
          kv,
          "forget",
          "mem::forget",
          [...deletedMemoryIds, ...deletedObservationIds],
          {
            sessionId: data.sessionId,
            deleted,
            memoriesDeleted: deletedMemoryIds.length,
            observationsDeleted: deletedObservationIds.length,
            sessionDeleted: deletedSession,
            reason: "user-initiated forget",
          },
        );
      }

      logger.info("Memory forgotten", { deleted });
      return { success: true, deleted };
    },
  );
}
