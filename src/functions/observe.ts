import { TriggerAction, type IIIClient } from "iii-sdk";
import { publishLiveObservation } from "./live-observation.js";
import type { RawObservation, HookPayload, Origin } from "../types.js";

const TOOL_HOOKS = new Set(["pre_tool_use", "post_tool_use", "post_tool_failure"]);
import { KV, generateId } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { stripPrivateData } from "./privacy.js";
import { DedupMap } from "./dedup.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import {
  getAgentId,
  isAutoCompressEnabled,
  isTypeSafeFeatureEnabled,
  TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD,
  TYPESAFE_SCORING_CONFIDENCE_THRESHOLD,
} from "../config.js";
import { getSearchIndex, scheduleIndexSave, vectorIndexAddGuarded } from "./search.js";
import { logger } from "../logger.js";
import { recordContextUsage, warnContextUsageFailure } from "./context-usage.js";
import { saveImageToDisk } from "../utils/image-store.js";
import type { TypeSafeDecisionProvider, TypeSafeQuestion } from "../providers/typesafe.js";
import { admitObservation, buildStoredSynthetic, observationIdentity, ObservationAdmissionError, captureTranscriptSnapshot } from "./observation-identity.js";

const READ_ONLY_TOOL_TOKENS = new Set([
  "read",
  "grep",
  "glob",
  "ls",
  "find",
  "search",
  "query",
  "get",
  "list",
  "view",
  "open",
  "inspect",
  "fetch",
]);
const MUTATING_TOOL_TOKENS = new Set([
  "write",
  "edit",
  "patch",
  "delete",
  "remove",
  "move",
  "rename",
  "create",
  "update",
  "set",
  "append",
  "replace",
  "install",
  "deploy",
  "commit",
  "push",
  "reset",
  "exec",
  "execute",
  "command",
  "shell",
  "bash",
  "powershell",
]);
const PROTECTED_OBSERVATION_SIGNAL = /\b(?:error|failed|failure|warning|decision|instruction|prompt|security|secret|token|password|credential|api[-_ ]?key|auth|permission|mutat(?:e|ion)|write|edit|patch|delete|remove|move|rename|deploy|install|commit|push|reset|sudo|shell|terminal|bash|powershell|environment|env|(?:AGENTS|CLAUDE|GEMINI|COPILOT)\.md)\b/i;
const IMPORTANCE_LEVELS = [
  "Routine, reproducible detail with little future value",
  "Low-value detail useful only as immediate context",
  "Minor project context or ordinary lookup result",
  "Some reusable detail, but limited durability",
  "Moderately useful project information",
  "Useful detail likely to help a later task",
  "Important project context or a recurring pattern",
  "High-value decision, fix, or technical discovery",
  "Very important durable project knowledge",
  "Critical instruction, security, or project constraint",
];

function compactObservationText(value: unknown, limit: number): string {
  let text: string;
  if (typeof value === "string") {
    text = value;
  } else if (value === undefined || value === null) {
    return "";
  } else {
    try {
      text = JSON.stringify(value) ?? "";
    } catch {
      return "";
    }
  }
  return text.replace(/\s+/g, " ").trim().slice(0, limit);
}

export function isTypeSafeObservationTool(toolName: string | undefined): boolean {
  if (!toolName) return false;
  const normalized = toolName
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase();
  const tokens = normalized.split(/[^a-z0-9]+/).filter(Boolean);
  if (tokens.some((token) => MUTATING_TOOL_TOKENS.has(token))) return false;
  return tokens.some((token) => READ_ONLY_TOOL_TOKENS.has(token));
}

function shouldProtectObservation(raw: RawObservation, imageData?: string): boolean {
  if (raw.hookType !== "post_tool_use" || imageData || raw.modality === "image" || raw.modality === "mixed") {
    return true;
  }
  if (!isTypeSafeObservationTool(raw.toolName)) return true;
  if ((JSON.stringify(raw.raw)?.length ?? 0) > 448) return true;
  const preview = compactObservationText(raw.raw, 448);
  return PROTECTED_OBSERVATION_SIGNAL.test(preview) || stripPrivateData(preview) !== preview;
}

export function extractImage(d: unknown): string | undefined {
  if (!d) return undefined;
  if (typeof d === "string") {
    if (d.startsWith("data:image/") || d.startsWith("iVBORw0KGgo") || d.startsWith("/9j/")) {
      return d;
    }
    return undefined;
  }
  if (typeof d === "object" && d !== null) {
    const obj = d as Record<string, unknown>;
    if (typeof obj["image_data"] === "string") return obj["image_data"];
    if (typeof obj["image_path"] === "string") return obj["image_path"];
    if (typeof obj["imageBase64"] === "string") return obj["imageBase64"];
    if (typeof obj["imagePath"] === "string") return obj["imagePath"];

    for (const key of Object.keys(obj)) {
      const match = extractImage(obj[key]);
      if (match) return match;
    }
  }
  return undefined;
}

export function registerObserveFunction(
  sdk: IIIClient,
  kv: StateKV,
  _dedupMap?: DedupMap,
  maxObservationsPerSession?: number,
  typeSafe?: TypeSafeDecisionProvider,
): void {
  sdk.registerFunction("mem::observe", 
    async (payload: HookPayload) => {

      if (
        !payload?.sessionId ||
        typeof payload.sessionId !== "string" ||
        !payload.hookType ||
        typeof payload.hookType !== "string" ||
        !payload.timestamp ||
        typeof payload.timestamp !== "string" ||
        !Number.isFinite(Date.parse(payload.timestamp))
      ) {
        return {
          success: false,
          error:
            "Invalid payload: sessionId, hookType, and timestamp are required",
        };
      }

      const telemetryData = payload.data && typeof payload.data === "object" && !Array.isArray(payload.data)
        ? payload.data as Record<string, unknown> : null;
      if (payload.hookType === "prompt_submit" && telemetryData?.event === "transcript_capture_v1") {
        return captureTranscriptSnapshot(sdk, kv, payload);
      }
      if (payload.hookType === "notification" && telemetryData?.event === "context_emitted_v1") {
        try {
          await recordContextUsage(kv, payload.sessionId, telemetryData);
          return { success: true, telemetry: true };
        } catch {
          warnContextUsageFailure();
          return { success: false, error: "Context emission telemetry rejected" };
        }
      }

      const obsId = generateId("obs");
      let typeSafeImportance: number | undefined;

      let serializedData: string;
      try {
        serializedData = JSON.stringify(payload.data) ?? JSON.stringify(String(payload.data));
      } catch {
        return { success: false, error: "Invalid payload: data must be JSON serializable" };
      }
      const sanitized = stripPrivateData(serializedData);
      let sanitizedRaw: unknown;
      try {
        sanitizedRaw = JSON.parse(sanitized);
      } catch {
        sanitizedRaw = sanitized;
      }
      const identity = observationIdentity(payload, sanitizedRaw, { toolInputIdentity: true });

      let originChannel: Origin["channel"] = "agent";
      if (payload.hookType === "prompt_submit") originChannel = "user";
      else if (TOOL_HOOKS.has(payload.hookType)) originChannel = "tool";
      const raw: RawObservation = {
        id: obsId,
        sessionId: payload.sessionId,
        timestamp: payload.timestamp,
        hookType: payload.hookType,
        raw: sanitizedRaw,
        identityDigest: identity.identityDigest,
        origin: {
          channel: originChannel,
          capturedAt: payload.timestamp,
        },
      };

      let extractedImage: string | undefined;

      if (typeof sanitizedRaw === "object" && sanitizedRaw !== null) {
        const d = sanitizedRaw as Record<string, unknown>;
        if (
          payload.hookType === "post_tool_use" ||
          payload.hookType === "post_tool_failure"
        ) {
          raw.toolName = d["tool_name"] as string | undefined;
          raw.toolInput = d["tool_input"];
          raw.toolOutput = d["tool_output"] ?? d["error"];
          if (raw.origin && raw.toolName) raw.origin.detail = raw.toolName;
        }
        if (payload.hookType === "prompt_submit") {
          raw.userPrompt = d["prompt"] as string | undefined;
        }

        extractedImage = extractImage(sanitizedRaw);
        if (extractedImage) {
          raw.modality = (raw.toolInput || raw.toolOutput || raw.userPrompt) ? "mixed" : "image";
        }
      } else if (typeof sanitizedRaw === "string") {
        extractedImage = extractImage(sanitizedRaw);
        if (extractedImage) {
          raw.modality = "image";
        }
      }

      const pendingImageData = extractedImage;

      return withKeyedLock(`obs:${payload.sessionId}`, async () => {
        const readiness = await kv.get<{ version?: number; ready?: boolean }>(`mem:obs-identity:${payload.sessionId}`, "__ready__");
        if (readiness?.version !== 1 || readiness.ready !== true) {
          for await (const page of kv.pages<{ id: string }>(`mem:obs:${payload.sessionId}`, { limit: 1, maxBytes: 4096, fields: ["id"] })) {
            if (page.items.length) {
              return { success: false, pending: true, code: "STATE_OBSERVATION_INDEX_PENDING", sessionId: payload.sessionId };
            }
            break;
          }
        }
        const admissionEnabled = isTypeSafeFeatureEnabled("admission");
        const scoringEnabled = isTypeSafeFeatureEnabled("scoring");
        const retainedIdentity = typeSafe && (admissionEnabled || scoringEnabled)
          ? await kv.get(`mem:obs-identity:${payload.sessionId}`, identity.identityDigest) : null;
        if (
          typeSafe &&
          !retainedIdentity &&
          !shouldProtectObservation(raw, pendingImageData) &&
          (admissionEnabled || scoringEnabled)
        ) {
          const preview = stripPrivateData(compactObservationText(raw.raw, 448));
          const state = { preview };
          const questions: Record<string, TypeSafeQuestion> = {};
          if (admissionEnabled) {
            questions.admission = {
              type: "choice",
              instructions: "Should this non-mutating tool observation be retained for future agent work?",
              criteria: {
                keep: "Retain this observation because it contains useful, durable project context.",
                discard: "Discard only if this is clearly routine, reproducible, low-value output.",
              },
            };
          }
          if (scoringEnabled) {
            questions.importance = {
              type: "score",
              instructions: "Score this non-mutating observation's durable importance to future work.",
              criteria: IMPORTANCE_LEVELS,
            };
          }
          try {
            const answers = await typeSafe.evaluate(
              admissionEnabled ? "admission" : "scoring",
              state,
              questions,
            );
            const admission = answers?.admission;
            if (
              admissionEnabled &&
              admission?.type === "choice" &&
              admission.choice === "discard" &&
              admission.confidence >= TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD
            ) {
              return {
                success: true,
                skipped: true,
                reason: "TypeSafe admission",
                sessionId: payload.sessionId,
              };
            }
            const importance = answers?.importance;
            if (
              scoringEnabled &&
              importance?.type === "score" &&
              importance.confidence >= TYPESAFE_SCORING_CONFIDENCE_THRESHOLD &&
              Number.isFinite(importance.score)
            ) {
              typeSafeImportance = Math.max(1, Math.min(10, Math.round(importance.score) + 1));
            }
          } catch (error) {
            logger.warn("TypeSafe observation admission failed open", {
              errorType: error instanceof Error ? error.name : "unknown",
            });
          }
        }

        // Existing session is the source of truth for agentId (even
        // undefined). Env AGENT_ID only fires when no session row
        // exists yet — otherwise an unscoped session would get
        // retroactively scoped by a later AGENT_ID export.
        const existingSession = await kv.get<{
          agentId?: string;
        }>(KV.sessions, payload.sessionId);
        const inheritedAgentId = existingSession
          ? existingSession.agentId
          : getAgentId();
        if (inheritedAgentId) {
          raw.agentId = inheritedAgentId;
        }

        if (pendingImageData && (pendingImageData.startsWith("data:image/") || pendingImageData.startsWith("iVBORw0KGgo") || pendingImageData.startsWith("/9j/"))) {
          const { filePath, bytesWritten } = await saveImageToDisk(pendingImageData);
          raw.imageData = filePath;
          const { incrementImageRef } = await import("./image-refs.js");
          await incrementImageRef(kv, filePath);
          sdk.trigger({
            function_id: "mem::disk-size-delta",
            payload: { deltaBytes: bytesWritten },
            action: TriggerAction.Void(),
          });
        }

        const autoCompress = isAutoCompressEnabled();
        const synthetic = buildStoredSynthetic(raw);
        if (typeSafeImportance !== undefined) synthetic.importance = typeSafeImportance;
        const trimmedPrompt = raw.userPrompt?.replace(/\s+/g, " ").trim().slice(0, 200);
        const sessionCandidate = typeof payload.project === "string" && payload.project.trim()
          && typeof payload.cwd === "string" && payload.cwd.trim() ? {
            id: payload.sessionId, project: payload.project, cwd: payload.cwd,
            startedAt: payload.timestamp, updatedAt: payload.timestamp,
            status: "active", observationCount: 0,
            ...(inheritedAgentId ? { agentId: inheritedAgentId } : {}),
            ...(trimmedPrompt ? { firstPrompt: trimmedPrompt } : {}),
          } : undefined;
        let admission;
        try {
          admission = await admitObservation(sdk, identity, autoCompress ? raw : { ...synthetic, ...raw }, raw, sessionCandidate, maxObservationsPerSession);
        } catch (error) {
          if (raw.imageData && error instanceof ObservationAdmissionError && error.releaseImage) {
            // Roll back the ref taken above. decrementImageRef deletes the file
            // only when no other observation still references it (deduped images
            // survive) and emits the disk-size delta itself — deleting the file
            // directly here would orphan shared images and leave a stale ref.
            // If the rollback itself fails, log it but still surface the
            // original write error (the more useful failure to diagnose).
            try {
              const { decrementImageRef } = await import("./image-refs.js");
              await decrementImageRef(kv, sdk, raw.imageData);
            } catch (rollbackError) {
              logger.error("Failed to roll back image ref after observation write failure", {
                imageRef: raw.imageData,
                error: rollbackError instanceof Error ? rollbackError.message : String(rollbackError),
              });
            }
          }
          if (error instanceof Error && error.message.includes("STATE_OBSERVATION_LIMIT")) {
            return { success: false, code: "SESSION_OBSERVATION_LIMIT", error: `Session observation limit reached (${maxObservationsPerSession})` };
          }
          if (error instanceof Error && error.message.includes("STATE_OBSERVATION_INDEX_PENDING")) {
            return { success: false, pending: true, code: "STATE_OBSERVATION_INDEX_PENDING", sessionId: payload.sessionId };
          }
          throw error;
        }

        if (!admission.created && admission.observation_id !== obsId) {
          if (raw.imageData) {
            const { decrementImageRef } = await import("./image-refs.js");
            await decrementImageRef(kv, sdk, raw.imageData);
          }
          return {
            observationId: admission.observation_id, deduplicated: true,
            duplicateEvent: admission.duplicate_event, sessionId: payload.sessionId,
            occurrenceCount: admission.occurrence_count,
          };
        }
        if (raw.imageData && process.env["AGENTMEMORY_IMAGE_EMBEDDINGS"] === "true") {
          await sdk.trigger({
            function_id: "mem::vision-embed",
            payload: { imageRef: raw.imageData, sessionId: payload.sessionId, observationId: obsId },
            action: TriggerAction.Void(),
          });
        }

        await publishLiveObservation(sdk, "raw", admission.observation);

        // Synthetic search/display projections never replace the complete source.
        if (autoCompress) {
          await sdk.trigger({
            function_id: "mem::compress",
            payload: {
              observationId: obsId,
              sessionId: payload.sessionId,
              raw,
              ...(typeSafeImportance !== undefined
                ? { importanceOverride: typeSafeImportance }
                : {}),
            },
            action: TriggerAction.Void(),
          });
        } else {
          const stored = { ...admission.observation, ...synthetic };
          getSearchIndex().add(stored);
          scheduleIndexSave();
          await vectorIndexAddGuarded(
            stored.id,
            stored.sessionId,
            stored.title + " " + (stored.narrative || ""),
            { kind: "synthetic", logId: stored.id },
          );
          await publishLiveObservation(sdk, "compressed", stored);
        }

        logger.info("Observation captured", {
          obsId,
          sessionId: payload.sessionId,
          hook: payload.hookType,
          compress: isAutoCompressEnabled() ? "llm" : "synthetic",
        });
        return { observationId: obsId };
      });
    },
  );
}
