import { TriggerAction, type ISdk } from "iii-sdk";
import type { RawObservation, HookPayload } from "../types.js";
import { KV, STREAM, generateId } from "../state/schema.js";
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
import { buildSyntheticCompression } from "./compress-synthetic.js";
import { getSearchIndex, vectorIndexAddGuarded } from "./search.js";
import { logger } from "../logger.js";
import { saveImageToDisk } from "../utils/image-store.js";
import type { TypeSafeDecisionProvider, TypeSafeQuestion } from "../providers/typesafe.js";

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
  const preview = `${compactObservationText(raw.toolInput, 256)} ${compactObservationText(raw.toolOutput, 256)}`;
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
  sdk: ISdk,
  kv: StateKV,
  dedupMap?: DedupMap,
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
        typeof payload.timestamp !== "string"
      ) {
        return {
          success: false,
          error:
            "Invalid payload: sessionId, hookType, and timestamp are required",
        };
      }

      const obsId = generateId("obs");
      let typeSafeImportance: number | undefined;

      let dedupHash: string | undefined;
      if (dedupMap) {
        const d =
          typeof payload.data === "object" && payload.data !== null
            ? (payload.data as Record<string, unknown>)
            : {};
        const toolName = (d["tool_name"] as string) || payload.hookType;
        dedupHash = dedupMap.computeHash(
          payload.sessionId,
          toolName,
          d["tool_input"],
        );
        if (dedupMap.isDuplicate(dedupHash)) {
          return { deduplicated: true, sessionId: payload.sessionId };
        }
      }

      let sanitizedRaw: unknown = payload.data;
      try {
        const jsonStr = JSON.stringify(payload.data);
        const sanitized = stripPrivateData(jsonStr);
        sanitizedRaw = JSON.parse(sanitized);
      } catch {
        sanitizedRaw = stripPrivateData(String(payload.data));
      }

      const raw: RawObservation = {
        id: obsId,
        sessionId: payload.sessionId,
        timestamp: payload.timestamp,
        hookType: payload.hookType,
        raw: sanitizedRaw,
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
          raw.toolOutput = d["tool_output"] || d["error"];
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
        if (maxObservationsPerSession && maxObservationsPerSession > 0) {
          const existing = await kv.list(KV.observations(payload.sessionId));
          if (existing.length >= maxObservationsPerSession) {
            return {
              success: false,
              error: `Session observation limit reached (${maxObservationsPerSession})`,
            };
          }
        }

        const admissionEnabled = isTypeSafeFeatureEnabled("admission");
        const scoringEnabled = isTypeSafeFeatureEnabled("scoring");
        if (
          typeSafe &&
          !shouldProtectObservation(raw, pendingImageData) &&
          (admissionEnabled || scoringEnabled)
        ) {
          const inputPreview = compactObservationText(raw.toolInput, 192);
          const outputPreview = compactObservationText(raw.toolOutput, 320);
          const preview = stripPrivateData(
            `${inputPreview}${inputPreview && outputPreview ? " | " : ""}${outputPreview}`,
          ).slice(0, 448);
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
              if (dedupMap && dedupHash) dedupMap.record(dedupHash);
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
          observationCount?: number;
          firstPrompt?: string;
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
          if (process.env["AGENTMEMORY_IMAGE_EMBEDDINGS"] === "true") {
            sdk.trigger({
              function_id: "mem::vision-embed",
              payload: {
                imageRef: filePath,
                sessionId: payload.sessionId,
                observationId: obsId,
              },
              action: TriggerAction.Void(),
            });
          }
        }

        try {

          await kv.set(KV.observations(payload.sessionId), obsId, raw);

        } catch (error) {
          if (raw.imageData) {
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
          throw error;
        }

        if (dedupMap && dedupHash) {
          dedupMap.record(dedupHash);
        }

        await sdk.trigger({
          function_id: "stream::set",
          payload: {
          stream_name: STREAM.name,
          group_id: STREAM.group(payload.sessionId),
          item_id: obsId,
          data: { type: "raw", observation: raw },
          },
        });

        await sdk.trigger({
          function_id: "stream::send",
          payload: {
            stream_name: STREAM.name,
            group_id: STREAM.viewerGroup,
            id: `raw-${obsId}`,
            type: "raw_observation",
            data: { type: "raw", observation: raw, sessionId: payload.sessionId },
          },
          action: TriggerAction.Void(),
        });

        const session = existingSession;
        if (session) {
          const updates: Array<{ type: "set"; path: string; value: unknown }> = [
            { type: "set", path: "updatedAt", value: new Date().toISOString() },
            {
              type: "set",
              path: "observationCount",
              value: (session.observationCount || 0) + 1,
            },
          ];
          if (!session.firstPrompt && typeof raw.userPrompt === "string") {
            const trimmed = raw.userPrompt.replace(/\s+/g, " ").trim();
            if (trimmed.length > 0) {
              updates.push({
                type: "set",
                path: "firstPrompt",
                value: trimmed.slice(0, 200),
              });
            }
          }
          await kv.update(KV.sessions, payload.sessionId, updates);
        } else if (
          typeof payload.project === "string" &&
          payload.project.trim().length > 0 &&
          typeof payload.cwd === "string" &&
          payload.cwd.trim().length > 0
        ) {
          // OpenCode (and any plugin that skips POST /session/start)
          // can fire observations before the session record exists. Without
          // an implicit create, those observations stack up but
          // `memory_sessions` never lists them, and summarize bails with
          // "Session not found for summarize". Create the session now from
          // the observation payload — but only when project + cwd are
          // present (HookPayload contract). Older test payloads without
          // those fields keep their original no-op behaviour.
          const trimmedPrompt =
            typeof raw.userPrompt === "string"
              ? raw.userPrompt.replace(/\s+/g, " ").trim().slice(0, 200)
              : undefined;
          const ts = new Date().toISOString();
          await kv.set(KV.sessions, payload.sessionId, {
            id: payload.sessionId,
            project: payload.project,
            cwd: payload.cwd,
            startedAt: payload.timestamp ?? ts,
            updatedAt: ts,
            status: "active",
            observationCount: 1,
            ...(inheritedAgentId ? { agentId: inheritedAgentId } : {}),
            ...(trimmedPrompt && trimmedPrompt.length > 0
              ? { firstPrompt: trimmedPrompt }
              : {}),
          });
        }

        // Per-observation LLM compression is opt-in as of 0.8.8.
        // Default path: build a zero-LLM synthetic compression so recall
        // and BM25 search still work without burning the user's Claude
        // token allocation on every tool invocation.
        if (isAutoCompressEnabled()) {
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
          const synthetic = buildSyntheticCompression(raw);
          if (typeSafeImportance !== undefined) {
            synthetic.importance = typeSafeImportance;
          }
          await kv.set(
            KV.observations(payload.sessionId),
            obsId,
            synthetic,
          );
          getSearchIndex().add(synthetic);
          await vectorIndexAddGuarded(
            synthetic.id,
            synthetic.sessionId,
            synthetic.title + " " + (synthetic.narrative || ""),
            { kind: "synthetic", logId: synthetic.id },
          );
          await sdk.trigger({
            function_id: "stream::set",
            payload: {
              stream_name: STREAM.name,
              group_id: STREAM.group(payload.sessionId),
              item_id: obsId,
              data: { type: "compressed", observation: synthetic },
            },
          });
          await sdk.trigger({
            function_id: "stream::set",
            payload: {
              stream_name: STREAM.name,
              group_id: STREAM.viewerGroup,
              item_id: obsId,
              data: {
                type: "compressed",
                observation: synthetic,
                sessionId: payload.sessionId,
              },
            },
          });
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
