import type { IIIClient } from "iii-sdk";
import { readFileSync } from "node:fs";
import { isManagedImagePath } from "../utils/image-store.js";
import type {
  RawObservation,
  CompressedObservation,
  ObservationType,
  MemoryProvider,
} from "../types.js";
import { publishLiveObservation } from "./live-observation.js";
import { StateKV } from "../state/kv.js";
import {
  COMPRESSION_SYSTEM,
  buildCompressionPrompt,
} from "../prompts/compression.js";
import { VISION_DESCRIPTION_PROMPT } from "../prompts/vision.js";
import { getXmlTag, getXmlChildren } from "../prompts/xml.js";
import { getSearchIndex, scheduleIndexSave, vectorIndexAddGuarded } from "./search.js";
import { CompressOutputSchema } from "../eval/schemas.js";
import { validateOutput } from "../eval/validator.js";
import { scoreCompression } from "../eval/quality.js";
import { compressWithRetry } from "../eval/self-correct.js";
import { KV } from "../state/schema.js";
import type { MetricsStore } from "../eval/metrics-store.js";
import { logger } from "../logger.js";
import { recordOperation } from "./dashboard-activity.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { replaceObservation } from "./observation-identity.js";

const VALID_TYPES = new Set<string>([
  "file_read",
  "file_write",
  "file_edit",
  "command_run",
  "search",
  "web_fetch",
  "conversation",
  "error",
  "decision",
  "discovery",
  "subagent",
  "notification",
  "task",
  "image",
  "other",
]);

function parseCompressionXml(
  xml: string,
): Omit<CompressedObservation, "id" | "sessionId" | "timestamp"> | null {
  const rawType = getXmlTag(xml, "type");
  const title = getXmlTag(xml, "title");
  if (!rawType || !title) return null;
  const type = VALID_TYPES.has(rawType) ? rawType : "other";

  return {
    type: type as ObservationType,
    title,
    subtitle: getXmlTag(xml, "subtitle") || undefined,
    facts: getXmlChildren(xml, "facts", "fact"),
    narrative: getXmlTag(xml, "narrative"),
    concepts: getXmlChildren(xml, "concepts", "concept"),
    files: getXmlChildren(xml, "files", "file"),
    importance: Math.max(
      1,
      Math.min(10, parseInt(getXmlTag(xml, "importance") || "5", 10) || 5),
    ),
  };
}

export function registerCompressFunction(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  metricsStore?: MetricsStore,
  llmRouter?: LlmTaskRouter,
): void {
  sdk.registerFunction("mem::compress", 
    async (data: {
      observationId: string;
      sessionId: string;
      raw: RawObservation;
      importanceOverride?: number;
      selected?: { project: string; sessionId: string; sourceFingerprint: string };
    }) => {
      const startMs = Date.now();
      const current = await sdk.trigger<unknown, { version: string; value: RawObservation | CompressedObservation | null }>({
        function_id: "state::get_versioned",
        payload: { scope: `mem:obs:${data.sessionId}`, key: data.observationId },
      });
      if (!current?.value || current.value.id !== data.observationId || current.value.sessionId !== data.sessionId) {
        return { success: false, error: "STATE_OBSERVATION_MISSING" };
      }
      if (!("hookType" in current.value) || typeof current.value.hookType !== "string" || !("raw" in current.value)) {
        return { success: true, compressed: current.value, deduplicated: true };
      }
      // Read the durable original, never a caller-supplied excerpt or stale event.
      data = { ...data, raw: { ...current.value, hookType: current.value.hookType, raw: current.value.raw } };
      if (data.selected) {
        const session = await kv.get<{ project: string }>(KV.sessions, data.sessionId);
        if (!session || session.project !== data.selected.project || data.selected.sessionId !== data.sessionId ||
            data.selected.sourceFingerprint !== current.value.identityDigest) {
          return { success: false, error: "selection_source_mismatch" };
        }
      }
      const completeSource = JSON.stringify(data.raw.raw) ?? "null";
      if (!data.raw.identityDigest || completeSource.length > 65536 || "storageBounds" in data.raw) {
        return { success: false, pending: true, error: "source_requires_complete_compression" };
      }

      let imageDescription: string | undefined;
      const hasImage = data.raw.modality === "image" || data.raw.modality === "mixed";

      if (hasImage && data.raw.imageData && provider.describeImage) {
        try {
          let base64Data = data.raw.imageData;
          let mimeType = "image/png";

          if (!data.raw.imageData.startsWith("/9j/") && !data.raw.imageData.startsWith("iVBOR")) {
            if (!isManagedImagePath(data.raw.imageData)) {
              throw new Error(`Refusing to read image outside managed store: ${data.raw.imageData}`);
            }
            const fileBuffer = readFileSync(data.raw.imageData);
            base64Data = fileBuffer.toString("base64");
            if (data.raw.imageData.endsWith(".jpg") || data.raw.imageData.endsWith(".jpeg")) mimeType = "image/jpeg";
            else if (data.raw.imageData.endsWith(".webp")) mimeType = "image/webp";
            else if (data.raw.imageData.endsWith(".gif")) mimeType = "image/gif";
          }

          imageDescription = await provider.describeImage(base64Data, mimeType, VISION_DESCRIPTION_PROMPT);
          logger.info("Image described by vision model", { obsId: data.observationId });
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          logger.warn("Vision model call failed, falling back to text-only compression", {
            obsId: data.observationId,
            error: msg,
          });
        }
      }

      if (hasImage && !imageDescription) {
        return { success: false, pending: true, error: "image_preservation_pending" };
      }

      const prompt = buildCompressionPrompt({
        hookType: data.raw.hookType,
        toolName: data.raw.toolName,
        toolInput: data.raw.toolInput,
        toolOutput: imageDescription
          ? `[Image Description]: ${imageDescription}\n\n${data.raw.toolOutput ?? ""}`
          : data.raw.toolOutput,
        userPrompt: data.raw.userPrompt,
        timestamp: data.raw.timestamp,
      }) + `\n\nComplete sanitized source (preserve all useful information, including details omitted from previews above):\n${completeSource}`;
      if (prompt.length > 65536) {
        return { success: false, pending: true, error: "source_requires_complete_compression" };
      }

      try {
        const validator = (response: string) => {
          const parsed = parseCompressionXml(response);
          if (!parsed) return { valid: false, errors: ["xml_parse_failed"] };
          const result = validateOutput(
            CompressOutputSchema,
            parsed,
            "mem::compress",
          );
          return result.valid
            ? { valid: true }
            : { valid: false, errors: result.result.errors };
        };

        const execute = async () => {
        const routedResponse = llmRouter?.hasAuxiliaryProvider
          ? await llmRouter.run(
            llmRouter.hasExplicitRoute("classification") ? "classification" : "compression",
            (selectedProvider) => selectedProvider.compress(COMPRESSION_SYSTEM, prompt),
            (response) => validator(response).valid,
          )
          : undefined;
        const { response, retried } = routedResponse === undefined
          ? await compressWithRetry(
            provider,
            COMPRESSION_SYSTEM,
            prompt,
            validator,
            1,
          )
          : { response: routedResponse, retried: false };
        return { response, retried };
        };
        const { response, retried } = await execute();

        const parsed = parseCompressionXml(response);
        if (!parsed) {
          const latencyMs = Date.now() - startMs;
          if (metricsStore) {
            await metricsStore.record("mem::compress", latencyMs, false);
          }
          logger.warn("Failed to parse compression XML", {
            obsId: data.observationId,
            retried,
          });
          await recordOperation(kv, "mem::compress", "failed", { observationsProcessed: 1, observationsCompressed: 0 }, data.raw.agentId, { durationMs: latencyMs, errorCode: "parse_failed" });
          return { success: false, error: "parse_failed" };
        }

        const qualityScore = scoreCompression(parsed);
        if (!parsed.narrative.trim() || !parsed.title.trim()) {
          return { success: false, pending: true, error: "useful_compression_pending" };
        }
        const importanceOverride =
          typeof data.importanceOverride === "number" &&
          Number.isFinite(data.importanceOverride) &&
          data.importanceOverride >= 1 &&
          data.importanceOverride <= 10
            ? Math.round(data.importanceOverride)
            : undefined;

        let compressed: CompressedObservation = {
          id: data.observationId,
          sessionId: data.sessionId,
          timestamp: data.raw.timestamp,
          ...parsed,
          ...(importanceOverride !== undefined
            ? { importance: importanceOverride }
            : {}),
          confidence: qualityScore / 100,
          compressionMethod: "llm",
          ...(data.raw.identityDigest ? { sourceIdentityDigests: [data.raw.identityDigest] } : {}),
          ...(hasImage ? { modality: data.raw.modality } : {}),
          ...(imageDescription ? { imageDescription } : {}),
          ...(data.raw.imageData ? { imageRef: data.raw.imageData } : {}),
          ...(data.raw.agentId ? { agentId: data.raw.agentId } : {}),
          ...(data.raw.origin ? { origin: data.raw.origin } : {}),
        };

        compressed = await replaceObservation(sdk, compressed, current.version);

        try {
          getSearchIndex().add(compressed);
          scheduleIndexSave();
        } catch (err) {
          logger.warn("Failed to index compressed observation into BM25", {
            obsId: compressed.id,
            sessionId: compressed.sessionId,
            title: compressed.title,
            error: err instanceof Error ? err.message : String(err),
          });
        }

        await vectorIndexAddGuarded(
          compressed.id,
          compressed.sessionId,
          compressed.title + " " + (compressed.narrative || ""),
          { kind: "observation", logId: compressed.id },
        );

        await publishLiveObservation(sdk, "compressed", compressed);

        const latencyMs = Date.now() - startMs;
        if (metricsStore) {
          await metricsStore.record(
            "mem::compress",
            latencyMs,
            true,
            qualityScore,
          );
        }

        logger.info("Observation compressed", {
          obsId: data.observationId,
          type: compressed.type,
          importance: compressed.importance,
          qualityScore,
          retried,
        });

        await recordOperation(kv, "mem::compress", "completed", { observationsProcessed: 1, observationsCompressed: 1 }, data.raw.agentId, { durationMs: latencyMs });
        return { success: true, compressed, qualityScore };
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        const latencyMs = Date.now() - startMs;
        if (metricsStore) {
          await metricsStore.record("mem::compress", latencyMs, false);
        }
        logger.error("Compression failed", {
          obsId: data.observationId,
          error: msg,
        });
        await recordOperation(kv, "mem::compress", "failed", { observationsProcessed: 1 }, data.raw.agentId, { durationMs: latencyMs, errorCode: "compression_failed" });
        return { success: false, error: "compression_failed" };
      }
    },
  );
}
