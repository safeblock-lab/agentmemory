import type { IIIClient } from "iii-sdk";
import { STREAM } from "../state/schema.js";
import { logger } from "../logger.js";

const DISPLAY_FIELDS = ["id", "sessionId", "agentId", "timestamp", "type", "title", "subtitle", "narrative", "facts", "concepts", "files", "importance", "tokens", "toolName", "hookType", "userPrompt", "compressionMethod"];

export async function publishLiveObservation(sdk: IIIClient, kind: "raw" | "compressed", source: object): Promise<void> {
  const observation: Record<string, unknown> = {};
  const row = source as Record<string, unknown>;
  const truncatedFields: string[] = [];
  for (const field of DISPLAY_FIELDS) {
    const value = row[field];
    if (typeof value === "string") {
      const limit = ["narrative", "userPrompt"].includes(field) ? 4096 : 2048;
      observation[field] = value.slice(0, limit);
      if (value.length > limit) truncatedFields.push(field);
    } else if (typeof value === "number" && Number.isFinite(value) || typeof value === "boolean") observation[field] = value;
    else if (Array.isArray(value)) {
      const strings = value.slice(0, 16).filter((item): item is string => typeof item === "string");
      observation[field] = strings.map(item => item.slice(0, 256));
      if (value.length > 16 || strings.length !== value.length || strings.some(item => item.length > 256)) truncatedFields.push(field);
    }
  }
  const data = { type: kind, observation, sessionId: row.sessionId, projection: { truncated_fields: truncatedFields } };
  const results = await Promise.allSettled([STREAM.group(String(row.sessionId)), STREAM.viewerGroup].map(async group_id =>
    sdk.trigger({ function_id: "stream::send", payload: { stream_name: STREAM.name, group_id,
      type: `${kind}_observation`, data } })));
  for (const result of results) if (result.status === "rejected") logger.warn("Non-fatal observation notification failure after commit", {
    observationId: row.id, sessionId: row.sessionId, kind,
    errorType: result.reason instanceof Error ? result.reason.name : "unknown",
  });
}
