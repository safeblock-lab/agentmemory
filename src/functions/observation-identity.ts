import { createHash } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import type { CompressedObservation, HookPayload, RawObservation, Session } from "../types.js";
import { buildSyntheticCompression } from "./compress-synthetic.js";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";

export interface ObservationIdentity {
  canonicalIdentity: string;
  identityDigest: string;
  eventId: string;
}

export interface ObservationAdmission {
  created: boolean;
  duplicate_event: boolean;
  observation_id: string;
  observation: RawObservation | CompressedObservation;
  version: string;
  occurrence_count: number;
  first_seen_at: string;
  last_seen_at: string;
}

export class ObservationAdmissionError extends Error {
  constructor(readonly releaseImage: boolean, cause: unknown) {
    super(cause instanceof Error ? cause.message : "Observation admission failed", { cause });
  }
}

export function stableObservationJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableObservationJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().filter((key) => record[key] !== undefined)
      .map((key) => `${JSON.stringify(key)}:${stableObservationJson(record[key])}`).join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}

type TranscriptCheckpoint = NonNullable<Session["captureCheckpoints"]>[string];

export async function captureTranscriptSnapshot(sdk: IIIClient, kv: StateKV, payload: HookPayload): Promise<unknown> {
  const data = payload.data as Record<string, unknown>;
  if (data.producer !== "session-end-transcript-v1" || typeof data.sourceKey !== "string" || !/^[a-f0-9]{64}$/.test(data.sourceKey)
    || typeof data.revision !== "string" || !data.revision || data.revision.length > 256 || typeof data.text !== "string"
    || Buffer.byteLength(data.text) > 256 * 1024 || !Number.isSafeInteger(data.windowStart) || Number(data.windowStart) < 0
    || !Number.isSafeInteger(data.windowEnd) || data.windowEnd !== Number(data.windowStart) + Buffer.byteLength(data.text)
    || !data.text.endsWith("\n")) return { success: false, error: "Invalid transcript capture window" };
  const sourceKey = data.sourceKey;
  const revision = data.revision;
  const windowStart = Number(data.windowStart);
  const bytes = Buffer.from(data.text);
  return withKeyedLock(`capture:${payload.sessionId}:${sourceKey}`, async () => {
    const session = await kv.get<{ captureCheckpoints?: Record<string, TranscriptCheckpoint> }>(KV.sessions, payload.sessionId);
    if (session?.captureCheckpoints && !Object.hasOwn(session.captureCheckpoints, sourceKey)
      && Object.keys(session.captureCheckpoints).length >= 8) {
      return { success: false, pending: true, code: "TRANSCRIPT_PRODUCER_LIMIT" };
    }
    const checkpoint = session?.captureCheckpoints?.[sourceKey];
    let from = 0;
    let mode: "incremental" | "bounded_revision" = "bounded_revision";
    if (checkpoint && checkpoint.revision === revision && Number.isSafeInteger(checkpoint.windowStart) && Number.isSafeInteger(checkpoint.end)
      && checkpoint.windowStart >= windowStart && checkpoint.end >= checkpoint.windowStart && checkpoint.end <= Number(data.windowEnd)
      && digest(bytes.subarray(checkpoint.windowStart - windowStart, checkpoint.end - windowStart).toString("utf8")) === checkpoint.prefixDigest) {
      from = checkpoint.end - windowStart;
      mode = "incremental";
    }
    if (from === bytes.length) return { success: true, deduplicated: true, capture: { mode, published: 0, checkpoint: checkpoint?.end } };
    let cursor = from;
    let published = 0;
    let malformedLines = 0;
    while (cursor < bytes.length && published < 50) {
      const end = bytes.indexOf(10, cursor);
      if (end < 0) break;
      const line = bytes.subarray(cursor, end).toString("utf8");
      let message: unknown;
      try { message = JSON.parse(line); } catch { malformedLines++; cursor = end + 1; continue; }
      if (message && typeof message === "object" && !Array.isArray(message)) {
        const entry = message as Record<string, unknown>;
        const nested = entry.message && typeof entry.message === "object" && !Array.isArray(entry.message)
          ? entry.message as Record<string, unknown> : undefined;
        if ((entry.role === "user" || nested?.role === "user") && Array.isArray(nested?.content)) {
          const prompt = nested.content.filter((block): block is { type: "text"; text: string } => !!block && typeof block === "object"
            && block.type === "text" && typeof block.text === "string")
            .map(block => (block.text.match(/<user_query>\n?([\s\S]*?)\n?<\/user_query>/)?.[1] ?? block.text).trim()).filter(Boolean).join("\n");
          if (prompt) {
            const result = await sdk.trigger<HookPayload, { success?: boolean; observationId?: string; pending?: boolean }>({
              function_id: "mem::observe", payload: { ...payload, data: { prompt, capture: {
                producer: "session-end-transcript-v1", sourceKey, revision, offset: windowStart + cursor, lineDigest: digest(line),
                availability: "[bounded transcript capture; bytes outside delivered window unavailable]",
              } } },
            });
            if (!result?.observationId || result.success === false || result.pending) {
              return { success: false, pending: true, code: "TRANSCRIPT_PUBLICATION_PENDING", capture: { mode, published, checkpoint: checkpoint?.end } };
            }
            published++;
          }
        }
      }
      cursor = end + 1;
    }
    if (!await kv.get(KV.sessions, payload.sessionId)) return { success: false, pending: true, code: "TRANSCRIPT_SESSION_REQUIRED" };
    const next: TranscriptCheckpoint = { revision, windowStart, end: windowStart + cursor,
      prefixDigest: digest(bytes.subarray(0, cursor).toString("utf8")), publishedAt: payload.timestamp };
    // A lost checkpoint acknowledgement only causes replay: stable row events are already canonical.
    await kv.update(KV.sessions, payload.sessionId, [{ type: "set", path: `captureCheckpoints.${sourceKey}`, value: next }]);
    return { success: true, capture: { mode, published, malformedLines, checkpoint: next.end,
      partial: cursor < bytes.length, prefixOutsideWindow: windowStart > 0 } };
  });
}

function digest(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function observationIdentity(
  payload: Pick<HookPayload, "sessionId" | "hookType" | "timestamp">,
  sanitizedData: unknown,
  _options: { toolInputIdentity?: boolean } = {},
): ObservationIdentity {
  // Changed output is a distinct original, even when tool input is unchanged.
  const canonicalIdentity = stableObservationJson({ version: 1, sessionId: payload.sessionId, hookType: payload.hookType, data: sanitizedData });
  const identityDigest = digest(canonicalIdentity);
  const capture = sanitizedData && typeof sanitizedData === "object" && !Array.isArray(sanitizedData)
    ? (sanitizedData as Record<string, unknown>).capture : undefined;
  if (payload.hookType === "prompt_submit" && capture && typeof capture === "object" && !Array.isArray(capture)) {
    const source = capture as Record<string, unknown>;
    if (source.producer === "session-end-transcript-v1" && typeof source.sourceKey === "string" && /^[a-f0-9]{64}$/.test(source.sourceKey)
      && typeof source.revision === "string" && Number.isSafeInteger(source.offset) && Number(source.offset) >= 0
      && typeof source.lineDigest === "string" && /^[a-f0-9]{64}$/.test(source.lineDigest)) {
      return { canonicalIdentity, identityDigest, eventId: digest(stableObservationJson([payload.sessionId, source, identityDigest])) };
    }
  }
  return { canonicalIdentity, identityDigest, eventId: digest(stableObservationJson([payload.sessionId, payload.timestamp, identityDigest])) };
}

export function reconstructLegacyIdentity(value: unknown): ObservationIdentity | null {
  if (!value || typeof value !== "object") return null;
  const raw = value as Record<string, unknown>;
  if (typeof raw.sessionId !== "string" || typeof raw.hookType !== "string" || typeof raw.timestamp !== "string"
    || !Object.hasOwn(raw, "raw") || Object.hasOwn(raw, "storageBounds") || Object.hasOwn(raw, "title")) return null;
  return observationIdentity({ sessionId: raw.sessionId, hookType: raw.hookType as HookPayload["hookType"], timestamp: raw.timestamp }, raw.raw);
}

export function boundedObservationText(value: unknown, limit: number): string {
  const text = typeof value === "string" ? value : JSON.stringify(value) ?? "";
  if (text.length <= limit) return text;
  const part = Math.floor((limit - 100) / 3);
  const relevant: string[] = [];
  let remaining = part;
  for (const match of text.matchAll(/[^\r\n]+/g)) {
    const line = match[0];
    if (!/\b(?:error|fail(?:ed|ure)?|decision|learn(?:ed|ing)?|summary|important|warning|fix(?:ed)?|constraint)\b/i.test(line)) continue;
    const excerpt = line.slice(0, remaining);
    relevant.push(excerpt);
    remaining -= excerpt.length + 1;
    if (remaining <= 0) break;
  }
  return `${text.slice(0, part)}\n[search/display excerpt; original source remains protected]\n${relevant.join("\n")}\n${text.slice(-part)}`;
}

export function buildStoredSynthetic(raw: RawObservation): CompressedObservation {
  const synthetic = buildSyntheticCompression(raw);
  const parts = [raw.userPrompt, raw.toolInput, raw.toolOutput].filter((part) => part !== undefined);
  synthetic.narrative = boundedObservationText(parts.length ? parts.map((part) => typeof part === "string" ? part : JSON.stringify(part)).join(" | ") : raw.raw, 8192);
  return synthetic;
}

export async function admitObservation(
  sdk: IIIClient,
  identity: ObservationIdentity,
  observation: RawObservation | CompressedObservation,
  raw: RawObservation,
  session?: object,
  maxObservations?: number,
): Promise<ObservationAdmission> {
  const request = {
    function_id: "state::observation_admit_v1",
    payload: {
      session_id: raw.sessionId, identity_digest: identity.identityDigest,
      event_id: identity.eventId, occurred_at: raw.timestamp, observation,
      provenance: { sessionId: raw.sessionId, hookType: raw.hookType, ...(raw.agentId ? { agentId: raw.agentId } : {}), ...(raw.origin ? { origin: { channel: raw.origin.channel, ...(raw.origin.detail ? { detail: raw.origin.detail } : {}) } } : {}) },
      ...(session ? { session } : {}),
      ...(maxObservations && maxObservations > 0 ? { max_observations: Math.floor(maxObservations) } : {}),
    },
  };
  let result: ObservationAdmission;
  try {
    result = await sdk.trigger<unknown, ObservationAdmission>(request);
  } catch (error) {
    if (error instanceof Error && /STATE_[A-Z_]+/.test(error.message)) throw new ObservationAdmissionError(true, error);
    // Resolve a lost acknowledgement with the same event, candidate ID and body.
    try { result = await sdk.trigger<unknown, ObservationAdmission>(request); }
    catch (retryError) { throw new ObservationAdmissionError(false, retryError); }
  }
  if (!result || typeof result.created !== "boolean" || typeof result.duplicate_event !== "boolean"
    || typeof result.observation_id !== "string" || !/^\d+$/.test(result.version)
    || !result.observation || result.observation.id !== result.observation_id
    || result.observation.sessionId !== raw.sessionId || !Number.isSafeInteger(result.occurrence_count)
    || result.occurrence_count < 1 || (result.created && result.duplicate_event)
    || !Number.isFinite(Date.parse(result.first_seen_at)) || !Number.isFinite(Date.parse(result.last_seen_at))
    || Date.parse(result.first_seen_at) > Date.parse(result.last_seen_at)
    || result.observation.occurrenceCount !== result.occurrence_count
    || result.observation.firstSeenAt !== result.first_seen_at || result.observation.lastSeenAt !== result.last_seen_at
    || stableObservationJson(result.observation.raw) !== stableObservationJson(raw.raw)
    || !Array.isArray(result.observation.provenance) || result.observation.provenance.length > 32
    || !result.observation.provenance.some((entry) => entry && entry.sessionId === raw.sessionId
      && entry.hookType === raw.hookType && entry.agentId === raw.agentId
      && entry.origin?.channel === raw.origin?.channel && entry.origin?.detail === raw.origin?.detail
      && Number.isSafeInteger(entry.count) && Number(entry.count) >= 1
      && Number(entry.count) <= result.occurrence_count
      && typeof entry.firstSeenAt === "string" && typeof entry.lastSeenAt === "string"
      && Number.isFinite(Date.parse(entry.firstSeenAt)) && Number.isFinite(Date.parse(entry.lastSeenAt))
      && Date.parse(entry.firstSeenAt) <= Date.parse(entry.lastSeenAt))) {
    throw new ObservationAdmissionError(false, new Error("STATE_OBSERVATION_INVALID_RESPONSE"));
  }
  return result;
}

function isConflict(error: unknown): boolean {
  return error instanceof Error && (error.message.includes("STATE_TX_CONFLICT")
    || ("code" in error && error.code === "STATE_TX_CONFLICT"));
}

export async function replaceObservation(
  sdk: IIIClient,
  observation: CompressedObservation,
  initialVersion?: string,
  onlyRaw = false,
): Promise<CompressedObservation> {
  let version = initialVersion;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (version === undefined || onlyRaw) {
      const current = await sdk.trigger<unknown, { version: string; value: RawObservation | CompressedObservation | null }>({ function_id: "state::get_versioned", payload: { scope: `mem:obs:${observation.sessionId}`, key: observation.id } });
      if (!current?.value || !/^\d+$/.test(current.version)) throw new Error("STATE_OBSERVATION_MISSING");
      if (onlyRaw && "title" in current.value) return current.value;
      version = current.version;
    }
    try {
      const result = await sdk.trigger<unknown, { observation: CompressedObservation }>({
        function_id: "state::observation_replace_v1",
        payload: { session_id: observation.sessionId, observation_id: observation.id, expected_version: version, observation },
      });
      if (!result?.observation || result.observation.id !== observation.id || result.observation.sessionId !== observation.sessionId) throw new Error("STATE_OBSERVATION_INVALID_RESPONSE");
      return result.observation;
    } catch (error) {
      if (!isConflict(error) || attempt === 2) throw error;
      version = undefined;
    }
  }
  throw new Error("STATE_TX_CONFLICT");
}
