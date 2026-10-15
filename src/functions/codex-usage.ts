import { opendir, open, stat } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";

import { addModelUsage, finalizeModelUsage, newModelUsage, priceUsage, summarizePricing } from "./usage-pricing.js";
import type { ModelUsage, UsagePricingMetadata } from "./usage-pricing.js";
export type JsonRecord = Record<string, unknown>;
export function object(value: unknown): JsonRecord | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonRecord : null;
}
export function count(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}
export function metadataId(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 256;
}
export function priceSolUsage(input: number, cached: number, writes: number, output: number) {
  // Legacy processing-sample scenario only; never used to value measured model usage or exposed by savings.
  return priceUsage("gpt-6.1-sol", { input, cached, writes, output }).costs.total ?? 0;
}
export function pricingMetadata(payload: JsonRecord, usage: JsonRecord): UsagePricingMetadata {
  const text = (value: unknown) => value === undefined ? undefined : metadataId(value) ? value : "invalid";
  const creation = object(usage.cache_creation);
  return { tier: text(payload.service_tier ?? usage.service_tier), platform: text(payload.provider ?? payload.platform),
    geography: text(payload.inference_geo ?? payload.geography), speed: text(payload.speed),
    writes5m: count(creation?.ephemeral_5m_input_tokens) ?? undefined,
    writes1h: count(creation?.ephemeral_1h_input_tokens) ?? undefined };
}
export const USAGE_LIMITS = { entries: 4096, directories: 512, files: 100, bytes: 64 * 1024 * 1024,
  lineBytes: 256 * 1024, records: 20_000, milliseconds: 2500 } as const;
export type UsageOptions = { root?: string; now?: Date; limits?: Partial<Record<keyof typeof USAGE_LIMITS, number>> };
export function usageSnapshot(client: "codex" | "claude", now = new Date()) {
  return {
    client, source: `${client}-response-usage`, scope: "local profile; own response records; last 7 days",
    since: new Date(now.getTime() - 7 * 86_400_000).toISOString(), until: now.toISOString(), checkedAt: now.toISOString(),
    sessions: 0, requests: 0, inputTokens: 0, cachedInputTokens: 0, cacheWriteInputTokens: 0,
    outputTokens: 0, reasoningOutputTokens: 0, longContextRequests: 0,
    apiEquivalentUsd: null as number | null, uncachedEquivalentUsd: null as number | null,
    inputApiEquivalentUsd: null as number | null, cacheDiscountUsd: null as number | null,
    cacheHitRatio: null as number | null, inputUsdPerMillion: null as number | null,
    filesFound: 0, filesRead: 0, filesSkipped: 0, bytesRead: 0, entriesVisited: 0,
    malformedRecords: 0, oversizedLines: 0, unpricedRequests: 0, compactedSessions: 0,
    models: Object.create(null) as Record<string, number>, modelUsage: [] as ModelUsage[],
    pricing: summarizePricing([]), partial: false, unavailable: false,
    status: "empty" as "empty" | "unavailable" | "partial" | "available",
  };
}
export type UsageSnapshot = ReturnType<typeof usageSnapshot>;

// Directory handles and bounded line buffers avoid materializing a profile or an oversized line.
export async function scanUsage(root: string | string[], snapshot: UsageSnapshot, options: UsageOptions,
  accept: (record: JsonRecord, file: string) => void) {
  const limits = { ...USAGE_LIMITS, ...options.limits };
  const deadline = Date.now() + limits.milliseconds;
  const roots = new Set(typeof root === "string" ? [root] : root);
  const directories = [...roots].map(path => ({ path, depth: 0 }));
  let foundRoot = false;
  let visitedDirectories = 0, stopped = false;
  while (directories.length && !stopped) {
    if (++visitedDirectories > limits.directories || Date.now() >= deadline) { snapshot.partial = true; break; }
    const { path: directory, depth } = directories.shift()!;
    try {
      const handle = await opendir(directory);
      if (roots.has(directory)) foundRoot = true;
      for await (const entry of handle) {
        if (++snapshot.entriesVisited > limits.entries || Date.now() >= deadline) { snapshot.partial = true; stopped = true; break; }
        const path = join(directory, entry.name);
        if (path.length > 4096) { snapshot.partial = true; continue; }
        if (entry.isDirectory()) {
          if (depth < 16 && directories.length + visitedDirectories < limits.directories) directories.push({ path, depth: depth + 1 });
          else snapshot.partial = true;
          continue;
        }
        if (!entry.isFile() || !entry.name.endsWith(".jsonl")) continue;
        snapshot.filesFound++;
        if (snapshot.filesRead >= limits.files || snapshot.bytesRead >= limits.bytes) {
          snapshot.filesSkipped++; snapshot.partial = true; stopped = true; break;
        }
        const info = await stat(path);
        if (info.mtimeMs < Date.parse(snapshot.since)) continue;
        snapshot.filesRead++;
        const file = await open(path, "r");
        try {
          const buffer = Buffer.alloc(64 * 1024);
          let line = Buffer.alloc(0), oversized = false;
          const consume = () => {
            if (!oversized && line.length) {
              try { const record = object(JSON.parse(line.toString("utf8"))); if (record) accept(record, path); }
              catch { snapshot.malformedRecords++; snapshot.partial = true; }
            }
            line = Buffer.alloc(0); oversized = false;
          };
          while (true) {
            if (Date.now() >= deadline || snapshot.bytesRead >= limits.bytes) { snapshot.partial = true; stopped = true; break; }
            const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, limits.bytes - snapshot.bytesRead), null);
            if (!bytesRead) { consume(); break; }
            snapshot.bytesRead += bytesRead;
            let offset = 0;
            while (offset < bytesRead) {
              const newline = buffer.indexOf(10, offset);
              const end = newline < 0 || newline >= bytesRead ? bytesRead : newline;
              if (!oversized) {
                if (line.length + end - offset > limits.lineBytes) {
                  oversized = true; line = Buffer.alloc(0); snapshot.oversizedLines++; snapshot.partial = true;
                } else line = Buffer.concat([line, buffer.subarray(offset, end)]);
              }
              if (end < bytesRead) consume();
              offset = end + 1;
            }
          }
        } finally { await file.close(); }
        if (stopped) break;
      }
    } catch (error) {
      if (!(object(error)?.code === "ENOENT" && roots.has(directory))) snapshot.partial = true;
    }
  }
  snapshot.unavailable = !foundRoot && !snapshot.partial;
}
export function finalizeUsage(result: UsageSnapshot) {
  result.modelUsage = finalizeModelUsage(result.modelUsage, result);
  result.pricing = summarizePricing(result.modelUsage);
  result.unpricedRequests = result.pricing.unpricedRequests;
  if (result.requests) {
    result.apiEquivalentUsd = result.pricing.costsUsd.total;
    const { input, cacheRead, cacheWrite } = result.pricing.costsUsd;
    result.inputApiEquivalentUsd = input === null || cacheRead === null || cacheWrite === null ? null : input + cacheRead + cacheWrite;
  }
  result.cacheHitRatio = result.inputTokens > 0 ? result.cachedInputTokens / result.inputTokens : null;
  if (result.inputApiEquivalentUsd !== null && result.inputTokens > 0)
    result.inputUsdPerMillion = result.inputApiEquivalentUsd / result.inputTokens * 1_000_000;
  result.status = result.partial ? "partial" : result.unavailable ? "unavailable" : result.requests ? "available" : "empty";
  return result;
}
export async function collectCodexUsage(options: UsageOptions = {}) {
  const result = usageSnapshot("codex", options.now);
  const sessionRoot = join(process.env.CODEX_HOME || join(homedir(), ".codex"), "sessions");
  const root = options.root ?? Array.from({ length: 8 }, (_, day) => join(sessionRoot,
    ...new Date(Date.parse(result.until) - day * 86_400_000).toISOString().slice(0, 10).split("-")));
  const responses = new Set<string>(), sessions = new Set<string>(), compacted = new Set<string>();
  const threads = new Map<string, string>();
  const maxRecords = options.limits?.records ?? USAGE_LIMITS.records;
  const contexts = new Map<string, Map<string, { model: string; metadata: UsagePricingMetadata }>>();
  const providers = new Map<string, string>();
  let contextCount = 0;
  const models = new Map<string, ModelUsage>();
  let uncachedPrice = 0, allUncachedPriced = true;
  await scanUsage(root, result, options, (record, file) => {
    const payload = object(record.payload);
    if (!payload) return;
    if (record.type === "session_meta") {
      threads.delete(file); providers.delete(file);
      contextCount -= contexts.get(file)?.size ?? 0; contexts.delete(file);
      if (metadataId(payload.id)) {
        threads.set(file, payload.id);
        if (metadataId(payload.model_provider)) providers.set(file, payload.model_provider);
      }
      else { result.malformedRecords++; result.partial = true; }
    }
    if (record.type === "turn_context" && threads.has(file) && metadataId(payload.turn_id)) {
      const owner = threads.get(file);
      if (payload.thread_id !== undefined && payload.thread_id !== owner
        || payload.session_id !== undefined && payload.session_id !== owner) {
        result.malformedRecords++; result.partial = true; return;
      }
      const turns = contexts.get(file) ?? new Map();
      if (contextCount >= maxRecords && !turns.has(payload.turn_id)) { result.partial = true; return; }
      if (!turns.has(payload.turn_id)) contextCount++;
      turns.set(payload.turn_id, { model: metadataId(payload.model) ? payload.model : "unknown",
        metadata: pricingMetadata(payload, {}) });
      contexts.set(file, turns);
    }
    if (record.type === "compacted" || payload.type === "context_compacted") compacted.add(file);
    const thread = threads.get(file);
    if (record.type !== "token_usage_record" || !thread || payload.thread_id !== thread) return;
    if (!metadataId(payload.response_id)) { result.malformedRecords++; result.partial = true; return; }
    if (responses.has(payload.response_id)) return;
    const time = typeof record.timestamp === "string" ? Date.parse(record.timestamp) : NaN;
    if (!Number.isFinite(time)) { result.malformedRecords++; result.partial = true; return; }
    if (time < Date.parse(result.since) || time > Date.parse(result.until)) return;
    if (responses.size >= maxRecords) { result.partial = true; return; }
    const usage = object(payload.usage);
    const input = count(usage?.input_tokens), cached = count(usage?.cached_input_tokens);
    const writes = count(usage?.cache_write_input_tokens), output = count(usage?.output_tokens);
    if (input === null || cached === null || writes === null || output === null || cached + writes > input) {
      result.malformedRecords++; result.partial = true; return;
    }
    if (![result.inputTokens + input, result.cachedInputTokens + cached, result.cacheWriteInputTokens + writes,
      result.outputTokens + output].every(Number.isSafeInteger)) { result.partial = true; return; }
    responses.add(payload.response_id); sessions.add(thread); result.requests++;
    result.inputTokens += input; result.cachedInputTokens += cached; result.cacheWriteInputTokens += writes;
    result.outputTokens += output;
    const reasoning = count(usage?.reasoning_output_tokens) ?? 0;
    if (reasoning <= output && Number.isSafeInteger(result.reasoningOutputTokens + reasoning)) result.reasoningOutputTokens += reasoning;
    else { result.malformedRecords++; result.partial = true; }
    const context = metadataId(payload.turn_id) ? contexts.get(file)?.get(payload.turn_id) : undefined;
    const model = payload.model === undefined ? context?.model ?? "unknown" : metadataId(payload.model) ? payload.model : "unknown";
    result.models[model] = (result.models[model] ?? 0) + 1;
    const row = models.get(model) ?? newModelUsage("codex", model);
    models.set(model, row);
    const metadata = { platform: providers.get(file), ...context?.metadata };
    for (const [field, value] of Object.entries(pricingMetadata(payload, usage!)))
      if (value !== undefined) Object.assign(metadata, { [field]: value });
    // A turn context with no platform must not erase the owning session's recorded provider.
    metadata.platform ??= providers.get(file);
    const priced = addModelUsage(row, { input, cached, writes, output }, metadata);
    if (priced.longContext) result.longContextRequests++;
    if (priced.uncachedEquivalentUsd === null) allUncachedPriced = false;
    else uncachedPrice += priced.uncachedEquivalentUsd;
  });
  result.sessions = sessions.size; result.compactedSessions = compacted.size;
  result.modelUsage = [...models.values()];
  finalizeUsage(result);
  if (result.requests && allUncachedPriced && result.apiEquivalentUsd !== null) {
    result.uncachedEquivalentUsd = uncachedPrice; result.cacheDiscountUsd = uncachedPrice - result.apiEquivalentUsd;
  }
  return result;
}
export type CodexUsageSnapshot = UsageSnapshot;
let cached: UsageSnapshot | undefined;
let pending: Promise<UsageSnapshot> | undefined;
export async function readCodexUsage(): Promise<UsageSnapshot> {
  if (cached && Date.now() - Date.parse(cached.checkedAt) < 5 * 60_000) return cached;
  return pending ??= collectCodexUsage().then(value => cached = value).finally(() => { pending = undefined; });
}
