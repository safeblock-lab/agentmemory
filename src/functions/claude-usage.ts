import { homedir } from "node:os";
import { join } from "node:path";
import { count, finalizeUsage, metadataId, object, pricingMetadata, scanUsage, usageSnapshot, USAGE_LIMITS } from "./codex-usage.js";
import type { UsageOptions, UsageSnapshot } from "./codex-usage.js";
import { addModelUsage, newModelUsage } from "./usage-pricing.js";
import type { ModelUsage, UsagePricingMetadata } from "./usage-pricing.js";
type ResponseUsage = { input: number; cached: number; writes: number; output: number; session: string; model: string;
  metadata: UsagePricingMetadata };

export async function collectClaudeUsage(options: UsageOptions = {}) {
  const result = usageSnapshot("claude", options.now);
  const root = options.root ?? join(process.env.CLAUDE_CONFIG_DIR || join(homedir(), ".claude"), "projects");
  const responses = new Map<string, ResponseUsage>();
  const maxRecords = options.limits?.records ?? USAGE_LIMITS.records;
  await scanUsage(root, result, options, record => {
    if (record.type !== "assistant") return;
    const message = object(record.message), usage = object(message?.usage);
    if (!message || !usage) return;
    const time = typeof record.timestamp === "string" ? Date.parse(record.timestamp) : NaN;
    if (!Number.isFinite(time)) { result.malformedRecords++; result.partial = true; return; }
    if (time < Date.parse(result.since) || time > Date.parse(result.until)) return;
    if (!metadataId(message.id) || !metadataId(record.sessionId)) {
      result.malformedRecords++; result.partial = true; return;
    }
    const input = count(usage.input_tokens), output = count(usage.output_tokens);
    const cached = count(usage.cache_read_input_tokens), writes = count(usage.cache_creation_input_tokens);
    if (input === null || output === null || cached === null || writes === null
      || !Number.isSafeInteger(input + cached + writes)) { result.malformedRecords++; result.partial = true; return; }
    // Streaming content blocks and fork copies repeat a message's cumulative usage.
    const previous = responses.get(message.id);
    if (!previous && responses.size >= maxRecords) { result.partial = true; return; }
    const model = metadataId(message.model) ? message.model : "unknown";
    const metadata = pricingMetadata({ ...record, ...message }, usage);
    if (previous && previous.model !== model) { result.partial = true; return; }
    if (previous && (previous.input !== input || previous.cached !== cached || previous.writes !== writes)) {
      result.malformedRecords++; result.partial = true; return;
    }
    if (previous && JSON.stringify(previous.metadata) !== JSON.stringify(metadata)) {
      result.malformedRecords++; result.partial = true; return;
    }
    responses.set(message.id, { input, cached, writes, output: Math.max(previous?.output ?? 0, output),
      session: previous?.session ?? record.sessionId, model, metadata });
  });
  const sessions = new Set<string>();
  const models = new Map<string, ModelUsage>();
  for (const usage of responses.values()) {
    if (![result.inputTokens + usage.input + usage.cached + usage.writes, result.cachedInputTokens + usage.cached,
      result.cacheWriteInputTokens + usage.writes, result.outputTokens + usage.output].every(Number.isSafeInteger)) {
      result.partial = true; continue;
    }
    sessions.add(usage.session); result.requests++;
    result.inputTokens += usage.input + usage.cached + usage.writes;
    result.cachedInputTokens += usage.cached; result.cacheWriteInputTokens += usage.writes; result.outputTokens += usage.output;
    result.models[usage.model] = (result.models[usage.model] ?? 0) + 1;
    const row = models.get(usage.model) ?? newModelUsage("claude", usage.model);
    models.set(usage.model, row);
    const priced = addModelUsage(row, { ...usage, input: usage.input + usage.cached + usage.writes }, usage.metadata);
    if (priced.longContext) result.longContextRequests++;
  }
  result.sessions = sessions.size;
  result.modelUsage = [...models.values()];
  return finalizeUsage(result);
}
let cached: UsageSnapshot | undefined;
let pending: Promise<UsageSnapshot> | undefined;
export async function readClaudeUsage(): Promise<UsageSnapshot> {
  if (cached && Date.now() - Date.parse(cached.checkedAt) < 5 * 60_000) return cached;
  return pending ??= collectClaudeUsage().then(value => cached = value).finally(() => { pending = undefined; });
}
