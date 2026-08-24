import type { FireworksBatchConfig, FireworksBatchJob, FireworksBatchRequest, FireworksBatchWorkItem } from "../types.js";
import { fingerprintId, generateId, KV } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import type { FireworksBatchTransport } from "../providers/fireworks-batch.js";
import { logger } from "../logger.js";

export interface FireworksBatchQueue {
  enqueue(request: FireworksBatchRequest): Promise<{ queued: boolean; workItemId?: string; reason?: string }>;
}

type CompletedHandler = (item: FireworksBatchWorkItem, content: string) => Promise<void>;

function retryAt(config: FireworksBatchConfig, attempts: number): string {
  const delay = Math.min(config.retryMaxMs, config.retryBaseMs * 2 ** Math.max(0, attempts - 1));
  return new Date(Date.now() + delay).toISOString();
}

function resultContent(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const row = value as Record<string, unknown>;
  const response = row.response;
  if (!response || typeof response !== "object") return undefined;
  const body = (response as Record<string, unknown>).body;
  if (!body || typeof body !== "object") return undefined;
  const choices = (body as Record<string, unknown>).choices;
  if (!Array.isArray(choices) || choices.length === 0) return undefined;
  const message = choices[0] && typeof choices[0] === "object"
    ? (choices[0] as Record<string, unknown>).message
    : undefined;
  if (!message || typeof message !== "object") return undefined;
  const content = (message as Record<string, unknown>).content;
  return typeof content === "string" ? content : undefined;
}

function compatibilityKey(item: FireworksBatchWorkItem): string {
  return `${item.task}\0${item.model}\0${item.maxTokens}\0${item.systemPrompt}`;
}

function oldestCreatedAt(items: FireworksBatchWorkItem[]): number {
  return Math.min(...items.map((item) => Date.parse(item.createdAt)).filter(Number.isFinite));
}

export class FireworksBatchCoordinator implements FireworksBatchQueue {
  constructor(
    private readonly kv: StateKV,
    private readonly config: FireworksBatchConfig,
    private readonly transport: FireworksBatchTransport | undefined,
    private readonly onCompleted: CompletedHandler,
  ) {}

  async enqueue(request: FireworksBatchRequest): Promise<{ queued: boolean; workItemId?: string; reason?: string }> {
    if (!this.config.enabled || !this.transport || !this.config.model) {
      return { queued: false, reason: "Fireworks Batch is disabled" };
    }
    const requestChars = request.systemPrompt.length + request.userPrompt.length + (Object.values(request.metadata ?? {}) as string[]).reduce((total, value) => total + value.length, 0);
    if (!request.systemPrompt || !request.userPrompt || requestChars > this.config.maxRequestChars) {
      return { queued: false, reason: "Batch request exceeded configured limits" };
    }
    const fingerprint = fingerprintId("fwb", `${request.task}\0${request.systemPrompt}\0${request.userPrompt}`);
    const existingId = await this.kv.get<string>(KV.fireworksBatchFingerprints, fingerprint);
    if (existingId) {
      const existing = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, existingId);
      if (existing && existing.state !== "dead-letter" && existing.state !== "failed") {
        return { queued: true, workItemId: existing.id };
      }
    }
    const pending = await this.kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems);
    if (pending.filter((item) => item.state === "queued" || item.state === "submitted" || item.state === "polling").length >= this.config.maxQueuedItems) {
      return { queued: false, reason: "Batch queue is full" };
    }
    const now = new Date().toISOString();
    const item: FireworksBatchWorkItem = {
      id: generateId("fwbwork"),
      customId: request.correlationId,
      correlationId: request.correlationId,
      task: request.task,
      model: request.model || this.config.model,
      systemPrompt: request.systemPrompt,
      userPrompt: request.userPrompt,
      maxTokens: request.maxTokens || 1024,
      metadata: request.metadata,
      state: "queued",
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now,
      updatedAt: now,
    };
    await Promise.all([
      this.kv.set(KV.fireworksBatchWorkItems, item.id, item),
      this.kv.set(KV.fireworksBatchFingerprints, fingerprint, item.id),
    ]);
    logger.info("Fireworks Batch work queued", { task: item.task, workItemId: item.id });
    return { queued: true, workItemId: item.id };
  }

  async process(): Promise<void> {
    if (!this.config.enabled || !this.transport) return;
    await this.pollSubmitted();
    await this.submitQueued();
    await this.pollSubmitted();
  }

  private async submitQueued(): Promise<void> {
    const transport = this.transport;
    if (!transport) return;
    const now = new Date().toISOString();
    const [items, jobs] = await Promise.all([
      this.kv.list<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems),
      this.kv.list<FireworksBatchJob>(KV.fireworksBatchJobs),
    ]);
    if (jobs.filter((job) => job.state === "submitted" || job.state === "polling").length >= this.config.maxConcurrency) return;
    const groups = new Map<string, FireworksBatchWorkItem[]>();
    for (const item of items.filter((candidate) => candidate.state === "queued" && candidate.nextAttemptAt <= now)) {
      const key = compatibilityKey(item);
      const group = groups.get(key) ?? [];
      group.push(item);
      groups.set(key, group);
    }
    const eligible = [...groups.values()]
      .map((group) => group.sort((left, right) => left.createdAt.localeCompare(right.createdAt)).slice(0, this.config.maxBatchItems))
      .filter((group) => group.length >= this.config.minBatchItems || Date.now() - oldestCreatedAt(group) >= this.config.maxWaitMs)
      .sort((left, right) => oldestCreatedAt(left) - oldestCreatedAt(right));
    const compatible = eligible[0];
    if (!compatible || compatible.length === 0) return;
    const first = compatible[0];
    const jobId = generateId("fwbjob");
    const inputDatasetId = `${jobId}-input`;
    const outputDatasetId = `${jobId}-output`;
    const jsonl = compatible.map((item) => JSON.stringify({ custom_id: item.customId, body: { model: item.model, messages: [{ role: "system", content: item.systemPrompt }, { role: "user", content: item.userPrompt }], max_tokens: item.maxTokens } })).join("\n");
    if (Buffer.byteLength(jsonl, "utf8") > this.config.maxRequestBytes) {
      logger.warn("Fireworks Batch compatible work exceeds request byte limit", { task: first.task, itemCount: compatible.length });
      return;
    }
    const job: FireworksBatchJob = { id: jobId, inputDatasetId, outputDatasetId, model: first.model, task: first.task, workItemIds: compatible.map((item) => item.id), state: "submitted", attempts: 1, nextAttemptAt: new Date(Date.now() + this.config.pollIntervalMs).toISOString(), createdAt: now, updatedAt: now, remoteJobId: jobId };
    await this.kv.set(KV.fireworksBatchJobs, job.id, job);
    await Promise.all(compatible.map(async (item) => {
      item.state = "submitted";
      item.batchJobId = job.id;
      item.updatedAt = now;
      await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
    }));
    logger.info("Fireworks Batch submitting work", { jobId, task: first.task, itemCount: compatible.length });
    try {
      await transport.createDataset(inputDatasetId, compatible.length);
      await transport.uploadDataset(inputDatasetId, jsonl);
      await transport.submitJob({ jobId, inputDatasetId, outputDatasetId, model: job.model, maxTokens: first.maxTokens });
    } catch (error) {
      const message = error instanceof Error ? error.message : "batch submission failed";
      job.state = "polling";
      job.lastError = message;
      job.updatedAt = new Date().toISOString();
      await this.kv.set(KV.fireworksBatchJobs, job.id, job);
      logger.warn("Fireworks Batch submit uncertain; reconciling by job ID", { jobId: job.id, error: message });
    }
  }

  private async pollSubmitted(): Promise<void> {
    const now = new Date().toISOString();
    const jobs = await this.kv.list<{ id: string; remoteJobId?: string; outputDatasetId: string; workItemIds: string[]; state: string; attempts: number; nextAttemptAt: string; updatedAt: string; lastError?: string }>(KV.fireworksBatchJobs);
    for (const job of jobs.filter((candidate) => (candidate.state === "submitted" || candidate.state === "polling") && candidate.nextAttemptAt <= now).slice(0, this.config.maxConcurrency)) {
      try {
        const status = await this.transport!.getJobStatus(job.remoteJobId || job.id);
        const state = status.state.toUpperCase();
        if (state.includes("SUCCEEDED") || state.includes("COMPLETED")) {
          await this.applyResults(job);
          job.state = "completed";
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        } else if (state.includes("FAILED") || state.includes("CANCELLED")) {
          job.state = "dead-letter";
          job.lastError = status.message || "remote batch failed";
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
          await this.markDeadLetter(job.workItemIds, job.lastError);
        } else {
          job.state = "polling";
          job.attempts += 1;
          job.nextAttemptAt = retryAt(this.config, job.attempts);
          job.updatedAt = new Date().toISOString();
          await this.kv.set(KV.fireworksBatchJobs, job.id, job);
        }
      } catch (error) {
        job.attempts += 1;
        job.updatedAt = new Date().toISOString();
        job.lastError = error instanceof Error ? error.message : "batch polling failed";
        if (job.attempts >= this.config.maxAttempts) {
          job.state = "dead-letter";
          await this.markDeadLetter(job.workItemIds, job.lastError);
        } else {
          job.state = "polling";
          job.nextAttemptAt = retryAt(this.config, job.attempts);
        }
        await this.kv.set(KV.fireworksBatchJobs, job.id, job);
      }
    }
  }

  private async applyResults(job: { outputDatasetId: string; workItemIds: string[] }): Promise<void> {
    const raw = await this.transport!.downloadResults(job.outputDatasetId);
    if (raw.length > this.config.maxResultChars) throw new Error("batch result exceeded configured limits");
    const allowed = new Set(job.workItemIds);
    const items = await Promise.all(job.workItemIds.map((id) => this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id)));
    const byCustomId = new Map(items.filter((item): item is FireworksBatchWorkItem => Boolean(item)).map((item) => [item.customId, item]));
    for (const line of raw.split(/\r?\n/).filter(Boolean)) {
      let parsed: unknown;
      try { parsed = JSON.parse(line); } catch { continue; }
      const rawCustomId = parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>).custom_id : undefined;
      const customId = typeof rawCustomId === "string" ? rawCustomId : undefined;
      if (!customId) continue;
      const item = byCustomId.get(customId);
      if (!item || !allowed.has(item.id) || item.state === "completed") continue;
      const content = resultContent(parsed);
      if (!content) continue;
      await this.onCompleted(item, content);
      item.state = "completed";
      const receivedAt = new Date().toISOString();
      item.result = { customId, content, receivedAt };
      item.updatedAt = receivedAt;
      await this.kv.set(KV.fireworksBatchWorkItems, item.id, item);
    }
  }

  private async markDeadLetter(ids: string[], error: string | undefined): Promise<void> {
    const now = new Date().toISOString();
    await Promise.all(ids.map(async (id) => {
      const item = await this.kv.get<FireworksBatchWorkItem>(KV.fireworksBatchWorkItems, id);
      if (!item || item.state === "completed") return;
      item.state = "dead-letter";
      item.deadLetteredAt = now;
      item.lastError = error;
      item.updatedAt = now;
      await this.kv.set(KV.fireworksBatchWorkItems, id, item);
    }));
  }
}
