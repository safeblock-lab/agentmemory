import { fetchWithTimeout } from "./_fetch.js";
import type { FireworksBatchConfig } from "../types.js";

export const FIREWORKS_BATCH_API_BASE = "https://api.fireworks.ai";

export interface FireworksBatchSubmitInput {
  jobId: string;
  inputDatasetId: string;
  outputDatasetId: string;
  model: string;
  maxTokens: number;
}

export interface FireworksBatchRemoteStatus {
  state: string;
  message?: string;
}

export interface FireworksBatchTransport {
  createDataset(datasetId: string, exampleCount: number): Promise<void>;
  uploadDataset(datasetId: string, jsonl: string): Promise<void>;
  submitJob(input: FireworksBatchSubmitInput): Promise<{ remoteJobId: string }>;
  getJobStatus(remoteJobId: string): Promise<FireworksBatchRemoteStatus>;
  downloadResults(outputDatasetId: string): Promise<string>;
}

export class FireworksBatchError extends Error {
  readonly retryable: boolean;
  readonly status?: number;
  readonly operation: string;

  constructor(
    operation: string,
    message: string,
    options: { retryable?: boolean; status?: number } = {},
  ) {
    super(message);
    this.name = "FireworksBatchError";
    this.operation = operation;
    this.retryable = options.retryable ?? false;
    this.status = options.status;
  }
}

function accountPath(accountId: string): string {
  return `/v1/accounts/${encodeURIComponent(accountId)}`;
}

function datasetPath(accountId: string, datasetId: string): string {
  return `${accountPath(accountId)}/datasets/${encodeURIComponent(datasetId)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function lastPathSegment(value: string): string {
  const segment = value.split("/").filter(Boolean).at(-1);
  return segment || value;
}

async function readLimitedBody(response: Response, maxBytes: number, operation: string): Promise<string> {
  const contentLength = response.headers.get("content-length");
  if (contentLength && /^\d+$/.test(contentLength) && Number(contentLength) > maxBytes) {
    throw new FireworksBatchError(operation, "response exceeded configured size limit");
  }

  if (!response.body) {
    const text = await response.text();
    if (new TextEncoder().encode(text).byteLength > maxBytes) {
      throw new FireworksBatchError(operation, "response exceeded configured size limit");
    }
    return text;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (total > maxBytes) {
        await reader.cancel();
        throw new FireworksBatchError(operation, "response exceeded configured size limit");
      }
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(merged);
}

export class FireworksBatchClient implements FireworksBatchTransport {
  constructor(private readonly config: FireworksBatchConfig) {
    if (!config.enabled || !config.accountId || !config.apiKey) {
      throw new Error("Fireworks Batch client requires enabled configuration, account ID, and API key");
    }
  }

  private url(path: string): string {
    return `${FIREWORKS_BATCH_API_BASE}${path}`;
  }

  private async jsonRequest<T>(
    operation: string,
    path: string,
    init: { method: "GET" | "POST"; body?: string },
  ): Promise<T> {
    if (init.body && new TextEncoder().encode(init.body).byteLength > this.config.maxRequestBytes) {
      throw new FireworksBatchError(operation, "request exceeded configured size limit");
    }

    let response: Response;
    try {
      response = await fetchWithTimeout(
        this.url(path),
        {
          method: init.method,
          headers: {
            Authorization: `Bearer ${this.config.apiKey}`,
            ...(init.body ? { "Content-Type": "application/json" } : {}),
          },
          ...(init.body ? { body: init.body } : {}),
          redirect: "error",
        },
        this.config.timeoutMs,
      );
    } catch {
      throw new FireworksBatchError(operation, "network request failed", { retryable: true });
    }

    const body = await readLimitedBody(response, this.config.maxResponseBytes, operation);
    if (!response.ok) {
      throw new FireworksBatchError(operation, `remote request failed (${response.status})`, {
        retryable: response.status === 408 || response.status === 409 || response.status === 425 || response.status === 429 || response.status >= 500,
        status: response.status,
      });
    }
    try {
      const parsed: unknown = body ? JSON.parse(body) : {};
      if (!isRecord(parsed)) throw new Error("not an object");
      return parsed as T;
    } catch {
      throw new FireworksBatchError(operation, "remote response was not valid JSON");
    }
  }

  async createDataset(datasetId: string, exampleCount: number): Promise<void> {
    if (!Number.isInteger(exampleCount) || exampleCount < 1) {
      throw new FireworksBatchError("dataset-create", "dataset requires at least one example");
    }
    await this.jsonRequest(
      "dataset-create",
      `${accountPath(this.config.accountId!)}/datasets`,
      {
        method: "POST",
        body: JSON.stringify({ datasetId, dataset: { exampleCount: String(exampleCount), userUploaded: {} } }),
      },
    );
  }

  async uploadDataset(datasetId: string, jsonl: string): Promise<void> {
    const bodyBytes = new TextEncoder().encode(jsonl).byteLength;
    if (bodyBytes > this.config.maxRequestBytes || jsonl.length > this.config.maxRequestChars) {
      throw new FireworksBatchError("dataset-upload", "request exceeded configured size limit");
    }

    const form = new FormData();
    form.append("file", new Blob([jsonl], { type: "application/jsonl" }), "batch-input.jsonl");
    let response: Response;
    try {
      response = await fetchWithTimeout(
        this.url(`${datasetPath(this.config.accountId!, datasetId)}:upload`),
        {
          method: "POST",
          headers: { Authorization: `Bearer ${this.config.apiKey}` },
          body: form,
          redirect: "error",
        },
        this.config.timeoutMs,
      );
    } catch {
      throw new FireworksBatchError("dataset-upload", "network request failed", { retryable: true });
    }
    await readLimitedBody(response, this.config.maxResponseBytes, "dataset-upload");
    if (!response.ok) {
      throw new FireworksBatchError("dataset-upload", `remote request failed (${response.status})`, {
        retryable: response.status === 408 || response.status === 409 || response.status === 425 || response.status === 429 || response.status >= 500,
        status: response.status,
      });
    }
  }

  async submitJob(input: FireworksBatchSubmitInput): Promise<{ remoteJobId: string }> {
    const query = new URLSearchParams({ batchInferenceJobId: input.jobId });
    const body = JSON.stringify({
      model: input.model,
      inputDatasetId: `${accountPath(this.config.accountId!).slice(4)}/datasets/${input.inputDatasetId}`,
      outputDatasetId: `${accountPath(this.config.accountId!).slice(4)}/datasets/${input.outputDatasetId}`,
      inferenceParameters: { maxTokens: input.maxTokens },
    });
    const response = await this.jsonRequest<Record<string, unknown>>(
      "job-submit",
      `${accountPath(this.config.accountId!)}/batchInferenceJobs?${query.toString()}`,
      { method: "POST", body },
    );
    const name = typeof response.name === "string" ? response.name : input.jobId;
    return { remoteJobId: lastPathSegment(name) };
  }

  async getJobStatus(remoteJobId: string): Promise<FireworksBatchRemoteStatus> {
    const response = await this.jsonRequest<Record<string, unknown>>(
      "job-status",
      `${accountPath(this.config.accountId!)}/batchInferenceJobs/${encodeURIComponent(remoteJobId)}`,
      { method: "GET" },
    );
    return {
      state: typeof response.state === "string" ? response.state : "JOB_STATE_UNSPECIFIED",
      message: isRecord(response.status) && typeof response.status.message === "string"
        ? response.status.message.slice(0, 512)
        : undefined,
    };
  }

  async downloadResults(outputDatasetId: string): Promise<string> {
    const response = await this.jsonRequest<Record<string, unknown>>(
      "dataset-download",
      `${datasetPath(this.config.accountId!, outputDatasetId)}:getDownloadEndpoint`,
      { method: "GET" },
    );
    const urls = response.filenameToSignedUrls;
    if (!isRecord(urls)) {
      throw new FireworksBatchError("dataset-download", "remote response did not include signed result URLs");
    }
    const entries = Object.entries(urls).filter((entry): entry is [string, string] => typeof entry[1] === "string");
    const result = entries.find(([name]) => /result|output|success/i.test(name)) ?? entries[0];
    if (!result) throw new FireworksBatchError("dataset-download", "remote response did not include a result file");

    let parsed: URL;
    try {
      parsed = new URL(result[1]);
    } catch {
      throw new FireworksBatchError("dataset-download", "remote signed result URL was invalid");
    }
    if (parsed.protocol !== "https:" || parsed.username || parsed.password || parsed.hash) {
      throw new FireworksBatchError("dataset-download", "remote signed result URL was not safe");
    }
    let download: Response;
    try {
      download = await fetchWithTimeout(
        parsed.toString(),
        { method: "GET", redirect: "error" },
        this.config.timeoutMs,
      );
    } catch {
      throw new FireworksBatchError("dataset-download", "network request failed", { retryable: true });
    }
    const text = await readLimitedBody(download, this.config.maxResponseBytes, "dataset-download");
    if (!download.ok) {
      throw new FireworksBatchError("dataset-download", `remote request failed (${download.status})`, {
        retryable: download.status === 408 || download.status === 425 || download.status === 429 || download.status >= 500,
        status: download.status,
      });
    }
    return text;
  }
}
