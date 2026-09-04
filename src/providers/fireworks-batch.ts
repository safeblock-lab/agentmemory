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

export interface FireworksBatchErrorDiagnostic {
  code?: string;
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
  readonly diagnostic: FireworksBatchErrorDiagnostic;

  constructor(
    operation: string,
    message: string,
    options: { retryable?: boolean; status?: number; diagnostic?: FireworksBatchErrorDiagnostic } = {},
  ) {
    super(sanitizeDiagnosticText(message) ?? "Fireworks Batch request failed");
    this.name = "FireworksBatchError";
    this.operation = operation;
    this.retryable = options.retryable ?? false;
    this.status = options.status;
    this.diagnostic = {
      code: sanitizeDiagnosticText(options.diagnostic?.code),
      message: sanitizeDiagnosticText(options.diagnostic?.message),
    };
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

const MAX_DIAGNOSTIC_CHARS = 512;

function sanitizeDiagnosticText(value: unknown): string | undefined {
  if (typeof value !== "string" && typeof value !== "number") return undefined;
  const text = String(value)
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\bhttps?:\/\/[^\s<>"']+/gi, "[redacted-url]")
    .replace(/\bBearer\s+[A-Za-z0-9._~+/=-]+/gi, "Bearer [redacted]")
    .replace(/(["']?)(authorization|api[-_ ]?key|access[-_ ]?token|token|secret|password)\1\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:bearer\s+)?[^,;\s}]+)/gi, "$2=[redacted]")
    .replace(/(["']?)((?:user|system)?prompt|messages?|jsonl)\1\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^,;\s}]+)/gi, "$2=[redacted]");
  return text ? text.slice(0, MAX_DIAGNOSTIC_CHARS) : undefined;
}

function parseErrorDiagnostic(body: string): FireworksBatchErrorDiagnostic {
  let parsed: unknown;
  try {
    parsed = body ? JSON.parse(body) : undefined;
  } catch {
    return {};
  }
  if (!isRecord(parsed)) return {};
  const error = isRecord(parsed.error) ? parsed.error : parsed;
  return {
    code: sanitizeDiagnosticText(error.code ?? parsed.code ?? error.status ?? parsed.status),
    message: sanitizeDiagnosticText(error.message ?? parsed.message),
  };
}

function formatRemoteFailure(status: number, diagnostic: FireworksBatchErrorDiagnostic): string {
  const details = [
    diagnostic.code ? `code=${diagnostic.code}` : undefined,
    diagnostic.message ? `message=${diagnostic.message}` : undefined,
  ].filter((detail): detail is string => Boolean(detail));
  return `remote request failed (${status})${details.length > 0 ? `: ${details.join("; ")}` : ""}`;
}

async function readResponseBody(response: Response, maxBytes: number, operation: string): Promise<string> {
  try {
    return await readLimitedBody(response, maxBytes, operation);
  } catch (error) {
    const message = error instanceof FireworksBatchError ? error.message : "failed to read remote response";
    throw new FireworksBatchError(operation, message, { status: response.status });
  }
}

function isSafeRemoteJobId(value: string): boolean {
  return value.length > 0
    && value.length <= 512
    && !/[\u0000-\u0020\u007f/?#\\]/.test(value);
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

    const body = await readResponseBody(response, this.config.maxResponseBytes, operation);
    if (!response.ok) {
      const diagnostic = parseErrorDiagnostic(body);
      throw new FireworksBatchError(operation, formatRemoteFailure(response.status, diagnostic), {
        retryable: response.status === 408 || response.status === 409 || response.status === 425 || response.status === 429 || response.status >= 500,
        status: response.status,
        diagnostic,
      });
    }
    try {
      const parsed: unknown = body ? JSON.parse(body) : {};
      if (!isRecord(parsed)) throw new Error("not an object");
      return parsed as T;
    } catch {
      throw new FireworksBatchError(operation, "remote response was not valid JSON", { status: response.status });
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
    const body = await readResponseBody(response, this.config.maxResponseBytes, "dataset-upload");
    if (!response.ok) {
      const diagnostic = parseErrorDiagnostic(body);
      throw new FireworksBatchError("dataset-upload", formatRemoteFailure(response.status, diagnostic), {
        retryable: response.status === 408 || response.status === 409 || response.status === 425 || response.status === 429 || response.status >= 500,
        status: response.status,
        diagnostic,
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
    const name = typeof response.name === "string" ? response.name : undefined;
    const remoteJobId = name ? lastPathSegment(name) : undefined;
    if (!remoteJobId || !isSafeRemoteJobId(remoteJobId)) {
      throw new FireworksBatchError("job-submit", "remote response did not include a safe job ID");
    }
    return { remoteJobId };
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
        ? sanitizeDiagnosticText(response.status.message)
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
    const text = await readResponseBody(download, this.config.maxResponseBytes, "dataset-download");
    if (!download.ok) {
      const diagnostic = parseErrorDiagnostic(text);
      throw new FireworksBatchError("dataset-download", formatRemoteFailure(download.status, diagnostic), {
        retryable: download.status === 408 || download.status === 425 || download.status === 429 || download.status >= 500,
        status: download.status,
        diagnostic,
      });
    }
    return text;
  }
}
