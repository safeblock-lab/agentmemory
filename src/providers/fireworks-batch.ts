import { fetchWithTimeout } from "./_fetch.js";
import type { FireworksBatchConfig } from "../types.js";
import { lookup } from "node:dns/promises";
import { request as httpsRequest } from "node:https";
import type { IncomingHttpHeaders, ClientRequest } from "node:http";
import { isIP } from "node:net";
import { Readable } from "node:stream";

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
  remoteJobId?: string;
  remoteJobName?: string;
  inputDatasetId?: string;
  outputDatasetId?: string;
}

export interface FireworksBatchErrorDiagnostic {
  code?: string;
  message?: string;
}

export type FireworksBatchResultFile = string | string[];

export interface FireworksBatchDownloadedResults {
  resultRows?: FireworksBatchResultFile;
  errorRows?: FireworksBatchResultFile;
  results?: FireworksBatchResultFile;
  errors?: FireworksBatchResultFile;
  result?: FireworksBatchResultFile;
  error?: FireworksBatchResultFile;
}

export interface FireworksBatchTransport {
  createDataset(datasetId: string, exampleCount: number): Promise<void>;
  uploadDataset(datasetId: string, jsonl: string): Promise<void>;
  submitJob(input: FireworksBatchSubmitInput): Promise<{
    remoteJobId: string;
    remoteJobName?: string;
    inputDatasetId?: string;
    outputDatasetId?: string;
  }>;
  getJobStatus(remoteJobId: string): Promise<FireworksBatchRemoteStatus>;
  downloadResults(outputDatasetId: string): Promise<string | FireworksBatchDownloadedResults>;
  listRecentJobIds?(limit: number): Promise<string[]>;
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
  const id = lastPathSegment(datasetId);
  if (!isSafeRemoteJobId(id)) {
    throw new FireworksBatchError("dataset-path", "dataset ID was invalid");
  }
  return `${accountPath(accountId)}/datasets/${encodeURIComponent(id)}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function lastPathSegment(value: string): string {
  const segment = value.split("/").filter(Boolean).at(-1);
  return segment || value;
}

const MAX_DIAGNOSTIC_CHARS = 512;
const MAX_JOB_LIST_PAGES = 32;
const MAX_RESULT_FILES = 32;
const MAX_DOWNLOAD_CONCURRENCY = 4;
const MAX_RESULT_MATERIALIZED_BYTES = 64 * 1024 * 1024;

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

function isSafeRemoteResourceName(value: string): boolean {
  return value.length > 0
    && value.length <= 1_024
    && !/[\u0000-\u0020\u007f?#\\]/.test(value);
}

function remoteResourceValue(
  response: Record<string, unknown>,
  field: string,
  operation: string,
): string | undefined {
  if (!(field in response) || response[field] === undefined || response[field] === null) {
    return undefined;
  }
  const value = response[field];
  if (typeof value !== "string" || !isSafeRemoteResourceName(value)) {
    throw new FireworksBatchError(operation, `remote response field ${field} was invalid`);
  }
  return value;
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

type ResultKind = "result" | "error";

interface ResultFileAccumulator {
  kind: ResultKind;
  index: number;
  parts: string[];
}

class ResultMaterializer {
  private readonly parts: Record<ResultKind, Array<string | undefined>> = { result: [], error: [] };
  private totalChars = 0;
  private totalBytes = 0;
  private readonly maxChars: number;
  private readonly maxBytes: number;

  constructor(maxChars: number, maxResponseBytes: number) {
    this.maxChars = Number.isFinite(maxChars) && maxChars >= 0 ? Math.floor(maxChars) : 0;
    const maxResponseAggregate = Number.isFinite(maxResponseBytes) && maxResponseBytes >= 0
      ? Math.min(MAX_RESULT_MATERIALIZED_BYTES, Math.floor(maxResponseBytes) * MAX_RESULT_FILES)
      : MAX_RESULT_MATERIALIZED_BYTES;
    const maxCharsBytes = this.maxChars > MAX_RESULT_MATERIALIZED_BYTES / 4
      ? MAX_RESULT_MATERIALIZED_BYTES
      : this.maxChars * 4;
    this.maxBytes = Math.min(MAX_RESULT_MATERIALIZED_BYTES, maxResponseAggregate, maxCharsBytes);
  }

  start(kind: ResultKind, index: number): ResultFileAccumulator {
    const parts: string[] = [];
    if (index > 0) {
      this.reserve(1, 1);
      parts.push("\n");
    }
    return { kind, index, parts };
  }

  append(file: ResultFileAccumulator, text: string, bytes: number): void {
    this.reserve(text.length, bytes);
    if (text) file.parts.push(text);
  }

  assertBytesAvailable(bytes: number): void {
    if (bytes < 0 || this.totalBytes + bytes > this.maxBytes) {
      throw new FireworksBatchError("dataset-download", "materialized result exceeded configured limits");
    }
  }

  finish(file: ResultFileAccumulator): void {
    this.parts[file.kind][file.index] = file.parts.join("");
  }

  text(kind: ResultKind): string {
    return this.parts[kind].filter((part): part is string => part !== undefined).join("");
  }

  private reserve(chars: number, bytes: number): void {
    if (this.totalChars + chars > this.maxChars || this.totalBytes + bytes > this.maxBytes) {
      throw new FireworksBatchError("dataset-download", "materialized result exceeded configured limits");
    }
    this.totalChars += chars;
    this.totalBytes += bytes;
  }
}

async function cancelResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // The response is already being rejected; cleanup is best effort.
  }
}

async function materializeResultBody(
  response: Response,
  maxResponseBytes: number,
  materializer: ResultMaterializer,
  kind: ResultKind,
  fileIndex: number,
): Promise<void> {
  const contentLength = response.headers.get("content-length");
  if (contentLength && /^\d+$/.test(contentLength) && Number(contentLength) > maxResponseBytes) {
    await cancelResponseBody(response);
    throw new FireworksBatchError("dataset-download", "response exceeded configured size limit");
  }

  const file = materializer.start(kind, fileIndex);
  if (contentLength && /^\d+$/.test(contentLength)) {
    try {
      materializer.assertBytesAvailable(Number(contentLength));
    } catch (error) {
      await cancelResponseBody(response);
      throw error;
    }
  }
  if (!response.body) {
    const text = await response.text();
    const bytes = new TextEncoder().encode(text).byteLength;
    if (bytes > maxResponseBytes) {
      throw new FireworksBatchError("dataset-download", "response exceeded configured size limit");
    }
    materializer.append(file, text, bytes);
    materializer.finish(file);
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let fileBytes = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      fileBytes += next.value.byteLength;
      if (fileBytes > maxResponseBytes) {
        await reader.cancel();
        throw new FireworksBatchError("dataset-download", "response exceeded configured size limit");
      }
      materializer.assertBytesAvailable(next.value.byteLength);
      materializer.append(file, decoder.decode(next.value, { stream: true }), next.value.byteLength);
    }
    materializer.append(file, decoder.decode(), 0);
    materializer.finish(file);
  } catch (error) {
    await reader.cancel().catch(() => undefined);
    throw error;
  } finally {
    reader.releaseLock();
  }
}

function parseIpv4(value: string): number[] | undefined {
  const parts = value.split(".");
  if (parts.length !== 4 || parts.some((part) => !/^\d{1,3}$/.test(part))) return undefined;
  const octets = parts.map(Number);
  return octets.every((octet) => octet >= 0 && octet <= 255) ? octets : undefined;
}

function parseIpv6(value: string): number[] | undefined {
  if (value.includes("%")) return undefined;
  let normalized = value.toLowerCase();
  if (normalized.includes(".")) {
    const separator = normalized.lastIndexOf(":");
    if (separator < 0) return undefined;
    const ipv4 = parseIpv4(normalized.slice(separator + 1));
    if (!ipv4) return undefined;
    const high = ((ipv4[0]! << 8) | ipv4[1]!).toString(16);
    const low = ((ipv4[2]! << 8) | ipv4[3]!).toString(16);
    normalized = `${normalized.slice(0, separator)}:${high}:${low}`;
  }

  const halves = normalized.split("::");
  if (halves.length > 2) return undefined;
  const left = halves[0] ? halves[0].split(":") : [];
  const right = halves.length === 2 && halves[1] ? halves[1].split(":") : [];
  const parseGroup = (group: string): number | undefined => {
    if (!/^[0-9a-f]{1,4}$/.test(group)) return undefined;
    const parsed = Number.parseInt(group, 16);
    return parsed <= 0xffff ? parsed : undefined;
  };
  const leftGroups = left.map(parseGroup);
  const rightGroups = right.map(parseGroup);
  if (leftGroups.some((group) => group === undefined) || rightGroups.some((group) => group === undefined)) {
    return undefined;
  }
  const totalGroups = leftGroups.length + rightGroups.length;
  if (halves.length === 1 && totalGroups !== 8) return undefined;
  if (halves.length === 2 && totalGroups >= 8) return undefined;
  const groups = [
    ...leftGroups,
    ...Array.from({ length: 8 - totalGroups }, () => 0),
    ...rightGroups,
  ] as number[];
  return groups.flatMap((group) => [group >> 8, group & 0xff]);
}

function matchesPrefix(bytes: number[], prefix: number[], bits: number): boolean {
  const fullBytes = Math.floor(bits / 8);
  for (let index = 0; index < fullBytes; index += 1) {
    if (bytes[index] !== prefix[index]) return false;
  }
  const remainingBits = bits % 8;
  return remainingBits === 0
    || (bytes[fullBytes]! & (0xff << (8 - remainingBits)))
      === (prefix[fullBytes]! & (0xff << (8 - remainingBits)));
}

function isForbiddenIpv4(bytes: number[]): boolean {
  const [first, second, third] = bytes;
  if (first === undefined || second === undefined || third === undefined) return true;
  return first === 0
    || first === 10
    || first === 127
    || (first === 100 && second >= 64 && second <= 127)
    || (first === 169 && second === 254)
    || (first === 172 && second >= 16 && second <= 31)
    || (first === 192 && second === 0)
    || (first === 192 && second === 2)
    || (first === 192 && second === 88 && third === 99)
    || (first === 192 && second === 168)
    || (first === 198 && (second === 18 || second === 19))
    || (first === 198 && second === 51)
    || (first === 203 && second === 0)
    || first >= 224;
}

function isForbiddenAddress(value: string): boolean {
  const address = value.replace(/^\[|\]$/g, "");
  const ipv4 = parseIpv4(address);
  if (ipv4) return isForbiddenIpv4(ipv4);

  const ipv6 = parseIpv6(address);
  if (!ipv6) return true;
  const allZero = ipv6.every((byte) => byte === 0);
  const loopback = allZero || (ipv6.slice(0, 15).every((byte) => byte === 0) && ipv6[15] === 1);
  if (loopback || matchesPrefix(ipv6, [0xfc], 7) || matchesPrefix(ipv6, [0xfe, 0x80], 10)) return true;
  if (matchesPrefix(ipv6, [0xff], 8) || matchesPrefix(ipv6, [0xfe, 0xc0], 10)) return true;
  if (matchesPrefix(ipv6, [0x20, 0x01, 0x00], 23)
    || matchesPrefix(ipv6, [0x20, 0x01, 0x0d, 0xb8], 32)
    || matchesPrefix(ipv6, [0x20, 0x01, 0x00, 0x02], 48)
    || matchesPrefix(ipv6, [0x20, 0x01, 0x00, 0x10], 28)
    || matchesPrefix(ipv6, [0x01, 0x00, 0x00, 0x00], 64)
    || matchesPrefix(ipv6, [0x20, 0x02], 16)) return true;

  const mappedIpv4 = ipv6.slice(0, 10).every((byte) => byte === 0)
    && ipv6[10] === 0xff && ipv6[11] === 0xff;
  const compatibleIpv4 = ipv6.slice(0, 12).every((byte) => byte === 0);
  if (compatibleIpv4) return true;
  if (mappedIpv4) return isForbiddenIpv4(ipv6.slice(12));
  return false;
}

interface SignedResultDestination {
  parsed: URL;
  hostname: string;
  address: string;
  family: 4 | 6;
}

async function assertSafeSignedResultUrl(parsed: URL): Promise<SignedResultDestination> {
  if (parsed.protocol !== "https:" || parsed.username || parsed.password || parsed.hash) {
    throw new FireworksBatchError("dataset-download", "remote signed result URL was not safe");
  }
  const hostname = parsed.hostname.startsWith("[") && parsed.hostname.endsWith("]")
    ? parsed.hostname.slice(1, -1)
    : parsed.hostname;
  if (!hostname || /(?:^|\.)(?:localhost|local)$/i.test(hostname)) {
    throw new FireworksBatchError("dataset-download", "remote signed result URL was not safe");
  }
  if (isIP(hostname) !== 0) {
    if (isForbiddenAddress(hostname)) {
      throw new FireworksBatchError("dataset-download", "remote signed result URL resolved to a private or special-use address");
    }
    return { parsed, hostname, address: hostname, family: isIP(hostname) as 4 | 6 };
  }

  let resolved: Array<{ address: string; family: number }>;
  try {
    resolved = await lookup(hostname, { all: true, verbatim: true });
  } catch {
    throw new FireworksBatchError("dataset-download", "remote signed result URL DNS resolution failed");
  }
  const addresses = Array.isArray(resolved)
    ? resolved.map((entry) => {
      if (!isRecord(entry) || typeof entry.address !== "string" || isForbiddenAddress(entry.address)) return undefined;
      const family = isIP(entry.address);
      return family === 4 || family === 6 ? { address: entry.address, family: family as 4 | 6 } : undefined;
    })
    : [];
  if (addresses.length === 0 || addresses.some((entry) => entry === undefined)) {
    throw new FireworksBatchError("dataset-download", "remote signed result URL resolved to a private or special-use address");
  }
  const first = addresses[0]!;
  return { parsed, hostname, address: first.address, family: first.family };
}

function incomingHeaders(headers: IncomingHttpHeaders): Headers {
  const result = new Headers();
  for (const [name, value] of Object.entries(headers)) {
    if (Array.isArray(value)) {
      for (const item of value) result.append(name, item);
    } else if (value !== undefined) {
      result.set(name, value);
    }
  }
  return result;
}

async function fetchPinnedSignedResult(
  destination: SignedResultDestination,
  timeoutMs: number,
): Promise<Response> {
  return await new Promise<Response>((resolve, reject) => {
    let settled = false;
    let request: ClientRequest;
    const fail = (error: unknown): void => {
      if (settled) return;
      settled = true;
      reject(error);
    };
    try {
      request = httpsRequest(destination.parsed, {
        method: "GET",
        agent: false,
        headers: { Host: destination.parsed.host },
        servername: destination.hostname,
        lookup: (_hostname, options, callback) => {
          if (options.all) {
            callback(null, [{ address: destination.address, family: destination.family }]);
          } else {
            callback(null, destination.address, destination.family);
          }
        },
      }, (response) => {
        const status = response.statusCode ?? 0;
        if (status >= 300 && status < 400) {
          response.resume();
          fail(new FireworksBatchError("dataset-download", "remote signed result URL redirect was not allowed"));
          request.destroy();
          return;
        }
        settled = true;
        resolve(new Response(Readable.toWeb(response) as ReadableStream<Uint8Array>, {
          status,
          statusText: response.statusMessage,
          headers: incomingHeaders(response.headers),
        }));
      });
      request.on("error", fail);
      request.setTimeout(timeoutMs, () => {
        request.destroy();
        fail(new Error("signed result request timed out"));
      });
      request.end();
    } catch (error) {
      fail(error);
    }
  });
}

async function mapWithConcurrency<T, R>(
  items: readonly T[],
  concurrency: number,
  mapper: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIndex = 0;
  const worker = async (): Promise<void> => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= items.length) return;
      results[index] = await mapper(items[index]!);
    }
  };
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, () => worker()));
  return results;
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

  async submitJob(input: FireworksBatchSubmitInput): Promise<{
    remoteJobId: string;
    remoteJobName?: string;
    inputDatasetId?: string;
    outputDatasetId?: string;
  }> {
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
    const name = remoteResourceValue(response, "name", "job-submit");
    const responseId = remoteResourceValue(response, "id", "job-submit");
    const remoteJobId = responseId ? lastPathSegment(responseId) : name ? lastPathSegment(name) : undefined;
    const inputDatasetId = remoteResourceValue(response, "inputDatasetId", "job-submit");
    const outputDatasetId = remoteResourceValue(response, "outputDatasetId", "job-submit");
    if (!remoteJobId || !isSafeRemoteJobId(remoteJobId)) {
      throw new FireworksBatchError("job-submit", "remote response did not include a safe job ID");
    }
    return {
      remoteJobId,
      ...((name ?? (responseId && responseId.includes("/") ? responseId : undefined))
        ? { remoteJobName: name ?? responseId } : {}),
      ...(inputDatasetId ? { inputDatasetId } : {}),
      ...(outputDatasetId ? { outputDatasetId } : {}),
    };
  }

  async getJobStatus(remoteJobId: string): Promise<FireworksBatchRemoteStatus> {
    const response = await this.jsonRequest<Record<string, unknown>>(
      "job-status",
      `${accountPath(this.config.accountId!)}/batchInferenceJobs/${encodeURIComponent(remoteJobId)}`,
      { method: "GET" },
    );
    const name = remoteResourceValue(response, "name", "job-status");
    const responseId = remoteResourceValue(response, "id", "job-status");
    const outputDatasetId = remoteResourceValue(response, "outputDatasetId", "job-status");
    const inputDatasetId = remoteResourceValue(response, "inputDatasetId", "job-status");
    const resolvedRemoteJobId = responseId ? lastPathSegment(responseId) : name ? lastPathSegment(name) : undefined;
    if (resolvedRemoteJobId && !isSafeRemoteJobId(resolvedRemoteJobId)) {
      throw new FireworksBatchError("job-status", "remote response did not include a safe job ID");
    }
    return {
      state: typeof response.state === "string" ? response.state : "JOB_STATE_UNSPECIFIED",
      message: isRecord(response.status) && typeof response.status.message === "string"
        ? sanitizeDiagnosticText(response.status.message)
        : undefined,
      ...(resolvedRemoteJobId ? { remoteJobId: resolvedRemoteJobId } : {}),
      ...(name ? { remoteJobName: name } : {}),
      ...(inputDatasetId ? { inputDatasetId } : {}),
      ...(outputDatasetId ? { outputDatasetId } : {}),
    };
  }

  async listRecentJobIds(limit: number): Promise<string[]> {
    const boundedLimit = Math.max(0, Math.min(4_096, Math.floor(limit)));
    if (boundedLimit === 0) return [];
    const ids: string[] = [];
    const seenIds = new Set<string>();
    const seenTokens = new Set<string>();
    const pageSize = Math.min(200, boundedLimit);
    let pageToken: string | undefined;
    let pageCount = 0;
    while (ids.length < boundedLimit) {
      if (pageCount >= MAX_JOB_LIST_PAGES) {
        throw new FireworksBatchError("job-list", "remote job list exceeded the configured page limit", { retryable: true });
      }
      pageCount += 1;
      const query = new URLSearchParams({
        pageSize: String(pageSize),
        orderBy: "create_time desc",
      });
      if (pageToken) query.set("pageToken", pageToken);
      const response = await this.jsonRequest<Record<string, unknown>>(
        "job-list",
        `${accountPath(this.config.accountId!)}/batchInferenceJobs?${query.toString()}`,
        { method: "GET" },
      );
      if (!Array.isArray(response.batchInferenceJobs)) {
        throw new FireworksBatchError("job-list", "remote response did not include a job list");
      }
      for (const value of response.batchInferenceJobs) {
        if (!isRecord(value)) continue;
        const resource = typeof value.name === "string"
          ? value.name
          : typeof value.id === "string" ? value.id : undefined;
        if (!resource || !isSafeRemoteResourceName(resource)) continue;
        const id = lastPathSegment(resource);
        if (!id.startsWith("fwbjob-") || !isSafeRemoteJobId(id) || seenIds.has(id)) continue;
        seenIds.add(id);
        ids.push(id);
        if (ids.length >= boundedLimit) break;
      }
      if (ids.length >= boundedLimit) break;
      const nextToken = response.nextPageToken;
      if (nextToken === undefined || nextToken === null || nextToken === "") break;
      if (typeof nextToken !== "string" || nextToken.length > 4_096 || /[\u0000-\u001f\u007f]/.test(nextToken)) {
        throw new FireworksBatchError("job-list", "remote response included an invalid page token");
      }
      if (seenTokens.has(nextToken)) {
        throw new FireworksBatchError("job-list", "remote response repeated a page token");
      }
      seenTokens.add(nextToken);
      pageToken = nextToken;
    }
    return ids;
  }

  async downloadResults(outputDatasetId: string): Promise<string | FireworksBatchDownloadedResults> {
    const response = await this.jsonRequest<Record<string, unknown>>(
      "dataset-download",
      `${datasetPath(this.config.accountId!, outputDatasetId)}:getDownloadEndpoint`,
      { method: "GET" },
    );
    const urls = response.filenameToSignedUrls;
    if (!isRecord(urls)) {
      throw new FireworksBatchError("dataset-download", "remote response did not include signed result URLs");
    }
    const rawEntries = Object.entries(urls);
    if (rawEntries.length > MAX_RESULT_FILES) {
      throw new FireworksBatchError("dataset-download", "remote result file count exceeded configured limit");
    }
    const entries = rawEntries.filter((entry): entry is [string, string] => typeof entry[1] === "string");
    const relevantEntries = entries.filter(([name]) => /result|output|success|error|fail/i.test(name));
    const files = relevantEntries.length > 0 ? relevantEntries : entries;
    if (files.length === 0) throw new FireworksBatchError("dataset-download", "remote response did not include result or error files");

    const safeFiles: Array<{ name: string; destination: SignedResultDestination; kind: ResultKind; index: number }> = [];
    const nextKindIndex: Record<ResultKind, number> = { result: 0, error: 0 };
    const hasRelevantEntries = relevantEntries.length > 0;
    for (const [name, signedUrl] of files) {
      let parsed: URL;
      try {
        parsed = new URL(signedUrl);
      } catch {
        throw new FireworksBatchError("dataset-download", "remote signed result URL was invalid");
      }
      const destination = await assertSafeSignedResultUrl(parsed);
      const kind: ResultKind = !hasRelevantEntries || /result|output|success/i.test(name) ? "result" : "error";
      safeFiles.push({ name, destination, kind, index: nextKindIndex[kind] });
      nextKindIndex[kind] += 1;
    }

    const configuredConcurrency = Number.isInteger(this.config.maxConcurrency) && this.config.maxConcurrency > 0
      ? this.config.maxConcurrency
      : 1;
    const downloadConcurrency = Math.min(MAX_DOWNLOAD_CONCURRENCY, configuredConcurrency);
    const materializer = new ResultMaterializer(this.config.maxResultChars, this.config.maxResponseBytes);
    await mapWithConcurrency(safeFiles, downloadConcurrency, async ({ destination, kind, index }) => {
      let download: Response;
      try {
        download = await fetchPinnedSignedResult(destination, this.config.timeoutMs);
      } catch (error) {
        if (error instanceof FireworksBatchError) throw error;
        throw new FireworksBatchError("dataset-download", "network request failed", { retryable: true });
      }
      if (!download.ok) {
        const text = await readResponseBody(download, this.config.maxResponseBytes, "dataset-download");
        const diagnostic = parseErrorDiagnostic(text);
        throw new FireworksBatchError("dataset-download", formatRemoteFailure(download.status, diagnostic), {
          retryable: download.status === 408 || download.status === 425 || download.status === 429 || download.status >= 500,
          status: download.status,
          diagnostic,
        });
      }
      await materializeResultBody(download, this.config.maxResponseBytes, materializer, kind, index);
      return undefined;
    });
    const resultFiles = safeFiles.filter(({ kind }) => kind === "result");
    const errorFiles = safeFiles.filter(({ kind }) => kind === "error");
    const resultText = materializer.text("result");
    const errorText = materializer.text("error");
    if (errorFiles.length === 0 && resultFiles.length === 1 && files.length === 1 && hasRelevantEntries) {
      return resultText;
    }
    return {
      resultRows: resultFiles.length > 0 ? resultText : undefined,
      errorRows: errorFiles.length > 0 ? errorText : undefined,
    };
  }
}
