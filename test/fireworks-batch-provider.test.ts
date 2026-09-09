import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { lookup } from "node:dns/promises";
import { EventEmitter } from "node:events";
import { request as httpsRequest } from "node:https";
import { Readable } from "node:stream";
import {
  FireworksBatchClient,
} from "../src/providers/fireworks-batch.js";
import type { FireworksBatchConfig } from "../src/types.js";

vi.mock("node:dns/promises", () => ({ lookup: vi.fn() }));
vi.mock("node:https", () => ({ request: vi.fn() }));

const lookupMock = vi.mocked(lookup);
const httpsRequestMock = vi.mocked(httpsRequest);

interface FakeRequest extends EventEmitter {
  setTimeout: (timeoutMs: number, callback: () => void) => FakeRequest;
  destroy: (error?: Error) => FakeRequest;
  end: () => void;
}

interface PinnedResponse {
  body: string;
  status?: number;
  headers?: Record<string, string>;
}

function stubPinnedRequests(
  handler: (url: URL, options: Record<string, unknown>) => PinnedResponse | Promise<PinnedResponse>,
): void {
  httpsRequestMock.mockImplementation(((url, options, callback) => {
    const request = new EventEmitter() as FakeRequest;
    request.setTimeout = vi.fn(() => request);
    request.destroy = vi.fn((error?: Error) => {
      if (error) queueMicrotask(() => request.emit("error", error));
      return request;
    });
    request.end = vi.fn(() => {
      Promise.resolve(handler(new URL(String(url)), options as Record<string, unknown>))
        .then((spec) => {
          const response = Readable.from([Buffer.from(spec.body)]);
          Object.assign(response, {
            statusCode: spec.status ?? 200,
            statusMessage: "OK",
            headers: spec.headers ?? {},
          });
          queueMicrotask(() => callback(response as never));
        })
        .catch((error: unknown) => request.emit("error", error));
    });
    return request as never;
  }) as never);
}

const config: FireworksBatchConfig = {
  enabled: true,
  accountId: "test-account",
  apiKey: "test-key",
  model: "accounts/test/models/test",
  timeoutMs: 1_000,
  minBatchItems: 1,
  maxWaitMs: 60_000,
  maxBatchItems: 10,
  maxRequestChars: 10_000,
  maxRequestBytes: 10_000,
  maxResponseBytes: 10_000,
  maxResultChars: 10_000,
  maxConcurrency: 1,
  maxAttempts: 3,
  retryBaseMs: 1,
  retryMaxMs: 10,
  pollIntervalMs: 0,
  pollMaxIntervalMs: 10,
  recoveryStaleMs: 1_000,
  maxQueuedItems: 10,
};

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

beforeEach(() => {
  lookupMock.mockReset();
  lookupMock.mockResolvedValue([{ address: "93.184.216.34", family: 4 }]);
  httpsRequestMock.mockReset();
});

describe("FireworksBatchClient provider contract", () => {
  it("lists recent AgentMemory jobs with bounded Fireworks pagination", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = new URL(String(input));
      if (!url.searchParams.has("pageToken")) {
        return jsonResponse({
          batchInferenceJobs: [
            { name: "accounts/test-account/batchInferenceJobs/fwbjob-new" },
            { name: "accounts/test-account/batchInferenceJobs/manual-job" },
          ],
          nextPageToken: "next-page",
        });
      }
      return jsonResponse({
        batchInferenceJobs: [
          { name: "accounts/test-account/batchInferenceJobs/fwbjob-old" },
          { name: "accounts/test-account/batchInferenceJobs/fwbjob-new" },
        ],
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).listRecentJobIds(10)).resolves.toEqual([
      "fwbjob-new",
      "fwbjob-old",
    ]);
    const firstUrl = new URL(String(fetchMock.mock.calls[0]?.[0]));
    const secondUrl = new URL(String(fetchMock.mock.calls[1]?.[0]));
    expect(firstUrl.searchParams.get("pageSize")).toBe("10");
    expect(firstUrl.searchParams.get("orderBy")).toBe("create_time desc");
    expect(secondUrl.searchParams.get("pageToken")).toBe("next-page");
  });

  it("keeps canonical job and dataset identities returned by submit", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({
      name: "accounts/test-account/batchInferenceJobs/remote-canonical",
      inputDatasetId: "accounts/test-account/datasets/input-canonical",
      outputDatasetId: "accounts/test-account/datasets/output-canonical",
    }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await new FireworksBatchClient(config).submitJob({
      jobId: "fwbjob-requested",
      inputDatasetId: "fwbjob-requested-input",
      outputDatasetId: "fwbjob-requested-output",
      model: config.model!,
      maxTokens: 512,
    });

    expect(result).toEqual({
      remoteJobId: "remote-canonical",
      remoteJobName: "accounts/test-account/batchInferenceJobs/remote-canonical",
      inputDatasetId: "accounts/test-account/datasets/input-canonical",
      outputDatasetId: "accounts/test-account/datasets/output-canonical",
    });
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(String(url)).toBe("https://api.fireworks.ai/v1/accounts/test-account/batchInferenceJobs?batchInferenceJobId=fwbjob-requested");
    expect(JSON.parse(String(init?.body))).toMatchObject({
      inputDatasetId: "accounts/test-account/datasets/fwbjob-requested-input",
      outputDatasetId: "accounts/test-account/datasets/fwbjob-requested-output",
    });
  });

  it("keeps canonical identities returned while polling", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({
      name: "accounts/test-account/batchInferenceJobs/remote-canonical",
      state: "JOB_STATE_COMPLETED",
      inputDatasetId: "accounts/test-account/datasets/input-canonical",
      outputDatasetId: "accounts/test-account/datasets/output-canonical",
      status: { message: "saved output" },
    }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).getJobStatus("remote-canonical")).resolves.toEqual({
      state: "JOB_STATE_COMPLETED",
      message: "saved output",
      remoteJobId: "remote-canonical",
      remoteJobName: "accounts/test-account/batchInferenceJobs/remote-canonical",
      inputDatasetId: "accounts/test-account/datasets/input-canonical",
      outputDatasetId: "accounts/test-account/datasets/output-canonical",
    });
    expect(String(fetchMock.mock.calls[0]?.[0])).toBe(
      "https://api.fireworks.ai/v1/accounts/test-account/batchInferenceJobs/remote-canonical",
    );
  });

  it("downloads both Fireworks result and error files from a canonical dataset", async () => {
    const resultRow = JSON.stringify({ custom_id: "ok", response: { body: { choices: [] } } });
    const errorRow = JSON.stringify({ custom_id: "failed", error: { code: "INVALID_ARGUMENT" } });
    const resultUrl = "https://signed.example/results.jsonl";
    const errorUrl = "https://signed.example/errors.jsonl";
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      if (url.endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: {
          "results.jsonl": resultUrl,
          "errors.jsonl": errorUrl,
        } });
      }
      if (url === resultUrl) return new Response(resultRow);
      if (url === errorUrl) return new Response(errorRow);
      throw new Error(`unexpected URL ${url}`);
    });
    stubPinnedRequests((url) => {
      if (url.toString() === resultUrl) return { body: resultRow };
      if (url.toString() === errorUrl) return { body: errorRow };
      throw new Error(`unexpected pinned URL ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults(
      "accounts/test-account/datasets/output-canonical",
    )).resolves.toEqual({ resultRows: resultRow, errorRows: errorRow });
    expect(String(fetchMock.mock.calls[0]?.[0])).toBe(
      "https://api.fireworks.ai/v1/accounts/test-account/datasets/output-canonical:getDownloadEndpoint",
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(httpsRequestMock).toHaveBeenCalledTimes(2);
  });

  it.each([
    "127.0.0.1",
    "10.20.30.40",
    "169.254.169.254",
    "::1",
    "fc00::1",
    "fe80::1",
    "ff02::1",
    "::ffff:127.0.0.1",
  ])("rejects signed result URLs resolving to %s", async (address) => {
    lookupMock.mockResolvedValue([{ address, family: address.includes(":") ? 6 : 4 }]);
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": "https://signed.example/results.jsonl" } });
      }
      throw new Error("signed result fetch must not run");
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .rejects.toThrow(/private or special-use/);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("accepts a public DNS result and validates every resolved address", async () => {
    lookupMock.mockResolvedValue([
      { address: "93.184.216.34", family: 4 },
      { address: "2001:4860:4860::8888", family: 6 },
    ]);
    const signedUrl = "https://signed.example/results.jsonl";
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": signedUrl } });
      }
      throw new Error("signed result fetch must use the pinned request");
    });
    stubPinnedRequests((url, options) => {
      expect(url.toString()).toBe(signedUrl);
      expect(options.servername).toBe("signed.example");
      expect(options.agent).toBe(false);
      expect(options.headers).toEqual({ Host: "signed.example" });
      return { body: "result-row" };
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .resolves.toBe("result-row");
    expect(lookupMock).toHaveBeenCalledWith("signed.example", { all: true, verbatim: true });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(httpsRequestMock).toHaveBeenCalledTimes(1);
  });

  it("pins the public DNS answer when a later lookup would be private", async () => {
    lookupMock
      .mockResolvedValueOnce([{ address: "93.184.216.34", family: 4 }])
      .mockResolvedValueOnce([{ address: "127.0.0.1", family: 4 }]);
    const signedUrl = "https://signed.example/results.jsonl";
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": signedUrl } });
      }
      throw new Error("signed result fetch must use the pinned request");
    });
    let pinnedAddress: string | undefined;
    stubPinnedRequests((_url, options) => {
      const pinnedLookup = options.lookup as (
        hostname: string,
        lookupOptions: { all?: boolean },
        callback: (error: Error | null, address: string, family?: number) => void,
      ) => void;
      pinnedLookup("signed.example", { all: false }, (error, address) => {
        if (error) throw error;
        pinnedAddress = address;
      });
      return { body: "result-row" };
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .resolves.toBe("result-row");
    expect(pinnedAddress).toBe("93.184.216.34");
    expect(lookupMock).toHaveBeenCalledTimes(1);
    expect(httpsRequestMock).toHaveBeenCalledTimes(1);
  });

  it("rejects a DNS result when any resolved address is private", async () => {
    lookupMock.mockResolvedValue([
      { address: "93.184.216.34", family: 4 },
      { address: "192.168.1.10", family: 4 },
    ]);
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": "https://signed.example/results.jsonl" } });
      }
      throw new Error("signed result fetch must not run");
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .rejects.toThrow(/private or special-use/);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("rejects private IP literals without performing a DNS lookup", async () => {
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": "https://[::1]/results.jsonl" } });
      }
      throw new Error("signed result fetch must not run");
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .rejects.toThrow(/private or special-use/);
    expect(lookupMock).not.toHaveBeenCalled();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fails closed when signed-result DNS resolution fails", async () => {
    lookupMock.mockRejectedValue(new Error("DNS unavailable"));
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": "https://signed.example/results.jsonl" } });
      }
      throw new Error("signed result fetch must not run");
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .rejects.toThrow(/DNS resolution failed/);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("rejects unsafe signed-result URL components before DNS or download", async () => {
    for (const signedUrl of [
      "http://signed.example/results.jsonl",
      "https://user:pass@signed.example/results.jsonl",
      "https://signed.example/results.jsonl#fragment",
    ]) {
      const fetchMock = vi.fn(async (input: string | URL | Request) => {
        if (String(input).endsWith(":getDownloadEndpoint")) {
          return jsonResponse({ filenameToSignedUrls: { "results.jsonl": signedUrl } });
        }
        throw new Error("signed result fetch must not run");
      });
      vi.stubGlobal("fetch", fetchMock);

      await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
        .rejects.toThrow(/not safe/);
      expect(fetchMock).toHaveBeenCalledTimes(1);
      vi.unstubAllGlobals();
    }
  });

  it("rejects redirects from signed-result downloads", async () => {
    const signedUrl = "https://signed.example/results.jsonl";
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": signedUrl } });
      }
      throw new Error("signed result fetch must use the pinned request");
    });
    stubPinnedRequests(() => ({
      body: "",
      status: 302,
      headers: { location: "https://other.example/results.jsonl" },
    }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .rejects.toThrow(/redirect was not allowed/);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(httpsRequestMock).toHaveBeenCalledTimes(1);
  });

  it("rejects a result file manifest above the configured file limit", async () => {
    const files = Object.fromEntries(Array.from({ length: 33 }, (_, index) => [
      `results-${index}.jsonl`,
      `https://signed.example/results-${index}.jsonl`,
    ]));
    const fetchMock = vi.fn(async () => jsonResponse({ filenameToSignedUrls: files }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).downloadResults("output-dataset"))
      .rejects.toThrow(/file count exceeded/);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(lookupMock).not.toHaveBeenCalled();
  });

  it("bounds concurrent signed-result downloads", async () => {
    const files = Object.fromEntries(Array.from({ length: 8 }, (_, index) => [
      `results-${index}.jsonl`,
      `https://signed.example/results-${index}.jsonl`,
    ]));
    let active = 0;
    let peak = 0;
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = String(input);
      if (url.endsWith(":getDownloadEndpoint")) return jsonResponse({ filenameToSignedUrls: files });
      throw new Error("signed result fetch must use the pinned request");
    });
    stubPinnedRequests(async (url) => {
      active += 1;
      peak = Math.max(peak, active);
      const delayMs = url.pathname.includes("results-0") ? 15 : 1;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      active -= 1;
      return { body: url.toString() };
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await new FireworksBatchClient({ ...config, maxConcurrency: 2 }).downloadResults("output-dataset");

    expect(result).toEqual({
      resultRows: Array.from({ length: 8 }, (_, index) => `https://signed.example/results-${index}.jsonl`).join("\n"),
      errorRows: undefined,
    });
    expect(peak).toBe(2);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(httpsRequestMock).toHaveBeenCalledTimes(8);
  });

  it("enforces the cumulative materialized character bound while streaming files", async () => {
    const files = {
      "results-a.jsonl": "https://signed.example/results-a.jsonl",
      "results-b.jsonl": "https://signed.example/results-b.jsonl",
    };
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: files });
      }
      throw new Error("signed result fetch must use the pinned request");
    });
    stubPinnedRequests((url) => ({ body: url.pathname.includes("results-a") ? "a".repeat(60) : "b".repeat(60) }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient({
      ...config,
      maxConcurrency: 1,
      maxResultChars: 100,
    }).downloadResults("output-dataset"))
      .rejects.toThrow(/materialized result exceeded configured limits/);
    expect(httpsRequestMock).toHaveBeenCalledTimes(2);
  });

  it("enforces the per-file byte bound during streaming materialization", async () => {
    const signedUrl = "https://signed.example/results.jsonl";
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      if (String(input).endsWith(":getDownloadEndpoint")) {
        return jsonResponse({ filenameToSignedUrls: { "results.jsonl": signedUrl } });
      }
      throw new Error("signed result fetch must use the pinned request");
    });
    stubPinnedRequests(() => ({ body: "1".repeat(101) }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient({
      ...config,
      maxResponseBytes: 100,
    }).downloadResults("output-dataset"))
      .rejects.toThrow(/response exceeded configured size limit/);
    expect(httpsRequestMock).toHaveBeenCalledTimes(1);
  });

  it("fails instead of completing a job-list checkpoint after unique page-token exhaustion", async () => {
    let page = 0;
    const fetchMock = vi.fn(async () => jsonResponse({
      batchInferenceJobs: [],
      nextPageToken: `unique-token-${page++}`,
    }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).listRecentJobIds(1))
      .rejects.toThrow(/page limit/);
    expect(fetchMock.mock.calls.length).toBe(32);
  });

  it("stops repeated job-list tokens without returning a false completion", async () => {
    const fetchMock = vi.fn(async () => jsonResponse({
      batchInferenceJobs: [],
      nextPageToken: "repeated-token",
    }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).listRecentJobIds(1))
      .rejects.toThrow(/repeated a page token/);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not fetch job-list pages when the requested limit is zero", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(new FireworksBatchClient(config).listRecentJobIds(0)).resolves.toEqual([]);
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
