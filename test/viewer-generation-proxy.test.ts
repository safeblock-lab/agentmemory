import { request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { startViewerServer } from "../src/viewer/server.js";

interface ViewerResponse {
  statusCode: number;
  body: string;
}

const restPort = 49133;
const secret = "viewer-proxy-test-secret";
const originalViewerHost = process.env.AGENTMEMORY_VIEWER_HOST;
const originalAllowedHosts = process.env.VIEWER_ALLOWED_HOSTS;

let viewer: Server | undefined;
let viewerPort: number;

function requestViewer(
  method: string,
  path: string,
  body?: string,
): Promise<ViewerResponse> {
  return new Promise((resolve, reject) => {
    const headers: Record<string, string> = {};
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      headers["Content-Length"] = String(Buffer.byteLength(body));
    }

    const request = httpRequest(
      {
        host: "127.0.0.1",
        port: viewerPort,
        method,
        path,
        headers,
      },
      (response) => {
        const chunks: Buffer[] = [];
        response.on("data", (chunk: Buffer | string) => {
          chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        response.on("end", () => {
          resolve({
            statusCode: response.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
          });
        });
      },
    );
    request.on("error", reject);
    if (body !== undefined) request.write(body);
    request.end();
  });
}

function abortError(): Error {
  const error = new Error("The operation was aborted.");
  error.name = "AbortError";
  return error;
}

describe("viewer generation proxy timeout", () => {
  beforeEach(async () => {
    process.env.AGENTMEMORY_VIEWER_HOST = "127.0.0.1";
    delete process.env.VIEWER_ALLOWED_HOSTS;
    vi.spyOn(console, "log").mockImplementation(() => {});
    viewer = startViewerServer(0, null, null, secret, restPort);
    await new Promise<void>((resolve) => viewer!.once("listening", resolve));
    viewerPort = (viewer!.address() as AddressInfo).port;
  });

  afterEach(async () => {
    vi.useRealTimers();
    if (viewer) {
      const current = viewer;
      viewer = undefined;
      await new Promise<void>((resolve) => current.close(() => resolve()));
    }
    vi.restoreAllMocks();
    if (originalViewerHost === undefined) {
      delete process.env.AGENTMEMORY_VIEWER_HOST;
    } else {
      process.env.AGENTMEMORY_VIEWER_HOST = originalViewerHost;
    }
    if (originalAllowedHosts === undefined) {
      delete process.env.VIEWER_ALLOWED_HOSTS;
    } else {
      process.env.VIEWER_ALLOWED_HOSTS = originalAllowedHosts;
    }
  });

  it("lets manual summary finish after ten seconds and forwards request identity", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let markFetchStarted!: () => void;
    const fetchStarted = new Promise<void>((resolve) => {
      markFetchStarted = resolve;
    });
    let upstreamUrl = "";
    let upstreamInit: RequestInit | undefined;
    const requestBody = JSON.stringify({
      sessionId: "session-1",
      continuation: {
        jobId: "job-1",
        step: 1,
        unitId: "unit-1",
        deliveryId: "delivery-1",
      },
    });

    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation((input, init) => {
      upstreamUrl = String(input);
      upstreamInit = init;
      markFetchStarted();
      return new Promise<Response>((resolve, reject) => {
        const responseTimer = setTimeout(
          () => resolve(new Response('{"accepted":true}', {
            status: 201,
            headers: { "Content-Type": "application/json" },
          })),
          10_050,
        );
        init?.signal?.addEventListener(
          "abort",
          () => {
            clearTimeout(responseTimer);
            reject(abortError());
          },
          { once: true },
        );
      });
    });

    const responsePromise = requestViewer(
      "POST",
      "/summarize?source=viewer",
      requestBody,
    );
    await fetchStarted;
    await vi.advanceTimersByTimeAsync(10_050);
    const response = await responsePromise;

    expect(response).toEqual({ statusCode: 201, body: '{"accepted":true}' });
    expect(upstreamUrl).toBe(
      `http://127.0.0.1:${restPort}/agentmemory/summarize?source=viewer`,
    );
    expect(upstreamInit?.method).toBe("POST");
    expect(upstreamInit?.headers).toEqual({
      Authorization: `Bearer ${secret}`,
      "Content-Type": "application/json",
    });
    expect(upstreamInit?.body).toBe(requestBody);
    expect(upstreamInit?.signal?.aborted).toBe(false);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("extends only the exact generation POST paths", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const timers = vi.spyOn(globalThis, "setTimeout");
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async () => new Response("ok", { status: 200 }));
    const cases = [
      ["POST", "/summarize", 360_000],
      ["POST", "/agentmemory/reflect", 180_000],
      ["POST", "/agentmemory/consolidate", 180_000],
      ["POST", "/agentmemory/graph/extract", 180_000],
      ["POST", "/agentmemory/graph/build", 180_000],
      ["POST", "/agentmemory/consolidate-pipeline", 180_000],
      ["POST", "/agentmemory/slot/reflect", 180_000],
      ["POST", "/agentmemory/graph/query", 10_000],
      ["POST", "/agentmemory/graph/reset", 10_000],
      ["GET", "/agentmemory/summarize", 10_000],
      ["POST", "/agentmemory/summarize/extra", 10_000],
    ] as const;

    for (const [method, path, expectedTimeout] of cases) {
      timers.mockClear();
      const response = await requestViewer(
        method,
        path,
        method === "POST" ? "{}" : undefined,
      );
      expect(response.statusCode).toBe(200);
      const proxyTimeouts = timers.mock.calls
        .map(([, delay]) => delay)
        .filter((delay) => delay === 10_000 || delay === 180_000 || delay === 360_000);
      expect(proxyTimeouts).toEqual([expectedTimeout]);
    }

    expect(fetchSpy).toHaveBeenCalledTimes(cases.length);
  });

  it("maps one bounded generation timeout to 504 without retrying", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let markFetchStarted!: () => void;
    const fetchStarted = new Promise<void>((resolve) => {
      markFetchStarted = resolve;
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation((_input, init) => {
      markFetchStarted();
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(abortError()), {
          once: true,
        });
      });
    });

    const responsePromise = requestViewer("POST", "/reflect", "{}");
    await fetchStarted;
    await vi.advanceTimersByTimeAsync(180_000);
    const response = await responsePromise;

    expect(response).toEqual({
      statusCode: 504,
      body: '{"error":"upstream timeout"}',
    });
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("keeps upstream failures as a single 502 response", async () => {
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockRejectedValue(new TypeError("upstream unavailable"));

    const response = await requestViewer("POST", "/summarize", "{}");

    expect(response).toEqual({
      statusCode: 502,
      body: '{"error":"upstream error"}',
    });
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});
