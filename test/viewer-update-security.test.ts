import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";

const updater = vi.hoisted(() => ({
  getUpdateSupport: vi.fn(),
  checkForUpdate: vi.fn(),
  startReleaseUpdate: vi.fn(),
  readUpdateStatus: vi.fn(),
}));

vi.mock("../src/update/lifecycle.js", () => updater);

import { startViewerServer } from "../src/viewer/server.js";

type Reply = { status: number; body: Record<string, unknown> | string };

describe("viewer update routes", () => {
  const originalHost = process.env.AGENTMEMORY_VIEWER_HOST;
  const originalAllowedHosts = process.env.VIEWER_ALLOWED_HOSTS;
  const originalUpdateSecret = process.env.AGENTMEMORY_UPDATE_SECRET;
  const updateSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  let server: Server | undefined;
  let port = 0;
  let logSpy: ReturnType<typeof vi.spyOn>;

  async function listen(bindHost = "127.0.0.1"): Promise<void> {
    process.env.AGENTMEMORY_VIEWER_HOST = bindHost;
    if (bindHost !== "127.0.0.1") {
      process.env.VIEWER_ALLOWED_HOSTS = "viewer.test";
    }
    server = startViewerServer(0, null, null, bindHost === "127.0.0.1" ? undefined : "test-secret", 0);
    if (!server.listening) await new Promise<void>((resolve) => server!.once("listening", resolve));
    port = (server.address() as AddressInfo).port;
  }

  function request(
    path: string,
    method = "GET",
    headers: Record<string, string> = {},
    body?: string,
  ): Promise<Reply> {
    return new Promise((resolve, reject) => {
      const req = httpRequest({
        hostname: "127.0.0.1",
        port,
        path,
        method,
        headers: {
          Host: `127.0.0.1:${port}`,
          ...headers,
          ...(body === undefined ? {} : { "Content-Length": String(Buffer.byteLength(body)) }),
        },
      }, (res) => {
        let text = "";
        res.on("data", (chunk: Buffer) => { text += chunk.toString(); });
        res.on("end", () => {
          let parsed: Reply["body"] = text;
          try { parsed = JSON.parse(text) as Record<string, unknown>; } catch {}
          resolve({ status: res.statusCode ?? 0, body: parsed });
        });
      });
      req.on("error", reject);
      req.end(body);
    });
  }

  async function token(): Promise<string> {
    const status = await request("/update/status");
    expect(status.status).toBe(200);
    expect(typeof status.body).toBe("object");
    return (status.body as Record<string, unknown>).csrfToken as string;
  }

  beforeEach(() => {
    process.env.AGENTMEMORY_UPDATE_SECRET = updateSecret;
    updater.getUpdateSupport.mockReset().mockReturnValue({ supported: true });
    updater.checkForUpdate.mockReset().mockResolvedValue({
      currentVersion: "0.9.58", version: "0.9.59", tag: "v0.9.59", available: true,
    });
    updater.startReleaseUpdate.mockReset().mockResolvedValue({
      jobId: "test-job", phase: "downloading", currentVersion: "0.9.58",
    });
    updater.readUpdateStatus.mockReset().mockReturnValue({ phase: "idle", currentVersion: "0.9.58" });
    logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
  });

  afterEach(async () => {
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
    server = undefined;
    logSpy.mockRestore();
    if (originalHost === undefined) delete process.env.AGENTMEMORY_VIEWER_HOST;
    else process.env.AGENTMEMORY_VIEWER_HOST = originalHost;
    if (originalAllowedHosts === undefined) delete process.env.VIEWER_ALLOWED_HOSTS;
    else process.env.VIEWER_ALLOWED_HOSTS = originalAllowedHosts;
    if (originalUpdateSecret === undefined) delete process.env.AGENTMEMORY_UPDATE_SECRET;
    else process.env.AGENTMEMORY_UPDATE_SECRET = originalUpdateSecret;
  });

  it("allows local updater reads without a password and requires a token to start", async () => {
    delete process.env.AGENTMEMORY_UPDATE_SECRET;
    await listen();
    for (const path of ["/update/support", "/update/check", "/update/status"]) {
      expect((await request(path)).status).toBe(200);
    }
    expect((await request("/update/start", "POST", { Origin: `http://127.0.0.1:${port}` })).status).toBe(403);
    expect(updater.startReleaseUpdate).not.toHaveBeenCalled();
  });

  it("returns local status and a token without touching the provider", async () => {
    await listen();
    const response = await request("/update/status");
    expect(response).toMatchObject({ status: 200, body: {
      support: { supported: true },
      status: { phase: "idle", currentVersion: "0.9.58" },
      csrfToken: expect.stringMatching(/^[0-9a-f]{64}$/),
    } });
    expect(updater.checkForUpdate).not.toHaveBeenCalled();
    expect(updater.startReleaseUpdate).not.toHaveBeenCalled();
  });

  it("checks the release only on explicit request and reports unsupported installs", async () => {
    await listen();
    expect(await request("/update/check")).toMatchObject({ status: 200, body: { available: true } });
    expect(updater.checkForUpdate).toHaveBeenCalledTimes(1);
    updater.getUpdateSupport.mockReturnValue({ supported: false, reason: "Unsupported installation." });
    expect(await request("/update/check")).toEqual({ status: 409, body: { error: "Unsupported installation." } });
    expect((await request("/update/status")).body).not.toHaveProperty("csrfToken");
    expect(updater.checkForUpdate).toHaveBeenCalledTimes(1);
  });

  it("rejects cross-origin, missing-origin, invalid-token and request-body starts", async () => {
    await listen();
    const csrfToken = await token();
    const headers = { "X-AgentMemory-Update-Token": csrfToken };
    expect((await request("/update/start", "POST", headers)).status).toBe(403);
    expect((await request("/update/start", "POST", { ...headers, Origin: "http://evil.test" })).status).toBe(403);
    expect((await request("/update/start", "POST", { ...headers, Origin: `http://127.0.0.1:${port + 1}` })).status).toBe(403);
    expect((await request("/update/start", "POST", { Origin: `http://127.0.0.1:${port}`, "X-AgentMemory-Update-Token": "wrong" })).status).toBe(403);
    expect((await request("/update/start", "POST", { ...headers, Origin: `http://127.0.0.1:${port}` }, "{}")).status).toBe(400);
    expect(updater.startReleaseUpdate).not.toHaveBeenCalled();
  });

  it("starts once, consumes the token and rejects replay", async () => {
    await listen();
    const csrfToken = await token();
    const headers = { Origin: `http://127.0.0.1:${port}`, "X-AgentMemory-Update-Token": csrfToken };
    expect(await request("/update/start", "POST", headers)).toMatchObject({ status: 202, body: { jobId: "test-job" } });
    expect((await request("/update/start", "POST", headers)).status).toBe(403);
    expect(updater.startReleaseUpdate).toHaveBeenCalledTimes(1);
  });

  it("rejects concurrent starts even with a fresh token", async () => {
    await listen();
    let finish!: (value: unknown) => void;
    updater.startReleaseUpdate.mockImplementation(() => new Promise((resolve) => { finish = resolve; }));
    const firstToken = await token();
    const origin = `http://127.0.0.1:${port}`;
    const first = request("/update/start", "POST", { Origin: origin, "X-AgentMemory-Update-Token": firstToken });
    await vi.waitFor(() => expect(updater.startReleaseUpdate).toHaveBeenCalledTimes(1));
    const freshToken = await token();
    expect((await request("/update/start", "POST", { Origin: origin, "X-AgentMemory-Update-Token": freshToken })).status).toBe(409);
    finish({ jobId: "test-job", phase: "downloading", currentVersion: "0.9.58" });
    expect((await first).status).toBe(202);
    expect(updater.startReleaseUpdate).toHaveBeenCalledTimes(1);
  });

  it("denies all update routes when the viewer is bound beyond loopback", async () => {
    await listen("0.0.0.0");
    for (const [path, method] of [["/update/support", "GET"], ["/update/status", "GET"], ["/update/check", "GET"], ["/update/start", "POST"]]) {
      const response = await request(path, method, { Host: "viewer.test", Origin: "http://viewer.test" });
      expect(response.status).toBe(403);
    }
    expect(updater.checkForUpdate).not.toHaveBeenCalled();
    expect(updater.startReleaseUpdate).not.toHaveBeenCalled();
  });

  it("rejects forged Host before exposing status or checking GitHub", async () => {
    await listen();
    const response = await request("/update/status", "GET", { Host: `evil.test:${port}` });
    expect(response.status).toBe(403);
    expect(updater.readUpdateStatus).not.toHaveBeenCalled();
  });

  it("does not let a cross-site browser trigger the release check", async () => {
    await listen();
    const response = await request("/update/check", "GET", { "Sec-Fetch-Site": "cross-site" });
    expect(response.status).toBe(403);
    expect(updater.checkForUpdate).not.toHaveBeenCalled();
  });

  it("turns provider and installer errors into sanitized responses", async () => {
    await listen();
    updater.checkForUpdate.mockRejectedValue(new Error("private upstream detail"));
    expect(await request("/update/check")).toEqual({ status: 502, body: { error: "Unable to check for updates." } });
    updater.startReleaseUpdate.mockRejectedValue(new Error("private installer detail"));
    const csrfToken = await token();
    expect(await request("/update/start", "POST", { Origin: `http://127.0.0.1:${port}`, "X-AgentMemory-Update-Token": csrfToken })).toEqual({
      status: 500, body: { error: "Unable to start the update." },
    });
  });

  it("reports an updater lock conflict without starting a second job", async () => {
    await listen();
    updater.startReleaseUpdate.mockRejectedValue(new Error("An AgentMemory update is already in progress."));
    const csrfToken = await token();
    expect(await request("/update/start", "POST", { Origin: `http://127.0.0.1:${port}`, "X-AgentMemory-Update-Token": csrfToken })).toEqual({
      status: 409, body: { error: "An AgentMemory update is already in progress." },
    });
  });
});
