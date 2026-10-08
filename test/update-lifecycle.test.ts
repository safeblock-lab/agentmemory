import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { UPDATE_HELPER_SOURCE } from "../src/update/helper-source.js";
import { checkForUpdate, downloadVerifiedRelease, isNewerRelease, verifyPackageArchive } from "../src/update/release.js";
import { getUpdateSupport, inspectSupervisorSupport } from "../src/update/lifecycle.js";
import { VERSION } from "../src/version.js";

const [major, minor, patch] = VERSION.split(".").map(Number);
const nextVersion = `${major}.${minor}.${patch + 1}`;
const nextTag = `v${nextVersion}`;
const assetName = `agentmemory-${nextTag}.tgz`;
const base = `https://github.com/safeblock-lab/agentmemory/releases/download/${nextTag}/`;

function packageArchive(version = nextVersion, name = "@agentmemory/agentmemory", includeCli = true): Buffer {
  const entries: Array<[string, Buffer]> = [
    ["package/package.json", Buffer.from(JSON.stringify({ name, version }))],
  ];
  if (includeCli) entries.push(["package/dist/cli.mjs", Buffer.from("export {};\n")]);
  const blocks: Buffer[] = [];
  for (const [path, contents] of entries) {
    const header = Buffer.alloc(512);
    header.write(path, 0, "utf8");
    header.write(contents.length.toString(8).padStart(11, "0") + "\0", 124, "ascii");
    header.fill(32, 148, 156);
    header[156] = 48;
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148, "ascii");
    blocks.push(header, contents, Buffer.alloc((512 - contents.length % 512) % 512));
  }
  blocks.push(Buffer.alloc(1024));
  return gzipSync(Buffer.concat(blocks));
}

function releaseResponse() {
  return new Response(JSON.stringify({
    tag_name: nextTag, draft: false, prerelease: false,
    assets: [assetName, "SHA256SUMS.txt"].map((name) => ({ name, browser_download_url: base + name })),
  }), { status: 200 });
}

async function stopFakeService(pidPath: string): Promise<void> {
  if (!existsSync(pidPath)) return;
  const pid = Number(readFileSync(pidPath, "utf8"));
  if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error("Invalid fake service PID");
  try {
    process.kill(pid, "SIGTERM");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
    throw error;
  }
  for (let attempt = 0; attempt < 100; attempt++) {
    try {
      process.kill(pid, 0);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
      throw error;
    }
    await delay(50);
  }
  throw new Error(`Fake service process ${pid} did not exit`);
}

async function runReadinessUpdate(options: {
  livez?: Array<{ viewerPort: number | null; viewerSkipped: boolean }>;
  versions: string[];
  engineStates: Array<"connected" | "disconnected">;
  workerConnectedAtMs?: Array<number | "now">;
  readyTimeoutMs?: number;
}) {
  const root = mkdtempSync(join(process.cwd(), "test", ".update-readiness-"));
  try {
    const jobDir = join(root, "job");
    const packageRoot = join(root, "agentmemory");
    const cliPath = join(packageRoot, "dist", "cli.mjs");
    const pidPath = join(root, "fake-service.pid");
    const tracePath = join(root, "readiness-trace.json");
    mkdirSync(jobDir);
    mkdirSync(join(packageRoot, "dist"), { recursive: true });
    writeFileSync(cliPath, `import { readFileSync, writeFileSync } from "node:fs";
      const path = ${JSON.stringify(pidPath)};
      if (process.argv.includes("stop")) { try { process.kill(Number(readFileSync(path, "utf8")), "SIGTERM"); } catch {} }
      else { writeFileSync(path, String(process.pid)); setInterval(() => {}, 1000); }`);
    writeFileSync(join(packageRoot, "package.json"), JSON.stringify({ name: "@agentmemory/agentmemory", version: "0.9.58" }));
    const npmCli = join(root, "fake-npm.mjs");
    writeFileSync(npmCli, `import { writeFileSync } from "node:fs"; writeFileSync(${JSON.stringify(join(packageRoot, "package.json"))}, JSON.stringify({name:"@agentmemory/agentmemory",version:"0.9.59"}));`);
    const tarballPath = join(jobDir, "release.tgz");
    const helperPath = join(jobDir, "helper.mjs");
    const wrapperPath = join(jobDir, "wrapper.mjs");
    const planPath = join(jobDir, "plan.json");
    const statusPath = join(root, "status.json");
    writeFileSync(tarballPath, "verified");
    writeFileSync(helperPath, UPDATE_HELPER_SOURCE);
    writeFileSync(planPath, JSON.stringify({
      jobId: "readiness", jobDir, packageRoot, cliPath, npmCli, tarballPath,
      sha256: createHash("sha256").update("verified").digest("hex"),
      targetVersion: "0.9.59", currentVersion: "0.9.58", statusPath,
      lockPath: join(root, "update.lock"), logPath: join(root, "update.log"),
      restPort: 3111, dataDir: join(root, "data"), readyTimeoutMs: options.readyTimeoutMs ?? 500,
    }));
    const responses = JSON.stringify({
      livez: options.livez ?? [{ viewerPort: 4317, viewerSkipped: false }],
      versions: options.versions,
      engineStates: options.engineStates,
      workerConnectedAtMs: options.workerConnectedAtMs ?? ["now"],
    });
    writeFileSync(wrapperPath, `import { createServer } from "node:net";
      import { readFileSync, writeFileSync } from "node:fs";
      const plan = JSON.parse(readFileSync(${JSON.stringify(planPath)}, "utf8"));
      const responses = ${responses};
      const counts = { livez: 0, versions: 0, health: 0 };
      const trace = [];
      const engine = createServer(() => trace.push("engine"));
      engine.listen(0, "127.0.0.1", async () => {
        const address = engine.address();
        process.env.III_ENGINE_URL = "ws://127.0.0.1:" + address.port;
        globalThis.fetch = async (input) => {
          const url = new URL(String(input));
          if (url.pathname === "/agentmemory/livez") {
            trace.push("livez");
            const item = responses.livez[Math.min(counts.livez++, responses.livez.length - 1)];
            return Response.json({ service: "agentmemory", ...item });
          }
          if (url.pathname === "/agentmemory/config/flags") {
            trace.push("versioned-api");
            const version = responses.versions[Math.min(counts.versions++, responses.versions.length - 1)];
            return Response.json({ version });
          }
          if (url.pathname === "/agentmemory/health") {
            trace.push("health");
            const healthIndex = counts.health++;
            const connectionState = responses.engineStates[Math.min(healthIndex, responses.engineStates.length - 1)];
            const configuredTimestamp = responses.workerConnectedAtMs[Math.min(healthIndex, responses.workerConnectedAtMs.length - 1)];
            const connectedAt = configuredTimestamp === "now" ? Date.now() : configuredTimestamp;
            return Response.json({
              version: healthIndex === 0 ? responses.versions[0] : "0.9.58",
              health: { connectionState, workers: [{ name: "agentmemory", status: connectionState, connected_at_ms: connectedAt }] },
            });
          }
          trace.push("viewer:" + url.port);
          return new Response("<html>AgentMemory</html>", { status: 200 });
        };
        await import(${JSON.stringify(pathToFileURL(helperPath).href)});
        writeFileSync(${JSON.stringify(tracePath)}, JSON.stringify(trace));
        engine.close();
      });`);
    const result = spawnSync(process.execPath, [wrapperPath, planPath], { encoding: "utf8", timeout: 8000 });
    const status = JSON.parse(readFileSync(statusPath, "utf8"));
    const trace = JSON.parse(readFileSync(tracePath, "utf8")) as string[];
    const log = existsSync(join(root, "update.log")) ? readFileSync(join(root, "update.log"), "utf8") : "";
    const installedVersion = existsSync(packageRoot)
      ? JSON.parse(readFileSync(join(packageRoot, "package.json"), "utf8")).version
      : "missing";
    return { result, status, trace, log, installedVersion };
  } finally {
    await stopFakeService(join(root, "fake-service.pid"));
    rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 50 });
  }
}

afterEach(() => vi.unstubAllGlobals());

describe("GitHub release update", () => {
  it("compares stable versions numerically and refuses invalid tags", () => {
    expect(isNewerRelease("v0.9.59", "0.9.58")).toBe(true);
    expect(isNewerRelease("v0.10.0", "0.9.58")).toBe(true);
    expect(isNewerRelease("v0.9.57", "0.9.58")).toBe(false);
    expect(isNewerRelease("v0.9.58", "0.9.58")).toBe(false);
    expect(() => isNewerRelease("v0.9.60-rc.1", "0.9.58")).toThrow("Unsupported");
  });

  it("accepts only the fixed release assets and matching SHA256", async () => {
    const tarball = packageArchive();
    const hash = createHash("sha256").update(tarball).digest("hex");
    const fetchMock = vi.fn(async (url: URL, options: RequestInit) => {
      expect(options.headers).toMatchObject({
        Accept: url.hostname === "api.github.com" ? "application/vnd.github+json" : "application/octet-stream",
      });
      if (url.hostname === "api.github.com") return releaseResponse();
      if (url.pathname.endsWith("SHA256SUMS.txt")) return new Response(`${hash}  ${assetName}\n`);
      return new Response(tarball);
    });
    vi.stubGlobal("fetch", fetchMock);
    expect(await checkForUpdate()).toMatchObject({ version: nextVersion, available: true });
    const result = await downloadVerifiedRelease();
    expect(result.sha256).toBe(hash);
    expect(result.tarball).toEqual(tarball);
    expect(fetchMock.mock.calls.every(([url]) => url.hostname === "api.github.com" || url.hostname === "github.com")).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it("rejects release metadata pointing to any other asset URL", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
      tag_name: nextTag, draft: false, prerelease: false,
      assets: [{ name: assetName, browser_download_url: "https://evil.example/package.tgz" }],
    }))));
    await expect(checkForUpdate()).rejects.toThrow("missing verified asset");
  });

  it("rejects a mismatched package checksum before installation", async () => {
    vi.stubGlobal("fetch", vi.fn(async (url: URL) => {
      if (url.hostname === "api.github.com") return releaseResponse();
      if (url.pathname.endsWith("SHA256SUMS.txt")) return new Response(`${"0".repeat(64)}  ${assetName}\n`);
      return new Response("wrong bytes");
    }));
    await expect(downloadVerifiedRelease()).rejects.toThrow("checksum verification failed");
  });

  it("rejects a correctly checksummed archive with the wrong package identity", async () => {
    const tarball = packageArchive(nextVersion, "unexpected-package");
    const hash = createHash("sha256").update(tarball).digest("hex");
    vi.stubGlobal("fetch", vi.fn(async (url: URL) => {
      if (url.hostname === "api.github.com") return releaseResponse();
      if (url.pathname.endsWith("SHA256SUMS.txt")) return new Response(`${hash}  ${assetName}\n`);
      return new Response(tarball);
    }));
    await expect(downloadVerifiedRelease()).rejects.toThrow("identity or version");
  });

  it("rejects a matching manifest without the AgentMemory CLI", () => {
    expect(() => verifyPackageArchive(packageArchive(nextVersion, "@agentmemory/agentmemory", false), nextVersion))
      .toThrow("identity or version");
  });

  it("rejects a package version that disagrees with the GitHub release", () => {
    expect(() => verifyPackageArchive(packageArchive(VERSION), nextVersion))
      .toThrow("identity or version");
  });

  it("refuses release redirects outside the GitHub asset hosts", async () => {
    vi.stubGlobal("fetch", vi.fn(async (url: URL) => url.hostname === "api.github.com"
      ? releaseResponse()
      : new Response(null, { status: 302, headers: { location: "https://evil.example/package.tgz" } })));
    await expect(downloadVerifiedRelease()).rejects.toThrow("untrusted host");
  });
});

describe("update lifecycle", () => {
  it("keeps the detached helper independent from the installed package", () => {
    const checked = spawnSync(process.execPath, ["--check", "--input-type=module"], {
      input: UPDATE_HELPER_SOURCE, encoding: "utf8",
    });
    expect(checked.status).toBe(0);
    expect(UPDATE_HELPER_SOURCE).not.toMatch(/from ["']\.\.?\//);
  });

  it("aborts before stopping the service when staged bytes are tampered", () => {
    const root = mkdtempSync(join(process.cwd(), "test", ".update-helper-"));
    try {
      const jobDir = join(root, "job");
      const packageRoot = join(root, "agentmemory");
      mkdirSync(jobDir);
      mkdirSync(packageRoot);
      writeFileSync(join(packageRoot, "package.json"), JSON.stringify({ version: "0.9.58" }));
      const helperPath = join(jobDir, "helper.mjs");
      const tarballPath = join(jobDir, "release.tgz");
      const planPath = join(jobDir, "plan.json");
      const lockPath = join(root, "update.lock");
      const statusPath = join(root, "update-status.json");
      writeFileSync(helperPath, UPDATE_HELPER_SOURCE);
      writeFileSync(tarballPath, "tampered");
      writeFileSync(lockPath, "owned");
      writeFileSync(planPath, JSON.stringify({
        jobId: "test-job", jobDir, packageRoot, npmCli: join(root, "unused-npm.js"),
        cliPath: join(packageRoot, "dist", "cli.mjs"), tarballPath,
        sha256: "0".repeat(64), targetVersion: "0.9.59", currentVersion: "0.9.58",
        statusPath, lockPath, logPath: join(root, "update.log"), restPort: 3111,
        dataDir: join(root, "data"),
      }));
      const result = spawnSync(process.execPath, [helperPath, planPath], { encoding: "utf8" });
      expect(result.status, result.stderr).toBe(0);
      expect(JSON.parse(readFileSync(statusPath, "utf8"))).toMatchObject({ phase: "failed" });
      expect(existsSync(packageRoot)).toBe(true);
      expect(existsSync(lockPath)).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("does not install while a supervised service has not acknowledged the pause", () => {
    const root = mkdtempSync(join(process.cwd(), "test", ".update-ack-"));
    try {
      const jobDir = join(root, "job");
      const packageRoot = join(root, "agentmemory");
      const cliPath = join(packageRoot, "dist", "cli.mjs");
      const stoppedPath = join(root, "stopped");
      const installedPath = join(root, "installed");
      mkdirSync(jobDir);
      mkdirSync(join(packageRoot, "dist"), { recursive: true });
      writeFileSync(cliPath, `import { writeFileSync } from "node:fs"; if (process.argv.includes("stop")) writeFileSync(${JSON.stringify(stoppedPath)}, "yes");`);
      writeFileSync(join(packageRoot, "package.json"), JSON.stringify({ name: "@agentmemory/agentmemory", version: "0.9.58" }));
      const npmCli = join(root, "fake-npm.mjs");
      writeFileSync(npmCli, `import { writeFileSync } from "node:fs"; writeFileSync(${JSON.stringify(installedPath)}, "yes");`);
      const tarballPath = join(jobDir, "release.tgz");
      const helperPath = join(jobDir, "helper.mjs");
      const planPath = join(jobDir, "plan.json");
      const statusPath = join(root, "status.json");
      writeFileSync(tarballPath, "verified");
      writeFileSync(helperPath, UPDATE_HELPER_SOURCE);
      writeFileSync(planPath, JSON.stringify({
        jobId: "ack-gate", jobDir, packageRoot, cliPath, npmCli, tarballPath,
        sha256: createHash("sha256").update("verified").digest("hex"),
        targetVersion: "0.9.59", currentVersion: "0.9.58", statusPath,
        lockPath: join(root, "update.lock"), logPath: join(root, "update.log"),
        restPort: 3111, dataDir: join(root, "data"), runtimeDir: root,
        supervised: true, supervisorAckTimeoutMs: 600, readyTimeoutMs: 500,
      }));
      const result = spawnSync(process.execPath, [helperPath, planPath], { encoding: "utf8", timeout: 5000 });
      expect(result.status, result.stderr).toBe(0);
      expect(existsSync(stoppedPath)).toBe(true);
      expect(existsSync(installedPath)).toBe(false);
      expect(JSON.parse(readFileSync(statusPath, "utf8"))).toMatchObject({ phase: "failed" });
      expect(existsSync(join(root, "update-maintenance.json"))).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("uses the supervisor to resume and does not launch a second CLI", () => {
    const root = mkdtempSync(join(process.cwd(), "test", ".update-supervised-"));
    try {
      const jobDir = join(root, "job");
      const packageRoot = join(root, "agentmemory");
      const cliPath = join(packageRoot, "dist", "cli.mjs");
      const startedPath = join(root, "started-by-helper");
      const argsPath = join(root, "npm-args.json");
      mkdirSync(jobDir);
      mkdirSync(join(packageRoot, "dist"), { recursive: true });
      writeFileSync(cliPath, `import { readFileSync, writeFileSync } from "node:fs";
        if (process.argv.includes("stop")) {
          const marker = JSON.parse(readFileSync(${JSON.stringify(join(root, "update-maintenance.json"))}, "utf8"));
          writeFileSync(${JSON.stringify(join(root, "update-supervisor-ack.json"))}, JSON.stringify({jobId:marker.jobId,generation:marker.generation}));
        } else writeFileSync(${JSON.stringify(startedPath)}, "yes");`);
      writeFileSync(join(packageRoot, "package.json"), JSON.stringify({ name: "@agentmemory/agentmemory", version: "0.9.58" }));
      const npmCli = join(root, "fake-npm.mjs");
      writeFileSync(npmCli, `import { writeFileSync } from "node:fs";
        writeFileSync(${JSON.stringify(argsPath)}, JSON.stringify(process.argv));
        writeFileSync(${JSON.stringify(join(packageRoot, "package.json"))}, JSON.stringify({name:"@agentmemory/agentmemory",version:"0.9.59"}));`);
      const tarballPath = join(jobDir, "release.tgz");
      const helperPath = join(jobDir, "helper.mjs");
      const wrapperPath = join(jobDir, "wrapper.mjs");
      const planPath = join(jobDir, "plan.json");
      const statusPath = join(root, "status.json");
      writeFileSync(tarballPath, "verified");
      writeFileSync(helperPath, UPDATE_HELPER_SOURCE);
      writeFileSync(wrapperPath, `process.env.III_ENGINE_URL = "ws://engine.example:49134";
        globalThis.fetch = async (input) => {
          const url = new URL(String(input));
          if (url.pathname.endsWith("livez")) return Response.json({service:"agentmemory",viewerPort:4321,viewerSkipped:false});
          if (url.pathname.endsWith("config/flags")) return Response.json({version:"0.9.59"});
          if (url.pathname.endsWith("health")) return Response.json({version:"0.9.59",health:{connectionState:"connected",workers:[{name:"agentmemory",status:"connected",connected_at_ms:Date.now()}]}});
          return new Response("<html>AgentMemory</html>", {status:200});
        };
        await import(${JSON.stringify(pathToFileURL(helperPath).href)});`);
      writeFileSync(planPath, JSON.stringify({
        jobId: "supervised-success", jobDir, packageRoot, cliPath, npmCli, tarballPath,
        sha256: createHash("sha256").update("verified").digest("hex"),
        targetVersion: "0.9.59", currentVersion: "0.9.58", statusPath,
        lockPath: join(root, "update.lock"), logPath: join(root, "update.log"),
        restPort: 3111, dataDir: join(root, "data"), runtimeDir: root,
        supervised: true, readyTimeoutMs: 1000,
      }));
      const result = spawnSync(process.execPath, [wrapperPath, planPath], { encoding: "utf8", timeout: 5000 });
      expect(result.status, result.stderr).toBe(0);
      expect(JSON.parse(readFileSync(statusPath, "utf8"))).toMatchObject({ phase: "complete" });
      expect(existsSync(startedPath)).toBe(false);
      expect(JSON.parse(readFileSync(argsPath, "utf8"))).toContain("--ignore-scripts");
      expect(existsSync(join(root, "update-maintenance.json"))).toBe(false);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("tries to restart the old CLI if backing it up fails after stop", () => {
    const root = mkdtempSync(join(process.cwd(), "test", ".update-backup-"));
    try {
      const jobDir = join(root, "job");
      const packageRoot = join(root, "agentmemory");
      const cliPath = join(packageRoot, "dist", "cli.mjs");
      const restartedPath = join(root, "restart-attempted");
      const jobId = "backup-failure";
      mkdirSync(jobDir);
      mkdirSync(join(packageRoot, "dist"), { recursive: true });
      writeFileSync(cliPath, `import { writeFileSync } from "node:fs"; if (!process.argv.includes("stop")) writeFileSync(${JSON.stringify(restartedPath)}, "yes");`);
      writeFileSync(join(root, `.agentmemory-update-backup-${jobId}`), "block backup directory");
      const tarballPath = join(jobDir, "release.tgz");
      const helperPath = join(jobDir, "helper.mjs");
      const planPath = join(jobDir, "plan.json");
      const statusPath = join(root, "status.json");
      writeFileSync(tarballPath, "verified");
      writeFileSync(helperPath, UPDATE_HELPER_SOURCE);
      writeFileSync(planPath, JSON.stringify({
        jobId, jobDir, packageRoot, cliPath, npmCli: join(root, "unused-npm.js"),
        tarballPath, sha256: createHash("sha256").update("verified").digest("hex"),
        targetVersion: "0.9.59", currentVersion: "0.9.58", statusPath,
        lockPath: join(root, "update.lock"), logPath: join(root, "update.log"),
        restPort: 10000, dataDir: join(root, "data"), readyTimeoutMs: 1000,
      }));
      spawnSync(process.execPath, [helperPath, planPath], { encoding: "utf8" });
      expect(existsSync(restartedPath)).toBe(true);
      expect(JSON.parse(readFileSync(statusPath, "utf8")).phase).toBe("failed");
      expect(existsSync(packageRoot)).toBe(true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  it("waits for the advertised fallback viewer port before completing", async () => {
    const update = await runReadinessUpdate({
      livez: [{ viewerPort: 4327, viewerSkipped: false }],
      versions: ["0.9.59"], engineStates: ["connected"],
    });
    expect(update.result.status, update.result.stderr).toBe(0);
    expect(update.status.phase).toBe("complete");
    expect(update.trace).toContain("viewer:4327");
    expect(update.trace.indexOf("engine")).toBeLessThan(update.trace.indexOf("livez"));
    expect(update.trace.indexOf("livez")).toBeLessThan(update.trace.indexOf("versioned-api"));
    expect(update.trace.indexOf("versioned-api")).toBeLessThan(update.trace.indexOf("health"));
    expect(update.trace.indexOf("health")).toBeLessThan(update.trace.indexOf("viewer:4327"));
  });

  it("rolls back when an open engine socket has no connected worker health", async () => {
    const update = await runReadinessUpdate({
      versions: ["0.9.59", "0.9.58"], engineStates: ["disconnected", "connected"],
    });
    expect(update.status).toMatchObject({ phase: "failed", errorCode: "UPDATE_FAILED" });
    expect(update.status.message).toContain("previous version was restored and restarted");
    expect(update.installedVersion).toBe("0.9.58");
    expect(update.trace).toContain("engine");
  });

  it("does not accept a persisted connected snapshot from before the restart", async () => {
    const update = await runReadinessUpdate({
      versions: ["0.9.59", "0.9.58"],
      engineStates: ["connected", "connected"],
      workerConnectedAtMs: [Date.now() - 60_000, "now"],
    });
    expect(update.status).toMatchObject({ phase: "failed", errorCode: "UPDATE_FAILED" });
    expect(update.status.message).toContain("previous version was restored and restarted");
    expect(update.installedVersion).toBe("0.9.58");
    expect(update.trace.filter((entry) => entry === "health")).toHaveLength(2);
  });

  it("rejects a stale viewer port when the worker says its viewer was skipped", async () => {
    const update = await runReadinessUpdate({
      livez: [
        { viewerPort: 4327, viewerSkipped: true },
        { viewerPort: 4327, viewerSkipped: false },
      ],
      versions: ["0.9.58"], engineStates: ["connected"],
    });
    expect(update.status).toMatchObject({ phase: "failed", errorCode: "UPDATE_FAILED" });
    expect(update.status.message).toContain("previous version was restored and restarted");
    expect(update.log).toContain("started without a viewer");
    expect(update.installedVersion).toBe("0.9.58");
  });

  it("rolls back when readiness times out on the wrong API version", async () => {
    const update = await runReadinessUpdate({
      versions: ["0.9.58"], engineStates: ["connected"], readyTimeoutMs: 300,
    });
    expect(update.status).toMatchObject({ phase: "failed", errorCode: "UPDATE_FAILED" });
    expect(update.status.message).toContain("previous version was restored and restarted");
    expect(update.installedVersion).toBe("0.9.58");
    expect(update.trace).toContain("viewer:4317");
  });

  it("refuses a source checkout or unsupported host without triggering installation", () => {
    const support = getUpdateSupport();
    expect(support.supported).toBe(false);
  });

  it("does not depend on a dedicated update secret", () => {
    const original = process.env.AGENTMEMORY_UPDATE_SECRET;
    try {
      delete process.env.AGENTMEMORY_UPDATE_SECRET;
      const support = getUpdateSupport();
      process.env.AGENTMEMORY_UPDATE_SECRET = "too-short";
      expect(getUpdateSupport()).toEqual(support);
      expect(support.reason ?? "").not.toContain("AGENTMEMORY_UPDATE_SECRET");
    } finally {
      if (original === undefined) delete process.env.AGENTMEMORY_UPDATE_SECRET;
      else process.env.AGENTMEMORY_UPDATE_SECRET = original;
    }
  });

  it("requires both local supervisor scripts to opt into maintenance coordination", () => {
    const root = mkdtempSync(join(process.cwd(), "test", ".update-supervisor-support-"));
    try {
      expect(inspectSupervisorSupport(root)).toBe(false);
      writeFileSync(join(root, "Start-AgentMemory.ps1"), "# legacy supervisor\n");
      expect(() => inspectSupervisorSupport(root)).toThrow("maintenance protocol");
      writeFileSync(join(root, "Start-AgentMemory.ps1"), "# AgentMemory updater protocol v1\n");
      writeFileSync(join(root, "Invoke-AgentMemoryWatchdog.ps1"), "# legacy watchdog\n");
      expect(() => inspectSupervisorSupport(root)).toThrow("maintenance protocol");
      writeFileSync(join(root, "Invoke-AgentMemoryWatchdog.ps1"), "# AgentMemory updater protocol v1\n");
      expect(inspectSupervisorSupport(root)).toBe(true);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
