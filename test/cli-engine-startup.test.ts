import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";

describe("fresh native engine startup", () => {
  const source = readFileSync("src/cli.ts", "utf8").replace(/\r\n/g, "\n");

  it("routes existing and newly installed engines through one runtime config", () => {
    const prepareStart = source.indexOf("function prepareEngineLaunch");
    const prepareEnd = source.indexOf("function startIiiBin", prepareStart);
    const prepareBody = source.slice(prepareStart, prepareEnd);
    expect(prepareBody).toContain("dataDir: dataDirResolution.dataDir");
    expect(prepareBody).toContain("restPort: getRestPort()");
    expect(prepareBody).toContain("streamPort: getStreamPort()");
    expect(prepareBody).toContain("viewerPort: getConfiguredViewerPort()");
    expect(prepareBody).toContain("enginePort: getEnginePort()");

    const engineStart = source.indexOf("async function startEngine");
    const engineEnd = source.indexOf("async function waitForEngine", engineStart);
    const engineBody = source.slice(engineStart, engineEnd);
    expect(engineBody).not.toContain("writeRuntimeIiiConfig");
    expect(engineBody.match(/startIiiBinOnce\(iiiBin, configPath\)/g)).toHaveLength(2);
  });

  it("reports buffered engine stderr when a verbose startup times out", () => {
    const spawnStart = source.indexOf("function spawnEngineBackground");
    const spawnEnd = source.indexOf("function prepareEngineLaunch", spawnStart);
    const spawnBody = source.slice(spawnStart, spawnEnd);
    expect(spawnBody).toContain(
      "activeStartupStderr = stderrCapture",
    );
    expect(spawnBody).toContain("stderrCapture.append(chunk)");

    const timeoutStart = source.indexOf("const ready = await waitForEngine(ENGINE_READINESS_TIMEOUT_MS)");
    const timeoutEnd = source.indexOf('s.stop(c.ok("iii-engine is ready"))', timeoutStart);
    expect(source.slice(timeoutStart, timeoutEnd)).toContain(
      "printCapturedStartupStderr()",
    );

    const demoTimeoutStart = source.indexOf("await waitForEngine(ENGINE_READINESS_TIMEOUT_MS)", timeoutEnd);
    const demoTimeoutEnd = source.indexOf("await import(\"./index.js\")", demoTimeoutStart);
    expect(source.slice(demoTimeoutStart, demoTimeoutEnd)).toContain(
      "printCapturedStartupStderr()",
    );
  });

  it("exports the complete derived port set for Docker and native launchers", () => {
    expect(source).toContain(
      'process.env["III_STREAM_PORT"] ??=\n    process.env["III_STREAMS_PORT"] ?? String(restPort + 1)',
    );
    expect(source).toContain(
      'process.env["III_VIEWER_PORT"] ??= String(restPort + 2)',
    );
    expect(source).toContain(
      'process.env["III_ENGINE_PORT"] ??= String(restPort + 46023)',
    );
    expect(source).toContain("new URL(configuredEngineUrl).port");
    expect(source).toContain(
      'process.env["III_ENGINE_PORT"] = parsedEnginePort',
    );
    expect(source).toContain(
      'process.env["AGENTMEMORY_METRICS_PORT"] ??= String(restPort + 6353)',
    );
    expect(source).toContain(
      'process.env["AGENTMEMORY_DATA_DIR"] = dataDirResolution.dataDir',
    );
  });

  it("exports the default REST port for updater ownership checks while preserving custom ports", () => {
    const start = source.indexOf('const restPort = parseInt(process.env["III_REST_PORT"] || "3111", 10);');
    const end = source.indexOf("const dataDirResolution =", start);
    expect(start).toBeGreaterThanOrEqual(0);
    expect(end).toBeGreaterThan(start);
    const bootstrap = new Function("process", `${source.slice(start, end)}\nreturn process.env;`) as
      (context: { env: Record<string, string> }) => Record<string, string>;
    expect(bootstrap({ env: {} })).toMatchObject({
      III_REST_PORT: "3111", III_STREAM_PORT: "3112", III_VIEWER_PORT: "3113", III_ENGINE_PORT: "49134",
    });
    expect(bootstrap({ env: { III_REST_PORT: "3211", III_STREAM_PORT: "9000" } })).toMatchObject({
      III_REST_PORT: "3211", III_STREAM_PORT: "9000", III_VIEWER_PORT: "3213", III_ENGINE_PORT: "49234",
    });
  });

  it("installs only the bundled engine after checksum, version, and capability checks", () => {
    const installerStart = source.indexOf("async function runIiiInstaller");
    const installerEnd = source.indexOf("type StartupFailure", installerStart);
    const installerBody = source.slice(installerStart, installerEnd);
    expect(installerBody).toContain("resolveBundledEngineArtifact(");
    expect(installerBody).toContain("iiiBinVersion(temporaryPath) !== IIPINNED_VERSION");
    expect(installerBody).toContain("iiiHasRequiredStateCapabilities(temporaryPath)");
    expect(installerBody).toContain("renameSync(temporaryPath, binPath)");
    expect(installerBody).not.toContain("curl");
    expect(installerBody).not.toContain("install.sh");

    expect(source).not.toContain("iiiReleaseUrl");
    expect(source).not.toContain("IIIENGINE_INSTALL_CMD");
  });

  it("lets the CLI own one bundled worker and reuses a live worker", () => {
    const prepareStart = source.indexOf("function prepareEngineLaunch");
    const prepareEnd = source.indexOf("function startIiiBin", prepareStart);
    const prepareBody = source.slice(prepareStart, prepareEnd);
    expect(prepareBody).toContain(
      "rewriteBundledConfig(",
    );
    expect(prepareBody).toContain(": renderEngineConfig(rawConfig, options)");

    const workerStart = source.indexOf("async function startWorkerForEngineState");
    const workerEnd = source.indexOf("async function startEngine", workerStart);
    const workerBody = source.slice(workerStart, workerEnd);
    expect(workerBody).toContain("readWorkerPidfile()");
    expect(workerBody).toContain("pidAlive(workerPid)");
    expect(workerBody).toContain("waitForConfiguredWorker");
    expect(workerBody).toContain('await import("./index.js")');

    expect(source.match(/await startWorkerForEngineState\(\)/g)).toHaveLength(3);
    expect(source).toContain("agentmemory worker did not become ready within 15 minutes");
  });

  it("stores lifecycle metadata per instance and scopes Docker ownership checks", () => {
    expect(source).toContain('runtimeMetadataPath("iii.pid")');
    expect(source).toContain('runtimeMetadataPath("engine-state.json")');
    expect(source).toContain('runtimeMetadataPath("worker.pid")');
    expect(source).toContain('process.env["AGENTMEMORY_RUNTIME_DIR"] = selectedInstance > 0');

    const dockerStart = source.indexOf("function inspectOwnedDockerEngine");
    const dockerEnd = source.indexOf("function ", dockerStart + 1);
    const dockerOwnershipBody = source.slice(dockerStart, dockerEnd);
    expect(dockerOwnershipBody).toContain("dockerProjectName(ownerPort)");
    expect(dockerOwnershipBody).toContain("dockerComposeArgs(");
  });

  it("keeps AGENTMEMORY_URL client-only and lets local port flags win", () => {
    expect(source).toContain('const URL_CLIENT_COMMANDS = new Set(["status", "doctor", "mcp"])');
    expect(source).toContain("hasExplicitLocalPortOverride");
    expect(source).toContain("shouldUseAgentmemoryUrl()");
    expect(source).toContain('process.env["III_REST_PORT"] = String(base)');
  });

  it("rejects an unverified persisted Docker engine before container inspection or startup", () => {
    const mainStart = source.indexOf("async function main()");
    const mainEnd = source.indexOf("async function apiFetch", mainStart);
    const mainBody = source.slice(mainStart, mainEnd);
    expect(mainBody).toContain("reconcilePersistedDockerEngine()");
    expect(mainBody.indexOf("reconcilePersistedDockerEngine()"))
      .toBeLessThan(mainBody.indexOf("if (await isEngineRunning())"));

    const reconcileStart = source.indexOf("async function reconcilePersistedDockerEngine");
    const reconcileEnd = source.indexOf("function installInstructions", reconcileStart);
    const reconcileBody = source.slice(reconcileStart, reconcileEnd);
    expect(reconcileBody).toContain("all required state capabilities");
    expect(reconcileBody).toContain("process.exitCode = 1");
    expect(reconcileBody).not.toContain("inspectOwnedDockerEngine");
    expect(reconcileBody).not.toContain('["start", inspection.containerId]');
  });

  it("rejects Docker opt-in before attempting container startup", () => {
    const start = source.indexOf("async function startEngine");
    const dockerStart = source.indexOf("const dockerOptIn =", start);
    const nativeStart = source.indexOf("const fallbacks =", dockerStart);
    const dockerSelection = source.slice(dockerStart, nativeStart);
    expect(start).toBeGreaterThanOrEqual(0);
    expect(dockerStart).toBeGreaterThan(start);
    expect(nativeStart).toBeGreaterThan(dockerStart);
    expect(dockerSelection).toContain("Docker startup is unavailable until a patched image can be verified");
    expect(dockerSelection).toContain("No Docker container was started.");
    expect(dockerSelection).not.toContain("dockerComposeArgs(");
    expect(dockerSelection).not.toContain("spawnSync(");
  });

  it("fails closed on legacy directories until a matching receipt and target identity exist", () => {
    const mainStart = source.indexOf("async function main()");
    const mainEnd = source.indexOf("async function apiFetch", mainStart);
    const mainBody = source.slice(mainStart, mainEnd);
    expect(mainBody.indexOf("assertLegacyStateMigrationReady();"))
      .toBeLessThan(mainBody.indexOf("await assertRuntimePortOwnership();"));

    const gateStart = source.indexOf("function legacyStateStoreGateError()");
    const gateEnd = source.indexOf("function assertLegacyStateMigrationReady", gateStart);
    const gate = source.slice(gateStart, gateEnd);
    expect(gate).toContain("hasValidStateMigrationReceipt(");
    expect(gate).toContain("stateMigrationTargetIdentity(target)");
    expect(gate).toContain('pendingStatus === "missing"');
    expect(gate).toContain('sourceInspection.status !== "directory"');

    const prepareStart = source.indexOf("function prepareEngineLaunch");
    const prepareEnd = source.indexOf("function startIiiBin", prepareStart);
    const prepare = source.slice(prepareStart, prepareEnd);
    expect(prepare).toContain("assertLegacyStateMigrationReady();");
    expect(prepare).toContain('if (basename(m.from) === "state_store.db") continue;');
    expect(prepare).toContain("Runtime engine config could not be made safe");
  });

  it("requires explicit paths and budget for the checksum-verified native migration command", () => {
    const migrateStart = source.indexOf("async function runStateMigrationCmd()");
    const migrateEnd = source.indexOf("async function main()", migrateStart);
    const migrate = source.slice(migrateStart, migrateEnd);
    expect(migrate).toContain("resolveBundledEngineArtifact(");
    expect(migrate).toContain("hasRequiredEngineCapabilitiesOutput(capabilityOutput)");
    expect(migrate).toContain('["state-migrate", "--source", source, "--target", target, "--disk-budget-bytes", diskBudgetBytes]');
    expect(migrate).toContain("createStateMigrationReceipt(");
    expect(migrate).toContain("stateMigrationTargetIdentity(target)");
    expect(migrate).toContain("source retained at");
    expect(source).toContain('"state-migrate": runStateMigrationCmd');

    const parserStart = source.indexOf("function parseStateMigrationArgs");
    const parserEnd = source.indexOf("async function runStateMigrationCmd", parserStart);
    const parser = source.slice(parserStart, parserEnd);
    expect(parser).toContain('"--source", "--target", "--disk-budget-bytes"');
    expect(parser).toContain("may be supplied only once");
    expect(parser).toContain("positive unsigned 64-bit integer");
  });

  it("keeps canonical compatibility and rejects Docker ownership on another port", () => {
    expect(source).toContain('function cliArgValue(name: string)');
    expect(source).toContain('arg.startsWith(`${name}=`)');
    expect(source).toContain("async function assertRuntimePortOwnership");
    expect(source).toContain("restPort: getRestPort()");
    expect(source).toContain('inspection.status === "running" || inspection.status === "stopped"');
    expect(source).not.toContain("migrateLegacyRuntimeMetadata");
    expect(source).not.toContain("clearLegacyRuntimeMetadata");
  });

  it("preserves Docker ownership when compose startup fails", () => {
    const spawnStart = source.indexOf("function spawnEngineBackground");
    const spawnEnd = source.indexOf("function prepareEngineLaunch", spawnStart);
    const spawnBody = source.slice(spawnStart, spawnEnd);
    expect(spawnBody).toContain('if (!isDocker && typeof child.pid === "number")');
    expect(spawnBody).toContain("clearExitedNativeEngineOwnership(child.pid)");
    expect(spawnBody).not.toContain("clearEngineState()");
  });

  it("waits for a delayed REST endpoint and does not mistake another child's crash for engine failure", async () => {
    vi.useFakeTimers();
    try {
      const start = source.indexOf("async function waitForEngine(timeoutMs: number): Promise<boolean> {");
      const end = source.indexOf("\n}", start) + 2;
      const waitSource = source.slice(start, end).replace(
        "async function waitForEngine(timeoutMs: number): Promise<boolean>", "async function waitForEngine(timeoutMs)",
      );
      const since = Date.now();
      const probe = vi.fn(async () => Date.now() - since >= 46_000);
      const wait = new Function("isEngineRunning", "startupFailure", "liveOwnedNativeEnginePid", "expectedEnginePid", "pidAlive",
        "IS_WINDOWS", "getRestPort", "windowsListenerBelongsToOwnedEngine",
        `${waitSource}; return waitForEngine;`)(probe, { kind: "engine-crashed" }, () => 101, 101, () => true,
        false, () => 3111, () => true) as
        (timeoutMs: number) => Promise<boolean>;
      const result = wait(15 * 60 * 1000);
      await vi.advanceTimersByTimeAsync(46_000);
      await expect(result).resolves.toBe(true);
      expect(probe).toHaveBeenCalled();
      expect(source).toContain("const ENGINE_READINESS_TIMEOUT_MS = 15 * 60 * 1000;");
      expect(source.match(/waitForEngine\(ENGINE_READINESS_TIMEOUT_MS\)/g)).toHaveLength(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("reuses a live owned engine and only clears metadata for the exited PID and state", () => {
    const liveStart = source.indexOf("function liveOwnedNativeEnginePid(): number | null {");
    const liveEnd = source.indexOf("\n}", liveStart) + 2;
    const liveSource = source.slice(liveStart, liveEnd).replace(
      "function liveOwnedNativeEnginePid(): number | null", "function liveOwnedNativeEnginePid()",
    );
    const state = { kind: "native", configPath: "config", restPort: 3111, pid: 202 };
    let recordedPid = 202;
    let requestedPort = 3111;
    const live = new Function("readEngineState", "readEnginePidfile", "engineStateRestPort", "getRestPort", "pidAlive",
      `${liveSource}; return liveOwnedNativeEnginePid;`)(
      () => state, () => recordedPid, (value: typeof state) => value.restPort, () => requestedPort, (pid: number) => pid === 202,
    ) as () => number | null;
    expect(live()).toBe(202);
    const originalPid = state.pid;
    state.pid = undefined as unknown as number;
    expect(live()).toBeNull();
    state.pid = originalPid;
    requestedPort = 3211;
    expect(live()).toBeNull();
    state.restPort = 3211;
    expect(live()).toBe(202);
    recordedPid = 101;
    expect(live()).toBeNull();
    recordedPid = 202;

    const clearStart = source.indexOf("function clearExitedNativeEngineOwnership(exitedPid: number): void {");
    const clearEnd = source.indexOf("\n}", clearStart) + 2;
    const clearSource = source.slice(clearStart, clearEnd).replace(
      "function clearExitedNativeEngineOwnership(exitedPid: number): void", "function clearExitedNativeEngineOwnership(exitedPid)",
    );
    const clearPid = vi.fn();
    const clearState = vi.fn();
    const clear = new Function("readEngineState", "readEnginePidfile", "clearEnginePidfile", "clearEngineState",
      `${clearSource}; return clearExitedNativeEngineOwnership;`)(
      () => state, () => recordedPid, clearPid, clearState,
    ) as (pid: number) => void;
    clear(101);
    expect(clearPid).not.toHaveBeenCalled();
    expect(clearState).not.toHaveBeenCalled();
    clear(202);
    expect(clearPid).toHaveBeenCalledOnce();
    expect(clearState).toHaveBeenCalledOnce();

    const engineStart = source.indexOf("async function startEngine()");
    const engineEnd = source.indexOf("async function waitForEngine", engineStart);
    const engineBody = source.slice(engineStart, engineEnd);
    expect(engineBody.indexOf("liveOwnedNativeEnginePid()")).toBeLessThan(engineBody.indexOf("startIiiBinOnce(iiiBin, configPath)"));
  });

  it("fails promptly if the owned engine exits before REST becomes ready", async () => {
    vi.useFakeTimers();
    try {
      const start = source.indexOf("async function waitForEngine(timeoutMs: number): Promise<boolean> {");
      const end = source.indexOf("\n}", start) + 2;
      const waitSource = source.slice(start, end).replace(
        "async function waitForEngine(timeoutMs: number): Promise<boolean>", "async function waitForEngine(timeoutMs)",
      );
      let alive = true;
      const wait = new Function("isEngineRunning", "startupFailure", "liveOwnedNativeEnginePid", "expectedEnginePid", "pidAlive",
        "IS_WINDOWS", "getRestPort", "windowsListenerBelongsToOwnedEngine",
        `${waitSource}; return waitForEngine;`)(async () => false, null, () => null, 202, () => alive,
        false, () => 3111, () => true) as
        (timeoutMs: number) => Promise<boolean>;
      const result = wait(15 * 60 * 1000);
      alive = false;
      await vi.advanceTimersByTimeAsync(500);
      await expect(result).resolves.toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });

  it("requires a matching owned PID on Windows before attaching to a running listener", () => {
    const mainStart = source.indexOf("async function main()");
    const mainEnd = source.indexOf("async function apiFetch", mainStart);
    const running = source.slice(mainStart, mainEnd);
    const branch = running.indexOf("if (await isEngineRunning())");
    const adoption = running.indexOf("adoptRunningEngine(attachedBin", branch);
    const verification = running.indexOf("windowsListenerBelongsToOwnedEngine", branch);
    const capabilityCheck = running.indexOf("iiiHasRequiredStateCapabilities(attachedBin)", branch);
    expect(verification).toBeGreaterThan(branch);
    expect(verification).toBeLessThan(adoption);
    expect(capabilityCheck).toBeGreaterThan(branch);
    expect(capabilityCheck).toBeLessThan(adoption);
    expect(running.slice(branch, adoption)).toContain("process.exit(1)");

    const start = source.indexOf("function windowsListenerBelongsToOwnedEngine(");
    const end = source.indexOf("\n}", start) + 2;
    const body = source.slice(start, end).replace(
      "function windowsListenerBelongsToOwnedEngine(port: number, ownedPid: number): boolean",
      "function windowsListenerBelongsToOwnedEngine(port, ownedPid)",
    );
    const verify = new Function("execFileSync", `${body}; return windowsListenerBelongsToOwnedEngine;`)(
      () => "  TCP    127.0.0.1:3111    0.0.0.0:0    LISTENING    202\r\n",
    ) as (port: number, pid: number) => boolean;
    expect(verify(3111, 202)).toBe(true);
    expect(verify(3111, 101)).toBe(false);
    expect(verify(3211, 202)).toBe(false);
  });

  it("rejects REST readiness from a Windows listener owned by another process", async () => {
    const start = source.indexOf("async function waitForEngine(timeoutMs: number): Promise<boolean> {");
    const end = source.indexOf("\n}", start) + 2;
    const waitSource = source.slice(start, end).replace(
      "async function waitForEngine(timeoutMs: number): Promise<boolean>", "async function waitForEngine(timeoutMs)",
    );
    const verify = vi.fn(() => false);
    const wait = new Function("isEngineRunning", "startupFailure", "liveOwnedNativeEnginePid", "expectedEnginePid", "pidAlive",
      "IS_WINDOWS", "getRestPort", "windowsListenerBelongsToOwnedEngine",
      `${waitSource}; return waitForEngine;`)(async () => true, null, () => 202, 202, () => true,
      true, () => 3111, verify) as (timeoutMs: number) => Promise<boolean>;
    await expect(wait(1000)).resolves.toBe(false);
    expect(verify).toHaveBeenCalledWith(3111, 202);
  });
});
