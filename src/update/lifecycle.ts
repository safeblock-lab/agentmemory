import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  realpathSync,
  rmSync,
  writeFileSync,
  closeSync,
  renameSync,
  appendFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runtimeMetadataPath } from "../runtime-paths.js";
import { VERSION } from "../version.js";
import { downloadVerifiedRelease } from "./release.js";
import { UPDATE_HELPER_SOURCE } from "./helper-source.js";

export { checkForUpdate } from "./release.js";
export type { ReleaseInfo } from "./release.js";

export type UpdatePhase = "idle" | "downloading" | "stopping" | "installing" | "starting" | "complete" | "failed";
export type UpdateStatus = {
  jobId?: string;
  phase: UpdatePhase;
  currentVersion: string;
  targetVersion?: string;
  message?: string;
  errorCode?: string;
  updatedAt?: string;
};
export type UpdateSupport = { supported: true } | { supported: false; reason: string };

type InstallContext = {
  packageRoot: string;
  npmCli: string;
  npmRoot: string;
  restPort: number;
  dataDir: string;
  runtimeDir: string;
  supervised: boolean;
};

export function inspectSupervisorSupport(runtimeDir: string): boolean {
  const supervisor = join(runtimeDir, "Start-AgentMemory.ps1");
  const watchdog = join(runtimeDir, "Invoke-AgentMemoryWatchdog.ps1");
  if (!existsSync(supervisor) && !existsSync(watchdog)) return false;
  const marker = "AgentMemory updater protocol v1";
  if (!existsSync(supervisor) || !readFileSync(supervisor, "utf8").includes(marker) ||
      (existsSync(watchdog) && !readFileSync(watchdog, "utf8").includes(marker))) {
    throw new Error("The local supervisor scripts need the update maintenance protocol before UI updates can run.");
  }
  return true;
}

function ownPackageRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 4; i++) {
    try {
      const metadata = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")) as { name?: unknown };
      if (metadata.name === "@agentmemory/agentmemory") return realpathSync(dir);
    } catch {}
    dir = dirname(dir);
  }
  throw new Error("Could not identify the running AgentMemory package.");
}

function npmCliPath(): string {
  const nodeDir = dirname(process.execPath);
  const candidates = [
    join(nodeDir, "node_modules", "npm", "bin", "npm-cli.js"),
    resolve(nodeDir, "..", "lib", "node_modules", "npm", "bin", "npm-cli.js"),
  ];
  const found = candidates.find((candidate) => existsSync(candidate));
  if (!found) throw new Error("The npm bundled with this Node.js installation is unavailable.");
  return found;
}

function localInstallContext(): InstallContext {
  if (process.platform !== "win32") {
    throw new Error("UI updates currently support native Windows global npm installations only.");
  }
  const runtimeDir = process.env["AGENTMEMORY_RUNTIME_DIR"];
  const dataDir = process.env["AGENTMEMORY_DATA_DIR"];
  const restPort = Number(process.env["III_REST_PORT"]);
  if (!runtimeDir || !isAbsolute(runtimeDir) || !dataDir || !isAbsolute(dataDir) ||
      !Number.isInteger(restPort) || restPort < 1024 || restPort > 65535) {
    throw new Error("Running instance has no complete local runtime configuration.");
  }
  const state = JSON.parse(readFileSync(runtimeMetadataPath("engine-state.json"), "utf8")) as {
    kind?: unknown; restPort?: unknown;
  };
  if (state.kind !== "native" || state.restPort !== restPort ||
      process.env["AGENTMEMORY_USE_DOCKER"] === "1" || process.env["AGENTMEMORY_USE_DOCKER"] === "true") {
    throw new Error("UI updates require an owned native AgentMemory engine.");
  }
  const workerPid = Number(readFileSync(runtimeMetadataPath("worker.pid"), "utf8").trim());
  if (workerPid !== process.pid) throw new Error("The current worker does not own this instance.");

  const npmCli = npmCliPath();
  const root = spawnSync(process.execPath, [npmCli, "root", "--global"], {
    encoding: "utf8", timeout: 10_000, windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (root.error || root.status !== 0) throw new Error("Could not inspect the global npm installation.");
  const npmRoot = root.stdout.trim();
  if (!isAbsolute(npmRoot)) throw new Error("npm returned an invalid global package directory.");
  const packageRoot = realpathSync(join(npmRoot, "@agentmemory", "agentmemory"));
  if (packageRoot.toLowerCase() !== ownPackageRoot().toLowerCase()) {
    throw new Error("The running AgentMemory package is not the global npm installation.");
  }
  if (!existsSync(join(packageRoot, "dist", "cli.mjs"))) {
    throw new Error("The global AgentMemory CLI is missing.");
  }
  const supervised = inspectSupervisorSupport(runtimeDir);
  return { packageRoot, npmCli, npmRoot, restPort, dataDir, runtimeDir, supervised };
}

export function getUpdateSupport(): UpdateSupport {
  try {
    localInstallContext();
    return { supported: true };
  } catch (error) {
    const reason = error instanceof Error ? error.message : "";
    const safeReasons = [
      "UI updates currently support native Windows global npm installations only.",
      "Running instance has no complete local runtime configuration.",
      "UI updates require an owned native AgentMemory engine.",
      "The current worker does not own this instance.",
      "The npm bundled with this Node.js installation is unavailable.",
      "Could not inspect the global npm installation.",
      "npm returned an invalid global package directory.",
      "The running AgentMemory package is not the global npm installation.",
      "The global AgentMemory CLI is missing.",
      "The local supervisor scripts need the update maintenance protocol before UI updates can run.",
      "Could not identify the running AgentMemory package.",
    ];
    return { supported: false, reason: safeReasons.includes(reason) ? reason : "UI updates are unavailable for this installation." };
  }
}

function statusPath(): string {
  return runtimeMetadataPath("update-status.json");
}

function writeStatus(status: UpdateStatus): void {
  const path = statusPath();
  mkdirSync(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify({ ...status, updatedAt: new Date().toISOString() })}\n`, { mode: 0o600 });
  renameSync(temporary, path);
}

export function readUpdateStatus(): UpdateStatus {
  try {
    const value = JSON.parse(readFileSync(statusPath(), "utf8")) as Partial<UpdateStatus>;
    if (typeof value.phase === "string" && ["downloading", "stopping", "installing", "starting", "complete", "failed"].includes(value.phase)) {
      return { phase: value.phase, currentVersion: typeof value.currentVersion === "string" ? value.currentVersion : VERSION,
        jobId: typeof value.jobId === "string" ? value.jobId : undefined,
        targetVersion: typeof value.targetVersion === "string" ? value.targetVersion : undefined,
        message: typeof value.message === "string" ? value.message : undefined,
        errorCode: typeof value.errorCode === "string" ? value.errorCode : undefined,
        updatedAt: typeof value.updatedAt === "string" ? value.updatedAt : undefined };
    }
  } catch {}
  return { phase: "idle", currentVersion: VERSION };
}

function lockOwnedByLiveProcess(lockPath: string): boolean {
  try {
    const lock = JSON.parse(readFileSync(lockPath, "utf8")) as { pid?: unknown };
    if (typeof lock.pid !== "number" || !Number.isInteger(lock.pid) || lock.pid <= 0) return true;
    try { process.kill(lock.pid, 0); return true; }
    catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
  } catch { return true; }
}

function acquireLock(lockPath: string, jobId: string): void {
  mkdirSync(dirname(lockPath), { recursive: true });
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const fd = openSync(lockPath, "wx", 0o600);
      writeFileSync(fd, JSON.stringify({ pid: process.pid, jobId }));
      closeSync(fd);
      return;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      if (attempt > 0 || lockOwnedByLiveProcess(lockPath)) throw new Error("An AgentMemory update is already in progress.");
      rmSync(lockPath, { force: true });
    }
  }
}

export async function startReleaseUpdate(): Promise<UpdateStatus> {
  const context = localInstallContext();
  const jobId = randomUUID();
  const lockPath = join(context.npmRoot, ".agentmemory-update.lock");
  acquireLock(lockPath, jobId);
  let jobDir: string | undefined;
  try {
    writeStatus({ jobId, phase: "downloading", currentVersion: VERSION, message: "Downloading the latest GitHub release." });
    const release = await downloadVerifiedRelease();
    jobDir = mkdtempSync(join(tmpdir(), "agentmemory-update-"));
    const tarballPath = join(jobDir, release.filename);
    const helperPath = join(jobDir, "update-helper.mjs");
    const planPath = join(jobDir, "plan.json");
    writeFileSync(tarballPath, release.tarball, { mode: 0o600 });
    writeFileSync(helperPath, UPDATE_HELPER_SOURCE, { mode: 0o600 });
    writeFileSync(planPath, JSON.stringify({
      jobId, jobDir, packageRoot: context.packageRoot, npmCli: context.npmCli,
      cliPath: join(context.packageRoot, "dist", "cli.mjs"),
      tarballPath, sha256: release.sha256, targetVersion: release.version,
      currentVersion: VERSION, statusPath: statusPath(), lockPath,
      logPath: runtimeMetadataPath("update.log"), restPort: context.restPort,
      dataDir: context.dataDir, runtimeDir: context.runtimeDir, supervised: context.supervised,
    }), { mode: 0o600 });
    const status: UpdateStatus = { jobId, phase: "stopping", currentVersion: VERSION,
      targetVersion: release.version, message: "Verified release staged; stopping AgentMemory." };
    writeStatus(status);
    const child = spawn(process.execPath, [helperPath, planPath], {
      cwd: dirname(context.packageRoot), env: process.env,
      detached: true, windowsHide: true, stdio: "ignore",
    });
    await new Promise<void>((resolveSpawn, rejectSpawn) => {
      child.once("spawn", resolveSpawn);
      child.once("error", rejectSpawn);
    });
    child.unref();
    return status;
  } catch (error) {
    if (jobDir) rmSync(jobDir, { recursive: true, force: true });
    rmSync(lockPath, { force: true });
    try {
      appendFileSync(runtimeMetadataPath("update.log"), `${new Date().toISOString()} preparation: ${error instanceof Error ? error.message : String(error)}\n`, { mode: 0o600 });
    } catch {}
    writeStatus({ jobId, phase: "failed", currentVersion: VERSION,
      message: "Update preparation failed. Inspect the local update log.", errorCode: "UPDATE_PREPARE_FAILED" });
    throw error;
  }
}
