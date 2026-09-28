// This script is copied to a private temporary directory before the running
// package is replaced. It must not import files from the AgentMemory package.
export const UPDATE_HELPER_SOURCE = String.raw`
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, cpSync, existsSync, openSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { dirname, join } from "node:path";

const plan = JSON.parse(readFileSync(process.argv[2], "utf8"));
process.env.AGENTMEMORY_UPDATE_RESTART = "1";
const backup = join(dirname(plan.packageRoot), ".agentmemory-update-backup-" + plan.jobId);
let backedUp = false;
let installedNew = false;
let stoppedOld = false;
let replacementMayBeRunning = false;
let generation = 0;
const maintenancePath = join(plan.runtimeDir || dirname(plan.statusPath), "update-maintenance.json");
const ackPath = join(plan.runtimeDir || dirname(plan.statusPath), "update-supervisor-ack.json");

function log(message) {
  try { appendFileSync(plan.logPath, new Date().toISOString() + " " + message + "\n", { mode: 0o600 }); }
  catch {}
}

function status(phase, message, errorCode) {
  const path = plan.statusPath;
  const entry = { jobId: plan.jobId, phase, currentVersion: plan.currentVersion,
    targetVersion: plan.targetVersion, message, errorCode, updatedAt: new Date().toISOString() };
  const temporary = path + "." + process.pid + ".tmp";
  writeFileSync(temporary, JSON.stringify(entry), { mode: 0o600 });
  renameSync(temporary, path);
}

function installEnvironment() {
  const allowed = ["PATH", "Path", "SystemRoot", "WINDIR", "TEMP", "TMP", "USERPROFILE",
    "APPDATA", "LOCALAPPDATA", "HOME", "COMSPEC", "PATHEXT", "HOMEDRIVE", "HOMEPATH",
    "ProgramFiles", "ProgramFiles(x86)"];
  const env = {};
  for (const name of allowed) if (process.env[name]) env[name] = process.env[name];
  return env;
}

function command(args, timeout, env = process.env) {
  const result = spawnSync(process.execPath, args, {
    env, encoding: "utf8", timeout, windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.error || result.status !== 0) {
    throw new Error(result.error?.message ||
      (result.signal ? "Command timed out or was interrupted." : "Command exited with code " + result.status + "."));
  }
}

function cliArgs(commandName) {
  const flags = ["--port", String(plan.restPort), "--data-dir", plan.dataDir];
  return commandName ? [plan.cliPath, commandName, ...flags] : [plan.cliPath, ...flags];
}

function alive(pid) {
  try { process.kill(pid, 0); return true; } catch { return false; }
}

function maintenance(phase) {
  if (!plan.supervised) return;
  if (phase === "pause") generation++;
  const entry = { jobId: plan.jobId, pid: process.pid, phase, generation,
    startedAt: new Date().toISOString() };
  const temporary = maintenancePath + "." + process.pid + ".tmp";
  writeFileSync(temporary, JSON.stringify(entry), { mode: 0o600 });
  renameSync(temporary, maintenancePath);
}

async function waitForSupervisorAck() {
  if (!plan.supervised) return;
  const deadline = Date.now() + Math.min(plan.supervisorAckTimeoutMs || 30_000, 30_000);
  while (Date.now() < deadline) {
    try {
      const ack = JSON.parse(readFileSync(ackPath, "utf8"));
      if (ack.jobId === plan.jobId && ack.generation === generation) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error("Supervisor did not acknowledge maintenance mode.");
}

function clearMaintenance() {
  if (!plan.supervised) return;
  try {
    const marker = JSON.parse(readFileSync(maintenancePath, "utf8"));
    if (marker.jobId === plan.jobId && marker.pid === process.pid) rmSync(maintenancePath, { force: true });
  } catch {}
  try {
    const ack = JSON.parse(readFileSync(ackPath, "utf8"));
    if (ack.jobId === plan.jobId) rmSync(ackPath, { force: true });
  } catch {}
}

function configuredEngineAddress() {
  if (process.env.III_ENGINE_URL) {
    const endpoint = new URL(process.env.III_ENGINE_URL);
    const configuredHost = endpoint.hostname.replace(/^\[|\]$/g, "").toLowerCase();
    if (!["localhost", "127.0.0.1", "::1", "0.0.0.0", "::"].includes(configuredHost)) return null;
    const host = configuredHost === "localhost" || configuredHost === "0.0.0.0" ? "127.0.0.1" : configuredHost === "::" ? "::1" : configuredHost;
    return { host, port: Number(endpoint.port) || (endpoint.protocol === "wss:" ? 443 : 80) };
  }
  return {
    host: "127.0.0.1",
    port: Number(plan.enginePort || process.env.III_ENGINE_PORT) || plan.restPort + 46023,
  };
}

function canConnectToEngine({ host, port }) {
  return new Promise(resolve => {
    const socket = createConnection({ host, port });
    let settled = false;
    const timer = setTimeout(() => finish(false), 1000);
    const finish = connected => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve(connected);
    };
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}

async function waitForEngine(deadline) {
  const address = configuredEngineAddress();
  if (!address) return;
  while (Date.now() < deadline) {
    if (await canConnectToEngine(address)) return;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("The AgentMemory engine did not become reachable after restart.");
}

function viewerBaseUrl(port) {
  let host = (process.env.AGENTMEMORY_VIEWER_HOST || "127.0.0.1").replace(/^\[|\]$/g, "");
  if (host === "localhost" || host === "0.0.0.0") host = "127.0.0.1";
  if (host === "::") host = "::1";
  const formattedHost = host.includes(":") ? "[" + host + "]" : host;
  return "http://" + formattedHost + ":" + port;
}

async function waitForWorker(expectedVersion, deadline, headers, restartStartedAt) {
  const base = "http://127.0.0.1:" + plan.restPort + "/agentmemory/";
  while (Date.now() < deadline) {
    try {
      const live = await fetch(base + "livez", { headers, signal: AbortSignal.timeout(1500) });
      if (!live.ok) throw new Error("worker liveness pending");
      const instance = await live.json();
      if (instance?.service !== "agentmemory") throw new Error("worker identity pending");
      if (instance.viewerSkipped === true) {
        throw new Error("AgentMemory started without a viewer; the update cannot be marked ready.");
      }
      if (!Number.isInteger(instance.viewerPort) || instance.viewerPort <= 0 || instance.viewerPort > 65535) {
        throw new Error("viewer binding pending");
      }
      const flags = await fetch(base + "config/flags", { headers, signal: AbortSignal.timeout(1500) });
      if (!flags.ok || (await flags.json())?.version !== expectedVersion) throw new Error("versioned API pending");
      const health = await fetch(base + "health", { headers, signal: AbortSignal.timeout(1500) });
      if (!health.ok) throw new Error("health probe pending");
      const healthState = await health.json();
      const engineWorker = healthState?.health?.workers?.find(worker => worker?.name === "agentmemory");
      if (healthState?.version === expectedVersion && healthState?.health?.connectionState === "connected" &&
          engineWorker?.status === "connected" && Number.isFinite(engineWorker.connected_at_ms) &&
          engineWorker.connected_at_ms >= restartStartedAt) return instance.viewerPort;
    } catch (error) {
      if (error instanceof Error && error.message.includes("without a viewer")) throw error;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("The AgentMemory worker API did not become ready with a connected engine.");
}

async function waitForViewer(port, deadline) {
  const url = viewerBaseUrl(port) + "/";
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(1500) });
      if (response.ok) return;
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("The AgentMemory viewer did not become reachable after restart.");
}

async function restart(cliPath, expectedVersion) {
  let child;
  const restartStartedAt = Date.now();
  if (plan.supervised) {
    maintenance("resume");
  } else {
    const logFd = openSync(plan.logPath, "a", 0o600);
    const flags = ["--port", String(plan.restPort), "--data-dir", plan.dataDir];
    child = spawn(process.execPath, [cliPath, ...flags], {
      env: process.env, detached: true, windowsHide: true,
      stdio: ["ignore", logFd, logFd],
    });
    child.unref();
  }
  const deadline = Date.now() + (plan.readyTimeoutMs || 15 * 60_000);
  const headers = process.env.AGENTMEMORY_SECRET
    ? { Authorization: "Bearer " + process.env.AGENTMEMORY_SECRET } : {};
  await waitForEngine(deadline);
  if (child && !alive(child.pid)) throw new Error("Restarted AgentMemory process exited before becoming ready.");
  const viewerPort = await waitForWorker(expectedVersion, deadline, headers, restartStartedAt);
  await waitForViewer(viewerPort, deadline);
}

try {
  writeFileSync(plan.lockPath, JSON.stringify({ pid: process.pid, jobId: plan.jobId }), { mode: 0o600 });
  const tarball = readFileSync(plan.tarballPath);
  const actualHash = createHash("sha256").update(tarball).digest("hex");
  if (actualHash !== plan.sha256) throw new Error("Staged package checksum changed.");
  status("stopping", "Stopping the current AgentMemory instance.");
  maintenance("pause");
  command(cliArgs("stop"), 30_000);
  stoppedOld = true;
  await waitForSupervisorAck();
  status("installing", "Installing the verified GitHub release.");
  cpSync(plan.packageRoot, backup, { recursive: true, errorOnExist: true });
  backedUp = true;
  command([plan.npmCli, "install", "--global", "--ignore-scripts", "--no-audit", "--no-fund", plan.tarballPath], 300_000, installEnvironment());
  const installed = JSON.parse(readFileSync(join(plan.packageRoot, "package.json"), "utf8"));
  if (installed.name !== "@agentmemory/agentmemory" || installed.version !== plan.targetVersion) {
    throw new Error("Installed package identity or version did not match the verified release.");
  }
  installedNew = true;
  status("starting", "Starting the updated AgentMemory instance.");
  replacementMayBeRunning = true;
  await restart(join(plan.packageRoot, "dist", "cli.mjs"), plan.targetVersion);
  status("complete", "AgentMemory " + plan.targetVersion + " is ready.");
  try { rmSync(backup, { recursive: true, force: true }); } catch {}
} catch (error) {
  log("Update failed: " + (error instanceof Error ? error.stack || error.message : String(error)));
  let message = "Update failed; inspect the local update log.";
  let errorCode = "UPDATE_FAILED";
  if (backedUp) {
    try {
      if (replacementMayBeRunning) {
        maintenance("pause");
        command([join(plan.packageRoot, "dist", "cli.mjs"), "stop", "--port", String(plan.restPort), "--data-dir", plan.dataDir], 30_000);
        await waitForSupervisorAck();
      }
      if (existsSync(plan.packageRoot)) rmSync(plan.packageRoot, { recursive: true, force: true });
      renameSync(backup, plan.packageRoot);
      await restart(plan.cliPath, plan.currentVersion);
      message = "Update failed; the previous version was restored and restarted.";
    } catch (rollbackError) {
      log("Rollback failed: " + (rollbackError instanceof Error ? rollbackError.stack || rollbackError.message : String(rollbackError)));
      message = "Update and automatic restoration failed; inspect the local update log and preserved backup.";
      errorCode = "UPDATE_ROLLBACK_FAILED";
    }
  } else if (stoppedOld) {
    try {
      await restart(plan.cliPath, plan.currentVersion);
      message = "Update failed before installation; the previous version restarted.";
    } catch (rollbackError) {
      log("Restart failed: " + (rollbackError instanceof Error ? rollbackError.stack || rollbackError.message : String(rollbackError)));
      message = "Update failed; the previous version could not be restarted automatically.";
      errorCode = "UPDATE_RESTART_FAILED";
    }
  }
  status("failed", message, errorCode);
} finally {
  clearMaintenance();
  try { rmSync(plan.lockPath, { force: true }); } catch {}
  try { rmSync(plan.jobDir, { recursive: true, force: true }); } catch {}
}
`;
