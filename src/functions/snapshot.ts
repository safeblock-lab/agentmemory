import { observeWorkflowTerminal, markWorkflowStatisticsGap } from "./dashboard-activity.js";
import { getAgentId } from "../config.js";
import type { IIIClient } from "iii-sdk";
import { BatchMaintenanceBusyError, withBatchMutationLocks, preserveBatchProvenance } from "../state/batch-effects.js";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { createWriteStream, existsSync, mkdirSync } from "node:fs";
import { rename, rm, stat, statfs, open, opendir } from "node:fs/promises";
import type { FileHandle } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createGzip } from "node:zlib";
import { join } from "node:path";
import type { SnapshotMeta, WorkflowTerminalReceipt } from "../types.js";
import { KV, generateId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { recordAudit } from "./audit.js";
import { VERSION } from "../version.js";
import { logger } from "../logger.js";
import { cleanupAuthorized, retentionEligibility, STORAGE_RETENTION } from "./storage-retention.js";
import { graphKV, registerGraphJobHandler, runGraphJob, withGraphDelta } from "./graph-jobs.js";
import { withSnapshotReadView, SNAPSHOT_EXPORT_MS, SNAPSHOT_PUBLICATION_BYTES, SNAPSHOT_PUBLICATION_HEADROOM, type SnapshotReadView } from "../state/snapshot-read-view.js";
import { encodeSnapshot, readSnapshot, type SnapshotRecord } from "../state/snapshot-codec.js";

const COMMIT_HASH_RE = /^[0-9a-f]{7,40}$/i;
const execFileAsync = promisify(execFile);
async function gitExec(dir: string, args: string[]): Promise<string> {
  const { stdout } = await execFileAsync("git", args, { cwd: dir, timeout: SNAPSHOT_EXPORT_MS, maxBuffer: 1024 * 1024 });
  return stdout.trim();
}
async function ensureGitRepo(dir: string): Promise<void> {
  mkdirSync(dir, { recursive: true });
  if (!existsSync(join(dir, ".git"))) {
    await gitExec(dir, ["init"]);
    await gitExec(dir, ["config", "user.email", "agentmemory@local"]);
    await gitExec(dir, ["config", "user.name", "agentmemory"]);
  }
}
interface SnapshotStats { sessions: number; memories: number; observations: number; graphNodes: number }
async function* snapshotRecords(view: SnapshotReadView, stats: SnapshotStats): AsyncGenerator<SnapshotRecord> {
  async function* scopeRecords(scope: string, counter?: keyof SnapshotStats): AsyncGenerator<SnapshotRecord> {
    for await (const key of view.keys(scope)) {
      view.check();
      const value = await view.get<Record<string, unknown>>(scope, key);
      if (counter) stats[counter]++;
      yield { scope, key, value };
    }
  }
  yield* scopeRecords(KV.sessions, "sessions");
  yield* scopeRecords(KV.memories, "memories");
  yield* scopeRecords(KV.graphNodes, "graphNodes");
  for await (const sessionId of view.keys(KV.sessions)) yield* scopeRecords(KV.observations(sessionId), "observations");
  yield* scopeRecords(KV.accessLog);
}
function byteLimit(limit: number): Transform {
  let bytes = 0;
  return new Transform({ transform(chunk: Buffer, _encoding, callback) {
    bytes += chunk.length;
    callback(bytes > limit ? new Error("STATE_SNAPSHOT_DISK_LIMIT") : null, chunk);
  } });
}

// The exclusive manifest is also the cross-process writer lock. Unknown prior jobs require explicit recovery.
async function withSnapshotTemporary<T>(dir: string, purpose: "publish" | "restore", run: (path: string, manifest: (phase: string) => Promise<void>, execution: { id: string; startedAt: string }) => Promise<T>): Promise<T> {
  const owner = randomUUID();
  const temporary = `.state-${owner}.tmp`;
  const manifestPath = join(dir, ".state-snapshot.manifest.json");
  let manifest: FileHandle;
  try { manifest = await open(manifestPath, "wx"); }
  catch (error) {
    if (error instanceof Error && "code" in error && error.code === "EEXIST") throw new Error("STATE_SNAPSHOT_MANIFEST_RECOVERY_REQUIRED");
    throw error;
  }
  const path = join(dir, temporary);
  let phase = "opened";
  const update = async (next: string) => {
    phase = next;
    const body = Buffer.from(JSON.stringify({ version: 1, owner, purpose, temporary, phase, createdAt }));
    await manifest.truncate(0);
    await manifest.write(body, 0, body.length, 0);
    await manifest.sync();
  };
  const createdAt = new Date().toISOString();
  let failure: unknown;
  let ownsTemporary = false;
  try {
    await update("writing");
    let entries = 0;
    for await (const entry of await opendir(dir)) {
      if (++entries > 128) throw new Error("STATE_SNAPSHOT_RESOURCE_LIMIT");
      if (entry.name.startsWith(".state-") && entry.name.endsWith(".tmp")) {
        throw new Error("STATE_SNAPSHOT_LEGACY_TEMPORARY_RECOVERY_REQUIRED");
      }
    }
    const temporaryHandle = await open(path, "wx");
    ownsTemporary = true;
    await temporaryHandle.close();
    return await run(path, update, { id: owner, startedAt: createdAt });
  } catch (error) { failure = error; throw error; }
  finally {
    try {
      if (ownsTemporary) await rm(path, { force: true });
      await rm(manifestPath);
      await manifest.close();
    } catch (cleanup) {
      const causes = failure === undefined ? [cleanup] : [failure, cleanup];
      // After publishing closed, this owner must never unlink the shared manifest path again.
      // A successor can then safely collect the exact closed owner's temporary.
      try { await update("closed"); } catch (closureError) { causes.push(closureError); }
      try { await manifest.close(); } catch (closeError) { causes.push(closeError); }
      throw new AggregateError(causes, "STATE_SNAPSHOT_CLEANUP_PENDING");
    }
  }
}
async function publishSnapshot(kv: StateKV, dir: string, stats: SnapshotStats, path: string, manifest: (phase: string) => Promise<void>): Promise<string> {
    const statePath = join(dir, "state.json");
    const priorBytes = await stat(statePath).then(value => value.size, error => {
      if (error.code === "ENOENT") return 0;
      throw error;
    });
    const free = await statfs(dir, { bigint: true });
    const ceiling = SNAPSHOT_PUBLICATION_BYTES - priorBytes - SNAPSHOT_PUBLICATION_HEADROOM;
    if (ceiling <= 0 || free.bavail * free.bsize < BigInt(ceiling + SNAPSHOT_PUBLICATION_HEADROOM)) throw new Error("STATE_SNAPSHOT_DISK_LIMIT");
    let cutAt = "";
    await withSnapshotReadView(kv.snapshotTransport, async view => {
      cutAt = view.cut.cut_at;
      await pipeline(Readable.from(encodeSnapshot(snapshotRecords(view, stats), VERSION, cutAt)),
        byteLimit(SNAPSHOT_PUBLICATION_BYTES), createGzip(), byteLimit(ceiling), createWriteStream(path, { flags: "r+" }),
        { signal: AbortSignal.timeout(SNAPSHOT_EXPORT_MS) });
      view.check();
    });
    const file = await open(path, "r+");
    try { await file.sync(); } finally { await file.close(); }
    await manifest("prepared");
    await rename(path, statePath);
    await manifest("published");
    return cutAt;
}
async function extractCommit(dir: string, commit: string, path: string): Promise<void> {
  const child = spawn("git", ["show", `${commit}:state.json`], { cwd: dir, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  let errorText = "";
  child.stderr.on("data", (chunk: Buffer) => { if (errorText.length < 4096) errorText += chunk.toString().slice(0, 4096 - errorText.length); });
  const exited = new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", code => code === 0 ? resolve() : reject(new Error(`SNAPSHOT_GIT_READ_FAILED: ${errorText}`)));
  });
  try {
    await Promise.all([exited, pipeline(child.stdout, byteLimit(SNAPSHOT_PUBLICATION_BYTES), createWriteStream(path, { flags: "r+" }),
      { signal: AbortSignal.timeout(SNAPSHOT_EXPORT_MS) })]);
  } finally { if (child.exitCode === null) child.kill(); }
}
export function registerSnapshotFunction(sdk: IIIClient, kv: StateKV, snapshotDir: string): void {
  kv = graphKV(kv);
  type RestoredNode = { id: string } & Record<string, unknown>;
  const restoreGraph = async (nodes: RestoredNode[], durableId?: string) => runGraphJob(kv, "restore", nodes, input => withGraphDelta(kv, async () => {
    for (const node of input as RestoredNode[]) {
      const current = await kv.get<RestoredNode>(KV.graphNodes, node.id);
      await kv.set(KV.graphNodes, node.id, preserveBatchProvenance(current, node));
    }
    return { success: true };
  }), durableId);
  registerGraphJobHandler(kv, "restore", (input, id) => restoreGraph(input as RestoredNode[], id));
  let snapshotInFlight = false;
  sdk.registerFunction("mem::snapshot-create", async (data?: { message?: string }) => {
    if (snapshotInFlight) return { success: true, message: "Snapshot already in progress" };
    snapshotInFlight = true;
    let snapshotExecution: { id: string; startedAt: string } | undefined;
    let snapshotTerminal: WorkflowTerminalReceipt | undefined;
    const recordSnapshotTerminal = async (outcome: "completed" | "failed") => {
      if (!snapshotExecution) { await markWorkflowStatisticsGap(kv, "snapshots"); return; }
      snapshotTerminal ??= { executionId: snapshotExecution.id, agentId: getAgentId(), occurredAt: new Date().toISOString(), outcome };
      try {
        await kv.set(KV.state, `workflow-snapshot:${snapshotExecution.id}`, snapshotTerminal);
        await observeWorkflowTerminal(kv, { ...snapshotTerminal, workflow: "snapshots", producer: "snapshot-publication", executionLevel: "workflow" });
      } catch { await markWorkflowStatisticsGap(kv, "snapshots"); }
    };
    try {
      mkdirSync(snapshotDir, { recursive: true });
      await ensureGitRepo(snapshotDir);
      if (existsSync(join(snapshotDir, "state.json")) && await gitExec(snapshotDir, ["status", "--porcelain", "--", "state.json"])) {
        throw new Error("STATE_SNAPSHOT_PRIOR_PUBLICATION_RECOVERY_REQUIRED");
      }
      const stats: SnapshotStats = { sessions: 0, memories: 0, observations: 0, graphNodes: 0 };
      return await withSnapshotTemporary(snapshotDir, "publish", async (path, manifest, execution) => {
      snapshotExecution = execution;
      const ts = await publishSnapshot(kv, snapshotDir, stats, path, manifest);
      await gitExec(snapshotDir, ["add", "--", "state.json"]);
      const message = data?.message || `Snapshot ${ts}`;
      try { await gitExec(snapshotDir, ["commit", "-m", message]); }
      catch (error) { if (error instanceof Error && error.message.includes("nothing to commit")) return { success: true, message: "No changes to snapshot" }; throw error; }
      const commitHash = await gitExec(snapshotDir, ["rev-parse", "HEAD"]);
      const meta: SnapshotMeta = { id: generateId("snap"), commitHash, createdAt: ts, message, stats };
      await recordAudit(kv, "export", "mem::snapshot-create", [meta.id], { commitHash, stats });
      await manifest("committed");
      await recordSnapshotTerminal("completed");
      return { success: true, snapshot: meta };
      });
    } catch (error) { await recordSnapshotTerminal("failed"); return snapshotFailure(error); }
    finally { snapshotInFlight = false; }
  });
  sdk.registerFunction("mem::snapshot-list", async () => {
    if (!existsSync(join(snapshotDir, ".git"))) return { snapshots: [] };
    try {
      const log = await gitExec(snapshotDir, ["log", "--format=%H|%aI|%s", "-20"]);
      return { retention: { ...STORAGE_RETENTION.backups, automaticCollection: false, reason: "partial_export_and_git_references_require_operator_review" }, snapshots: log.split("\n").filter(Boolean).map(line => {
        const [commitHash, createdAt, ...message] = line.split("|");
        return { commitHash, createdAt, message: message.join("|"), retention: retentionEligibility("backups", {
          origin: createdAt, protected: true, dependenciesClosed: false,
        }) };
      }) };
    } catch (error) { return snapshotFailure(error); }
  });
  sdk.registerFunction("mem::snapshot-restore", async (data?: { commitHash: string }) => {
    if (!data || typeof data.commitHash !== "string" || !COMMIT_HASH_RE.test(data.commitHash)) return { success: false, error: "Invalid commitHash format" };
    if (snapshotInFlight) return { success: false, deferred: true, error: "Snapshot already in progress" };
    snapshotInFlight = true;
    let snapshotExecution: { id: string; startedAt: string } | undefined;
    let snapshotTerminal: WorkflowTerminalReceipt | undefined;
    const recordSnapshotTerminal = async (outcome: "completed" | "failed") => {
      if (!snapshotExecution) { await markWorkflowStatisticsGap(kv, "snapshots"); return; }
      snapshotTerminal ??= { executionId: snapshotExecution.id, agentId: getAgentId(), occurredAt: new Date().toISOString(), outcome };
      try {
        await kv.set(KV.state, `workflow-snapshot:${snapshotExecution.id}`, snapshotTerminal);
        await observeWorkflowTerminal(kv, { ...snapshotTerminal, workflow: "snapshots", producer: "snapshot-publication", executionLevel: "workflow" });
      } catch { await markWorkflowStatisticsGap(kv, "snapshots"); }
    };
    try {
      return await withSnapshotTemporary(snapshotDir, "restore", async (path, manifest) => {
        await extractCommit(snapshotDir, data.commitHash, path);
        for await (const _record of readSnapshot(path)) { /* Validate entire immutable input before applying any row. */ }
        await manifest("validated");
        return withBatchMutationLocks(kv, async () => {
          let nodes: RestoredNode[] = [];
          let nodeBytes = 0;
          for await (const record of readSnapshot(path)) {
            if (record.scope === KV.graphNodes) {
              const bytes = Buffer.byteLength(JSON.stringify(record.value));
              if (nodes.length && (nodes.length >= 128 || nodeBytes + bytes > 4 * 1024 * 1024)) {
                await restoreGraph(nodes); nodes = []; nodeBytes = 0;
              }
              nodes.push(record.value as RestoredNode); nodeBytes += bytes;
            } else await kv.set(record.scope, record.key, record.value);
          }
          if (nodes.length) await restoreGraph(nodes);
          await recordAudit(kv, "import", "mem::snapshot-restore", [], { commitHash: data.commitHash });
          await manifest("applied");
          return { success: true, commitHash: data.commitHash };
        });
      });
    } catch (error) { await recordSnapshotTerminal("failed"); return snapshotFailure(error); }
    finally { snapshotInFlight = false; }
  });
}
function snapshotFailure(error: unknown) {
  if (error instanceof Error && error.message === "STATE_SNAPSHOT_MANIFEST_RECOVERY_REQUIRED") {
    return { success: false, deferred: true, retryable: false, code: error.message, error: "Snapshot ownership manifest requires explicit recovery" };
  }
  if (error instanceof BatchMaintenanceBusyError) {
    return { success: false, deferred: true, retryable: true, code: error.code, error: error.message, details: error.details };
  }
  const message = error instanceof Error ? error.message : String(error);
  logger.error("Snapshot failed", { error: message });
  return { success: false, error: message };
}

export async function collectClosedSnapshotTemporary(kv: StateKV, dir: string, closedOwner: string,
  assertMaintenanceOwnership: () => Promise<void>): Promise<{ collected: boolean; reason: string; manifestRetained?: boolean }> {
  if (typeof assertMaintenanceOwnership !== "function") throw new Error("STATE_SNAPSHOT_MAINTENANCE_OWNER_REQUIRED");
  return withKeyedLock(join(dir, ".state-snapshot.manifest.json"), () => collectOwnedTemporary(kv, dir, closedOwner, assertMaintenanceOwnership));
}
async function collectOwnedTemporary(kv: StateKV, dir: string, closedOwner: string,
  assertMaintenanceOwnership: () => Promise<void>): Promise<{ collected: boolean; reason: string; manifestRetained?: boolean }> {
  await assertMaintenanceOwnership();
  if (!/^[0-9a-f-]{36}$/.test(closedOwner)) throw new Error("STATE_SNAPSHOT_INVALID_OWNER");
  if (!await cleanupAuthorized(kv)) return { collected: false, reason: "cleanup_not_authorized" };
  const manifestPath = join(dir, ".state-snapshot.manifest.json");
  let handle: FileHandle;
  try { handle = await open(manifestPath, "r"); }
  catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return { collected: false, reason: "already_collected" };
    throw error;
  }
  let manifest: unknown;
  try {
    if ((await handle.stat()).size > 8192) throw new Error("STATE_SNAPSHOT_INVALID_MANIFEST");
    manifest = JSON.parse(await handle.readFile("utf8")) as unknown;
  } finally { await handle.close(); }
  if (typeof manifest !== "object" || manifest === null || Array.isArray(manifest)) throw new Error("STATE_SNAPSHOT_INVALID_MANIFEST");
  const row = manifest as Record<string, unknown>;
  if (row.version !== 1 || row.owner !== closedOwner || row.temporary !== `.state-${closedOwner}.tmp`
    || !["publish", "restore"].includes(String(row.purpose))) throw new Error("STATE_SNAPSHOT_INVALID_MANIFEST");
  if (row.phase !== "closed") return { collected: false, reason: "owner_closure_not_recorded", manifestRetained: true };
  const decision = retentionEligibility("viewer", { origin: row.createdAt, dependenciesClosed: true });
  if (!decision.eligible) return { collected: false, reason: decision.reason, manifestRetained: true };
  const temporaryPath = join(dir, row.temporary as string);
  await assertMaintenanceOwnership();
  try { await rm(temporaryPath); }
  catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return { collected: false, reason: "already_collected_manifest_recovery_required", manifestRetained: true };
    }
    throw error;
  }
  return { collected: true, reason: "closed_owned_temporary_manifest_recovery_required", manifestRetained: true };
}
