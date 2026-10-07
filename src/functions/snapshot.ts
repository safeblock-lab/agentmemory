import type { IIIClient } from "iii-sdk";
import { BatchMaintenanceBusyError, withBatchMutationLocks, preserveBatchProvenance } from "../state/batch-effects.js";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createWriteStream, existsSync, mkdirSync, readFileSync } from "node:fs";
import { rename, rm } from "node:fs/promises";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { join } from "node:path";
import type {
  SnapshotMeta,
  Session,
  Memory,
  GraphNode,
  AccessLogExport,
} from "../types.js";
import { KV, generateId } from "../state/schema.js";
import type { StateKV } from "../state/kv.js";
import { recordAudit } from "./audit.js";
import { VERSION } from "../version.js";
import { logger } from "../logger.js";
import { graphKV, registerGraphJobHandler, runGraphJob, withCompletedGraphRead, withGraphDelta } from "./graph-jobs.js";
import { iterateProjectedRecords } from "./projected-record-reader.js";

const COMMIT_HASH_RE = /^[0-9a-f]{7,40}$/i;

const execFileAsync = promisify(execFile);

async function gitExec(dir: string, args: string[]): Promise<string> {
  const { stdout } = await execFileAsync("git", args, { cwd: dir });
  return stdout.trim();
}

async function ensureGitRepo(dir: string): Promise<void> {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  if (!existsSync(join(dir, ".git"))) {
    await gitExec(dir, ["init"]);
    await gitExec(dir, ["config", "user.email", "agentmemory@local"]);
    await gitExec(dir, ["config", "user.name", "agentmemory"]);
  }
}

function serializeSnapshotValue(value: unknown, indent: string): string {
  const serialized = JSON.stringify(value, null, 2);
  if (serialized === undefined) throw new Error("Snapshot contains a non-serializable value");
  return `${indent}${serialized.replace(/\n/g, `\n${indent}`)}`;
}

async function* snapshotJsonArray<T>(
  values: Iterable<T> | AsyncIterable<T>,
  itemIndent: string,
  closingIndent: string,
  onValue?: () => void,
): AsyncGenerator<string> {
  yield "[";
  let first = true;
  for await (const value of values) {
    if (!first) yield ",";
    yield `\n${serializeSnapshotValue(value, itemIndent)}`;
    onValue?.();
    first = false;
  }
  if (!first) yield `\n${closingIndent}`;
  yield "]";
}

async function* prependValue<T>(first: T, values: AsyncIterator<T>): AsyncGenerator<T> {
  yield first;
  for (;;) {
    const next = await values.next();
    if (next.done) return;
    yield next.value;
  }
}

interface SnapshotStats {
  observations: number;
  graphNodes: number;
}

async function* snapshotJson(
  kv: StateKV,
  timestamp: string,
  sessions: Session[],
  memories: Memory[],
  accessLogs: AccessLogExport[],
  stats: SnapshotStats,
): AsyncGenerator<string> {
  yield `{"version":${JSON.stringify(VERSION)},"timestamp":${JSON.stringify(timestamp)},\n  "sessions":`;
  yield* snapshotJsonArray(sessions, "    ", "  ");
  yield ",\n  \"memories\":";
  yield* snapshotJsonArray(memories, "    ", "  ");
  yield ",\n  \"graphNodes\":";
  yield* snapshotJsonArray(
    iterateProjectedRecords<GraphNode>(kv, KV.graphNodes),
    "    ",
    "  ",
    () => { stats.graphNodes++; },
  );
  yield ",\n  \"observations\":{";
  let firstSession = true;
  for (const session of sessions) {
    const observations = kv.values<Record<string, unknown>>(KV.observations(session.id))[Symbol.asyncIterator]();
    const first = await observations.next();
    if (first.done) continue;
    if (!firstSession) yield ",";
    yield `\n    ${JSON.stringify(session.id)}:`;
    yield* snapshotJsonArray(
      prependValue(first.value, observations),
      "      ",
      "    ",
      () => { stats.observations++; },
    );
    firstSession = false;
  }
  if (!firstSession) yield "\n  ";
  yield "},\n  \"accessLogs\":";
  yield* snapshotJsonArray(accessLogs, "    ", "  ");
  yield "\n}\n";
}

async function writeSnapshotAtomically(
  kv: StateKV,
  snapshotDir: string,
  timestamp: string,
  sessions: Session[],
  memories: Memory[],
  accessLogs: AccessLogExport[],
  stats: SnapshotStats,
): Promise<void> {
  const statePath = join(snapshotDir, "state.json");
  const temporaryPath = join(snapshotDir, `.state-${generateId("write")}.tmp`);
  try {
    await withCompletedGraphRead(kv, () => pipeline(
      Readable.from(snapshotJson(kv, timestamp, sessions, memories, accessLogs, stats)),
      createWriteStream(temporaryPath, { flags: "wx", encoding: "utf8" }),
    ));
    await rename(temporaryPath, statePath);
  } catch (error) {
    try {
      await rm(temporaryPath, { force: true });
    } catch (cleanupError) {
      logger.warn("Snapshot temporary file cleanup failed", {
        error: cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
      });
    }
    throw error;
  }
}

export function registerSnapshotFunction(
  sdk: IIIClient,
  kv: StateKV,
  snapshotDir: string,
): void {
  kv = graphKV(kv);
  type RestoredNode = { id: string } & Record<string, unknown>;
  const restoreGraph = async (nodes: RestoredNode[], durableId?: string) => runGraphJob(kv, "restore", nodes, (input) => withGraphDelta(kv, async () => {
    for (const node of input as RestoredNode[]) {
      const current = await kv.get<RestoredNode>(KV.graphNodes, node.id);
      await kv.set(KV.graphNodes, node.id, preserveBatchProvenance(current, node));
    }
    return { success: true };
  }), durableId);
  registerGraphJobHandler(kv, "restore", (input, id) => restoreGraph(input as RestoredNode[], id));
  // Serialize snapshots: the periodic timer, REST (api::snapshot-create), and
  // MCP can all trigger this concurrently. Two runs writing state.json and
  // committing in the same git repo at once race on the index lock. An
  // overlapping call is a no-op success; the winner captures current state.
  let snapshotInFlight = false;

  sdk.registerFunction("mem::snapshot-create",
    async (data?: { message?: string }) => {
      if (snapshotInFlight) {
        return { success: true, message: "Snapshot already in progress" };
      }
      snapshotInFlight = true;

      try {
        return await withBatchMutationLocks(kv, async () => {
          await ensureGitRepo(snapshotDir);
          const ts = new Date().toISOString();

          const sessions = await kv.list<Session>(KV.sessions);
          const memories = await kv.list<Memory>(KV.memories);
          const accessLogs = await kv
            .list<AccessLogExport>(KV.accessLog)
            .catch(() => [] as AccessLogExport[]);
          const stats = { observations: 0, graphNodes: 0 } satisfies SnapshotStats;
          await writeSnapshotAtomically(
            kv,
            snapshotDir,
            ts,
            sessions,
            memories,
            accessLogs,
            stats,
          );

          await gitExec(snapshotDir, ["add", "--", "state.json"]);

          const message = data?.message || `Snapshot ${ts}`;
          try {
            await gitExec(snapshotDir, ["commit", "-m", message]);
          } catch (commitErr) {
            const errMsg =
              commitErr instanceof Error ? commitErr.message : String(commitErr);
            if (errMsg.includes("nothing to commit")) {
              return { success: true, message: "No changes to snapshot" };
            }
            throw commitErr;
          }

          const commitHash = await gitExec(snapshotDir, ["rev-parse", "HEAD"]);

          const meta: SnapshotMeta = {
            id: generateId("snap"),
            commitHash,
            createdAt: ts,
            message,
            stats: {
              sessions: sessions.length,
              observations: stats.observations,
              memories: memories.length,
              graphNodes: stats.graphNodes,
            },
          };

          await recordAudit(kv, "export", "mem::snapshot-create", [meta.id], {
            commitHash,
            stats: meta.stats,
          });

          logger.info("Snapshot created", { commitHash });
          return { success: true, snapshot: meta };
        });
      } catch (err) {
        if (err instanceof BatchMaintenanceBusyError) {
          logger.warn("Snapshot deferred", { code: err.code, ...err.details });
          return { success: false, deferred: true, retryable: true, code: err.code, error: err.message, details: err.details };
        }
        const msg = err instanceof Error ? err.message : String(err);
        logger.error("Snapshot failed", { error: msg });
        return { success: false, error: msg };
      } finally {
        snapshotInFlight = false;
      }
    },
  );

  sdk.registerFunction("mem::snapshot-list", async () => {
    try {
      if (!existsSync(join(snapshotDir, ".git"))) {
        return { snapshots: [] };
      }
      const log = await gitExec(snapshotDir, [
        "log",
        "--format=%H|%aI|%s",
        "-20",
      ]);
      const snapshots = log
        .split("\n")
        .filter(Boolean)
        .map((line) => {
          const parts = line.split("|");
          const [hash, date] = parts;
          const msg = parts.slice(2).join("|");
          return { commitHash: hash, createdAt: date, message: msg };
        });
      return { snapshots };
    } catch {
      return { snapshots: [] };
    }
  });

  sdk.registerFunction("mem::snapshot-restore",
    async (data: { commitHash: string } | undefined) => {
      if (!data || typeof data.commitHash !== "string" || !data.commitHash.trim()) {
        return { success: false, error: "commitHash is required" };
      }
      if (!COMMIT_HASH_RE.test(data.commitHash)) {
        return { success: false, error: "Invalid commitHash format" };
      }

      try {
        return await withBatchMutationLocks(kv, async () => {
          await gitExec(snapshotDir, [
            "checkout",
            data.commitHash,
            "--",
            "state.json",
          ]);
          const content = readFileSync(join(snapshotDir, "state.json"), "utf-8");
          const state = JSON.parse(content) as {
            sessions?: Array<{ id: string } & Record<string, unknown>>;
            memories?: Array<{ id: string } & Record<string, unknown>>;
            graphNodes?: Array<{ id: string } & Record<string, unknown>>;
            observations?: Record<
              string,
              Array<{ id: string } & Record<string, unknown>>
            >;
            accessLogs?: AccessLogExport[];
          };

          if (state.sessions) {
            for (const session of state.sessions) {
              await kv.set(KV.sessions, session.id, session);
            }
          }
          if (state.memories) {
            for (const memory of state.memories) {
              await kv.set(KV.memories, memory.id, memory);
            }
          }
          if (state.graphNodes) {
            await restoreGraph(state.graphNodes);
          }
          if (state.observations) {
            for (const [sessionId, obs] of Object.entries(state.observations)) {
              for (const o of obs) {
                await kv.set(KV.observations(sessionId), o.id, o);
              }
            }
          }
          if (state.accessLogs) {
            for (const log of state.accessLogs) {
              await kv.set(KV.accessLog, log.memoryId, log);
            }
          }

          await gitExec(snapshotDir, ["checkout", "HEAD", "--", "state.json"]);

          await recordAudit(kv, "import", "mem::snapshot-restore", [], {
            commitHash: data.commitHash,
            sessions: state.sessions?.length || 0,
            memories: state.memories?.length || 0,
            graphNodes: state.graphNodes?.length || 0,
          });

          logger.info("Snapshot restored", {
            commitHash: data.commitHash,
          });
          return { success: true, commitHash: data.commitHash };
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error("Snapshot restore failed", { error: msg });
        return { success: false, error: msg };
      }
    },
  );
}
