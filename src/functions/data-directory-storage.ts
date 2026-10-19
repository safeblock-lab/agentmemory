import { lstat, opendir, realpath } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { performance } from "node:perf_hooks";

const CACHE_MS = 60_000;
const BUDGET_MS = 2_000;
const MAX_ENTRIES = 100_000;
const MAX_DEPTH = 64;

type ScanReason = "time_budget" | "entry_limit" | "depth_limit" | "links_excluded"
  | "unsupported_entry" | "unsafe_path" | "read_failed" | "size_overflow";

interface StorageAttempt {
  startedAt: string;
  finishedAt: string;
  complete: boolean;
  observedBytes: number;
  files: number;
  entries: number;
  reason: ScanReason | null;
}

export interface DataDirectoryStorageMetric {
  source: "local_file_metadata";
  directory: string | null;
  state: "current" | "stale" | "partial" | "unavailable" | "measuring";
  fileLengthBytes: number | null;
  allocatedBytes: null;
  files: number | null;
  sampleAt: string | null;
  complete: boolean;
  coverage: "all_descendant_regular_file_lengths";
  refreshing: boolean;
  reason: ScanReason | "directory_unconfigured" | "directory_not_absolute" | null;
  lastAttempt: StorageAttempt | null;
}

function inside(root: string, path: string): boolean {
  const rel = relative(root, path);
  return rel === "" || (!isAbsolute(rel) && rel !== ".." && !rel.startsWith(`..${sep}`));
}

export function createDataDirectoryStorageReader(
  getDirectory: () => string | undefined = () => process.env["AGENTMEMORY_DATA_DIR"],
) {
  let directory: string | null = null;
  let sample: StorageAttempt | null = null;
  let attempt: StorageAttempt | null = null;
  let pending: Promise<void> | null = null;
  let retryAt = 0;

  function startScan(root: string): void {
    const started = performance.now();
    const progress = { observedBytes: 0, files: 0, entries: 0 };
    const startedAt = new Date().toISOString();
    let reason: ScanReason | null = null;
    let timedOut = false;
    const stop = (value: ScanReason): never => { reason = value; throw new Error(value); };
    const withinBudget = () => {
      if (performance.now() - started >= BUDGET_MS) stop("time_budget");
    };
    const finish = (complete: boolean): StorageAttempt => ({
      startedAt, finishedAt: new Date().toISOString(), complete, ...progress, reason,
    });
    const publish = (value: StorageAttempt) => {
      if (directory !== root) return;
      attempt = value;
      if (value.complete) sample = value;
      retryAt = performance.now() + CACHE_MS;
    };
    const timer = setTimeout(() => {
      timedOut = true;
      reason = "time_budget";
      publish(finish(false));
    }, BUDGET_MS);
    timer.unref();

    const run = async () => {
      try {
        const rootInfo = await lstat(root);
        withinBudget();
        if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) stop("unsafe_path");
        const canonicalRoot = await realpath(root);
        withinBudget();
        const walk = async (path: string, depth: number): Promise<void> => {
          withinBudget();
          if (depth > MAX_DEPTH) stop("depth_limit");
          const canonical = await realpath(path);
          withinBudget();
          if (!inside(canonicalRoot, canonical)) stop("unsafe_path");
          const info = await lstat(path);
          withinBudget();
          if (info.isSymbolicLink()) { reason ??= "links_excluded"; return; }
          if (info.isFile()) {
            if (!Number.isSafeInteger(info.size) || info.size < 0
              || !Number.isSafeInteger(progress.observedBytes + info.size)) stop("size_overflow");
            progress.observedBytes += info.size;
            progress.files++;
            return;
          }
          if (!info.isDirectory()) { reason ??= "unsupported_entry"; return; }
          const handle = await opendir(path);
          try {
            while (true) {
              withinBudget();
              const entry = await handle.read();
              withinBudget();
              if (!entry) break;
              if (++progress.entries > MAX_ENTRIES) stop("entry_limit");
              if (entry.isSymbolicLink()) { reason ??= "links_excluded"; continue; }
              await walk(join(path, entry.name), depth + 1);
            }
          } finally {
            await handle.close();
          }
        };
        await walk(root, 0);
        withinBudget();
      } catch {
        reason ??= "read_failed";
      } finally {
        clearTimeout(timer);
        if (!timedOut) publish(finish(reason === null));
      }
    };
    pending = run().finally(() => { pending = null; });
  }

  return function read(): DataDirectoryStorageMetric {
    const configured = getDirectory();
    const root = configured && isAbsolute(configured) ? resolve(configured) : null;
    if (root !== directory) {
      directory = root;
      sample = null;
      attempt = null;
      retryAt = 0;
    }
    if (root && !pending && performance.now() >= retryAt) startScan(root);
    const stale = sample !== null && (performance.now() >= retryAt || attempt !== sample || pending !== null);
    return {
      source: "local_file_metadata", directory: root,
      state: sample ? stale ? "stale" : "current"
        : attempt ? attempt.files > 0 ? "partial" : "unavailable" : root && pending ? "measuring" : "unavailable",
      fileLengthBytes: sample?.observedBytes ?? null, allocatedBytes: null,
      files: sample?.files ?? null, sampleAt: sample?.finishedAt ?? null,
      complete: sample !== null, coverage: "all_descendant_regular_file_lengths",
      refreshing: root !== null && pending !== null,
      reason: !configured ? "directory_unconfigured" : !root ? "directory_not_absolute" : attempt?.reason ?? null,
      lastAttempt: attempt,
    };
  };
}

export const readDataDirectoryStorage = createDataDirectoryStorageReader();
