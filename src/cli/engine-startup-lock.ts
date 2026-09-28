import { randomUUID } from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

function createOwnerLock(path: string, owner: string): () => void {
  const fd = openSync(path, "wx", 0o600);
  try {
    writeFileSync(fd, owner);
  } catch (error) {
    closeSync(fd);
    unlinkSync(path);
    throw error;
  }
  return () => {
    closeSync(fd);
    try {
      if (readFileSync(path, "utf8") === owner) unlinkSync(path);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  };
}

function staleOwner(path: string, isProcessAlive: (pid: number) => boolean): boolean {
  try {
    const recorded = readFileSync(path, "utf8");
    const pid = Number(recorded.split(":", 1)[0]);
    return Number.isInteger(pid) && pid > 0
      ? !isProcessAlive(pid)
      : Date.now() - statSync(path).mtimeMs > 30_000;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

export async function acquireEngineStartupLock(
  path: string,
  isProcessAlive: (pid: number) => boolean,
  timeoutMs = 10_000,
): Promise<() => void> {
  mkdirSync(dirname(path), { recursive: true });
  const owner = `${process.pid}:${randomUUID()}`;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      return createOwnerLock(path, owner);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      if (staleOwner(path, isProcessAlive)) {
        let releaseRecovery: (() => void) | null = null;
        try {
          releaseRecovery = createOwnerLock(`${path}.recovery`, owner);
          // Only one reclaimer can remove a dead owner's lock. A second
          // contender must re-check after the first reclaimer releases it.
          if (staleOwner(path, isProcessAlive)) {
            try {
              unlinkSync(path);
            } catch (removeError) {
              if ((removeError as NodeJS.ErrnoException).code !== "ENOENT") throw removeError;
            }
          }
        } catch (recoveryError) {
          if ((recoveryError as NodeJS.ErrnoException).code !== "EEXIST") throw recoveryError;
        } finally {
          releaseRecovery?.();
        }
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  throw new Error("Another AgentMemory engine startup is still in progress.");
}
