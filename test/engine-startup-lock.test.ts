import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { acquireEngineStartupLock } from "../src/cli/engine-startup-lock.js";

const directories: string[] = [];

function lockPath(): string {
  const directory = mkdtempSync(join(tmpdir(), "agentmemory-startup-lock-"));
  directories.push(directory);
  return join(directory, "engine-startup.lock");
}

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe("native engine startup lock", () => {
  it("serializes launch decisions across concurrent CLI processes", async () => {
    const path = lockPath();
    const firstRelease = await acquireEngineStartupLock(path, () => true);
    let secondAcquired = false;
    const second = acquireEngineStartupLock(path, () => true, 1000).then((release) => {
      secondAcquired = true;
      return release;
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(secondAcquired).toBe(false);
    firstRelease();
    const secondRelease = await second;
    expect(secondAcquired).toBe(true);
    expect(readFileSync(path, "utf8")).toContain(String(process.pid));
    secondRelease();
  });

  it("recovers a lock from a dead process", async () => {
    const path = lockPath();
    writeFileSync(path, "99999999:abandoned");
    const release = await acquireEngineStartupLock(path, () => false);
    expect(readFileSync(path, "utf8")).toContain(String(process.pid));
    release();
  });

  it("keeps two simultaneous stale-lock reclaimers mutually exclusive", async () => {
    const path = lockPath();
    writeFileSync(path, "99999999:abandoned");
    let active = 0;
    let maxActive = 0;
    const run = async () => {
      const release = await acquireEngineStartupLock(path, () => false, 3000);
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 150));
      active -= 1;
      release();
    };
    await Promise.all([run(), run()]);
    expect(maxActive).toBe(1);
  });

  it("fails closed if the recovery lock cannot be acquired", async () => {
    const path = lockPath();
    writeFileSync(path, "99999999:abandoned");
    writeFileSync(`${path}.recovery`, "12345:unverifiable");
    await expect(acquireEngineStartupLock(path, () => false, 120))
      .rejects.toThrow("Another AgentMemory engine startup is still in progress.");
    expect(readFileSync(path, "utf8")).toBe("99999999:abandoned");
  });

  it("fails within a bounded time while another owner is alive", async () => {
    const path = lockPath();
    writeFileSync(path, "12345:active");
    await expect(acquireEngineStartupLock(path, () => true, 120))
      .rejects.toThrow("Another AgentMemory engine startup is still in progress.");
  });
});
