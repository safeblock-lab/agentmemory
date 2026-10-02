import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { captureGraphCallbackSummary } from "../.native-pagination-build/graph-resource/campaign-runtime-common.js";
import { waitForGraphLeaseExpiry } from "../.native-pagination-build/graph-resource/campaign-lease-wait.js";
import type { StateKV } from "../src/state/kv.js";
import {
  assertResourceRunGates,
  captureExternalProcessMemory,
  captureNodeProcessMemory,
  largestJsonlRecordBytes,
  measurePeakProcessMemory,
  MAX_COMMIT_UTF8_BYTES,
  preflightIsolatedRuntimeBudget,
  preflightResourceBudget,
  probeResourceHost,
  resolveAcceptedNativeArtifact,
  sha256File,
  startIsolatedNativeGraphRuntime,
  utf8CommitBodyBytes,
  writeAmplification,
  writeStreamingGraphFixture,
} from "../.native-pagination-build/graph-resource/resource-runner.js";

const resourceRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../.native-pagination-build/graph-resource");
const fixtureDirectories: string[] = [];

afterAll(() => {
  for (const fixtureDirectory of fixtureDirectories) {
    const target = resolve(fixtureDirectory);
    if (!target.startsWith(`${resourceRoot}${process.platform === "win32" ? "\\" : "/"}`)) throw new Error("Refusing to remove a fixture directory outside the owned resource path");
    rmSync(target, { recursive: true, force: true });
  }
});

describe("native graph resource-runner preparation", () => {
  it("waits for genuine fixture lease expiry without changing durable state", async () => {
    const expiresAtMs = Date.now() + 20;
    const kv = { get: async () => ({ lease: { expires_at_ms: expiresAtMs } }) };
    const result = await waitForGraphLeaseExpiry(kv as unknown as StateKV);
    expect(result.expiresAtMs).toBe(expiresAtMs);
    expect(result.recoveredAfterMs).toBeGreaterThan(expiresAtMs);
    const absent = await waitForGraphLeaseExpiry({ get: async () => null } as unknown as StateKV);
    expect(absent.expiresAtMs).toBeNull();
    await expect(waitForGraphLeaseExpiry({ get: async () => ({ lease: { expires_at_ms: Date.now() + 121_000 } }) } as unknown as StateKV))
      .rejects.toThrow(/unchanged campaign TTL/);
  });
  it("uses versioned existence and rejects any persisted active graph callback", async () => {
    const absent = {
      pages: async function* () {},
      getVersioned: async () => ({ exists: false, value: null, version: "0" }),
    };
    await expect(captureGraphCallbackSummary(absent as unknown as StateKV)).resolves.toMatchObject({ rows: 0 });
    for (const value of [null, { state: "started" }, false]) {
      const present = { ...absent, getVersioned: async () => ({ exists: true, value, version: "1" }) };
      await expect(captureGraphCallbackSummary(present as unknown as StateKV)).rejects.toThrow(/remains active/);
    }
  });
  it("requires the accepted repository artifact path and hash without widening fixture containment", async () => {
    const fixtureDirectory = mkdtempSync(join(resourceRoot, "artifact-boundary-"));
    fixtureDirectories.push(fixtureDirectory);
    const artifactPath = join(fixtureDirectory, "accepted.exe");
    const substitutePath = join(fixtureDirectory, "substitute.exe");
    writeFileSync(artifactPath, "accepted fixture");
    writeFileSync(substitutePath, "accepted fixture");
    const acceptance = { artifactPath, artifactSha256: await sha256File(artifactPath) };
    await expect(resolveAcceptedNativeArtifact(artifactPath, acceptance)).resolves.toBe(artifactPath);
    await expect(resolveAcceptedNativeArtifact(process.execPath, acceptance)).rejects.toThrow(/inside the repository/);
    await expect(resolveAcceptedNativeArtifact(substitutePath, acceptance)).rejects.toThrow(/accepted artifact path/);
    writeFileSync(artifactPath, "changed fixture");
    await expect(resolveAcceptedNativeArtifact(artifactPath, acceptance)).rejects.toThrow(/accepted artifact hash/);
    const escapedDirectory = join(fixtureDirectory, "escaped-directory");
    symlinkSync(dirname(process.execPath), escapedDirectory, "junction");
    await expect(resolveAcceptedNativeArtifact(join(escapedDirectory, process.platform === "win32" ? "node.exe" : "node"), acceptance))
      .rejects.toThrow(/inside the repository/);
    await expect(preflightIsolatedRuntimeBudget(resourceRoot, join(escapedDirectory, process.platform === "win32" ? "node.exe" : "node"), 1))
      .rejects.toThrow(/inside the owned resource directory/);
  });
  it("preflights RAM and disk from the frozen provisional target", () => {
    expect(preflightResourceBudget({
      inputBytes: 1_000_000,
      outputBytes: 1_500_000,
      largestIndivisibleRecordBytes: 1_000_000,
      availableRamBytes: 1_500_000_000,
      availableDiskBytes: 3_000_000,
    })).toEqual({ requiredRamBytes: 269_435_456, requiredDiskBytes: 2_500_000 });
    expect(preflightResourceBudget({
      inputBytes: 2_000_000_000,
      outputBytes: 3_000_000_000,
      largestIndivisibleRecordBytes: 1_000_000,
      availableRamBytes: 269_435_456,
      availableDiskBytes: 5_000_000_000,
    })).toEqual({ requiredRamBytes: 269_435_456, requiredDiskBytes: 5_000_000_000 });
    expect(() => preflightResourceBudget({ inputBytes: 10, outputBytes: 0, largestIndivisibleRecordBytes: 10, availableRamBytes: 10, availableDiskBytes: 10 })).toThrow(/Insufficient RAM preflight/);
    expect(() => preflightResourceBudget({
      inputBytes: 10,
      outputBytes: 1,
      largestIndivisibleRecordBytes: 10,
      availableRamBytes: 256 * 1024 * 1024 + 10,
      availableDiskBytes: 10,
    })).toThrow(/Insufficient disk preflight/);
  });

  it("streams a small synthetic UTF-8 JSONL fixture without assembling it in memory", async () => {
    const fixtureDirectory = mkdtempSync(join(resourceRoot, "fixture-") );
    fixtureDirectories.push(fixtureDirectory);
    const fixturePath = join(fixtureDirectory, "synthetic.jsonl");
    const bytesWritten = await writeStreamingGraphFixture(fixturePath, 1_000_000);
    const bytes = readFileSync(fixturePath);
    const records = bytes.toString("utf8").trimEnd().split("\n").map((line) => JSON.parse(line) as { id: string; title: string; ordinal: number });
    const largestRecordBytes = Math.max(...bytes.toString("utf8").trimEnd().split("\n").map((line) => Buffer.byteLength(line, "utf8")));
    expect(bytesWritten).toBe(bytes.byteLength);
    expect(bytesWritten).toBeGreaterThanOrEqual(1_000_000);
    expect(await largestJsonlRecordBytes(fixturePath)).toBe(largestRecordBytes);
    expect(records.length).toBeGreaterThan(1);
    expect(new Set(records.map((record) => record.id)).size).toBe(records.length);
    expect(records[0]).toMatchObject({ id: "resource-000000000000", ordinal: 0 });
    expect(records[0]?.title).toContain("UTF-8");

    const preflight = await preflightIsolatedRuntimeBudget(resourceRoot, fixturePath, 1_500_000);
    expect(preflight).toMatchObject({
      inputBytes: bytesWritten,
      projectedOutputBytes: 1_500_000,
      largestIndivisibleRecordBytes: largestRecordBytes,
      requiredRamBytes: 256 * 1024 * 1024 + largestRecordBytes,
      requiredDiskBytes: bytesWritten + 1_500_000,
    });
  });

  it("rejects isolated runtime startup before launch when resource preflight inputs or budgets are invalid", async () => {
    const fixtureDirectory = mkdtempSync(join(resourceRoot, "preflight-") );
    fixtureDirectories.push(fixtureDirectory);
    const resourceInputPath = join(fixtureDirectory, "input.jsonl");
    await writeStreamingGraphFixture(resourceInputPath, 1_024);
    await expect(startIsolatedNativeGraphRuntime({
      resourceRoot,
      enginePath: join(resourceRoot, "missing-engine.exe"),
      configTemplatePath: join(resourceRoot, "native-engine-config-template.yaml"),
      resourceInputPath: "",
      projectedOutputBytes: 1_000,
      fixedProviderResponse: "<entities/>",
    })).rejects.toThrow(/Main acceptance manifest required/);
    await expect(preflightIsolatedRuntimeBudget(resourceRoot, "", 1_000))
      .rejects.toThrow(/Resource input path is required/);
    await expect(preflightIsolatedRuntimeBudget(resourceRoot, resourceInputPath, Number.MAX_SAFE_INTEGER))
      .rejects.toThrow(/Resource budget totals exceed the safe integer range/);

    await expect(preflightIsolatedRuntimeBudget(
      resourceRoot,
      resolve(resourceRoot, "..", "outside-resource.jsonl"),
      1_000,
    )).rejects.toThrow(/inside the owned resource directory/);
  });

  it("reports both Node and native-process memory dimensions and commit sizing", async () => {
    const nodeSample = captureNodeProcessMemory();
    const engineLikeSample = await captureExternalProcessMemory(process.pid);
    expect(nodeSample.rssBytes).toBeGreaterThan(0);
    expect(nodeSample.heapUsedBytes).toBeGreaterThan(0);
    expect(engineLikeSample.privateBytes).toBeGreaterThan(0);
    expect(engineLikeSample.residentBytes).toBeGreaterThan(0);
    const measured = await measurePeakProcessMemory(process.pid, async () => "measured", 10);
    expect(measured.result).toBe("measured");
    expect(measured.peak.node.rssBytes).toBeGreaterThan(0);
    expect(measured.peak.external.privateBytes).toBeGreaterThan(0);
    expect(measured.peak.external.residentBytes).toBeGreaterThan(0);
    expect(utf8CommitBodyBytes({ response: "á→" })).toBe(Buffer.byteLength(JSON.stringify({ response: "á→" }), "utf8"));
    expect(MAX_COMMIT_UTF8_BYTES).toBe(4 * 1024 * 1024);
    expect(writeAmplification(1000, 1750)).toBe(1.75);
  });

  it("probes host capacity and refuses runtime acceptance while any gate is open", async () => {
    const host = await probeResourceHost(resourceRoot);
    expect(host.availableRamBytes).toBeGreaterThan(0);
    expect(host.availableDiskBytes).toBeGreaterThan(0);
    expect(() => assertResourceRunGates({
      explicitActivation: false,
      acceptedSourcePins: false,
      acceptedNativeArtifactProof: true,
      passingExistingHarness: false,
    })).toThrow(/explicitActivation.*acceptedSourcePins.*passingExistingHarness/);
  });
});
