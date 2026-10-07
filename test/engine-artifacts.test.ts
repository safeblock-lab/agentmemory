import { createHash } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  parseEngineVersion,
  validateStagedEngineArtifacts,
} from "../scripts/build-iii-engine.mjs";
import {
  createStateMigrationReceipt,
  hasRequiredEngineCapabilitiesOutput,
  hasValidStateMigrationReceipt,
  PINNED_ENGINE_SOURCE_COMMIT,
  REQUIRED_ENGINE_CAPABILITIES,
  resolveBundledEngineArtifact,
  stateMigrationTargetIdentity,
} from "../src/cli/engine-artifacts.js";

const buildRoot = join(process.cwd(), ".native-pagination-build", "engine-activation", "artifact-tests");
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function createBundle() {
  mkdirSync(buildRoot, { recursive: true });
  const root = mkdtempSync(join(buildRoot, "case-"));
  roots.push(root);
  const binaryPath = join(root, "win32-x64", "iii.exe");
  mkdirSync(join(root, "win32-x64"), { recursive: true });
  const bytes = Buffer.from("verified patched engine fixture");
  writeFileSync(binaryPath, bytes);
  writeFileSync(
    join(root, "manifest.json"),
    JSON.stringify({
      schemaVersion: 1,
      engineVersion: "0.22.1",
      capabilities: [...REQUIRED_ENGINE_CAPABILITIES],
      source: {
        repository: "https://github.com/iii-hq/iii.git",
        commit: PINNED_ENGINE_SOURCE_COMMIT,
        patchPath: "patches/iii-engine/0.22.1-state-pagination.patch",
        patchSha256: "a".repeat(64),
      },
      artifacts: {
        "win32-x64": {
          path: "win32-x64/iii.exe",
          sha256: createHash("sha256").update(bytes).digest("hex"),
          sizeBytes: bytes.byteLength,
        },
      },
    }),
  );
  return { root, binaryPath };
}

describe("bundled iii-engine artifacts", () => {
  it("selects the pinned artifact only after its checksum verifies", () => {
    const { root, binaryPath } = createBundle();

    expect(resolveBundledEngineArtifact(root, "win32", "x64")).toEqual({
      ok: true,
      binaryPath,
      sha256: createHash("sha256")
        .update(Buffer.from("verified patched engine fixture"))
        .digest("hex"),
    });
  });

  it("reports a missing manifest and an unsupported platform", () => {
    const missing = mkdtempSync(join(buildRoot, "missing-"));
    roots.push(missing);
    expect(resolveBundledEngineArtifact(missing, "win32", "x64")).toMatchObject({
      ok: false,
      code: "missing-manifest",
    });

    const { root } = createBundle();
    expect(resolveBundledEngineArtifact(root, "linux", "x64")).toMatchObject({
      ok: false,
      code: "unsupported-platform",
    });
  });

  it("rejects a modified artifact and a path outside the package", () => {
    const { root, binaryPath } = createBundle();
    writeFileSync(binaryPath, "changed after manifest generation");
    expect(resolveBundledEngineArtifact(root, "win32", "x64")).toMatchObject({
      ok: false,
      code: "checksum-mismatch",
    });

    const escaped = createBundle();
    const manifestPath = join(escaped.root, "manifest.json");
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
      artifacts: Record<string, { path: string }>;
    };
    manifest.artifacts["win32-x64"]!.path = "../outside.exe";
    writeFileSync(manifestPath, JSON.stringify(manifest));
    expect(resolveBundledEngineArtifact(escaped.root, "win32", "x64")).toMatchObject({
      ok: false,
      code: "invalid-manifest",
    });
  });

  it("rejects the wrong engine version, capability, and source pin", () => {
    const wrongVersion = createBundle();
    const versionManifest = JSON.parse(
      readFileSync(join(wrongVersion.root, "manifest.json"), "utf8"),
    ) as { engineVersion: string };
    versionManifest.engineVersion = "0.22.0";
    writeFileSync(join(wrongVersion.root, "manifest.json"), JSON.stringify(versionManifest));
    expect(resolveBundledEngineArtifact(wrongVersion.root, "win32", "x64")).toMatchObject({
      ok: false,
      code: "invalid-manifest",
    });

    const missingCapability = createBundle();
    const capabilityManifest = JSON.parse(
      readFileSync(join(missingCapability.root, "manifest.json"), "utf8"),
    ) as { capabilities: string[] };
    capabilityManifest.capabilities = [];
    writeFileSync(
      join(missingCapability.root, "manifest.json"),
      JSON.stringify(capabilityManifest),
    );
    expect(
      resolveBundledEngineArtifact(missingCapability.root, "win32", "x64"),
    ).toMatchObject({ ok: false, code: "invalid-manifest" });

    const wrongSource = createBundle();
    const sourceManifest = JSON.parse(
      readFileSync(join(wrongSource.root, "manifest.json"), "utf8"),
    ) as { source: { commit: string } };
    sourceManifest.source.commit = "0".repeat(40);
    writeFileSync(join(wrongSource.root, "manifest.json"), JSON.stringify(sourceManifest));
    expect(resolveBundledEngineArtifact(wrongSource.root, "win32", "x64")).toMatchObject({
      ok: false,
      code: "invalid-manifest",
    });
  });

  it("validates staged package manifests, paths, patch pins, and artifact hashes", () => {
    const valid = createBundle();
    expect(validateStagedEngineArtifacts(valid.root).engineVersion).toBe("0.22.1");
    expect(() => validateStagedEngineArtifacts(valid.root, "f".repeat(64))).toThrow(
      "patch hash does not match",
    );

    const badCapability = createBundle();
    const capabilityManifest = JSON.parse(
      readFileSync(join(badCapability.root, "manifest.json"), "utf8"),
    ) as { capabilities: string[] };
    capabilityManifest.capabilities = [];
    writeFileSync(
      join(badCapability.root, "manifest.json"),
      JSON.stringify(capabilityManifest),
    );
    expect(() => validateStagedEngineArtifacts(badCapability.root)).toThrow(
      "Staged engine manifest does not describe the pinned indexed state build.",
    );

    const escaped = createBundle();
    const pathManifest = JSON.parse(
      readFileSync(join(escaped.root, "manifest.json"), "utf8"),
    ) as { artifacts: Record<string, { path: string }> };
    pathManifest.artifacts["win32-x64"]!.path = "../outside.exe";
    writeFileSync(join(escaped.root, "manifest.json"), JSON.stringify(pathManifest));
    expect(() => validateStagedEngineArtifacts(escaped.root)).toThrow(
      "Invalid staged engine entry",
    );

    const tampered = createBundle();
    writeFileSync(tampered.binaryPath, "changed after manifest generation");
    expect(() => validateStagedEngineArtifacts(tampered.root)).toThrow(
      "failed its size or SHA-256 check",
    );
  });

  it("requires every exact state capability marker", () => {
    expect(hasRequiredEngineCapabilitiesOutput(JSON.stringify({ capabilities: [
      "state::list_page", "state::get_versioned", "state::lease", "state::commit_batch",
      "state::sqlite_wal_v1", "state::shadow_migration_v1",
    ] }))).toBe(false);
    expect(hasRequiredEngineCapabilitiesOutput(JSON.stringify({ capabilities: [...REQUIRED_ENGINE_CAPABILITIES] }))).toBe(true);
    expect(hasRequiredEngineCapabilitiesOutput(JSON.stringify({ capabilities: [...REQUIRED_ENGINE_CAPABILITIES, "state::unknown"] }))).toBe(false);
    expect(hasRequiredEngineCapabilitiesOutput(JSON.stringify({ capabilities: REQUIRED_ENGINE_CAPABILITIES.filter((capability) => capability !== "state::scope_revision_v1") }))).toBe(false);
    expect(hasRequiredEngineCapabilitiesOutput("iii 0.22.1")).toBe(false);
    expect(hasRequiredEngineCapabilitiesOutput("state::list_page_old")).toBe(false);
    for (const missing of REQUIRED_ENGINE_CAPABILITIES) {
      expect(hasRequiredEngineCapabilitiesOutput(JSON.stringify({
        capabilities: REQUIRED_ENGINE_CAPABILITIES.filter((capability) => capability !== missing),
      }))).toBe(false);
    }
  });

  it("accepts only structured READY receipts bound to paths and target identity", () => {
    const root = mkdtempSync(join(buildRoot, "receipt-"));
    roots.push(root);
    const source = join(root, "state_store.db");
    const target = join(root, "state_store.sqlite3");
    const replacement = join(root, "replacement.sqlite3");
    mkdirSync(source);
    writeFileSync(target, "native READY shadow fixture");
    const identity = stateMigrationTargetIdentity(target);
    expect(identity).not.toBeNull();
    const ready = JSON.stringify({
      status: "ready",
      target,
      scopes: 2,
      records: 17,
      source_retained: true,
      source_manifest_sha256: "b".repeat(64),
    });
    const receipt = createStateMigrationReceipt(ready, source, target, "134217728", identity!);
    const serialized = JSON.stringify(receipt);
    expect(hasValidStateMigrationReceipt(serialized, source, target, identity!)).toBe(true);
    expect(receipt.sourceManifestSha256).toBe("b".repeat(64));
    expect(hasValidStateMigrationReceipt(
      JSON.stringify({ ...receipt, schemaVersion: 2 }),
      source,
      target,
      identity!,
    )).toBe(false);
    expect(hasValidStateMigrationReceipt(
      JSON.stringify({ ...receipt, engineVersion: "0.22.0" }),
      source,
      target,
      identity!,
    )).toBe(false);
    expect(hasValidStateMigrationReceipt(serialized, join(root, "other-source"), target, identity!)).toBe(false);
    expect(hasValidStateMigrationReceipt("{malformed", source, target, identity!)).toBe(false);
    expect(hasValidStateMigrationReceipt("{}", source, target, identity!)).toBe(false);

    writeFileSync(replacement, "different empty SQLite fixture");
    const replacementIdentity = stateMigrationTargetIdentity(replacement);
    expect(replacementIdentity).not.toEqual(identity);
    rmSync(target);
    renameSync(replacement, target);
    const currentIdentity = stateMigrationTargetIdentity(target);
    expect(currentIdentity).not.toEqual(identity);
    expect(hasValidStateMigrationReceipt(serialized, source, target, currentIdentity!)).toBe(false);
  });

  it("rejects native migration output without an exact READY result", () => {
    const root = mkdtempSync(join(buildRoot, "bad-receipt-"));
    roots.push(root);
    const source = join(root, "state_store.db");
    const target = join(root, "state_store.sqlite3");
    mkdirSync(source);
    writeFileSync(target, "native READY shadow fixture");
    const identity = stateMigrationTargetIdentity(target)!;
    const valid = {
      status: "ready",
      target,
      scopes: 1,
      records: 1,
      source_retained: true,
      source_manifest_sha256: "c".repeat(64),
    };
    expect(() => createStateMigrationReceipt(JSON.stringify({ ...valid, status: "pending" }), source, target, "134217728", identity)).toThrow(/READY/);
    expect(() => createStateMigrationReceipt(JSON.stringify({ ...valid, target: join(root, "other.sqlite3") }), source, target, "134217728", identity)).toThrow(/READY/);
    expect(() => createStateMigrationReceipt(JSON.stringify({ ...valid, source_retained: false }), source, target, "134217728", identity)).toThrow(/READY/);
    expect(() => createStateMigrationReceipt(JSON.stringify({ ...valid, source_manifest_sha256: "bad" }), source, target, "134217728", identity)).toThrow(/READY/);
  });

  it("parses exact engine versions instead of accepting a numeric prefix", () => {
    expect(parseEngineVersion("iii-engine 0.22.1")).toBe("0.22.1");
    expect(parseEngineVersion("iii-engine 0.22.10")).toBe("0.22.10");
    expect(parseEngineVersion("iii-engine 0.22.10")).not.toBe("0.22.1");
  });
});
