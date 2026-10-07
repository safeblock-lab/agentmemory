import { createHash } from "node:crypto";
import { lstatSync, readFileSync, realpathSync, statSync } from "node:fs";
import { isAbsolute, relative, resolve, sep } from "node:path";

export const REQUIRED_ENGINE_CAPABILITIES = [
  "state::list_page",
  "state::get_versioned",
  "state::lease",
  "state::commit_batch",
  "state::sqlite_wal_v2",
  "state::native_storage_v2",
  "state::shadow_migration_v1",
  "state::terminal_retention_v1",
  "state::scope_revision_v1",
] as const;
export const PINNED_ENGINE_VERSION = "0.22.1";
export const PINNED_ENGINE_REPOSITORY = "https://github.com/iii-hq/iii.git";
export const PINNED_ENGINE_PATCH_PATH =
  "patches/iii-engine/0.22.1-state-pagination.patch";
export const PINNED_ENGINE_SOURCE_COMMIT =
  "e7de3820d1e558f3762edf95e4440552444d48d3";

type EngineArtifactEntry = {
  path: string;
  sha256: string;
  sizeBytes: number;
};

type EngineArtifactManifest = {
  schemaVersion: 1;
  engineVersion: string;
  capabilities: string[];
  source: {
    repository: string;
    commit: string;
    patchPath: string;
    patchSha256: string;
  };
  artifacts: Record<string, EngineArtifactEntry>;
};

const ENGINE_ARTIFACT_EXECUTABLES: Record<string, string> = {
  "win32-x64": "iii.exe",
  "win32-arm64": "iii.exe",
  "linux-x64": "iii",
  "linux-arm64": "iii",
  "darwin-x64": "iii",
  "darwin-arm64": "iii",
};

export type EngineArtifactResolution =
  | { ok: true; binaryPath: string; sha256: string }
  | {
      ok: false;
      code:
        | "missing-manifest"
        | "invalid-manifest"
        | "unsupported-platform"
        | "missing-artifact"
        | "checksum-mismatch";
      message: string;
    };

export type StateMigrationReceipt = {
  schemaVersion: 1;
  engineVersion: string;
  sourcePath: string;
  targetPath: string;
  status: "ready";
  sourceRetained: true;
  scopes: number;
  records: number;
  sourceManifestSha256: string;
  diskBudgetBytes: string;
  targetIdentity: StateMigrationTargetIdentity;
};

export type StateMigrationTargetIdentity = {
  device: string;
  inode: string;
  birthtimeMs: number;
};

export function stateMigrationTargetIdentity(
  targetPath: string,
): StateMigrationTargetIdentity | null {
  try {
    const target = lstatSync(targetPath);
    if (target.isSymbolicLink() || !target.isFile() || target.ino <= 0 || target.birthtimeMs <= 0) {
      return null;
    }
    return {
      device: String(target.dev),
      inode: String(target.ino),
      birthtimeMs: target.birthtimeMs,
    };
  } catch {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSha256(value: unknown): value is string {
  return typeof value === "string" && /^[a-f0-9]{64}$/i.test(value);
}

function normalizedAbsolutePath(path: string): string {
  return resolve(path);
}

function samePath(left: string, right: string): boolean {
  const normalizedLeft = normalizedAbsolutePath(left);
  const normalizedRight = normalizedAbsolutePath(right);
  return process.platform === "win32"
    ? normalizedLeft.toLowerCase() === normalizedRight.toLowerCase()
    : normalizedLeft === normalizedRight;
}

function isDiskBudget(value: unknown): value is string {
  if (typeof value !== "string" || !/^[1-9]\d*$/.test(value)) return false;
  try {
    return BigInt(value) <= 18_446_744_073_709_551_615n;
  } catch {
    return false;
  }
}

function parseManifest(
  raw: string,
  expectedVersion: string,
): EngineArtifactManifest | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(value) || value.schemaVersion !== 1) return null;
  if (
    expectedVersion !== PINNED_ENGINE_VERSION ||
    value.engineVersion !== expectedVersion ||
    !hasRequiredEngineCapabilities(value.capabilities)
  ) {
    return null;
  }
  if (!isRecord(value.source) || !isRecord(value.artifacts)) return null;
  if (
    value.source.repository !== PINNED_ENGINE_REPOSITORY ||
    value.source.commit !== PINNED_ENGINE_SOURCE_COMMIT ||
    value.source.patchPath !== PINNED_ENGINE_PATCH_PATH ||
    !isSha256(value.source.patchSha256)
  ) {
    return null;
  }

  const artifacts: Record<string, EngineArtifactEntry> = {};
  for (const [key, entry] of Object.entries(value.artifacts)) {
    const executable = ENGINE_ARTIFACT_EXECUTABLES[key];
    if (
      !executable ||
      !isRecord(entry) ||
      typeof entry.path !== "string" ||
      entry.path !== `${key}/${executable}` ||
      !isSha256(entry.sha256) ||
      !Number.isSafeInteger(entry.sizeBytes) ||
      (entry.sizeBytes as number) < 1
    ) {
      return null;
    }
    artifacts[key] = {
      path: entry.path,
      sha256: entry.sha256,
      sizeBytes: entry.sizeBytes as number,
    };
  }

  return {
    schemaVersion: 1,
    engineVersion: value.engineVersion,
    capabilities: value.capabilities as string[],
    source: {
      repository: value.source.repository,
      commit: value.source.commit,
      patchPath: value.source.patchPath,
      patchSha256: value.source.patchSha256,
    },
    artifacts,
  };
}

function isWithinPath(root: string, candidate: string): boolean {
  const pathFromRoot = relative(root, candidate);
  return (
    pathFromRoot === "" ||
    (pathFromRoot !== ".." &&
      !pathFromRoot.startsWith(`..${sep}`) &&
      !isAbsolute(pathFromRoot))
  );
}

function isSafeArtifactPath(root: string, relativePath: string): boolean {
  if (!relativePath || isAbsolute(relativePath) || relativePath.includes("\\")) {
    return false;
  }
  const parts = relativePath.split("/");
  if (parts.some((part) => !part || part === "." || part === "..")) return false;
  const resolvedRoot = resolve(root);
  const resolvedArtifact = resolve(resolvedRoot, ...parts);
  if (!isWithinPath(resolvedRoot, resolvedArtifact)) return false;

  let currentPath = resolvedRoot;
  try {
    for (const part of parts) {
      currentPath = resolve(currentPath, part);
      if (lstatSync(currentPath).isSymbolicLink()) return false;
    }
    return isWithinPath(realpathSync(resolvedRoot), realpathSync(resolvedArtifact));
  } catch {
    return true;
  }
}

export function resolveBundledEngineArtifact(
  engineRoot: string,
  platform: string,
  arch: string,
  expectedVersion = PINNED_ENGINE_VERSION,
): EngineArtifactResolution {
  const manifestPath = resolve(engineRoot, "manifest.json");
  let raw: string;
  try {
    const stat = lstatSync(manifestPath);
    if (stat.isSymbolicLink() || !stat.isFile() || stat.size > 64 * 1024) {
      return {
        ok: false,
        code: "invalid-manifest",
        message: "The bundled iii-engine manifest is not a small regular file.",
      };
    }
    raw = readFileSync(manifestPath, "utf8");
  } catch {
    return {
      ok: false,
      code: "missing-manifest",
      message: "This agentmemory package does not include the patched iii-engine manifest.",
    };
  }

  const manifest = parseManifest(raw, expectedVersion);
  if (!manifest) {
    return {
      ok: false,
      code: "invalid-manifest",
      message:
        "The bundled iii-engine manifest is invalid or does not declare all required state capabilities.",
    };
  }

  const key = `${platform}-${arch}`;
  const artifact = manifest.artifacts[key];
  if (!artifact) {
    return {
      ok: false,
      code: "unsupported-platform",
      message: `This package has no patched iii-engine artifact for ${key}.`,
    };
  }
  if (!isSafeArtifactPath(engineRoot, artifact.path)) {
    return {
      ok: false,
      code: "invalid-manifest",
      message: `The bundled iii-engine path for ${key} is invalid.`,
    };
  }

  const binaryPath = resolve(engineRoot, ...artifact.path.split("/"));
  let bytes: Buffer;
  try {
    const stat = statSync(binaryPath);
    if (!stat.isFile() || stat.size !== artifact.sizeBytes) {
      return {
        ok: false,
        code: "checksum-mismatch",
        message: `The bundled iii-engine artifact for ${key} has an unexpected size.`,
      };
    }
    bytes = readFileSync(binaryPath);
  } catch {
    return {
      ok: false,
      code: "missing-artifact",
      message: `The bundled iii-engine executable for ${key} is missing.`,
    };
  }

  const sha256 = createHash("sha256").update(bytes).digest("hex");
  if (sha256 !== artifact.sha256) {
    return {
      ok: false,
      code: "checksum-mismatch",
      message: `The bundled iii-engine artifact for ${key} failed its SHA-256 check.`,
    };
  }

  return { ok: true, binaryPath, sha256 };
}

function hasRequiredEngineCapabilities(value: unknown): value is string[] {
  return Array.isArray(value) &&
    value.length === REQUIRED_ENGINE_CAPABILITIES.length &&
    REQUIRED_ENGINE_CAPABILITIES.every((capability) => value.includes(capability));
}

export function hasRequiredEngineCapabilitiesOutput(output: string): boolean {
  const trimmed = output.trim();
  try {
    const parsed: unknown = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      return hasRequiredEngineCapabilities(parsed);
    }
    if (isRecord(parsed) && Array.isArray(parsed.capabilities)) {
      return hasRequiredEngineCapabilities(parsed.capabilities);
    }
  } catch {
    // The native probe may emit one capability per line.
  }
  const advertised = trimmed.split(/[\r\n,\s]+/).filter(Boolean);
  return hasRequiredEngineCapabilities(advertised);
}

export function createStateMigrationReceipt(
  output: string,
  sourcePath: string,
  targetPath: string,
  diskBudgetBytes: string,
  targetIdentity: StateMigrationTargetIdentity,
): StateMigrationReceipt {
  let value: unknown;
  try {
    value = JSON.parse(output);
  } catch {
    throw new Error("The patched iii-engine returned no structured migration result.");
  }
  if (!isRecord(value)) {
    throw new Error("The patched iii-engine returned an invalid migration result.");
  }
  if (
    value.status !== "ready" ||
    value.source_retained !== true ||
    typeof value.target !== "string" ||
    !samePath(value.target, targetPath) ||
    !Number.isSafeInteger(value.scopes) ||
    (value.scopes as number) < 0 ||
    !Number.isSafeInteger(value.records) ||
    (value.records as number) < 0 ||
    !isSha256(value.source_manifest_sha256) ||
    !isDiskBudget(diskBudgetBytes)
  ) {
    throw new Error("The patched iii-engine did not confirm a READY shadow for the requested target.");
  }
  return {
    schemaVersion: 1,
    engineVersion: PINNED_ENGINE_VERSION,
    sourcePath: normalizedAbsolutePath(sourcePath),
    targetPath: normalizedAbsolutePath(targetPath),
    status: "ready",
    sourceRetained: true,
    scopes: value.scopes as number,
    records: value.records as number,
    sourceManifestSha256: value.source_manifest_sha256,
    diskBudgetBytes,
    targetIdentity,
  };
}

export function hasValidStateMigrationReceipt(
  raw: string,
  sourcePath: string,
  targetPath: string,
  targetIdentity: StateMigrationTargetIdentity,
): boolean {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return false;
  }
  return isRecord(value) &&
    value.schemaVersion === 1 &&
    value.engineVersion === PINNED_ENGINE_VERSION &&
    typeof value.sourcePath === "string" && samePath(value.sourcePath, sourcePath) &&
    typeof value.targetPath === "string" && samePath(value.targetPath, targetPath) &&
    value.status === "ready" &&
    value.sourceRetained === true &&
    Number.isSafeInteger(value.scopes) && (value.scopes as number) >= 0 &&
    Number.isSafeInteger(value.records) && (value.records as number) >= 0 &&
    isSha256(value.sourceManifestSha256) &&
    isDiskBudget(value.diskBudgetBytes) &&
    isRecord(value.targetIdentity) &&
    value.targetIdentity.device === targetIdentity.device &&
    value.targetIdentity.inode === targetIdentity.inode &&
    value.targetIdentity.birthtimeMs === targetIdentity.birthtimeMs;
}
