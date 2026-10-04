#!/usr/bin/env node

import { createHash } from "node:crypto";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  realpathSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync, spawnSync } from "node:child_process";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const VERSION = "0.22.1";
const SOURCE_COMMIT = "e7de3820d1e558f3762edf95e4440552444d48d3";
const REPOSITORY = "https://github.com/iii-hq/iii.git";
const PATCH_RELATIVE_PATH = "patches/iii-engine/0.22.1-state-pagination.patch";
const NATIVE_MANIFEST_RELATIVE_PATH = "patches/iii-engine/manifest.json";
const REQUIRED_CAPABILITIES = [
  "state::list_page",
  "state::get_versioned",
  "state::lease",
  "state::commit_batch",
  "state::sqlite_wal_v1",
  "state::shadow_migration_v1",
];
const BUILD_TIMEOUT_MS = 30 * 60 * 1000;
const TARGETS = {
  "win32-x64": { platform: "win32", arch: "x64", triple: "x86_64-pc-windows-msvc", executable: "iii.exe" },
  "win32-arm64": { platform: "win32", arch: "arm64", triple: "aarch64-pc-windows-msvc", executable: "iii.exe" },
  "linux-x64": { platform: "linux", arch: "x64", triple: "x86_64-unknown-linux-gnu", executable: "iii" },
  "linux-arm64": { platform: "linux", arch: "arm64", triple: "aarch64-unknown-linux-gnu", executable: "iii" },
  "darwin-x64": { platform: "darwin", arch: "x64", triple: "x86_64-apple-darwin", executable: "iii" },
  "darwin-arm64": { platform: "darwin", arch: "arm64", triple: "aarch64-apple-darwin", executable: "iii" },
};

function fail(message) {
  throw new Error(message);
}

function hasRequiredCapabilities(value) {
  return Array.isArray(value) &&
    REQUIRED_CAPABILITIES.every((capability) => value.includes(capability));
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd ?? ROOT,
    env: options.env ?? process.env,
    encoding: "utf8",
    stdio: options.inherit ? "inherit" : ["ignore", "pipe", "pipe"],
    timeout: options.timeout ?? BUILD_TIMEOUT_MS,
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error) fail(`${command} failed: ${result.error.message}`);
  if (result.status !== 0) {
    const detail = `${result.stdout ?? ""}${result.stderr ?? ""}`.trim();
    fail(`${command} exited with ${result.status ?? "no status"}${detail ? `: ${detail.slice(-6000)}` : ""}`);
  }
  return result.stdout ?? "";
}

function hashFile(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function isWithinPath(root, candidate) {
  const pathFromRoot = relative(root, candidate);
  return pathFromRoot === "" ||
    (pathFromRoot !== ".." && !pathFromRoot.startsWith(`..${sep}`) && !isAbsolute(pathFromRoot));
}

function assertWorkspacePath(path, label) {
  const workspaceRoot = resolve(ROOT);
  const candidate = resolve(path);
  if (!isWithinPath(workspaceRoot, candidate)) {
    fail(`${label} must stay inside the agentmemory workspace.`);
  }

  let currentPath = workspaceRoot;
  const parts = relative(workspaceRoot, candidate).split(sep).filter(Boolean);
  for (const part of parts) {
    currentPath = join(currentPath, part);
    if (!existsSync(currentPath)) break;
    if (lstatSync(currentPath).isSymbolicLink()) {
      fail(`${label} must not traverse symbolic links.`);
    }
    if (!isWithinPath(realpathSync(workspaceRoot), realpathSync(currentPath))) {
      fail(`${label} resolves outside the agentmemory workspace.`);
    }
  }
  return candidate;
}

function readNativeManifest(patchPath) {
  const manifestPath = assertWorkspacePath(
    join(ROOT, NATIVE_MANIFEST_RELATIVE_PATH),
    "Native patch manifest",
  );
  assertWorkspacePath(patchPath, "Pinned engine patch");
  if (!existsSync(manifestPath)) fail(`Missing native patch manifest: ${NATIVE_MANIFEST_RELATIVE_PATH}`);
  const manifestStat = lstatSync(manifestPath);
  if (manifestStat.isSymbolicLink() || !manifestStat.isFile() || manifestStat.size > 64 * 1024) {
    fail(`Invalid native patch manifest: ${NATIVE_MANIFEST_RELATIVE_PATH}`);
  }
  let value;
  try {
    value = JSON.parse(readFileSync(manifestPath, "utf8"));
  } catch {
    fail(`Invalid native patch manifest: ${NATIVE_MANIFEST_RELATIVE_PATH}`);
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    fail(`Invalid native patch manifest: ${NATIVE_MANIFEST_RELATIVE_PATH}`);
  }
  const commit = value.sourceCommit;
  const patchHash = value.patchSha256;
  if (commit !== SOURCE_COMMIT) fail(`Native patch manifest must pin source commit ${SOURCE_COMMIT}.`);
  if (
    typeof patchHash !== "string" ||
    !/^[a-f0-9]{64}$/i.test(patchHash) ||
    patchHash.toLowerCase() !== hashFile(patchPath)
  ) {
    fail("Native patch manifest SHA-256 does not match the checked-in patch.");
  }
  if (
    typeof value.artifactSha256 !== "string" ||
    !/^[a-f0-9]{64}$/i.test(value.artifactSha256) ||
    !Number.isSafeInteger(value.artifactBytes) ||
    value.artifactBytes < 1 ||
    typeof value.buildTarget !== "string" ||
    !Object.values(TARGETS).some((target) => target.triple === value.buildTarget)
  ) {
    fail("Native patch manifest must bind the verified binary hash, size, and target.");
  }
  if (!hasRequiredCapabilities(value.capabilities)) {
    fail(`Native patch manifest must declare all ${REQUIRED_CAPABILITIES.length} required state capabilities.`);
  }
  return {
    patchSha256: patchHash.toLowerCase(),
    artifactSha256: value.artifactSha256.toLowerCase(),
    artifactBytes: value.artifactBytes,
    buildTarget: value.buildTarget,
    deploymentReady: value.deploymentReady !== false,
    artifactProfile: value.buildProfile?.name ?? null,
  };
}

export function validatePinnedEngineSource() {
  const patchPath = assertWorkspacePath(join(ROOT, PATCH_RELATIVE_PATH), "Pinned engine patch");
  return readNativeManifest(patchPath);
}

export function validateStagedEngineArtifacts(artifactRoot, expectedPatchSha256 = null) {
  artifactRoot = assertWorkspacePath(artifactRoot, "Staged engine artifacts");
  const manifestPath = join(artifactRoot, "manifest.json");
  let manifest;
  try {
    const stat = lstatSync(manifestPath);
    if (stat.isSymbolicLink() || !stat.isFile() || stat.size > 64 * 1024) {
      fail(`Invalid staged engine manifest: ${manifestPath}`);
    }
    manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  } catch {
    fail(`Missing or invalid staged engine manifest: ${manifestPath}`);
  }
  if (!manifest || typeof manifest !== "object" || Array.isArray(manifest)) {
    fail(`Invalid staged engine manifest: ${manifestPath}`);
  }
  if (
    manifest.schemaVersion !== 1 ||
    manifest.engineVersion !== VERSION ||
    !hasRequiredCapabilities(manifest.capabilities) ||
    manifest.source?.repository !== REPOSITORY ||
    manifest.source?.commit !== SOURCE_COMMIT ||
    manifest.source?.patchPath !== PATCH_RELATIVE_PATH ||
    !/^[a-f0-9]{64}$/i.test(manifest.source?.patchSha256 ?? "") ||
    !manifest.artifacts || typeof manifest.artifacts !== "object" ||
    Object.keys(manifest.artifacts).length === 0
  ) {
    fail("Staged engine manifest does not describe the pinned indexed state build.");
  }
  if (
    expectedPatchSha256 !== null &&
    manifest.source.patchSha256.toLowerCase() !== expectedPatchSha256.toLowerCase()
  ) {
    fail("Staged engine manifest patch hash does not match the checked-in native manifest.");
  }
  for (const [key, entry] of Object.entries(manifest.artifacts)) {
    const target = TARGETS[key];
    if (
      !target ||
      !entry || typeof entry !== "object" ||
      entry.path !== `${key}/${target.executable}` ||
      typeof entry.sha256 !== "string" ||
      !/^[a-f0-9]{64}$/i.test(entry.sha256) ||
      !Number.isSafeInteger(entry.sizeBytes) || entry.sizeBytes < 1
    ) {
      fail(`Invalid staged engine entry for ${key}.`);
    }
    const parts = entry.path.split("/");
    if (parts.some((part) => !part || part === "." || part === "..") || entry.path.includes("\\")) {
      fail(`Invalid staged engine path for ${key}.`);
    }
    const binaryPath = assertWorkspacePath(
      resolve(artifactRoot, ...parts),
      `Staged engine artifact ${key}`,
    );
    const stat = lstatSync(binaryPath);
    if (
      stat.isSymbolicLink() ||
      !stat.isFile() ||
      stat.size !== entry.sizeBytes ||
      hashFile(binaryPath) !== entry.sha256
    ) {
      fail(`Staged engine artifact ${key} failed its size or SHA-256 check.`);
    }
  }
  return manifest;
}

function parseArgs(args) {
  const parsed = { binary: null, platform: process.platform, arch: process.arch };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === "--help" || arg === "-h") {
      process.stdout.write("Usage: node scripts/build-iii-engine.mjs [--binary <path>] [--platform <node-platform>] [--arch <node-arch>]\n");
      process.exit(0);
    }
    if (arg === "--binary" || arg === "--platform" || arg === "--arch") {
      const value = args[i + 1];
      if (!value) fail(`${arg} requires a value.`);
      if (arg === "--binary") parsed.binary = resolve(ROOT, value);
      if (arg === "--platform") parsed.platform = value;
      if (arg === "--arch") parsed.arch = value;
      i += 1;
      continue;
    }
    fail(`Unknown argument: ${arg}`);
  }
  return parsed;
}

export function parseEngineVersion(output) {
  const match = output.match(/(\d+\.\d+\.\d+(?:[-+][\w.]+)?)/);
  return match ? match[1] : null;
}

function verifyBinary(binaryPath) {
  let versionOutput;
  let capabilityOutput;
  try {
    versionOutput = execFileSync(binaryPath, ["--version"], { encoding: "utf8", timeout: 5000, windowsHide: true });
    capabilityOutput = execFileSync(binaryPath, ["--capabilities"], { encoding: "utf8", timeout: 5000, windowsHide: true });
  } catch (error) {
    fail(`Could not probe patched iii-engine binary ${binaryPath}: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (parseEngineVersion(versionOutput) !== VERSION) {
    fail(`Engine binary does not report pinned version ${VERSION}.`);
  }
  let capabilities = [];
  try {
    const parsed = JSON.parse(capabilityOutput);
    capabilities = Array.isArray(parsed) ? parsed : parsed?.capabilities ?? [];
  } catch {
    capabilities = capabilityOutput.trim().split(/[\r\n,\s]+/);
  }
  if (!hasRequiredCapabilities(capabilities)) {
    const missing = REQUIRED_CAPABILITIES.filter((capability) => !capabilities.includes(capability));
    fail(`Engine binary does not advertise required state capabilities: ${missing.join(", ")}.`);
  }
}

function prepareSource(buildDir, patchPath) {
  const sourceDir = assertWorkspacePath(
    join(buildDir, "source", SOURCE_COMMIT),
    "Engine source checkout",
  );
  mkdirSync(dirname(sourceDir), { recursive: true });
  if (!existsSync(sourceDir)) {
    mkdirSync(sourceDir, { recursive: true });
    run("git", ["init"], { cwd: sourceDir });
    run("git", ["remote", "add", "origin", REPOSITORY], { cwd: sourceDir });
    run("git", ["fetch", "--depth=1", "origin", SOURCE_COMMIT], { cwd: sourceDir });
    run("git", ["checkout", "--detach", "FETCH_HEAD"], { cwd: sourceDir });
  }
  const commit = run("git", ["rev-parse", "HEAD"], { cwd: sourceDir }).trim();
  if (commit !== SOURCE_COMMIT) fail(`Engine source checkout is ${commit}, expected ${SOURCE_COMMIT}.`);
  try {
    run("git", ["apply", "--reverse", "--check", patchPath], { cwd: sourceDir });
  } catch {
    run("git", ["apply", "--check", patchPath], { cwd: sourceDir });
    run("git", ["apply", patchPath], { cwd: sourceDir });
  }
  return sourceDir;
}

function buildBinary(target, sourceDir, buildDir) {
  const cargoHome = join(buildDir, "cargo-home");
  const targetDir = join(buildDir, "cargo-target");
  mkdirSync(cargoHome, { recursive: true });
  mkdirSync(targetDir, { recursive: true });
  const env = {
    ...process.env,
    CARGO_HOME: cargoHome,
    CARGO_TARGET_DIR: targetDir,
    CARGO_NET_RETRY: "2",
    CARGO_HTTP_TIMEOUT: "30",
    CARGO_INCREMENTAL: "0",
    CARGO_PROFILE_RELEASE_OPT_LEVEL: "s",
    CARGO_PROFILE_RELEASE_STRIP: "true",
    CARGO_PROFILE_RELEASE_DEBUG: "0",
    CARGO_PROFILE_RELEASE_LTO: "false",
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS: "16",
  };
  run("cargo", ["build", "--locked", "--release", "--no-default-features", "--jobs", "1", "--package", "iii", "--bin", "iii", "--target", target.triple], {
    cwd: sourceDir,
    env,
    inherit: true,
  });
  return join(targetDir, target.triple, "release", target.executable);
}

function stageArtifact(binaryPath, key, target, buildDir, patchSha256, expectedNativeArtifact) {
  binaryPath = assertWorkspacePath(binaryPath, "Input engine binary");
  if (!existsSync(binaryPath)) fail(`Built engine executable is missing: ${binaryPath}`);
  verifyBinary(binaryPath);
  if (expectedNativeArtifact) {
    if (
      (expectedNativeArtifact.buildTarget && expectedNativeArtifact.buildTarget !== target.triple) ||
      (expectedNativeArtifact.artifactBytes && statSync(binaryPath).size !== expectedNativeArtifact.artifactBytes) ||
      (expectedNativeArtifact.artifactSha256 && hashFile(binaryPath) !== expectedNativeArtifact.artifactSha256)
    ) {
      fail("Native staged executable does not match its source manifest target, size, or SHA-256.");
    }
  }
  const artifactRoot = assertWorkspacePath(join(buildDir, "artifacts"), "Staged engine artifacts");
  const relativeBinary = `${key}/${target.executable}`;
  const outputPath = assertWorkspacePath(
    join(artifactRoot, ...relativeBinary.split("/")),
    "Staged engine executable",
  );
  const manifestPath = assertWorkspacePath(join(artifactRoot, "manifest.json"), "Staged engine manifest");
  const previous = existsSync(manifestPath) ? validateStagedEngineArtifacts(artifactRoot) : null;
  if (
    previous &&
    (previous.source.patchSha256 !== patchSha256 || previous.source.commit !== SOURCE_COMMIT)
  ) {
    fail("Existing staged engine artifacts were built from a different patch; clear .iii-engine-build/artifacts before rebuilding.");
  }
  if (existsSync(outputPath)) {
    const existingArtifact = lstatSync(outputPath);
    if (existingArtifact.isSymbolicLink() || !existingArtifact.isFile() || existingArtifact.nlink > 1) {
      fail("Refusing to overwrite a linked or non-file staged engine artifact.");
    }
  }
  if (existsSync(manifestPath)) {
    const existingManifest = lstatSync(manifestPath);
    if (existingManifest.isSymbolicLink() || !existingManifest.isFile() || existingManifest.nlink > 1) {
      fail("Refusing to overwrite a linked or non-file staged engine manifest.");
    }
  }
  mkdirSync(dirname(outputPath), { recursive: true });
  if (resolve(binaryPath) !== resolve(outputPath)) copyFileSync(binaryPath, outputPath);
  if (target.platform !== "win32") chmodSync(outputPath, 0o755);
  const bytes = statSync(outputPath).size;
  const artifacts = { ...(previous?.artifacts ?? {}) };
  artifacts[key] = {
    path: relativeBinary,
    sha256: hashFile(outputPath),
    sizeBytes: bytes,
  };
  const manifest = {
    schemaVersion: 1,
    engineVersion: VERSION,
    capabilities: REQUIRED_CAPABILITIES,
    source: {
      repository: REPOSITORY,
      commit: SOURCE_COMMIT,
      patchPath: PATCH_RELATIVE_PATH,
      patchSha256,
    },
    artifacts,
  };
  mkdirSync(artifactRoot, { recursive: true });
  writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
  validateStagedEngineArtifacts(artifactRoot, patchSha256);
  process.stdout.write(`Staged ${key} iii-engine v${VERSION} with ${REQUIRED_CAPABILITIES.length} required state capabilities at ${outputPath}\n`);
}

export function buildIiiEngine({ binary = null, platform = process.platform, arch = process.arch } = {}) {
  const key = `${platform}-${arch}`;
  const target = TARGETS[key];
  if (!target) fail(`Unsupported engine build target ${key}. Supported targets: ${Object.keys(TARGETS).join(", ")}.`);
  const buildDir = resolve(ROOT, process.env["AGENTMEMORY_III_BUILD_DIR"] ?? ".iii-engine-build");
  assertWorkspacePath(buildDir, "AGENTMEMORY_III_BUILD_DIR");
  if (resolve(buildDir) === resolve(ROOT)) fail("AGENTMEMORY_III_BUILD_DIR must stay inside the agentmemory workspace.");
  mkdirSync(buildDir, { recursive: true });
  const patchPath = join(ROOT, PATCH_RELATIVE_PATH);
  if (!existsSync(patchPath)) fail(`Missing engine pagination patch: ${PATCH_RELATIVE_PATH}`);
  const nativeManifest = readNativeManifest(patchPath);
  if (binary && (!nativeManifest.deploymentReady || nativeManifest.artifactProfile === "debug")) {
    fail("The pinned native executable is a measurement-only debug build and cannot be staged for distribution.");
  }
  const binaryPath = binary
    ? resolve(ROOT, binary)
    : buildBinary(target, prepareSource(buildDir, patchPath), buildDir);
  stageArtifact(
    binaryPath,
    key,
    target,
    buildDir,
    nativeManifest.patchSha256,
    binary ? nativeManifest : null,
  );
}

const invokedPath = process.argv[1];
if (invokedPath && resolve(invokedPath) === fileURLToPath(import.meta.url)) {
  try {
    buildIiiEngine(parseArgs(process.argv.slice(2)));
  } catch (error) {
    process.stderr.write(`iii-engine build failed: ${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}
