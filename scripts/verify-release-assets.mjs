import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readFile, readdir } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const publicAssetNames = (version) => [
  `agentmemory-agentmemory-${version}.tgz`,
  "qwen3-reranker-0.6b-q8_0.gguf",
  "agentmemory-qwen-cpu-runtime-b11371-win-x64.zip",
  "qwen-gpu-upstream-assets.json",
  "qwen-model-notice.md",
  "Apache-2.0.txt",
  "README.txt",
];

const fail = (message) => {
  throw new Error(message);
};

const ensureSafeName = (name) => {
  if (typeof name !== "string" || name !== basename(name) || /[\\/\0]/.test(name)) {
    fail("Invalid asset filename.");
  }
};

const sha256File = async (path) => {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
};

const readRegularFile = async (path) => {
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink()) fail(`Expected a regular file: ${path}`);
  return info;
};

const parseChecksums = (contents) => {
  const lines = contents.toString("utf8").split(/\r?\n/);
  if (lines.at(-1) === "") lines.pop();
  const entries = new Map();
  for (const line of lines) {
    const match = /^([a-f0-9]{64})  ([^\\/\0]+)$/.exec(line);
    if (!match) fail("SHA256SUMS contains an invalid line.");
    const [, digest, name] = match;
    ensureSafeName(name);
    if (entries.has(name)) fail(`SHA256SUMS contains a duplicate entry: ${name}`);
    entries.set(name, digest);
  }
  return entries;
};

export async function verifyReleaseAssets({
  assetsDirectory,
  manifestPath,
  checksumsPath,
  release,
  tag,
  packageVersion,
  resolvedTagCommit,
  expectedCommit,
}) {
  const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
  if (typeof packageVersion !== "string" || !semver.test(packageVersion) || tag !== `v${packageVersion}`) {
    fail("Release tag does not match a valid package version.");
  }
  if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/i.test(expectedCommit ?? "") ||
      !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/i.test(resolvedTagCommit ?? "") ||
      resolvedTagCommit.toLowerCase() !== expectedCommit.toLowerCase()) {
    fail("The pushed tag does not resolve to the workflow commit.");
  }
  if (!release || release.tag_name !== tag || release.draft !== true) {
    fail("The exact-tag GitHub release must exist as a draft.");
  }

  const manifestBytes = await readFile(manifestPath);
  const manifest = JSON.parse(manifestBytes.toString("utf8"));
  if (manifest.schemaVersion !== 1 || manifest.publishable !== true ||
      manifest.packageVersion !== packageVersion) {
    fail("Frozen package-candidate metadata does not match the release version.");
  }
  if (manifest.cpuRuntime?.includesCudaDlls !== false ||
      manifest.cuda?.publishedBinaryCount !== 0) {
    fail("The public candidate must contain CPU runtime metadata and no published CUDA binaries.");
  }

  const allowedPublicNames = publicAssetNames(packageVersion);
  if (!Array.isArray(manifest.publicAssets) || manifest.publicAssets.length !== allowedPublicNames.length) {
    fail("The package candidate must declare exactly seven approved public assets.");
  }
  const declaredAssets = new Map();
  for (const asset of manifest.publicAssets) {
    ensureSafeName(asset?.file);
    if (!allowedPublicNames.includes(asset.file) || declaredAssets.has(asset.file) ||
        !Number.isSafeInteger(asset.sizeBytes) || asset.sizeBytes < 0 ||
        !/^[a-f0-9]{64}$/.test(asset.sha256 ?? "")) {
      fail("Invalid or unapproved public asset metadata.");
    }
    declaredAssets.set(asset.file, asset);
  }
  if (allowedPublicNames.some((name) => !declaredAssets.has(name))) {
    fail("The package candidate is missing an approved public asset.");
  }

  const expectedNames = [...allowedPublicNames, "package-candidate.json", "SHA256SUMS"];
  const directory = resolve(assetsDirectory);
  const actualEntries = await readdir(directory, { withFileTypes: true });
  const actualNames = actualEntries.map((entry) => entry.name).sort();
  if (JSON.stringify(actualNames) !== JSON.stringify([...expectedNames].sort())) {
    fail("The release draft has missing or extra assets.");
  }

  const localSizes = new Map();
  for (const name of expectedNames) {
    const filePath = join(directory, name);
    const info = await readRegularFile(filePath);
    localSizes.set(name, info.size);
  }
  if (!(await readFile(join(directory, "package-candidate.json"))).equals(manifestBytes)) {
    fail("The release package-candidate.json differs from the frozen source manifest.");
  }
  const expectedChecksums = await readFile(checksumsPath);
  const downloadedChecksums = await readFile(join(directory, "SHA256SUMS"));
  if (!downloadedChecksums.equals(expectedChecksums)) {
    fail("The release SHA256SUMS differs from the frozen source checksum file.");
  }

  const checksums = parseChecksums(downloadedChecksums);
  const expectedChecksumNames = [...allowedPublicNames, "package-candidate.json"];
  if (checksums.size !== expectedChecksumNames.length ||
      expectedChecksumNames.some((name) => !checksums.has(name))) {
    fail("SHA256SUMS must cover exactly the seven public assets and package-candidate.json.");
  }
  const releaseAssets = new Map();
  if (!Array.isArray(release.assets) || release.assets.length !== expectedNames.length) {
    fail("The GitHub release must contain exactly nine assets.");
  }
  for (const asset of release.assets) {
    ensureSafeName(asset?.name);
    if (releaseAssets.has(asset.name) || !Number.isSafeInteger(asset.size)) {
      fail("Invalid or duplicate GitHub release asset metadata.");
    }
    releaseAssets.set(asset.name, asset.size);
  }
  if (expectedNames.some((name) => !releaseAssets.has(name))) {
    fail("The GitHub release is missing an approved asset.");
  }

  for (const name of expectedNames) {
    if (releaseAssets.get(name) !== localSizes.get(name)) {
      fail(`GitHub asset size does not match the downloaded asset: ${name}`);
    }
  }
  for (const name of allowedPublicNames) {
    const expected = declaredAssets.get(name);
    const actualSize = localSizes.get(name);
    if (actualSize !== expected.sizeBytes) fail(`Asset size mismatch: ${name}`);
    if ((await sha256File(join(directory, name))) !== expected.sha256) fail(`Asset SHA-256 mismatch: ${name}`);
    if (checksums.get(name) !== expected.sha256) fail(`SHA256SUMS mismatch: ${name}`);
  }
  const manifestHash = createHash("sha256").update(manifestBytes).digest("hex");
  if (checksums.get("package-candidate.json") !== manifestHash) {
    fail("SHA256SUMS does not match package-candidate.json.");
  }

  return { tag, packageVersion, commit: resolvedTagCommit.toLowerCase(), verifiedAssets: expectedNames.length };
}

const parseArguments = (args) => {
  const values = new Map();
  for (let index = 0; index < args.length; index += 2) {
    const key = args[index];
    if (!key?.startsWith("--") || !args[index + 1] || values.has(key)) fail("Invalid verifier arguments.");
    values.set(key, args[index + 1]);
  }
  const required = ["--assets-dir", "--manifest", "--checksums", "--release", "--tag", "--version", "--tag-commit", "--expected-commit"];
  if (required.some((key) => !values.has(key)) || values.size !== required.length) fail("Missing or unsupported verifier arguments.");
  return Object.fromEntries([...values].map(([key, value]) => [key.slice(2).replaceAll("-", ""), value]));
};

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = parseArguments(process.argv.slice(2));
    const result = await verifyReleaseAssets({
      assetsDirectory: args.assetsdir,
      manifestPath: args.manifest,
      checksumsPath: args.checksums,
      release: JSON.parse(await readFile(args.release, "utf8")),
      tag: args.tag,
      packageVersion: args.version,
      resolvedTagCommit: args.tagcommit,
      expectedCommit: args.expectedcommit,
    });
    console.log(`Verified ${result.verifiedAssets} frozen assets for ${result.tag} at ${result.commit}.`);
  } catch (error) {
    console.error(`Frozen release asset verification failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
