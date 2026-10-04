import { createHash } from "node:crypto";
import { afterEach, describe, expect, it } from "vitest";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { verifyReleaseAssets } from "../scripts/verify-release-assets.mjs";

const roots: string[] = [];
const version = "0.9.81";
const tag = `v${version}`;
const commit = "0123456789abcdef0123456789abcdef01234567";
const publicNames = [
  `agentmemory-agentmemory-${version}.tgz`,
  "qwen3-reranker-0.6b-q8_0.gguf",
  "agentmemory-qwen-cpu-runtime-b11371-win-x64.zip",
  "qwen-gpu-upstream-assets.json",
  "qwen-model-notice.md",
  "Apache-2.0.txt",
  "README.txt",
];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

const digest = (contents: Buffer) => createHash("sha256").update(contents).digest("hex");

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "agentmemory-release-assets-"));
  roots.push(root);
  const assetsDirectory = join(root, "downloaded");
  mkdirSync(assetsDirectory);
  const content = new Map(publicNames.map((name) => [name, Buffer.from(`frozen:${name}`)]));
  const manifest = {
    schemaVersion: 1,
    packageVersion: version,
    publishable: true,
    cpuRuntime: { includesCudaDlls: false },
    cuda: { publishedBinaryCount: 0 },
    publicAssets: publicNames.map((file) => ({
      file,
      sizeBytes: content.get(file)!.length,
      sha256: digest(content.get(file)!),
    })),
  };
  const manifestBytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
  const manifestPath = join(root, "package-candidate.json");
  writeFileSync(manifestPath, manifestBytes);
  const checksumsBytes = Buffer.from([
    ...publicNames.map((name) => `${digest(content.get(name)!)}  ${name}`),
    `${digest(manifestBytes)}  package-candidate.json`,
    "",
  ].join("\n"));
  const checksumsPath = join(root, "SHA256SUMS");
  writeFileSync(checksumsPath, checksumsBytes);
  for (const [name, bytes] of content) writeFileSync(join(assetsDirectory, name), bytes);
  writeFileSync(join(assetsDirectory, "package-candidate.json"), manifestBytes);
  writeFileSync(join(assetsDirectory, "SHA256SUMS"), checksumsBytes);
  const release = {
    tag_name: tag,
    draft: true,
    assets: [
      ...publicNames.map((name) => ({ name, size: content.get(name)!.length })),
      { name: "package-candidate.json", size: manifestBytes.length },
      { name: "SHA256SUMS", size: checksumsBytes.length },
    ],
  };
  return {
    assetsDirectory,
    manifestPath,
    checksumsPath,
    release,
    tag,
    packageVersion: version,
    resolvedTagCommit: commit,
    expectedCommit: commit,
    content,
  };
}

describe("frozen release asset verification", () => {
  it("accepts the matching nine-asset draft for the exact tag commit", async () => {
    const input = fixture();
    await expect(verifyReleaseAssets(input)).resolves.toEqual({
      tag,
      packageVersion: version,
      commit,
      verifiedAssets: 9,
    });
  });

  it("rejects an altered asset even when its byte length is unchanged", async () => {
    const input = fixture();
    const changed = Buffer.from(input.content.get("README.txt")!);
    changed[0] ^= 1;
    writeFileSync(join(input.assetsDirectory, "README.txt"), changed);
    await expect(verifyReleaseAssets(input)).rejects.toThrow("Asset SHA-256 mismatch: README.txt");
  });

  it("rejects a release with the wrong tag identity or source commit", async () => {
    const wrongTag = fixture();
    wrongTag.release = { ...wrongTag.release, tag_name: "v0.9.80" };
    await expect(verifyReleaseAssets(wrongTag)).rejects.toThrow("exact-tag GitHub release");

    const wrongCommit = fixture();
    wrongCommit.expectedCommit = "fedcba9876543210fedcba9876543210fedcba98";
    await expect(verifyReleaseAssets(wrongCommit)).rejects.toThrow("does not resolve to the workflow commit");
  });

  it.each([
    "agentmemory-qwen-runtime-private-cuda.zip",
    "agentmemory.sqlite3",
    "qwen-model.cache",
  ])("rejects unapproved private, database, or cache asset %s", async (name) => {
    const input = fixture();
    writeFileSync(join(input.assetsDirectory, name), "unapproved");
    input.release.assets.push({ name, size: Buffer.byteLength("unapproved") });
    await expect(verifyReleaseAssets(input)).rejects.toThrow("missing or extra assets");
  });

  it("rejects a manifest that adds a private CUDA archive to the public list", async () => {
    const input = fixture();
    const current = JSON.parse(readFileSync(input.manifestPath, "utf8"));
    current.publicAssets.push({ file: "agentmemory-qwen-runtime-private-cuda.zip", sizeBytes: 1, sha256: "a".repeat(64) });
    writeFileSync(input.manifestPath, JSON.stringify(current));
    writeFileSync(join(input.assetsDirectory, "package-candidate.json"), JSON.stringify(current));
    await expect(verifyReleaseAssets(input)).rejects.toThrow("exactly seven approved public assets");
  });

  it("rejects a missing release asset and a non-draft release", async () => {
    const missing = fixture();
    rmSync(join(missing.assetsDirectory, "qwen3-reranker-0.6b-q8_0.gguf"));
    await expect(verifyReleaseAssets(missing)).rejects.toThrow("missing or extra assets");

    const published = fixture();
    published.release = { ...published.release, draft: false };
    await expect(verifyReleaseAssets(published)).rejects.toThrow("exact-tag GitHub release must exist as a draft");
  });
});
