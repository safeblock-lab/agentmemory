import { createHash } from "node:crypto";
import { gunzipSync } from "node:zlib";
import { VERSION } from "../version.js";

const RELEASE_API = "https://api.github.com/repos/safeblock-lab/agentmemory/releases/latest";
const RELEASE_BASE = "https://github.com/safeblock-lab/agentmemory/releases/download/";
const ALLOWED_DOWNLOAD_HOSTS = new Set([
  "github.com",
  "release-assets.githubusercontent.com",
  "objects.githubusercontent.com",
]);
const MAX_METADATA_BYTES = 256 * 1024;
const MAX_CHECKSUM_BYTES = 1024 * 1024;
const MAX_PACKAGE_BYTES = 100 * 1024 * 1024;
const MAX_UNPACKED_BYTES = 256 * 1024 * 1024;

export type ReleaseInfo = {
  currentVersion: string;
  version: string;
  tag: string;
  available: boolean;
};

type GithubRelease = {
  tag_name?: unknown;
  draft?: unknown;
  prerelease?: unknown;
  assets?: Array<{ name?: unknown; browser_download_url?: unknown }>;
};

export type VerifiedRelease = ReleaseInfo & {
  filename: string;
  tarball: Buffer;
  sha256: string;
};

function tarField(block: Buffer, start: number, length: number): string {
  const field = block.subarray(start, start + length);
  const end = field.indexOf(0);
  return field.subarray(0, end < 0 ? field.length : end).toString("utf8");
}

export function verifyPackageArchive(tarball: Buffer, version: string): void {
  let archive: Buffer;
  try {
    archive = gunzipSync(tarball, { maxOutputLength: MAX_UNPACKED_BYTES });
  } catch {
    throw new Error("Release package is not a bounded gzip archive.");
  }
  let metadata: { name?: unknown; version?: unknown } | undefined;
  let hasCli = false;
  let hasEntry = false;
  const paths = new Set<string>();
  for (let offset = 0; offset + 512 <= archive.length;) {
    const header = archive.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) break;
    hasEntry = true;
    const name = tarField(header, 0, 100);
    const prefix = tarField(header, 345, 155);
    const path = prefix ? `${prefix}/${name}` : name;
    const sizeField = tarField(header, 124, 12).trim();
    const checksumField = tarField(header, 148, 8).trim();
    if (!/^[0-7]+$/.test(sizeField) || !/^[0-7]+$/.test(checksumField)) {
      throw new Error("Release archive has an invalid entry header.");
    }
    const checksum = header.reduce((sum, byte, index) => sum + (index >= 148 && index < 156 ? 32 : byte), 0);
    if (checksum !== Number.parseInt(checksumField, 8)) throw new Error("Release archive has an invalid entry checksum.");
    const size = Number.parseInt(sizeField, 8);
    const type = header[156];
    if (!Number.isSafeInteger(size) || size < 0 || path.startsWith("/") || path.includes("\\") ||
        !path.startsWith("package/") || path.split("/").some((part) => part === ".." || part === "." || part === "") ||
        (type !== 0 && type !== 48 && type !== 53) || paths.has(path) || paths.size >= 20_000) {
      throw new Error("Release archive contains an unsafe entry.");
    }
    paths.add(path);
    const contentStart = offset + 512;
    const next = contentStart + Math.ceil(size / 512) * 512;
    if (next > archive.length) throw new Error("Release archive is truncated.");
    if (path === "package/package.json") {
      if (metadata || size > 64 * 1024 || (type !== 0 && type !== 48)) {
        throw new Error("Release package manifest is invalid.");
      }
      try {
        metadata = JSON.parse(archive.subarray(contentStart, contentStart + size).toString("utf8")) as typeof metadata;
      } catch {
        throw new Error("Release package manifest is invalid.");
      }
    }
    if (path === "package/dist/cli.mjs") {
      if (hasCli || size === 0 || (type !== 0 && type !== 48)) {
        throw new Error("Release package CLI is invalid.");
      }
      hasCli = true;
    }
    offset = next;
  }
  if (!hasEntry || metadata?.name !== "@agentmemory/agentmemory" || metadata.version !== version || !hasCli) {
    throw new Error("Release package identity or version does not match the GitHub release.");
  }
}

function numericVersion(tag: string): [number, number, number] | null {
  const match = /^v?(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.exec(tag);
  return match ? [Number(match[1]), Number(match[2]), Number(match[3])] : null;
}

export function isNewerRelease(tag: string, current = VERSION): boolean {
  const next = numericVersion(tag);
  const installed = numericVersion(current);
  if (!next || !installed || next.some((n) => !Number.isSafeInteger(n))) {
    throw new Error("Unsupported release version.");
  }
  return next.some((n, i) => n !== installed[i] && next.slice(0, i).every((part, j) => part === installed[j]) && n > installed[i]);
}

async function boundedFetch(url: string, maxBytes: number, allowedHosts: Set<string>, accept: string): Promise<Buffer> {
  let current = url;
  for (let redirect = 0; redirect <= 5; redirect++) {
    const uri = new URL(current);
    if (uri.protocol !== "https:" || !allowedHosts.has(uri.hostname) || uri.username || uri.password) {
      throw new Error("Release download redirected to an untrusted host.");
    }
    const response = await fetch(uri, {
      redirect: "manual",
      headers: { "User-Agent": "agentmemory-updater", Accept: accept },
      signal: AbortSignal.timeout(60_000),
    });
    if (response.status >= 300 && response.status < 400) {
      const location = response.headers.get("location");
      if (!location) throw new Error("Release download redirect has no location.");
      current = new URL(location, uri).href;
      await response.body?.cancel();
      continue;
    }
    if (!response.ok || !response.body) throw new Error(`Release download failed (${response.status}).`);
    const declared = Number(response.headers.get("content-length"));
    if (declared > maxBytes) throw new Error("Release download exceeds the size limit.");
    const chunks: Buffer[] = [];
    let size = 0;
    for await (const chunk of response.body) {
      const bytes = Buffer.from(chunk);
      size += bytes.length;
      if (size > maxBytes) {
        await response.body.cancel().catch(() => {});
        throw new Error("Release download exceeds the size limit.");
      }
      chunks.push(bytes);
    }
    return Buffer.concat(chunks, size);
  }
  throw new Error("Release download redirected too many times.");
}

async function latestRelease(): Promise<{ info: ReleaseInfo; assetUrl: string; sumsUrl: string }> {
  const raw = await boundedFetch(RELEASE_API, MAX_METADATA_BYTES, new Set(["api.github.com"]), "application/vnd.github+json");
  let release: GithubRelease;
  try {
    release = JSON.parse(raw.toString("utf8")) as GithubRelease;
  } catch {
    throw new Error("GitHub returned invalid release metadata.");
  }
  const tag = release.tag_name;
  if (typeof tag !== "string" || !/^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(tag) || release.draft || release.prerelease) {
    throw new Error("Latest GitHub release is not a supported stable version.");
  }
  const available = isNewerRelease(tag);
  const filename = `agentmemory-${tag}.tgz`;
  const expected = `${RELEASE_BASE}${tag}/`;
  const assets = Array.isArray(release.assets) ? release.assets : [];
  function asset(name: string): string {
    const matching = assets.filter((entry) => entry.name === name && entry.browser_download_url === `${expected}${name}`);
    if (matching.length !== 1) throw new Error(`GitHub release is missing verified asset ${name}.`);
    return `${expected}${name}`;
  }
  return {
    info: { currentVersion: VERSION, version: tag.slice(1), tag, available },
    assetUrl: asset(filename),
    sumsUrl: asset("SHA256SUMS.txt"),
  };
}

export async function checkForUpdate(): Promise<ReleaseInfo> {
  return (await latestRelease()).info;
}

export async function downloadVerifiedRelease(): Promise<VerifiedRelease> {
  const { info, assetUrl, sumsUrl } = await latestRelease();
  if (!info.available) throw new Error("AgentMemory is already up to date.");
  const filename = `agentmemory-${info.tag}.tgz`;
  const [tarball, sums] = await Promise.all([
    boundedFetch(assetUrl, MAX_PACKAGE_BYTES, ALLOWED_DOWNLOAD_HOSTS, "application/octet-stream"),
    boundedFetch(sumsUrl, MAX_CHECKSUM_BYTES, ALLOWED_DOWNLOAD_HOSTS, "application/octet-stream"),
  ]);
  const line = sums.toString("utf8").split(/\r?\n/).find((entry) => entry.endsWith(`  ${filename}`) || entry.endsWith(` *${filename}`));
  const match = line && /^([0-9a-fA-F]{64})\s+\*?(.+)$/.exec(line);
  if (!match || match[2] !== filename) throw new Error("Release checksums do not contain the expected package.");
  const sha256 = createHash("sha256").update(tarball).digest("hex");
  if (sha256 !== match[1].toLowerCase()) throw new Error("Release package checksum verification failed.");
  verifyPackageArchive(tarball, info.version);
  return { ...info, filename, tarball, sha256 };
}
