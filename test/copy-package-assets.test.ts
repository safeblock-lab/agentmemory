import { afterEach, describe, expect, it } from "vitest";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { copyPackageAssets } from "../scripts/copy-package-assets.mjs";

const temporaryRoots: string[] = [];

afterEach(() => {
  for (const root of temporaryRoots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

describe("copyPackageAssets", () => {
  it("copies viewer and distribution assets into dist", () => {
    const root = mkdtempSync(join(tmpdir(), "agentmemory-assets-"));
    temporaryRoots.push(root);
    const assets = [
      "iii-config.yaml",
      "iii-config.docker.yaml",
      "docker-compose.yml",
      ".env.example",
      "src/viewer/index.html",
      "src/viewer/favicon.svg",
    ];
    for (const path of assets) {
      const target = join(root, path);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, path);
    }

    copyPackageAssets(root);

    expect(readFileSync(join(root, "dist", "viewer", "index.html"), "utf8")).toBe("src/viewer/index.html");
    expect(existsSync(join(root, "dist", "viewer", "favicon.svg"))).toBe(true);
    expect(existsSync(join(root, "dist", "iii-config.yaml"))).toBe(true);
  });
});
