import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const ASSETS = [
  "iii-config.yaml",
  "iii-config.docker.yaml",
  "docker-compose.yml",
  ".env.example",
  "src/viewer/index.html",
  "src/viewer/favicon.svg",
];

export function copyPackageAssets(rootDir) {
  for (const sourceRelativePath of ASSETS) {
    const source = join(rootDir, sourceRelativePath);
    const targetRelativePath = sourceRelativePath.replace(/^src\//, "");
    const target = join(rootDir, "dist", targetRelativePath);
    mkdirSync(dirname(target), { recursive: true });
    copyFileSync(source, target);
  }
}

const invokedPath = process.argv[1];
if (invokedPath && resolve(invokedPath) === fileURLToPath(import.meta.url)) {
  copyPackageAssets(fileURLToPath(new URL("..", import.meta.url)));
}
