import {
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  realpathSync,
  rmSync,
} from "node:fs";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import {
  validatePinnedEngineSource,
  validateStagedEngineArtifacts,
} from "./build-iii-engine.mjs";

const ASSETS = [
  "iii-config.yaml",
  "iii-config.docker.yaml",
  "docker-compose.yml",
  ".env.example",
  "src/viewer/index.html",
  "src/viewer/favicon.svg",
];
const DIRECT_ASSETS = [["scripts/qwen-reranker.py", "dist/qwen-reranker.py"]];

function isWithinPath(root, candidate) {
  const pathFromRoot = relative(root, candidate);
  return pathFromRoot === "" ||
    (pathFromRoot !== ".." && !pathFromRoot.startsWith(`..${sep}`) && !isAbsolute(pathFromRoot));
}

function assertPackagePath(rootDir, path, label) {
  const root = realpathSync(rootDir);
  const candidate = resolve(path);
  if (!isWithinPath(root, candidate)) {
    throw new Error(`${label} must stay inside the agentmemory workspace.`);
  }

  let currentPath = root;
  for (const part of relative(root, candidate).split(sep).filter(Boolean)) {
    currentPath = join(currentPath, part);
    if (!existsSync(currentPath)) break;
    if (lstatSync(currentPath).isSymbolicLink()) {
      throw new Error(`${label} must not traverse symbolic links.`);
    }
    if (!isWithinPath(root, realpathSync(currentPath))) {
      throw new Error(`${label} resolves outside the agentmemory workspace.`);
    }
  }
  return candidate;
}

export function copyPackageAssets(rootDir, { requireEngineArtifact = false } = {}) {
  const buildDir = process.env["AGENTMEMORY_III_BUILD_DIR"] ?? ".iii-engine-build";
  const buildRoot = assertPackagePath(
    rootDir,
    resolve(rootDir, buildDir),
    "AGENTMEMORY_III_BUILD_DIR",
  );
  if (buildRoot === realpathSync(rootDir)) {
    throw new Error("AGENTMEMORY_III_BUILD_DIR must stay inside the agentmemory workspace.");
  }
  const stagedEngine = join(buildRoot, "artifacts");
  const manifestPath = join(stagedEngine, "manifest.json");
  let manifest = null;
  if (!existsSync(manifestPath)) {
    if (requireEngineArtifact) {
      throw new Error(
        "Patched iii-engine artifact is missing. Run `npm run build:engine` before packaging.",
      );
    }
  } else {
    const pinnedSource = validatePinnedEngineSource();
    manifest = validateStagedEngineArtifacts(stagedEngine, pinnedSource.patchSha256);
  }

  for (const sourceRelativePath of ASSETS) {
    const source = join(rootDir, sourceRelativePath);
    const targetRelativePath = sourceRelativePath.replace(/^src\//, "");
    const target = assertPackagePath(
      rootDir,
      join(rootDir, "dist", targetRelativePath),
      "Package asset destination",
    );
    mkdirSync(dirname(target), { recursive: true });
    copyFileSync(source, target);
  }
  for (const [sourceRelativePath, targetRelativePath] of DIRECT_ASSETS) {
    const source = assertPackagePath(
      rootDir,
      join(rootDir, sourceRelativePath),
      "Package asset source",
    );
    const target = assertPackagePath(
      rootDir,
      join(rootDir, targetRelativePath),
      "Package asset destination",
    );
    mkdirSync(dirname(target), { recursive: true });
    copyFileSync(source, target);
  }
  if (!manifest) return;

  const packageEngine = assertPackagePath(
    rootDir,
    join(rootDir, "dist", "engine"),
    "Packaged engine destination",
  );
  rmSync(packageEngine, { recursive: true, force: true });
  mkdirSync(packageEngine, { recursive: true });
  copyFileSync(manifestPath, join(packageEngine, "manifest.json"));
  for (const artifact of Object.values(manifest.artifacts)) {
    const source = assertPackagePath(
      rootDir,
      resolve(stagedEngine, ...artifact.path.split("/")),
      "Staged engine artifact",
    );
    const target = assertPackagePath(
      rootDir,
      resolve(packageEngine, ...artifact.path.split("/")),
      "Packaged engine artifact destination",
    );
    mkdirSync(dirname(target), { recursive: true });
    copyFileSync(source, target);
  }
}

const invokedPath = process.argv[1];
if (invokedPath && resolve(invokedPath) === fileURLToPath(import.meta.url)) {
  const requireEngineArtifact = process.argv.includes("--require-engine");
  copyPackageAssets(fileURLToPath(new URL("..", import.meta.url)), { requireEngineArtifact });
}
