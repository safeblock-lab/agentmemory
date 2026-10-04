import { createHash } from "node:crypto";
import {
  copyFileSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const ROOT = realpathSync(fileURLToPath(new URL("..", import.meta.url)));
const VERSION = JSON.parse(readFileSync(join(ROOT, "package.json"), "utf8")).version;
const PINS = {
  model: { path: ".native-pagination-build/qwen-cpu-evaluation/qwen3-reranker-0.6b-q8_0.gguf", bytes: 639153184, sha256: "22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48" },
  scorer: { path: "scripts/qwen-reranker.py", bytes: null, sha256: "a96d30e6c64b58a5fc8ef807287d6b3315d50d00c8018e6e289ce14fa7db2e76" },
  native: { path: ".native-pagination-build/native-graph-high-fanout-repair/iii.exe", bytes: 28849152, sha256: "bd3126c4549b04c0fb31ce8fd9b298e21dcf20c60b40ee8b0b4105ad7bd5a91d" },
  cpu: { path: ".native-pagination-build/qwen-cpu-evaluation/llama-b11371-bin-win-cpu-x64.zip", bytes: 19352488, sha256: "085d650f1b4b0725a32c2421b970da5c5d2f769202cd16f89591ac9dcce835c6" },
  cuda: { path: ".native-pagination-build/qwen-gpu-evaluation/llama-b11371-bin-win-cuda-12.4-x64.zip", bytes: 263486696, sha256: "72cd63b7daea25bcf9e638e144588c1b53351127c2ad9e1810ee9188d3d82abf" },
  cudart: { path: ".native-pagination-build/qwen-gpu-evaluation/cudart-llama-bin-win-cuda-12.4-x64.zip", bytes: 391443627, sha256: "8c79a9b226de4b3cacfd1f83d24f962d0773be79f1e7b75c6af4ded7e32ae1d6" },
  llamaLicense: { path: "docs/graph/licenses/llama.cpp-b11371-LICENSE.txt", bytes: 1078, sha256: "94f29bbed6a22c35b992c5c6ebf0e7c92f13b836b90f36f461c9cf2f0f1d010d" },
  apacheLicense: { path: "docs/graph/licenses/Apache-2.0.txt", bytes: 11358, sha256: "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30" },
};
const MODEL_NAME = "qwen3-reranker-0.6b-q8_0.gguf";
const CPU_ZIP_NAME = "agentmemory-qwen-cpu-runtime-b11371-win-x64.zip";
const PRIVATE_ZIP_NAME = "agentmemory-qwen-runtime-b11371-win-x64-private.zip";

function within(root, path) {
  const rel = relative(root, path);
  return rel === "" || (rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
}

function safePath(path) {
  const candidate = resolve(ROOT, path);
  if (!within(ROOT, candidate)) throw new Error(`Path escapes workspace: ${path}`);
  let cursor = ROOT;
  for (const part of relative(ROOT, candidate).split(sep).filter(Boolean)) {
    cursor = join(cursor, part);
    if (!existsSync(cursor)) break;
    if (lstatSync(cursor).isSymbolicLink() || !within(ROOT, realpathSync(cursor))) {
      throw new Error(`Path traverses a symbolic link: ${path}`);
    }
  }
  return candidate;
}

async function sha256(path) {
  const hash = createHash("sha256");
  const { createReadStream } = await import("node:fs");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

async function verifyPin(pin, label) {
  const path = safePath(pin.path);
  const stat = statSync(path);
  if ((pin.bytes !== null && stat.size !== pin.bytes) || await sha256(path) !== pin.sha256) {
    throw new Error(`${label} does not match its accepted size and SHA256 pin.`);
  }
  return { path, bytes: stat.size, sha256: pin.sha256 };
}

function psQuote(value) { return `'${value.replaceAll("'", "''")}'`; }

function runPowerShell(script) {
  const encoded = Buffer.from(script, "utf16le").toString("base64");
  const result = spawnSync("powershell.exe", ["-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-EncodedCommand", encoded], {
    cwd: ROOT, encoding: "utf8", windowsHide: true, maxBuffer: 8 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(`PowerShell asset operation failed (exit ${result.status}): ${result.stderr.trim()}`);
  return result.stdout.trim();
}

function extractZip(archive, destination) {
  mkdirSync(destination, { recursive: true });
  runPowerShell(`Expand-Archive -LiteralPath ${psQuote(archive)} -DestinationPath ${psQuote(destination)} -Force`);
}

function filesBelow(directory) {
  const root = realpathSync(directory);
  const files = [];
  function visit(current) {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const path = join(current, entry.name);
      const info = lstatSync(path);
      if (info.isSymbolicLink() || !within(root, realpathSync(path))) throw new Error(`Extracted archive contains a link: ${relative(root, path)}`);
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile()) files.push(path);
      else throw new Error(`Extracted archive has an unsupported entry: ${relative(root, path)}`);
    }
  }
  visit(root);
  return files;
}

async function copyDlls(source, destination) {
  mkdirSync(destination, { recursive: true });
  let count = 0;
  for (const file of filesBelow(source).filter((item) => item.toLowerCase().endsWith(".dll"))) {
    const target = join(destination, basename(file));
    if (existsSync(target)) {
      if (await sha256(target) !== await sha256(file)) throw new Error(`Conflicting runtime DLL basename: ${basename(file)}`);
      continue;
    }
    copyFileSync(file, target);
    count += 1;
  }
  if (!count && !readdirSync(destination).length) throw new Error(`Runtime archive contains no DLLs: ${source}`);
  return count;
}

function compressFolder(source, destination) {
  runPowerShell(`Compress-Archive -Path ${psQuote(join(source, "*"))} -DestinationPath ${psQuote(destination)} -CompressionLevel Optimal -Force`);
}

function zipEntries(path) {
  const output = runPowerShell(
    `Add-Type -AssemblyName System.IO.Compression.FileSystem; $z=[IO.Compression.ZipFile]::OpenRead(${psQuote(path)}); try { ConvertTo-Json -InputObject @($z.Entries | ForEach-Object { $_.FullName }) -Compress } finally { $z.Dispose() }`,
  );
  const parsed = JSON.parse(output);
  return Array.isArray(parsed) ? parsed : [parsed];
}

function zipFileEntries(path) {
  return zipEntries(path).map((name) => name.replaceAll("\\", "/")).filter((name) => name && !name.endsWith("/")).sort();
}

async function inventoryFiles(directory) {
  const files = await Promise.all(filesBelow(directory).map(async (path) => ({
    path: relative(directory, path).replaceAll(sep, "/"),
    sizeBytes: statSync(path).size,
    sha256: await sha256(path),
  })));
  return files.sort((left, right) => left.path.localeCompare(right.path));
}

async function writeRuntimeManifest(directory, runtimeKind) {
  const payloadFiles = await inventoryFiles(directory);
  writeText(join(directory, "runtime-manifest.json"), JSON.stringify({
    schemaVersion: 1,
    packageVersion: VERSION,
    runtimeKind,
    files: payloadFiles,
  }, null, 2));
  return { payloadFiles, archiveFiles: await inventoryFiles(directory) };
}

async function verifyRuntimeArchive(archive, extractedRoot, expectedFiles, payloadFiles, label) {
  const expectedPaths = expectedFiles.map((file) => file.path).sort();
  const archivePaths = zipFileEntries(archive);
  if (JSON.stringify(archivePaths) !== JSON.stringify(expectedPaths)) {
    throw new Error(`${label} ZIP entries differ from its per-file manifest.`);
  }
  const extractedFiles = await inventoryFiles(extractedRoot);
  if (JSON.stringify(extractedFiles) !== JSON.stringify(expectedFiles)) {
    throw new Error(`${label} extracted paths, sizes or SHA256 values differ from its per-file manifest.`);
  }
  const runtimeManifest = JSON.parse(readFileSync(join(extractedRoot, "runtime-manifest.json"), "utf8"));
  if (runtimeManifest.schemaVersion !== 1 || runtimeManifest.packageVersion !== VERSION
    || JSON.stringify(runtimeManifest.files) !== JSON.stringify(payloadFiles)) {
    throw new Error(`${label} embedded per-file manifest does not match the extracted payload.`);
  }
}

function runTar(args) {
  const result = spawnSync("tar.exe", args, { cwd: ROOT, encoding: "utf8", windowsHide: true, maxBuffer: 16 * 1024 * 1024 });
  if (result.status !== 0) throw new Error(`tar.exe failed (exit ${result.status}): ${result.stderr.trim()}`);
  return result.stdout.split(/\r?\n/).filter(Boolean);
}

function resolveNpmCli() {
  const candidates = [];
  if (process.env.npm_execpath && existsSync(process.env.npm_execpath)) {
    candidates.push(resolve(process.env.npm_execpath));
  }
  const located = spawnSync("where.exe", ["npm.cmd"], {
    cwd: ROOT, encoding: "utf8", windowsHide: true, shell: false,
  });
  if (located.status === 0) {
    for (const commandPath of located.stdout.split(/\r?\n/).filter(Boolean)) {
      candidates.push(join(dirname(commandPath), "node_modules", "npm", "bin", "npm-cli.js"));
    }
  }
  const npmCli = candidates.find((candidate) => existsSync(candidate));
  if (!npmCli) throw new Error("Could not resolve the installed npm CLI for package creation.");
  return npmCli;
}

function getPython() {
  const python = process.env.AGENTMEMORY_QWEN_PYTHON || "python";
  const result = spawnSync(python, ["-I", "-c", "import struct,sys; print(struct.calcsize('P') * 8)"], { encoding: "utf8", windowsHide: true });
  if (result.status !== 0 || result.stdout.trim() !== "64") throw new Error("A working 64-bit Python is required; set AGENTMEMORY_QWEN_PYTHON to its executable.");
  return python;
}

function writeText(path, text) { writeFileSync(path, `${text.trimEnd()}\n`, "utf8"); }

async function smokePackagedCpu(packageRoot, modelPath, cpuPath, python) {
  const entry = pathToFileURL(join(packageRoot, "dist", "qwen-reranker.mjs")).href;
  const script = `
import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { syncBuiltinESMExports } from "node:module";
let childPid;
let childExitCode;
let childSignal;
let childClosed = false;
const originalSpawn = childProcess.spawn;
childProcess.spawn = function(...args) {
  const child = originalSpawn.apply(this, args);
  if (String(args[0]) === ${JSON.stringify(python)} && childPid === undefined) {
    childPid = child.pid;
    child.once("close", (code, signal) => {
      childExitCode = code;
      childSignal = signal;
      childClosed = true;
    });
  }
  return child;
};
syncBuiltinESMExports();
const { createQwenReranker } = await import(${JSON.stringify(entry)});
const runtime = await createQwenReranker({
  python: ${JSON.stringify(python)},
  script: ${JSON.stringify(join(packageRoot, "dist", "qwen-reranker.py"))},
  model: ${JSON.stringify(modelPath)},
  cpuRuntime: ${JSON.stringify(cpuPath)},
  gpuRuntime: ${JSON.stringify(cpuPath)},
  device: "cpu",
});
let scores;
try {
  scores = await runtime.scoreBatch("What is the capital of France?", [
    "Paris is the capital and largest city of France.",
    "Berlin is the capital and largest city of Germany.",
  ]);
  assert.equal(runtime.device, "cpu");
  assert.equal(scores.length, 2);
  assert.ok(scores.every((score) => Number.isFinite(score) && score >= 0 && score <= 1));
  assert.ok(scores[0] > 0.5 && scores[1] < 0.5, JSON.stringify(scores));
} finally {
  await runtime.close();
}
assert.ok(Number.isInteger(childPid), "provider did not spawn the pinned Python child");
assert.equal(childClosed, true, "provider close did not observe the actual Python child close");
assert.equal(childExitCode, 0, "Python child did not exit cleanly");
assert.equal(childSignal, null, "Python child required forced termination");
console.log(JSON.stringify({ device: runtime.device, scores, childPid, childExitCode, childSignal, childClosed, cleanupResolved: true }));
`;
  const result = spawnSync(process.execPath, ["--input-type=module", "--eval", script], {
    cwd: ROOT, encoding: "utf8", windowsHide: true, timeout: 180_000, maxBuffer: 4 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error(`Packaged CPU provider control failed (exit ${result.status}): ${result.stderr.trim()}`);
  return JSON.parse(result.stdout.trim().split(/\r?\n/).at(-1));
}

export async function packageQwenAssets() {
  if (process.platform !== "win32") throw new Error("The pinned runtime candidate is Windows x64 only.");
  if (VERSION !== "0.9.81") throw new Error(`Expected staged release version 0.9.81, found ${VERSION}.`);
  const buildRoot = safePath(".native-pagination-build");
  if (!existsSync(buildRoot)) throw new Error("Accepted workspace asset directory is missing.");
  const temporary = mkdtempSync(join(buildRoot, "qwen-package-stage-"));
  const publicOutput = safePath(".native-pagination-build/native-high-fanout-release-refreeze/package/public");
  const privateOutput = safePath(".native-pagination-build/native-high-fanout-release-refreeze/package/private");
  if (existsSync(publicOutput) || existsSync(privateOutput)) throw new Error("Candidate output already exists; refusing to overwrite it.");
  const extracted = join(temporary, "extracted");
  const publicStage = join(temporary, "public");
  const privateStage = join(temporary, "private");
  mkdirSync(extracted, { recursive: true });
  mkdirSync(publicStage, { recursive: true });
  mkdirSync(privateStage, { recursive: true });
  try {
    const [model, scorer, native, cpu, cuda, cudart] = await Promise.all([
      verifyPin(PINS.model, "Qwen GGUF"), verifyPin(PINS.scorer, "Qwen scorer"),
      verifyPin(PINS.native, "accepted native engine"), verifyPin(PINS.cpu, "CPU runtime archive"),
      verifyPin(PINS.cuda, "CUDA llama runtime archive"), verifyPin(PINS.cudart, "CUDA runtime archive"),
    ]);
    const [llamaLicense, apacheLicense] = await Promise.all([
      verifyPin(PINS.llamaLicense, "llama.cpp MIT notice"), verifyPin(PINS.apacheLicense, "Qwen Apache-2.0 license"),
    ]);
    const python = getPython();
    const cpuExtract = join(extracted, "cpu-source");
    const cudaExtract = join(extracted, "cuda-source");
    const cudartExtract = join(extracted, "cudart-source");
    extractZip(cpu.path, cpuExtract);
    extractZip(cuda.path, cudaExtract);
    extractZip(cudart.path, cudartExtract);

    const publicRuntime = join(temporary, "public-runtime");
    const publicCpu = join(publicRuntime, "cpu");
    const publicNotices = join(publicRuntime, "notices");
    await copyDlls(cpuExtract, publicCpu);
    mkdirSync(publicNotices, { recursive: true });
    const openMp = join(cpuExtract, "LICENSE-LLVM-OpenMP");
    if (!existsSync(openMp)) throw new Error("Pinned CPU archive is missing its LLVM OpenMP notice.");
    copyFileSync(openMp, join(publicNotices, "LLVM-OpenMP-LICENSE.txt"));
    copyFileSync(llamaLicense.path, join(publicNotices, "llama.cpp-b11371-LICENSE.txt"));
    const publicManifest = await writeRuntimeManifest(publicRuntime, "cpu-only");
    const publicCpuZip = join(publicStage, CPU_ZIP_NAME);
    compressFolder(publicRuntime, publicCpuZip);
    const cpuNames = zipFileEntries(publicCpuZip);
    if (cpuNames.some((name) => /(^|\/)(cuda|private)(\/|$)|cublas|cudart|ggml-cuda/i.test(name))) {
      throw new Error("Public CPU runtime ZIP contains a CUDA or private runtime entry.");
    }
    if (!cpuNames.includes("cpu/ggml-cpu-x64.dll") || !cpuNames.includes("notices/LLVM-OpenMP-LICENSE.txt")
      || !cpuNames.includes("notices/llama.cpp-b11371-LICENSE.txt") || !cpuNames.includes("runtime-manifest.json")) {
      throw new Error("Public CPU ZIP is missing required runtime, notices or its per-file manifest.");
    }
    if (JSON.stringify(cpuNames) !== JSON.stringify(publicManifest.archiveFiles.map((file) => file.path).sort())) {
      throw new Error("Public CPU ZIP entries differ from its per-file manifest.");
    }

    const privateRuntime = join(temporary, "private-runtime");
    const privateCpu = join(privateRuntime, "cpu");
    const privateCuda = join(privateRuntime, "cuda");
    const privateNotices = join(privateRuntime, "notices");
    await copyDlls(cpuExtract, privateCpu);
    await copyDlls(cudaExtract, privateCuda);
    await copyDlls(cudartExtract, privateCuda);
    mkdirSync(privateNotices, { recursive: true });
    copyFileSync(openMp, join(privateNotices, "LLVM-OpenMP-LICENSE.txt"));
    copyFileSync(llamaLicense.path, join(privateNotices, "llama.cpp-b11371-LICENSE.txt"));
    writeText(join(privateRuntime, "NOT-PUBLISHABLE.txt"), `PRIVATE LOCAL ACTIVATION ARTIFACT — NOT FOR PUBLICATION OR REDISTRIBUTION.\nThis combined CPU/CUDA runtime is retained for the explicitly authorized local installation only. CUDA files came from the exact upstream pins in docs/graph/qwen-gpu-upstream-assets.json. Follow the NVIDIA CUDA 12.4 EULA: https://docs.nvidia.com/cuda/archive/12.4.1/pdf/EULA.pdf`);
    const privateManifest = await writeRuntimeManifest(privateRuntime, "private-cpu-cuda-local-only");
    const privateZipTemp = join(privateStage, PRIVATE_ZIP_NAME);
    compressFolder(privateRuntime, privateZipTemp);
    const privateNames = zipFileEntries(privateZipTemp);
    if (!privateNames.includes("cpu/ggml-cpu-x64.dll") || !privateNames.includes("cuda/ggml-cuda.dll")
      || !privateNames.includes("cuda/cudart64_12.dll") || !privateNames.includes("NOT-PUBLISHABLE.txt")
      || !privateNames.includes("runtime-manifest.json")) {
      throw new Error("Private combined ZIP is missing required CPU/CUDA files or its nonpublishable marker.");
    }
    if (JSON.stringify(privateNames) !== JSON.stringify(privateManifest.archiveFiles.map((file) => file.path).sort())) {
      throw new Error("Private combined ZIP entries differ from its per-file manifest.");
    }
    const privateZipHash = await sha256(privateZipTemp);
    const privateZipBytes = statSync(privateZipTemp).size;
    writeFileSync(join(privateStage, "private-manifest.json"), `${JSON.stringify({
      schemaVersion: 1, packageVersion: VERSION, publishable: false, githubAsset: false,
      purpose: "Private retained artifact for this user's local activation only.",
      file: PRIVATE_ZIP_NAME, sizeBytes: privateZipBytes, sha256: privateZipHash,
      cpuArchiveSha256: cpu.sha256, cudaArchiveSha256: cuda.sha256, cudartArchiveSha256: cudart.sha256,
      modelSha256: model.sha256, pythonBits: 64,
      manifestPath: "runtime-manifest.json", files: privateManifest.archiveFiles,
    }, null, 2)}\n`, "utf8");

    const pack = spawnSync(process.execPath, [resolveNpmCli(), "pack", "--json", `--pack-destination=${publicStage}`], {
      cwd: ROOT, encoding: "utf8", windowsHide: true, shell: false, maxBuffer: 12 * 1024 * 1024,
    });
    if (pack.status !== 0) throw new Error(`npm pack failed (exit ${pack.status}): ${pack.stderr.trim()}`);
    const packed = JSON.parse(pack.stdout.trim())[0];
    const tarball = join(publicStage, packed.filename);
    const tarEntries = runTar(["-tzf", tarball]);
    const normalizedEntries = tarEntries.map((entry) => entry.replaceAll("\\", "/"));
    const scorerEntry = "package/dist/qwen-reranker.py";
    if (!normalizedEntries.includes(scorerEntry)) throw new Error("npm tarball does not contain dist/qwen-reranker.py.");
    if (normalizedEntries.some((entry) => /\.gguf$|\.native-pagination-build|(^|\/)eval\//i.test(entry))) {
      throw new Error("npm tarball unexpectedly includes a model, evaluation evidence or private build path.");
    }
    if (await sha256(safePath("dist/qwen-reranker.py")) !== scorer.sha256) throw new Error("Built scorer differs from the accepted source bytes.");

    const extractedPackageRoot = join(extracted, "npm-package");
    mkdirSync(extractedPackageRoot, { recursive: true });
    runTar(["-xzf", tarball, "-C", extractedPackageRoot]);
    const extractedPackage = join(extractedPackageRoot, "package");
    if (await sha256(join(extractedPackage, "dist", "qwen-reranker.py")) !== scorer.sha256) {
      throw new Error("Extracted npm tarball scorer differs from the accepted source bytes.");
    }
    const extractedCpu = join(extracted, "public-cpu-runtime");
    extractZip(publicCpuZip, extractedCpu);
    await verifyRuntimeArchive(publicCpuZip, extractedCpu, publicManifest.archiveFiles, publicManifest.payloadFiles, "Public CPU runtime");
    const extractedPrivate = join(extracted, "private-runtime-check");
    extractZip(privateZipTemp, extractedPrivate);
    await verifyRuntimeArchive(privateZipTemp, extractedPrivate, privateManifest.archiveFiles, privateManifest.payloadFiles, "Private combined runtime");
    const modelOut = join(publicStage, MODEL_NAME);
    copyFileSync(model.path, modelOut);
    if (await sha256(modelOut) !== model.sha256) throw new Error("Copied GGUF differs from the accepted model bytes.");
    const smoke = await smokePackagedCpu(extractedPackage, modelOut, join(extractedCpu, "cpu"), python);

    copyFileSync(apacheLicense.path, join(publicStage, "Apache-2.0.txt"));
    copyFileSync(safePath("docs/graph/qwen-model-notice.md"), join(publicStage, "qwen-model-notice.md"));
    copyFileSync(safePath("docs/graph/qwen-gpu-upstream-assets.json"), join(publicStage, "qwen-gpu-upstream-assets.json"));
    writeText(join(publicStage, "README.txt"), `AgentMemory ${VERSION} Qwen release candidate assets.\n\nThe public files include the package tarball, separate Qwen GGUF, CPU-only runtime ZIP with notices, and upstream GPU acquisition manifest. No CUDA DLLs are included. Use one absolute AGENTMEMORY_QWEN_ASSET_ROOT for the model and extracted cpu/ and optional cuda/ folders. An installed 64-bit Python is required. See qwen-model-notice.md and qwen-gpu-upstream-assets.json.`);

    const trackedPublic = [
      packed.filename, MODEL_NAME, CPU_ZIP_NAME, "qwen-gpu-upstream-assets.json",
      "qwen-model-notice.md", "Apache-2.0.txt", "README.txt",
    ];
    const records = [];
    for (const file of trackedPublic) records.push({ file, sizeBytes: statSync(join(publicStage, file)).size, sha256: await sha256(join(publicStage, file)) });
    const receipt = {
      schemaVersion: 1, packageVersion: VERSION, publishable: true,
      distributionDecision: "Public assets exclude NVIDIA CUDA DLLs; the combined CPU/CUDA ZIP is private and separately retained outside this directory. This interprets check 4's required combined ZIP as local-only while check 9 explicitly forbids publication; Main authorized this addendum.",
      cpuRuntime: { archive: CPU_ZIP_NAME, sha256: records.find((item) => item.file === CPU_ZIP_NAME).sha256, zipEntryCount: cpuNames.length, includesCudaDlls: false, files: publicManifest.archiveFiles },
      cuda: { upstreamManifest: "qwen-gpu-upstream-assets.json", publishedBinaryCount: 0 },
      model: { file: MODEL_NAME, bytes: model.bytes, sha256: model.sha256 },
      package: { file: packed.filename, bytes: packed.size, sha256: await sha256(tarball), scorerSha256: scorer.sha256, scorerTarEntry: scorerEntry, extractedScorerExact: true },
      native: { bytes: native.bytes, sha256: native.sha256 },
      pythonBits: 64,
      packagedCpuSmoke: smoke,
      publicAssets: records,
      excludedFromPublicWhitelist: ["artifacts/private/qwen-runtime", "private-manifest.json", "private CUDA DLLs", "private corpus", "credentials", "evaluation logs"],
      checks: { pinnedInputs: true, cpuNotices: true, noCudaInPublicZip: true, cpuManifestMatchesExtraction: true, tarballExtracts: true, scorerExact: true, modelSeparate: true, forcedCpuControls: true, privateManifestMatchesExtraction: true, childPidCleaned: smoke.childClosed },
    };
    writeFileSync(join(publicStage, "package-candidate.json"), `${JSON.stringify(receipt, null, 2)}\n`, "utf8");
    const checksumFiles = [...trackedPublic, "package-candidate.json"];
    const sums = [];
    for (const file of checksumFiles) sums.push(`${await sha256(join(publicStage, file))}  ${file}`);
    writeText(join(publicStage, "SHA256SUMS"), sums.join("\n"));

    mkdirSync(dirname(publicOutput), { recursive: true });
    mkdirSync(dirname(privateOutput), { recursive: true });
    renameSync(publicStage, publicOutput);
    renameSync(privateStage, privateOutput);
    return {
      version: VERSION, publicDirectory: relative(ROOT, publicOutput), privateDirectory: relative(ROOT, privateOutput),
      publicFiles: checksumFiles, privateFile: PRIVATE_ZIP_NAME, privateSha256: privateZipHash,
      privateFiles: privateManifest.archiveFiles,
      modelSha256: model.sha256, packageSha256: receipt.package.sha256, cpuZipSha256: receipt.cpuRuntime.sha256,
      privateZipBytes, smoke,
    };
  } finally {
    if (existsSync(temporary) && within(buildRoot, temporary)) rmSync(temporary, { recursive: true, force: true });
  }
}

const invokedPath = process.argv[1];
if (invokedPath && resolve(invokedPath) === fileURLToPath(import.meta.url)) {
  packageQwenAssets().then((result) => {
    console.log(JSON.stringify({ check: "package-candidate", exitCode: 0, ...result }));
  }).catch((error) => {
    console.error(JSON.stringify({ check: "package-candidate", exitCode: 1, error: error.message }));
    process.exitCode = 1;
  });
}
