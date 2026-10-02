import { lstatSync, readFileSync, rmSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";

const SEEDED_BUILTIN_WORKERS = [
  "http",
  "iii-http",
  "state",
  "iii-state",
  "queue",
  "iii-queue",
  "pubsub",
  "iii-pubsub",
  "cron",
  "iii-cron",
  "iii-stream",
  "iii-observability",
  "iii-worker-manager",
];

export function configuredPersistDir(renderedConfig: string): string | null {
  const lines = renderedConfig.split("\n");
  const block = workerBlock(lines, "configuration");
  if (!block) return null;
  for (let i = block.start + 1; i < block.end; i++) {
    const match = lines[i]!.trim().match(/^directory:\s*(.+?)\s*$/);
    if (match) return match[1]!.replace(/^(['"])(.*)\1$/, "$2");
  }
  return null;
}

export function persistedBuiltinConfigDirs(
  engineCwd: string,
  configPath: string,
  renderedConfig?: string,
): string[] {
  const dirs = [
    join(engineCwd, "config"),
    join(dirname(configPath), "config"),
    join(engineCwd, "data", "configuration"),
  ];
  const custom = renderedConfig ? configuredPersistDir(renderedConfig) : null;
  if (custom) dirs.unshift(resolve(engineCwd, custom));
  return [...new Set(dirs)];
}

export function persistedBuiltinConfigPaths(
  engineCwd: string,
  configPath: string,
  renderedConfig?: string,
): string[] {
  return persistedBuiltinConfigDirs(engineCwd, configPath, renderedConfig).flatMap((dir) =>
    SEEDED_BUILTIN_WORKERS.map((id) => join(dir, `${id}.yaml`)),
  );
}

export function isPersistedBuiltinEntry(content: string, id: string): boolean {
  const firstLine = content.split("\n").find((line) => line.trim() !== "");
  return firstLine?.trim() === `id: ${id}`;
}

export function clearPersistedBuiltinConfig(
  engineCwd: string,
  configPath: string,
  renderedConfig?: string,
): string[] {
  const cleared: string[] = [];
  for (const path of persistedBuiltinConfigPaths(engineCwd, configPath, renderedConfig)) {
    try {
      const content = readFileSync(path, "utf8");
      if (!isPersistedBuiltinEntry(content, basename(path, ".yaml"))) continue;
      rmSync(path);
      cleared.push(path);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") continue;
      throw new Error(
        `could not remove persisted engine config ${path}: ${String(err)}`,
      );
    }
  }
  return cleared;
}

export interface EngineConfigOptions {
  dataDir: string;
  ports?: EngineRuntimePorts;
}

export interface EngineRuntimePorts {
  restPort: number;
  streamPort: number;
  viewerPort: number;
  enginePort: number;
}

function yamlSingleQuote(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function workerBlock(
  lines: string[],
  name: string,
): { start: number; end: number; indent: string } | null {
  const marker = `- name: ${name}`;
  const start = lines.findIndex((line) => line.trim() === marker);
  if (start === -1) return null;
  const indent = lines[start]!.match(/^\s*/)?.[0] ?? "";
  let end = lines.length;
  for (let i = start + 1; i < lines.length; i++) {
    const line = lines[i]!;
    if (line.startsWith(indent) && line.trim().startsWith("- name: ")) {
      end = i;
      break;
    }
  }
  return { start, end, indent };
}

function setWorkerPort(lines: string[], name: string, port: number): void {
  let block = workerBlock(lines, name);
  if (!block && name === "iii-worker-manager") {
    const workersIndex = lines.findIndex((line) => line.trim() === "workers:");
    if (workersIndex === -1) return;
    lines.splice(
      workersIndex + 1,
      0,
      "  - name: iii-worker-manager",
      "    config:",
      `      port: ${port}`,
      "      host: 127.0.0.1",
    );
    return;
  }
  if (!block) return;

  const portIndex = lines.findIndex(
    (line, index) =>
      index > block!.start &&
      index < block!.end &&
      line.trim().startsWith("port:"),
  );
  if (portIndex !== -1) {
    const indent = lines[portIndex]!.match(/^\s*/)?.[0] ?? `${block.indent}    `;
    lines[portIndex] = `${indent}port: ${port}`;
    return;
  }

  const configIndex = lines.findIndex(
    (line, index) =>
      index > block!.start && index < block!.end && line.trim() === "config:",
  );
  if (configIndex !== -1) {
    const configIndent = lines[configIndex]!.match(/^\s*/)?.[0] ?? `${block.indent}  `;
    lines.splice(configIndex + 1, 0, `${configIndent}  port: ${port}`);
  }
}

function setManagedCorsOrigins(
  lines: string[],
  restPort: number,
  viewerPort: number,
): void {
  const block = workerBlock(lines, "iii-http");
  if (!block) return;
  const originsIndex = lines.findIndex(
    (line, index) =>
      index > block.start &&
      index < block.end &&
      line.trim().startsWith("allowed_origins:"),
  );
  if (originsIndex === -1) return;
  const indent = lines[originsIndex]!.match(/^\s*/)?.[0] ?? "        ";
  lines[originsIndex] =
    `${indent}allowed_origins: [` +
    `"http://localhost:${restPort}", ` +
    `"http://localhost:${viewerPort}", ` +
    `"http://127.0.0.1:${restPort}", ` +
    `"http://127.0.0.1:${viewerPort}"]`;
}

function setBuiltinQueueStore(lines: string[], dataDir: string): void {
  const worker = workerBlock(lines, "iii-queue");
  if (!worker) throw new Error("AgentMemory durable summaries require an iii-queue worker");
  const indent = (line: string) => line.length - line.trimStart().length;
  const child = (from: number, to: number, depth: number, key: string) =>
    lines.findIndex((line, index) => index > from && index < to && indent(line) === depth &&
      line.trimStart().startsWith(`${key}:`));
  const adapterIndex = lines.findIndex((line, index) =>
    index > worker.start && index < worker.end && line.trim() === "adapter:");
  if (adapterIndex === -1) throw new Error("AgentMemory durable summaries require a configured iii-queue adapter");
  const adapterIndent = indent(lines[adapterIndex]!);
  let adapterEnd = worker.end;
  for (let i = adapterIndex + 1; i < worker.end; i++) {
    if (lines[i]!.trim() && indent(lines[i]!) <= adapterIndent) { adapterEnd = i; break; }
  }
  const nameIndent = adapterIndent + 2;
  const nameIndex = child(adapterIndex, adapterEnd, nameIndent, "name");
  const adapterName = nameIndex < 0 ? "" : lines[nameIndex]!.trim().slice(5).trim()
    .replace(/\s+#.*$/, "").replace(/^(['"])(.*)\1$/, "$2");
  if (!adapterName) throw new Error("AgentMemory durable summaries require a named iii-queue adapter");
  if (adapterName !== "builtin") return;

  const configIndent = nameIndent;
  const configIndex = child(adapterIndex, adapterEnd, configIndent, "config");
  const values = [["store_method", "file_based"], ["file_path", yamlSingleQuote(resolve(dataDir, "queue_store"))]];
  const storeLines = values.map(([key, value]) => `${" ".repeat(configIndent + 2)}${key}: ${value}`);

  if (configIndex === -1) {
    lines.splice(nameIndex + 1, 0, `${" ".repeat(configIndent)}config:`, ...storeLines);
    return;
  }

  const inline = lines[configIndex]!.trim().slice(7).trim();
  if (inline === "{}" || inline === "null") {
    lines.splice(configIndex, 1, `${" ".repeat(configIndent)}config:`, ...storeLines);
    return;
  }
  if (inline.startsWith("{") && inline.endsWith("}")) {
    let entries = inline.slice(1, -1).trim();
    for (const [key, value] of values) {
      if (new RegExp(`\\b${key}\\s*:`).test(entries)) entries = entries.replace(new RegExp(`(\\b${key}\\s*:\\s*)[^,}]*`), `$1${value}`);
      else entries += `${entries ? ", " : ""}${key}: ${value}`;
    }
    lines[configIndex] = `${" ".repeat(configIndent)}config: {${entries}}`;
    return;
  }
  if (inline) throw new Error("AgentMemory cannot enforce persistent iii-queue storage for this adapter config");

  const storeIndent = configIndent + 2;
  const missing: string[] = [];
  for (const [key, value] of values) {
    const fieldIndex = child(configIndex, adapterEnd, storeIndent, key);
    if (fieldIndex < 0) missing.push(`${" ".repeat(storeIndent)}${key}: ${value}`);
    else lines[fieldIndex] = `${" ".repeat(storeIndent)}${key}: ${value}`;
  }
  if (missing.length === 0) return;

  let configEnd = adapterEnd;
  for (let i = configIndex + 1; i < adapterEnd; i++) {
    if (lines[i]!.trim() && indent(lines[i]!) <= configIndent) { configEnd = i; break; }
  }
  lines.splice(configEnd, 0, ...missing);
}

export type LegacyStateStoreInspection =
  | { status: "missing" }
  | {
      status: "directory" | "file" | "symlink" | "other" | "unreadable";
      path: string;
    };

export function inspectLegacyStateStore(path: string): LegacyStateStoreInspection {
  try {
    const entry = lstatSync(path);
    if (entry.isSymbolicLink()) return { status: "symlink", path };
    if (entry.isDirectory()) return { status: "directory", path };
    if (entry.isFile()) return { status: "file", path };
    return { status: "other", path };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { status: "missing" };
    return { status: "unreadable", path };
  }
}

export function stateMigrationReceiptPath(targetPath: string): string {
  return `${targetPath}.migration-ready.json`;
}

function setBuiltinStateStore(lines: string[], dataDir: string): void {
  const worker = workerBlock(lines, "iii-state");
  if (!worker) throw new Error("AgentMemory state pagination requires an iii-state worker");
  const indent = (line: string) => line.length - line.trimStart().length;
  const directChild = (from: number, to: number, depth: number, key: string) =>
    lines.findIndex((line, index) => index > from && index < to && indent(line) === depth &&
      line.trimStart().startsWith(`${key}:`));
  const workerIndent = indent(lines[worker.start]!);
  const workerConfig = directChild(worker.start, worker.end, workerIndent + 2, "config");
  if (workerConfig < 0 || lines[workerConfig]!.trim() !== "config:") {
    throw new Error("AgentMemory requires a multiline iii-state config with an adapter");
  }
  const adapterIndex = directChild(workerConfig, worker.end, workerIndent + 4, "adapter");
  if (adapterIndex < 0 || lines[adapterIndex]!.trim() !== "adapter:") {
    throw new Error("AgentMemory requires a multiline iii-state adapter config");
  }

  const adapterIndent = indent(lines[adapterIndex]!);
  let adapterEnd = worker.end;
  for (let i = adapterIndex + 1; i < worker.end; i++) {
    if (lines[i]!.trim() && indent(lines[i]!) <= adapterIndent) {
      adapterEnd = i;
      break;
    }
  }
  const childIndent = adapterIndent + 2;
  let nameIndex = directChild(adapterIndex, adapterEnd, childIndent, "name");
  if (nameIndex < 0) {
    lines.splice(adapterIndex + 1, 0, `${" ".repeat(childIndent)}name: sqlite`);
    nameIndex = adapterIndex + 1;
    adapterEnd += 1;
  } else {
    lines[nameIndex] = `${" ".repeat(childIndent)}name: sqlite`;
  }

  let configIndex = directChild(adapterIndex, adapterEnd, childIndent, "config");
  const pathLine = `${" ".repeat(childIndent + 2)}file_path: ${yamlSingleQuote(resolve(dataDir, "state_store.sqlite3"))}`;
  if (configIndex < 0) {
    lines.splice(nameIndex + 1, 0, `${" ".repeat(childIndent)}config:`, pathLine);
    return;
  }

  let configEnd = configIndex + 1;
  if (lines[configIndex]!.trim() === `${" ".repeat(childIndent)}config:`.trim()) {
    while (configEnd < adapterEnd) {
      if (lines[configEnd]!.trim() && indent(lines[configEnd]!) <= childIndent) break;
      configEnd += 1;
    }
  }
  lines.splice(
    configIndex,
    configEnd - configIndex,
    `${" ".repeat(childIndent)}config:`,
    pathLine,
  );
}

export function renderEngineConfig(
  template: string,
  options: EngineConfigOptions,
): string {
  const rendered = template
    .replace(
      "file_path: ./data/stream_store",
      `file_path: ${yamlSingleQuote(join(options.dataDir, "stream_store"))}`,
    )
    .replace(
      "file_path: ./data/queue_store",
      `file_path: ${yamlSingleQuote(resolve(options.dataDir, "queue_store"))}`,
    );
  const lines = rendered.split("\n");
  setBuiltinStateStore(lines, options.dataDir);
  setBuiltinQueueStore(lines, options.dataDir);
  if (options.ports) {
    setWorkerPort(lines, "iii-http", options.ports.restPort);
    setWorkerPort(lines, "iii-stream", options.ports.streamPort);
    setWorkerPort(lines, "iii-worker-manager", options.ports.enginePort);
    setManagedCorsOrigins(lines, options.ports.restPort, options.ports.viewerPort);
  }
  return lines.join("\n");
}
