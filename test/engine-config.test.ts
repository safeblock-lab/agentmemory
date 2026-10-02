import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
  clearPersistedBuiltinConfig,
  persistedBuiltinConfigDirs,
  persistedBuiltinConfigPaths,
  renderEngineConfig,
} from "../src/cli/engine-config.js";

const customDirConfig = [
  "workers:",
  "  - name: iii-http",
  "    config:",
  "      port: 3111",
  "  - name: configuration",
  "    config:",
  "      adapter:",
  "        name: fs",
  "        config:",
  "          directory: ./custom-cfg",
  "  - name: iii-state",
  "    config:",
  "      adapter:",
  "        name: kv",
  "        config:",
  "          file_path: ./data/state_store.db",
].join("\n");

const builtinStateWorker = [
  "  - name: iii-state",
  "    config:",
  "      adapter:",
  "        name: kv",
  "        config:",
  "          file_path: ./data/state_store.db",
];

function withBuiltinStateWorker(lines: string[]): string {
  return [...lines, ...builtinStateWorker].join("\n");
}

describe("renderEngineConfig", () => {
  it("stores engine state in the resolved data directory", () => {
    const source = readFileSync(
      join(import.meta.dirname, "..", "iii-config.yaml"),
      "utf8",
    );
    const dataDir = join("/var", "lib", "agentmemory");

    const rendered = renderEngineConfig(source, { dataDir });

    expect(rendered).toContain("        name: sqlite");
    expect(rendered).toContain(
      `file_path: '${resolve(dataDir, "state_store.sqlite3")}'`,
    );
    expect(rendered).toContain(
      `file_path: '${join(dataDir, "stream_store")}'`,
    );
    expect(rendered).toContain(
      `file_path: '${resolve(dataDir, "queue_store")}'`,
    );
    expect(rendered).toContain("store_method: file_based");
    expect(rendered).not.toContain("./data/");
    expect(rendered).not.toContain("state_store.db");
  });

  it("migrates a legacy builtin queue adapter to the persistent data directory", () => {
    const legacy = withBuiltinStateWorker([
      "workers:",
      "  - name: iii-queue",
      "    config:",
      "      adapter:",
      "        name: builtin",
    ]);
    const dataDir = join("/var", "lib", "agentmemory");

    const rendered = renderEngineConfig(legacy, { dataDir });

    expect(rendered).toContain("        name: builtin\n        config:\n          store_method: file_based");
    expect(rendered).toContain(`file_path: '${resolve(dataDir, "queue_store")}'`);
  });

  it("preserves custom builtin adapter fields when adding the persistent queue store", () => {
    const legacy = withBuiltinStateWorker([
      "workers:",
      "  - name: iii-queue",
      "    config:",
      "      adapter:",
      "        name: builtin",
      "        config:",
      "          retention: 17",
    ]);

    const dataDir = join("/var", "lib", "agentmemory");
    const rendered = renderEngineConfig(legacy, { dataDir });

    expect(rendered).toContain("          retention: 17");
    expect(rendered).toContain("          store_method: file_based");
    expect(rendered).toContain(`          file_path: '${resolve(dataDir, "queue_store")}'`);
  });

  it("forces explicit in-memory builtin queue storage to the persistent data directory", () => {
    const explicitInMemory = withBuiltinStateWorker([
      "workers:",
      "  - name: iii-queue",
      "    config:",
      "      adapter:",
      "        name: builtin",
      "        config:",
      "          store_method: in_memory",
    ]);

    const rendered = renderEngineConfig(explicitInMemory, {
      dataDir: "/var/lib/agentmemory",
    });

    expect(rendered).toContain("store_method: file_based");
    expect(rendered).not.toContain("store_method: in_memory");
    expect(rendered).toContain(`file_path: '${join(resolve("/var", "lib", "agentmemory"), "queue_store")}'`);
  });

  it.skipIf(process.platform !== "win32")("writes an absolute Windows queue path with spaces", () => {
    const legacy = withBuiltinStateWorker([
      "workers:",
      "  - name: iii-queue",
      "    config:",
      "      adapter:",
      "        name: builtin",
    ]);
    const dataDir = "C:\\Users\\Agent Memory\\.agentmemory\\data";

    const rendered = renderEngineConfig(legacy, { dataDir });

    expect(rendered).toContain(`file_path: '${resolve(dataDir, "queue_store")}'`);
  });

  it("leaves non-builtin queue adapters untouched", () => {
    const customAdapter = withBuiltinStateWorker([
      "workers:",
      "  - name: iii-queue",
      "    config:",
      "      adapter:",
      "        name: redis",
    ]);

    const rendered = renderEngineConfig(customAdapter, { dataDir: "/var/lib/agentmemory" });
    expect(rendered).toContain("        name: redis");
    expect(rendered).toContain(`file_path: '${resolve("/var/lib/agentmemory", "state_store.sqlite3")}'`);
  });

  it("keeps the Docker queue store under the mounted data directory", () => {
    const source = readFileSync(
      join(import.meta.dirname, "..", "iii-config.docker.yaml"),
      "utf8",
    );

    expect(source).toContain("file_path: /data/queue_store");
  });

  it("moves the complete native port quartet from one REST override", () => {
    const source = readFileSync(
      join(import.meta.dirname, "..", "iii-config.yaml"),
      "utf8",
    );

    const rendered = renderEngineConfig(source, {
      dataDir: "/tmp/agentmemory",
      ports: {
        restPort: 3211,
        streamPort: 3212,
        viewerPort: 3213,
        enginePort: 49234,
      },
    });

    expect(rendered).toMatch(
      /- name: iii-http\r?\n\s+config:\r?\n\s+port: 3211/,
    );
    expect(rendered).toMatch(
      /- name: iii-stream\r?\n\s+config:\r?\n\s+port: 3212/,
    );
    expect(rendered).toContain(
      'allowed_origins: ["http://localhost:3211", "http://localhost:3213", "http://127.0.0.1:3211", "http://127.0.0.1:3213"]',
    );
    expect(rendered).toMatch(
      /- name: iii-worker-manager\r?\n\s+config:\r?\n\s+port: 49234\r?\n\s+host: 127\.0\.0\.1/,
    );
  });

  it("keeps the iii- prefixed builtin names: on 0.22.1 the unprefixed names resolve to registry workers", () => {
    const source = readFileSync(
      join(import.meta.dirname, "..", "iii-config.yaml"),
      "utf8",
    );
    const names = [...source.matchAll(/^\s*- name: (\S+)$/gm)].map((m) => m[1]);

    expect(names).toEqual(
      expect.arrayContaining([
        "iii-http",
        "iii-state",
        "iii-pubsub",
        "iii-cron",
        "iii-queue",
        "iii-stream",
        "iii-observability",
      ]),
    );
    for (const bare of ["http", "state", "pubsub", "cron", "queue"]) {
      expect(names).not.toContain(bare);
    }
  });
});

describe("clearPersistedBuiltinConfig", () => {
  const configPath = join("/srv", "state", "iii-config.runtime.yaml");

  it("covers the engine cwd, the config file directory and the legacy 0.19 location", () => {
    expect(persistedBuiltinConfigDirs("/srv/engine", configPath)).toEqual([
      join("/srv", "engine", "config"),
      join("/srv", "state", "config"),
      join("/srv", "engine", "data", "configuration"),
    ]);
  });

  it("lists both the unprefixed and the legacy entry name for every seeded builtin", () => {
    const paths = persistedBuiltinConfigPaths("/srv/engine", configPath);

    expect(paths).toContain(join("/srv", "state", "config", "http.yaml"));
    expect(paths).toContain(join("/srv", "state", "config", "iii-http.yaml"));
    expect(paths).toContain(join("/srv", "state", "config", "iii-worker-manager.yaml"));
    expect(paths).toContain(
      join("/srv", "engine", "data", "configuration", "iii-http.yaml"),
    );
    expect(paths.every((p) => p.endsWith(".yaml"))).toBe(true);
  });

  it("adds the directory a configuration worker entry names, resolved against the engine cwd", () => {
    expect(persistedBuiltinConfigDirs("/srv/engine", configPath, customDirConfig)).toEqual([
      resolve("/srv", "engine", "custom-cfg"),
      join("/srv", "engine", "config"),
      join("/srv", "state", "config"),
      join("/srv", "engine", "data", "configuration"),
    ]);
    expect(
      persistedBuiltinConfigDirs(
        "/srv/engine",
        configPath,
        customDirConfig.replace("./custom-cfg", "'/var/lib/iii-cfg'"),
      )[0],
    ).toBe(resolve("/var", "lib", "iii-cfg"));
    expect(
      persistedBuiltinConfigDirs("/srv/engine", configPath, "workers:\n  - name: iii-http\n"),
    ).toEqual(persistedBuiltinConfigDirs("/srv/engine", configPath));
  });

  it("removes persisted builtin entries from both locations and leaves everything else in place", () => {
    const cwd = mkdtempSync(join(tmpdir(), "agentmemory-engine-"));
    const dataDir = mkdtempSync(join(tmpdir(), "agentmemory-data-"));
    const runtimeConfig = join(dataDir, "iii-config.runtime.yaml");
    const current = join(dataDir, "config");
    const legacy = join(cwd, "data", "configuration");
    mkdirSync(current, { recursive: true });
    mkdirSync(legacy, { recursive: true });
    writeFileSync(join(current, "http.yaml"), "id: http\nvalue:\n  port: 3111\n");
    writeFileSync(join(current, "iii-state.yaml"), "id: iii-state\nvalue: {}\n");
    writeFileSync(join(current, "agentmemory.yaml"), "id: agentmemory\nvalue: {}\n");
    writeFileSync(join(legacy, "iii-http.yaml"), "id: iii-http\nvalue:\n  port: 3111\n");

    const cleared = clearPersistedBuiltinConfig(cwd, runtimeConfig);

    expect(cleared).toHaveLength(3);
    expect(existsSync(join(current, "http.yaml"))).toBe(false);
    expect(existsSync(join(current, "iii-state.yaml"))).toBe(false);
    expect(existsSync(join(legacy, "iii-http.yaml"))).toBe(false);
    expect(existsSync(join(current, "agentmemory.yaml"))).toBe(true);
  });

  it("removes the entries the engine persisted under a custom configuration directory", () => {
    const cwd = mkdtempSync(join(tmpdir(), "agentmemory-engine-"));
    const dir = join(cwd, "custom-cfg");
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "iii-http.yaml"), "id: iii-http\nvalue:\n  port: 3111\n");
    writeFileSync(join(dir, "agentmemory.yaml"), "id: agentmemory\nvalue: {}\n");

    const cleared = clearPersistedBuiltinConfig(
      cwd,
      join(cwd, "iii-config.runtime.yaml"),
      customDirConfig,
    );

    expect(cleared).toEqual([join(dir, "iii-http.yaml")]);
    expect(existsSync(join(dir, "agentmemory.yaml"))).toBe(true);
  });

  it("leaves files that are not engine entries alone, even under a seeded builtin name", () => {
    const cwd = mkdtempSync(join(tmpdir(), "agentmemory-engine-"));
    const dir = join(cwd, "config");
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "http.yaml"), "host: 0.0.0.0\nport: 8080\n");
    writeFileSync(join(dir, "state.yaml"), "id: something-else\nvalue: {}\n");
    writeFileSync(join(dir, "iii-http.yaml"), "\nid: iii-http\nname: HTTP\nvalue:\n  port: 3111\n");

    const cleared = clearPersistedBuiltinConfig(cwd, join(cwd, "iii-config.runtime.yaml"));

    expect(cleared).toEqual([join(dir, "iii-http.yaml")]);
    expect(existsSync(join(dir, "http.yaml"))).toBe(true);
    expect(existsSync(join(dir, "state.yaml"))).toBe(true);
  });

  it("is a no-op when the engine has never persisted anything", () => {
    const cwd = mkdtempSync(join(tmpdir(), "agentmemory-engine-"));

    expect(
      clearPersistedBuiltinConfig(cwd, join(cwd, "iii-config.runtime.yaml")),
    ).toEqual([]);
  });

  it("fails loudly when a persisted entry exists but cannot be removed", () => {
    const cwd = mkdtempSync(join(tmpdir(), "agentmemory-engine-"));
    mkdirSync(join(cwd, "config", "http.yaml"), { recursive: true });

    expect(() =>
      clearPersistedBuiltinConfig(cwd, join(cwd, "iii-config.runtime.yaml")),
    ).toThrow(/http\.yaml/);
  });
});
