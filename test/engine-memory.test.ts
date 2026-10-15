import { describe, expect, it } from "vitest";
import { createEngineMemoryReader, powershellScript } from "../src/functions/engine-memory.js";

describe("configured iii.exe memory probe", () => {
  it("returns only resident bytes after validating a local listener and caches for 30 seconds", async () => {
    let now = 1000;
    let probes = 0;
    const read = createEngineMemoryReader({
      platform: "win32",
      getEngineUrl: () => "ws://localhost:49134",
      now: () => now,
      probe: async (port) => {
        expect(port).toBe(49134);
        probes++;
        return { pid: 42, identity: "created-at", residentBytes: 16_384 };
      },
    });
    const first = await read();
    const cached = await read();
    expect(first).toEqual({ residentBytes: 16_384, sampledAt: new Date(now).toISOString(), source: "iii.exe" });
    expect(cached).toEqual(first);
    expect(probes).toBe(1);
    now += 30_001;
    await read();
    expect(probes).toBe(2);
  });

  it("does not probe remote engines or non-Windows hosts", async () => {
    let probes = 0;
    const remote = createEngineMemoryReader({
      platform: "win32", getEngineUrl: () => "ws://engine.example:49134",
      probe: async () => { probes++; return null; },
    });
    const unsupported = createEngineMemoryReader({
      platform: "linux", getEngineUrl: () => "ws://localhost:49134",
      probe: async () => { probes++; return null; },
    });
    expect(await remote()).toMatchObject({ residentBytes: null, source: "unavailable", reason: "remote_engine" });
    expect(await unsupported()).toMatchObject({ residentBytes: null, source: "unavailable", reason: "unsupported_platform" });
    expect(probes).toBe(0);
  });

  it("invalidates a cached reading when the configured engine URL changes", async () => {
    let engineUrl = "ws://localhost:49134";
    let probes = 0;
    const read = createEngineMemoryReader({
      platform: "win32",
      getEngineUrl: () => engineUrl,
      probe: async () => ({ pid: 42, identity: "created-at", residentBytes: ++probes * 1024 }),
    });
    expect((await read()).residentBytes).toBe(1024);
    engineUrl = "ws://engine.example:49134";
    expect(await read()).toMatchObject({ residentBytes: null, reason: "remote_engine" });
    engineUrl = "ws://localhost:49234";
    expect((await read()).residentBytes).toBe(2048);
    expect(probes).toBe(2);
  });

  it("uses locale-independent TCP listener detection and checks process identity twice", () => {
    const script = powershellScript(49134);
    expect(script).toContain("\\S+:0");
    expect(script).not.toContain("LISTENING");
    expect(script).toContain("[IO.Path]::GetFileName([string]$first.Path)");
    expect(script).toContain("$first.StartTime.ToUniversalTime().Ticks");
    expect(script).toContain("$second.StartTime.ToUniversalTime().Ticks");
    expect(script).toContain("$netstat = Join-Path $env:SystemRoot");
    expect(script.match(/& \$netstat -ano -p tcp/g)).toHaveLength(1);
  });

  it("shares concurrent probes and returns unavailable for ambiguous or failed probes", async () => {
    let release!: (value: { pid: number; identity: string; residentBytes: number } | null) => void;
    let probes = 0;
    const read = createEngineMemoryReader({
      platform: "win32", getEngineUrl: () => "ws://127.0.0.1:49234",
      probe: async () => {
        probes++;
        return await new Promise((resolve) => { release = resolve; });
      },
    });
    const left = read();
    const right = read();
    release(null);
    expect(await left).toMatchObject({ residentBytes: null, reason: "process_unavailable" });
    expect(await right).toMatchObject({ residentBytes: null, reason: "process_unavailable" });
    expect(probes).toBe(1);

    const failed = createEngineMemoryReader({
      platform: "win32", getEngineUrl: () => "ws://localhost:49134",
      probe: async () => { throw new Error("permission denied"); },
    });
    expect(await failed()).toMatchObject({ residentBytes: null, reason: "probe_failed" });
  });
});
