import { execFile } from "node:child_process";

export interface EngineMemoryMetric {
  residentBytes: number | null;
  sampledAt: string | null;
  source: "iii.exe" | "unavailable";
  reason?: "unsupported_platform" | "remote_engine" | "invalid_engine_url" | "process_unavailable" | "probe_failed";
}

interface EngineProcessSample {
  pid: number;
  identity: string;
  residentBytes: number;
}

interface EngineMemoryReaderOptions {
  getEngineUrl: () => string;
  platform?: string;
  now?: () => number;
  probe?: (port: number) => Promise<EngineProcessSample | null>;
  cacheMs?: number;
}

const PROBE_TIMEOUT_MS = 1000;
const PROBE_MAX_BUFFER_BYTES = 8192;
const CACHE_MS = 30_000;

function localPort(engineUrl: string): number | null {
  try {
    const url = new URL(engineUrl);
    const host = url.hostname.toLowerCase().replace(/^\[|\]$/g, "");
    if (!new Set(["localhost", "127.0.0.1", "::1", "::ffff:127.0.0.1"]).has(host)) return null;
    const port = url.port ? Number(url.port) : url.protocol === "wss:" ? 443 : 80;
    return Number.isInteger(port) && port >= 1 && port <= 65_535 ? port : null;
  } catch {
    return null;
  }
}

export function powershellScript(port: number): string {
  return `
$ErrorActionPreference = 'Stop'
$port = ${port}
try {
  $netstat = Join-Path $env:SystemRoot 'System32\\netstat.exe'
  $pattern = "^\\s*TCP\\s+\\S+:$port\\s+\\S+:0\\s+\\S+\\s+([1-9]\\d*)\\s*$"
  $listeners = @(& $netstat -ano -p tcp | ForEach-Object { if ($_ -match $pattern) { [int]$Matches[1] } } | Sort-Object -Unique)
  if ($listeners.Count -ne 1) { exit 2 }
  $targetPid = [int]$listeners[0]
  $first = Get-Process -Id $targetPid
  if ([IO.Path]::GetFileName([string]$first.Path) -ine 'iii.exe') { exit 2 }
  $identity = [string]$first.StartTime.ToUniversalTime().Ticks
  $residentBytes = [int64]$first.WorkingSet64
  $second = Get-Process -Id $targetPid
  if ([string]$second.StartTime.ToUniversalTime().Ticks -cne $identity) { exit 2 }
  if ($residentBytes -lt 0) { exit 2 }
  [Console]::WriteLine("$targetPid|$identity|$residentBytes")
} catch { exit 3 }
`.trim();
}

function runWindowsProbe(port: number): Promise<EngineProcessSample | null> {
  const script = powershellScript(port);
  const encoded = Buffer.from(script, "utf16le").toString("base64");
  return new Promise((resolve, reject) => {
    execFile(
      "powershell.exe",
      ["-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
      { timeout: PROBE_TIMEOUT_MS, maxBuffer: PROBE_MAX_BUFFER_BYTES, windowsHide: true, encoding: "utf8" },
      (error, stdout) => {
        if (error) {
          if ((error as NodeJS.ErrnoException).code === "ERR_CHILD_PROCESS_STDIO_MAXBUFFER") return reject(error);
          return resolve(null);
        }
        const match = /^([1-9]\d*)\|([^\r\n|]{1,64})\|(\d+)\s*$/.exec(stdout);
        if (!match) return resolve(null);
        const pid = Number(match[1]);
        const residentBytes = Number(match[3]);
        if (!Number.isSafeInteger(pid) || !Number.isSafeInteger(residentBytes) || residentBytes < 0) return resolve(null);
        resolve({ pid, identity: match[2]!, residentBytes });
      },
    );
  });
}

function unavailable(reason: EngineMemoryMetric["reason"]): EngineMemoryMetric {
  return { residentBytes: null, sampledAt: null, source: "unavailable", reason };
}

export function createEngineMemoryReader(options: EngineMemoryReaderOptions) {
  const platform = options.platform ?? process.platform;
  const getNow = options.now ?? Date.now;
  const probe = options.probe ?? runWindowsProbe;
  const cacheMs = options.cacheMs ?? CACHE_MS;
  let cachedAt = 0;
  let cached: EngineMemoryMetric | null = null;
  let cachedTarget: string | null = null;
  let pending: { target: string; promise: Promise<EngineMemoryMetric> } | null = null;

  return async function read(): Promise<EngineMemoryMetric> {
    const now = getNow();
    let url = "";
    try { url = options.getEngineUrl(); } catch {}
    const target = `${platform}\u0000${url}`;
    if (cached && cachedTarget === target && now - cachedAt < cacheMs) return cached;
    if (pending?.target === target) return pending.promise;
    const promise = (async () => {
      let result: EngineMemoryMetric;
      if (platform !== "win32") result = unavailable("unsupported_platform");
      else {
        if (!url) result = unavailable("invalid_engine_url");
        else {
          const port = localPort(url);
          if (port === null) result = unavailable("remote_engine");
          else {
            try {
              const sample = await probe(port);
              result = sample && Number.isSafeInteger(sample.pid) && sample.pid > 0
                && typeof sample.identity === "string" && sample.identity.length > 0
                && Number.isSafeInteger(sample.residentBytes) && sample.residentBytes >= 0
                ? { residentBytes: sample.residentBytes, sampledAt: new Date(getNow()).toISOString(), source: "iii.exe" }
                : unavailable("process_unavailable");
            } catch {
              result = unavailable("probe_failed");
            }
          }
        }
      }
      cachedAt = getNow();
      cached = result;
      cachedTarget = target;
      return result;
    })().finally(() => { if (pending?.promise === promise) pending = null; });
    pending = { target, promise };
    return promise;
  };
}
