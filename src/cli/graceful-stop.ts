import { execFile } from "node:child_process";
import { join } from "node:path";
import { SHUTDOWN_ADMITTED_DRAIN_MS, SHUTDOWN_HARD_EXIT_MS } from "../shutdown.js";

const WORKER_EXIT_WAIT_MS = Math.max(210000, SHUTDOWN_ADMITTED_DRAIN_MS + SHUTDOWN_HARD_EXIT_MS);

export type GracefulStopResult =
  | { exited: true }
  | { exited: false; reason: string };

function windowsConsoleStop(pid: number, entryPaths: string[]): Promise<void> {
  const request = Buffer.from(JSON.stringify({ pid, executable: process.execPath, entryPaths })).toString("base64");
  const script = String.raw`
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$request = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${request}')) | ConvertFrom-Json
$targetPid = [uint32]$request.pid
function Read-OwnedWorker {
  $current = Get-CimInstance Win32_Process -Filter "ProcessId=$targetPid"
  if (-not $current) { return $null }
  if ([IO.Path]::GetFullPath($current.ExecutablePath) -ine [IO.Path]::GetFullPath($request.executable)) { throw 'Worker executable identity is not admissible.' }
  $arguments = [AgentMemoryOwnedConsole]::Arguments($current.CommandLine)
  if ($arguments.Length -lt 2) { throw 'Worker entry point is unavailable.' }
  $entry = [IO.Path]::GetFullPath($arguments[1])
  $matchesEntry = $false
  foreach ($expected in $request.entryPaths) {
    if ($entry -ieq [IO.Path]::GetFullPath($expected)) { $matchesEntry = $true }
  }
  if (-not $matchesEntry) { throw 'Worker command line is not this installation entry point.' }
  return $current
}
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class AgentMemoryOwnedConsole {
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool FreeConsole();
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool AttachConsole(uint pid);
  [DllImport("kernel32.dll", SetLastError=true)] static extern uint GetConsoleProcessList(uint[] ids, uint count);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool GenerateConsoleCtrlEvent(uint signal, uint group);
  delegate bool Handler(uint signal);
  [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetConsoleCtrlHandler(Handler handler, bool add);
  [DllImport("shell32.dll", SetLastError=true, CharSet=CharSet.Unicode)] static extern IntPtr CommandLineToArgvW(string line, out int count);
  [DllImport("kernel32.dll")] static extern IntPtr LocalFree(IntPtr memory);
  static readonly Handler ignore = signal => true;
  public static string[] Arguments(string line) {
    int count;
    IntPtr memory = CommandLineToArgvW(line, out count);
    if (memory == IntPtr.Zero) throw new InvalidOperationException("Worker command line cannot be parsed.");
    try {
      string[] arguments = new string[count];
      for (int i=0; i<count; i++) arguments[i] = Marshal.PtrToStringUni(Marshal.ReadIntPtr(memory, i*IntPtr.Size));
      return arguments;
    } finally { LocalFree(memory); }
  }
  public static void AssertIsolated(uint worker, uint helper) {
    uint[] ids = new uint[64];
    uint count = GetConsoleProcessList(ids, 64);
    if (count == 0 || count > 64) throw new InvalidOperationException("Console inventory unavailable.");
    bool foundWorker = false, foundHelper = false;
    for (int i=0; i<count; i++) {
      if (ids[i] == worker) foundWorker = true;
      else if (ids[i] == helper) foundHelper = true;
      else throw new InvalidOperationException("Worker console is shared; shutdown held.");
    }
    if (!foundWorker || !foundHelper) throw new InvalidOperationException("Console ownership incomplete.");
  }
  public static void Attach(uint worker, uint helper) {
    FreeConsole();
    if (!AttachConsole(worker)) throw new InvalidOperationException("Worker console cannot be attached: " + Marshal.GetLastWin32Error());
    if (!SetConsoleCtrlHandler(ignore, true)) throw new InvalidOperationException("Console handler registration failed.");
    AssertIsolated(worker, helper);
  }
  public static void Send() {
    if (!GenerateConsoleCtrlEvent(0, 0)) throw new InvalidOperationException("CTRL_C delivery failed: " + Marshal.GetLastWin32Error());
  }
  public static void Detach() { FreeConsole(); }
}
'@
$before = Read-OwnedWorker
if (-not $before) { exit 0 }
try {
  [AgentMemoryOwnedConsole]::Attach($targetPid, [uint32]$PID)
  $after = Read-OwnedWorker
  if (-not $after) { exit 0 }
  if ($before.CreationDate.ToUniversalTime().Ticks -ne $after.CreationDate.ToUniversalTime().Ticks -or $before.ExecutablePath -cne $after.ExecutablePath -or $before.CommandLine -cne $after.CommandLine) { throw 'Worker identity changed; shutdown held.' }
  [AgentMemoryOwnedConsole]::AssertIsolated($targetPid, [uint32]$PID)
  [AgentMemoryOwnedConsole]::Send()
} finally { [AgentMemoryOwnedConsole]::Detach() }
`;
  return new Promise<void>((resolve, reject) => {
    execFile(
      join(process.env["SystemRoot"] ?? "C:\\Windows", "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
      ["-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
      { windowsHide: true, timeout: 30000, maxBuffer: 64 * 1024 },
      (error, _stdout, stderr) => {
        if (error) reject(new Error(stderr.trim() || error.message));
        else resolve();
      },
    );
  });
}

export async function stopWorkerGracefully(
  pid: number,
  entryPaths: string[],
  isAlive: (pid: number) => boolean,
): Promise<GracefulStopResult> {
  if (!Number.isSafeInteger(pid) || pid <= 0 || pid === process.pid) {
    return { exited: false, reason: "Worker PID identity is not admissible." };
  }
  if (!isAlive(pid)) return { exited: true };
  try {
    if (process.platform === "win32") await windowsConsoleStop(pid, entryPaths);
    else process.kill(pid, "SIGTERM");
  } catch (error) {
    if (!isAlive(pid)) return { exited: true };
    return { exited: false, reason: error instanceof Error ? error.message : String(error) };
  }
  const deadline = Date.now() + WORKER_EXIT_WAIT_MS;
  while (isAlive(pid)) {
    if (Date.now() >= deadline) {
      return { exited: false, reason: "DRAINING: worker is still alive; engine and lifecycle metadata are preserved. Wait for its admitted work to settle." };
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return { exited: true };
}
