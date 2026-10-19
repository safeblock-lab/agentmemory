import { constants } from "node:fs";
import { lstat, open, realpath, rename, unlink } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash, randomUUID } from "node:crypto";
import type { GraphRecoveryAuthorityProfile, GraphRecoveryServiceAuthority } from "../state/graph-automatic-recovery.js";
import { getGraphAutomaticStateDirectory } from "../config.js";

const run = promisify(execFile);
const fail = (): never => { throw new Error("STATE_GRAPH_AUTOMATIC_AUTHORITY_NOT_PRIVATE"); };

export interface GraphAutomaticBootstrapDiagnostic {
  phase: "directory" | "previous_file" | "temporary_file" | "parent" | "beforeimage";
  failure: "canonical" | "file_type" | "owner_or_mode" | "acl_policy" | "script_launch" | "script_exit" | "timeout" | "filesystem";
  helperClass?: "module_load" | "command_unavailable" | "parser" | "descriptor" | "acl_read" | "unknown";
}
class GraphAutomaticAuthorityError extends Error {
  constructor(readonly diagnostic: GraphAutomaticBootstrapDiagnostic) {
    super(["canonical", "file_type", "owner_or_mode", "acl_policy"].includes(diagnostic.failure)
      ? "STATE_GRAPH_AUTOMATIC_AUTHORITY_NOT_PRIVATE" : "STATE_GRAPH_AUTOMATIC_AUTHORITY_UNAVAILABLE");
  }
}
export interface GraphAutomaticAvailability { enabled: boolean; reason: string | null; diagnostic?: GraphAutomaticBootstrapDiagnostic }
let availability: GraphAutomaticAvailability = { enabled: false, reason: "GRAPH_AUTOMATIC_INITIALIZING" };
export function graphAutomaticAvailability(): GraphAutomaticAvailability { return { ...availability }; }
export function setGraphAutomaticAvailability(enabled: boolean, reason: string | null = null, diagnostic?: GraphAutomaticBootstrapDiagnostic): void {
  availability = { enabled, reason: enabled ? null : reason, ...(!enabled && diagnostic ? { diagnostic: { ...diagnostic } } : {}) };
}
export function graphAutomaticBootstrapDiagnostic(error: unknown): GraphAutomaticBootstrapDiagnostic | undefined {
  return error instanceof GraphAutomaticAuthorityError ? { ...error.diagnostic } : undefined;
}
export function graphAutomaticBootstrapFailure(error: unknown): string {
  const safe = ["STATE_GRAPH_AUTOMATIC_AUTHORITY_NOT_PRIVATE", "STATE_GRAPH_AUTOMATIC_AUTHORITY_CHANGED", "STATE_GRAPH_AUTOMATIC_AUTHORITY_LIMIT"];
  return error instanceof Error && safe.includes(error.message) ? error.message : "STATE_GRAPH_AUTOMATIC_AUTHORITY_UNAVAILABLE";
}

async function privatePath(path: string, directory: boolean, phase: GraphAutomaticBootstrapDiagnostic["phase"]): Promise<void> {
  const reject = (failure: GraphAutomaticBootstrapDiagnostic["failure"], helperClass?: GraphAutomaticBootstrapDiagnostic["helperClass"]): never => {
    throw new GraphAutomaticAuthorityError({ phase, failure, ...(helperClass ? { helperClass } : {}) });
  };
  const stat = await lstat(path).catch(error => {
    if (phase === "previous_file" && error?.code === "ENOENT") throw error;
    return reject("filesystem");
  });
  const canonical = resolve(await realpath(path).catch(() => reject("filesystem"))), requested = resolve(path);
  if (stat.isSymbolicLink() || (directory ? !stat.isDirectory() : !stat.isFile())) reject("file_type");
  if (process.platform === "win32" ? canonical.toLowerCase() !== requested.toLowerCase() : canonical !== requested) reject("canonical");
  if (process.platform === "win32") {
    const windowsPowerShellHome = join(process.env.SystemRoot ?? "C:\\Windows", "System32", "WindowsPowerShell", "v1.0");
    const environment = { ...process.env };
    for (const key of Object.keys(environment)) if (key.toUpperCase() === "PSMODULEPATH") delete environment[key];
    environment.PSModulePath = join(windowsPowerShellHome, "Modules");
    environment.GRAPH_AUTHORITY_PRIVATE_PATH = path;
    const script = `$ErrorActionPreference='Stop'; $PSModuleAutoLoadingPreference='None'; $stage='module_load'; try { $env:PSModulePath=[IO.Path]::Combine($PSHOME,'Modules'); Import-Module ([IO.Path]::Combine($PSHOME,'Modules','Microsoft.PowerShell.Management','Microsoft.PowerShell.Management.psd1')) -ErrorAction Stop; Import-Module ([IO.Path]::Combine($PSHOME,'Modules','Microsoft.PowerShell.Security','Microsoft.PowerShell.Security.psd1')) -ErrorAction Stop; $stage='acl_read'; $p=$env:GRAPH_AUTHORITY_PRIVATE_PATH; $i=Microsoft.PowerShell.Management\\Get-Item -LiteralPath $p -Force; if(($i.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0){exit 2}; $a=Microsoft.PowerShell.Security\\Get-Acl -LiteralPath $p; $stage='descriptor'; $u=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value; $ok=@($u,'S-1-5-18','S-1-5-32-544'); if($a.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin $ok){exit 2}; $raw=[Security.AccessControl.RawSecurityDescriptor]::new($a.GetSecurityDescriptorBinaryForm(),0); $rules=$raw.DiscretionaryAcl; if($null -eq $rules -or $rules.Count -gt 64){exit 2}; foreach($r in $rules){if([int]$r.AceType -notin @(0,1) -or $r.SecurityIdentifier.Value -notin $ok){exit 2}} } catch { $kind='unknown'; $id=$_.FullyQualifiedErrorId; if($_.Exception -is [Management.Automation.ParseException]){$kind='parser'} elseif($_.Exception -is [Management.Automation.CommandNotFoundException]){$kind='command_unavailable'} elseif($stage -eq 'module_load' -or $id -match 'Modules_ModuleNotFound|CouldNotAutoLoad|CouldNotAutoload'){$kind='module_load'} elseif($stage -eq 'acl_read'){$kind='acl_read'} elseif($stage -eq 'descriptor'){$kind='descriptor'}; [Console]::Error.WriteLine('GRAPH_AUTHORITY_HELPER:'+$kind); exit 3 }`;
    await run(join(windowsPowerShellHome, "powershell.exe"), ["-NoProfile", "-NonInteractive", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
      { windowsHide: true, timeout: 10_000, maxBuffer: 32_768, env: environment }).catch(error => {
        if (error?.killed === true) reject("timeout");
        if (error?.code === 2) reject("acl_policy");
        if (typeof error?.code === "string") reject("script_launch");
        const captured = typeof error?.stderr === "string" ? error.stderr.slice(0, 32_768) : "";
        const matched = captured.match(/GRAPH_AUTHORITY_HELPER:(module_load|command_unavailable|parser|descriptor|acl_read|unknown)(?:[\s<]|$)/)?.[1];
        const helperClass = matched ?? (/\bParserError\b/.test(captured) ? "parser" : "unknown");
        reject("script_exit", helperClass as GraphAutomaticBootstrapDiagnostic["helperClass"]);
      });
  } else {
    if (!process.getuid || ![process.getuid(), 0].includes(stat.uid) || (stat.mode & 0o077) !== 0) reject("owner_or_mode");
  }
}

export async function installGraphAutomaticAuthority(profiles: GraphRecoveryAuthorityProfile[]): Promise<void> {
  const directory = getGraphAutomaticStateDirectory();
  await privatePath(directory, true, "directory");
  const path = join(directory, "graph-automatic-authority.json");
  let previous: GraphRecoveryServiceAuthority = { version: 1, profiles: [] };
  let previousBody: string | null = null;
  try {
    await privatePath(path, false, "previous_file");
    const handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    try {
      if ((await handle.stat()).size > 65_536) fail();
      previousBody = await handle.readFile("utf8");
      const value = JSON.parse(previousBody) as GraphRecoveryServiceAuthority;
      if (value.version !== 1 || !Array.isArray(value.profiles)) fail();
      previous = value;
    } finally { await handle.close(); }
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) throw error;
  }
  const merged = new Map(previous.profiles.map(profile => [profile.id, profile]));
  let retiredInvalid = false;
  for (const profile of profiles) {
    const { id, ...declaration } = profile;
    if (!id.startsWith("graph-openai-v1-") || !profile.endpoint.startsWith("POST ")
      || id !== `graph-openai-v1-${createHash("sha256").update(JSON.stringify(declaration)).digest("hex")}`) continue;
    const oldDeclaration = { ...declaration, endpoint: profile.endpoint.slice(5) };
    const oldId = `graph-openai-v1-${createHash("sha256").update(JSON.stringify(oldDeclaration)).digest("hex")}`;
    const old = merged.get(oldId);
    if (old && JSON.stringify(old) === JSON.stringify({ id: oldId, ...oldDeclaration })) {
      merged.delete(oldId);
      retiredInvalid = true;
    }
  }
  for (const profile of profiles) {
    const existing = merged.get(profile.id);
    if (existing && JSON.stringify(existing) !== JSON.stringify(profile)) throw new Error("STATE_GRAPH_AUTOMATIC_AUTHORITY_CHANGED");
    merged.set(profile.id, profile);
  }
  const authority: GraphRecoveryServiceAuthority = { version: 1, profiles: [...merged.values()] };
  const body = JSON.stringify(authority);
  if (authority.profiles.length > 32 || Buffer.byteLength(body) > 65_536) throw new Error("STATE_GRAPH_AUTOMATIC_AUTHORITY_LIMIT");
  if (JSON.stringify(previous) === body) return;
  if (retiredInvalid) {
    if (previousBody === null) throw new Error("STATE_GRAPH_AUTOMATIC_AUTHORITY_CHANGED");
    const beforeimage = join(directory, "graph-automatic-authority.before-invalid-endpoint-v1.json");
    let backup: Awaited<ReturnType<typeof open>> | undefined;
    try {
      backup = await open(beforeimage, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW ?? 0), 0o600);
      await privatePath(beforeimage, false, "beforeimage");
      await backup.writeFile(previousBody, "utf8");
      await backup.sync();
    } catch (error) {
      if (backup) { await backup.close(); backup = undefined; await unlink(beforeimage).catch(() => undefined); }
      if (!(error && typeof error === "object" && "code" in error && error.code === "EEXIST")) throw error;
      await privatePath(beforeimage, false, "beforeimage");
      const existing = await open(beforeimage, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
      try {
        if ((await existing.stat()).size > 65_536 || await existing.readFile("utf8") !== previousBody) throw new Error("STATE_GRAPH_AUTOMATIC_AUTHORITY_CHANGED");
      } finally { await existing.close(); }
    } finally { await backup?.close(); }
  }
  const temporary = join(directory, `.graph-automatic-authority-${randomUUID()}.tmp`);
  const handle = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW ?? 0), 0o600);
  try {
    try {
      await privatePath(temporary, false, "temporary_file");
      await handle.writeFile(body, "utf8");
      await handle.sync();
    } finally { await handle.close(); }
    await privatePath(temporary, false, "temporary_file");
    await privatePath(dirname(path), true, "parent");
    await rename(temporary, path);
  } catch (error) { await unlink(temporary).catch(() => undefined); throw error; }
}
