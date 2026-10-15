import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { resolve, join } from "node:path";
import { randomUUID, createHash } from "node:crypto";
import { beforeAll, afterAll, describe, expect, it } from "vitest";

const evidence = resolve(".codex-task/local-reactivation-20261010/backup-efficiency");
const fixture = join(evidence, `fixture-${randomUUID()}`);
const helper = resolve(".codex-task/local-reactivation-20261010/install-preparation/backup-inventory-helper.cs");
let result: Record<string, unknown>;
const quote = (text: string) => `'${text.replaceAll("'", "''")}'`;

describe.skipIf(process.platform !== "win32")("native full recovery inventory", () => {
  beforeAll(() => {
    mkdirSync(fixture, { recursive: true });
    const script = join(fixture, "exercise.ps1");
    writeFileSync(script, `
$ErrorActionPreference = 'Stop'
Add-Type -Path ${quote(helper)}
$root = Join-Path ${quote(fixture)} 'source'
$copy = Join-Path ${quote(fixture)} 'copy'
[void][IO.Directory]::CreateDirectory((Join-Path $root 'nested'))
[IO.File]::WriteAllText((Join-Path $root 'nested/data.txt'), 'original')
[IO.File]::WriteAllText((Join-Path $root 'nested/state_store.sqlite3-shm'), 'retained')
[IO.File]::WriteAllText((Join-Path $root 'state_store.sqlite3-shm'), 'omit')
$exclude = Join-Path $root 'state_store.sqlite3-shm'
$initial = [AgentMemoryRecovery.BackupInventory]::Capture($root, $exclude, $true)
$expectedAcl = (Get-Acl -LiteralPath (Join-Path $root 'nested/data.txt')).Sddl
$aclMatches = ($initial.entries | Where-Object path -eq 'nested\\data.txt').sddl -ceq $expectedAcl
$metadata = [AgentMemoryRecovery.BackupInventory]::Capture($root, $exclude, $false)
[AgentMemoryRecovery.BackupInventory]::AssertUnchanged($initial, $metadata, $false)
$hashes = [AgentMemoryRecovery.BackupInventory]::Hash($root, $exclude)
& robocopy $root $copy /E /COPY:DAT /DCOPY:DAT /XJ /R:0 /W:0 /MT:1 /NP /NFL /NDL /NJH /NJS /XF $exclude | Out-Null
if ($LASTEXITCODE -ge 8) { throw 'Fixture robocopy failed' }
if ((Test-Path -LiteralPath (Join-Path $copy 'state_store.sqlite3-shm')) -or -not (Test-Path -LiteralPath (Join-Path $copy 'nested/state_store.sqlite3-shm'))) { throw 'Robocopy did not omit only the exact transient SHM path' }
$copyHashes = [AgentMemoryRecovery.BackupInventory]::Hash($copy, (Join-Path $copy 'state_store.sqlite3-shm'))
[AgentMemoryRecovery.BackupInventory]::AssertMatchingHashes($hashes, $copyHashes, 'fixture')
$roundtrip = $hashes | ConvertTo-Json | ConvertFrom-Json
[AgentMemoryRecovery.BackupInventory]::AssertMatchingHashes([AgentMemoryRecovery.FileDigest[]]@($roundtrip), $copyHashes, 'manifest roundtrip')
$tokens = $null; $errors = $null
$restoreAst = [Management.Automation.Language.Parser]::ParseFile(${quote(resolve(".codex-task/local-reactivation-20261010/install-preparation/restore-isolated.ps1"))}, [ref]$tokens, [ref]$errors)
foreach ($function in $restoreAst.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -in @('Get-FileTotals', 'Get-Sha256Inventory', 'Assert-MatchingHashes') }, $true)) { . ([ScriptBlock]::Create($function.Extent.Text)) }
Assert-MatchingHashes @($roundtrip) (Get-Sha256Inventory $copy (Join-Path $copy 'state_store.sqlite3-shm')) 'actual restore wrapper'
$wrapperTotals = Get-FileTotals $copy (Join-Path $copy 'state_store.sqlite3-shm')
if ($wrapperTotals.files -ne 2 -or $wrapperTotals.bytes -ne 16) { throw 'Actual restore totals wrapper differs' }
$emptyRoot = Join-Path ${quote(fixture)} 'empty'
[void][IO.Directory]::CreateDirectory($emptyRoot)
$emptyHashes = Get-Sha256Inventory $emptyRoot ''
Assert-MatchingHashes @() $emptyHashes 'empty'
if ($emptyHashes.Count -ne 0) { throw 'Empty digest inventory wrapper differs' }
$manifestPath = Join-Path ${quote(fixture)} 'manifest.json'
@{ schemaVersion = 1; classification = 'full_only'; backupId = 'fixture-backup'; sourceRoots = @(@{ inventory = $initial }, @{ inventory = @{ fileCount = 0; bytes = 0; entries = @() } }); copyVerification = @{ fileCountsAndLengthsMatch = $true; sourceAndBackupHashesMatch = $true; sha256FileCount = 2 }; sha256 = @{ runtime = $hashes; toolchain = @() } } | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $manifestPath -Encoding utf8
$projection = [AgentMemoryRecovery.BackupInventory]::ReadRestoreManifest($manifestPath)
if ($projection.sourceRoots[0].inventory.fileCount -ne 2 -or $projection.sha256.runtime.Length -ne 2 -or $projection.sha256.toolchain.Length -ne 0 -or $projection.sourceRoots[0].inventory.GetType().GetProperty('entries')) { throw 'Minimal manifest projection lost required data or hydrated ACL entries' }
Assert-MatchingHashes $projection.sha256.runtime $copyHashes 'typed manifest'
[IO.File]::WriteAllText($manifestPath, '{}')
$manifestFailure = $false
try { [void][AgentMemoryRecovery.BackupInventory]::ReadRestoreManifest($manifestPath) } catch { $manifestFailure = $_.Exception.ToString().Contains('manifest is incomplete') }
if (-not $manifestFailure) { throw 'Incomplete manifest accepted' }
$changedFile = Join-Path $copy 'nested/data.txt'
$mtime = [IO.File]::GetLastWriteTimeUtc($changedFile)
[IO.File]::WriteAllText($changedFile, 'modified')
[IO.File]::SetLastWriteTimeUtc($changedFile, $mtime)
$hashFailure = $false
try { [AgentMemoryRecovery.BackupInventory]::AssertMatchingHashes($hashes, [AgentMemoryRecovery.BackupInventory]::Hash($copy, (Join-Path $copy 'state_store.sqlite3-shm')), 'corrupt') } catch { $hashFailure = $_.Exception.ToString().Contains('SHA-256 differs') }
[IO.File]::WriteAllText((Join-Path $root 'nested/data.txt'), 'different length')
$mutationFailure = $false
try { [AgentMemoryRecovery.BackupInventory]::AssertUnchanged($initial, [AgentMemoryRecovery.BackupInventory]::Capture($root, $exclude, $true), $true) } catch { $mutationFailure = $_.Exception.ToString().Contains('Source metadata changed') }
$aclBaseline = [AgentMemoryRecovery.BackupInventory]::Capture($root, $exclude, $true)
$acl = Get-Acl -LiteralPath (Join-Path $root 'nested/data.txt')
$acl.SetAccessRuleProtection($true, $false)
$rule = [Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.WindowsIdentity]::GetCurrent().User, 'FullControl', 'Allow')
$acl.AddAccessRule($rule)
Set-Acl -LiteralPath (Join-Path $root 'nested/data.txt') -AclObject $acl
$aclFailure = $false
try { [AgentMemoryRecovery.BackupInventory]::AssertUnchanged($aclBaseline, [AgentMemoryRecovery.BackupInventory]::Capture($root, $exclude, $true), $true) } catch { $aclFailure = $_.Exception.ToString().Contains('Source metadata changed') }
$junction = Join-Path $root 'junction'
New-Item -ItemType Junction -Path $junction -Target $copy | Out-Null
$reparseInventoryFailure = $false
$reparseHashFailure = $false
try { [void][AgentMemoryRecovery.BackupInventory]::Capture($root, $exclude, $true) } catch { $reparseInventoryFailure = $_.Exception.ToString().Contains('Reparse points') }
try { [void][AgentMemoryRecovery.BackupInventory]::Hash($root, $exclude) } catch { $reparseHashFailure = $_.Exception.ToString().Contains('Reparse points') }
Remove-Item -LiteralPath $junction
$missingFailure = $false
try { [void][AgentMemoryRecovery.BackupInventory]::Capture((Join-Path $root 'missing'), '', $true) } catch { $missingFailure = $true }
$parsed = @()
foreach ($name in @('backup-state.ps1', 'restore-isolated.ps1')) {
  $tokens = $null; $errors = $null
  [void][Management.Automation.Language.Parser]::ParseFile((Join-Path ${quote(resolve(".codex-task/local-reactivation-20261010/install-preparation"))} $name), [ref]$tokens, [ref]$errors)
  if ($errors.Count -ne 0) { throw "Parser errors in $name" }
  $parsed += $name
}
@{ files = $initial.fileCount; directories = $initial.directoryCount; bytes = $initial.bytes; sddlPresent = (@($initial.entries | Where-Object { -not $_.sddl }).Count -eq 0); aclMatches = $aclMatches; sha256 = ($hashes | Where-Object path -eq 'nested\\data.txt').sha256; hashFailure = $hashFailure; mutationFailure = $mutationFailure; aclFailure = $aclFailure; reparseInventoryFailure = $reparseInventoryFailure; reparseHashFailure = $reparseHashFailure; missingFailure = $missingFailure; parsed = $parsed; manifestRoundtrip = $true; actualRestoreWrappers = $true; emptyHashes = $emptyHashes.Count; exactShmCopyExclusion = $true; minimalManifestProjection = $true; manifestFailure = $manifestFailure } | ConvertTo-Json -Compress
`, "utf8");
    const output = execFileSync("pwsh", ["-NoProfile", "-NonInteractive", "-File", script], { encoding: "utf8", timeout: 60000 });
    result = JSON.parse(output.trim());
    writeFileSync(join(evidence, "fixture-results.json"), JSON.stringify(result, null, 2));
  }, 65000);

  afterAll(() => rmSync(fixture, { recursive: true, force: true }));
  it("captures every file/directory ACL and omits only the exact SHM", () => {
    expect(result).toMatchObject({ files: 2, directories: 2, bytes: 16, sddlPresent: true, aclMatches: true, exactShmCopyExclusion: true });
  });
  it("hashes bytes correctly and preserves the existing JSON manifest format", () => {
    expect(result.sha256).toBe(createHash("sha256").update("original").digest("hex"));
    expect(result.manifestRoundtrip).toBe(true);
    expect(result.actualRestoreWrappers).toBe(true); expect(result.emptyHashes).toBe(0);
  });
  it("rejects content corruption even when length and timestamp match", () => expect(result.hashFailure).toBe(true));
  it("rejects metadata and ACL changes", () => {
    expect(result.mutationFailure).toBe(true); expect(result.aclFailure).toBe(true);
  });
  it("rejects reparse traversal in both inventory and hash paths", () => {
    expect(result.reparseInventoryFailure).toBe(true); expect(result.reparseHashFailure).toBe(true);
    expect(result.missingFailure).toBe(true);
  });
  it("parses both production PowerShell scripts", () => expect(result.parsed).toEqual(["backup-state.ps1", "restore-isolated.ps1"]));
  it("reads only the required manifest projection and rejects an incomplete manifest", () => {
    expect(result.minimalManifestProjection).toBe(true); expect(result.manifestFailure).toBe(true);
  });
});
