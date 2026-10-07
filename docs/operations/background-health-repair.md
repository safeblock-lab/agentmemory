# Background health repair

This document records offline repair behavior and the proposed application update for the accepted Windows runtime. Deployment, restart, provider requests and state changes require explicit approval. No live step below has been executed by the offline integration leaf.

## Source behavior

- Semantic request input is bounded to 8 KiB of UTF-8 encoded JSON containing `systemPrompt` and `userPrompt`. Aggregate inputs use evidence extraction and candidate reduction before final consolidation. Provenance is checked before fact writes and checkpoint advancement. Legacy bare fact responses remain compatible with their original selected source cohort; split evidence and candidate reduction require explicit provenance.
- A single summary exceeding that input budget fails locally and retains its source and checkpoint. The repair does not truncate or discard it. The budget covers prompt input, not the provider's complete HTTP envelope or model token window.
- Snapshot maintenance closes admission before draining active callbacks. Admitted local descendants can finish; fresh callbacks wait. Graph callback admission includes its staged delta commit.
- After 30 seconds without quiescence, creation returns `success:false`, `deferred:true`, `retryable:true`, `code:BATCH_MAINTENANCE_BUSY`. The warning includes callback families, counts and ages; no partial snapshot is written. A later timer tick or manual request retries. There is no immediate retry loop.
- `POST /agentmemory/snapshot/create` returns 503 for deferral/busy results, 500 for other returned failures, and 201 for success. Existing overlapping/no-change success behavior remains. Authentication precedes dispatch. The existing exception response remains 404, `Snapshots not enabled`.

## Offline evidence and unresolved gates

Evidence directory: `D:\agentmemory\.native-pagination-build\background-health-20261004\verification`.

The initial focused run passed 62 tests in four files. The integrated REST file subsequently passed nine tests, including a real semantic pipeline callback waiting on a mocked provider while the real snapshot function and REST adapter defer; releasing the provider then persists a fact and checkpoint. TypeScript passed after the REST signature correction. The complete project tsdown configuration passed with every output redirected into `verification/build`; `artifact-manifest.json` records application file hashes and relative import closure. This build leaves the installed package and workspace `dist/` and `plugin/scripts/` intact. Normal `npm run build` also requires a staged patched engine and copies package assets; that packaging step was not run because this update preserves the accepted installed engine and assets.

The initial complete suite had 2684 passes, 15 skips and four failures. Source-owner compatibility corrections fixed the three semantic failures in batch recovery and TypeSafe fixtures. Review also found malformed but enveloped responses could be treated as empty; the source owner fixed that gap and added regression coverage. The final full suite with four workers had 2692 passes, 15 skips and one preexisting failure. The isolated graph oracle test exposes native manifest receipt drift: expected `062b0cff8f0fd9e153178d3d584a5f755f7538bdc8904f46be95ede22078007c`, actual `91f2a23518f82f8bcd4883490a636d125b43528b248011bacf9bae5a94fa5b79`; its preceding native binary hash check passed. Main verified the actual manifest matches tracked Git HEAD and has no task diff. The 0.9.82 candidate resolves this temporal comparison: the old receipt is bound to its unchanged historical source-freeze entry, while the current manifest and patch are checked against the existing high-fanout source freeze and its current binary. No frozen receipt was edited and no graph campaign was rebuilt. The full candidate suite passed with 2694 tests and 15 skips on 5 October 2026.

Final logs: `full-test-accepted-source.log`, `typescript-accepted-source.log`, `build-accepted-source.log`, `diff-check.log`. Earlier logs retain the initial failures and intermediate results, including `rest-integration.log` and `graph-isolated.log`. Source bytes were pinned before the final build; manifest generation rejects source drift. The 36 top-level application files have complete checked static relative MJS imports and pass `node --check`. Main owns acceptance and must rebuild and replace the manifest after any further source change.

For the complete versioned package update, follow [the 0.9.82 release acceptance plan](agentmemory-0.9.82-release.md). The application-only proposal below retains 0.9.81 package metadata and is historical.

## Approved runtime boundary

The accepted baseline is [AgentMemory local runtime](agentmemory-status.md). Installed version: 0.9.81. Installed package: `C:\Program Files\nodejs\node_modules\@agentmemory\agentmemory`. Service home: `C:\Users\harus\.agentmemory`. Database: `data\state_store.sqlite3` with any WAL/SHM companions. Log: `agentmemory-service.log`. Scheduled tasks: `AgentMemory` and `AgentMemory-Watchdog`. API: 3111; viewer: 3113; iii: 49134. Preserve the accepted engine SHA-256 `bd3126c4549b04c0fb31ce8fd9b298e21dcf20c60b40ee8b0b4105ad7bd5a91d`.

Only the compiled application files listed in the refreshed manifest are proposed for replacement. Preserve package metadata/dependencies, engine/manifest, hooks/assets, provider configuration, launcher/watchdog definitions, SQLite state, snapshots, historical jobs, local embedding and reranking assets. No migration, re-embedding, replay campaign or graph rebuild belongs to this update.

## Historical application-only backup and deployment proposal

Run only after Main closes offline acceptance, checks the current live ownership and obtains approval for changes outside the workspace, a controlled stop/start and the bounded live requests. Use an elevated PowerShell session. These commands are a reviewable proposal, not an unattended installer. Stop on any failed precondition. Do not use historical process IDs.

```powershell
$ErrorActionPreference = 'Stop'
$installed = 'C:\Program Files\nodejs\node_modules\@agentmemory\agentmemory'
$serviceHome = 'C:\Users\harus\.agentmemory'
$evidence = 'D:\agentmemory\.native-pagination-build\background-health-20261004\verification'
$application = Join-Path $evidence 'build\dist'
$rollback = Join-Path $serviceHome 'rollback\background-health-20261004'
if (Test-Path -LiteralPath $rollback) { throw 'Use a separately reviewed fresh rollback directory' }
if ((Get-Content -LiteralPath (Join-Path $installed 'package.json') -Raw | ConvertFrom-Json).version -ne '0.9.81') { throw 'Installed baseline changed' }
$manifest = Get-Content -LiteralPath (Join-Path $evidence 'artifact-manifest.json') -Raw | ConvertFrom-Json
$engine = Join-Path $installed 'dist\engine\win32-x64\iii.exe'
$engineManifest = Join-Path $installed 'dist\engine\manifest.json'
if ((Get-FileHash -LiteralPath $engine -Algorithm SHA256).Hash.ToLowerInvariant() -ne 'bd3126c4549b04c0fb31ce8fd9b298e21dcf20c60b40ee8b0b4105ad7bd5a91d') { throw 'Installed engine baseline changed' }
$originalEngineManifestHash = (Get-FileHash -LiteralPath $engineManifest -Algorithm SHA256).Hash
foreach ($file in $manifest.applicationFiles) {
  if ([IO.Path]::GetFileName($file.path) -ne $file.path) { throw 'Unexpected application path' }
  if ((Get-FileHash -LiteralPath (Join-Path $application $file.path) -Algorithm SHA256).Hash.ToLowerInvariant() -ne $file.sha256) { throw 'Staged artifact changed' }
}
function Get-BackupFiles([string]$root) {
  if ((Get-Item -LiteralPath $root -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Backup root is a reparse point' }
  $entries = @(Get-ChildItem -LiteralPath $root -Recurse -Force)
  if ($entries | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint }) { throw 'Backup tree contains a reparse point; review it separately' }
  return @($entries | Where-Object { -not $_.PSIsContainer })
}
function Assert-BackupSpace([long]$copyBytes) {
  $volume = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($rollback))
  $requiredBytes = [long][Math]::Ceiling($copyBytes * 2.0) + 2GB
  if (-not $volume.IsReady -or $volume.AvailableFreeSpace -lt $requiredBytes) { throw "Insufficient rollback volume space: need $requiredBytes bytes, including growth/reserve" }
}
function Confirm-BackupCopy([string]$sourceRoot, [string]$backupRoot, [array]$files, [string]$receiptName) {
  if (@(Get-ChildItem -LiteralPath $backupRoot -Recurse -File -Force).Count -ne $files.Count) { throw 'Backup file count mismatch' }
  $receipt = foreach ($file in $files) {
    $relative = $file.FullName.Substring($sourceRoot.TrimEnd('\').Length + 1)
    $copied = Join-Path $backupRoot $relative
    if (-not (Test-Path -LiteralPath $copied -PathType Leaf)) { throw "Backup path missing: $relative" }
    $sourceHash = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
    if ((Get-Item -LiteralPath $copied -Force).Length -ne $file.Length -or (Get-FileHash -LiteralPath $copied -Algorithm SHA256).Hash -ne $sourceHash) { throw "Backup content mismatch: $relative" }
    [pscustomobject]@{ Path = $relative; Bytes = $file.Length; Sha256 = $sourceHash }
  }
  @($receipt) | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $rollback $receiptName)
}
$packageFiles = @(Get-BackupFiles $installed)
$dataFiles = @(Get-BackupFiles (Join-Path $serviceHome 'data'))
$snapshotFiles = @()
if (Test-Path -LiteralPath (Join-Path $serviceHome 'snapshots')) { $snapshotFiles = @(Get-BackupFiles (Join-Path $serviceHome 'snapshots')) }
$homeFiles = @(Get-ChildItem -LiteralPath $serviceHome -File -Force)
$backupBytes = [long](($packageFiles + $dataFiles + $snapshotFiles + $homeFiles | Measure-Object -Property Length -Sum).Sum)
$applicationBytes = [long](($manifest.applicationFiles | Measure-Object -Property bytes -Sum).Sum)
Assert-BackupSpace ($backupBytes + $applicationBytes)
New-Item -ItemType Directory -Path $rollback | Out-Null
& icacls.exe $rollback /inheritance:r /grant:r "$($env:USERDOMAIN)\$($env:USERNAME):(OI)(CI)F" '*S-1-5-18:(OI)(CI)F' '*S-1-5-32-544:(OI)(CI)F' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Private backup ACL failed' }
$tasks = Get-ScheduledTask -TaskName AgentMemory,AgentMemory-Watchdog
$tasks | Select-Object TaskName,@{Name='Enabled';Expression={$_.Settings.Enabled}} | Export-Clixml (Join-Path $rollback 'task-state.xml')
foreach ($task in $tasks) {
  Export-ScheduledTask -TaskName $task.TaskName | Set-Content -LiteralPath (Join-Path $rollback "$($task.TaskName).xml")
  Disable-ScheduledTask -TaskName $task.TaskName | Out-Null
}
$packageBackup = Join-Path $rollback 'package'
if (Test-Path -LiteralPath $packageBackup) { throw 'Package backup destination must be absent' }
Copy-Item -LiteralPath $installed -Destination $packageBackup -Recurse -Force
# An absent destination receives the source directory contents directly, not another nested package.
if (-not (Test-Path -LiteralPath (Join-Path $packageBackup 'package.json') -PathType Leaf) -or -not (Test-Path -LiteralPath (Join-Path $packageBackup 'dist\cli.mjs') -PathType Leaf)) { throw 'Unexpected package backup directory layout' }
Confirm-BackupCopy $installed $packageBackup $packageFiles 'package-backup-hashes.json'
# Refresh live data sizes after package copying and before stopping the service.
$dataFiles = @(Get-BackupFiles (Join-Path $serviceHome 'data'))
if (Test-Path -LiteralPath (Join-Path $serviceHome 'snapshots')) { $snapshotFiles = @(Get-BackupFiles (Join-Path $serviceHome 'snapshots')) }
$homeFiles = @(Get-ChildItem -LiteralPath $serviceHome -File -Force)
$remainingBytes = [long](($dataFiles + $snapshotFiles + $homeFiles | Measure-Object -Property Length -Sum).Sum) + $applicationBytes
Assert-BackupSpace $remainingBytes
# Confirm the current launcher, CLI and port owners form the accepted single service tree.
$supervisor = @(Get-CimInstance Win32_Process | Where-Object {
  $_.Name -eq 'powershell.exe' -and $_.CommandLine -like '*C:\Users\harus\.agentmemory\Start-AgentMemory.ps1*'
})
if ($supervisor.Count -ne 1) { throw 'Ambiguous supervisor ownership' }
$children = @(Get-CimInstance Win32_Process | Where-Object { $_.ParentProcessId -eq $supervisor[0].ProcessId })
if (-not ($children | Where-Object { $_.CommandLine -like '*\@agentmemory\agentmemory\dist\cli.mjs*' })) { throw 'Launcher does not own expected CLI' }
# Stop only the supervisor first so it cannot respawn the application during maintenance.
Stop-Process -Id $supervisor[0].ProcessId
& 'C:\Program Files\nodejs\node.exe' (Join-Path $installed 'dist\cli.mjs') stop
if ($LASTEXITCODE -ne 0) { throw 'Managed CLI stop failed; preserve files and inspect' }
if (Get-NetTCPConnection -State Listen -LocalPort 3111,3112,3113,49134 -ErrorAction SilentlyContinue) { throw 'Managed ports remain occupied' }
# Copy the database directory only after every owned worker and engine has exited.
$dataRoot = Join-Path $serviceHome 'data'
$dataFiles = @(Get-BackupFiles $dataRoot)
Assert-BackupSpace ([long](($dataFiles + $snapshotFiles + $homeFiles | Measure-Object -Property Length -Sum).Sum) + $applicationBytes)
Copy-Item -LiteralPath $dataRoot -Destination (Join-Path $rollback 'data') -Recurse -Force
Confirm-BackupCopy $dataRoot (Join-Path $rollback 'data') $dataFiles 'data-backup-hashes.json'
New-Item -ItemType Directory -Path (Join-Path $rollback 'home-files') | Out-Null
$homeFiles = @(Get-ChildItem -LiteralPath $serviceHome -File -Force)
$homeFiles | ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination (Join-Path $rollback 'home-files') -Force }
Confirm-BackupCopy $serviceHome (Join-Path $rollback 'home-files') $homeFiles 'home-backup-hashes.json'
if (Test-Path -LiteralPath (Join-Path $serviceHome 'snapshots')) {
  $snapshotRoot = Join-Path $serviceHome 'snapshots'
  $snapshotFiles = @(Get-BackupFiles $snapshotRoot)
  Copy-Item -LiteralPath $snapshotRoot -Destination (Join-Path $rollback 'snapshots') -Recurse -Force
  Confirm-BackupCopy $snapshotRoot (Join-Path $rollback 'snapshots') $snapshotFiles 'snapshot-backup-hashes.json'
}
foreach ($file in $manifest.applicationFiles) {
  Copy-Item -LiteralPath (Join-Path $application $file.path) -Destination (Join-Path $installed "dist\$($file.path)")
  if ((Get-FileHash -LiteralPath (Join-Path $installed "dist\$($file.path)") -Algorithm SHA256).Hash.ToLowerInvariant() -ne $file.sha256) { throw 'Installed artifact mismatch' }
}
if ((Get-FileHash -LiteralPath $engine -Algorithm SHA256).Hash.ToLowerInvariant() -ne 'bd3126c4549b04c0fb31ce8fd9b298e21dcf20c60b40ee8b0b4105ad7bd5a91d') { throw 'Engine changed during application copy' }
if ((Get-FileHash -LiteralPath $engineManifest -Algorithm SHA256).Hash -ne $originalEngineManifestHash) { throw 'Engine manifest changed during application copy' }
Enable-ScheduledTask -TaskName AgentMemory | Out-Null
Start-ScheduledTask -TaskName AgentMemory
```

Space checks require twice the measured remaining copy size plus a 2 GiB reserve on the actual rollback volume, covering the package, live data/WAL/SHM, snapshots, home files and staged application. They run before any backup, again before stopping, and after stopping before the data copy. Package layout, file counts, sizes and SHA-256 hashes must verify before stopping; stopped-state data, configuration and snapshot copies must verify before application replacement. Hash receipts remain inside the private rollback directory. If a space/backup gate fails before stopping and tasks were already disabled, restore their original schedule flags from `task-state.xml` and leave the existing service running. After stopping, preserve its state and backup evidence; do not replace application files until the gate is resolved.

Before copying, additionally compare the installed engine and its manifest with the saved accepted baseline, and record their hashes in the private rollback receipt. Capture the original log length, config file hashes and provider-independent API/status counts. Do not print `.env` values or request credentials into public logs. If an existing supervisor has a different executable or launcher path, Main must supply a freshly reviewed ownership check rather than widening the process match.

Keep the watchdog disabled through initial health validation. Confirm one supervisor/CLI/engine tree, unchanged engine hash and configuration hashes, API health/status, viewer readiness and existing graph/semantic readiness without triggering preparation. Then restore each task's original enabled state from `task-state.xml`; run the watchdog once if originally enabled and verify result 0. Preserve its task XML and `IgnoreNew` policy.

## Proposed bounded acceptance

Approval must cover provider cost and state writes. Use the existing authenticated session mechanism; do not change credentials. Make at most one semantic request (`POST /agentmemory/consolidate-pipeline`, body `{"tier":"semantic"}`) with a reviewed timeout and at most one snapshot request (`POST /agentmemory/snapshot/create`, body `{"message":"Background health repair acceptance"}`). A semantic HTTP 200 alone is insufficient: inspect `results.semantic` for errors, actual work or explicit skip. Snapshot 503 is an honest deferral, not a completed snapshot; observe the next configured timer tick instead of repeatedly invoking it. A transport timeout is ambiguous and does not authorize another provider request.

Compare only the new log suffix. Reject new 413 errors, unexpected snapshot failures, loss of retained sources/checkpoints, config changes, graph generation replacement or concurrent service trees. Check health, viewer and watchdog after the request window. Report a skipped semantic run as unexercised provider behavior. Large-input safety is established offline; do not generate another live campaign.

## Proposed application rollback

If acceptance fails, preserve its evidence. Disable both task schedules, revalidate and stop only the current owned supervisor, and invoke the installed CLI `stop` as above. Confirm all managed ports are free before restoring files. Restore the previous application files from the private package backup; do not restore SQLite state as part of an application rollback, because accepted post-deployment writes must remain.

```powershell
$previousDist = Join-Path $rollback 'package\dist'
foreach ($file in $manifest.applicationFiles) {
  $previous = Join-Path $previousDist $file.path
  if (Test-Path -LiteralPath $previous) {
    Copy-Item -LiteralPath $previous -Destination (Join-Path $installed "dist\$($file.path)")
  }
}
# Old entry files use their retained old chunks; new hashed chunks may remain unreferenced.
# Verify restored cli.mjs/index.mjs hashes against package backup before starting.
$originalTasks = Import-Clixml (Join-Path $rollback 'task-state.xml')
Enable-ScheduledTask -TaskName AgentMemory | Out-Null
Start-ScheduledTask -TaskName AgentMemory
# After health checks, restore original schedule flags, including the watchdog.
foreach ($task in $originalTasks) {
  if ($task.Enabled) { Enable-ScheduledTask -TaskName $task.TaskName | Out-Null }
  else { Disable-ScheduledTask -TaskName $task.TaskName | Out-Null }
}
```

The package backup and stopped-state data image remain available for recovery. A data restore, forced termination, task definition replacement, credential change or rollback-directory deletion requires its own reviewed approval. Never delete the retained native store, legacy data, jobs or graph indexes to repair a background warning.
