# Captured procedural response repair

This document records offline proposals for captured procedural responses. They do not admit corrections into runtime state or change captured requests, response pages, or completed results.

## Capture and proposed edit

Capture 56 is bound to its completed direct request, manifest, and extraction cursor (page 6, offset 2). Its response is 998 UTF-8 bytes across one page. The private proposal at `.codex-task/panel-restoration-20261017/procedural-repair/proposal.json` contains the exact proposed response and edit. It escapes only the opening and closing angle delimiters of one tag-shaped literal in procedure 1, step 1; the literal's inner characters and all surrounding response bytes stay unchanged. The edit span is UTF-16 offsets 274–284, and the proposed response is 1004 UTF-8 bytes.

The capture's recorded SHA-256 remains unverified; its metadata says checksum verification was not executed. The proposal stores separate response digests to bind the original and proposed text for review and later admission. Those identity digests do not verify the capture checksum. Capture 31 remains excluded because its evidence is ambiguous.

The durable procedural path parses the admitted response through `parseProcedural` and `parseXmlResult`. The XML parser accepts the escaped delimiters and decodes them back into the same step text. Candidate provenance and publication therefore continue through their existing path with the original literal preserved.

## Admission boundary

Each proposal remains pending Main review. Admission requires a new immutable record in the existing `KV.state` scope, bound to the manifest, request, checkpoint, extraction cursor, captured response identity, rule version, rule-specific transformation, and admission metadata. It leaves the original request, captured pages, and completed response untouched. Exact replay is idempotent; stale or conflicting identities are rejected. The admitted response then follows the ordinary parser, provenance, and publication flow.

Provider callbacks cannot authorize or apply a repair, and a callback cannot replace a completed captured response. The current `/agentmemory/consolidate-pipeline` route forwards its request body to `mem::consolidate-pipeline`; there is no repair field or repair branch in that function.

## Prepared internal contract

Source now registers `mem::consolidation-admit-response-repair` during existing consolidation pipeline initialization. It has no HTTP trigger or MCP tool. Source inspection of the REST/MCP dispatchers found fixed destinations; the MCP bridge's variable destination is restricted to its two bridge operations. Engine-level calling authority and Main's review, backup/recovery, and execution approval remain prerequisites outside this payload. Neither provider text nor admission metadata authorizes the operation.

The operation takes a closed rule-specific input. Both variants carry the manifest, request and checkpoint IDs; nonzero expected root and request versions; exact cursor; original and proposed response digests; rule version; and bounded admission metadata. The XML variant also carries its exact edit span and text. The JSON-fence variant has no edit field: its only transformation is removing the exact outer delimiter pair documented above.

- `checkpointId` must equal `requestId`.
- Main supplies current nonzero native StateModule versions; no version is inferred from timestamps or invented from capture evidence.
- `originalResponseSha256` and `proposedResponseSha256` bind the exact original and corrected responses.
- `ruleVersion` is either `literal-step-xml-escape/v1` or `outer-json-fence/v1`; extra or cross-rule fields reject.
- `edit` is required only for the XML variant and contains `responseStartUtf16`, `responseEndUtf16`, one-based `procedureOrdinal` and `stepOrdinal`, and exact `before` and `after` strings.
- `admission` contains bounded nonempty `operationId`, `actor`, `proposalId`, and `reason`, retained as review evidence rather than authorization claims.

The SDK may add its own top-level `_caller_worker_id` transport field. Admission removes only that own field before closed application-input validation, and only when its value is a string of 1–128 characters. Missing metadata leaves the payload unchanged; invalid metadata rejects with `consolidation_response_repair_invalid`. No other fields are stripped or accepted, and the normalized application input alone participates in replay equality and the durable repair record, so the transport caller ID is not retained.

The production contract parametrizes identity and cursor; capture IDs and response text are not embedded in production source. The literal XML variant remains specific to capture 56. Main must establish that its marker is unequivocally literal; the code cannot infer semantic intent. That rule only accepts one unique tag-shaped token without attributes, excluding procedural structure tags, inside one step with no other nested markup. It escapes only that token's two delimiters, requires both response digests, and parses the corrected response through `parseProcedural`. It does not normalize arbitrary XML or retry a provider.

## Outer JSON envelope proposal (capture 74)

The separate offline proposal at `.codex-task/panel-restoration-20261017/json-envelope-repair/proposal.json` is bound to captured request `conrequest_c858e1ed1bd2b747`, manifest `conmanifest_a6485cd5cd6e6333`, and cursor page 10, offset 9. Its captured response is 861 characters and has exactly one outer JSON fence: the opener is three backticks followed by `json` and LF, and the closer is LF followed by three backticks. The proposal removes only those delimiters. It preserves every inner JSON byte and rejects leading/trailing prose or any interior fence.

The ordinary procedural JSON parser now applies the same deterministic envelope rule before parsing. It unwraps only that exact delimiter pair, passes the same inner bytes to `JSON.parse` and the duplicate-key scanner, and keeps the 65,536-byte bound on the original response. Any interior triple-backtick sequence prevents unwrapping, so the strict parser rejects the fenced response. This normalization changes representation only: it does not discard surrounding prose, infer intent, coerce incomplete JSON, or edit parsed fields. The captured response remains unchanged. Historical repair admission still requires the exact outer wrapper and the original and proposed response digests; automatic normalization does not create or admit a repair record.

The unchanged inner object has only a `procedures` property. Its two entries each have exactly `name`, `trigger`, and `steps`; both text fields are strings and each entry has two nonempty string steps. This bounded shape read is not an application-parser acceptance check. Admission and every overlay use the existing strict `parseProceduralStructured` parser with the current cursor's original source-session provenance; the exact prompt check also binds the captured frequency. No empty response is coerced and no field is repaired.

The proposal records the captured V2 output contract and canonical request ID. The internal operation requires the exact already-persisted `conbinding`, a completed direct checkpoint, the original response-page digest, and current nonzero StateModule root and checkpoint versions. Those native versions were not captured offline, so the proposal leaves admission unavailable until Main reads current StateModule state. It changes neither the captured request/response nor the cursor and publishes nothing.

The reduction caller can use `effectiveProceduralJsonResponse(kv, root, systemPrompt, effectiveUserPrompt, outputContract, rawResponse)`. The effective user prompt includes the `Consolidation manifest: …` prefix used in the saved checkpoint. The caller passes the logical V2 contract; the helper resolves the existing bound contract from `conbinding` before deriving the canonical request ID. The proposal carries the capture's recorded response digest without verifying it; only the proposed response identity digest is computed for binding.

Admission holds the existing manifest lock followed by the existing request lock. It requires a completed direct request, validates its deterministic prompt identity and current fragment/cursor association, and parses against exactly the current group's original session IDs, as normal extraction does. Manifest references are bounded and validated before mapping; a group without original sources rejects. It then creates one immutable `KV.state` response-repair record. The record contains the input, checkpoint digest, corrected response, byte lengths, and admission timestamp. The original request and capture pages are never updated. Exact replay of the same admission is idempotent, including after cursor advancement; conflicting admission metadata or edits reject. Initial admission rejects stale native versions or cursor. Consumption also rejects changed root/checkpoint identity before using the correction.

The ordinary procedural extraction path reads an admitted correction only for its exact request and cursor, then continues through the matching strict parser, provenance checks, and publication flow. The JSON helper requires the captured V2 binding and derives request identity from its bound contract; it does not prepare or dispatch a provider call. Admission does not advance the cursor, resume consolidation, or publish memory. Runtime operation remains subject to the existing single-writer deployment model; the process-local locks do not create cross-process CAS or new engine authorization.

The existing `recordAudit(..., "update", ...)` API is called with repair identities and metadata. Its current implementation intentionally performs no write, so this call does **not** establish a persisted audit-log entry. The immutable repair record retains the admission evidence. No audit scope or operation union is added.

## Remaining gates

Main must review the exact private proposals and current identities/versions before any effect, arrange approved recovery/backup/restore, emit and install the prepared source, and explicitly invoke the internal operation through existing engine authority. Source preparation does not establish installation, runtime admission, or successful resumed publication. The native request/root versions have not been established by this worker. The offline proposals are preserved without fabricating them.

No runtime admission, provider call, state-store write, build, test, typecheck, checksum verification, or health probe was performed while preparing this source and proposal.

## Recovery preparation

The files under `.codex-task/panel-restoration-20261017/procedural-repair/recovery-tools/` prepare a Main-gated copy of the active recovery set and a separate isolated file-stage copy. Neither script was run. The existing repair proposal remains unadmitted, and capture 31 remains excluded.

### Active closure and evidence limits

Main accepted the physical Node and active-launcher closure in `.codex-task/panel-restoration-20261017/recovery-runtime-closure-delta/planner-acceptance.md`. The earlier inventory is historical sizing evidence, not current admission. Its live-root candidate had 1,550 files and 14,113,647,200 logical bytes at `2026-10-18T02:40:27Z`; its D: free-space figure cannot establish current capacity. Logical FileInfo lengths do not measure allocated NTFS usage.

The full physical Node root is `C:/Users/harus/AppData/Roaming/nvm/v24.14.0`, with executable `node.exe`. `C:/Program Files/nodejs` is an explicitly admitted directory link to that exact root; the observed link on 2026-10-18 is a `SymbolicLink`, not a junction. The helper accepts either directory-link kind only with that exact single target, validated before and after copying. It copies the physical root, never the alias. The earlier inventory measured 33,032 files and 1,614,624,492 logical bytes without reparse points. The installed application is also retained as a separate role, so file and capacity totals deliberately count that subtree twice. Main must approve that duplication and the fresh totals.

The launcher pointer is `C:/Users/harus/.codex/toolchain/agentmemory/current.json`. The helper requires its exact active packageRoot to remain `C:/Users/harus/.codex/toolchain/agentmemory/versions/0.9.89-storage-realproof-20261010/node_modules/@agentmemory/agentmemory-source-20261016-fts-repack-v3`, repository `safeblock-lab/agentmemory`, version `0.9.89`, and compatible Node metadata. This package's accepted inventory was 45 files and 47,020,265 bytes without its own node_modules. It is copied separately to `runtime/launcher-package`.

The containing version's `node_modules` is copied to its version-specific runtime target, excluding exactly its immediate `@agentmemory` subtree. All 19 known junctions were inside inactive historical packages there. Traversal does not enter that excluded subtree; the active launcher is preserved separately. Other scopes and every sibling dependency remain included. Arbitrary exclusions and unexpected reparse points in included paths or their ancestors reject. The historical parent total of 102,794 files and 9,078,267,299 bytes includes the excluded archives and is not the admitted filtered total. Main must gather fresh filtered metadata using the same exclusion; do not reuse the historical aggregate. Inclusion sources, target mapping, exclusion, physical Node binding, duplicated installed app, directory topology and per-file metadata are retained in backup and stage manifests.

The selected root files and `data`, `config`, `bin`, `.cache`, and `models` remain included. Root logs, `logs`, `snapshots`, temporary material, Git history and standalone SQLite utilities remain excluded. This is the active `full_only` recovery unit, not an archive of all history. Secrets are copied as files without displaying their contents. The pointer and copied configurations retain original paths; neither backup nor stage is a launchable relocated runtime.

### Main-controlled sequence

1. Re-admit exact current process identities and the existing native graceful-stop operations. Historical PIDs are not authorization. Main must preserve the current disabled states of exact tasks `\AgentMemory` and `\AgentMemory-Watchdog`; do not reactivate tasks or fabricate an updater marker. If a real supervisor is active, use its actual updater protocol instead. This is a separate maintenance effect.
2. Under fresh per-effect authorization, stop the app through the existing app graceful-stop helper, then close the native engine through its existing helper. Each requires its own admission; the app stop also requires fresh `runtime/normal-app-start-intent.json`. Main must reconcile pending stop journals and establish exits and writer closure. No force checkpoint, independent SQLite, force-stop or mass-stop substitutes.
3. After native close, Main must observe the complete frozen data tree and bind DB/WAL/SHM presence explicitly. `state_store.sqlite3` is mandatory. WAL and SHM may each be present or absent after a clean close; neither is fabricated, deleted or required unconditionally. Main must establish fresh writer closure independently of source metadata and payload flags.
4. Main measures the exact included source metadata, filtered dependency tree, allocated-space requirement and current destination free capacity; issues a short-lived `quiescent-backup` admission; invokes the backup helper. The helper rechecks pause, listeners/processes, Node alias binding and DB sidecar topology at completion, compares before/after source file metadata and directory topology, and copies the whole frozen data tree. Metadata comparisons do not prove SQLite integrity or discover every writer.
5. After a completed backup, Main may issue a distinct `isolated-restore-stage` and destination-mutation admission, including capacity for a second copy. The stage duplicates the completed backup into a new private directory, compares its admitted role mappings and metadata, and leaves the backup unchanged. It opens no engine, app, provider or database.
6. Native recovery evidence remains a separate Main operation. First identify a supported iii/StateModule operation, then admit an isolated native configuration explicitly redirecting all state, queue and stream paths and all ports. Exclude app and provider execution. Copied production configuration and current.json must never be launched as-is. If no supported native restore/integrity operation is identified, retain that explicit acceptance gap rather than inventing an API.

The existing graceful-stop invocations (Main only) are:

```powershell
pwsh -NoProfile -File '.codex-task/remaining-plan-20261016/six-fronts/combined/normal-app-graceful-stop.ps1' -MainExecute -AdmissionFile '<fresh Main app-stop admission>'
pwsh -NoProfile -File '.codex-task/remaining-plan-20261016/six-fronts/combined/normal-engine-graceful-stop.ps1' -MainExecute -AdmissionFile '<fresh Main engine-stop admission>'
```

After the corresponding separate admission:

```powershell
pwsh -NoProfile -File '.codex-task/panel-restoration-20261017/procedural-repair/recovery-tools/New-QuiescentRecoveryBackup.ps1' -MainExecute -AdmissionFile '<fresh Main backup admission>'
pwsh -NoProfile -File '.codex-task/panel-restoration-20261017/procedural-repair/recovery-tools/Stage-IsolatedRecoveryCopy.ps1' -MainExecute -AdmissionFile '<fresh Main stage admission>'
```

These commands are an ordered runbook, not an instruction to run both copy phases under one admission. Main owns all live effects. Interrupted copies retain their incomplete paths without a completion manifest; Main must reconcile them. Helpers never delete or overwrite an existing destination.

Run each console-based graceful-stop helper in its own `pwsh -NoProfile -File`
process, as shown above. Calling it in the parent PowerShell process detaches
that parent's console and can invalidate handles used by subsequent Git or
Robocopy commands. After establishing the exact stopped processes have exited,
Main archives their pending stop journals before invoking the copy helper.
Admission JSON is parsed with `-DateKind String` so UTC expiry checks do not
depend on PowerShell's automatic date conversion or the host's regional format.

### Exact admission envelope

The JSON is a binding envelope, not authority or cryptographic authentication. Native effect gates and Main's separate current authorization must already have passed. Each phase requires:

| Field | Required value or binding |
| --- | --- |
| `schemaVersion` | `1` (admission envelope; completed manifests use `2`) |
| `attemptId`, `issuer` | `panel-restoration-20261017`, `Main` |
| `phase` | `quiescent-backup` or `isolated-restore-stage` |
| `authorized`, `authorizationId`, `expiresAtUtc` | `true`, nonempty current authorization identity, future UTC expiry |
| `impactClass`, `mutationAuthorizationStatus` | `full_only`, `approved` |
| `operationId` | New identifier matching `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` |
| `capacityApproved`, `allocatedSizeReviewed` | Both `true`, backed by Main's actual review |
| `expectedFileCount`, `expectedFileLengthBytes`, `requiredFreeBytes` | Fresh exact source totals and positive reviewed capacity at least the logical length |
| `sourceRoot`, `destinationRoot` | Exact phase source and new destination described below |
| `entries` | Every required role exactly once, with `role`, `sourcePath`, `targetRelativePath`, `expectedFileCount`, `expectedFileLengthBytes`, `excludedRelativePaths` |

Backup additionally requires `backupAuthorizationStatus: approved`, `dependencyClosureApproved: true`, `maintenanceWindowApproved: true`, exact `sourceRoot: C:/Users/harus/.agentmemory`, and destination `D:/agentmemory/.codex-task/panel-restoration-20261017/procedural-repair/recovery/backup/<operationId>`. Bind `nodeRuntimeAlias: C:/Program Files/nodejs`, `nodeRuntimePhysicalRoot: C:/Users/harus/AppData/Roaming/nvm/v24.14.0`, and `nodeExecutable: C:/Users/harus/AppData/Roaming/nvm/v24.14.0/node.exe`.

Bind fresh `quiescenceProven: true`, `writerClosureProven: true`, nonempty `quiescenceEvidenceId`, `watchdogRuntimeDirectoryConfirmed: true`, nonempty `watchdogRuntimeEvidenceId`, and `runtimeDirectory` within the AgentMemory root. Payload booleans alone do not establish these facts. `closedDataTopology` must contain nonempty `evidenceId`, `databasePresent: true`, and explicit Boolean `walPresent` and `shmPresent` from Main's observation after native close. A changed sidecar state rejects.

Select exactly one pause mode:

- `pauseMode: disabled-tasks-no-supervisor` requires nonempty `disabledTasksEvidenceId` and `noSupervisorEvidenceId`. The helper independently reads exact root-path task names, requiring `State: Disabled` and `Settings.Enabled: false`, absence of a maintenance marker at the root and admitted runtime directory, and no supervisor/launcher candidate. Unreadable shell command lines reject. It repeats these checks before declaring completion. It never changes tasks or markers.
- `pauseMode: updater-marker` retains the actual supervised protocol. An existing `update-maintenance.json` in the confirmed runtime directory must contain `phase: pause`, positive `pid` and `generation`, valid `startedAt`, and a live owner. Main binds `maintenanceJobId`, `maintenanceGeneration` and `maintenanceStartedAtUtc` exactly to that marker. A missing/dead/mismatched marker rejects. The helper never creates, clears or rewrites it.

Backup entries use these exact roles and targets. Root relative sources are under `C:/Users/harus/.agentmemory`; runtime sources are specified separately.

| Role | Source | Target relative to backup |
| --- | --- | --- |
| `state-data` | `data` | `agentmemory/data` |
| `state-config` | `config` | `agentmemory/config` |
| `state-bin` | `bin` | `agentmemory/bin` |
| `state-cache` | `.cache` | `agentmemory/.cache` |
| `state-models` | `models` | `agentmemory/models` |
| `root-env` | `.env` | `agentmemory/.env` |
| `root-auth-token` | `router-auth.token` | `agentmemory/router-auth.token` |
| `root-engine-state` | `engine-state.json` | `agentmemory/engine-state.json` |
| `root-iii-config` | `iii-config.yaml` | `agentmemory/iii-config.yaml` |
| `root-iii-config-candidate` | `iii-config.value-project-direct-syntax-20261014T223533Z.yaml` | `agentmemory/iii-config.value-project-direct-syntax-20261014T223533Z.yaml` |
| `root-preferences` | `preferences.json` | `agentmemory/preferences.json` |
| `root-standalone` | `standalone.json` | `agentmemory/standalone.json` |
| `root-launcher` | `Start-AgentMemory.ps1` | `agentmemory/Start-AgentMemory.ps1` |
| `root-watchdog` | `Invoke-AgentMemoryWatchdog.ps1` | `agentmemory/Invoke-AgentMemoryWatchdog.ps1` |
| `root-hidden-launcher` | `Run-AgentMemoryHidden.vbs` | `agentmemory/Run-AgentMemoryHidden.vbs` |
| `root-hidden-script-launcher` | `Run-AgentMemoryScriptHidden.vbs` | `agentmemory/Run-AgentMemoryScriptHidden.vbs` |
| `current-pointer` | Exact current.json above | `runtime/toolchain/agentmemory/current.json` |
| `launcher-package` | Exact active packageRoot above | `runtime/launcher-package` |
| `source-dependencies` | `C:/Users/harus/.codex/toolchain/agentmemory/versions/0.9.89-storage-realproof-20261010/node_modules` | `runtime/toolchain/agentmemory/versions/0.9.89-storage-realproof-20261010/node_modules` |
| `installed-app` | `C:/Users/harus/AppData/Roaming/nvm/v24.14.0/node_modules/@agentmemory/agentmemory` | `runtime/installed-app` |
| `node-runtime` | Physical NVM root above | `runtime/node` |

Set `excludedRelativePaths: ["@agentmemory"]` only for `source-dependencies`; set `[]` for all other roles. That exclusion's active package is included through `launcher-package`; no omission of launch dependencies is authorized. Each backup entry count/length describes its filtered source. Global counts/lengths are sums of all role snapshots, including the intentional installed-app duplicate.

Stage additionally requires `restoreAuthorizationStatus: approved`, `backupAuthorizationStatus: approved`, and exact `backupOperationId` and `backupAuthorizationId` from the completed schema-2 `backup-manifest.json`. Its source must be that manifest's exact backup destination under `recovery/backup`; its new destination is `recovery/restore/<operationId>`. Stage entries bind each backup role's `sourcePath` to `<backupRoot>/<targetRelativePath>`, retaining its target, exclusion metadata and role count/length. The stage total covers the whole backup tree including `backup-manifest.json`, so it differs from the backup role total. The stage copies it under `<stageRoot>/backup-copy`. The stage manifest carries the original role mappings, exclusion and runtime binding, actual sidecars, and `launchSafe: false`.

### Native recovery proof gap and handoff

The inspected repository and installed iii-sdk / @iii-dev/helpers 0.22.1 evidence has not identified a supported StateModule backup, restore or integrity-check operation. An engine file's metadata does not establish engine or SQLite runtime version. Neither file duplication nor metadata equality proves a StateModule restore or database integrity. No standalone SQLite, SQL checkpoint, `PRAGMA integrity_check`, invented HTTP route or new engine feature is introduced.

The next Main operation is fresh admission of the actual native app-stop and engine-close sequence, followed by the exact post-close topology and filtered active-closure manifest above. Main must separately resolve the supported isolated native recovery proof gap before treating file staging as recoverability evidence. Repair admission and resumed publication retain their existing native identity/version gates.

This source revision performed no tests, typecheck, build, diff, hashes, artifact verification, health probes, live SDK/provider calls, stop/restart, task change, backup, restore or outside-workspace write. The helpers were not executed. Current operational proof, copy completion and native recovery remain Main-owned.
