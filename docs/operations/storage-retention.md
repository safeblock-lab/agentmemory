# Storage retention and maintenance

The local storage-maintenance worker samples the native state store and indexes
legacy observation identities in bounded pages. Startup and scheduled ticks do
not delete logical records. Physical page reclamation is a separate bounded
native action and can reuse free pages without a deletion in that same tick.
Cleanup remains inactive until a complete baseline and legacy index pass exist
and an authenticated operator activates it.

The endpoint is `POST /agentmemory/storage/maintenance`. It uses the same
optional bearer authentication as the other REST endpoints: when the existing
`AGENTMEMORY_SECRET` is configured, send its bearer token; no maintenance-only
secret is required. The request body accepts `status`,
`diagnostics`, `activate`, `run`, `reclaim`, or the explicit one-time
`migrate_incremental` operation. `diagnostics` advances one bounded native scan
page; `run` advances one scheduled maintenance tick. The worker also runs a
local-only tick every minute.

```powershell
$maintenanceUrl = 'http://127.0.0.1:3111/agentmemory/storage/maintenance'
$maintenanceHeaders = @{}
if ($env:AGENTMEMORY_SECRET) { $maintenanceHeaders.Authorization = "Bearer $env:AGENTMEMORY_SECRET" }
$statusBody = @{ action = 'status' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri $maintenanceUrl -Headers $maintenanceHeaders `
  -ContentType 'application/json' -Body $statusBody
```

Wait until `state.diagnostics.lastComplete` exists and
`state.observationIndex.complete` is `true`. The index pass preserves existing
observation IDs and only indexes exact identities reconstructible from full
sanitized raw rows. Legacy compressed rows whose original input cannot be
reconstructed are counted as unindexable; they are not aliased or rewritten.
Rows with a reconstructible identity that cannot be indexed remain pending,
and new observations in that session fail closed until indexing succeeds.

Activate cleanup only after reviewing that baseline:

```powershell
$activateBody = @{ action = 'activate' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri $maintenanceUrl -Headers $maintenanceHeaders `
  -ContentType 'application/json' -Body $activateBody
```

The authenticated `run` action performs one bounded tick immediately. Later
ticks continue automatically. `status` reports the current partial category
scan, up to 30 comparable completed samples, deletion counters, the latest
cleanup and reclamation outcomes, and durable compaction scheduling state. That
state records pending work, next eligibility, adaptive page allowance, backoff,
errors, counters, and an overlap marker. It contains aggregate counts only, not
memory payloads. Category scans use a live keyset cursor, not an atomic
snapshot, and cover native `state_rows` only. `diagnostics` and `run` can be
repeated while a cursor is in progress.

Technical records are eligible only when their newest supported timestamp is
at least seven days old and their dependencies are resolved. Unknown or malformed
timestamps are retained. Summary queue units require a completed non-recovery
job, matching canonical publication effect and completed snapshot marker.
Job identities, failed jobs, recovery chains and partial outputs remain retained.
Observation event receipts are retained permanently for deduplication and
lost-acknowledgement recovery. Canonical observation identity mappings remain
while their observations exist. Failed routine-run rows are retained; only
completed routine runs are eligible for the seven-day policy. Automatic raw observation stripping is
disabled. A direct native request requires a separate retained observation with
exactly equal original fields and a matching expected version. That copy cannot
be another deletion candidate in the same batch. An LLM summary or identity
attestation alone does not establish equivalence.

Terminal graph cleanup runs before dependent technical cleanup. Native code
rechecks the terminal job, completed checkpoint, replay receipt, generation,
and lease state, so resolved temporary graph data can be removed on the next
maintenance tick after the seven-day window. The protected graph jobs
`graphjob_muzu6a4k_c7aa10a891eb` and `graphjob_mv0567t0_7c392765f08d`,
their families and referencing records are excluded.

The native technical-pruning allowlist does not include durable or mixed-use
application records such as actions, checkpoints, sentinels, or sketches.
Their obsolescence is not proven by age alone, so they remain outside this
seven-day policy.

Temporal work uses `mem:temporal-graph-work` for bounded manifests and captured
source pages, and `mem:temporal-graph-pending` for its durable recovery index.
The recovery timer admits at most one due job each minute and honors
`AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED`. A pending manifest, page, cursor or
active unit reference is recovery state and must remain available. Completed
manifest candidates older than seven days are selected one projected row at a
time / 64 KiB and sent with their expected native version to technical pruning.
Interrupted source-page and pending-index acknowledgments are repaired through
the same native transaction, with an expected-version reference to the old
completed parent. Each cut probes at most four page identities and one pending
identity; a durable cursor continues the next cut.
Native dependency checks, rather than application prechecks alone, decide
whether deletion is safe, including a pending manifest's unit acknowledgment.

Native terminal graph pruning can delete one eligible row larger than its page
byte budget when no row has yet been deleted in that cut. It reads length
metadata first, then classifies the bounded generated payload for protected
references, reports actual accounted bytes and
yields immediately with `done:false`. When earlier rows already consumed the
cut, it yields before the oversized row without advancing past it. Failed jobs
stopped for recovery remain nonprunable; this progress exception does not widen
eligibility.

Project catalog preparation rotates sessions, summaries and project vectors
through one native cut per minute, with at most 16 rows / 64 KiB per cut. Native
cursors survive restarts. The worker yields to active REST/MCP requests and a
five-second quiet period, coalesces overlapping ticks and backs off failures for
15 minutes. `projectIndexes` in native index status exposes the last per-source
partial/ready response and cursor; it does not attest complete coverage when
native authority is incomplete. Preparation runs outside query and startup
readiness paths and honors the background recovery pause.

Graph REST query/rebuild accept `project` in the body; graph stats accepts it in
the query string. Existing `memory_graph_query` accepts the same project field.
Omitted project produces the existing honest partial graph response. Indexed
lexical/semantic callers can explicitly request bounded global search for
cross-project work; failed or empty project search never silently broadens.

Temporal unit cleanup retains its verified parent identity on the native job
and preserves captured input, provider outputs and delta manifests. Only generated
technical rows are eligible. Once those technical ranges are gone, native
cleanup credits the completed parent's pruned-unit count once per admission
epoch. Manifest expiration requires that count to equal its completed work-unit
count; a missing or ambiguous parent remains protected. Deferred units rotate
through the bounded job scan and are retried on later cycles. Continuous writes
can invalidate pagination and delay later rows; no starvation-free bound is
claimed under continuous churn.

The source candidate retains observation event receipts and identity maps for
exact replay regardless of age. Raw-copy stripping requires a separate indexed
canonical original at its pinned version, with complete raw fields, occurrence
counts, timestamps and provenance coverage. Failed and unknown work, protected
jobs, captures and unclassifiable references remain retained. Dependency and
generated-row decoding is bounded to 128 KiB of both logical and stored bytes;
oversized, corrupt or protected references refuse the transaction atomically.

Maintenance uses an additive singleton lease in the existing native SQLite state
store, with a monotonic fence and a 120-second expiry. Each vacuum page checks and
extends that lease in the same immediate transaction as the page operation.
The internal `maintenance_state_save` action writes only
`mem:storage-maintenance/current` while checking that owner, fence and expiry under
the same lock. Final state publication and lease release commit together; stale
owners cannot overwrite a successor's cursors or release its lease. An unknown
save acknowledgement is not redispatched. Isolated native and shared contract
checks described in earlier releases do not attest this source revision; installed-cycle acceptance remains pending.
Historical build/install metadata does not
attest this newly integrated behavior.

Remaining scale limits include a canonical node/edge provenance record bounded
by the native 16 MiB logical record format, without provenance truncation, and
the separate 4 MiB stored/logical snapshot-cache guard before decoding. Insight
list/search/decay and Fireworks active-job hydration are separate residual paths.
These changes do not establish a general billion-entry throughput guarantee.

Category diagnostics cover native `state_rows`; retrieval-index allocation is
not separately categorized. Native page totals include retrieval indexes and
SQLite overhead. `database_bytes`, WAL bytes, allocated bytes, and reusable
freelist bytes are reported separately. Periodic reclamation starts with one
page and a 25 ms SQLite budget, grows to at most 16 pages when cuts finish
quickly, and waits at least five seconds between cuts. SQLite defers work under
foreground or WAL pressure; an idle check runs once a minute and failures back
off for 15 minutes. Each cut reads native physical counters only; it does not
run a `state_rows` diagnostic scan. Reclamation does not checkpoint or truncate
the WAL.

Incremental vacuum runs only with `auto_vacuum=2`. Mode `0` returns
`migration_required`; mode `1` reports `deferred`. Neither mode is silently
converted. Scheduled reclamation never runs a full `VACUUM`.

Startup recovery for reflect and skill lifecycle is bounded to one page per
queue tick and runs only when `AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED` is not
`1`. Reflect scans at most 16 pending keys per call; skill lifecycle scans at
most eight completed-summary markers. Graph dispatch uses its local-only
bounded recovery path. Configure one subscriber/worker for these queues: the
durable queue does not globally fence duplicate work across multiple configured
workers.

`AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED=1` explicitly permits automatic
storage maintenance and deterministic native project catalog/vector preparation
while provider recovery remains paused. It is off by default; temporal, graph,
reflect and skill recovery keep their pause guards. Installation and actual pending-work proof keep this
flag unset or `0`. Enable it only in the authorized cleanup phase after the
baseline, observation-index completion, durable `cleanupActivatedAt` and initial
compaction. Existing activation checks, foreground priority and bounded reclaim
budgets still apply; derived index preparation keeps its one-cut limits,
foreground priority and backoff. The flag does not activate cleanup or bypass
those checks.

The emitted native package manifest records the compile result and its
capabilities while keeping `deploymentReady: false` and
`verifiedPlatform: null`. Package emission does not claim native runtime
acceptance or platform verification.

The optional initial transition is a blocking database rebuild. Invoke it only
inside a planned maintenance window after pausing producers and reserving free
disk space comparable to the current main database file; SQLite may need a full
temporary copy. It occupies the serialized state worker, can delay queued I/O,
and should run with memory and disk I/O limits appropriate for the host. The
native operation requires `allow_blocking=true` and `max_ms` from 1 through
86,400,000. This budget is cooperative: SQLite's full-file copy and commit can
run beyond it without consulting the progress callback. A client timeout does
not establish cancellation or rollback. Preserve the process and DB/WAL/SHM,
and reconcile the native physical state before considering another migration.
SQLite rolls back an actual `VACUUM` failure atomically; no migration offset is
persisted.

```powershell
$migrationBody = @{ action = 'migrate_incremental'; allow_blocking = $true; max_ms = 3600000 } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri $maintenanceUrl -Headers $maintenanceHeaders `
  -ContentType 'application/json' -Body $migrationBody
```

The example requests a one-hour cooperative budget; reserve a maintenance
window that can accommodate the full rebuild. The operation is never run by startup or periodic maintenance. Once the
transition is complete, ordinary reclamation uses small incremental cuts.

The integrated source candidate's periodic plan uses the one-minute worker and
a durable native SQLite maintenance lease.
The lease is acquired with a unique owner and monotonic fence, refreshed while
maintenance runs, and released only by the matching owner/fence.
An expired owner is recorded as an unknown prior outcome before a new fence is
issued. Admission occurs before reading or writing the maintenance snapshot; a
busy caller may return the current snapshot but never overwrites the active
owner's status or cursor. Each incremental-vacuum page must revalidate the fence under the native
SQLite write lock; stale owners cannot continue a cut. Busy leases defer work,
and ambiguous acknowledgements are retained in history until expiry/recovery
instead of being blindly retried. After the initial measured cleanup and
incremental migration, retain the 25 ms cuts, one-to-sixteen-page range,
five-second cooldown, one-minute idle poll and fifteen-minute failure backoff
above. Keep provider recovery, automatic compression and consolidation paused
independently. Activation is a separate explicit operation; writing this plan
does not enable maintenance.

Maintenance status retains the most recent 30 action outcomes, including the
reason, elapsed time, pages and physical bytes reclaimed, before/after physical
samples, and a sanitized error code where applicable. The last successful
compaction timestamp advances only when the native file-byte counter decreases;
page-count progress alone is not reported as physical reduction. A review is
due when database plus WAL bytes exceed the first completed physical sample
after cleanup activation and a non-pending cleanup pass by 20%, or when reusable
pages remain pending for 24 hours. Category diagnostics cover
`state_rows` only; native retrieval indexes and SQLite overhead contribute to
physical byte totals but are not separately allocated by category.

The shared TypeScript and native lease/state-publication interfaces are integrated.
Isolated tests with independent connections cover contention, expiry, stale fence,
reopen durability, lock-wait clock ordering, atomic state/release rollback and
per-page vacuum exclusion. These checks do not establish an installed effective
cycle or production physical recovery. The frozen source receipts are recorded
in `.codex-task/full-closure/storage-integration/final-handoff.md`; build,
installation and actual cycle acceptance remain separate gates.

Compare database plus WAL bytes at comparable idle points. Review growth when
that total exceeds the post-cleanup baseline by 20%, or when reusable pages
remain pending for 24 hours. Report both the last maintenance attempt and the
last successful reclamation that actually recovered bytes. Neither a smaller
JSON payload nor fewer visible nodes is evidence of physical recovery.

The historical initial measured cleanup removed 7,738 technical rows across 15 reviewed
graph jobs. Database plus WAL fell from 7,225,821,776 to 7,031,827,536 bytes
(193,994,240 bytes; 2.68%), and incremental vacuum mode is now enabled
(`auto_vacuum=2`) in that recorded installation. Its 7,031,827,536-byte reference
is historical; establish a fresh comparable idle sample before the next physical
cleanup acceptance. Maintenance remains inactive until
explicitly enabled.

To request a bounded reclamation pass after activation:

```powershell
$reclaimBody = @{ action = 'reclaim' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri $maintenanceUrl -Headers $maintenanceHeaders `
  -ContentType 'application/json' -Body $reclaimBody
```

## Storage-first source revision (2026-10-15)

This revision is source only. No tests, typechecking, artifact checks, provider
calls, live cleanup, installation, migration or restart were run for it. It does
not establish physical savings or installed behavior. Existing operator
activation is still required; this revision does not set `cleanupActivatedAt`.

`src/functions/storage-retention.ts` defines the category defaults: viewer events
24 hours, tool inputs/outputs seven days, completed job bodies seven days after
terminal closure, and backups 30 days. Durable decisions, preferences, source
observations and useful knowledge have no implicit age TTL. Unknown or future
origins, malformed usage evidence, protection roots, active dependencies,
recovery and replay requirements block collection. Closed jobs without a separate
closure field use their immutable terminal `updatedAt`; automatic maintenance
must not rewrite this timestamp. Native dependency checks remain authoritative.

Renewable eligibility is `min(lastMeaningfulUse + inactivityWindow,
retentionOrigin + maximumAge)`. Renewable defaults use a hard maximum of four times inactivity: viewer 96 hours, tools and completed bodies 28 days. Backups remain fixed at 30 days. These are implementation defaults, not measured savings. Generic access logs,
maintenance, indexing, candidate retrieval and file mtimes are not trusted
meaningful-use signals. No public client-controlled renewal endpoint is added.
Viewer stream/file deletion remains unavailable until its native adapter and
rollback protections establish event ownership; the policy constant alone does
not activate stream expiry. Backups require a usable replacement and protected
rollback baseline. No Git objects, branches, packs or historical snapshots are
removed automatically by this revision; the snapshot export is partial and is
not a usable full-native backup.

Auto-forget and score-based retention eviction default to dry-run and also
require existing cleanup activation to apply an explicit `forgetAfter`. A low
score or old observation alone no longer deletes knowledge. Lexical similarity
no longer supersedes a memory automatically. Explicit memory expiry leaves
shared source observations and replay identities intact. Image refcount release
retains image files and embeddings even at zero: counts do not establish that
version, replay, active-job and rollback roots are absent. Physical image
collection needs a separately authorized reachability operation.

## Compressed snapshots and owned temporaries

New `state.json` Git blobs contain gzip-compressed `agentmemory-records-v1`
records. The name remains stable for existing Git history. Each record stores
an existing scope, key and unchanged JSON value; there is an explicit versioned
header and terminal record count. Supported sections remain sessions, memories,
graph nodes, per-session observations and access logs. Other native state,
budget ledgers and recovery queues are not part of this partial export and are
never reset by restore.

The reader detects gzip magic and accepts historical uncompressed snapshot JSON.
Both formats decode one bounded record at a time, with 128-level JSON nesting,
10 million record and 4 GiB uncompressed-byte ceilings. Individual source values
retain the native snapshot row bound; encoded envelopes allow bounded overhead.
Input is read in 64 KiB chunks; whole-file `JSON.parse` and buffered `git show`
are not used. Capture/export deadlines and snapshot-view cancellation remain in
place. Graph restore uses bounded existing durable graph jobs, preserving the
existing provenance merge path. Restore is additive; it does not delete absent
rows or lower lifetime budget ledgers.

Publication writes an exclusive UUID temporary, closes and syncs it, then
renames it over `state.json`. The exclusive `.state-snapshot.manifest.json`
records version, owner, exact temporary basename, purpose, creation time and
phase. It also serializes create/restore across processes through Git commit or
restore completion. Failed cleanup is reported and leaves a recovery barrier;
a prior or malformed manifest is never silently overwritten. A crash during
manifest update can leave malformed metadata requiring operator investigation.
Restore streams the selected immutable Git blob into its own temporary,
validates the entire input before applying any row, then reopens that same file
for bounded replay. It never checks an old snapshot out into the worktree.
Input validation does not make restore transactional across all scopes: an I/O
failure during application can leave an additive partial restore, with the
original Git blob still available for retry and graph jobs retaining recovery.

`collectClosedSnapshotTemporary(kv, dir, closedOwner)` is a trusted internal
TypeScript interface, not a REST/MCP operation or scheduled task. Before calling
it, an authorized operator must establish that the exact manifest owner has
stopped or been fenced and explicitly authorize this cleanup. The helper also
requires existing cleanup activation, exact manifest ownership and a creation
age of at least 24 hours. It removes only that manifest's UUID temporary and
then the manifest; repeating after interruption is safe. It never removes
`state.json`, legacy `.state-*.tmp` files without this manifest, or Git objects.
A public operator integration would require an authenticated iii-engine/SDK
function, handler registration and the repository's corresponding tool/API
count updates; none is implicitly enabled here. Never clear a manifest for a
writer that might still be running.

Git retention and legacy snapshot-temporary cleanup remain separate authorized
operations with ownership, references, usable backup and rollback review. Byte
counters for logical deletion are not evidence of SQLite/file shrinkage.
Snapshot creation also refuses to replace an existing `state.json` that is
untracked or differs from its Git baseline. A failed commit therefore preserves
that pending publication for explicit recovery instead of overwriting it with
a later capture.

### Trusted use integration boundary

`TrustedRetentionUse` accepts only server-produced task-context-delivery or
confirmed-task-use events with a task identity and observed occurrence time.
`retentionEligibility` applies the sliding inactivity window with the finite
maximum-age cap and rejects invalid/future use timestamps. This typed label is
not authentication: only a trusted server emitter may construct this evidence,
and REST/MCP/client hook payloads must never be promoted into it by assertion.
Current callers provide no meaningful-use event because no trusted emitter is
available in this unit. `retentionUseIntegration.status` in maintenance status
therefore reports `integration_needed`. Without such evidence, origin starts
the inactivity window; unknown use does not invent a renewal.

The capture owner confirms that current hooks and `context_emitted_v1` telemetry
are client-supplied observations. Even canonical storage of an observation does
not authenticate task delivery or usefulness. Required follow-up ownership is
server context delivery/access tracking (M14): emit only after real selected
context delivery or confirmed use, persist event identity and monotonic observed
time through iii-engine outside canonical content versions, and bind it to the
exact resource/retention origin. Collection must read that durable evidence and
recheck it atomically with native root/dependency/version gates. No speculative
candidate, client assertion, maintenance read or index operation may emit it.
That producer-to-native-collection integration is required before claiming
sliding renewal is deployed.

Maximum-age configuration is server environment only, in hours:
`AGENTMEMORY_RETENTION_VIEWER_MAX_AGE_HOURS`,
`AGENTMEMORY_RETENTION_TOOLS_MAX_AGE_HOURS`, and
`AGENTMEMORY_RETENTION_COMPLETED_MAX_AGE_HOURS`. Values must be finite, at least
the category inactivity window, and no more than 8760 hours. Defaults are 96,
672 and 672 hours respectively. Invalid values fail configuration explicitly;
no unlimited retention or silent fallback is accepted.

Remaining U13 integration is required, not optional: the native viewer adapter
must classify individual events by trusted origin, protect active/rollback
streams and perform resumable fenced deletion; legacy whole files need explicit
ownership and rollback closure. Backup rotation needs full-native usable-copy
manifests, protected rollback/current roots, count/byte budgets, and native/Git
reference-aware collection. Git object/pack modification time is insufficient.
The trusted temporary collector also needs an authenticated operator SDK surface
with verified owner closure before public operation. These boundaries prevent
claiming U13 complete or physical storage reduced by this source delivery.

New snapshot operations refuse legacy `.state-*.tmp` files because their owner
may still be active. They do not reclaim those files. Internal temporary
collection serializes same-process callers; its future operator surface must
also hold exclusive maintenance ownership across processes until collection
finishes. Supplying a closed owner without that serialization is insufficient
for public operation. This cross-process operator gate is part of the required
integration, not a guarantee supplied by an exported helper alone.
