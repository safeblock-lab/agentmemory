# Storage runtime activation — 2026-10-15

**State: direct reduction authorized, copy resumed from generation 9237 with the complete list-path correction at 21:18 UTC.** The user's later authorization permits one coherent replay baseline on D; `extreme-case-completion/baseline-receipt.json` records the completed generation-1274 DB/WAL/SHM copy. This supersedes the earlier no-backup condition. The private native runtime loads only StateModule and worker management on port 49159; application, queue, cron, and provider dispatch remain off. The staged isolated application and active pointer are unchanged. Cut, collect, and physical reclaim have not yet occurred.

## Direct reduction checkpoint

The authoritative unit is `storage-direct-reduction`; its authorization and route receipt are in `runtime-activation/direct-reduction/registration-authorized.json`. Fresh preflight recorded DB 9,293,025,280 bytes, WAL 59,637,032 bytes, SHM 131,072 bytes, and 65,769,017,344 bytes free on C. Scheduled tasks remain disabled. Existing MCP client processes cannot connect to the private maintenance port; production ports 49134 and 3111 remain closed.

The native physical read confirmed `auto_vacuum=2`. Ordinary reclaim enforces an unconditional five-second interval and at most 16 pages per call; enabling incremental mode again would do nothing. The narrow candidate correction adds explicit `compact_apply` step `rebuild`, reusing the checkpoint handler outside a SQL transaction. Its request time budget is bounded at 240,000 ms, with a 300,000 ms maintenance lease refreshed immediately before use. SQLite temporary rebuild files are allowed operational workspace, not a backup. Existing ordinary reclaim limits remain unchanged.

The rebuild handler commits durable intent and generation before `VACUUM`, then records the result in a separate transaction after completion and lease validation. Pending intent is not proof of successful completion. An unknown response must be reconciled with the identical request; a pending receipt must never be represented as a completed rebuild or blindly retried under a new operation ID.

Live copy saves each exact pending request before dispatch and appends receipts after success. Conversion compares reconstructed row bytes and vector bytes within the same transaction; mismatch aborts it. Initial 1 MiB requests exposed time and size bounds. Identical-request reconciliation recovered one successful result and confirmed later explicit rejections with unchanged generation/cursor. The 16 MiB request was rejected by the existing admission reserve before mutation. Copy now uses one row, 4 MiB, and at most 3,000 ms per transaction to distinguish cumulative execution cost without relaxing native resource admission. Evidence and preserved requests remain under `runtime-activation/direct-reduction/`.

The one-row diagnostic runner was stopped at generation 1274 after reconciling its exact last request. It is not the production throughput solution: its measured median upper bound was 1.87 rows/second in the provider work-item phase. That rate must not be presented as a measured vector-phase rate. The corrected candidate will use up to 128 rows and 4 MiB per 3,000 ms request, with conservative early partial progress and a measured throughput sample before the remaining volume.

## Generation 1274 coherent replay baseline and successor

The successor `storage-extreme-case-completion` now has a coherent stopped
baseline at `runtime-activation/extreme-case-completion/baseline-gen1274`.
`baseline-receipt.json` records the 2026-10-15 18:40:45–18:42:42 UTC copy:
DB 9,316,118,528 bytes, WAL 59,637,032 bytes, SHM 32,768 bytes, runtime
configuration and the empty canonical-images directory. Source files remained
unchanged with the engine stopped and both scheduled tasks disabled. This is
generation 1274 in the partial `copy` phase, not a pre-migration backup.
Historical snapshots remain at their original path; dependencies/toolchain and
credentials were not duplicated. The baseline is for isolated replay only;
restoring it to production would roll back spending/grants and is prohibited.

The source now implements explicit native `reclaim` with `maintenance: true`,
up to 4096 pages/3000 ms, chunks at most 128 pages, lease/fence and pressure
checks between chunks, yielding, TRUNCATE checkpoint and zero cooldown.
Ordinary reclaim remains 16 pages/25 ms/five seconds. The release emission and
focused SDK/runtime acceptance for this source revision are pending; source
implementation alone is not operational acceptance. Reclaim has no operation
ID: an uncertain ACK requires physical-progress reconciliation, not a claim of
receipt replay idempotence. The direct operator will use incremental reclaim;
the previously prepared full rebuild remains an explicit manual option.

## Membership query repair during the extreme-case run

The first resumed native run reached generation 8124, then paused before cut
or collection. Two `STATE_TX_LIMIT_EXCEEDED` failures at the node transition
were reconciled by replaying each identical request/fence: generation 8123
acknowledged 19 rows and generation 8124 acknowledged one 765-byte row.
Original pending payloads, failures and reconciliation receipts remain in
`runtime-activation/extreme-case-completion`. A bounded next-row diagnosis
found 1,966 logical bytes, 940 stored bytes and 64 source IDs; StateModule read
took 3 ms. No private contents were emitted.

The native SQLite 3.51.3 plan on the immutable D database checkpoint used
`PRIMARY KEY (epoch=?)` for the membership DELETE, whereas the existing index
gave `(epoch=? AND key=?)`. Immutable opening deliberately ignored WAL and
performed only schema/plan reads; it is not a logical generation-1274 read or
a current remaining-row census. The minimum candidate repair adds only the
existing-index hint in compact membership conversion. The real store remains
paused at 8124, with no pending ACK and original inline representations still
retained, while its required release emission and affected fixture complete.
The fixture must demonstrate missing-index startup repair and preservation of
other keys/epochs before the corrected release resumes the same real cursor.

The first copy segment exceeded 25 minutes; its performance is not accepted
merely because it progressed. `phase-metrics.json` preserves actual client
durations by phase and batch-ending scope, including the completed edge segment.
Client wall time reached 6.399 s despite the requested cooperative 3,000 ms
budget. Final reporting must separate active copy time from total elapsed wall
time, pauses and required builds, and must not describe copied logical bytes
as physical savings.

## Set-based membership correction at generation 8133

The exact one-row request at generation 8132 first returned a time-limit error;
one identical replay acknowledged generation 8133 and cursor
`gn_msdcf98h_e43514e3fc80` in 6173 ms client wall time. The row contains 44,430
references and 1,246,083 logical bytes. Its original request, raw error, replay,
and durable receipt remain preserved. Production is not rewound to repeat it.

The former membership binding loop executed about 222,150 SQL statements for
that row. The candidate replaces it with three set-based inserts and two
validations. Native identifier creation retains first-occurrence order;
canonical packed list order and duplicates remain unchanged. Validation checks
for missing identifiers and non-string tokens before inserting secondary data.

The focused isolated native-SQL fixture initially found a quadratic final
validation plan: an epoch-wide mapping scan outside `json_each`. The first four
statements took about 554 ms while the final statement was cancelled at 60 s.
Fixing JSON as the outer join preserves point lookups by identifier and epoch.
The corrected five source statements completed in 495 ms for 44,430 references
plus two duplicate entries, with exact native IDs, invalid/missing detection,
and other-epoch preservation. The fixture transaction rolled back. Its plans
and timings are in `extreme-case-completion/set-based-sql-fixture.log` and
`set-based-sql-before-join-plan.log`. This SQL-only result is not a live
end-to-end throughput measurement. Required release and SDK acceptance remain
pending at this checkpoint.

## Complete list-path correction and replacement

The real run continued safely during source work and compilation. Source review
found about five SQL calls per reference still present across dictionary lookup,
root-list insertion, and repeated membership-list construction. The joint
correction uses indexed set-based dictionary and root-member operations, and
reuses the freshly built canonical list during physical copy only after exact
canonical equality. It retains packed-format, dictionary, epoch, native-ID,
position, and binding checks before removing legacy memberships.

Eight exact source SQL statements covering dictionary, root members, and bindings
completed in 388 ms with 44,430 real references, duplicates, and a special string.
Plans and ID/order assertions are preserved in `complete-list-sql-fixture.log`.
This primitive result is not a claim of real end-to-end improvement.

The first emission failed with `E0061/E0603` because an existing database caller
requires the five-argument `membership::row` API. The correction retains that API
and adds a copy-only prepared-list entry point. The failed log remains intact.
The corrected release exited 0 in 508.24 seconds wall time (Cargo: 8m 25s).
The full Rust SDK fixture then copied the same 44,430-reference value in a
two-row batch in 1299 ms client wall time. Canonical values, versions, ordering,
duplicates, graph/vector results, float32 bytes, exact cut replay, and private
spending remained unchanged through cut and rollback. Its final state was legacy
generation 45 with the lease released. See `sdk-complete-list-outcome.json`.

For replacement, only the old operator client was stopped first. Its pending
`copy-9236` request was replayed with the unchanged fence 2093 while the engine
remained running. The acknowledged generation 9237 and matching native status
were durably recorded before releasing the lease and stopping the engine.
The cached replay's 38 ms client time is not a copy-throughput measurement.
Installation retained 17 capabilities, reused the accepted app, and left the
active pointer unchanged. Private engine 25088 and operator 22544 resumed from
9237 with 128 rows / 4 MiB / cooperative 3000 ms budgets. The next operator
supports an owner stopfile between acknowledged batches and before cut.

## Reusable primitives and periodic callers

The normal native `compact::encode`, `compact::vector`, and membership writers use the compact representation when mode 1 is active. Immutable blob interning and shared identifier lists therefore apply to subsequent normal writes. The corrected piece reconstruction view and prepared identifier/blob statements serve normal reads and writes, rather than identifying or special-casing this database.

The accepted application already schedules `runAutomaticStorageMaintenance` from `src/index.ts`; it calls `mem::storage-maintenance` with `action: tick`. `triggerFencedReclaim` in `src/functions/storage-maintenance.ts` calls the existing native `reclaim` action with its small background budget, owner, and fence. Normal native reclaim retains its pressure, WAL, and cooldown deferrals. Automatic maintenance is paused during this operation by `AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=1` without the maintenance override.

The application's typed `src/state/storage.ts` whitelist currently has reclaim, incremental-mode migration, and maintenance leases, but does not expose `compact_status`, `compact_apply`, or the new explicit rebuild step. The current periodic caller therefore does not yet orchestrate copy, cut, collect, or explicit rebuild. That U13 integration remains with Main; this delivery must not be described as a completed automatic compact-format lifecycle. The direct operator exercises native reusable primitives while providing deployment evidence.

`auto-forget.ts` imports the retention protection and cleanup-authorization helpers from `storage-retention.ts`. No historical TTL cleanup, snapshot/stream cleanup, provider work, or new scheduler is activated by this operation. Explicit full rebuild requires a quiet maintenance window, adequate temporary capacity, and exclusion of application/queue/cron writers; it blocks the state worker and does not promise foreground preemption inside SQLite `VACUUM`.

## Scope and inputs

The only runtime candidates are `native-candidate/artifacts/win32-x64/iii.exe` and `native-candidate/app-dist-isolated`. The non-isolated `app-dist` is not installable. Native delivery and composition evidence is in `native-candidate/source-delivery.json`, `native-candidate/task-deltas/composition.json`, and `native-candidate/task-deltas/runtime-assets.json`. Main accepted the isolated application composition in `runtime-activation/summary-integration/delivery-receipt.json`; it was not launched.

The isolated native engine ran against `runtime-activation/fixture/native-only/data/state_store.sqlite3` on port 49158. The fixture uses the native StateModule configuration and does not load the application or call a provider endpoint. The original failure evidence, rejected attempts, fixture database, and receipts remain preserved.

Earlier focused checks on the same isolated fixture recorded exact JSON/list/Unicode/version preservation, mixed inline writes, fenced activation/copy/cut/replay/rollback, and native snapshot read/cancel parity. The private-spend fixture recorded an `unknown` attempt as consumed once and same-attempt replay as duplicate before and after compaction; its endpoint was never called. These original receipts were retained and were not repeated for the narrow membership-query repair.

## Native repair and focused acceptance

The reproduction failed because SQLite queries forced `INDEXED BY retrieval_membership_key` on physical membership writes and a membership view. That forced hint is not valid across the supported legacy/compact schema states. The repair removes those forced hints from the bounded membership DELETE/UPDATE and view queries; it keeps the existing `(epoch,key)` index and does not add a duplicate expanded index. The canonical source is under `native-candidate/sqlite`, with its build mirror under `native-candidate/build-workspace/engine/src/workers/state/adapters/sqlite`. The emitted patch is `patches/iii-engine/0.22.1-state-pagination.patch`.

The focused membership regression passed (1 test). The offline, locked native release build exited 0 and emitted `native-candidate/artifacts/win32-x64/iii.exe`.

The final fixture completed `source_prepare`, semantic search, GraphRead, graph seeds, and edges before cut, after cut, and after rollback. Main compared the JSON results and confirmed exact parity at all three phases. Float32 bytes remained `cdcccc3dcdcc4cbe00005040000090c0`; semantic score remained 1; graph seeds remained `alpha` and `beta`; the edge remained `edge-ab`. The exact cut replay returned the same receipt, the maintenance lease was released, and the final mode was legacy generation 7. Evidence is in `runtime-activation/fixture/native-only/evidence/index-repair-vector-compaction-final-outcome.json` and `index-repair-compact-operation-final-receipts.json`. The prior failure remains preserved at `graph-search-acceptance-failure.json`.

GraphRead with a query continues to report `graph_membership_partial` with `projection_incomplete=false` and `has_more=false`; this is the same response before, during, and after the fixture migration. It is established query behavior, not evidence that the membership index is unready.

Initial 25 ms fixture transactions were explicitly rejected with `STATE_TX_LIMIT_EXCEEDED` and reconciled without cursor or generation advancement. The accepted continuation kept the row and byte bounds at 16 and 1 MiB and used a 1000 ms transaction budget within the supported limit. Reconciliation evidence is preserved in `index-repair-rejection-reconciliations.json`.

## Fresh read-only preflight

The preflight found `current.json` on v0.9.89 pointing to the previously observed provider-stages package. No installed `iii.exe` process or matching service was present; ports 49134 and 3111 had no listener; and the AgentMemory and watchdog scheduled tasks were disabled. The only engine process found was the isolated fixture on port 49158. The installed StateModule config still selects SQLite at `C:\Users\harus\.agentmemory\data\state_store.sqlite3`.

The live database measured 9,293,025,280 bytes, its WAL 59,637,032 bytes, and its SHM 131,072 bytes. The full `.agentmemory` tree measured 18,091,106,651 bytes. At that preflight C: had 65,937,600,512 bytes free and D: had 136,528,392,192 bytes free. These are pre-change measurements only. No backup overhead or live after measurement exists.

## Authorized live reduction sequence

After Main restores the unit's execution authorization, refresh target identity, current StateModule status, writer/dispatch exclusion, and free capacity. Do not assume live generation or phase from the fixture. Keep the app and watchdog stopped; use the isolated StateModule-only engine and a persistent SDK client so application cron, queue subscribers, and provider endpoints do not run.

Reconcile `compact_status` and any pending intent/receipt first. Continue only from its exact phase and generation under the current maintenance lease/fence. Use durable `compact_apply` requests with fresh IDs for new operations and reconcile unknown acknowledgements by replaying the identical request. Start with bounded cuts of 16 rows, 1 MiB, and 1000 ms; adjust a bound only for a demonstrated row or time limit and remain within the native limits.

Copy must finish both row and vector scans, with exact reconstruction checks and unchanged IDs, versions, positions, ordering, and float32 bytes. Cut is allowed only after both scans complete. Compare focused StateModule results across cut before collecting. The distinct fenced `collect` step then removes unreachable roots/segments, unreferenced blobs, and retained inline rollback copies while keeping data referenced by canonical segments, vector bindings, and graph lists. This deliberately closes the physical legacy rollback window; it does not authorize deleting canonical content, replay records, queue/stream work, or private spending state.

After collection, inspect the StateModule reclamation mode. If incremental vacuum is needed, use the native `enable_incremental` operation only when its full-database temporary-space requirement fits the freshly measured capacity. Then run acknowledged `reclaim` cuts of 1–16 pages with 1–25 ms budgets under the current fence and cooldown. Finish with the authorized bounded WAL checkpoint; defer on busy readers and never kill or unpin them. Measure the database, WAL, queue/stream tree, and total runtime tree after completion. Compare those figures to the preflight values above and report any remaining WAL or receipt overhead.

The application configuration includes cron and queue subscribers. No application was started during acceptance. Preserve queued and unknown work and monotonic private spending throughout any later live operation. No provider endpoint was called during the isolated acceptance.

## Extreme-case continuation and representative profile (2026-10-15 UTC)

The coherent D baseline is the paused generation-1274 state, not a pre-migration image. Its independent diagnostic variant preserves that original baseline and was prepared with a fixture-only cursor for the actual 44,430-reference graph node. That cursor is not evidence of a complete migration and must never be restored into production. Receipts and private fixture material reside under `runtime-activation/extreme-case-completion/`; diagnostic logs contain metadata rather than source contents.

The current private C runtime uses only StateModule and worker-manager, with application, scheduled tasks and providers disabled. Its authorized copy bounds are 128 rows, 4 MiB and a cooperative 3000 ms budget. Client wall time can exceed that budget because connection, admission, checkpoint and commit work lie outside portions of the bounded job. The maintenance reclaim extension separately supports up to 4096 pages and 3000 ms, with chunks of at most 128 pages, fence/pressure/yield checks between chunks, and no explicit five-second maintenance cooldown. Normal reclamation retains its original defaults. Reclaim has no operation ID; uncertain outcomes require physical/progress reconciliation rather than a replay-idempotence claim.

The single diagnostic profile release completed successfully in 12m31s compiler time (approximately 12m51s process wall time). It gates profiling by the exact configured D database path and does not replace the production binary. The complete native 44,430-reference copy rejected at the unchanged three-second deadline inside membership replacement: execute wall_us=3012115, thread_cpu_us=609375, cache_misses=21665, fullscan_steps=0. Pre-WAL wall_us=148, BEGIN wall_us=18, layout/store wall_us=181329 and snapshot/row write wall_us=98831. It did not reach verification or COMMIT. Allocated WAL was 45352 bytes, with 11 frames and zero backfilled frames; autocheckpoint remained 1024 pages. Raw failure, pending and unchanged generation 1274 are preserved; the pending intent was closed as an explicit rejection without commit before the isolated engine was stopped.

A bounded native-library SQL attribution on that same large variant reproduced the exact layout/bind statements and rolled back both passes. The indexed legacy membership DELETE took wall_us=5011957 on the first pass and 536160 on the second; the corresponding whole SQL paths took 5384821 and 920240 microseconds. This identifies indexed deletion and cache-sensitive page access rather than a quadratic join or pre-WAL checkpoint as the dominant measured cost. `retrieval_membership_values` currently uses legacy `UNION ALL` compact membership, so skipping deletion would duplicate results. Native cache size is currently fixed at 64 MiB. No production cache or membership semantics were changed on this evidence alone.

After host interruption, production's exact pending operation at generation 11432 was replayed with its original operation ID, payload and expired fence. It was fenced, and status plus the last durable receipt remained generation 11432. Copy data, generation and receipt share one transaction; unchanged generation proves no commit of that pending request. The original intent and reconciliation were archived without claiming acknowledgement success or changing a private ledger. The same private runtime then resumed with fresh authorized fence 2096. Copy, vectors, cut, collect, physical reclamation and final comparable size acceptance remain separate milestones; logical throughput is not physical storage savings. Total wall duration includes earlier build and recovery pauses and already exceeds the minutes target.
