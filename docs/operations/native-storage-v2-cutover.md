# Native storage V2 cutover

The final active-source cutover on 6 October 2026 preserves every memory row,
configuration value and historical receipt. The final migrated database is
9,451,446,272 bytes with no WAL, compared with 16,079,679,488 bytes frozen V1
(41.22% smaller). No memory or historical data was pruned.

## Frozen source and installation

Both AgentMemory scheduled tasks and their worker/engine processes were stopped
before a successful SQLite `wal_checkpoint(TRUNCATE)` returned `(0,0,0)`.
The current V1 database was renamed within its data directory to
`state_store.v1-frozen-0.9.81.sqlite3`, preserving the original file without a
copy. It measures 16,079,679,488 bytes. A separate shadow under
`native-v2-shadow/state_store.sqlite3` was migrated by the accepted V2
engine. A 40,000,000,000-byte budget satisfies the source-size reserve; free
space before migration was 99,757,899,776 bytes on C.

The installed package payload now matches every one of the 210 regular files
in the accepted 0.9.82 tarball. Twelve prior payload files were retained for
rollback. Dependencies and configuration retain their existing installation.
The installed engine SHA-256 is
`9f6b3024ea45a3e50e3def81a1006faf0ffcbee8a532e12c1f0de6d0d4da1225`.
Its seven capabilities include `state::sqlite_wal_v2`,
`state::native_storage_v2`, and `state::shadow_migration_v1`.

The migrator exited successfully after exact decoded parity of all 22 tables,
including 7,914,892 state rows and 37,753,781 membership pairs, structural and
foreign-key checks, and ready fences. Bounded point/list/export samples,
membership order, deep pagination and edge hydration matched V1. The closed
shadow was renamed atomically to the active database path.

An observation captured during shutdown left one semantic source dirty.
One local embedding upsert repaired that document through the engine, raising
semantic and lexical counts together from 156,139 to 156,140. After the engine
proved complete coverage, the stale `indexed-corpus` marker was changed only
from `pending` to `ready`. Neither operation rebuilt the corpus or graph.

Both scheduled tasks are enabled. API health reports healthy 0.9.82 with a
closed circuit breaker, viewer HTTP/HTML passes, and the watchdog completed
with exit 0. Consolidation status is enabled and ready; a provider-backed run
was not manually triggered. Failed graph jobs remain recovery-stopped and
receipt metadata is unchanged since the frozen source.

Full live-health acceptance remains pending: real snapshot creation fails with
`STATE_RECORD_TOO_LARGE`, because existing graph nodes exceed the 1 MiB page
budget; the largest measured node is 1,320,550 logical bytes. Export also
fails. Keep the frozen V1 until this contract is repaired and snapshot health
passes. New logs currently contain no HTTP 413, timeout or corruption matches.

## Rollback

Retain the frozen V1 file, the previous compatible engine
`bin/iii-v1-retained-0.9.81.exe`, the previous package payload and the exported
task definitions. Before V2 writes, stop both tasks and workers, restore the
previous engine/package payload and rename the frozen V1 file to its active
path. Never start a V1 engine on V2 data.

After any V2 writes, V2 is the authoritative source. A rollback must stop all
writers, checkpoint V2, run the verified native `state-migrate --reverse`
converter into a fresh V1 shadow and pass full decoded parity before switching.
Fixtures cover preservation of later writes; a full physical reverse of this
large dataset has not been run. Keep enough free space for that reverse target.

Private operational receipts reside under
`artifacts/private/native-storage-v2/live/` and must remain outside Git.
