# Native storage V2 feasibility

The cleaned SQLite copy occupies 15,457,689,600 bytes with no free pages. Its
`state_rows` btree occupies 8,736,157,696 bytes. The two membership btrees
occupy 4,477,435,904 bytes and hold 37,753,781 distinct active pairs. These
figures come from `artifacts/private/agentmemory-growth-cleaned/remaining-size-report.md`;
they are measurements of the cleaned copy, not projections for a new format.

| Stored class | Logical payload bytes in cleaned copy | Physical limitation |
| --- | ---: | --- |
| Graph nodes | 1,073,975,476 | Shared `state_rows` btree; per-scope allocation unavailable |
| Graph edges | 1,042,448,984 | Shared `state_rows` btree; per-scope allocation unavailable |
| Failed delta:4 receipts | 2,977,684,951 | Replay must remain fenced or preserved |
| Completed job staging | 808,749,267 | Recovery and provenance contracts require verification |
| Membership | 37,753,781 pairs | Two btrees allocate 4,477,435,904 bytes |

The node and edge JSON alone totals 2,116,424,460 bytes. Consequently a
2,000,000,000-byte database is impossible while those values remain as raw JSON,
even if all other rows and indexes were absent. This is a conditional floor,
not a lower bound for a losslessly compressed design. Retaining the two current
membership btrees also exceeds that target by itself. A one-to-two GB target
requires a measured change in both value and index representation. No measured
full-database bound yet demonstrates that the target is feasible.

A bounded first/last-key sample of 64 rows per scope (nodes, edges, receipts,
observations) yielded 440,471 raw bytes, 262,541 bytes with per-row zlib level 9,
and 196,985 bytes with per-scope block zlib. The sample is biased toward key
extremes. Block compression assumes shared dictionaries and cannot be treated
as a random-access row estimate. The source details are in the private
`compression-sample.json` and feasibility fixture.

## Measured V2 candidate (2026-10-06)

The complete frozen V1 source measured 15,457,689,600 bytes. Its closed V2
shadow measures 9,095,467,008 bytes, with no WAL or SHM. All 22 decoded table
streams match exactly, including 7,572,838 state rows and 37,753,781 membership
pairs in both directions. Values, receipts, staging, tombstones and configuration
are retained. This proves the size of this dataset; it does not establish a
one-to-two GB target or the size of another user's migration.

V2 stores logical JSON through the versioned AMV2Z-1 row codec (zlib with raw
UTF-8 BLOB fallback), with a 16 MiB decoded value limit. Membership uses an
integer identifier dictionary and two ordered indexes. Old engines reject V2
before mutation. The package requires both `state::sqlite_wal_v2` and
`state::native_storage_v2`, in addition to its transaction and migration flags.

The SQLite migration uses a resumable shadow, a single writer and bounded
encoder workers. Acceptance compares every decoded tuple and simultaneous EOF,
checks frozen-source identity and SHA before/after, validates target indexes,
and runs target `quick_check` plus exhaustive foreign-key checks. Redundant full
source and target `integrity_check` walks have been removed. The native migration
suite passed 32 tests; separate corruption and orphan-FK checks passed two tests.

The large pilot was interrupted by the free-RAM guard after complete parity,
during the previous source integrity walk. Its final readiness used an explicitly
reviewed private custody proof and target quick/FK checks. Its final source SHA
was not repeated. That operational exception is excluded from product code and
must not be described as a generic migration verification path.

Live installation, final active-source migration and service health remain
separate gates. Keep a rollback database and compatible executable until those
checks pass. Full reverse migration of this large shadow has not been measured;
fixtures verify the reverse converter and preservation of later V2 writes.

## Original acceptance contract

Prototype a versioned lossless value codec in the native SQLite adapter. Keep
the logical JSON bytes, `value_bytes`, keys, revisions, versions, positions,
and tombstones exact after decode. Migrate into a separate shadow database;
do not rewrite the active database in place. Old engines must reject the new
storage version. A separate compact bidirectional membership representation
must preserve all active pairs and query ordering. Graph extraction and
semantic providers are not part of this migration.

Before cutover, stream a complete source/target comparison of decoded values,
metadata, receipts and all membership pairs. Exercise `get`, `list`, paging,
graph retrieval order, receipt replay/conflict, exports, updates, interruption
recovery, and SQLite integrity. Measure the closed shadow database, WAL and
required archives together. Accept the size target only from that full
measurement. Preserve the old database and executable as a rollback set until
the new service has passed live checks.
