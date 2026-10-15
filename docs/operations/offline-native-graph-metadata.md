# Native graph storage metadata

## Measure

`graph_metadata` reports only live `state_rows.value` BLOB lengths in the seven `GRAPH_RECORD_SCOPES`: graph nodes, edges, snapshot, name index, edge key, node degree, and edge history. `stored_bytes` is the sum of encoded BLOB lengths; `logical_bytes` is the separate `value_bytes` sum. SQLite pages, indexes, WAL, reusable space, and graph jobs are excluded from the graph payload figure and require their own physical measurements.

Dashboard reads do not enumerate graph rows or decode graph values. They read seven cached scope counters and compare each cached revision with the current native scope revision. A missing, incomplete, invalid, or stale cache returns null values with an explicit unavailable status. The dashboard never starts preparation and never substitutes aggregate state totals.

## Prepare an existing database

After installing a native engine build that includes this adapter, an operator can make one bounded preparation request through the existing authenticated `POST /agentmemory/storage/maintenance` route:

```json
{
  "action": "graph_metadata_prepare",
  "limit": 256,
  "max_bytes": 262144,
  "max_ms": 3000
}
```

Repeat the request while `status` is `preparing`; stop when it is `complete`. Each request consumes at most 1,024 rows and may inspect one additional row as a lookahead (up to 1,025 row-metadata records), accounts at most 1 MiB of key metadata, enforces the native 3-second execution cap, and rejects keys over 4 KiB. It reads scope, key, logical byte count, stored BLOB length, and nullness only. It does not select, hydrate, or decode graph payloads. Cursor progress is held in a native SQLite metadata table and is not returned to the caller.

`max_bytes` limits accounted metadata for consumed rows: each key plus its scope and a fixed 32-byte allowance. It does not bound or measure BLOB payload bytes or SQLite page I/O, and does not include the one lookahead record. A key over 4 KiB is rejected.

Only one adapter worker executes at a time. The scan captures all seven scope revisions and checks them on each page and before publication. A graph write during preparation invalidates the scan; a later explicit prepare request restarts it. Once ready, normal graph set/delete/tombstone writes update the relevant counters and revision in the same database transaction. If a revision changes without a matching counter update, reads report stale until an operator prepares again.

Preparation changes only this internal measurement cache; it does not alter graph records. The existing audit operation set has no storage-preparation event, and `recordAudit` is currently a compatibility no-op, so this cache preparation does not emit a misleading graph-data audit entry.

## Proof state

The native SQLite adapter now has focused synthetic regression source in `graph_metadata_tests.rs`, registered by `tests.rs`. Its cases cover missing metadata versus a ready empty cache; exact seven-scope live-row totals with separate logical and stored BLOB lengths and exclusion of graph jobs, other state, and tombstones; two-row preparation pages with resume; invalidation by a graph write during preparation; transactional overwrite growth, shrink, delete, and tombstone accounting; and an external revision mismatch. The independent SQL oracle aggregates only `count(*)`, `value_bytes`, and `length(value)` for each of the seven scopes, without selecting or decoding BLOB values.

These are regression-source fixtures only. Under this source unit the engine was not built or installed, the storage-maintenance route was not called, and no tests were compiled or run. The fixtures do not represent user data or installed runtime evidence. Runtime proof must still cover real-data per-scope reconciliation and the dashboard behavior before preparation and after staleness, after the engine is regenerated, installed, and explicitly reactivated.
