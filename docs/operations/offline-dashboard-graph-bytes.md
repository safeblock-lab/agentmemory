# Offline dashboard graph storage correction

Prepared on 2026-10-10 while AgentMemory remains stopped. This records source inspection and edits only. No compilation, tests, rendering, installation, service calls, database access, provider calls or scheduled-task changes were performed by this unit. Installed behavior and the requested real graph size remain unproved.

## Changed paths

- `src/functions/dashboard-operational-metrics.ts`: maps the native `graph_metadata` aggregate supplied by `StateKV.graphStorageMetadata()`. It reports values only for a ready, complete response covering all seven scopes with valid counts, timestamp and coverage; unavailable metadata remains null.
- `src/viewer/index.html`: GB rendering requires `source: "native_scoped_metadata"` and `complete: true`. Missing native coverage has an explicit dependency message, with no collecting state or fallback to state totals. The graph sidebar labels snapshot statistics as historical, matching the existing dashboard card.
- `docs/operations/offline-dashboard-graph-bytes.md`: this source provenance and remaining runtime proof.

The other exclusively assigned candidate files (`dashboard-activity.ts`, `graph-statistics.ts`, `storage.ts`) required no immediate edit. Main owns the documentation index update.

## Source provenance and byte meaning

Exact source inspection found that `adapters/sqlite/storage_diagnostics.rs::page` selects `(scope,key,value_bytes,length(value),value IS NULL)` from all `state_rows` by keyset. It categorizes rows but has no scope filter. The response describes `state_rows` only; native retrieval indexes and SQLite overhead are included only in physical database bytes. `complete` on its final page means that page reached the end, not an atomic snapshot of a whole multi-page scan.

The dashboard previously trusted `StorageMaintenanceState.diagnostics.lastComplete` and added both `durable_graph` and `graph_technical`. This is not a bounded native graph-scoped metadata source. The reported 1.341 GB / 765635 state-row incident is user-provided evidence, not a measurement reproduced in this offline unit. Aggregate rows, database size, WAL size and reusable pages cannot establish graph payload bytes.

`GRAPH_RECORD_SCOPES` in `src/state/schema.ts` defines the durable graph payload scopes:

1. `mem:graph:nodes`
2. `mem:graph:edges`
3. `mem:graph:snapshot`
4. `mem:graph:name-index`
5. `mem:graph:edge-key`
6. `mem:graph:node-degree`
7. `mem:graph:edge-history`

The native `durable_graph` category matches these seven scopes. `graph_technical` catches other `mem:graph:*` scopes, including jobs, checkpoints, provider results and working state. These are not durable graph payload. Other related namespaces, such as graph dispatch, also must not silently enter this measurement.

For the successor contract, **stored durable graph payload bytes** means the exact encoded BLOB byte lengths of live `state_rows.value` entries in the seven scopes, counted once. Logical `value_bytes` measures a different quantity. Decimal GB is bytes / 1,000,000,000. Native retrieval indexes, SQLite indexes/page overhead, WAL, freelist/reusable space and technical job payload need separate measurements; this row must never imply total physical disk attribution to the graph.

`src/functions/graph-statistics.ts` already exposes `provenance: "historical_snapshot"`, `countsAreCurrent: false`, and partial status for snapshot counts. The user-supplied 77674 nodes / 119388 edges remain historical until current authoritative counts are proved. The RAM row still requires `source: "iii.exe"` and reports resident working set; this edit does not substitute the dashboard process.

## Accepted native successor and remaining proof

The earlier source inspection found no graph-scoped byte metadata request in the then-current native storage diagnostics. The accepted successor is now integrated in `.codex-task/bounded-engine/source/engine/src/workers/state/adapters/sqlite/storage_graph_metadata.rs` and the app adapter. The native `state::storage` action `graph_metadata` reads cached metadata for the seven durable graph scopes; `graph_metadata_prepare` performs bounded, resumable cache preparation. `StateKV.graphStorageMetadata()` supplies dashboard reads. The existing authenticated `POST /agentmemory/storage/maintenance` route exposes preparation through its whitelist; no new endpoint or `src/index.ts` registration was added.

The public read returns aggregate `rows`, `logical_bytes`, `stored_bytes`, `scope_count`, `sampled_at`, `source`, `coverage`, `status`, `complete` and `reason`. It covers exactly the seven scopes above; it does not return per-scope counters, revisions or epochs. The native reader checks scope revisions internally. A ready value requires complete seven-scope metadata with nonnegative safe counts and a valid sample timestamp. Missing, incomplete, stale or unsupported metadata remains unavailable with null counts. Dashboard reads neither scan nor hydrate graph records and never prepare the cache implicitly.

The metadata read is source code only until the combined native patch/manifest and optimized engine package are regenerated and installed under their separate release gate. Independent authorized native reconciliation must still compare real per-scope row, logical-byte and stored-byte totals. Runtime proof must also cover empty scopes, partial preparation, stale revisions, graph writes, and exclusion of non-graph and technical graph state.

## Remaining acceptance after explicit reactivation

1. After the memory incident is resolved and reactivation explicitly authorized, regenerate and freeze the combined native patch/manifest and optimized engine package, then compile/install the coherent native/app change under the separate release gate.
2. Read real scoped metadata; independently reconcile exact source provenance and per-scope row/byte totals using authorized bounded native operations. Include empty scopes, partial preparation, stale revisions and unavailable/unsupported metadata.
4. Prove a bounded copied real graph record mutation changes only its relevant stored/logical counters, with restoration confined to the new test session. Do not alter historical knowledge to test accounting.
5. Prove non-graph state growth cannot change graph bytes and graph technical job payload cannot enter durable bytes. Check counter correctness across overwrite/delete/prune and interrupted/resumed preparation.
6. Observe installed dashboard decimal-GB output, explicit unavailable behavior, historical labels, responsive rendering and all requested rows: crystals above; memory emitted, both providers' input cache hits and total, context selection reduction, 24-hour failures, graph size, `iii.exe` RAM and compaction below. No Memory efficiency block.

All these runtime acceptance items remain pending. Existing graph/statistics and RAM implementation were inspected, not executed or revalidated here. No old tests, partial receipts, mocks or source assertions are counted as real functional proof.
