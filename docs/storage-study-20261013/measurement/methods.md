# Read-only measurement methods

Reported measurement runtime: Python sqlite3 3.53.1. Active data was opened through SQLite URI `mode=ro`, with `query_only=ON`, 4 MiB cache and mmap disabled. This was diagnostic inspection only; no alternative application persistence implementation was introduced. No provider or live API call, data migration/deletion, service action, build/test, active configuration change, checkpoint, VACUUM or snapshot creation was performed by the owner. All measurement readers and processes closed at completion.

## Coverage and pagination

Canonical state metadata used `SELECT scope,key,length(value) FROM state_rows WHERE (scope,key) > (?,?) ORDER BY scope, key LIMIT 128`, followed by bounded matching-endpoint BLOB fetches with groups at most 24 MiB. Every logical PK range was exhausted. Native tables used 8,192-row primary-key endpoint windows and SQL scalar aggregates inside each range. Type and epoch groups were also range-bounded; integers/reals cast to BLOB use SQLite's textual CAST representation, so these bytes are not packed native record sizes.

For WITHOUT ROWID tables, explicit actual primary indexes obtained from `pragma index_list` prevented the planner from choosing a slow reverse covering index. `NOT INDEXED` alone did not fix that case. Interrupted queries closed/reopened their connections and retried from unchanged committed cursors with bounded 2/4/6-second attempts, retaining accumulators. Incomplete earlier membership counters were excluded, not added to completed-table results.

All 49 decoded families and all 66 physical SQL tables have aggregate CSV coverage. The two virtual FTS tables are represented through physical shadows. All 35 indexes and 101 B-tree roots including `sqlite_schema` were physically measured; `state_rows` alone was not traversed to completion by dbstat and remains a nominal temporal residual. No sample is represented as a whole-corpus count.

## Codec, compression and duplication

V2 marker: hex `89 41 4D 56 32 5A 01`, followed by zlib bytes. Decoding requires complete EOF, no trailing/unconsumed bytes, UTF-8 and decoded length exactly `value_bytes`, at most 16 MiB. Raw fallback also matches the declared length.

Experiments use zlib level 6 per record and namespace-separated chunks with 8-byte big-endian length framing; namespace transitions flush chunks. Records larger than the chunk target form their own chunk, so the 1 MiB target is not an unconditional largest-record guarantee. This corrects earlier shorthand saying every candidate chunk was at most 1 MiB. Whole-value and raw top-level JSON-field proof caches are bounded to 32 MiB/100,000 entries; field spans preserve raw bytes. Cache proof yields lower bounds, distinct from full global fingerprint candidates.

Native vectors use a complete fingerprint map with representative PK/length. Every duplicate fetches representative and current record together in one short snapshot and byte-compares them. Provenance uses canonical compact UTF-8 JSON for ordered string lists, a global fingerprint map and a separate 32 MiB/80,000-entry proof cache. Original IDs and contents were not printed.

Sidecars are read with 1 MiB buffers; full-file fingerprints are followed by complete byte comparisons for candidate matches. Before/after file metadata flags drift. These hashes quantify requested content repetition; no artifact checksum verification was run. Filesystem dates are not event dates or reference-liveness evidence.

## Physical allocation and reserved pages

Per-root dbstat aggregation used `aggregate=1` and an exact root name. OS process watchdogs bounded slow traversals, with WAL growth stop thresholds of 32 MiB additional growth or 64 MiB absolute. The state_rows attempt was stopped at approximately ten seconds rather than holding an unbounded WAL reader. A brief early overlap between two diagnostic workloads was disclosed; subsequent workloads ran sequentially.

With observed `auto_vacuum=2`, 4,096-byte usable pages and page count 1,870,139, the SQLite format implies 2,281 pointer-map pages (9,342,976 bytes) and one 4,096-byte lock page. These are format-derived, not separately read page-type measurements. Five freelist pages add 20,480 bytes. Subtracting those and the earlier measured 4,116,987,904 root bytes from final 7,660,089,344 bytes gives nominal state_rows residual 3,533,733,888 bytes. Sequential measurements allow live page redistribution, so there is no tight coherent-cut error bound.

Official format documentation explains that WITHOUT ROWID uses an index B-tree; with usable size 4,096 its maximum local payload is 1,002 and minimum local payload 489, while a rowid table leaf can retain up to 4,061 bytes locally. This matches the measured 489 local bytes per vector cell and explains overflow amplification. [SQLite file format](https://www.sqlite.org/fileformat.html#b_tree_pages), [pointer maps](https://www.sqlite.org/fileformat.html#pointer_map_or_ptrmap_pages), [lock page](https://www.sqlite.org/fileformat.html#the_lock_byte_page).

## Remaining limitations

These are complete temporal traversals, not one coherent snapshot. Global strict byte equality of all state values/provenance groups, operational TTL eligibility and combined candidate physical size remain separate questions. Capped value-size histograms (1,000 bins plus other) are not complete exact histograms. Data sizes, physical allocation, duplicate bounds and compression alternatives overlap and cannot be blindly added. A future migration must preserve all native tables actually present at its cut, including any subsequently installed private provider-accounting state.
