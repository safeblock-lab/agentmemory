# Native table census and physical amplification

All 66 physical tables were traversed for logical metadata; two virtual FTS tables are attributed through their physical shadow tables without counting the same storage twice. The owner retained 25 completed tables from the first method, discarded incomplete membership counters and completed the remaining 41 with bounded primary-key scalar SQL ranges. Final exact table output is pending integration.

## Read method and consistency

The remaining 41 tables required 6,188 bounded ranges over 136.64 seconds with no retries. An earlier epoch `GROUP BY` selected the reverse membership index and exceeded bounded limits. `NOT INDEXED` did not fix that WITHOUT ROWID plan. A discriminating query plan showed that explicit `INDEXED BY sqlite_autoindex_retrieval_membership_1` selected the actual primary key; the same 8,192-row epoch distribution then completed and returned epoch 2/count 8,192. No incomplete counters are included in final totals.

Native census interval: 2026-10-13 22:34:58 UTC to approximately 22:37:15 UTC. Data version changed 2 to 164; page count 1,870,092 to 1,870,139; freelist 1 to 5; schema version stayed 169. WAL grew from 8,796,232 to 17,151,592 bytes; SHM from 32,768 to 65,536 bytes. Final database file size was 7,660,089,344 bytes. These observations establish live drift, not a coherent source cut. No write, checkpoint or VACUUM was issued by the scanner.

## Major counts

- Memberships: 37,766,686, all epoch 2.
- Name grams: 6,449,436, all epoch 2.
- Vectors: 232,436, all epoch 1. Every embedding BLOB is exactly 1,536 bytes, consistent with 384 float32 values. Total raw embedding bytes: 357,021,696.
- LSH postings: 2,789,232, exactly 12 per vector, all epoch 1.
- Canonical metadata: 2,772 active rows and one inactive row; scope and semantic meaning need exact final table labels.
- Scoped metadata included pending status and coverage states five partial/one ready/one pending. Protection and retirement necessity remain unknown.
- `state_rows`: 1,502,193 rows and 2,458,894,948 actual BLOB bytes at this interval. Column UTF-8/BLOB logical totals: 2,575,151,412, which is not the packed SQLite byte size. Scope catalog: 7,270.

There is no multi-epoch duplication in the major membership/gram/vector/posting roots measured here. This does not prove global embedding uniqueness across different record IDs.

## Vector page diagnosis

A separate bounded page-type read completed in 0.360 seconds with WAL stable at 17,287,552 bytes and its reader then closed.

| Page type | Pages | Allocated bytes | Payload bytes | Unused bytes | Cells |
| --- | ---: | ---: | ---: | ---: | ---: |
| internal | 3,296 | 13,500,416 | 12,739,428 | 408,812 | 26,052 |
| leaf | 26,053 | 106,713,088 | 100,930,089 | 3,923,367 | 206,401 |
| overflow | 232,453 | 952,127,488 | 261,974,327 | 689,223,349 | — |

Total allocation: 1,072,340,992 bytes. The 232,453 total cells match 232,453 overflow pages at this later interval. Combined with 1,536-byte embedding BLOBs and the observed WITHOUT ROWID DDL, this supports one-overflow-page-per-vector packing amplification. It is not a count of obsolete vectors or a deletion opportunity. Changing the native layout may recover this overhead while preserving every float32 bit; final schema/index and migration costs still need a candidate implementation before any savings claim.

## Graph field attribution

`src/types.ts:988` declares `GraphNode.sourceObservationIds`; `GraphEdge` has the same field. Main read both interfaces. A bounded one-node/one-edge schema sample confirmed the stored key `sourceObservationIds` is an array; other node keys include createdAt/id/name/properties/type and edge keys include sourceNodeId/targetNodeId/weight. This sample confirms field presence, not whole-corpus byte attribution. The decoded census's full `<other>` bucket cannot be retroactively relabelled as this field.

Retention eligibility, meaningful-use timestamps, global exact vector sharing and sidecar content accounting remain separate pending evidence. No runtime policy was activated.
