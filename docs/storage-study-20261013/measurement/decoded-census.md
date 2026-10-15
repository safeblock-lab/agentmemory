# Complete decoded-value census

Reported by the sole measurement owner after cursor exhaustion and reader closure. The scan traversed the live database in bounded key ranges over 1,469.48 seconds; it is complete temporal traversal, not a coherent immutable cut. It visited 1,501,092 rows across 11,728 batches, compared with 1,499,193 rows in the earlier encoded pass. Results from these different intervals must not be combined as one exact snapshot.

## Totals and resources

- Decoded bytes: 6,377,996,436, exactly matching the declared logical lengths for visited non-null values.
- Stored BLOB bytes: 2,457,958,599.
- Codec errors: zero. JSON errors: zero.
- Null/tombstone values: 49,960. Compressed values: 808,403. Raw values: 642,729.
- Transient SQL retries: three. Maximum read: 3.907 seconds. Observed peak working set: 211,820,544 bytes.

The scanner validates the native V2 marker, zlib EOF, absence of trailing bytes, declared decoded length and UTF-8. It closes cursors/connections on bounded interruptions and retains the unchanged committed cursor and accumulated metrics for retries. CPU decoding/compression runs without a pinned read cursor. Native-table metadata census follows as a separate sequential workload.

## Compression alternatives

Per-record zlib level 6 produced 2,439,065,273 bytes; this does not include all native envelope/layout/reference costs. Framed chunks of at most 1 MiB, partitioned per namespace, produced 1,913,291,116 bytes across 13,022 chunks from 6,389,605,492 framed raw bytes. Against the stored BLOB baseline this is a 22.16% payload reduction, not a tenfold database reduction. Final indexes, references, dictionaries, mutable-update costs and retained rollback files are not included in that candidate.

| Family | Stored bytes | Decoded bytes | Candidate chunk bytes |
| --- | ---: | ---: | ---: |
| graph nodes | 490,920,891 | 1,075,343,157 | 357,462,057 |
| graph edges | 482,667,143 | 1,046,012,275 | 291,086,935 |
| graph dispatch inputs | 180,159,228 | 1,270,861,565 | 170,098,584 |
| graph deltas | 105,440,293 | 246,728,778 | 94,340,255 |
| graph prepared bodies | 92,713,918 | 245,936,589 | 91,215,647 |
| graph remaps | 98,152,037 | 235,029,504 | 91,365,658 |
| graph receipts | 5,055,104 | 28,859,804 | 2,388,895 |

This table shows reported leading families, not the complete family output. Complete scope metadata from the earlier interval counted 7,178 row-bearing namespaces versus 7,267 catalog scopes; 89 were empty at that interval, including audit and 88 graph namespaces.

## Exact and structural repetition

The earlier encoded pass produced complete global SHA256 candidates: 1,291,649 unique fingerprints and 207,544 candidate duplicate rows totaling 92,623,320 stored bytes. Fingerprints alone are not strict byte-equality proof.

The decoded pass uses bounded 32 MiB/100,000-entry LRU caches. Every credited equality is byte-compared. Whole-value cache hits: 152,479, corresponding to 8,568,584 stored bytes and 62,291,778 decoded bytes. These are verified lower bounds, not exhaustive global duplicate totals.

Raw top-level JSON value spans total 4,514,838,853 bytes; byte-confirmed repeated spans total 1,388,850,830 decoded bytes, also a cache lower bound. Spans preserve raw UTF-8 serialization, array/string order and number spelling. No raw content was printed. Whole-value sharing, field sharing and chunk compression overlap; their savings must not be added.

Semantic records contain 476,198,600 raw embedding-field bytes across 232,292 rows and 245,646,403 text-field bytes. The field cache confirmed 24,364,250 duplicate embedding-field bytes; global embedding uniqueness remains unresolved.

Dominant graph fields were aggregated under the scanner's unrecognized-field bucket: nodes 1,057,543,277 bytes, including 380,611,501 repeated bytes; edges 1,026,507,154 bytes, including 813,596,001 repeated bytes. Exact field names require source/schema confirmation. These totals must not be called provenance/source-observation arrays without evidence.

## Limits relevant to the proposal

Full scalar and compression coverage is established for the temporal state-value traversal. Global strict structural duplicate totals, a coherent source cut, native-table logical census, retention necessity and final physical candidate size are separate gates. No data deletion, migration, VACUUM, service restart or configuration change is authorized by these measurements.
