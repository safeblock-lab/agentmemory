# Complete encoded-value census, preliminary integration

The sole measurement owner completed a bounded temporal scan of `state_rows` and closed its reader. This is complete coverage of the visited key ranges over the scan interval, not an immutable snapshot or complete analysis of all derived tables.

Reported totals: 1,499,193 rows and 2,455,700,608 stored BLOB bytes across 11,713 keyset batches. Largest batch was 24,451,720 bytes; longest read was 1.562 seconds. No oversized rows were reported. Scan duration was 361.6 seconds. OS-observed peak working set was approximately 207 MB; the first internal memory counter was ineffective, so its zero result is not used as evidence. External monitoring supplied the resource observations.

| Prefix family | Stored BLOB bytes | Declared decoded bytes |
| --- | ---: | ---: |
| graph | 1,285,493,192 | 2,903,648,640 |
| semantic | 518,521,389 | 784,589,878 |
| obs | 215,470,725 | 921,554,169 |
| index | 183,653,858 | 242,235,308 |
| graph-dispatch | 181,239,699 | 1,273,034,508 |
| fireworks-batch | 24,545,265 | 154,856,547 |

These are the largest families, not a complete category table. The broad graph prefix still needs subdivision into canonical and recovery/history families.

SHA256 fingerprints identified 207,544 duplicate-row candidates totaling 92,623,320 stored bytes. Strict byte-equality confirmation remains pending. This is not yet an accepted exact-deduplication saving. There were 49,961 tombstones, including 49,951 in summary-queue scopes.

Compression of stored BLOBs in bounded family chunks yielded approximately 2.338 GB versus the 2.456 GB stored baseline. Because V2 already uses per-row zlib, this experiment measures extra redundancy in the stored encoding. It is not a decoded-content compression result. Decoded compression and structural field redundancy measurements remain in progress.

Per-object `dbstat` measured 99 of 101 physical root objects and all 35 indexes. `state_rows` was deferred; `retrieval_vectors` exceeded a 3.5-second subprocess bound and its reader was terminated. The two unmeasured roots remain a combined residual; do not present partial per-object measurements as full physical attribution.

Reported leading physical components:

- Membership table 656,408,576 bytes and reverse-key index 520,069,120 bytes for 37,766,686 members.
- Name-gram table 245,321,728 bytes and key index 222,179,328 bytes.
- Legacy lexical content 376,008,704 bytes and FTS data 133,320,704 bytes.
- Vector postings 156,360,704 bytes and reverse index 138,600,448 bytes.
- State insertion-order index 116,506,624 bytes.
- Viewer rows and two indexes approximately 131,305,472 bytes.

These observations support investigating native index layout and cross-record representation. They do not establish a tenfold reduction or authorize deleting any object.

An adapted bounded `retrieval_vectors` physical read subsequently completed in 1.282 seconds: 261,161 pages, 1,069,715,456 allocated bytes, 374,737,268 payload bytes, 691,844,260 unused bytes, 231,892 cells and maximum payload 1,639 bytes. Schema pages account for 32,768 bytes. `state_rows` remained unmeasured after a watchdog stopped its process at 10.031 seconds; WAL stayed at 8,796,232 bytes. Unused bytes inside allocated B-tree pages are distinct from the zero freelist count.

The physical call briefly overlapped the decoded scanner before the sequential-only clarification reached the owner; both were closed afterwards. The decoded scanner stopped at 54,912 rows/429 batches because of a two-second SQLite VM interruption. Its whole-corpus result was not emitted. No complete decoded/structural census is claimed. The next diagnostic must retain accumulators and cursor on bounded interruptions rather than treating this partial pass as complete.

Subsequent physical arithmetic: the 101 measured roots including `sqlite_schema`, excluding only `state_rows`, total 4,116,987,904 bytes. Against an earlier 7,644,033,024-byte database size, the `state_rows` residual is approximately 3,527,045,120 bytes. This is a temporal estimate; growth alone cannot bound error because live changes can redistribute pages without changing total size.

The adapted decoded pass retains accumulators, closes/reopens connections on bounded SQL interruptions and retries from its unchanged committed cursor. At 716,800 rows/5,600 batches it remained partial, with an observed peak around 177 MB. CPU decoding/compression occurs after the read cursor closes. Raw top-level JSON value spans preserve bytes/order/numeric spelling. Whole-value and field duplicate caches are bounded to 32 MiB/100,000 entries; every credited hit is byte-compared, so their counts are verified lower bounds, not exhaustive global duplicate totals. Full scalar and family totals remain whole-corpus objectives. Duplicate, field-sharing and chunk-compression alternatives overlap and cannot be added.
