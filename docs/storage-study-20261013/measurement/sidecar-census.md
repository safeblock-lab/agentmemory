# Complete sidecar and snapshot file census

The sole measurement owner read 1,158 files totaling 6,620,035,785 bytes in 708.61 seconds. Readers closed afterwards; zero changed files were detected by before/after file metadata during this pass. Fixed 1 MiB buffers kept observed working set around 20 MB and peak around 22.5 MB at the reported late checkpoint. This establishes per-file temporal coverage, not one atomic snapshot of the whole storage tree.

| File category | Files | Source bytes | In-memory zlib-6 candidate bytes |
| --- | ---: | ---: | ---: |
| Streams | 662 | 1,391,161,564 | 185,492,792 |
| Snapshot temporary files | 2 | 1,560,929,329 | 280,087,856 |
| Snapshot state.json | 1 | 1,674,744,481 | 458,482,014 |
| Snapshot Git plus lifecycle-reinstall | 59 | 1,992,818,335 | 1,960,359,858 |
| Queue | 434 | 382,076 | 166,530 |

The compression label combined Git and lifecycle-reinstall because of a path-separator classification issue. Earlier exact filesystem metadata separates Git 1,990,079,615 bytes and lifecycle-reinstall 2,738,720 bytes. No full-file rerun was needed to preserve the correct aggregate perimeter. Candidate compressed streams are 7.50 times smaller than source bytes; this is not an installed codec or active-runtime disk reduction.

Only two exact duplicate-file hits totaling 2,093 bytes were found in the Git/miscellaneous group after fingerprint candidates were byte-compared. No file was replaced, deleted or compressed on disk by the pass. Content fingerprints measured requested duplication; they were not artifact checksum verification.

## Filesystem modification-time buckets

- All 1,391,161,564 stream bytes have file modification age greater than one day.
- Stream bytes older than seven days: 225,963,508.
- Stream bytes older than thirty days: 59,155,632.
- Snapshot bytes older than thirty days: only 293,370.
- The two temporary snapshots and `state.json` fall in the one-to-seven-day bucket.
- Queue files include 161,012 bytes with future modification timestamps, explicitly flagged rather than treated as expired.

These are whole-file idle-age buckets. They do not establish embedded event age, Git commit retention, meaningful task use, lack of references, completed-job ownership or rollback dispensability. A 24-hour stream policy could identify the stream files as age candidates, but safe deletion still requires active-adapter and rollback checks. A thirty-day backup rotation cannot be credited with deleting all 5.23 GB of snapshots based on these buckets.

## Perimeter and recovery constraints

The SQLite file is a separate roughly 7.66 GB component. Streams and queues belong to the installed data tree; snapshots are an additional archival perimeter. Do not change between SQLite-only and whole-storage boundaries when computing a reduction factor.

Legacy stream files remain rollback material according to current operational source. The snapshot export format also omits some native operational/recovery state; `state.json` is not a substitute for a complete StateModule backup. Git object liveness cannot be decided from pack-file timestamps. None of these files is declared disposable by this census.
