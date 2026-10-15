# Preliminary storage identity

Reported by the measurement owner from local file/configuration inspection at 2026-10-13 21:39:39 UTC. Database content measurements are still in progress; these observations are not a completed storage analysis.

Installed configuration: `C:/Users/harus/.agentmemory/iii-config.yaml`; StateModule selects SQLite, `file_path: ./data/state_store.sqlite3`.

Authoritative installed candidate: `C:/Users/harus/.agentmemory/data/state_store.sqlite3`, 7,641,169,920 bytes. Its WAL was 6,880,432 bytes and recently modified. The repository path `D:/agentmemory/data/state_store.db` is a directory and must not be mistaken for this installed SQLite file.

The installed data tree contains 1,108 files totaling 9,039,641,152 bytes. Approximately 1.39 GB are stream-store sidecars; the largest reported viewer stream file is 590,948,412 bytes. Exact category attribution and whether every auxiliary file is required remain pending.

Two target boundaries must be retained: SQLite-only and all required persisted storage. A tenfold target means at most one tenth of the same measured baseline, not a comparison between different boundaries. These file sizes are observations at one time; live WAL changes prevent treating them as an immutable coherent content cut.

Later file inventory from the sole measurement owner: `stream_store` contains 662 files totaling 1,391,161,564 bytes; `queue_store` contains 434 files totaling 382,076 bytes. A separate snapshots perimeter totals 5,228,492,145 bytes: `state.json` 1,674,744,481; two temporary files 1,560,929,329; snapshots Git storage 1,990,079,615; lifecycle-reinstall directory 2,738,720. These aggregates do not establish byte/content identity or safe disposability of archives. No contents of those copies had been read at that checkpoint.

Native schema inventory reports 68 tables and 35 indexes. SQLite page size is 4,096, page count 1,865,673 and freelist count zero at the initial reading. The live database grew about 2.86 MB during a later inventory interval. State scope metadata reports 7,267 scopes; the prefix counts describe naming categories, not byte contributions. Per-table attribution and complete logical measurement remain in progress.
