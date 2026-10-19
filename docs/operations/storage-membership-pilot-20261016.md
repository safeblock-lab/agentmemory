# Packed inverse pilot boundary

Prepared executable: `.codex-task/remaining-plan-20261016/large-candidate/membership-pilot.cjs`. Readonly physical helper: `membership-object-cost.py` alongside it. Neither has been executed. Main must authorize real DB access, exclusive runtime ownership, launch/stop and these exact effects before use. No new Rust, dependencies, toolchain, backup or installation is required.

The separate release passed with exit 0 in session 11609 (7m 18s), emitting `iii-inverse-candidate.exe`, 26,969,088 bytes. The installed FTSv3 binary and pointers remain unchanged. The nine focused native tests passed earlier; these prepared scripts have not been checked by execution.

## Future launch and SDK run

After Main resolves the existing runtime's exclusive ownership and authorizes the service transition, launch the separate candidate with PowerShell `Start-Process -WindowStyle Hidden`, executable `D:\agentmemory\.codex-task\remaining-plan-20261016\large-candidate\iii-inverse-candidate.exe`, arguments `--config maintenance.yaml --no-update-check`, and working directory `D:\agentmemory\.codex-task\storage-priority-20261015\runtime-activation\three-fronts`. Save its PID, launch arguments and redirected stdout/stderr under `large-candidate/pilot`. Only that owned process may later be stopped under Main's service authorization. Reuse the existing maintenance config; its disabled providers, queues, triggers and periodic work stay disabled.

Exact prepared driver command:

```powershell
node D:\agentmemory\.codex-task\remaining-plan-20261016\large-candidate\membership-pilot.cjs --run 300000
```

The driver connects through the existing iii SDK at `ws://127.0.0.1:49159` with a 90-second invocation timeout. It reads graph control, rejects recovery, acquires the existing maintenance and graph leases for the persisted run owner, validates generation/fence, renews at 180 seconds with a 120-second safety floor, and releases on a known clean boundary. Every lease mutation and native apply request is durably persisted before dispatch; acknowledgments are fsynced before clearing pending state. An uncertain native apply is reconciled only with its exact request's native receipt. An uncertain lease mutation stops for Main; no blind retry or changed-fence replay is attempted.

The initial native startup installs inverse tables, views and UDF-dependent triggers even before Begin. Begin, Copy and Verify mutate only through `state::storage`. Each request permits up to 1,000,000 pairs, 4 MiB work and 3 seconds; the native implementation internally pages at most 512 pairs and stores blocks up to 128. There is one receipt per bounded native request, no per-pair artifact. Aggregated metrics record calls, elapsed/max time, processed units, work bytes, durable job cursor, pair totals and revision. Restoring a known native receipt does not double count an already-accounted ordinal. The session time budget and optional `pilot/STOP` file pause between requests; the committed native cursor supports continuation. Cancellation and unexpected/foreign job phases stop for Main.

The driver ends at ready with current exact pair parity and the original secondary index protected. It contains no CUT, Retire, reclaim, installation, provider call or service-stop action. Main receives `pilot/ready-before-cut.json`; Main separately decides any subsequent step. A failed/unknown operation preserves pending state and does not automatically Abort.

## Physical accounting

After exact proof the driver force-renews leases and invokes the prepared Python helper once, asynchronously so SDK traffic remains serviced. It opens only the fixed real DB using SQLite URI `mode=ro`, sets query_only, 8 MiB page cache and mmap disabled, and takes a read snapshot. It streams `dbstat WHERE name=?` for exactly these five objects: `compact_inverse_blocks`, `compact_inverse_config`, `membership_repack_job`, `membership_repack_receipts`, and retained `compact_list_inverse`. There is no global census, EXPLAIN, provider call or data-body logging.

Each object query has a 3-second progress-handler deadline; the total ceiling is 120 seconds. Fetches are at most 512 rows, and Windows process memory is checked against 64 MiB each batch. Process termination has an outer 125-second timeout. Failure to obtain Windows memory accounting or `dbstat` stops with preserved partial output; no package installation or retry is implied. Data-version change across the snapshot also invalidates this observation. Timeout/absence/partial results cannot be accepted as exact cost or savings.

Completed object totals count actual leaf, interior and overflow page allocation, payload and unused bytes. Candidate total includes all four candidate/config/job/receipt objects. DB/WAL/SHM file sizes are recorded separately. Pointer-map pages, global database metadata and shared WAL cannot be attributed precisely to individual objects; the report makes no global net-reduction claim. Main must consider peak physical usage and eventual retirement/reclaim separately. A later source write invalidates native proof; the driver rechecks proof after the diagnostic before saving readiness.

## Legacy recovery before Retire

Keep the candidate executable available. Main may explicitly authorize:

```powershell
node D:\agentmemory\.codex-task\remaining-plan-20261016\large-candidate\membership-pilot.cjs --abort
```

This reacquires the established leases with the same persisted owner and force-renews before native Abort. The original inverse index and captured SQL must still match. Abort atomically turns off packed readers/dualwrites, removes UDF-dependent triggers, restores legacy-safe inverse aliases, invalidates proof and persists terminal job/ordinal/receipt. Blocks remain inert. Only after its known receipt may Main stop the candidate and return to the previous binary. Abort is terminal; do not try to revive the job or repeat migration automatically. After Retire this recovery window is closed, and the driver rejects it without index reconstruction.

Required additional effect perimeter is the real source `C:\Users\harus\.agentmemory\data\state_store.sqlite3` plus WAL/SHM, existing exclusive engine/DB ownership and service launch/stop, SDK control/leases and native Begin/Copy/Verify or explicitly requested Abort, bounded readonly dbstat diagnostic, and local aggregate artifacts under `large-candidate/pilot`. No installed package, pointer, native executable in C, credentials or new monitor needs modification for this pilot.
