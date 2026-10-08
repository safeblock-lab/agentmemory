# Bounded graph staging and local recovery

## Incident on 2026-10-08

The 16:35:37 Europe/Madrid `mem::graph-extract` failure was a recovery barrier:
the logger identifies `graphjob_muzn2d3r_9424f796e1a9` with
`STATE_GRAPH_RECOVERY_REQUIRED`. The preceding captured job
`graphjob_muzmgf3d_2d56b87eea0a` failed at 14:33:17.039Z with
`STATE_TX_FAILED`. At 14:45:17Z native metadata reported generation 1,
no active lease or control recovery, and that earlier job still pending with
`recoveryStopped: false`. Its delta 4 checkpoint was staging at chunk 80,512,
version 86,656; its manifest was preparing. This was below the 100,000-chunk
recovery admission ceiling. A pending job independently blocks new graph work,
even when the control recovery field is null.

Existing logs do not retain the transaction's underlying cause. A first bounded
metadata request timed out after five seconds; a later twenty-second request
completed. This does not establish whether the original transaction failure was
a transport timeout, database contention, or another native error.

## Correction

Graph preparation previously wrote an order marker and a full shadow record in
separate transactions for every guarded read, and rewrote the shadow for every
subsequent mutation. Each transaction also persisted a checkpoint and replay
receipt. This amplified large heuristic deltas and retained intermediate values.

Preparation now coalesces repeated reads and mutations in a bounded shadow
buffer, flushing at 128 rows or approximately 512 KiB. Native batches remain
bounded to 256 operations and the existing exact envelope validation. Large
individual records retain the existing single-record handling. Buffered reads
observe preceding writes. Every shadow retains its original live-row version;
freezing flushes all pending shadows before generating application templates.
Only existing fenced native transactions publish changes. On failure the
unpublished in-memory buffer is discarded; captured inputs and durable staging
remain intact and preparing attempts are reconstructed through the existing
recovery protocol. No graph reset or captured-input deletion is involved.

Staging failures now include sanitized cause codes and delta/chunk/visibility
metadata, without payloads or provider error text.

## Recovery without provider calls

Internal callers may trigger `mem::graph-recover` with `{ "localOnly": true }`.
Recovery reuses existing captured values. Only explicitly local frozen-value
creators (options, heuristics and response parsers) may execute when a value is
missing. It pauses before compaction, provider generation or enqueueing that
has not already been captured, returning `providerRequired: true`. This pause
does not consume the staging failure retry allowance. The default background
recovery behavior is unchanged. Do not replace this call with a new extraction:
that would encounter the pending-job barrier and would not resume its capture.

Delivery must install the source correction before invoking local recovery.
Native metadata and operation receipts should distinguish completed recovery
from a pause awaiting explicitly authorized provider work. The correction has
not been benchmarked or tested; the user requested no tests, typechecking or
optional verification. The exact original `STATE_TX_FAILED` cause remains
unresolved unless new sanitized diagnostics identify it.
