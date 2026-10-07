# Graph job retention

The source candidate's native final `complete` transaction closes a job when its persisted state is
`completed`, `invalidated`, or `recoveryStopped: true`. It removes the five job
staging scopes (inputs, provider results, deltas, prepared payloads and remaps),
the working snapshot and earlier commit receipts for that job generation, in
one bounded page. A continuation drains remaining pages after a large close.
Rows are physically removed, including tombstones. Each transaction removes at
most 256 rows and targets at most 4 MiB of logical payload plus key bytes. A
single row larger than that target is removed alone so cleanup always advances;
the storage codec's 16 MiB decoded value ceiling still applies. Freed database pages become
available for reuse; this does not shrink the database file immediately.

The job record, completed checkpoint and final receipt remain. The job's input
digest and result support durable request replay; the final receipt resolves a
lost acknowledgement, including a generation advance and process restart.
An unseen chunk for a terminal job with a completed checkpoint is rejected
before effects. Older pruned chunks fail with a checkpoint conflict, or a stale
generation error. A completed delta of a resumable job retains all recovery data.
Graph nodes, edges, membership, published snapshot, observations and configuration
are outside this cleanup.

Repeated identical staging writes no longer create another version or receipt.
The native close and cleanup run in the same transaction: an error saving the
final receipt rolls back the publication, checkpoint and cleanup together.

## Existing terminal jobs

Existing live storage has not been cleaned by this source change. The native
`state::graph_prune_terminal` request has `guard`, `job_id`,
`expected_checkpoint_version`, `limit` (1–256), `max_bytes` (1–4,194,304), and
optional `cursor: {scope_index, after_key}`. The checkpoint version is pinned
across pages; the guard must identify a current, unexpired lease even when the
job belongs to an earlier generation. Before each deletion transaction it verifies:

- The persisted job is terminal and its checkpoint is `complete`.
- The control recovery barrier does not identify this job.
- The retained receipt identity is the checkpoint's job, generation, logical
  delta and `next_chunk_ordinal - 1`, with matching checkpoint version.
- The receipt remains available for exact final replay after partial cleanup.

Use indexed key ranges to delete a bounded number of physical staging rows and
older receipts per transaction. Invalidate affected page revisions. Never remove
the job, checkpoint or final receipt. Return a continuation cursor until no
eligible rows remain. A failure or restart can repeat the page without graph
effects; terminal checkpoint validation prevents the staging rows from being
recreated. Reject resumable jobs, incomplete checkpoints and missing final
receipts. Do not infer eligibility from age alone.

Results contain `deleted_count`, `deleted_bytes`, `done`, and `cursor` (null only
when done). Repeat the same cursor after a lost response, or begin again without
one. Physical deletion makes those retries safe. Successful pages advance the
affected scope revisions, invalidating previously issued list cursors.

The application calls bounded terminal maintenance after its existing graph
recovery tick. It acquires a fresh lease, scans one projected page of eight job
IDs, and issues at most one prune page per eligible job. `completed` and
`invalidated` jobs are considered directly. A `recoveryStopped` job is also
eligible only when it has a complete checkpoint; this includes a failed job
that was explicitly stopped. The native operation then verifies the final
receipt and recovery barrier before deleting any staging data. The job row,
checkpoint, final receipt and `recoveryStopped` marker remain unchanged, so the
two protected stopped campaigns keep their metadata and cannot be resumed;
their completed staging data can be reclaimed when the native proof passes.
Incomplete checkpoints and missing receipts remain intact. Maintenance persists one scan cursor in
`mem:config / graph-terminal-retention-page`; a page with remaining work is
revisited on the next tick. Engine restart or stale pagination resets only
that scan cursor. A recovery barrier or busy lease pauses maintenance. Jobs,
checkpoints, final receipts and `recoveryStopped` are never changed by
maintenance. Ineligible legacy jobs remain intact and log the refusal code.

## Historical audit

`state::audit_prune_history` accepts a current `guard`, an explicit frozen
`through_key`, the same `limit`/`max_bytes`, and optional cursor with
`scope_index: 0`. It physically deletes only `mem:audit` keys at or below the
ceiling. Any recovery barrier refuses the operation. It does not enumerate or
delete observations, graph state, jobs, checkpoints, replay receipts, skills or
configuration. The audit producer has been disabled separately; this endpoint
cleans existing historical rows only when explicitly invoked by maintenance.
The timeline's visible limit of 20 has no relationship to observation deletion.

After the healthy candidate is installed, run the bounded private cleanup from
the repository root. The default scope removes historical audit rows:

```powershell
node artifacts/private/native-storage-v2/live/prune-audit-history.mjs --apply
```

It removes at most 64 acknowledged pages, 30 seconds or 64 MiB of logical audit
payload per invocation, whichever limit comes first. An incomplete run exits
with failure and writes a private JSON receipt; continue it with
`--apply --resume <receipt-path>`. Completion requires both the frozen
`aud_~` range and the active audit scope to be empty.

The same runner can explicitly drain existing terminal graph staging:

```powershell
node artifacts/private/native-storage-v2/live/prune-audit-history.mjs --apply --scope graph-work
```

It scans at most 64 pages of eight projected job IDs and issues at most 64
native prune pages, 30 seconds or 64 MiB of logical payload per invocation.
Each page removes at most 256 rows and targets 1 MiB; a single oversized row
may be removed alone under the native 16 MiB value ceiling. Its receipt keeps
the scan cursor and the current job's native continuation cursor, so continue
an incomplete batch with `--apply --scope graph-work --resume <receipt-path>`.
The `terminalStagingComplete` result becomes true only after reaching the end
of the job-ID scope and draining every job that passed native terminal,
checkpoint and final-receipt checks. The receipt counts nonterminal jobs,
incomplete checkpoints and native refusals separately; those records remain
intact and require another run only if their state later becomes safely
eligible. The two protected stopped jobs' rows and `recoveryStopped` markers
are compared before and after each run; their staging is reclaimed only when
the native replay guard passes. A bounded batch that has not reached the end
can be resumed from its receipt. If the scan reached the end but reports an
incomplete checkpoint or native refusal, it exits unsuccessfully and retains
that job's data; after its state is repaired, start a fresh `graph-work` run.

Both modes preserve observations and the published graph. They free SQLite
pages for reuse and do not promise an immediate reduction in the database file
size. They require the managed current pointer to identify healthy 0.9.83; the
runner does not fall back to the older Program Files package.

## Deployment gate

The native candidate advertises `state::terminal_retention_v1`; package and
installer bindings must require it. A missing maintenance endpoint is a failure,
not a successful cleanup. Deployment requires passing the native tests,
rebuilding the native candidate and updating bound manifests/checksums.
These source edits do not install a candidate or clean the live database.

The retained debug tree passed all seven selected terminal-retention tests and
the admission/queue-overload test. The original three-test restart gate first
failed with `database is locked`; owning and joining the SQL worker after
closing its sender fixed the race. No lock bypass or retry hides that failure.
The TypeScript transport, continuation, transaction and staging suites passed
26 tests; `tsc --noEmit` passed. Private evidence is in
`artifacts/private/native-retention/result.json`.

That retained tree is pinned to `e7de3820d1e558f3762edf95e4440552444d48d3`, but
lacks later V2 migration modules present in the canonical patch. Reverse patch
checks pass for the nine changed native files; a full reverse check fails on
the absent/older migration and crash-test files. It is not a deployable build
source. The canonical builder creates the full pinned checkout at
`<AGENTMEMORY_III_BUILD_DIR>/source/e7de3820d1e558f3762edf95e4440552444d48d3`
(default `.iii-engine-build/source/...`). Verify the full patch on that fresh
checkout, apply it, repeat the selected tests there, and build the bound release
before installation or live cleanup.
