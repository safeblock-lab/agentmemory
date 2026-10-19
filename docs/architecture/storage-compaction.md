# Native lossless compaction source candidate

This delivery compiled the isolated native release and emitted application bundles
without typechecking. Both compiler/emitter processes returned exit 0; no runtime
acceptance is implied. No tests, installation, migration, database access, provider
call, service action, checksum, health probe or physical measurement was performed.
It establishes neither a reduction ratio nor runtime acceptance. The installed
engine remains a separate artifact.

## Source composition

The owned candidate is `.codex-task/storage-priority-20261015/native-candidate/sqlite`.
Its base is the current canonical native source under
`.iii-engine-build/source/e7de3820d1e558f3762edf95e4440552444d48d3`, documented by
`full-closure/functional-integration/INPUTS.md` and `source-current-handoff.md` as
the combined provider-budget/snapshot source. The later accepted
`graph-preparation-stall-repair/fixed-candidate` scoped/write repair is retained.
This composition is source context, not proof that any source tree equals the
installed binary. No entire candidate was promoted.

The canonical native patch carries this composed SQLite adapter. Existing
non-SQLite patch sections remain, with current native Cargo manifests/lock and
CLI source deltas represented against the original upstream commit. Private
provider-budget tables, pinned grants, counts, claim/consume/finish rules and
snapshot owner-loop behavior remain in the adapter. Compaction does not copy,
refund, recreate or restore those private tables through generic KV snapshots.

## Representation and compatibility

`state_rows` remains the authority for scope/key, version, insertion position,
logical byte length and tombstones. Format 3 adds immutable rowid payloads in
`compact_blobs`, root/ordered segment references, and an exact-ID dictionary.
Fingerprint lookup also compares kind, length and complete payload bytes; hash
collisions therefore produce distinct immutable blobs. No semantic identities
are merged. Whole values and exact prepared-body fragments use the same pool.

`state_values` reconstructs the existing encoded-value interface. New compact
values reconstruct original UTF-8 JSON bytes, including whitespace, escaping,
property order, unknown fields and number spelling. Literal fragments retain
the existing V2 raw/zlib codec. Canonically serialized string arrays in selected
provenance fields use a versioned ordered list of dictionary IDs: three-byte
little-endian IDs below 2^24, eight bytes beyond that domain. Noncanonical arrays
and other shapes retain literal bytes. Duplicate IDs and original ordering
remain in the packed list. Missing references or length mismatches fail closed.
The reconstructed row retains the existing 16 MiB logical-value ceiling.

The inverse is `compact_list_members(identifier,list_id)` plus
`compact_graph_lists(epoch,node_identifier,position,list_id)`. Readers of the
existing graph membership contract use `retrieval_membership_values`, combining
legacy membership and the shared representation. Writers consolidate a node's
membership into its shared list in the same transaction and remove that node's
expanded projection rows. Name/gram/endpoint indexes remain. Ordered canonical
provenance is distinct from the deduplicated membership query contract.

Float32 payloads live outside the composite vector key in the same rowid pool.
Canonical Base64 strings reconstruct from their original little-endian bytes.
`retrieval_vector_values` resolves compact vector references for existing readers.
Retrieval normalizes vectors in the existing implementation; its normalized
bytes remain separate when they differ from the canonical original. Equal raw
bitstreams share one payload. No quantization, dimension change, replacement
model, re-embedding, LSH parameter, FTS tokenizer or ranking change is introduced.

Ordinary KV/transaction, retrieval, paging and snapshot readers use the views;
snapshot backup readers register the same reconstruction functions. The engine's
existing queue, transactions, decode budgets and snapshot backup lifecycle stay
in place. SQL index hints that name physical indexes cannot be applied to views;
the corresponding read hints are removed. Query-plan/performance parity is a
required later acceptance item, not an established fact.

## Internal transport and lifecycle

No new REST endpoint or MCP tool is added. `state::storage` accepts
`{action:"compact_status"}` and `{action:"compact_apply",request:{...}}`.
The TypeScript request/response boundary is `src/state/native-compaction.ts`,
connected through the existing `StateKV.storage` transport.

Apply requests contain `operation_id`, `expected_generation`, `step`, `limit`,
`max_bytes`, `max_ms`, `lease_owner_id`, and `lease_fence`. Limits are 1..128 rows,
1024..16777216 input bytes and 1..3000 ms for logical cuts. Use fresh operation IDs for new cuts;
replay an unknown ACK only with the identical request. Durable receipt/digest
comparison rejects identity reuse with changed bytes. The current maintenance
owner, positive fence and expiry are checked inside the existing write
transaction; the format generation advances atomically with each accepted cut.
An authorization to edit this source does not authorize invoking these actions.

1. `activate` changes default `legacy`/inactive mode to `copy`/active. Later
   canonical writes use compact roots; old inline values remain readable.
2. `copy` advances a durable scope/key cursor over rows, then a separate
   index/epoch/id vector cursor. Admission reads metadata before decoding.
   Each converted value is reconstructed and compared byte-for-byte inside its
   transaction before commit. IDs, versions, positions and scope revisions are
   unchanged. Existing dirty triggers can invalidate derived cursors during
   representation updates; normal bounded preparation must settle them later.
   The physical update does not call `database::write`/`source_changed`, advance
   logical revisions, emit observation events, or dirty semantic source items.
   Crystal/viewer dirty work is local projection work. Migration is never
   authority to extract or embed again. Keep provider dispatch held during the
   separately authorized maintenance window; this source does not claim a
   provider-wide pause mechanism or change private spend/grant authority.
3. `cut` requires both row and vector scans complete. Inline imports during active
   mode invalidate completion and return the format to `copy`.
4. `collect` is a distinct fenced step allowed after cut, or after completed
   rollback. It removes unreachable roots/segments and unreferenced blobs in
   bounded cuts, retaining blobs used by canonical segments, vector bindings or
   graph lists. It can release retained inline rollback copies. SQLite readers
   retain their transaction snapshot through ordinary SQLite/WAL semantics.
   Logical disposal does not imply file-size reclamation.
5. `rollback` disables compact writers, then reconstructs every current value
   and vector into the V2 representation in bounded durable cuts. Shared graph
   memberships are expanded back into the legacy table. Complete rollback is
   required before running an older binary; old binaries cannot decode markers.
   Never restore a stale whole database or budget snapshot to downgrade source.

## Physical reclamation

Graph membership conversion removes the expanded memberships for one
`(epoch,key)` using `DELETE FROM retrieval_membership INDEXED BY
retrieval_membership_key WHERE epoch=?1 AND key=?2`. The existing index is
created with `IF NOT EXISTS` by `retrieval::graph::schema` on every successful
database open; initialization errors propagate, so historical databases missing
the index are repaired before the hinted statement can run. A native SQLite
3.51.3 schema/plan fixture exposed the unhinted primary-key search on `epoch`
alone despite the `(epoch,key)` index. The hint selects both equalities and
does not change which memberships are deleted. The request time budget is
cooperative; commit, transport and I/O can make client wall time exceed it.

List construction serializes each identifier token with the existing Rust JSON
encoder, then inserts the token array in ordinal order and resolves IDs through
the unique token index. This preserves first-occurrence ID assignment, existing
IDs, duplicates, and packed list width/order without SQL calls per reference.
Root list members use a set-based insert plus exact distinct cardinality check.
During physical copy, membership conversion may reuse that freshly built list
only when its canonical JSON equals the node's complete source-ID array. The
packed format is still validated, dictionary and epoch bindings are checked,
native IDs retain first-occurrence order, and key position is explicitly stored.
The legacy projection is still removed only after its compact representation
exists; a ready legacy index alone never permits skipping conversion or repair.

The existing native `reclaim` action remains the physical page mechanism:
1..16 pages per cut, 1..25 ms, a current maintenance fence checked for every
page, adaptive cuts, five-second cooldown, and foreground/WAL pressure refusal.
It executes incremental vacuum through the StateModule owner and preserves all
private tables. Reusable pages and a smaller freelist do not establish a smaller
file while WAL still retains page changes.

The explicit `reclaim` maintenance request adds `maintenance: true`,
`max_pages: 1..4096`, `max_ms: 1..3000`, and `checkpoint: "truncate"`.
The default remains `maintenance: false`; ordinary limits and cooldown stay
unchanged. Maintenance uses transactions of at most 128 pages, checks the
current owner/fence and foreground pressure between chunks, yields between
chunks, and has no five-second cooldown. It drains every row returned by
`PRAGMA incremental_vacuum(N)` before committing each chunk. WAL pressure
requires an acknowledged TRUNCATE checkpoint rather than unbounded WAL growth.
The response records actual before/after physical state, reclaimed pages,
checkpoint outcome, `max_chunk_pages: 128`, and `min_interval_ms: 0`.
This action has no operation ID or replay receipt: after an unknown ACK,
reconcile actual page count, freelist and WAL before issuing a new bounded
call. Never describe a repeated reclaim request as receipt-idempotent.

Three additional `compact_apply` steps run outside the ordinary mutation
transaction on the same serialized owner. Both require compact phase, the
expected format generation and a current maintenance lease/fence. They first
commit a durable identity/digest intent and generation advance, perform the
physical operation, then recheck the fence and commit its result. A repeated
completed request returns its stored result; a pending intent returns pending
without repeating the operation. Errors, cancellation or lease expiry retain
the unresolved intent. No stale database swap or budget restoration occurs.

- `enable_incremental` reuses the native explicit `migrate_incremental` rebuild
  with a 1..86400000 ms cooperative budget. If auto_vacuum is already 2 it is a
  no-op; mode 0 requires SQLite's atomic VACUUM and temporary space proportional
  to the whole database. This is a separately approved quiescent maintenance
  window, not a background cut. The serialized source owner cannot process a
  concurrent snapshot-open/backup during it; already-created snapshot backups
  are separate files. SQLite locks and busy/deadline refusal protect other
  connections. Lease expiry before the final receipt leaves pending even if
  VACUUM completed; reconcile rather than repeat automatically.
- `checkpoint` uses `PRAGMA wal_checkpoint(TRUNCATE)` with 1..25 ms budget and
  the existing guarded settings/short busy timeout. Busy readers produce a
  deferred result; they are never killed or unpinned. Before/after physical
  state and actual SQLite busy/frame counts are recorded. Committing the result
  itself can create new WAL metadata pages, so this is not a promise of a
  permanently zero-byte WAL or a complete final-tree measurement.
- `rebuild` is an explicitly authorized atomic VACUUM with a 1..240000 ms
  cooperative budget. It is a manual maintenance-window option; incremental,
  interruptible `reclaim` remains the shared primitive for direct and future
  periodic maintenance. A full rebuild does not provide foreground preemption.

After accepted logical cut/collection, enable incremental mode if required,
run acknowledged bounded `reclaim` cuts, then an explicitly authorized compact
checkpoint. Keep original snapshots/rollback assets according to their separate
policy. Interrupted VACUUM uses SQLite atomic recovery; logical format rollback
still reconstructs current rows and memberships without rolling back spend.
Physical shrink is not reversed by source downgrade. Exact final and peak sizes
remain to be measured after all retained files and receipt/index overhead exist.

Copies of the original inline values are retained during coexistence until the
explicit collection phase. Peak space therefore includes original rows,
rollback copies, shared payload/dictionaries/references/indexes, WAL and pinned
snapshot backups. The final candidate includes every remaining table and index;
do not add separate overlapping dedup/list/vector savings estimates. Existing
diagnostic category BLOB totals cover physical state-row references, excluding
shared pools/rollback copies/indexes; database/page/WAL totals include them.

## Integrated admission and retention boundaries

Internal memory `embeddingReuse:{sourceMemoryId,sourceVersion}` is consumed by
native Evolve. The existing exact predecessor/version CAS precedes capture of
original canonical embedding Base64. Title/content, session scope, permission
fields and embedding model/dimensions/generation/binding must match. Retirement,
successor publication and rebinding share one transaction. Missing originals or
unsupported bindings reject before publication. Dirty recovery refuses a reuse
marker before any embedding provider call; it cannot silently re-embed metadata.

Graph generated-body cleanup rechecks completed/invalidated terminal age of at
least seven days and protection flags inside the native transaction. Technical
cleanup uses seven days. Summary-unit/intents candidates require an exact
`preserved` parent job/version, confirmed terminal age, existing publication
dependencies and protections. Captured inputs/provider results, durable replay
receipts, unknown/failed work and raw originals without an exact retained source
continue to be preserved. These source changes do not schedule or run cleanup.

## Build assets and next explicit authorization

The native source-only mirror is `native-candidate/build-workspace`, excluding
`.git`, `target`, `artifacts`, `node_modules` and generated directories.
`native-candidate/build-native.cmd` takes that workspace's `Cargo.toml` as
its first argument. It initializes VS2022 Community `vcvars64.bat` and invokes
`C:/Users/harus/.cargo/bin/cargo.exe build --offline --locked --release
--no-default-features --jobs 1 --package iii --bin iii --target
x86_64-pc-windows-msvc`. `CARGO_HOME` is candidate-local `cargo-home`;
`CARGO_TARGET_DIR` is the existing
`D:/agentmemory/.codex-task/bounded-engine/cargo-target`.
`CARGO_INCREMENTAL=0`, TEMP/TMP are candidate-local `temp`. Accepted source
was not overwritten and no binary/cache tree was copied wholesale.

Selective locked registry metadata/archives (172638548 bytes, 1440 files) were
copied without credential/config files. Authorized `cargo fetch --locked`
completed with the same candidate-local home and Windows target. Initial missing
`anyhow`/`fs2`, an intermediate output-directory error and two compiler type
errors have preserved logs. The final native release invocation returned exit 0
and emitted `native-candidate/artifacts/win32-x64/iii.exe`; raw build/exit and
emission logs remain beside the recipe. No tests or native capability probes ran.

`app-source` starts from the installed
`agentmemory-source-20261015-provider-stages-20261015T115401Z` source package,
then applies this task's exact retention/capture mutations recovered as data from
the two worker rollouts. `task-deltas/composition.json` records the baseline
and applied paths; the extraction does not execute rollout JavaScript. The new
snapshot read-view module was recovered from its initial source reading and its
task edits applied. Only its StateKV/storage transport bridge was added to the
installed baseline. In particular, installed `completeSession` and unrelated
extract/provider stages remain in that baseline. Native compaction transport and
the embedding-reuse recovery guard are narrow additions. The retention-evict
handler is integrated as the directly required cleanup-authorization/explicit-TTL/
protection change; no entire workspace retention file is promoted. Its recorded
type-only adjustments targeting an unpublished handler were superseded by that
integration. No other root workspace files are copied wholesale.

`tsdown.isolated.config.mts` emits this isolated source to
`native-candidate/app-dist-isolated` (index, CLI, standalone, reranker and hooks),
using the existing workspace tsdown and `dts:false`; the emitter returned exit 0.
Runtime assets come from the installed baseline, with their source paths recorded
in `task-deltas/runtime-assets.json`. The prior `app-dist` built from the whole
workspace is marked `NOT-INSTALLABLE.txt` and must never be promoted. These are
candidate assets for a later coherent package/install decision, without a passing
runtime acceptance receipt or permission to restart services or migrate data.

Later acceptance must cover exact reconstruction, mixed inline/compact writes,
collision handling, list widening/order/duplicates, both membership directions,
vector bit equality and search results, snapshot readers, old-ACK replay,
concurrent imports, generation/fence expiry, rollback, interruption/limits,
protected recovery jobs and monotonic private spend. Existing fixtures require
appropriate view/function initialization; no new passing test receipt exists.

Only after that acceptance and separate install/migration authority: install a
coherent engine/application package, acquire the existing maintenance lease,
read status, and begin with `limit:16,max_bytes:1048576,max_ms:25`. Continue only
through bounded acknowledged cuts with current generation/fence; a refusal or
unknown outcome requires reconciliation rather than blind repetition. A single
large row can require a deliberately larger authorized byte/time cut or remain
pending. Capture final full database/tree and peak-space measurements before
choosing collection or physical reclamation. Existing generic offline migration
rejects schemas containing triggers/views; use this fenced native format path,
not a legacy bulk converter or generic application export/import.

The repeatable isolated application emitter is `native-candidate/build-app-isolated.cmd`; it does not run package scripts, tests, declaration emission or runtime checks. Recomposition uses `compose-app.py` (line-anchored source patches), `connect-isolated-app.mjs` and `connect-retention-handler.mjs`; the compiler exposed an initial line-anchor replay error, corrected before the final isolated emission. Retention protections are mirrored at the native transaction boundary, including explicit rollback/replay/recovery flags, active leases/generations and unknown/nonempty dependency references.

The final native emission includes the complete retention protection helper and returned exit 0. `source-delivery.json` records this final compiler outcome alongside the isolated application emission; it is a build receipt, not runtime acceptance or a physical-size measurement.

### Bounded collector candidate (2026-10-16)

The collector now carries one durable phase/pass cursor across graph mappings,
identifier mappings, roots, blobs and redundant inline rows. A request admits at
most 4096 inspected candidates and, independently, 4096 physically deleted rows
across all phases, with internal chunks of at most 128. Copy remains capped at
128. The existing 4 MiB and cooperative 3000 ms budgets, transaction fencing,
receipt replay and FULL durability remain in force. The time budget is
cooperative; client wall time is not a hard three-second guarantee.

A pass that deletes anything starts another complete pass; only a complete pass
with no deletions reaches `gc_complete`. Actual reference changes invalidate
completion. Separate cumulative inspected/deleted metrics describe work and
retirement; inspections do not represent storage savings. Parent retirement
rechecks references and retains a parent while child chunks remain. Reusing an
unreferenced list blob reconstructs and validates its complete membership before
publishing references in the same transaction. Physical reclamation is a later
operation and must be measured separately. This candidate's runtime acceptance
and actual C database reduction are recorded in the operational receipts, not
in its compiler exit status.
