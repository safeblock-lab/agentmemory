# Graph memory transaction contract

This document freezes the internal TypeScript/native boundary for bounded graph persistence and durable extraction. The helpers and mocked contract tests implement this boundary; native storage, migration and graph writer adoption are separate implementation units. This document does not claim that the current native artifact already enforces these guarantees. Public MCP/REST tools, ordinary `StateKV` signatures and existing graph algorithms remain compatible.

## Resource boundary

- Native cache target: 64 MiB. One active graph extraction/application owns the global graph lease.
- Commit target: 4 MiB of UTF-8 JSON for the complete `state::commit_batch` request, including the escaped prepared payload, digest and guard. At most 256 ordered operations, including check-only operations, per commit.
- Extra RAM target: 256 MiB plus the largest indivisible existing record, original prompt or provider response. These are provisional targets; delivery requires measurements against the actual packaged native artifact.
- No input truncation, dropped records, prompt regrouping, lossy graph caps or unpaged fallback. Split independent prepared operations into more commits; keep one existing provider input unit intact.
- `allow_oversized_record: true` permits one set operation, with optional check-only expectations, above the commit target. The atomic checkpoint and receipt still accompany that operation. Native transport/frame limits remain authoritative. An indivisible value outside that verified envelope fails visibly with `STATE_RECORD_TOO_LARGE`; it remains retained, never skipped or partially applied. The delivery unit must document the actual envelope and supported record boundary.

The serialized prepared body is one bounded chunk, not a graph-wide snapshot. Large prompts/results and frozen remaps are stored in individual durable records. Paging retains its existing count, byte and ordering contract; a record that cannot fit a page must be accessed by its known key or handled with an explicitly supported native envelope. Never fall back to `state::list`.

Native implementation stays inside the StateWorker adapter; do not alter the separate BuiltinKvStore used by cron/queues. SQLite point/versioned/paged operations must avoid scope cloning or load-all. Legacy native `state::list` keeps compatible small responses and preflights its admitted response envelope before materializing a large scope; over-budget responses fail clearly and direct callers to pagination.

## Guarded records and counters

[`schema.ts`](../../src/state/schema.ts) defines the shared scope names. Protect graph nodes, edges, name/edge indexes, degrees, edge history, snapshots, graph jobs, inputs, provider results, deltas, prepared chunks, remaps and working snapshots. Native-managed control, checkpoints and receipts are protected too. In `mem:batch-callbacks`, protect only `active:graph` and `graph:*`; other callback families retain their existing behavior.

Native `state::set`, `state::update`, `state::delete`, scope replacement/clear, imports and other direct mutation routes must reject mutation of these records outside the guarded transaction path. Checking only the TypeScript caller is insufficient. Every graph writer participates: extraction, batch callbacks, cascades, graph import, export/import, snapshot repair, rebuild and reset/replace. Graph-specific callback receipts and provenance changes commit with their graph effects.

Row versions, checkpoint versions, fences and generations are canonical decimal unsigned 64-bit strings (`0` or a nonzero digit followed by digits, at most `18446744073709551615`). They never pass through JavaScript numbers. Missing rows start at version `0`; deletion retains a tombstone version, and reinsertion advances it. A present JSON-null value differs from an absent row. Incrementing an exhausted counter fails before any effect; counters never wrap or reset. Generation and issued fences are positive. Ordinals and epoch millisecond timestamps are bounded JavaScript safe integers; chunk ordinals reserve room for the next ordinal.

The native graph control row is key `current`, initialized to generation `1`, fence `0`, no lease and no recovery barrier. Native owns this row and the checkpoint/receipt scopes; callers cannot mutate them as ordinary operations. A checkpoint is keyed by job ID. Each original call has a distinct durable job ID. A receipt is keyed by the collision-free tuple `(generation, job_id, logical_delta_id, chunk_ordinal)`, never by concatenation that can alias different tuples. Receipt retention must cover every possible retry; deletion requires a separate proven retention policy.

## Internal functions

[`state-transactions.ts`](../../src/state/state-transactions.ts) exports the wire types, preparation helper and stable sanitized errors. [`kv.ts`](../../src/state/kv.ts) adds `getVersioned`, `lease` and `commitBatch`. They invoke only the corresponding iii-engine functions; an incapable engine returns `STATE_TX_UNSUPPORTED`, without a client-side lock, standalone SQLite or ordinary KV fallback.

### `state::get_versioned`

Request: `{ scope, key, guard? }`. Response: `{ exists, value, version }`. An absent row has `exists: false`, `value: null` and its current tombstone version, including `0` for a never-created key. A present row has a positive version and any strict JSON value, including null.

The optional guard contains `{ owner_id, generation, fence }`. Native validates it before permitting a writer to read through an active recovery barrier. Unguarded readers may read completed `graphSnapshot/current`; reads of live graph rows, indexes and degrees must fail/wait through the persistent recovery barrier. Ordinary `get` and paged reads/exports receive the same native protection. A completed snapshot fast path must not join its cached rows to partially applied live rows. An unguarded control read may report recovery metadata so another owner can resume.

### `state::lease`

- Acquire: `{ action: "acquire", owner_id, generation, ttl_ms }`.
- Renew: `{ action: "renew", owner_id, generation, fence, ttl_ms }`.
- Release: `{ action: "release", owner_id, generation, fence }`.

Acquire/renew return `{ owner_id, generation, fence, expires_at_ms }`. Release returns `{ released: true }`. TTL is an integer from 1 to 300,000 ms. Native time determines expiry. Acquire for a different active owner fails with `STATE_TX_LEASE_BUSY`; acquisition after expiry/release advances the persisted global fence. Repeated acquisition by the same still-active owner returns that lease without extending its expiry; renew is explicit and retains the fence. Use a fresh owner ID for each worker attempt, with a stable job ID across recovery.

Native validates current generation, owner, unexpired lease and exact fence at every guarded read/commit/renew/release. A valid lease is authorization for one writer, not proof that its prepared graph state is current. Expected row/checkpoint versions are still required. Acquiring a lease does not clear recovery metadata. If a persistent barrier exists, the new owner must recover exactly its job/delta before beginning another job, even if the original lease expired.

### `state::commit_batch`

Request: `{ guard, payload_json, payload_digest }`. `payload_digest` is lowercase SHA-256 of the exact UTF-8 `payload_json` bytes. The body is strict JSON produced by `prepareStateCommitBatch`:

```typescript
{
  identity: { generation, job_id, logical_delta_id, chunk_ordinal },
  expected_checkpoint_version,
  checkpoint: {
    generation, job_id, logical_delta_id,
    delta_ordinal, next_chunk_ordinal, visibility
  },
  operations: [
    { type: "set", scope, key, expected_version, value },
    { type: "delete", scope, key, expected_version },
    { type: "check", scope, key, expected_version }
  ],
  advance_generation?: true,
  allow_oversized_record?: true
}
```

The checkpoint identity matches the commit identity, and `next_chunk_ordinal` equals `chunk_ordinal + 1`. Native validates job ownership, legal checkpoint progression and original delta order, not merely field shape. Staging may mutate only this job's durable staging records and working state. A `GraphWorkingSnapshot` stores `{ version: 1, generation, jobId, logicalDeltaId, snapshotExists, snapshot }`; `snapshotExists: false` requires `snapshot: null`, while `snapshotExists: true` preserves either a `GraphSnapshot` value or a present JSON `null`. Application writes do not publish `graphSnapshot/current`. At completion, native resolves the matching working snapshot from durable state or this commit, verifies its generation/job/delta identity against the checkpoint and commit identity, then compares the exact existence and JSON value of `graphSnapshot/current` with the wrapper. A match is checked at its unchanged row version (including the absent row's tombstone version); a mismatch is corrected atomically to the wrapper's target state. An absent target remains absent, so a genuine no-op cannot create a snapshot row that was previously absent. No single snapshot rewrite at the end of an entire extraction job.

Operations use frozen resulting values, never increment callbacks or a read-modify-write description recomputed after effects. Each `(scope, key)` appears once per chunk. A check operation validates a read dependency without changing its version. Set/delete advance their expected versions exactly once. The native engine repeats all request, JSON, identity, ownership, version, digest, size and scope checks before effects; TypeScript validation does not replace this requirement.

Strict JSON excludes undefined, nonfinite numbers, bigint, sparse arrays, accessors, custom prototypes, cycles, symbol/hidden properties and unpaired UTF-16 surrogates. Object keys sort by UTF-8 bytes; array/operation order remains unchanged. The native implementation hashes the transmitted body bytes directly before parsing, so differences in Rust/JavaScript float serialization cannot change the replay digest. It need not reserialize the body to recompute the digest. Ephemeral lease owner/fence/expiry are excluded; identity, expected versions, prepared values, checkpoint, visibility and generation intent are included. Different whitespace or body bytes cannot masquerade as the identical prepared retry.

Successful response is the immutable receipt:

```typescript
{
  identity, payload_digest, generation,
  checkpoint_version,
  row_versions: [{ scope, key, version }]
}
```

`row_versions` follows operation order, including unchanged check versions. Checkpoint version advances once. Response generation matches the identity, except a generation-advancing completion returns its successor. A retry returns the original receipt exactly; no new timestamp, IDs, row writes, degree changes or checkpoint increment.

In one native database transaction:

1. Validate the current guard using persisted generation, fence, owner and expiry.
2. Look up the exact durable receipt identity. Identical digest returns the original receipt before row/checkpoint preconditions; changed digest returns `STATE_TX_REPLAY_CONFLICT`.
3. For a new identity, require its generation to be current, its job/delta to match any recovery barrier, its legal checkpoint progression and all expected versions.
4. Atomically write ordered graph rows, indexes, degrees, provenance, working snapshot, native checkpoint and receipt. The first live effect also sets the persistent recovery barrier. A rollback commits none of these effects.
5. At this logical delta's completion, atomically publish its completed snapshot and graph callback completion state, then clear the barrier. A requested reset/replace generation advance occurs in this same commit.

Historical replay is permitted only after a valid *current* guard and an existing exact receipt. It permits recovering a lost reset completion acknowledgment after reacquiring a lease for the new generation; it never permits an old-generation identity to apply new effects. Native generation changes update/invalidate the lease consistently. Old fences/owners remain rejected.

## Durable extraction and equivalent results

Before effects, durably freeze original input grouping, prompt, provider response, IDs, node remaps, timestamps and prepared resulting values. A deterministic chunk retries its stored bytes. Its ordinal does not identify a new provider call. Distinct captures/calls remain distinct jobs even when their source observations, prompts or responses happen to match. Deduplicate only retries of the same durable identity.

Preserve the heuristic delta first (`graph.ts` heuristic extraction), then each existing provider input unit as its own original logical delta. `persistGraphDelta` pushes newly added `topEdges` only after all degree updates for that original delta. `applyBatchGraph` remains a distinct algorithm with its own deterministic ID and effect rules. Do not coalesce these algorithms or units. Internal chunks may change write timing; completed weights, degrees, ordering, provenance, counts, snapshots and query behavior must match the original sequence under frozen inputs/time/IDs.

Reuse `batchEffectKey`, `effectMetadata`, `preserveBatchProvenance` and callback identities. Current keyed locks, separate callback receipt writes and `appliedBatchEffects` metadata alone do not supply atomic durable graph application. Callback completion must be in the same transaction as final graph effects/checkpoint/receipt. Preserve existing reset `resetAt` orphan checks so old indexes/rows cannot silently reconnect to post-reset extracts.

Staged provider results survive restart, avoiding a second provider call after durable capture. There remains an exact gap: the provider may finish and return a response, then the worker may die **before the response is durably staged**. Recovery can repeat that provider call and receive a different response. Local transactions cannot guarantee exactly-once remote provider execution across this gap; no stronger guarantee is claimed.

## Failure, recovery and rollback

Known errors have stable sanitized messages: `STATE_TX_INVALID_REQUEST`, `STATE_TX_INVALID_RESPONSE`, `STATE_TX_UNSUPPORTED`, `STATE_TX_CONFLICT`, `STATE_TX_CHECKPOINT_CONFLICT`, `STATE_TX_REPLAY_CONFLICT`, `STATE_TX_LEASE_BUSY`, `STATE_TX_FENCED`, `STATE_TX_GENERATION_STALE`, `STATE_GRAPH_RECOVERY_REQUIRED`, `STATE_TX_LIMIT_EXCEEDED`, `STATE_RECORD_TOO_LARGE`, and `STATE_TX_FAILED`. The original cause remains internal. Transport failure after send has an ambiguous outcome: reacquire/renew a valid guard and retry the identical prepared identity/bytes; never assume rollback or regenerate the effects.

A crash leaves either the whole committed chunk and receipt or none of it. A recovery owner reads control/checkpoint and resumes the pending original delta. New work stays blocked until completion. Generation-advancing reset/replace invalidates old unfinished jobs for new effects while retaining exact historical receipts. Failed jobs cannot silently discard a live barrier or publish incomplete state.

The native unit must test actual database rollback, fencing, paging order/tombstones, cache budgets and crash recovery. Mocked tests establish the wire and control-flow contract only. The migration unit owns isolated ordered hashes, rollback and original data preservation. Production migration, provider calls, service restarts and credential changes require separate explicit approval. Keep the original artifact/data/config until migration and rollback gates close; a new storage format must never silently run under an incapable older binary.
