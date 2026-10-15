# Canonical memory index

The application uses `state::retrieval` for exact identity, atomic effects and bounded source selection. These internal modes do not add public MCP tools or REST endpoints. The compatible iii-engine patch must be installed with the application adapter.

## Identity and preparation

Semantic facts and procedural names preserve the historical global JavaScript `toLowerCase()` identity, without trimming legacy values. Long-term memory identity is the exact project and lowercase title; only latest versions qualify. Global fact/name equality is an exact indexed lookup, not an unbounded global search.

`canonical_prepare` processes at most 16 rows and normally 64 KiB per cut, with a native durable cursor. One valid exceptional record may consume the existing codec budget and then yield. Malformed or over-budget source rows leave explicit partial coverage; an incomplete index never establishes authoritative absence. Normal writes, deletes and imports maintain the derived index in their transaction.

## Atomic writes and replay

`canonical_lookup` returns the canonical ID, exact row version, scope revision and coverage epoch. The adapter carries these guards into `canonical_apply`; a negative lookup also checks the target tombstone version. It retries a conflict at most three times, re-reading and recomputing the state update while retaining the captured provider output.

Memory evolution writes the previous `isLatest: false`, the next memory and an independent effect receipt in one transaction. The adapter recomputes parent/version/supersedes after a concurrent winner change while preserving the captured ID, content, provenance and timestamps.

Receipts bind operation kind, partition, identity, effect key and a stable operation fingerprint. They contain IDs, versions and result fingerprints rather than complete record copies. `canonical_apply` with `receipt_only: true` checks a receipt before any source read. Consequently, a lost acknowledgement remains recoverable after subsequent evolution or deletion; replay never resurrects a deleted memory. `not_applied` means receipt absence only.

Results separate `applied` and `created`. A replay has `applied: false` and retains the original receipt's `created` value. An invocation may count a new creation only when both are true. Legacy `appliedBatchEffects` remain intact; new effects use independent native receipts.

One atomic record update may exceed a 64 KiB page cut to preserve valid legacy data. Its value must fit the native transport budget of 16 MiB minus 68 KiB, and the complete request must fit 16 MiB minus 64 KiB. The adapter returns explicit pending work when a record cannot fit. This is a bound on one record, never permission to hydrate a scope.

## Selected sources and decay

Observation selection composes the existing local embedding and reranking components. Lexical and vector queries include the exact project and `source_kind: observation` before candidate limits. Skill extraction additionally includes the exact `session_id` and `source_scope: mem:obs:<sessionId>` before every limit. This prevents high-ranked memory entries or other sessions from consuming the observation budget.

The selector reads `index_status` with the exact `index_id` and `metadata_only: true`. It requires the matching single-index response before validating the native semantic capability and the exact model, dimensions and generation against the application marker. This avoids decorating status with global source counts or other indexes. A pending global marker with a valid identity is allowed: unrelated projects do not block selected observation preparation. Before embedding or searching, it executes one `project_vectors_prepare` cut for the exact project and observation kind, with at most 16 rows and a 64 KiB request budget. Native `ready` proves only that selected project's observation coverage; a pending or partial cut yields `STATE_INDEX_NOT_READY`. Native cursors remain durable. The existing generic IndexedVector readiness contract is unchanged.

With `include_text: true`, native results provide existing indexed text snippets capped at 2 KiB, exact source versions and scopes. The adapter merges at most 24 candidates from two streams. Each native stream's rank is normalized to `(count - zeroBasedRank) / count`; the two rank values are added and receive a 5% agreement bonus when both streams contain the candidate. Stable ID ties make this order deterministic. Raw BM25 magnitudes are never added to vector similarities, so the fallback can retain candidates from both streams even when BM25 scores exceed one. The existing local reranker then scores these bounded snippets and selects at most 12 references; if its runtime is unavailable, the normalized source order remains. Skill selection does not hydrate full observations; the extractor validates a selected row version before capturing its real content.

Scoped lexical search assigns a constant `1.0` score to bounded text matches, avoiding global term-frequency computation. Its initial ordering is a bounded candidate order rather than a BM25 relevance ranking; vector rank and the local reranker refine it. Either stream can exhaust a candidate or query-work budget. Frequent-term limitations remain explicit in coverage metadata or the resource-limit error below, and require the native exhausted flag.

Incomplete project authority or source projection produces `STATE_INDEX_NOT_READY` before source capture or provider generation, including when projected items are empty. The caller keeps the operation pending. Nonempty bounded results from an exhausted candidate or query-work budget carry optional `retrievalCoverage` metadata. It contains at most the two closed labels `bounded_candidate_budget` and `bounded_query_budget` in that canonical order; duplicate or unknown labels are rejected. Stream limitations are combined on selected references and combined per session on summary references. Native authority and projection failures take precedence over approximation reasons.

If budget-interrupted streams leave no usable candidates, selection throws `STATE_RETRIEVAL_RESOURCE_LIMIT` with the budget reason and exact selected project/session diagnostics. The error preserves the selected input for bounded retry and does not claim authoritative absence or index preparation. A valid nonempty stream remains usable when another stream is interrupted, with the limitation included in its coverage metadata.

Project consolidation selection also returns only versioned references. Summary selection returns session references and obtains each row version on its first capture cut. Procedural selection filters observation references to the selected summary sessions; the capture engine must validate actual headers and repeated evidence before generating a procedure. These consumers require the matching durable manifest pager; the reference adapter alone is not a deployable consolidation implementation.

`canonical_source_page` returns up to 12 unique IDs with exact row versions for the selected source sessions. Its opaque cursor binds the scope, selected session set, project, coverage epoch and postings membership generation. Strength changes do not change membership generation, so decay can continue across its own writes.

Decay stores a durable pending page before applying effects. Each record update checks its row version, source-session membership and current project membership in the native transaction. Page completion and individual acknowledgements are checkpointed. After a membership conflict, selection restarts with the same effect keys and existing receipts; after interruption, the saved page resumes. Changing the configured decay interval does not alter an already captured operation.

The decay anchor includes the latest update, access and previous decay timestamps. A captured cut that resumes after a newer update leaves the record intact; it cannot move timestamps backward or decay strength based on an older activity boundary.

New decay manifests include the configured decay interval and its time epoch in their identity. Repeated work in the same epoch resumes or replays the existing manifest, while a later epoch can decay unchanged sources again. An explicit `manifestId` always resumes its captured interval and timestamp. A zero interval reports `decay_disabled` with no record effects.

## Continuation interfaces

The application entry point injects selection, canonical lookup/write and atomic decay callbacks directly. Resuming these functions does not require additional entry-point fields.

| Function | Fresh selected input | Durable continuation |
| --- | --- | --- |
| `mem::consolidate` | `project`, optional `minObservations` | `consolidationCaptureId`; before capture exists, retain `project` and `minObservations` |
| `mem::consolidate-pipeline` | `tier`, `project`, optional `force`, `deferred`, `batchSourceIds` | `consolidationManifestId`; an unfinished multi-tier selection retains `tier`, `project`, flags, `batchSourceIds`, `consolidationSelectionId` and `consolidationCatalogProject` |
| Provider batch callback | Existing captured request | `consolidationManifestId`, `consolidationPartitionId`, `batchResponse`, `batchEffectKey` |

Identifiers must be bounded strings and must resolve to matching durable project/tier/request identities. Source-session lists remain bounded to 12. Public continuation handling must preserve these selected inputs and distinguish resource failure from index preparation. Provider-response fields belong to the callback protocol.

The REST handlers forward continuation fields. The MCP `memory_consolidate` definition and handler also expose bounded project, source-selection and manifest continuation fields. The handler preserves exact returned identities and whitelists public inputs, excluding provider callback payloads. Supported tier selectors include `all`, `semantic`, `reflect`, `procedural` and `decay`; the existing tool count is unchanged. MCP transport behavior is validated by its separate surface unit.

Provider batch metadata preserves manifest and partition IDs through the entry point. The corresponding durable root holds retrieval coverage, so the provider payload needs no copy of that metadata. For durable consolidation work, replacement dispatch invokes the request recovery helper before its eligibility guard, allowing an already captured response to replay without enqueueing. It rejects missing request identities. Other task branches retain their existing eligibility checks.

The recovery helper retains the same request identity and captured prompts, reuses coordinator eligibility/deduplication, and records bounded append-only replacement links instead of overwriting captured request state. Completed roots still require exact request and work binding before replay. Its pager integration and concurrency tests require separate acceptance; terminal work produces an explicit retained diagnostic rather than an endless pending root.

The request saves its started dispatch before generation; an uncertain outcome without a captured response remains retained until exact result redelivery. Ordinary provider routing and retry behavior remain configured by the application.

The non-idempotent Fireworks batch-submit POST also has one internal transport attempt; an uncertain submission uses the existing saved submission identity and remote reconciliation. Dataset setup and status/result queries are separate operations. This does not claim one HTTP request for the entire batch or distributed exactly-once provider execution.

## Verification and recovery

Focused adapter tests are in `test/canonical-memory.test.ts` and `test/consolidation-decay.test.ts`. They cover incomplete coverage, bounded preparation, positive/negative conflicts, retry exhaustion, replay after deletion, evolution recomputation, legacy metadata preservation, scoped selection and interrupted multi-page decay. Entry-point tests also cover durable recovery dispatch, invalid request identities and provider callback identity propagation. Existing `test/fireworks-batch-callbacks.test.ts` cases cover the other callback and replacement branches. Native transactional tests and a production binary build are separate required evidence.

Passing these isolated tests does not establish installed runtime correctness. Installation and every affected public workflow require real bounded execution, inspection of persisted content/coverage/effect receipts and replay checks. A `completed` state alone is not acceptance evidence.

This change does not activate retention, reclaim database pages or run compaction. Cleanup remains a separate operation. Rollback requires the previous compatible application and engine pair; captured manifests and source records remain available for recovery.
