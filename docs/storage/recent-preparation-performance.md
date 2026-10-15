# Recent history preparation performance

Installed preparation measurement completed on 2026-10-13. The preparation repair preserves global readiness; full catalog, large-project graphs, publication, storage maintenance and representative memory acceptance remain open.

## Installed measurement

The combined native/caller candidate `source-20261013-native-caller-core` completed the remaining history without resetting its cursor: 49 acknowledged cuts processed 178,729 rows and 822,729,171 source bytes in 161.669 seconds, about 1,106 rows/second. The historical measured rate was 74–93 rows/second. Every cut respected its declared row and byte bounds; the longest roundtrip was 5.940 seconds. Durable status finished with `scanned=true`, `dirty=0`, `issues=0` and `ready=true`. A recent sessions read returned 16 real rows with continuation and an honest partial total.

During that preparation, 56 samples preserved the exact application and engine identities. Combined peak working set was 583,274,496 bytes and peak private memory was 617,639,936 bytes. All 22 post-completion samples stayed below 1,000,000,000 bytes for both metrics; the last settled sample was 449,937,408 working-set bytes and 435,904,512 private bytes. This establishes memory release for this preparation workload, not a universal ceiling across reflection, summary, large graphs, reconnect and maintenance.

Main reviewed the raw cut responses, memory samples and passing native outputs. Evidence is in `.codex-task/full-closure/runtime-core/combined-20261013/`: `recent-outcome.json`, `recent-cut-*.response.json`, `ram-series.jsonl` and the focused native logs. The review receipt is `.codex-task/full-closure/main-recent-installed-review.json`. The affected 69 TypeScript cases and 14 native executions (13 distinct cases; the lost-ACK case ran twice) passed before installation.

Large-project graph preparation subsequently timed out with an unknown outcome, and the exact reflection captured generation but remained pending publication. Neither operation was blindly replayed. These separate failures and the remaining full task are recorded in the operator handoff; the preparation result does not close them. The runbook below remains the procedure for a later changed candidate, not an instruction to repeat the completed history load.

## Observed problem

The three acknowledged historical windows processed 11,320 rows at 74–93 rows/second. Their final cuts processed three rows and under 3 KB before the 25 ms cutoff. The old source ignored caller `max_bytes` and instead allowed up to 16 MiB cumulative decode. These receipts establish the cutoff bottleneck; they do not establish commit, WAL, queue, or context timeout causality. See `.codex-task/full-closure/runtime-core/resume-20261013/readonly-diagnosis.md`.

## Source behavior and provisional limits

`viewer_recent_prepare` accepts omitted work fields. Central native defaults are `limit=4096`, `max_bytes=33554432`, `max_ms=60000`. Those defaults were used in the installed measurement above. Finite maxima are 16,384 rows, 134,217,728 bytes, and 300,000 ms; maximum settings have not been measured on the installed history. Minimum values remain one row, 1,024 bytes and one ms.

The successful cut is bounded by rows and bytes, with no elapsed-time slice. `max_ms` is an upward safety hint, not a request to stop useful work after that interval. One deadline covers queue, WAL, BEGIN, retrieval, COMMIT and ACK. Its allowance is:

```text
min(300000, max(max_ms, 30000 + 4*limit + 10*floor(max_bytes/65536))) milliseconds
```

The default deadline is 60 seconds. Maximum work with `max_ms=1` receives 116,016 ms; the explicit safety hint can raise this to 300 seconds. The base allowance accommodates queue/commit overhead; row and byte terms scale with work. They are not throughput predictions. The installed viewer caller uses the supported operation-specific SDK timeout of 70 seconds, leaving a 10-second ACK margin above the native default deadline. A future caller selecting larger work must leave an ACK margin above that work's native deadline. A caller's short remaining wall window must stop submitting new work, not shrink the native cut to 25 ms.

The selected work is fetched by indexed keyset SQL in pages of at most 256 metadata records, then one source body is loaded and decoded at a time. The scan uses the existing `(scope,key)` primary key; dirty keys use their primary key and point joins. Cached fixed SQL statements eliminate repeated compilation of payload fetches and projection updates. No corpus vector, source-body page, or application-side corpus hydration is introduced. All metadata pages, encoded buffers, decoded strings, and parsed source values leave scope within the cut. SQLite's existing bounded page cache and small statement cache remain; release of process allocations must still be measured.

`max_bytes` now bounds cumulative `source_bytes = sum(max(logical_bytes, conservative_stored_bytes))`. `decoded_bytes` is also bounded by the same maximum. The text storage estimate remains conservative (`length(text)*4`); blobs use their actual stored length. This bounds encoded and decoded work individually, rather than claiming that caller payload limits previously enforced these bounds. One source retains the existing 16 MiB logical/stored cap.

If a valid next source would exceed the remaining byte budget, the response has `reason=decode_budget_exceeded` and `required_bytes` for that whole source. No part of that source is decoded, projected, or consumed. `processed=0` is allowed and requires the caller to increase its bounded next budget or show a blocked state; identical zero-progress retries are incorrect. Sources beyond the 16 MiB cap or with invalid encoding retain persistent issues, never truncate silently, and never produce `preparation.ready=true`. `continue=false` means the scan/deltas are exhausted; issues can still leave public readiness false.

Responses retain durable checkpoint/generation/cursor semantics and add `source_bytes`, `required_bytes`, `elapsed_ms`, and `batch_limits`. The elapsed value covers preparation before COMMIT, not the roundtrip. Checkpoint and effects commit in the existing transaction. Current inserts, edits, moves and deletes remain in the dirty-key path, including writes behind the scan cursor. Cancellation is checked between sources and at existing transaction boundaries; the existing SQLite interrupt hook handles long SQL. Rollback clears the handler before undoing effects. A cancelled cut must not advance either effects or checkpoint. A lost ACK can occur after COMMIT, so reconcile status before another mutation.

## Context reads

Ordinary `mem::context` now uses `selectIndexedContextObservations`: metadata validation and scoped lexical/semantic reads only. It never calls or queues `project_vectors_prepare`, `source_prepare`, or vector recovery. Native current-version hits remain usable while coverage is partial. Known incomplete-coverage reasons and empty partial pages propagate `partial=true`. Existing source identity/project/agent checks, token budgeting and context telemetry remain. Explicit consolidation/skill selection preparation retains its existing dedicated path; background maintenance policy is unchanged.

## Focused regression execution, later operator only

No commands in this section were run by the source owner. Reuse the existing Cargo home, target, compiler environment and isolated `III_SQLITE_TEST_ROOT` from the accepted native receipts. Do not create another cache or delete anything. Hold one compiler and freeze native source throughout.

From the existing engine workspace, run serialized with `--locked --package iii --lib -j1` and the existing cache:

```text
cargo test --locked --package iii --lib -j1 viewer_recent::prepare -- --nocapture
cargo test --locked --package iii --lib -j1 preparation_queue_tests -- --nocapture
cargo test --locked --package iii --lib -j1 preparation_budget_tests -- --nocapture
```

The new `batch_tests` cover a 513-row initial cut with legacy one-ms input, exact byte boundary/zero-progress/resume, tombstone and oversized issue handling, cancelled batch rollback, lost response/reopen/current write behind cursor, and finite request defaults/maxima. The existing lost-ACK metadata-only status test remains. The focused TypeScript files are `test/bounded-context-sources.test.ts` and `test/consolidation-selection.test.ts`: complete and partial useful context, empty partial coverage, no preparation requests, known/unknown native reasons, and existing explicit-selection receipt validation. Run existing affected context budgeting/telemetry tests only where Main judges their receipt invalidated by these source changes. Do not recreate deleted tests.

First failed check: preserve exact output, stop widening work, identify the failing invariant and one discriminating next check. Compilation alone supplies no installed acceptance.

## Installed runtime runbook, later sole operator

1. Accept and freeze this source plus caller integration. Confirm the caller uses native defaults or equivalent explicit fields, accepts `processed` through 16,384, handles `required_bytes` and partial exhaustion, and sets an operation-specific transport deadline above the native allowance. Build/install only that combined candidate under the existing single-operator authority and rollback inventory; preserve workflow pauses, protected jobs and the unknown summary. Never restore the live database.
2. Read durable recent status before preparation. Preserve the actual checkpoint, generation, phase and readiness. Reuse the 11,320 already acknowledged rows; do not reset/rebuild or repeat old windows. A timeout is slow/unknown, not proof the service died. After timeout or lost delivery, issue one read-only status reconciliation and decide from durable evidence whether COMMIT happened.
3. Capture exact app/engine PID plus CIM creation time and command line. Before, during, and after each bounded cut, sample both processes' WorkingSet64 and PrivateMemorySize64 on the same cadence. Sum each metric separately. Preserve identity on every sample; a PID reuse or restart invalidates comparisons. Latest existing nine-sample diagnostic baseline is combined max working set 606,334,976 B and private 612,352,000 B, collected after preparation had stopped. It is not a new current measurement or completion acceptance.
4. Start at proposed defaults, one cut at a time. Record input work limits, wall roundtrip duration, native pre-COMMIT elapsed_ms, stage logs, processed/source_bytes/decoded_bytes, returned phase/cursor/checkpoint/generation and status. Derive rows/sec and bytes/sec from actual work and duration. Record queue/WAL/BEGIN/retrieval/COMMIT/ACK timing when available; do not infer absent timings. Stop at any unexplained nonprogress, resource error or identity change; reconcile before retry.
5. Compare initial throughput with historical 74–93 rows/sec and count commits/calls per useful row. Measure sessions-to-observations phase progress and remaining preparation, not merely the width of a client polling window. Adjust rows/bytes within maxima only from observed latency, foreground fairness and memory. Do not use the provisional maximum as an instruction to submit it immediately. Existing `phase` is source cursor phase, not a promise of session-only availability.
6. During initial scan and dirty replay, run the isolated behavioral matrix below using only already authorized disposable fixtures. Retain raw request/response/checkpoint receipts. Do not alter protected live records. Reads must report honest partial state until full preparation is ready.
7. After actual completion and idle settling, continue the same identity-bound combined memory sampling. Preparation may use up to 3 GB combined app+engine only with measured throughput gain; post-completion must return below 1 GB. Record peak, completion value, settled value and recovery duration for both metrics. Continue to representative context, summary/reflection, graph, reconnect and maintenance acceptance under their separate leaves. A baseline or one idle sample cannot close the overall RAM criterion.

| Case | Required evidence |
| --- | --- |
| Row and byte cap | No more than requested rows; source_bytes and decoded_bytes each within max_bytes; no hidden source-body corpus buffer. |
| Exact boundary / oversized-for-cut | Whole source or explicit required_bytes; unchanged cursor when processed=0; resume with finite larger budget. |
| Source exceeds 16 MiB / corrupt source | Persisted issue, no truncated projection or false ready; valid neighboring rows preserved. |
| Cancellation queued / retrieving / before COMMIT | No late mutation for cancelled queued job; retrieving/pre-COMMIT cut rolls back rows and cursor together; next request works after cleanup. |
| COMMIT succeeded, ACK lost | Read-only checkpoint identifies durable advancement; reopen/resume skips committed cursor work; no blind replay/reset. |
| Edit/delete/insert behind initial cursor | Dirty delta replays current version; deleted rows vanish; canonical provenance and current authority preserved. |
| Foreground context during preparation | No vector preparation action, useful existing scoped hits or truthful empty partial result; token budgets and telemetry preserved. |
| Lock / queue delay / transport timeout | Bound respected; slow/unknown wording; one status reconcile before any retry; no process-death inference. |
| Phase completion with issues | continue=false never substitutes for preparation.ready=true; partial sources remain visible as issues. |
| Large history and memory release | Actual row/byte throughput, foreground latency, <=3 GB transient combined memory and <1 GB settled after completion. |

Affected receipts to rerun are current native recent preparation/cancellation/queue/lost-ACK tests and changed context selection/budgeting cases, then installed recent preparation/readiness/paging and caller timeout/oversized cases. The crystal capability/dirty-trigger and concept-query source sections are preserved; reuse their valid evidence unless the combined build or shared transaction changes invalidate a concrete assertion. Existing 49 TypeScript and six concept-query receipts must not be indiscriminately repeated. All runtime/catalog/panel/storage/maintenance and representative RAM closure remain open.
