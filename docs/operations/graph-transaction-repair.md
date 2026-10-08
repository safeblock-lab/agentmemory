# Graph transaction failure diagnosis

The preserved extraction job `graphjob_muzmgf3d_2d56b87eea0a` failed again
at 15:10:10.499Z on 2026-10-08 while staging delta 4, checkpoint chunk
80940. Batching staging writes did not establish the cause of this failure.

`StateKV` sends reads, leases and batch commits through the same native
transaction wrapper. Previously unknown SDK failures became `STATE_TX_FAILED`
without identifying which native function failed. The graph logger also
filtered out lowercase SDK codes such as `invocation_failed`.

The wrapper now retains the fixed native function ID, an allowlisted transport
code and a failure category. It recognizes a known native state error at the
start of an SDK-wrapped message. No request values, row keys, raw messages or
stack traces are logged. The graph recovery logger includes these fields on
all failure outcomes.

Native SQLite failures are sanitized by the engine to `state operation
rejected`. The engine writes the internal cause to its stdout, which the CLI
previously discarded. A separate bounded, sanitized CLI diagnostic capture is
needed before the next recovery to distinguish an underlying SQLite rejection
from a read or transport failure.

This diagnostic change does not itself resolve the underlying staging error.
The original job, captured input, staged rows and checkpoint remain intact.
No tests, typechecking, runtime recovery or provider calls were run for this
diagnostic milestone, following the explicit session execution preference.

## Instrumented failure and remaining diagnosis

The instrumented recovery failed at 15:29:07.967Z with no transaction-wrapper
diagnostic fields or native SQLite rejection category. The outer logger had
incorrectly labelled every other exception `STATE_TX_FAILED`. Nontransaction
errors now retain an allowlisted class, known code and at most four source
locations containing only fixed module basenames and line/column numbers.
Unclassified failures use `GRAPH_EXECUTION_FAILED`. The failure checkpoint is
captured before terminalization changes its visibility.

After three failures the original job stopped with delta 4 complete, while its
unfinished preparation remains preserved. Clearing `recoveryStopped` alone is
unsafe: native checkpoint progression forbids reopening the same completed
logical delta, and application replay could mistake the terminal checkpoint
for a successfully applied manifest. An explicit continuation must preserve
the original capture and prior completed deltas before another recovery.

Source inspection found no unbounded call-argument spread in the affected
graph persistence/freezing path. The only `push(...additions)` expands at most
two operations. No speculative data normalization or retry-limit increase was
made. The actual nontransaction exception still needs identification.

## Explicit local continuation

The internal `mem::graph-recover` function accepts an operator-only continuation
descriptor alongside `localOnly: true`: `jobId`, `generation`,
`checkpointVersion` and `chunkOrdinal`. Normal recovery never supplies this
descriptor and never rearms a stopped job automatically. Public REST recovery
does not expose the descriptor.

Continuation requires the exact stopped extraction checkpoint, a complete
capture, no active recovery or pending job, and an unfinished `preparing`
manifest without prepared application metadata or result. It runs under the
native writer lease and refuses repeat continuation. The terminal delta stays
complete and immutable. The failed logical operation maps to the next native
delta ordinal; prior completed manifests and frozen input/provider values stay
at their original ordinals. No new capture or graph reset occurs.

The authorized metadata read at 15:34:45Z found checkpoint version `87514`,
delta 4 complete at chunk 81370, and manifest version 3 still preparing with no
template/application metadata or result. No continuation has been invoked by
the implementation worker. Deployment and a single explicitly guarded local
continuation remain separate operator actions.

The first guarded continuation attempt at 16:00:18.891Z was rejected before
changing the checkpoint. Native commits reject a terminal job with a complete
checkpoint before considering the incoming job update. Advancing to delta 5
alone therefore could not restart the preserved preparation.

The native terminal guard now permits one narrowly defined atomic continuation:
the existing failed, stopped extraction has a complete capture and no earlier
continuation; the next staging delta starts at chunk zero; the original delta
manifest is still preparing without result or application metadata. The only
operation must update that same job, preserving every field except its staging
state, stopped flag, retry deadline and original continuation ordinal. Native
lease, generation, checkpoint version and expected row version checks still
apply. Completed deltas remain immutable. This requires the corrected native
binary as well as the TypeScript worker; runtime recovery remains pending.

Recovery failures before handler execution now include the sanitized validation,
admission or discovery phase and the same bounded error diagnostic used by the
handler. Execution failures retain the execution phase. Raw messages and
payloads are not logged.

## Confirmed cache limit failure

The newer captured job `graphjob_muzp0kpa_46f443ad3341` also failed in delta 4.
Its local recovery at 16:31:25.586Z identified `snapshot-size-limit`: the
snapshot projection exceeded its 4 MiB cache budget. Earlier diagnostics
missed source frames because the packaged graph code lives in a generated
`src-<hash>.mjs` bundle. Diagnostics now recognize fixed generated bundle
names and classify exact static graph errors without exposing raw messages.

Cached edges could accumulate after their endpoints left the top-node cache.
Projection now retains only edges whose endpoints remain in the projected
top-node set. Ranked nodes, their degree entries and endpoint-valid edges
share the existing exact serialized byte budget. Oversized cache entries are
omitted; complete native node and edge records are untouched. Aggregate totals,
type counts, timestamps and batch receipt metadata remain intact. Snapshot
queries already report truncation using the complete aggregate counts.

The original and newer stopped jobs retain their captures. Other pending jobs
must recover first under the existing admission rule; stopped-job continuation
requires fresh exact checkpoint metadata and does not skip pending work.
Installation and successful runtime recovery of the cache correction remain
pending. No tests or typechecking were run.

## Historical capture loss and safe closure

At 16:40Z, existence-only native reads found `capture:0` absent for both
historical jobs, despite their `captureComplete` flags. Frozen options,
heuristics and prefix manifests still exist. Their presence cannot reconstruct
the original request flags or satisfy its capture digest. These operations
cannot be reported as recovered.

Native terminal retention previously treated a failed job with
`recoveryStopped: true` as eligible for automatic pruning, beginning with its
input fragments. Retention now accepts only completed or explicitly invalidated
jobs. Failed stopped jobs remain terminal for admission but retain their
remaining artifacts. Explicit pruning uses the same stricter eligibility.

Continuation validates the actual captured request and digest before changing
any checkpoint. A missing fragment returns `captureUnavailable: true` and
`recovered: false`. The already continued newer job can close its nonvisible
preparation only under the writer lease, with no recovery barrier and a staging
checkpoint whose preparing manifest has no result or application metadata.
Closure records `GRAPH_CAPTURE_UNAVAILABLE`, stops recovery, clears the
inaccurate capture-complete flag and completes the checkpoint using the current
snapshot check. It preserves remaining staged data and live graph records;
it never invents a replacement request or calls a provider.

The retention fix requires a newly compiled native binary. Its installation
and the guarded closure are pending operator actions.
