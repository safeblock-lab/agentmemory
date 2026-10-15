# Graph extraction barrier, 2026-10-08

Current scheduled recovery reconciles a stopped job's dispatch intent to a
failed disposition once, retaining the job, failure code, captures and native
checkpoint. It does not automatically reopen the stopped job. Other eligible
intents can proceed once native recovery has completed its existing terminal
transition; an actual unfinished or unknown head remains a recovery barrier.
No global feature pause is needed to retain these per-operation safeguards.

Native metadata identified one nonterminal extraction job in generation `1`:
`graphjob_muytdb4j_3dedda7638da`. It failed at `2026-10-08T00:44:57.240Z`
with `STATE_TX_FAILED`; its retry time had elapsed, but automatic recovery
remained paused. New jobs encountered this pending job even though the control
record's `recovery` field was null.

The saved checkpoint was `delta:4`, visibility `staging`, chunk ordinal `1716`,
version `1859`. Its manifest remained `preparing`, with no generation advance.
Preparation stores shadow records; it does not apply that delta's live graph
changes. Previously completed deltas are separate and remain valid.

The requested repair acquired a native graph lease and rechecked the exact job,
checkpoint and manifest under its guard. It preserved the job and its failure
code, set `recoveryStopped: true` and cleared `recoveryAfter`, then completed the
staging checkpoint using `GraphJobStorage.completeStaging`. The native result
was checkpoint version `1862`, visibility `complete`, chunk ordinal `1719`.
The lease was released. Captured input, shadow records and previously completed
graph changes were retained. No provider, extraction, reconstruction, cleanup
or standalone database access was used.

The repair removes this abandoned job from pending-job discovery. It does not
replay its unfinished delta or refresh the graph snapshot. A subsequent
successful operation that changes the saved graph summary can update the
snapshot timestamp.

`STATE_TX_FAILED` wraps unrecognized native/transport failures. The saved job
does not retain its underlying cause, so it cannot establish whether timeout,
disconnect or another transport failure caused this incident. Existing code
already retries an exact failed commit once and bounds staging recovery to
three failures, with permanent payload failures stopped immediately. No
unproven change to those limits was made, and no new extraction was invoked to
claim recurrence is prevented.
