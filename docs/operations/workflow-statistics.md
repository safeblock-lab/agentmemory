# Workflow statistics

The dashboard reads durable workflow receipts and minute buckets through the
iii-engine StateModule retrieval protocol. A collector restart does not imply
lost executions. A failed statistics read does not imply failed recording.
Only an actual recording failure or a producer reporting missing execution
evidence invalidates coverage.

## Counts and coverage

The rolling interval contains the previous 1,440 completed minute buckets; the
active minute is excluded. `observedCount` is the recorded subtotal for that
interval. With incomplete coverage, even a recorded subtotal of zero does not
prove an empty interval. `null` means the subtotal itself is unavailable.

Producer coverage is not yet proven across every execution path, so `/24h`
counts remain explicitly incomplete. Elapsed collector uptime alone cannot
establish complete coverage. Durable last-run receipts remain historical facts
even when other executions might be missing.

Coverage gaps are retained separately from receipts and buckets. A gap only
invalidates rolling windows it overlaps, so it ages out after the affected
interval leaves the window. Because invalidation messages contain no missing
execution timestamp, the gap conservatively spans the original coverage start
through invalidation time. Legacy sticky invalidations are retained through
the next collector activation before clearing the legacy flag. Activation
preserves the original coverage start and all recorded counts; it cannot prove
producer completeness or repair missing execution receipts.

## Graph invocation evidence

Graph extraction displays its last retained invocation separately from its last
terminal logical job. The invocation operation ID is allocated once and reused
by the activity feed and the durable StateModule history. A known job ID comes
only from the execution's supplied durable identity; missing associations stay
unknown. Intake failures before job admission are recorded without inventing a
job. Retrying or blocked invocations never terminalize a job through statistics.

The existing workflow-attempt accounting module stores bounded invocation
evidence under `KV.state`, for all agents and the originating agent. The latest
100 invocations and latest 100 failed invocations are retained independently;
successful traffic cannot evict all failure evidence. Reads deduplicate operation
IDs before deriving the retained failure subtotal. This is evidence retention,
not a new cumulative counter. The failure interval excludes the active minute,
while the last invocation is visible immediately. Retained invocation failures
are a lower bound, including when zero: eviction, old producers and recording
loss prevent a complete historical total. Logical job counters retain their
existing native producer/job identity and count no invocation a second time.

`op_mvdzo2qn_09169c1b7cf3` was reported failed at 15:37:12Z. Main's feed
read at 15:43:55Z found the feed but no retained entry for that ID. No exact
failure cause or job association was recovered, and no backfill is fabricated.

## Queues

Queue counts represent eligible logical jobs, not individual queued units.
`current` means the authoritative source traversal completed within its row,
byte and duration budgets. A current count of zero proves an empty eligible
queue at the sample time. Agent-specific attribution and unsupported queue
sources remain unavailable rather than assuming zero.

`partial` with `queue_budget_exceeded` describes a bounded measurement that did
not finish. It does not report a job execution failure. A numeric partial count
is a lower bound from fully evaluated jobs; its total remains unknown. Invalid
or inconsistent source data remains unavailable rather than publishing an
apparently authoritative subtotal.

## Source delivery

These changes require Main's grouped TypeScript/viewer emission and native
engine compilation and activation. No test, typecheck, runtime measurement or
service operation was performed for this source-only change.
