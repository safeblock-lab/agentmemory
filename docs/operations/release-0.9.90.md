# AgentMemory 0.9.90 — release draft

This draft describes the SafeBlock fork's source and observed limits. The release
repository is `safeblock-lab/agentmemory`; upstream attribution and the iii-engine
source repository remain unchanged. The paired 0.9.90 application and native
package with 21 required CLI capabilities is installed; application and native
compilation and the paired installation completed with exit code 0. Installation
completed on 2026-10-19 at 06:01:03 UTC. No tests were run. Publication remains pending and this draft
is still work in progress. Installation and the source features below do not
establish complete runtime acceptance or resolution of every incident.

## Graph recovery and startup

Automatic graph recovery uses native policy, source and lease guards, retained
continuations, and explicit availability. A disabled or unreported capability
does not authorize automatic continuation. Private-authority setup fails closed
when directory privacy cannot be established while allowing normal memory startup
to continue.

Native status reports the separate recovery reservation coherently as
`reservation: 1`. An expired, unspent `PREPARED` reservation can be adopted only
through complete native proof, when it is unclaimed and has no child slot. This
adopts the same single extra reservation; it does not add another allowance.
Epoch rotation does not reset consumption or authorize retry of an unknown
provider outcome. Valid retained child captures and frozen apply checkpoints
continue through their original identities and guards. Strict local recovery
cannot fall through to provider dispatch.

Reevaluation of an expired, unspent reservation requires fresh native proof
and retains the same single allowance and native budget. Terminal-projection
migration applies only to an incompatible cursor. Candidate errors are safely
attributed without exposing raw content.

Usage projection no longer gates recovery. Startup uses bounded paired reads
and schedules recovery after policy setup and function registration, before
index initialization. A single coalesced adaptive scheduler continues observed
progress, backs off when appropriate and holds unknown outcomes for
reconciliation. These changes do not prove repair or measured time savings.

The retained failed empty-XML operation now has a durable repair record reporting
`explicitEmptyXML`, zero nodes and zero edges. Its linked workflow is `COMPLETED`
with 10 nodes, 20 edges and five units, as observed on 2026-10-19 at 05:01:38 UTC.
That completion timestamp predates the latest installation, so these results
are not attributed to that installation. A second reservation was observed
adopted through its normal path with epoch 1. The latest retained observation
at 05:42 UTC still showed it expired and unspent with no private claim; final
workflow completion has not been established. Results after the latest
installation remain pending. Other context-loss and history-unknown cases remain unresolved.
No recovered or irrecoverable classification is inferred from uninspected backups.

## Native paging and storage maintenance

Opt-in `consistency: "keyset_live"` paging uses an opaque native cursor and a
finite initial position watermark. It tolerates concurrent writes and deletion
of an anchor without promising a historical snapshot. Strict paging remains the
default. Generation, scope, projection, page and byte limits remain enforced;
clients pass through native cursors without constructing them. See
[native state inspection](native-state-inspection.md).

COPY uses a finite seed and position bound, adopts legacy progress without
resetting it, and coalesces inline-write queue work. Unchanged-header no-ops and
the exact valid inline maintenance-control exemption avoid invalidating progress
through its own bookkeeping. Other data and reference changes retain their
native guards. Existing operation identities, leases and cursors remain
authoritative.

New COPY requests use at most 1,024 rows, processed in internal chunks of 128
with a 129-row lookahead. The 1-MiB byte limit, 2,500-ms work budget and 45-second
transport timeout remain unchanged. These bounds do not establish faster
execution or completed compaction.

Concurrent GC alternates finite seed traversal with a coalesced FIFO queue of
current candidates and retirement cascades. Reachability checks protect revived
candidates and shared references; deletion and queue acknowledgement share the
native transaction. Conflict-safe enqueues preserve queue state and progress.
GC completion requires a complete seed and empty queue in the same transaction;
later writes can reopen work. The scheduler continues actual progress with
backoff and an unknown-outcome hold, rather than interpreting generation or
cycle changes as completed work.

New collection requests retain row, time and byte bounds; already pending
requests retain their original bounds. Reclaim has a separate bounded contract.
Advancing a physical baseline requires terminal collection, completed layouts,
coherent reclaim evidence and a newer comparable physical sample. COPY and text
verification are advancing, but GC, reclaim and physical-baseline advancement
are not complete. Physical reduction and a complete maintenance cycle have not
been established. Filesystem housekeeping is not evidence of native compaction
or reclaim.

Bounded dashboard previews and explicitly marked automatic post-tool captures
are distinguished from interactive foreground activity. Manual and ambiguous
requests retain foreground priority. Workflow statistics retain explicit missing
coverage. Hook configuration changes cover 18 configurations; no measured time
savings or reduction in total agent cost has been established.

## Retained publication and inspection evidence

A retained consolidation manifest completed on 2026-10-18 with 32 candidates
examined, 22 published and 10 rejected. Recovery reused captured evidence without
resetting spent budgets. Exact outer JSON-fence normalization retains the raw
response and closed-schema guards. Versioned diagnostic projection preserves
the verdict and full rationale, marks the original reason-length contract as
unsatisfied, and runs after durable capture without another provider call.
Initial execution and replay retain distinct replay indicators.

Bounded invocation history retains failures without inventing logical job
completion or backfilling an evicted unknown operation. An earlier isolated-copy
inspection reported complete coverage, `integrity_ok: true` and zero checked
errors. Its scope is that copy; it does not establish current live integrity,
repair of the target graph job or acceptance of this candidate.

## Dated savings evidence

The fixed seven-day snapshot from 2026-10-11T15:30:24.833Z through
2026-10-18T15:30:24.833Z contains 123,591 emitted-context tokens and 3,159,393
processing tokens. Its explicit zero-proven-benefit scenario is -3,282,984 tokens:
`100 * (0 - 123591 - 3159393) / denominator`.
The denominator range is 9,018,514,251–9,021,673,644 tokens, yielding
-0.0364027% to -0.0363900%, approximately **-0.0364%**. This is a scenario
contribution, **not actual savings**, causal attribution or proof that actual
benefit is zero.

Semantic review found response-level usage deduplicated by response identity,
without reading cumulative usage counters; no numerical adjustment followed.
The finding does not establish complete coverage, billing accuracy, processing
overlap or causality. Context emission does not establish consumption or repeated
exposure. Available local Codex records and unavailable Claude coverage do not
constitute a full account bill. Global and causal net savings and net USD remain
unknown. The earlier dated study uses a different window and must not be merged
with this snapshot. See [dashboard savings](dashboard-savings.md).

## Acceptance before publication

The paired package is installed, but installation does not establish complete
runtime acceptance or physical storage recovery. The repaired empty-XML record
and linked completed workflow are distinct observed facts whose completion
predates that installation. Reevaluation of the remaining reservation and the
maintenance cycle remain pending. Final operational results must be incorporated
before publication. Export/import compatibility preserves older accepted export
versions. This release remains unpublished and work in progress; no claim of
complete incident resolution or measured causal savings is made.
