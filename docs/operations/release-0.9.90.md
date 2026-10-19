# AgentMemory 0.9.90 — release draft

This draft describes the SafeBlock fork's source and observed limits. The release
repository is `safeblock-lab/agentmemory`; upstream attribution and the iii-engine
source repository remain unchanged. The latest paired application and native
update was installed on 2026-10-19 at 09:12:39 UTC, advertising 22 native
capabilities. Both prior processes completed normal CTRL-C shutdown; SQLite
was not copied and the watchdog was restored. The earlier native update at
08:00 UTC retained the application. At 08:37 UTC the application and engine
processes were unexpectedly absent. Startup was recovered at 08:39:41 UTC
after correcting the existing launcher's file-replacement handling, without
reinstallation or a SQLite copy. The cause of the earlier process exit remains
unknown. No tests were run. Publication and full runtime acceptance remain
pending. The features below do not establish resolution of every incident.

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

The installed application checks for pending graph work before reserving recovery capacity.
It retires superseded work only when canonical and native proof agree and bind
the successor fingerprint. A pending job or a newer-looking entry alone does
not satisfy that retirement proof.

Usage projection no longer gates recovery. Startup uses bounded paired reads
and schedules recovery after policy setup and function registration, before
index initialization. A single coalesced adaptive scheduler continues observed
progress, backs off when appropriate and holds unknown outcomes for
reconciliation. These changes do not prove repair or measured time savings.

The retained failed empty-XML operation now has a durable repair record reporting
`explicitEmptyXML`, zero nodes and zero edges. Its linked workflow is `COMPLETED`
with 10 nodes, 20 edges and five units, as observed on 2026-10-19 at 05:01:38 UTC.
That completion timestamp predates the latest installation, so these results
are not attributed to that installation. Final
completion of a second workflow remains pending. Its previously blocking job
completed, but its child failed with `STATE_GRAPH_AUTOMATIC_REGENERATION_REQUIRED`
while its cache still reported a child present. Automatic recovery of that
expired child before execution is implemented in the latest installed update,
but this workflow's outcome under that update remains pending. Earlier reservation
adoption does not establish completion. Other context-loss and history-unknown cases remain unresolved.
No recovered or irrecoverable classification is inferred from uninspected backups.

The new expired-child recovery path requires native proof bound to the same
child and its retained history. It preserves the existing allowance and spent
history, limits recovery to four attempts per hour with a 60-second interval,
renews the 120-second reservation before preparation, and returns typed waiting
outcomes instead of treating temporary ineligibility as completion. These
compiled and installed controls do not establish the affected workflow's repair.

Further source changes tighten revision and cache reevaluation around the
required native proof and reset the query cursor when that reevaluation changes
the selection. An old pending operation can be retired only through atomic
`notApplied` proof under the newer, stronger lease. Stale local state alone does
not authorize retirement, replay or another provider call. Sparse session
summaries also receive a dedicated prompt. These source changes have compiled;
their installation result and operational outcomes remain pending. Compilation
does not establish that a previously unsupported summary now succeeds.
The attempted update remains on a shutdown HOLD after its 210-second wait,
before any update file copy. Additional summary corrective-revision and bounded
index-drain diagnostic changes remain source work; optional pruning is
deferred until closure. They are not claimed as installed or accepted.

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

The installed native update separates first-page and continuation SQL for COPY,
removing the mode-selecting `OR` predicate. Its native build completed with exit
code 0. Installation does not establish complete runtime acceptance, and no
measured speed improvement is claimed.

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
coherent reclaim evidence and a newer comparable physical sample. The last valid
maintenance observation at 2026-10-19 08:41:30 UTC showed text layout in
`CUTTING` at ordinal 4,969. COPY seed, row traversal, GC, final reclaim and physical-baseline advancement
were incomplete; a later observation at 08:47 UTC recorded cursor advancement
without proving completion. Startup recovery after the process absence does not
establish resumed maintenance or completion. Physical reduction and a complete
maintenance cycle have not been established. Filesystem housekeeping is not evidence of native compaction
or reclaim.

Bounded dashboard previews and explicitly marked automatic post-tool captures
are distinguished from interactive foreground activity. Manual and ambiguous
requests retain foreground priority. Workflow statistics retain explicit missing
coverage. Hook configuration changes cover 18 configurations; no measured time
savings or reduction in total agent cost has been established.

## Coordinated shutdown

Shutdown draining tracks actual callback promises, FIFO work and maintenance
activity. Admission gates return HTTP/MCP status 503 while draining. A drain
that cannot finish within 180 seconds remains on HOLD rather than aborting the
SDK or forcing shutdown. These controls do not establish universal atomicity:
external direct SDK reentry into memory functions and detached asynchronous
work are outside the tracked callback boundary. These application controls are
installed; complete operational acceptance remains pending. Further CLI shutdown
changes were compiled and installed at 08:41:44 UTC through a CLI-only update,
without restarting the backend. No graceful-stop functional test was run;
installation alone does not establish operational acceptance. The watchdog's
original enabled state was restored at 08:43 UTC without forcing a run; the
primary task remained disabled.

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
-0.0364027146% to -0.0363899663%, approximately **-0.0364%**. This is a scenario
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

The application and native updates were installed and startup was recovered
after the observed process absence. Complete runtime acceptance, the cause of
the earlier exit and physical storage recovery remain pending. The repaired empty-XML record
and linked completed workflow are distinct observed facts whose completion
predates that installation. Reevaluation of the remaining reservation and the
maintenance cycle remain pending. Final operational results must be incorporated
before publication. Export/import compatibility preserves older accepted export
versions. This release remains unpublished and work in progress; no claim of
complete incident resolution or measured causal savings is made.
