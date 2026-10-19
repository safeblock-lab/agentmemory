# Graph recovery in the viewer

The project graph panel renders the automatic recovery state returned by `POST /agentmemory/graph/build`. The viewer uses the retained job and its server continuation; it does not construct replacement job IDs or provider retry arguments.

## Server contract

The build response retains `status`, `project`, counters, `graphJobId`, and the opaque `continuation` string. Its optional `automaticRecovery` projection has version `1`, `slotId`, `rootId`, `state`, `reason`, `captured`, `potentiallySpent`, `retryAdmitted`, `deadline`, `nextCheckAt`, `replacementJobId`, and `action`.

`deadline` and `nextCheckAt` are Unix timestamps in milliseconds or `null`. The deadline describes recovery expiration; the next check time controls scheduling. The viewer does not interpret the deadline as a retry cooldown.

The top-level `action`, when present, contains the fixed endpoint `/agentmemory/graph/build`, the same project, and the exact returned continuation. The viewer accepts an automatic action only when these fields agree with the response. It posts `{ project, continuation }` through the existing graph build endpoint without decoding or modifying the continuation.

For legacy stopped jobs, `automaticTerminalRecovery` can instead contain version `1`, `sourceJobId`, `resolution` (`local_replay`, `regeneration`, or `terminal`), `reason`, `nextCheckAt`, `replacementJobId`, and `action`. This projection takes priority when both recovery projections exist. It uses the same top-level action validation and scheduling. The viewer does not manufacture a native slot or root when the backend cannot establish one.

Both the existing `GET /agentmemory/config/flags` response and each graph build response include `graphAutomaticAvailability: { enabled, reason }`. The viewer consumes the latest received availability and displays available, disabled, or not reported explicitly. Only `enabled: true` permits automatic dispatch. A disabled result clears the recovery timer, preserves the job state and opaque continuation, and explains the pause using the safe reason code. The backend may disable automatic recovery when private authority is unavailable, changed, over a limit, not private, initializing, or unsupported by the configured provider. This status describes graph automatic recovery; ordinary memory service can remain available.

## Panel behavior

The panel distinguishes prepared recovery, a recorded provider dispatch, a complete captured response, completion, unknown provider outcome, expiration, failure, and terminal results. It displays the backend reason and replacement job when provided. A complete capture is reusable. Potential provider spending remains visible, and unknown spending does not imply that a new attempt is safe. Retry admission is displayed separately from completion.

`action` controls continuation even when the root state is terminal: a terminal root with a linked replacement can still require continuation. `action: none` ends automatic requests. Active recovery prevents starting a separate rebuild from the panel. After recovery ends, the ordinary confirmed build action remains available; the backend remains responsible for source identity and admission limits.

Legacy decisions identify the original stopped job and a linked replacement separately. The panel displays the backend reason, with explanatory labels for unavailable or incomplete source, unproven lineage, exhausted admission budget, cooldown, and other known terminal reasons. Ending a replacement chain does not change the original job's displayed recorded status. Raw reason codes remain visible, including unfamiliar reasons. A subsequent source unit can be continued explicitly only when the build response is `partial`, carries a continuation, and its recovery action is `none`.

For `wait`, and for an unknown outcome, automatic requests require a finite `nextCheckAt`. A missing check time leaves the retained state visible without guessing a polling interval. Other continuation actions can proceed after a one-second scheduling interval. The viewer allows only one graph build request at a time and retains the exact action between scheduled calls. A project selection change clears scheduled recovery and its displayed result. Navigation and rendering do not create a new provider invocation.

The ordinary build confirmation covers the source unit and its automatic recovery. Further source units remain explicit actions after recovery completes. Recovery does not ask the user to run repair commands.

## Unconfirmed requests

A missing or invalid HTTP result is an unconfirmed request outcome. The viewer preserves the last confirmed build result and displays the request issue separately. It stops automatically sending recovery requests after that loss rather than inferring that the job failed, that spending did not occur, or that a replacement is safe. Without a returned continuation it cannot reconstruct one.

The build panel describes its current operation. Dashboard workflow statistics retain their separate distinction between invocation failures and terminal jobs.

## Delivery status

This change is source implementation only. No browser session, runtime request, build, typecheck, test, or verification command was run for this integration. Automatic scheduling runs in the open viewer; it is not a claim that a closed browser continues recovery.
