# Retained reflection publication recovery

This source contract adds bounded publication observation and provider-free continuation of one retained normal reflection unit. Installation and live acceptance require the separately authorized runtime operator. These examples describe existing iii function calls; they add no REST endpoint or MCP tool.

## Durable identity and pending diagnostics

The publication helper resolves v1, then v2, then v3 using its normal identity preparation and exact state keys. An older retained checkpoint takes precedence even when a newer checkpoint exists. It persists an intention before dispatch and awaits the job reference write before invoking the executor. A failed reference write prevents dispatch; the retained intention subsequently remains unknown.

`captured.publication` contains the effective request ID, sanitized checkpoint observation, status and bounded host reason. `captured.evidenceRequestId` remains the supported-evidence linkage, written only after supported evidence and the current-manifest check. Pending identity never implies support.

The observation includes version, request ID, state, valid ISO creation time, raw-presence/overflow flags, parsed verdict status when available, and a diagnostic category with optional numeric HTTP status. It excludes raw response, prompts, sources, provider messages and credentials. OpenAI-compatible HTTP errors expose typed numeric status and a sanitized message; response bodies are discarded.

## Exact read-only observation

The operator first reads the exact retained job through normal iii state access and obtains its project, request ID, manifest ID, unit sequence and captured effect key. Do not derive identity from a generation effect key, scan `mem:state`, or invent a checkpoint key. Job state is sensitive; avoid logging captured response or source payloads.

Call the existing function:

```typescript
await sdk.trigger({
  function_id: "mem::reflect-status",
  payload: {
    jobId,
    publication: { project, requestId, manifestId, unitSequence, effectKey },
  },
});
```

The control must contain exactly those five fields. Observation validates the job/request/project/manifest/unit/captured-effect identity, prepares the normal historical/current identities, and reads at most three exact checkpoint keys. It makes no writes, parses retained response without saving a decision, and supplies no executor. Missing checkpoints return `unavailable/checkpoint_not_found`; identity conflicts fail closed.

## Provider-free replay of one normal unit

After separate runtime effect authorization, use the same exact retained control:

```typescript
await sdk.trigger({
  function_id: "mem::reflect-unit",
  payload: {
    jobId,
    replayOnly: { project, requestId, manifestId, unitSequence, effectKey },
  },
});
```

Replay rejects conflicting foreground/manual/selected controls and changed identities before mutation. It requires an existing manifest and capture, no generation in progress, and a normal job without legacy/global/successor construction. It never performs source selection, generation, batch enqueue, or executor-backed judging. It does not create a new publication intention or a successor job, change the original effect key, or revive a provider reservation.

- Retained `intended`: remains blocked with `dispatch_outcome_unknown`, including its diagnostic. No automatic redispatch. A new judge call needs separately scoped, accounted authority and a supported disposition; this path cannot supply one.
- Retained `captured`: resolves the exact durable response and saves the decision without an executor. Invalid, unsupported or uncertain evidence does not admit effects.
- Supported evidence: current source/graph/index authority is checked before linkage and first callback admission. Stale sources retain capture and admit zero effects.
- Started/completed callback or durable result: reconciles the original effect and result receipt. Existing admitted callback recovery preserves per-record effect idempotency; an acknowledged effect may resume despite source changes caused by its earlier partial effects.
- Successful selected unit: records its original result and advances its bookkeeping once. If more fragments remain, the job becomes `blocked/reflect_replay_unit_complete`, with dispatch disabled. No second manifest is selected or run.

Observe the exact job/result/callback again after any timeout or lost ACK. Never blindly retry with changed identity or create another request to conceal an unknown outcome. A completed job may no longer retain its manifest/capture; an old replay control then fails closed, and its result receipt is the reconciliation authority.

## Explicit fresh judge-only disposition

The existing `mem::reflect-unit` function also accepts a separate `judgeDisposition` control. This is an explicit operator operation, never automatic retry. It supports the retained v3 `intended`, absent-raw, `executor_error` checkpoint only. That original outcome remains unknown; supersession does not establish that the original request was undispatched.

Read the exact original job and publication observation first. Configure the trusted private provider budget for the deterministic fresh attempt before invoking the function. The operation ID is `fingerprintId("publication-judge-attempt", JSON.stringify([original.requestId, original.identity, original.createdAt, dispositionId]))`, using the exact retained evidence record. The new configured window and operation ceiling must match the control; the control supplies lookup assertions, never authorization. See [provider attempt budget](../operations/provider-attempt-budget.md).

```typescript
await sdk.trigger({
  function_id: "mem::reflect-unit",
  payload: {
    jobId,
    judgeDisposition: {
      unit: { project, requestId, manifestId, unitSequence, effectKey },
      dispositionId,
      captureFingerprint, // batchEffectKey of the exact retained generated response
      checkpoint, // exact original sanitized publication observation
      budget: { windowId, ceiling },
    },
  },
});
```

The unit and capture controls bind the original job, request, project, manifest, sequence, generated response and effect. Historical v1/v2 evidence cannot be bypassed. Current source authority is checked before fresh judging and again before effect admission. This path does not select sources, generate another response, construct a manifest, advance another unit, or reopen the old closed reservation.

The boundary validates the complete declared control before reading or claiming the job: a closed budget with text window ID and positive safe-integer ceiling, and a closed observation with required request/version/state/boolean fields, valid optional timestamp/verdict, and closed diagnostic category/status. Malformed controls return `invalid_reflect_judge_disposition` without any job or attempt write or budget invocation.

A separate intended attempt record is persisted first. Its `supersedes` and `budgetBinding` fields and the original record's `supersededBy` link are persisted before dispatch. The link retains the original checkpoint identity and selected budget binding. Original state, raw capture, diagnostics, creation time and provenance are preserved. `withProviderAttemptBudget(binding, run)` then requires the trusted configured operation/window/ceiling and accounts actual fetch retries and fallback. The new attempt ID is also the budget operation ID; the safe budget receipt can be read with `getProviderAttemptBudgetReceipt(operationId)`.

Concurrent or repeated matching controls resolve the selected attempt without calling a provider again. Changed controls are rejected. A fresh unknown outcome stays consumed and blocks publication; neither normal recovery nor replay dispatches another judge. Failure to persist linkage also dispatches zero calls; an already-created orphan intention is not revived. Configure accounting correctly before admission because a rejected scoped execution still leaves an intended attempt, conservatively unknown.

All later observation and replay follow the selected attempt. A late original callback can retain its raw response on the original checkpoint, but its verdict cannot replace the selected decision or admit another effect. Supported selected evidence links to the original reflection effect. Existing callback/result receipts preserve once-only publication and lost-ACK recovery. No installed or live success is established by the mocked source proofs.

## Original captured operation

The supplied runtime receipt identified `reflectjob_e7a9c192888162d1`, request `2448fc67-affe-4c50-9679-311344495a08`, generation retained at v16, applied zero, and the original two provider slots closed with zero available. The later operator handoff retained publication `publication-evidence_15b8d6ff76caa3b9`, v3/intended/hasRawfalse, created `2026-10-13T07:55:48.580Z`, diagnostic `executor_error`. No HTTP status or absence of dispatch was established. This source repair performed no live observation or mutation of that operation.

## Source evidence and runtime limits

Focused provider-mocked checks cover pre-dispatch reference persistence, intended zero dispatch, captured read-only observation and replay, exact supported linkage, legacy precedence, stale zero admission, lost result ACK without duplicate insights/reinforcement, identity rejection, missing-checkpoint behavior, shared summary/skill/consolidation helper behavior, and sanitized typed HTTP errors. Raw receipts are retained under `.codex-task/full-closure/reflection-publication-recovery-repair/`.

No build, installation, restart, live API/DB/provider call, checksum, unpause or protected-job mutation is part of this source repair. Runtime operator must verify the installed candidate and exact retained operation before claiming acceptance. An unresolved retained intention remains a real blocker.
