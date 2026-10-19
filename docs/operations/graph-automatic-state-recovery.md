# Automatic graph recovery in native State

This document describes the source contract `state::graph_automatic_recovery_v1`. This worker has not compiled, installed, exercised or checked it. Main's earlier builds reported unsupported `Option<u64>` SQLite reads and `u64` SQL parameters. This revision uses checked conversions between SQLite `i64` and unsigned counters, including unit/cursor/delivery values and response byte counts, and awaits Main's emission. Workflow integration and live activation belong to Main.

## Installation authority

Automatic recovery uses a service authority installed once alongside the configured SQLite State database: `graph-automatic-authority.json`. The path is derived natively from the real database path. Requests cannot supply a path or an authority declaration.

The authority file and its directory use the existing native provider-budget private-file boundary: owner/private ACL checks, no symlink/reparse target, a 64 KiB limit, and opened handles retained through COMMIT. Windows sharing prevents write/delete while the declaration is used. Unix uses the existing owner/mode and `O_NOFOLLOW` checks. The original `provider-budget-grants.json` path, declarations, counters and per-operation approval checks are unchanged.

Main must derive service profiles from the actual configured provider, request builder and parser contract, install the secret-free authority using that private boundary, and integrate the workflow. No authority file, permissive default profile or per-incident grant is created by this source unit. Missing, malformed, non-private or incompatible authority fails closed before any fresh dispatch. Status, local response reads, completion, cleanup and deadline reconciliation do not need authority for another external effect.

The typed declaration is `GraphRecoveryServiceAuthority` in [the State bridge](../../src/state/graph-automatic-recovery.ts). Its version is `1`; it contains between one and 32 profiles. Each profile fixes:

- A unique immutable `id`, canonical `POST` endpoint, model, timeout and response byte ceiling.
- `value_name_template` and optional `unit_pointer`. `{delta}` comes from the native checkpoint; `{unit}` comes from the selected field in the native job record. The caller does not supply proof of that field.
- The exact parser contract: `id`, `version`, `parser_version` and `schema_json`.
- `request_template`, including model, system text, provider options and response schema. Only the configured string source positions are represented by JSON `null` placeholders.
- Ordered source segments with exact request pointers, native source rules, prefixes, separators and suffixes. Source scopes are `job_inputs` or `provider_values`, resolved within the current job. Source keys support `{delta}` and the native `{unit}`; `capture:{part}` expands to every initial capture part in order.

Before accepting a preparation, native code compares all fixed fields with an installed profile, replaces only its configured source strings with `null`, and compares the whole resulting request with the exact template. It also compares the source selectors and wrappers with native rules. Arbitrary endpoint, model, options, schema, target slot, source pointer or prefix cannot become authorized merely by having valid JSON. Used profile declarations are pinned by ID in `engine_graph_auto_authorities`; reusing an ID with changed contents is rejected. An installer can append a distinct versioned profile without resetting existing slots or policy counters.

Credential headers, API keys and runtime authentication are excluded from captured bindings and profiles. Structural secret-field rejection is implemented; the service must also keep secrets out of ordinary prompt text. Credentials remain in the existing provider configuration and are attached only by the future service dispatch path.

## Native policy and identities

The engine pins policy version `1` and its complete declaration. Its limits are one additional automatic attempt per original slot, four automatic admissions across the service in a rolling hour, a global 60 second admission cooldown and a durable 120 second deadline. These limits are not request parameters.

The native slot identity binds generation, job ID, checkpoint delta and value name. Original attempt `:0` and additional attempt `:1` are separate durable rows. Regeneration reserves the same root `:1` admission, produces a new linked job identity, and cannot create a fresh retry chain. The original unknown intent, potential spent dispatch and original provider-budget records remain intact. The root status continues to describe the original attempt; a linked new job has its own slot and captured response.

Admissions, attempts, consumed flags and audit events are retained. Completion, expiry, missing ACKs, process restarts and cleanup never refund an admission or dispatch. The rolling hour is calculated from durable admission timestamps rather than a resettable hourly counter. The service clock cannot decrease; a wall-clock rollback closes active deadlines and blocks fresh effect admission until the clock catches up.

An additional attempt with `unknown`, `expired` or `failed` state and no complete capture closes its slot as `GRAPH_AUTOMATIC_SLOT_EXHAUSTED`. `finish`, deadline reconciliation, `status` and `admit` settle this state natively. Settling an exhausted attempt does not need new provider authority and does not change its unknown/spent record. An additional attempt with a complete capture remains available for local reuse instead.

The implementation promises at most one fresh native permission per attempt. It does not promise exactly-once delivery at the provider. An intended effect with no complete response stays unknown and potentially spent, even when a network call might never have begun.

## Wire and workflow integration

All commands use `state::storage` with `{ action: "graph_automatic_recovery", request: { command, ... } }`. `StateKV.graphAutomaticRecovery` and `createGraphAutomaticRecoveryTransport` perform request and response validation. There are no new MCP tools, REST routes or KV scopes.

`GraphRecoveryAnchor` contains `guard`, `job_id`, `job_version`, `checkpoint_version`, `delta_ordinal`, `value_name`, `manifest_version` and `manifest_attempt`. Mutations validate the current lease owner, generation, fence, job/checkpoint/manifest row versions and delta identity in the native transaction. Preparation, admission, dispatch, regeneration and cleanup require an unapplied staging manifest. A manifest with application/template/result metadata cannot be cleaned through this contract.

The job selector accepts existing `graphjob_…` identities and `graphbuild_` followed by exactly sixteen lowercase hexadecimal characters. Supporting an existing build identity does not manufacture a new job or relax its native guard, source capture or authority checks.

`GraphRecoveryBinding` contains exact secret-free `request_json`, provider endpoint/model/limits, parser contract and ordered source segments. Each source has a real job-scoped State row, version and JSON pointer. Native code reconstructs the initial complete job capture, validates its content, and binds the actual part versions. Frozen provider sources are reconstructed from all parts and checked against their complete reference. The native source and binding digests are generated after these checks; caller hashes are never accepted as proof.

Bindings are limited to 6 MiB, exact request bodies to 4 MiB, and aggregate native decode work to 64 MiB. A source beyond the applicable work limit is retained and refused with a stable limit outcome; it is not classified as an invalid derivative to delete.

| Command | Result and integration requirement |
| --- | --- |
| `prepare` | Persist exact authorized request/source/contract before the first effect. Requires a real `not-dispatched` intent and absent complete target output. It cannot retrofit an unknown legacy request. A linked new job also supplies its native `regeneration_id`. |
| `dispatch` | Compare the supplied binding with the native preparation and installed profile, recheck current sources, and atomically mark this attempt consumed/intended. Only a fresh post-COMMIT ACK permits an effect. |
| `capture_part` | Persist one immutable response string part, at most 128 KiB, under the native attempt identity. Duplicate identical parts are harmless; conflicting contents are rejected. |
| `capture_complete` | Require all sequential parts, compute the native response digest and publish the complete capture reference atomically. The configured response bound cannot exceed 8 MiB. |
| `read_part` | Read only a complete native capture for local parsing/persistence. A complete response remains available after a local processing deadline or failure. |
| `finish` | Mark local completion/failure or an unknown provider outcome. Completion requires a complete capture. A consumed effect without capture cannot be relabeled a known local failure or refunded. |
| `admit` | Reuse complete output locally, or reserve the sole additional attempt for a compatible captured unknown slot. Policy rejection, incompatible binding and incomplete legacy capture have stable reasons. |
| `regenerate` | Reserve the root's remaining automatic admission and return `regeneration_id` plus `new_job_id`. The workflow creates a new job with a fresh complete capture and prepares it under that link. The old request is never invented. |
| `cleanup` | Drain owned invalidated derivative ranges in bounded pages. Continue while `cleanup.pending` is true. |
| `reconcile` / `status` | Reconcile elapsed native deadlines and return durable status. `status` selects a known slot ID without a live graph lease. It cannot grant a dispatch. |

Every response contains `retry_after_ms: number | null`. This is an absolute UTC timestamp in milliseconds, separate from the attempt's `deadline_ms`. For a cooldown/global refusal it is the maximum of the durable clock, last admission plus 60 seconds, and the fourth most recent still-relevant admission plus one hour. Taking the maximum covers both global constraints even when the reported reason is cooldown. A stale refusal whose constraints have elapsed returns the native current clock, allowing immediate reconsideration. All other reasons, including slot exhaustion, return `null`. A cooldown/global slot refusal may be reconsidered at that exact native timestamp without resetting any credit; the caller must not invent a fresh 60 second wait.

The workflow must run the preparation/admission preflight before `beginDelta` replaces a manifest attempt. Before cleanup/admission, it must stop and drain the old writer and heartbeat. Cleanup rotates the native fence as well as the manifest attempt; the response supplies the authoritative updated anchor. Adopt its guard and manifest version, discard buffered shadows and resume under that identity. Replaying a request with the old fence is rejected. A lost ACK is recovered with `status` and the known slot ID (`graphRecoverySlotId` is a selector helper), followed by refreshed State reads where needed.

For a fresh `dispatch` response, call `graphRecoveryDispatchPermit` once, immediately before one raw provider call. The bridge accepts only the original validated fresh response object, freezes it and consumes its local permit once. The permit caps timeout by the remaining durable deadline. Do not replay serialized ACKs, reconstruct permissions from receipts, retry a raw request, redirect, repair, fall back to another provider or invoke an additional compaction under that permission. On ambiguity, retain unknown/spent and reconcile.

The future workflow owns provider invocation, response capture, local parsing and ordinary job terminalization through existing graph commit APIs. Native policy terminal states do not by themselves publish graph data or complete the existing workflow checkpoint. On restart, inspect the pending graph job and reconcile its slot before doing work. After a valid complete response, resume locally without another provider admission. Drain cleanup before leaving the staged job. These integration steps are intentionally outside this source leaf.

## Cleanup and preserved evidence

Native incomplete response parts have explicit attempt ownership. Their incomplete references are invalidated before the parts are deleted. Complete captured responses remain intact, including complete output whose subsequent local processing failed.

For graph staging derivatives, native code requires the exact job, generation, checkpoint, delta, manifest version and attempt with no application/template/result metadata. In one transaction it changes the manifest attempt, resets its shadow reference count, records the invalidated prior attempt and advances the native writer fence. Only then does it drain the old attempt's job-scoped `row-order` references, followed by its `row` shadows. Each page retains a durable cursor. This ordering makes old references unreachable before payload removal and fences late writes from the old context.

An incomplete legacy frozen value reference is invalidated only when native reconstruction demonstrates missing/invalid parts or a content mismatch. A resource-limit or unknown decode failure is not proof that a complete reference is invalid. Legacy part rows without provable attempt ownership are retained. The cleanup receipt reports `retained_unproven: true` conservatively. Inputs, manifests, valid published graph rows, memories, provider intents, cost history and existing grants are not deleted. Removal uses native State writes/tombstones and private native-part tables, not a standalone database connection or file deletion. File shrinkage is not implied by logical cleanup.

## Durable outcomes and failure handling

Native attempts move through `prepared`, `intended`, `captured`, `completed`, `unknown`, `expired` or `failed`. Slot-level refusals render as `terminal` with a stable sanitized reason. Relevant reasons include:

- `GRAPH_AUTOMATIC_LEGACY_REQUEST_MISSING` and `GRAPH_AUTOMATIC_LEGACY_COMPLETE_OUTPUT`.
- `GRAPH_AUTOMATIC_SOURCE_CHANGED`, `GRAPH_AUTOMATIC_SOURCE_INCOMPLETE`, `GRAPH_AUTOMATIC_SOURCE_LIMIT`, `GRAPH_AUTOMATIC_BINDING_INCOMPATIBLE` and `GRAPH_AUTOMATIC_INTENT_CHANGED`.
- `GRAPH_AUTOMATIC_PROFILE_NOT_APPROVED` and `GRAPH_AUTOMATIC_AUTHORITY_CHANGED`.
- `GRAPH_AUTOMATIC_SLOT_EXHAUSTED`, `GRAPH_AUTOMATIC_GLOBAL_LIMIT` and `GRAPH_AUTOMATIC_COOLDOWN`.
- `GRAPH_AUTOMATIC_PREPARED_DEADLINE`, `GRAPH_AUTOMATIC_DISPATCH_DEADLINE`, `GRAPH_AUTOMATIC_LOCAL_DEADLINE`, `GRAPH_AUTOMATIC_PROVIDER_UNKNOWN` and `GRAPH_AUTOMATIC_LOCAL_FAILED`.
- `GRAPH_AUTOMATIC_REGENERATION_READY`, `GRAPH_AUTOMATIC_REGENERATION_LINKED` and `GRAPH_AUTOMATIC_REGENERATION_DEADLINE`.

Authority/transaction/guard failures use sanitized `STATE_GRAPH_AUTOMATIC_*` or existing State transaction errors. No raw prompts or provider responses are placed in audit events or error messages. A fresh ACK is returned only after the SQLite IMMEDIATE transaction commits with FULL durability; duplicates and status responses have `dispatch_allowed: false`.

## Compatibility, activation and rollback

### Legacy terminal jobs

`StateKV.graphAutomaticRecovery({command: "terminal_recover", anchor: {guard, job_id, job_version, checkpoint_version}})` has a separate typed response. The caller supplies current versions and a live native guard; it supplies no old ordinal, value name or reconstructed request. The response has `resolution: local_replay | regeneration | terminal`, nullable `original_slot`, a non-dispatching `recovery` status snapshot, nullable `replacement` and the native absolute `retry_after_ms`.

Native code requires an extraction job with `state: failed`, `recoveryStopped: true`, a complete current checkpoint with its native receipt, and the same eligible generation as today's control/guard. A graph reset therefore cannot revive a previous generation merely because its capture remains. The original source generation still defines the root identity and its consumed allowance.

For grouped work, the historical slot comes from retained `continuationState.activeUnit.resultOrdinal` and its unit ID. Native code reproduces that ID and the prompt from the original page, cursor and envelope, and binds the retained page versions and digests. A later applied finalizer is left intact. Applied deltas are proved using their final native receipt and checkpoint version: a persisted manifest can remain `freezing`, so its phase alone is not proof of application. Without grouped continuity, only a unique compaction slot at delta 3 can proceed, after demonstrating that earlier templates wrote only job-local preparation and that no earlier provider intent exists. Other ambiguous slots return `SOURCE_UNIT_UNPROVEN` before admission.

The private link pins the complete original capture, retained page plan and original selection. For selected work, native validation checks the original dispatch intent, source fingerprint, session project, captured observations and their current retained fields. A replacement capture must be identical after excluding only `graphJobId` and `replacementOf`; remaining pages, envelope and cursor remain bound. Selection constraints are inherited through native lineage, including children without `selectedOrigin`. Unprovable selection returns `SCOPE_UNPROVEN` without effect.

Existing native allowances and exactly correlated private additional-attempt receipts/reservations are preserved. Matching private context that cannot identify the same slot safely closes as `HISTORY_UNBOUND`; opaque operation names are never reconstructed. Exhausted reservations do not obtain another root allowance.

A complete retained frozen string creates a local link under the same root `:1` reservation with no HTTP permission. The first delivery records descriptive `job.automaticLocalReplayDelivery = {deltaOrdinal, valueName}`. Before any later fresh unit, native code proves the delivery's applied receipt, frozen logical string digest and original unit name. It then replaces its private `local:<new_job_id>` claim with the delivered slot. Unit IDs are bound to one slot, and subsequent cursor positions must equal the previous unit's computed end, with an applied receipt before advancement. A local child failing in the same unit closes as `LOCAL_FAILED`; it does not form a replay chain.

A provider replacement must include the native `regeneration_id` when claiming its first reserved slot. Omitting the token on an unclaimed child is rejected, rather than creating another original allowance. Future untouched units use their normal original slots only after the native lineage and continuity checks.

The terminal command never resets the original job, finalizer, checkpoint, published prefix, failed dispatch intent, original provider intent or cost history. Cleanup for this command does not delete legacy data. Rootless refusals are durable and version-bound; no synthetic old slot is created when derivation fails.

The engine changes add private `engine_graph_auto_*` tables and one storage action. They keep the existing State database format version, graph wire, existing provider-budget grants and older job records. The bridge fails closed with an engine that lacks the new action. This unit does not install configuration, compile either runtime, restart a service, call a provider or change production data.

The bridge calls the existing `state::storage` RPC directly and validates the response capability. Main identified that normal CLI engine upgrade selection also needs this capability advertised as entry 18; a separate owner handles the engine CLI and build-script arrays. Those files are outside this State leaf.

Main must compile/emit the authorized sources, close its installation/database recovery gates, install the matching service authority derived from the real builder, and integrate the workflow before activating automatic dispatch. Repository tests, typechecking, hashes, artifact reviews and health probes were not run; the user's explicit execution preference prohibits them unless requested. No runtime compatibility or recovery success is claimed here.

For rollback, first disable the new workflow dispatch path. Preserve the authority declarations, all new tables, root links, original unknown intents, captures, admission history and consumed counters. The recovery unit must include the native database and its private accounting tables; an application JSON export alone does not preserve this authority/history. Restore the prior application/engine sources or binaries under Main's authorized recovery procedure. Do not drop tables, rewrite identities, delete unknown effects or reset clocks/budgets to make a retry appear available. Captured responses can still be handled locally through the corresponding trusted integration. The source-only worker makes no live rollback or backup changes.
