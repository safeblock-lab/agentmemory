# TTL cleanup operation, 2026-10-16

This operation installed the corrected r2 candidate and closed the admitted
native TTL scan on the local C-drive runtime. It recorded zero eligible effects
and zero TTL saving; originals, unique bodies, oversized rows and protected or
unowned backups remain outside that result. No remaining-space attribution was
performed. Official acceptance remains Main's responsibility.

The implementation and operational scripts are under
`.codex-task/storage-priority-20261015/native-candidate` and
`.codex-task/storage-priority-20261015/runtime-activation/ttl-cleanup`.
The durable native run cursor, cumulative counts and per-page receipt share one
transaction under `mem:storage-maintenance`. Workspace `pending.json` identifies
an uncertain acknowledgement; restart reuses its run ID and ordinal. It does not
allocate another deletion identity. `STOP` requests a pause between pages.

The runtime remains quiet: no provider, automatic recovery, queues, cron,
consolidation, decay or diagnostic sweep is enabled for the maintenance engine.
The prepared launcher preserves the complete 17-capability catalog and explicitly
disables both lesson and insight decay. Application context records meaningful
use only for selected delivered observations of an existing active session.

Installation preserves the previous pointer, binary and launcher in this
operation directory. Only the two SHA-256 fields required to package the new
engine manifest are generated; there are no additional checksum comparisons.
No tests, typechecking, global suites or optional health probes are executed.
Build warnings inherited from the preceding source remain unchanged.

Eligible categories and retained exclusions are defined in
[storage retention](storage-retention.md). In particular, unique tool bodies,
originals, unknown dates or ownership, protected backups, replay and audit
receipts, and rows above the bounded dependency decoder remain protected.
This operation must not be described as complete expiry of unique tool outputs.

After a terminal TTL receipt with logical effects and Main's operational
authorization, `collect-reclaim.cjs` can perform native bounded collection and
page reclamation. A zero-effect TTL result does not automatically start either
phase. Each request is recorded before dispatch and
each acknowledgement is appended to its effect ledger. An uncertain reclaim
acknowledgement stops for physical reconciliation; reclaim is not claimed to
have replay idempotence. Final database/WAL/SHM sizes use the same perimeter as
the preceding physical receipt, separately from logical deletion counters.

Initial native and application emission both succeeded with exit 0. The first package was
`agentmemory-source-20261016-ttl`; installation recorded 32 emitted application
files and a 26,376,192-byte native engine. Previous pointer, binary and launcher
remain preserved. The maintenance engine uses port 49159 with triggers disabled.

The first native TTL run was
`ttl-20261016-99b48d86-53d6-4da1-81de-a9f9daf178e4`, maintenance fence 2099 and
graph fence 12993. Its receipts remain preserved separately from the corrected
second pass below.

## First pass and source correction

The first pass closed 356 pages in 318,247 ms: 449,305 rows inspected and
retained, zero rows deleted, zero raw bodies stripped, and 4,592 rows retained
above the 128 KiB decoder bound. Native and client maxima were 2,403 and
2,423 ms. The run cursor and receipts are terminal; both leases were released
and no uncertain acknowledgement remains.

This first pass does not establish queue-body coverage. Source review found that
completed queue units remove `startedAt`, while the old technical-pruning path
required a timestamp on that child. The corrected path uses the completed
parent's validated terminal date, publication effect, completed snapshot marker,
recovery and active-job checks. It does not invent a child timestamp. The second
pass uses `ttl-cleanup/pass2`, retaining the first receipts.

Source schema checks confirmed numeric `RecentSearch.at = Date.now()` keyed by
session ID; observation timestamps are RFC3339 strings in `mem:obs:<session>`;
identity mappings use `observationId` and `identityDigest`; completed routines
persist server-generated `completedAt`. Native `database::row` decodes the stored
codec directly to that JSON object. Summary publication stores the same canonical
`effect.summary`, job fingerprint and completed marker that native pruning checks.
These are source checks, not live database attribution or additional tests.

Main stopped the already-started extra collector between acknowledged batches
because TTL had no logical effects. It completed 1,766 bounded calls and removed
725 orphan metadata records, but GC remained incomplete at generation 38150.
There were no reclaim calls. The private engine was stopped after lease release.
The first-pass comparable DB/WAL/SHM total was 7,309,668,720 bytes, versus
7,296,987,192 before this operation: an increase of 12,681,528 bytes. TTL saving
is zero; no attribution of the increase was performed. `physical-close.json`
records the exact file perimeter and stopped process. Previous parity evidence
remains referenced; no new parity probe was performed.

## Corrected second pass and Main review boundary

The native correction rebuilt successfully in 6 minutes 1 second, reusing the
existing release target and unchanged application assets. The installed package
is `agentmemory-source-20261016-ttl-r2`. Run
`ttl-20261016-c9760ad7-451b-4dd0-afcf-899e6b2e8a06` closed 359 pages in
282,686 ms with the parent-age correction: 449,305 rows inspected and retained,
zero deleted, zero raw bodies stripped, zero logical/stored bytes deleted, and
4,592 oversized rows retained. Native/client maxima were 2,402/2,421 ms.
No per-reason breakdown beyond oversized versus eligibility not established was
recorded. The empty effect-category map means no category produced deletions;
it must not be described as complete tool-body expiry.

The graph lease and maintenance fence 2101 were released, no pending operation
remains, and private engine PID 62340 was stopped. Following Main's direction,
the second pass started no collector or reclaim calls because TTL had no effects.
The previous incomplete GC cursor remains at generation 38150. DB/WAL/SHM total
7,310,598,848 bytes: 930,128 bytes above the second-pass baseline and 13,611,656
above the original comparable baseline. Claimed TTL physical saving is zero.
No analysis of the remaining storage distribution was performed.

Canonical local evidence is `pass2/ttl-outcome.json`, `pass2/physical-close.json`,
`pass2/installation.json` and the corresponding native/workspace effect receipts.
Acceptance mode remains `main-review`; the worker has not performed semantic
acceptance. Full application startup and periodic maintenance remain paused at
this Main acceptance boundary. The prepared quiet runtime omits queue and cron
workers and keeps state triggers disabled; it has not been started.
