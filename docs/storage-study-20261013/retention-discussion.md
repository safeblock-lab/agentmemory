# Proposed retention policy and unanswered savings questions

Discussion only, 2026-10-14. No deletion, TTL configuration, migration or service change has been activated by this study. Existing retention mechanisms remain in place; their presence must not be confused with implementation of this proposed cross-category policy.

## Proposed defaults

| Category | Proposed lifetime | Conditions |
| --- | --- | --- |
| Viewer events | 24 hours | Separate actual event expiry from whole legacy file removal and rollback protection |
| Tool inputs and outputs | 7 days | Promote useful facts first; keep necessary provenance; distinguish raw fields from durable observation content |
| Completed job bodies | 7 days after confirmed completion | No pending dependent job, replay, recovery barrier or protected reference; retain required receipt/checkpoint identity |
| Backup copies | 30-day rotation with count/byte caps | Preserve a usable current backup and required rollback baseline; Git object dates and file modification dates alone do not establish safe removal |
| Decisions, preferences and current useful knowledge | No automatic expiration | Normal updates, supersession and explicit user deletion remain separate |

These durations are initial design choices, not measured optimal settings or user authorization to execute cleanup.

## Expiration after use

Renewal is optional and must be explicit. For reusable data that has an inactivity limit, a meaningful task use may move the deadline forward. Traces, viewer events and completed recovery bodies should have fixed expiration or a hard maximum age so repeated reads cannot retain them forever. Active recovery is protected until its dependencies close.

Renewal changes the inactivity deadline, not the original creation/completion timestamp. With both limits, the proposed deadline is `min(lastMeaningfulUse + inactivityWindow, retentionOrigin + maximumAge)`. For completed job bodies the retention origin is confirmed completion; an active dependency prevents collection even after the deadline. A missing or untrusted usage timestamp must not invent a recent use. These are proposed semantics, not a claim about installed expiry behavior.

Maintenance scans, indexing, health probes, speculative retrieval candidates and this study's read-only census must not count as meaningful use. Delivery of selected context to a real task is a possible usage signal; merely visiting a row is insufficient. Whether existing access metadata has this meaning is unassessed.

Usage accounting should not mutate the canonical content version or make semantic indexes dirty on every read. A later implementation must use StateModule-owned usage metadata and preserve source-version, checkpoint, fencing and idempotency contracts. Scope and caller trust for renewal need a concrete design; clients cannot be allowed to keep every temporary artifact alive merely by labelling reads as useful.

## Quantified footprint versus actual eligible savings

Measured storage groups include about 215 MB of compressed observation values, about 482 MB of identified graph-dispatch/recovery body families, 1.39 GB of stream files and 5.23 GB of snapshots/temporary/Git files. These are group sizes, not expired-byte counts. Only part of observation storage is raw transient material. Protected and recent records remain unknown until dates, terminal state and references are reconciled.

Activating a policy alone immediately reduces the existing SQLite file by zero bytes. A migration/cleanup must remove eligible content and reclaim physical space separately. Rows deleted inside SQLite can make pages reusable without returning bytes to the filesystem. No cleanup or physical reclamation has been performed here.

File modification-time buckets describe whole-file idle age, not embedded event age, Git commit age or meaningful last use. Current live source cuts also differ between scans, so counts from separate intervals cannot be presented as one coherent exact expiration result.

Actual savings for the proposed 7/24-hour/30-day windows remain unquantified. There is no supported inference that TTL alone can reduce the current roughly 7.66 GB SQLite file to roughly 766 MB. Both retained knowledge and representation/index amplification need evaluation.

## Comparative hypotheses

An independently researched TencentDB-Agent-Memory note was received from another chat. Candidate ideas include incremental capture, store/update/merge/skip admission decisions, avoiding low-value extraction and complete ownership/reachability of derived artifacts. These are hypotheses to compare with existing local mechanisms, not measurements or proof that Tencent uses less disk. No external-source claim is used in this study's current byte estimates. No reply or external operation was authorized by that note.
