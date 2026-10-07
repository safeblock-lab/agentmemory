# AgentMemory 0.9.82 release acceptance

This candidate combines consolidation HTTP 413 and snapshot timeout repairs,
bounded terminal graph-job recovery, observation provenance, and native SQLite
storage format V2. Existing graph jobs retain their frozen extraction contract.

## Candidate verification

The engine patch is pinned to iii-engine 0.22.1 commit
`e7de3820d1e558f3762edf95e4440552444d48d3`. The source manifest binds its exact
patch, optimized Windows binary, size and seven required state capabilities.
Packaging rejects a V1 engine, an incomplete capability set and mismatched
binary or patch hashes. Read `package-candidate.json` and `SHA256SUMS` in the
release assets for exact candidate bindings.

The complete AgentMemory run recorded 2,703 passing cases, eight failures and
15 skips. Seven failures were five-second timeouts during host I/O contention;
one was an outdated current engine manifest freeze. After that correction, all
failed files and affected engine/package contracts passed: 52 focused cases and
eight graph-equivalence cases. The historical graph oracle pins are unchanged.
This is complete failure closure, not a claim that one full run passed cleanly.

The complete shadow measurement is documented in
[Native storage format V2](native-storage-format-v2.md). Its closed database is
9,095,467,008 bytes, compared with the 15,457,689,600-byte frozen source. All
logical data is retained. This is a dataset measurement, not a promised user
database size. Full-table parity, migration fixtures and package checks do not
replace final migration and live service acceptance.

Public assets contain the npm tarball, separate model, CPU runtime, notices and
upstream GPU acquisition metadata. No CUDA DLLs, databases, backups, logs or
private recovery modules belong in the package or Git. The unchanged CPU ZIP
retains embedded runtime metadata version 0.9.81 and inherited model/runtime
control evidence. This release does not rerun semantic evaluation campaigns.

## Installation and live gates

The user has authorized controlled pause/restart, installation and publication
after verification. Preserve configuration and rollback data. Check capacity
against the measured active source, target and required retained files before
stopping writers. Disable the watchdog during migration and verify owned
processes and ports have stopped. Never hash the running active database: the
native migration hashes its frozen source after writers stop.

Install the verified offline candidate with lifecycle scripts suppressed. The
installed version, every package file, engine and manifest must match the
accepted candidate. Migrate the final frozen SQLite source to a separate V2
shadow, retain the old source and compatible executable, verify readiness and
only then activate the new database. The older measured pilot does not contain
subsequent active writes.

Rollback must preserve writes accepted after cutover. The old executable rejects
V2 directly; restoring only the old application cannot reopen the new database.
Use the verified reverse converter or an explicitly verified write-preserving
recovery procedure. Large-database reverse migration remains a separate live
gate, since the pilot did not create another full reverse copy.

Require API version 0.9.82, viewer responsiveness, expected process ownership,
watchdog success and unchanged configuration. Exercise real consolidation and
snapshot behavior within existing authorized bounded requests, recording actual
work, explicit skip or error. A skipped consolidation leaves provider behavior
unexercised. A transient snapshot deferral must be followed by successful
snapshot acceptance when its read boundary is healthy. Inspect fresh logs for
HTTP 413 and timeouts, and measure database plus WAL after live checks.

## Publication gate

Candidate packaging eligibility does not assert production health. Commit and
publish only after Main accepts final migration, rollback, installed health and
real installer execution. Verify remote version/tag and published asset hashes
against the accepted candidate. Keep private evidence outside Git.
