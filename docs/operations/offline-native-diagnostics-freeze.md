# Offline native catalog diagnostics freeze

Reviewed 2026-10-10 from the bounded engine source checkout and existing receipts. This records the `project_catalog_unknowns` source slice; it does not certify the full engine release set, an installed binary, or live catalog behavior.

## Source contract observed

The catalog stores logical names `sessions` and `summaries` and maps them to the physical SQLite scopes `mem:sessions` and `mem:summaries`. The diagnostics request accepts only those logical names. It returns the affected key and versions, logical and stored byte counts, and fixed reason codes; it does not return the stored record contents.

Pagination uses `source_key > last_key` with `ORDER BY source_key`, and requests `limit + 1` candidates for lookahead. There is no `OFFSET`. A v2 cursor carries the logical source, catalog status, indexed revision, epoch, catalog cursor, incomplete-row count, and last key. Each page also reads the current source revision and rejects a stale catalog revision before returning rows. Cursor bytes are capped at 32 KiB and keys at 4 KiB. A page contains at most 16 useful rows and its serialized response is limited to the caller's 64 KiB maximum. Individual source rows are checked against 64 KiB logical and stored byte limits before the encoded value is fetched and decoded.

The dispatch branch wraps the read in a three-second SQLite progress-handler budget. This bounds SQLite work cooperatively; it is not a hard wall-clock bound on operating-system I/O or non-SQLite processing.

The reviewed source includes the logical-to-physical mapping, keyset query, v2 cursor checks, row and byte limits, decode limits, and timeout. No additional source correction was demonstrated by this review. The new diagnostics module and its registration still need to be carried into the distributable patch before this source can be treated as a frozen release input.

Source references: logical scope mapping and diagnostics module registration are in `project_catalog/mod.rs` lines 2, 11, and 359; request cursor bound and variant are in `request.rs` lines 4 and 184; request validation is in `request_validate.rs` lines 163 and 172; dispatch timeout is in `dispatch.rs` lines 24–25; bounded-operation registration and progress handler are in retrieval `mod.rs` lines 186 and 217; key/row/response limits, cursor validation, keyset query, and decode checks are in `project_catalog/diagnostics.rs` lines 7–9, 40, 121, 147, 184, 201, 230, and 298. The test module is registered at `tests/graph_read_tests.rs` line 4; its six cases begin at lines 37, 81, 104, 164, 201, and 244 of `tests/project_catalog_diagnostics_tests.rs`.

## Existing test evidence

The existing `focused-native-catalog-03-keyset-tests` receipt records the library test target with 6 passed, 0 failed, 0 ignored, 2,153 filtered out, and exit code 0. Its six cases cover invalid bounds and stale revisions, missing or stale source rows, keyset pagination and stale cursors, two separate metadata-only unknowns, logical-source authority reasons, and size/encoding/JSON/authority reasons. The log shows a debug test-profile build, not an optimized release build.

The earlier attempt 01 has exit code -1 and is interrupted evidence. Attempt 02 ran the binary test target, selected 0 tests, and is excluded. Neither is counted as passing evidence.

The accepted 118-test native receipt and the separate 3-test observation-cap receipt predate this diagnostics change. Preserve them as predecessor evidence only: they do not prove the new request path, and the broad SQLite receipt is not accepted as unaffected across changes to shared request, validation, dispatch, and retrieval modules. The 3 observation-cap cases cover a separate observation module and may be reused for that unchanged behavior; they are not diagnostics coverage.

## Packaging and release gaps

The bounded source checkout has eight relevant paths: `retrieval/dispatch.rs`, `retrieval/mod.rs`, `retrieval/request.rs`, `retrieval/request_validate.rs`, `retrieval/project_catalog/mod.rs`, `retrieval/project_catalog/diagnostics.rs`, `retrieval/tests/graph_read_tests.rs`, and `retrieval/tests/project_catalog_diagnostics_tests.rs`. Four are staged additions and four are untracked additions in that checkout. The repository root treats `.codex-task/` as untracked, so its outer status does not describe these individual source changes.

The current `patches/iii-engine/0.22.1-state-pagination.patch` contains 126 paths, including predecessor copies of several shared retrieval files and `project_catalog/mod.rs`. It has no `ProjectCatalogUnknowns` request/dispatch branch, no `project_catalog/diagnostics.rs`, and no diagnostics test module registration. Its `project_catalog/mod.rs` is the predecessor version with only `authority` and `prepare` modules. The current manifest records the earlier canonical build and test receipts but has no catalog-diagnostics entry. Regenerate the patch and reconcile its manifest from the frozen combined source before the next optimized build.

The saved optimized-build log was last written on 2026-10-09 at 23:23 local time, before the diagnostics test receipt on 2026-10-10 at 01:22. Its successful 9m57s build is therefore predecessor evidence and does not include the reviewed diagnostics source. No diagnostics build, installation, or runtime probe is accepted here.

The dashboard owner has also identified a separate pending native `graph_metadata` source delta over the seven graph record scopes. Include that accepted change in the final patch and manifest before the single optimized build. This freeze does not modify the native storage diagnostics modules or claim that aggregate state bytes are graph bytes.

## Future single-build gate

After the memory incident is resolved and explicit reactivation is authorized, first freeze the complete source set, regenerate the engine patch, and reconcile the patch manifest. Then run the saved production recipe once at `BelowNormal` priority. The recipe targets `x86_64-pc-windows-msvc`, uses locked release mode and one job, and sets iii's codegen units to 1; its saved environment selects size optimization, stripping, no debug information, LTO disabled, and incremental compilation disabled. The command file does not set process priority itself, so the caller must provide `BelowNormal`.

The optimized build is a prerequisite for later installation and bounded real diagnostics. It does not establish that the two remaining catalog rows are resolved or that `project_vectors_prepare`, graph queries, temporal queries, or differential queries are correct. Those remain pending until installed behavior returns current, complete evidence from real state.
