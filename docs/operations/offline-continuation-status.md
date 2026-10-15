# Offline continuation status

Current state as of 2026-10-10: accepted offline source preparation and graph-contract fixture/doc alignment are integrated; installed real proof remains blocked. Main accepted the four predecessor source-preparation units. This document records their source state and the remaining gates; it does not certify installed behavior.

## Reconciled source

The native `state::storage` action `graph_metadata` reads cached counters for the seven durable graph scopes: `mem:graph:nodes`, `mem:graph:edges`, `mem:graph:snapshot`, `mem:graph:name-index`, `mem:graph:edge-key`, `mem:graph:node-degree`, and `mem:graph:edge-history`. `graph_metadata_prepare` builds the cache. `StateKV.graphStorageMetadata()` supplies dashboard reads. The authenticated existing `POST /agentmemory/storage/maintenance` route exposes the preparation action through its whitelist; no new endpoint or `src/index.ts` registration was added.

Preparation consumes at most 1,024 rows per call and may inspect one additional lookahead row. It accounts at most 1 MiB of consumed-row key metadata, rejects keys over 4 KiB, and uses a native three-second execution cap. This metadata limit does not cap graph payload reads because preparation does not hydrate or decode graph values. Dashboard reads return unavailable/null values for missing, incomplete, invalid, or stale metadata and never substitute aggregate state storage.

The proof matrix retains 56 semantic rows and the original 13-check mapping. Its 44 literal registered entrypoints are counted from the affected function modules (32 baseline plus 12 additional); the index, REST API, MCP and event files were reconciled as literal registration/consumer source. The 56 accepted source units in `.codex-task/application-regression-close/completed.csv` are a separate count. Neither count is a test count or proof of installed behavior. Dynamic registration and transitive consumer coverage remain unproved.

The native catalog diagnostics freeze retains its accepted focused receipt of six source tests. The receipt is from the debug test profile and is not installation or runtime evidence. Prior 118 native tests and three observation-cap tests remain qualified predecessor evidence; they do not cover diagnostics or establish overall behavior. The saved optimized engine binary predates catalog diagnostics and graph metadata; the engine patch, manifest and optimized package still require regeneration from the combined source under the separate build gate.

## Runtime and proof gates

The memory incident has been resolved, and the user has authorized local compilation, installation, reactivation, and tests; the provider gate remains closed. This source-preparation unit performed no build, test, database, provider, service, installation, queue, or scheduler operation. No installed real-proof receipt exists yet; real proof remains open for every affected function, and no cleanup chat is authorized until all required real proofs pass.

The provider gate remains closed with zero new calls. The historical 20-call ceiling applies only to the original 13 checks. A separate proposal for six more calls remains `NOT_AUTHORIZED`. Preserve cursor 5, keep recovery, auto-compress, and consolidation paused, and protect jobs `graphjob_mv0567t0_7c392765f08d` and `graphjob_muzu6a4k_c7aa10a891eb`. Do not restore the five deleted tests or remove historical data.

Catalog observations remain historical: sessions were ready at cursor 361; summaries were pending at cursor 448 with two incomplete rows. Do not treat these as a fresh read or as resolved unknowns. Reflect reuse remains six historical real sources and four insights plus exact terminal replay; it does not prove fresh generation.

## Remaining source and evidence gaps

The dashboard metric fixture now models the complete seven-scope native aggregate and covers unavailable/stale metadata, read errors, measured empty zero, incomplete scope coverage, and exclusion of the prior full-state category sum. The viewer fixture supplies the native source and coverage fields. No tests were run for these source changes. The predecessor protocol and graph-bytes notes now identify the accepted `graph_metadata` read, bounded `graph_metadata_prepare` action, existing preparation route, and remaining runtime/package gates. The matrix row is `SOURCE_UPDATED_UNEXECUTED`; this status records no test or installed behavior.

The SQLite adapter has focused regression source in `.codex-task/bounded-engine/source/engine/src/workers/state/adapters/sqlite/graph_metadata_tests.rs`, registered from `tests.rs`. It covers missing versus legitimate empty metadata, exact seven-scope metadata-only SQL reconciliation and technical/other-state exclusion, bounded two-row pagination and resume, write invalidation during preparation, post-ready overwrite growth/shrink/delete with tombstone accounting, and external revision mismatch. The oracle reads aggregate metadata only and never selects graph BLOB values. These synthetic isolated-adapter cases remain uncompiled and unexecuted; they do not change the 56 semantic rows, original 13-check mapping, or runtime-complete status.

Codebase Memory reported a fast index from 2026-10-03. It marked several source files stale or untracked and excluded `docs/`. Literal source inspection therefore reconciles the specific index, API, event, MCP, StateKV, dashboard, and native action files only. It does not establish exhaustive dynamic, endpoint, event, or transitive reachability.
