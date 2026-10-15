# Graph Browse preparation budgets

This document describes the source candidate for bounded Graph Browse preparation. Installed acceptance remains a separate runtime operation; the old installed timeout has not been reproduced or explained by isolated measurements.

## Work and delivery limits

`prepareGraphBrowse` submits `index_prepare` for `mem:graph:browse` with 256 rows and 1 MiB of work. The graph-only native request ceiling is 1,024 rows and 160 MiB of work. `prepareGraphBrowse(kv, project, { maxWorkBytes })` accepts an explicit work budget from 1 KiB through 160 MiB; it does not retry or raise that budget automatically. Global retrieval response limits stay at 8 MiB, and display reads stay at 64 KiB. These are preparation work limits, separate from the unchanged 64 KiB public display-response limit. Existing project catalog, project vector and scoped read request validation retains its public limits.

`index_prepare` participates in the existing controlled SQLite preparation path. One 60-second budget covers queue admission, WAL checks, BEGIN, retrieval, COMMIT and acknowledgement. The production SDK request allows 70 seconds. Cancellation does not create a new deadline at each stage. Delivery failure can follow a committed cut: reconcile the durable checkpoint before another mutation.

Prerequisite node/edge, semantic, source, catalog, project-vector and scoped preparation receives the same byte allowance. Metadata selection precedes source decoding. Source work charges stored plus logical bytes for each actual decode, not the compact projected JSON response. Catalog preparation no longer permits the first row to bypass the budget. An oversized catalog row returns zero progress and `required_bytes` without advancing its cursor.

The cut also installs a synchronous thread-local decode guard at the owned database boundary. Both SQL `state_json` and Rust `decode_value` admit stored-plus-logical work and check the active deadline before decoding. Nested source validation and canonical embedding restoration therefore share the same allowance. Exhaustion rolls back the preparation savepoint, returns bounded failure, and consumes no prerequisite cursor. The guard clears on all Rust return paths. It does not replace the existing transaction/deadline owner.

Scoped restoration checks source and canonical embedding metadata before restoration, and existing lexical byte admission precedes reading lexical text. Project/source versions, semantic epochs, catalog epochs and current session authority remain required. The native codec supports at most 16 MiB logical bytes per row and stores no more than that logical size. One full decode therefore charges at most 32 MiB. Source eligibility, validation and lexical extraction plus canonical embedding restoration can charge up to 128 MiB per restored row; 160 MiB leaves a bounded margin for lexical/authority work. The work-duration formula at 1,024 rows and 160 MiB produces 59,696 ms, below the existing 60-second floor. This arithmetic is not proof that any production cut will finish before the deadline; cancellation remains authoritative.

## Evidence and limits

The retained original diagnostics are under `.codex-task/full-closure/graph-budget-throughput-repair/measurement.*`. Their 512-node fixture reached ready in 38 cuts at 16 rows; the 50,000-scope source lookup used indexed scans. Those observations do not justify a SQL rewrite or establish the old installed timeout cause.

Two retained Browse failures demand 100-KB and 15-MiB original decoding with a declared 64-KiB preparation work budget. Twelve other Browse regressions passed. Their old request contract is incompatible with actual-work admission. A request-only fixture patch is supplied for serial integration: the two large-original cases explicitly request 160 MiB, keeping every supported-original assertion intact. The original failure receipts remain failures, not passing evidence. An owned isolated fixture checks low-budget refusal and large-budget success separately.

The final stored-plus-logical512-node fixture reached ready in38/8/6cuts for16/256/1024row batches. Maximum measured cuts were3.5/23.4/44.8ms respectively, supporting the256row default. An explicit160MiB work fixture prepared15MiB originals in7cuts (~515ms total) and verified unchanged readback; sampled whole-process peak working set was100.3MB including setup. These are isolated observations, not installed latency or RAM acceptance. Caller checks passed10/10 and final focused native source/provenance/stale/lost-ACK/Browse/admission checks passed; the two original large-source test request updates remain integration work.

Current source checks and measurements are retained separately as `repair-70a0-*` in that directory. Consult their raw output and exit receipts before treating this candidate as accepted. Process memory observations cover the whole isolated native test process, including fixture setup; they do not establish production RAM acceptance or per-batch retained memory.

## Runtime continuation

The integration owner must publish the canonical native patch and manifest plus any shared declaration deltas. This source front does not install or restart the runtime.

The sole authorized runtime operator should first read durable state for the original unknown graph operation. Preserve both protected graph jobs and the retained reflection/summary identities and closed reservations. Do not replay an unknown mutation blindly. Apply the combined candidate only through the already authorized integration/runtime procedure.

After reconciliation, perform one controlled preparation cut for the exact project, record phase/progress/revision and elapsed time, then read its checkpoint before continuing. Stop on `continue:false`, a resource error, changed epoch or authority failure. A larger work budget is bounded by the 160-MiB graph-only ceiling and requires a deliberate caller/operator choice; do not silently bypass an oversized source. A lost acknowledgement requires checkpoint reconciliation rather than automatic retry.

Installed graph readiness, the prior recent-history readiness and representative production RAM/fairness still require the runtime owner's original acceptance procedure. Isolated source tests cannot substitute for them.
