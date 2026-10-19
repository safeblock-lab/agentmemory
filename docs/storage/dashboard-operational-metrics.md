# Dashboard operational metrics

## Workflow terminal attribution

The source contract for new workflow terminals requires `agentId: string | null`
in TypeScript and an explicit `agent_id` string or null in native retrieval.
Null means no agent identity is available. A workflow-level terminal increments
the global projection and minute bucket once; a valid string additionally
updates that agent's projection and bucket. Null never creates an individual
agent bucket. The global `lastRun.agentId` can therefore be null. Unit-level
terminals retain receipts without updating projections or counters.

An omitted native `agent_id` remains invalid. Present strings must be nonempty
after trimming, at most 256 UTF-8 bytes, and free of control characters; other
values are rejected. No synthetic identity substitutes for missing attribution.
The receipt key remains `(producer, execution_id)`: exact replay returns the
original receipt without another increment, while any payload change conflicts.
Existing string-agent payload serialization remains unchanged.

Compression persists explicit null only for new terminals. Cached historical
receipts without their own `agentId` field are not resent or normalized; their
coverage gap remains unknown. This change does not rewrite records, backfill
counts, clear sticky invalidation, reset coverage, or certify complete producer
coverage. The retrieval timeout remains 1500 ms.

Workflow warnings expose only closed diagnostic classes and codes. The installed
SDK's `InvocationError` class, `TIMEOUT` code and native
`STATE_TX_INVALID_REQUEST` code are recognized, without emitting error messages,
stack traces, payloads or private identifiers. These source changes have not
been compiled or exercised against a running engine.

## Operational summaries

`GET /agentmemory/status` includes an `operationalMetrics` object. The endpoint reads persisted summaries only; it does not run storage diagnostics, reclaim pages, activate cleanup, or scan graph records.

`failures24h` covers failed processing operations recorded by `recordOperation` in `[intervalStart, intervalEnd)`, within the configured agent scope. It does not count every HTTP, MCP, native-engine, or hook error. `intervalEnd` is the start of the current minute, so the displayed period ends at the last complete minute. The service persists atomic minute-bucket increments in a bounded state record and removes expired buckets in the same update. On the first status read, a missing counter gets a small durable coverage marker; the value stays `null` until that marker covers a full 24 hours, then a quiet window can be reported as zero. A failed counter initialization or update leaves coverage unavailable/invalid and raises a sanitized warning; the bounded recent-operation feed remains a separate source.

Failure counters use a SHA-256-derived key for the configured agent ID, so the raw ID is not stored in the counter key. With no agent ID, the key is `global`. This mirrors the configured instance scope used by the status activity feed.

`graphStorage` reports `storedBytes`, `logicalBytes`, and `rows` from ready, complete native metadata covering all seven durable graph scopes. `sampleAt` and `coverage` identify the retained measurement. Missing, stale, malformed, or incomplete metadata produces `null` values rather than an assumed zero; status requests never prepare metadata. A complete measurement of an empty graph can report zero. SQLite database size, WAL size, retrieval-index storage, technical graph jobs, and other state categories are not relabelled as durable graph bytes.

`engineMemory` reports the resident working set of the `iii.exe` process listening on the configured local engine port. On Windows the probe verifies a single listener, the executable basename, and a stable process creation identity around the memory read. It uses a fixed PowerShell probe with a one-second timeout, an 8 KiB output cap, a 30-second cache, and single-flight execution. Remote engine URLs, unsupported platforms, ambiguous listeners, permission errors, and process changes report `residentBytes: null`. The response omits process IDs, executable paths, and command lines.

`lastSuccessfulCompactionAt` changes only after reclaimed pages or a blocking migration that measurably reduces allocated storage. Deferred and idle polls preserve the previous value. `automaticCompaction` reflects the background-maintenance pause setting and the last complete diagnostic's SQLite `auto_vacuum` mode. It includes that mode so a paused reclaimer still shows whether a migration is also needed. A mode that needs migration is not reported as enabled.

Context selection reduction uses same-operation eligible and emitted token counts with `o200k_base`. The retained feed contains at most 500 entries; readings deduplicate retained event IDs. Only integer eligible counts at least as large as the measured emitted text, and at most 1,000,000, enter paired totals. Invalid or absent eligible counts leave the actual emission unpaired. Transport wrappers whose eligible baseline was not measured must not be treated as a paired reduction. This sample measures emitted text, not model consumption or monetary savings.

Local Codex and Claude usage includes only attributable response records with explicit valid input, cache-read, cache-write, and output categories. Missing categories are not zero: the response is excluded and coverage is partial. Empty or unavailable clients have no cache-hit ratio or valuation. Unknown model tariffs and unsupported pricing metadata remain unpriced; published API equivalents are scenarios, not invoices or memory-storage savings.
