# Recent operation feed

Dashboard renders the latest 10 entries and Health the latest 30 from the same
`api::status` activity result. It reads only
`mem:recent-operations/current` through native iii-engine StateKV, with no corpus
enumeration or historical audit scan. The record holds the latest 30 outcomes in
native append order; the UI displays newest first. Service restarts retain it.

Compression, direct summaries and graph extraction publish their existing outcomes.
The durable summary queue also records new enqueues, successfully saved map/reduce
units, failed unit attempts, and final persisted summaries or failed jobs.
Duplicate deliveries and dispatcher/recovery polls do not create entries.
Final summaries identify deterministic fallback separately from LLM synthesis.
An enqueue is recorded only for a newly created job, not a deduplicated request.

An explicit processing catalog in `dashboard-activity.ts`, wrapped by
`operation-tracking.ts`, records profile generation,
skill extraction, consolidation and its pipeline, pattern/rule generation, reflection,
crystallization, workflow/file compression, window/session enrichment, temporal graph
extraction, retention scoring/eviction, automatic forgetting, eviction, cascade updates,
healing, slot reflection, image embedding, migration and graph import. Cached profiles,
skips, queued work, partial failures and thrown/returned errors have distinct outcomes.
The instrumentation preserves original results and thrown errors. It does not invoke
providers or schedule work. Registration names outside this catalog are excluded,
including API/status calls, searches, matching/listing/getters, dispatcher/recovery
polls, routine coordination and ordinary writes. New processing functions must be
added explicitly rather than inheriting telemetry from a `mem::` prefix.

Routine session and observation writes do not displace these operations. Existing
legacy audit history remains read-only; `recordAudit` stays disabled.

Each entry contains a generated operation ID, timestamp, fixed function name,
outcome, optional sanitized agent ID, and whitelisted nonnegative integer counts.
Observation text, titles, prompts, file paths, target IDs, provider responses and
error details are excluded. Agent isolation filters the retained global 30 entries;
it does not maintain a separate 30-entry history per agent.

Compression reports observations processed and compressed. Summarization reports
input observation count and summaries written. Graph extraction reports actual
new nodes and edges returned by graph persistence, including batch application;
queued units, skips, zero-change outcomes and partial provider failures remain
visible. Counts are per operation outcome, not current corpus totals. Replayed
durable graph jobs can return their original stored result counts. Unknown counts
are omitted rather than replaced with zero.

## Native mutation requirement

Retention depends on the native `state::update` operation:

```json
{"type":"append_bounded","path":"items","value":{"item":{"id":"op_example"},"limit":30}}
```

The engine must append and evict the oldest entries within one atomic state
mutation. There is no client read/modify/set fallback or process-local persistence.
An unsupported engine or failed feed write produces a sanitized warning without
changing the domain operation result. Read failures make activity unavailable in
status; a missing record produces an empty feed.

This implementation requires the companion native engine change before delivery.
Tests, runtime probes and restart persistence checks were not run, per the user's
execution preference.
