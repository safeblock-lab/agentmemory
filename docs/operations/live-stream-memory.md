# Live stream memory and recent viewer recovery

This source change makes observation streams transient. Delivery and live acceptance remain pending; no memory reduction has been measured for this implementation.

## Canonical records and notifications

Complete admitted observations remain in the canonical SQLite scopes. Observation admission and compression publish bounded `stream::send` notifications to the session group and `viewer` group. They do not write observation bodies through `stream::set`. Delivery failures are logged after commit and do not undo admitted records.

Notifications contain display fields, not full raw/tool/image payloads. Strings are bounded to 2,048 characters (narrative and user prompt: 4,096), with at most 16 strings of 256 characters per array. `projection.truncated_fields` identifies shortened fields. Existing canonical retrieval remains the source for complete records.

## Latest history

Existing authenticated `GET /agentmemory/sessions` and `/agentmemory/observations` support explicit `latest=true`. Ordinary forward paging remains available. Observations without a session are permitted only in latest mode. Agent-filter restrictions remain enforced. Input fields sent to native retrieval are whitelisted; no endpoint or MCP tool is added.

Native `viewer_recent_page` joins canonical rows through a metadata index, orders observations by timestamp and sessions by the latest valid `startedAt`/`endedAt`, then uses stable scope/key ties. Invalid timestamps remain visible with projection metadata and sort oldest. Each response is limited to 16 rows and 64 KiB. Projection truncation is exposed per row; canonical full records remain retained.

When native coverage is preparing, a latest request performs one `viewer_recent_prepare` cut (16 rows, 25 ms, 64 KiB response). It returns the truthful preparing response; a later request resumes persisted progress. It does not drain history during startup. Native preparation bounds canonical decoding to 16 MiB per cut, one source at a time. Oversized or incomplete coverage remains partial rather than becoming an empty-ready result.

The viewer subscribes before recovery when the stream connection is available. Initial loading falls back after seven seconds to bounded polling. Recovery reads at most 200 recent rows in 13 requests per attempt, with a 15-second attempt deadline. Identical concurrent requests share one flight; at most 16 flights can exist. Live observations merge by ID into timeline and activity, and compressed data takes precedence over raw data. Replay logs and visible collections are bounded. Cursor staleness or replay overflow permits one fresh-snapshot attempt; failures retain displayed data and expose status text. Timeline participates in polling and reconnect recovery.

Reflection requests `graph_read` with `mode: concept_seeds`, applying eligibility before native ranking/limit. A genuinely ready empty result remains empty so the existing reflection state machine can complete. This change does not execute reflection or modify stored jobs.

## Proposed managed configuration transition

This procedure requires Main's explicit approval for outside-workspace configuration changes and the service restart. Source implementation does not authorize those effects.

1. Freeze accepted application and native sources together. Build compatible artifacts advertising `state::viewer_recent_v1` and `state::graph_concept_seeds_v1`; preserve the prior installed application and engine artifacts for rollback.
2. Before stopping anything, record the active instance, resolved runtime YAML path, engine working directory, data directory, and configuration-worker persistence directory. Preserve the actual runtime YAML and matching persisted builtin YAML entries. For the diagnosed instance, the stream override is `C:/Users/harus/.agentmemory/config/iii-stream.yaml`; use the resolved paths rather than assuming another instance has the same layout.
3. After approval, stop the managed instance and install the coherent application/native artifacts. The managed renderer now sets only the stream adapter to `kv` with `store_method: in_memory` and no `file_path`. State, queue, ports and authentication retain their existing handling. Both bundled stream templates use the same volatile settings.
4. The existing startup path preserves an existing runtime YAML and each recognized builtin persisted YAML as a `.before-rendered-config-<timestamp>` sibling before replacing or clearing it. Copy failures abort startup. Clearing recognized persisted builtin entries lets the configuration worker seed the rendered YAML instead of overriding it with its previous stream configuration. Preserve all these siblings and the explicit pre-transition backup inventory.
5. Start the approved managed instance. Continue recent-index preparation only through bounded cuts. Main owns finite live acceptance: canonical recent recovery, reconnect overlap, compressed replacement, preparing/partial/error states, responsive controls and assistive status messages, and measured resident memory. No reflection, LLM, replay of protected summaries, or background-provider unpause is required by this transition.

Legacy `stream_store` and `.bin` files must remain untouched. They are no longer loaded by the volatile adapter but remain available for rollback. No queue or canonical-state deletion is part of this procedure.

## Rollback

After explicit restart approval, stop the instance, restore the preserved application/native artifacts, and restore the exact pre-transition runtime and persisted builtin YAML files from the inventory. Restore configuration files before restarting; a persisted configuration-worker entry can override runtime YAML. Use the prior startup procedure so the new renderer does not immediately enforce volatile streams again. Leave canonical SQLite, durable queues and legacy stream files in place. Changes admitted during the new runtime remain canonical; restoring an old stream snapshot cannot reconstruct notifications emitted during that period. History remains recoverable from canonical records.

Source-only work ran no builds, tests, typechecks, hashes, artifact checks, live calls, installations or restarts. Runtime behavior and memory savings are unverified.
