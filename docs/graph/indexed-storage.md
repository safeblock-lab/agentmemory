# Native indexed graph and semantic retrieval

This internal v1 contract replaces scope-wide graph and vector reads with bounded
queries owned by iii-engine's SQLite StateModule. The native focused suite and
offline executable build pass; this is not an activated production capability.
Canonical `state_rows`, graph leases, generation checks and recovery receipts
remain authoritative. Derived tables are disposable indexes in the same file.

## Function and capabilities

`sdk.trigger({function_id: "state::retrieval", payload})` accepts the actions
below. The SQLite adapter advertises `state::indexed_graph_v1` and
`state::semantic_lsh_v1`. Other adapters return `STATE_TX_UNSUPPORTED`.
`index_status` is the definitive runtime capability/readiness probe; never fall
back to enumerating graph rows or restoring all vectors when it is unavailable.
Engine caller metadata is stripped at the same boundary as `commit_batch`.

All requests reject unknown fields. Identifiers are nonempty, input arrays and
UTF-8 sizes are bounded. Durable revisions, positions and dirty counts use
decimal strings; per-request processed and candidate counts are bounded numbers.
Graph queries run in one SQLite read snapshot and enforce the existing recovery
barrier. Semantic model, dimension and generation must match exactly.

## Graph index preparation

```json
{"action":"index_status"}
{"action":"index_prepare","scope":"mem:graph:nodes","max_rows":256,"max_bytes":1048576}
{"action":"index_prepare","scope":"mem:graph:edges","max_rows":256,"max_bytes":1048576}
```

`index_status` returns `{version:1,capabilities:[...],graph:[{scope,status,
cursor,revision}],semantic:[{index_id,model,dimensions,generation,status,count}]}`.
`status` is `pending` or `ready`. Empty scopes start ready. Existing scopes are
pending until explicitly prepared. Preparation returns `{scope,status,cursor,
processed,revision}`; the durable cursor is engine-owned, not supplied by callers.
One call processes at most 1,024 rows and at most 8 MiB of projected index input.
Repeat while pending; interruption resumes the committed cursor. Canonical
records are not copied or altered by preparation. Writes, deletes and fenced
batches update derived rows in their canonical transaction, including while
preparation is pending. Readiness becomes visible only at commit.

Replacing a node removes its prior name-gram and observation-membership postings
through their `(epoch,key)` indexes. The primary keys lead with `gram` or
`observation`, so an epoch-only delete plan could scan the full derived posting
tables as preparation progressed. The delete statements explicitly select the
key indexes; the canonical rows, durable cursor and transaction boundaries are
unchanged.

## Exact graph reads

```json
{"action":"graph_seeds","entity_names":["agentmemory"],"observation_ids":["obs-1"],"match":"substring","max_items":4096,"max_bytes":1048576}
{"action":"graph_seeds","entity_names":["agentmemory"],"observation_ids":[],"match":"exact","max_items":4096,"max_bytes":1048576}
{"action":"graph_edges","node_ids":["node-1","node-2"],"max_items":4096,"max_bytes":1048576}
```

Seed items are `{key,id,position,entity,observation}`, ordered by canonical
insertion position. `substring` means case-insensitive bidirectional containment:
`nodeName.includes(query) || query.includes(nodeName)`. `exact` means normalized
equality. Empty entity names retain their existing match-all semantics and are
subject to the same resource limits. Observation membership is exact. Stale
nodes are excluded. Names use Unicode scalar gram postings with final string
verification; short strings and empty names have explicit indexed paths.

Edge items are `{key,position,value}` where `value` is the canonical edge JSON.
Either source or target membership selects an edge, including self-edges once.
Stale edges are excluded. Results retain canonical insertion ordering and all
temporal fields. Each response includes the graph control `generation`.
Input lists contain at most 512 items; results contain at most 16,384 items and
8 MiB, defaulting to 4,096 items and 1 MiB. If the complete exact result exceeds
a request limit, the function fails; it never returns a silently truncated graph.

## Disk-backed semantic candidates

```json
{"action":"semantic_configure","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1"}
{"action":"semantic_upsert","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1","items":[{"id":"obs-1","session_id":"session-1","embedding":"FLOAT32_LE_BASE64"}]}
{"action":"semantic_delete","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1","ids":["obs-1"]}
{"action":"semantic_search","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1","embedding":"FLOAT32_LE_BASE64","limit":50,"max_candidates":4096,"probes":32}
```

Configuration is idempotent only for the same identity; a mismatched identity
returns `STATE_SEMANTIC_IDENTITY_MISMATCH`. No implicit model swap, wipe or
generation replacement occurs. Canonical embeddings live in
`mem:semantic:<index_id>`, with the same StateModule durability as other records.
Each value is `{id,sessionId,embedding,model,dimensions,generation}`. Upserts and
deletes contain at most 256 items and commit canonical rows, vector blobs and
posting maintenance atomically. Embeddings are finite, nonzero float32 vectors
encoded little-endian; dimensions are 1..4,096. No provider calls occur.

The derived index is multiprobe random-hyperplane LSH: fixed deterministic
projections produce disk-resident posting buckets. Search visits indexed buckets
and computes exact cosine scores only for the admitted candidates; it never
performs a corpus-wide SQL vector scan. At most `max_candidates` distinct vectors
are scored (1..16,384; default 4,096), with at most `probes` bucket probes per
table (1..128; default 32) and a 1..100 result limit. Candidates stream one at a
time; only IDs, signatures and the bounded top results remain in memory.

The response is `{items:[{obsId,sessionId,score}],approximate:true,candidates,
budget_exhausted,model,dimensions,generation}`. Cosine scores and deterministic
ties describe this candidate set; ANN recall and equivalence to exhaustive
nearest-neighbor ranking are not guaranteed. `budget_exhausted` is explicit
semantic approximation metadata, not permission to truncate exact graph facts.
The integrated quality benchmark must measure Spanish, English and code recall.

## Disk-backed lexical candidates

`semantic_upsert.items` additionally accepts optional `text`, containing the
observation title, narrative and applicable code identifiers (nonempty, at most
65,536 UTF-8 bytes). The same canonical transaction maintains a SQLite FTS5
index. Canonical vector values retain that field. Status includes
`lexical_count` and `lexical_ready`; lexical retrieval is ready only when every
indexed vector has text, so partial lexical coverage cannot silently appear ready.

```json
{"action":"keyword_search","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1","query":"get_code_snippet","limit":50}
```

The result is `{items:[{obsId,sessionId,score}],model,dimensions,generation}`.
Scores are positive BM25 relevance magnitudes; callers fuse ranks with semantic
candidates before a single final rerank. Unicode words and underscore identifiers
are indexed. At most 32 safely quoted query terms are ORed; user input cannot
inject FTS operators. `limit` is 1..100. Missing full lexical coverage rejects
`STATE_LEXICAL_INDEX_NOT_READY`. No lexical corpus is restored into a Node Map.
Each native retrieval operation has a 45-second SQLite execution budget and
returns `STATE_RETRIEVAL_RESOURCE_LIMIT` on interruption, with transactional
rollback and no partially visible mutation.

## Canonical source coverage

The application index configures immutable `source_kind:"agentmemory"`. It
tracks every `mem:obs:*` scope, including observations whose session metadata is
absent, plus current `mem:memories` records (`isLatest:false` is excluded).
Eligible observations require nonempty string title and narrative; eligible
memories require nonempty string title and content. Whitespace follows JavaScript
trim semantics. Empty or uncompressed records do not become pending embeddings.
Untracked configuration remains available for isolated adapter fixtures.

```json
{"action":"semantic_configure","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1","source_kind":"agentmemory"}
{"action":"source_prepare","index_id":"observations","model":"Xenova/all-MiniLM-L6-v2","dimensions":384,"generation":"local-v1","max_rows":256,"max_bytes":1048576}
```

Status/configuration returns `source_kind`, `source_prepared`, `dirty_count` (a
decimal string) and
`coverage_ready`. Source preparation uses durable native scope/position cursors
and returns `processed`, `scope`, `cursor` and bounded
`items:[{source_scope,id,source_version}]`. It reconciles source versions and
deletions in pages; ordinary startup only checks indexed scope revisions.
Each upsert item for this index requires `source_scope` and `source_version`,
the canonical row version from `state::get_versioned`. They are checked inside
the vector transaction. Deleted, inactive or changed source versions reject
`STATE_SEMANTIC_SOURCE_STALE`. Repeated preparation after source enumeration
returns indexed dirty references until guarded upserts clear them. A crash after
the source cursor commits cannot lose a pending embedding.

Canonical source mutations mark searchable content dirty in the same commit.
Deletion or `isLatest:false` removes derived candidates atomically. Metadata-only
updates retain cleanliness; title, subtitle, narrative/content, facts, concepts,
files, sessionId/sessionIds identity and latest-version changes require refreshed candidates.
Queries reject `STATE_SEMANTIC_SOURCE_NOT_READY` until every source scope is
prepared and no dirty item remains, including after an older writer changes a
canonical scope revision. No partial source corpus is silently searched.

For tracked indexes, FTS text comes from the guarded canonical source rather
than clipped embedding text. Observation fields include title, subtitle,
narrative, facts, concepts and files; memory fields include title, content,
concepts and files. The lexical document is bounded to 1 MiB of UTF-8 input;
oversize input rejects explicitly instead of discarding code identifiers.

## Limits, recovery and operation

`STATE_INDEX_NOT_READY` rejects pending or stale derived indexes.
`STATE_RETRIEVAL_RESOURCE_LIMIT` rejects oversized complete graph results or
index-input budgets. Existing `STATE_GRAPH_RECOVERY_REQUIRED`, fenced writer and
transaction errors remain unchanged. Invalid input is `STATE_TX_INVALID_REQUEST`.
The generic canonical state contract still accepts arbitrary graph-scope JSON;
an unindexable graph row atomically marks that scope pending, and preparation
rejects `STATE_INDEX_RECORD_INVALID`. It never publishes partial graph readiness.
Malformed configured semantic records fail their transaction.

There is one SQLite actor/connection per configured adapter, with a 64 MiB page
cache, file-backed temporary storage and no memory mapping. Query execution is
serialized by that actor. Response/input admission and ANN candidate limits are
independent of corpus size; multiple adapter instances would each consume their
own cache. Account for the total engine plus Node process RSS in measurements.

This change does not activate a binary, migrate current production data, download
models or restart services. Rollback uses the retained earlier binary before any
activation. Derived v1 tables do not change the canonical format/version; an
older engine ignores them, and newer code rejects a revision mismatch rather
than trusting stale derived rows after an older writer has run.

## Retained implementation evidence

The native focused command is
`cargo test --offline --locked --no-default-features --package iii --lib --jobs 1 workers::state::adapters::sqlite:: -- --test-threads=1`.
The latest run passed 43 tests, with no failures and two ignored diagnostics or
subprocess helpers. The passing crash atomicity test invokes its helper
separately. The preparation regression checks that replacing a node performs
bounded SQLite VM work as the synthetic gram index grows from 16 to 256 nodes.
Existing cases cover
exact Unicode graph selection, canonical ordering and adjacency, atomic rollback,
resumable preparation, indexed query plans, ANN budgets, lexical identifiers,
canonical source coverage, stale version guards, eligibility and interruption.
Logs and the effective exit code are retained under
`.native-pagination-build/native-preparation-performance/sqlite-focused-tests.stdout.log`,
`sqlite-focused-tests.stderr.log` and `sqlite-focused-tests-exit.json`.

The compatible executable is retained at
`.native-pagination-build/native-preparation-performance/iii-indexed-preparation-debug.exe`,
built offline with `cargo build --offline --locked --no-default-features --package iii --bin iii --jobs 1`.
It is an unoptimized debug build with debug information disabled, retained for
integrated measurement. `patches/iii-engine/manifest.json` binds its hash, size,
source commit and complete patch, marks `deploymentReady:false`, and retains the
previous release manifest as historical evidence. The builder rejects this debug
artifact when supplied for distribution through `--binary`.

The preparation performance proof records the successful reverse apply check
for all changed source paths and the exact new patch hash. The focused suite and
offline build command, environment, stdout, stderr and exit codes are retained
under `.native-pagination-build/native-preparation-performance/`.
The six existing CLI capabilities remain unchanged; the two retrieval capabilities
are advertised by the definitive `state::retrieval/index_status` runtime probe.
Integrated real-model recall, combined engine and Node RSS, and subminute search
latency require measured acceptance; the native fixture suite proves no such
production performance claim.
