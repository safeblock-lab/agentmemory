# Storage contracts and lossless reduction candidates

Source-only analysis, 2026-10-13. Current workspace behavior is distinguished from installed runtime behavior. No database reads, tests, builds, provider calls, service effects or product source edits were performed by this source-analysis owner.

## Authority and coverage

`src/state/kv.ts:54,90,143` routes canonical state reads, writes, paging, revisions, graph transactions and retrieval through iii-engine. Application persistence redesign must remain behind the native StateModule adapter.

Native source is available in `patches/iii-engine/0.22.1-state-pagination.patch`; references below are one-based patch lines and the Rust modules named in its headers. The former `.native-pagination-build/iii-source/engine/src` checkout was absent.

The code graph matched `D:/agentmemory` uniquely but its generation is 2026-10-03T21:05:54Z. Coverage checks flagged metadata changes for `src/state/schema.ts`, `src/types.ts` and `src/index.ts`; graph discovery returned no `src/state` nodes. Analysis used current-source fallback without reindexing. Discovery covered `src/functions/*.ts` KV references and targeted state, native patch, types and operational contracts. It does not prove exhaustive dynamic callers, installed revision or physical byte attribution.

## Existing representation

Native schema: `state_scopes(scope,revision,next_position)`; `state_rows(scope,key,value BLOB,version,position,value_bytes)` keyed by `(scope,key)`; insertion-order index `state_rows_order`; graph metadata/coordination; derived retrieval, FTS, membership, readiness, project, browse and recent-viewer tables. See patch line 6906. `value_bytes` is decoded logical length, not allocation.

V2 already uses zlib level 6 with raw fallback, a seven-byte compressed marker `89 41 4D 56 32 5A 01`, and a 16 MiB decoded limit. Compression is selected only when compressed bytes plus marker plus 16 are smaller than the original. Decode requires exact declared length, UTF-8, complete zlib stream and no trailing bytes. See patch lines 5759–5818. Main independently read this codec source. Compressing stored BLOBs measures additional encoding redundancy; it does not measure the original JSON's compressibility.

Historical V2 pilot results in `docs/operations/native-storage-format-v2.md:33` concern a different frozen copy. They cannot establish savings for the present baseline.

## Canonical originals and identity

`src/state/schema.ts:4` declares sessions, observations, memories, summaries, graph records, semantic/procedural memories, workflows and operational scopes. Observations may retain raw/tool input/output, prompt/response, narrative, facts, concepts, files, images and provenance (`src/types.ts:312`).

Identity covers canonical sorted JSON of session ID, hook type and complete sanitized data. Changed output gives a different identity; event identity also includes timestamp. Durable admission checks original equality, occurrence counts and bounded provenance (`src/functions/observation-identity.ts:30,43,91`).

Compression reloads the durable original, declines incomplete/oversized sources and treats missing image preservation as pending (`src/functions/compress.ts:87`). Native replacement preserves original/raw/tool/prompt/image fields and identity/provenance (patch line 8468). LLM compression therefore does not prove original bytes disappeared. Physical sharing may retain separate logical observations/events; it must not merge identities, permissions, counts or provenance across sessions.

Current native identity/event indexes hold references and digests rather than full canonical identity strings (patch line 8464). Their contribution requires measurement.

## Vectors and text

Current indexed writes encode `Float32Array` to explicit little-endian bytes, send Base64 plus text and exact source scope/version (`src/state/indexed-vector.ts:171`). Canonical semantic records hold Base64 embedding, text, model, dimensions, generation and source binding (`src/types.ts:1713`). Native `retrieval_vectors` stores another raw float32 BLOB; LSH has 12 tables, postings and reverse indexes (patch lines 21774,21891,16052).

Scoped FTS is already contentless and scoped retrieval has bucket metadata/postings (patch line 20276). Global lexical FTS stores text and links (patch line 15891). Lexical text may derive from fuller canonical content than the embedding input (`indexed-vector.ts:175`; patch line 22590), so text copies are not automatically identical.

MiniLM/384 dimensions is a source capability (`src/state/indexed-embedding.ts:6`), not proof of installed vector dimensions.

Potential legacy coexistence includes BM25 vector snapshots/manifests/shards, bucket records `{id,s,e}`, current semantic records, native vectors/postings and image embeddings stored as JSON number arrays (`src/state/index-persistence.ts:12,322,395`; `src/functions/vision-search.ts:10`). Existing cleanup requires successful migration and readability (`index-persistence.ts:411`); coexistence alone does not justify deletion.

Lossless candidate: one engine-owned float32 bitstream referenced by canonical data and retrieval, reconstructing Base64 at API/export boundaries. Preserve every float32 bit, IDs, source versions and deterministic search behavior. Float16/int8 quantization, fewer dimensions, model replacement and re-embedding are excluded from a strict fidelity savings budget.

## Graph and recovery

Canonical nodes hold names, properties, aliases and ordered source-observation arrays; edges retain endpoints, weight, source observations and temporal/version/supersession fields (`src/types.ts:988,1018`). Native projections include names/prefixes/positions, Unicode grams and reverse indexes, integer IDs, observation/node membership with reverse indexes, and endpoint projections with source/target indexes (patch line 12659). Integer membership normalization already exists; count no new savings for it.

Temporal writers retain previous edges in `mem:graph:edge-history` (`src/functions/selected-temporal-apply.ts:99`; `temporal-graph.ts:212`). Superseded rows remain history unless a separate policy changes their meaning.

Recovery scopes include jobs, inputs, provider results, deltas, remaps, prepared bodies, working snapshots, checkpoints and receipts (`src/state/schema.ts:59`). Concrete representations include:

- Delta shadows with original values/expected versions (`src/functions/graph-job-storage.ts:167`).
- Ordered remap/template operations (`graph-job-storage.ts:277`).
- Exact prepared serialized bodies in fragments plus digest (`graph-job-storage.ts:331`).
- Reassembly/digest checks and identical-body transport replay (`graph-job-storage.ts:101,351`).
- Native receipts with identity, digest, generation, checkpoint version and row-version arrays (patch line 6308).

Current receipts are not full-body copies. Same replay identity with a different digest must fail (patch line 6220). A shared immutable blob design must bind content and version, preserve operation order, expected versions, generation/fence, checkpoint progression and replay-conflict behavior. Sharing by mutable row ID alone is unsafe.

Terminal retention requires activated maintenance, no recovery barrier, a lease, terminal state, complete checkpoint, native dependency checks and protected-job exceptions (`src/functions/graph-retention.ts:11,25`). Deletion/retention is separate from lossless sharing.

## Remaining workflows and backups

Distinct source ownership includes summary queues and immutable completion metadata (`summary-queue.ts:127`), reflection inputs/results and receipts (`reflect-queue.ts:117,290`), Fireworks jobs/fingerprints/journals (`fireworks-batch.ts:545,568`), temporal pages/manifests (`temporal-graph.ts:838`), enriched observation chunks (`sliding-window.ts:201`), memory version relations (`relations.ts:136,209`) and managed image references (`image-refs.ts:8`). Operational metrics, usage, recent operations, routine runs, logs and domain records need separate attribution.

Current `recordAudit` is a compatibility no-op while `queryAudit` reads legacy rows (`src/functions/audit.ts:7,45`). Old documentation does not establish ongoing audit growth.

Application export is not a full database backup. Declared collections omit graph recovery scopes, history, receipts, audit, embedding indexes and other operational scopes (`src/types.ts:916`; `src/functions/export-import.ts:60,253`). Import reindexes separately (`export-import.ts:834`). Migration parity must cover decoded native tuples and metadata, not only export JSON. Git snapshot archives add `state.json` and commit history outside SQLite (`src/functions/snapshot.ts:149,203`).

## Streams

Current source publishes bounded observation display projections to both session and viewer groups; raw/tool/image bodies are excluded and truncation is explicit. Notification follows canonical commit (`src/functions/live-observation.ts:5,24`). Other viewer events use `stream::send` (`src/triggers/events.ts:305`); viewer subscribes to `mem-live/viewer` (`src/viewer/index.html:1132`).

Repository YAML selects `in_memory` streams, separate from SQLite and queue persistence (`iii-config.yaml:14`). Installed adapter must be established separately. `docs/operations/live-stream-memory.md:3,33` says live transition acceptance remains pending and legacy stream files must remain for rollback. Measured files cannot be assumed disposable.

## Opportunity matrix and limits

Candidate improvements are shared immutable exact-value/body blobs, shared ordered provenance/ID arrays, bounded block/shared-dictionary compression beyond current per-row zlib, compact native postings preserving query directions/order, single float32 storage authority, and equivalent FTS layouts preserving ranking/tokenization/text access.

Every candidate needs measured joint output size including dictionaries, references, indexes, retained originals and rollback copies. Byte equality must confirm exact duplicate candidates; structural equivalence is a separate category. Existing zlib, integer membership and contentless scoped FTS are baseline features.

No source-only evidence establishes a tenfold reduction. Full bounded physical/logical measurement remains necessary. A later offline shadow encoding and decoded-tuple parity experiment would discriminate architectural options, but implementation, migration and actual reclamation require separately scoped authorization.

## Tool capture and RTK clarification

Main inspected `src/hooks/post-tool-use.ts` after the user's RTK question. This hook selects the host's `tool_response`, `tool_output` or tool-result text; extracts image data; sends `tool_output: truncate(cleanOutput, 8000)` and sends `tool_input` without that hook-level size limit (lines 39–65,107–115). The output limit is JavaScript string length, not an exact UTF-8 byte cap; an added truncation marker slightly increases length. The hook does not invoke RTK. An upstream RTK-wrapped command may already provide reduced output, but this capture source does not establish which payload every host supplies or prove installed hook parity.

Therefore earlier conversational wording about retaining complete tool outputs was too broad: this post-tool hook truncates output. Other producers and historical rows must be assessed separately. RTK output filtering, persistence admission limits, lossless storage encoding and expiration are four different mechanisms. Filtering before storage may reduce incoming traces; it does not expire existing records, and CLI filtering does not define retention for viewer events or replay bodies. Inputs can remain large even where output is truncated.
