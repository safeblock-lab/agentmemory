# Local indexed retrieval runtime

The source runtime defaults to indexed retrieval. `AGENTMEMORY_RETRIEVAL_MODE=legacy`
explicitly selects the previous resident implementation. Public MCP tools, REST
routes, result fields and counts are unchanged. The indexed runtime requires the
[native StateModule contract](indexed-storage.md); an older engine or an
unprepared corpus produces an explicit readiness error.

Search uses disk FTS5 lexical candidates, local embedding queries and bounded
multi-probe LSH candidates. Existing rank fusion and graph enrichment preserve
relationship and temporal context. Expanded queries deduplicate candidates before
one [local reranking](local-reranking.md) pass, limited to 50 pairs of 512 tokens.
Lexical coverage must equal vector coverage. Partial lexical readiness does not
fall back to a resident BM25 corpus.
For tracked sources, the native FTS document preserves observation title,
subtitle, narrative, facts, concepts and files, or memory title, content,
concepts and files. It is derived from the guarded canonical source independently
of the shorter embedding input. Documents over the native 1 MiB lexical bound
fail explicitly.

## Assets and preparation

Embeddings use `Xenova/all-MiniLM-L6-v2`, 384 dimensions, q8 CPU inference and one
ONNX thread. Assets must already exist in `.cache/agentmemory/embeddings`.
`local_files_only` prevents implicit downloads. Embedding admission is limited to
eight batches, each with at most 32 documents and 16,000 characters per document.
The optional Qwen reranker model and native CPU runtime are provisioned under its configured absolute asset root; its guide documents the separate model, runtime notices, GPU acquisition and Python requirements.

Initial preparation is an explicit `rebuildIndex` operation with a stopped or
otherwise quiescent corpus. It streams native eligible-source references, including
observation scopes without a session row, and reads each canonical source directly.
It embeds 32 records at a time, writes Float32LE base64 vectors and lexical text through
`state::retrieval`, and resumes native graph preparation using engine-owned
cursors. Native `source_prepare` first establishes bounded source coverage and
dirty tracking. It publishes the durable `indexed-corpus` ready marker only after all
writes and graph preparation succeed. Failure leaves the marker pending.
Semantic preparation can be rerun idempotently; dirty references survive interrupted
embedding and are offered again until their guarded upserts succeed. Startup probes
readiness and never performs an implicit corpus scan.

The indexed startup path skips legacy JSON/vector restoration and keyword corpus
rebuild. Search mutations await durable native upserts; synchronous delete callers
enqueue native deletes and `flushIndexSave` waits for their completion. The native
mutation queue admits at most 128 records. Failed embeddings mark coverage pending.
The tracked index uses `source_kind: agentmemory`. Canonical content mutations
atomically mark the corresponding native source dirty; deletion removes its
derived vector. Every upsert supplies `source_scope` and the decimal
`getVersioned().version`, and stale source versions reject the write. Startup
requires prepared source coverage and zero dirty sources even if the process
stopped before it could update the TypeScript ready marker. Explicit preparation
can repair a previous runtime mutation failure without restarting the service;
ordinary queries cannot clear it.

## Graph bounds and consistency

Queries use native seed/membership/incidence indexes and targeted canonical reads.
Each identifier request contains at most 512 IDs. Each native response admits at
most 16,384 items and 8 MiB. Multi-request results union seed flags, deduplicate by
canonical key, retain decimal positions as `BigInt`, and reject generation changes.
The completed-graph generation/fence barrier remains in place around reads.
Graph retrieval admission is limited to eight queued or active calls per KV client.
Exact oversized results fail explicitly; they are never silently truncated.
Serialized graph data is bounded to 64 MiB and the estimated working set to
256 MiB, including retained raw indexed edges and projected graph structures.

## Quality and measurement

Graph and temporal retrieval remain exact within the accepted resource limits.
LSH retrieval is approximate, examines at most 4,096 candidates with 32 probes,
and logs both approximation and budget exhaustion. FTS5 Unicode/identifier
tokenization differs from the former resident BM25 tokenizer. Neither ANN recall
nor equal end-to-end answer quality is implied by the contract tests.

The real-data benchmark must measure retrieval quality, full engine plus Node RSS,
initial preparation cost, and cold/warm query latency before claiming the target
of less than one minute. This source integration does not activate binaries,
download models, prepare the current database or restart services.
