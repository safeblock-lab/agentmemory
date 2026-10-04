# Local indexed retrieval measurements

Real model inference and integrated native graph/ANN measurements passed on
the ten-document corpus. The existing large synthetic graph is now indexed and
nine targeted canonical graph queries passed in 1.870–34.467 ms. These results
do not establish equal answer quality or full production semantic-corpus latency.

## Provisioned models

The workspace evidence directory `.native-pagination-build/local-indexed-verification`
contains caches for offline CPU q8 inference, one ONNX thread per model.
The ten downloaded files total 47,542,258 bytes. `model-assets.json` retains each
file's size, SHA-256, public source URL and pinned revision:

- `Xenova/all-MiniLM-L6-v2`: `751bff37182d3f1213fa05d7196b954e230abad9`.
- `Xenova/ms-marco-MiniLM-L-6-v2`: `a09144355adeed5f58c8ed011d209bf8ee5a1fec`.

No hosted inference or paid provider calls were used. Model asset provisioning is
distinct from preparation of the searchable corpus.

## Model-only measurements

On this Windows host, Node v24.14.0, the first two-document embedding load and
inference took 5.764 seconds. The initial smoke process sampled 371,118,080 bytes
of Node RSS. A second fresh process embedded ten curated documents in 0.966
seconds; filesystem caches were already warm, so this is not a cold disk result.

The ten-document English, Spanish and code-identifier evaluation retrieved every
judged relevant document in the first five exhaustive cosine candidates. Actual
paired-token reranking placed a relevant document first for all three queries.
For Spanish, unrelated music and code records still ranked above the second
relevant English document. This demonstrates a multilingual ranking limitation,
and does not support an equal-quality claim.

The bounded 50-pair reranker test used repeated synthetic documents clipped to
8,192 characters and truncated to 512 tokens per pair. It took 6.547 seconds.
Node sampled peak RSS was 479,109,120 bytes (approximately 457 MiB), sampled
every 25 ms. This includes embeddings and reranking in one process, but no engine.
It cannot be presented as total system memory. Exact records are retained in
`model-smoke.json` and `model-quality.json`.

## Integrated measurement procedure

`benchmark/local-indexed-retrieval.ts` has two independent modes. `quality` runs
the actual application hybrid retrieval against a ten-document corpus, with
fenced native graph writes and exact name/membership/relationship parity.
Candidate recall and reranker ordering are reported separately. `large` prepares
indexes in the existing mutable synthetic SQLite database in place and times
targeted node and edge queries. It creates no full graph copy and does not
restore the JSON graph. Large graph measurements do not establish semantic ANN
quality on an equivalently large text corpus.

Both modes require the Main-accepted executable recorded in the owned
`native-accepted.json` gate. The harness uses a loopback engine process and
configuration rooted in the owned evidence directory. It does not activate the
installed binary or restart an existing service.

The native page cache is bounded to 64 MiB. Measurement stops above 2 GiB joint
sampled RSS or ten minutes of process lifetime. Native calls have a 55-second SDK
timeout. Node RSS is sampled every 25 ms; engine and simultaneous combined RSS
every second, with the engine's OS `PeakWorkingSet64` also recorded. Sampled
maxima may miss short peaks. First-search timings distinguish the embedding
model warmed by preparation from a cold reranker, and preparation is reported
separately from ordinary queries.

After native acceptance, run each mode separately:

```powershell
node --import tsx benchmark/local-indexed-retrieval.ts ACCEPTED_EXE quality
node --import tsx benchmark/local-indexed-retrieval.ts ACCEPTED_EXE large
```

The native integration test is opt-in and requires the accepted artifact:

```powershell
$env:LOCAL_INDEXED_ACCEPTED_ENGINE = 'ACCEPTED_EXE'
npx --no-install vitest run test/local-indexed-retrieval.integration.test.ts
```

Strict standalone TypeScript checking of the harness and its tests passed.
Three tests passed with the accepted native executable and real local models
(2026-10-02, 9.95 seconds). The executable is debug and unoptimized; no service
was activated.

## Integrated baseline measurement

`quality-integration-1.json` records ten prepared semantic observations and exact
small-graph parity. First search took 0.854 seconds with embeddings warm from
preparation and a cold reranker. Nine warm end-to-end queries took 49–63 ms.
All three curated queries reached final recall@5 of one and a relevant top1.
The Spanish native ANN candidates alone had recall@5 of 0.5; lexical/graph
fusion recovered the second relevant document. Candidate recall and final
ranking are therefore reported separately.

Fifty bounded baseline reranker pairs took 6.065 seconds. Joint Node+engine
sampled RSS peaked at 363,196,416 bytes (346 MiB); engine OS peak was
59,719,680 bytes (57 MiB). A preceding CLI campaign observed 518 MiB joint
sampled RSS, so the lower test-run maximum is not a universal resource bound.
The earlier CLI result file was overwritten by that integration run; its
metrics remain in the captured tool output. Subsequent harness campaigns save
timestamped JSON files as well as the latest-result pointer.

## Large graph preparation status

The first in-place campaign on the existing synthetic 1.64 GB SQLite database
reached durable node cursor 5119 before a `STATE_RETRIEVAL_RESOURCE_LIMIT`
failure. No ordinary large-graph query was measured. Engine OS peak was
132,964,352 bytes (127 MiB); simultaneous sampled Node+engine peak was
226,746,368 bytes (216 MiB). `large-failure-1.json` retains this campaign.

`large-diagnostic.json` showed zero invalid records and advanced one valid
node from cursor 5119 to 5120 in 122 ms. Pinned canonical nodes had 29-character
names, one observation member and 13,065-byte total records. This supports a
45-second native work-budget failure for the 1024-row batch, rather than an
oversized first row. Preparation now resumes with 64-row batches and durable
per-call JSONL evidence under the same ten-minute process deadline. Large-graph
query scalability remains unverified until preparation reaches ready. The first
17 resumed batches processed 1,088 nodes in 52.456 seconds (20.74 nodes/second),
reaching cursor 6208. At that observed rate each remaining 100,000 nodes would
take about 80 minutes, excluding edges. This is a measured extrapolation of
initial preparation, not measured query latency or an exact completion time.

Main stopped the resumed campaign early once the slowdown was established.
`large-stopped-status.json` verifies durable node cursor 9984, zero invalid
records, and edges still pending at cursor -1. From initial cursor 5120, another
4,864 nodes committed; ordinary large-graph queries were not attempted.
The stopped campaign observed 133,062,656 bytes engine OS peak (127 MiB) and
241,561,600 bytes joint sampled peak (230 MiB). The engine and the subsequent
read-only status probe both closed successfully.

The already-running campaign lacked cooperative cancellation. Temporarily
making its owned progress file read-only caused the next append, after the
native batch committed, to fail with EPERM and enter normal harness cleanup.
That filesystem error is retained and explicitly classified as requested stop;
the attribute was restored. Future campaigns check an owned `stop-large` file
after writing completed-batch evidence, record final index status, and close.
Main subsequently accepted the native preparation correction after 43 native
tests and artifact verification. SQLite 3.51.3 had chosen whole-epoch scans for
gram and membership deletion despite existing `(epoch,key)` indexes. Explicit
`INDEXED BY` clauses now select those existing indexes. The controlled native
operation comparison fell from 2,031,038 VM steps / 129 ms to 1,566 / 1.26 ms;
this comparison is not an end-to-end query measurement.

The authorized continuation uses debug, unoptimized artifact SHA256
`086b36a42b92625cba0479b5de4a73f847d993fe653fc2d5fb928e21af45644d`
and resumes the same durable cursor 9984 and epoch. Its first 64-row batch
completed in 2.550 seconds. The campaign committed 48,512 nodes in 543.637
seconds across 758 batches (89.2 nodes/second), reaching durable cursor 58496.
It stopped cooperatively after a committed batch at the nine-minute work
budget, reserving time for status capture and cleanup within the ten-minute
process bound. Both graph scopes remain pending with zero invalid records;
edges have not yet advanced. The engine and benchmark Node process are absent.

Joint sampled RSS peaked at 245,108,736 bytes (234 MiB); engine OS peak was
133,103,616 bytes (127 MiB), with 539 one-second engine samples. Evidence is
retained in `large-results-2026-10-02T20-11-54-072Z.json` and its per-batch JSONL.
The retained failure text says "Main requested stop after committed preparation
batch" because the loaded harness used the sentinel cancellation branch; this
was the bounded campaign stop, not a native preparation failure. Readiness and
large indexed queries remain pending. The index revision is not a corpus row
count and is not used to estimate the remaining corpus size. The 2 GiB combined
memory bound remains enforced. The current harness also checks exact canonical
seed and incident-edge sets for the three pinned ordinals, rather than only
verifying the values of returned edges.

## Large graph ready and targeted query results

The subsequent 256-row continuation reused cursor 58496 and the existing epoch,
then reached `ready` for both graph scopes with zero invalid records. It
processed 54,617 remaining node records in 463.979 seconds and 56,557 edge
records in another 15.811 seconds: 479.790 seconds across 435 calls. The two
corrected continuations together spent 543.637 + 479.790 = 1,023.427 seconds
(17.057 minutes) on preparation. Earlier progress and failures are retained;
this is not a fresh full-index construction timing from an empty index.

The nine subsequent queries targeted ordinals 0, 1000 and 56000, three times
per ordinal. Every query returned exactly the expected one canonical seed and
one canonical incident edge, including the expected edge key and full value.
Measured seeds + incident-edge retrieval + canonical edge read took
1.870–34.467 ms; the median was 2.083 ms. Preparation had warmed the graph
process and filesystem, so these are not cold disk or cold model measurements.
The result demonstrates the under-one-minute objective for these selective
queries on this synthetic graph, not arbitrary broad queries or production
semantic retrieval quality.

Joint Node+engine sampled RSS peaked at 243,736,576 bytes (232 MiB); engine
OS peak was 133,234,688 bytes (127 MiB), with 477 engine samples. These are
large graph-only measurements without loaded embedding or reranker models.
The database grew from 1,640,005,632 to 2,381,586,432 bytes (1.640 to 2.382 GB),
an additional 741,580,800 bytes for the in-place derived preparation. No full
graph copies or restored JSON were used.

The exact accepted executable, debug/unoptimized profile and bounds remain
as above. `large-results-2026-10-02T20-22-20-754Z.json` and the corresponding
`large-progress-*.jsonl` retain results and preparation timing. The actual CLI
command returned exit 0. `large-cleanup-2026-10-02T20-22-20-754Z.json` confirms
engine PID 14700 is absent and both graph scopes ready. The engine received
SIGTERM during normal cleanup after all requests completed. The ten-document
semantic integration remains the separate quality evidence; large semantic
corpus population, ANN recall and full production quality remain unmeasured.
## Multilingual candidate comparison

The public [SugoLabs ONNX export](https://huggingface.co/SugoLabs/mmarco-mMiniLMv2-L12-H384-v1)
was evaluated without changing the runtime default. Revision
`6772eee1ea62dc82e1fcaaf3ed90c269fdcb1fcf` occupies 135,320,852 bytes for
the five required assets; exact SHA256 values are in `multilingual-assets.json`.
Only cached local q8 CPU inference was used.

`multilingual-quality.json` preserves the same five candidates and original
judgements as the baseline. Spanish relevant documents moved from positions
one and five to positions one and two. Code retained its relevant first result.
English placed the Spanish RAM explanation first, so the original English
judgement marked top1 false. A separate `content-judgements.json` retains that
original result and adds the Spanish document as relevant: its text explicitly
answers the English RAM question by storing the graph on disk with indexes and
retrieving only relevant nodes and relations. Both models then have a relevant
English top1 under this broader, documented content judgement. This is a tiny
curated comparison, not evidence of universal quality equivalence.

The multilingual model loaded in 0.910 seconds; five-pair inference took
0.090–0.104 seconds per query. Fifty bounded 512-token pairs took 13.244 seconds.
Node sampled RSS peaked at 660,361,216 bytes (630 MiB), including the embedding
model and this multilingual reranker. The baseline reranker was not loaded in
that process. This alternative was measured without an engine; its combined
runtime memory and full production quality remain unmeasured.

## Qwen3 local reranker acceptance

The pinned Qwen3-Reranker-0.6B Q8_0 model scored a 54-example internal
calibration set. The accepted Spanish result was 17/18. The set was not
independently human-reviewed and does not establish broad multilingual parity
or equivalent answer quality. No retuning followed acceptance. Exact scorer
parity, asset pins and GPU/CPU runtime controls are recorded in the release
candidate receipts; the operational setup and machine-specific controls are in
the [local reranking guide](local-reranking.md).
