# Retrieval ranking quality and failure behavior

## Ranking rule

Indexed retrieval ranks at most 50 candidates before local reranking. The selected ranking rule adds two independently min-max-normalized scores for those same candidates:

```text
combined = minMax(fused retrieval score) + minMax(raw Qwen score)
```

The Qwen values are model scores, not ranks; they are normalized over the current candidate set so their scale can be combined with retrieval scores. The fused retrieval score is the existing lexical/vector/graph score. Equal combined scores retain the original `preRerank50` candidate order. The candidate set is validated before fusion; reranking must not add, drop, or duplicate candidate IDs.

This was selected from the preregistered, no-inference comparison over the frozen 54-query calibration set. The three alternatives were (1) reciprocal-rank fusion (RRF, K=60) of fused retrieval and Qwen ranks, (2) RRF of lexical, vector, graph, and Qwen ranks, and (3) equal-weight CombSUM of normalized fused retrieval and raw Qwen scores. The existing Qwen-only result and retrieval-only fusion are controls. Qwen ranks are restricted to the existing pre-rerank candidate set for every comparison. The tie rule for each candidate-ranking comparison is stable original `preRerank50` position.

| Ranking | Overall Recall@10 | English | Spanish | Code identifier | Mean NDCG@10 drop vs Qwen-only |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing Qwen-only | 51/54 (0.9444) | 1.0000 | 0.8333 | 1.0000 | -0.00082 |
| Retrieval fusion only | 50/54 (0.9259) | 0.8889 | 0.8889 | 1.0000 | +0.08456 |
| RRF fused retrieval + Qwen | 53/54 (0.9815) | 1.0000 | 0.9444 | 1.0000 | +0.01662 |
| RRF lexical + vector + graph + Qwen | 50/54 (0.9259) | 0.9444 | 0.8333 | 1.0000 | +0.06411 |
| **Selected CombSUM** | **53/54 (0.9815)** | **1.0000** | **0.9444** | **1.0000** | **-0.00879** |

Positive NDCG drop means the comparison ranked lower than Qwen-only; a negative value means its mean NDCG@10 was higher. Selected CombSUM and RRF both recall 53/54 relevant targets. CombSUM has the better mean NDCG result, while RRF places the single remaining Spanish miss at rank 18 and CombSUM at rank 25 (`q-es-release-cal-03`, target `es-release-cal-03`). For that target, its raw Qwen score is -11.2800 (rank 16/50), its fused retrieval score is 0.5155 (rank 24/50), and its normalized CombSUM score is 0.3591 (rank 25/50). This tradeoff is retained in the calibration report rather than hidden by the aggregate.

The replay, complete per-query relevant-candidate score/rank comparisons, metrics, tie policy, and hashes are retained under `.native-pagination-build/quality-remediation-calibration/`. Reproduce them without model inference with:

```powershell
node .native-pagination-build/quality-remediation-calibration/compare.mjs
```

That script reads only `.native-pagination-build/semantic-quality-correction/corrected128/result.json` and its cached Qwen score ranks. It requires exactly 54 calibration rows and refuses query IDs outside the calibration naming pattern. It does not open a held-out quality set, labels, or provider.

## Cancellation and failures

`HybridSearch.search` and `searchWithExpansion` carry their optional `AbortSignal` through final reranking. An already-aborted request rejects before reranking; cancellation during scoring also rejects even if a scorer later resolves. Search no longer hides integrity, bad-input, queue-saturation, or timeout failures by returning retrieval results as if reranking succeeded.

Reranking remains intentionally optional when no local provider is configured or available: `loadReranker()` returning `null` preserves the original retrieval order. Once a provider is available, invalid scores, provider errors, cancellation, queue saturation, and deadlines propagate to the caller. Existing Qwen strict tokenizer validation, model/runtime choice, GPU-to-CPU fallback policy, and native indexed retrieval implementation are unchanged by this ranking rule.

Operational limits remain a 50-candidate reranker batch, at most 8 waiting batches behind the active batch, and a 60-second queue deadline. Existing model tokenizer, process RSS, and inference deadlines remain owned by the reranker runtime; this change does not weaken them.
