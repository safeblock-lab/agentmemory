# Indexed retrieval quality contract

This document defines the preregistered offline campaign for indexed retrieval. It sets acceptance criteria; it does not report a passing quality measurement. One calibration attempt ran a local model-readiness probe for 243.8 seconds, then failed with `Invalid engine RSS` before scoring any queries. The saved result `calibration-2026-10-02T23-49-42-707Z.json` has SHA-256 `a6a7f2f0160cba839a27c6e54f305d14a32b8e3e374d6090bbca25219bfe3293`, reports zero completed queries, and contains no quality metrics. The data are synthetic and authored for this repository’s implementation contracts, so any eventual result is limited to this fixture and must not be presented as evidence of equal quality on real user memories.

## Frozen fixture and labels

The fixture under `eval/local-indexed/` contains 162 synthetic observations and 162 unique query texts and intent ids: 54 calibration and 108 sealed held-out. Each split is a global retrieval pool; every query searches all 54 or all 108 observations in its split, rather than a query-specific set of judged positives and negatives. The held-out corpus is therefore larger than the 50-result ANN cutoff.

Each split has 18 English, 18 Spanish, and 18 code-identifier calibration queries, or 36 per stratum for held-out. The authored cases cover paraphrase and synonym variation, code symbols and nearby APIs, lexical and semantic competitors, temporal and entity relations, session/scope boundaries, retrieval recovery, API behavior, and release/package facts. Every query has a positive judgement and at least two same-subsystem hard negatives. Six queries intentionally have multiple relevant observations.

Judgements are agent-authored predetermined content-intent labels. They were written without retrieval, ANN, reranker, or model-score output and have not been independently human reviewed. They are not production relevance judgements. `freeze.json` binds source file SHA-256 values, row hashes and order, split counts, frozen thresholds, model identities/revisions, q8 settings, and the local model-asset manifest hash. `verify-preregistered.mjs` checks those invariants without loading a model.

Run the static verifier:

```powershell
node eval/local-indexed/verify-preregistered.mjs
```

The generator rewrites the frozen fixture and manifest. Use it only when deliberately creating a new preregistration, then rerun the verifier. Do not regenerate to make a changed fixture pass.

## Harness and comparator

`benchmark/local-indexed-quality-campaign.ts` seeds only the selected split into an isolated SQLite file, using the guarded graph transaction path for graph nodes, edges, and snapshot. It then exercises indexed preparation, native ANN candidates, TypeScript `HybridSearch` fusion, and the final local reranker. It records candidate ids/recall separately from final ids/recall. For each query it also applies the same cached q8 reranker to every document in the same split; this is a bounded same-model exhaustive comparator, not a human relevance oracle.

The harness requires Main’s accepted native executable and verifies its path and SHA-256. It verifies the fixture and cached model manifest before inference, uses local-only model loading, forbids state-scope enumeration, asserts all candidate and final-result ids remain inside the selected split, and checks temporal graph before/after behavior plus unrelated-entity isolation. Resource samples capture simultaneous native-plus-Node RSS; latency includes model probes, preparation, fresh native process starts, first queries in those processes, and warm queries. Each ordinary `HybridSearch.search` call must finish under 60 seconds. The harness restarts native workers in bounded 12-query chunks to respect the native harness process deadline; all chunks for a replicate use that replicate’s single isolated synthetic database.

RSS is valid only when all expected native process snapshots are present (one preparation process plus one per 12-query batch), each has a positive sample count and positive finite engine-sampled, OS-peak, Node-sampled, and joint-peak RSS values, zero failed samples, no sampler error, at most 5 seconds between samples, startup/work/terminal phase coverage, and confirmed process/listener/collector cleanup with no cleanup errors. Missing, malformed, or incomplete RSS evidence fails acceptance; it is never interpreted as zero. Coalesced skipped samples are reported but may pass when sample-gap and phase-coverage requirements still hold. The sampler’s `performanceTargetsPassed` flag is recorded separately: it describes collection overhead targets, not whether the RSS evidence is complete or valid.

Held-out acceptance thresholds are frozen as follows:

| Check | Acceptance |
| --- | ---: |
| Temporal graph fixture parity | 100% (all three as-of/isolation checks) |
| Mean ANN candidate recall@50 | ≥0.95 overall |
| ANN candidate recall@50 | ≥0.90 in each English, Spanish, and code-identifier stratum |
| Final recall@10 after fusion and reranking | ≥0.95 in each stratum |
| Mean nDCG@10 drop vs. same-model exhaustive | ≤0.02 |
| Each ordinary query | <60 seconds |
| Simultaneous sampled native+Node RSS | ≤2 GiB |

Calibration can be repeated to diagnose a candidate and applies no held-out relevance thresholds. The final held-out result is a single sealed campaign for one frozen candidate. That invocation launches three sequential, independent fresh application processes; each process creates a fresh synthetic database and starts fresh native processes. The campaign checks before and after each replicate that the application source, harness, package manifests, freeze, accepted native gate, binary hash, model manifest, and runtime version remain pinned. Any change aborts the campaign. All three replicates execute without source edits or tuning between them. A candidate fingerprint can launch only one held-out campaign; a failed or interrupted candidate campaign cannot be silently rerun under the same fingerprint. A changed candidate requires a new frozen candidate fingerprint and its own newly authorized acceptance campaign.

The sealed campaign writes its three subprocess stdout/stderr streams and exit codes under `.native-pagination-build/release-heldout-evidence/results/heldout-campaign-<id>.json`, plus per-replicate result files. Its isolated runtime databases live under `.native-pagination-build/release-heldout-evidence/runtime/`. The top-level stdout contains only campaign identity, replicate count, result path, and exit status. Calibration results remain under `eval/local-indexed/results/`. Do not treat static fixture verification, optimized compilation, or isolated model probes as a held-out quality result.

## Commands and handoff

Calibration requires Main’s accepted native artifact, or an exact SHA-256 pin for a locally authorized offline candidate. The optimized binary currently retained in this workspace has SHA-256 `bb25470359e1f8cb76905adf8ccf0bf68d0d83f2a5b143055773a831fe607aae`; Main approved it for calibration diagnostics. To calibrate that exact binary:

```powershell
$calibrationNative = (Resolve-Path .native-pagination-build/optimized-release/iii.exe).Path
$calibrationNativeSha256 = 'bb25470359e1f8cb76905adf8ccf0bf68d0d83f2a5b143055773a831fe607aae'
node --import tsx benchmark/local-indexed-quality-campaign.ts calibration $calibrationNative $calibrationNativeSha256
```

For a native executable already accepted in `native-accepted.json`, the optional SHA argument can be omitted and the gate path/hash will be checked. Retain the result JSON, process stdout/stderr, and exit code when reporting calibration.

Before held-out, Main must accept the final candidate freeze, candidate package/binary hash, and authorize the held-out leaf. Then run exactly one sealed campaign:

```powershell
$acceptedNative = (Get-Content .native-pagination-build/local-indexed-verification/native-accepted.json | ConvertFrom-Json).executable
node --import tsx benchmark/local-indexed-quality-campaign.ts heldout $acceptedNative --authorize-sealed-heldout
```

An accepted debug executable may be used for calibration diagnostics only. Held-out release acceptance must bind the optimized packaged candidate’s exact binary hash; an optimized compile alone does not establish quality. If the campaign fails an acceptance criterion, report that criterion and evidence to Main; do not tune and rerun the same candidate campaign.
