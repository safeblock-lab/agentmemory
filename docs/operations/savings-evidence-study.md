# Current savings evidence study

This bounded local study supplies the observable inputs for the dashboard savings calculation. It cannot identify net savings: a comparable without-memory baseline and disjoint processing attribution are unavailable. The metadata snapshot is retained at `.codex-task/panel-restoration-20261017/savings-evidence/current-study.json`; it is a dated study, not a live health claim.

## Period and method

All observations use **2026-10-11T10:04:31.942Z through 2026-10-18T10:04:31.942Z**, inclusive. Existing `collectCodexUsage` and `collectClaudeUsage` ran with the same `now`. At 10:04:54.602Z, two exact read-only `state::get_versioned` queries read configured agent key `default`: `mem:context-usage/context-usage:default` version 76 and `mem:processing-usage/default` version 574. Aggregation filtered and deduplicated retained events inside that same period. No standalone SQLite access, provider call, state mutation, service restart, health probe, build, or test was performed.

Codebase graph coverage was dated October 3 and did not track these new usage modules; exact source reads supplied the implementation facts. No graph refresh was performed.

Scope is available local Codex/Claude profiles, including responses owned by main and subagent threads that the bounded collectors encounter. It does not identify a project, a timeline task, colleagues, or team-wide usage. Codex uses response ID deduplication and owning-thread checks. Claude deduplicates cumulative streaming messages/fork copies by message ID. There is no common provider request identity proving cross-client deduplication; these are client-specific sources, not two sums of one shared log.

## Observed components

| Component | Observed result | Coverage |
| --- | ---: | --- |
| Codex responses | 1,383 across 39 sessions | Partial: 45 files, 64 MiB cap reached, seven oversized lines skipped |
| Agent input, including cache | 157,938,125 tokens | Observed subset |
| Cached input, already included above | 153,867,648 tokens | Do not add again |
| Agent output | 881,716 tokens | Includes 446,774 reasoning output tokens; do not add reasoning again |
| Agent input plus output | 158,819,841 tokens | Complete profile total unknown |
| Claude usage | Unavailable | No local profile found; not evidence of zero use |
| Hook context | 45 emissions, 64,379 tokens | 74 total in-period context events; capture coverage unknown |
| MCP text replies | 29 replies, 34,770 tokens | Separate from hook emissions |
| Eligible memory before selection | 71,615 tokens across 45 emissions | Selection metadata only |
| Candidate minus emitted hook context | 7,236 tokens | Not observed avoided reads or net savings |
| AM processing | 499 calls; 624,063 input + 148,892 output | 500 retained events; saturated, oldest in-period event October 17 at 22:19:40.636Z |
| Processing cache categories | Unknown for all 499 calls | No defensible cached/uncached valuation |
| Comparable retrieval/read pairs | No defensible pairs in studied metadata | Counterpart identity absent |

Context estimates use `o200k_base`, count one text emission, and do not prove model consumption, tool framing, client transformation, or repeated prompt exposure. The 500-event retention limit is a limit on events, not a seven-day completeness guarantee. No invalid events or duplicate event IDs were observed within the retained period samples. Capture began at an unknown time. The profile collector also limits directories, entries, files, line bytes, record count and elapsed time; exact bounds are in the snapshot.

## Attribution and comparable baseline gap

`MetricsStore.recordLlmUsage` receives task, provider and model, but the historical `recordProcessingUsage` call retains only tokens, timestamp and effect ID. Batch effect keys already prevent duplicate batch application and serve as processing event IDs; ordinary processing uses a generated event ID. Neither those event IDs nor the existing usage fields establish equivalence to a client response ID. Direct external HTTP processing could be disjoint from local agent rollouts; a CLI-backed path could overlap. Historical events do not establish which case applies. Processing must remain non-additive with `overlapWithAgentUsage: "unknown"` and `disjointTokens: null`.

The studied records contain no linked tuple of task, artifact identity, artifact version, required content, actual source-read token exposure, and substituted retrieval exposure. No defensible paired baseline can therefore be reconstructed from these feeds and usage counters. This does not claim that no such evidence exists anywhere in raw project history; raw content history and other state scopes were outside this bounded study. Candidate memories are eligible context, not the counterpart of a file read. No central substitution fraction or extrapolation from these samples to the complete profile is justified.

## Sensitivity without invented benefits

Let `B` be comparable avoided-read input tokens supported by future paired evidence, `C` attributable introduced context exposure, and `P` processing tokens demonstrated to be disjoint from agent usage. A one-exposure token scenario is `B - C - P`. None of `B`, actual model exposure `C`, or disjoint `P` is fully identified here. This equation is a scenario, not an estimator or monetary valuation.

For illustration only, hook plus MCP text is 99,149 estimated tokens. If every emission were consumed once and no processing were disjoint, the scenario breaks even at `B = 99,149`. If every observed processing input/output token were additionally disjoint, the threshold becomes `B = 872,104`. These assumptions are deliberately explicit; they are not defensible lower/upper bounds on actual net savings, and missing processing retention can move the threshold further. The 7,236 selection difference supplies neither `B` nor a substitution factor. Monetary sensitivity additionally requires per-model cache categories and tariffs and must not treat cached input discounts as agentmemory savings.

## Shared implementation contract

`src/functions/savings-evidence.ts` exports `readSavingsEvidence(kv, suppliedAgentUsage?)`, `aggregateSavingsFeeds(contextFeed, processingFeed, period)`, and `SavingsEvidence`. `readSavingsEvidence` uses the actual local agent collector period when available; it can accept the already-read snapshot to avoid a second scan. Agent isolation suppresses profile attribution. Feed failures produce unavailable status and null token totals, not assumed zero usage. Context and processing totals retain coverage, invalid/duplicate counts and oldest/newest observed timestamps. Processing cache totals remain null when absent; reported input/output subsets remain separately identifiable. A negative candidate-minus-emission difference is preserved when formatting expands context.

The implementation adds no MCP tool, endpoint, KV scope or provider call. Installation and live activation belong to Main. This study executed only the specifically authorized local data reads; no tests, typecheck, artifact verification, build or extra health checks were run.

Forward capture accepts an optional fourth `{task, provider, model}` argument to `recordProcessingUsage`, retaining only the three whitelisted strings, each at most 256 ASCII identifier characters. Main owns the `MetricsStore` callsite integration. It does not backfill historical events, retain raw content, fabricate a provider request ID, or establish disjointness by itself. `missingProvenanceCalls` identifies the remaining metadata gap in each bounded aggregate.
