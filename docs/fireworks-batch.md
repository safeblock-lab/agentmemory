# Fireworks Batch

This document describes the bounded, restart-safe Fireworks Batch path and the operator checks needed when remote work or downstream callbacks cannot be completed automatically.

Batch is disabled by default. It runs only explicitly deferred consolidation, graph extraction, reflection, and automatic crystallization; direct API, MCP, and runtime calls remain synchronous. Summaries and flow compression continue to use the local auxiliary route.

```env
AGENTMEMORY_FIREWORKS_BATCH_ENABLED=true
AGENTMEMORY_FIREWORKS_BATCH_MIN_ITEMS=16
AGENTMEMORY_FIREWORKS_BATCH_MAX_WAIT_MS=3600000
FIREWORKS_ACCOUNT_ID=your-account
FIREWORKS_API_KEY=your-fireworks-key
OPENAI_BASE_URL=https://api.fireworks.ai/inference/v1
OPENAI_MODEL=accounts/your-account/models/your-batch-model
```

This reuses the same Fireworks account, API key, and model as the primary OpenAI-compatible setup. It is independent of `AGENTMEMORY_AUX_LLM_*`, so a local/free auxiliary can continue handling summary and flow compression. The `AGENTMEMORY_FIREWORKS_BATCH_*` values are optional overrides; existing Fireworks auxiliary settings remain a compatibility fallback only.

Queued work, job IDs, and completed responses are stored through iii-engine state. Enqueue uses a bounded write-ahead journal (`mem:fireworks-batch:enqueue-intents`, at most 4,096 intents and 8 MiB) before publishing the active-work index. Startup and every processing pass replay that journal, restore missing canonical records, and repair the active index; no historical queue scan is needed. Compatible deferred work waits until the queue reaches `MIN_ITEMS` or its oldest item reaches `MAX_WAIT_MS`; restarting the service preserves both the work and its age. Batch results are polled in the background and applied to the same consolidation and graph paths as online responses. Failed or unconfirmed jobs are retained as dead letters rather than submitted again with a new ID.

Fireworks returns successful rows and failed rows in separate output-dataset files. Reconciliation validates every JSONL row, accounts for every locally owned `custom_id`, applies only owned successful rows, and dead-letters owned error rows. Unknown IDs are ignored with a warning; missing, malformed, or duplicate rows keep the job retryable until the bounded attempt limit. A completed job is terminal only after result reconciliation and its completion callback both succeed. `JOB_STATE_EXPIRED` is treated as result-bearing when Fireworks has saved output files.

The default poll interval is one minute. Polling only observes remote jobs and checks the local queue; it does not force a submission before the size or age policy is met.

Batch is eventual work: Fireworks can take up to 24 hours. Session-stop graph extraction, scheduled or session-end consolidation, deferred reflection, and automatic crystallization are eligible because their callers do not wait for a result. Direct graph extraction, consolidation, reflection, and crystallization calls do not enter Batch, so they preserve their normal response behavior.

Large graph inputs use `AGENTMEMORY_GRAPH_INPUT_TARGET_CHARS` as a soft prompt target, not a truncation limit. The local Ollama auxiliary compacts routine input only when needed; decisions, errors, and file changes remain source-backed, and complete units are partitioned into separate queued rows when one prompt is too large. The old `AGENTMEMORY_GRAPH_MAX_INPUT_CHARS` name remains accepted as a compatibility alias.

Each queued maintenance request carries a fingerprint of the source data used to create its prompt. Before a completed Batch response mutates local state, AgentMemory recomputes that fingerprint. If the source changed during the delay, the old response is marked stale, discarded, and a fresh deferred request is queued. This prevents delayed work from overwriting newer maintenance state.

Submission records `submitAttemptedAt` before the remote submit call. If the response is ambiguous, the worker reconciles the requested ID and at most 32 recent `fwbjob-` candidates, for at most three reconciliation attempts, then polls only a proven remote identity. It never submits the same work again under a new ID. A verified duplicate/already-existing response follows the same reconciliation path; validation failures go directly to dead letter.

The provider applies explicit resource and network bounds: job discovery stops at 32 pages, result manifests at 32 files, downloads at four concurrent files, each response at `AGENTMEMORY_FIREWORKS_BATCH_MAX_RESPONSE_BYTES`, and materialized results at the configured character limit plus a 64 MiB hard ceiling. Signed result URLs must be HTTPS without credentials or fragments, DNS resolution rejects every private, loopback, link-local, multicast, metadata, or other special-use address, the first validated address is pinned for the request, and redirects are rejected.

Callback effects use one in-process processor flight, deterministic destination locks, and a durable key derived from the work-item ID. Callback receipts live in `mem:batch-callbacks`; destination records retain `appliedBatchEffects`. The graph, reflection, crystallization, consolidation, lesson, and related writer paths acquire the shared admission/record locks so a retry after a crash cannot apply the same effect twice. Maintenance waits for admitted callbacks to quiesce and refuses to proceed while an ambiguous receipt or graph application is unresolved.

AgentMemory also caps output by maintenance task: graph extraction uses 512 completion tokens, consolidation 768, and reflection or crystallization 1024. The same caps apply to synchronous primary calls, so a task cannot consume the global completion limit merely because it is not batched.

When a provider returns token usage, AgentMemory persists aggregate usage by task, provider, and model in its existing metrics state. This records counts only; prompts, responses, credentials, and headers are never stored. Providers that omit usage are counted separately as unreported usage calls.

Each job uploads only its input JSONL dataset and declares its exact row count as `exampleCount`. The provider preserves the canonical remote job name and input/output dataset identifiers returned by Fireworks, even when they differ from locally requested IDs. On the first startup after the active-index migration, AgentMemory reads at most 4,096 recent remote job IDs through Fireworks' paginated API, keeps only its `fwbjob-` IDs, and repairs matching active local jobs by direct key lookup. The recovery checkpoint is persisted, so normal polling uses only bounded local active indexes and never scans historical canonical scopes. If submission returns a verified duplicate/already-existing diagnostic, the known requested job ID is polled; permanent validation errors are not blindly retried.

If callback attempts are exhausted, the job and its work items become dead letters with their callback receipts retained. Manual recovery must inspect `mem:fireworks-batch:jobs`, `mem:fireworks-batch:work-items`, and `mem:batch-callbacks` through the viewer's KV browser or an equivalent authenticated state tool, then reconcile the downstream records named by the receipt before clearing an active receipt or creating replacement work. Do not delete receipts, reset `completionIntent`, or resubmit a dead-letter item while the downstream effect is ambiguous; the locks deliberately keep later maintenance blocked until the partial application is resolved.
