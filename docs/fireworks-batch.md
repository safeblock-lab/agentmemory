# Fireworks Batch

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

Queued work, job IDs, and completed responses are stored through iii-engine state. Compatible deferred work waits until the queue reaches `MIN_ITEMS` or its oldest item reaches `MAX_WAIT_MS`; restarting the service preserves both the work and its age. Batch results are polled in the background and applied to the same consolidation and graph paths as online responses. Failed or unconfirmed jobs are retained as dead letters rather than submitted again with a new ID.

The default poll interval is one minute. Polling only observes remote jobs and checks the local queue; it does not force a submission before the size or age policy is met.

Batch is eventual work: Fireworks can take up to 24 hours. Session-stop graph extraction, scheduled or session-end consolidation, deferred reflection, and automatic crystallization are eligible because their callers do not wait for a result. Direct graph extraction, consolidation, reflection, and crystallization calls do not enter Batch, so they preserve their normal response behavior.

Each queued maintenance request carries a fingerprint of the source data used to create its prompt. Before a completed Batch response mutates local state, AgentMemory recomputes that fingerprint. If the source changed during the delay, the old response is marked stale, discarded, and a fresh deferred request is queued. This prevents delayed work from overwriting newer maintenance state.

Each job uploads only its input JSONL dataset and declares its exact row count as `exampleCount`. Fireworks creates the output dataset named by the job, which is later downloaded to apply the completed responses.
