# Fireworks Batch

Batch is disabled by default. It only runs consolidation and graph extraction; summaries and flow compression continue to use the normal auxiliary route.

```env
AGENTMEMORY_FIREWORKS_BATCH_ENABLED=true
FIREWORKS_ACCOUNT_ID=your-account
FIREWORKS_API_KEY=your-fireworks-key
OPENAI_BASE_URL=https://api.fireworks.ai/inference/v1
OPENAI_MODEL=accounts/your-account/models/your-batch-model
```

This reuses the same Fireworks account, API key, and model as the primary OpenAI-compatible setup. It is independent of `AGENTMEMORY_AUX_LLM_*`, so a local/free auxiliary can continue handling summary and flow compression. The `AGENTMEMORY_FIREWORKS_BATCH_*` values are optional overrides; existing Fireworks auxiliary settings remain a compatibility fallback only.

Queued work, job IDs, and completed responses are stored through iii-engine state. Batch results are polled in the background and applied to the same consolidation and graph paths as online responses. Failed or unconfirmed jobs are retained as dead letters rather than submitted again with a new ID.

Each job uploads only its input JSONL dataset and declares its exact row count as `exampleCount`. Fireworks creates the output dataset named by the job, which is later downloaded to apply the completed responses.
