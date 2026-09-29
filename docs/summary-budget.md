# Session summary budgets

`mem::summarize` budgets each complete map and reduce prompt against the context
window configured for the models that can serve the summary route. Settings use
the normal merged configuration: `~/.agentmemory/.env`, then process environment
overrides. File settings are cached until the worker restarts.

## Settings

```dotenv
AGENTMEMORY_SUMMARY_CONTEXT_TOKENS=131072
AGENTMEMORY_SUMMARY_SAFETY_MARGIN_TOKENS=4096
AGENTMEMORY_SUMMARY_MAX_CALL_INPUT_BYTES=7500
SUMMARIZE_CHUNK_SIZE=400
SUMMARIZE_CHUNK_CONCURRENCY=12
```

Each map and reduce call calculates its output ceiling from that complete prompt:
the ceiling is the minimum of the conservative input estimate, the remaining
configured context after the safety margin, and `AGENTMEMORY_SUMMARY_OUTPUT_TOKENS`
when an administrator explicitly sets it. There is no default fixed output cap;
the 7500-byte per-call input ceiling still bounds ordinary request size.
Every fit check reserves the same per-call ceiling that is sent to the provider.
Choose a context window supported by every primary, auxiliary and fallback model
reachable by the summary route.
The separate 7500-byte ceiling applies to the complete system and user prompt
of every map and reduce call, including a single-call summary. It keeps the
estimated input below the router's 8000-byte Groq cutoff. Increase it only when every routed provider
accepts the resulting request size.

Configured settings must be positive finite safe integers. Concurrency cannot exceed
32. Invalid context reserves, or a context or per-call ceiling too small for the fixed
prompts and 500 estimated bytes of content,
fail before a summary is persisted. `SUMMARIZE_CHUNK_SIZE` remains an optional
additional observation cap (default 400); it is no longer a token estimate.

## Input estimation and splitting

The dependency-free estimate counts UTF-8 bytes of the JSON-escaped system and
user strings, plus 512 for message framing and the supported Ollama structured
output instruction and schema. This deliberately conservative estimate targets
byte and subword tokenizers; it is **not a universal proof for arbitrary model
tokenizers**, provider-added instructions or undisclosed provider context costs.
Keep the margin and use the actual supported context window for your route.

Observations are packed in their existing source order. Oversized observations
are split at Unicode code point boundaries, including titles, narratives, facts,
files and concepts. Their fragments retain the original observation number.
Reducer inputs use the same fitting checks and can split oversized partials;
fragments retain their source observation ranges and order. Fragmentation never
truncates input text or increases the persisted `observationCount`.

Chunk groups are balanced by estimated prompt size. The available input budget is
apportioned across the configured concurrency, with a target of at least 500
estimated content tokens per call when the workload allows. `SUMMARIZE_CHUNK_SIZE`
remains an additional observation-count cap. Map calls and each reduce round run
in parallel batches, up to the configured concurrency (default 12, maximum 32);
results retain source order. Reduce calls run in bounded rounds: each completed
nonfinal round must strictly decrease the total serialized partial size plus
per-partial framing cost. Outputs that do not shrink cause
`summary_reduce_no_progress`. Work is limited to 4096 packed items per packing
operation, 4096 selected-provider calls across a summarize invocation (including
its final parse retry), and 12 adaptive/reduction levels. Provider wrappers and
transports retain their existing bounded internal fallback/retry policies; those
internal attempts are additional to the selected-provider call limit. These
limits bound work, not elapsed completion time or the iii invocation deadline.

Explicit context or token-limit errors trigger bounded subdivision with a
smaller input budget; each retry recalculates its output ceiling from the new
prompt. Ambiguous
errors such as `invalid_content: empty or too large` do not prove a size failure
and retain the existing retry-once behavior. Unusable map chunks can still be
skipped; more than half failing aborts the summary. Intermediate reduce failures,
budget exhaustion and lack of progress abort without persisting a final summary.

## Output reserve and providers

Summary requests send the calculated output ceiling as an explicit per-call
option. `AGENTMEMORY_SUMMARY_OUTPUT_TOKENS`, when set, is an administrator ceiling
and has no default. The task router preserves the per-call value through auxiliary
selection and primary fallback.
OpenAI-compatible (including Fireworks, DeepSeek and Azure), OpenRouter/Gemini
compatibility, Anthropic, MiniMax and native Ollama requests receive the ceiling
in their request bodies. Existing resilience, account-pool and fallback-chain
wrappers forward it. The calculated ceiling can exceed the old summary task cap
of 768 and constructor cap of 4096 **only for session summary calls**.
`MAX_TOKENS`,
`AGENTMEMORY_AUX_LLM_MAX_TOKENS`, other tasks, Fireworks Batch and TypeSafe keep
their existing behavior.

Claude Agent SDK does not expose an equivalent per-call output cap in this
adapter. Input packing still applies, but its actual output limit cannot be
enforced by these settings. Use a supported REST provider when the reserved
output cap must match the transmitted request. Thinking models may consume part
of their requested output allowance on reasoning according to provider behavior.

Diagnostics record counts and failure categories without prompt or provider
payloads. Existing XML parsing, summary validation, audit persistence, routing
usage telemetry and circuit-breaker behavior remain in place.

## Durable queue recovery

Summary jobs keep their units in iii state and publish deliveries through
`agentmemory.summary.unit`. The worker reconciles pending jobs every minute.
A first delivery that remains unresolved while the topic is busy is eligible
for replay after the configured provider timeout plus a one-minute margin,
with a minimum of ten minutes. Failed provider calls retain their bounded
retry delay. Each replay gets a new delivery ID, so an older queued delivery
cannot start the same unit after the replay is recorded. A unit already running
in the current worker is protected for the same runtime window.

The application-level reconciliation does not alter iii-queue's transport
records. If a completed job still appears as an active delivery after an
engine restart, inspect the queue records and job state separately before any
transport repair; do not purge the topic or its backing store.
