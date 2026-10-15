# Dashboard savings methodology

## Current implementation (2026-10-09)

The dashboard now separates observed Codex usage, emitted memory context,
context selection and recorded processing overhead. Net savings still require
a matched without-memory baseline; they are unknown, not zero.

### Price benchmark

All valuations use GPT 6.1 Sol, medium, Standard API-equivalent pricing in USD,
including when the original request used another model. These are comparison
values, not provider invoices, Codex subscription refunds or quota credits.

Prices checked against the [official model page](https://developers.openai.com/api/docs/models/gpt-6.1-sol)
on 2026-10-09, per million tokens: $2 input, $0.10 cached input, $2.50 cache
writes and $10 output. Above 272,000 input tokens, input/cache prices double
and output increases by 1.5 for the entire request. Reasoning is already part
of output and is not added twice. Medium has no separate per-token price.
Batch, Flex, Fast, Ultrafast and regional adjustments are not assumed.

Cost = ((input - cached - writes) × input rate + cached × cache rate +
writes × write rate + output × output rate) / 1,000,000.

### Codex usage sample

The authenticated status function reads the current OS user's rollout files
under `CODEX_HOME/sessions` or `~/.codex/sessions`, selecting the last seven
days. Only aggregates leave the collector; prompts, responses, session IDs
and paths are not returned. Isolated agentmemory profiles disable this
profile-wide collection because rollouts cannot establish agent ownership.

`token_usage_record` events are deduplicated by response ID. A record's thread
ID must match the rollout's own thread, excluding inherited fork/subagent
history. Own subagent responses remain eligible. Cumulative `token_count`
events are ignored. Unsupported older records, archived/deleted rollouts and
unrecorded requests are not reconstructed.

Read limits: 100 recent files, 64 MiB total, 16 MiB per file and a five-second
read deadline. Results are cached five minutes with a shared in-flight read.
The UI reports skipped files, malformed records, missing usage and compaction
markers. Partial samples are labeled and are not extrapolated to all-time use.
The cache ratio is token-weighted cached input / total input, not the share of
requests with any cache hit. Per-request prices handle long context.

Cache discounts belong to the model cache and are not agentmemory savings.
The observed blended input price is descriptive; attributing that cache mix
to memory specifically remains a scenario assumption.

### Context counts

Updated SessionStart, PreToolUse and PreCompact hooks report text after their
stdout write completes. Notifications use the existing observation transport
but are routed to metrics before admission, preventing memory creation or
compression/LLM processing. Telemetry is best effort and time bounded.

Local `o200k_base` BPE counts actual emitted text, including host-specific JSON
wrappers. Only counts, source, receipt time, event ID and a hashed session ID
are stored. A native bounded append retains the latest 500 combined hook/MCP
events per server agent identity. Aggregation excludes duplicate event IDs.
Payloads above 32,000 characters are not sampled. Older hooks, failed reports
and larger payloads create gaps; no historical token count is invented.

Successful MCP tool responses count text blocks separately. They have no
reliable caller session ID and do not enter hook per-session averages. Serving
a response or writing stdout is not proof of model consumption. Protocol
framing, resources, prompts, schemas, client transformations and repeat
exposures in later model requests are not attributed.

Hook means and medians use sessions present in the bounded sample, including
multiple emissions per session. They exclude sessions without observed
emissions; boundary sessions may be incomplete. The server injection flag is
shown separately because hook processes have their own environment.

Context budgets now tokenize complete rendered text, including tags,
separators and newlines, before accepting blocks. Search budgets count
rendered narrative or serialized result arrays, excluding the search envelope
and transport metadata. Sol's exact server tokenizer/framing are not
established: local text counts remain labeled estimates. No remote tokenizer
API is called.

### Selection scenario and processing

Paired hook events compare eligible candidate memory before selection with
emitted text. This is selection reduction, not raw-source compression or proof
of avoided file reads. Single-exposure valuations show fully cached, uncached
short-context and profile-cache-weighted scenarios, before processing costs.
Negative differences are preserved. Eligible context is not a proven
without-memory workload or equivalent-quality baseline.

New processing records retain 1,000 reported usage events per server agent
identity, selecting the latest seven days. Input, cache read/write and output
counts are stored without text. Missing cache fields widen the valuation
envelope; missing input/output excludes a record from pricing and is reported.
Anthropic's exclusive input counts are normalized to include cache reads and
writes. Reported usage is retained when task operations fail after a response.

Legacy processing totals are shown separately: their period, per-request
cache and context thresholds are unavailable. Neither sample bounds total
overhead: failed billed calls without usage, providers outside the callback,
embeddings and infrastructure may be missing. Bounded processing/context
samples may cover different events even within seven days. Do not subtract
their totals and call the result net savings.

The code and bundled hooks must be installed and started before these new
context/processing samples accumulate. No settings outside the repository are
changed by this implementation.

The dashboard treats **net savings as unknown** until comparable
usage data exists. This means unknown, not zero. Stored memories, session
counts and configured context budgets cannot establish net savings.

## Why the previous estimate was removed

The previous calculation assigned 80 tokens to every observation, assumed one
context injection per session capped at 2,000 tokens (overridable through the
viewer URL), and treated the difference as saved tokens. It converted that
difference to dollars at $0.30 per thousand tokens ($300 per million).

None of those inputs measured actual avoided usage. The comparison assumed
all stored observations would otherwise be sent to a model. It ignored
different observation lengths, whether recalled content was useful, repeated
injections, retrieval responses, prompt caching and memory processing costs.
Increasing stored history could therefore increase the displayed savings
without demonstrating any reduction in model usage.

## Requirements for a defensible value

1. Compare equivalent tasks with and without agentmemory, using comparable
   models, settings and outcome quality. Record the measurement period,
   sample size and variability rather than extrapolating all-time savings
   from a session snapshot.
2. Record actual input, output and cached tokens for agent requests, including
   repeated context injections and retrieval responses. A response token
   estimate alone is not proof that the agent consumed it or avoided work.
3. Include agentmemory model usage for compression, summarization,
   consolidation and embeddings. Account for shared processing costs within
   the same measurement scope without double counting.
4. Compute net tokens as baseline usage minus usage with agentmemory,
   including its processing overhead. Preserve negative results.
5. Compute costs separately per model and token category using applicable
   prices, discounts and cache treatment. Token reduction is not necessarily
   an equal percentage reduction in cost. Report infrastructure costs
   separately if claiming total financial savings.

Until these measurements exist, do not substitute a hypothetical full-history
baseline for observed savings. A future context-size comparison may be useful,
but must be labeled as such and must not be presented as realized savings.
