# TypeSafe.ai decisions

This document describes AgentMemory's TypeSafe.ai configuration and the scope of its compaction decisions.

## Configuration

TypeSafe.ai supplies typed decisions, including choices, scores, and Noul decisions. It is separate from the primary or auxiliary model used to generate summaries, facts, and prose.

| Variable | Default | Purpose |
|---|---:|---|
| `TYPESAFE_API_KEY` | unset | Enables authenticated TypeSafe.ai requests. |
| `AGENTMEMORY_TYPESAFE_ENABLED` | `true` | Master switch for every TypeSafe feature. |
| `AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED` | `true` | Selects eligible, non-protected inputs before graph extraction and consolidation. |
| `AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED` | `true` | Controls automatic rejection for eligible non-mutating tool observations. |
| `AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED` | `true` | Gates eligible automatic graph, consolidation, reflection, and skill analysis. |
| `AGENTMEMORY_TYPESAFE_SCORING_ENABLED` | `true` | Scores eligible non-mutating observations before synthetic or LLM compression. |
| `AGENTMEMORY_TYPESAFE_TIMEOUT_MS` | `5000` | Request timeout, bounded to 30 seconds. |
| `AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS` | `16000` | Maximum decision-state size, bounded to 64,000 characters. |

The feature switches default to enabled. Actual requests require `TYPESAFE_API_KEY`. Set the master switch to `false` to disable every TypeSafe feature, or set an individual feature switch to `false` to disable only that feature. A missing key, disabled switch, TypeSafe error, or unusable response falls back to the operation's existing behavior.

## Current integrations

Compaction runs before graph extraction and consolidation. Decisions, errors, writes, recent observations, high-importance items, and image or mixed-modality observations are pinned deterministically. Only eligible ambiguous items go to TypeSafe in one bounded batch; state is privacy-filtered and reduced to a short preview. A deterministic empty-notification rule can drop obvious noise locally. TypeSafe drops an ambiguous item only at confidence `0.85` or higher; a keep answer, lower-confidence drop, missing answer, or error preserves the item.

Admission and importance scoring share one request for eligible non-mutating tool observations, including qualified Codex and MCP names such as `read_mcp_resource` and `mcp__...__search_graph`. Protected signals bypass TypeSafe. Admission discards only on a `discard` answer with confidence at least `0.75`. Scoring updates importance only when confidence is at least `0.55`. With `AGENTMEMORY_AUTO_COMPRESS=true`, this decision happens before the LLM call: a confident discard avoids compression spend, while an accepted score is forwarded into `mem::compress` as the authoritative importance.

Pipeline gates run only on eligible automatic/deferred work: semantic and procedural consolidation, graph extraction, reflection clusters, and completed-session skill extraction. Explicit/manual or forced requests and batch replays bypass these gates. Calibrated skip thresholds are `0.60` for graph extraction, `0.60` for semantic consolidation, `0.65` for procedural consolidation, `0.80` for reflection, and `0.75` for low-signal skill extraction. Otherwise the existing pipeline runs.

```env
TYPESAFE_API_KEY=your-typesafe-api-key
AGENTMEMORY_TYPESAFE_ENABLED=true
AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED=true
AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED=true
AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED=true
AGENTMEMORY_TYPESAFE_SCORING_ENABLED=true
```

Keep the API key in the local `.env` file or deployment secret store; do not commit it.

## Graph input compaction

Compaction makes keep/drop decisions about graph-extraction observations and consolidation inputs; it does not generate replacement summaries. AgentMemory pins decisions, errors, writes, recent observations, high-importance entries, and image or mixed-modality observations locally. Empty, low-importance notifications can be dropped locally. Only the remaining ambiguous items are sent in one batch with a privacy-filtered preview, capped at 16 candidates and 16,000 serialized state characters by default.

TypeSafe drops an ambiguous item only when it selects `drop` with confidence at least `0.85`. A keep answer, lower-confidence drop, missing answer, size-limit overflow, timeout, or provider error preserves the observation. The feature never asks TypeSafe to invent a summary.

## Observation admission and scoring

Admission and importance scoring share one request for eligible non-mutating tool observations. Protected signals bypass TypeSafe. The decision state contains compact input/output previews rather than the full tool result. Admission discards only on a `discard` answer with confidence at least `0.75`. Scoring updates importance only when confidence is at least `0.55`; the score is bounded to the observation's supported importance range.

When `AGENTMEMORY_AUTO_COMPRESS=true`, the shared TypeSafe request runs before `mem::compress`. A confident rejection prevents the LLM call; otherwise the bounded TypeSafe importance is passed into the compressed observation. `mem::compress` does not make a duplicate TypeSafe request. If admission or scoring is disabled, the existing observation path remains in effect. `AGENTMEMORY_AUTO_REJECT` is not a configuration key; automatic rejection is controlled by `AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED`.

Successful, unavailable, timed-out, and rejected TypeSafe requests emit metadata-only runtime logs containing the feature, outcome, question count, and latency. State, answers, and credentials are never logged.

## Automatic pipeline gates

The pipeline-gate switch covers semantic and procedural consolidation, graph extraction, reflection clusters, and completed-session skill extraction. Gates run only on eligible automatic/deferred work. Explicit/manual or forced requests and batch replays bypass them. A gate skips work only for a high-confidence `skip`; otherwise the existing pipeline runs.

This feature does **not** rewrite the host agent's conversation transcript. AgentMemory's `PreCompact` hook receives session metadata and writes recalled memory context to hook output; its contract does not provide editable transcript messages or a replacement-history response. Enabling `AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED` therefore does not remove or truncate prior host messages or tool results.

## Future host transcript compaction

The `fast-jev-compaction` approach is only applicable when a host exposes a supported way to read and replace transcript content. An adapter for such a host should:

1. Preserve user instructions, decisions, errors, commands, code changes, security-relevant output, and exact identifiers deterministically.
2. Drop only unambiguous noise locally, such as duplicate reads or empty status output.
3. Send only ambiguous candidates to TypeSafe, using a compact state rather than the full transcript on every request.
4. Decide separately whether a tool call and its result are worth keeping, with bounded candidate count, input size, and request count.
5. Fail open: on timeouts, errors, or unusable answers, preserve the original transcript.

AgentMemory does not currently install such a host adapter. These rules describe a safe future integration boundary; they do not describe behavior of the current `PreCompact` hook.
