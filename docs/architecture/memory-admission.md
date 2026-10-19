# Capture and memory admission

This document describes source changes for storage-growth control. These changes have not been built, tested, installed, or exercised against a live database. They do not establish a physical storage reduction.

## Capture bounds

`src/hooks/_capture-bounds.ts` applies UTF-8 byte limits to tool input (16,384 bytes) and tool output/error (8,192 bytes). Both successful and failed tool hooks use these bounds. Short values retain their JSON shape. Oversized values become text excerpts with an explicit marker containing original byte count, cap, and notice that the suffix was not captured. The marker consumes part of the cap. Prefix cuts preserve complete UTF-8 code points.

Images retain the existing separate extraction path. These text caps do not constitute a limit on image payloads or the complete hook request. Captured excerpts cannot reconstruct an uncaptured suffix.

## Deterministic admission

`src/functions/memory-admission.ts` separates publication decisions from evidence capture. `mem::remember` uses exact content, type, project, agent scope, attribution, and evidence metadata. An unscoped record is not a wildcard for a scoped record.

| Action | Condition | Publication |
| --- | --- | --- |
| `store` | No exact compatible current record | New memory |
| `update` | Explicit current target, expected version, same scope and attribution, changed content | Linked successor retaining predecessor sources |
| `merge` | Exact content and attribution, additional evidence/concepts/files | Linked successor with unchanged text and union of metadata |
| `skip` | Exact compatible content with no new evidence | Existing memory; no new embedding call |

Lexical similarity is advisory only. It cannot establish equivalence, compatible expansion, supersession, or temporal change. A merge with changed content is rejected. Explicit updates and metadata merges preserve historical versions, source IDs, session IDs, and provenance. A metadata merge increments the domain version; it is not an in-place overwrite. Neither skip nor merge deletes original observations.

## Attribution boundary

Source kinds distinguish `unknown`, `human_source`, `ai_output`, `proposal`, and `observed_result`. These are descriptions supplied with evidence, not authenticated roles. Legacy records remain unknown. A source kind never asserts human adoption, approval, membership, or permission. There is deliberately no client-settable adopted-decision kind. Proposals remain proposals unless a separate trusted adoption workflow establishes evidence and authority.

## Publication and limits

The existing observation admission primitive remains responsible for canonical observation identity and event replay. Content changes remain distinct originals. Memory admission itself does not invoke a semantic provider. Metadata merges and duplicate skips avoid re-embedding unchanged text. Metadata-only versions also avoid content-change cascade invalidation.

Scoped memory successors use the existing native canonical `evolve` transaction. An explicit predecessor guard checks both ID and domain version on every bounded attempt; a changed canonical winner cannot silently become the new target. Native publication and predecessor retirement occur together. The stable canonical title is retained across revisions. Unscoped or noncanonical targets return an explicit pending/conflict result; there is no two-write fallback. Matching direct successors that already contain all submitted evidence allow completed update/merge retries to resolve without republishing. A stale request introducing different evidence remains a conflict. Scoped new stores use native create-only evolve: a concurrently occupied canonical title is a conflict, never permission to supersede. A title collision returns the retained input and existing memory ID without publishing a derivative. This conservative outcome also applies to different facts/types/agents sharing the same canonical project/title key. Unscoped new stores retain the existing local keyed lock and KV publication; global duplicate suppression across multiple daemons is not supplied for that legacy path. Candidate lookup currently covers the memory corpus to avoid interpreting a ranked top-k miss as proof of novelty; large-corpus admission indexing is a separate optimization.

Audit calls retain the existing producer interface. The repository's `recordAudit` implementation currently does not persist records; returned `admission.action` and `admission.reason` expose each decision to the caller. No new durable audit capability is claimed.

## Incremental transcript producer

`session-end.ts` is the demonstrated repeated-prefix producer: it previously replayed user prompts from its transcript tail on each end/Stop invocation. Individual tool and prompt hooks remain individual events. The transcript hook now sends a complete-line UTF-8 window of at most 256 KiB through the existing observe endpoint. A prefix outside that window is explicitly marked unavailable. Partial trailing JSONL lines wait for a later delivery.

The server checks source revision, window start, previous end, and a digest of the complete previously checkpointed prefix. A matching prefix processes only appended lines; edits, truncation, replacement, or a displaced window cause bounded replay. Each prompt event uses producer/source/revision/absolute row offset/line digest identity, so replay after a lost acknowledgement does not increment the event again. At most 50 user rows are published per delivery; the checkpoint stops at the processed line, leaving the remainder for later delivery.

Checkpoint metadata lives on the existing session row, with at most eight producer keys per session. Checkpoints advance only after canonical admission acknowledges every selected original. Failure or interruption before checkpoint storage leaves replayable work. A stale cross-process checkpoint can cause replay, but cannot establish continuity for a mismatching prefix. Non-user and malformed JSONL rows are not promoted to prompts; malformed-line counts appear in the capture result. No promise covers transcript bytes outside the delivered window.

## Pre-persistence input gate

The revision-2 source filter in `memory-admission.ts` runs in `mem::observe` before image persistence, canonical observation creation, optional provider work, and search indexing. `mem::remember` rejects empty content and skips compatible exact duplicates before publication and embedding. Existing MCP/REST content validation remains ahead of domain admission. No public option bypasses the gate.

The empty-content rule accepts only unequivocally empty JSON/text: null/undefined, whitespace, empty containers, or empty values under known content-wrapper fields. Numbers and booleans remain meaningful. Unknown fields, domain-shaped objects, excessive nesting, provenance/evidence fields, errors, decisions, changes, permission/security context, failure events, lifecycle events, and unknown event types are preserved. Whitespace under `prompt` can be rejected; an unfamiliar empty `config` object remains ambiguous and is retained. Neither length, age, nor lexical similarity establishes rejection. Tool byte caps are capture bounds, not semantic value classification.

Recognized `context_emitted_v1` requests continue through the existing server telemetry handler instead of becoming canonical originals. Routing requires its exact known field/source/event-ID shape and successful handler validation. A declared event label alone is insufficient: extra semantic/error fields or unfamiliar shapes fall through to ordinary observation capture. This protocol dispatch does not establish authenticated human authorship, adoption, or meaningful task use. Other telemetry-looking events remain ambiguous and are retained.

Exact observation retries probe the existing native event receipt and compare its digest, observation ID, session, hook type, complete sanitized raw body, and retained occurrence count. A matching original returns before image writes or optional providers. Changed content/output, a new event timestamp, or changed producer-row identity does not qualify as that same retry. Native canonical admission remains authoritative for races; repeated content with a distinct event preserves occurrence/provenance updates while avoiding another original and derived embedding. No rejected payload or per-rejection receipt is stored.

Admission reasons are bounded: `empty_content`, `recognized_telemetry`, `duplicate_event`, `exact_duplicate`, and the pre-existing optional classifier's `configured_policy`. The new deterministic filter introduces no provider call and does not activate or expand the optional classifier. Configured classifier decisions remain separately identified.

One fixed aggregate row, `KV.metrics / admission-growth-v1`, holds only five reason counters, summed input byte counts, schema version, and update time. Counts saturate at `Number.MAX_SAFE_INTEGER`; there are no rejected bodies, IDs, unbounded labels, per-session rows, or rejection event logs. Updates are best effort under the existing local mutex: multiple daemons can undercount concurrent attempts. Counter failure does not turn rejection into publication. Byte totals describe incoming content not admitted to another original/derivative, not net physical storage savings, provider spend, or authoritative global accounting. They must not be subtracted from the historical physical storage baseline.

## Delivery status

No tests, typecheck, hashes, extra verification, provider calls, installed hook changes, service actions, or historical cleanup were run for this source delivery. Existing installation requires a separately authorized build/install/startup workflow. The existing MCP `memory_save` and REST remember endpoint expose the additive action, target, expected version, source kind, and source observation fields. No new tool or endpoint is introduced. Native preparation, conflict, or unsupported-publication outcomes return a retryable result with the submitted input retained in the response; callers must retry identical input to resolve a lost acknowledgement before changing it. Metadata successors carry an internal `embeddingReuse` marker with the predecessor ID and native row version. The native `evolve` contract captures the ORIGINAL embedding and rebinds it to the new source version within the publication transaction, retaining model, dimensions, generation, and unchanged embedding input. An unavailable original or incompatible binding rejects the complete evolve operation before publication. The marker is never accepted from HTTP or MCP clients. Native dirty-source recovery must reject unsupported reuse rather than spend on another embedding. Delivery depends on the native owner’s matching patch and recovery changes; no installed behavior is claimed.
