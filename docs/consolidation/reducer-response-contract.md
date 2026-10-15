# Consolidation reducer response contract

Reducer output must contain one `<facts>` root. A plain-text preface of at most 256 UTF-8 bytes is accepted; XML or trailing text outside the root, extra or nested roots, malformed facts, and unknown candidate IDs are rejected. Candidate IDs remain covered exactly once, and source provenance is reconstructed from the represented input candidates.

Confidence may be one finite scalar in `[0, 1]` or an ordered interval whose endpoints are finite and within `[0, 1]`. Intervals normalize to their lower endpoint (for example, `0.85-1.0` becomes `0.85`); reversed, out-of-range, non-finite, and malformed values fail closed. New reducer prompts show one scalar and request one scalar.

Consolidation request IDs include the exact manifest, system prompt, and user prompt. The previous reducer prompt is retained as an exact compatibility lookup so captured responses and pending queue work can replay under their original IDs and bytes. Captured pages, request records, and history are not rewritten.

After one semantic reduction pass, a single immutable output page means all input candidates were handled in the same bounded group; the pipeline proceeds to source verification instead of buying a redundant fixed-point pass. Passes that emit multiple pages keep reducing when candidate count shrinks. For a checkpoint created by the earlier behavior that already opened a next round after one output page, the reducer point-reads the exact current and compatible request IDs before skipping that untouched round. Captured requests are replayed, queued work remains pending, and uncertain requests remain blocked. Source verification and canonical publication continue unchanged.
