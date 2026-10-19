import {
  buildSummaryItemsPrompt as buildBaseSummaryItemsPrompt,
  buildReduceItemsPrompt as buildBaseReduceItemsPrompt,
  SUMMARY_SOURCE_EVIDENCE_RULES,
  type SummaryPromptItem,
} from "../prompts/summary.js";

export const SUMMARY_FIDELITY_VERSION = "summary-fidelity-v2";
export const SUMMARY_RECOVERY_FIDELITY_VERSION = "summary-full-originals-v1";
export const SUMMARY_GENERATION_REVISION = "summary-sparse-evidence-v1";

const SUMMARY_XML = `<summary><title>Short title, at most 100 characters</title><narrative>As short as the evidence supports; no minimum sentence count</narrative><decisions><decision>Supported decision and rationale, only if observed</decision></decisions><files><file>Supported current-session edit</file></files><concepts><concept>Searchable concept</concept></concepts></summary>`;

const SPARSE_SOURCE_RULES = `Instructions, prompts, and app-generated requests describe requested actions, not evidence of completed outcomes or decisions. Describe them as requests unless the sources record execution or an actual decision. If no actual decision is evidenced, return an empty decisions element. Missing telemetry or empty path lists do not prove that no files changed, no other work occurred, or that a session was brief; omit unsupported negative claims, timing, and duration. Do not add sentences merely to fill the narrative.`;

export const SUMMARY_SYSTEM = `Summarize AI coding-agent session observations. Return only XML in this shape: ${SUMMARY_XML}
Focus on evidenced outcomes and decisions when present. List only literal paths semantically supported as changed in the session being summarized; never infer paths. Keep factual, validation, provider, and benefit claims source-supported. ${SUMMARY_SOURCE_EVIDENCE_RULES} ${SPARSE_SOURCE_RULES} Sparse evidence permits cautious wording and empty lists.`;

export const REDUCE_SYSTEM = `Merge chronological partials from the supplied coding-session sources into one concise summary. Return only XML in this shape: ${SUMMARY_XML}
Preserve the source arc, every distinct supported decision and searchable concept. Keep only literal paths supported as changed in the session being summarized; never infer paths. Preserve uncertainty; keep factual, causal, validation, provider, and benefit claims source-supported. ${SUMMARY_SOURCE_EVIDENCE_RULES} ${SPARSE_SOURCE_RULES} Sparse or conflicting evidence permits cautious wording and empty lists.`;

export function buildSummaryItemsPrompt(items: SummaryPromptItem[]): string {
  const context = items.map(item => ({ ...item, modifiedPaths: [] }));
  return buildBaseSummaryItemsPrompt(context);
}

export function buildReduceItemsPrompt(items: SummaryPromptItem[]): string {
  const context = items.map(item => ({ ...item, modifiedPaths: [] }));
  return buildBaseReduceItemsPrompt(context).replace(
    /^Partial summaries \(\d+ chunks of one session, chronological\):/,
    "Partial summaries from supplied sources (chronological):",
  );
}
