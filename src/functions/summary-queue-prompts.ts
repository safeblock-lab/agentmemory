import {
  buildSummaryItemsPrompt as buildBaseSummaryItemsPrompt,
  buildReduceItemsPrompt as buildBaseReduceItemsPrompt,
  SUMMARY_SOURCE_EVIDENCE_RULES,
  type SummaryPromptItem,
} from "../prompts/summary.js";

export const SUMMARY_FIDELITY_VERSION = "summary-fidelity-v2";
export const SUMMARY_RECOVERY_FIDELITY_VERSION = "summary-full-originals-v1";

const SUMMARY_XML = `<summary><title>Short title, at most 100 characters</title><narrative>Three to five sentences</narrative><decisions><decision>Supported decision and rationale</decision></decisions><files><file>Supported current-session edit</file></files><concepts><concept>Searchable concept</concept></concepts></summary>`;

export const SUMMARY_SYSTEM = `Summarize AI coding-agent session observations. Return only XML in this shape: ${SUMMARY_XML}
Focus on outcomes and decisions, not tool calls. List only literal paths semantically supported as changed in the session being summarized; never infer paths. Keep factual, validation, provider, and benefit claims source-supported. ${SUMMARY_SOURCE_EVIDENCE_RULES} Sparse evidence permits cautious wording and empty lists.`;

export const REDUCE_SYSTEM = `Merge chronological partials from the supplied coding-session sources into one concise summary. Return only XML in this shape: ${SUMMARY_XML}
Preserve the source arc, every distinct supported decision and searchable concept. Keep only literal paths supported as changed in the session being summarized; never infer paths. Preserve uncertainty; keep factual, causal, validation, provider, and benefit claims source-supported. ${SUMMARY_SOURCE_EVIDENCE_RULES} Sparse or conflicting evidence permits cautious wording and empty lists.`;

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
