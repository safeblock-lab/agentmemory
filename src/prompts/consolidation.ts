export const SEMANTIC_MERGE_SYSTEM = `You are a memory consolidation engine. Given overlapping episodic memories (session summaries), extract stable factual knowledge.

Output format (XML):
<facts>
  <fact confidence="0.0-1.0" sourceIds="session-id,...">Concise factual statement</fact>
</facts>

Rules:
- Extract only facts that appear in 2+ episodes or are highly confident
- sourceIds must list only episode session IDs that directly support the fact
- Never invent or include an episode ID that does not support the fact
- Confidence reflects how well-supported the fact is across episodes
- Combine overlapping information into single concise facts
- Skip ephemeral details (specific error messages, temporary states)`;

export const SEMANTIC_EVIDENCE_SYSTEM = `You are a factual evidence extractor. Given episodic memories, extract concise candidate facts and identify exactly which episodes support each candidate.

Output format (XML):
<facts>
  <fact confidence="0.0-1.0" sourceIds="session-id,...">Concise factual statement</fact>
</facts>

Rules:
- Include stable factual candidates supported by one or more episodes; a later pass checks cross-episode support
- sourceIds must list only episode session IDs that directly support the candidate
- Never invent or include an episode ID that does not support the candidate
- Preserve distinct facts; combine only equivalent claims
- Skip ephemeral details (specific error messages, temporary states)`;

export const SEMANTIC_CANDIDATE_REDUCTION_SYSTEM = `You merge candidate facts from bounded evidence groups. Preserve every distinct claim and merge only equivalent claims. When claims merge, union only the source IDs that directly support the merged claim. Do not apply the multi-episode support threshold yet.

Output format (XML):
<facts>
  <fact confidence="0.0-1.0" candidateIds="candidate-id,...">Concise factual statement</fact>
</facts>

Each input candidate has one or more candidateIds. Every output candidate must list all input candidateIds represented by it. Include every input candidate exactly once across the output. Never invent candidate IDs or omit distinct claims.`;

export const SEMANTIC_CANDIDATE_FINAL_SYSTEM = `You are a memory consolidation engine. Merge equivalent candidate facts, then retain only stable facts supported by at least two distinct episode session IDs or by exceptionally strong evidence from one episode.

Output format (XML):
<facts>
  <fact confidence="0.0-1.0" candidateIds="candidate-id,...">Concise factual statement</fact>
</facts>

Rules:
- candidateIds must include every input candidate represented by the fact
- Never invent candidate IDs or attribute unrelated candidates to the fact
- Combine overlapping information into one concise fact
- Skip ephemeral details (specific error messages, temporary states)`;

function formatSemanticEpisodes(
  episodes: Array<{ sessionId?: string; title: string; narrative: string; concepts: string[] }>,
): string {
  return episodes
    .map((episode, index) =>
      `[Episode ${index + 1}]\n${episode.sessionId ? `Session ID: ${episode.sessionId}\n` : ""}Title: ${episode.title}\nNarrative: ${episode.narrative}\nConcepts: ${episode.concepts.join(", ")}`,
    )
    .join("\n\n");
}

export function buildSemanticMergePrompt(
  episodes: Array<{ sessionId?: string; title: string; narrative: string; concepts: string[] }>,
): string {
  const items = formatSemanticEpisodes(episodes);
  return `Consolidate these episodic memories into stable facts:\n\n${items}`;
}

export function buildSemanticEvidencePrompt(
  episodes: Array<{ sessionId: string; title: string; narrative: string; concepts: string[] }>,
): string {
  return `Extract factual candidates from these episodes and identify their supporting session IDs:\n\n${formatSemanticEpisodes(episodes)}`;
}

export function buildSemanticCandidatePrompt(
  candidates: Array<{ fact: string; confidence: number; sourceSessionIds: string[] }>,
): string {
  return `Candidate facts with source evidence:\n\n${JSON.stringify(candidates)}`;
}

export const PROCEDURAL_EXTRACTION_SYSTEM = `You are a procedural memory extractor. Given repeated patterns and workflows observed across sessions, extract reusable procedures.

Output format (XML):
<procedures>
  <procedure name="short descriptive name" trigger="when to use this procedure">
    <step>Step 1 description</step>
    <step>Step 2 description</step>
  </procedure>
</procedures>

Rules:
- Only extract procedures observed 2+ times
- Steps should be concrete and actionable
- Trigger condition should be specific enough to match automatically`;

export function buildProceduralExtractionPrompt(
  patterns: Array<{ content: string; frequency: number }>,
): string {
  const items = patterns
    .map((p, i) => `[Pattern ${i + 1}] (seen ${p.frequency}x)\n${p.content}`)
    .join("\n\n");
  return `Extract reusable procedures from these recurring patterns:\n\n${items}`;
}
