export const SUMMARY_SOURCE_EVIDENCE_RULES = `Embedded originals keep their source provenance and event time; the enclosing session ID and capture time describe collection, not authorship. Do not recast historical work as current. Merge same-session chunks and preserve distinct origins among full originals. Qualify missing or conflicting provenance; invent no dates or identities. Paths and file lists may be historical; include only files supported as changed in the session being summarized, or none. An IDE-tab path does not prove a file changed; neither do open, read, attached, or mentioned paths. Treat configuration, script propagation, provider selection, and outcomes as distinct; claim causal links only when sources connect the specific setting to behavior. Evaluate evidence semantically; word overlap is not support.`

export const SUMMARY_SYSTEM = `You are a session summarizer for an AI coding agent's memory system. Given all compressed observations from a coding session, produce a concise session summary.

Output EXACTLY this XML format with no additional text:

<summary>
  <title>Short session title (max 100 chars)</title>
  <narrative>3-5 sentence narrative of what was accomplished</narrative>
  <decisions>
    <decision>Key technical decision made</decision>
  </decisions>
  <files>
    <file>path/to/modified/file</file>
  </files>
  <concepts>
    <concept>key concept from session</concept>
  </concepts>
</summary>

Rules:
- Focus on outcomes, not individual tool calls
- Highlight decisions and their rationale
- List only files whose path is explicitly present in the source observations or cited source paths
- Do not infer directory prefixes or invent paths from concepts or feature names
${SUMMARY_SOURCE_EVIDENCE_RULES}
- Concepts should be searchable terms for future context retrieval`

export function buildSummaryPrompt(observations: Array<{
  type: string
  title: string
  facts: string[]
  narrative: string
  files: string[]
  concepts: string[]
}>): string {
  const lines = observations.map((obs, i) => {
    const facts = obs.facts.map((f) => `  - ${f}`).join('\n')
    return `[${i + 1}] ${obs.type}: ${obs.title}\n${obs.narrative}\nFacts:\n${facts}\nFiles: ${obs.files.join(', ')}`
  })
  return `Session observations (${observations.length} total):\n\n${lines.join('\n\n---\n\n')}`
}

export interface SummaryPromptItem {
  text: string
  obsRangeStart: number
  obsRangeEnd: number
  fragment?: boolean
  sourcePaths?: string[]
  modifiedPaths?: string[]
}

export function formatSummaryObservation(observation: {
  type: string; title: string; narrative: string; facts: string[]; files: string[]; concepts: string[]
}, index: number): string {
  return `[${index}] ${observation.type}: ${observation.title}\n${observation.narrative}\nFacts:\n${observation.facts.map(f => `  - ${f}`).join('\n')}\nFiles: ${observation.files.join(', ')}\nConcepts: ${observation.concepts.join(', ')}`
}

export function buildSummaryItemsPrompt(items: SummaryPromptItem[]): string {
  const count = new Set(items.flatMap(item => [item.obsRangeStart])).size
  const sections = items.map(item => item.fragment
    ? `[Observation ${item.obsRangeStart} fragment, in source order]\n${item.text}`
    : item.text)
  const modifiedPaths = [...new Set(items.flatMap(item => item.modifiedPaths ?? []))]
  const pathContext = modifiedPaths.length
    ? `\n\nPath candidates for grounding only (not evidence of current edits):\n${modifiedPaths.join('\n')}`
    : ""
  return `Session observations (${count} total):\n\n${sections.join('\n\n---\n\n')}${pathContext}`
}

export function buildReduceItemsPrompt(items: SummaryPromptItem[]): string {
  const sections = items.map((item, index) => `[Chunk ${index + 1} of ${items.length} — obs ${item.obsRangeStart}-${item.obsRangeEnd}${item.fragment ? ', fragment in source order' : ''}]\n${item.text}`)
  const modifiedPaths = [...new Set(items.flatMap(item => item.modifiedPaths ?? []))]
  const pathContext = modifiedPaths.length
    ? `\n\nPath candidates for grounding only (not evidence of current edits):\n${modifiedPaths.join('\n')}`
    : ""
  return `Partial summaries (${items.length} chunks of one session, chronological):\n\n${sections.join('\n\n---\n\n')}${pathContext}`
}

export function formatSummaryPartial(partial: {
  title: string; narrative: string; keyDecisions: string[]; filesModified: string[]; concepts: string[]
}): string {
  return `Title: ${partial.title}\nNarrative: ${partial.narrative}\nDecisions:\n${partial.keyDecisions.map(d => `  - ${d}`).join('\n')}\nFiles:\n${partial.filesModified.map(f => `  - ${f}`).join('\n')}\nConcepts: ${partial.concepts.join(', ')}`
}

export const REDUCE_SYSTEM = `You are merging multiple partial summaries of the SAME coding session into one final session summary. The partials are chronological chunks of one continuous session — not separate sessions.

Output EXACTLY this XML format with no additional text:

<summary>
  <title>Short session title (max 100 chars)</title>
  <narrative>3-5 sentence narrative covering the whole session</narrative>
  <decisions>
    <decision>Key technical decision made</decision>
  </decisions>
  <files>
    <file>path/to/modified/file</file>
  </files>
  <concepts>
    <concept>key concept from session</concept>
  </concepts>
</summary>

Rules:
- Synthesize a single narrative that reflects the whole arc, not a chunk-by-chunk recap
- Preserve every distinct decision across chunks
- Union (deduplicate) concepts and files supported as changed in the session being summarized
- Do not infer directory prefixes or invent paths from concepts or feature names
${SUMMARY_SOURCE_EVIDENCE_RULES}
- Title should capture the session's overall outcome`

export function buildReducePrompt(partials: Array<{
  title: string
  narrative: string
  keyDecisions: string[]
  filesModified: string[]
  concepts: string[]
  obsRangeStart: number
  obsRangeEnd: number
}>): string {
  const sections = partials.map((p, i) => {
    const decisions = p.keyDecisions.map((d) => `  - ${d}`).join('\n')
    const files = p.filesModified.map((f) => `  - ${f}`).join('\n')
    const concepts = p.concepts.join(', ')
    return `[Chunk ${i + 1} of ${partials.length} — obs ${p.obsRangeStart}-${p.obsRangeEnd}]
Title: ${p.title}
Narrative: ${p.narrative}
Decisions:
${decisions}
Files:
${files}
Concepts: ${concepts}`
  })
  return `Partial summaries (${partials.length} chunks of one session, chronological):\n\n${sections.join('\n\n---\n\n')}`
}
