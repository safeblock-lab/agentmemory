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
- List all files that were created or modified
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
  return `Session observations (${count} total):\n\n${sections.join('\n\n---\n\n')}`
}

export function buildReduceItemsPrompt(items: SummaryPromptItem[]): string {
  const sections = items.map((item, index) => `[Chunk ${index + 1} of ${items.length} — obs ${item.obsRangeStart}-${item.obsRangeEnd}${item.fragment ? ', fragment in source order' : ''}]\n${item.text}`)
  return `Partial summaries (${items.length} chunks of one session, chronological):\n\n${sections.join('\n\n---\n\n')}`
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
- Union (deduplicate) all files and concepts
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
