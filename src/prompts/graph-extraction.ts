export const GRAPH_EXTRACTION_SYSTEM = `You are a knowledge graph extraction engine. Given a compressed observation from a coding session, extract entities and relationships.

Output format (XML):
<entities>
  <entity type="file|function|concept|error|decision|pattern|library|person" name="exact name">
    <property key="key">value</property>
  </entity>
</entities>
<relationships>
  <relationship type="uses|imports|modifies|causes|fixes|depends_on|related_to" source="entity name" target="entity name" weight="0.1-1.0"/>
</relationships>

Rules:
- Extract concrete entities only (real file paths, function names, library names)
- Use the most specific type available
- Weight relationships by how strong/direct the connection is
- If no entities found, output empty tags`;

export const GRAPH_EXTRACTION_SYSTEM_WITH_PROVENANCE = `You are a knowledge graph extraction engine. Given numbered coding-session observations, extract entities and relationships.

Output format (XML):
<entities>
  <entity type="file|function|concept|error|decision|pattern|library|person" name="exact name" observations="1">
    <property key="key">value</property>
  </entity>
</entities>
<relationships>
  <relationship type="uses|imports|modifies|causes|fixes|depends_on|related_to" source="entity name" target="entity name" weight="0.1-1.0" observations="1"/>
</relationships>

Rules:
- Extract concrete entities only (real file paths, function names, library names), using the most specific type.
- Each input observation has a 1-based number in square brackets.
- Every entity and relationship MUST list exactly its directly supporting observation numbers in observations="1,2".
- Use one or more unique, in-range numbers. Never cite an observation merely because it shares the batch.
- Do not infer a relationship from two unrelated observations; cite the observation that states the relationship.
- Weight relationships by how strong and direct the connection is.
- If there is no supporting observation, omit that entity or relationship. If none remain, output empty tags.`;

export function buildGraphExtractionPrompt(
  observations: Array<{
    title: string;
    narrative: string;
    concepts: string[];
    files: string[];
    type: string;
  }>,
): string {
  const items = observations
    .map(
      (o, i) =>
        `[${i + 1}] Type: ${o.type}\nTitle: ${o.title}\nNarrative: ${o.narrative}\nConcepts: ${(o.concepts ?? []).join(", ")}\nFiles: ${(o.files ?? []).join(", ")}`,
    )
    .join("\n\n");
  // Some local models default to a hidden reasoning pass that consumes
  // most of the token budget before any output. The suffix is their
  // documented soft switch to skip it; other models ignore the token.
  const noThink = process.env.AGENTMEMORY_LLM_NOTHINK === "1" ? "\n/no_think" : "";
  return `Extract entities and relationships from these observations:\n\n${items}${noThink}`;
}
