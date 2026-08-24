import type { CompressedObservation, ObservationType } from "../types.js";

const TYPE_PRIORITY: Record<ObservationType, number> = {
  decision: 5,
  error: 5,
  file_write: 4,
  file_edit: 4,
  task: 3,
  command_run: 2,
  conversation: 2,
  discovery: 2,
  file_read: 1,
  search: 1,
  web_fetch: 1,
  subagent: 1,
  notification: 0,
  image: 0,
  other: 0,
};

const PROTECTED_TYPES = new Set<ObservationType>([
  "decision",
  "error",
  "file_write",
  "file_edit",
]);

export interface GraphPromptObservation {
  title: string;
  narrative: string;
  concepts: string[];
  files: string[];
  type: string;
}

export interface GraphExtractionUnit {
  sourceObservations: CompressedObservation[];
  promptObservations: GraphPromptObservation[];
}

export function toGraphPromptObservation(
  observation: CompressedObservation,
): GraphPromptObservation {
  return {
    title: observation.title,
    narrative: observation.narrative,
    concepts: observation.concepts,
    files: observation.files,
    type: observation.type,
  };
}

export function estimateGraphObservationChars(
  observation: GraphPromptObservation | CompressedObservation,
): number {
  return observation.title.length + observation.narrative.length +
    observation.concepts.join(", ").length + observation.files.join(", ").length + 96;
}

function priority(observation: CompressedObservation): number {
  return observation.importance * 10 + TYPE_PRIORITY[observation.type];
}

export function isProtectedGraphObservation(
  observation: CompressedObservation,
): boolean {
  return PROTECTED_TYPES.has(observation.type);
}

/**
 * Packs complete observations into prompt-sized groups. It deliberately
 * never slices narrative text and never drops an observation. A single item
 * larger than the target remains an oversized unit for the caller to compact
 * locally or send as-is when the provider accepts it.
 */
export function partitionGraphExtractionObservations(
  observations: CompressedObservation[],
  targetChars: number,
): CompressedObservation[][] {
  const target = Math.max(1, targetChars);
  const partitions: CompressedObservation[][] = [];
  let current: CompressedObservation[] = [];
  let currentChars = 0;

  for (const observation of observations) {
    const size = estimateGraphObservationChars(observation);
    if (current.length > 0 && currentChars + size > target) {
      partitions.push(current);
      current = [];
      currentChars = 0;
    }
    current.push(observation);
    currentChars += size;
    if (currentChars >= target) {
      partitions.push(current);
      current = [];
      currentChars = 0;
    }
  }
  if (current.length > 0) partitions.push(current);
  return partitions;
}

/**
 * Keeps the historical selector API but changes its contract: it performs
 * no lossy budget selection. Callers that need a bounded prompt must use
 * partitionGraphExtractionObservations, which preserves every source row.
 */
export function selectGraphExtractionObservations(
  observations: CompressedObservation[],
  _targetChars: number,
): CompressedObservation[] {
  return [...observations].sort((left, right) => priority(right) - priority(left));
}

/**
 * Creates complete source-backed units for local compaction. Protected rows
 * remain verbatim; routine rows can be represented by one local digest while
 * retaining the complete source list for graph provenance.
 */
export function splitGraphCompactionUnits(
  observations: CompressedObservation[],
  targetChars: number,
): CompressedObservation[][] {
  const protectedRows = observations.filter(isProtectedGraphObservation);
  const routineRows = observations.filter((observation) => !isProtectedGraphObservation(observation));
  const units = protectedRows.map((observation) => [observation]);
  units.push(...partitionGraphExtractionObservations(routineRows, targetChars));
  return units;
}
