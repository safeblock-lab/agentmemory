import type { IIIClient } from "iii-sdk";
import { IndexedRetrievalError } from "../state/indexed-retrieval.js";
import type {
  GraphNode,
  GraphEdge,
  GraphQueryResult,
  GraphSnapshot,
  CompressedObservation,
  MemoryProvider,
  BatchEffectMetadata,
  RecentOperation,
} from "../types.js";
import { KV, generateId, fingerprintId } from "../state/schema.js";
import { batchEffectKey, createGraphBatchCallbackPreflight, effectMetadata, runBatchCallback } from "../state/batch-effects.js";
import type { StateKV } from "../state/kv.js";
import {
  GRAPH_EXTRACTION_SYSTEM,
  GRAPH_EXTRACTION_SYSTEM_WITH_PROVENANCE,
  buildGraphExtractionPrompt,
} from "../prompts/graph-extraction.js";
import { isGraphExtractionEnabled } from "../config.js";
import { recordAudit } from "./audit.js";
import { recordOperation } from "./dashboard-activity.js";
import { logger } from "../logger.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { withStrictGraphResponse } from "../providers/openai.js";
import type { TypeSafeDecisionProvider, TypeSafeQuestion } from "../providers/typesafe.js";
import {
  getGraphExtractionInputTargetChars,
  getTypeSafeConfig,
  isTypeSafeFeatureEnabled,
  TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD,
} from "../config.js";
import { stripPrivateData } from "./privacy.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import { selectCompactionCandidates } from "./typesafe-compaction.js";
import { graphStrictProviderResponse, freezeGraphInputUnits, freezeGraphHandledValue, freezeGraphSessionObservations, freezeGraphValue, graphCapturedAt, graphJobId, graphJobNextDeltaOrdinal, graphKV, hasGraphDelta, isGraphJobYield, readGraphJobContinuation, registerGraphJobHandler, runGraphJob, withGraphDelta, writeGraphJobContinuation, yieldGraphJob, graphTransactionFailure, graphUnknownCompactionRepeatProvider, graphUnknownCompactionRepeatFetchPermit, type GraphJobYield } from "./graph-jobs.js";
import { prepareGraphBrowse, readScopedGraphPage } from "./graph-retrieval-read.js";
import { GRAPH_STATISTICS_SAMPLE_LIMIT, readProjectGraphStatistics } from "./graph-statistics.js";
import { registerGraphDispatch, selectedGraphGeneration, type GraphDispatchRequest } from "./graph-dispatch.js";
import {
  estimateGraphObservationChars,
  isProtectedGraphObservation,
  splitGraphCompactionUnits,
  toGraphPromptObservation,
  type GraphExtractionUnit,
} from "./graph-input.js";
import { createNextGraphWorkUnit, GRAPH_WORK_LIMITS, type GraphWorkCursor } from "./graph-work-groups.js";

const PROTECTED_GRAPH_SIGNAL = /\b(?:error|failed|failure|decision|instruction|prompt|security|secret|token|password|credential|api[-_ ]?key|auth|permission|mutation|write|edit|patch|delete|move|rename|commit|push|reset|deploy|install|shell|terminal|bash|powershell|environment|env|(?:AGENTS|CLAUDE|GEMINI|COPILOT)\.md)\b/i;

function* splitGraphWorkContent(value: string, maxBytes: number): Generator<string> {
  let part = "", bytes = 0;
  for (const character of value) {
    const size = Buffer.byteLength(character, "utf8");
    if (part && bytes + size > maxBytes) { yield part; part = ""; bytes = 0; }
    part += character;
    bytes += size;
  }
  if (part) yield part;
}

function graphWorkObservationContent(observation: GraphExtractionUnit["promptObservations"][number]): string {
  return `Type: ${observation.type}\nTitle: ${observation.title}\nNarrative: ${observation.narrative}\nConcepts: ${observation.concepts.join(", ")}\nFiles: ${observation.files.join(", ")}`;
}

interface GraphExtractionOptions {
  graphEnabled: boolean; llmEnabled: boolean; auxiliary: boolean; targetChars: number;
  typeSafe: boolean; batchQueue: boolean; providerName: string; localCompactor: boolean;
  typeSafeCompaction: boolean; typeSafeMaxStateChars: number;
}
export function graphExtractionOptionsCompatible(saved: GraphExtractionOptions, current: GraphExtractionOptions): boolean {
  return saved.providerName === current.providerName
    && saved.auxiliary === current.auxiliary
    && ["graphEnabled", "llmEnabled", "typeSafe", "batchQueue", "localCompactor", "typeSafeCompaction"].every(key =>
      typeof saved[key as keyof GraphExtractionOptions] === "boolean"
      && (!saved[key as keyof GraphExtractionOptions] || current[key as keyof GraphExtractionOptions] === true))
    && Number.isSafeInteger(saved.targetChars) && saved.targetChars > 0
    && Number.isSafeInteger(saved.typeSafeMaxStateChars) && saved.typeSafeMaxStateChars > 0;
}
interface ProjectGraphWorkPage {
  version: 2; project: string; sources: Array<{ id: string; version: string; content: string }>;
}
interface ProjectGraphWorkProgress {
  version: 2; pageCount: number; pageIndex: number; cursor: GraphWorkCursor | null;
  nodesAdded: number; edgesAdded: number; workUnits: number;
  extractionSystem: string; envelopeSuffix: string; options: GraphExtractionOptions;
  activeUnit?: { id: string; pageIndex: number; resultOrdinal: number };
}
const MAX_PROJECT_GRAPH_PLAN_BYTES = 64 * 1024 * 1024;

function hasProtectedGraphSignal(text: string): boolean {
  return PROTECTED_GRAPH_SIGNAL.test(text) || stripPrivateData(text) !== text;
}

async function compactGraphObservations(
  observations: CompressedObservation[],
  typeSafe?: TypeSafeDecisionProvider,
  options?: Pick<GraphExtractionOptions, "typeSafeCompaction" | "typeSafeMaxStateChars">,
): Promise<CompressedObservation[]> {
  const repeat = graphUnknownCompactionRepeatProvider();
  if (!typeSafe || !(options?.typeSafeCompaction ?? isTypeSafeFeatureEnabled("compaction"))) return observations;
  const observationsByRecency = [...observations].sort((left, right) => {
    const leftTimestamp = Date.parse(left.timestamp);
    const rightTimestamp = Date.parse(right.timestamp);
    if (!Number.isFinite(leftTimestamp)) return Number.isFinite(rightTimestamp) ? -1 : 0;
    if (!Number.isFinite(rightTimestamp)) return 1;
    return rightTimestamp - leftTimestamp;
  });
  const recentIds = new Set(observationsByRecency.slice(0, 6).map((observation) => observation.id));
  const candidates = observations.map((observation) => {
    const text = `${observation.title} ${observation.narrative} ${observation.facts.join(" ")} ${observation.concepts.join(" ")}`;
    const protectedItem =
      isProtectedGraphObservation(observation) ||
      observation.type === "command_run" ||
      recentIds.has(observation.id) ||
      observation.importance >= 8 ||
      observation.modality === "image" ||
      observation.modality === "mixed" ||
      hasProtectedGraphSignal(text);
    const deterministicDrop =
      observation.type === "notification" &&
      observation.importance <= 2 &&
      !observation.narrative.trim() &&
      observation.facts.length === 0 &&
      observation.concepts.length === 0 &&
      observation.files.length === 0;
    const state = stripPrivateData(JSON.stringify({
      type: observation.type,
      importance: observation.importance,
      title: observation.title.slice(0, 64),
      concepts: observation.concepts.slice(0, 3).map((concept) => concept.slice(0, 24)),
      factCount: observation.facts.length,
      preview: observation.narrative.slice(0, 96),
      filesCount: observation.files.length,
    })).slice(0, 512);
    if (protectedItem) {
      return { id: observation.id, value: observation, disposition: "protected" as const };
    }
    if (deterministicDrop) {
      return { id: observation.id, value: observation, disposition: "deterministic-drop" as const };
    }
    return { id: observation.id, value: observation, disposition: "ambiguous" as const, state };
  });

  let repeatDecisionFailed = false;
  try {
    const selected = await selectCompactionCandidates(
      candidates,
      typeSafe
        ? async (batch) => {
            const questions: Record<string, TypeSafeQuestion> = {};
            for (const item of batch) {
              questions[item.questionId] = {
                type: "choice",
                instructions: `Should graph-extraction candidate ${item.questionId} remain in the bounded input?`,
                criteria: {
                  keep: "Keep if this observation may contribute a useful entity, relationship, decision, or discovery.",
                  drop: "Drop only if it is clearly routine, reproducible, and unlikely to affect durable graph knowledge.",
                },
              };
            }
            let answers: Awaited<ReturnType<TypeSafeDecisionProvider["evaluate"]>>;
            try {
              const permission = repeat ? graphUnknownCompactionRepeatFetchPermit() : undefined;
              if (repeat && !permission) throw new Error("Unknown compaction repetition has no admitted fetch permission");
              answers = await typeSafe.evaluate("compaction", { candidates: batch }, questions, permission);
              if (repeat && !answers) throw new Error("Unknown compaction repetition has no valid response");
            } catch (error) {
              repeatDecisionFailed = true;
              throw error;
            }
            if (!answers) return undefined;
            const decisions: Record<string, { choice: "keep" | "drop"; confidence: number }> = {};
            for (const item of batch) {
              const answer = answers?.[item.questionId];
              if (answer?.type === "choice" && (answer.choice === "keep" || answer.choice === "drop")) {
                decisions[item.questionId] = { choice: answer.choice, confidence: answer.confidence };
              }
            }
            return decisions;
          }
        : undefined,
      { maxStateChars: options?.typeSafeMaxStateChars ?? getTypeSafeConfig().maxStateChars },
    );
    if (repeat && repeatDecisionFailed) throw new Error("Unknown compaction repetition outcome unavailable");
    const decisionFailures = selected.fallbackReasons.filter((reason) =>
      reason === "decision-error" || reason === "decision-unavailable" || reason === "invalid-answer",
    );
    if (repeat && decisionFailures.length > 0) throw new Error("Unknown compaction repetition response was not valid");
    if (decisionFailures.length > 0) {
      logger.warn("TypeSafe graph compaction failed open", { reasons: decisionFailures });
    }
    return selected.kept.map((candidate) => candidate.value);
  } catch (error) {
    if (repeat) throw error;
    logger.warn("TypeSafe graph compaction failed open", {
      errorType: error instanceof Error ? error.name : "unknown",
    });
    return observations;
  }
}
const GRAPH_INPUT_COMPACTION_SYSTEM = `You compact coding-session observations before knowledge-graph extraction.
Return one concise factual digest. Preserve every decision, error, file write/edit, named entity, file path, command result, and causal relationship present in the input. Do not invent facts, merge unrelated names, or use vague summaries. Keep exact identifiers and paths. The digest is an intermediate representation, not XML.`;

type LocalGraphCompactor = Pick<MemoryProvider, "summarize">;

function buildGraphInputCompactionPrompt(
  observations: CompressedObservation[],
): string {
  return observations.map((observation, index) => [
    `[${index + 1}] Source ID: ${observation.id}`,
    `Type: ${observation.type}`,
    `Title: ${observation.title}`,
    `Narrative: ${observation.narrative}`,
    `Facts: ${observation.facts.join("; ")}`,
    `Concepts: ${observation.concepts.join(", ")}`,
    `Files: ${observation.files.join(", ")}`,
  ].join("\n")).join("\n\n");
}

function unitPromptChars(unit: GraphExtractionUnit): number {
  return unit.promptObservations.reduce(
    (total, observation) => total + estimateGraphObservationChars(observation),
    0,
  );
}

function packGraphInputUnits(
  units: GraphExtractionUnit[],
  targetChars: number,
): GraphExtractionUnit[][] {
  const target = Math.max(1, targetChars);
  const partitions: GraphExtractionUnit[][] = [];
  let current: GraphExtractionUnit[] = [];
  let currentChars = 0;
  for (const unit of units) {
    const size = unitPromptChars(unit);
    if (current.length > 0 && currentChars + size > target) {
      partitions.push(current);
      current = [];
      currentChars = 0;
    }
    current.push(unit);
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

function mergeGraphInputUnits(units: GraphExtractionUnit[]): GraphExtractionUnit {
  return {
    sourceObservations: units.flatMap((unit) => unit.sourceObservations),
    promptObservations: units.flatMap((unit) => unit.promptObservations),
  };
}

async function compactGraphInputUnit(
  sourceObservations: CompressedObservation[],
  targetChars: number,
  localCompactor: LocalGraphCompactor | undefined,
): Promise<GraphExtractionUnit> {
  const completeUnit: GraphExtractionUnit = {
    sourceObservations,
    promptObservations: sourceObservations.map(toGraphPromptObservation),
  };
  const unitChars = sourceObservations.reduce(
    (total, observation) => total + estimateGraphObservationChars(observation),
    0,
  );
  const hasOversizedProtectedObservation = sourceObservations.some(
    isProtectedGraphObservation,
  ) && unitChars > targetChars;
  if (!localCompactor || (!hasOversizedProtectedObservation && sourceObservations.some(isProtectedGraphObservation))) {
    return completeUnit;
  }
  try {
    const digest = await localCompactor.summarize(
      GRAPH_INPUT_COMPACTION_SYSTEM,
      buildGraphInputCompactionPrompt(sourceObservations),
      { task: "graph_extraction", thinking: false },
    );
    if (!digest.trim()) return completeUnit;
    const concepts = [...new Set(sourceObservations.flatMap((item) => item.concepts))];
    const files = [...new Set(sourceObservations.flatMap((item) => item.files))];
    return {
      sourceObservations,
      promptObservations: [{
        title: `Local digest of ${sourceObservations.length} routine observations`,
        narrative: digest.trim(),
        concepts,
        files,
        type: sourceObservations.length === 1
          ? sourceObservations[0].type
          : "discovery",
      }],
    };
  } catch (error) {
    logger.warn("Local graph input compaction failed; preserving complete observations", {
      error: error instanceof Error ? error.message : String(error),
      observations: sourceObservations.length,
    });
    return completeUnit;
  }
}

async function prepareGraphExtractionInputs(
  sourceObservations: CompressedObservation[],
  targetChars: number,
  localCompactor?: LocalGraphCompactor,
  preserveSourceIndices = false,
): Promise<GraphExtractionUnit[]> {
  const complete = {
    sourceObservations,
    promptObservations: sourceObservations.map(toGraphPromptObservation),
  } satisfies GraphExtractionUnit;
  const totalChars = sourceObservations.reduce(
    (total, observation) => total + estimateGraphObservationChars(observation),
    0,
  );
  if (totalChars <= targetChars) return [complete];

  const fullUnits = splitGraphCompactionUnits(sourceObservations, targetChars);
  const compactedUnits: GraphExtractionUnit[] = [];
  for (const unit of fullUnits) {
    if (preserveSourceIndices) {
      for (const observation of unit) {
        compactedUnits.push(await compactGraphInputUnit([observation], targetChars, localCompactor));
      }
    } else {
      compactedUnits.push(await compactGraphInputUnit(unit, targetChars, localCompactor));
    }
  }
  return packGraphInputUnits(compactedUnits, targetChars).map(mergeGraphInputUnits);
}

async function captureProjectGraphWorkPages(
  kv: StateKV,
  observations: CompressedObservation[],
  targetChars: number,
): Promise<number> {
  const jobId = graphJobId();
  if (!jobId) throw new Error("Grouped graph work requires a durable root job");
  const byProject = new Map<string, { observations: CompressedObservation[]; ids: Set<string> }>();
  const observationProjects = new Map<string, string>();
  for (const observation of observations) {
    const session = observation.sessionId ? await kv.get<{ project?: string }>(KV.sessions, observation.sessionId) : null;
    const project = session?.project?.trim();
    if (!project) throw new Error("GRAPH_PROJECT_SCOPE_UNAVAILABLE");
    const priorProject = observationProjects.get(observation.id);
    if (priorProject && priorProject !== project) throw new Error("GRAPH_PROJECT_SCOPE_MISMATCH");
    observationProjects.set(observation.id, project);
    if (!byProject.has(project)) byProject.set(project, { observations: [], ids: new Set() });
    const group = byProject.get(project)!;
    if (!group.ids.has(observation.id)) {
      group.ids.add(observation.id);
      group.observations.push(observation);
    }
  }

  let pageIndex = 0;
  let planBytes = 0;
  for (const [project, group] of byProject) {
    const units = await prepareGraphExtractionInputs(group.observations, targetChars, undefined, true);
    for (const unit of units) {
      let sources: ProjectGraphWorkPage["sources"] = [];
      let sourceBytes = 0;
      const persistPage = async () => {
        if (!sources.length) return;
        const page: ProjectGraphWorkPage = { version: 2, project, sources };
        const serializedBytes = Buffer.byteLength(JSON.stringify(page), "utf8");
        planBytes += serializedBytes;
        if (planBytes > MAX_PROJECT_GRAPH_PLAN_BYTES) throw new Error("Project graph work plan exceeds the 64 MiB capture limit");
        await withGraphDelta(kv, () => kv.set(KV.graphProviderResults(jobId), `project-work-page:v3:${pageIndex}`, page));
        pageIndex++;
        sources = [];
        sourceBytes = 0;
      };
      for (let index = 0; index < unit.promptObservations.length; index++) {
        const observation = unit.promptObservations[index]!;
        const source = unit.sourceObservations[index];
        if (!source) throw new Error("Graph prompt source mapping is incomplete");
        let fragment = 0;
        for (const content of splitGraphWorkContent(graphWorkObservationContent(observation), GRAPH_WORK_LIMITS.maxPageBytes - 512)) {
          const entry = { id: source.id, version: `${source.timestamp || "unknown"}:${fragment++}`, content };
          const bytes = Buffer.byteLength(entry.id, "utf8") + Buffer.byteLength(entry.version, "utf8") + Buffer.byteLength(entry.content, "utf8");
          if (bytes > GRAPH_WORK_LIMITS.maxPageBytes) throw new Error("Project graph source exceeds its bounded page budget");
          if (sources.length >= GRAPH_WORK_LIMITS.maxPageSources || sourceBytes + bytes > GRAPH_WORK_LIMITS.maxPageBytes) await persistPage();
          sources.push(entry);
          sourceBytes += bytes;
        }
      }
      await persistPage();
    }
  }
  return pageIndex;
}

// #753: keep the response payload below the iii state channel ceiling.
// 500 nodes + their incident edges hold well under the limit on the
// reported 11k-node / 28k-edge corpus, and 5,000 is the upper bound a
// caller can request explicitly. Tuned conservatively because edges
// fan out faster than nodes.
const DEFAULT_GRAPH_QUERY_LIMIT = 500;
const MAX_GRAPH_QUERY_LIMIT = 5000;

// #814: the precomputed snapshot covers the top-degree subgraph used by
// the empty-body / nodeType-only branch — the path the viewer hits on
// tab load. Sized to match the default query limit so the snapshot can
// service a default-cap request without falling back to live
// enumeration. Aggregate stats (nodesByType / edgesByType) are computed
// fresh during rebuild and stored alongside.
const SNAPSHOT_TOP_NODES = DEFAULT_GRAPH_QUERY_LIMIT;
const SNAPSHOT_MAX_OBSERVATIONS_PER_RECORD = 64;
const MAX_GRAPH_SNAPSHOT_BYTES = 4 * 1024 * 1024;
const SNAPSHOT_KEY = "current";

function emptySnapshot(): GraphSnapshot {
  return {
    version: 1,
    topNodes: [],
    topEdges: [],
    topDegrees: {},
    stats: {
      totalNodes: 0,
      totalEdges: 0,
      nodesByType: {},
      edgesByType: {},
    },
    updatedAt: new Date(0).toISOString(),
    dirty: true,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function hasCountMap(value: unknown): value is Record<string, number> {
  return (
    isRecord(value) &&
    Object.values(value).every(
      (count) => Number.isSafeInteger(count) && (count as number) >= 0,
    )
  );
}

function hasSnapshotNode(value: unknown): value is GraphNode {
  if (!isRecord(value)) return false;
  return (
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.name === "string"
  );
}

function hasSnapshotEdge(value: unknown): value is GraphEdge {
  if (!isRecord(value)) return false;
  return (
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.sourceNodeId === "string" &&
    typeof value.targetNodeId === "string" &&
    typeof value.weight === "number" &&
    Number.isFinite(value.weight)
  );
}

function isGraphSnapshot(value: unknown): value is GraphSnapshot {
  if (!isRecord(value) || !isRecord(value.stats)) return false;
  return (
    value.version === 1 &&
    // Snapshot rebuilds can include rows written by older graph schemas.
    // Validate the fields these cached-query and incremental-update paths use.
    Array.isArray(value.topNodes) &&
    value.topNodes.every(hasSnapshotNode) &&
    Array.isArray(value.topEdges) &&
    value.topEdges.every(hasSnapshotEdge) &&
    isRecord(value.topDegrees) &&
    Object.values(value.topDegrees).every(
      (degree) => typeof degree === "number" && Number.isFinite(degree) && degree >= 0,
    ) &&
    Number.isSafeInteger(value.stats.totalNodes) &&
    (value.stats.totalNodes as number) >= 0 &&
    Number.isSafeInteger(value.stats.totalEdges) &&
    (value.stats.totalEdges as number) >= 0 &&
    hasCountMap(value.stats.nodesByType) &&
    hasCountMap(value.stats.edgesByType) &&
    typeof value.updatedAt === "string" &&
    typeof value.dirty === "boolean" &&
    (value.batchInProgress === undefined || typeof value.batchInProgress === "string") &&
    (value.resetAt === undefined || typeof value.resetAt === "string") &&
    (value.appliedBatchEffects === undefined ||
      (Array.isArray(value.appliedBatchEffects) &&
        value.appliedBatchEffects.every((key) => typeof key === "string")))
  );
}

async function readSnapshot(kv: StateKV): Promise<GraphSnapshot | null> {
  let snap: unknown;
  try {
    snap = await kv.get<GraphSnapshot>(KV.graphSnapshot, SNAPSHOT_KEY);
  } catch (err) {
    if (graphTransactionFailure(err)) throw err;
    logger.warn("Graph snapshot read failed", {
      error: err instanceof Error ? err.message : String(err),
    });
    throw new Error("Graph snapshot read failed; refusing to continue");
  }

  if (snap == null) return null;
  if (!isGraphSnapshot(snap)) {
    logger.warn("Graph snapshot is malformed; refusing to continue");
    throw new Error("Graph snapshot is malformed; refusing to continue");
  }
  return snap;
}

function projectGraphSnapshot(snapshot: GraphSnapshot): GraphSnapshot {
  const projected = structuredClone(snapshot);
  const nodes = projected.topNodes;
  const edges = projected.topEdges;
  const degrees = projected.topDegrees;
  projected.topNodes = [];
  projected.topEdges = [];
  projected.topDegrees = {};
  let bytes = Buffer.byteLength(JSON.stringify(projected), "utf8");
  if (bytes > MAX_GRAPH_SNAPSHOT_BYTES) {
    throw new Error(
      `Graph snapshot projection is ${bytes} bytes, over the ${MAX_GRAPH_SNAPSHOT_BYTES}-byte cache limit`,
    );
  }
  const retainedIds = new Set<string>();
  const retainedDegrees: Array<[string, number]> = [];
  for (const node of nodes.slice(0, SNAPSHOT_TOP_NODES)) {
    if (retainedIds.has(node.id)) continue;
    if (
      Array.isArray(node.sourceObservationIds) &&
      node.sourceObservationIds.length > SNAPSHOT_MAX_OBSERVATIONS_PER_RECORD
    ) {
      node.sourceObservationIds = node.sourceObservationIds.slice(
        -SNAPSHOT_MAX_OBSERVATIONS_PER_RECORD,
      );
    }
    const degree = degrees[node.id] ?? 0;
    const addition = Buffer.byteLength(JSON.stringify(node), "utf8")
      + Buffer.byteLength(`${JSON.stringify(node.id)}:${JSON.stringify(degree)}`, "utf8")
      + (retainedIds.size ? 2 : 0);
    if (bytes + addition > MAX_GRAPH_SNAPSHOT_BYTES) continue;
    projected.topNodes.push(node);
    retainedIds.add(node.id);
    retainedDegrees.push([node.id, degree]);
    bytes += addition;
  }
  projected.topDegrees = Object.fromEntries(retainedDegrees);
  for (const edge of edges) {
    if (!retainedIds.has(edge.sourceNodeId) || !retainedIds.has(edge.targetNodeId)) continue;
    if (
      Array.isArray(edge.sourceObservationIds) &&
      edge.sourceObservationIds.length > SNAPSHOT_MAX_OBSERVATIONS_PER_RECORD
    ) {
      edge.sourceObservationIds = edge.sourceObservationIds.slice(
        -SNAPSHOT_MAX_OBSERVATIONS_PER_RECORD,
      );
    }
    const addition = Buffer.byteLength(JSON.stringify(edge), "utf8")
      + (projected.topEdges.length ? 1 : 0);
    if (bytes + addition > MAX_GRAPH_SNAPSHOT_BYTES) continue;
    projected.topEdges.push(edge);
    bytes += addition;
  }
  return projected;
}

async function writeGraphSnapshot(
  kv: StateKV,
  snapshot: GraphSnapshot,
): Promise<void> {
  await kv.set(
    KV.graphSnapshot,
    SNAPSHOT_KEY,
    projectGraphSnapshot(snapshot),
  );
}

// Legacy global snapshot enumeration is disabled; bounded project reads
// use native candidate selectors instead.

function nameIndexKey(type: string, name: string): string {
  return `${type}|${name}`;
}

function edgeIndexKey(
  sourceNodeId: string,
  targetNodeId: string,
  type: string,
): string {
  return `${sourceNodeId}|${targetNodeId}|${type}`;
}

function scopedGraphItemId(item: unknown): string | undefined {
  if (!item || typeof item !== "object") return undefined;
  const record = item as Record<string, unknown>;
  return typeof record.id === "string" ? record.id :
    typeof record.key === "string" ? record.key : undefined;
}

function scopedGraphEdge(item: unknown): GraphEdge | undefined {
  if (!item || typeof item !== "object") return undefined;
  const record = item as Record<string, unknown>;
  const value = record.value && typeof record.value === "object"
    ? record.value as GraphEdge
    : record as unknown as GraphEdge;
  return typeof value.id === "string" && typeof value.sourceNodeId === "string" &&
    typeof value.targetNodeId === "string" ? value : undefined;
}

async function hydrateScopedGraphNodes(
  kv: StateKV,
  ids: readonly string[],
): Promise<{ nodes: GraphNode[]; incomplete: boolean }> {
  const nodes: GraphNode[] = [];
  let incomplete = false;
  for (let offset = 0; offset < ids.length; offset += 12) {
    const batch = ids.slice(offset, offset + 12);
    const rows = await Promise.all(batch.map((id) => kv.get<GraphNode>(KV.graphNodes, id)));
    for (const [index, node] of rows.entries()) {
      if (!node || node.id !== batch[index] || node.stale) {
        incomplete = true;
        continue;
      }
      nodes.push(node);
    }
  }
  return { nodes, incomplete };
}

async function readScopedGraphEdges(
  kv: StateKV,
  project: string,
  nodeIds: readonly string[],
): Promise<{ edges: GraphEdge[]; partial: boolean; reason?: string }> {
  const edges: GraphEdge[] = [];
  let partial = false;
  let reason: string | undefined;
  for (let offset = 0; offset < nodeIds.length && edges.length < 16; offset += 12) {
    const selected = nodeIds.slice(offset, offset + 12);
    const page = await readScopedGraphPage<{ id?: string; key?: string; value?: GraphEdge }>(kv, {
      project, mode: "neighbors", node_ids: selected, limit: 16, max_bytes: 64 * 1024,
    });
    if (page.status !== "ready" || page.has_more) {
      partial = true;
      reason ??= page.reason;
    }
    for (const item of page.items) {
      const edge = scopedGraphEdge(item);
      if (!edge || edge.stale || edges.some((existing) => existing.id === edge.id) ||
        !nodeIds.includes(edge.sourceNodeId) || !nodeIds.includes(edge.targetNodeId)) continue;
      edges.push(edge);
      if (edges.length === 16) {
        partial = true;
        break;
      }
    }
  }
  return { edges, partial, ...(reason ? { reason } : {}) };
}

function scopedGraphWarning(reason: string | undefined, fallback: string): string {
  return reason ? `${fallback} (${reason}).` : `${fallback}.`;
}

// Mutates `snap` to apply a +1 (or -1) degree delta for nodeId,
// maintaining the top-N ranking. Returns the new degree. Reads /
// writes the per-node degree counter via targeted kv.get/set so we
// never enumerate. Top-N membership flips when:
//   - node's new degree > current min in topNodes AND it's not in
//     topNodes (promote, evict tail if topNodes is full)
//   - node IS in topNodes and its position needs resorting (re-sort
//     topNodes in place)
async function applyDegreeDelta(
  kv: StateKV,
  snap: GraphSnapshot,
  nodeId: string,
  delta: number,
): Promise<number> {
  const stored = await kv.get<number | BatchGraphDegree>(KV.graphNodeDegree, nodeId);
  const prev = typeof stored === "number" ? stored : stored?.value ?? 0;
  const next = Math.max(0, prev + delta);
  await kv.set(KV.graphNodeDegree, nodeId, typeof stored === "object" && stored ? { ...stored, value: next } : next);

  const inTop = snap.topNodes.findIndex((n) => n.id === nodeId);
  if (inTop !== -1) {
    // Cache the new degree in topDegrees so the comparator runs
    // synchronously over numbers, not async kv.get calls. Re-sort
    // descending by degree.
    snap.topDegrees[nodeId] = next;
    snap.topNodes.sort(
      (a, b) =>
        (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0),
    );
    return next;
  }

  if (snap.topNodes.length < SNAPSHOT_TOP_NODES) {
    // Capacity available — fetch + promote.
    const node = await kv.get<GraphNode>(KV.graphNodes, nodeId);
    if (node && !node.stale) {
      snap.topNodes.push(node);
      snap.topDegrees[node.id] = next;
      snap.topNodes.sort(
        (a, b) =>
          (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0),
      );
    }
    return next;
  }

  // topNodes is full; the cutoff is the tail's cached degree.
  const tailEntry = snap.topNodes[snap.topNodes.length - 1];
  if (!tailEntry) return next;
  const tailDegree = snap.topDegrees[tailEntry.id] ?? 0;
  if (next > tailDegree) {
    const node = await kv.get<GraphNode>(KV.graphNodes, nodeId);
    if (node && !node.stale) {
      const evicted = snap.topNodes.pop();
      if (evicted) delete snap.topDegrees[evicted.id];
      snap.topNodes.push(node);
      snap.topDegrees[node.id] = next;
      snap.topNodes.sort(
        (a, b) =>
          (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0),
      );
    }
  }
  return next;
}

function snapshotPushEdgeIfBothInTop(
  snap: GraphSnapshot,
  edge: GraphEdge,
): void {
  const topIds = new Set(snap.topNodes.map((n) => n.id));
  if (topIds.has(edge.sourceNodeId) && topIds.has(edge.targetNodeId)) {
    // Dedupe in case the same edge gets pushed twice.
    if (!snap.topEdges.find((e) => e.id === edge.id)) {
      snap.topEdges.push(edge);
    }
  }
}

function mergeNode(
  existing: GraphNode,
  incoming: GraphNode,
  obsIds: string[],
  capturedAt: string,
): GraphNode {
  return {
    ...existing,
    sourceObservationIds: [
      ...new Set([
        ...existing.sourceObservationIds,
        ...incoming.sourceObservationIds,
        ...obsIds,
      ]),
    ],
    properties: { ...existing.properties, ...incoming.properties },
    updatedAt: capturedAt,
  };
}

function mergeEdge(
  existing: GraphEdge,
  incoming: GraphEdge,
  obsIds: string[],
): GraphEdge {
  return {
    ...existing,
    sourceObservationIds: [
      ...new Set([...existing.sourceObservationIds, ...incoming.sourceObservationIds, ...obsIds]),
    ],
  };
}

function resolvePagination(
  rawLimit: number | undefined,
  rawOffset: number | undefined,
): { limit: number; offset: number } {
  const requested = typeof rawLimit === "number" && Number.isFinite(rawLimit)
    ? Math.floor(rawLimit)
    : DEFAULT_GRAPH_QUERY_LIMIT;
  const limit = Math.max(1, Math.min(requested, MAX_GRAPH_QUERY_LIMIT));
  const offset = Math.max(
    0,
    typeof rawOffset === "number" && Number.isFinite(rawOffset)
      ? Math.floor(rawOffset)
      : 0,
  );
  return { limit, offset };
}

function paginate(
  nodes: GraphNode[],
  allEdges: GraphEdge[],
  depth: number,
  limit: number,
  offset: number,
): GraphQueryResult {
  const totalNodes = nodes.length;
  const pageNodes = nodes.slice(offset, offset + limit);
  const pageNodeIds = new Set(pageNodes.map((n) => n.id));
  // Edges restricted to the page so the response payload scales with
  // `limit`, not with the global edge count. An edge is included only
  // when BOTH endpoints land in the page — half-edges to nodes outside
  // the page would render as dangling links in the viewer.
  const pageEdges = allEdges.filter(
    (e) => pageNodeIds.has(e.sourceNodeId) && pageNodeIds.has(e.targetNodeId),
  );
  // Total edges (for the same node universe). Counted unbounded so the
  // viewer can show "showing X of Y" without re-querying.
  const universeIds = new Set(nodes.map((n) => n.id));
  const totalEdges = allEdges.reduce(
    (count, e) =>
      universeIds.has(e.sourceNodeId) && universeIds.has(e.targetNodeId)
        ? count + 1
        : count,
    0,
  );
  return {
    nodes: pageNodes,
    edges: pageEdges,
    depth,
    totalNodes,
    totalEdges,
    truncated: totalNodes > pageNodes.length,
    limit,
    offset,
  };
}

// Parse all key="value" pairs from a tag's attribute string, in any
// order. The previous parser hard-coded attribute order
// (type before name on <entity>, type/source/target/weight on
// <relationship>) and silently dropped nodes/edges when the upstream
// LLM emitted attributes in a different order — Codex in particular
// likes to lead with `name=` (#635).
function parseAttrs(raw: string): Record<string, string> {
  const attrs: Record<string, string> = {};
  const attrRegex = /([A-Za-z_][\w:-]*)="([^"]*)"/g;
  let m;
  while ((m = attrRegex.exec(raw)) !== null) {
    attrs[m[1]] = m[2];
  }
  return attrs;
}

function parseGraphXml(
  xml: string,
  observationIds: string[],
  requireProvenance = false,
): {
  nodes: GraphNode[];
  edges: GraphEdge[];
} {
  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const now = graphCapturedAt();
  const sourceIds = (raw: string | undefined): string[] => {
    if (!requireProvenance) return observationIds;
    if (!raw) throw new Error("Graph extraction omitted observation references");
    const indices = raw.split(",").map((part) => part.trim());
    if (indices.some((part) => !/^[1-9]\d*$/.test(part))) {
      throw new Error("Graph extraction has invalid observation references");
    }
    const unique = new Set(indices);
    if (unique.size !== indices.length || indices.some((part) => Number(part) > observationIds.length)) {
      throw new Error("Graph extraction has duplicate or out-of-range observation references");
    }
    return indices.map((part) => observationIds[Number(part) - 1]);
  };

  // Two passes because <entity> can be self-closing or have a body
  // (<property> children). The self-closing form needs `[^>]*[^/]` on
  // the attr group so the trailing `/` isn't swallowed into the match
  // (root cause of #494). The explicit-close form picks up the
  // property block.
  const entitySelfClose = /<entity\b([^>]*?)\/>/g;
  const entityWithBody = /<entity\b([^>]*[^/])>([\s\S]*?)<\/entity>/g;

  const addEntity = (rawAttrs: string, propsBlock = ""): void => {
    const attrs = parseAttrs(rawAttrs);
    const type = attrs["type"] as GraphNode["type"] | undefined;
    const name = attrs["name"];
    if (!type || !name) return;
    const referencedIds = sourceIds(attrs["observations"]);
    const properties: Record<string, string> = {};
    const propRegex = /<property\s+key="([^"]+)">([^<]*)<\/property>/g;
    let propMatch;
    while ((propMatch = propRegex.exec(propsBlock)) !== null) {
      properties[propMatch[1]] = propMatch[2];
    }
    nodes.push({
      id: generateId("gn"),
      type,
      name,
      properties,
      sourceObservationIds: referencedIds,
      createdAt: now,
    });
  };

  let match;
  while ((match = entitySelfClose.exec(xml)) !== null) {
    addEntity(match[1]);
  }
  while ((match = entityWithBody.exec(xml)) !== null) {
    addEntity(match[1], match[2]);
  }

  const relRegex = /<relationship\b([^>]*?)\/>/g;
  while ((match = relRegex.exec(xml)) !== null) {
    const attrs = parseAttrs(match[1]);
    const type = attrs["type"] as GraphEdge["type"] | undefined;
    const sourceName = attrs["source"];
    const targetName = attrs["target"];
    if (!type || !sourceName || !targetName) continue;
    const referencedIds = sourceIds(attrs["observations"]);
    const parsedWeight = parseFloat(attrs["weight"] ?? "");
    const weight = Number.isFinite(parsedWeight) ? parsedWeight : 0.5;

    const sourceNode = nodes.find((n) => n.name === sourceName);
    const targetNode = nodes.find((n) => n.name === targetName);
    if (!sourceNode || !targetNode) continue;
    edges.push({
      id: generateId("ge"),
      type,
      sourceNodeId: sourceNode.id,
      targetNodeId: targetNode.id,
      weight: Math.max(0, Math.min(1, weight)),
      sourceObservationIds: referencedIds,
      createdAt: now,
    });
  }

  return { nodes, edges };
}

type ParsedGraphExtraction = {
  nodes: GraphNode[];
  edges: GraphEdge[];
};

const EMPTY_GRAPH_EXTRACTION_ERROR = "Graph extraction response contained no nodes or edges";

function parseGraphExtractionResponse(
  response: unknown,
  observationIds: string[],
  requireProvenance = false,
): ParsedGraphExtraction | null {
  if (typeof response !== "string") return null;
  try {
    const parsed = parseGraphXml(response, observationIds, requireProvenance);
    return parsed.nodes.length === 0 && parsed.edges.length === 0 ? null : parsed;
  } catch (error) {
    if (!requireProvenance) throw error;
    logger.warn("Graph extraction rejected invalid observation references", {
      error: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}

interface BatchGraphDegree extends BatchEffectMetadata { value: number }

async function applyBatchGraph(kv: StateKV, parsed: ParsedGraphExtraction, obsIds: string[], key: string): Promise<{ nodesAdded: number; edgesAdded: number }> {
  const snap = structuredClone((await readSnapshot(kv)) ?? emptySnapshot());
  if (snap.appliedBatchEffects?.includes(key)) return { nodesAdded: 0, edgesAdded: 0 };
  if (snap.batchInProgress && snap.batchInProgress !== key) throw new Error("A batch graph application must be recovered first");
  if (!snap.batchInProgress) {
    snap.batchInProgress = key;
    await writeGraphSnapshot(kv, snap);
  }
  const metadata = effectMetadata(snap, key);
  const { nodes, edges } = parsed;
  const remapped = new Map<string, string>();
  const countedNodes = new Set<string>();
  let edgesAdded = 0;
  for (const node of nodes) {
    const index = nameIndexKey(node.type, node.name);
    const indexedId = await kv.get<string>(KV.graphNameIndex, index);
    const candidateId = fingerprintId("gn", `${key}:${index}`);
    let current = await kv.get<GraphNode>(KV.graphNodes, indexedId ?? candidateId);
    if (current && snap.resetAt && current.createdAt < snap.resetAt) current = null;
    const id = current?.id ?? candidateId;
    remapped.set(node.id, id);
    const next = current ? mergeNode(current, node, obsIds, current.updatedAt ?? current.createdAt) : { ...node, id };
    await kv.set(KV.graphNodes, id, next);
    await kv.set(KV.graphNameIndex, index, id);
    // Deterministic IDs distinguish this callback's creations even if a
    // preceding attempt died between the row, index, and snapshot writes.
    if (id === candidateId && !countedNodes.has(id)) {
      countedNodes.add(id);
      snap.stats.totalNodes++;
      snap.stats.nodesByType[node.type] = (snap.stats.nodesByType[node.type] ?? 0) + 1;
      if (snap.topNodes.length < SNAPSHOT_TOP_NODES && !snap.topNodes.some((n) => n.id === id)) {
        snap.topNodes.push(next);
        snap.topDegrees[id] = 0;
      }
    } else {
      const position = snap.topNodes.findIndex((n) => n.id === id);
      if (position >= 0) snap.topNodes[position] = next;
    }
  }
  const edgesByIndex = new Map<string, GraphEdge>();
  for (const edge of edges) {
    edge.sourceNodeId = remapped.get(edge.sourceNodeId)!;
    edge.targetNodeId = remapped.get(edge.targetNodeId)!;
    const index = edgeIndexKey(edge.sourceNodeId, edge.targetNodeId, edge.type);
    const prior = edgesByIndex.get(index);
    edgesByIndex.set(index, prior ? mergeEdge(prior, edge, []) : edge);
  }
  for (const [index, edge] of edgesByIndex) {
    const indexedId = await kv.get<string>(KV.graphEdgeKey, index);
    const candidateId = fingerprintId("ge", `${key}:${index}`);
    let current = await kv.get<GraphEdge>(KV.graphEdges, indexedId ?? candidateId);
    if (current && snap.resetAt && current.createdAt < snap.resetAt) current = null;
    const id = current?.id ?? candidateId;
    const next = current ? mergeEdge(current, edge, obsIds) : { ...edge, id };
    await kv.set(KV.graphEdges, id, next);
    await kv.set(KV.graphEdgeKey, index, id);
    if (id === candidateId) {
      edgesAdded++;
      snap.stats.totalEdges++;
      snap.stats.edgesByType[edge.type] = (snap.stats.edgesByType[edge.type] ?? 0) + 1;
      for (const [endpoint, nodeId] of [edge.sourceNodeId, edge.targetNodeId].entries()) {
        const degree = await kv.get<number | BatchGraphDegree>(KV.graphNodeDegree, nodeId);
        const record: BatchGraphDegree = typeof degree === "number" ? { value: degree } : degree ?? { value: 0 };
        const degreeKey = batchEffectKey(`${key}:${id}:${endpoint}`);
        const value = record.value + (record.appliedBatchEffects?.includes(degreeKey) ? 0 : 1);
        await kv.set(KV.graphNodeDegree, nodeId, { value, ...effectMetadata(record, degreeKey) });
        const position = snap.topNodes.findIndex((n) => n.id === nodeId);
        if (position >= 0) snap.topDegrees[nodeId] = value;
        else if (snap.topNodes.length < SNAPSHOT_TOP_NODES || value > (snap.topDegrees[snap.topNodes.at(-1)!.id] ?? 0)) {
          const node = await kv.get<GraphNode>(KV.graphNodes, nodeId);
          if (node) {
            if (snap.topNodes.length >= SNAPSHOT_TOP_NODES) delete snap.topDegrees[snap.topNodes.pop()!.id];
            snap.topNodes.push(node);
            snap.topDegrees[nodeId] = value;
          }
        }
        snap.topNodes.sort((a, b) => (snap.topDegrees[b.id] ?? 0) - (snap.topDegrees[a.id] ?? 0));
      }
    }
    snapshotPushEdgeIfBothInTop(snap, next);
  }
  const topIds = new Set(snap.topNodes.map((n) => n.id));
  snap.topEdges = snap.topEdges.filter((e) => topIds.has(e.sourceNodeId) && topIds.has(e.targetNodeId));
  const { batchInProgress: _pending, ...completedSnapshot } = snap;
  await writeGraphSnapshot(kv, {
    ...completedSnapshot,
    ...metadata,
    dirty: false,
    updatedAt: graphCapturedAt(),
  });
  return { nodesAdded: countedNodes.size, edgesAdded };
}

const HEURISTIC_EDGE_WEIGHT = 0.4;
const MAX_HEURISTIC_EDGES_PER_OBS = 12;

export function extractGraphHeuristics(
  observations: CompressedObservation[],
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const now = graphCapturedAt();
  const nodes: GraphNode[] = [];
  const nodeByKey = new Map<string, GraphNode>();
  const edges: GraphEdge[] = [];
  const edgeByPair = new Map<string, GraphEdge>();

  const nodeFor = (
    type: GraphNode["type"],
    name: string,
    obsId: string,
  ): GraphNode | null => {
    const trimmed = name.trim();
    if (!trimmed) return null;
    const key = `${type}\0${trimmed.toLowerCase()}`;
    let node = nodeByKey.get(key);
    if (!node) {
      node = {
        id: generateId("gn"),
        type,
        name: trimmed,
        properties: {},
        sourceObservationIds: [obsId],
        createdAt: now,
      };
      nodeByKey.set(key, node);
      nodes.push(node);
    } else if (!node.sourceObservationIds.includes(obsId)) {
      node.sourceObservationIds.push(obsId);
    }
    return node;
  };

  for (const obs of observations) {
    let budget = MAX_HEURISTIC_EDGES_PER_OBS;
    const link = (a: GraphNode | null, b: GraphNode | null): void => {
      if (!a || !b || a.id === b.id) return;
      const pair = a.id < b.id ? `${a.id}|${b.id}` : `${b.id}|${a.id}`;
      const existing = edgeByPair.get(pair);
      if (existing) {
        if (!existing.sourceObservationIds.includes(obs.id)) {
          existing.sourceObservationIds.push(obs.id);
        }
        return;
      }
      if (budget <= 0) return;
      budget -= 1;
      const edge: GraphEdge = {
        id: generateId("ge"),
        type: "related_to",
        sourceNodeId: a.id,
        targetNodeId: b.id,
        weight: HEURISTIC_EDGE_WEIGHT,
        sourceObservationIds: [obs.id],
        createdAt: now,
      };
      edgeByPair.set(pair, edge);
      edges.push(edge);
    };

    const fileNodes = (obs.files ?? []).map((f) =>
      nodeFor("file", f, obs.id),
    );
    const conceptNodes = (obs.concepts ?? []).map((c) =>
      nodeFor("concept", c, obs.id),
    );

    for (const concept of conceptNodes) {
      for (const file of fileNodes) link(concept, file);
    }
    for (let i = 0; i + 1 < conceptNodes.length; i++) {
      link(conceptNodes[i], conceptNodes[i + 1]);
    }
    for (let i = 0; i + 1 < fileNodes.length; i++) {
      link(fileNodes[i], fileNodes[i + 1]);
    }
  }

  return { nodes, edges };
}

// Shared persistence for a batch of extracted/imported nodes and edges.
// Factored out of mem::graph-extract so structural importers (graphify)
// reuse the exact same name-index upsert, degree bookkeeping, and snapshot
// maintenance — which also makes re-imports idempotent: an existing
// (type, name) resolves through the name index and merges instead of
// duplicating.
//
// #814 v2: targeted name-index lookups replace the O(n) scan over
// `kv.list<GraphNode>(KV.graphNodes)`. At 75K nodes the list payload
// exceeds the iii heartbeat budget and the worker dies before merge can
// complete. Each name-index entry is a single small kv.get/set pair.
export async function persistGraphDelta(
  kv: StateKV,
  nodes: GraphNode[],
  edges: GraphEdge[],
  obsIds: string[],
): Promise<{ newNodeCount: number; newEdgeCount: number }> {
  const guarded = graphKV(kv);
  if (hasGraphDelta()) return persistGraphDeltaValues(guarded, nodes, edges, obsIds);
  return runGraphJob(guarded, "import", { nodes, edges, obsIds }, async (input) => {
    const frozen = input as { nodes: GraphNode[]; edges: GraphEdge[]; obsIds: string[] };
    return withGraphDelta(guarded, () => persistGraphDeltaValues(guarded, frozen.nodes, frozen.edges, frozen.obsIds));
  });
}

async function persistGraphDeltaValues(
  kv: StateKV,
  nodes: GraphNode[],
  edges: GraphEdge[],
  obsIds: string[],
): Promise<{ newNodeCount: number; newEdgeCount: number }> {
  const snap = (await readSnapshot(kv)) ?? emptySnapshot();
  const capturedAt = graphCapturedAt();
  let newNodeCount = 0;
  let newEdgeCount = 0;
  // Merge-only batches mutate cached topNodes/topEdges entries without
  // changing the counts; track that separately so the snapshot still persists.
  let snapMutated = false;
  const newEdgesForTopCheck: GraphEdge[] = [];
  // When a freshly-minted node merges into an existing row via the name
  // index, edges in the same batch still reference the fresh id. Remap edge
  // endpoints to the persisted ids so edges never dangle and re-runs hit the
  // same edge-index key instead of duplicating.
  const idRemap = new Map<string, string>();

  for (const node of nodes) {
    const indexKey = nameIndexKey(node.type, node.name);
    const existingId = await kv.get<string>(KV.graphNameIndex, indexKey);

    let existing: GraphNode | null = null;
    if (existingId) {
      existing = await kv.get<GraphNode>(KV.graphNodes, existingId);
      // #825 follow-up: name-index lookups can resolve into
      // pre-reset rows. Drop them so extract writes a fresh
      // node + index entry instead of silently reconnecting
      // to a legacy orphan (which would keep the snapshot at
      // 0 forever after a reset).
      if (
        existing &&
        snap.resetAt &&
        typeof existing.createdAt === "string" &&
        existing.createdAt < snap.resetAt
      ) {
        existing = null;
      }
    }

    if (existing) {
      idRemap.set(node.id, existing.id);
      const merged = mergeNode(existing, node, obsIds, capturedAt);
      await kv.set(KV.graphNodes, existing.id, merged);
      // Update topNodes entry if present so a stale clone isn't
      // returned from the snapshot fast path.
      const topIdx = snap.topNodes.findIndex((n) => n.id === existing!.id);
      if (topIdx !== -1) {
        snap.topNodes[topIdx] = merged;
        snapMutated = true;
      }
    } else {
      await kv.set(KV.graphNodes, node.id, node);
      await kv.set(KV.graphNameIndex, indexKey, node.id);
      await kv.set(KV.graphNodeDegree, node.id, 0);
      snap.stats.totalNodes += 1;
      snap.stats.nodesByType[node.type] =
        (snap.stats.nodesByType[node.type] ?? 0) + 1;
      newNodeCount += 1;
      if (snap.topNodes.length < SNAPSHOT_TOP_NODES) {
        // Degree 0 still beats an empty slot — sit at the tail
        // until edges arrive and promote.
        snap.topNodes.push(node);
        snap.topDegrees[node.id] = 0;
      }
    }
  }

  for (const rawEdge of edges) {
    const edge: GraphEdge = {
      ...rawEdge,
      sourceNodeId: idRemap.get(rawEdge.sourceNodeId) ?? rawEdge.sourceNodeId,
      targetNodeId: idRemap.get(rawEdge.targetNodeId) ?? rawEdge.targetNodeId,
    };
    const eKey = edgeIndexKey(edge.sourceNodeId, edge.targetNodeId, edge.type);
    const existingId = await kv.get<string>(KV.graphEdgeKey, eKey);

    let existing: GraphEdge | null = null;
    if (existingId) {
      existing = await kv.get<GraphEdge>(KV.graphEdges, existingId);
      // Same #825 orphan check as the node path above.
      if (
        existing &&
        snap.resetAt &&
        typeof existing.createdAt === "string" &&
        existing.createdAt < snap.resetAt
      ) {
        existing = null;
      }
    }

    if (existing) {
      const merged = mergeEdge(existing, edge, obsIds);
      await kv.set(KV.graphEdges, existing.id, merged);
      // Replace cached topEdges entry too if present.
      const topIdx = snap.topEdges.findIndex((e) => e.id === existing!.id);
      if (topIdx !== -1) {
        snap.topEdges[topIdx] = merged;
        snapMutated = true;
      }
    } else {
      await kv.set(KV.graphEdges, edge.id, edge);
      await kv.set(KV.graphEdgeKey, eKey, edge.id);
      snap.stats.totalEdges += 1;
      snap.stats.edgesByType[edge.type] =
        (snap.stats.edgesByType[edge.type] ?? 0) + 1;
      newEdgeCount += 1;
      await applyDegreeDelta(kv, snap, edge.sourceNodeId, +1);
      await applyDegreeDelta(kv, snap, edge.targetNodeId, +1);
      newEdgesForTopCheck.push(edge);
    }
  }

  // Push newly-added edges into snapshot.topEdges if both
  // endpoints are in the top-N (post-degree-delta). Done after
  // all degree updates so the topIds set is stable.
  for (const edge of newEdgesForTopCheck) {
    snapshotPushEdgeIfBothInTop(snap, edge);
  }

  if (newNodeCount > 0 || newEdgeCount > 0 || snapMutated) {
    snap.updatedAt = capturedAt;
    snap.dirty = false;
    await writeGraphSnapshot(kv, snap);
  }

  return { newNodeCount, newEdgeCount };
}

export function registerGraphFunction(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  llmRouter?: LlmTaskRouter,
  batchQueue?: FireworksBatchQueue,
  localCompactor?: LocalGraphCompactor,
  typeSafe?: TypeSafeDecisionProvider,
): void {
  kv = graphKV(kv);
  type ExtractionRequest = GraphDispatchRequest & { graphWorkGroupsVersion?: 2 };
  const currentExtractionOptions = (): GraphExtractionOptions => ({
    graphEnabled: isGraphExtractionEnabled(), llmEnabled: isGraphExtractionEnabled() && !provider.name.includes("noop"),
    auxiliary: llmRouter?.hasAuxiliaryProvider === true, targetChars: getGraphExtractionInputTargetChars(),
    typeSafe: Boolean(typeSafe), batchQueue: Boolean(batchQueue), providerName: provider.name,
    localCompactor: Boolean(localCompactor), typeSafeCompaction: isTypeSafeFeatureEnabled("compaction"),
    typeSafeMaxStateChars: getTypeSafeConfig().maxStateChars,
  });
  const processProjectWorkStep = async (progress: ProjectGraphWorkProgress) => {
    const jobId = graphJobId();
    if (!jobId || progress.version !== 2 || !Number.isSafeInteger(progress.pageCount) || progress.pageCount < 0
      || !Number.isSafeInteger(progress.pageIndex) || progress.pageIndex < 0 || progress.pageIndex > progress.pageCount
      || !Number.isSafeInteger(progress.workUnits) || progress.workUnits < 0
      || !Number.isSafeInteger(progress.nodesAdded) || progress.nodesAdded < 0
      || !Number.isSafeInteger(progress.edgesAdded) || progress.edgesAdded < 0
      || typeof progress.extractionSystem !== "string" || typeof progress.envelopeSuffix !== "string") {
      throw new Error("Grouped graph continuation state is invalid");
    }
    const liveOptions = currentExtractionOptions();
    if (!progress.options || !graphExtractionOptionsCompatible(progress.options, liveOptions)) {
      throw new Error("Graph job configuration changed; restore the captured provider/compaction configuration before recovery");
    }
    if (progress.pageIndex >= progress.pageCount) return {
      success: true, nodesAdded: progress.nodesAdded, edgesAdded: progress.edgesAdded, workUnits: progress.workUnits,
    };
    const page = await kv.get<ProjectGraphWorkPage>(KV.graphProviderResults(jobId), `project-work-page:v3:${progress.pageIndex}`);
    if (!page || page.version !== 2 || !page.project || !Array.isArray(page.sources)
      || page.sources.length < 1 || page.sources.length > GRAPH_WORK_LIMITS.maxPageSources) {
      throw new Error("Captured project graph work page is unavailable");
    }
    const workInput = {
      audience: { scope: "project" as const, project: page.project }, sources: page.sources,
      systemPrompt: progress.extractionSystem,
      envelopePrefix: "Extract entities and relationships from these project-scoped observations:\n\n",
      envelopeSuffix: progress.envelopeSuffix,
    };
    const next = createNextGraphWorkUnit({ ...workInput, cursor: progress.cursor });
    if (!next.unit) {
      const updated: ProjectGraphWorkProgress = { ...progress, pageIndex: progress.pageIndex + 1, cursor: null, activeUnit: undefined };
      await writeGraphJobContinuation(kv, updated);
      return updated.pageIndex >= updated.pageCount
        ? { success: true, nodesAdded: updated.nodesAdded, edgesAdded: updated.edgesAdded, workUnits: updated.workUnits }
        : yieldGraphJob({ success: true, queued: true, ...updated });
    }
    const unitId = fingerprintId("graphunit", `${jobId}:${progress.pageIndex}:${next.unit.fingerprint}`);
    let activeUnit = progress.activeUnit;
    if (activeUnit) {
      if (activeUnit.pageIndex !== progress.pageIndex || activeUnit.id !== unitId
        || activeUnit.resultOrdinal !== graphJobNextDeltaOrdinal(kv)) {
        throw new Error("Grouped graph active unit does not match its durable cursor");
      }
    } else {
      await writeGraphJobContinuation(kv, undefined, (resultOrdinal) => ({
        ...progress, activeUnit: { id: unitId, pageIndex: progress.pageIndex, resultOrdinal },
      } satisfies ProjectGraphWorkProgress));
      activeUnit = { id: unitId, pageIndex: progress.pageIndex, resultOrdinal: graphJobNextDeltaOrdinal(kv) };
    }
    const sourceIds = [...new Set(next.unit.fragments.map((fragment) => fragment.sourceId))];
    const prompt = `${workInput.envelopePrefix}${next.unit.fragments.map((fragment, index) =>
      `[${index + 1}] Source ID: ${fragment.sourceId}\n${fragment.content}`).join("\n\n")}${workInput.envelopeSuffix}`;
    if (Buffer.byteLength(JSON.stringify({ systemPrompt: progress.extractionSystem, userPrompt: prompt }), "utf8") > GRAPH_WORK_LIMITS.maxInputBytes) {
      throw new Error("Graph work prompt exceeds its bounded input budget");
    }
    let parsed: ParsedGraphExtraction;
    try {
      const response = await freezeGraphHandledValue(kv, `project-work-response:v3:${activeUnit.id}`, async () => {
        const execute = () => llmRouter
          ? llmRouter.run("graph_extraction",
            selectedProvider => selectedProvider.compress(progress.extractionSystem, prompt),
            candidate => parseGraphExtractionResponse(candidate, sourceIds, true) !== null)
          : provider.compress(progress.extractionSystem, prompt);
        const candidate = await execute();
        if (Buffer.byteLength(candidate, "utf8") > GRAPH_WORK_LIMITS.maxResponseBytes) {
          throw new Error("Graph extraction response exceeds its bounded 16 KiB result budget");
        }
        return candidate;
      });
      if (Buffer.byteLength(response, "utf8") > GRAPH_WORK_LIMITS.maxResponseBytes) {
        throw new Error("Captured graph response exceeds its bounded 16 KiB result budget");
      }
      parsed = await freezeGraphHandledValue(kv, `project-work-parsed:v3:${activeUnit.id}`, () => {
        const result = parseGraphExtractionResponse(response, sourceIds, true);
        if (!result) throw new Error(EMPTY_GRAPH_EXTRACTION_ERROR);
        return result;
      });
    } catch (error) {
      if (graphTransactionFailure(error)) throw error;
      const message = error instanceof Error ? error.message : String(error);
      logger.error("Graph extraction failed", { error: message });
      return {
        success: false, incomplete: true, terminalFailure: true, failureCode: "GRAPH_WORK_UNIT_FAILED",
        nodesAdded: progress.nodesAdded, edgesAdded: progress.edgesAdded, workUnits: progress.workUnits, error: message,
      };
    }
    const unitResult = await withGraphDelta(kv, async () => {
      const applied = await persistGraphDelta(kv, parsed.nodes, parsed.edges, []);
      await recordAudit(kv, "observe", "mem::graph-extract", sourceIds, {
        nodesExtracted: parsed.nodes.length, edgesExtracted: parsed.edges.length, project: page.project,
      }, undefined, undefined, `graph-extraction:${jobId}:provider:v3:${activeUnit!.id}`);
      return { nodesAdded: applied.newNodeCount, edgesAdded: applied.newEdgeCount };
    });
    const updated: ProjectGraphWorkProgress = {
      ...progress,
      pageIndex: next.complete ? progress.pageIndex + 1 : progress.pageIndex,
      cursor: next.complete ? null : next.cursor,
      nodesAdded: progress.nodesAdded + unitResult.nodesAdded,
      edgesAdded: progress.edgesAdded + unitResult.edgesAdded,
      workUnits: progress.workUnits + 1,
      activeUnit: undefined,
    };
    await writeGraphJobContinuation(kv, updated);
    if (updated.pageIndex >= updated.pageCount) return {
      success: true, nodesAdded: updated.nodesAdded, edgesAdded: updated.edgesAdded, workUnits: updated.workUnits,
    };
    return yieldGraphJob({ success: true, queued: true, ...updated });
  };
  const extract = async (request: ExtractionRequest, durableId?: string) => {
    const startMs = Date.now();
    try {
    const preflight = request.batchEffectKey === undefined
      ? undefined
      : createGraphBatchCallbackPreflight(request.batchEffectKey, (state) =>
        state === "stale"
          ? { success: true as const, stale: true as const, nodesAdded: 0, edgesAdded: 0 }
          : { success: true as const, nodesAdded: 0, edgesAdded: 0 },
      );
    const input = durableId || request.graphJobId || request.batchEffectKey
      ? request
      : { ...request, graphProvenanceVersion: 2 as const };
      const executionResult = await runGraphJob(kv, request.batchEffectKey ? "batch_callback" : "extraction", input, async (frozen) => {
    const captured = frozen as ExtractionRequest;
    const auditEffectKey = (phase: string): string | undefined => {
      const jobId = graphJobId();
      return jobId ? `graph-extraction:${jobId}:${phase}` : undefined;
    };
    const strictProvenance = captured.graphProvenanceVersion === 2;
    const useProjectWorkGroups = strictProvenance && captured.graphWorkGroupsVersion === 2 && !captured.batchResponse && !captured.deferred;
    const extractionSystem = strictProvenance
      ? GRAPH_EXTRACTION_SYSTEM_WITH_PROVENANCE
      : GRAPH_EXTRACTION_SYSTEM;
    let auditObservationIds: string[] = [];
    let yieldedProjectWork: GraphJobYield<unknown> | undefined;
    const callbackResult = await runBatchCallback(kv, "graph", captured.batchEffectKey, async (_resuming, admit) => {
      if (useProjectWorkGroups) {
        const savedProgress = await readGraphJobContinuation<ProjectGraphWorkProgress>(kv);
        if (savedProgress) {
          const result = await processProjectWorkStep(savedProgress);
          if (isGraphJobYield(result)) { yieldedProjectWork = result; return { success: false }; }
          return result;
        }
      }
      const data = { ...captured, observations: captured.observations ?? (captured.sessionId ? await freezeGraphSessionObservations(kv, captured.sessionId) : []) };
      auditObservationIds = data.observations.map((observation) => observation.id);
      if (!data.observations || data.observations.length === 0) {
        return { success: false, error: "No observations provided" };
      }
      const liveOptions = currentExtractionOptions();
      const options = await freezeGraphValue(kv, "options", () => liveOptions);
      if (!graphExtractionOptionsCompatible(options, liveOptions)) {
        throw new Error("Graph job configuration changed; restore the captured provider/compaction configuration before recovery");
      }
      if (data.batchResponse !== undefined && data.batchEffectKey) {
        const obsIds = data.observations.map((o) => o.id);
        const parsed = await freezeGraphValue(kv, "parsed-batch", () => parseGraphExtractionResponse(data.batchResponse, obsIds, strictProvenance));
        if (!parsed) {
          return { success: false, error: EMPTY_GRAPH_EXTRACTION_ERROR };
        }
        await admit();
        const added = await applyBatchGraph(kv, parsed, strictProvenance ? [] : obsIds, data.batchEffectKey);
        await recordAudit(kv, "observe", "mem::graph-extract", obsIds, {}, undefined, undefined, data.batchEffectKey).catch(() => {});
        return { success: true, ...added };
      }
      if ((await kv.get<GraphSnapshot>(KV.graphSnapshot, SNAPSHOT_KEY))?.batchInProgress) {
        return { success: false, error: "A batch graph application must be recovered first" };
      }

      const heuristic = await freezeGraphValue(kv, "heuristic", () => extractGraphHeuristics(data.observations));
      let heuristicNodesAdded = 0;
      let heuristicEdgesAdded = 0;
      if (heuristic.nodes.length || heuristic.edges.length) {
        const persistHeuristic = () => persistGraphDelta(kv, heuristic.nodes, heuristic.edges, strictProvenance ? [] : data.observations!.map((o) => o.id));
        const persisted = useProjectWorkGroups
          ? await freezeGraphValue(kv, "heuristic-persisted:v2", persistHeuristic)
          : await persistHeuristic();
        heuristicNodesAdded = persisted.newNodeCount;
        heuristicEdgesAdded = persisted.newEdgeCount;
        await recordAudit(kv, "observe", "mem::graph-extract", data.observations.map((o) => o.id), {
          nodesExtracted: heuristic.nodes.length, edgesExtracted: heuristic.edges.length, structural: true,
        }, undefined, undefined, auditEffectKey("heuristic"));
      }
      if (!data.batchResponse && (!options.graphEnabled ||
        (!options.llmEnabled && !options.auxiliary && !(data.deferred && options.batchQueue)))) {
        return { success: true, nodesAdded: heuristicNodesAdded, edgesAdded: heuristicEdgesAdded };
      }

      let observationsForGraph = data.observations;
      if (!data.force && data.batchResponse === undefined && !data.batchEffectKey && !data.replacementOf) {
        if (graphUnknownCompactionRepeatProvider()) {
          const typeSafeConfig = getTypeSafeConfig();
          graphUnknownCompactionRepeatProvider({ typeSafe: Boolean(options.typeSafe && typeSafe), compaction: options.typeSafeCompaction,
            runtimeEnabled: typeSafeConfig.enabled && typeSafeConfig.features.compaction && Boolean(typeSafeConfig.apiKey), timeoutMs: typeSafeConfig.timeoutMs,
            maxStateChars: options.typeSafeMaxStateChars, runtimeMaxStateChars: typeSafeConfig.maxStateChars });
        }
        observationsForGraph = await freezeGraphValue(kv, "compaction", () => compactGraphObservations(observationsForGraph, options.typeSafe ? typeSafe : undefined, options));
        if (observationsForGraph.length === 0) {
          return {
            success: true,
            skipped: true,
            nodesAdded: heuristicNodesAdded,
            edgesAdded: heuristicEdgesAdded,
            reason: "TypeSafe compaction removed only empty routine notifications",
            observationsRetained: true,
          };
        }
      }

      if (
        options.typeSafe && typeSafe &&
        data.deferred &&
        !data.force &&
        data.batchResponse === undefined &&
        !data.batchEffectKey &&
        !data.replacementOf &&
        !observationsForGraph.some((observation) =>
          isProtectedGraphObservation(observation) ||
          observation.type === "command_run" ||
          observation.importance >= 8 ||
          observation.modality === "image" ||
          observation.modality === "mixed" ||
          hasProtectedGraphSignal(`${observation.title} ${observation.narrative} ${observation.facts.join(" ")}`),
        )
      ) {
        try {
          const decision = await freezeGraphHandledValue(kv, "pipeline-gate", () => typeSafe!.evaluateChoice(
            "pipelineGates",
            stripPrivateData(JSON.stringify({
              workflow: "graph-extraction",
              observationCount: observationsForGraph.length,
              observationTypes: [...new Set(observationsForGraph.map((observation) => observation.type))],
              samples: observationsForGraph.slice(0, 3).map((observation) => ({
                title: observation.title.slice(0, 32),
                concepts: observation.concepts.slice(0, 3).map((concept) => concept.slice(0, 16)),
                facts: observation.facts.length,
                preview: observation.narrative.slice(0, 20),
              })),
            })).slice(0, 512),
            "Should this scheduled graph extraction run?",
            {
              run: "Run if the observations may add useful entities or relationships to the graph.",
              skip: "Skip only when the batch is clearly routine and unlikely to add any durable graph information.",
            },
          ));
          if (decision?.choice === "skip" && decision.confidence >= TYPESAFE_GRAPH_GATE_CONFIDENCE_THRESHOLD) {
            return {
              success: true,
              skipped: true,
              nodesAdded: heuristicNodesAdded,
              edgesAdded: heuristicEdgesAdded,
              reason: "TypeSafe pipeline gate",
              observationsRetained: true,
            };
          }
        } catch (error) {
          if (graphTransactionFailure(error)) throw error;
          logger.warn("TypeSafe graph pipeline gate failed open", {
            errorType: error instanceof Error ? error.name : "unknown",
          });
        }
      }

      let inputUnits: GraphExtractionUnit[] = [];
      if (!useProjectWorkGroups) {
        inputUnits = await freezeGraphInputUnits(kv, options.localCompactor, async () => data.batchResponse
          ? [{
            sourceObservations: observationsForGraph,
            promptObservations: observationsForGraph.map(toGraphPromptObservation),
          } satisfies GraphExtractionUnit]
          : await prepareGraphExtractionInputs(
            observationsForGraph,
            data.deferred ? Math.min(8000, options.targetChars) : options.targetChars,
            data.replacementOf || !options.localCompactor ? undefined : localCompactor,
            strictProvenance,
          ));
      }

      let totalNodesAdded = heuristicNodesAdded;
      let totalEdgesAdded = heuristicEdgesAdded;
      const workItemIds: string[] = [];
      try {
        if (useProjectWorkGroups) {
          let pageCount: number;
          try {
            pageCount = await captureProjectGraphWorkPages(kv, observationsForGraph, options.targetChars);
          } catch (error) {
            if (error instanceof Error && error.message === "GRAPH_PROJECT_SCOPE_UNAVAILABLE") return {
              success: false, partial: true, reason: "waiting_scope", error: "Graph extraction requires a verified source project for every observation.",
            };
            if (error instanceof Error && error.message === "GRAPH_PROJECT_SCOPE_MISMATCH") return {
              success: false, partial: true, reason: "project_scope_mismatch", error: "An observation has conflicting project authority.",
            };
            throw error;
          }
          const progress: ProjectGraphWorkProgress = {
            version: 2, pageCount, pageIndex: 0, cursor: null,
            nodesAdded: heuristicNodesAdded, edgesAdded: heuristicEdgesAdded, workUnits: 0,
            extractionSystem, envelopeSuffix: process.env.AGENTMEMORY_LLM_NOTHINK === "1" ? "\n/no_think" : "",
            options,
          };
          await writeGraphJobContinuation(kv, progress);
          const result = await processProjectWorkStep(progress);
          if (isGraphJobYield(result)) { yieldedProjectWork = result; return { success: false }; }
          return result;
        }
        if (!data.batchResponse && data.deferred && options.batchQueue && batchQueue) {
          for (const unit of inputUnits) {
            const prompt = buildGraphExtractionPrompt(unit.promptObservations);
            if (prompt.length > 10000) return {
              success: false, queuedWorkItemIds: workItemIds,
              nodesAdded: heuristicNodesAdded, edgesAdded: heuristicEdgesAdded,
              error: "Graph source exceeds the bounded batch input; source retained locally",
            };
            const queued = await freezeGraphHandledValue(kv, `queued:${workItemIds.length}`, () => batchQueue.enqueue({
              replacementOf: data.replacementOf,
              correlationId: generateId("fwbgraph"),
              task: "graph_extraction",
              systemPrompt: extractionSystem,
              userPrompt: prompt,
              metadata: { observations: JSON.stringify(unit.sourceObservations), sourceFingerprint: fingerprintId("fwbgraphsrc", JSON.stringify(unit.sourceObservations)), ...(strictProvenance ? { graphProvenanceVersion: "2" } : {}) },
            }));
            if (!queued.queued || !queued.workItemId) {
              logger.warn("Graph extraction batch unit was not queued; retaining source locally", {
                queuedUnits: workItemIds.length,
                totalUnits: inputUnits.length,
                reason: queued.reason,
    });
              return {
                success: false,
                deferred: true,
                queued: false,
                queuedWorkItemIds: workItemIds,
                nodesAdded: heuristicNodesAdded,
                edgesAdded: heuristicEdgesAdded,
                error: queued.reason ?? "Graph extraction batch queue rejected a complete unit",
              };
            }
            workItemIds.push(queued.workItemId);
          }
          return {
            success: true,
            queued: true,
            nodesAdded: heuristicNodesAdded,
            edgesAdded: heuristicEdgesAdded,
            workItemId: workItemIds[0],
            workItemIds,
          };
        }

        if (data.batchResponse && inputUnits.length !== 1) {
          return {
            success: false,
            error: "Batch response requires exactly one graph input unit",
          };
        }
        for (const [unitOrdinal, unit] of inputUnits.entries()) {
          const prompt = buildGraphExtractionPrompt(unit.promptObservations);
          const response = await freezeGraphHandledValue(kv, "provider-response", async () => {
            const strict = graphStrictProviderResponse();
            if (strict) return withStrictGraphResponse(strict.provider, strict.admit, () => provider.compress(extractionSystem, prompt, { task: "graph_extraction" }));
            return data.batchResponse ?? (llmRouter
            ? await llmRouter.run(
              "graph_extraction",
              (selectedProvider) => selectedProvider.compress(
                extractionSystem,
                prompt,
              ),
              (candidate) => {
                return parseGraphExtractionResponse(
                  candidate,
                  unit.sourceObservations.map((o) => o.id),
                  strictProvenance,
                ) !== null;
              },
            )
            : provider.compress(extractionSystem, prompt));
          });

          const obsIds = unit.sourceObservations.map((o) => o.id);
          const parsed = await freezeGraphHandledValue(kv, "parsed-response", () => parseGraphExtractionResponse(response, obsIds, strictProvenance));
          if (!parsed) {
            throw new Error(EMPTY_GRAPH_EXTRACTION_ERROR);
          }
          const { nodes, edges } = parsed;

          const { newNodeCount, newEdgeCount } = await persistGraphDelta(kv, nodes, edges, strictProvenance ? [] : obsIds);
        await recordAudit(kv, "observe", "mem::graph-extract", obsIds, {
          nodesExtracted: nodes.length,
          edgesExtracted: edges.length,
        }, undefined, undefined, auditEffectKey(`provider:${unitOrdinal}`));

        logger.info("Graph extraction complete", {
          nodes: nodes.length,
          edges: edges.length,
          newNodes: newNodeCount,
          newEdges: newEdgeCount,
          llm: true,
        });
          totalNodesAdded += newNodeCount;
          totalEdgesAdded += newEdgeCount;
        }
        return {
          success: true,
          nodesAdded: totalNodesAdded,
          edgesAdded: totalEdgesAdded,
        };
      } catch (err) {
        if (graphTransactionFailure(err)) throw err;
        const msg = err instanceof Error ? err.message : String(err);
        logger.error("Graph extraction failed", { error: msg });
        return heuristic.nodes.length || heuristic.edges.length || totalNodesAdded || totalEdgesAdded
          ? { success: true, nodesAdded: totalNodesAdded, edgesAdded: totalEdgesAdded, queuedWorkItemIds: workItemIds, llmError: msg }
          : { success: false, queuedWorkItemIds: workItemIds, error: msg };
      }
    }, () => recordAudit(kv, "observe", "mem::graph-extract", auditObservationIds, {}, undefined, undefined, captured.batchEffectKey));
    return yieldedProjectWork ?? callbackResult;
    }, durableId ?? request.graphJobId ?? (request.batchEffectKey ? `graphcallback:${request.batchEffectKey}` : undefined), preflight);
      const yielded = isGraphJobYield(executionResult);
      const progress = yielded && executionResult.progress && typeof executionResult.progress === "object"
        ? executionResult.progress as Record<string, unknown> : undefined;
      const result = yielded
        ? { ...progress, graphContinuation: true, queued: true }
        : executionResult;
      const outcome = result && typeof result === "object" ? result as Record<string, unknown> : {};
      const counts: RecentOperation["counts"] = {};
      if (typeof outcome.nodesAdded === "number") counts.nodesAdded = outcome.nodesAdded;
      if (typeof outcome.edgesAdded === "number") counts.edgesAdded = outcome.edgesAdded;
      if (Array.isArray(outcome.workItemIds)) counts.unitsQueued = outcome.workItemIds.length;
      if (Array.isArray(outcome.queuedWorkItemIds)) counts.unitsQueued = outcome.queuedWorkItemIds.length;
      const madeProgress = Object.values(counts).some(count => count > 0);
      let status: RecentOperation["outcome"] = "completed";
      if (outcome.success !== true) status = madeProgress ? "partial" : "failed";
      else if (outcome.llmError) status = "partial";
      else if (outcome.queued === true) status = "queued";
      else if (outcome.skipped === true || outcome.stale === true) status = "skipped";
      await recordOperation(kv, "mem::graph-extract", status, counts, undefined, {
        durationMs: Date.now() - startMs,
        ...((outcome.success !== true || outcome.llmError) ? { errorCode: "graph_extraction_failed" } : {}),
      });
      return result;
    } catch (error) {
      await recordOperation(kv, "mem::graph-extract", "failed", {}, undefined, {
        durationMs: Date.now() - startMs, errorCode: "graph_extraction_failed",
      });
      throw error;
    }
  };
  const intake = registerGraphDispatch(sdk, kv, extract);
  sdk.registerFunction("mem::graph-extract", (input: ExtractionRequest) => {
    if (input?.graphWorkGroupsVersion !== undefined && input.graphWorkGroupsVersion !== 2) throw new Error("Unsupported graph work-group version");
    const request = input && !input.graphJobId && !input.batchEffectKey && input.batchResponse === undefined
      ? { ...input, graphWorkGroupsVersion: 2 as const } : input;
    return intake(request);
  });
  registerGraphJobHandler(kv, "extraction", (data, id) => extract(data as ExtractionRequest, id));
  registerGraphJobHandler(kv, "batch_callback", (data, id) => extract(data as ExtractionRequest, id));
  registerGraphJobHandler(kv, "import", (input, id) => runGraphJob(kv, "import", input, async (frozen) => {
    const data = frozen as { nodes: GraphNode[]; edges: GraphEdge[]; obsIds: string[] };
    return withGraphDelta(kv, () => persistGraphDeltaValues(kv, data.nodes, data.edges, data.obsIds));
  }, id));

  // #753: every branch now applies a default cap and reports the
  // unbounded `total*` counts. Before this change, an unfiltered POST
  // /graph/query body (`{}`) on a corpus with ~10k+ nodes serialized
  // to a payload large enough that the iii state response channel
  // rejected it with HTTP 500 "Invocation stopped", leaving the viewer
  // graph tab silently blank.
  sdk.registerFunction("mem::graph-query",
    async (data: {
      display?: boolean;
      prepare?: boolean;
      cursor?: string;
      startNodeId?: string;
      nodeType?: string;
      project?: string;
      maxDepth?: number;
      query?: string;
      limit?: number;
      offset?: number;
    }): Promise<GraphQueryResult | import("../types.js").GraphDisplayPage | import("../types.js").GraphDisplayPreparation> => {
      if (data.display !== undefined && typeof data.display !== "boolean" ||
        data.prepare !== undefined && (typeof data.prepare !== "boolean" || data.display !== true) ||
        data.cursor !== undefined && data.display !== true) {
        throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Invalid graph display request.");
      }
      if (data.display === true) {
        const project = typeof data.project === "string" ? data.project.trim() : "";
        if (!project || typeof data.project !== "string" || Buffer.byteLength(data.project) > 256 ||
          [data.startNodeId, data.query, data.nodeType, data.offset, data.maxDepth].some(value => value !== undefined)) {
          throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Display browsing requires an exact project without walk or offset selectors.");
        }
        if (data.cursor !== undefined && (typeof data.cursor !== "string" || !data.cursor || Buffer.byteLength(data.cursor) > 4096)) {
          throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Invalid graph display cursor.");
        }
        if (data.limit !== undefined && (!Number.isInteger(data.limit) || data.limit < 1 || data.limit > 128)) {
          throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Invalid graph display page limit.");
        }
        if (data.prepare === true) {
          if (data.cursor !== undefined || data.limit !== undefined) {
            throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Graph Browse preparation does not accept pagination selectors.");
          }
          const result: import("../types.js").GraphDisplayPreparation = {
            display: true, project, preparation: await prepareGraphBrowse(kv, project),
          };
          if (Buffer.byteLength(JSON.stringify(result), "utf8") > 64 * 1024) {
            throw new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "Graph preparation response exceeded its byte limit.");
          }
          return result;
        }
        const page = await readScopedGraphPage<import("../types.js").GraphDisplayNode>(kv, {
          project, mode: "browse", limit: data.limit ?? 128, cursor: data.cursor,
          max_bytes: 64 * 1024 - 256,
        });
        if (page.status !== "ready" && page.status !== "preparing" && page.status !== "error" ||
          page.revision !== null && typeof page.revision !== "string") {
          throw new IndexedRetrievalError("STATE_TX_INVALID_RESPONSE", "Native graph Browse returned an unsupported status.");
        }
        const result: import("../types.js").GraphDisplayPage = {
          display: true, project,
          nodes: page.items.map(({ id, type, name }) => ({ id, type, name })),
          edges: (page.edges ?? []).map(({ id, type, sourceNodeId, targetNodeId, weight }) => ({ id, type, sourceNodeId, targetNodeId, weight })),
          status: page.status, ...(page.reason ? { reason: page.reason } : {}),
          revision: page.revision, nextCursor: page.next_cursor ?? null, hasMore: page.has_more,
          edgeScope: "page", crossPageEdges: "not_loaded",
          edgesTruncated: page.edgesTruncated === true,
          ...(page.progress ? { progress: page.progress } : {}),
        };
        if (Buffer.byteLength(JSON.stringify(result), "utf8") > 64 * 1024) {
          throw new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "Graph display response exceeded its byte limit.");
        }
        return result;
      }
      const maxDepth = Math.min(data.maxDepth || 3, 5);
      const requested = resolvePagination(data.limit, data.offset);
      const limit = Math.min(requested.limit, 16);
      const offset = requested.offset;
      const offsetLimit = 128;

      // Project membership is applied before native candidates are hydrated.
      const noWalk = !data.query && !data.startNodeId;
      if (noWalk) {
        const project = data.project?.trim();
        if (project && offset > offsetLimit) return {
          nodes: [], edges: [], depth: 0, totalNodes: 0, totalEdges: 0,
          truncated: true, limit, offset,
          warning: "Project graph offset exceeds the bounded native cursor window.",
        };
        if (project) {
          const nodes: GraphNode[] = [];
          let cursor: string | undefined;
          let remainingOffset = offset;
          let partial = requested.limit > 16;
          let reason: string | undefined;
          let hasMore = false;
          for (let pageIndex = 0; pageIndex < 9 && nodes.length < limit; pageIndex++) {
            const page = await readScopedGraphPage<{ id?: string; key?: string; value?: GraphNode }>(kv, {
              project, mode: "top_n", limit: 16, top_n: 16,
              ...(cursor ? { cursor } : {}),
            });
            reason ??= page.reason;
            if (page.status !== "ready") partial = true;
            const ids = page.items.map(scopedGraphItemId).filter((id): id is string => Boolean(id));
            const hydrated = await hydrateScopedGraphNodes(kv, ids);
            if (hydrated.incomplete || hydrated.nodes.length !== ids.length) partial = true;
            const selected = data.nodeType
              ? hydrated.nodes.filter((node) => node.type === data.nodeType)
              : hydrated.nodes;
            if (remainingOffset > 0) {
              const skipped = Math.min(remainingOffset, selected.length);
              remainingOffset -= skipped;
              selected.splice(0, skipped);
            }
            nodes.push(...selected.slice(0, limit - nodes.length));
            if (!page.has_more || !page.next_cursor) {
              if (remainingOffset > 0) partial = true;
              break;
            }
            hasMore = true;
            cursor = page.next_cursor;
            if (pageIndex === 8) partial = true;
          }
          if (hasMore && nodes.length >= limit) partial = true;
          const edgePage = await readScopedGraphEdges(kv, project, nodes.map((node) => node.id));
          partial ||= edgePage.partial;
          reason ??= edgePage.reason;
          return {
            nodes, edges: edgePage.edges, depth: 0,
            totalNodes: nodes.length, totalEdges: edgePage.edges.length,
            truncated: partial, limit, offset,
            ...(partial ? {
              warning: scopedGraphWarning(reason, "Project graph page is bounded and may be incomplete"),
            } : {}),
          };
        }
        return {
          nodes: [], edges: [], depth: 0, totalNodes: 0, totalEdges: 0,
          truncated: true, limit, offset,
          warning: "Project scope is required for graph browse; no global graph or snapshot read was run.",
        };
      }

      const project = data.project?.trim();
      if (!project) {
        return {
          nodes: [], edges: [], depth: 0, totalNodes: 0, totalEdges: 0,
          truncated: true, limit, offset,
          warning: "Project scope is required for graph search and traversal; no global graph query was run.",
        };
      }

      if (data.query) {
        if (Buffer.byteLength(data.query, "utf8") > 512) {
          return { nodes: [], edges: [], depth: 0, totalNodes: 0, totalEdges: 0,
            truncated: true, limit, offset, warning: "Graph query exceeds 512 UTF-8 bytes." };
        }
        if (offset > offsetLimit) return {
          nodes: [], edges: [], depth: 0, totalNodes: 0, totalEdges: 0,
          truncated: true, limit, offset,
          warning: "Graph search offset exceeds the bounded native cursor window.",
        };
        const nodes: GraphNode[] = [];
        let cursor: string | undefined;
        let remainingOffset = offset;
        let partial = requested.limit > 16;
        let reason: string | undefined;
        let hasMore = false;
        for (let pageIndex = 0; pageIndex < 9 && nodes.length < limit; pageIndex++) {
          const page = await readScopedGraphPage<{ id?: string; key?: string; entity?: boolean; observation?: boolean }>(kv, {
            project, mode: "search", query: data.query, limit: 16, max_bytes: 64 * 1024,
            ...(cursor ? { cursor } : {}),
          });
          reason ??= page.reason;
          if (page.status !== "ready") partial = true;
          const ids = page.items.map(scopedGraphItemId).filter((id): id is string => Boolean(id));
          const hydrated = await hydrateScopedGraphNodes(kv, ids);
          if (hydrated.incomplete) partial = true;
          const selected = data.nodeType
            ? hydrated.nodes.filter((node) => node.type === data.nodeType)
            : hydrated.nodes;
          if (selected.length !== ids.length) partial = true;
          if (remainingOffset > 0) {
            const skipped = Math.min(remainingOffset, selected.length);
            remainingOffset -= skipped;
            selected.splice(0, skipped);
          }
          nodes.push(...selected.slice(0, limit - nodes.length));
          if (!page.has_more || !page.next_cursor) {
            if (remainingOffset > 0) partial = true;
            break;
          }
          hasMore = true;
          cursor = page.next_cursor;
          if (pageIndex === 8) partial = true;
        }
        if (hasMore && nodes.length >= limit) partial = true;
        const edgePage = await readScopedGraphEdges(kv, project, nodes.map((node) => node.id));
        partial ||= edgePage.partial;
        reason ??= edgePage.reason;
        return {
          nodes, edges: edgePage.edges, depth: 0, totalNodes: nodes.length, totalEdges: edgePage.edges.length,
          truncated: partial, limit, offset,
          ...(partial ? { warning: scopedGraphWarning(reason, "Project graph search is partial") } : {}),
        };
      }

      if (data.startNodeId) {
        const proof = await readScopedGraphPage<never>(kv, {
          project, mode: "aggregate", node_ids: [data.startNodeId], limit: 1,
        });
        const proofCount = Number((proof.aggregate as { node_count?: unknown } | undefined)?.node_count);
        if (proof.status !== "ready" || proofCount !== 1) return {
          nodes: [], edges: [], depth: 0, totalNodes: 0, totalEdges: 0,
          truncated: true, limit, offset,
          warning: scopedGraphWarning(proof.reason ?? "project_authority_incomplete",
            "Starting node is not proven in the requested project"),
        };
        const visited = new Set<string>();
        const visitedEdges = new Set<string>();
        const resultNodes: GraphNode[] = [];
        const resultEdges: GraphEdge[] = [];
        let frontier = [{ nodeId: data.startNodeId, depth: 0 }];
        let partial = requested.limit > 16 || offset > 0;
        let reason: string | undefined;
        while (frontier.length > 0 && resultNodes.length < limit && resultEdges.length < 16) {
          const nextFrontier: Array<{ nodeId: string; depth: number }> = [];
          for (let index = 0; index < frontier.length && resultNodes.length < limit && resultEdges.length < 16; index += 12) {
            const batch = frontier.slice(index, index + 12);
            const fresh = batch.filter(({ nodeId }) => !visited.has(nodeId));
            if (fresh.length === 0) continue;
            const { nodes: hydrated, incomplete } = await hydrateScopedGraphNodes(kv, fresh.map(({ nodeId }) => nodeId));
            if (incomplete) partial = true;
            for (const node of hydrated) {
              visited.add(node.id);
              if (!data.nodeType || node.type === data.nodeType) resultNodes.push(node);
            }
            const page = await readScopedGraphPage<{ id?: string; key?: string; value?: GraphEdge }>(kv, {
              project, mode: "neighbors", node_ids: fresh.map(({ nodeId }) => nodeId), limit: 16,
            });
            if (page.status !== "ready" || page.has_more) {
              partial = true;
              reason ??= page.reason;
            }
            for (const item of page.items) {
              const edge = scopedGraphEdge(item);
              if (!edge || edge.stale) continue;
              if (!visitedEdges.has(edge.id) && resultEdges.length < 16) {
                visitedEdges.add(edge.id);
                resultEdges.push(edge);
              }
              const current = fresh.find(({ nodeId }) => nodeId === edge.sourceNodeId || nodeId === edge.targetNodeId);
              if (!current) continue;
              const nextId = current.nodeId === edge.sourceNodeId ? edge.targetNodeId : edge.sourceNodeId;
              if (current.depth < maxDepth && !visited.has(nextId)) nextFrontier.push({ nodeId: nextId, depth: current.depth + 1 });
            }
            if (resultEdges.length >= 16) partial = true;
          }
          frontier = nextFrontier;
        }
        return {
          nodes: resultNodes.slice(0, limit), edges: resultEdges, depth: maxDepth,
          totalNodes: resultNodes.length, totalEdges: resultEdges.length,
          truncated: partial || frontier.length > 0, limit, offset,
          ...((partial || frontier.length > 0) ? { warning: scopedGraphWarning(reason, "Project graph traversal is partial") } : {}),
        };
      }

      return paginate([], [], 0, limit, offset);
    },
  );

  // Global counts come from identified historical metadata, never a graph traversal.
  sdk.registerFunction("mem::graph-stats", async (data?: { project?: string }) => {
    const project = typeof data?.project === "string" ? data.project.trim() : "";
    return readProjectGraphStatistics(kv, project || undefined, (selectedProject) => sdk.trigger({
      function_id: "mem::graph-query",
      payload: { project: selectedProject, maxDepth: 1, limit: GRAPH_STATISTICS_SAMPLE_LIMIT },
    }));
  });

  const rebuild = async (request?: { force?: boolean; project?: string }, durableId?: string) =>
    runGraphJob(kv, "rebuild", request ?? {}, async (input) => {
      const project = (input as { project?: string }).project?.trim();
      if (!project) return {
        success: false, partial: true, reason: "project_required",
        error: "Snapshot rebuild requires a project-scoped native index; global graph enumeration is disabled.",
      };
      const started = Date.now();
      try {
        const candidates = await readScopedGraphPage<{ id?: string; key?: string }>(kv, {
          project, mode: "top_n", top_n: 16, limit: 16,
        });
        const ids = candidates.items.map(scopedGraphItemId).filter((id): id is string => Boolean(id));
        if (candidates.status === "waiting_scope" || ids.length === 0) return {
          success: false, partial: true, project,
          reason: candidates.reason ?? "index_not_ready", sampledNodes: 0, sampledEdges: 0,
        };
        const { nodes, incomplete } = await hydrateScopedGraphNodes(kv, ids);
        const edges: GraphEdge[] = [];
        let partial = candidates.status !== "ready" || candidates.has_more || incomplete;
        let reason = candidates.reason;
        for (let offset = 0; offset < ids.length && edges.length < 16; offset += 12) {
          const page = await readScopedGraphPage<{ id?: string; key?: string; value?: GraphEdge }>(kv, {
            project, mode: "neighbors", node_ids: ids.slice(offset, offset + 12), limit: 16,
          });
          if (page.status !== "ready" || page.has_more) {
            partial = true;
            reason ??= page.reason;
          }
          for (const item of page.items) {
            const edge = scopedGraphEdge(item);
            if (!edge || edge.stale || !ids.includes(edge.sourceNodeId) || !ids.includes(edge.targetNodeId)) continue;
            if (!edges.some((existing) => existing.id === edge.id)) edges.push(edge);
            if (edges.length >= 16) { partial = true; break; }
          }
        }
        const sampleAggregate = { node_count: 0, edge_count: 0 };
        let aggregateAvailable = ids.length > 0;
        for (let offset = 0; offset < ids.length && aggregateAvailable; offset += 12) {
          const page = await readScopedGraphPage<never>(kv, {
            project, mode: "aggregate", node_ids: ids.slice(offset, offset + 12), limit: 1,
          }).catch(() => undefined);
          const counts = page?.aggregate as { node_count?: unknown; edge_count?: unknown } | undefined;
          const nodeCount = Number(counts?.node_count);
          const edgeCount = Number(counts?.edge_count ?? 0);
          if (page?.status !== "ready" || !Number.isSafeInteger(nodeCount) || nodeCount < 0 ||
            !Number.isSafeInteger(edgeCount) || edgeCount < 0) {
            aggregateAvailable = false;
            break;
          }
          sampleAggregate.node_count += nodeCount;
          sampleAggregate.edge_count += edgeCount;
        }
        partial ||= !aggregateAvailable || candidates.status !== "ready";
        return {
          success: true, partial: true, approximate: true,
          reason: reason ?? "candidate_top_n",
          project,
          sampledNodes: nodes.length,
          sampledEdges: edges.length,
          nodes,
          edges,
          ...(aggregateAvailable ? { aggregate: sampleAggregate } : {}),
          tookMs: Date.now() - started,
          ...(partial ? { warning: "Native project top_n is a bounded candidate sample; this did not rebuild a complete snapshot." } : {}),
        };
      } catch (err) {
        if (graphTransactionFailure(err)) throw err;
        return { success: false, partial: true, project, reason: "bounded_native_index_unavailable",
          error: err instanceof Error ? err.message : String(err) };
      }
    }, durableId);
  sdk.registerFunction("mem::graph-snapshot-rebuild", (input?: { force?: boolean; project?: string }) => rebuild(input));
  registerGraphJobHandler(kv, "rebuild", (input, id) => rebuild(input as { force?: boolean; project?: string }, id));

  // #814 v2 + #825: clean-restart escape hatch for corpora of any
  // size, including the legacy 75K+ case that crashes kv.list.
  //
  // Previous reset walked kv.list<GraphNode/Edge>(...) which is the
  // exact primitive that heartbeat-crashes the worker on the corpus
  // this reset was meant to recover (Allan's repro, 0.35s death).
  //
  // The new design is enumeration-free: write an empty snapshot and
  // return. The hot path (mem::graph-query empty-body, mem::graph-stats)
  // reads ONLY the snapshot post-#816, so a fresh empty snapshot
  // makes the graph behave as if it were empty for every read.
  //
  // Future extracts repopulate the snapshot + side-indexes
  // incrementally (graph-extract is O(1) per node post-#816 — it does
  // not consult the legacy rows).
  //
  // Trade-off: legacy rows in KV.graphNodes / KV.graphEdges remain on
  // disk as unreferenced orphans. They consume disk but are never
  // read by any post-#816 code path. Cleanup is deferred to a future
  // chunked-vacuum job; #816's broken vacuum-via-list strategy is
  // what we are leaving behind here.
  const reset = async (_input?: unknown, durableId?: string) => runGraphJob(kv, "reset", {}, async () => runBatchCallback(kv, "graph", undefined, () => withGraphDelta(kv, async () => {
    if ((await kv.get<GraphSnapshot>(KV.graphSnapshot, SNAPSHOT_KEY))?.batchInProgress) {
      return { success: false, error: "A batch graph application must be recovered first" };
    }
    const started = Date.now();
    // Stamp resetAt=now on the empty snapshot. Future
    // mem::graph-extract calls compare each name-index lookup's
    // existing node `createdAt` against this timestamp; anything
    // older counts as an orphan and is dropped from the merge path,
    // forcing extract to write a fresh row instead of reconnecting
    // to a pre-reset entry.
    const resetSnapshot: GraphSnapshot = {
      ...emptySnapshot(),
      resetAt: graphCapturedAt(),
    };
    await writeGraphSnapshot(kv, resetSnapshot);
    const counts: Record<string, number> = {
      [KV.graphSnapshot]: 1,
    };
    const tookMs = Date.now() - started;
    logger.info("Graph state reset", { counts, tookMs });
    return { success: true, cleared: counts, tookMs };
  }, true)), durableId);
  sdk.registerFunction("mem::graph-reset", () => reset());
  registerGraphJobHandler(kv, "reset", (input, id) => reset(input, id));
}
