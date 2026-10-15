import { createHash } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import type {
  GraphNode,
  GraphEdge,
  EdgeContext,
  TemporalState,
  MemoryProvider,
  TemporalWorkManifest,
  TemporalPendingIndex,
} from "../types.js";
import { KV, fingerprintId } from "../state/schema.js";
import { batchEffectKey, runBatchCallback, withBatchRecordLocks, preserveBatchProvenance } from "../state/batch-effects.js";
import type { StateKV } from "../state/kv.js";
import { logger } from "../logger.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { StatePageError } from "../state/state-pages.js";
import { isBackgroundRecoveryPaused } from "../config.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { freezeGraphValue, graphCapturedAt, graphKV, graphTransactionFailure, readCompletedGraphJobReplay,
  graphJobId, registerGraphJobHandler, runGraphJob } from "./graph-jobs.js";
import { GraphRetrievalReader, readScopedGraphPage } from "./graph-retrieval-read.js";
import { createNextGraphWorkUnit, GRAPH_WORK_LIMITS } from "./graph-work-groups.js";
import { validateSelectedGraphControl, validateSelectedGraphSources, selectedGraphGeneration,
  withSelectedGraphGeneration, type SelectedGraphControl } from "./graph-dispatch.js";
import { withLocalGraphRecovery, withSelectedGraphOrigin } from "./graph-jobs.js";
import { applySelectedTemporalGraph } from "./selected-temporal-apply.js";
import { parseTemporalGraphResult, type TemporalGraphResult } from "./temporal-result.js";

interface TemporalExtractionRequest {
  observations: Array<{ id: string; title: string; narrative: string; concepts: string[]; files: string[]; type: string; timestamp: string; sessionId?: string; project?: string }>;
  project?: string;
  graphWorkGroupsVersion?: 2;
  selected?: SelectedGraphControl;
}
type SelectedTemporalManifest = TemporalWorkManifest & {
  selectedSource?: { project: string; sessionId: string; sourceFingerprint: string };
  selectedReplay?: { checkpoint: string; unitId?: string };
  repairNodesAdded?: number;
  repairEdgesAdded?: number;
};

interface TemporalSourceAuthority { version: 1; sessionId: string; project: string }
interface TemporalWorkSource { id: string; version: string; content: string; authority?: TemporalSourceAuthority }
interface TemporalWorkPage { project: string; sources: TemporalWorkSource[] }
type TemporalGraphWorkUnit = NonNullable<ReturnType<typeof createNextGraphWorkUnit>["unit"]>;
interface TemporalUnitRequest {
  temporalUnitVersion: 2; jobId: string; pageIndex: number; project: string;
  workInput: {
    audience: { scope: "project"; project: string };
    sources: TemporalWorkSource[];
    systemPrompt: string; envelopePrefix: string; envelopeSuffix: string;
  };
  unit: TemporalGraphWorkUnit;
}

const TEMPORAL_TOPIC = "agentmemory.temporal.work";
const TEMPORAL_WORK_SCOPE = KV.temporalGraphWork;
const TEMPORAL_PENDING_SCOPE = KV.temporalGraphPending;
const TEMPORAL_PENDING_SCAN_KEY = "temporal-work:pending-scan";
const TEMPORAL_RESULT_CONTRACT = "temporal-result-v1";
const temporalManifestKey = (id: string) => `temporal-work:${id}:manifest`;
const temporalPageKey = (id: string, page: number) => `temporal-work:${id}:page:${page}`;
const temporalPendingScanKey = (jobId: string) => jobId;
const manifestCounts = (manifest: SelectedTemporalManifest) => ({
  nodesAdded: manifest.nodesAdded, edgesAdded: manifest.edgesAdded, workUnits: manifest.workUnits,
  repairNodesAdded: manifest.repairNodesAdded ?? 0, repairEdgesAdded: manifest.repairEdgesAdded ?? 0,
});

class TemporalScopeError extends Error {
  constructor(readonly reason: string, message: string) { super(message); }
}

async function temporalNode(kv: StateKV, node: GraphNode, project: string): Promise<GraphNode | undefined> {
  const page = await readScopedGraphPage<{ id?: string; key?: string }>(kv, {
    project, mode: "search", query: node.name, limit: 16, max_bytes: 64 * 1024,
  });
  if (page.status !== "ready" || page.has_more) throw new Error(page.reason ?? "Temporal graph project membership is incomplete");
  for (const item of page.items) {
    const id = typeof item.id === "string" ? item.id : item.key;
    if (!id) continue;
    const current = await kv.get<GraphNode>(KV.graphNodes, id);
    if (current && current.id === id && !current.stale && current.name === node.name && current.type === node.type) return current;
  }
  return undefined;
}

async function temporalPredecessor(kv: StateKV, edge: GraphEdge, index: number, plannedIds: string[], added: GraphEdge[], project: string): Promise<GraphEdge | undefined> {
  const plannedPredecessor = added.slice().reverse().find((candidate) =>
    candidate.id !== edge.id && candidate.isLatest !== false &&
    candidate.sourceNodeId === edge.sourceNodeId && candidate.targetNodeId === edge.targetNodeId && candidate.type === edge.type &&
    plannedIds.indexOf(candidate.id) < index);
  if (plannedPredecessor) return plannedPredecessor;

  const key = `${edge.sourceNodeId}|${edge.targetNodeId}|${edge.type}`;
  const existingId = await kv.get<string>(KV.graphEdgeKey, key);
  const indexed = existingId ? await kv.get<GraphEdge>(KV.graphEdges, existingId) : undefined;
  if (indexed && existingId) {
    const proof = await readScopedGraphPage<never>(kv, {
      project, mode: "aggregate", node_ids: [edge.sourceNodeId, edge.targetNodeId], edge_ids: [existingId], limit: 1,
    });
    const aggregate = proof.aggregate as { node_count?: unknown; edge_count?: unknown } | undefined;
    if (proof.status !== "ready") throw new Error(proof.reason ?? "Temporal edge project membership is incomplete");
    if (aggregate?.node_count !== 2 || aggregate.edge_count !== 1) return undefined;
  }
  return indexed && indexed.id !== edge.id && indexed.isLatest !== false ? indexed : undefined;
}

export const TEMPORAL_EXTRACTION_SYSTEM = `You are a temporal knowledge extraction engine. Given observations, extract entities AND their temporal relationships with full context metadata.

For each relationship, you MUST provide:
1. Semantic relation type
2. Temporal validity (when this fact became true in the real world)
3. Context metadata: WHY this relationship exists, what reasoning led to it, what alternatives were considered

Output EXACTLY this XML:
<temporal_graph>
  <entities>
    <entity type="file|function|concept|error|decision|pattern|library|person|project|preference|location|organization|event" name="exact name">
      <property key="key">value</property>
      <alias>alternate name</alias>
    </entity>
  </entities>
  <relationships>
    <relationship type="uses|imports|modifies|causes|fixes|depends_on|related_to|works_at|prefers|blocked_by|caused_by|optimizes_for|rejected|avoids|located_in|succeeded_by"
      source="entity name" target="entity name" weight="0.1-1.0"
      valid_from="ISO date or 'unknown'" valid_to="ISO date or 'current'">
      <reasoning>WHY this relationship exists</reasoning>
      <sentiment>positive|negative|neutral</sentiment>
      <alternatives>
        <alt>alternative that was considered</alt>
      </alternatives>
    </relationship>
  </relationships>
</temporal_graph>

Rules:
- NEVER overwrite existing relationships — always create new versioned edges
- Extract temporal validity from context clues ("since last month", "in 2024", "currently")
- Capture reasoning/motivation behind each relationship
- Weight relationships by directness: 1.0 = explicit statement, 0.5 = inferred, 0.1 = speculative`;

function parseTemporalGraphXml(xml: string, observationIds: string[]): TemporalGraphResult {
  return parseTemporalGraphResult(xml, observationIds, graphCapturedAt());
}

function* splitTemporalSource(value: string, maxBytes: number): Generator<string> {
  let part = "";
  let bytes = 0;
  for (const character of value) {
    const characterBytes = Buffer.byteLength(character, "utf8");
    if (part && bytes + characterBytes > maxBytes) {
      yield part;
      part = "";
      bytes = 0;
    }
    part += character;
    bytes += characterBytes;
  }
  if (part) yield part;
}

async function applyTemporalGraph(
  kv: StateKV,
  nodes: GraphNode[],
  edges: GraphEdge[],
  obsIds: string[],
  effectKey: string,
  project: string,
): Promise<{ success: boolean; nodesAdded?: number; edgesAdded?: number; error?: string }> {
  return runBatchCallback(kv, "graph", effectKey, async (_resuming, admit, receipt) => {
    const resultHash = batchEffectKey(JSON.stringify({ nodes, edges }));
    if (receipt?.resultHash && receipt.resultHash !== resultHash) throw new Error("Temporal extraction result changed during recovery");
    if ((await kv.get<{ batchInProgress?: string }>(KV.graphSnapshot, "current"))?.batchInProgress) throw new Error("A batch graph application must be recovered first");
    const now = receipt?.effectTimestamp ?? graphCapturedAt();
    await admit({ resultHash, effectTimestamp: now });
    const addedNodes: GraphNode[] = [];
    const addedEdges: GraphEdge[] = [];
    const plannedEdgeIds = edges.map((_, index) => fingerprintId("ge", `${effectKey}:${index}`));
    const idRemap = new Map<string, string>();
    for (const node of nodes) {
      const existing = await temporalNode(kv, node, project) ?? addedNodes.find((candidate) => candidate.name === node.name && candidate.type === node.type);
      const oldId = node.id;
      node.id = existing?.id ?? fingerprintId("gn", `temporal:${project}:${node.type}:${node.name}`);
      node.createdAt = now;
      const nameKey = `${node.type}|${node.name}`;
      await withBatchRecordLocks([[KV.graphNodes, node.id], [KV.graphNameIndex, nameKey]], async () => {
        const current = await kv.get<GraphNode>(KV.graphNodes, node.id);
        const merged = preserveBatchProvenance(current, current
          ? { ...current, properties: { ...current.properties, ...node.properties }, sourceObservationIds: obsIds, aliases: node.aliases, updatedAt: now }
          : node);
        await kv.set(KV.graphNodes, node.id, merged);
        await kv.set(KV.graphNameIndex, nameKey, node.id);
      });
      idRemap.set(oldId, node.id);
      if (!existing) addedNodes.push(node);
    }
    for (const [index, edge] of edges.entries()) {
      edge.id = plannedEdgeIds[index];
      edge.createdAt = now;
      edge.tcommit = now;
      if (idRemap.has(edge.sourceNodeId)) edge.sourceNodeId = idRemap.get(edge.sourceNodeId)!;
      if (idRemap.has(edge.targetNodeId)) edge.targetNodeId = idRemap.get(edge.targetNodeId)!;
      const existingEdge = await temporalPredecessor(kv, edge, index, plannedEdgeIds, addedEdges, project);
      if (existingEdge?.id === edge.id) {
        addedEdges.push(existingEdge);
        continue;
      }
      const edgeKey = `${edge.sourceNodeId}|${edge.targetNodeId}|${edge.type}`;
      const records: Array<[string, string]> = [[KV.graphEdges, edge.id]];
      records.push([KV.graphEdgeKey, edgeKey]);
      if (existingEdge) records.push([KV.graphEdges, existingEdge.id], [KV.graphEdgeHistory, existingEdge.id]);
      await withBatchRecordLocks(records, async () => {
        if (existingEdge) {
          const current = await kv.get<GraphEdge>(KV.graphEdges, existingEdge.id);
          if (!current) throw new Error("Temporal predecessor disappeared during recovery");
          const updatedOld = { ...current, isLatest: false, tvalidEnd: current.tvalidEnd || now, supersededBy: edge.id };
          await kv.set(KV.graphEdges, existingEdge.id, updatedOld);
          await kv.set(KV.graphEdgeHistory, existingEdge.id, updatedOld);
          edge.version = (current.version || 1) + 1;
        }
        const current = await kv.get<GraphEdge>(KV.graphEdges, edge.id);
        await kv.set(KV.graphEdges, edge.id, preserveBatchProvenance(current, edge));
        await kv.set(KV.graphEdgeKey, edgeKey, edge.id);
      });
      addedEdges.push(edge);
    }
    logger.info("Temporal graph extraction complete", { nodes: nodes.length, edges: edges.length });
    return { success: true, nodesAdded: nodes.length, edgesAdded: edges.length };
  });
}

type TemporalRepairOutcome =
  | { state: "not-needed" }
  | { state: "repaired"; childJobId: string; nodesAdded: number; edgesAdded: number }
  | { state: "recovery-needed"; reason: string };

interface TemporalReplayPlan {
  rawOrdinal: number;
  rawName: string;
  parsedOrdinal: number;
  parsedName: string;
  applyOrdinal: number;
  unitFingerprint: string;
  effectKey: string;
  project: string;
  sourceIds: string[];
  unit?: TemporalUnitRequest["unit"];
  pageSources?: TemporalWorkSource[];
  observations?: TemporalExtractionRequest["observations"];
}

function temporalObservationContent(observation: TemporalExtractionRequest["observations"][number]): string {
  return JSON.stringify({ id: observation.id, type: observation.type, timestamp: observation.timestamp,
    title: observation.title, narrative: observation.narrative, concepts: observation.concepts ?? [], files: observation.files ?? [] });
}

function temporalSourcePart(observation: TemporalExtractionRequest["observations"][number], version: string): string | undefined {
  const separator = version.lastIndexOf(":");
  if (separator < 0 || version.slice(0, separator) !== (observation.timestamp || "unknown")) return undefined;
  const partText = version.slice(separator + 1);
  if (!/^\d+$/.test(partText)) return undefined;
  const part = Number(partText);
  if (!Number.isSafeInteger(part) || part < 0) return undefined;
  let current = 0;
  for (const fragment of splitTemporalSource(temporalObservationContent(observation), GRAPH_WORK_LIMITS.maxPageBytes - 512)) {
    if (current++ === part) return fragment;
  }
  return undefined;
}

function fragmentMatchesSource(fragment: { sourceId: string; sourceVersion: string | number; sourceFingerprint: string; startByte: number; endByte: number; content: string }, source: TemporalWorkSource): boolean {
  if (fragment.sourceId !== source.id || fragment.sourceVersion !== source.version
    || fragment.sourceFingerprint !== fingerprintId("gws", JSON.stringify([source.id, source.version, source.content]))) return false;
  const bytes = Buffer.from(source.content, "utf8");
  if (!Number.isSafeInteger(fragment.startByte) || !Number.isSafeInteger(fragment.endByte)
    || fragment.startByte < 0 || fragment.endByte <= fragment.startByte || fragment.endByte > bytes.length) return false;
  const matchesBoundary = (offset: number) => offset === 0 || offset === bytes.length || (bytes[offset]! & 0xc0) !== 0x80;
  return matchesBoundary(fragment.startByte) && matchesBoundary(fragment.endByte)
    && bytes.subarray(fragment.startByte, fragment.endByte).toString("utf8") === fragment.content;
}

function observationMatchesRows(
  observation: TemporalExtractionRequest["observations"][number],
  rows: TemporalWorkSource[],
  fragments: TemporalGraphWorkUnit["fragments"],
): boolean {
  if (rows.some((source) => temporalSourcePart(observation, source.version) !== source.content)) return false;
  for (const fragment of fragments) {
    if (fragment.sourceId !== observation.id) continue;
    const source = rows.find((candidate) => candidate.version === fragment.sourceVersion);
    if (!source || !fragmentMatchesSource(fragment, source)) return false;
  }
  return true;
}

async function observationHasProjectAuthority(
  kv: StateKV,
  observation: TemporalExtractionRequest["observations"][number],
  project: string,
): Promise<boolean> {
  if (!observation.sessionId) return false;
  const [session, current] = await Promise.all([
    kv.get<{ project?: string }>(KV.sessions, observation.sessionId),
    kv.get<TemporalExtractionRequest["observations"][number]>(KV.observations(observation.sessionId), observation.id),
  ]);
  return session?.project?.trim() === project && current?.id === observation.id
    && (!current.sessionId || current.sessionId === observation.sessionId)
    && (!current.project || current.project === project)
    && temporalObservationContent(current) === temporalObservationContent(observation);
}

async function readTemporalSessionPage(kv: StateKV, cursor?: string) {
  const pages = kv.pages<{ id?: string; project?: string }>(KV.sessions, {
    ...(cursor ? { cursor } : {}), limit: 100, maxBytes: 32 * 1024,
  });
  try { return await pages.next(); }
  finally { await pages.return(undefined); }
}

async function findCurrentObservationForRows(
  kv: StateKV,
  project: string,
  sourceId: string,
  rows: TemporalWorkSource[],
  fragments: TemporalGraphWorkUnit["fragments"],
): Promise<TemporalExtractionRequest["observations"][number] | undefined> {
  let cursor: string | undefined;
  let scanned = 0;
  let match: TemporalExtractionRequest["observations"][number] | undefined;
  do {
    const page = await readTemporalSessionPage(kv, cursor);
    if (page.done || !page.value) return match;
    for (const session of page.value.items) {
      if (++scanned > 10_000) return undefined;
      if (session.id !== undefined && session.project?.trim() === project) {
        const current = await kv.get<TemporalExtractionRequest["observations"][number]>(KV.observations(session.id), sourceId);
        if (current?.id === sourceId && (!current.sessionId || current.sessionId === session.id)
          && (!current.project || current.project === project) && observationMatchesRows(current, rows, fragments)) {
          if (match) return undefined;
          match = current;
        }
      }
    }
    if (page.done) break;
    cursor = page.value.next_cursor ?? undefined;
  } while (cursor);
  return match;
}

async function validateTemporalUnitProvenance(
  kv: StateKV,
  plan: TemporalReplayPlan,
  unit: TemporalGraphWorkUnit,
): Promise<boolean> {
  const fragmentIds = [...new Set(unit.fragments.map((fragment) => fragment.sourceId))];
  if (!fragmentIds.length || fragmentIds.some((id) => !plan.sourceIds.includes(id))) return false;

  for (const sourceId of fragmentIds) {
    let rows: TemporalWorkSource[];
    let observation: TemporalExtractionRequest["observations"][number] | undefined;
    if (plan.pageSources) {
      rows = plan.pageSources.filter((source) => source.id === sourceId);
      if (!rows.length) return false;
      const requiredFragments = unit.fragments.filter((fragment) => fragment.sourceId === sourceId);
      if (requiredFragments.some((fragment) => !rows.some((source) => fragmentMatchesSource(fragment, source)))) return false;
      if (rows.some((source) => source.authority !== undefined && (!source.authority || typeof source.authority !== "object"))) return false;
      const authorities = rows.map((source) => source.authority).filter((authority): authority is TemporalSourceAuthority => authority !== undefined);
      if (authorities.length && (authorities.length !== rows.length || authorities.some((authority) => authority.version !== 1
        || authority.project !== plan.project || authority.sessionId !== authorities[0]!.sessionId))) return false;
      if (authorities.length) {
        const authority = authorities[0]!;
        const [session, current] = await Promise.all([
          kv.get<{ project?: string }>(KV.sessions, authority.sessionId),
          kv.get<TemporalExtractionRequest["observations"][number]>(KV.observations(authority.sessionId), sourceId),
        ]);
        if (session?.project?.trim() !== plan.project || current?.id !== sourceId
          || current.sessionId !== authority.sessionId || current.project && current.project !== plan.project
          || !observationMatchesRows(current, rows, unit.fragments)) return false;
        observation = current;
      } else observation = await findCurrentObservationForRows(kv, plan.project, sourceId, rows, unit.fragments);
    } else if (plan.observations) {
      const captured = plan.observations.filter((candidate) => candidate.id === sourceId);
      const candidates = captured.filter((candidate) => {
        const candidateRows: TemporalWorkSource[] = [...splitTemporalSource(temporalObservationContent(candidate), GRAPH_WORK_LIMITS.maxPageBytes - 512)]
          .map((content, part) => ({ id: candidate.id, version: `${candidate.timestamp || "unknown"}:${part}`, content }));
        return observationMatchesRows(candidate, candidateRows, unit.fragments);
      });
      if (candidates.length !== 1 || !await observationHasProjectAuthority(kv, candidates[0]!, plan.project)) return false;
      observation = candidates[0];
    } else return false;
    if (!observation) return false;
  }
  return true;
}

function cachedLegacyEmpty(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  const parsed = value as { kind?: unknown; nodes?: unknown; edges?: unknown };
  return parsed.kind === undefined && Array.isArray(parsed.nodes) && parsed.nodes.length === 0
    && Array.isArray(parsed.edges) && parsed.edges.length === 0
    && Object.keys(value).length === 2 && Object.hasOwn(value, "nodes") && Object.hasOwn(value, "edges");
}

function temporalRecoveryNeeded(reason: string): TemporalRepairOutcome {
  return { state: "recovery-needed", reason };
}

async function repairLegacyTemporalResult(kv: StateKV, parentJobId: string, plan: TemporalReplayPlan): Promise<TemporalRepairOutcome> {
  let replay;
  try {
    replay = await readCompletedGraphJobReplay(kv, {
      jobId: parentJobId,
      values: [{ ordinal: plan.rawOrdinal, name: plan.rawName }, { ordinal: plan.parsedOrdinal, name: plan.parsedName }],
      deltas: [plan.applyOrdinal], callbackKeys: [plan.effectKey],
    });
  } catch (error) {
    return temporalRecoveryNeeded(error instanceof Error ? error.name : "completed_replay_unavailable");
  }
  const raw = replay.values.find((value) => value.ordinal === plan.rawOrdinal && value.name === plan.rawName)?.value;
  const parsed = replay.values.find((value) => value.ordinal === plan.parsedOrdinal && value.name === plan.parsedName)?.value;
  const applied = replay.deltas.find((delta) => delta.ordinal === plan.applyOrdinal)?.result as { success?: unknown; nodesAdded?: unknown; edgesAdded?: unknown } | undefined;
  const callback = replay.callbacks.find((item) => item.key === plan.effectKey)?.receipt as { state?: unknown } | null;
  if (typeof raw !== "string") return temporalRecoveryNeeded("captured_temporal_response_invalid");
  if (!cachedLegacyEmpty(parsed)) {
    if (parsed && typeof parsed === "object" && ((parsed as { kind?: unknown }).kind === "valid-empty"
      || (parsed as { kind?: unknown }).kind === "valid-result")) return { state: "not-needed" };
    return temporalRecoveryNeeded("historical_temporal_parse_unsupported");
  }
  if (applied?.success !== true || applied.nodesAdded !== 0 || applied.edgesAdded !== 0
    || callback?.state !== "completed") {
    return temporalRecoveryNeeded("historical_temporal_effect_unverified");
  }
  try {
    if (!await validateTemporalPlanProvenance(kv, plan)) return temporalRecoveryNeeded("historical_temporal_source_unverified");
  } catch {
    return temporalRecoveryNeeded("historical_temporal_source_unverified");
  }

  const rawDigest = createHash("sha256").update(JSON.stringify({ value: raw })).digest("hex");
  const childJobId = `graphjob_${fingerprintId("temporalrepair", JSON.stringify([
    parentJobId, plan.unitFingerprint, rawDigest, "temporal-result-v1",
  ])).slice("temporalrepair_".length)}`;
  let child: { success?: unknown; repairState?: unknown; nodesAdded?: unknown; edgesAdded?: unknown };
  try {
    child = await withLocalGraphRecovery(true, () => runGraphJob(kv, "temporal", {
      temporalRepairVersion: 1, parentJobId, unitFingerprint: plan.unitFingerprint, rawDigest, raw,
      project: plan.project, sourceIds: plan.sourceIds, effectKey: batchEffectKey(`temporal-repair:${childJobId}:${rawDigest}`),
    }, async (frozen) => {
      const input = frozen as { temporalRepairVersion: number; raw: string; project: string; sourceIds: string[]; effectKey: string };
      if (input.temporalRepairVersion !== 1 || input.raw !== raw || input.project !== plan.project
        || JSON.stringify(input.sourceIds) !== JSON.stringify(plan.sourceIds)) throw new Error("temporal_repair_capture_conflict");
      const cached = await freezeGraphValue(graphKV(kv), "temporal-repair-parsed:v1", () => {
        try { return { parsed: parseTemporalGraphXml(input.raw, input.sourceIds) }; }
        catch { return { invalid: true as const }; }
      });
      if ("invalid" in cached) return { success: false, repairState: "invalid-result", nodesAdded: 0, edgesAdded: 0 };
      if (cached.parsed.kind === "valid-empty") return { success: true, repairState: "valid-empty", nodesAdded: 0, edgesAdded: 0 };
      const selection = selectedGraphGeneration();
      const applied = selection
        ? await applySelectedTemporalGraph(graphKV(kv), cached.parsed.nodes, cached.parsed.edges, input.sourceIds,
          input.effectKey, input.project, selection.sessionId)
        : await applyTemporalGraph(graphKV(kv), cached.parsed.nodes, cached.parsed.edges, input.sourceIds, input.effectKey, input.project);
      return applied.success
        ? { success: true, repairState: "repaired", nodesAdded: applied.nodesAdded ?? 0, edgesAdded: applied.edgesAdded ?? 0 }
        : { success: false, repairState: "application-failed", nodesAdded: 0, edgesAdded: 0 };
    }, childJobId));
  } catch (error) {
    return temporalRecoveryNeeded(error instanceof Error ? error.name : "temporal_repair_child_unavailable");
  }
  if (child.success !== true || child.repairState === "invalid-result" || child.repairState === "application-failed") {
    return temporalRecoveryNeeded("temporal_repair_child_rejected");
  }
  if (child.repairState === "valid-empty") return { state: "not-needed" };
  const nodesAdded = child.nodesAdded;
  const edgesAdded = child.edgesAdded;
  if (child.repairState !== "repaired" || typeof nodesAdded !== "number" || !Number.isSafeInteger(nodesAdded) || nodesAdded < 0
    || typeof edgesAdded !== "number" || !Number.isSafeInteger(edgesAdded) || edgesAdded < 0) {
    return temporalRecoveryNeeded("temporal_repair_child_result_invalid");
  }
  return { state: "repaired", childJobId, nodesAdded, edgesAdded };
}

async function validateTemporalPlanProvenance(kv: StateKV, plan: TemporalReplayPlan): Promise<boolean> {
  if (!plan.project || !plan.sourceIds.length) return false;
  if (plan.unit) return validateTemporalUnitProvenance(kv, plan, plan.unit);
  if (plan.observations) {
    for (const sourceId of plan.sourceIds) {
      const candidates = plan.observations.filter((observation) => observation.id === sourceId);
      if (candidates.length !== 1 || !await observationHasProjectAuthority(kv, candidates[0]!, plan.project)) return false;
    }
    return true;
  }
  return false;
}

export function registerTemporalGraphFunctions(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  llmRouter?: LlmTaskRouter,
): void {
  kv = graphKV(kv);
  const workInputFor = (page: TemporalWorkPage) => ({
    audience: { scope: "project" as const, project: page.project },
    sources: page.sources,
    systemPrompt: TEMPORAL_EXTRACTION_SYSTEM,
    envelopePrefix: "Extract temporal graph XML from these source fragments.\n",
    envelopeSuffix: "\nReturn only the required XML document.",
  });
  const publishTemporal = (jobId: string) => sdk.trigger({ function_id: "iii::durable::publish",
    payload: { topic: TEMPORAL_TOPIC, data: { jobId } }, timeoutMs: 5000 });
  const readPendingPage = async (cursor?: string) => {
    const pages = kv.pages<TemporalPendingIndex>(TEMPORAL_PENDING_SCOPE, {
      ...(cursor ? { cursor } : {}), limit: 1, maxBytes: 16 * 1024,
    });
    try { return await pages.next(); }
    finally { await pages.return(undefined); }
  };
  const reconcileTemporalWork = async () => {
    if (isBackgroundRecoveryPaused()) return { success: true, queued: 0, paused: true };
    const saved = await kv.get<{ cursor: string | null }>(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY);
    let cursor = typeof saved?.cursor === "string" ? saved.cursor : undefined;
    let page: Awaited<ReturnType<typeof readPendingPage>>;
    try { page = await readPendingPage(cursor); }
    catch (error) {
      if (!(error instanceof StatePageError) || error.code !== "STATE_PAGE_CURSOR_STALE") throw error;
      cursor = undefined;
      await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: null });
      try { page = await readPendingPage(); }
      catch (retryError) {
        if (!(retryError instanceof StatePageError) || retryError.code !== "STATE_PAGE_CURSOR_STALE") throw retryError;
        await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: null });
        return { success: true, queued: 0, deferred: true, reason: retryError.code };
      }
    }
    if (page.done || page.value.items.length === 0) {
      await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: null });
      return { success: true, queued: 0, scanned: 0 };
    }
    const index = page.value.items[0]!;
    if (index.version !== 1 || index.state !== "pending" || !/^temporaljob_[a-f0-9]{16}$/.test(index.jobId)
      || !Number.isSafeInteger(index.pageCount) || index.pageCount < 1 || typeof index.manifestReady !== "boolean"
      || typeof index.createdAt !== "string" || typeof index.updatedAt !== "string") {
      throw new Error("Temporal pending-work index is invalid");
    }
    let manifest = await kv.get<SelectedTemporalManifest>(TEMPORAL_WORK_SCOPE, temporalManifestKey(index.jobId));
    let indexChanged = false;
    if (!manifest) {
      if (index.manifestReady) throw new Error("Temporal manifest is missing after durable admission");
      const firstPage = await kv.get<TemporalWorkPage>(TEMPORAL_WORK_SCOPE, temporalPageKey(index.jobId, 0));
      if (!firstPage || !firstPage.project || !Array.isArray(firstPage.sources) || firstPage.sources.length > GRAPH_WORK_LIMITS.maxPageSources) {
        throw new Error("Temporal source capture is incomplete; refusing to reconstruct its manifest");
      }
      manifest = { version: 1, id: index.jobId, state: "pending", pageCount: index.pageCount,
        currentPage: 0, nodesAdded: 0, edgesAdded: 0, workUnits: 0,
        createdAt: index.createdAt, updatedAt: index.updatedAt };
      await kv.set(TEMPORAL_WORK_SCOPE, temporalManifestKey(index.jobId), manifest);
    }
    if (manifest.id !== index.jobId || manifest.version !== 1 || manifest.pageCount !== index.pageCount) {
      throw new Error("Temporal pending-work index conflicts with its manifest");
    }
    if (manifest.selectedSource) {
      await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: page.value.next_cursor });
      return { success: true, queued: 0, deferred: true, reason: "selected_foreground_required" };
    }
    if (manifest.state === "completed") {
      await kv.delete(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(index.jobId));
      await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: null });
      return { success: true, queued: 0, cleaned: 1, jobId: index.jobId };
    }
    if (!index.manifestReady) {
      await kv.set(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(index.jobId), {
        ...index, manifestReady: true, updatedAt: new Date().toISOString(),
      } satisfies TemporalPendingIndex);
      indexChanged = true;
    }
    if (manifest.activeUnitJobId) {
      const activeUnit = await kv.get<{ recoveryAfter?: string | null; recoveryStopped?: boolean }>(KV.graphJobs, manifest.activeUnitJobId);
      const notDue = activeUnit?.recoveryStopped === true
        || typeof activeUnit?.recoveryAfter === "string" && Date.parse(activeUnit.recoveryAfter) > Date.now();
      if (notDue) {
        await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: indexChanged ? null : page.value.next_cursor });
        return { success: true, queued: 0, scanned: 1, deferred: true, reason: activeUnit?.recoveryStopped ? "unit_recovery_stopped" : "unit_backoff" };
      }
    }
    try { await publishTemporal(index.jobId); }
    catch {
      await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: indexChanged ? null : page.value.next_cursor });
      return { success: true, queued: 0, scanned: 1, deferred: true, reason: "publish_failed" };
    }
    const nextCursor = indexChanged ? null : page.value.next_cursor;
    await kv.set(TEMPORAL_WORK_SCOPE, TEMPORAL_PENDING_SCAN_KEY, { cursor: nextCursor });
    return { success: true, queued: 1, published: 1, jobId: index.jobId };
  };
  const runTemporalUnit = async (request: TemporalUnitRequest, durableId?: string) => {
    let parentJobId = durableId;
    const result = await runGraphJob(kv, "temporal", request, async (frozen) => {
    const data = frozen as TemporalUnitRequest;
    parentJobId = graphJobId() ?? parentJobId;
    if (data.temporalUnitVersion !== 2 || !data.project || !data.unit) throw new Error("Invalid captured temporal work unit");
    const unit = data.unit;
    const selection = selectedGraphGeneration();
    if (selection) {
      const manifest = await kv.get<SelectedTemporalManifest>(TEMPORAL_WORK_SCOPE, temporalManifestKey(data.jobId));
      const session = await kv.get<{ project?: string }>(KV.sessions, selection.sessionId);
      if (selection.project !== data.project || selection.jobId !== data.jobId || selection.unitId !== unit.fingerprint
        || session?.project !== data.project || manifest?.selectedSource?.project !== data.project
        || manifest.id !== data.jobId || manifest.version !== 1 || manifest.currentPage !== data.pageIndex
        || manifest.selectedSource.sessionId !== selection.sessionId || manifest.selectedSource.sourceFingerprint !== selection.sourceFingerprint
        || selection.checkpoint !== temporalCheckpoint(manifest)
        || data.workInput.audience.scope !== "project" || data.workInput.audience.project !== data.project
        || !Array.isArray(data.workInput.sources) || !data.workInput.sources.length
        || data.workInput.sources.length > GRAPH_WORK_LIMITS.maxPageSources) throw new Error("Temporal selected frozen authority conflicts");
      for (const source of data.workInput.sources) {
        const observation = await kv.get<TemporalExtractionRequest["observations"][number]>(KV.observations(selection.sessionId), source.id);
        if (!observation || observation.id !== source.id || observation.sessionId !== selection.sessionId
          || observation.project && observation.project !== data.project) throw new Error("Temporal selected source membership changed");
        const content = JSON.stringify({ id: observation.id, type: observation.type, timestamp: observation.timestamp,
          title: observation.title, narrative: observation.narrative, concepts: observation.concepts ?? [], files: observation.files ?? [] });
        const part = Number(source.version.slice(source.version.lastIndexOf(":") + 1));
        const fragments = [...splitTemporalSource(content, GRAPH_WORK_LIMITS.maxPageBytes - 512)];
        if (!Number.isSafeInteger(part) || source.version !== `${observation.timestamp || "unknown"}:${part}`
          || fragments[part] !== source.content) throw new Error("Temporal selected source version changed");
      }
      const expected = createNextGraphWorkUnit({ ...data.workInput, cursor: manifest.cursor ?? null }).unit;
      if (!expected || JSON.stringify(expected) !== JSON.stringify(unit)) throw new Error("Temporal selected frozen unit changed");
    }
    const response = await freezeGraphValue(kv, "temporal-response:v3", async () => {
      const execute = () => llmRouter
      ? llmRouter.run("temporal_graph_extraction",
        selectedProvider => selectedProvider.compress(unit.systemPrompt, unit.userPrompt),
        (candidate) => {
          const sourceIds = [...new Set(unit.fragments.map((fragment) => fragment.sourceId))];
            parseTemporalGraphXml(candidate, sourceIds);
            return true;
        })
      : provider.compress(unit.systemPrompt, unit.userPrompt);
      return execute();
    });
    if (Buffer.byteLength(response, "utf8") > GRAPH_WORK_LIMITS.maxResponseBytes) throw new Error("Temporal graph response exceeds the bounded 16 KiB result budget.");
    const observationIds = [...new Set(unit.fragments.map((fragment) => fragment.sourceId))];
    const parsed = await freezeGraphValue(kv, "temporal-parsed:v3", () => parseTemporalGraphXml(response, observationIds));
    const effectKey = batchEffectKey(`temporal:${data.project}:${unit.fingerprint}`);
    const applied = selection
      ? await applySelectedTemporalGraph(kv, parsed.nodes, parsed.edges, observationIds, effectKey, data.project, selection.sessionId)
      : await applyTemporalGraph(kv, parsed.nodes, parsed.edges, observationIds, effectKey, data.project);
    if (!applied.success) throw new Error(applied.error ?? "Temporal graph unit application failed");
    return { ...applied, temporalResultContract: TEMPORAL_RESULT_CONTRACT };
    }, durableId);
    if (!parentJobId) return { ...result, temporalRepair: temporalRecoveryNeeded("completed_parent_identity_missing") };
    if ((result as { temporalResultContract?: unknown })?.temporalResultContract === TEMPORAL_RESULT_CONTRACT) {
      return { ...result, temporalRepair: { state: "not-needed" } satisfies TemporalRepairOutcome };
    }
    const sourceIds = [...new Set(request.unit.fragments.map((fragment) => fragment.sourceId))];
    const plan: TemporalReplayPlan = {
      rawOrdinal: 1, rawName: "temporal-response:v3", parsedOrdinal: 2, parsedName: "temporal-parsed:v3",
      applyOrdinal: 3, unitFingerprint: request.unit.fingerprint,
      effectKey: batchEffectKey(`temporal:${request.project}:${request.unit.fingerprint}`),
      project: request.project, sourceIds, unit: request.unit, pageSources: request.workInput.sources,
    };
    const temporalRepair = await repairLegacyTemporalResult(kv, parentJobId, plan);
    return { ...result, temporalRepair };
  };

  const temporalCheckpoint = (manifest: TemporalWorkManifest) => createHash("sha256").update(JSON.stringify({
    currentPage: manifest.currentPage, cursor: manifest.cursor ?? null, workUnits: manifest.workUnits,
  })).digest("hex");
  const cleanupCompletedTemporalWork = async (manifest: SelectedTemporalManifest) => {
    if (manifest.currentPage > 0 && manifest.currentPage <= manifest.pageCount) {
      await kv.delete(TEMPORAL_WORK_SCOPE, temporalPageKey(manifest.id, manifest.currentPage - 1));
    }
    await kv.delete(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(manifest.id));
  };
  const selectedContinuation = async (manifest: SelectedTemporalManifest, selected: SelectedGraphControl) => {
    if (manifest.state === "completed") return undefined;
    const page = await kv.get<TemporalWorkPage>(TEMPORAL_WORK_SCOPE, temporalPageKey(manifest.id, manifest.currentPage));
    if (!page) throw new Error("Temporal selected source page is unavailable");
    const next = createNextGraphWorkUnit({ ...workInputFor(page), cursor: manifest.cursor ?? null });
    return { ...selected, jobId: manifest.id, checkpoint: temporalCheckpoint(manifest), unitId: next.unit?.fingerprint };
  };
  const deliverTemporalWork = async (delivery: { jobId?: string; reconcile?: boolean; selected?: SelectedGraphControl }) => {
    if (delivery?.selected !== undefined) validateSelectedGraphControl(delivery.selected);
    if (delivery?.selected && (!delivery.selected.jobId || !delivery.selected.unitId || !delivery.selected.checkpoint)) throw new Error("Selected temporal delivery requires an exact unit and checkpoint");
    if (delivery?.selected && delivery.reconcile) throw new Error("Selected temporal work cannot scan pending work");
    if (delivery?.reconcile === true) return reconcileTemporalWork();
    if (!delivery || typeof delivery.jobId !== "string" || !/^temporaljob_[a-f0-9]{16}$/.test(delivery.jobId)) throw new Error("Invalid temporal work delivery");
    if (!delivery.selected && isBackgroundRecoveryPaused()) return { success: true, paused: true, queued: true };
    return withKeyedLock(`temporal-work:${delivery.jobId}`, async () => {
      const manifest = await kv.get<SelectedTemporalManifest>(TEMPORAL_WORK_SCOPE, temporalManifestKey(delivery.jobId!));
      if (!manifest || manifest.id !== delivery.jobId || manifest.version !== 1) return { success: true, stale: true };
      if (delivery.selected) {
        const selected = delivery.selected;
        const session = await kv.get<{ project?: string }>(KV.sessions, selected.sessionId);
        if (selected.jobId !== manifest.id || session?.project !== selected.project || !manifest.selectedSource
          || manifest.selectedSource.project !== selected.project || manifest.selectedSource.sessionId !== selected.sessionId
          || manifest.selectedSource.sourceFingerprint !== selected.sourceFingerprint) throw new Error("Temporal selected source identity conflicts");
        if (manifest.state === "completed" && manifest.selectedReplay?.checkpoint === selected.checkpoint
          && manifest.selectedReplay?.unitId === selected.unitId) {
          await cleanupCompletedTemporalWork(manifest);
          return { success: true, completed: true, replayed: true, jobId: manifest.id, ...manifestCounts(manifest) };
        }
        if (selected.checkpoint !== temporalCheckpoint(manifest)) return { success: true, stale: true, selected: await selectedContinuation(manifest, selected) };
      }
      if (manifest.state === "completed") {
        await cleanupCompletedTemporalWork(manifest);
        return { success: true, completed: true, jobId: manifest.id, ...manifestCounts(manifest) };
      }
      if (manifest.currentPage >= manifest.pageCount) throw new Error("Temporal work manifest cursor exceeds its captured pages");
      const page = await kv.get<TemporalWorkPage>(TEMPORAL_WORK_SCOPE, temporalPageKey(manifest.id, manifest.currentPage));
      if (!page || !page.project || !Array.isArray(page.sources) || page.sources.length > GRAPH_WORK_LIMITS.maxPageSources) throw new Error("Temporal work page is unavailable");
      if (delivery.selected) {
        for (const source of page.sources) {
          const observation = await kv.get<TemporalExtractionRequest["observations"][number]>(KV.observations(delivery.selected.sessionId), source.id);
          if (!observation || observation.sessionId !== delivery.selected.sessionId) throw new Error("Temporal selected source membership changed");
          const content = JSON.stringify({ id: observation.id, type: observation.type, timestamp: observation.timestamp,
            title: observation.title, narrative: observation.narrative, concepts: observation.concepts ?? [], files: observation.files ?? [] });
          const part = Number(source.version.slice(source.version.lastIndexOf(":") + 1));
          let fragment: string | undefined, index = 0;
          for (const candidate of splitTemporalSource(content, GRAPH_WORK_LIMITS.maxPageBytes - 512)) {
            if (index++ === part) { fragment = candidate; break; }
          }
          if (!Number.isSafeInteger(part) || source.version !== `${observation.timestamp || "unknown"}:${part}` || fragment !== source.content) throw new Error("Temporal selected source version changed");
        }
      }
      const workInput = workInputFor(page);
      const next = createNextGraphWorkUnit({ ...workInput, cursor: manifest.cursor ?? null });
      if (delivery.selected && (page.project !== delivery.selected.project || delivery.selected.unitId !== next.unit?.fingerprint)) throw new Error("Temporal selected unit identity conflicts");
      let unitResult: { nodesAdded?: number; edgesAdded?: number; temporalRepair?: TemporalRepairOutcome } = {};
      if (next.unit) {
        const unitRequest: TemporalUnitRequest = { temporalUnitVersion: 2, jobId: manifest.id, pageIndex: manifest.currentPage,
          project: page.project, workInput, unit: next.unit };
        const durableId = `graphjob_${fingerprintId("temporalunit", `${manifest.id}:${manifest.currentPage}:${next.unit.fingerprint}`).slice("temporalunit_".length)}`;
        const savedJob = await kv.get<{ state?: string; recoveryStopped?: boolean; recoveryAfter?: string | null }>(KV.graphJobs, durableId);
        if (savedJob?.recoveryStopped || savedJob?.recoveryAfter && Date.parse(savedJob.recoveryAfter) > Date.now()) {
          return { success: true, deferred: true, reason: savedJob.recoveryStopped ? "unit_recovery_stopped" : "unit_backoff" };
        }
        if (manifest.activeUnitJobId && manifest.activeUnitJobId !== durableId) throw new Error("Temporal manifest points to a different captured work unit");
        if (!manifest.activeUnitJobId) {
          await kv.set(TEMPORAL_WORK_SCOPE, temporalManifestKey(manifest.id), {
            ...manifest, activeUnitJobId: durableId, updatedAt: new Date().toISOString(),
          });
        }
        try {
          const localOnlyResume = Boolean(savedJob && savedJob.state !== "completed");
          unitResult = await withSelectedGraphOrigin(delivery.selected, () => withSelectedGraphGeneration(delivery.selected, () =>
            withLocalGraphRecovery(localOnlyResume, () => runTemporalUnit(unitRequest, durableId))));
        } catch (error) {
          if (delivery.selected && error instanceof Error && error.name === "GraphProviderRecoveryRequired") return {
            success: true, queued: true, providerRequired: true, selected: delivery.selected,
          };
          throw error;
        }
      }
      if (unitResult.temporalRepair?.state === "recovery-needed") {
        return { success: true, queued: true, deferred: true, needsRecovery: true,
          reason: "temporal_result_recovery_needed", temporalRepair: unitResult.temporalRepair };
      }
      const pageDone = next.complete || !next.unit;
      const updated: SelectedTemporalManifest = {
        ...manifest,
        state: pageDone && manifest.currentPage + 1 >= manifest.pageCount ? "completed" : "pending",
        currentPage: pageDone ? manifest.currentPage + 1 : manifest.currentPage,
        cursor: pageDone ? undefined : next.cursor ?? undefined,
        activeUnitJobId: undefined,
        nodesAdded: manifest.nodesAdded + (unitResult.nodesAdded ?? 0),
        edgesAdded: manifest.edgesAdded + (unitResult.edgesAdded ?? 0),
        repairNodesAdded: (manifest.repairNodesAdded ?? 0) + (unitResult.temporalRepair?.state === "repaired" ? unitResult.temporalRepair.nodesAdded : 0),
        repairEdgesAdded: (manifest.repairEdgesAdded ?? 0) + (unitResult.temporalRepair?.state === "repaired" ? unitResult.temporalRepair.edgesAdded : 0),
        workUnits: manifest.workUnits + (next.unit ? 1 : 0),
        updatedAt: new Date().toISOString(),
        ...(delivery.selected ? { selectedReplay: { checkpoint: delivery.selected.checkpoint!, unitId: delivery.selected.unitId } } : {}),
      };
      await kv.set(TEMPORAL_WORK_SCOPE, temporalManifestKey(updated.id), updated);
      if (pageDone) await kv.delete(TEMPORAL_WORK_SCOPE, temporalPageKey(updated.id, manifest.currentPage));
      if (updated.state !== "completed") {
        await publishTemporal(updated.id);
      } else await kv.delete(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(updated.id));
      return { success: true, queued: updated.state !== "completed", completed: updated.state === "completed", jobId: updated.id,
        ...manifestCounts(updated), workUnitProcessed: Boolean(next.unit),
        ...(unitResult.temporalRepair ? { temporalRepair: unitResult.temporalRepair } : {}),
        ...(delivery.selected ? { selected: await selectedContinuation(updated, delivery.selected) } : {}) };
    });
  };

  const enqueueTemporalExtraction = async (request: TemporalExtractionRequest) => {
    if (request?.selected !== undefined) validateSelectedGraphControl(request.selected);
    if (!request?.selected && isBackgroundRecoveryPaused()) return { success: true, paused: true, queued: false };
    if (!request || !Array.isArray(request.observations) || request.observations.length === 0) return { success: false, error: "No observations provided" };
    if (request.observations.length > 100_000 || request.project !== undefined && typeof request.project !== "string") return { success: false, error: "Invalid temporal extraction request" };
    const hash = createHash("sha256").update(JSON.stringify({ project: request.project?.trim() ?? "" }));
    for (const observation of request.observations) {
      const encoded = JSON.stringify(observation);
      hash.update(String(Buffer.byteLength(encoded, "utf8"))).update(":").update(encoded);
    }
    const sourceFingerprint = hash.digest("hex");
    const jobId = `temporaljob_${sourceFingerprint.slice(0, 16)}`;
    if (request.selected) {
      const selected = request.selected;
      if (selected.sourceFingerprint !== sourceFingerprint || selected.jobId !== undefined && selected.jobId !== jobId
        || request.project !== selected.project || request.observations.some(observation => observation.sessionId !== selected.sessionId)) throw new Error("Temporal selected source identity conflicts");
      const session = await kv.get<{ project?: string }>(KV.sessions, selected.sessionId);
      if (session?.project !== selected.project) throw new Error("Temporal selected session authority conflicts");
      await validateSelectedGraphSources(kv, selected, request.observations);
    }
    return withKeyedLock(`temporal-work:${jobId}`, async () => {
      const existing = await kv.get<SelectedTemporalManifest>(TEMPORAL_WORK_SCOPE, temporalManifestKey(jobId));
      const priorIndex = await kv.get<TemporalPendingIndex>(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(jobId));
      if (existing) {
        if (request.selected && (!existing.selectedSource || existing.selectedSource.sourceFingerprint !== sourceFingerprint
          || existing.selectedSource.sessionId !== request.selected.sessionId || existing.selectedSource.project !== request.selected.project)) throw new Error("Temporal selected capture conflicts");
        if (existing.id !== jobId || existing.version !== 1) throw new Error("Temporal extraction identity conflicts with saved work");
        if (existing.state === "pending") {
          const indexed = await kv.get<TemporalPendingIndex>(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(jobId));
          if (!indexed) await kv.set(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(jobId), {
            version: 1, jobId, state: "pending", manifestReady: true, pageCount: existing.pageCount,
            createdAt: existing.createdAt, updatedAt: new Date().toISOString(),
          } satisfies TemporalPendingIndex);
          if (!request.selected) await publishTemporal(jobId);
        }
        return { success: true, queued: existing.state !== "completed", completed: existing.state === "completed", jobId, ...manifestCounts(existing),
          ...(request.selected ? { selected: await selectedContinuation(existing, request.selected) } : {}) };
      }
      if (priorIndex) {
        if (priorIndex.jobId !== jobId || !Number.isSafeInteger(priorIndex.pageCount) || priorIndex.pageCount < 1) {
          throw new Error("Temporal pending-work identity conflicts with its captured pages");
        }
        if (priorIndex.manifestReady) throw new Error("Temporal manifest is missing after durable admission");
        return { success: true, queued: true, jobId, workUnits: 0, recoveryPending: true };
      }

      let pageIndex = 0;
      let project = "";
      let pageSources: TemporalWorkPage["sources"] = [];
      let pageBytes = 0;
      const flush = async () => {
        if (!pageSources.length) return;
        await kv.set(TEMPORAL_WORK_SCOPE, temporalPageKey(jobId, pageIndex++), { project, sources: pageSources } satisfies TemporalWorkPage);
        pageSources = [];
        pageBytes = 0;
      };
      try {
        for (const observation of request.observations) {
          if (!observation || typeof observation.id !== "string" || !observation.id || typeof observation.sessionId !== "string" ||
            typeof observation.title !== "string" || typeof observation.narrative !== "string") {
            throw new Error("Temporal observations require an id, source session, title and narrative");
          }
          const session = await kv.get<{ project?: string }>(KV.sessions, observation.sessionId);
          const sourceProject = session?.project?.trim();
          if (!sourceProject) throw new TemporalScopeError("waiting_scope", `Project authority for observation ${observation.id} is unavailable.`);
          if (request.project?.trim() && request.project.trim() !== sourceProject || observation.project && observation.project !== sourceProject) {
            throw new TemporalScopeError("project_scope_mismatch", `Observation ${observation.id} is outside the requested project.`);
          }
          if (project && project !== sourceProject) await flush();
          project = sourceProject;
          const content = JSON.stringify({ id: observation.id, type: observation.type, timestamp: observation.timestamp,
            title: observation.title, narrative: observation.narrative, concepts: observation.concepts ?? [], files: observation.files ?? [] });
          let part = 0;
          for (const contentPart of splitTemporalSource(content, GRAPH_WORK_LIMITS.maxPageBytes - 512)) {
            const source = { id: observation.id, version: `${observation.timestamp || "unknown"}:${part++}`, content: contentPart,
              authority: { version: 1 as const, sessionId: observation.sessionId, project: sourceProject } };
            const bytes = Buffer.byteLength(source.id, "utf8") + Buffer.byteLength(source.version, "utf8") + Buffer.byteLength(source.content, "utf8");
            if (pageSources.length >= GRAPH_WORK_LIMITS.maxPageSources || pageBytes + bytes > GRAPH_WORK_LIMITS.maxPageBytes) await flush();
            pageSources.push(source);
            pageBytes += bytes;
          }
        }
        await flush();
      } catch (error) {
        for (let index = 0; index < pageIndex; index++) await kv.delete(TEMPORAL_WORK_SCOPE, temporalPageKey(jobId, index)).catch(() => undefined);
        if (error instanceof TemporalScopeError) return { success: false, partial: true, reason: error.reason, error: error.message };
        throw error;
      }
      if (pageIndex === 0) return { success: false, error: "No temporal work was captured" };
      const now = new Date().toISOString();
      const manifest: SelectedTemporalManifest = { version: 1, id: jobId, state: "pending", pageCount: pageIndex,
        currentPage: 0, nodesAdded: 0, edgesAdded: 0, workUnits: 0, createdAt: now, updatedAt: now,
        ...(request.selected ? { selectedSource: { project: request.selected.project, sessionId: request.selected.sessionId, sourceFingerprint } } : {}) };
      await kv.set(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(jobId), {
        version: 1, jobId, state: "pending", manifestReady: false, pageCount: pageIndex,
        createdAt: now, updatedAt: now,
      } satisfies TemporalPendingIndex);
      await kv.set(TEMPORAL_WORK_SCOPE, temporalManifestKey(jobId), manifest);
      await kv.set(TEMPORAL_PENDING_SCOPE, temporalPendingScanKey(jobId), {
        version: 1, jobId, state: "pending", manifestReady: true, pageCount: pageIndex,
        createdAt: now, updatedAt: now,
      } satisfies TemporalPendingIndex);
      if (!request.selected) await publishTemporal(jobId);
      return { success: true, queued: true, jobId, workUnits: 0,
        ...(request.selected ? { selected: await selectedContinuation(manifest, request.selected) } : {}) };
    });
  };

  const extract = async (request: TemporalExtractionRequest, durableId?: string) => {
    let parentJobId = durableId;
    const result = await runGraphJob(kv, "temporal", request, async (frozen) => {
    parentJobId = graphJobId() ?? parentJobId;
    const data = frozen as TemporalExtractionRequest;
    if (!Array.isArray(data.observations) || data.observations.length === 0) return { success: false, error: "No observations provided" };
    try {
      if (data.graphWorkGroupsVersion !== 2) {
        const response = await freezeGraphValue(kv, "temporal-response", () => {
          throw new Error("Legacy temporal job has no captured provider response; submit a new project-scoped extraction.");
        });
        const obsIds = data.observations.map((observation) => observation.id);
        const parsed = await freezeGraphValue(kv, "temporal-parsed", () => parseTemporalGraphXml(response, obsIds));
        let project = data.project?.trim();
        for (const observation of data.observations) {
          if (!observation.sessionId) return { success: false, partial: true, reason: "project_scope_unavailable", error: "Captured temporal source has no session authority." };
          const session = await kv.get<{ project?: string }>(KV.sessions, observation.sessionId);
          const sourceProject = session?.project?.trim();
          if (!sourceProject || observation.project && observation.project !== sourceProject || project && project !== sourceProject) {
            return { success: false, partial: true, reason: "project_scope_unavailable", error: "Captured temporal result cannot be applied without one verified project scope." };
          }
          project = sourceProject;
        }
        if (!project) {
          return { success: false, partial: true, reason: "project_scope_unavailable", error: "Captured temporal result cannot be applied without one verified project scope." };
        }
        return applyTemporalGraph(kv, parsed.nodes, parsed.edges, obsIds, batchEffectKey(`temporal:${JSON.stringify(data.observations)}`), project);
      }

      let nodesAdded = 0;
      let edgesAdded = 0;
      let unitOrdinal = 0;
      const observations = data.observations;
      for (let offset = 0; offset < observations.length; offset += GRAPH_WORK_LIMITS.maxPageSources) {
        const sourceBatch = observations.slice(offset, offset + GRAPH_WORK_LIMITS.maxPageSources);
        const groups = new Map<string, typeof sourceBatch>();
        for (const observation of sourceBatch) {
          if (!observation.sessionId) return { success: false, partial: true, reason: "waiting_scope", error: `Observation ${observation.id} has no source session.` };
          const session = await kv.get<{ project?: string }>(KV.sessions, observation.sessionId);
          const project = session?.project?.trim();
          if (!project) return { success: false, partial: true, reason: "waiting_scope", error: `Project authority for observation ${observation.id} is unavailable.` };
          if (data.project?.trim() && data.project.trim() !== project || observation.project && observation.project !== project) {
            return { success: false, partial: true, reason: "project_scope_mismatch", error: `Observation ${observation.id} is outside the requested project.` };
          }
          if (!groups.has(project)) groups.set(project, []);
          groups.get(project)!.push(observation);
        }

        for (const [project, projectSources] of groups) {
          let pageSources: Array<{ id: string; version: string; content: string }> = [];
          let pageBytes = 0;
          const processPage = async () => {
            if (pageSources.length === 0) return;
            const workInput = {
              audience: { scope: "project" as const, project },
              sources: pageSources,
              systemPrompt: TEMPORAL_EXTRACTION_SYSTEM,
              envelopePrefix: "Extract temporal graph XML from these source fragments.\n",
              envelopeSuffix: "\nReturn only the required XML document.",
            };
            let cursor = null;
            let pageDone = false;
            while (!pageDone) {
              const next = createNextGraphWorkUnit({ ...workInput, cursor });
              if (!next.unit) break;
              const ordinal = unitOrdinal++;
              const unit = await freezeGraphValue(kv, `temporal-work-unit:v2:${ordinal}`, () => next.unit!);
              const promptResponse = await freezeGraphValue(kv, `temporal-response:v2:${ordinal}`, () => llmRouter
                ? llmRouter.run("temporal_graph_extraction",
                  (selectedProvider) => selectedProvider.compress(unit.systemPrompt, unit.userPrompt),
                  (candidate) => {
                    const sourceIds = [...new Set(unit.fragments.map((fragment) => fragment.sourceId))];
                    parseTemporalGraphXml(candidate, sourceIds);
                    return true;
                  })
                : provider.compress(unit.systemPrompt, unit.userPrompt));
              if (Buffer.byteLength(promptResponse, "utf8") > GRAPH_WORK_LIMITS.maxResponseBytes) {
                throw new Error("Temporal graph response exceeds the bounded 16 KiB result budget.");
              }
              const obsIds = [...new Set(unit.fragments.map((fragment) => fragment.sourceId))];
              const parsed = await freezeGraphValue(kv, `temporal-parsed:v2:${ordinal}`, () => parseTemporalGraphXml(promptResponse, obsIds));
              const applied = await applyTemporalGraph(kv, parsed.nodes, parsed.edges, obsIds,
                batchEffectKey(`temporal:${project}:${unit.fingerprint}`), project);
              if (!applied.success) return applied;
              nodesAdded += applied.nodesAdded ?? 0;
              edgesAdded += applied.edgesAdded ?? 0;
              cursor = next.cursor;
              pageDone = next.complete;
            }
            pageSources = [];
            pageBytes = 0;
          };

          for (const observation of projectSources) {
            const content = JSON.stringify({
              id: observation.id, type: observation.type, timestamp: observation.timestamp,
              title: observation.title, narrative: observation.narrative,
              concepts: observation.concepts ?? [], files: observation.files ?? [],
            });
            let part = 0;
            for (const contentPart of splitTemporalSource(content, GRAPH_WORK_LIMITS.maxPageBytes - 512)) {
              const source = { id: observation.id, version: `${observation.timestamp || "unknown"}:${part++}`, content: contentPart };
              const bytes = Buffer.byteLength(source.id, "utf8") + Buffer.byteLength(source.version, "utf8") + Buffer.byteLength(source.content, "utf8");
              if (pageSources.length >= GRAPH_WORK_LIMITS.maxPageSources || pageBytes + bytes > GRAPH_WORK_LIMITS.maxPageBytes) await processPage();
              pageSources.push(source);
              pageBytes += bytes;
            }
          }
          await processPage();
        }
      }
      return { success: true, nodesAdded, edgesAdded, workUnits: unitOrdinal, temporalResultContract: TEMPORAL_RESULT_CONTRACT };
    } catch (error) {
      if (graphTransactionFailure(error)) throw error;
      const message = error instanceof Error ? error.message : String(error);
      logger.error("Temporal graph extraction failed", { error: message });
      return { success: false, error: message };
    }
    }, durableId);
    if (!parentJobId) return { ...result, temporalRepair: temporalRecoveryNeeded("completed_parent_identity_missing") };
    if ((result as { temporalResultContract?: unknown })?.temporalResultContract === TEMPORAL_RESULT_CONTRACT) return result;
    const plans: TemporalReplayPlan[] = [];
    if (request.graphWorkGroupsVersion !== 2) {
      let project = request.project?.trim() ?? "";
      for (const observation of request.observations) {
        if (!observation.sessionId) continue;
        const session = await kv.get<{ project?: string }>(KV.sessions, observation.sessionId);
        const currentProject = session?.project?.trim();
        if (currentProject && (!project || project === currentProject)) project = currentProject;
      }
      plans.push({ rawOrdinal: 1, rawName: "temporal-response", parsedOrdinal: 2, parsedName: "temporal-parsed",
        applyOrdinal: 3, unitFingerprint: fingerprintId("temporalrepairunit", JSON.stringify([
          project, request.observations.map((observation) => [observation.id, observation.sessionId ?? null, observation.timestamp]),
        ])), effectKey: batchEffectKey(`temporal:${JSON.stringify(request.observations)}`), project,
        sourceIds: [...new Set(request.observations.map((observation) => observation.id))], observations: request.observations });
    } else {
      const workUnits = Number((result as { workUnits?: unknown })?.workUnits);
      if (!Number.isSafeInteger(workUnits) || workUnits < 0 || workUnits > 10_000) {
        return { ...result, temporalRepair: temporalRecoveryNeeded("grouped_temporal_work_count_invalid") };
      }
      for (let index = 0; index < workUnits; index++) {
        try {
          const replay = await readCompletedGraphJobReplay(kv, { jobId: parentJobId,
            values: [{ ordinal: 4 * index + 1, name: `temporal-work-unit:v2:${index}` }], deltas: [] });
          const unit = replay.values[0]?.value as TemporalUnitRequest["unit"] | undefined;
          if (!unit || !Array.isArray(unit.fragments) || unit.audience.scope !== "project") {
            return { ...result, temporalRepair: temporalRecoveryNeeded("grouped_temporal_unit_invalid") };
          }
          const project = unit.audience.project;
          const sourceIds = [...new Set(unit.fragments.map((fragment) => fragment.sourceId))];
          plans.push({ rawOrdinal: 4 * index + 2, rawName: `temporal-response:v2:${index}`,
            parsedOrdinal: 4 * index + 3, parsedName: `temporal-parsed:v2:${index}`, applyOrdinal: 4 * index + 4,
            unitFingerprint: unit.fingerprint, effectKey: batchEffectKey(`temporal:${project}:${unit.fingerprint}`),
            project, sourceIds, unit, observations: request.observations });
        } catch (error) {
          return { ...result, temporalRepair: temporalRecoveryNeeded(error instanceof Error ? error.name : "grouped_temporal_replay_unavailable") };
        }
      }
    }
    const repairs: TemporalRepairOutcome[] = [];
    let repairNodesAdded = 0;
    let repairEdgesAdded = 0;
    for (const plan of plans) {
      const repair = await repairLegacyTemporalResult(kv, parentJobId, plan);
      if (repair.state === "recovery-needed") return { ...result, temporalRepair: repair };
      repairs.push(repair);
      if (repair.state === "repaired") {
        repairNodesAdded += repair.nodesAdded;
        repairEdgesAdded += repair.edgesAdded;
      }
    }
    if (!repairs.some((repair) => repair.state === "repaired")) return result;
    return { ...result, temporalRepairs: repairs, repairNodesAdded, repairEdgesAdded };
  };
  sdk.registerFunction("mem::temporal-graph-extract", enqueueTemporalExtraction);
  sdk.registerFunction("mem::temporal-work-unit", deliverTemporalWork);
  sdk.registerTrigger({ type: "durable:subscriber", function_id: "mem::temporal-work-unit",
    config: { topic: TEMPORAL_TOPIC, queue_config: { type: "fifo", concurrency: 1, maxRetries: 5, backoffDelayMs: 30000 } } });
  registerGraphJobHandler(kv, "temporal", (input, id) => {
    if (input && typeof input === "object" && "temporalUnitVersion" in input && input.temporalUnitVersion === 2) {
      return runTemporalUnit(input as TemporalUnitRequest, id);
    }
    return extract(input as TemporalExtractionRequest, id);
  });
  const graphReader = new GraphRetrievalReader(kv);
  sdk.registerFunction("mem::temporal-query",
    async (data: {
      entityName: string;
      project?: string;
      asOf?: string;
      includeHistory?: boolean;
    }): Promise<(TemporalState & { partial: boolean; reason?: string }) | { error: string; status?: string; partial?: boolean; reason?: string }> => {
      const project = data.project?.trim();
      if (!project) return { error: "Project scope is required for temporal graph reads.", status: "waiting_scope", partial: true, reason: "project_required" };
      const scoped = await graphReader.readTemporalEntity(data.entityName, project);
      if (!scoped.entity) return { error: `Entity "${data.entityName}" not found in project scope.`, status: "partial", partial: true, reason: scoped.reason ?? "entity_not_found" };
      const allEntityEdges = scoped.edges;
      if (data.asOf) {
        const asOfTime = new Date(data.asOf).getTime();
        if (!Number.isFinite(asOfTime)) return { error: "asOf must be a valid date." };
        const validEdges = allEntityEdges.filter((edge) => {
          const commitTime = new Date(edge.tcommit || edge.createdAt).getTime();
          if (commitTime > asOfTime) return false;
          if (edge.tvalid && new Date(edge.tvalid).getTime() > asOfTime) return false;
          if (edge.tvalidEnd && new Date(edge.tvalidEnd).getTime() < asOfTime) return false;
          return true;
        });
        return {
          entity: scoped.entity,
          currentEdges: getLatestByKey(validEdges),
          historicalEdges: data.includeHistory ? validEdges : [],
          timeline: buildTimeline(allEntityEdges),
          partial: scoped.partial,
          ...(scoped.reason ? { reason: scoped.reason } : {}),
        };
      }
      const currentEdges = allEntityEdges.filter((edge) => edge.isLatest !== false);
      const historicalEdges = data.includeHistory ? allEntityEdges.filter((edge) => edge.isLatest === false) : [];
      return {
        entity: scoped.entity,
        currentEdges,
        historicalEdges,
        timeline: buildTimeline(allEntityEdges),
        partial: scoped.partial,
        ...(scoped.reason ? { reason: scoped.reason } : {}),
      };
    },
  );

  sdk.registerFunction("mem::differential-state",
    async (data: {
      entityName: string;
      project?: string;
      from?: string;
      to?: string;
    }) => {
      const project = data.project?.trim();
      if (!project) return { error: "Project scope is required for differential graph reads.", status: "waiting_scope", partial: true, reason: "project_required" };
      const scoped = await graphReader.readTemporalEntity(data.entityName, project);
      if (!scoped.entity) return { error: "Entity not found in project scope.", status: "partial", partial: true, reason: scoped.reason ?? "entity_not_found" };
      const allEntityEdges = [...scoped.edges].sort((left, right) =>
        new Date(left.tcommit || left.createdAt).getTime() - new Date(right.tcommit || right.createdAt).getTime());
      const fromTime = data.from ? new Date(data.from).getTime() : 0;
      const toTime = data.to ? new Date(data.to).getTime() : Date.now();
      if (!Number.isFinite(fromTime) || !Number.isFinite(toTime) || fromTime > toTime) return { error: "from and to must form a valid increasing date range." };
      const filtered = allEntityEdges.filter((edge) => {
        const committed = new Date(edge.tcommit || edge.createdAt).getTime();
        return committed >= fromTime && committed <= toTime;
      });
      const changes = filtered.slice(0, 64).map((edge) => ({
        type: edge.type,
        target: edge.sourceNodeId === scoped.entity!.id ? edge.targetNodeId : edge.sourceNodeId,
        validFrom: edge.tvalid || edge.createdAt,
        validTo: edge.tvalidEnd,
        reasoning: edge.context?.reasoning,
        sentiment: edge.context?.sentiment,
        version: edge.version || 1,
        isLatest: edge.isLatest !== false,
      }));
      const partial = scoped.partial || filtered.length > changes.length;
      return {
        entity: scoped.entity.name,
        totalChanges: changes.length,
        changes,
        partial: true,
        reason: scoped.reason ?? (partial ? "bounded_temporal_history" : "revision_changefeed_unavailable"),
      };
    },
  );
}

function getLatestByKey(edges: GraphEdge[]): GraphEdge[] {
  const byKey = new Map<string, GraphEdge>();
  for (const e of edges) {
    const key = `${e.sourceNodeId}|${e.targetNodeId}|${e.type}`;
    const existing = byKey.get(key);
    if (
      !existing ||
      new Date(e.tcommit || e.createdAt).getTime() >
      new Date(existing.tcommit || existing.createdAt).getTime()
    ) {
      byKey.set(key, e);
    }
  }
  return Array.from(byKey.values());
}

function buildTimeline(
  edges: GraphEdge[],
): Array<{
  edge: GraphEdge;
  validFrom: string;
  validTo?: string;
  context?: EdgeContext;
}> {
  const sorted = [...edges].sort(
    (a, b) =>
      new Date(a.tcommit || a.createdAt).getTime() -
      new Date(b.tcommit || b.createdAt).getTime(),
  );

  return sorted.map((e) => ({
    edge: e,
    validFrom: e.tvalid || e.createdAt,
    validTo: e.tvalidEnd,
    context: e.context,
  }));
}
