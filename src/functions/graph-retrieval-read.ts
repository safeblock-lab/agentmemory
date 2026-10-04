import type { GraphEdge, GraphNode } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { withCompletedGraphRead } from "./graph-jobs.js";
import { chunks, comparePositions, indexedGraphItems, IndexedRetrievalError, retrievalRequest, type IndexedSeed, type IndexedEdge } from '../state/indexed-retrieval.js';

const MAX_SERIALIZED_GRAPH_BYTES = 64 * 1024 * 1024;
const MAX_ESTIMATED_WORKING_BYTES = 256 * 1024 * 1024;
const TARGETED_GET_WINDOW = 4;
const ENTITY_SEED_FIELDS = ["id", "name", "stale", "sourceObservationIds"] as const;
const TEMPORAL_SEED_FIELDS = ["id", "name", "stale"] as const;
const utf8 = new TextEncoder();

type EntitySeed = Pick<GraphNode, "id" | "name" | "stale" | "sourceObservationIds">;
type TemporalSeed = Pick<GraphNode, "id" | "name" | "stale">;

export type RetrievalNode = Pick<
  GraphNode,
  "id" | "type" | "name" | "properties" | "sourceObservationIds"
>;

export type RetrievalEdge = Pick<
  GraphEdge,
  "type" | "sourceNodeId" | "targetNodeId" | "weight" | "tvalid"
> & { context?: { reasoning?: string } };

export interface GraphRetrievalSubgraph {
  nodes: Map<string, RetrievalNode>;
  entityStarts: RetrievalNode[];
  observationStarts: RetrievalNode[];
  edges: RetrievalEdge[];
  budget: GraphRetrievalBudget;
}

export class GraphRetrievalResourceError extends Error {
  readonly code = "GRAPH_RETRIEVAL_RESOURCE_LIMIT";

  constructor() {
    super(
      "Graph retrieval exceeded its 64 MiB serialized or 256 MiB estimated working-set limit.",
    );
    this.name = "GraphRetrievalResourceError";
  }
}

export class GraphRetrievalBudget {
  private serializedGraphBytes = 0;
  private estimatedWorkingBytes = 0;

  retainGraphValue(value: unknown, structuralBytes: number): void {
    const serialized = JSON.stringify(value);
    if (serialized === undefined) throw new GraphRetrievalResourceError();
    const bytes = utf8.encode(serialized).byteLength;
    this.serializedGraphBytes += bytes;
    this.estimatedWorkingBytes += bytes * 2 + structuralBytes;
    this.enforceLimits();
  }

  retainStructure(serializedBytes: number, structuralBytes: number): void {
    this.estimatedWorkingBytes += serializedBytes + structuralBytes;
    this.enforceLimits();
  }

  retainResult(observationId: string, graphContext: string): void {
    const bytes = utf8.encode(observationId).byteLength +
      utf8.encode(graphContext).byteLength;
    this.estimatedWorkingBytes += bytes * 2 + 256;
    this.enforceLimits();
  }

  private enforceLimits(): void {
    if (
      this.serializedGraphBytes > MAX_SERIALIZED_GRAPH_BYTES ||
      this.estimatedWorkingBytes > MAX_ESTIMATED_WORKING_BYTES
    ) {
      throw new GraphRetrievalResourceError();
    }
  }
}

export interface GraphRetrievalReadRequest {
  entityNames: string[];
  observationIds: string[];
  entityDepth: number;
  observationDepth: number;
}

const retrievalTails = new WeakMap<StateKV, Promise<void>>();
const retrievalAdmissions = new WeakMap<StateKV, number>();

export async function withSerializedGraphRetrieval<T>(
  kv: StateKV,
  retrieval: () => Promise<T>,
): Promise<T> {
  const admitted = retrievalAdmissions.get(kv) ?? 0;
  if (kv.indexedRetrieval && admitted >= 8) throw new GraphRetrievalResourceError();
  retrievalAdmissions.set(kv, admitted + 1);
  const previous = retrievalTails.get(kv) ?? Promise.resolve();
  let release!: () => void;
  const current = new Promise<void>((resolve) => {
    release = resolve;
  });
  retrievalTails.set(kv, current);
  await previous;
  try {
    return await retrieval();
  } finally {
    retrievalAdmissions.set(kv, (retrievalAdmissions.get(kv) ?? 1) - 1);
    release();
    if (retrievalTails.get(kv) === current) retrievalTails.delete(kv);
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  throw signal.reason instanceof Error
    ? signal.reason
    : new Error("Graph retrieval was cancelled.");
}

function projectNode(node: GraphNode): RetrievalNode {
  return {
    id: node.id,
    type: node.type,
    name: node.name,
    properties: Object.fromEntries(Object.entries(node.properties).slice(0, 3)),
    sourceObservationIds: [...node.sourceObservationIds],
  };
}

function projectEdge(edge: GraphEdge): RetrievalEdge {
  return {
    type: edge.type,
    sourceNodeId: edge.sourceNodeId,
    targetNodeId: edge.targetNodeId,
    weight: edge.weight,
    tvalid: edge.tvalid,
    ...(edge.context?.reasoning
      ? { context: { reasoning: edge.context.reasoning } }
      : {}),
  };
}

function matchesEntity(nodeName: string, entityNames: string[]): boolean {
  const name = nodeName.toLowerCase();
  return entityNames.some((entity) => {
    const normalized = entity.toLowerCase();
    return name.includes(normalized) || normalized.includes(name);
  });
}

class GraphRetrievalNodeResolutionError extends Error {
  readonly code = "GRAPH_RETRIEVAL_NODE_RESOLUTION_FAILED";

  constructor(nodeId: string, reason: string) {
    super(`Graph retrieval could not resolve selected node ${nodeId}: ${reason}.`);
    this.name = "GraphRetrievalNodeResolutionError";
  }
}

async function fetchNodes(
  kv: StateKV,
  ids: string[],
  onNode: (index: number, node: GraphNode) => void,
  signal?: AbortSignal,
  failOnMissingOrStale = false,
): Promise<number> {
  let missingNodeCount = 0;
  for (let offset = 0; offset < ids.length; offset += TARGETED_GET_WINDOW) {
    throwIfAborted(signal);
    const batch = ids.slice(offset, offset + TARGETED_GET_WINDOW);
    const settled = await Promise.allSettled(
      batch.map(async (requestedId) => ({
        requestedId,
        node: await kv.get<GraphNode>(KV.graphNodes, requestedId),
      })),
    );
    throwIfAborted(signal);
    let failure: unknown;
    let failed = false;
    for (let index = 0; index < settled.length; index++) {
      const result = settled[index];
      throwIfAborted(signal);
      if (result.status === "rejected") {
        failed = true;
        failure ??= result.reason;
        continue;
      }
      const { requestedId, node } = result.value;
      if (!node) {
        if (failOnMissingOrStale) {
          failed = true;
          failure ??= new GraphRetrievalNodeResolutionError(requestedId, "the record is missing");
        } else {
          missingNodeCount += 1;
        }
        continue;
      }
      if (node.id !== requestedId) {
        failed = true;
        failure ??= new GraphRetrievalNodeResolutionError(requestedId, "the record identity changed");
        continue;
      }
      if (node.stale) {
        if (failOnMissingOrStale) {
          failed = true;
          failure ??= new GraphRetrievalNodeResolutionError(requestedId, "the selected record became stale");
        }
        continue;
      }
      onNode(offset + index, node);
    }
    if (failed) throw failure;
    throwIfAborted(signal);
  }
  return missingNodeCount;
}

export class GraphRetrievalReader {
  constructor(private readonly kv: StateKV) {}

  async readSubgraph(
    request: GraphRetrievalReadRequest,
    signal?: AbortSignal,
  ): Promise<GraphRetrievalSubgraph> {
    let missingExpansionNeighborCount = 0;
    const subgraph = await withCompletedGraphRead(this.kv, async () => {
      const budget = new GraphRetrievalBudget();
      const nodes = new Map<string, RetrievalNode>();
      const entityStarts: RetrievalNode[] = [];
      const observationStarts: RetrievalNode[] = [];
      const wantedObservations = new Set(request.observationIds);
      for (const id of wantedObservations) {
        budget.retainStructure(utf8.encode(id).byteLength, 128);
      }
      for (const name of request.entityNames) {
        budget.retainStructure(utf8.encode(name).byteLength, 128);
      }

      const seeds: Array<{ id: string; entity: boolean; observation: boolean }> = [];
      const seedIds: string[] = [];
      if (this.kv.indexedRetrieval) {
        const requests = [
          ...chunks(request.entityNames).map(entity_names => ({ action: 'graph_seeds', entity_names, observation_ids: [], match: 'substring' })),
          ...chunks(request.observationIds).map(observation_ids => ({ action: 'graph_seeds', entity_names: [], observation_ids, match: 'substring' })),
        ];
        for (const seed of await indexedGraphItems<IndexedSeed>(this.kv, requests, signal)) {
          budget.retainStructure(utf8.encode(seed.id).byteLength, 128);
          seeds.push(seed);
          seedIds.push(seed.id);
        }
      } else {
      for await (const page of this.kv.pages<EntitySeed>(KV.graphNodes, {
        fields: [...ENTITY_SEED_FIELDS],
      })) {
        throwIfAborted(signal);
        for (const stored of page.items) {
          throwIfAborted(signal);
          if (typeof stored.id !== "string" || typeof stored.name !== "string" || !Array.isArray(stored.sourceObservationIds)) {
            throw new Error("Graph retrieval received an incomplete projected graph node.");
          }
          if (stored.stale) continue;
          const entityMatch = request.entityNames.length > 0 &&
            matchesEntity(stored.name, request.entityNames);
          const observationMatch = wantedObservations.size > 0 &&
            stored.sourceObservationIds.some((id) => wantedObservations.has(id));
          if (!entityMatch && !observationMatch) continue;
          budget.retainStructure(utf8.encode(stored.id).byteLength, 128);
          seeds.push({ id: stored.id, entity: entityMatch, observation: observationMatch });
          seedIds.push(stored.id);
        }
      }
      }

      await fetchNodes(
        this.kv,
        seedIds,
        (index, selected) => {
          const seed = seeds[index];
          if (!seed) throw new Error("Graph retrieval lost the selected node order.");
          const node = projectNode(selected);
          this.retainNode(nodes, node, budget);
          if (seed.entity) {
            entityStarts.push(node);
            budget.retainStructure(0, 32);
          }
          if (seed.observation) {
            observationStarts.push(node);
            budget.retainStructure(0, 32);
          }
        },
        signal,
        true,
      );

      const depth = Math.max(
        entityStarts.length > 0 ? request.entityDepth : 0,
        observationStarts.length > 0 ? request.observationDepth : 0,
      );
      const checkedEntityIds = new Set(entityStarts.map((node) => node.id));
      const checkedObservationIds = new Set(
        observationStarts.map((node) => node.id),
      );
      const resolvedNodeIds = new Set(nodes.keys());
      const candidateEdges = new Map<bigint, RetrievalEdge>();
      let entityFrontier = Array.from(entityStarts, (node) => node.id);
      let observationFrontier = Array.from(observationStarts, (node) => node.id);

      for (let level = 0; level < depth; level++) {
        throwIfAborted(signal);
        const activeEntityFrontier = level < request.entityDepth
          ? new Set(entityFrontier)
          : new Set<string>();
        const activeObservationFrontier = level < request.observationDepth
          ? new Set(observationFrontier)
          : new Set<string>();
        const frontierIds = new Set([
          ...activeEntityFrontier,
          ...activeObservationFrontier,
        ]);
        if (frontierIds.size === 0) continue;
        const entityCandidates = new Set<string>();
        const observationCandidates = new Set<string>();
        const candidateIds = new Set<string>();
        let ordinal = 0;

        for await (const page of this.edgePages([...frontierIds], budget, signal)) {
          throwIfAborted(signal);
          let pageIndex = 0;
          for (const stored of page.items) {
            throwIfAborted(signal);
            const position = BigInt(page.positions?.[pageIndex++] ?? ordinal++);
            if (stored.stale) continue;
            const sourceInFrontier = frontierIds.has(stored.sourceNodeId);
            const targetInFrontier = frontierIds.has(stored.targetNodeId);
            if (!sourceInFrontier && !targetInFrontier) continue;
            if (!candidateEdges.has(position)) {
              const edge = projectEdge(stored);
              candidateEdges.set(position, edge);
              budget.retainGraphValue(edge, 384);
            }
            if (sourceInFrontier) {
              if (activeEntityFrontier.has(stored.sourceNodeId)) {
                this.addCandidate(
                  stored.targetNodeId,
                  checkedEntityIds,
                  entityCandidates,
                  resolvedNodeIds,
                  candidateIds,
                  budget,
                );
              }
              if (activeObservationFrontier.has(stored.sourceNodeId)) {
                this.addCandidate(
                  stored.targetNodeId,
                  checkedObservationIds,
                  observationCandidates,
                  resolvedNodeIds,
                  candidateIds,
                  budget,
                );
              }
            }
            if (targetInFrontier) {
              if (activeEntityFrontier.has(stored.targetNodeId)) {
                this.addCandidate(
                  stored.sourceNodeId,
                  checkedEntityIds,
                  entityCandidates,
                  resolvedNodeIds,
                  candidateIds,
                  budget,
                );
              }
              if (activeObservationFrontier.has(stored.targetNodeId)) {
                this.addCandidate(
                  stored.sourceNodeId,
                  checkedObservationIds,
                  observationCandidates,
                  resolvedNodeIds,
                  candidateIds,
                  budget,
                );
              }
            }
          }
        }

          missingExpansionNeighborCount += await fetchNodes(
            this.kv,
            Array.from(candidateIds),
          (_index, selected) => {
            this.retainNode(nodes, projectNode(selected), budget);
          },
          signal,
        );
        entityFrontier = Array.from(entityCandidates).filter((id) => nodes.has(id));
        observationFrontier = Array.from(observationCandidates).filter((id) => nodes.has(id));
      }

        const edges = Array.from(candidateEdges.entries())
          .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
          .map(([, edge]) => edge)
          .filter((edge) => nodes.has(edge.sourceNodeId) && nodes.has(edge.targetNodeId));
        return { nodes, entityStarts, observationStarts, edges, budget };
      });
    if (missingExpansionNeighborCount > 0) {
      console.warn(
        `Graph retrieval omitted ${missingExpansionNeighborCount} missing expansion neighbor(s) and their incident edges.`,
      );
    }
    return subgraph;
  }

  async readTemporalEntity(
    entityName: string,
    signal?: AbortSignal,
  ): Promise<{ entity: GraphNode | null; edges: GraphEdge[] }> {
    return withCompletedGraphRead(this.kv, async () => {
      const budget = new GraphRetrievalBudget();
      let entityId: string | null = null;
      if (this.kv.indexedRetrieval) {
        const seeds = await indexedGraphItems<IndexedSeed>(this.kv, [{ action: 'graph_seeds', entity_names: [entityName], observation_ids: [], match: 'exact' }], signal);
        entityId = seeds[0]?.id ?? null;
      } else {
      for await (const page of this.kv.pages<TemporalSeed>(KV.graphNodes, {
        fields: [...TEMPORAL_SEED_FIELDS],
      })) {
        throwIfAborted(signal);
        for (const seed of page.items) {
          throwIfAborted(signal);
          if (typeof seed.id !== "string" || typeof seed.name !== "string") {
            throw new Error("Graph retrieval received an incomplete projected temporal node.");
          }
          if (seed.stale || seed.name.toLowerCase() !== entityName.toLowerCase()) continue;
          entityId = seed.id;
          break;
        }
        if (entityId !== null) break;
      }
      }
      if (entityId === null) return { entity: null, edges: [] };
      let entity: GraphNode | undefined;
      await fetchNodes(
        this.kv,
        [entityId],
        (_index, selected) => { entity = selected; },
        signal,
        true,
      );
      if (!entity) throw new GraphRetrievalNodeResolutionError(entityId, "the selected record is missing");
      if (entity.name.toLowerCase() !== entityName.toLowerCase()) {
        throw new GraphRetrievalNodeResolutionError(entityId, "the selected record name changed");
      }
      budget.retainGraphValue(entity, 1024);

      const edges: GraphEdge[] = [];
      for await (const page of this.edgePages([entity.id], budget, signal)) {
        throwIfAborted(signal);
        for (const edge of page.items) {
          throwIfAborted(signal);
          if (
            edge.stale ||
            (edge.sourceNodeId !== entity.id && edge.targetNodeId !== entity.id)
          ) {
            continue;
          }
          budget.retainGraphValue(edge, 512);
          edges.push(edge);
        }
      }
      return { entity, edges };
    });
  }

  private async *edgePages(ids: string[], budget: GraphRetrievalBudget, signal?: AbortSignal): AsyncGenerator<{ items: GraphEdge[]; positions?: string[] }> {
    if (!this.kv.indexedRetrieval) {
      yield* this.kv.pages<GraphEdge>(KV.graphEdges);
      return;
    }
    const merged = new Map<string, IndexedEdge>();
    let generation: string | undefined;
    let bytes = 0;
    const pending = chunks(ids).reverse();
    while (pending.length > 0) {
      throwIfAborted(signal);
      const nodeIds = pending.pop()!;
      let result: { items: IndexedEdge[]; generation: string };
      try {
        result = await retrievalRequest(this.kv, {
          action: 'graph_edges', node_ids: nodeIds,
          max_items: 16_384, max_bytes: 8 * 1024 * 1024,
        });
      } catch (error) {
        throwIfAborted(signal);
        if (!(error instanceof IndexedRetrievalError) || error.code !== 'STATE_RETRIEVAL_RESOURCE_LIMIT' || nodeIds.length < 2) throw error;
        const middle = Math.floor(nodeIds.length / 2);
        pending.push(nodeIds.slice(middle), nodeIds.slice(0, middle));
        continue;
      }
      throwIfAborted(signal);
      if (!Array.isArray(result.items) || typeof result.generation !== 'string') throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid indexed graph response.');
      if (generation !== undefined && generation !== result.generation) throw new IndexedRetrievalError('STATE_GRAPH_RECOVERY_REQUIRED', 'Graph generation changed between indexed requests.');
      generation = result.generation;
      for (const item of result.items) {
        throwIfAborted(signal);
        if (typeof item.key !== 'string' || !/^\d+$/.test(item.position)) throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid indexed insertion position.');
        if (merged.has(item.key)) continue;
        const itemBytes = Buffer.byteLength(JSON.stringify(item));
        bytes += itemBytes;
        if (bytes > MAX_SERIALIZED_GRAPH_BYTES) throw new IndexedRetrievalError('STATE_RETRIEVAL_RESOURCE_LIMIT', 'Indexed graph response exceeds 64 MiB.');
        budget.retainStructure(itemBytes * 2, 384);
        merged.set(item.key, item);
      }
    }
    throwIfAborted(signal);
    const items = [...merged.values()].sort(comparePositions);
    yield { items: items.map(item => item.value), positions: items.map(item => item.position) };
  }

  private retainNode(
    nodes: Map<string, RetrievalNode>,
    node: RetrievalNode,
    budget: GraphRetrievalBudget,
  ): void {
    if (nodes.has(node.id)) return;
    budget.retainGraphValue(node, 768);
    budget.retainStructure(utf8.encode(node.id).byteLength, 128);
    nodes.set(node.id, node);
  }

  private addCandidate(
    id: string,
    checked: Set<string>,
    candidates: Set<string>,
    resolved: Set<string>,
    toFetch: Set<string>,
    budget: GraphRetrievalBudget,
  ): void {
    if (checked.has(id)) return;
    checked.add(id);
    candidates.add(id);
    budget.retainStructure(utf8.encode(id).byteLength, 128);
    if (!resolved.has(id)) {
      resolved.add(id);
      toFetch.add(id);
    }
  }
}
