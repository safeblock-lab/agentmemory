import type { GraphBrowsePreparation, GraphBrowseProgress, GraphDisplayEdge, GraphEdge, GraphNode } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { withCompletedGraphRead } from "./graph-jobs.js";
import { IndexedRetrievalError, retrievalRequest, chunks, comparePositions, type IndexedSeed, type IndexedEdge } from "../state/indexed-retrieval.js";

export type ScopedGraphMode = "neighbors" | "history" | "time" | "differential" | "top_n" | "aggregate" | "search" | "browse";

export interface ScopedGraphReadRequest {
  project: string;
  mode: ScopedGraphMode;
  node_ids?: string[];
  edge_ids?: string[];
  observation_ids?: string[];
  query?: string;
  from_time?: string;
  through_time?: string;
  since_revision?: string | number;
  cursor?: string;
  limit?: number;
  max_bytes?: number;
  top_n?: number;
}

export interface ScopedGraphReadPage<T = unknown> {
  status: "ready" | "partial" | "waiting_scope" | "preparing" | "error";
  reason?: string;
  project: string;
  items: T[];
  aggregate?: unknown;
  next_cursor?: string;
  has_more: boolean;
  revision?: unknown;
  approximate?: boolean;
  edges?: GraphDisplayEdge[];
  edgeScope?: "page";
  crossPageEdges?: "not_loaded";
  edgesTruncated?: boolean;
  progress?: GraphBrowseProgress;
}

const SCOPED_GRAPH_MAX_IDS = 12;
const SCOPED_GRAPH_MAX_ITEMS = 16;
const SCOPED_GRAPH_MAX_BYTES = 64 * 1024;

export async function readScopedGraphPage<T = unknown>(
  kv: StateKV,
  request: ScopedGraphReadRequest,
): Promise<ScopedGraphReadPage<T>> {
  const project = request.project.trim();
  if (!project) return {
    status: "waiting_scope", reason: "project_required", project: "", items: [], has_more: false,
  };
  if (kv.indexedRetrieval === false && request.mode === "browse") return {
    status: "preparing", reason: "index_not_ready", project, items: [], edges: [], has_more: false,
    revision: null, edgeScope: "page", crossPageEdges: "not_loaded", edgesTruncated: false,
  };
  if (kv.indexedRetrieval === false) return {
    status: "partial", reason: "index_not_ready", project, items: [], has_more: false,
  };
  const nodeIds = request.node_ids;
  const edgeIds = request.edge_ids;
  const observationIds = request.observation_ids;
  for (const ids of [nodeIds, edgeIds, observationIds]) {
    if (ids !== undefined && (!Array.isArray(ids) || ids.length > SCOPED_GRAPH_MAX_IDS ||
      ids.some((id) => typeof id !== "string" || !id.trim()))) {
      throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Scoped graph selectors must contain at most twelve exact IDs.");
    }
  }
  if (request.mode === "search" &&
    ((typeof request.query !== "string" || !request.query.trim()) &&
      (!observationIds || observationIds.length === 0))) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Scoped graph search requires query text or exact observation IDs.");
  }
  if (request.query !== undefined && Buffer.byteLength(request.query, "utf8") > 512) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Scoped graph query exceeds 512 UTF-8 bytes.");
  }
  const itemLimit = request.mode === "browse" ? 128 : SCOPED_GRAPH_MAX_ITEMS;
  const limit = request.limit ?? itemLimit;
  const maxBytes = request.max_bytes ?? SCOPED_GRAPH_MAX_BYTES;
  if (!Number.isInteger(limit) || limit < 1 || limit > itemLimit ||
    !Number.isInteger(maxBytes) || maxBytes < 1024 || maxBytes > SCOPED_GRAPH_MAX_BYTES) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Scoped graph page exceeds its native item or byte limit.");
  }
  const payload: Record<string, unknown> = {
    action: "graph_read",
    project,
    mode: request.mode,
    limit,
    max_bytes: maxBytes,
  };
  for (const key of ["node_ids", "edge_ids", "observation_ids", "query", "from_time", "through_time", "since_revision", "cursor", "top_n"] as const) {
    const value = request[key];
    if (value !== undefined) payload[key] = value;
  }
  const response = await retrievalRequest<ScopedGraphReadPage<T>>(kv, payload);
  if (response?.reason === null) delete response.reason;
  if (!response || typeof response !== "object" || !Array.isArray(response.items) ||
    response.project !== project ||
    !(request.mode === "browse" ? ["ready", "preparing", "error"] : ["ready", "partial", "waiting_scope"]).includes(response.status) ||
    typeof response.has_more !== "boolean") {
    throw new IndexedRetrievalError("STATE_TX_INVALID_RESPONSE", "Native scoped graph retrieval returned an invalid page.");
  }
  if (request.mode === "browse") validateBrowsePage(response, request.cursor);
  if (response.items.length > limit || Buffer.byteLength(JSON.stringify(response), "utf8") > maxBytes) {
    throw new IndexedRetrievalError("STATE_RETRIEVAL_RESOURCE_LIMIT", "Native scoped graph response exceeded its declared page budget.");
  }
  return response;
}

function validBrowseProgress(value: GraphBrowseProgress): boolean {
  return value !== null && typeof value === "object" && typeof value.phase === "string" &&
    value.phase.length > 0 && Number.isSafeInteger(value.processed) && value.processed >= 0;
}

function validateBrowsePage<T>(page: ScopedGraphReadPage<T>, cursor?: string): void {
  const ids = new Set<string>();
  const validNodes = page.items.every(item => {
    if (!item || typeof item !== "object") return false;
    const node = item as Record<string, unknown>;
    if (typeof node.id !== "string" || !node.id || ids.has(node.id) || typeof node.type !== "string" ||
      typeof node.name !== "string" || Array.from(node.name).length > 256) return false;
    ids.add(node.id);
    return true;
  });
  if (!validNodes || !Array.isArray(page.edges) || page.edges.length > 256 ||
    page.edges.some(edge => !edge || typeof edge.id !== "string" || typeof edge.type !== "string" ||
      !ids.has(edge.sourceNodeId) || !ids.has(edge.targetNodeId) || !Number.isFinite(edge.weight)) ||
    page.edgeScope !== "page" || page.crossPageEdges !== "not_loaded" || typeof page.edgesTruncated !== "boolean" ||
    page.revision !== null && typeof page.revision !== "string" ||
    page.reason !== undefined && typeof page.reason !== "string" ||
    page.progress !== undefined && !validBrowseProgress(page.progress) ||
    page.has_more && (page.status !== "ready" || page.items.length === 0 || typeof page.next_cursor !== "string" ||
      !page.next_cursor || page.next_cursor === cursor || Buffer.byteLength(page.next_cursor) > 4096) ||
    !page.has_more && page.next_cursor != null ||
    page.status !== "ready" && (page.items.length > 0 || page.edges.length > 0)) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_RESPONSE", "Native graph Browse returned an invalid display page.");
  }
}

const GRAPH_PREPARATION_ROWS = 256;
const GRAPH_PREPARATION_BYTES = 1024 * 1024;

export interface GraphBrowseWorkOptions { maxWorkBytes?: number }
export type BoundedGraphBrowsePreparation = GraphBrowsePreparation & { required_bytes?: number };

export async function prepareGraphBrowse(kv: StateKV, project: string, options?: GraphBrowseWorkOptions): Promise<BoundedGraphBrowsePreparation> {
  const maxWorkBytes = options?.maxWorkBytes ?? GRAPH_PREPARATION_BYTES;
  if (!Number.isSafeInteger(maxWorkBytes) || maxWorkBytes < 1024 || maxWorkBytes > 160 * 1024 * 1024) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_REQUEST", "Graph preparation work budget is outside its native limit.");
  }
  const result = await retrievalRequest<BoundedGraphBrowsePreparation & { scope: string; project: string }>(kv, {
    action: "index_prepare", scope: "mem:graph:browse", project, max_rows: GRAPH_PREPARATION_ROWS, max_bytes: maxWorkBytes,
  }, { timeoutMs: 310_000 });
  if (result.reason === null) delete result.reason;
  if (result.scope !== "mem:graph:browse" || result.project !== project ||
    !["ready", "preparing", "error"].includes(result.status) || !validBrowseProgress(result) ||
    result.processed > GRAPH_PREPARATION_ROWS ||
    typeof result.continue !== "boolean" || result.continue && result.status !== "preparing" ||
    result.revision !== null && typeof result.revision !== "string" ||
    result.reason !== undefined && typeof result.reason !== "string" ||
    result.required_bytes !== undefined && (!Number.isSafeInteger(result.required_bytes) || result.required_bytes < 1) ||
    Buffer.byteLength(JSON.stringify(result)) > SCOPED_GRAPH_MAX_BYTES) {
    throw new IndexedRetrievalError("STATE_TX_INVALID_RESPONSE", "Native graph Browse preparation returned an invalid batch.");
  }
  return {
    status: result.status, phase: result.phase, processed: result.processed, revision: result.revision,
    continue: result.continue, ...(result.reason ? { reason: result.reason } : {}),
    ...(result.required_bytes !== undefined ? { required_bytes: result.required_bytes } : {}),
  };
}

const MAX_SERIALIZED_GRAPH_BYTES = 64 * 1024 * 1024;
const MAX_ESTIMATED_WORKING_BYTES = 256 * 1024 * 1024;
const TARGETED_GET_WINDOW = 4;
const utf8 = new TextEncoder();

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
  partial: boolean;
  reason?: string;
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
  project?: string;
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

async function indexedGraphItems<T extends { key: string; position: string }>(
  kv: StateKV, requests: Record<string, unknown>[], signal?: AbortSignal, retain?: (bytes: number) => void,
): Promise<T[]> {
  const merged = new Map<string, T>();
  let generation: string | undefined;
  let bytes = 0;
  for (const payload of requests) {
    signal?.throwIfAborted();
    const result = await retrievalRequest<{ items: T[]; generation: string }>(kv, {
      ...payload, max_items: 16_384, max_bytes: 8 * 1024 * 1024,
    });
    if (!Array.isArray(result.items) || typeof result.generation !== 'string') throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid indexed graph response.');
    if (generation !== undefined && generation !== result.generation) throw new IndexedRetrievalError('STATE_GRAPH_RECOVERY_REQUIRED', 'Graph generation changed between indexed requests.');
    generation = result.generation;
    for (const item of result.items) {
      if (typeof item.key !== 'string' || !/^\d+$/.test(item.position)) throw new IndexedRetrievalError('STATE_TX_INVALID_REQUEST', 'Invalid indexed insertion position.');
      const previous = merged.get(item.key);
      if (previous) {
        for (const field of ['entity', 'observation'] as const) {
          if (field in previous && field in item) (previous as unknown as Record<string, unknown>)[field] ||= (item as unknown as Record<string, unknown>)[field];
        }
        continue;
      }
      const itemBytes = Buffer.byteLength(JSON.stringify(item));
      bytes += itemBytes;
      if (bytes > 64 * 1024 * 1024) throw new IndexedRetrievalError('STATE_RETRIEVAL_RESOURCE_LIMIT', 'Indexed graph response exceeds 64 MiB.');
      retain?.(itemBytes);
      merged.set(item.key, item);
    }
  }
  signal?.throwIfAborted();
  return [...merged.values()].sort(comparePositions);
}

export class GraphRetrievalReader {
  constructor(private readonly kv: StateKV) {}

  async readSubgraph(
    request: GraphRetrievalReadRequest,
    signal?: AbortSignal,
  ): Promise<GraphRetrievalSubgraph> {
    if (request.project === undefined && this.kv.indexedRetrieval) return this.readGlobalSubgraph(request, signal);
    return withCompletedGraphRead(this.kv, async () => {
      const budget = new GraphRetrievalBudget();
      const nodes = new Map<string, RetrievalNode>();
      const entityStarts: RetrievalNode[] = [];
      const observationStarts: RetrievalNode[] = [];
      let partial = !request.project?.trim();
      let reason = partial ? "project_required" : undefined;
      const empty = (): GraphRetrievalSubgraph => ({ nodes, entityStarts, observationStarts, edges: [], budget, partial, reason });
      const project = request.project?.trim();
      if (!project || !this.kv.indexedRetrieval) {
        return { ...empty(), partial: true, reason: reason ?? "index_not_ready" };
      }

      const MAX_SEEDS = 16;
      const MAX_NODES = 64;
      const MAX_EDGES = 64;
      const MAX_PAGES_PER_SELECTOR = 4;
      const seedFlags = new Map<string, { entity: boolean; observation: boolean }>();
      const addSeeds = (items: readonly { id?: string; key?: string; entity?: boolean; observation?: boolean }[]) => {
        for (const item of items) {
          const id = typeof item.id === "string" ? item.id : item.key;
          if (!id) continue;
          const prior = seedFlags.get(id);
          seedFlags.set(id, {
            entity: Boolean(prior?.entity || item.entity),
            observation: Boolean(prior?.observation || item.observation),
          });
          if (seedFlags.size >= MAX_SEEDS) break;
        }
      };

      for (const name of request.entityNames.slice(0, 8)) {
        throwIfAborted(signal);
        if (seedFlags.size >= MAX_SEEDS) { partial = true; break; }
        if (Buffer.byteLength(name, "utf8") > 512) { partial = true; reason ??= "query_too_large"; continue; }
        let cursor: string | undefined;
        for (let pageIndex = 0; pageIndex < MAX_PAGES_PER_SELECTOR && seedFlags.size < MAX_SEEDS; pageIndex++) {
          const page = await readScopedGraphPage<{ id?: string; key?: string; entity?: boolean; observation?: boolean }>(this.kv, {
            project, mode: "search", query: name, limit: 16, max_bytes: 64 * 1024,
            ...(cursor ? { cursor } : {}),
          });
          if (page.status !== "ready" || page.has_more) { partial = true; reason ??= page.reason; }
          addSeeds(page.items);
          if (!page.has_more || !page.next_cursor) break;
          cursor = page.next_cursor;
          if (pageIndex === MAX_PAGES_PER_SELECTOR - 1) partial = true;
        }
      }

      const observationIds = request.observationIds.slice(0, 12);
      if (request.observationIds.length > observationIds.length) partial = true;
      if (observationIds.length > 0 && seedFlags.size < MAX_SEEDS) {
        let cursor: string | undefined;
        for (let pageIndex = 0; pageIndex < MAX_PAGES_PER_SELECTOR && seedFlags.size < MAX_SEEDS; pageIndex++) {
          const page = await readScopedGraphPage<{ id?: string; key?: string; entity?: boolean; observation?: boolean }>(this.kv, {
            project, mode: "search", observation_ids: observationIds, limit: 16, max_bytes: 64 * 1024,
            ...(cursor ? { cursor } : {}),
          });
          if (page.status !== "ready" || page.has_more) { partial = true; reason ??= page.reason; }
          addSeeds(page.items);
          if (!page.has_more || !page.next_cursor) break;
          cursor = page.next_cursor;
          if (pageIndex === MAX_PAGES_PER_SELECTOR - 1) partial = true;
        }
      }
      if (request.entityNames.length > 8) partial = true;

      const seedEntries = [...seedFlags.entries()];
      const seedIds = seedEntries.map(([id]) => id);
      const missingSeeds = await fetchNodes(this.kv, seedIds, (index, selected) => {
        const flags = seedEntries[index]?.[1];
        if (!flags) throw new Error("Graph retrieval lost its bounded seed order.");
        const node = projectNode(selected);
        this.retainNode(nodes, node, budget);
        if (flags.entity) entityStarts.push(node);
        if (flags.observation) observationStarts.push(node);
      }, signal);
      if (missingSeeds > 0 || seedEntries.length !== seedIds.length || nodes.size !== seedIds.length) {
        partial = true;
        reason ??= "selected_nodes_incomplete";
      }

      const edges = new Map<string, RetrievalEdge>();
      const maxDepth = Math.max(request.entityDepth, request.observationDepth);
      let frontier = new Map<string, number>();
      for (const node of entityStarts) if (request.entityDepth > 0) frontier.set(node.id, request.entityDepth);
      for (const node of observationStarts) if (request.observationDepth > (frontier.get(node.id) ?? 0)) frontier.set(node.id, request.observationDepth);
      for (let depth = 0; depth < maxDepth && frontier.size > 0 && nodes.size < MAX_NODES && edges.size < MAX_EDGES; depth++) {
        throwIfAborted(signal);
        const currentIds = [...frontier.keys()];
        const nextFrontier = new Map<string, number>();
        for (let offset = 0; offset < currentIds.length && nodes.size < MAX_NODES && edges.size < MAX_EDGES; offset += 12) {
          const batch = currentIds.slice(offset, offset + 12);
          let cursor: string | undefined;
          for (let pageIndex = 0; pageIndex < MAX_PAGES_PER_SELECTOR && edges.size < MAX_EDGES; pageIndex++) {
            const page = await readScopedGraphPage<{ id?: string; key?: string; value?: GraphEdge }>(this.kv, {
              project, mode: "neighbors", node_ids: batch, limit: 16, max_bytes: 64 * 1024,
              ...(cursor ? { cursor } : {}),
            });
            if (page.status !== "ready" || page.has_more) { partial = true; reason ??= page.reason; }
            const neighbors = new Set<string>();
            for (const item of page.items) {
              const record = item && typeof item === "object" ? item as { id?: string; key?: string; value?: GraphEdge } : {};
              const edge = record.value;
              if (!edge || typeof edge.id !== "string" || typeof edge.sourceNodeId !== "string" ||
                typeof edge.targetNodeId !== "string" || edge.stale) { partial = true; continue; }
              if (!batch.some((id) => id === edge.sourceNodeId || id === edge.targetNodeId)) continue;
              if (!edges.has(edge.id)) {
                const projected = projectEdge(edge);
                budget.retainGraphValue(projected, 384);
                edges.set(edge.id, projected);
              }
              for (const id of [edge.sourceNodeId, edge.targetNodeId]) {
                if (batch.includes(id) || nodes.has(id)) continue;
                const parentId = edge.sourceNodeId === id ? edge.targetNodeId : edge.sourceNodeId;
                const remaining = Math.max(0, (frontier.get(parentId) ?? 0) - 1);
                if (remaining > (nextFrontier.get(id) ?? 0)) nextFrontier.set(id, remaining);
                neighbors.add(id);
              }
              if (edges.size >= MAX_EDGES) { partial = true; break; }
            }
            const candidateIds = [...neighbors].slice(0, Math.max(0, MAX_NODES - nodes.size));
            if (candidateIds.length < neighbors.size) partial = true;
            const missingNeighbors = await fetchNodes(this.kv, candidateIds, (_index, selected) => {
              this.retainNode(nodes, projectNode(selected), budget);
            }, signal);
            if (missingNeighbors > 0) { partial = true; reason ??= "neighbor_nodes_incomplete"; }
            if (!page.has_more || !page.next_cursor) break;
            cursor = page.next_cursor;
            if (pageIndex === MAX_PAGES_PER_SELECTOR - 1) partial = true;
          }
        }
        frontier = new Map([...nextFrontier].filter(([id, remaining]) => nodes.has(id) && remaining > 0));
      }
      if (frontier.size > 0 || nodes.size >= MAX_NODES || edges.size >= MAX_EDGES) partial = true;
      const visibleEdges = [...edges.values()].filter((edge) => nodes.has(edge.sourceNodeId) && nodes.has(edge.targetNodeId));
      return { nodes, entityStarts, observationStarts, edges: visibleEdges, budget, partial, ...(reason ? { reason } : {}) };
    });
  }

  async readTemporalEntity(
    entityName: string,
    project: string | undefined,
    signal?: AbortSignal,
  ): Promise<{ entity: GraphNode | null; edges: GraphEdge[]; partial: boolean; reason?: string }> {
    if (project === undefined && this.kv.indexedRetrieval) return this.readGlobalTemporalEntity(entityName, signal);
    return withCompletedGraphRead(this.kv, async () => {
      const normalizedProject = project?.trim();
      if (!normalizedProject || !this.kv.indexedRetrieval) {
        return { entity: null, edges: [], partial: true, reason: normalizedProject ? "index_not_ready" : "project_required" };
      }
      if (Buffer.byteLength(entityName, "utf8") > 512) {
        return { entity: null, edges: [], partial: true, reason: "query_too_large" };
      }
      const budget = new GraphRetrievalBudget();
      let partial = false;
      let reason: string | undefined;
      const candidates = await readScopedGraphPage<{ id?: string; key?: string }>(this.kv, {
        project: normalizedProject, mode: "search", query: entityName, limit: 16, max_bytes: 64 * 1024,
      },);
      if (candidates.status !== "ready" || candidates.has_more) { partial = true; reason = candidates.reason; }
      const ids = candidates.items.map((item) => typeof item.id === "string" ? item.id : item.key).filter((id): id is string => Boolean(id));
      let entity: GraphNode | undefined;
      for (const id of ids) {
        throwIfAborted(signal);
        const candidate = await this.kv.get<GraphNode>(KV.graphNodes, id);
        if (!candidate || candidate.id !== id || candidate.stale) { partial = true; continue; }
        if (candidate.name.toLowerCase() === entityName.toLowerCase() || candidate.aliases?.some((alias) => alias.toLowerCase() === entityName.toLowerCase())) {
          entity = candidate;
          break;
        }
      }
      if (!entity) return { entity: null, edges: [], partial: true, reason: reason ?? "entity_not_found" };
      budget.retainGraphValue(entity, 1024);
      const edgePage = await readScopedGraphPage<{ value?: GraphEdge }>(this.kv, {
        project: normalizedProject, mode: "neighbors", node_ids: [entity.id], limit: 16, max_bytes: 64 * 1024,
      });
      if (edgePage.status !== "ready" || edgePage.has_more) { partial = true; reason ??= edgePage.reason; }
      const edges = edgePage.items.map((item) => item.value).filter((edge): edge is GraphEdge => Boolean(edge) &&
        !edge!.stale && (edge!.sourceNodeId === entity!.id || edge!.targetNodeId === entity!.id));
      for (const edge of edges) budget.retainGraphValue(edge, 512);
      const history = await readScopedGraphPage<{ value?: GraphEdge }>(this.kv, {
        project: normalizedProject, mode: "history", edge_ids: edges.slice(0, 12).map((edge) => edge.id), limit: 16, max_bytes: 64 * 1024,
      });
      if (history.status !== "ready" || history.has_more) { partial = true; reason ??= history.reason ?? "history_unavailable"; }
      for (const item of history.items) {
        const edge = item.value;
        if (!edge || edge.stale || edges.some((current) => current.id === edge.id)) continue;
        edges.push(edge);
        budget.retainGraphValue(edge, 512);
      }
      return { entity, edges, partial, ...(reason ? { reason } : {}) };
    });
  }
  private async readGlobalSubgraph(
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
        return { nodes, entityStarts, observationStarts, edges, budget, partial: false };
      });
    if (missingExpansionNeighborCount > 0) {
      console.warn(
        `Graph retrieval omitted ${missingExpansionNeighborCount} missing expansion neighbor(s) and their incident edges.`,
      );
    }
    return subgraph;
  }

  private async readGlobalTemporalEntity(
    entityName: string,
    signal?: AbortSignal,
  ): Promise<{ entity: GraphNode | null; edges: GraphEdge[]; partial: boolean }> {
    return withCompletedGraphRead(this.kv, async () => {
      const budget = new GraphRetrievalBudget();
      let entityId: string | null = null;
      if (this.kv.indexedRetrieval) {
        const seeds = await indexedGraphItems<IndexedSeed>(this.kv, [{ action: 'graph_seeds', entity_names: [entityName], observation_ids: [], match: 'exact' }], signal);
        entityId = seeds[0]?.id ?? null;
      }
      if (entityId === null) return { entity: null, edges: [], partial: false };
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
      return { entity, edges, partial: false };
    });
  }

  private async *edgePages(ids: string[], budget: GraphRetrievalBudget, signal?: AbortSignal): AsyncGenerator<{ items: GraphEdge[]; positions?: string[] }> {
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

}
