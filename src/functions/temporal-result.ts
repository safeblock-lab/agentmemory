import type { GraphEdge, GraphEdgeType, GraphNode } from "../types.js";
import { generateId } from "../state/schema.js";
import { parseXmlResult, type XmlElement } from "../utils/xml-result.js";

const NODE_TYPES = new Set<GraphNode["type"]>([
  "file", "function", "concept", "error", "decision", "pattern", "library",
  "person", "project", "preference", "location", "organization", "event",
]);
const EDGE_TYPES = new Set<GraphEdgeType>([
  "uses", "imports", "modifies", "causes", "fixes", "depends_on", "related_to",
  "works_at", "prefers", "blocked_by", "caused_by", "optimizes_for", "rejected",
  "avoids", "located_in", "succeeded_by",
]);
const SENTIMENTS = new Set(["positive", "negative", "neutral"]);
const MAX_RESPONSE_BYTES = 16 * 1024;

export type TemporalGraphResult =
  | { kind: "valid-empty"; nodes: []; edges: [] }
  | { kind: "valid-result"; nodes: GraphNode[]; edges: GraphEdge[] };

function invalid(): never {
  throw new Error("temporal_result_invalid");
}

function attributes(element: XmlElement, required: string[], optional: string[] = []): void {
  const allowed = new Set([...required, ...optional]);
  if (required.some((name) => typeof element.attributes[name] !== "string")
    || Object.keys(element.attributes).some((name) => !allowed.has(name))) invalid();
}

function leafText(element: XmlElement): string {
  if (element.children.length) return invalid();
  return element.text.trim();
}

function onlyWhitespace(element: XmlElement): void {
  if (element.text.trim()) invalid();
}

function uniqueChild(parent: XmlElement, name: string): XmlElement {
  const matching = parent.children.filter((child) => child.name === name);
  if (matching.length !== 1) return invalid();
  return matching[0];
}

function validDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

function parseEntity(element: XmlElement, observationIds: string[], createdAt: string): GraphNode {
  attributes(element, ["type", "name"]);
  onlyWhitespace(element);
  const type = element.attributes.type as GraphNode["type"];
  const name = element.attributes.name.trim();
  if (!NODE_TYPES.has(type) || !name) return invalid();

  const properties: Record<string, string> = {};
  const aliases: string[] = [];
  for (const child of element.children) {
    if (child.name === "property") {
      attributes(child, ["key"]);
      const key = child.attributes.key.trim();
      if (!key || Object.hasOwn(properties, key)) return invalid();
      properties[key] = leafText(child);
    } else if (child.name === "alias") {
      attributes(child, []);
      const alias = leafText(child);
      if (!alias || aliases.includes(alias)) return invalid();
      aliases.push(alias);
    } else return invalid();
  }

  return {
    id: generateId("gn"), type, name, properties, sourceObservationIds: observationIds,
    createdAt, ...(aliases.length ? { aliases } : {}),
  };
}

function parseRelationship(
  element: XmlElement,
  nodes: GraphNode[],
  observationIds: string[],
  createdAt: string,
): GraphEdge {
  attributes(element, ["type", "source", "target", "weight"], ["valid_from", "valid_to"]);
  onlyWhitespace(element);
  const type = element.attributes.type as GraphEdgeType;
  const source = element.attributes.source.trim();
  const target = element.attributes.target.trim();
  const weightText = element.attributes.weight.trim();
  const weight = Number(weightText);
  if (!EDGE_TYPES.has(type) || !source || !target || !/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(weightText)
    || !Number.isFinite(weight) || weight < 0.1 || weight > 1) return invalid();

  const validFrom = element.attributes.valid_from?.trim();
  const validTo = element.attributes.valid_to?.trim();
  if (validFrom !== undefined && validFrom !== "unknown" && !validDate(validFrom)
    || validTo !== undefined && validTo !== "current" && !validDate(validTo)) return invalid();
  const sourceNodes = nodes.filter((node) => node.name === source || node.aliases?.includes(source));
  const targetNodes = nodes.filter((node) => node.name === target || node.aliases?.includes(target));
  if (sourceNodes.length !== 1 || targetNodes.length !== 1) return invalid();
  const sourceNode = sourceNodes[0]!;
  const targetNode = targetNodes[0]!;

  let reasoning: string | undefined;
  let sawReasoning = false;
  let sentiment: string | undefined;
  const alternatives: string[] = [];
  for (const child of element.children) {
    if (child.name === "reasoning") {
      attributes(child, []);
      if (sawReasoning) return invalid();
      sawReasoning = true;
      reasoning = leafText(child) || undefined;
    } else if (child.name === "sentiment") {
      attributes(child, []);
      if (sentiment !== undefined) return invalid();
      sentiment = leafText(child);
      if (!SENTIMENTS.has(sentiment)) return invalid();
    } else if (child.name === "alternatives") {
      attributes(child, []);
      onlyWhitespace(child);
      for (const alternative of child.children) {
        if (alternative.name !== "alt") return invalid();
        attributes(alternative, []);
        const text = leafText(alternative);
        if (!text) return invalid();
        alternatives.push(text);
      }
    } else return invalid();
  }

  return {
    id: generateId("ge"), type, sourceNodeId: sourceNode.id, targetNodeId: targetNode.id,
    weight, sourceObservationIds: observationIds, createdAt, tcommit: createdAt,
    ...(validFrom && validFrom !== "unknown" ? { tvalid: validFrom } : {}),
    ...(validTo && validTo !== "current" ? { tvalidEnd: validTo } : {}),
    ...(reasoning || sentiment || alternatives.length ? { context: {
      ...(reasoning ? { reasoning } : {}), ...(sentiment ? { sentiment } : {}),
      ...(alternatives.length ? { alternatives } : {}), confidence: weight,
    } } : {}),
    version: 1, isLatest: true,
  };
}

export function parseTemporalGraphResult(
  xml: string,
  observationIds: string[],
  createdAt = new Date().toISOString(),
): TemporalGraphResult {
  const normalized = xml.trim().replace(/^```(?:xml)?\s*/i, "").replace(/\s*```$/, "").trim();
  const sourceIds = [...new Set(observationIds)];
  if (sourceIds.some((id) => typeof id !== "string" || !id.trim()) || !Number.isFinite(Date.parse(createdAt))) invalid();
  const root = parseXmlResult(normalized, { maxBytes: MAX_RESPONSE_BYTES });
  if (root.name !== "temporal_graph") return invalid();
  attributes(root, []);
  onlyWhitespace(root);
  const entities = uniqueChild(root, "entities");
  const relationships = uniqueChild(root, "relationships");
  if (root.children.length !== 2) return invalid();
  attributes(entities, []);
  attributes(relationships, []);
  onlyWhitespace(entities);
  onlyWhitespace(relationships);
  if (entities.children.some((child) => child.name !== "entity")
    || relationships.children.some((child) => child.name !== "relationship")) return invalid();

  const nodes = entities.children.map((entity) => parseEntity(entity, sourceIds, createdAt));
  const edges = relationships.children.map((relationship) => parseRelationship(relationship, nodes, sourceIds, createdAt));
  if (nodes.length === 0 && edges.length === 0) return { kind: "valid-empty", nodes: [], edges: [] };
  if (!sourceIds.length) return invalid();
  return { kind: "valid-result", nodes, edges };
}
