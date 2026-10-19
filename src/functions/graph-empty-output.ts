export const GRAPH_EMPTY_OUTPUT_PARSER = "graph-explicit-empty-xml:v1";

export function isExplicitEmptyGraphXml(value: unknown): value is string {
  return typeof value === "string"
    && /^\s*(?:<entities\s*\/>|<entities>\s*<\/entities>)\s*(?:<relationships\s*\/>|<relationships>\s*<\/relationships>)\s*$/.test(value);
}
