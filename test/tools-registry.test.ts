import { describe, expect, it } from "vitest";
import { getAllTools } from "../src/mcp/tools-registry.js";
import { EXPORT_COLLECTIONS } from "../src/types.js";

describe("memory_export tool schema", () => {
  it("exposes the same bounded collection page contract without adding a tool", () => {
    const tools = getAllTools();
    const exports = tools.filter((tool) => tool.name === "memory_export");
    expect(exports).toHaveLength(1);
    const schema = exports[0].inputSchema.properties;
    expect(schema.collection.enum).toEqual([...EXPORT_COLLECTIONS]);
    expect(schema.offset).toMatchObject({ type: "integer", minimum: 0 });
    expect(schema.limit).toMatchObject({ type: "integer", minimum: 1, maximum: 1_000 });
  });
});
