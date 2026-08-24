import { describe, expect, it } from "vitest";
import { batchTaskLlmTask, taskOutputTokens } from "../src/providers/task-output-limits.js";

describe("task output limits", () => {
  it("caps a primary graph response below the configured global maximum", () => {
    expect(taskOutputTokens("graph_extraction", 4096)).toBe(512);
  });

  it("never raises an operator-configured completion limit", () => {
    expect(taskOutputTokens("consolidation", 256)).toBe(256);
  });

  it("maps batch maintenance tasks to their routed LLM task", () => {
    expect(batchTaskLlmTask("graph_extraction")).toBe("graph_extraction");
    expect(batchTaskLlmTask("consolidation")).toBe("consolidation");
    expect(batchTaskLlmTask("crystallization")).toBe("reflection");
  });
});
