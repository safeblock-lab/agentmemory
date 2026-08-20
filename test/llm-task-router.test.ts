import { describe, expect, it, vi } from "vitest";
import { LlmTaskRouter } from "../src/providers/task-router.js";
import type { LlmRoutingConfig, MemoryProvider } from "../src/types.js";

const routes: LlmRoutingConfig = {
  routes: {
    graph_extraction: "aux",
    temporal_graph_extraction: "aux",
    consolidation: "aux",
    compression: "aux",
    summary: "aux",
    entity_extraction: "aux",
    classification: "aux",
    reflection: "primary",
    conflict_resolution: "primary",
    skill_extraction: "aux",
    query_expansion: "aux",
    flow_compression: "aux",
  },
  explicitRoutes: {},
  warnings: [],
};

function provider(name: string, response = name): MemoryProvider {
  return {
    name,
    compress: vi.fn(async () => response),
    summarize: vi.fn(async () => response),
  };
}

function router(primary: MemoryProvider, auxiliary?: MemoryProvider): LlmTaskRouter {
  return new LlmTaskRouter({
    primary: { provider: primary, model: "primary-model" },
    ...(auxiliary ? { auxiliary: { provider: auxiliary, model: "aux-model" } } : {}),
    routing: routes,
  });
}

describe("LlmTaskRouter", () => {
  it("uses primary when auxiliary is not configured", async () => {
    const primary = provider("primary");
    const result = await router(primary).run(
      "compression",
      (selected) => selected.compress("system", "user"),
      (value) => value === "primary",
    );
    expect(result).toBe("primary");
    expect(primary.compress).toHaveBeenCalledTimes(1);
  });

  it("accepts valid auxiliary output without calling primary", async () => {
    const primary = provider("primary");
    const auxiliary = provider("auxiliary");
    const result = await router(primary, auxiliary).run(
      "graph_extraction",
      (selected) => selected.compress("system", "user"),
      (value) => value === "auxiliary",
    );
    expect(result).toBe("auxiliary");
    expect(primary.compress).not.toHaveBeenCalled();
  });

  it("allows a complex workload to override its configured auxiliary route", async () => {
    const primary = provider("primary");
    const auxiliary = provider("auxiliary");
    await expect(router(primary, auxiliary).run(
      "consolidation",
      (selected) => selected.summarize("system", "user"),
      (value) => value === "primary",
      "primary",
    )).resolves.toBe("primary");
    expect(auxiliary.summarize).not.toHaveBeenCalled();
    expect(primary.summarize).toHaveBeenCalledTimes(1);
  });

  it("falls back once when auxiliary output fails validation", async () => {
    const primary = provider("primary", "accepted");
    const auxiliary = provider("auxiliary", "invalid");
    const result = await router(primary, auxiliary).run(
      "compression",
      (selected) => selected.compress("system", "user"),
      (value) => value === "accepted",
    );
    expect(result).toBe("accepted");
    expect(auxiliary.compress).toHaveBeenCalledTimes(1);
    expect(primary.compress).toHaveBeenCalledTimes(1);
  });

  it("falls back once when auxiliary throws", async () => {
    const primary = provider("primary", "accepted");
    const auxiliary = provider("auxiliary");
    auxiliary.compress = vi.fn(async () => { throw new Error("offline"); });
    await expect(router(primary, auxiliary).run(
      "entity_extraction",
      (selected) => selected.compress("system", "user"),
      (value) => value === "accepted",
    )).resolves.toBe("accepted");
    expect(primary.compress).toHaveBeenCalledTimes(1);
  });

  it("rejects invalid primary output", async () => {
    const primary = provider("primary", "invalid");
    await expect(router(primary).run(
      "reflection",
      (selected) => selected.summarize("system", "user"),
      () => false,
    )).rejects.toThrow("LLM reflection response failed deterministic validation");
  });
});
