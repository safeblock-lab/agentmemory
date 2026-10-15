import { describe, expect, it, vi } from "vitest";
import type { LlmCallOptions } from "../src/types.js";
import { publicationCallOptions } from "../src/functions/publication-call-options.js";

describe("publicationCallOptions", () => {
  it("enforces a closed native verdict schema while preserving unrelated call options", () => {
    const onUsage = vi.fn();
    const options: LlmCallOptions = publicationCallOptions({
      task: "reflection",
      thinking: false,
      outputTokens: 96,
      onUsage,
      responseFormat: { type: "json_object" },
    });

    expect(options).toMatchObject({ task: "reflection", thinking: false, outputTokens: 96 });
    expect(options.onUsage).toBe(onUsage);
    expect(options.responseFormat).toEqual({
      type: "json_schema",
      json_schema: {
        name: "publication_verdict",
        strict: true,
        schema: {
          type: "object",
          properties: {
            verdict: { type: "string", enum: ["supported", "unsupported", "uncertain"] },
            reason: { type: "string", minLength: 1, maxLength: 512 },
          },
          required: ["verdict", "reason"],
          additionalProperties: false,
        },
      },
    });
  });

  it("sets the structured format by default", () => {
    expect(publicationCallOptions()).toMatchObject({
      responseFormat: { type: "json_schema", json_schema: { strict: true } },
    });
  });
});
