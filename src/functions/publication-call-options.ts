import type { LlmCallOptions, LlmResponseFormat } from "../types.js";

const PUBLICATION_VERDICT_FORMAT: LlmResponseFormat = {
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
};

export function publicationCallOptions(options: LlmCallOptions = {}): LlmCallOptions {
  return {
    ...options,
    responseFormat: PUBLICATION_VERDICT_FORMAT,
  };
}
