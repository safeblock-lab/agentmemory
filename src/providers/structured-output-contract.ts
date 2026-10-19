import type { MemoryProvider, PreparedStructuredLlmCall, StructuredLlmRequest, StructuredLlmBinding } from "../types.js";

export function canPrepareStructuredCall(provider: MemoryProvider | undefined): boolean {
  return getStructuredCallPreparer(provider) !== undefined;
}

export function prepareStructuredCall(provider: MemoryProvider, request: StructuredLlmRequest): PreparedStructuredLlmCall {
  const prepare = getStructuredCallPreparer(provider);
  if (!prepare) throw new Error("structured_output_capability_unavailable");
  return prepare.call(provider, request);
}

function getStructuredCallPreparer(provider: MemoryProvider | undefined): MemoryProvider["prepareStructuredCall"] {
  if (!provider) return undefined;
  let current: object | null = provider;
  try {
    while (current !== null) {
      const descriptor = Object.getOwnPropertyDescriptor(current, "prepareStructuredCall");
      if (descriptor) return "value" in descriptor && typeof descriptor.value === "function"
        ? descriptor.value
        : undefined;
      current = Object.getPrototypeOf(current);
    }
  } catch {
    return undefined;
  }
  return undefined;
}

export function singleStructuredDispatch(binding: StructuredLlmBinding, execute: () => Promise<string>): PreparedStructuredLlmCall {
  let used = false;
  return Object.freeze({
    binding: Object.freeze({ ...binding }),
    async execute() {
      if (used) throw new Error("structured_output_dispatch_already_used");
      used = true;
      const result = await execute();
      if (typeof result !== "string") throw new Error("structured_output_content_missing");
      return result;
    },
  });
}

/** Batch consumers must resolve their own declared transport, never inherit an HTTP profile. */
export const FIREWORKS_BATCH_OUTPUT_CAPABILITY = Object.freeze({
  transport: "fireworks-batch" as const,
  profile: "legacy-native" as const,
  responseFormat: "json_schema" as const,
});
