import { Buffer } from "node:buffer";
import { isDeepStrictEqual } from "node:util";
import type { LlmCallOptions, LlmResponseFormat, LlmTask, MemoryProvider, PreparedStructuredLlmCall, StructuredLlmBinding } from "../types.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import { prepareStructuredCall } from "../providers/structured-output-contract.js";
import { unwrapExactOuterJsonFence } from "./consolidation-procedural-format.js";
import type { PublicationEvidenceExecutor, PublicationEvidenceRequest } from "./publication-evidence.js";

export const PUBLICATION_OUTPUT_CONTRACT_VERSION = "publication-verdict-v2";
export interface PublicationOutputContract {
  version: typeof PUBLICATION_OUTPUT_CONTRACT_VERSION;
  parser: "closed-json-verdict-v2";
  binding: Readonly<StructuredLlmBinding>;
  options: Omit<LlmCallOptions, "onUsage">;
}

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

export function publicationExecutor(
  provider: MemoryProvider, router: LlmTaskRouter | undefined, task: LlmTask,
  options: LlmCallOptions = {},
  around: (request: PublicationEvidenceRequest, dispatch: () => Promise<string>) => Promise<string> = (_request, dispatch) => dispatch(),
): PublicationEvidenceExecutor {
  let prepared: PreparedStructuredLlmCall | undefined;
  let capturedContract: PublicationOutputContract | undefined;
  const available = router ? router.canPrepareStructuredCall(task) : typeof provider.prepareStructuredCall === "function";
  const callOptions = publicationCallOptions(options);
  const execute: PublicationEvidenceExecutor = async (request, captureRaw) => {
    if (!available) {
      if (request.contract) throw new Error("publication_output_contract_mismatch");
      const call = async (selected: MemoryProvider) => {
        const raw = await selected.summarize(request.systemPrompt, request.userPrompt, callOptions);
        await captureRaw(raw);
        return raw;
      };
      return around(request, () => router ? router.run(task, call,
        raw => task !== "consolidation" || typeof raw === "string" && raw.trim().length > 0) : call(provider));
    }
    if (!prepared || !request.contract || !isDeepStrictEqual(request.contract, capturedContract))
      throw new Error("publication_output_contract_mismatch");
    const call = prepared;
    return around(request, async () => {
      const raw = await call.execute();
      await captureRaw(raw);
      return raw;
    });
  };
  execute.contractAvailable = available;
  if (!available) return execute;
  execute.prepare = request => {
    if (prepared) throw new Error("publication_output_contract_already_prepared");
    const responseFormat = PUBLICATION_VERDICT_FORMAT;
    if (responseFormat.type !== "json_schema") throw new Error("publication_output_schema_invalid");
    const structured = { operation: "summarize" as const, systemPrompt: request.systemPrompt,
      userPrompt: request.userPrompt, contractVersion: PUBLICATION_OUTPUT_CONTRACT_VERSION,
      options: { ...callOptions, responseFormat } };
    prepared = router ? router.prepareStructuredCall(task, structured) : prepareStructuredCall(provider, structured);
    const { onUsage: _onUsage, ...durableOptions } = callOptions;
    const contract: PublicationOutputContract = { version: PUBLICATION_OUTPUT_CONTRACT_VERSION,
      parser: "closed-json-verdict-v2", binding: { ...prepared.binding },
      options: JSON.parse(JSON.stringify(durableOptions)) as PublicationOutputContract["options"] };
    capturedContract = JSON.parse(JSON.stringify(contract)) as PublicationOutputContract;
    return { ...request, contract: JSON.parse(JSON.stringify(capturedContract)) as PublicationOutputContract };
  };
  return execute;
}

export function parsePublicationVerdict(raw: string): { status: "supported" | "unsupported" | "uncertain"; reason: string } | undefined {
  return parseBoundedPublicationVerdict(raw, 512);
}

export const PUBLICATION_REASON_PROJECTION_VERSION = "publication-reason-projection-v1";
export interface PublicationReasonProjection {
  version: typeof PUBLICATION_REASON_PROJECTION_VERSION;
  originalContract: "publication-verdict-v2";
  originalContractSatisfied: false;
  violation: "reason_character_limit";
  originalReason: string;
  originalCharacters: number;
  originalBytes: number;
  projectedCharacters: number;
}

export function projectCapturedPublicationVerdict(raw: string): {
  status: "supported" | "unsupported" | "uncertain"; reason: string; projection: PublicationReasonProjection;
} | undefined {
  if (Buffer.byteLength(raw, "utf8") > 16_384 || parsePublicationVerdict(raw)) return undefined;
  const original = parseBoundedPublicationVerdict(raw, 2048);
  if (!original) return undefined;
  const characters = Array.from(original.reason);
  if (characters.length <= 512) return undefined;
  const suffix = " [diagnostic excerpt; full reason retained]";
  const reason = characters.slice(0, 512 - Array.from(suffix).length).join("") + suffix;
  return { status: original.status, reason, projection: {
    version: PUBLICATION_REASON_PROJECTION_VERSION, originalContract: "publication-verdict-v2",
    originalContractSatisfied: false, violation: "reason_character_limit",
    originalReason: original.reason, originalCharacters: characters.length,
    originalBytes: Buffer.byteLength(original.reason, "utf8"), projectedCharacters: Array.from(reason).length,
  } };
}

function parseBoundedPublicationVerdict(raw: string, reasonCharacters: number): {
  status: "supported" | "unsupported" | "uncertain"; reason: string;
} | undefined {
  const normalizedRaw = unwrapExactOuterJsonFence(raw) ?? raw;
  // Matching the entire two-string object rejects duplicate keys, including escaped aliases.
  const string = '"(?:[^"\\\\\\x00-\\x1f]|\\\\(?:["\\\\/bfnrt]|u[0-9a-fA-F]{4}))*"';
  const whitespace = "[ \\t\\r\\n]*";
  const pair = `(${string})${whitespace}:${whitespace}(${string})`;
  const match = new RegExp(`^${whitespace}\\{${whitespace}${pair}${whitespace},${whitespace}${pair}${whitespace}\\}${whitespace}$`).exec(normalizedRaw);
  if (!match) return undefined;
  let first: unknown, second: unknown, firstValue: unknown, secondValue: unknown;
  try { [first, firstValue, second, secondValue] = [match[1], match[2], match[3], match[4]].map(value => JSON.parse(value)); }
  catch { return undefined; }
  if (first === second || ![first, second].every(key => key === "verdict" || key === "reason")) return undefined;
  const verdict = first === "verdict" ? firstValue : secondValue;
  const reason = first === "reason" ? firstValue : secondValue;
  if (verdict !== "supported" && verdict !== "unsupported" && verdict !== "uncertain") return undefined;
  if (typeof reason !== "string" || !reason.trim() || Array.from(reason).length > reasonCharacters
    || Buffer.byteLength(reason, "utf8") > 2048 || /[\u0000-\u001f\u007f-\u009f\uD800-\uDFFF]/u.test(reason)) return undefined;
  return { status: verdict, reason };
}
