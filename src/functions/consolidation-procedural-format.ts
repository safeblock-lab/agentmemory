import type { LlmCallOptions, LlmResponseFormat, StructuredLlmBinding } from "../types.js";
import { FIREWORKS_BATCH_OUTPUT_CAPABILITY } from "../providers/structured-output-contract.js";
import { fingerprintId } from "../state/schema.js";

export const PROCEDURAL_OUTPUT_VERSION = "procedural-json-v1" as const;
export interface ProceduralOutputContractV1 {
  version: typeof PROCEDURAL_OUTPUT_VERSION;
  options: { responseFormat: LlmResponseFormat };
}
export type ProceduralTransport = { kind: "unbound" }
  | { kind: "direct"; binding: Readonly<StructuredLlmBinding> }
  | { kind: "fireworks-batch"; capability: typeof FIREWORKS_BATCH_OUTPUT_CAPABILITY; model: string };
export interface ProceduralOutputContractV2 {
  version: "procedural-json-v2";
  options: { responseFormat: LlmResponseFormat };
  transport: ProceduralTransport;
}
export type ProceduralOutputContract = ProceduralOutputContractV1 | ProceduralOutputContractV2;
export function proceduralOutputContractV2(transport: ProceduralTransport = { kind: "unbound" }): ProceduralOutputContractV2 {
  return { version: "procedural-json-v2", options: proceduralOutputContract().options, transport };
}
export function validatedProceduralContract(contract: ProceduralOutputContract): ProceduralOutputContract {
  const expected = contract.version === PROCEDURAL_OUTPUT_VERSION ? proceduralOutputContract()
    : contract.version === "procedural-json-v2" ? proceduralOutputContractV2(contract.transport) : undefined;
  if (!expected || !sameProceduralContract(expected, contract)) throw new Error("procedural_output_contract_invalid");
  if (contract.version === "procedural-json-v2") {
    if (contract.transport.kind === "unbound") throw new Error("procedural_output_transport_unbound");
    if (contract.transport.kind === "direct") {
      const binding = contract.transport.binding;
      if (binding.contractVersion !== contract.version || binding.operation !== "summarize"
        || !["legacy-native", "validated-json-text"].includes(binding.profile)
        || !binding.provider || !binding.model || !binding.endpoint || !/^[a-f0-9]{64}$/.test(binding.requestDigest)
        || !Number.isFinite(binding.timeoutMs) || binding.timeoutMs <= 0
        || !sameStructuredJsonValue(JSON.parse(binding.responseFormat), contract.options.responseFormat)) throw new Error("procedural_output_binding_mismatch");
    }
    if (contract.transport.kind === "fireworks-batch" && !sameStructuredJsonValue(contract.transport.capability, FIREWORKS_BATCH_OUTPUT_CAPABILITY)) throw new Error("procedural_batch_capability_mismatch");
    if (contract.transport.kind === "fireworks-batch" && (typeof contract.transport.model !== "string" || !contract.transport.model.trim())) throw new Error("procedural_batch_model_missing");
  }
  return structuredClone(contract);
}
export function proceduralOutputContract(): ProceduralOutputContractV1 {
  return { version: PROCEDURAL_OUTPUT_VERSION, options: { responseFormat: {
    type: "json_schema",
    json_schema: { name: "procedural_output_v1", strict: true, schema: {
      type: "object", additionalProperties: false, required: ["procedures"], properties: {
        procedures: { type: "array", maxItems: 4095, items: {
          type: "object", additionalProperties: false, required: ["name", "trigger", "steps"], properties: {
            name: { type: "string", minLength: 1, maxLength: 65536 },
            trigger: { type: "string", minLength: 1, maxLength: 65536 },
            steps: { type: "array", minItems: 2, maxItems: 4094,
              items: { type: "string", minLength: 1, maxLength: 65536 } },
          },
        } },
      },
    } },
  } } };
}
export function consolidationRequestId(manifestId: string, system: string, prompt: string, contract?: ProceduralOutputContract): string {
  const identity = contract ? [manifestId, system, prompt, contract] : [manifestId, system, prompt];
  return fingerprintId("conrequest", contract?.version === "procedural-json-v2" ? canonicalStructuredJson(identity) : JSON.stringify(identity));
}
export function canonicalStructuredJson(value: unknown): string {
  const serialized = JSON.stringify(value, (_key, entry: unknown) => record(entry)
    ? Object.fromEntries(Object.entries(entry).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)) : entry);
  if (serialized === undefined) throw new Error("structured_json_identity_invalid");
  return serialized;
}
export function proceduralCallOptions(contract?: ProceduralOutputContract): LlmCallOptions | undefined {
  return contract?.options;
}
export function sameProceduralContract(left?: ProceduralOutputContract, right?: ProceduralOutputContract): boolean {
  return sameStructuredJsonValue(left, right);
}
export function sameStructuredJsonValue(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    for (let index = 0; index < left.length; index++) {
      const leftHasIndex = Object.hasOwn(left, index);
      if (leftHasIndex !== Object.hasOwn(right, index)) return false;
      if (leftHasIndex && !sameStructuredJsonValue(left[index], right[index])) return false;
    }
    return true;
  }
  if (!record(left) || !record(right)) return false;
  const leftKeys = Object.keys(left);
  const rightKeys = Object.keys(right);
  return leftKeys.length === rightKeys.length
    && leftKeys.every(key => Object.hasOwn(right, key) && sameStructuredJsonValue(left[key], right[key]));
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function fields(value: Record<string, unknown>, expected: string[]): boolean {
  return Object.keys(value).length === expected.length && expected.every(key => Object.hasOwn(value, key));
}
function validText(value: unknown): value is string {
  if (typeof value !== "string" || !value.trim() || /[\u0000-\u0008\u000B\u000C\u000E-\u001F\uFFFE\uFFFF]/u.test(value)) return false;
  for (const character of value) {
    const point = character.codePointAt(0)!;
    if (point >= 0xD800 && point <= 0xDFFF) return false;
  }
  return true;
}
function rejectDuplicateKeys(json: string): void {
  const tokens = json.match(/"(?:[^"\\]|\\[\s\S])*"|[{}\[\]:,]|[^\s{}\[\]:,]+/g) ?? [];
  const stack: Array<Set<string> | null> = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index];
    if (token === "{" || token === "[") {
      if (stack.length >= 32) throw new Error("procedural_json_depth_limit");
      stack.push(token === "{" ? new Set() : null);
    } else if (token === "}" || token === "]") stack.pop();
    else if (token.startsWith('"') && tokens[index + 1] === ":") {
      const keys = stack.at(-1);
      const key: string = JSON.parse(token);
      if (!keys || keys.has(key)) throw new Error("procedural_json_duplicate_key");
      keys.add(key);
    }
  }
}
export function unwrapExactOuterJsonFence(response: string): string | undefined {
  const opener = "```json\n";
  const closer = "\n```";
  if (!response.startsWith(opener) || !response.endsWith(closer)) return undefined;
  const body = response.slice(opener.length, -closer.length);
  return body.includes("```") ? undefined : body;
}
export function parseProceduralJson(response: string): Array<{ name: string; trigger: string; steps: string[] }> {
  const fail = (): never => { throw new Error("Malformed procedural JSON output; originals retained locally"); };
  if (Buffer.byteLength(response, "utf8") > 65536) return fail();
  const normalizedResponse = unwrapExactOuterJsonFence(response) ?? response;
  let value: unknown;
  try { value = JSON.parse(normalizedResponse); rejectDuplicateKeys(normalizedResponse); } catch { return fail(); }
  if (!record(value) || !fields(value, ["procedures"]) || !Array.isArray(value.procedures)) return fail();
  let nodes = 1;
  return value.procedures.map((procedure: unknown) => {
    if (!record(procedure) || !fields(procedure, ["name", "trigger", "steps"])
      || !validText(procedure.name) || !validText(procedure.trigger) || !Array.isArray(procedure.steps)
      || procedure.steps.length < 2 || !procedure.steps.every(validText)) return fail();
    nodes += 1 + procedure.steps.length;
    if (nodes > 4096) return fail();
    return { name: procedure.name.trim(), trigger: procedure.trigger.trim(), steps: procedure.steps.map(step => step.trim()) };
  });
}
