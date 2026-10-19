import type { IIIClient } from "iii-sdk";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import { isStateCounter } from "../state/state-transactions.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { boundedBytes, immutablePage, pageKey, type PagedConsolidation, type FragmentPage } from "./consolidation-manifest.js";
import { capturedResponse, boundedPartitions, type RequestCheckpoint } from "./consolidation-partitions.js";
import { parseProcedural, parseProceduralStructured } from "./consolidation-reduction.js";
import { canonicalStructuredJson, consolidationRequestId, proceduralOutputContractV2, sameProceduralContract,
  sameStructuredJsonValue, unwrapExactOuterJsonFence, validatedProceduralContract,
  type ProceduralOutputContract, type ProceduralOutputContractV2 } from "./consolidation-procedural-format.js";
import { recordAudit } from "./audit.js";
import { PROCEDURAL_FRAGMENT_SYSTEM, PROCEDURAL_JSON_FRAGMENT_SYSTEM, buildConsolidationFragmentPrompt } from "../prompts/consolidation.js";
import { proceduralFrequency } from "./consolidation-capture.js";

const FUNCTION_ID = "mem::consolidation-admit-response-repair";
const XML_RULE = "literal-step-xml-escape/v1" as const;
const JSON_FENCE_RULE = "outer-json-fence/v1" as const;
interface RepairInputBase {
  manifestId: string; requestId: string; checkpointId: string;
  expectedRootVersion: string; expectedRequestVersion: string;
  cursor: { page: number; offset: number };
  originalResponseSha256: string; proposedResponseSha256: string;
  admission: { operationId: string; actor: string; proposalId: string; reason: string };
}
interface XmlEscapeEdit {
  responseStartUtf16: number; responseEndUtf16: number; procedureOrdinal: number; stepOrdinal: number; before: string; after: string;
}
type RepairInput = RepairInputBase & (
  | { ruleVersion: typeof XML_RULE; edit: XmlEscapeEdit }
  | { ruleVersion: typeof JSON_FENCE_RULE }
);
interface RepairRecord {
  schema: 1; input: RepairInput; requestCheckpointSha256: string;
  originalBytes: number; correctedBytes: number; correctedResponse: string; admittedAt: string;
}
function digest(text: string): string { return createHash("sha256").update(text).digest("hex"); }
function repairKey(manifestId: string, requestId: string): string { return pageKey(manifestId, "response-repair", requestId); }
function object(value: unknown): value is Record<string, unknown> { return typeof value === "object" && value !== null && !Array.isArray(value); }
function keys(value: Record<string, unknown>, expected: string[]): boolean {
  return Object.keys(value).length === expected.length && expected.every(key => Object.hasOwn(value, key));
}
function invalid(): never { throw new Error("consolidation_response_repair_invalid"); }
function invocationPayload(value: unknown): unknown {
  if (!object(value) || !Object.hasOwn(value, "_caller_worker_id")) return value;
  const { _caller_worker_id: caller, ...payload } = value;
  if (typeof caller !== "string" || caller.length < 1 || caller.length > 128) invalid();
  return payload;
}
function validateInput(value: unknown): asserts value is RepairInput {
  const baseKeys = ["manifestId", "requestId", "checkpointId", "expectedRootVersion", "expectedRequestVersion", "cursor",
    "originalResponseSha256", "proposedResponseSha256", "ruleVersion", "admission"];
  if (!object(value) || value.ruleVersion !== XML_RULE && value.ruleVersion !== JSON_FENCE_RULE
    || !keys(value, value.ruleVersion === XML_RULE ? [...baseKeys, "edit"] : baseKeys)) invalid();
  boundedBytes(value, 4096);
  if (typeof value.manifestId !== "string" || !/^conmanifest_[a-f0-9]{16}$/.test(value.manifestId)
    || typeof value.requestId !== "string" || !/^conrequest_[a-f0-9]{16}$/.test(value.requestId) || value.checkpointId !== value.requestId
    || typeof value.originalResponseSha256 !== "string" || !/^[a-f0-9]{64}$/.test(value.originalResponseSha256)
    || typeof value.proposedResponseSha256 !== "string" || !/^[a-f0-9]{64}$/.test(value.proposedResponseSha256)
    || !isStateCounter(value.expectedRootVersion) || value.expectedRootVersion === "0"
    || !isStateCounter(value.expectedRequestVersion) || value.expectedRequestVersion === "0") invalid();
  const cursor = value.cursor; const admission = value.admission;
  if (!object(cursor) || !keys(cursor, ["page", "offset"])
    || !Number.isSafeInteger(cursor.page) || Number(cursor.page) < 0 || !Number.isSafeInteger(cursor.offset) || Number(cursor.offset) < 0) invalid();
  if (value.ruleVersion === XML_RULE) {
    const edit = value.edit;
    if (!object(edit) || !keys(edit, ["responseStartUtf16", "responseEndUtf16", "procedureOrdinal", "stepOrdinal", "before", "after"])
      || !Number.isSafeInteger(edit.responseStartUtf16) || Number(edit.responseStartUtf16) < 0
      || !Number.isSafeInteger(edit.responseEndUtf16) || Number(edit.responseEndUtf16) <= Number(edit.responseStartUtf16)
      || !Number.isSafeInteger(edit.procedureOrdinal) || Number(edit.procedureOrdinal) < 1
      || !Number.isSafeInteger(edit.stepOrdinal) || Number(edit.stepOrdinal) < 1
      || typeof edit.before !== "string" || typeof edit.after !== "string") invalid();
  }
  if (!object(admission) || !keys(admission, ["operationId", "actor", "proposalId", "reason"])
    || Object.values(admission).some(item => typeof item !== "string" || !item.trim() || Buffer.byteLength(item) > 256)) invalid();
}
function corrected(original: string, input: RepairInput): string {
  if (Buffer.byteLength(original) > 65_536 || digest(original) !== input.originalResponseSha256) invalid();
  if (input.ruleVersion === JSON_FENCE_RULE) {
    const response = unwrapExactOuterJsonFence(original);
    if (response === undefined || Buffer.byteLength(response) > 65_536 || digest(response) !== input.proposedResponseSha256) invalid();
    return response;
  }
  const edit = input.edit;
  const before = original.slice(edit.responseStartUtf16, edit.responseEndUtf16);
  if (before !== edit.before || before.length > 128 || !/^<[A-Za-z_][A-Za-z0-9_.:-]*>$/.test(before)
    || /^<(?:procedures|procedure|step)>$/i.test(before) || original.indexOf(before) !== original.lastIndexOf(before)) invalid();
  const procedures = [...original.matchAll(/<procedure\b[^>]*>([\s\S]*?)<\/procedure>/g)];
  const procedure = procedures[edit.procedureOrdinal - 1];
  if (!procedure || procedure.index === undefined) invalid();
  const steps = [...procedure[0].matchAll(/<step>([\s\S]*?)<\/step>/g)];
  const step = steps[edit.stepOrdinal - 1];
  if (!step || step.index === undefined) invalid();
  const start = procedure.index + step.index + "<step>".length;
  if (edit.responseStartUtf16 < start || edit.responseEndUtf16 > start + step[1].length
    || step[1].replace(before, "").includes("<")) invalid();
  const after = `&lt;${before.slice(1, -1)}&gt;`;
  if (after !== edit.after) invalid();
  const response = original.slice(0, edit.responseStartUtf16) + after + original.slice(edit.responseEndUtf16);
  if (Buffer.byteLength(response) > 65_536 || digest(response) !== input.proposedResponseSha256) invalid();
  return response;
}
async function checkpoint(kv: StateKV, input: RepairInput): Promise<{ request: RequestCheckpoint; response: string }> {
  const row = await kv.getVersioned<RequestCheckpoint>(KV.state, input.requestId);
  const request = row.value;
  const requestIdentity = input.ruleVersion === JSON_FENCE_RULE && request?.outputContract?.version === "procedural-json-v2"
    ? consolidationRequestId(request.manifestId, request.systemPrompt, request.userPrompt, validatedProceduralContract(request.outputContract))
    : fingerprintId("conrequest", JSON.stringify([request?.manifestId, request?.systemPrompt, request?.userPrompt]));
  if (!row.exists || row.version !== input.expectedRequestVersion || !request || request.id !== input.requestId
    || request.manifestId !== input.manifestId || request.status !== "completed" || request.dispatchMode !== "direct"
    || request.workItemId !== undefined || !request.responsePages || request.responsePages.sha256 !== input.originalResponseSha256
    || requestIdentity !== request.id || input.ruleVersion === JSON_FENCE_RULE && request.outputContract?.version !== "procedural-json-v2") {
    throw new Error("consolidation_response_repair_checkpoint_stale");
  }
  const response = await capturedResponse(kv, request);
  if (response === undefined) invalid();
  return { request, response };
}
async function assertCursorRequest(kv: StateKV, root: PagedConsolidation, request: RequestCheckpoint): Promise<string[]> {
  boundedBytes(root);
  if (!Array.isArray(root.refs) || root.refs.length < 1 || root.refs.length > 12 || root.refs.some(ref => !object(ref)
    || !["summary", "observation", "memory"].includes(ref.kind) || typeof ref.id !== "string" || !ref.id.trim()
    || typeof ref.sessionId !== "string" || !ref.sessionId.trim() || Buffer.byteLength(ref.id) > 256 || Buffer.byteLength(ref.sessionId) > 256)) invalid();
  const page = await kv.get<FragmentPage>(KV.state, pageKey(root.id, "fragments", root.extract.page));
  if (!page || !root.eligibleSources.includes(page.source)) invalid();
  boundedBytes(page);
  const frequency = await proceduralFrequency(kv, root, page.source);
  const remaining = page.fragments.slice(root.extract.offset).map(fragment => ({ ...fragment, ...(frequency ? { frequency } : {}) }));
  const jsonV2 = request.outputContract?.version === "procedural-json-v2";
  const systemPrompt = jsonV2 ? PROCEDURAL_JSON_FRAGMENT_SYSTEM : PROCEDURAL_FRAGMENT_SYSTEM;
  const group = boundedPartitions(remaining, buildConsolidationFragmentPrompt, systemPrompt)[0];
  if (!group?.length || request.systemPrompt !== systemPrompt
    || request.userPrompt !== `Consolidation manifest: ${root.id}\n\n${buildConsolidationFragmentPrompt(group)}`) {
    throw new Error("consolidation_response_repair_cursor_request_mismatch");
  }
  if (jsonV2) {
    const contract = validatedProceduralContract(request.outputContract!);
    if (contract.version !== "procedural-json-v2"
      || consolidationRequestId(root.id, request.systemPrompt, request.userPrompt, contract) !== request.id) invalid();
    const bindingKey = fingerprintId("conbinding", canonicalStructuredJson([root.id, request.systemPrompt, request.userPrompt,
      "procedural-json-v2", proceduralOutputContractV2().options]));
    const binding = await kv.get<ProceduralOutputContract>(KV.state, bindingKey);
    if (!binding || !sameProceduralContract(validatedProceduralContract(binding), contract)) invalid();
  }
  const originals = root.refs.filter(ref => group.some(fragment => fragment.sourceId === (ref.kind === "summary" ? ref.sessionId : ref.id)));
  const sources = [...new Set(originals.map(ref => ref.sessionId))];
  if (!sources.length) invalid();
  return sources;
}
export function registerConsolidationResponseRepair(sdk: IIIClient, kv: StateKV): void {
  sdk.registerFunction(FUNCTION_ID, async (value: unknown) => {
    const input = invocationPayload(value);
    validateInput(input);
    // Engine access and operational approval are prerequisites, never payload claims.
    return withKeyedLock(`consolidation-paged:${input.manifestId}`, () =>
      withKeyedLock(`consolidation-request:${input.requestId}`, async () => {
        const key = repairKey(input.manifestId, input.requestId);
        const existing = await kv.get<RepairRecord>(KV.state, key);
        if (existing && !isDeepStrictEqual(existing.input, input)) throw new Error("consolidation_response_repair_conflict");
        const capture = await checkpoint(kv, input);
        const response = corrected(capture.response, input);
        const requestCheckpointSha256 = digest(JSON.stringify(capture.request));
        if (existing) {
          if (existing.schema !== 1 || existing.correctedResponse !== response || existing.requestCheckpointSha256 !== requestCheckpointSha256) invalid();
          return { success: true, admitted: true, replayed: true, manifestId: input.manifestId, requestId: input.requestId };
        }
        const rootRow = await kv.getVersioned<PagedConsolidation>(KV.state, pageKey(input.manifestId, "root", 0));
        const root = rootRow.value;
        if (!rootRow.exists || rootRow.version !== input.expectedRootVersion || !root || root.schema !== 2 || root.id !== input.manifestId
          || root.kind !== "procedural" || root.phase !== "extract" || root.legacy?.completed
          || !isDeepStrictEqual(root.extract, input.cursor)) throw new Error("consolidation_response_repair_cursor_stale");
        const sources = await assertCursorRequest(kv, root, capture.request);
        if (input.ruleVersion === JSON_FENCE_RULE) parseProceduralStructured(response, sources);
        else parseProcedural(response, sources);
        const record: RepairRecord = { schema: 1, input, requestCheckpointSha256,
          originalBytes: Buffer.byteLength(capture.response), correctedBytes: Buffer.byteLength(response), correctedResponse: response,
          admittedAt: new Date().toISOString() };
        await immutablePage(kv, key, record);
        await recordAudit(kv, "update", FUNCTION_ID, [input.manifestId, input.requestId], {
          repairKey: key, ruleVersion: input.ruleVersion, originalResponseSha256: input.originalResponseSha256,
          proposedResponseSha256: input.proposedResponseSha256, requestCheckpointSha256, cursor: input.cursor, admission: input.admission,
        });
        return { success: true, admitted: true, replayed: false, manifestId: input.manifestId, requestId: input.requestId };
      }));
  });
}
export async function effectiveProceduralResponse(kv: StateKV, root: PagedConsolidation, requestId: string, original: string): Promise<string> {
  const record = await kv.get<RepairRecord>(KV.state, repairKey(root.id, requestId));
  if (!record) return original;
  validateInput(record.input);
  const input = record.input;
  if (record.schema !== 1 || input.ruleVersion !== XML_RULE || input.manifestId !== root.id || input.requestId !== requestId || root.kind !== "procedural"
    || root.phase !== "extract" || !isDeepStrictEqual(root.extract, input.cursor)) invalid();
  const capture = await checkpoint(kv, input);
  const rootRow = await kv.getVersioned<PagedConsolidation>(KV.state, pageKey(root.id, "root", 0));
  if (rootRow.version !== input.expectedRootVersion || !isDeepStrictEqual(rootRow.value, root)
    || capture.response !== original || record.requestCheckpointSha256 !== digest(JSON.stringify(capture.request))) {
    throw new Error("consolidation_response_repair_identity_stale");
  }
  const response = corrected(original, input);
  if (record.correctedResponse !== response || record.originalBytes !== Buffer.byteLength(original) || record.correctedBytes !== Buffer.byteLength(response)) invalid();
  return response;
}

export async function effectiveProceduralJsonResponse(kv: StateKV, root: PagedConsolidation, systemPrompt: string,
  effectiveUserPrompt: string, outputContract: ProceduralOutputContractV2, original: string): Promise<string> {
  if (outputContract.version !== "procedural-json-v2"
    || !sameStructuredJsonValue(outputContract.options, proceduralOutputContractV2().options)) invalid();
  const bindingKey = fingerprintId("conbinding", canonicalStructuredJson([root.id, systemPrompt, effectiveUserPrompt,
    "procedural-json-v2", proceduralOutputContractV2().options]));
  const binding = await kv.get<ProceduralOutputContract>(KV.state, bindingKey);
  if (!binding) return original;
  const boundContract = validatedProceduralContract(binding);
  if (boundContract.version !== "procedural-json-v2") invalid();
  const requestId = consolidationRequestId(root.id, systemPrompt, effectiveUserPrompt, boundContract);
  const record = await kv.get<RepairRecord>(KV.state, repairKey(root.id, requestId));
  if (!record) return original;
  validateInput(record.input);
  const input = record.input;
  if (record.schema !== 1 || input.ruleVersion !== JSON_FENCE_RULE || input.manifestId !== root.id || input.requestId !== requestId
    || root.kind !== "procedural" || root.phase !== "extract" || !isDeepStrictEqual(root.extract, input.cursor)) invalid();
  const capture = await checkpoint(kv, input);
  if (capture.request.systemPrompt !== systemPrompt || capture.request.userPrompt !== effectiveUserPrompt
    || !sameProceduralContract(capture.request.outputContract, boundContract)) invalid();
  const sources = await assertCursorRequest(kv, root, capture.request);
  const rootRow = await kv.getVersioned<PagedConsolidation>(KV.state, pageKey(root.id, "root", 0));
  if (rootRow.version !== input.expectedRootVersion || !isDeepStrictEqual(rootRow.value, root)
    || capture.response !== original || record.requestCheckpointSha256 !== digest(JSON.stringify(capture.request))) {
    throw new Error("consolidation_response_repair_identity_stale");
  }
  const response = corrected(original, input);
  if (record.correctedResponse !== response || record.originalBytes !== Buffer.byteLength(original) || record.correctedBytes !== Buffer.byteLength(response)) invalid();
  parseProceduralStructured(response, sources);
  return response;
}
