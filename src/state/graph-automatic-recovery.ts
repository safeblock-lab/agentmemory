import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import type { IIIClient } from "iii-sdk";
import { isStateCounter, type StateCounter, type StateGraphGuard } from "./state-transactions.js";

export const GRAPH_AUTOMATIC_RECOVERY_CAPABILITY = "state::graph_automatic_recovery_v1";
export const GRAPH_AUTOMATIC_ADOPTION_REVISION = "unspent-reservation-adoption-v1";
export const GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION = "unspent-child-recovery-v1";
export const GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION = "staging-checkpoint-recovery-v1";
export class GraphAutomaticUnspentPrepareWait extends Error {
  readonly retryAfter = new Date(Date.now() + 60_000).toISOString();
  constructor(readonly jobId: string) {
    super("STATE_GRAPH_AUTOMATIC_UNSPENT_PREPARE_WAIT_V1");
    this.name = "GraphAutomaticUnspentPrepareWait";
  }
}
interface GraphUnspentPrepareWaitResult {
  success: true;
  queued: true;
  deferred: true;
  jobId: string;
  retryAfter: string;
  reason: "STATE_GRAPH_AUTOMATIC_UNSPENT_PREPARE_WAIT_V1";
}
const unspentPrepareWaitResults = new WeakSet<object>();
export function graphUnspentPrepareWaitResult(wait: GraphAutomaticUnspentPrepareWait): GraphUnspentPrepareWaitResult {
  const result: GraphUnspentPrepareWaitResult = { success: true, queued: true, deferred: true,
    jobId: wait.jobId, retryAfter: wait.retryAfter, reason: "STATE_GRAPH_AUTOMATIC_UNSPENT_PREPARE_WAIT_V1" };
  unspentPrepareWaitResults.add(result);
  return result;
}
export function isGraphUnspentPrepareWaitResult(value: unknown): value is GraphUnspentPrepareWaitResult {
  return typeof value === "object" && value !== null && unspentPrepareWaitResults.has(value);
}
export const GRAPH_AUTOMATIC_RECOVERY_POLICY = Object.freeze({
  version: 1, additional_attempts_per_slot: 1, admissions_per_hour: 4,
  cooldown_ms: 60_000, deadline_ms: 120_000,
} as const);

export interface GraphRecoveryAnchor {
  guard: StateGraphGuard;
  source_generation?: StateCounter;
  job_id: string;
  job_version: StateCounter;
  checkpoint_version: StateCounter;
  delta_ordinal: number;
  value_name: string;
  manifest_version: StateCounter;
  manifest_attempt: string;
}
export interface GraphRecoverySource {
  scope: string;
  key: string;
  version: StateCounter;
  pointer: string;
}
export interface GraphRecoveryRequestSegment {
  request_pointer: string;
  sources: GraphRecoverySource[];
  prefix: string;
  separator: string;
  suffix: string;
}
export interface GraphRecoveryBinding {
  request_json: string;
  endpoint: string;
  model: string;
  timeout_ms: number;
  max_response_bytes: number;
  contract: { id: string; version: string; parser_version: string; schema_json: string };
  segments: GraphRecoveryRequestSegment[];
}
export interface GraphRecoveryAuthorityProfile {
  id: string;
  endpoint: string;
  model: string;
  timeout_ms: number;
  max_response_bytes: number;
  value_name_template: string;
  unit_pointer?: string | null;
  contract: GraphRecoveryBinding["contract"];
  request_template: Record<string, unknown>;
  segments: Array<{
    request_pointer: string;
    sources: Array<{ scope: "job_inputs" | "provider_values"; key: string; pointer: string }>;
    prefix: string;
    separator: string;
    suffix: string;
  }>;
}
export interface GraphRecoveryServiceAuthority { version: 1; profiles: GraphRecoveryAuthorityProfile[] }
export interface GraphTerminalRecoveryAnchor {
  guard: StateGraphGuard;
  job_id: string;
  job_version: StateCounter;
  checkpoint_version: StateCounter;
}
export interface GraphTerminalRecoveryCommand { command: "terminal_recover"; anchor: GraphTerminalRecoveryAnchor }
export type GraphRecoveryCommand =
  | { command: "prepare"; anchor: GraphRecoveryAnchor; binding: GraphRecoveryBinding; regeneration_id?: string }
  | { command: "admit" | "reconcile" | "regenerate" | "cleanup"; anchor: GraphRecoveryAnchor }
  | { command: "dispatch"; anchor: GraphRecoveryAnchor; attempt_id: string; binding: GraphRecoveryBinding }
  | { command: "capture_part"; anchor: GraphRecoveryAnchor; attempt_id: string; ordinal: number; text: string }
  | { command: "capture_complete"; anchor: GraphRecoveryAnchor; attempt_id: string; parts: number }
  | { command: "read_part"; anchor: GraphRecoveryAnchor; attempt_id: string; ordinal: number }
  | { command: "finish"; anchor: GraphRecoveryAnchor; attempt_id: string; outcome: "completed" | "unknown" | "failed" }
  | { command: "status"; slot_id: string };
export interface GraphAutomaticRecoveryRequest {
  action: "graph_automatic_recovery";
  request: GraphRecoveryCommand | GraphTerminalRecoveryCommand;
}
export type GraphRecoveryState = "prepared" | "intended" | "captured" | "completed" | "unknown" | "expired" | "failed" | "terminal";
export interface GraphRecoveryCapture { parts: number; bytes: number; digest: string }
export interface GraphRecoveryReservation {
  attempt_id: string;
  attempt_ordinal: 1;
  state: Exclude<GraphRecoveryState, "terminal">;
  consumed: boolean;
  capture: GraphRecoveryCapture | null;
  deadline_ms: number;
  claim_present: boolean;
  epoch: number;
  adoption_count: number;
}
export interface GraphAutomaticRecoveryResponse {
  capability: typeof GRAPH_AUTOMATIC_RECOVERY_CAPABILITY;
  unspent_child_revision: typeof GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION;
  unspent_child_proof_revision: typeof GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION;
  adoption_revision: typeof GRAPH_AUTOMATIC_ADOPTION_REVISION;
  reservation: GraphRecoveryReservation | null;
  policy: typeof GRAPH_AUTOMATIC_RECOVERY_POLICY;
  command: GraphRecoveryCommand["command"];
  slot_id: string;
  root_id: string;
  attempt_id: string | null;
  attempt_ordinal: 0 | 1 | null;
  state: GraphRecoveryState;
  reason: string | null;
  retry_after_ms: number | null;
  deadline_ms: number | null;
  consumed: boolean;
  retry_admitted: boolean;
  total_admitted: number;
  fresh: boolean;
  dispatch_allowed: boolean;
  regeneration_id: string | null;
  new_job_id: string | null;
  anchor: GraphRecoveryAnchor;
  binding_digest: string | null;
  capture: GraphRecoveryCapture | null;
  part?: string;
  prepared?: GraphRecoveryBinding;
  cleanup?: { invalidated: number; deleted: number; pending: boolean; retained_unproven: boolean };
}
export interface GraphTerminalRecoveryResponse {
  capability: typeof GRAPH_AUTOMATIC_RECOVERY_CAPABILITY;
  unspent_child_revision: typeof GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION;
  unspent_child_proof_revision: typeof GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION;
  adoption_revision: typeof GRAPH_AUTOMATIC_ADOPTION_REVISION;
  policy: typeof GRAPH_AUTOMATIC_RECOVERY_POLICY;
  command: "terminal_recover";
  job_id: string;
  job_version: StateCounter;
  checkpoint_version: StateCounter;
  resolution: "local_replay" | "regeneration" | "terminal";
  reason: string | null;
  retry_after_ms: number | null;
  fresh: boolean;
  original_slot: {
    generation: StateCounter;
    delta_ordinal: number;
    value_name: string;
    manifest_version: StateCounter;
    manifest_attempt: string;
    value_ref: { scope: string; key: string; version: StateCounter } | null;
  } | null;
  recovery: GraphAutomaticRecoveryResponse | null;
  replacement: { job_id: string; regeneration_id: string; mode: "local_replay" | "provider" } | null;
}
const dispatchAcknowledgements = new WeakSet<GraphAutomaticRecoveryResponse>();

const object = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const count = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
const text = (value: unknown, max = 512): value is string => typeof value === "string" && value.length > 0 && Buffer.byteLength(value) <= max;
const exact = (value: unknown, required: string[], optional: string[] = []): value is Record<string, unknown> => object(value)
  && required.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => required.includes(key) || optional.includes(key));
const pointer = (value: unknown): value is string => typeof value === "string" && Buffer.byteLength(value) <= 1024
  && (value === "" || value.startsWith("/") && !/~(?:[^01]|$)/.test(value));
const digest = (value: unknown): value is string => typeof value === "string" && /^[a-f0-9]{64}$/.test(value);
function validCapture(value: unknown): value is GraphRecoveryCapture | null {
  return value === null || exact(value, ["parts", "bytes", "digest"]) && count(value.parts)
    && value.parts >= 1 && value.parts <= 512 && count(value.bytes) && value.bytes <= 8 * 1024 * 1024 && digest(value.digest);
}
function validReservation(value: unknown): value is GraphRecoveryReservation | null {
  return value === null || exact(value, ["attempt_id", "attempt_ordinal", "state", "consumed", "capture", "deadline_ms", "claim_present", "epoch", "adoption_count"])
    && text(value.attempt_id, 128) && value.attempt_ordinal === 1
    && ["prepared", "intended", "captured", "completed", "unknown", "expired", "failed"].includes(String(value.state))
    && typeof value.consumed === "boolean" && validCapture(value.capture)
    && count(value.deadline_ms) && value.deadline_ms > 0 && typeof value.claim_present === "boolean"
    && count(value.epoch) && value.epoch < Number.MAX_SAFE_INTEGER && count(value.adoption_count)
    && value.adoption_count === value.epoch;
}
export function graphRecoverySlotId(anchor: GraphRecoveryAnchor): string {
  if (!validAnchor(anchor)) throw new Error("STATE_GRAPH_AUTOMATIC_INVALID_REQUEST");
  return `ga:${createHash("sha256").update(JSON.stringify([anchor.source_generation ?? anchor.guard.generation, anchor.job_id, anchor.delta_ordinal, anchor.value_name])).digest("hex")}`;
}
function ordered(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(ordered);
  if (object(value)) return Object.fromEntries(Object.keys(value).sort().map(key => [key, ordered(value[key])]));
  return value;
}
function freeze(value: unknown): void {
  if (!object(value) && !Array.isArray(value)) return;
  for (const child of Object.values(value)) freeze(child);
  Object.freeze(value);
}
function validAnchor(value: unknown): value is GraphRecoveryAnchor {
  return exact(value, ["guard", "job_id", "job_version", "checkpoint_version", "delta_ordinal", "value_name", "manifest_version", "manifest_attempt"], ["source_generation"])
    && exact(value.guard, ["owner_id", "generation", "fence"]) && text(value.guard.owner_id)
    && isStateCounter(value.guard.generation) && value.guard.generation !== "0" && isStateCounter(value.guard.fence) && value.guard.fence !== "0"
    && text(value.job_id, 128) && /^(?:graphjob_[A-Za-z0-9_-]+|graphbuild_[a-f0-9]{16})$/.test(value.job_id) && isStateCounter(value.job_version)
    && isStateCounter(value.checkpoint_version) && isStateCounter(value.manifest_version)
    && count(value.delta_ordinal) && value.delta_ordinal > 0 && text(value.value_name, 192)
    && (value.source_generation === undefined || isStateCounter(value.source_generation) && value.source_generation !== "0")
    && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value.value_name) && text(value.manifest_attempt, 128);
}
function validTerminalAnchor(value: unknown): value is GraphTerminalRecoveryAnchor {
  return exact(value, ["guard", "job_id", "job_version", "checkpoint_version"])
    && exact(value.guard, ["owner_id", "generation", "fence"]) && text(value.guard.owner_id)
    && isStateCounter(value.guard.generation) && value.guard.generation !== "0" && isStateCounter(value.guard.fence) && value.guard.fence !== "0"
    && text(value.job_id, 128) && /^(?:graphjob_[A-Za-z0-9_-]+|graphbuild_[a-f0-9]{16})$/.test(value.job_id)
    && isStateCounter(value.job_version) && isStateCounter(value.checkpoint_version);
}
function validSource(value: unknown): value is GraphRecoverySource {
  return exact(value, ["scope", "key", "version", "pointer"]) && text(value.scope) && text(value.key, 2048)
    && isStateCounter(value.version) && value.version !== "0" && pointer(value.pointer);
}
function validBinding(value: unknown): value is GraphRecoveryBinding {
  if (!exact(value, ["request_json", "endpoint", "model", "timeout_ms", "max_response_bytes", "contract", "segments"])
    || !text(value.request_json, 4 * 1024 * 1024) || !text(value.endpoint, 2048) || !text(value.model, 128)
    || !count(value.timeout_ms) || value.timeout_ms < 1 || value.timeout_ms > GRAPH_AUTOMATIC_RECOVERY_POLICY.deadline_ms
    || !count(value.max_response_bytes) || value.max_response_bytes < 1 || value.max_response_bytes > 8 * 1024 * 1024
    || !exact(value.contract, ["id", "version", "parser_version", "schema_json"]) || !text(value.contract.id, 128)
    || !text(value.contract.version, 128) || !text(value.contract.parser_version, 128) || !text(value.contract.schema_json, 256 * 1024)
    || !Array.isArray(value.segments) || value.segments.length < 1 || value.segments.length > 32) return false;
  try {
    const endpoint = new URL(value.endpoint.slice(5));
    if (!value.endpoint.startsWith("POST ") || !["http:", "https:"].includes(endpoint.protocol)
      || endpoint.username || endpoint.password || endpoint.search || endpoint.hash
      || value.endpoint !== `POST ${endpoint.origin}${endpoint.pathname}`) return false;
    if (!object(JSON.parse(value.request_json)) || !object(JSON.parse(value.contract.schema_json))) return false;
  } catch { return false; }
  return value.segments.every(segment => exact(segment, ["request_pointer", "sources", "prefix", "separator", "suffix"])
    && pointer(segment.request_pointer) && Array.isArray(segment.sources) && segment.sources.length > 0 && segment.sources.length <= 512
    && segment.sources.every(validSource) && [segment.prefix, segment.separator, segment.suffix].every(part => typeof part === "string" && Buffer.byteLength(part) <= 1024 * 1024));
}
export function isGraphAutomaticRecoveryRequest(value: { action: string }): value is GraphAutomaticRecoveryRequest {
  return value.action === "graph_automatic_recovery";
}
export function validateGraphAutomaticRecoveryRequest(value: GraphAutomaticRecoveryRequest): void {
  const invalid = (): never => { throw new Error("STATE_GRAPH_AUTOMATIC_INVALID_REQUEST"); };
  if (!exact(value, ["action", "request"]) || !object(value.request)) invalid();
  const request = value.request;
  if (request.command === "terminal_recover") {
    if (!exact(request, ["command", "anchor"]) || !validTerminalAnchor(request.anchor)) invalid();
  } else if (request.command === "status") {
    if (!exact(request, ["command", "slot_id"]) || !text(request.slot_id, 128)) invalid();
  } else {
    if (!validAnchor(request.anchor)) invalid();
    if (["admit", "reconcile", "regenerate", "cleanup"].includes(request.command)) {
      if (!exact(request, ["command", "anchor"])) invalid();
    } else if (request.command === "prepare") {
      if (!exact(request, ["command", "anchor", "binding"], ["regeneration_id"]) || !validBinding(request.binding)
        || request.regeneration_id !== undefined && !text(request.regeneration_id, 128)) invalid();
    } else if (request.command === "dispatch") {
      if (!exact(request, ["command", "anchor", "attempt_id", "binding"]) || !text(request.attempt_id, 128) || !validBinding(request.binding)) invalid();
    } else if (request.command === "capture_part") {
      if (!exact(request, ["command", "anchor", "attempt_id", "ordinal", "text"]) || !text(request.attempt_id, 128)
        || !count(request.ordinal) || request.ordinal >= 512 || !text(request.text, 128 * 1024)) invalid();
    } else if (request.command === "capture_complete") {
      if (!exact(request, ["command", "anchor", "attempt_id", "parts"]) || !text(request.attempt_id, 128) || !count(request.parts) || request.parts < 1 || request.parts > 512) invalid();
    } else if (request.command === "read_part") {
      if (!exact(request, ["command", "anchor", "attempt_id", "ordinal"]) || !text(request.attempt_id, 128) || !count(request.ordinal) || request.ordinal >= 512) invalid();
    } else if (request.command === "finish") {
      if (!exact(request, ["command", "anchor", "attempt_id", "outcome"]) || !text(request.attempt_id, 128)
        || !["completed", "unknown", "failed"].includes(request.outcome)) invalid();
    } else invalid();
  }
  if (Buffer.byteLength(JSON.stringify(value)) > 6 * 1024 * 1024) invalid();
}
export function validGraphAutomaticRecoveryResponse(request: GraphAutomaticRecoveryRequest, value: unknown): value is GraphAutomaticRecoveryResponse {
  if (request.request.command === "terminal_recover") return false;
  if (!object(value) || !object(value.policy)) return false;
  const policy = value.policy;
  if (!object(value) || value.capability !== GRAPH_AUTOMATIC_RECOVERY_CAPABILITY || value.unspent_child_revision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION || value.adoption_revision !== GRAPH_AUTOMATIC_ADOPTION_REVISION
    || value.unspent_child_proof_revision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION
    || !validReservation(value.reservation) || value.command !== request.request.command
    || Object.keys(GRAPH_AUTOMATIC_RECOVERY_POLICY).some(key => policy[key] !== GRAPH_AUTOMATIC_RECOVERY_POLICY[key as keyof typeof GRAPH_AUTOMATIC_RECOVERY_POLICY])
    || !text(value.slot_id, 128) || !text(value.root_id, 128) || !validAnchor(value.anchor)
    || !["prepared", "intended", "captured", "completed", "unknown", "expired", "failed", "terminal"].includes(String(value.state))
    || !(value.reason === null || text(value.reason, 128)) || !(value.attempt_id === null || text(value.attempt_id, 128))
    || ![null, 0, 1].includes(value.attempt_ordinal as number | null) || !(value.deadline_ms === null || count(value.deadline_ms))
    || !(value.retry_after_ms === null || count(value.retry_after_ms))
    || !count(value.total_admitted) || ![value.consumed, value.retry_admitted, value.fresh, value.dispatch_allowed].every(part => typeof part === "boolean")
    || !(value.binding_digest === null || digest(value.binding_digest))
    || ![value.regeneration_id, value.new_job_id].every(part => part === null || text(part, 128))) return false;
  const command = request.request;
  if (command.command === "status") { if (value.slot_id !== command.slot_id) return false; }
  else if (value.anchor.job_id !== command.anchor.job_id || value.anchor.guard.generation !== command.anchor.guard.generation
    || value.anchor.guard.owner_id !== command.anchor.guard.owner_id || BigInt(value.anchor.guard.fence) < BigInt(command.anchor.guard.fence)
    || value.anchor.delta_ordinal !== command.anchor.delta_ordinal || value.anchor.value_name !== command.anchor.value_name
    || (value.anchor.source_generation ?? value.anchor.guard.generation) !== (command.anchor.source_generation ?? command.anchor.guard.generation)
    || value.slot_id !== graphRecoverySlotId(command.anchor)) return false;
  if (!validCapture(value.capture)) return false;
  if (value.attempt_ordinal === 1 && (!value.reservation || value.reservation.attempt_id !== value.attempt_id
    || value.reservation.state !== value.state || value.reservation.consumed !== value.consumed
    || value.reservation.deadline_ms !== value.deadline_ms
    || JSON.stringify(ordered(value.reservation.capture)) !== JSON.stringify(ordered(value.capture)))) return false;
  if (value.dispatch_allowed === true && (command.command !== "dispatch" || value.fresh !== true || value.state !== "intended"
    || value.consumed !== true || value.attempt_id !== command.attempt_id || !validBinding(value.prepared)
    || JSON.stringify(ordered(value.prepared)) !== JSON.stringify(ordered(command.binding)))) return false;
  if (command.command !== "dispatch" && value.dispatch_allowed !== false) return false;
  if (command.command === "dispatch" && value.dispatch_allowed !== value.fresh) return false;
  if (command.command === "read_part" && (typeof value.part !== "string" || value.capture === null || value.attempt_id !== command.attempt_id)) return false;
  const admissionWait = value.reason === "GRAPH_AUTOMATIC_COOLDOWN" || value.reason === "GRAPH_AUTOMATIC_GLOBAL_LIMIT";
  if (admissionWait ? value.retry_after_ms === null : value.retry_after_ms !== null) return false;
  if (value.cleanup !== undefined && (!exact(value.cleanup, ["invalidated", "deleted", "pending", "retained_unproven"])
    || !count(value.cleanup.invalidated) || !count(value.cleanup.deleted) || typeof value.cleanup.pending !== "boolean" || typeof value.cleanup.retained_unproven !== "boolean")) return false;
  return true;
}
export function validGraphTerminalRecoveryResponse(command: GraphTerminalRecoveryCommand, value: unknown): value is GraphTerminalRecoveryResponse {
  if (!object(value) || !object(value.policy) || value.capability !== GRAPH_AUTOMATIC_RECOVERY_CAPABILITY
    || value.unspent_child_revision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION
    || value.unspent_child_proof_revision !== GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION
    || value.adoption_revision !== GRAPH_AUTOMATIC_ADOPTION_REVISION || value.command !== "terminal_recover") return false;
  const policy = value.policy;
  if (Object.keys(GRAPH_AUTOMATIC_RECOVERY_POLICY).some(key => policy[key] !== GRAPH_AUTOMATIC_RECOVERY_POLICY[key as keyof typeof GRAPH_AUTOMATIC_RECOVERY_POLICY])
    || value.job_id !== command.anchor.job_id || value.job_version !== command.anchor.job_version || value.checkpoint_version !== command.anchor.checkpoint_version
    || !["local_replay", "regeneration", "terminal"].includes(String(value.resolution)) || typeof value.fresh !== "boolean"
    || !(value.reason === null || text(value.reason, 128)) || !(value.retry_after_ms === null || count(value.retry_after_ms))) return false;
  if (value.original_slot !== null) {
    const slot = value.original_slot;
    if (!exact(slot, ["generation", "delta_ordinal", "value_name", "manifest_version", "manifest_attempt", "value_ref"])
      || !isStateCounter(slot.generation) || slot.generation === "0" || !count(slot.delta_ordinal) || slot.delta_ordinal < 1
      || !text(slot.value_name, 192) || !/^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(slot.value_name)
      || !isStateCounter(slot.manifest_version) || !text(slot.manifest_attempt, 128)) return false;
    if (slot.value_ref !== null && (!exact(slot.value_ref, ["scope", "key", "version"])
      || slot.value_ref.scope !== `mem:graph:provider-results:${command.anchor.job_id}`
      || slot.value_ref.key !== `value:${slot.delta_ordinal}:${slot.value_name}` || !isStateCounter(slot.value_ref.version) || slot.value_ref.version === "0")) return false;
  }
  if (value.recovery !== null) {
    if (!object(value.recovery) || typeof value.recovery.slot_id !== "string"
      || !validGraphAutomaticRecoveryResponse({ action: "graph_automatic_recovery", request: { command: "status", slot_id: value.recovery.slot_id } }, value.recovery)
      || value.recovery.fresh || value.recovery.dispatch_allowed || value.recovery.anchor.job_id !== command.anchor.job_id
      || value.original_slot === null || value.recovery.anchor.delta_ordinal !== value.original_slot.delta_ordinal
      || value.recovery.anchor.value_name !== value.original_slot.value_name
      || (value.recovery.anchor.source_generation ?? value.recovery.anchor.guard.generation) !== value.original_slot.generation) return false;
  }
  if (value.replacement !== null && (!exact(value.replacement, ["job_id", "regeneration_id", "mode"])
    || !text(value.replacement.job_id, 128) || !/^graphjob_[A-Za-z0-9_-]+$/.test(value.replacement.job_id)
    || !text(value.replacement.regeneration_id, 128) || !["local_replay", "provider"].includes(String(value.replacement.mode)))) return false;
  const admissionWait = value.reason === "GRAPH_AUTOMATIC_COOLDOWN" || value.reason === "GRAPH_AUTOMATIC_GLOBAL_LIMIT";
  if (admissionWait ? value.retry_after_ms === null || value.fresh : value.retry_after_ms !== null) return false;
  if (value.recovery !== null && (value.recovery.retry_after_ms !== value.retry_after_ms
    || admissionWait && value.recovery.reason !== value.reason)) return false;
  if (value.replacement !== null && (value.recovery === null || value.recovery.new_job_id !== value.replacement.job_id
    || value.recovery.regeneration_id !== value.replacement.regeneration_id)) return false;
  if (value.original_slot === null && (value.recovery !== null || value.replacement !== null || value.resolution !== "terminal")) return false;
  if (value.resolution === "local_replay" && (value.replacement === null || value.replacement.mode !== "local_replay" || value.original_slot?.value_ref === null)) return false;
  if (value.resolution === "regeneration" && (value.replacement === null || value.replacement.mode !== "provider")) return false;
  return true;
}
export function createGraphAutomaticRecoveryTransport(sdk: IIIClient) {
  function transport(command: GraphTerminalRecoveryCommand): Promise<GraphTerminalRecoveryResponse>;
  function transport(command: GraphRecoveryCommand): Promise<GraphAutomaticRecoveryResponse>;
  function transport(command: GraphRecoveryCommand | GraphTerminalRecoveryCommand): Promise<GraphAutomaticRecoveryResponse | GraphTerminalRecoveryResponse>;
  async function transport(command: GraphRecoveryCommand | GraphTerminalRecoveryCommand): Promise<GraphAutomaticRecoveryResponse | GraphTerminalRecoveryResponse> {
    const request: GraphAutomaticRecoveryRequest = { action: "graph_automatic_recovery", request: command };
    validateGraphAutomaticRecoveryRequest(request);
    let result: unknown;
    try {
      result = await sdk.trigger<GraphAutomaticRecoveryRequest, unknown>({ function_id: "state::storage", payload: request, timeoutMs: 30_000 });
    } catch (error) {
      const record = error instanceof Error ? error as Error & { function_id?: unknown; code?: unknown } : null;
      if (command.command === "prepare" && record
        && (record.function_id === undefined || record.function_id === "state::storage")
        && (record.code === "STATE_GRAPH_AUTOMATIC_UNSPENT_PREPARE_WAIT_V1"
          || /\bSTATE_GRAPH_AUTOMATIC_UNSPENT_PREPARE_WAIT_V1\b/.test(record.message.slice(0, 8192))))
        throw new GraphAutomaticUnspentPrepareWait(command.anchor.job_id);
      throw error;
    }
    if (command.command === "terminal_recover") {
      if (!validGraphTerminalRecoveryResponse(command, result)) throw new Error("STATE_GRAPH_AUTOMATIC_INVALID_RESPONSE");
      return result;
    }
    if (!validGraphAutomaticRecoveryResponse(request, result)) throw new Error("STATE_GRAPH_AUTOMATIC_INVALID_RESPONSE");
    if (result.dispatch_allowed && (result.deadline_ms === null || Date.now() >= result.deadline_ms)) throw new Error("STATE_GRAPH_AUTOMATIC_ACK_EXPIRED");
    if (result.dispatch_allowed) { freeze(result); dispatchAcknowledgements.add(result); }
    return result;
  }
  return transport;
}

export function graphRecoveryDispatchPermit(response: GraphAutomaticRecoveryResponse): { binding: Readonly<GraphRecoveryBinding>; attempt_id: string; timeout_ms: number } {
  const remaining = (response.deadline_ms ?? 0) - Date.now();
  if (!dispatchAcknowledgements.has(response) || response.command !== "dispatch" || !response.dispatch_allowed || !response.fresh || response.state !== "intended"
    || !response.consumed || response.attempt_id === null || response.prepared === undefined || remaining <= 0) throw new Error("STATE_GRAPH_AUTOMATIC_DISPATCH_NOT_ADMITTED");
  dispatchAcknowledgements.delete(response);
  return { binding: response.prepared, attempt_id: response.attempt_id, timeout_ms: Math.min(response.prepared.timeout_ms, remaining) };
}
