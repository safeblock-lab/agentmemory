import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { validBudgetId, validBudgetContext, validProviderBudgetResponse, validateProviderBudgetRequest,
  type ProviderBudgetContext, type ProviderBudgetRequest, type ProviderBudgetResponse, type ProviderBudgetTransport } from "../state/provider-attempt-budget.js";

export interface ProviderAttemptBudgetBinding { operationId: string; windowId: string; ceiling: number; context?: ProviderBudgetContext }
export interface ProviderAttemptBudgetReceipt extends ProviderAttemptBudgetBinding {
  windowCeiling: number; consumed: number; windowConsumed: number; remaining: number; windowRemaining: number;
  state: "available" | "started" | "completed" | "unknown" | "expired" | "exhausted";
}
export class ProviderAttemptBudgetError extends Error {
  constructor(public readonly code: string) { super(`Provider attempt budget denied: ${code}`); this.name = "ProviderAttemptBudgetError"; }
}
interface Scope {
  binding: Readonly<ProviderAttemptBudgetBinding>; claimId: string; transport: ProviderBudgetTransport;
  consumed: number; closed: boolean; failed: boolean; tail: Promise<void>;
}
const active = new AsyncLocalStorage<Scope>();
const MAX_ADMISSION_MS = 3000;
let installedTransport: ProviderBudgetTransport | undefined;
const deny = (code: string): never => { throw new ProviderAttemptBudgetError(code); };
export function installProviderAttemptBudgetTransport(transport: ProviderBudgetTransport): void {
  if (typeof transport !== "function" || installedTransport) deny("transport_already_installed_or_invalid");
  installedTransport = transport;
}
function snapshot(value: ProviderAttemptBudgetBinding): Readonly<ProviderAttemptBudgetBinding> {
  if (!value || Object.keys(value).sort().join(",") !== (value.context === undefined ? "ceiling,operationId,windowId" : "ceiling,context,operationId,windowId")
    || !validBudgetId(value.operationId) || !validBudgetId(value.windowId)
    || (value.context !== undefined && !validBudgetContext(value.context))
    || !Number.isSafeInteger(value.ceiling) || value.ceiling < 1 || value.ceiling > 1000) deny("invalid_binding");
  return Object.freeze({ operationId: value.operationId, windowId: value.windowId, ceiling: value.ceiling,
    ...(value.context === undefined ? {} : { context: Object.freeze({ ...value.context }) }) });
}
async function admission(transport: ProviderBudgetTransport, request: ProviderBudgetRequest, deadline: number, signal?: AbortSignal | null): Promise<ProviderBudgetResponse> {
  if (signal?.aborted) deny("aborted");
  const remaining = Math.min(MAX_ADMISSION_MS, deadline - Date.now());
  if (remaining <= 0) deny("deadline_exceeded");
  const admissionDeadline = Date.now() + remaining;
  validateProviderBudgetRequest(request);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const stop = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new ProviderAttemptBudgetError("admission_outcome_unknown")), remaining);
    timer.unref();
    onAbort = () => reject(new ProviderAttemptBudgetError("aborted"));
    signal?.addEventListener("abort", onAbort, { once: true });
  });
  try {
    // An uncertain acknowledgment is never retried or reconciled into dispatch permission.
    const result = await Promise.race([Promise.resolve().then(() => transport(request)), stop]);
    if (signal?.aborted) deny("aborted");
    if (Date.now() >= deadline) deny("deadline_exceeded");
    if (Date.now() >= admissionDeadline) deny("admission_outcome_unknown");
    if (!validProviderBudgetResponse(request, result)) deny("invalid_acknowledgment");
    return result;
  } catch (error) {
    if (error instanceof ProviderAttemptBudgetError) throw error;
    deny("admission_outcome_unknown");
  } finally {
    clearTimeout(timer);
    if (onAbort) signal?.removeEventListener("abort", onAbort);
  }
}
function requestIdentity(scope: Scope) {
  return { operation_id: scope.binding.operationId, window_id: scope.binding.windowId, claim_id: scope.claimId };
}
export async function withProviderAttemptBudget<T>(binding: ProviderAttemptBudgetBinding, run: () => Promise<T>): Promise<T> {
  if (active.getStore() || typeof run !== "function") deny("invalid_scope");
  const frozen = snapshot(binding);
  const transport = installedTransport ?? deny("transport_unavailable");
  const scope: Scope = { binding: frozen, claimId: randomUUID(), transport, consumed: 0, closed: false, failed: false, tail: Promise.resolve() };
  const claimed = await admission(transport, { action: "provider_budget_claim", ...requestIdentity(scope), expected_ceiling: frozen.ceiling,
    ...(frozen.context === undefined ? {} : { context: frozen.context }) }, Date.now() + MAX_ADMISSION_MS);
  if (!claimed.fresh || claimed.status !== "claimed") deny("claim_not_fresh");
  return active.run(scope, async () => {
    let result: T;
    try { result = await run(); }
    catch (error) {
      scope.closed = true;
      await scope.tail;
      await finish(scope, "unknown");
      throw error;
    }
    scope.closed = true;
    await scope.tail;
    await finish(scope, scope.failed ? "unknown" : "completed");
    if (scope.failed) deny("attempt_outcome_unknown");
    return result;
  });
}
async function finish(scope: Scope, outcome: "completed" | "unknown"): Promise<void> {
  const result = await admission(scope.transport, { action: "provider_budget_finish", ...requestIdentity(scope), outcome }, Date.now() + MAX_ADMISSION_MS);
  if (!result.fresh || result.status !== "finished") deny("finish_outcome_unknown");
}
export async function consumeProviderAttempt(url: string, method = "GET", deadline = Date.now() + MAX_ADMISSION_MS, signal?: AbortSignal | null): Promise<boolean> {
  const scope = active.getStore();
  if (!scope) return false;
  const prior = scope.tail;
  let release!: () => void;
  scope.tail = new Promise<void>((resolve) => { release = resolve; });
  try {
    await prior;
    if (scope.closed || scope.failed) deny("scope_closed_or_unknown");
    const parsed = new URL(url);
    if (parsed.username || parsed.password || !["http:", "https:"].includes(parsed.protocol)) deny("endpoint_invalid");
    const endpoint = `${method.toUpperCase()} ${parsed.origin}${parsed.pathname}`;
    const result = await admission(scope.transport, { action: "provider_budget_consume", ...requestIdentity(scope), attempt_id: randomUUID(), endpoint }, deadline, signal);
    if (scope.closed || scope.failed) deny("scope_closed_or_unknown");
    if (!result.fresh || result.status !== "consumed" || !result.receipt
      || result.receipt.ceiling !== scope.binding.ceiling || result.receipt.consumed !== scope.consumed + 1) deny("consume_not_fresh");
    scope.consumed = result.receipt.consumed;
    return true;
  } catch (error) {
    scope.failed = true;
    if (error instanceof ProviderAttemptBudgetError) throw error;
    deny("admission_outcome_unknown");
  } finally { release(); }
}
export async function getProviderAttemptBudgetReceipt(operationId: string): Promise<ProviderAttemptBudgetReceipt | null> {
  if (!validBudgetId(operationId)) deny("invalid_binding");
  const transport = installedTransport ?? deny("transport_unavailable");
  const result = await admission(transport, { action: "provider_budget_status", operation_id: operationId }, Date.now() + MAX_ADMISSION_MS);
  const receipt = result.receipt;
  if (!receipt) return null;
  return { operationId: receipt.operation_id, windowId: receipt.window_id, ceiling: receipt.ceiling,
    windowCeiling: receipt.window_ceiling, consumed: receipt.consumed, windowConsumed: receipt.window_consumed,
    remaining: receipt.remaining, windowRemaining: receipt.window_remaining, state: receipt.state };
}
