import type { IIIClient } from "iii-sdk";

export const PROVIDER_BUDGET_CAPABILITY = "state::provider_attempt_budget_v1";
export interface ProviderBudgetContext {
  workflow: string; functionId: string; sourceId?: string; jobId?: string;
  unitId?: string; phase: string; continuationId?: string;
}
export type ProviderBudgetRequest =
  | { action: "provider_budget_claim"; operation_id: string; window_id: string; expected_ceiling: number; claim_id: string; context?: ProviderBudgetContext }
  | { action: "provider_budget_consume"; operation_id: string; window_id: string; claim_id: string; attempt_id: string; endpoint: string }
  | { action: "provider_budget_finish"; operation_id: string; window_id: string; claim_id: string; outcome: "completed" | "unknown" }
  | { action: "provider_budget_status"; operation_id: string };
export interface NativeProviderBudgetReceipt {
  operation_id: string; window_id: string; ceiling: number; window_ceiling: number;
  consumed: number; window_consumed: number; remaining: number; window_remaining: number;
  state: "available" | "started" | "completed" | "unknown" | "expired" | "exhausted";
}
export interface ProviderBudgetResponse {
  capability: typeof PROVIDER_BUDGET_CAPABILITY; action: ProviderBudgetRequest["action"];
  operation_id: string; window_id?: string; claim_id?: string; attempt_id?: string;
  fresh: boolean; status: "claimed" | "consumed" | "finished" | "duplicate" | "status";
  receipt: NativeProviderBudgetReceipt | null;
}
export type ProviderBudgetTransport = (request: ProviderBudgetRequest) => Promise<unknown>;
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
export const validBudgetId = (value: unknown): value is string =>
  typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value);
const count = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
export function validBudgetContext(value: unknown): value is ProviderBudgetContext {
  return record(value) && validBudgetId(value.workflow) && validBudgetId(value.functionId) && validBudgetId(value.phase)
    && Object.keys(value).every((key) => ["workflow", "functionId", "sourceId", "jobId", "unitId", "phase", "continuationId"].includes(key) && validBudgetId(value[key]));
}
export function isProviderBudgetAction(value: string): value is ProviderBudgetRequest["action"] {
  return ["provider_budget_claim", "provider_budget_consume", "provider_budget_finish", "provider_budget_status"].includes(value);
}
export function isProviderBudgetRequest(value: { action: string }): value is ProviderBudgetRequest {
  return isProviderBudgetAction(value.action);
}
export function validateProviderBudgetRequest(request: ProviderBudgetRequest): void {
  if (!record(request) || !validBudgetId(request.operation_id)) throw new Error("STATE_STORAGE_INVALID_REQUEST");
  if (request.action === "provider_budget_status") {
    if (Object.keys(request).length !== 2) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    return;
  }
  if (!validBudgetId(request.window_id) || !validBudgetId(request.claim_id)) throw new Error("STATE_STORAGE_INVALID_REQUEST");
  if (request.action === "provider_budget_claim") {
    if (Object.keys(request).length !== (request.context === undefined ? 5 : 6)
      || (request.context !== undefined && !validBudgetContext(request.context))
      || !count(request.expected_ceiling) || request.expected_ceiling < 1 || request.expected_ceiling > 1000) throw new Error("STATE_STORAGE_INVALID_REQUEST");
  } else if (request.action === "provider_budget_consume") {
    if (Object.keys(request).length !== 6 || !validBudgetId(request.attempt_id) || typeof request.endpoint !== "string" || request.endpoint.length > 2048) throw new Error("STATE_STORAGE_INVALID_REQUEST");
    const split = request.endpoint.indexOf(" ");
    let url: URL;
    try { url = new URL(request.endpoint.slice(split + 1)); } catch { throw new Error("STATE_STORAGE_INVALID_REQUEST"); }
    const method = request.endpoint.slice(0, split);
    if (!/^[A-Z]{3,10}$/.test(method) || !["http:", "https:"].includes(url.protocol)
      || url.username || url.password || url.search || url.hash
      || request.endpoint !== `${method} ${url.origin}${url.pathname}`) throw new Error("STATE_STORAGE_INVALID_REQUEST");
  } else if (request.action === "provider_budget_finish") {
    if (Object.keys(request).length !== 5 || !["completed", "unknown"].includes(request.outcome)) throw new Error("STATE_STORAGE_INVALID_REQUEST");
  } else throw new Error("STATE_STORAGE_INVALID_REQUEST");
}
function validReceipt(value: unknown): value is NativeProviderBudgetReceipt {
  if (!record(value) || !validBudgetId(value.operation_id) || !validBudgetId(value.window_id)) return false;
  return count(value.ceiling) && value.ceiling >= 1 && value.ceiling <= 1000
    && count(value.window_ceiling) && value.window_ceiling >= value.ceiling && value.window_ceiling <= 10000
    && count(value.consumed) && value.consumed <= value.ceiling
    && count(value.window_consumed) && value.window_consumed >= value.consumed && value.window_consumed <= value.window_ceiling
    && value.remaining === value.ceiling - value.consumed && value.window_remaining === value.window_ceiling - value.window_consumed
    && ["available", "started", "completed", "unknown", "expired", "exhausted"].includes(String(value.state));
}
export function validProviderBudgetResponse(request: ProviderBudgetRequest, value: unknown): value is ProviderBudgetResponse {
  if (!record(value) || value.capability !== PROVIDER_BUDGET_CAPABILITY || value.action !== request.action
    || value.operation_id !== request.operation_id || typeof value.fresh !== "boolean") return false;
  if (request.action === "provider_budget_status") return value.status === "status" && value.fresh === false
    && (value.receipt === null || (validReceipt(value.receipt) && value.receipt.operation_id === request.operation_id));
  if (value.window_id !== request.window_id || value.claim_id !== request.claim_id || !validReceipt(value.receipt)
    || value.receipt.operation_id !== request.operation_id || value.receipt.window_id !== request.window_id) return false;
  if (value.status === "duplicate") return value.fresh === false
    && (request.action !== "provider_budget_consume" || value.attempt_id === request.attempt_id);
  if (value.fresh !== true) return false;
  if (request.action === "provider_budget_claim") return value.status === "claimed" && value.receipt.ceiling === request.expected_ceiling
    && value.receipt.consumed === 0 && value.receipt.state === "started";
  if (request.action === "provider_budget_consume") return value.status === "consumed" && value.attempt_id === request.attempt_id
    && value.receipt.consumed > 0 && value.receipt.state === "started";
  return value.status === "finished" && value.receipt.state === request.outcome;
}
export function createProviderAttemptBudgetTransport(sdk: IIIClient): ProviderBudgetTransport {
  return async (request) => {
    validateProviderBudgetRequest(request);
    const response = await sdk.trigger<ProviderBudgetRequest, unknown>({ function_id: "state::storage", payload: request });
    if (!validProviderBudgetResponse(request, response)) throw new Error("STATE_STORAGE_INVALID_RESPONSE");
    return response;
  };
}
