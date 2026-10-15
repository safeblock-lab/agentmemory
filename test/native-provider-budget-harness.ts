import { vi } from "vitest";
import { PROVIDER_BUDGET_CAPABILITY, type ProviderBudgetRequest, type ProviderBudgetResponse } from "../src/state/provider-attempt-budget.js";
export const primary = "https://primary.test/v1/chat";
export const auxiliary = "https://auxiliary.test/v1/chat";
export const context = { workflow: "summary", functionId: "mem::summarize", sourceId: "source-1", jobId: "job-1", unitId: "unit-1", phase: "map", continuationId: "continuation-1" };
export function harness(ceiling = 4, expectedContext?: typeof context) {
  let consumed = 0;
  let claimed = false;
  let state: "started" | "completed" | "unknown" = "started";
  let claim = "";
  const attempts = new Set<string>();
  const binding = { operationId: "operation-1", windowId: "window-1", ceiling, ...(expectedContext ? { context: { ...expectedContext } } : {}) };
  const receipt = () => ({ operation_id: binding.operationId, window_id: binding.windowId, ceiling,
    window_ceiling: ceiling, consumed, window_consumed: consumed, remaining: ceiling - consumed, window_remaining: ceiling - consumed, state });
  const native = async (request: ProviderBudgetRequest): Promise<ProviderBudgetResponse> => {
    const result = { capability: PROVIDER_BUDGET_CAPABILITY, action: request.action, operation_id: request.operation_id,
      fresh: false, status: "status", receipt: receipt() } as ProviderBudgetResponse;
    if (request.action === "provider_budget_status") return result;
    Object.assign(result, { window_id: request.window_id, claim_id: request.claim_id });
    if (request.action === "provider_budget_claim") {
      if (request.expected_ceiling !== ceiling || request.window_id !== binding.windowId
        || JSON.stringify(request.context) !== JSON.stringify(expectedContext)) throw new Error("STATE_PROVIDER_BUDGET_BINDING_MISMATCH");
      if (claimed) return { ...result, status: "duplicate" };
      claimed = true; claim = request.claim_id;
      return { ...result, fresh: true, status: "claimed" };
    }
    if (!claimed || request.claim_id !== claim) throw new Error("STATE_PROVIDER_BUDGET_ID_CONFLICT");
    if (request.action === "provider_budget_finish") {
      state = request.outcome;
      return { ...result, fresh: true, status: "finished", receipt: receipt() };
    }
    Object.assign(result, { attempt_id: request.attempt_id });
    if (attempts.has(request.attempt_id)) return { ...result, status: "duplicate" };
    if (!["POST " + primary, "POST " + auxiliary].includes(request.endpoint)) throw new Error("STATE_PROVIDER_BUDGET_ENDPOINT_NOT_APPROVED");
    if (consumed >= ceiling || state !== "started") throw new Error("STATE_PROVIDER_BUDGET_EXHAUSTED");
    attempts.add(request.attempt_id); consumed++;
    return { ...result, fresh: true, status: "consumed", receipt: receipt() };
  };
  return { binding, receipt, native, transport: vi.fn(native) };
}
