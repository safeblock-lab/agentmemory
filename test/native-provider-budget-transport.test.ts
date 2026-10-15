import { describe, expect, it, vi } from "vitest";
import type { IIIClient } from "iii-sdk";
import { createProviderAttemptBudgetTransport, validateProviderBudgetRequest, type ProviderBudgetRequest } from "../src/state/provider-attempt-budget.js";
import { harness } from "./native-provider-budget-harness.js";
describe("explicit State SDK transport", () => {
  it("uses only state::storage with identity assertions", async () => {
    const m = harness();
    const trigger = vi.fn(async ({ payload }: { payload: ProviderBudgetRequest }) => m.native(payload));
    const transport = createProviderAttemptBudgetTransport({ trigger } as unknown as IIIClient);
    const request: ProviderBudgetRequest = { action: "provider_budget_claim", operation_id: "operation-1", window_id: "window-1", expected_ceiling: 4, claim_id: "claim-1" };
    expect(await transport(request)).toMatchObject({ fresh: true, status: "claimed" });
    expect(trigger).toHaveBeenCalledExactlyOnceWith({ function_id: "state::storage", payload: request });
    expect(JSON.stringify(request)).not.toMatch(/config|path|grant|ledger/);
  });
  it("rejects caller path/authority fields and malformed native ACK", async () => {
    const invalid = { action: "provider_budget_status", operation_id: "operation-1", configPath: "caller-path" } as unknown as ProviderBudgetRequest;
    expect(() => validateProviderBudgetRequest(invalid)).toThrow("STATE_STORAGE_INVALID_REQUEST");
    const trigger = vi.fn(async () => ({ fresh: true, status: "consumed" }));
    await expect(createProviderAttemptBudgetTransport({ trigger } as unknown as IIIClient)({ action: "provider_budget_status", operation_id: "operation-1" })).rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
  });
  it("the shared StateStorage consumer preserves exact correlated budget validation", async () => {
    const { triggerStateStorage } = await import("../src/state/storage.js");
    const m = harness();
    const trigger = vi.fn(async ({ payload }: { payload: ProviderBudgetRequest }) => m.native(payload));
    const sdk = { trigger } as unknown as IIIClient;
    const request: ProviderBudgetRequest = { action: "provider_budget_claim", operation_id: "operation-1", window_id: "window-1", expected_ceiling: 4, claim_id: "consumer-claim" };
    expect(await triggerStateStorage(sdk, request)).toMatchObject({ fresh: true, claim_id: "consumer-claim" });
    trigger.mockImplementation(async ({ payload }) => ({ ...await m.native(payload), claim_id: "wrong" }));
    await expect(triggerStateStorage(sdk, request)).rejects.toThrow("STATE_STORAGE_INVALID_RESPONSE");
  });
});
