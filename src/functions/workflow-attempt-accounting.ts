import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId } from "../state/schema.js";
import { withProviderAttemptBudget, type ProviderAttemptBudgetBinding } from "../providers/provider-attempt-budget.js";
import { validBudgetId } from "../state/provider-attempt-budget.js";
import type { DashboardWorkflowInvocation, DashboardWorkflowInvocationHistory } from "../types.js";

const invocationHistoryKey = (agentId?: string) => `workflow-invocations:graph:v1:${agentId ? fingerprintId("agent", agentId) : "all"}`;

export async function recordGraphInvocation(kv: StateKV, attempt: DashboardWorkflowInvocation): Promise<void> {
  const histories = [invocationHistoryKey(), ...(attempt.agentId ? [invocationHistoryKey(attempt.agentId)] : [])];
  const keys = [...histories, ...(attempt.outcome === "failed" ? histories.map(key => `${key}:failures`) : [])];
  await Promise.all(keys.map(async key => {
    const result = await kv.update<{ errors?: unknown }>(KV.state, key, [
      { type: "append_bounded", path: "items", value: { item: attempt, limit: 100 } },
    ]);
    if (Array.isArray(result?.errors) && result.errors.length) throw new Error("workflow_invocation_record_rejected");
  }));
}

export async function readGraphInvocations(kv: StateKV, asOf: string, agentId?: string): Promise<DashboardWorkflowInvocationHistory> {
  const end = Math.floor(Date.parse(asOf) / 60_000) * 60_000;
  const start = end - 24 * 60 * 60_000;
  const window = { intervalStart: new Date(start).toISOString(), intervalEnd: new Date(end).toISOString(),
    coverageSince: null, complete: false as const };
  try {
    const histories = await Promise.all([invocationHistoryKey(agentId), `${invocationHistoryKey(agentId)}:failures`]
      .map(key => kv.get<{ items?: DashboardWorkflowInvocation[] }>(KV.state, key)));
    if (histories.some(history => history && !Array.isArray(history.items))) throw new Error("workflow_invocation_read_invalid");
    const unique = new Map<string, DashboardWorkflowInvocation>();
    for (const item of histories.flatMap(history => history?.items ?? [])) {
      if (!item || typeof item.executionId !== "string" || !/^op_[a-z0-9_]{1,80}$/.test(item.executionId)
        || !Number.isFinite(Date.parse(item.occurredAt)) || !["completed", "partial", "failed", "queued", "skipped"].includes(item.outcome)
        || item.agentId !== null && typeof item.agentId !== "string" || agentId && item.agentId !== agentId)
        throw new Error("workflow_invocation_read_invalid");
      unique.set(item.executionId, item);
    }
    const attempts = [...unique.values()].sort((a, b) => Date.parse(b.occurredAt) - Date.parse(a.occurredAt) || b.executionId.localeCompare(a.executionId));
    return { source: "state-workflow-invocations-v1", state: "retained", lastAttempt: attempts[0] ?? null,
      failures24h: { ...window, observedCount: attempts.filter(item => item.outcome === "failed" && Date.parse(item.occurredAt) >= start && Date.parse(item.occurredAt) < end).length,
        state: "partial", reason: "bounded_invocation_history_full_total_unknown" } };
  } catch {
    return { source: "state-workflow-invocations-v1", state: "unavailable", lastAttempt: null,
      failures24h: { ...window, observedCount: null, state: "unavailable", reason: "invocation_history_unavailable" } };
  }
}

export interface WorkflowAttemptIdentity {
  workflow: "summary" | "skill" | "consolidation" | "graph";
  functionId: string;
  project: string;
  sourceId: string;
  sourceFingerprint: string;
  jobId: string;
  unitId: string;
  phase: "generation" | "publication";
  continuation: string;
}
type Scope = Pick<WorkflowAttemptIdentity, "workflow" | "project" | "sourceId">;
export interface TrustedWorkflowAttempt extends WorkflowAttemptIdentity, ProviderAttemptBudgetBinding {}
const fields = ["workflow", "functionId", "project", "sourceId", "sourceFingerprint", "jobId", "unitId", "phase", "continuation"] as const;
const scopeFields = ["workflow", "project", "sourceId"] as const;
const bindingFields = [...fields, "operationId", "windowId", "ceiling"] as const;
function configurationRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function closedFields(value: Record<string, unknown>, expected: readonly string[]): boolean {
  return Object.keys(value).length === expected.length && Object.keys(value).every(field => expected.includes(field));
}
function validScope(value: unknown): value is Scope {
  return configurationRecord(value) && ["summary", "skill", "consolidation", "graph"].includes(String(value.workflow))
    && typeof value.project === "string" && value.project.length > 0 && Buffer.byteLength(value.project) <= 4096
    && validBudgetId(value.sourceId);
}
function validConfiguredBinding(value: unknown): value is TrustedWorkflowAttempt {
  return validScope(value) && configurationRecord(value) && closedFields(value, bindingFields)
    && fields.every(field => typeof value[field] === "string" && Buffer.byteLength(value[field]) <= 4096 && value[field].length > 0)
    && ["functionId", "sourceId", "jobId", "unitId", "operationId", "windowId"].every(field => validBudgetId(value[field]))
    && ["generation", "publication"].includes(String(value.phase))
    && Number.isSafeInteger(value.ceiling) && Number(value.ceiling) >= 1 && Number(value.ceiling) <= 1000;
}

/** Process-private startup configuration; native private grants authorize every claim. */
export function loadWorkflowAttemptAccounting(raw: string | undefined): WorkflowAttemptAccounting | undefined {
  if (raw === undefined) return undefined;
  const invalid = () => new Error("workflow_binding_configuration_invalid");
  if (!raw || Buffer.byteLength(raw) > 65536) throw invalid();
  let value: unknown;
  try { value = JSON.parse(raw); } catch { throw invalid(); }
  if (!configurationRecord(value) || !closedFields(value, ["version", "scopes", "bindings"]) || value.version !== 1
    || !Array.isArray(value.scopes) || !Array.isArray(value.bindings)
    || value.scopes.length > 256 || value.bindings.length > 256
    || !value.scopes.every(scope => validScope(scope) && closedFields(scope, scopeFields))
    || !value.bindings.every(validConfiguredBinding)) throw invalid();
  const scopes: Scope[] = value.scopes;
  const bindings: TrustedWorkflowAttempt[] = value.bindings;
  if (new Set(scopes.map(key)).size !== scopes.length) throw invalid();
  return new WorkflowAttemptAccounting(scopes, bindings);
}
function exact(left: WorkflowAttemptIdentity, right: WorkflowAttemptIdentity): boolean {
  return fields.every(field => left[field] === right[field]);
}
function key(identity: Scope): string {
  return fingerprintId("workflow-binding", JSON.stringify(scopeFields.map(field => identity[field])));
}

/** Construct only from private daemon configuration; never from function payloads. */
export class WorkflowAttemptAccounting {
  private readonly scopes: readonly Readonly<Scope>[];
  private readonly bindings: readonly Readonly<TrustedWorkflowAttempt>[];
  constructor(scopes: readonly Scope[], bindings: readonly TrustedWorkflowAttempt[]) {
    this.scopes = Object.freeze(scopes.map(scope => Object.freeze({ ...scope })));
    this.bindings = Object.freeze(bindings.map(binding => Object.freeze({ ...binding })));
    const operations = new Set<string>();
    for (const binding of this.bindings) {
      if (fields.some(field => typeof binding[field] !== "string" || !binding[field]) ||
          !binding.operationId || !binding.windowId || !Number.isSafeInteger(binding.ceiling) || binding.ceiling < 1 ||
          operations.has(binding.operationId) || !this.isScoped(binding)) throw new Error("workflow_binding_configuration_invalid");
      operations.add(binding.operationId);
    }
  }
  isScoped(identity: Scope): boolean {
    return this.scopes.some(scope => scopeFields.every(field => scope[field] === identity[field]));
  }
  async run<T>(kv: StateKV, identity: WorkflowAttemptIdentity, run: () => Promise<T>): Promise<T> {
    const rootKey = key(identity);
    const retained = await kv.get<{ scoped: true }>(KV.state, rootKey);
    if (!this.isScoped(identity)) {
      if (retained) throw new Error("workflow_binding_configuration_missing");
      return run();
    }
    const matches = this.bindings.filter(binding => exact(binding, identity));
    if (matches.length !== 1) throw new Error("workflow_binding_identity_mismatch");
    const approved = matches[0];
    const assertion = { ...identity, operationId: approved.operationId, windowId: approved.windowId, ceiling: approved.ceiling };
    const assertionKey = `${rootKey}:${fingerprintId("dispatch", JSON.stringify(fields.map(field => identity[field])))}`;
    const previous = await kv.get<TrustedWorkflowAttempt>(KV.state, assertionKey);
    if (previous && (!exact(previous, assertion) || previous.operationId !== assertion.operationId ||
        previous.windowId !== assertion.windowId || previous.ceiling !== assertion.ceiling))
      throw new Error("workflow_binding_assertion_mismatch");
    await kv.set(KV.state, rootKey, { scoped: true });
    if (!previous) await kv.set(KV.state, assertionKey, assertion);
    const budgetBinding = { operationId: approved.operationId, windowId: approved.windowId, ceiling: approved.ceiling,
      context: { workflow: identity.workflow, functionId: identity.functionId, sourceId: identity.sourceId,
        jobId: identity.jobId, unitId: identity.unitId, phase: identity.phase,
        continuationId: fingerprintId("workflow-continuation", identity.continuation) } };
    return withProviderAttemptBudget(budgetBinding, run);
  }
}

export function runWorkflowAttempt<T>(accounting: WorkflowAttemptAccounting | undefined, kv: StateKV,
  identity: WorkflowAttemptIdentity, run: () => Promise<T>): Promise<T> {
  if (accounting) return accounting.run(kv, identity, run);
  return kv.get(KV.state, key(identity)).then(retained => {
    if (retained) throw new Error("workflow_binding_configuration_missing");
    return run();
  });
}
