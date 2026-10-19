import type { StateKV } from "../state/kv.js";
import { KV, generateId } from "../state/schema.js";
import type { DashboardWorkflowKey, DashboardWorkflowStatistics, DashboardWorkflowTerminal, RecentOperation, RecentOperationFeed } from "../types.js";
import { getAgentId } from "../config.js";
import { stripPrivateData } from "./privacy.js";
import { logger } from "../logger.js";
import { recordFailureMinute } from "./dashboard-operational-metrics.js";
import { readGraphInvocations, recordGraphInvocation } from "./workflow-attempt-accounting.js";
export const PROCESSING_OPERATION_NAMES = {
  "mem::skill-extract": "Skill extraction", "mem::profile": "Project profile",
  "mem::consolidate": "Memory consolidation", "mem::consolidate-pipeline": "Consolidation pipeline",
  "mem::patterns": "Pattern analysis", "mem::generate-rules": "Rule generation",
  "mem::reflect": "Reflection", "mem::crystallize": "Crystallization",
  "mem::reflect-unit": "Reflection unit",
  "mem::skill-lifecycle-dispatch": "Queued skill extraction",
  "mem::graph-dispatch-deliver": "Queued graph extraction",
  "mem::flow-compress": "Workflow compression", "mem::enrich-window": "Observation enrichment",
  "mem::enrich-session": "Session enrichment", "mem::compress-file": "File compression",
  "mem::temporal-graph-extract": "Temporal graph extraction", "mem::retention-score": "Retention scoring",
  "mem::retention-evict": "Retention eviction", "mem::auto-forget": "Automatic forgetting",
  "mem::evict": "Memory eviction", "mem::cascade-update": "Cascade update", "mem::heal": "Memory repair",
  "mem::slot-reflect": "Slot reflection", "mem::vision-embed": "Image embedding",
  "mem::migrate": "Memory migration", "mem::graph::import-graphify": "Graph import",
} as const;

export type DashboardActivityItem = RecentOperation;
export interface DashboardActivity {
  source: "native-operation-feed";
  limit: 30;
  items: RecentOperation[];
}

const FUNCTIONS = new Set(["mem::compress", "mem::summarize", "mem::graph-extract", "mem::summary-enqueue", "mem::summary-unit", "mem::summary-final", ...Object.keys(PROCESSING_OPERATION_NAMES)]);
const OUTCOMES = new Set(["completed", "failed", "skipped", "queued", "partial"]);
const COUNTS = ["observationsProcessed", "observationsCompressed", "summariesCreated", "nodesAdded", "edgesAdded", "unitsQueued", "unitsCompleted", "sessionsProcessed", "skillsCreated", "skillsReinforced", "itemsProcessed", "itemsCreated", "itemsRemoved", "itemsFailed"] as const;
const OPERATION_REASONS = new Set(["cached", "no_work", "paused", "deduplicated", "stale", "deterministic_fallback", "fallback"]);
const NAMES: Record<RecentOperation["functionId"], string> = {
  "mem::compress": "Compression", "mem::summarize": "Session summary", "mem::graph-extract": "Graph extraction",
  "mem::summary-enqueue": "Summary queued", "mem::summary-unit": "Summary unit", "mem::summary-final": "Queued session summary",
  ...PROCESSING_OPERATION_NAMES,
};
const PUBLIC_ERRORS = {
  invalid_input: "Required operation input is missing or invalid.",
  session_not_found: "Session was not found.",
  no_observations: "No observations are available.",
  no_provider: "No provider is configured.",
  empty_provider_response: "Provider returned no usable output.",
  parse_failed: "Operation output could not be parsed.",
  validation_failed: "Operation output did not pass validation.",
  compression_failed: "Compression failed.",
  summary_failed: "Session summary failed.",
  graph_extraction_failed: "Graph extraction failed.",
  operation_failed: "Operation failed.",
} as const;

export interface OperationDetails {
  operationId?: string;
  jobId?: string;
  durationMs?: number;
  errorCode?: string;
  reason?: RecentOperation["reason"];
  executionProducer?: string;
  executionId?: string;
  executionLevel?: "workflow" | "unit";
  occurredAt?: string;
}

const WORKFLOW_FUNCTIONS: Partial<Record<RecentOperation["functionId"], DashboardWorkflowKey>> = {
  "mem::summarize": "summaries", "mem::summary-final": "summaries", "mem::summary-unit": "summaries",
  "mem::skill-extract": "skills", "mem::graph-extract": "graph",
  "mem::reflect": "reflection", "mem::reflect-unit": "reflection",
  "mem::consolidate": "consolidation", "mem::consolidate-pipeline": "consolidation",
  "mem::compress": "compression",
};
export const DASHBOARD_WORKFLOWS: DashboardWorkflowKey[] = [
  "summaries", "skills", "graph", "reflection", "consolidation", "compression", "snapshots", "maintenance",
];
interface WorkflowRead { version: 1; revision: string; asOf: string; workflows: DashboardWorkflowStatistics[] }
const collectors = new WeakMap<StateKV, Promise<void>>();
const pendingInvalidations = new WeakMap<StateKV, Set<DashboardWorkflowKey>>();
const collectorId = generateId("collector");

type WorkflowStage = "collector_read" | "collector_activate" | "pending_invalidation" | "terminal_record" | "receipt_validation";
const WORKFLOW_ERROR_NAMES = new Set(["Error", "TypeError", "RangeError", "TimeoutError", "AbortError", "InvocationError"]);
const WORKFLOW_ERROR_CODES = new Set([
  "STATE_TX_REPLAY_CONFLICT", "STATE_TX_INVALID_REQUEST", "STATE_RETRIEVAL_RESOURCE_LIMIT", "ETIMEDOUT", "TIMEOUT",
  "workflow_statistics_record_rejected", "workflow_statistics_read_invalid",
]);
const WORKFLOW_PRODUCERS = new Set(["observation-compression", "summary-queue", "graph-job", ...FUNCTIONS]);

class WorkflowDiagnosticError extends Error {
  constructor(readonly stage: WorkflowStage, cause: unknown, readonly operationStage?: WorkflowStage) {
    super("workflow_statistics_failed", { cause });
  }
}
const workflowErrorStages = new WeakMap<object, { stage: WorkflowStage; operationStage?: WorkflowStage }>();

function workflowFailure(stage: WorkflowStage, error: unknown, operationStage?: WorkflowStage): unknown {
  if (error && typeof error === "object") {
    if (!workflowErrorStages.has(error)) workflowErrorStages.set(error, { stage, operationStage });
    return error;
  }
  return new WorkflowDiagnosticError(stage, error, operationStage);
}

async function workflowStage<T>(stage: WorkflowStage, operation: () => Promise<T>): Promise<T> {
  try { return await operation(); }
  catch (error) {
    throw workflowFailure(stage, error);
  }
}

function workflowErrorDiagnostic(error: unknown) {
  const original = error instanceof WorkflowDiagnosticError ? error.cause : error;
  const diagnostic = error instanceof WorkflowDiagnosticError ? error
    : error && typeof error === "object" ? workflowErrorStages.get(error) : undefined;
  const value = original && typeof original === "object" ? original as Record<string, unknown> : undefined;
  const name = value?.name;
  const code = typeof value?.code === "string" && WORKFLOW_ERROR_CODES.has(value.code) ? value.code
    : typeof value?.message === "string" && WORKFLOW_ERROR_CODES.has(value.message) ? value.message : "unknown";
  return {
    stage: diagnostic?.stage ?? "unknown",
    ...(diagnostic?.operationStage ? { operationStage: diagnostic.operationStage } : {}),
    errorClass: typeof name === "string" && WORKFLOW_ERROR_NAMES.has(name) ? name : "unknown",
    errorCode: code,
    timeout: name === "TimeoutError" || code === "ETIMEDOUT" || code === "TIMEOUT",
  };
}

function workflowTerminalDiagnostic(terminal: DashboardWorkflowTerminal & { producer: string }) {
  return {
    workflow: DASHBOARD_WORKFLOWS.includes(terminal.workflow) ? terminal.workflow : "unknown",
    producer: WORKFLOW_PRODUCERS.has(terminal.producer) ? terminal.producer : "other",
    executionIdPresent: typeof terminal.executionId === "string" && terminal.executionId.length > 0,
    executionIdLength: typeof terminal.executionId === "string" ? terminal.executionId.length : 0,
    agentIdPresent: typeof terminal.agentId === "string" && terminal.agentId.length > 0,
    agentIdLength: typeof terminal.agentId === "string" ? terminal.agentId.length : 0,
    occurredAtValid: typeof terminal.occurredAt === "string" && Number.isFinite(Date.parse(terminal.occurredAt)),
  };
}

async function workflowMutation(kv: StateKV, input: Record<string, unknown>): Promise<void> {
  const stage = input.kind === "terminal" ? "terminal_record" : input.kind === "activate" ? "collector_activate" : "pending_invalidation";
  const receipt = await workflowStage(stage, () => kv.retrieval<{ version: number; recorded?: boolean; activated?: boolean; invalid?: boolean; sticky?: boolean }>({ action: "workflow_statistics_record_v1", input }, { timeoutMs: 1500 }));
  const accepted = input.kind === "terminal" ? receipt?.recorded === true
    : input.kind === "activate" ? typeof receipt?.activated === "boolean" && receipt.sticky === true
    : input.kind === "invalidate" && receipt?.invalid === true && receipt.sticky === true;
  if (receipt?.version !== 1 || !accepted) throw workflowFailure("receipt_validation", new Error("workflow_statistics_record_rejected"), stage);
}

export async function invalidateWorkflowStatistics(kv: StateKV, workflow: DashboardWorkflowKey, reason = "producer_gap"): Promise<void> {
  let pending = pendingInvalidations.get(kv);
  if (!pending) { pending = new Set(); pendingInvalidations.set(kv, pending); }
  pending.add(workflow);
  await workflowMutation(kv, { kind: "invalidate", workflow, reason });
  pending.delete(workflow);
}

async function ensureWorkflowCollector(kv: StateKV): Promise<void> {
  let ready = collectors.get(kv);
  if (!ready) {
    ready = (async () => {
      for (const workflow of DASHBOARD_WORKFLOWS) {
        await workflowMutation(kv, { kind: "activate", workflow, collector_id: collectorId });
      }
    })();
    collectors.set(kv, ready);
  }
  try {
    await ready;
    for (const workflow of [...(pendingInvalidations.get(kv) ?? [])]) {
      await invalidateWorkflowStatistics(kv, workflow, "recorder_loss");
    }
  } catch (error) {
    collectors.delete(kv);
    throw workflowFailure("collector_read", error);
  }
}

export async function readWorkflowStatistics(kv: StateKV, agentId?: string): Promise<WorkflowRead> {
  await ensureWorkflowCollector(kv);
  const result = await kv.retrieval<WorkflowRead>({ action: "workflow_statistics_read_v1", ...(agentId ? { agent_id: agentId } : {}) }, { timeoutMs: 1500 });
  if (result?.version !== 1 || !Array.isArray(result.workflows) || result.workflows.length !== DASHBOARD_WORKFLOWS.length
    || DASHBOARD_WORKFLOWS.some(key => !result.workflows.some(item => item.workflow === key && item.agentScope === (agentId ?? null))))
    throw new Error("workflow_statistics_read_invalid");
  const graph = result.workflows.find(item => item.workflow === "graph");
  if (graph) graph.invocationHistory = await readGraphInvocations(kv, result.asOf, agentId);
  return result;
}

export async function recordWorkflowTerminal(kv: StateKV, terminal: DashboardWorkflowTerminal & { producer: string }): Promise<void> {
  try {
    await ensureWorkflowCollector(kv);
    await workflowMutation(kv, {
      kind: "terminal", workflow: terminal.workflow, producer: terminal.producer,
      execution_id: terminal.executionId, execution_level: terminal.executionLevel,
      agent_id: terminal.agentId, occurred_at: terminal.occurredAt, outcome: terminal.outcome,
    });
  } catch (error) {
    try { await invalidateWorkflowStatistics(kv, terminal.workflow, "recorder_failure"); }
    catch (invalidationError) {
      logger.warn("Workflow terminal failure invalidation pending", { ...workflowTerminalDiagnostic(terminal), ...workflowErrorDiagnostic(invalidationError), failureRole: "secondary" });
    }
    throw error;
  }
}

export async function observeWorkflowTerminal(kv: StateKV, terminal: DashboardWorkflowTerminal & { producer: string }): Promise<void> {
  try { await recordWorkflowTerminal(kv, terminal); }
  catch (error) { logger.warn("Durable workflow terminal could not be recorded", { ...workflowTerminalDiagnostic(terminal), ...workflowErrorDiagnostic(error), failureRole: "primary" }); }
}

export async function markWorkflowStatisticsGap(kv: StateKV, workflow: DashboardWorkflowKey): Promise<void> {
  try { await invalidateWorkflowStatistics(kv, workflow); }
  catch (error) { logger.warn("Workflow coverage invalidation pending", { workflow: DASHBOARD_WORKFLOWS.includes(workflow) ? workflow : "unknown", ...workflowErrorDiagnostic(error) }); }
}

async function recordWorkflowOperation(kv: StateKV, functionId: RecentOperation["functionId"], outcome: RecentOperation["outcome"], agentId: string | undefined, details: OperationDetails): Promise<void> {
  const workflow = WORKFLOW_FUNCTIONS[functionId];
  if (!workflow || outcome !== "completed" && outcome !== "partial" && outcome !== "failed") return;
  if (details.executionLevel === "unit") return;
  try {
    await ensureWorkflowCollector(kv);
    if (!details.executionId || !details.occurredAt || details.executionLevel !== "workflow") {
      await invalidateWorkflowStatistics(kv, workflow);
      return;
    }
    await recordWorkflowTerminal(kv, {
      workflow, producer: details.executionProducer ?? functionId,
      executionId: details.executionId, executionLevel: details.executionLevel,
      agentId: agentId ?? null, occurredAt: details.occurredAt, outcome,
    });
  } catch (error) {
    try { await invalidateWorkflowStatistics(kv, workflow, "recorder_failure"); }
    catch (invalidationError) {
      logger.warn("Workflow operation failure invalidation pending", { workflow, ...workflowErrorDiagnostic(invalidationError), failureRole: "secondary" });
    }
    logger.warn("Workflow statistics could not be persisted", { functionId, outcome, ...workflowErrorDiagnostic(error), failureRole: "primary" });
  }
}

function publicError(code: unknown): RecentOperation["error"] {
  if (typeof code !== "string") return undefined;
  const safeCode = Object.hasOwn(PUBLIC_ERRORS, code) ? code as keyof typeof PUBLIC_ERRORS : "operation_failed";
  return { code: safeCode, message: PUBLIC_ERRORS[safeCode] };
}

function sanitizedOperation(value: unknown): RecentOperation | null {
  if (!value || typeof value !== "object") return null;
  const entry = value as Record<string, unknown>;
  if (typeof entry.id !== "string" || !/^op_[a-z0-9_]{1,80}$/.test(entry.id)
    || typeof entry.timestamp !== "string" || !Number.isFinite(Date.parse(entry.timestamp))
    || typeof entry.functionId !== "string" || !FUNCTIONS.has(entry.functionId)
    || typeof entry.outcome !== "string" || !OUTCOMES.has(entry.outcome)) return null;
  const counts: RecentOperation["counts"] = {};
  const raw = entry.counts && typeof entry.counts === "object" ? entry.counts as Record<string, unknown> : {};
  for (const key of COUNTS) {
    const count = raw[key];
    if (typeof count === "number" && Number.isSafeInteger(count) && count >= 0) counts[key] = count;
  }
  const agentId = typeof entry.agentId === "string"
    ? stripPrivateData(entry.agentId).replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 128) : undefined;
  const durationMs = typeof entry.durationMs === "number" && Number.isSafeInteger(entry.durationMs) && entry.durationMs >= 0
    ? entry.durationMs : undefined;
  const error = publicError(entry.error && typeof entry.error === "object"
    ? (entry.error as Record<string, unknown>).code : undefined);
  return {
    id: entry.id, timestamp: new Date(entry.timestamp).toISOString(),
    functionId: entry.functionId as RecentOperation["functionId"],
    outcome: entry.outcome as RecentOperation["outcome"], counts,
    name: NAMES[entry.functionId as RecentOperation["functionId"]],
    ...(durationMs !== undefined ? { durationMs } : {}),
    ...(typeof entry.reason === "string" && OPERATION_REASONS.has(entry.reason) ? { reason: entry.reason as RecentOperation["reason"] } : {}),
    ...(error ? { error } : {}),
    ...(agentId ? { agentId } : {}),
  };
}

export async function recordOperation(
  kv: StateKV,
  functionId: RecentOperation["functionId"],
  outcome: RecentOperation["outcome"],
  counts: RecentOperation["counts"],
  agentId = getAgentId(),
  details: OperationDetails = {},
): Promise<void> {
  const operationId = details.operationId ?? generateId("op");
  const occurredAt = details.occurredAt ?? new Date().toISOString();
  if (functionId === "mem::graph-extract" && details.executionLevel === "unit") {
    try {
      await recordGraphInvocation(kv, { executionId: operationId, occurredAt, agentId: agentId ?? null, outcome,
        ...(details.jobId ? { jobId: details.jobId } : {}),
        ...(details.errorCode === "graph_extraction_failed" ? { errorCode: "graph_extraction_failed" as const } : {}) });
    } catch {
      await markWorkflowStatisticsGap(kv, "graph");
      logger.warn("Graph invocation history could not be persisted", { functionId, outcome });
    }
  }
  await recordWorkflowOperation(kv, functionId, outcome, agentId, details);
  try {
    const item = sanitizedOperation({
      id: operationId, timestamp: occurredAt, functionId, outcome, counts, agentId,
      durationMs: details.durationMs, error: publicError(details.errorCode), reason: details.reason,
    });
    if (!item) throw new Error("Invalid operation metadata");
    if (item.outcome === "failed") await recordFailureMinute(kv, Date.parse(item.timestamp), agentId);
    // Native append and eviction share one state mutation; never read/modify/set.
    const result = await kv.update<{ errors?: unknown }>(KV.recentOperations, "current", [
      { type: "append_bounded", path: "items", value: { item, limit: 30 } },
    ]);
    if (Array.isArray(result?.errors) && result.errors.length > 0) throw new Error("Operation feed update rejected");
  } catch {
    logger.warn("Recent operation could not be persisted", { functionId, outcome });
  }
}

export function createDashboardActivity(kv: StateKV) {
  return {
    async snapshot(agentId?: string): Promise<DashboardActivity> {
      const feed = await kv.get<RecentOperationFeed>(KV.recentOperations, "current");
      const items = (Array.isArray(feed?.items) ? feed.items.slice(-30) : [])
        .map(sanitizedOperation).filter((item): item is RecentOperation => item !== null)
        .reverse().filter(item => !agentId || item.agentId === agentId);
      return { source: "native-operation-feed", limit: 30, items };
    },
  };
}
