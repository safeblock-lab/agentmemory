import { validManualReflectContinuation } from "../functions/reflect-queue-types.js";
import { InvocationError, type IIIClient } from "iii-sdk";
import type { HttpRequest } from "@iii-dev/helpers/http";
import { randomBytes } from "node:crypto";
import type { Session, CompressedObservation, HookPayload, CommitLink } from "../types.js";
import { EXPORT_COLLECTIONS, type ExportCollection, type GraphEdge, type GraphNode } from "../types.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { KV } from "../state/schema.js";
import { checkPayloadFrameSize } from "../state/frame-guard.js";
import { StateKV } from "../state/kv.js";
import { getLatestHealth } from "../health/monitor.js";
import type { MetricsStore } from "../eval/metrics-store.js";
import type { ResilientProvider } from "../providers/resilient.js";
import { III_PINNED_VERSION, VERSION } from "../version.js";
import { CONSOLIDATION_COUNTS_REUSE_MS, CONSOLIDATION_LAST_RUN_KEY, describeConsolidation, describeConsolidationPreview, type ConsolidationRunRecord } from "../functions/consolidation-status.js";
import { readConsolidationCounts } from "./consolidation-counts.js";
import { UNINDEXED_SCAN_REUSE_MS, evaluateStatus, prefersHtml, renderStatusHtml, singleFlight, type GraphStatsInput } from "../functions/status.js";
import { IndexedRetrievalError, readNativeIndexStatus } from "../state/indexed-retrieval.js";
import { createDashboardActivity } from "../functions/dashboard-activity.js";
import { buildWorkflowStatus, createDashboardOperationalMetrics } from "../functions/dashboard-operational-metrics.js";
import { createEngineMemoryReader } from "../functions/engine-memory.js";
import { readSavingsReport } from "../functions/savings.js";
import {
  getEmbeddingProvider,
  findUnindexedObservations,
  getIndexPersistenceStatus,
  getPendingVectorBackfillCount,
  getSearchIndex,
  getVectorIndex,
  isBm25RebuildIncomplete,
} from "../functions/search.js";
import { timingSafeCompare } from "../auth.js";
import { isSlotsEnabled, isReflectEnabled } from "../functions/slots.js";
import { renderViewerDocument } from "../viewer/document.js";
import { getBoundViewerPort, getViewerSkipped } from "../viewer/server.js";
import { MAX_FILES_UPPER_BOUND } from "../functions/replay.js";
import { validManualSummaryContinuation } from "../functions/summary-queue.js";
import { logger } from "../logger.js";
import { buildScopedGraph, graphBuildHttpStatus, MAX_GRAPH_BUILD_CONTINUATION_BYTES } from "./graph-build.js";
import { collectProjectedRecords } from "../functions/projected-record-reader.js";
import { viewerMemories, viewerPage, viewerRecentPage, viewerPageOptions, VIEWER_SESSION_FIELDS, VIEWER_OBSERVATION_FIELDS } from "../functions/viewer-memory-page.js";
import { StatePageError } from "../state/state-pages.js";
import { graphAutomaticAvailability } from "../functions/graph-automatic-authority.js";
import {
  isGraphExtractionEnabled,
  isConsolidationEnabled,
  isAutoCompressEnabled,
  isContextInjectionEnabled,
  detectEmbeddingProvider,
  detectLlmProviderKind,
  getAgentId,
  isAgentScopeIsolated,
  isBackgroundRecoveryPaused,
  getEnvVar,
  loadConfig,
  loadSnapshotConfig,
} from "../config.js";

type Response = {
  status_code: number;
  headers?: Record<string, string>;
  body: unknown;
};

function parseOptionalInt(raw: unknown): number | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = typeof raw === "number" ? raw : parseInt(String(raw), 10);
  return Number.isFinite(n) ? n : undefined;
}

function checkAuth(
  req: HttpRequest,
  secret: string | undefined,
): Response | null {
  if (!secret) return null;
  const auth = req.headers?.["authorization"] || req.headers?.["Authorization"];
  if (
    typeof auth !== "string" ||
    !timingSafeCompare(auth, `Bearer ${secret}`)
  ) {
    return { status_code: 401, body: { error: "unauthorized" } };
  }
  return null;
}

function requireConfiguredSecret(
  secret: string | undefined,
  feature: string,
): Response | null {
  if (secret) return null;
  return {
    status_code: 503,
    body: { error: `${feature} requires AGENTMEMORY_SECRET` },
  };
}

function flagDisabledResponse(opts: {
  error: string;
  flag: string;
  enableHow: string;
  docsHref: string;
}): Response {
  return {
    status_code: 503,
    body: opts,
  };
}

function graphDisabledResponse(): Response {
  return flagDisabledResponse({
    error: "Knowledge graph not enabled",
    flag: "GRAPH_EXTRACTION_ENABLED",
    enableHow: "Set GRAPH_EXTRACTION_ENABLED=true and restart. Requires an LLM provider key.",
    docsHref: "https://github.com/rohitg00/agentmemory#knowledge-graph",
  });
}

function graphReadFailureResponse(operation: "query" | "stats", error: unknown): Response {
  const nativeResourceCode = "STATE_RETRIEVAL_RESOURCE_LIMIT";
  const wrappedResourcePrefix = "invocation_failed: STATE_RETRIEVAL_RESOURCE_LIMIT:";
  const hasNativeResourceCode = error && typeof error === "object" && "code" in error
    && error.code === nativeResourceCode;
  const hasWrappedResourceCode = error instanceof InvocationError && error.code === "invocation_failed"
    && error.message.startsWith(wrappedResourcePrefix);
  const code = hasNativeResourceCode || hasWrappedResourceCode ? nativeResourceCode : undefined;
  logger.warn(`graph-${operation} failed`, { code: code ?? "unknown" });
  if (code) {
    return { status_code: 503, body: { error: `graph_${operation}_resource_limit`, code } };
  }
  if (!isGraphExtractionEnabled()) return graphDisabledResponse();
  return { status_code: 502, body: { error: `graph_${operation}_failed` } };
}

function consolidationDisabledResponse(): Response {
  return flagDisabledResponse({
    error: "Consolidation pipeline not enabled",
    flag: "CONSOLIDATION_ENABLED",
    enableHow: "Set CONSOLIDATION_ENABLED=true and restart. Requires an LLM provider key.",
    docsHref: "https://github.com/rohitg00/agentmemory#consolidation",
  });
}

function slotsDisabledResponse(): Response {
  return flagDisabledResponse({
    error: "Memory slots not enabled",
    flag: "AGENTMEMORY_SLOTS",
    enableHow: "Set AGENTMEMORY_SLOTS=true (in ~/.agentmemory/.env or the shell) and restart.",
    docsHref: "https://github.com/rohitg00/agentmemory#memory-slots",
  });
}

function reflectDisabledResponse(): Response {
  return flagDisabledResponse({
    error: "Slot reflection not enabled",
    flag: "AGENTMEMORY_REFLECT",
    enableHow: "Set AGENTMEMORY_REFLECT=true (in ~/.agentmemory/.env or the shell) and restart. Requires AGENTMEMORY_SLOTS=true.",
    docsHref: "https://github.com/rohitg00/agentmemory#memory-slots",
  });
}

function asNonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function parseOptionalFiniteNumber(value: unknown): number | undefined | null {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseOptionalPositiveInt(value: unknown): number | undefined | null {
  const parsed = parseOptionalFiniteNumber(value);
  if (parsed === undefined || parsed === null) return parsed;
  if (!Number.isInteger(parsed) || parsed < 1) return null;
  return parsed;
}

function buildConfigFlags() {
  return [
    {
      key: "AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED", label: "Background workflow recovery",
      enabled: !isBackgroundRecoveryPaused(), default: true, affects: ["Dashboard"], needsLlm: false,
      description: "Allows queued summaries, skills, graphs and reflection to recover and dispatch.",
      enableHow: "Set AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=0 and restart.",
    },
    {
      key: "SNAPSHOT_ENABLED", label: "Automatic snapshots", enabled: loadSnapshotConfig().enabled,
      default: false, affects: ["Dashboard"], needsLlm: false,
      description: "Periodically saves the durable memory snapshot.", enableHow: "Set SNAPSHOT_ENABLED=true and restart.",
    },
    {
      key: "AGENTMEMORY_REFLECT", label: "Automatic reflection", enabled: isReflectEnabled(),
      default: false, affects: ["Dashboard"], needsLlm: true,
      description: "Reflects into editable memory slots when slots and background recovery are enabled.",
      enableHow: "Set AGENTMEMORY_REFLECT=true and AGENTMEMORY_SLOTS=true and restart.",
    },
    {
      key: "GRAPH_EXTRACTION_ENABLED",
      label: "Knowledge graph extraction",
      enabled: isGraphExtractionEnabled(),
      default: false,
      affects: ["Graph", "Dashboard"],
      needsLlm: true,
      description: "Extracts entities and relations from observations into a knowledge graph.",
      enableHow: "Set GRAPH_EXTRACTION_ENABLED=true and provide an LLM key, then restart.",
      docsHref: "https://github.com/rohitg00/agentmemory#knowledge-graph",
    },
    {
      key: "CONSOLIDATION_ENABLED",
      label: "Memory consolidation",
      enabled: isConsolidationEnabled(),
      default: false,
      affects: ["Dashboard", "Memories", "Crystals"],
      needsLlm: true,
      description: "Periodically summarizes sessions into semantic facts + procedures.",
      enableHow: "Set CONSOLIDATION_ENABLED=true and provide an LLM key, then restart.",
      docsHref: "https://github.com/rohitg00/agentmemory#consolidation",
    },
    {
      key: "AGENTMEMORY_AUTO_COMPRESS",
      label: "LLM-powered observation compression",
      enabled: isAutoCompressEnabled(),
      default: false,
      affects: ["Memories", "Timeline"],
      needsLlm: true,
      description: "Every observation is compressed by the LLM for richer summaries (costs tokens). OFF uses zero-LLM synthetic compression.",
      enableHow: "Set AGENTMEMORY_AUTO_COMPRESS=true and provide an LLM key.",
      docsHref: "https://github.com/rohitg00/agentmemory/issues/138",
    },
    {
      key: "AGENTMEMORY_INJECT_CONTEXT",
      label: "In-conversation context injection",
      enabled: isContextInjectionEnabled(),
      default: false,
      affects: ["Hooks"],
      needsLlm: false,
      description: "Hooks write recalled context into Claude Code's conversation. OFF captures in the background without injecting.",
      enableHow: "Set AGENTMEMORY_INJECT_CONTEXT=true and restart.",
      docsHref: "https://github.com/rohitg00/agentmemory/issues/143",
    },
  ];
}

export function registerApiTriggers(
  sdk: IIIClient,
  kv: StateKV,
  secret?: string,
  metricsStore?: MetricsStore,
  provider?: ResilientProvider | { circuitState?: unknown },
): void {
  sdk.registerFunction(
    "middleware::api-auth",
    async (input: {
      request?: { headers?: Record<string, string | undefined> };
    }) => {
      if (!secret) return { action: "continue" };
      const headers = input?.request?.headers || {};
      const auth = headers["authorization"] || headers["Authorization"];
      if (
        typeof auth !== "string" ||
        !timingSafeCompare(auth, `Bearer ${secret}`)
      ) {
        return {
          action: "respond",
          response: { status_code: 401, body: { error: "unauthorized" } },
        };
      }
      return { action: "continue" };
    },
  );

  // Shared instance metadata for livez and health so the two never
  // drift. streamsPort lets the viewer resolve its stream WebSocket
  // target from the server instead of port arithmetic, which broke
  // whenever the viewer bound a fallback port. Config is boot-static,
  // so read it once instead of rebuilding the merged env per request.
  const bootStreamsPort = loadConfig().streamsPort;
  const instanceInfo = () => ({
    service: "agentmemory",
    viewerPort: getBoundViewerPort(),
    viewerSkipped: getViewerSkipped(),
    streamsPort: bootStreamsPort,
  });

  sdk.registerFunction("api::liveness",
    async (): Promise<Response> => ({
      status_code: 200,
      body: { status: "ok", ...instanceInfo() },
    }),
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::liveness",
    config: { api_path: "/agentmemory/livez", http_method: "GET" },
  });

  sdk.registerFunction("api::config-flags",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const providerKind = detectLlmProviderKind();
      const embeddingProvider = detectEmbeddingProvider() ? "embeddings" : "none";
      const flags = buildConfigFlags();
      return {
        status_code: 200,
        body: {
          version: VERSION,
          provider: providerKind,
          embeddingProvider,
          graphAutomaticAvailability: graphAutomaticAvailability(),
          flags,
        },
      };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::config-flags",
    config: {
      api_path: "/agentmemory/config/flags",
      http_method: "GET",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  const STATUS_CHECK_TIMEOUT_MS = 5000;

  sdk.registerFunction("api::health",
    async (): Promise<Response> => {
      const [health, functionMetrics] = await Promise.all([
        valueWithin(getLatestHealth(kv), STATUS_CHECK_TIMEOUT_MS),
        metricsStore
          ? valueWithin(metricsStore.getAll(), STATUS_CHECK_TIMEOUT_MS)
          : Promise.resolve([]),
      ]);
      const circuitBreaker =
        provider && "circuitState" in provider ? provider.circuitState : null;

      const unavailableComponents = [
        ...(health === null ? ["health"] : []),
        ...(functionMetrics === null ? ["functionMetrics"] : []),
      ];
      if (health === null || functionMetrics === null) {
        return {
          status_code: 503,
          body: {
            status: "unavailable",
            version: VERSION,
            health,
            functionMetrics: functionMetrics ?? [],
            unavailableComponents,
            circuitBreaker,
            ...instanceInfo(),
          },
        };
      }

      const status = health.status;
      const statusCode = status === "critical" ? 503 : 200;

      return {
        status_code: statusCode,
        body: {
          status,
          version: VERSION,
          health,
          functionMetrics,
          circuitBreaker,
          ...instanceInfo(),
        },
      };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::health",
    config: {
      api_path: "/agentmemory/health",
      http_method: "GET",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  async function valueWithin<T>(work: Promise<T>, ms: number): Promise<T | null> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expiry = new Promise<null>((resolve) => {
      timer = setTimeout(() => resolve(null), ms);
    });
    try {
      return await Promise.race([work.catch(() => null), expiry]);
    } finally {
      clearTimeout(timer);
    }
  }

  const pendingStatusReads = new Map<string, Promise<unknown>>();
  function coalescePendingStatusRead<T>(key: string, run: () => Promise<T>): Promise<T> {
    const pending = pendingStatusReads.get(key);
    if (pending) return pending as Promise<T>;
    const work = Promise.resolve().then(run);
    pendingStatusReads.set(key, work);
    const clear = () => {
      if (pendingStatusReads.get(key) === work) pendingStatusReads.delete(key);
    };
    void work.then(clear, clear);
    return work;
  }

  const activity = createDashboardActivity(kv);
  const engineMemory = createEngineMemoryReader({ getEngineUrl: () => loadConfig().engineUrl });
  const operationalMetrics = createDashboardOperationalMetrics(kv, engineMemory);
  const sharedUnindexedScan = singleFlight(() => findUnindexedObservations(kv), UNINDEXED_SCAN_REUSE_MS);

  sdk.registerFunction("api::status",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const checkedAt = new Date();
      const config = loadConfig();
      const isolatedAgentScope = isAgentScopeIsolated();
      const activityAgentId = isolatedAgentScope ? getAgentId() : undefined;
      const statusScopeKey = JSON.stringify({
        engineUrl: config.engineUrl,
        dataDir: config.dataDir,
        isolatedAgentId: isolatedAgentScope ? activityAgentId ?? null : null,
        graphExtractionEnabled: isGraphExtractionEnabled(),
        contextInjectionEnabled: isContextInjectionEnabled(),
      });
      const nativeProvider = kv.indexedRetrieval ? getEmbeddingProvider() : null;
      const idx = kv.indexedRetrieval ? null : getSearchIndex();
      const automaticMaintenancePaused = isBackgroundRecoveryPaused()
        && getEnvVar("AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED") !== "1";
      const functionMetricsWork = metricsStore
        ? valueWithin(coalescePendingStatusRead(
            JSON.stringify(["functions", statusScopeKey]),
            () => metricsStore.getAll(),
          ), STATUS_CHECK_TIMEOUT_MS)
        : Promise.resolve([]);
      const savingsWork = valueWithin(functionMetricsWork.then(metrics => {
        const savingsMetrics = metrics ?? [];
        return coalescePendingStatusRead(
          JSON.stringify(["savings", statusScopeKey, JSON.stringify(savingsMetrics)]),
          () => readSavingsReport(kv, savingsMetrics),
        );
      }), 6000);
      const [health, functionMetrics, graph, native, unindexed, recentActivity, operational, savings] = await Promise.all([
        valueWithin(coalescePendingStatusRead(
          JSON.stringify(["health", statusScopeKey]),
          () => getLatestHealth(kv),
        ), STATUS_CHECK_TIMEOUT_MS),
        functionMetricsWork,
        valueWithin(
          coalescePendingStatusRead(
            JSON.stringify(["graph", statusScopeKey]),
            () => sdk.trigger({ function_id: "mem::graph-stats", payload: {} }) as Promise<GraphStatsInput>,
          ),
          STATUS_CHECK_TIMEOUT_MS,
        ),
        kv.indexedRetrieval ? valueWithin(coalescePendingStatusRead(
          JSON.stringify(["index", statusScopeKey, nativeProvider?.name ?? null, nativeProvider?.dimensions ?? null]),
          () => readNativeIndexStatus(kv, nativeProvider),
        ), STATUS_CHECK_TIMEOUT_MS) : Promise.resolve(null),
        kv.indexedRetrieval ? Promise.resolve(null) : valueWithin(sharedUnindexedScan(), STATUS_CHECK_TIMEOUT_MS),
        valueWithin(coalescePendingStatusRead(
          JSON.stringify(["activity", statusScopeKey, activityAgentId ?? null]),
          () => activity.snapshot(activityAgentId),
        ), STATUS_CHECK_TIMEOUT_MS),
        valueWithin(operationalMetrics.read(
          checkedAt, automaticMaintenancePaused, getAgentId(), activityAgentId ?? null,
        ), STATUS_CHECK_TIMEOUT_MS),
        savingsWork,
      ]);
      const unavailableComponents = [
        ...(health === null ? ["health"] : []),
        ...(functionMetrics === null ? ["functions"] : []),
        ...(graph === null ? ["graph"] : []),
        ...((kv.indexedRetrieval ? native : unindexed) === null ? ["index"] : []),
        ...(recentActivity === null ? ["activity"] : []),
        ...(operational === null ? ["operationalMetrics"] : []),
        ...(savings === null ? ["savings"] : []),
      ];
      const observationsIndexed = idx ? [...idx.observationCountsBySession().values()].reduce((a, n) => a + n, 0) : null;
      const circuit =
        provider && "circuitState" in provider
          ? (provider.circuitState as { state?: string; failures?: number } | null)
          : null;
      if (operational) operational.workflows = buildWorkflowStatus({
        backgroundRecoveryPaused: isBackgroundRecoveryPaused(),
        providerAvailable: Boolean(provider), providerCircuitOpen: circuit?.state === "open",
        nativeAvailable: kv.indexedRetrieval ? native !== null && native !== undefined : false,
        slotsAvailable: isSlotsEnabled(),
        activity: recentActivity, statistics: operational.workflowStatistics,
        configured: {
          graph: isGraphExtractionEnabled(), consolidation: isConsolidationEnabled(), compression: isAutoCompressEnabled(),
          reflection: isReflectEnabled(), snapshots: loadSnapshotConfig().enabled, maintenance: !automaticMaintenancePaused,
        },
      });
      const report = evaluateStatus({
        now: checkedAt,
        version: VERSION,
        engineVersion: III_PINNED_VERSION,
        uptimeSeconds: Math.round(process.uptime()),
        ports: {
          rest: loadConfig().restPort ?? null,
          streams: bootStreamsPort ?? null,
          viewer: getViewerSkipped() ? null : (getBoundViewerPort() ?? null),
        },
        health: health
          ? {
              status: health.status,
              alerts: health.alerts,
              notes: health.notes,
              connectionState: health.connectionState,
            }
          : null,
        circuitBreaker: circuit,
        functionMetrics: functionMetrics ?? [],
        provider: detectLlmProviderKind(),
        embeddingProvider: detectEmbeddingProvider() ? "embeddings" : "none",
        flags: buildConfigFlags(),
        activity: recentActivity,
        operationalMetrics: operational,
        savings,
        index: {
          mode: kv.indexedRetrieval ? "native" : "legacy",
          bm25Documents: kv.indexedRetrieval ? native?.lexicalCount ?? null : idx?.size ?? null,
          vectorDocuments: kv.indexedRetrieval ? native?.vectorCount ?? null : getVectorIndex()?.size ?? null,
          observationsIndexed,
          missingObservations: unindexed ? unindexed.missing.length : null,
          sessions: unindexed ? unindexed.sessions : null,
          bm25Incomplete: kv.indexedRetrieval ? null : isBm25RebuildIncomplete(),
          pendingVectorBackfill: kv.indexedRetrieval ? null : getPendingVectorBackfillCount(),
          ...(kv.indexedRetrieval ? { native } : {}),
        },
        graph,
        graphExtractionEnabled: isGraphExtractionEnabled(),
        indexPersistence: kv.indexedRetrieval ? null : getIndexPersistenceStatus(),
      });
      const accept = req.headers?.["accept"] ?? req.headers?.["Accept"];
      const format = req.query_params?.["format"];
      if (prefersHtml(typeof accept === "string" ? accept : undefined, typeof format === "string" ? format : undefined)) {
        const nonce = randomBytes(16).toString("base64");
        return {
          status_code: 200,
          headers: {
            "Content-Type": "text/html; charset=utf-8",
            "Content-Security-Policy": `default-src 'none'; style-src 'nonce-${nonce}'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
            "Cache-Control": "no-store",
          },
          body: renderStatusHtml(report, nonce),
        };
      }
        return { status_code: 200, headers: { "Cache-Control": "no-store" }, body: { ...report, unavailableComponents } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::status",
    config: { api_path: "/agentmemory/status", http_method: "GET" },
  });

  sdk.registerFunction("api::observe",
    async (req: HttpRequest<Record<string, unknown>>): Promise<Response> => {
      const body = req.body ?? {};
      const hookType = asNonEmptyString(body.hookType);
      const sessionId = asNonEmptyString(body.sessionId);
      const project = asNonEmptyString(body.project);
      const cwd = asNonEmptyString(body.cwd);
      const timestamp = asNonEmptyString(body.timestamp);
      if (!hookType || !sessionId || !project || !cwd || !timestamp) {
        return {
          status_code: 400,
          body: {
            error:
              "hookType, sessionId, project, cwd, and timestamp are required strings",
          },
        };
      }
      const payload: HookPayload = {
        hookType: hookType as HookPayload["hookType"],
        sessionId,
        project,
        cwd,
        timestamp,
        data: body.data,
      };
      const result = await sdk.trigger({ function_id: "mem::observe", payload });
      if (!result || typeof result !== "object") {
        return { status_code: 500, body: { error: "Observation could not be captured" } };
      }
      if ("success" in result && result.success === false) {
        const code = "code" in result ? result.code : undefined;
        return { status_code: code === "SESSION_OBSERVATION_LIMIT" ? 409 : 500, body: result };
      }
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::observe",
    config: {
      api_path: "/agentmemory/observe",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::context",
    async (
      req: HttpRequest<{
        sessionId: string;
        project: string;
        budget?: number;
        agentId?: string;
      }>,
    ): Promise<Response> => {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const sessionId = asNonEmptyString(body.sessionId);
      const project = asNonEmptyString(body.project);
      if (!sessionId || !project) {
        return {
          status_code: 400,
          body: { error: "sessionId and project are required strings" },
        };
      }
      const budget = parseOptionalPositiveInt(body.budget);
      if (budget === null) {
        return {
          status_code: 400,
          body: { error: "budget must be a positive integer" },
        };
      }
      // Propagate agentId so mem::context applies the same cross-agent
      // isolation filter as mem::search. Honors body.agentId, ?agentId=,
      // or the worker's AGENT_ID fallback under AGENTMEMORY_AGENT_SCOPE=isolated.
      const queryAgentId =
        typeof (req as { query_params?: Record<string, string> })
          .query_params?.["agentId"] === "string"
          ? (req as { query_params: Record<string, string> })
              .query_params["agentId"]
          : undefined;
      const bodyAgentId =
        typeof body.agentId === "string" && body.agentId.trim().length > 0
          ? (body.agentId as string).trim()
          : undefined;
      const payload: {
        sessionId: string;
        project: string;
        budget?: number;
        agentId?: string;
      } = {
        sessionId,
        project,
      };
      if (budget !== undefined) payload.budget = budget;
      const agentId = bodyAgentId ?? queryAgentId;
      if (agentId !== undefined) payload.agentId = agentId;
      const result = await sdk.trigger({ function_id: "mem::context", payload });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::context",
    config: {
      api_path: "/agentmemory/context",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::search",
    async (
      req: HttpRequest<{
        query: string;
        limit?: number;
        project?: string;
        cwd?: string;
        format?: string;
        token_budget?: number;
        agentId?: string;
      }>,
    ): Promise<Response> => {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const queryAgentId =
        typeof (req as { query_params?: Record<string, string> })
          .query_params?.["agentId"] === "string"
          ? (req as { query_params: Record<string, string> })
              .query_params["agentId"]
          : undefined;
      if (typeof body.query !== "string" || !body.query.trim()) {
        return { status_code: 400, body: { error: "query is required and must be a non-empty string" } };
      }
      if (
        body.limit !== undefined &&
        (!Number.isInteger(body.limit) || (body.limit as number) < 1)
      ) {
        return { status_code: 400, body: { error: "limit must be a positive integer" } };
      }
      if (body.project !== undefined && typeof body.project !== "string") {
        return { status_code: 400, body: { error: "project must be a string" } };
      }
      if (body.cwd !== undefined && typeof body.cwd !== "string") {
        return { status_code: 400, body: { error: "cwd must be a string" } };
      }
      if (
        body.format !== undefined &&
        (typeof body.format !== "string" ||
          !["full", "compact", "narrative"].includes(body.format.trim().toLowerCase()))
      ) {
        return {
          status_code: 400,
          body: { error: "format must be one of: full, compact, narrative" },
        };
      }
      if (
        body.token_budget !== undefined &&
        (!Number.isInteger(body.token_budget) || (body.token_budget as number) < 1)
      ) {
        return {
          status_code: 400,
          body: { error: "token_budget must be a positive integer" },
        };
      }
      // #817: propagate agentId so the upstream isolation filter
      // applies. Honors body.agentId (POST body), ?agentId=... query
      // param, or implicit fallback to the worker's AGENT_ID when
      // AGENTMEMORY_AGENT_SCOPE=isolated.
      const bodyAgentId =
        typeof body.agentId === "string" && body.agentId.trim().length > 0
          ? (body.agentId as string).trim()
          : undefined;
      const payload = {
        query: body.query.trim(),
        limit: body.limit as number | undefined,
        project: body.project as string | undefined,
        cwd: body.cwd as string | undefined,
        format:
          typeof body.format === "string"
            ? body.format.trim().toLowerCase()
            : undefined,
        token_budget: body.token_budget as number | undefined,
        agentId: bodyAgentId ?? queryAgentId,
      };
      const result = await sdk.trigger({ function_id: "mem::search", payload: payload });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::search",
    config: {
      api_path: "/agentmemory/search",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::compress-file", 
    async (req: HttpRequest<{ filePath: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const body = (req.body ?? {}) as Record<string, unknown>;
      const filePath = asNonEmptyString(body.filePath);
      if (!filePath) {
        return {
          status_code: 400,
          body: { error: "filePath is required and must be a non-empty string" },
        };
      }
      const result = await sdk.trigger({
        function_id: "mem::compress-file",
        payload: { filePath },
      });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::compress-file",
    config: { api_path: "/agentmemory/compress-file", http_method: "POST" },
  });

  sdk.registerFunction("api::replay::load",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const sessionId = asNonEmptyString(req.query_params?.["sessionId"]);
      if (!sessionId) {
        return { status_code: 400, body: { error: "sessionId is required" } };
      }
      const result = await sdk.trigger({
        function_id: "mem::replay::load",
        payload: { sessionId },
      });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::replay::load",
    config: { api_path: "/agentmemory/replay/load", http_method: "GET" },
  });

  sdk.registerFunction("api::replay::sessions",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const sessions = await kv.list<Session>(KV.sessions);
      sessions.sort((a, b) =>
        (b.startedAt || "").localeCompare(a.startedAt || ""),
      );
      return { status_code: 200, body: { success: true, sessions } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::replay::sessions",
    config: { api_path: "/agentmemory/replay/sessions", http_method: "GET" },
  });

  sdk.registerFunction("api::replay::import",
    async (
      req: HttpRequest<{ path?: string; maxFiles?: number }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const body = (req.body ?? {}) as Record<string, unknown>;
      const payload: { path?: string; maxFiles?: number } = {};
      if (body.path !== undefined) {
        if (typeof body.path !== "string" || body.path.trim().length === 0) {
          return {
            status_code: 400,
            body: { error: "path must be a non-empty string" },
          };
        }
        payload.path = body.path.trim();
      }
      if (body.maxFiles !== undefined) {
        const n = body.maxFiles as number;
        if (
          !Number.isInteger(n) ||
          n < 1 ||
          n > MAX_FILES_UPPER_BOUND
        ) {
          return {
            status_code: 400,
            body: {
              error: `maxFiles must be an integer between 1 and ${MAX_FILES_UPPER_BOUND}`,
            },
          };
        }
        payload.maxFiles = n;
      }
      const result = await sdk.trigger({
        function_id: "mem::replay::import-jsonl",
        payload,
      });
      return { status_code: 202, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::replay::import",
    config: { api_path: "/agentmemory/replay/import-jsonl", http_method: "POST" },
  });

  sdk.registerFunction("api::session::start",
    async (
      req: HttpRequest<{ sessionId: string; project: string; cwd: string }>,
    ): Promise<Response> => {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const sessionId = asNonEmptyString(body.sessionId);
      const project = asNonEmptyString(body.project);
      const cwd = asNonEmptyString(body.cwd);
      if (!sessionId || !project || !cwd) {
        return {
          status_code: 400,
          body: {
            error: "sessionId, project, and cwd are required non-empty strings",
          },
        };
      }
      const title = typeof body.title === "string" ? body.title.trim() : undefined;
      // allow session/start to override AGENT_ID from request body
      // (multi-agent runtimes that route many roles through one server
      // process). Falls back to the AGENT_ID env on the server.
      const requestAgentId =
        typeof body.agentId === "string" && body.agentId.trim().length > 0
          ? body.agentId.trim().slice(0, 128)
          : undefined;
      const agentId = requestAgentId ?? getAgentId();
      const session: Session = {
        id: sessionId,
        project,
        cwd,
        startedAt: new Date().toISOString(),
        status: "active",
        observationCount: 0,
        ...(title ? { summary: title.slice(0, 200) } : {}),
        ...(title ? { firstPrompt: title.slice(0, 200) } : {}),
        ...(agentId ? { agentId } : {}),
      };
      await kv.set(KV.sessions, sessionId, session);
      const contextResult = await sdk.trigger<
        { sessionId: string; project: string; agentId?: string },
        { context: string }
      >({
        function_id: "mem::context",
        payload: { sessionId, project, ...(agentId ? { agentId } : {}) },
      });
      return {
        status_code: 200,
        body: { session, context: contextResult.context },
      };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::session::start",
    config: {
      api_path: "/agentmemory/session/start",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::session::end",
    async (req: HttpRequest<{ sessionId: string }>): Promise<Response> => {
      const sessionId = asNonEmptyString((req.body as Record<string, unknown>)?.sessionId);
      if (!sessionId) {
        return {
          status_code: 400,
          body: { error: "sessionId is required and must be a non-empty string" },
        };
      }
      const existingSession = await kv.get<Session>(KV.sessions, sessionId);
      if (!existingSession) return { status_code: 404, body: { success: false, error: "session_not_found" } };
      if (existingSession.id !== sessionId || typeof existingSession.project !== "string" || !existingSession.project.trim()) {
        return { status_code: 409, body: { success: false, error: "session_identity_invalid" } };
      }
      const endedAt = new Date().toISOString();
      try {
        const existingIntent = await kv.get<{
          sessionId?: string;
          createdAt?: string;
          executionOrigin?: unknown;
        }>(KV.summaryQueueIntents, sessionId);
        const preserveSelectedOrigin = existingIntent?.sessionId === sessionId &&
          existingIntent.executionOrigin === "selected";
        await kv.set(KV.summaryQueueIntents, sessionId, {
          sessionId,
          createdAt: preserveSelectedOrigin && typeof existingIntent?.createdAt === "string"
            ? existingIntent.createdAt : endedAt,
          ...(preserveSelectedOrigin && { executionOrigin: "selected" as const }),
        });
      } catch {
        logger.warn("session end summary intent write failed", {
          sessionId,
          reason: "state_write_rejected",
        });
        return { status_code: 503, body: { success: false, error: "summary_enqueue_failed" } };
      }
      await kv.update(KV.sessions, sessionId, [
        { type: "set", path: "endedAt", value: endedAt },
        { type: "set", path: "status", value: "completed" },
      ]);
      // The lifecycle returns once the summary job is durably accepted; the
      // queued LLM work runs independently of this HTTP request.
      try {
        const result: unknown = await sdk.trigger({
          function_id: "event::session::stopped",
          payload: { sessionId },
        });
        if (typeof result !== "object" || result === null ||
            !("success" in result) || result.success !== true) {
          logger.warn("event::session::stopped did not acknowledge summary enqueue", {
            sessionId,
            reason: "reported_failure",
          });
          return { status_code: 503, body: { success: false, error: "summary_enqueue_failed" } };
        }
      } catch {
        logger.warn("event::session::stopped trigger failed", {
          sessionId,
          reason: "trigger_rejected",
        });
        return { status_code: 503, body: { success: false, error: "summary_enqueue_failed" } };
      }
      return { status_code: 200, body: { success: true } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::session::end",
    config: {
      api_path: "/agentmemory/session/end",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::summarize", 
    async (req: HttpRequest<{ sessionId: string; continuation?: unknown; zeroSpendRecoveryOfJobId?: unknown }>): Promise<Response> => {
      const body = req.body as Record<string, unknown> | undefined;
      const sessionId = asNonEmptyString(body?.sessionId);
      if (!sessionId) {
        return { status_code: 400, body: { error: "sessionId is required" } };
      }
      const { continuation, zeroSpendRecoveryOfJobId } = body!;
      if (zeroSpendRecoveryOfJobId !== undefined && (typeof zeroSpendRecoveryOfJobId !== "string" ||
          !zeroSpendRecoveryOfJobId.trim() || zeroSpendRecoveryOfJobId !== zeroSpendRecoveryOfJobId.trim() ||
          zeroSpendRecoveryOfJobId.length > 128 || ["continuation", "selected", "manual"]
            .some(key => Object.hasOwn(body!, key))))
        return { status_code: 400, body: { error: "invalid_manual_summary_request" } };
      if (continuation !== undefined && (!validManualSummaryContinuation(continuation) || continuation.sessionId !== sessionId))
        return { status_code: 400, body: { error: "invalid_manual_summary_request" } };
      const result = await sdk.trigger({
        function_id: "mem::summarize",
        timeoutMs: 360_000,
        payload: { sessionId, manual: true, ...(zeroSpendRecoveryOfJobId !== undefined && { zeroSpendRecoveryOfJobId }), ...(continuation !== undefined && { continuation: {
          operationId: continuation.operationId, step: continuation.step, jobId: continuation.jobId,
          project: continuation.project, sessionId: continuation.sessionId, sourceFingerprint: continuation.sourceFingerprint,
          round: continuation.round, unitId: continuation.unitId, deliveryId: continuation.deliveryId,
          ...(continuation.recoveryGeneration !== undefined && { recoveryGeneration: continuation.recoveryGeneration }),
        } }) },
      });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::summarize",
    config: {
      api_path: "/agentmemory/summarize",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::session::commit",
    async (req: HttpRequest): Promise<Response> => {
      const body = (req.body ?? {}) as Record<string, unknown>;
      const sha = asNonEmptyString(body.sha);
      if (!sha) {
        return {
          status_code: 400,
          body: { error: "sha is required and must be a non-empty string" },
        };
      }
      const sessionId = asNonEmptyString(body.sessionId) ?? undefined;
      const branch = asNonEmptyString(body.branch) ?? undefined;
      const repo = asNonEmptyString(body.repo) ?? undefined;
      const message = asNonEmptyString(body.message) ?? undefined;
      const author = asNonEmptyString(body.author) ?? undefined;
      const authoredAt = asNonEmptyString(body.authoredAt) ?? undefined;
      const files = Array.isArray(body.files)
        ? (body.files as unknown[]).filter(
            (f): f is string => typeof f === "string" && f.length > 0,
          )
        : undefined;

      const link = await withKeyedLock(`commit:${sha}`, async () => {
        const existing = await kv.get<CommitLink>(KV.commits, sha);
        const sessionSet = new Set<string>(existing?.sessionIds ?? []);
        if (sessionId) sessionSet.add(sessionId);
        const merged: CommitLink = {
          sha,
          shortSha: existing?.shortSha ?? sha.slice(0, 7),
          branch: branch ?? existing?.branch,
          repo: repo ?? existing?.repo,
          message: message ?? existing?.message,
          author: author ?? existing?.author,
          authoredAt: authoredAt ?? existing?.authoredAt,
          files: files ?? existing?.files,
          sessionIds: Array.from(sessionSet),
          linkedAt: existing?.linkedAt ?? new Date().toISOString(),
        };
        await kv.set(KV.commits, sha, merged);
        return merged;
      });

      if (sessionId) {
        await withKeyedLock(`session:${sessionId}`, async () => {
          const session = await kv.get<Session>(KV.sessions, sessionId);
          if (!session) return;
          const shaSet = new Set<string>(session.commitShas ?? []);
          shaSet.add(sha);
          session.commitShas = Array.from(shaSet);
          await kv.set(KV.sessions, sessionId, session);
        });
      }

      return { status_code: 200, body: { commit: link } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::session::commit",
    config: {
      api_path: "/agentmemory/session/commit",
      http_method: "POST",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::session::by-commit",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const sha = asNonEmptyString(req.query_params?.["sha"]);
      if (!sha) {
        return {
          status_code: 400,
          body: { error: "sha is required and must be a non-empty string" },
        };
      }
      const link = await kv.get<CommitLink>(KV.commits, sha);
      if (!link) {
        return {
          status_code: 404,
          body: { error: "no sessions linked to this commit" },
        };
      }
      const fetched = await Promise.all(
        (link.sessionIds ?? []).map((sid) => kv.get<Session>(KV.sessions, sid)),
      );
      const sessions = fetched.filter((s): s is Session => s !== null);
      return { status_code: 200, body: { commit: link, sessions } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::session::by-commit",
    config: {
      api_path: "/agentmemory/session/by-commit",
      http_method: "GET",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::commits",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const branch = asNonEmptyString(req.query_params?.["branch"]);
      const repo = asNonEmptyString(req.query_params?.["repo"]);
      const rawLimit = parseOptionalInt(req.query_params?.["limit"]);
      const limit = Math.max(1, Math.min(500, rawLimit ?? 100));
      const all = await kv.list<CommitLink>(KV.commits);
      const filtered = all
        .filter((c) => !branch || c.branch === branch)
        .filter((c) => !repo || c.repo === repo)
        .sort((a, b) => ((a.linkedAt ?? "") < (b.linkedAt ?? "") ? 1 : -1))
        .slice(0, limit);
      return { status_code: 200, body: { commits: filtered } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::commits",
    config: {
      api_path: "/agentmemory/commits",
      http_method: "GET",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::sessions",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try { viewerPageOptions(req.query_params ?? {}); } catch { return { status_code: 400, body: { error: "invalid_viewer_page" } }; }
      const normalizedAgentId =
        typeof req.query_params?.["agentId"] === "string"
          ? req.query_params["agentId"].trim()
          : undefined;
      const wildcardAgent = normalizedAgentId === "*";
      const explicitAgentId =
        normalizedAgentId && !wildcardAgent ? normalizedAgentId : undefined;
      const filterAgentId = wildcardAgent
        ? undefined
        : explicitAgentId ??
          (isAgentScopeIsolated() ? getAgentId() : undefined);
      if (filterAgentId) return { status_code: 400, body: { error: "viewer_agent_filter_requires_native_selector" } };
      try {
        const query = { limit: req.query_params?.["limit"], cursor: req.query_params?.["cursor"], latest: req.query_params?.["latest"] };
        if (query.latest === "true") {
          const { items, ...metadata } = await viewerRecentPage(kv, "sessions", query);
          return { status_code: 200, body: { sessions: items, ...metadata } };
        }
        const page = await viewerPage(kv, KV.sessions, query, VIEWER_SESSION_FIELDS);
        return { status_code: 200, body: { sessions: page.items, partial: page.partial, nextCursor: page.nextCursor,
          total: null, returnedCount: page.items.length, countScope: "all_sessions", countCoverage: "unavailable", countRevision: null, countGeneration: null } };
      } catch (error) {
        if (error instanceof StatePageError && error.code === "STATE_PAGE_CURSOR_STALE" || error instanceof IndexedRetrievalError && error.code === "STATE_RECENT_CURSOR_STALE") {
          return { status_code: 409, body: { error: "viewer_cursor_stale", restartRequired: true } };
        }
        if (error instanceof StatePageError && error.code === "STATE_PAGE_CURSOR_INVALID" || error instanceof IndexedRetrievalError && error.code === "STATE_TX_INVALID_REQUEST") {
          return { status_code: 400, body: { error: "invalid_viewer_cursor" } };
        }
        throw error;
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::sessions",
    config: { api_path: "/agentmemory/sessions", http_method: "GET" },
  });

  sdk.registerFunction("api::observations",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try { viewerPageOptions(req.query_params ?? {}); } catch { return { status_code: 400, body: { error: "invalid_viewer_page" } }; }
      const sessionId = asNonEmptyString(req.query_params?.["sessionId"]);
      if (!sessionId && req.query_params?.["latest"] !== "true")
        return { status_code: 400, body: { error: "sessionId required" } };
      const normalizedAgentId =
        typeof req.query_params?.["agentId"] === "string"
          ? req.query_params["agentId"].trim()
          : undefined;
      const wildcardAgent = normalizedAgentId === "*";
      const explicitAgentId =
        normalizedAgentId && !wildcardAgent ? normalizedAgentId : undefined;
      const filterAgentId = wildcardAgent
        ? undefined
        : explicitAgentId ??
          (isAgentScopeIsolated() ? getAgentId() : undefined);
      if (filterAgentId) return { status_code: 400, body: { error: "viewer_agent_filter_requires_native_selector" } };
      try {
        const query = { limit: req.query_params?.["limit"], cursor: req.query_params?.["cursor"], latest: req.query_params?.["latest"] };
        if (query.latest === "true") {
          const { items, ...metadata } = await viewerRecentPage(kv, "observations", query, sessionId);
          return { status_code: 200, body: { observations: items, ...metadata } };
        }
        const page = await viewerPage(kv, KV.observations(sessionId!), query, VIEWER_OBSERVATION_FIELDS);
        return { status_code: 200, body: { observations: page.items, partial: page.partial, nextCursor: page.nextCursor, total: page.partial ? null : page.items.length } };
      } catch (error) {
        if (error instanceof IndexedRetrievalError && error.code === "STATE_RECENT_CURSOR_STALE")
          return { status_code: 409, body: { error: "viewer_cursor_stale", restartRequired: true } };
        if (error instanceof IndexedRetrievalError && error.code === "STATE_TX_INVALID_REQUEST")
          return { status_code: 400, body: { error: "invalid_viewer_cursor" } };
        throw error;
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::observations",
    config: { api_path: "/agentmemory/observations", http_method: "GET" },
  });

  sdk.registerFunction("api::file-context", 
    async (
      req: HttpRequest<{ sessionId: string; files: string[] }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::file-context", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::file-context",
    config: { api_path: "/agentmemory/file-context", http_method: "POST" },
  });

  sdk.registerFunction("api::enrich",
    async (
      req: HttpRequest<{
        sessionId: string;
        files: string[];
        terms?: string[];
        toolName?: string;
        project?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (
        !req.body?.sessionId ||
        typeof req.body.sessionId !== "string" ||
        !Array.isArray(req.body?.files) ||
        req.body.files.length === 0 ||
        !req.body.files.every((f: unknown) => typeof f === "string")
      ) {
        return {
          status_code: 400,
          body: {
            error: "sessionId (string) and files (string[]) are required",
          },
        };
      }
      if (
        req.body.terms !== undefined &&
        (!Array.isArray(req.body.terms) ||
          !req.body.terms.every((t: unknown) => typeof t === "string"))
      ) {
        return {
          status_code: 400,
          body: { error: "terms must be an array of strings" },
        };
      }
      if (
        req.body.project !== undefined &&
        (typeof req.body.project !== "string" || !req.body.project.trim())
      ) {
        return {
          status_code: 400,
          body: { error: "project must be a non-empty string" },
        };
      }
      const result = await sdk.trigger({
        function_id: "mem::enrich",
        payload: {
          sessionId: req.body.sessionId,
          files: req.body.files,
          ...(req.body.terms !== undefined && { terms: req.body.terms }),
          ...(req.body.toolName !== undefined && { toolName: req.body.toolName }),
          ...(req.body.project !== undefined && { project: req.body.project }),
        },
      });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::enrich",
    config: { api_path: "/agentmemory/enrich", http_method: "POST" },
  });

  sdk.registerFunction("api::remember",
    async (
      req: HttpRequest<{
        content: string;
        type?: string;
        concepts?: string[];
        files?: string[];
        ttlDays?: number;
        sourceObservationIds?: string[];
        project?: string;
        agentId?: string;
        action?: "store" | "update" | "merge";
        targetMemoryId?: string;
        expectedVersion?: number;
        sourceKind?: "unknown" | "human_source" | "ai_output" | "proposal" | "observed_result";
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (
        !req.body?.content ||
        typeof req.body.content !== "string" ||
        !req.body.content.trim()
      ) {
        return { status_code: 400, body: { error: "content is required" } };
      }
      if (
        req.body.project !== undefined &&
        (typeof req.body.project !== "string" || !req.body.project.trim())
      ) {
        return { status_code: 400, body: { error: "project must be a non-empty string" } };
      }
      const result = await sdk.trigger({
        function_id: "mem::remember",
        payload: {
          content: req.body.content,
          ...(req.body.type !== undefined && { type: req.body.type }),
          ...(req.body.concepts !== undefined && { concepts: req.body.concepts }),
          ...(req.body.files !== undefined && { files: req.body.files }),
          ...(req.body.ttlDays !== undefined && { ttlDays: req.body.ttlDays }),
          ...(req.body.sourceObservationIds !== undefined && { sourceObservationIds: req.body.sourceObservationIds }),
          ...(req.body.action !== undefined && { action: req.body.action }),
          ...(req.body.targetMemoryId !== undefined && { targetMemoryId: req.body.targetMemoryId }),
          ...(req.body.expectedVersion !== undefined && { expectedVersion: req.body.expectedVersion }),
          ...(req.body.sourceKind !== undefined && { sourceKind: req.body.sourceKind }),
          ...(req.body.project !== undefined && { project: req.body.project }),
          ...(typeof req.body.agentId === "string" && req.body.agentId.trim()
            ? { agentId: req.body.agentId.trim() }
            : {}),
        },
      });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::remember",
    config: { api_path: "/agentmemory/remember", http_method: "POST" },
  });

  sdk.registerFunction("api::forget", 
    async (
      req: HttpRequest<{
        sessionId?: string;
        observationIds?: string[];
        memoryId?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.sessionId && !req.body?.memoryId) {
        return {
          status_code: 400,
          body: { error: "sessionId or memoryId is required" },
        };
      }
      const result = await sdk.trigger({ function_id: "mem::forget", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::forget",
    config: { api_path: "/agentmemory/forget", http_method: "POST" },
  });

  sdk.registerFunction("api::consolidate", 
    async (
      req: HttpRequest<{ project?: string; minObservations?: number }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::consolidate", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::consolidate",
    config: { api_path: "/agentmemory/consolidate", http_method: "POST" },
  });

  sdk.registerFunction("api::patterns", 
    async (req: HttpRequest<{ project?: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::patterns", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::patterns",
    config: { api_path: "/agentmemory/patterns", http_method: "POST" },
  });

  sdk.registerFunction("api::generate-rules", 
    async (req: HttpRequest<{ project?: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::generate-rules", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::generate-rules",
    config: { api_path: "/agentmemory/generate-rules", http_method: "POST" },
  });

  sdk.registerFunction("api::migrate",
    async (
      req: HttpRequest<{ dbPath?: string; step?: string; dryRun?: boolean }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const hasStep =
        typeof req.body?.step === "string" && req.body.step.trim().length > 0;
      const hasDbPath =
        typeof req.body?.dbPath === "string" && req.body.dbPath.trim().length > 0;
      if (!hasStep && !hasDbPath) {
        return {
          status_code: 400,
          body: { error: "Either step (string) or dbPath (string) is required" },
        };
      }
      const result = await sdk.trigger({
        function_id: "mem::migrate",
        payload: {
          ...(req.body.step !== undefined && { step: req.body.step }),
          ...(req.body.dbPath !== undefined && { dbPath: req.body.dbPath }),
          ...(req.body.dryRun !== undefined && { dryRun: req.body.dryRun }),
        },
      });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::migrate",
    config: { api_path: "/agentmemory/migrate", http_method: "POST" },
  });

  sdk.registerFunction("api::evict", 
    async (req: HttpRequest<{ dryRun?: boolean }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const dryRun =
        req.query_params?.["dryRun"] === "true" || req.body?.dryRun === true;
      const result = await sdk.trigger({ function_id: "mem::evict", payload: { dryRun } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::evict",
    config: { api_path: "/agentmemory/evict", http_method: "POST" },
  });

  sdk.registerFunction("api::smart-search",
    async (
      req: HttpRequest<{
        query?: string;
        expandIds?: Array<string | { obsId: string; sessionId: string }>;
        limit?: number;
        project?: string;
        includeLessons?: boolean;
        agentId?: string;
        sessionId?: string;
        source?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (
        !req.body?.query &&
        (!req.body?.expandIds || req.body.expandIds.length === 0)
      ) {
        return {
          status_code: 400,
          body: { error: "query or expandIds is required" },
        };
      }
      // #771: route the X-Agentmemory-Source header into the payload so
      // the followup-rate diagnostic can skip viewer-originated calls.
      // Body wins if both are set (advanced callers explicitly override).
      const headers = (req.headers || {}) as Record<string, string | string[] | undefined>;
      const sourceHeader = headers["x-agentmemory-source"] ?? headers["X-Agentmemory-Source"];
      const sourceFromHeader = Array.isArray(sourceHeader) ? sourceHeader[0] : sourceHeader;
      // Whitelist payload fields explicitly — REST endpoints never pass
      // the raw request body through to sdk.trigger (AGENTS.md security
      // section). Drops unknown fields so a misbehaving client can't
      // inject downstream-only options.
      const payload = {
        query: req.body?.query,
        expandIds: req.body?.expandIds,
        limit: req.body?.limit,
        project: req.body?.project,
        includeLessons: req.body?.includeLessons,
        agentId: req.body?.agentId,
        sessionId: req.body?.sessionId,
        source: req.body?.source ?? sourceFromHeader,
      };
      const result = await sdk.trigger({ function_id: "mem::smart-search", payload });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::smart-search",
    config: { api_path: "/agentmemory/smart-search", http_method: "POST" },
  });

  // #771: read-back endpoint for the followup-rate diagnostic. Returns
  // a directional signal — overcounts on legitimate query refinement —
  // so help text + the CLI status line carry the same caveat.
  sdk.registerFunction("api::diagnostic-followup",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({
        function_id: "mem::diagnostic::followup-stats",
        payload: {},
      });
      return {
        status_code: 200,
        body: {
          ...(result as Record<string, unknown>),
          caveat:
            "Directional signal: overcounts on legitimate query refinement. " +
            "Tune via AGENTMEMORY_FOLLOWUP_WINDOW_SECONDS.",
        },
      };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::diagnostic-followup",
    config: {
      api_path: "/agentmemory/diagnostics/followup",
      http_method: "GET",
      middleware_function_ids: ["middleware::api-auth"],
    },
  });

  sdk.registerFunction("api::timeline", 
    async (
      req: HttpRequest<{
        anchor: string;
        project?: string;
        before?: number;
        after?: number;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.anchor) {
        return { status_code: 400, body: { error: "anchor is required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::timeline", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::timeline",
    config: { api_path: "/agentmemory/timeline", http_method: "POST" },
  });

  sdk.registerFunction("api::profile", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const project = req.query_params["project"] as string;
      if (!project) {
        return {
          status_code: 400,
          body: { error: "project query param is required" },
        };
      }
      const result = await sdk.trigger({ function_id: "mem::profile", payload: { project } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::profile",
    config: { api_path: "/agentmemory/profile", http_method: "GET" },
  });

  sdk.registerFunction("api::export",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      // Keep legacy session pagination and expose a collection page contract
      // for complete exports whose non-session data exceeds the frame cap.
      const rawMax = req.query_params?.["maxSessions"];
      const rawOffset = req.query_params?.["offset"];
      const rawCollection = req.query_params?.["collection"];
      const rawLimit = req.query_params?.["limit"];
      const rawCursor = req.query_params?.["cursor"];
      const payload: { maxSessions?: number; offset?: number; collection?: ExportCollection; limit?: number; cursor?: string } = {};
      if (rawCollection !== undefined) {
        if (typeof rawCollection !== "string" || !(EXPORT_COLLECTIONS as readonly string[]).includes(rawCollection)) {
          return { status_code: 400, body: { error: "Invalid export collection" } };
        }
        payload.collection = rawCollection as ExportCollection;
      } else if (rawLimit !== undefined) {
        return { status_code: 400, body: { error: "collection is required when limit is provided" } };
      }
      if (typeof rawMax === "string") {
        const n = Number(rawMax);
        if (Number.isInteger(n) && n > 0) payload.maxSessions = n;
      }
      if (typeof rawOffset === "string") {
        const n = Number(rawOffset);
        if (payload.collection !== undefined) {
          if (!Number.isSafeInteger(n) || n < 0) return { status_code: 400, body: { error: "offset must be a non-negative safe integer" } };
          if (n !== 0) return { status_code: 400, body: { error: "Offset continuation is no longer supported; pass the returned cursor" } };
        } else if (Number.isInteger(n) && n >= 0) {
          payload.offset = n;
        }
      } else if (rawOffset !== undefined) {
        return { status_code: 400, body: { error: "offset must be a non-negative integer" } };
      }
      if (rawCursor !== undefined) {
        if (payload.collection === undefined) return { status_code: 400, body: { error: "collection is required when cursor is provided" } };
        if (typeof rawCursor !== "string" || rawCursor.length === 0 || Buffer.byteLength(rawCursor, "utf8") > 32 * 1024) {
          return { status_code: 400, body: { error: "cursor must be a non-empty string no longer than 32768 bytes" } };
        }
        payload.cursor = rawCursor;
      }
      if (rawLimit !== undefined) {
        if (typeof rawLimit !== "string" || !/^\d+$/.test(rawLimit)) {
          return { status_code: 400, body: { error: "limit must be an integer from 1 to 1000" } };
        }
        const n = Number(rawLimit);
        if (!Number.isSafeInteger(n) || n < 1 || n > 1_000) {
          return { status_code: 400, body: { error: "limit must be an integer from 1 to 1000" } };
        }
        payload.limit = n;
      }
      const result = await sdk.trigger({
        function_id: "mem::export",
        payload,
      });
      if (result && typeof result === "object" && (result as { oversized?: unknown }).oversized === true) {
        return { status_code: 413, body: result };
      }
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::export",
    config: { api_path: "/agentmemory/export", http_method: "GET" },
  });

  sdk.registerFunction("api::import", 
    async (
      req: HttpRequest<{
        exportData: unknown;
        strategy?: "merge" | "replace" | "skip";
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.exportData) {
        return { status_code: 400, body: { error: "exportData is required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::import", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::import",
    config: { api_path: "/agentmemory/import", http_method: "POST" },
  });

  sdk.registerFunction("api::relations", 
    async (
      req: HttpRequest<{ sourceId: string; targetId: string; type: string }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.sourceId || !req.body?.targetId || !req.body?.type) {
        return {
          status_code: 400,
          body: { error: "sourceId, targetId, and type are required" },
        };
      }
      const result = await sdk.trigger({ function_id: "mem::relate", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::relations",
    config: { api_path: "/agentmemory/relations", http_method: "POST" },
  });

  sdk.registerFunction("api::evolve", 
    async (
      req: HttpRequest<{
        memoryId: string;
        newContent: string;
        newTitle?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.memoryId || !req.body?.newContent) {
        return {
          status_code: 400,
          body: { error: "memoryId and newContent are required" },
        };
      }
      const result = await sdk.trigger({ function_id: "mem::evolve", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::evolve",
    config: { api_path: "/agentmemory/evolve", http_method: "POST" },
  });

  sdk.registerFunction("api::auto-forget", 
    async (req: HttpRequest<{ dryRun?: boolean }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const dryRun =
        req.query_params?.["dryRun"] === "true" || req.body?.dryRun === true;
      const result = await sdk.trigger({ function_id: "mem::auto-forget", payload: { dryRun } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::auto-forget",
    config: { api_path: "/agentmemory/auto-forget", http_method: "POST" },
  });

  sdk.registerFunction("api::claude-bridge-read", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({ function_id: "mem::claude-bridge-read", payload: {} });
        return { status_code: 200, body: result };
      } catch {
        return {
          status_code: 404,
          body: { error: "Claude bridge not enabled" },
        };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::claude-bridge-read",
    config: { api_path: "/agentmemory/claude-bridge/read", http_method: "GET" },
  });

  sdk.registerFunction("api::claude-bridge-sync", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({ function_id: "mem::claude-bridge-sync", payload: {} });
        return { status_code: 200, body: result };
      } catch {
        return {
          status_code: 404,
          body: { error: "Claude bridge not enabled" },
        };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::claude-bridge-sync",
    config: {
      api_path: "/agentmemory/claude-bridge/sync",
      http_method: "POST",
    },
  });

  sdk.registerFunction("api::graph-query",
    async (
      req: HttpRequest<{
        display?: boolean;
        prepare?: boolean;
        cursor?: string;
        startNodeId?: string;
        project?: string;
        nodeType?: string;
        maxDepth?: number;
        query?: string;
        limit?: number;
        offset?: number;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      // Whitelist payload fields explicitly; AGENTS.md security rule:
      // REST endpoints never pass raw req.body through to sdk.trigger.
      const project = req.body?.project;
      if (project !== undefined && (typeof project !== "string" || !project.trim() || Buffer.byteLength(project) > 256)) {
        return { status_code: 400, body: { error: "Invalid project" } };
      }
      const display = req.body?.display;
      const prepare = req.body?.prepare;
      const cursor = req.body?.cursor;
      if (display !== undefined && typeof display !== "boolean" ||
        prepare !== undefined && (typeof prepare !== "boolean" || display !== true) ||
        prepare === true && (cursor !== undefined || req.body?.limit !== undefined) ||
        cursor !== undefined && (display !== true || typeof cursor !== "string" || !cursor || Buffer.byteLength(cursor) > 4096) ||
        display === true && (!project || [req.body?.startNodeId, req.body?.nodeType, req.body?.query, req.body?.offset, req.body?.maxDepth].some(value => value !== undefined)) ||
        display === true && req.body?.limit !== undefined && (!Number.isInteger(req.body.limit) || req.body.limit < 1 || req.body.limit > 128)) {
        return { status_code: 400, body: { error: "Invalid graph display request" } };
      }
      const payload = {
        display,
        prepare,
        cursor,
        project: project?.trim(),
        startNodeId: req.body?.startNodeId,
        nodeType: req.body?.nodeType,
        maxDepth: req.body?.maxDepth,
        query: req.body?.query,
        limit: req.body?.limit,
        offset: req.body?.offset,
      };
      try {
        const result = await sdk.trigger({ function_id: "mem::graph-query", payload });
        return { status_code: 200, body: result };
      } catch (error) {
        return graphReadFailureResponse("query", error);
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-query",
    config: { api_path: "/agentmemory/graph/query", http_method: "POST" },
  });

  sdk.registerFunction("api::graph-stats", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const project = req.query_params?.project;
      if (project !== undefined && (typeof project !== "string" || !project.trim() || Buffer.byteLength(project) > 256)) {
        return { status_code: 400, body: { error: "Invalid project" } };
      }
      try {
        const result = await sdk.trigger({ function_id: "mem::graph-stats", payload: project ? { project: project.trim() } : {} });
        return { status_code: 200, body: result };
      } catch (error) {
        return graphReadFailureResponse("stats", error);
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-stats",
    config: { api_path: "/agentmemory/graph/stats", http_method: "GET" },
  });

  sdk.registerFunction("api::graph-snapshot-rebuild",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const project = (req.body as { project?: unknown } | undefined)?.project;
      if (project !== undefined && (typeof project !== "string" || !project.trim() || Buffer.byteLength(project) > 256)) {
        return { status_code: 400, body: { error: "Invalid project" } };
      }
      try {
        const result = await sdk.trigger({
          function_id: "mem::graph-snapshot-rebuild",
          payload: project ? { project: project.trim() } : {},
        });
        return { status_code: 200, body: result };
      } catch {
        return graphDisabledResponse();
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-snapshot-rebuild",
    config: { api_path: "/agentmemory/graph/snapshot-rebuild", http_method: "POST" },
  });

  // #814 v2: clean-restart endpoint for legacy corpora too large for
  // safe rebuild. Wipes graph state without touching observations, so
  // recall + history stay intact while the graph rebuilds incrementally
  // from new extracts (or a one-shot /graph/build replay).
  sdk.registerFunction("api::graph-reset",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({
          function_id: "mem::graph-reset",
          payload: {},
        });
        return { status_code: 200, body: result };
      } catch {
        return graphDisabledResponse();
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-reset",
    config: { api_path: "/agentmemory/graph/reset", http_method: "POST" },
  });

  sdk.registerFunction("api::graph-extract",
    async (req: HttpRequest<{ observations: unknown[] }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (
        !Array.isArray(req.body?.observations) ||
        req.body.observations.length === 0
      ) {
        return {
          status_code: 400,
          body: { error: "observations array is required" },
        };
      }
      try {
        const result = await sdk.trigger({ function_id: "mem::graph-extract", payload: { observations: req.body.observations } });
        return { status_code: 200, body: result };
      } catch {
        return graphDisabledResponse();
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-extract",
    config: { api_path: "/agentmemory/graph/extract", http_method: "POST" },
  });

  // Build one bounded graph unit for the project selected in the viewer.
  sdk.registerFunction("api::graph-build",
    async (req: HttpRequest<unknown>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const body = req.body;
      if (!body || typeof body !== "object" || Array.isArray(body)) {
        return { status_code: 400, body: { error: "invalid_graph_build_request" } };
      }
      const input = body as Record<string, unknown>;
      if (Object.keys(input).some(key => key !== "project" && key !== "continuation")
        || typeof input.project !== "string" || !input.project.trim() || input.project.length > 4096
        || input.continuation !== undefined && (typeof input.continuation !== "string" || !input.continuation
          || input.continuation.length > MAX_GRAPH_BUILD_CONTINUATION_BYTES)) {
        return { status_code: 400, body: { error: "invalid_graph_build_request" } };
      }
      try {
        const result = await buildScopedGraph(sdk, kv, input.project, input.continuation as string | undefined);
        return { status_code: graphBuildHttpStatus(result), body: result };
      } catch (error) {
        logger.warn("graph-build request failed", { project: input.project, errorType: error instanceof Error ? error.name : "unknown" });
        return {
          status_code: 502,
          body: {
            status: "failed", project: input.project, sessionsProcessed: 0, observationsProcessed: 0,
            nodesAdded: 0, edgesAdded: 0, continuation: input.continuation ?? null, error: "graph_extraction_failed",
          },
        };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-build",
    config: { api_path: "/agentmemory/graph/build", http_method: "POST" },
  });

  // Import graphify's structural graph (graphify-out/graph.json) into the
  // memory graph. Deterministic, no LLM call; idempotent via the graph
  // name-index upsert.
  sdk.registerFunction("api::graph-import-graphify",
    async (req: HttpRequest<{ path?: string; cwd?: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const { path, cwd } = req.body ?? {};
      if (
        (path !== undefined && typeof path !== "string") ||
        (cwd !== undefined && typeof cwd !== "string")
      ) {
        return {
          status_code: 400,
          body: { error: "path and cwd must be strings when provided" },
        };
      }
      try {
        const result = await sdk.trigger({
          function_id: "mem::graph::import-graphify",
          payload: {
            ...(path !== undefined ? { path } : {}),
            ...(cwd !== undefined ? { cwd } : {}),
          },
        });
        return { status_code: 200, body: result };
      } catch {
        return graphDisabledResponse();
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::graph-import-graphify",
    config: { api_path: "/agentmemory/graph/import-graphify", http_method: "POST" },
  });

  sdk.registerFunction("api::consolidate-pipeline",
    async (req: HttpRequest<{ tier?: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({ function_id: "mem::consolidate-pipeline", payload: req.body || {},
         });
        return { status_code: 200, body: result };
      } catch {
        return consolidationDisabledResponse();
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::consolidate-pipeline",
    config: {
      api_path: "/agentmemory/consolidate-pipeline",
      http_method: "POST",
    },
  });

  sdk.registerFunction("api::team-share", 
    async (
      req: HttpRequest<{ itemId: string; itemType: string; project?: string }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.itemId || !req.body?.itemType) {
        return {
          status_code: 400,
          body: { error: "itemId and itemType are required" },
        };
      }
      try {
        const result = await sdk.trigger({ function_id: "mem::team-share", payload: req.body });
        return { status_code: 201, body: result };
      } catch {
        return { status_code: 404, body: { error: "Team memory not enabled" } };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::team-share",
    config: { api_path: "/agentmemory/team/share", http_method: "POST" },
  });

  sdk.registerFunction("api::team-feed", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const parsedLimit = parseOptionalInt(req.query_params?.["limit"]);
        const limit = parsedLimit ?? 20;
        const result = await sdk.trigger({ function_id: "mem::team-feed", payload: { limit } });
        return { status_code: 200, body: result };
      } catch {
        return { status_code: 404, body: { error: "Team memory not enabled" } };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::team-feed",
    config: { api_path: "/agentmemory/team/feed", http_method: "GET" },
  });

  sdk.registerFunction("api::team-profile", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({ function_id: "mem::team-profile", payload: {} });
        return { status_code: 200, body: result };
      } catch {
        return { status_code: 404, body: { error: "Team memory not enabled" } };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::team-profile",
    config: { api_path: "/agentmemory/team/profile", http_method: "GET" },
  });

  sdk.registerFunction("api::audit",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const parsedLimit = parseOptionalInt(req.query_params?.["limit"]);
      const entries = await sdk.trigger({ function_id: "mem::audit-query", payload: {
        operation: req.query_params?.["operation"],
        limit: parsedLimit ?? 50,
      } });
      return { status_code: 200, body: { entries, success: true } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::audit",
    config: { api_path: "/agentmemory/audit", http_method: "GET" },
  });

  sdk.registerFunction("api::governance-delete", 
    async (
      req: HttpRequest<{ memoryIds: string[]; reason?: string }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.memoryIds || !Array.isArray(req.body.memoryIds)) {
        return {
          status_code: 400,
          body: { error: "memoryIds array is required" },
        };
      }
      const result = await sdk.trigger({ function_id: "mem::governance-delete", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::governance-delete",
    config: {
      api_path: "/agentmemory/governance/memories",
      http_method: "DELETE",
    },
  });

  sdk.registerFunction("api::governance-bulk", 
    async (
      req: HttpRequest<{
        type?: string[];
        dateFrom?: string;
        dateTo?: string;
        qualityBelow?: number;
        dryRun?: boolean;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::governance-bulk", payload: req.body || {} });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::governance-bulk",
    config: {
      api_path: "/agentmemory/governance/bulk-delete",
      http_method: "POST",
    },
  });

  sdk.registerFunction("api::snapshots", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({ function_id: "mem::snapshot-list", payload: {} });
        return { status_code: 200, body: result };
      } catch {
        return { status_code: 404, body: { error: "Snapshots not enabled" } };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::snapshots",
    config: { api_path: "/agentmemory/snapshots", http_method: "GET" },
  });

  sdk.registerFunction("api::snapshot-create", 
    async (req: HttpRequest<{ message?: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger<{ message?: string }, { success: boolean; deferred?: boolean; code?: string }>({
          function_id: "mem::snapshot-create", payload: req.body || {}, timeoutMs: 10 * 60 * 1000,
        });
        const status = result?.success === true ? 201
          : result?.deferred === true || result?.code === "BATCH_MAINTENANCE_BUSY" ? 503 : 500;
        return { status_code: status, body: result };
      } catch {
        return { status_code: 404, body: { error: "Snapshots not enabled" } };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::snapshot-create",
    config: { api_path: "/agentmemory/snapshot/create", http_method: "POST" },
  });

  sdk.registerFunction("api::snapshot-restore", 
    async (req: HttpRequest<{ commitHash: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.commitHash) {
        return { status_code: 400, body: { error: "commitHash is required" } };
      }
      try {
        const result = await sdk.trigger({ function_id: "mem::snapshot-restore", payload: req.body });
        return { status_code: 200, body: result };
      } catch {
        return { status_code: 404, body: { error: "Snapshots not enabled" } };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::snapshot-restore",
    config: { api_path: "/agentmemory/snapshot/restore", http_method: "POST" },
  });

  sdk.registerFunction("api::memories",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try { viewerPageOptions(req.query_params ?? {}); } catch { return { status_code: 400, body: { error: "invalid_viewer_page" } }; }
      // agentId filter. Request param wins, env AGENT_ID (when
      // scope=isolated) is the fallback. Shared mode keeps the tag but
      // does not restrict the list endpoint. Pass agentId=* to opt out
      // of the env scope entirely. includeOrphans=true surfaces
      // pre-AGENT_ID memories whose agentId is undefined.
      const normalizedAgentId =
        typeof req.query_params?.["agentId"] === "string"
          ? req.query_params["agentId"].trim()
          : undefined;
      const wildcardAgent = normalizedAgentId === "*";
      const explicitAgentId =
        normalizedAgentId && !wildcardAgent ? normalizedAgentId : undefined;
      const filterAgentId = wildcardAgent
        ? undefined
        : explicitAgentId ?? (isAgentScopeIsolated() ? getAgentId() : undefined);
      if (filterAgentId) return { status_code: 400,
        body: { error: "viewer_scoped_memory_filter_requires_native_selector" } };
      if (req.query_params?.["project"] && req.query_params?.["latest"] !== "true") return { status_code: 400,
        body: { error: "viewer_project_history_requires_native_selector" } };
      return { status_code: 200, body: await viewerMemories(kv, req.query_params ?? {}) };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::memories",
    config: { api_path: "/agentmemory/memories", http_method: "GET" },
  });

  sdk.registerFunction("api::memory-by-id",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const id = req.path_params?.["id"];
      if (!id || typeof id !== "string") {
        return { status_code: 400, body: { error: "id path parameter is required" } };
      }
      const memory = await kv.get<import("../types.js").Memory>(KV.memories, id);
      if (!memory) {
        return { status_code: 404, body: { error: `memory not found: ${id}` } };
      }
      return { status_code: 200, body: { memory } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::memory-by-id",
    config: { api_path: "/agentmemory/memories/:id", http_method: "GET" },
  });

  sdk.registerFunction("api::semantic-list",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const preview = req.query_params?.preview;
      if (preview !== undefined && preview !== "true" && preview !== "false") return { status_code: 400, body: { error: "invalid preview" } };
      if (preview === "true") {
        const items: unknown[] = [];
        for await (const page of kv.pages(KV.semantic, { limit: 5, maxBytes: 65_536, fields: ["id", "fact", "category", "confidence", "createdAt", "updatedAt"] })) { items.push(...page.items); break; }
        return { status_code: 200, body: { semantic: items, preview: true, partial: true, total: null } };
      }
      const semantic = await kv.list<import("../types.js").SemanticMemory>(KV.semantic);
      return { status_code: 200, body: { semantic } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::semantic-list",
    config: { api_path: "/agentmemory/semantic", http_method: "GET" },
  });

  sdk.registerFunction("api::procedural-list",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const preview = req.query_params?.preview;
      if (preview !== undefined && preview !== "true" && preview !== "false") return { status_code: 400, body: { error: "invalid preview" } };
      if (preview === "true") {
        const items: unknown[] = [];
        for await (const page of kv.pages(KV.procedural, { limit: 5, maxBytes: 65_536, fields: ["id", "name", "title", "steps", "updatedAt"] })) { items.push(...page.items); break; }
        return { status_code: 200, body: { procedural: items, preview: true, partial: true, total: null } };
      }
      const procedural = await kv.list<import("../types.js").ProceduralMemory>(KV.procedural);
      return { status_code: 200, body: { procedural } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::procedural-list",
    config: { api_path: "/agentmemory/procedural", http_method: "GET" },
  });

  const sharedConsolidationCounts = singleFlight(
    () => readConsolidationCounts(kv),
    CONSOLIDATION_COUNTS_REUSE_MS,
  );

  sdk.registerFunction("api::consolidation-status",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const preview = req.query_params?.preview;
      if (preview !== undefined && preview !== "true" && preview !== "false") return { status_code: 400, body: { error: "invalid preview" } };
      if (preview === "true") {
        const lastRun = await kv.get<ConsolidationRunRecord>(KV.config, CONSOLIDATION_LAST_RUN_KEY).catch(() => null);
        return { status_code: 200, body: describeConsolidationPreview({ now: new Date(), enabled: isConsolidationEnabled(),
          llmConfigured: detectLlmProviderKind() === "llm", lastRun }) };
      }
      const [counts, lastRun] = await Promise.all([
        sharedConsolidationCounts(),
        kv.get<ConsolidationRunRecord>(KV.config, CONSOLIDATION_LAST_RUN_KEY).catch(() => null),
      ]);
      return {
        status_code: 200,
        body: describeConsolidation({
          now: new Date(),
          enabled: isConsolidationEnabled(),
          llmConfigured: detectLlmProviderKind() === "llm",
          ...counts,
          lastRun,
        }),
      };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::consolidation-status",
    config: { api_path: "/agentmemory/consolidation/status", http_method: "GET" },
  });

  sdk.registerFunction("api::relations-list",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const preview = req.query_params?.preview;
      if (preview !== undefined && preview !== "true" && preview !== "false") return { status_code: 400, body: { error: "invalid preview" } };
      if (preview === "true") {
        const items: unknown[] = [];
        for await (const page of kv.pages(KV.relations, { limit: 5, maxBytes: 65_536, fields: ["id", "sourceId", "targetId", "type"] })) { items.push(...page.items); break; }
        return { status_code: 200, body: { relations: items, preview: true, partial: true, total: null } };
      }
      const relations = await kv.list<import("../types.js").MemoryRelation>(KV.relations);
      return { status_code: 200, body: { relations } };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::relations-list",
    config: { api_path: "/agentmemory/relations", http_method: "GET" },
  });

  sdk.registerFunction("api::vision-search",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const body = (req.body ?? {}) as Record<string, unknown>;
      const queryText = asNonEmptyString(body["queryText"]);
      const queryImageRef = asNonEmptyString(body["queryImageRef"]);
      const queryImageBase64 = asNonEmptyString(body["queryImageBase64"]);
      const sessionId = asNonEmptyString(body["sessionId"]);
      if (!queryText && !queryImageRef && !queryImageBase64) {
        return {
          status_code: 400,
          body: { error: "queryText, queryImageRef, or queryImageBase64 required" },
        };
      }
      const topKParsed = parseOptionalPositiveInt(body["topK"]);
      if (topKParsed === null) {
        return { status_code: 400, body: { error: "topK must be a positive integer" } };
      }
      const payload: Record<string, unknown> = {};
      if (queryText) payload["queryText"] = queryText;
      if (queryImageRef) payload["queryImageRef"] = queryImageRef;
      if (queryImageBase64) payload["queryImageBase64"] = queryImageBase64;
      if (sessionId) payload["sessionId"] = sessionId;
      if (topKParsed !== undefined) payload["topK"] = Math.min(50, topKParsed);
      const result = await sdk.trigger({ function_id: "mem::vision-search", payload });
      const resp = result as { success?: boolean; error?: string };
      if (resp?.success === false) {
        return { status_code: resp.error?.includes("disabled") ? 503 : 400, body: resp };
      }
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::vision-search",
    config: { api_path: "/agentmemory/vision-search", http_method: "POST" },
  });

  sdk.registerFunction("api::vision-embed",
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const body = (req.body ?? {}) as Record<string, unknown>;
      const imageRef = asNonEmptyString(body["imageRef"]);
      const sessionId = asNonEmptyString(body["sessionId"]);
      const observationId = asNonEmptyString(body["observationId"]);
      if (!imageRef) {
        return { status_code: 400, body: { error: "imageRef is required" } };
      }
      const payload: Record<string, unknown> = { imageRef };
      if (sessionId) payload["sessionId"] = sessionId;
      if (observationId) payload["observationId"] = observationId;
      const result = await sdk.trigger({ function_id: "mem::vision-embed", payload });
      const resp = result as { success?: boolean; error?: string };
      if (resp?.success === false) {
        return { status_code: resp.error?.includes("disabled") ? 503 : 400, body: resp };
      }
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::vision-embed",
    config: { api_path: "/agentmemory/vision-embed", http_method: "POST" },
  });

  sdk.registerFunction("api::slot-list", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    const result = await sdk.trigger({ function_id: "mem::slot-list", payload: {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-list",
    config: { api_path: "/agentmemory/slots", http_method: "GET" },
  });

  sdk.registerFunction("api::slot-get", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    const label = asNonEmptyString(req.query_params?.["label"]);
    if (!label) return { status_code: 400, body: { error: "label query param required" } };
    const result = await sdk.trigger({ function_id: "mem::slot-get", payload: { label } });
    const resp = result as { success?: boolean; error?: string };
    if (resp?.success === false) {
      return { status_code: resp.error?.includes("not found") ? 404 : 400, body: resp };
    }
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-get",
    config: { api_path: "/agentmemory/slot", http_method: "GET" },
  });

  sdk.registerFunction("api::slot-create", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const label = asNonEmptyString(body["label"]);
    if (!label) return { status_code: 400, body: { error: "label required" } };
    // Reject malformed inputs instead of silently dropping them.
    if (body["content"] !== undefined && typeof body["content"] !== "string") {
      return { status_code: 400, body: { error: "content must be a string" } };
    }
    if (body["description"] !== undefined && typeof body["description"] !== "string") {
      return { status_code: 400, body: { error: "description must be a string" } };
    }
    if (body["pinned"] !== undefined && typeof body["pinned"] !== "boolean") {
      return { status_code: 400, body: { error: "pinned must be a boolean" } };
    }
    if (
      body["scope"] !== undefined &&
      body["scope"] !== "project" &&
      body["scope"] !== "global"
    ) {
      return { status_code: 400, body: { error: "scope must be 'project' or 'global'" } };
    }
    const sizeLimit = parseOptionalPositiveInt(body["sizeLimit"]);
    if (sizeLimit === null) {
      return { status_code: 400, body: { error: "sizeLimit must be a positive integer" } };
    }
    if (sizeLimit !== undefined && sizeLimit > 20000) {
      return { status_code: 400, body: { error: "sizeLimit must be <= 20000" } };
    }
    const payload: Record<string, unknown> = { label };
    if (typeof body["content"] === "string") payload["content"] = body["content"];
    if (typeof body["description"] === "string") payload["description"] = body["description"];
    if (sizeLimit !== undefined) payload["sizeLimit"] = sizeLimit;
    if (typeof body["pinned"] === "boolean") payload["pinned"] = body["pinned"];
    if (body["scope"] === "project" || body["scope"] === "global") payload["scope"] = body["scope"];
    const result = await sdk.trigger({ function_id: "mem::slot-create", payload });
    const resp = result as { success?: boolean; error?: string };
    if (resp?.success === false) {
      return { status_code: resp.error?.includes("exists") ? 409 : 400, body: resp };
    }
    return { status_code: 201, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-create",
    config: { api_path: "/agentmemory/slot", http_method: "POST" },
  });

  sdk.registerFunction("api::slot-append", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const label = asNonEmptyString(body["label"]);
    const text = typeof body["text"] === "string" ? body["text"] : null;
    if (!label || !text) return { status_code: 400, body: { error: "label and text required" } };
    const result = await sdk.trigger({ function_id: "mem::slot-append", payload: { label, text } });
    const resp = result as { success?: boolean; error?: string };
    if (resp?.success === false) {
      const notFound = resp.error?.includes("not found");
      const overLimit = resp.error?.includes("exceed");
      return { status_code: notFound ? 404 : overLimit ? 413 : 400, body: resp };
    }
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-append",
    config: { api_path: "/agentmemory/slot/append", http_method: "POST" },
  });

  sdk.registerFunction("api::slot-replace", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const label = asNonEmptyString(body["label"]);
    const content = body["content"];
    if (!label || typeof content !== "string") {
      return { status_code: 400, body: { error: "label and content (string) required" } };
    }
    const result = await sdk.trigger({ function_id: "mem::slot-replace", payload: { label, content } });
    const resp = result as { success?: boolean; error?: string };
    if (resp?.success === false) {
      const notFound = resp.error?.includes("not found");
      const overLimit = resp.error?.includes("exceed");
      return { status_code: notFound ? 404 : overLimit ? 413 : 400, body: resp };
    }
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-replace",
    config: { api_path: "/agentmemory/slot/replace", http_method: "POST" },
  });

  sdk.registerFunction("api::slot-delete", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    const label = asNonEmptyString(req.query_params?.["label"]);
    if (!label) return { status_code: 400, body: { error: "label query param required" } };
    const result = await sdk.trigger({ function_id: "mem::slot-delete", payload: { label } });
    const resp = result as { success?: boolean; error?: string };
    if (resp?.success === false) {
      return { status_code: resp.error?.includes("not found") ? 404 : 400, body: resp };
    }
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-delete",
    config: { api_path: "/agentmemory/slot", http_method: "DELETE" },
  });

  sdk.registerFunction("api::slot-reflect", async (req: HttpRequest): Promise<Response> => {
    const authErr = checkAuth(req, secret);
    if (authErr) return authErr;
    if (!isSlotsEnabled()) return slotsDisabledResponse();
    if (!isReflectEnabled()) return reflectDisabledResponse();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const sessionId = asNonEmptyString(body["sessionId"]);
    if (!sessionId) return { status_code: 400, body: { error: "sessionId required" } };
    const maxObservations = parseOptionalPositiveInt(body["maxObservations"]);
    if (maxObservations === null) return { status_code: 400, body: { error: "maxObservations must be a positive integer" } };
    const payload: Record<string, unknown> = { sessionId };
    if (maxObservations !== undefined) payload["maxObservations"] = maxObservations;
    const result = await sdk.trigger({ function_id: "mem::slot-reflect", payload });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({
    type: "http",
    function_id: "api::slot-reflect",
    config: { api_path: "/agentmemory/slot/reflect", http_method: "POST" },
  });

  sdk.registerFunction("api::action-create",
    async (
      req: HttpRequest<{
        title: string;
        description?: string;
        priority?: number;
        createdBy?: string;
        project?: string;
        tags?: string[];
        parentId?: string;
        edges?: Array<{ type: string; targetActionId: string }>;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.title) {
        return { status_code: 400, body: { error: "title is required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::action-create", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::action-create",
    config: { api_path: "/agentmemory/actions", http_method: "POST" },
  });

  sdk.registerFunction("api::action-update", 
    async (
      req: HttpRequest<{
        actionId: string;
        status?: string;
        title?: string;
        description?: string;
        priority?: number;
        result?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.actionId) {
        return { status_code: 400, body: { error: "actionId is required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::action-update", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::action-update",
    config: { api_path: "/agentmemory/actions/update", http_method: "POST" },
  });

  sdk.registerFunction("api::action-list", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::action-list", payload: {
        status: req.query_params?.["status"],
        project: req.query_params?.["project"],
        parentId: req.query_params?.["parentId"],
      } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::action-list",
    config: { api_path: "/agentmemory/actions", http_method: "GET" },
  });

  sdk.registerFunction("api::action-get", 
    async (req: HttpRequest<{ actionId: string }>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const actionId = req.query_params?.["actionId"] as string;
      if (!actionId) {
        return { status_code: 400, body: { error: "actionId required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::action-get", payload: { actionId } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::action-get",
    config: { api_path: "/agentmemory/actions/get", http_method: "GET" },
  });

  sdk.registerFunction("api::action-edge", 
    async (
      req: HttpRequest<{
        sourceActionId: string;
        targetActionId: string;
        type: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.sourceActionId || !req.body?.targetActionId || !req.body?.type) {
        return { status_code: 400, body: { error: "sourceActionId, targetActionId, and type are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::action-edge-create", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::action-edge",
    config: { api_path: "/agentmemory/actions/edges", http_method: "POST" },
  });

  sdk.registerFunction("api::frontier", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const parsedLimit = parseOptionalInt(req.query_params?.["limit"]);
      const result = await sdk.trigger({ function_id: "mem::frontier", payload: {
        project: req.query_params?.["project"],
        agentId: req.query_params?.["agentId"],
        limit: parsedLimit,
      } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::frontier",
    config: { api_path: "/agentmemory/frontier", http_method: "GET" },
  });

  sdk.registerFunction("api::next", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::next", payload: {
        project: req.query_params?.["project"],
        agentId: req.query_params?.["agentId"],
      } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::next",
    config: { api_path: "/agentmemory/next", http_method: "GET" },
  });

  sdk.registerFunction("api::lease-acquire", 
    async (
      req: HttpRequest<{ actionId: string; agentId: string; ttlMs?: number }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.actionId || !req.body?.agentId) {
        return { status_code: 400, body: { error: "actionId and agentId are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::lease-acquire", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::lease-acquire",
    config: { api_path: "/agentmemory/leases/acquire", http_method: "POST" },
  });

  sdk.registerFunction("api::lease-release", 
    async (
      req: HttpRequest<{ actionId: string; agentId: string; result?: string }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.actionId || !req.body?.agentId) {
        return { status_code: 400, body: { error: "actionId and agentId are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::lease-release", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::lease-release",
    config: { api_path: "/agentmemory/leases/release", http_method: "POST" },
  });

  sdk.registerFunction("api::lease-renew", 
    async (
      req: HttpRequest<{ actionId: string; agentId: string; ttlMs?: number }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.actionId || !req.body?.agentId) {
        return { status_code: 400, body: { error: "actionId and agentId are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::lease-renew", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::lease-renew",
    config: { api_path: "/agentmemory/leases/renew", http_method: "POST" },
  });

  sdk.registerFunction("api::routine-create",
    async (req: HttpRequest<Record<string, unknown>>): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const body = req.body ?? {};
      if (typeof body.name !== "string" || !body.name || !Array.isArray(body.steps)) {
        return {
          status_code: 400,
          body: { error: "name and steps are required" },
        };
      }
      const result = await sdk.trigger({
        function_id: "mem::routine-create",
        payload: {
          name: body.name,
          description: body.description,
          steps: body.steps,
          tags: body.tags,
          frozen: body.frozen,
          sourceProceduralIds: body.sourceProceduralIds,
        },
      });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::routine-create",
    config: { api_path: "/agentmemory/routines", http_method: "POST" },
  });

  sdk.registerFunction("api::routine-list", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::routine-list", payload: {
        frozen: req.query_params?.["frozen"] === "true" ? true : undefined,
      } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::routine-list",
    config: { api_path: "/agentmemory/routines", http_method: "GET" },
  });

  sdk.registerFunction("api::routine-run", 
    async (
      req: HttpRequest<{ routineId: string; project?: string; initiatedBy?: string }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.routineId) {
        return { status_code: 400, body: { error: "routineId is required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::routine-run", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::routine-run",
    config: { api_path: "/agentmemory/routines/run", http_method: "POST" },
  });

  sdk.registerFunction("api::routine-status", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const runId = req.query_params?.["runId"] as string;
      if (!runId) {
        return { status_code: 400, body: { error: "runId query param required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::routine-status", payload: { runId } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::routine-status",
    config: { api_path: "/agentmemory/routines/status", http_method: "GET" },
  });

  sdk.registerFunction("api::signal-send", 
    async (
      req: HttpRequest<{
        from: string;
        to?: string;
        content: string;
        type?: string;
        replyTo?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.from || !req.body?.content) {
        return { status_code: 400, body: { error: "from and content are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::signal-send", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::signal-send",
    config: { api_path: "/agentmemory/signals/send", http_method: "POST" },
  });

  sdk.registerFunction("api::signal-read", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const agentId = req.query_params?.["agentId"] as string;
      if (!agentId) {
        return { status_code: 400, body: { error: "agentId query param required" } };
      }
      const parsedLimit = parseOptionalInt(req.query_params?.["limit"]);
      const result = await sdk.trigger({ function_id: "mem::signal-read", payload: {
        agentId,
        unreadOnly: req.query_params?.["unreadOnly"] === "true",
        threadId: req.query_params?.["threadId"],
        limit: parsedLimit,
      } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::signal-read",
    config: { api_path: "/agentmemory/signals", http_method: "GET" },
  });

  sdk.registerFunction("api::checkpoint-create", 
    async (
      req: HttpRequest<{
        name: string;
        description?: string;
        type?: string;
        linkedActionIds?: string[];
        expiresInMs?: number;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.name) {
        return { status_code: 400, body: { error: "name is required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::checkpoint-create", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::checkpoint-create",
    config: { api_path: "/agentmemory/checkpoints", http_method: "POST" },
  });

  sdk.registerFunction("api::checkpoint-resolve", 
    async (
      req: HttpRequest<{
        checkpointId: string;
        status: string;
        resolvedBy?: string;
        result?: unknown;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.checkpointId || !req.body?.status) {
        return { status_code: 400, body: { error: "checkpointId and status are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::checkpoint-resolve", payload: req.body });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::checkpoint-resolve",
    config: { api_path: "/agentmemory/checkpoints/resolve", http_method: "POST" },
  });

  sdk.registerFunction("api::checkpoint-list", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::checkpoint-list", payload: {
        status: req.query_params?.["status"],
        type: req.query_params?.["type"],
      } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::checkpoint-list",
    config: { api_path: "/agentmemory/checkpoints", http_method: "GET" },
  });

  sdk.registerFunction("api::mesh-register", 
    async (
      req: HttpRequest<{ url: string; name: string; sharedScopes?: string[] }>,
    ): Promise<Response> => {
      const secretErr = requireConfiguredSecret(secret, "mesh");
      if (secretErr) return secretErr;
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      if (!req.body?.url || !req.body?.name) {
        return { status_code: 400, body: { error: "url and name are required" } };
      }
      const result = await sdk.trigger({ function_id: "mem::mesh-register", payload: req.body });
      return { status_code: 201, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::mesh-register",
    config: { api_path: "/agentmemory/mesh/peers", http_method: "POST" },
  });

  sdk.registerFunction("api::mesh-list", 
    async (req: HttpRequest): Promise<Response> => {
      const secretErr = requireConfiguredSecret(secret, "mesh");
      if (secretErr) return secretErr;
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::mesh-list", payload: {} });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::mesh-list",
    config: { api_path: "/agentmemory/mesh/peers", http_method: "GET" },
  });

  sdk.registerFunction("api::mesh-sync", 
    async (
      req: HttpRequest<{ peerId?: string; direction?: string }>,
    ): Promise<Response> => {
      const secretErr = requireConfiguredSecret(secret, "mesh");
      if (secretErr) return secretErr;
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::mesh-sync", payload: req.body || {} });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::mesh-sync",
    config: { api_path: "/agentmemory/mesh/sync", http_method: "POST" },
  });

  sdk.registerFunction("api::mesh-receive", 
    async (req: HttpRequest): Promise<Response> => {
      const secretErr = requireConfiguredSecret(secret, "mesh");
      if (secretErr) return secretErr;
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const result = await sdk.trigger({ function_id: "mem::mesh-receive", payload: req.body || {} });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::mesh-receive",
    config: { api_path: "/agentmemory/mesh/receive", http_method: "POST" },
  });

  sdk.registerFunction("api::mesh-export", 
    async (req: HttpRequest): Promise<Response> => {
      const secretErr = requireConfiguredSecret(secret, "mesh");
      if (secretErr) return secretErr;
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const since = req.query_params?.["since"] as string;
      if (since) {
        const parsed = new Date(since).getTime();
        if (Number.isNaN(parsed)) {
          return { status_code: 400, body: { error: "Invalid 'since' date format" } };
        }
      }
      const project = req.query_params?.["project"] as string | undefined;
      const sinceTime = since ? new Date(since).getTime() : 0;
      const df = <T>(items: T[], field: "updatedAt" | "createdAt") =>
        items.filter((i) => new Date((i as Record<string, unknown>)[field] as string).getTime() > sinceTime);
      let memories = await kv.list<import("../types.js").Memory>(KV.memories);
      let actions = await kv.list<import("../types.js").Action>(KV.actions);
      if (project) {
        memories = memories.filter((m) => m.project === project);
        actions = actions.filter((a) => a.project === project);
      }
      const body: Record<string, unknown> = {
        memories: df(memories, "updatedAt"),
        actions: df(actions, "updatedAt"),
      };
      if (!project) {
        const semantic = await kv.list<import("../types.js").SemanticMemory>(KV.semantic);
        const procedural = await kv.list<import("../types.js").ProceduralMemory>(KV.procedural);
        const relations = await kv.list<import("../types.js").MemoryRelation>(KV.relations);
        const graphNodes = await collectProjectedRecords<GraphNode>(kv, KV.graphNodes);
        const graphEdges = await collectProjectedRecords<GraphEdge>(kv, KV.graphEdges);
        body.semantic = df(semantic, "updatedAt");
        body.procedural = df(procedural, "updatedAt");
        body.relations = df(relations, "createdAt");
        body.graphNodes = graphNodes.filter(
          (n) => new Date(n.updatedAt || n.createdAt).getTime() > sinceTime,
        );
        body.graphEdges = df(graphEdges, "createdAt");
      }
      // Fail an over-frame export with 413 instead of dropping the worker.
      const oversized = checkPayloadFrameSize(
        body,
        "use ?since to fetch only changes after a timestamp, or ?project to scope the export",
      );
      if (oversized) {
        return { status_code: 413, body: oversized };
      }
      return { status_code: 200, body };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::mesh-export",
    config: { api_path: "/agentmemory/mesh/export", http_method: "GET" },
  });

  sdk.registerFunction("api::flow-compress", 
    async (
      req: HttpRequest<{
        runId?: string;
        actionIds?: string[];
        project?: string;
      }>,
    ): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      try {
        const result = await sdk.trigger({ function_id: "mem::flow-compress", payload: req.body || {} });
        return { status_code: 200, body: result };
      } catch {
        return {
          status_code: 404,
          body: { error: "Flow compression requires a provider" },
        };
      }
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::flow-compress",
    config: { api_path: "/agentmemory/flow/compress", http_method: "POST" },
  });

  sdk.registerFunction("api::branch-detect", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const cwd = (req.query_params?.["cwd"] as string) || process.cwd();
      const result = await sdk.trigger({ function_id: "mem::detect-worktree", payload: { cwd } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::branch-detect",
    config: { api_path: "/agentmemory/branch/detect", http_method: "GET" },
  });

  sdk.registerFunction("api::branch-worktrees", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const cwd = (req.query_params?.["cwd"] as string) || process.cwd();
      const result = await sdk.trigger({ function_id: "mem::list-worktrees", payload: { cwd } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::branch-worktrees",
    config: { api_path: "/agentmemory/branch/worktrees", http_method: "GET" },
  });

  sdk.registerFunction("api::branch-sessions", 
    async (req: HttpRequest): Promise<Response> => {
      const authErr = checkAuth(req, secret);
      if (authErr) return authErr;
      const cwd = (req.query_params?.["cwd"] as string) || process.cwd();
      const result = await sdk.trigger({ function_id: "mem::branch-sessions", payload: { cwd } });
      return { status_code: 200, body: result };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::branch-sessions",
    config: { api_path: "/agentmemory/branch/sessions", http_method: "GET" },
  });

  sdk.registerFunction("api::viewer", 
    async (req: HttpRequest): Promise<Response> => {
      const denied = checkAuth(req, secret);
      if (denied) return denied;
      const rendered = renderViewerDocument();
      if (rendered.found) {
        return {
          status_code: 200,
          headers: {
            "Content-Type": "text/html",
            "Content-Security-Policy": rendered.csp,
          },
          body: rendered.html,
        };
      }
      return {
        status_code: 404,
        headers: {
          "Content-Type": "text/html",
        },
        body: "<!DOCTYPE html><html><body><h1>agentmemory</h1><p>viewer not found</p></body></html>",
      };
    },
  );
  sdk.registerTrigger({
    type: "http",
    function_id: "api::viewer",
    config: { api_path: "/agentmemory/viewer", http_method: "GET" },
  });

  sdk.registerFunction("api::sentinel-create",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.name) return { status_code: 400, body: { error: "name is required" } };
    const result = await sdk.trigger({ function_id: "mem::sentinel-create", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sentinel-create", config: { api_path: "/agentmemory/sentinels", http_method: "POST" } });

  sdk.registerFunction("api::sentinel-trigger",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.sentinelId) return { status_code: 400, body: { error: "sentinelId is required" } };
    const result = await sdk.trigger({ function_id: "mem::sentinel-trigger", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sentinel-trigger", config: { api_path: "/agentmemory/sentinels/trigger", http_method: "POST" } });

  sdk.registerFunction("api::sentinel-check",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const result = await sdk.trigger({ function_id: "mem::sentinel-check", payload: {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sentinel-check", config: { api_path: "/agentmemory/sentinels/check", http_method: "POST" } });

  sdk.registerFunction("api::sentinel-cancel",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.sentinelId) return { status_code: 400, body: { error: "sentinelId is required" } };
    const result = await sdk.trigger({ function_id: "mem::sentinel-cancel", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sentinel-cancel", config: { api_path: "/agentmemory/sentinels/cancel", http_method: "POST" } });

  sdk.registerFunction("api::sentinel-list",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    const result = await sdk.trigger({ function_id: "mem::sentinel-list", payload: { status: params.status, type: params.type } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sentinel-list", config: { api_path: "/agentmemory/sentinels", http_method: "GET" } });

  sdk.registerFunction("api::sketch-create",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.title) return { status_code: 400, body: { error: "title is required" } };
    const result = await sdk.trigger({ function_id: "mem::sketch-create", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sketch-create", config: { api_path: "/agentmemory/sketches", http_method: "POST" } });

  sdk.registerFunction("api::sketch-add",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.sketchId || !body?.title) return { status_code: 400, body: { error: "sketchId and title are required" } };
    const result = await sdk.trigger({ function_id: "mem::sketch-add", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sketch-add", config: { api_path: "/agentmemory/sketches/add", http_method: "POST" } });

  sdk.registerFunction("api::sketch-promote",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.sketchId) return { status_code: 400, body: { error: "sketchId is required" } };
    const result = await sdk.trigger({ function_id: "mem::sketch-promote", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sketch-promote", config: { api_path: "/agentmemory/sketches/promote", http_method: "POST" } });

  sdk.registerFunction("api::sketch-discard",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.sketchId) return { status_code: 400, body: { error: "sketchId is required" } };
    const result = await sdk.trigger({ function_id: "mem::sketch-discard", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sketch-discard", config: { api_path: "/agentmemory/sketches/discard", http_method: "POST" } });

  sdk.registerFunction("api::sketch-list",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    const result = await sdk.trigger({ function_id: "mem::sketch-list", payload: { status: params.status, project: params.project } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sketch-list", config: { api_path: "/agentmemory/sketches", http_method: "GET" } });

  sdk.registerFunction("api::sketch-gc",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const result = await sdk.trigger({ function_id: "mem::sketch-gc", payload: {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::sketch-gc", config: { api_path: "/agentmemory/sketches/gc", http_method: "POST" } });

  sdk.registerFunction("api::crystallize",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.actionIds) return { status_code: 400, body: { error: "actionIds is required" } };
    const result = await sdk.trigger({ function_id: "mem::crystallize", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::crystallize", config: { api_path: "/agentmemory/crystals/create", http_method: "POST" } });

  sdk.registerFunction("api::crystal-list",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    const limit = parseOptionalPositiveInt(params.limit);
    if (limit === null || (limit !== undefined && limit > 16)) {
      return {
        status_code: 400,
        body: { error: "invalid numeric parameter: limit" },
      };
    }
    const cursor = params.cursor;
    if (cursor !== undefined && (typeof cursor !== "string" || !cursor || Buffer.byteLength(cursor, "utf8") > 8_192)) {
      return { status_code: 400, body: { error: "invalid cursor" } };
    }
    for (const field of ["project", "sessionId"] as const) {
      const value = params[field];
      if (value !== undefined && (typeof value !== "string" || !value.trim() || Buffer.byteLength(value, "utf8") > 256)) {
        return { status_code: 400, body: { error: `invalid ${field}` } };
      }
    }
    const result = await sdk.trigger({ function_id: "mem::crystal-list", payload: { project: params.project, sessionId: params.sessionId, limit, cursor } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::crystal-list", config: { api_path: "/agentmemory/crystals", http_method: "GET" } });

  sdk.registerFunction("api::auto-crystallize",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    const result = await sdk.trigger({ function_id: "mem::auto-crystallize", payload: body || {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::auto-crystallize", config: { api_path: "/agentmemory/crystals/auto", http_method: "POST" } });

  sdk.registerFunction("api::diagnose",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    const result = await sdk.trigger({ function_id: "mem::diagnose", payload: body || {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::diagnose", config: { api_path: "/agentmemory/diagnostics", http_method: "POST" } });

  sdk.registerFunction("api::heal",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    const result = await sdk.trigger({ function_id: "mem::heal", payload: body || {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::heal", config: { api_path: "/agentmemory/diagnostics/heal", http_method: "POST" } });

  sdk.registerFunction("api::facet-tag",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.targetId || !body?.dimension || !body?.value) return { status_code: 400, body: { error: "targetId, dimension, and value are required" } };
    const result = await sdk.trigger({ function_id: "mem::facet-tag", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::facet-tag", config: { api_path: "/agentmemory/facets", http_method: "POST" } });

  sdk.registerFunction("api::facet-untag",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.targetId || !body?.dimension) return { status_code: 400, body: { error: "targetId and dimension are required" } };
    const result = await sdk.trigger({ function_id: "mem::facet-untag", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::facet-untag", config: { api_path: "/agentmemory/facets/remove", http_method: "POST" } });

  sdk.registerFunction("api::facet-query",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    const result = await sdk.trigger({ function_id: "mem::facet-query", payload: body || {} });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::facet-query", config: { api_path: "/agentmemory/facets/query", http_method: "POST" } });

  sdk.registerFunction("api::facet-get",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    if (!params.targetId) return { status_code: 400, body: { error: "targetId query param is required" } };
    const result = await sdk.trigger({ function_id: "mem::facet-get", payload: { targetId: params.targetId } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::facet-get", config: { api_path: "/agentmemory/facets", http_method: "GET" } });

  sdk.registerFunction("api::facet-stats",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    const result = await sdk.trigger({ function_id: "mem::facet-stats", payload: { targetType: params.targetType } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::facet-stats", config: { api_path: "/agentmemory/facets/stats", http_method: "GET" } });

  sdk.registerFunction("api::verify",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.id || typeof body.id !== "string") return { status_code: 400, body: { error: "id is required" } };
    const result = await sdk.trigger({ function_id: "mem::verify", payload: { id: body.id } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::verify", config: { api_path: "/agentmemory/verify", http_method: "POST" } });

  sdk.registerFunction("api::cascade-update",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.supersededMemoryId || typeof body.supersededMemoryId !== "string") {
      return { status_code: 400, body: { error: "supersededMemoryId is required" } };
    }
    const result = await sdk.trigger({ function_id: "mem::cascade-update", payload: { supersededMemoryId: body.supersededMemoryId } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::cascade-update", config: { api_path: "/agentmemory/cascade-update", http_method: "POST" } });

  sdk.registerFunction("api::lesson-save",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.content || typeof body.content !== "string") return { status_code: 400, body: { error: "content is required" } };
    const tags = typeof body.tags === "string" ? (body.tags as string).split(",").map((t: string) => t.trim()).filter(Boolean) : Array.isArray(body.tags) ? body.tags : [];
    const result = (await sdk.trigger({
      function_id: "mem::lesson-save",
      payload: {
        content: body.content,
        context: body.context || "",
        confidence: typeof body.confidence === "number" ? body.confidence : undefined,
        project: typeof body.project === "string" ? body.project : undefined,
        tags,
        source: "manual",
      },
    })) as { action?: string };
    const statusCode = result?.action === "created" ? 201 : 200;
    return { status_code: statusCode, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::lesson-save", config: { api_path: "/agentmemory/lessons", http_method: "POST" } });

  sdk.registerFunction("api::lesson-list",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    if (params.preview !== undefined && params.preview !== "true" && params.preview !== "false") {
      return { status_code: 400, body: { error: "invalid preview" } };
    }
    if (params.preview === "true") {
      if (Object.keys(params).some(key => key !== "preview")) {
        return { status_code: 400, body: { error: "invalid lesson preview parameter" } };
      }
      const lessons: Array<{ id: string; content?: string; confidence?: number; project?: string;
        source?: string; createdAt?: string; updatedAt?: string; deleted?: boolean }> = [];
      for await (const page of kv.pages<(typeof lessons)[number]>(KV.lessons, {
        limit: 5, maxBytes: 65_536,
        fields: ["id", "content", "confidence", "project", "source", "createdAt", "updatedAt", "deleted"],
      })) {
        lessons.push(...page.items.filter(lesson => lesson.deleted !== true));
        break;
      }
      return { status_code: 200, body: { success: true, lessons, returnedCount: lessons.length,
        preview: true, partial: true, total: null } };
    }
    const minConfidence = parseOptionalFiniteNumber(params.minConfidence);
    if (minConfidence === null) {
      return {
        status_code: 400,
        body: { error: "invalid numeric parameter: minConfidence" },
      };
    }
    const limit = parseOptionalPositiveInt(params.limit);
    if (limit === null) {
      return {
        status_code: 400,
        body: { error: "invalid numeric parameter: limit" },
      };
    }
    const result = await sdk.trigger({ function_id: "mem::lesson-list", payload: {
      project: params.project,
      source: params.source,
      minConfidence,
      limit,
    } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::lesson-list", config: { api_path: "/agentmemory/lessons", http_method: "GET" } });

  sdk.registerFunction("api::lesson-search",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.query || typeof body.query !== "string") return { status_code: 400, body: { error: "query is required" } };
    const result = await sdk.trigger({ function_id: "mem::lesson-recall", payload: body });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::lesson-search", config: { api_path: "/agentmemory/lessons/search", http_method: "POST" } });

  sdk.registerFunction("api::lesson-strengthen",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.lessonId || typeof body.lessonId !== "string") return { status_code: 400, body: { error: "lessonId is required" } };
    const result = await sdk.trigger({ function_id: "mem::lesson-strengthen", payload: { lessonId: body.lessonId } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::lesson-strengthen", config: { api_path: "/agentmemory/lessons/strengthen", http_method: "POST" } });

  sdk.registerFunction("api::lesson-delete",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    const lessonId = typeof body?.lessonId === "string" ? body.lessonId.trim() : "";
    if (!lessonId) return { status_code: 400, body: { error: "lessonId is required" } };
    const result = await sdk.trigger({ function_id: "mem::lesson-delete", payload: { lessonId } });
    const resp = result as { success?: boolean; error?: string };
    if (resp?.success === false && resp.error === "lesson not found") {
      return { status_code: 404, body: { error: "lesson not found" } };
    }
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::lesson-delete", config: { api_path: "/agentmemory/lessons/delete", http_method: "POST" } });

  sdk.registerFunction("api::obsidian-export", async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = (req.body as Record<string, unknown>) || {};
    const vaultDir = asNonEmptyString(body.vaultDir);
    if (!vaultDir) {
      return {
        status_code: 400,
        body: { error: "vaultDir must be a non-empty string" },
      };
    }
    const types = typeof body.types === "string" ? body.types.split(",").map((t: string) => t.trim()).filter(Boolean) : undefined;
    const result = await sdk.trigger({ function_id: "mem::obsidian-export", payload: { vaultDir, types } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::obsidian-export", config: { api_path: "/agentmemory/obsidian/export", http_method: "POST" } });

  sdk.registerFunction("api::reflect",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = (req.body as Record<string, unknown>) || {};
    if (body.requestId !== undefined && typeof body.requestId !== "string" || body.continuation !== undefined && !validManualReflectContinuation(body.continuation)) {
      return { status_code: 400, body: { error: "invalid_manual_reflect_request" } };
    }
    const result = await sdk.trigger({ function_id: "mem::reflect", timeoutMs: 180_000, payload: {
      project: typeof body.project === "string" ? body.project : undefined,
      maxClusters: typeof body.maxClusters === "number" ? body.maxClusters : undefined,
      manual: true, requestId: body.requestId ?? randomBytes(16).toString("hex"),
      ...(body.continuation !== undefined && { continuation: { jobId: body.continuation.jobId,
        requestId: body.continuation.requestId, project: body.continuation.project,
        checkpoint: body.continuation.checkpoint, cut: body.continuation.cut } }),
    } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::reflect", config: { api_path: "/agentmemory/reflect", http_method: "POST" } });

  sdk.registerFunction("api::insight-list",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const params = req.query_params || {};
    const minConfidence = parseOptionalFiniteNumber(params.minConfidence);
    if (minConfidence === null) {
      return {
        status_code: 400,
        body: { error: "invalid numeric parameter: minConfidence" },
      };
    }
    const limit = parseOptionalPositiveInt(params.limit);
    if (limit === null) {
      return {
        status_code: 400,
        body: { error: "invalid numeric parameter: limit" },
      };
    }
    const result = await sdk.trigger({ function_id: "mem::insight-list", payload: {
      project: params.project,
      minConfidence,
      limit,
    } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::insight-list", config: { api_path: "/agentmemory/insights", http_method: "GET" } });

  sdk.registerFunction("api::insight-search",  async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body as Record<string, unknown>;
    if (!body?.query || typeof body.query !== "string") return { status_code: 400, body: { error: "query is required" } };
    const result = await sdk.trigger({ function_id: "mem::insight-search", payload: {
      query: body.query,
      project: typeof body.project === "string" ? body.project : undefined,
      minConfidence: typeof body.minConfidence === "number" ? body.minConfidence : undefined,
      limit: typeof body.limit === "number" ? body.limit : undefined,
    } });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::insight-search", config: { api_path: "/agentmemory/insights/search", http_method: "POST" } });

  sdk.registerFunction("api::storage-maintenance", async (req: HttpRequest) => {
    const denied = checkAuth(req, secret);
    if (denied) return denied;
    const body = req.body && typeof req.body === "object" && !Array.isArray(req.body)
      ? req.body as Record<string, unknown> : {};
    const action = typeof body.action === "string" ? body.action : "status";
    if (!["status", "diagnostics", "activate", "run", "reclaim", "migrate_incremental", "graph_metadata_prepare", "collect_snapshot_temporary"].includes(action)) {
      return { status_code: 400, body: { error: "action must be status, diagnostics, activate, run, reclaim, migrate_incremental, graph_metadata_prepare, or collect_snapshot_temporary" } };
    }
    if (action === "collect_snapshot_temporary") {
      if (typeof body.closed_owner_id !== "string" || !/^[0-9a-f-]{36}$/.test(body.closed_owner_id)) {
        return { status_code: 400, body: { error: "collect_snapshot_temporary requires closed_owner_id" } };
      }
      const result = await sdk.trigger({ function_id: "mem::storage-maintenance", payload: { action, closed_owner_id: body.closed_owner_id } });
      return { status_code: 200, body: result };
    }
    if (action === "graph_metadata_prepare") {
      const limit = body.limit === undefined ? 256 : body.limit;
      const maxBytes = body.max_bytes === undefined ? 262_144 : body.max_bytes;
      const maxMs = body.max_ms === undefined ? 3000 : body.max_ms;
      if (typeof limit !== "number" || !Number.isInteger(limit) || limit < 1 || limit > 1024
        || typeof maxBytes !== "number" || !Number.isInteger(maxBytes) || maxBytes < 8192 || maxBytes > 1_048_576
        || typeof maxMs !== "number" || !Number.isInteger(maxMs) || maxMs < 1 || maxMs > 3000) {
        return { status_code: 400, body: { error: "graph_metadata_prepare requires limit 1-1024, max_bytes 8192-1048576, and max_ms 1-3000" } };
      }
      const result = await kv.prepareGraphStorageMetadata({ limit, max_bytes: maxBytes, max_ms: maxMs });
      return { status_code: 200, body: result };
    }
    if (action === "migrate_incremental") {
      if (body.allow_blocking !== true || typeof body.max_ms !== "number"
        || !Number.isInteger(body.max_ms) || body.max_ms < 1 || body.max_ms > 86_400_000) {
        return { status_code: 400, body: { error: "migrate_incremental requires allow_blocking=true and max_ms from 1 through 86400000" } };
      }
      const result = await sdk.trigger({
        function_id: "mem::storage-maintenance",
        payload: { action, allow_blocking: true, max_ms: body.max_ms },
      });
      return { status_code: 200, body: result };
    }
    const result = await sdk.trigger({
      function_id: "mem::storage-maintenance",
      payload: { action: action === "run" ? "tick" : action },
    });
    return { status_code: 200, body: result };
  });
  sdk.registerTrigger({ type: "http", function_id: "api::storage-maintenance", config: { api_path: "/agentmemory/storage/maintenance", http_method: "POST" } });
}
