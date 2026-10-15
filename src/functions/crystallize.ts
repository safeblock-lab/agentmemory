import { Buffer } from "node:buffer";
import type { IIIClient } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { KV, fingerprintId, generateId } from "../state/schema.js";
import type { Action, ActionEdge, Crystal, CrystalListResponse, MemoryProvider } from "../types.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { FireworksBatchQueue } from "./fireworks-batch.js";
import { batchEffectKey, runBatchCallback, withBatchRecordLocks } from "../state/batch-effects.js";

interface CrystalDigest {
  narrative: string;
  keyOutcomes: string[];
  filesAffected: string[];
  lessons: string[];
}

const CRYSTAL_PAGE_LIMIT = 16;
const CRYSTAL_PAGE_MAX_BYTES = 65_536;
const CRYSTAL_CURSOR_MAX_BYTES = 8_192;

function crystalListFailure(reason: string): CrystalListResponse {
  return {
    success: false,
    crystals: [],
    partial: false,
    nextCursor: null,
    total: null,
    countScope: "page",
    reason,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isCrystalRecord(value: unknown): value is Crystal {
  if (!isRecord(value)) return false;
  return typeof value.id === "string" && value.id.length > 0 &&
    Buffer.byteLength(value.id, "utf8") <= 1_024 && typeof value.narrative === "string" &&
    Array.isArray(value.keyOutcomes) && value.keyOutcomes.every((item) => typeof item === "string") &&
    Array.isArray(value.filesAffected) && value.filesAffected.every((item) => typeof item === "string") &&
    Array.isArray(value.lessons) && value.lessons.every((item) => typeof item === "string") &&
    Array.isArray(value.sourceActionIds) && value.sourceActionIds.every((item) => typeof item === "string") &&
    isRfc3339Timestamp(value.createdAt) &&
    isCrystalScope(value.project) && isCrystalScope(value.sessionId);
}

function isCrystalScope(value: unknown): value is string | undefined {
  return value === undefined || (typeof value === "string" && value.length > 0 &&
    Buffer.byteLength(value, "utf8") <= 256);
}

function isRfc3339Timestamp(value: unknown): value is string {
  if (typeof value !== "string") return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
  if (!match || match[0] !== value) return false;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[8] === undefined ? 0 : Number(match[8]);
  const offsetMinute = match[9] === undefined ? 0 : Number(match[9]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  return month >= 1 && month <= 12 && day >= 1 && day <= daysInMonth[month - 1]! &&
    hour <= 23 && minute <= 59 && second <= 60 && offsetHour <= 23 && offsetMinute <= 59;
}

function isCrystalListResponse(value: unknown, limit: number): value is CrystalListResponse {
  if (!isRecord(value) || typeof value.success !== "boolean" || !Array.isArray(value.crystals) ||
    value.crystals.length > limit || !value.crystals.every(isCrystalRecord) ||
    typeof value.partial !== "boolean" || value.total !== null || value.countScope !== "page" ||
    (value.reason !== undefined && (typeof value.reason !== "string" || !value.reason))) {
    return false;
  }
  const validCursor = value.nextCursor === null ||
    (typeof value.nextCursor === "string" && value.nextCursor.length > 0 &&
      Buffer.byteLength(value.nextCursor, "utf8") <= CRYSTAL_CURSOR_MAX_BYTES);
  if (!validCursor) return false;
  if (!value.success) {
    return typeof value.reason === "string" && value.crystals.length === 0 && value.nextCursor === null;
  }
  if (typeof value.reason === "string" && !value.partial) return false;
  return true;
}

function crystalListResponseBytes(response: CrystalListResponse): number | null {
  try {
    const serialized = JSON.stringify(response);
    return serialized === undefined ? null : Buffer.byteLength(serialized, "utf8");
  } catch {
    return null;
  }
}

const CRYSTALLIZE_SYSTEM = `You are summarizing a completed chain of agent actions into a compact digest.
Extract: (1) what was accomplished in 1-2 sentences, (2) key decisions as bullet points,
(3) files affected, (4) any lessons or patterns worth remembering.
Return as JSON: { "narrative": "...", "keyOutcomes": ["..."], "filesAffected": ["..."], "lessons": ["..."] }`;

function crystallizationSourceFingerprint(
  actions: Action[],
  edges: ActionEdge[],
): string {
  return fingerprintId("fwbcrys", JSON.stringify({
    actions: actions.map((action) => [
      action.id,
      action.title,
      action.description,
      action.status,
      action.updatedAt,
      action.crystallizedInto,
    ]).sort(),
    edges: edges.map((edge) => [
      edge.id,
      edge.sourceActionId,
      edge.targetActionId,
      edge.type,
      edge.createdAt,
    ]).sort(),
  }));
}

export function registerCrystallizeFunction(
  sdk: IIIClient,
  kv: StateKV,
  provider: MemoryProvider,
  llmRouter?: LlmTaskRouter,
  batchQueue?: FireworksBatchQueue,
): void {
  sdk.registerFunction("mem::crystallize",
    async (data: {
      actionIds: string[];
      sessionId?: string;
      project?: string;
      deferred?: boolean;
      batchResponse?: string;
      batchSourceFingerprint?: string;
      batchEffectKey?: string;
    }) => runBatchCallback(kv, "crystallize", data.batchEffectKey, async (resuming, admit) => {
      if (!data.actionIds || data.actionIds.length === 0) {
        return { success: false, error: "actionIds is required" };
      }

      const actions: Action[] = [];
      for (const id of data.actionIds) {
        const action = await kv.get<Action>(KV.actions, id);
        if (!action) {
          return { success: false, error: `action not found: ${id}` };
        }
        if (action.status !== "done" && action.status !== "cancelled") {
          return {
            success: false,
            error: `action ${id} has status "${action.status}", expected "done" or "cancelled"`,
          };
        }
        actions.push(action);
      }

      const allEdges = await kv.list<ActionEdge>(KV.actionEdges);
      const idSet = new Set(data.actionIds);
      const relevantEdges = allEdges.filter(
        (e) => idSet.has(e.sourceActionId) || idSet.has(e.targetActionId),
      );

      const prompt = buildChainText(actions, relevantEdges);
      const sourceFingerprint = crystallizationSourceFingerprint(actions, relevantEdges);

      if (data.deferred && !data.batchResponse && batchQueue) {
        const enqueueResult = await batchQueue.enqueue({
          correlationId: data.batchEffectKey ? fingerprintId("fwbcrys", data.batchEffectKey) : generateId("fwbcrys"),
          task: "crystallization",
          systemPrompt: CRYSTALLIZE_SYSTEM,
          userPrompt: prompt,
          metadata: {
            actionIds: JSON.stringify(data.actionIds),
            sessionId: data.sessionId || "",
            project: data.project || "",
            sourceFingerprint,
            ...(data.batchEffectKey ? { batchEffectKey: data.batchEffectKey } : {}),
          },
        });
        if (enqueueResult.queued) {
          return { success: true, queued: true, workItemId: enqueueResult.workItemId };
        }
      }

      if (!resuming && data.batchResponse && data.batchSourceFingerprint !== sourceFingerprint) {
        return { success: true, stale: true };
      }

      try {
        const response = data.batchResponse ?? (llmRouter
          ? await llmRouter.run(
            "reflection",
            (selectedProvider) => selectedProvider.summarize(CRYSTALLIZE_SYSTEM, prompt),
            (candidate) => {
              const json = candidate.match(/\{[\s\S]*\}/)?.[0];
              if (!json) return false;
              try {
                const digest = JSON.parse(json) as Record<string, unknown>;
                return typeof digest.narrative === "string" && digest.narrative.trim().length > 0 &&
                  ["keyOutcomes", "filesAffected", "lessons"].every((key) =>
                    Array.isArray(digest[key]) && digest[key].every((value) => typeof value === "string"),
                  );
              } catch {
                return false;
              }
            },
          )
          : await provider.summarize(CRYSTALLIZE_SYSTEM, prompt));
        const digest = parseDigest(response);
        await admit();

        const crystalId = data.batchEffectKey ? fingerprintId("crys", data.batchEffectKey) : generateId("crys");
        const crystal = await withBatchRecordLocks([[KV.crystals, crystalId]], async () => {
          const storedCrystal = data.batchEffectKey ? await kv.get<Crystal>(KV.crystals, crystalId) : null;
          const crystal: Crystal = storedCrystal ?? {
            id: crystalId,
            narrative: digest.narrative,
            keyOutcomes: digest.keyOutcomes,
            filesAffected: digest.filesAffected,
            lessons: digest.lessons,
            sourceActionIds: data.actionIds,
            sessionId: data.sessionId,
            project: data.project,
            createdAt: new Date().toISOString(),
          };

          if (!storedCrystal) await kv.set(KV.crystals, crystal.id, crystal);
          return crystal;
        });

        await Promise.all(
          digest.lessons.map((lesson) =>
            sdk
              .trigger({
                function_id: "mem::lesson-save",
                payload: {
                  content: lesson,
                  context: crystal.narrative,
                  confidence: 0.6,
                  project: data.project,
                  tags: [],
                  source: "crystal",
                  sourceIds: [crystal.id],
                  ...(data.batchEffectKey ? { batchEffectKey: batchEffectKey(`${data.batchEffectKey}:${lesson.trim().toLowerCase()}`) } : {}),
                },
              })
              .then((result) => {
                if (data.batchEffectKey && result && typeof result === "object" && "success" in result && result.success === false) {
                  throw new Error("Batch crystal lesson application failed");
                }
              })
              .catch((error) => { if (data.batchEffectKey) throw error; }),
          ),
        );

        for (const action of actions) {
          const updated = { ...action, crystallizedInto: crystal.id };
          await kv.set(KV.actions, action.id, updated);
        }

        return { success: true, crystal };
      } catch (err) {
        return {
          success: false,
          error: `crystallization failed: ${String(err)}`,
        };
      }
    }),
  );

  sdk.registerFunction("mem::crystal-list",
    async (data: {
      project?: string;
      sessionId?: string;
      limit?: number;
      cursor?: string;
    }) => {
      if (!data || typeof data !== "object" || Array.isArray(data)) {
        return crystalListFailure("invalid_request");
      }
      if ((data.project !== undefined && (typeof data.project !== "string" || !data.project ||
          Buffer.byteLength(data.project, "utf8") > 256)) ||
        (data.sessionId !== undefined && (typeof data.sessionId !== "string" || !data.sessionId ||
          Buffer.byteLength(data.sessionId, "utf8") > 256)) ||
        (data.limit !== undefined && (!Number.isSafeInteger(data.limit) || data.limit < 1 ||
          data.limit > CRYSTAL_PAGE_LIMIT))) {
        return crystalListFailure("invalid_request");
      }
      if (data.cursor !== undefined && (typeof data.cursor !== "string" || !data.cursor ||
          Buffer.byteLength(data.cursor, "utf8") > CRYSTAL_CURSOR_MAX_BYTES)) {
        return crystalListFailure("invalid_cursor");
      }

      const limit = data.limit ?? CRYSTAL_PAGE_LIMIT;
      try {
        const response = await kv.retrieval<unknown>({
          action: "crystals_page",
          ...(data.project === undefined ? {} : { project: data.project }),
          ...(data.sessionId === undefined ? {} : { session_id: data.sessionId }),
          ...(data.cursor === undefined ? {} : { cursor: data.cursor }),
          limit,
          max_bytes: CRYSTAL_PAGE_MAX_BYTES,
        });
        if (!isCrystalListResponse(response, limit)) {
          return crystalListFailure("source_response_invalid");
        }
        const responseBytes = crystalListResponseBytes(response);
        if (responseBytes === null || responseBytes > CRYSTAL_PAGE_MAX_BYTES) {
          return crystalListFailure("page_budget_exceeded");
        }
        return response;
      } catch {
        return crystalListFailure("source_unavailable");
      }
    },
  );

  sdk.registerFunction("mem::crystal-get",
    async (data: { crystalId: string }) => {
      if (!data.crystalId) {
        return { success: false, error: "crystalId is required" };
      }

      const crystal = await kv.get<Crystal>(KV.crystals, data.crystalId);
      if (!crystal) {
        return { success: false, error: "crystal not found" };
      }

      return { success: true, crystal };
    },
  );

  sdk.registerFunction("mem::auto-crystallize",
    async (data: {
      olderThanDays?: number;
      project?: string;
      dryRun?: boolean;
      deferred?: boolean;
    }) => {
      const olderThanDays = data.olderThanDays ?? 7;
      const dryRun = data.dryRun ?? false;
      const cutoff = Date.now() - olderThanDays * 24 * 60 * 60 * 1000;

      let allActions = await kv.list<Action>(KV.actions);

      allActions = allActions.filter(
        (a) =>
          a.status === "done" &&
          !a.crystallizedInto &&
          new Date(a.createdAt).getTime() < cutoff,
      );

      if (data.project) {
        allActions = allActions.filter((a) => a.project === data.project);
      }

      if (allActions.length === 0) {
        return { success: true, groupCount: 0, crystalIds: [] };
      }

      const groups = new Map<string, Action[]>();
      for (const action of allActions) {
        const key = action.parentId ?? action.project ?? "_ungrouped";
        const group = groups.get(key);
        if (group) {
          group.push(action);
        } else {
          groups.set(key, [action]);
        }
      }

      if (dryRun) {
        const groupSummaries = Array.from(groups.entries()).map(
          ([key, actions]) => ({
            groupKey: key,
            actionCount: actions.length,
            actionIds: actions.map((a) => a.id),
          }),
        );
        return {
          success: true,
          dryRun: true,
          groupCount: groups.size,
          groups: groupSummaries,
          crystalIds: [],
        };
      }

      const crystalIds: string[] = [];
      for (const [, groupActions] of groups) {
        const actionIds = groupActions.map((a) => a.id);
        const project = groupActions[0].project;

        try {
          const result = (await sdk.trigger({
            function_id: "mem::crystallize", payload: {
              actionIds,
              project,
              deferred: data.deferred,
            }
          })) as { success: boolean; crystal?: Crystal };

          if (result.success && result.crystal) {
            crystalIds.push(result.crystal.id);
          }
        } catch {
          continue;
        }
      }

      return {
        success: true,
        groupCount: groups.size,
        crystalIds,
      };
    },
  );
}

function buildChainText(actions: Action[], edges: ActionEdge[]): string {
  const lines: string[] = ["## Completed Action Chain\n"];

  const sorted = [...actions].sort(
    (a, b) =>
      new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
  );

  for (const action of sorted) {
    lines.push(`### ${action.title}`);
    if (action.description) lines.push(action.description);
    if (action.result) lines.push(`Result: ${action.result}`);
    lines.push(
      `Tags: ${(action.tags ?? []).join(", ")}`,
    );
    lines.push("");
  }

  if (edges.length > 0) {
    lines.push("## Dependencies");
    for (const edge of edges) {
      lines.push(
        `- ${edge.sourceActionId} --${edge.type}--> ${edge.targetActionId}`,
      );
    }
  }

  return lines.join("\n");
}

function parseDigest(response: string): CrystalDigest {
  try {
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return {
        narrative: response,
        keyOutcomes: [],
        filesAffected: [],
        lessons: [],
      };
    }
    const parsed = JSON.parse(jsonMatch[0]) as Record<string, unknown>;
    return {
      narrative:
        typeof parsed.narrative === "string" ? parsed.narrative : response,
      keyOutcomes: Array.isArray(parsed.keyOutcomes)
        ? (parsed.keyOutcomes as string[])
        : [],
      filesAffected: Array.isArray(parsed.filesAffected)
        ? (parsed.filesAffected as string[])
        : [],
      lessons: Array.isArray(parsed.lessons)
        ? (parsed.lessons as string[])
        : [],
    };
  } catch {
    return {
      narrative: response,
      keyOutcomes: [],
      filesAffected: [],
      lessons: [],
    };
  }
}
