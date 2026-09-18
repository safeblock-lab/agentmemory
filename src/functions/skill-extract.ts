import type { ISdk } from "iii-sdk";
import { withBatchWriterLocks, withBatchRecordLocks } from "../state/batch-effects.js";
import type {
  CompressedObservation,
  SessionSummary,
  ProceduralMemory,
  Session,
  MemoryProvider,
} from "../types.js";
import { KV, fingerprintId } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { recordAudit } from "./audit.js";
import { logger } from "../logger.js";
import type { LlmTaskRouter } from "../providers/task-router.js";
import type { TypeSafeDecisionProvider } from "../providers/typesafe.js";
import { stripPrivateData } from "./privacy.js";
import { TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD } from "../config.js";

const PROTECTED_SKILL_SIGNAL = /\b(?:error|failed|failure|decision|instruction|prompt|security|secret|token|password|credential|api[-_ ]?key|auth|permission|mutation|write|edit|patch|delete|move|rename|commit|push|reset|deploy|install|shell|terminal|bash|powershell|environment|env|(?:AGENTS|CLAUDE|GEMINI|COPILOT)\.md)\b/i;

function hasProtectedSkillSignal(text: string): boolean {
  return PROTECTED_SKILL_SIGNAL.test(text) || stripPrivateData(text) !== text;
}

const SKILL_EXTRACT_SYSTEM = `You are a skill extraction engine. Given a completed multi-step task session, extract a reusable procedural skill document.

Output format:
<skill>
<trigger>When the agent encounters [specific situation/pattern]</trigger>
<title>Short skill title</title>
<steps>
<step>First concrete action</step>
<step>Second concrete action</step>
</steps>
<expected_outcome>What success looks like</expected_outcome>
<tags>comma,separated,tags</tags>
</skill>

Rules:
- Extract ONLY if the session shows a clear multi-step procedure that succeeded
- Steps must be concrete and actionable, not vague
- The trigger should describe WHEN to apply this skill
- If the session is exploratory with no clear procedure, output <no-skill/>
- Maximum 10 steps per skill`;

function buildSkillPrompt(
  summary: SessionSummary,
  observations: CompressedObservation[],
): string {
  const obsText = observations
    .filter((o) => o.importance >= 4)
    .sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime())
    .slice(0, 30)
    .map(
      (o) =>
        `[${o.type}] ${o.title}${o.narrative ? ": " + o.narrative : ""}`,
    )
    .join("\n");

  return `## Session Summary
Title: ${summary.title}
Narrative: ${summary.narrative}
Key Decisions: ${summary.keyDecisions.join("; ")}
Files Modified: ${summary.filesModified.join(", ")}
Concepts: ${summary.concepts.join(", ")}

## Observations (${observations.length} total, showing top by importance)
${obsText}`;
}

function parseSkillXml(
  xml: string,
): {
  trigger: string;
  title: string;
  steps: string[];
  expectedOutcome: string;
  tags: string[];
} | null {
  if (xml.includes("<no-skill/>")) return null;

  const triggerMatch = xml.match(/<trigger>([\s\S]*?)<\/trigger>/);
  const titleMatch = xml.match(/<title>([\s\S]*?)<\/title>/);
  const stepsMatch = xml.match(/<steps>([\s\S]*?)<\/steps>/);
  const outcomeMatch = xml.match(
    /<expected_outcome>([\s\S]*?)<\/expected_outcome>/,
  );
  const tagsMatch = xml.match(/<tags>([\s\S]*?)<\/tags>/);

  if (!triggerMatch || !titleMatch || !stepsMatch) return null;

  const stepRegex = /<step>([\s\S]*?)<\/step>/g;
  const steps: string[] = [];
  let match;
  while ((match = stepRegex.exec(stepsMatch[1])) !== null) {
    const step = match[1].trim();
    if (step) steps.push(step);
  }

  if (steps.length < 2) return null;

  return {
    trigger: triggerMatch[1].trim(),
    title: titleMatch[1].trim(),
    steps,
    expectedOutcome: outcomeMatch?.[1]?.trim() || "",
    tags: tagsMatch?.[1]
      ?.split(",")
      .map((t) => t.trim())
      .filter(Boolean) || [],
  };
}

export function registerSkillExtractFunctions(
  sdk: ISdk,
  kv: StateKV,
  provider: MemoryProvider,
  llmRouter?: LlmTaskRouter,
  typeSafe?: TypeSafeDecisionProvider,
): void {
  sdk.registerFunction("mem::skill-extract",
    async (data: { sessionId: string; force?: boolean }) => {
      if (!data?.sessionId) {
        return { success: false, error: "sessionId is required" };
      }

      const session = await kv
        .get<Session>(KV.sessions, data.sessionId)
        .catch(() => null);
      if (!session) {
        return { success: false, error: "session not found" };
      }
      if (session.status !== "completed") {
        return {
          success: false,
          error: "session must be completed before skill extraction",
        };
      }

      const [summary, observations] = await Promise.all([
        kv.get<SessionSummary>(KV.summaries, data.sessionId).catch(() => null),
        kv.list<CompressedObservation>(KV.observations(data.sessionId)).catch(() => []),
      ]);
      if (!summary) {
        return {
          success: false,
          error: "no summary — run mem::summarize first",
        };
      }
      if (observations.length < 3) {
        return { success: false, error: "too few observations for skill extraction" };
      }

      const protectedSession =
        (summary.keyDecisions?.length ?? 0) > 0 ||
        (summary.filesModified?.length ?? 0) > 0 ||
        hasProtectedSkillSignal(`${summary.title} ${summary.narrative} ${(summary.concepts ?? []).join(" ")}`) ||
        observations.some((observation) =>
          observation.importance >= 8 ||
          observation.type === "error" ||
          observation.type === "decision" ||
          observation.type === "file_write" ||
          observation.type === "file_edit" ||
          observation.type === "command_run" ||
          observation.modality === "image" ||
          observation.modality === "mixed" ||
          hasProtectedSkillSignal(
            `${observation.title} ${observation.narrative} ${(observation.facts ?? []).join(" ")} ${(observation.concepts ?? []).join(" ")}`,
          ),
        );
      if (typeSafe && !data.force && !protectedSession) {
        try {
          const typeCounts = observations.reduce<Record<string, number>>((counts, observation) => {
            counts[observation.type] = (counts[observation.type] ?? 0) + 1;
            return counts;
          }, {});
          const decision = await typeSafe.evaluateChoice(
            "pipelineGates",
            stripPrivateData(JSON.stringify({
              workflow: "skill-extraction",
              summaryTitle: summary.title.slice(0, 48),
              concepts: (summary.concepts ?? []).slice(0, 5).map((concept) => concept.slice(0, 20)),
              observationCount: observations.length,
              observationTypes: typeCounts,
              averageImportance: observations.reduce((sum, observation) => sum + observation.importance, 0) / observations.length,
              sampleTitles: observations.slice(0, 3).map((observation) => observation.title.slice(0, 24)),
            })).slice(0, 512),
            "Should this completed session be analyzed for a reusable skill?",
            {
              run: "Run when the session appears to contain a repeatable procedure that would help future tasks.",
              skip: "Skip only when the session is clearly exploratory, routine, or unlikely to contain a reusable procedure.",
            },
          );
          if (decision?.choice === "skip" && decision.confidence >= TYPESAFE_SKILL_GATE_CONFIDENCE_THRESHOLD) {
            logger.info("Skill extraction skipped by TypeSafe gate", {
              sessionId: data.sessionId,
            });
            return {
              success: true,
              extracted: false,
              skipped: true,
              reason: "TypeSafe pipeline gate",
            };
          }
        } catch (error) {
          logger.warn("TypeSafe skill extraction gate failed open", {
            errorType: error instanceof Error ? error.name : "unknown",
          });
        }
      }

      try {
        const prompt = buildSkillPrompt(summary, observations);
        const response = llmRouter
          ? await llmRouter.run(
            "skill_extraction",
            (selectedProvider) => selectedProvider.summarize(SKILL_EXTRACT_SYSTEM, prompt),
            (candidate) => parseSkillXml(candidate) !== null,
          )
          : await provider.summarize(SKILL_EXTRACT_SYSTEM, prompt);
        const parsed = parseSkillXml(response);

        if (!parsed) {
          logger.info("No skill extracted — session was exploratory", {
            sessionId: data.sessionId,
          });
          return { success: true, extracted: false, reason: "no clear procedure found" };
        }

        const fp = fingerprintId(
          "skill",
          JSON.stringify({
            title: parsed.title.toLowerCase(),
            trigger: parsed.trigger.toLowerCase(),
            steps: parsed.steps.map((s) => s.toLowerCase().trim()),
          }),
        );
        return await withBatchWriterLocks(kv, ["consolidation"], () => withBatchRecordLocks([[KV.procedural, fp]], async () => {
          const existing = await kv.get<ProceduralMemory>(KV.procedural, fp);

          if (existing) {
            const alreadyReinforced = existing.sourceSessionIds.includes(data.sessionId);
            if (!alreadyReinforced) {
              existing.strength = Math.min(1.0, existing.strength + 0.15);
              existing.frequency++;
              existing.sourceSessionIds = [...existing.sourceSessionIds, data.sessionId];
            }
            existing.updatedAt = new Date().toISOString();
            await kv.set(KV.procedural, existing.id, existing);

            try {
              await recordAudit(kv, "skill_extract", "mem::skill-extract", [], {
                skillId: existing.id,
                reinforced: true,
                sessionId: data.sessionId,
              });
            } catch { }

            logger.info("Skill reinforced", {
              id: existing.id,
              name: parsed.title,
            });
            return {
              success: true,
              extracted: true,
              reinforced: true,
              skill: existing,
            };
          }

          const now = new Date().toISOString();
          const skill: ProceduralMemory = {
            id: fp,
            name: parsed.title,
            triggerCondition: parsed.trigger,
            steps: parsed.steps,
            expectedOutcome: parsed.expectedOutcome,
            strength: 0.6,
            frequency: 1,
            tags: parsed.tags,
            concepts: summary.concepts,
            sourceSessionIds: [data.sessionId],
            sourceObservationIds: observations
              .slice(0, 10)
              .map((o) => o.id),
            createdAt: now,
            updatedAt: now,
          };

          await kv.set(KV.procedural, skill.id, skill);

          try {
            await recordAudit(kv, "skill_extract", "mem::skill-extract", [], {
              skillId: skill.id,
              title: parsed.title,
              steps: parsed.steps.length,
              sessionId: data.sessionId,
            });
          } catch { }

          logger.info("Skill extracted", {
            id: skill.id,
            title: parsed.title,
            steps: parsed.steps.length,
          });

          return { success: true, extracted: true, reinforced: false, skill };
        }));
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error("Skill extraction failed", { error: msg });
        return { success: false, error: msg };
      }
    },
  );

  sdk.registerFunction("mem::skill-list",
    async (data: { limit?: number }) => {
      const limit = data?.limit ?? 50;
      const skills = await kv.list<ProceduralMemory>(KV.procedural);
      const sorted = skills.sort((a, b) => b.strength - a.strength);
      return {
        success: true,
        skills: sorted.slice(0, limit),
        total: sorted.length,
      };
    },
  );

  sdk.registerFunction("mem::skill-match",
    async (data: { query: string; limit?: number }) => {
      if (!data?.query?.trim()) {
        return { success: false, error: "query is required" };
      }

      const limit = data.limit ?? 5;
      const query = data.query.toLowerCase();
      const terms = query.split(/\s+/).filter((t) => t.length > 2);

      const skills = await kv.list<ProceduralMemory>(KV.procedural);

      const scored = skills
        .map((skill) => {
          const text =
            `${skill.name} ${skill.triggerCondition} ${(skill.tags || []).join(" ")} ${skill.steps.join(" ")}`.toLowerCase();
          const matchCount = terms.filter((t) => text.includes(t)).length;
          if (matchCount === 0) return null;
          const relevance = matchCount / terms.length;
          return { skill, score: relevance * skill.strength };
        })
        .filter(Boolean) as Array<{
          skill: ProceduralMemory;
          score: number;
        }>;

      scored.sort((a, b) => b.score - a.score);

      return {
        success: true,
        matches: scored.slice(0, limit),
      };
    },
  );
}
