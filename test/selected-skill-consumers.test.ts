import { beforeEach, describe, expect, it, vi } from "vitest";
import { registerSkillExtractFunctions } from "../src/functions/skill-extract.js";
import { KV } from "../src/state/schema.js";

type Skill = {
  id: string;
  name: string;
  triggerCondition: string;
  steps: string[];
  sourceSessionIds: string[];
  strength: number;
  frequency: number;
  createdAt: string;
  updatedAt: string;
};

const session = { id: "session-1", project: "D:/workspace", status: "completed" };

function makeSkill(id: string, name: string, strength: number, sourceSessionIds = [session.id]): Skill {
  return {
    id, name, triggerCondition: "provider request timeout", steps: ["check response code", "retry safely"],
    sourceSessionIds, strength, frequency: 1, createdAt: "2026-10-10T00:00:00.000Z", updatedAt: "2026-10-10T00:00:00.000Z",
  };
}

function setup(records: Record<string, unknown> = {}, ordinarySkills: Skill[] = []) {
  const values = new Map(Object.entries(records));
  const get = vi.fn(async (scope: string, id: string) => values.get(`${scope}:${id}`) ?? null);
  const list = vi.fn(async () => ordinarySkills);
  const handlers = new Map<string, (data: any) => Promise<any>>();
  registerSkillExtractFunctions(
    { registerFunction: (id: string, handler: (data: any) => Promise<any>) => handlers.set(id, handler) } as any,
    { get, list } as any,
    {} as any,
  );
  return { get, list, handlers };
}

describe("selected skill consumers", () => {
  beforeEach(() => vi.clearAllMocks());

  it("lists exact selected skills after session/project authorization and applies the limit", async () => {
    const low = makeSkill("skill_low", "Payment Cache Cleanup", 0.4);
    const high = makeSkill("skill_high", "Payment Request Recovery", 0.9);
    const { get, list, handlers } = setup({
      [`${KV.sessions}:${session.id}`]: session,
      [`${KV.procedural}:${low.id}`]: low,
      [`${KV.procedural}:${high.id}`]: high,
    });

    const result = await handlers.get("mem::skill-list")!({
      selected: { project: session.project, sessionId: session.id, skillIds: [low.id, high.id] }, limit: 1,
    });

    expect(result).toMatchObject({ success: true, total: 2, skills: [high] });
    expect(get.mock.calls.map(([scope, id]) => [scope, id])).toEqual([
      [KV.sessions, session.id], [KV.procedural, low.id], [KV.procedural, high.id],
    ]);
    expect(list).not.toHaveBeenCalled();
  });

  it("scores only selected skills and preserves deterministic relevance order", async () => {
    const direct = makeSkill("skill_direct", "Payment Request Recovery", 0.9);
    const partial = makeSkill("skill_partial", "Payment Cache Cleanup", 0.8);
    direct.triggerCondition = "provider request timeout";
    partial.triggerCondition = "cache miss";
    const { list, handlers } = setup({
      [`${KV.sessions}:${session.id}`]: session,
      [`${KV.procedural}:${direct.id}`]: direct,
      [`${KV.procedural}:${partial.id}`]: partial,
    });

    const result = await handlers.get("mem::skill-match")!({
      query: "payment timeout", selected: { project: session.project, sessionId: session.id, skillIds: [partial.id, direct.id] },
    });

    expect(result.matches.map((match: { skill: Skill }) => match.skill.id)).toEqual([direct.id, partial.id]);
    expect(result.matches.map((match: { score: number }) => match.score)).toEqual([0.9, 0.4]);
    expect(list).not.toHaveBeenCalled();
  });

  it("rejects a project mismatch before reading any procedural skill", async () => {
    const { get, list, handlers } = setup({ [`${KV.sessions}:${session.id}`]: session });

    const result = await handlers.get("mem::skill-list")!({
      selected: { project: "D:/foreign", sessionId: session.id, skillIds: ["skill_1"] },
    });

    expect(result).toMatchObject({ success: false, error: "selected session project scope mismatch" });
    expect(get).toHaveBeenCalledTimes(1);
    expect(list).not.toHaveBeenCalled();
  });

  it("rejects a skill without selected-session provenance and never lists the corpus", async () => {
    const foreign = makeSkill("skill_foreign", "Foreign Skill", 0.8, ["another-session"]);
    const { list, handlers } = setup({
      [`${KV.sessions}:${session.id}`]: session,
      [`${KV.procedural}:${foreign.id}`]: foreign,
    });

    const result = await handlers.get("mem::skill-match")!({
      query: "foreign skill", selected: { project: session.project, sessionId: session.id, skillIds: [foreign.id] },
    });

    expect(result).toMatchObject({ success: false, error: "selected skill session scope mismatch" });
    expect(list).not.toHaveBeenCalled();
  });

  it("accepts the twelve-ID bound and reports an absent exact skill", async () => {
    const selectedSkills = Array.from({ length: 12 }, (_, index) => makeSkill(`skill_${index}`, `Skill ${index}`, index / 12));
    const records: Record<string, unknown> = { [`${KV.sessions}:${session.id}`]: session };
    for (const skill of selectedSkills) records[`${KV.procedural}:${skill.id}`] = skill;
    const { get, list, handlers } = setup(records);

    const bounded = await handlers.get("mem::skill-list")!({
      selected: { project: session.project, sessionId: session.id, skillIds: selectedSkills.map(skill => skill.id) }, limit: 12,
    });
    const missing = await handlers.get("mem::skill-list")!({
      selected: { project: session.project, sessionId: session.id, skillIds: ["skill_absent"] },
    });

    expect(bounded).toMatchObject({ success: true, total: 12 });
    expect(get.mock.calls.filter(([scope]) => scope === KV.procedural)).toHaveLength(13);
    expect(missing).toMatchObject({ success: false, error: "selected skill not found" });
    expect(list).not.toHaveBeenCalled();
  });

  it("accepts generated Unicode triggers and steps beyond 512 bytes while bounding them at 64 KiB", async () => {
    const longSkill = makeSkill("skill_long", "Long Procedure", 0.8);
    const longText = "ñ".repeat(300);
    longSkill.triggerCondition = longText;
    longSkill.steps = [longText, "retry safely"];
    const { list, handlers } = setup({
      [`${KV.sessions}:${session.id}`]: session,
      [`${KV.procedural}:${longSkill.id}`]: longSkill,
    });
    const accepted = await handlers.get("mem::skill-list")!({
      selected: { project: session.project, sessionId: session.id, skillIds: [longSkill.id] },
    });
    const acceptedStep = accepted.skills[0].steps[0];
    longSkill.steps[0] = "ñ".repeat(32_769);
    const oversized = await handlers.get("mem::skill-list")!({
      selected: { project: session.project, sessionId: session.id, skillIds: [longSkill.id] },
    });

    expect(accepted.success).toBe(true);
    expect(acceptedStep).toBe(longText);
    expect(oversized).toMatchObject({ success: false, error: "selected skill record is invalid" });
    expect(list).not.toHaveBeenCalled();
  });

  it.each([
    [],
    ["skill_1", "skill_1"],
    Array.from({ length: 13 }, (_, index) => `skill_${index}`),
  ])("rejects invalid selected ID sets before point reads: %j", async skillIds => {
    const { get, list, handlers } = setup({ [`${KV.sessions}:${session.id}`]: session });

    const result = await handlers.get("mem::skill-list")!({
      selected: { project: session.project, sessionId: session.id, skillIds },
    });

    expect(result).toMatchObject({ success: false, error: "selected skill scope is invalid" });
    expect(get).not.toHaveBeenCalled();
    expect(list).not.toHaveBeenCalled();
  });

  it("retains ordinary listing and validates consumer limits", async () => {
    const low = makeSkill("skill_a", "Low", 0.4);
    const high = makeSkill("skill_z", "High", 0.9);
    const { list, handlers } = setup({}, [low, high]);

    const listed = await handlers.get("mem::skill-list")!({ limit: 1 });
    const invalid = await handlers.get("mem::skill-match")!({ query: "timeout", limit: -1 });

    expect(listed).toMatchObject({ success: true, total: 2, skills: [high] });
    expect(invalid).toMatchObject({ success: false, error: "limit must be a non-negative safe integer" });
    expect(list).toHaveBeenCalledTimes(1);
  });
});
