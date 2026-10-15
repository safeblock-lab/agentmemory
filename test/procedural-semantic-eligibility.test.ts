import { describe, expect, it, vi } from "vitest";
import { KV } from "../src/state/schema.js";
import { newRoot, runPagedCut, saveRoot, loadRoot, pageKey } from "../src/functions/consolidation-manifest.js";
import { ConsolidationRequests } from "../src/functions/consolidation-partitions.js";

function fixture(empty = false) {
  const rows = new Map<string, unknown>(); const key = (scope: string, id: string) => `${scope}\0${id}`;
  const kv = {
    get: vi.fn(async (scope: string, id: string) => structuredClone(rows.get(key(scope, id)) ?? null)),
    set: vi.fn(async (scope: string, id: string, value: unknown) => { rows.set(key(scope, id), structuredClone(value)); return value; }),
    getVersioned: vi.fn(async (scope: string, id: string) => ({ exists: rows.has(key(scope, id)),
      value: structuredClone(rows.get(key(scope, id)) ?? null), version: "1" })),
    list: vi.fn(async () => { throw new Error("global scan forbidden"); }),
  };
  const root = newRoot("procedural-semantic-fixture", "procedural", "p", [0, 1].map(index => ({
    id: `o${index}`, sessionId: `s${index}`, project: "p", scope: KV.observations(`s${index}`), kind: "observation" as const, rowVersion: "1" })));
  for (const [index, ref] of root.refs.entries()) {
    rows.set(key(KV.sessions, ref.sessionId), { id: ref.sessionId, project: "p" });
    rows.set(key(ref.scope, ref.id), { id: ref.id, sessionId: ref.sessionId, title: "", importance: 1,
      narrative: empty ? "" : index === 0 ? "Pin the package version before installation." : "Verify the lockfile after installing the dependency.",
      facts: [], concepts: [index === 0 ? "dependency" : "lockfile"] });
  }
  const generate = vi.fn(async () => "<procedures/>"); const publish = vi.fn(async () => { throw new Error("empty candidates cannot publish"); });
  const cut = () => runPagedCut(kv as never, root.id, current => new ConsolidationRequests(kv as never, current.id, generate), publish);
  const drive = async () => {
    await saveRoot(kv as never, root);
    let result: Record<string, unknown> = {};
    for (let count = 0; count < 100; count++) {
      kv.getVersioned.mockClear(); const calls = generate.mock.calls.length;
      result = await cut();
      expect(kv.getVersioned.mock.calls.length).toBeLessThanOrEqual(1);
      expect(generate.mock.calls.length - calls).toBeLessThanOrEqual(1);
      if (!result.queued) return result;
    }
    throw new Error("bounded cut budget exhausted");
  };
  return { kv, rows, key, root, generate, publish, cut, drive };
}

describe("procedural semantic eligibility", () => {
  it("offers nonempty disjoint-concept low-score observations to the existing LLM, which can return no procedure", async () => {
    const f = fixture(); const result = await f.drive();
    expect(result).toMatchObject({ completed: true, reason: "no_supported_candidates", newProcedures: 0 });
    expect(f.generate).toHaveBeenCalledTimes(2); expect(f.publish).not.toHaveBeenCalled();
    expect((await loadRoot(f.kv as never, f.root.id))!.eligibleSources).toEqual([0, 1]);
    for (const [, value] of f.rows) if (value && typeof value === "object" && "frequency" in value) expect(value.frequency).toBe(1);
    expect(f.kv.list).not.toHaveBeenCalled();
  });
  it("does not send blank observations to a model", async () => {
    const f = fixture(true); expect(await f.drive()).toMatchObject({ completed: true, newProcedures: 0 });
    expect(f.generate).not.toHaveBeenCalled(); expect(f.publish).not.toHaveBeenCalled();
  });
  it.each(["foreign", "deleted"])("rejects %s authority before generation", async state => {
    const f = fixture(); const ref = f.root.refs[0];
    if (state === "foreign") f.rows.set(f.key(KV.sessions, ref.sessionId), { project: "foreign" });
    else f.rows.delete(f.key(ref.scope, ref.id));
    expect(await f.drive()).toMatchObject({ completed: false, partial: true });
    expect(f.generate).not.toHaveBeenCalled(); expect(f.publish).not.toHaveBeenCalled();
  });
  it("replays a completed legacy root without reading sources or dispatching a model", async () => {
    const f = fixture(); f.root.phase = "completed"; f.root.legacy = { originalId: "legacy", source: 2, fragment: 0, imported: true, completed: true };
    f.root.result = { completed: true, newProcedures: 2 }; await saveRoot(f.kv as never, f.root);
    const before = await f.kv.get(KV.state, pageKey(f.root.id, "root", 0)); f.kv.set.mockClear();
    expect(await f.cut()).toMatchObject({ completed: true, replayed: true, newProcedures: 0, appliedEffects: 0 });
    expect(f.generate).not.toHaveBeenCalled(); expect(f.publish).not.toHaveBeenCalled(); expect(f.kv.getVersioned).not.toHaveBeenCalled();
    expect(f.kv.set).not.toHaveBeenCalled(); expect(await f.kv.get(KV.state, pageKey(f.root.id, "root", 0))).toEqual(before);
  });
});
