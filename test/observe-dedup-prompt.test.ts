import { describe, it, expect, vi, beforeEach } from "vitest";
import { mockKV, mockSdk } from "./helpers/mocks.js";
import "../src/functions/observe.js";

vi.mock("iii-sdk", () => ({ TriggerAction: { Void: () => ({}) } }));
vi.mock("../src/config.js", () => ({
  getAgentId: () => undefined, isAutoCompressEnabled: () => false, isTypeSafeFeatureEnabled: () => false,
  TYPESAFE_ADMISSION_CONFIDENCE_THRESHOLD: 1, TYPESAFE_SCORING_CONFIDENCE_THRESHOLD: 1,
}));

vi.mock("../src/logger.js", () => ({
  logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));
vi.mock("../src/state/kv.js", () => ({ StateKV: class {} }));
vi.mock("../src/functions/search.js", () => ({
  getSearchIndex: () => ({ add: vi.fn() }), scheduleIndexSave: vi.fn(), vectorIndexAddGuarded: vi.fn(),
}));
vi.mock("../src/functions/live-observation.js", () => ({ publishLiveObservation: vi.fn() }));

function observePayload(hookType: string, data: unknown) {
  return {
    sessionId: "ses_dedup_test",
    project: "/home/user/myrepo",
    cwd: "/home/user/myrepo",
    hookType,
    timestamp: new Date().toISOString(),
    data,
  };
}

function registerObservationAdmission(sdk: ReturnType<typeof mockSdk>, kv: ReturnType<typeof mockKV>) {
  (kv as any).pages = async function* (scope: string, options: { limit: number; fields?: string[] }) {
    const rows = Array.from(kv.store.get(scope)?.entries() ?? [], ([id, value]) => options.fields?.length ? { id } : value);
    for (let offset = 0; offset < rows.length; offset += options.limit) yield { items: rows.slice(offset, offset + options.limit) };
  };
  sdk.registerFunction("state::observation_admit_v1", async (input: unknown) => {
    const request = input as Record<string, any>;
    const sessionId = request.session_id as string;
    const identityScope = `mem:obs-identity:${sessionId}`;
    const eventScope = `mem:obs-events:${sessionId}`;
    const observationScope = `mem:obs:${sessionId}`;
    const identity = await kv.get<{ observationId: string }>(identityScope, request.identity_digest);
    const observationId = identity?.observationId ?? request.observation.id;
    const current = await kv.get<Record<string, any>>(observationScope, observationId);
    const event = await kv.get(eventScope, request.event_id);
    if (event && current) return { created: false, duplicate_event: true, observation_id: observationId, observation: current, version: "1", occurrence_count: current.occurrenceCount ?? 1, first_seen_at: current.firstSeenAt, last_seen_at: current.lastSeenAt };
    if (identity && current) {
      const count = (current.occurrenceCount ?? 1) + 1;
      const updated = { ...current, occurrenceCount: count, firstSeenAt: [current.firstSeenAt, request.occurred_at].sort()[0], lastSeenAt: [current.lastSeenAt, request.occurred_at].sort().at(-1) };
      await kv.set(observationScope, observationId, updated);
      await kv.set(eventScope, request.event_id, { observationId, identityDigest: request.identity_digest, occurredAt: request.occurred_at });
      return { created: false, duplicate_event: false, observation_id: observationId, observation: updated, version: String(count), occurrence_count: count, first_seen_at: updated.firstSeenAt, last_seen_at: updated.lastSeenAt };
    }
    const session = await kv.get<Record<string, any>>("mem:sessions", sessionId) ?? request.session;
    if (request.max_observations && session && (session.observationCount ?? 0) >= request.max_observations) throw new Error("STATE_OBSERVATION_LIMIT");
    const observation = { ...request.observation, identityDigest: request.identity_digest, occurrenceCount: 1, firstSeenAt: request.occurred_at, lastSeenAt: request.occurred_at, provenance: [{ ...request.provenance, count: 1, firstSeenAt: request.occurred_at, lastSeenAt: request.occurred_at }] };
    await kv.set(observationScope, observationId, observation);
    await kv.set(identityScope, request.identity_digest, { observationId, identityDigest: request.identity_digest });
    await kv.set(identityScope, "__ready__", { version: 1, ready: true });
    await kv.set(eventScope, request.event_id, { observationId, identityDigest: request.identity_digest, occurredAt: request.occurred_at });
    if (session) await kv.set("mem:sessions", sessionId, { ...session, observationCount: (session.observationCount ?? 0) + 1, updatedAt: request.occurred_at });
    return { created: true, duplicate_event: false, observation_id: observationId, observation, version: "1", occurrence_count: 1, first_seen_at: observation.firstSeenAt, last_seen_at: observation.lastSeenAt };
  });
}

describe("observe dedup for hooks without tool_input (#1173)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("records consecutive prompt_submit observations with different prompts", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const { DedupMap } = await import("../src/functions/dedup.js");
    const sdk = mockSdk({ looseTrigger: true });
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never, new DedupMap());
    registerObservationAdmission(sdk, kv);

    const first = (await sdk.trigger(
      "mem::observe",
      observePayload("prompt_submit", { prompt: "ship the helm chart" }),
    )) as { observationId?: string; deduplicated?: boolean };
    const second = (await sdk.trigger(
      "mem::observe",
      observePayload("prompt_submit", { prompt: "now fix the failing test" }),
    )) as { observationId?: string; deduplicated?: boolean };

    expect(first.observationId).toBeTruthy();
    expect(second.deduplicated).toBeUndefined();
    expect(second.observationId).toBeTruthy();
  });

  it("records two prompt_submit observations whose data is distinct primitive strings", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const { DedupMap } = await import("../src/functions/dedup.js");
    const sdk = mockSdk({ looseTrigger: true });
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never, new DedupMap());
    registerObservationAdmission(sdk, kv);

    const first = (await sdk.trigger(
      "mem::observe",
      observePayload("prompt_submit", "ship the helm chart"),
    )) as { observationId?: string; deduplicated?: boolean };
    const second = (await sdk.trigger(
      "mem::observe",
      observePayload("prompt_submit", "now fix the failing test"),
    )) as { observationId?: string; deduplicated?: boolean };

    expect(first.observationId).toBeTruthy();
    expect(second.deduplicated).toBeUndefined();
    expect(second.observationId).toBeTruthy();
  });

  it("still dedups an identical prompt_submit within the TTL window", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const { DedupMap } = await import("../src/functions/dedup.js");
    const sdk = mockSdk({ looseTrigger: true });
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never, new DedupMap());
    registerObservationAdmission(sdk, kv);

    const payload = { prompt: "ship the helm chart" };
    const first = (await sdk.trigger(
      "mem::observe",
      observePayload("prompt_submit", payload),
    )) as { observationId?: string };
    const second = (await sdk.trigger(
      "mem::observe",
      observePayload("prompt_submit", payload),
    )) as { deduplicated?: boolean };

    expect(first.observationId).toBeTruthy();
    expect(second.deduplicated).toBe(true);
  });

  it("retains different tool responses as distinct complete originals", async () => {
    const { registerObserveFunction } = await import("../src/functions/observe.js");
    const { DedupMap } = await import("../src/functions/dedup.js");
    const sdk = mockSdk({ looseTrigger: true });
    const kv = mockKV();
    registerObserveFunction(sdk as never, kv as never, new DedupMap());
    registerObservationAdmission(sdk, kv);

    const first = (await sdk.trigger(
      "mem::observe",
      observePayload("post_tool_use", {
        tool_name: "Bash",
        tool_input: { command: "ls" },
        tool_response: "a.txt",
      }),
    )) as { observationId?: string };
    const second = (await sdk.trigger(
      "mem::observe",
      observePayload("post_tool_use", {
        tool_name: "Bash",
        tool_input: { command: "ls" },
        tool_response: "b.txt",
      }),
    )) as { deduplicated?: boolean };

    expect(first.observationId).toBeTruthy();
    expect(second.deduplicated).toBeUndefined();
    const originals = Array.from(kv.store.get("mem:obs:ses_dedup_test")?.values() ?? []);
    expect(originals).toHaveLength(2);
    const [stored] = originals;
    expect(stored).toMatchObject({
      raw: {
        tool_name: "Bash",
        tool_input: { command: "ls" },
        tool_response: "a.txt",
      },
    });
  });

  it("reconstructs legacy identity from the complete raw observation", async () => {
    const { reconstructLegacyIdentity } = await import("../src/functions/observation-identity.js");
    const originalRaw = {
      tool_name: "Bash",
      tool_input: { command: "ls" },
      tool_response: "a.txt",
    };
    const identity = reconstructLegacyIdentity({
      sessionId: "ses_legacy_identity",
      hookType: "post_tool_use",
      timestamp: "2026-01-01T00:00:00.000Z",
      raw: originalRaw,
    });

    expect(identity).not.toBeNull();
    expect(JSON.parse(identity!.canonicalIdentity).data).toEqual(originalRaw);
  });
});

describe("storage source exact observation contracts", () => {
  it("keeps legacy in-process dedup stable across object key order without merging sessions", async () => {
    const { DedupMap } = await import("../src/functions/dedup.js");
    const map = new DedupMap();
    try {
      const first = map.computeHash("s", "tool", { a: 1, b: 2 });
      map.record(first);
      expect(map.isDuplicate(map.computeHash("s", "tool", { b: 2, a: 1 }))).toBe(true);
      expect(map.isDuplicate(map.computeHash("other", "tool", { a: 1, b: 2 }))).toBe(false);
    } finally { map.stop(); }
  });

  it("ignores object key order but preserves changed output, array order, session and hook", async () => {
    const { observationIdentity } = await import("../src/functions/observation-identity.js");
    const payload = { sessionId: "s", hookType: "post_tool_use" as const, timestamp: "2000-01-01T00:00:00Z" };
    const data = { tool_input: { a: 1, b: 2 }, tool_output: [1, 2] };
    const first = observationIdentity(payload, data, { toolInputIdentity: true });
    expect(observationIdentity(payload, { tool_output: [1, 2], tool_input: { b: 2, a: 1 } }).identityDigest).toBe(first.identityDigest);
    for (const changed of [{ ...data, tool_output: [2, 1] }, { ...data, tool_output: "changed" }]) {
      expect(observationIdentity(payload, changed, { toolInputIdentity: true }).identityDigest).not.toBe(first.identityDigest);
    }
    expect(observationIdentity({ ...payload, sessionId: "other" }, data).identityDigest).not.toBe(first.identityDigest);
    expect(observationIdentity({ ...payload, hookType: "pre_tool_use" }, data).identityDigest).not.toBe(first.identityDigest);
    const later = observationIdentity({ ...payload, timestamp: "2000-01-02T00:00:00Z" }, data);
    expect(later.identityDigest).toBe(first.identityDigest);
    expect(later.eventId).not.toBe(first.eventId);
  });

  it("replays one exact admission body after lost acknowledgement without dropping originals", async () => {
    const { admitObservation, observationIdentity } = await import("../src/functions/observation-identity.js");
    const raw = { id: "o", sessionId: "s", hookType: "prompt_submit" as const, timestamp: "2000-01-01T00:00:00Z", raw: { prompt: "unique" } };
    const observation = { ...raw, occurrenceCount: 1, firstSeenAt: raw.timestamp, lastSeenAt: raw.timestamp,
      provenance: [{ sessionId: raw.sessionId, hookType: raw.hookType, count: 1, firstSeenAt: raw.timestamp, lastSeenAt: raw.timestamp }] };
    const trigger = vi.fn().mockRejectedValueOnce(new Error("transport lost ACK")).mockResolvedValueOnce({
      created: false, duplicate_event: true, observation_id: "o", observation, version: "1", occurrence_count: 1,
      first_seen_at: raw.timestamp, last_seen_at: raw.timestamp,
    });
    const result = await admitObservation({ trigger } as never, observationIdentity(raw, raw.raw), raw, raw);
    expect(trigger).toHaveBeenCalledTimes(2);
    expect(trigger.mock.calls[0][0]).toEqual(trigger.mock.calls[1][0]);
    expect(result.observation.raw).toEqual(raw.raw);
    expect(result.observation.provenance).toEqual(observation.provenance);
  });

  it.each(["STATE_TX_CONFLICT", "STATE_OBSERVATION_IDENTITY_DANGLING"])("does not retry native refusal %s", async (code) => {
    const { admitObservation, observationIdentity } = await import("../src/functions/observation-identity.js");
    const raw = { id: "o", sessionId: "s", hookType: "prompt_submit" as const, timestamp: "2000-01-01T00:00:00Z", raw: "original" };
    const trigger = vi.fn().mockRejectedValue(new Error(code));
    await expect(admitObservation({ trigger } as never, observationIdentity(raw, raw.raw), raw, raw)).rejects.toMatchObject({ releaseImage: true });
    expect(trigger).toHaveBeenCalledTimes(1);
  });

  it("rejects missing count/time metadata and changed originals rather than accepting lossy dedup", async () => {
    const { admitObservation, observationIdentity } = await import("../src/functions/observation-identity.js");
    const raw = { id: "o", sessionId: "s", hookType: "prompt_submit" as const, timestamp: "2000-01-01T00:00:00Z", raw: "unique" };
    const response = { created: false, duplicate_event: true, observation_id: "o", version: "1", occurrence_count: 1,
      first_seen_at: raw.timestamp, last_seen_at: raw.timestamp,
      observation: { ...raw, occurrenceCount: 1, firstSeenAt: raw.timestamp, lastSeenAt: raw.timestamp,
        provenance: [{ sessionId: raw.sessionId, hookType: raw.hookType, count: 1, firstSeenAt: raw.timestamp, lastSeenAt: raw.timestamp }] } };
    for (const invalid of [
      { ...response, observation: { ...response.observation, raw: "wrong" } },
      { ...response, observation: { ...response.observation, provenance: [] } },
      { ...response, occurrence_count: 0 }, { ...response, first_seen_at: "invalid" },
    ]) {
      await expect(admitObservation({ trigger: vi.fn().mockResolvedValue(invalid) } as never, observationIdentity(raw, raw.raw), raw, raw)).rejects.toMatchObject({ releaseImage: false });
    }
  });
});
