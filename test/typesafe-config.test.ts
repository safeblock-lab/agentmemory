import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { getTypeSafeConfig, isTypeSafeFeatureEnabled } from "../src/config.js";

const KEYS = [
  "TYPESAFE_API_KEY",
  "AGENTMEMORY_TYPESAFE_ENABLED",
  "AGENTMEMORY_TYPESAFE_TIMEOUT_MS",
  "AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS",
  "AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED",
  "AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED",
  "AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED",
  "AGENTMEMORY_TYPESAFE_SCORING_ENABLED",
] as const;

const original = new Map<string, string | undefined>();

beforeEach(() => {
  for (const key of KEYS) {
    original.set(key, process.env[key]);
    process.env[key] = "";
  }
});

afterEach(() => {
  for (const [key, value] of original) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  original.clear();
});

describe("TypeSafe configuration", () => {
  it("defaults the master off and stays unconfigured without a key", () => {
    const config = getTypeSafeConfig();

    expect(config).toEqual({
      enabled: false,
      apiKey: "",
      timeoutMs: 5_000,
      maxStateChars: 16_000,
      features: {
        compaction: false,
        admission: true,
        pipelineGates: true,
        scoring: true,
      },
    });
    expect(isTypeSafeFeatureEnabled("admission")).toBe(false);
  });

  it("accepts the documented boolean forms and lets the master flag disable all calls", () => {
    process.env["TYPESAFE_API_KEY"] = " test-key ";
    process.env["AGENTMEMORY_TYPESAFE_ENABLED"] = "0";
    process.env["AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED"] = "true";
    process.env["AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED"] = "1";

    const config = getTypeSafeConfig();

    expect(config.apiKey).toBe("test-key");
    expect(config.enabled).toBe(false);
    expect(config.features).toMatchObject({ compaction: true, admission: true });
    expect(isTypeSafeFeatureEnabled("admission")).toBe(false);
    expect(isTypeSafeFeatureEnabled("compaction")).toBe(false);
  });

  it("can independently disable each feature", () => {
    process.env["AGENTMEMORY_TYPESAFE_ENABLED"] = "true";
    process.env["AGENTMEMORY_TYPESAFE_COMPACTION_ENABLED"] = "0";
    process.env["AGENTMEMORY_TYPESAFE_ADMISSION_ENABLED"] = "false";
    process.env["AGENTMEMORY_TYPESAFE_PIPELINE_GATES_ENABLED"] = "false";
    process.env["AGENTMEMORY_TYPESAFE_SCORING_ENABLED"] = "0";

    const config = getTypeSafeConfig();

    expect(config.enabled).toBe(true);
    expect(Object.values(config.features)).toEqual([false, false, false, false]);
    expect(Object.keys(config.features).every((feature) =>
      !isTypeSafeFeatureEnabled(feature as keyof typeof config.features),
    )).toBe(true);
  });

  it("uses bounded defaults for invalid numbers and accepts values inside the limits", () => {
    process.env["AGENTMEMORY_TYPESAFE_TIMEOUT_MS"] = "999999";
    process.env["AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS"] = "not-a-number";
    expect(getTypeSafeConfig()).toMatchObject({ timeoutMs: 5_000, maxStateChars: 16_000 });

    process.env["AGENTMEMORY_TYPESAFE_TIMEOUT_MS"] = "12000";
    process.env["AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS"] = "32000";
    expect(getTypeSafeConfig()).toMatchObject({ timeoutMs: 12_000, maxStateChars: 32_000 });

    process.env["AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS"] = "255";
    expect(getTypeSafeConfig().maxStateChars).toBe(16_000);
    process.env["AGENTMEMORY_TYPESAFE_MAX_STATE_CHARS"] = "64000";
    expect(getTypeSafeConfig().maxStateChars).toBe(64_000);
  });

  it("falls back to the disabled master default for unrecognized boolean values", () => {
    process.env["AGENTMEMORY_TYPESAFE_ENABLED"] = "sometimes";
    process.env["AGENTMEMORY_TYPESAFE_SCORING_ENABLED"] = "sometimes";

    expect(getTypeSafeConfig()).toMatchObject({
      enabled: false,
      features: { scoring: true },
    });
  });
});
