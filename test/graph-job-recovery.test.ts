import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../.native-pagination-build/graph-resource");
const protocol = JSON.parse(readFileSync(resolve(root, "scenarios.json"), "utf8")) as {
  identity: { jobId: string; captureIds: string[]; heuristicDeltaId: string; providerDeltaIds: string[]; batchEffectKey: string };
  promptGroups: Array<{ id: string; observationIds: string[]; prompt: string; response: string }>;
  promptGroupsEvidence: string;
  originalOracleEntryPoint: string;
  semanticCoverage: string[];
  recoveryScenarios: Array<{ id: string; expected: string }>;
  resource: { initialInputBytes: number; extraRamTargetBytes: number; maximumCompleteCommitUtf8Bytes: number; acceptance: string };
};

describe("graph recovery acceptance protocol", () => {
  it("keeps captured provider prompt groups and delta identities distinct", () => {
    expect(protocol.promptGroupsEvidence).toBe("hand-authored-preparation-example-not-original-oracle-evidence");
    expect(protocol.originalOracleEntryPoint).toBe("original-oracle.ts::runFrozenOriginalGraphExtraction");
    expect(protocol.identity.jobId).toBeTruthy();
    expect(protocol.identity.captureIds).toEqual(["capture-a", "capture-b"]);
    expect(new Set(protocol.identity.providerDeltaIds).size).toBe(protocol.identity.providerDeltaIds.length);
    expect(protocol.promptGroups.map((group) => group.id)).toEqual(protocol.identity.providerDeltaIds);
    expect(protocol.promptGroups.map((group) => group.observationIds)).toEqual([["capture-a"], ["capture-b"]]);
    expect(protocol.promptGroups.every((group) => group.prompt.length > 0 && group.response.includes("<entities>"))).toBe(true);
  });

  it("enumerates functional replay, lease, receipt, freeze, and read-barrier boundaries", () => {
    const ids = protocol.recoveryScenarios.map((scenario) => scenario.id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids).toEqual(expect.arrayContaining([
      "initial-unknown-outcome",
      "final-inclusive-delta-counts",
      "provider-response-staged-before-crash",
      "provider-failure-frozen",
      "transaction-code-and-cause",
      "pipeline-gate-and-queue-outcomes",
      "distinct-captures-same-content",
      "lost-generation-advance-ack",
      "missing-receipt",
      "competing-lease",
      "stale-heartbeat",
      "read-barriers",
    ]));
    expect(protocol.semanticCoverage).toEqual(expect.arrayContaining([
      "persistGraphDelta", "applyBatchGraph", "weights", "sourceObservationIds", "nameIndex", "edgeIndex", "degrees", "snapshot", "topEdges", "query", "resetOrphans", "mesh", "temporal", "reflect",
    ]));
  });

  it("records the provisional resource campaign boundaries as measurements, not acceptance claims", () => {
    expect(protocol.resource).toMatchObject({
      initialInputBytes: 1_610_612_736,
      extraRamTargetBytes: 268_435_456,
      maximumCompleteCommitUtf8Bytes: 4_194_304,
      acceptance: "unmeasured-preparation-only",
    });
  });
});
