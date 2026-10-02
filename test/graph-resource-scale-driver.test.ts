import { createHash } from "node:crypto";
import type { StateKV } from "../src/state/kv.js";
import { KV } from "../src/state/schema.js";
import { describe, expect, it } from "vitest";
import {
  fingerprintStoredGraph,
  syntheticScalePairStoredBytes,
} from "../.native-pagination-build/graph-resource/campaign-scale-driver.js";

type DegreeLookup = (scope: string, key: string) => Promise<unknown>;

function stateWithDegreeLookup(lookup: DegreeLookup): StateKV {
  return {
    async get<T>(scope: string, key: string): Promise<T | null> {
      return await lookup(scope, key) as T | null;
    },
    async *pages() {
      yield { items: [], next_cursor: null };
    },
  } as unknown as StateKV;
}

function keyForOrdinal(ordinal: number): string {
  const pair = Math.floor(ordinal / 2).toString().padStart(12, "0");
  const side = ordinal % 2 === 0 ? "a" : "b";
  return `resource-scale-node-${pair}-${side}`;
}

function expectedDegreeDigest(pairCount: number): string {
  const hash = createHash("sha256");
  for (let ordinal = 0; ordinal < pairCount * 2; ordinal++) {
    const key = keyForOrdinal(ordinal);
    const value = ordinal + 1;
    hash.update(KV.graphNodeDegree);
    hash.update("\0");
    hash.update(key);
    hash.update("\0");
    hash.update(JSON.stringify(value));
    hash.update("\n");
  }
  return hash.digest("hex");
}

describe("scale driver protected degree fingerprint", () => {
  it("derives an exact equal-byte target for bounded whole-pair fixtures", () => {
    const firstPairBytes = syntheticScalePairStoredBytes(0);
    const lastPairBytes = syntheticScalePairStoredBytes(31);

    expect(firstPairBytes).toBeGreaterThan(0);
    expect(firstPairBytes).toBe(lastPairBytes);
    expect(Number.isSafeInteger(firstPairBytes * 32)).toBe(true);
    expect(firstPairBytes * 32).toBeGreaterThan(firstPairBytes);
    expect(() => syntheticScalePairStoredBytes(-1)).toThrow("nonnegative safe integer");
  });

  it("bounds reads at16 and hashes results in ordinal order after delayed out-of-order replies", async () => {
    const pairCount = 20;
    const completions: string[] = [];
    let pending = 0;
    let maximumPending = 0;
    const kv = stateWithDegreeLookup(async (scope, key) => {
      expect(scope).toBe(KV.graphNodeDegree);
      const match = key.match(/resource-scale-node-(\d{12})-([ab])$/);
      expect(match).not.toBeNull();
      const ordinal = Number(match![1]) * 2 + (match![2] === "b" ? 1 : 0);
      pending++;
      maximumPending = Math.max(maximumPending, pending);
      await new Promise((resolve) => setTimeout(resolve, (16 - ordinal % 16) * 2));
      pending--;
      completions.push(key);
      return ordinal + 1;
    });

    const fingerprint = await fingerprintStoredGraph(kv, pairCount);

    expect(maximumPending).toBe(16);
    expect(pending).toBe(0);
    expect(completions).not.toEqual(Array.from({ length: pairCount * 2 }, (_, ordinal) => keyForOrdinal(ordinal)));
    expect(fingerprint.protectedSeedRows).toBe(pairCount * 2);
    expect(fingerprint.protectedSeedSha256).toBe(expectedDegreeDigest(pairCount));
  });

  it("drains every in-flight request and reports all rejected keys before failing", async () => {
    const pairCount = 20;
    const started: string[] = [];
    let pending = 0;
    let maximumPending = 0;
    let settled = 0;
    const kv = stateWithDegreeLookup(async (_scope, key) => {
      const ordinalMatch = key.match(/resource-scale-node-(\d{12})-([ab])$/)!;
      const ordinal = Number(ordinalMatch[1]) * 2 + (ordinalMatch[2] === "b" ? 1 : 0);
      started.push(key);
      pending++;
      maximumPending = Math.max(maximumPending, pending);
      await new Promise((resolve) => setTimeout(resolve, ordinal === 15 ? 45 : 5));
      pending--;
      settled++;
      if (ordinal === 1 || ordinal === 4) throw new Error(`synthetic rejection ${ordinal}`);
      return ordinal + 1;
    });

    const failure = fingerprintStoredGraph(kv, pairCount);
    await expect(failure).rejects.toThrow("draining 16 requests");
    await expect(failure).rejects.toThrow("resource-scale-node-000000000000-b");
    await expect(failure).rejects.toThrow("resource-scale-node-000000000002-a");
    expect(started).toHaveLength(16);
    expect(settled).toBe(16);
    expect(pending).toBe(0);
    expect(maximumPending).toBe(16);
  });

  it("rejects missing and malformed values after the whole read window settles", async () => {
    const started: string[] = [];
    const kv = stateWithDegreeLookup(async (_scope, key) => {
      started.push(key);
      const ordinalMatch = key.match(/resource-scale-node-(\d{12})-([ab])$/)!;
      const ordinal = Number(ordinalMatch[1]) * 2 + (ordinalMatch[2] === "b" ? 1 : 0);
      await new Promise((resolve) => setTimeout(resolve, ordinal === 15 ? 25 : 1));
      if (ordinal === 2) return null;
      if (ordinal === 6) return -1;
      return ordinal + 1;
    });

    await expect(fingerprintStoredGraph(kv, 10)).rejects.toThrow(
      /resource-scale-node-000000000001-a: degree is missing or malformed; resource-scale-node-000000000003-a: degree is missing or malformed/,
    );
    expect(started).toHaveLength(16);
  });

  it.each([-1, 1.5, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    "rejects invalid protected pair count %s before making reads",
    async (pairCount) => {
      let reads = 0;
      const kv = stateWithDegreeLookup(async () => {
        reads++;
        return 1;
      });

      await expect(fingerprintStoredGraph(kv, pairCount)).rejects.toThrow("nonnegative safe integer");
      expect(reads).toBe(0);
    },
  );
});
