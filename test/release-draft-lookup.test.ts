import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

const workflow = readFileSync(".github/workflows/release.yml", "utf8");
const tag = "v0.9.81";
const lookupBlock = workflow.match(
  /RELEASE_ID="\$\(node - "\$RELEASE_LOOKUP_JSON" "\$RELEASE_TAG" <<'NODE'\r?\n([\s\S]*?)\r?\n[ \t]*NODE\r?\n[ \t]*\)"/,
);
const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function validateLookup(response: unknown | string) {
  if (!lookupBlock) throw new Error("Draft release lookup validator is missing from workflow.");

  const root = mkdtempSync(join(tmpdir(), "agentmemory-release-lookup-"));
  roots.push(root);
  const lookupPath = join(root, "release-lookup.json");
  writeFileSync(lookupPath, typeof response === "string" ? response : JSON.stringify(response));

  return spawnSync(process.execPath, ["-", lookupPath, tag], {
    input: lookupBlock[1].replace(/^[ \t]{10}/gm, ""),
    encoding: "utf8",
  });
}

describe("frozen draft release lookup", () => {
  it("uses the draft-aware exact-tag CLI and fetches by validated ID before download", () => {
    const viewIndex = workflow.indexOf('gh release view "$RELEASE_TAG"');
    const apiIndex = workflow.indexOf('gh api "repos/${RELEASE_REPOSITORY}/releases/${RELEASE_ID}"');
    const downloadIndex = workflow.indexOf('gh release download "$RELEASE_TAG"');
    const verifierIndex = workflow.indexOf("node scripts/verify-release-assets.mjs");
    const publishIndex = workflow.indexOf("- name: Publish the verified draft");
    const publishedCheckIndex = workflow.indexOf("- name: Confirm the release is published");

    expect(workflow).toContain("set -euo pipefail");
    expect(workflow).toContain("--json databaseId,isDraft,tagName");
    expect([...workflow.matchAll(/--pattern /g)]).toHaveLength(9);
    expect(viewIndex).toBeGreaterThan(-1);
    expect(apiIndex).toBeGreaterThan(viewIndex);
    expect(downloadIndex).toBeGreaterThan(apiIndex);
    expect(verifierIndex).toBeGreaterThan(downloadIndex);
    expect(workflow).toContain('--tag-commit "$TAG_COMMIT"');
    expect(workflow).toContain('--expected-commit "$RELEASE_COMMIT"');
    expect(publishIndex).toBeGreaterThan(verifierIndex);
    expect(publishedCheckIndex).toBeGreaterThan(publishIndex);
    expect(workflow).toContain('gh api "repos/${RELEASE_REPOSITORY}/releases/tags/${RELEASE_TAG}" --jq .draft');
  });

  it("accepts a matching draft and returns its positive release ID", () => {
    const result = validateLookup({ tagName: tag, isDraft: true, databaseId: 123456 });

    expect(result.status).toBe(0);
    expect(result.stdout).toBe("123456");
  });

  it.each([
    ["missing release", {}],
    ["non-draft release", { tagName: tag, isDraft: false, databaseId: 123456 }],
    ["wrong tag", { tagName: "v0.9.80", isDraft: true, databaseId: 123456 }],
    ["malformed ID", { tagName: tag, isDraft: true, databaseId: "123abc" }],
  ])("rejects %s lookup response", (_name, response) => {
    const result = validateLookup(response);

    expect(result.status).not.toBe(0);
    expect(result.stdout).toBe("");
  });

  it("rejects malformed JSON from the mocked CLI", () => {
    const result = validateLookup("not json");

    expect(result.status).not.toBe(0);
    expect(result.stdout).toBe("");
  });
});
