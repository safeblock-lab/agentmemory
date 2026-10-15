import { describe, expect, it, vi } from "vitest";
import {
  groundSummarySourcePaths, summarySourceModifiedPathLiterals, summarySourcePathLiterals,
} from "../src/functions/summary-source-grounding.js";
import type { SummaryPathSource } from "../src/functions/summary-source-grounding.js";
import { resolveSummaryUnitPathAuthority } from "../src/functions/summary-queue.js";
import { registerSummaryQueueFunctions } from "../src/functions/summary-queue.js";
import { fingerprintId, KV } from "../src/state/schema.js";
import { formatSummaryObservation } from "../src/prompts/summary.js";

const sourceOracle: SummaryPathSource[] = [
  { narrative: "Added 107 to the SDK types and shop-pos-api errorCodes.ts, shipped SDK v0.1.9." },
  { narrative: "Extension capture preserves failed windows and starts successors only after prior chunks settle." },
  { narrative: "Implemented offline inventory snapshot in feat/offline-product-inventory-snapshot. Only readCache.ts remained as an uncommitted formatting change at session end." },
  { narrative: "# Context from IDE setup\n## Open tabs:\n- notification.mjs\n- post-commit.mjs\n- openai-deepseek-thinking.test.ts" },
  { narrative: "Completed per-task dual-LLM routing with localhost-only Ollama." },
];

const mapText = [
  `Title: Comercia SDK Fixes & Extension Recovery Enhancements
Narrative: The session addressed two major areas: first, a critical Comercia 107 transaction result was added to the SDK, the WS reader discovery was improved for desktop Windows Service integration, and the production deployment of the shop‑pos‑api was successfully rolled out; second, the extension‑product‑recovery workflow was extended with a revisioned workspace API, a lifecycle sweep job, and durable 15‑second MediaRecorder capture windows, all validated by extensive contract tests and Prisma checks. Both initiatives were completed with comprehensive testing, retry logic, and recovery guarantees, ensuring robust payment processing and recording reliability.
Decisions:
  - Added Comercia transaction result code 107 to SDK and shop‑pos‑api errorCodes, parallel REST/Windows Service discovery for WS Comercia readers, and switched GitHub Actions release matrix to windows‑latest.
  - Implemented revisioned workspace API, lifecycle sweep job, and durable 15‑second WebM capture windows with retry and recovery paths for extension product recovery.
Files:

Concepts: ComerciaTransactionResultCode 107 issuer decline, WS Comercia Windows Service payment reader discovery, register cash openingAmountMinor minor units, Pendiente de conciliación refund reconciliation, private GitHub SDK install SDK_REPOSITORY_TOKEN extraheader, git bundle production deployment pscp, windows-only release matrix Tauri, subagent-driven-development review-fix rounds, workspace projection, lifecycle reconciliation of stale LIVE meetings, MediaRecorder independently transcodable 15s WebM windows, terminal failure / failedSequence semantics, bounded scheduler cadence config (60s-3600s), MeetingRecordAttendee clientRecordId backfill migration, contract tests and Prisma validate/generate verification, privacy-safe manager-only attendee identifiers, durable-successor window scheduling, recoverFailedCapture / CAPTURE_TERMINAL_RECOVERY / REMOTE_ABANDON_ONLY_RECOVERY, recoveryPending / recoveryRemoteAcknowledged recovery state, persistWindow Blob retry (3 attempts), recording preflight recovery contract testing`,
  `Title: Offline Inventory Snapshot & Dual-LLM Routing Implementation
Narrative: The session resolved a critical offline inventory bug by implementing a progressive snapshot system for product stock data, with staged/published durable states and realtime refresh. Concurrently, a dual-LLM routing architecture was completed, enabling dynamic task-based routing between primary/auxiliary models (including Ollama) with security restrictions and runtime overrides. Both features passed validation and were committed to dedicated branches.
Decisions:
  - Implemented progressive offline inventory snapshot with staged/published states and realtime refresh via TDD, resolving Products page loading bug
  - Adopted per-task dual-LLM routing (not fixed primary/auxiliary split) with explicit runtime overrides and Ollama-native local endpoint support
  - Restricted Ollama API calls to localhost:11434 for security while enabling flexible provider configuration
Files:
  - feat/offline-product-inventory-snapshot/readCache.ts
  - src/llm/routing/dual-llm-routing.ts
  - src/llm/config/AuxiliaryLlmConfig.ts
  - src/llm/utils/isLocalOllamaUrl.ts
  - tests/llm/routing.test.ts
Concepts: Progressive offline inventory snapshot, Dual-LLM task-based routing, Ollama-native local endpoint validation, Durable staged/published state pattern, Runtime LLM routing overrides`,
];

function parseCapturedPartial(text: string) {
  const result = { title: "", narrative: "", keyDecisions: [] as string[], filesModified: [] as string[], concepts: [] as string[] };
  let section: "decisions" | "files" | "concepts" | undefined;
  for (const line of text.split("\n")) {
    if (line.startsWith("Title: ")) { result.title = line.slice(7); section = undefined; }
    else if (line.startsWith("Narrative: ")) { result.narrative = line.slice(11); section = undefined; }
    else if (line === "Decisions:") section = "decisions";
    else if (line === "Files:") section = "files";
    else if (line.startsWith("Concepts: ")) { result.concepts = line.slice(10).split(", "); section = "concepts"; }
    else if (line.startsWith("  - ") && section === "decisions") result.keyDecisions.push(line.slice(4));
    else if (line.startsWith("  - ") && section === "files") result.filesModified.push(line.slice(4));
  }
  return result;
}

describe("summary source path grounding", () => {
  it("grounds the retained real map outputs without losing their supported decisions", () => {
    const sourcePaths = summarySourcePathLiterals(sourceOracle);
    const modifiedPaths = summarySourceModifiedPathLiterals(sourceOracle);
    expect(modifiedPaths).toEqual(sourcePaths);

    const [first, second] = mapText.map(parseCapturedPartial).map(summary =>
      groundSummarySourcePaths(summary, sourcePaths, modifiedPaths));
    expect(first.keyDecisions[0]).toContain("result code 107");
    expect(first.keyDecisions[1].replace(/[\u2010-\u2015]/g, "-")).toContain("15-second WebM capture windows");
    expect(first.filesModified).toEqual([]);
    expect(second.keyDecisions).toHaveLength(3);
    expect(second.keyDecisions.join(" ")).toContain("staged/published states");
    expect(second.keyDecisions.join(" ")).toContain("localhost:11434");
    expect(second.filesModified).toEqual([]);
  });

  it("grounds the retained real stored reduce result and strips unsupported prose paths", () => {
    const reduced = {
      title: "Comercia SDK Fixes, Extension Recovery, Offline Inventory and Dual-LLM Routing Enhancements",
      narrative: "The session tackled a range of critical improvements: it added a Comercia transaction result code 107, enhanced WS reader discovery for desktop services, and released the shop-pos-api deployment; it extended the extension-product-recovery workflow with a revisioned workspace API, lifecycle sweep job, and durable 15-second MediaRecorder capture windows; it fixed an offline inventory bug by introducing a progressive snapshot system with staged/published states and real-time refresh; and it implemented a flexible dual-LLM routing architecture with runtime overrides and secure Ollama local endpoints. All features were thoroughly tested through contract tests, Prisma checks, and TDD, and then committed to their respective branches for production readiness.",
      keyDecisions: [
        "Added Comercia transaction result code 107 to SDK and shop‑pos‑api errorCodes, improved WS Comercia reader discovery, and switched GitHub Actions release matrix to windows‑latest.",
        "Implemented revisioned workspace API, lifecycle sweep job, and durable 15‑second WebM capture windows with retry and recovery paths for extension product recovery.",
        "Implemented progressive offline inventory snapshot with staged/published states and realtime refresh via TDD, resolving Products page loading bug.",
        "Adopted per-task dual-LLM routing with explicit runtime overrides and Ollama-native local endpoint support, restricting Ollama API calls to localhost:11434 for security.",
      ],
      filesModified: [
        "feat/offline-product-inventory-snapshot/readCache.ts",
        "src/llm/routing/dual-llm-routing.ts",
        "src/llm/config/AuxiliaryLlmConfig.ts",
        "src/llm/utils/isLocalOllamaUrl.ts",
        "tests/llm/routing.test.ts",
      ],
      concepts: [
        "ComerciaTransactionResultCode 107 issuer decline", "WS Comercia Windows Service payment reader discovery",
        "register cash openingAmountMinor minor units", "Pendiente de conciliación refund reconciliation",
        "private GitHub SDK install SDK_REPOSITORY_TOKEN extraheader", "git bundle production deployment pscp",
        "windows-only release matrix Tauri", "subagent-driven-development review-fix rounds", "workspace projection",
        "lifecycle reconciliation of stale LIVE meetings", "MediaRecorder independently transcodable 15s WebM windows",
        "terminal failure / failedSequence semantics", "bounded scheduler cadence config (60s-3600s)",
        "MeetingRecordAttendee clientRecordId backfill migration", "contract tests and Prisma validate/generate verification",
        "privacy-safe manager-only attendee identifiers", "durable-successor window scheduling",
        "recoverFailedCapture / CAPTURE_TERMINAL_RECOVERY / REMOTE_ABANDON_ONLY_RECOVERY",
        "recoveryPending / recoveryRemoteAcknowledged recovery state", "persistWindow Blob retry (3 attempts)",
        "recording preflight recovery contract testing", "Progressive offline inventory snapshot",
        "Dual-LLM task-based routing", "Ollama-native local endpoint validation",
        "Durable staged/published state pattern", "Runtime LLM routing overrides",
      ],
    };
    const grounded = groundSummarySourcePaths(reduced, summarySourcePathLiterals(sourceOracle),
      summarySourceModifiedPathLiterals(sourceOracle));
    expect(grounded.filesModified).toEqual([]);
    expect(grounded.keyDecisions).toEqual(reduced.keyDecisions);
    expect(grounded.narrative).toBe(reduced.narrative);
    const retainedFacts = [...grounded.keyDecisions, ...grounded.concepts, grounded.narrative].join(" ")
      .replace(/[\u2010-\u2015]/g, "-");
    expect(retainedFacts).toContain("result code 107");
    expect(retainedFacts).toContain("staged/published states");
    expect(retainedFacts).toContain("per-task dual-LLM routing");
    expect(retainedFacts).toContain("localhost:11434");
    expect(retainedFacts).toContain("15s WebM windows");
    expect(retainedFacts).toContain("durable-successor window scheduling");

    const proseClaim = groundSummarySourcePaths({ ...reduced, narrative: "Kept the warmup behavior while editing src/llm/config/AuxiliaryLlmConfig.ts." },
      summarySourcePathLiterals(sourceOracle), summarySourceModifiedPathLiterals(sourceOracle));
    expect(proseClaim.narrative).not.toContain("AuxiliaryLlmConfig.ts");
    expect(proseClaim.narrative).toContain("Kept the warmup behavior");
  });

  it("keeps exact file mentions for semantic review and rejects inferred basenames", () => {
    const sources: SummaryPathSource[] = [
      { narrative: "The IDE had notification.mjs open." },
      { files: ["src/one/index.ts", "src/two/index.ts"] },
    ];
    const grounded = groundSummarySourcePaths({
      title: "Summary", narrative: "The editor displayed notification.mjs and index.ts.",
      keyDecisions: [], filesModified: ["notification.mjs", "another/index.ts"], concepts: [],
    }, summarySourcePathLiterals(sources), summarySourceModifiedPathLiterals(sources));
    expect(grounded.filesModified).toEqual(["notification.mjs"]);
    expect(grounded.narrative).toContain("notification.mjs");
    expect(grounded.narrative).not.toContain("index.ts");
  });
  it("never infers modification from English cue words or unique basenames", () => {
    const sources = [{ narrative: "Open src/cache.ts. Archivo leído, no modificado: src/read.ts." }];
    const paths = summarySourcePathLiterals(sources);
    expect(summarySourceModifiedPathLiterals(sources)).toEqual(paths);
    const output = groundSummarySourcePaths({ title: "Work", narrative: "Read src/cache.ts.",
      keyDecisions: [], concepts: [], filesModified: ["cache.ts", "src/cache.ts", "src/read.ts"] }, paths, []);
    expect(output.filesModified).toEqual(["src/cache.ts", "src/read.ts"]);
  });

  it("matches full extensions and preserves absolute source paths without accepting extension prefixes", () => {
    const sources: SummaryPathSource[] = [{ filesModified: [
      "/workspace/ui/Panel.tsx", "C:\\agentmemory\\src\\Route.jsx", "docs/guide.mdx", "state/snapshot.sqlite3",
    ] }];
    const sourcePaths = summarySourcePathLiterals(sources);
    const modifiedPaths = summarySourceModifiedPathLiterals(sources);
    expect(sourcePaths).toEqual(sources[0].filesModified);
    const grounded = groundSummarySourcePaths({
      title: "Summary", narrative: "Updated /workspace/ui/Panel.tsx and docs/guide.mdx.", keyDecisions: [],
      filesModified: [
        "/workspace/ui/Panel.tsx", "C:\\agentmemory\\src\\Route.jsx", "docs/guide.mdx", "state/snapshot.sqlite3",
        "/workspace/ui/Panel.tsx.backup",
      ], concepts: [],
    }, sourcePaths, modifiedPaths);
    expect(grounded.filesModified).toEqual(sources[0].filesModified);
    expect(grounded.narrative).toContain("/workspace/ui/Panel.tsx");
    expect(grounded.narrative).toContain("docs/guide.mdx");
    expect(grounded.narrative).not.toContain("Panel.tsx.backup");
  });

  it("does not interpret decimal and version suffixes as dotfiles", () => {
    const sources = [{ narrative: "The rate is 0.9 and release 1.2.3 completed." }];
    expect(summarySourcePathLiterals(sources)).toEqual([]);
    expect(summarySourcePathLiterals([{ narrative: "The literal config path is .env.local." }]))
      .toEqual([".env.local"]);
  });

  it("reconstructs legacy reduce path authority only from a bounded matching source snapshot", async () => {
    const sessionId = "legacy-session";
    const project = "legacy-project";
    const observation = {
      id: "obs-1", sessionId, timestamp: "2026-10-01T00:00:00.000Z", type: "file_edit" as const,
      title: "Offline inventory change", facts: [],
      narrative: "Only readCache.ts remained as an uncommitted formatting change.",
      concepts: [], files: [], importance: 5,
    };
    const capturedItem = { text: formatSummaryObservation(observation, 1), obsRangeStart: 1, obsRangeEnd: 1 };
    const fingerprint = fingerprintId("sq", JSON.stringify([sessionId, project, [capturedItem]]));
    let pageCalls = 0;
    const kv = {
      async *pages() {
        pageCalls++;
        yield { items: [observation] };
      },
    };
    const job = { sessionId, project, observationCount: 1, snapshotFingerprint: fingerprint };
    const legacyReduceUnit = {
      stage: "reduce" as const,
      items: [{ text: "Title: Generated partial\nFiles:\n  - fake/path/readCache.ts", obsRangeStart: 1, obsRangeEnd: 1 }],
    };

    const authority = await resolveSummaryUnitPathAuthority(kv as never, job, legacyReduceUnit);
    expect(authority.modifiedPaths).toEqual([]);
    expect(authority.sourcePaths).toContain("readCache.ts");
    const literalEdit = groundSummarySourcePaths({ title: "Current edit", narrative: "Updated readCache.ts.",
      keyDecisions: [], filesModified: ["readCache.ts"], concepts: [] }, authority.sourcePaths, authority.modifiedPaths);
    expect(literalEdit.filesModified).toEqual(["readCache.ts"]);

    await expect(resolveSummaryUnitPathAuthority(kv as never, { ...job, snapshotFingerprint: "stale" }, legacyReduceUnit))
      .rejects.toMatchObject({ message: "summary_source_grounding_snapshot_unavailable" });
    await expect(resolveSummaryUnitPathAuthority(kv as never, { ...job, observationCount: 257 }, legacyReduceUnit))
      .rejects.toMatchObject({ message: "summary_source_grounding_snapshot_limit_exceeded" });
    expect(pageCalls).toBe(2);
  });

  it("rejects a stale legacy reduce snapshot before any provider call", async () => {
    const rows = new Map<string, Map<string, unknown>>();
    const get = async <T>(scope: string, key: string): Promise<T | null> => (rows.get(scope)?.get(key) as T) ?? null;
    const set = async <T>(scope: string, key: string, value: T): Promise<T> => {
      if (!rows.has(scope)) rows.set(scope, new Map());
      rows.get(scope)!.set(key, value);
      return value;
    };
    const observation = {
      id: "obs-1", sessionId: "legacy-session", timestamp: "2026-10-01T00:00:00.000Z", type: "file_edit" as const,
      title: "Offline inventory change", facts: [], narrative: "Only readCache.ts changed.",
      concepts: [], files: [], importance: 5,
    };
    const kv = {
      get, set,
      async delete(scope: string, key: string) { rows.get(scope)?.delete(key); },
      async list<T>(scope: string) { return [...(rows.get(scope)?.values() ?? [])] as T[]; },
      async *pages<T>(scope: string) {
        yield { items: [...(rows.get(scope)?.values() ?? [])] as T[] };
      },
      async update() { return { errors: [] }; },
    };
    const jobId = "legacy-job";
    const unitId = "legacy-unit";
    await set(KV.summaryQueueJobs, jobId, {
      id: jobId, sessionId: observation.sessionId, project: "legacy-project", snapshotFingerprint: "stale",
      observationCount: 1, config: { contextTokens: 4096, outputTokens: 512, safetyMarginTokens: 256,
        maxCallInputBytes: 3328, chunkSize: 400, concurrency: 2 },
      createdAt: observation.timestamp, updatedAt: observation.timestamp, status: "pending", stage: "reduce",
      round: 1, unitIds: [unitId], sourceProgressSize: 0,
    });
    await set(KV.summaryQueueUnits(jobId), unitId, {
      id: unitId, jobId, stage: "reduce", round: 1,
      items: [{ text: "Title: Legacy partial", obsRangeStart: 1, obsRangeEnd: 1 }], attempts: 0,
    });
    await set(KV.observations(observation.sessionId), observation.id, observation);
    const handlers = new Map<string, (payload: never) => Promise<unknown>>();
    const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async () => "<summary/>") };
    const sdk = {
      registerFunction(id: string, handler: (payload: never) => Promise<unknown>) { handlers.set(id, handler); },
      registerTrigger() {},
      async trigger() { return {}; },
    };
    registerSummaryQueueFunctions(sdk as never, kv as never, provider as never);

    await expect(handlers.get("mem::summary-unit")!({ jobId, unitId } as never))
      .resolves.toMatchObject({ success: false, error: "summary_source_grounding_snapshot_unavailable", terminal: true });
    expect(provider.summarize).not.toHaveBeenCalled();
  });
});
