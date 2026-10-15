import type { CompressedObservation, Session, SessionSummary } from "../types.js";
import type { StateKV } from "../state/kv.js";
import { fingerprintId, KV } from "../state/schema.js";
import { formatSummaryObservation } from "../prompts/summary.js";
import { SummaryBudgetError, MAX_SUMMARY_ITEMS } from "./summary-budget.js";
import { preparePublicationEvidence, type PublicationEvidenceSource } from "./publication-evidence.js";

export interface SummaryPathSource {
  title?: string;
  narrative?: string;
  facts?: readonly string[];
  keyDecisions?: readonly string[];
  files?: readonly string[];
  filesModified?: readonly string[];
  concepts?: readonly string[];
}

const SOURCE_PATH_PREFIX = String.raw`(?:[A-Za-z]:[\\/]|[\\/]{2}[^\\/]+[\\/][^\\/]+[\\/]|[\\/])?`;
const SOURCE_DOTFILE = String.raw`\.[\p{L}\w@+-]+(?:\.[\p{L}\w@+-]+)*`;
const SOURCE_PATH_EXTENSION = String.raw`(?:tsx|jsx|mdx|sqlite3|mjs|cjs|rs|json|yaml|yml|toml|sql|java|kt|cs|php|html|scss|svelte|ps1|xml|txt|lock|wasm|sqlite|vue|sh|bat|css|md|ts|js|py|go|db)`;
const SOURCE_PATH_FILE = String.raw`(?:[\p{L}\w@+.-]+\.${SOURCE_PATH_EXTENSION}|${SOURCE_DOTFILE})`;
const SOURCE_PATH_SEGMENTS = String.raw`(?:[\p{L}\w@+.-]+[\\/])*${SOURCE_PATH_FILE}`;
const SOURCE_FILE_PATH = new RegExp(`^${SOURCE_PATH_PREFIX}${SOURCE_PATH_SEGMENTS}$`, "iu");
const SOURCE_FILE_PATHS = new RegExp(`(?<![\\p{L}\\p{N}_-])(?:${SOURCE_PATH_PREFIX}${SOURCE_PATH_SEGMENTS})(?![\\p{L}\\p{N}_-]|\\.[\\p{L}\\p{N}_-])`, "giu");

function normalizePath(path: string): string {
  return path.trim().replace(/^['"`]+|['"`),.;:!?]+$/g, "").replace(/\\/g, "/").replace(/^\.\//, "").toLowerCase();
}

function pathTokens(value: string): string[] {
  return [...value.matchAll(SOURCE_FILE_PATHS)].map(match => match[0]);
}

function sourceTexts(source: SummaryPathSource): string[] {
  return [source.title, source.narrative, ...(source.facts ?? []), ...(source.keyDecisions ?? []), ...(source.concepts ?? [])]
    .filter((text): text is string => Boolean(text));
}

export function summarySourcePathLiterals(sources: readonly SummaryPathSource[]): string[] {
  const paths = new Map<string, string>();
  const add = (candidate: string) => {
    const literal = candidate.trim().replace(/^['"`]+|['"`),.;:!?]+$/g, "");
    if (SOURCE_FILE_PATH.test(literal)) paths.set(normalizePath(literal), literal);
  };

  for (const source of sources) {
    for (const path of [...(source.files ?? []), ...(source.filesModified ?? [])]) add(path);
    for (const text of sourceTexts(source)) pathTokens(text).forEach(add);
  }
  return [...paths.values()];
}

export function summarySourceModifiedPathLiterals(sources: readonly SummaryPathSource[]): string[] {
  return summarySourcePathLiterals(sources);
}

function canonicalPath(candidate: string, sourcePaths: readonly string[]): string | undefined {
  const path = candidate.trim().replace(/^['"`]+|['"`),.;:!?]+$/g, "");
  if (!SOURCE_FILE_PATH.test(path)) return undefined;
  const normalized = normalizePath(path);
  return sourcePaths.find(source => normalizePath(source) === normalized);
}

function cleanText(text: string): string {
  return text
    .replace(/\s+([,.;:!?])/g, "$1")
    .replace(/([([{])\s*([)\]}])/g, "")
    .replace(/\s*([,;:])\s*(?=[,;:.!?]|$)/g, "")
    .replace(/\s+(?:in|at|under|within|from|to)\s*(?=[,.;:!?]|$)/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function groundText(text: string, sourcePaths: readonly string[]): string {
  const grounded = text.replace(SOURCE_FILE_PATHS, candidate => canonicalPath(candidate, sourcePaths) ?? "");
  return cleanText(grounded);
}

export function groundSummarySourcePaths<T extends Pick<SessionSummary, "title" | "narrative" | "keyDecisions" | "filesModified" | "concepts">>(
  summary: T,
  sourcePaths: readonly string[],
  _modifiedPaths: readonly string[] = sourcePaths,
): T {
  const filesModified = [...new Set(summary.filesModified
    .map(path => canonicalPath(path, sourcePaths))
    .filter((path): path is string => Boolean(path)))];
  return {
    ...summary,
    title: groundText(summary.title, sourcePaths),
    narrative: groundText(summary.narrative, sourcePaths),
    keyDecisions: summary.keyDecisions.map(decision => groundText(decision, sourcePaths)).filter(Boolean),
    filesModified,
    concepts: summary.concepts.map(concept => groundText(concept, sourcePaths)).filter(Boolean),
  };
}

export interface SummarySourceReference {
  id: string; version: string; hash: string; ordinal: number;
}
export interface SummaryEvidenceRoot { count: number; pages: number; mode: "complete" | "hierarchical" }
export interface SummaryEvidenceJob { id: string; sessionId: string; project: string; observationCount: number }
const referenceKey = (jobId: string, page: number): string => `${jobId}:source-page:${page}`;
const rootKey = (jobId: string): string => `${jobId}:source-root`;
export const summarySourceHash = (value: CompressedObservation): string => fingerprintId("summary-source", JSON.stringify(value));

export async function captureSummaryEvidence(
  kv: StateKV, job: SummaryEvidenceJob, observations: CompressedObservation[],
): Promise<SummaryEvidenceRoot> {
  if (observations.length !== job.observationCount || observations.length > MAX_SUMMARY_ITEMS)
    throw new SummaryBudgetError("summary_item_limit_exceeded");
  let complete: PublicationEvidenceSource[] | undefined = [];
  let referencePage: SummarySourceReference[] = [];
  let pages = 0;
  for (let index = 0; index < observations.length; index++) {
    const observation = observations[index];
    const row = await kv.getVersioned<CompressedObservation>(KV.observations(job.sessionId), observation.id);
    if (!row.exists || !row.value || observation.sessionId !== job.sessionId ||
        summarySourceHash(row.value) !== summarySourceHash(observation)) throw new SummaryBudgetError("summary_source_changed");
    referencePage.push({ id: observation.id, version: row.version, hash: summarySourceHash(observation), ordinal: index + 1 });
    if (referencePage.length === 12 || index === observations.length - 1) {
      if (Buffer.byteLength(JSON.stringify(referencePage), "utf8") > 16_384)
        throw new SummaryBudgetError("summary_source_reference_limit_exceeded");
      await kv.set(KV.state, referenceKey(job.id, pages++), referencePage);
      referencePage = [];
    }
    if (complete) {
      complete.push({ id: observation.id, scope: KV.observations(job.sessionId), key: observation.id,
        version: row.version, project: job.project, sessionId: job.sessionId, content: JSON.stringify(observation) });
      const probe = preparePublicationEvidence({ workflow: "summary", operationId: job.id, project: job.project,
        candidate: "\\".repeat(32_768), sources: complete, evidenceComplete: true });
      if ("status" in probe) complete = undefined;
    }
  }
  const root: SummaryEvidenceRoot = { count: observations.length, pages, mode: complete ? "complete" : "hierarchical" };
  await kv.set(KV.state, rootKey(job.id), root);
  return root;
}

export async function getSummaryEvidenceRoot(kv: StateKV, job: SummaryEvidenceJob): Promise<SummaryEvidenceRoot | null> {
  const root = await kv.get<SummaryEvidenceRoot>(KV.state, rootKey(job.id)) ?? null;
  if (root && (!Number.isSafeInteger(root.count) || root.count < 1 || root.count > MAX_SUMMARY_ITEMS ||
    root.pages !== Math.ceil(root.count / 12) || !["complete", "hierarchical"].includes(root.mode)))
    throw new SummaryBudgetError("summary_source_snapshot_invalid");
  return root;
}

export async function readSummaryEvidenceSource(
  kv: StateKV, job: SummaryEvidenceJob, ordinal: number,
): Promise<{ reference: SummarySourceReference; observation: CompressedObservation; text: string }> {
  const page = await kv.get<SummarySourceReference[]>(KV.state, referenceKey(job.id, Math.floor((ordinal - 1) / 12)));
  const reference = page?.find(item => item.ordinal === ordinal);
  if (!reference) throw new SummaryBudgetError("summary_source_changed");
  const row = await kv.getVersioned<CompressedObservation>(KV.observations(job.sessionId), reference.id);
  if (!row.exists || !row.value || row.version !== reference.version || row.value.sessionId !== job.sessionId ||
      summarySourceHash(row.value) !== reference.hash) throw new SummaryBudgetError("summary_source_changed");
  return { reference, observation: row.value, text: formatSummaryObservation(row.value, ordinal) };
}

export async function verifySummaryEvidence(
  kv: StateKV, job: SummaryEvidenceJob, root: SummaryEvidenceRoot,
): Promise<PublicationEvidenceSource[]> {
  const session = await kv.get<Session>(KV.sessions, job.sessionId);
  if (!session || session.project !== job.project || root.count !== job.observationCount)
    throw new SummaryBudgetError("summary_source_changed");
  const expected = new Set<string>();
  const complete: PublicationEvidenceSource[] = [];
  for (let ordinal = 1; ordinal <= root.count; ordinal++) {
    const { reference, observation } = await readSummaryEvidenceSource(kv, job, ordinal);
    expected.add(reference.id);
    if (root.mode === "complete") complete.push({ id: reference.id, scope: KV.observations(job.sessionId), key: reference.id,
      version: reference.version, project: job.project, sessionId: job.sessionId, content: JSON.stringify(observation) });
  }
  let count = 0;
  for await (const page of kv.pages<CompressedObservation>(KV.observations(job.sessionId), { limit: 12, maxBytes: 16_384 })) {
    for (const observation of page.items) if (observation.title) {
      if (++count > root.count || !expected.has(observation.id)) throw new SummaryBudgetError("summary_source_changed");
    }
  }
  if (count !== root.count) throw new SummaryBudgetError("summary_source_changed");
  return complete;
}
