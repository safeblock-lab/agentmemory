import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, open, readFile, readdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { promisify } from 'node:util';
import { GraphRetrieval } from '../src/functions/graph-retrieval.js';
import { GraphJobStorage } from '../src/functions/graph-job-storage.js';
import { IndexedLocalEmbedding } from '../src/state/indexed-embedding.js';
import { prepareIndexedCorpus } from '../src/state/indexed-preparation.js';
import { getIndexedVector, configureCandidateSemanticProbes, configuredSemanticProbes } from '../src/state/indexed-vector.js';
import { HybridSearch } from '../src/state/hybrid-search.js';
import { loadReranker, configureCandidateReranker, rerankerConfiguration, type RerankerConfiguration } from '../src/state/reranker-runtime.js';
import { SearchIndex } from '../src/state/search-index.js';
import { KV } from '../src/state/schema.js';
import { StateKV } from '../src/state/kv.js';
import type { CompressedObservation, GraphControlState, GraphEdge, GraphNode, GraphSnapshot, HybridSearchResult } from '../src/types.js';
import { startNative } from '../.native-pagination-build/local-indexed-verification/native-host.ts';

type Split = 'calibration' | 'heldout';
type Document = CompressedObservation & { split: Split; stratum: string; group: string };
type Query = { id: string; split: Split; stratum: string; group: string; query: string; intent: string };
type Judgement = { queryId: string; split: Split; relevant: Array<{ documentId: string; grade: number; rationale: string }>; hardNegatives: Array<{ documentId: string; grade: number; rationale: string }> };
type GraphFixture = { split: Split; projectName: string; foreignName: string; asOfBefore: string; asOfAfter: string; nodes: GraphNode[]; edges: GraphEdge[] };
type IndexedStatus = { semantic?: Array<{ index_id: string; count: string }>; graph?: Array<{ scope: string; status: string }> };
type Reranker = { score(query: string, document: string): Promise<number> };
type NativeGate = { accepted: boolean; executable: string; sha256: string };
type ResourceEvidence = {
  expectedProcesses: number;
  completedProcesses: number;
  peakJointRssBytes: number | null;
  samplerPerformanceTargetsPassed: boolean | null;
  failures: string[];
};
type Freeze = {
  files: Record<string, string>;
  models: { assetManifestSha256: string };
  thresholds: {
    annRecallAt50Overall: number;
    annRecallAt50ByStratum: number;
    finalRecallAt10ByStratum: number;
    maxNdcgAt10DropFromSameModelExhaustive: number;
    ordinaryQueryMaxMsExclusive: number;
    combinedSampledRssMaxBytes: number;
  };
};

const repositoryRoot = resolve(process.cwd());
const dataRoot = resolve(repositoryRoot, 'eval/local-indexed');
const cacheRoot = resolve(repositoryRoot, '.native-pagination-build/local-indexed-verification');
export const candidateEvidenceRoot = resolve(repositoryRoot, '.native-pagination-build/semantic-quality-correction/combined-bge256');
const candidateSidecarPath = resolve(dataRoot, 'candidate-model.json');
type CandidateSidecar = {
  schemaVersion: 1; freezeSha256: string; baselineAssetManifestSha256: string;
  embedding: { id: string; revision: string; dimensions: number; dtype: string };
  native: { executable: string; sha256: string; probes: 256; sourcePins: Record<string, string> };
  runtime: RerankerConfiguration;
  assets: Record<string, string>; provisionManifest: { path: string; sha256: string };
  limits: { cpuThreads: 1; maxTokens: 512; candidateWindow: 50; pendingBatches: 8; maxPairs: 5616 };
};
async function fileHash(path: string): Promise<string> {
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(path)) digest.update(chunk);
  return digest.digest('hex');
}
export async function verifyCandidatePins(executable: string, requireInferenceAcceptance = true): Promise<{ sidecar: CandidateSidecar; sha256: string; runtimeSha256: string }> {
  const freeze = await verifyFrozenFiles();
  const bytes = await readFile(candidateSidecarPath);
  const sidecar = JSON.parse(bytes.toString('utf8')) as CandidateSidecar;
  assert.equal(sidecar.schemaVersion, 1);
  assert.equal(sidecar.freezeSha256, hash(await readFile(resolve(dataRoot, 'freeze.json'))));
  assert.equal(sidecar.baselineAssetManifestSha256, freeze.models.assetManifestSha256);
  assert.deepEqual(sidecar.embedding, (freeze.models as Freeze['models'] & { embedding: unknown }).embedding);
  assert.equal(resolve(sidecar.native.executable), resolve(executable));
  assert.equal(sidecar.native.sha256, 'c7d3a0d17a2d2395e698e10835b261c926baf8d7de0dae36669e8927ef629652');
  assert.equal(hash(await readFile(executable)), sidecar.native.sha256);
  assert.equal(sidecar.native.probes, 256);
  assert.deepEqual(sidecar.limits, { cpuThreads: 1, maxTokens: 512, candidateWindow: 50, pendingBatches: 8, maxPairs: 5616 });
  assert.equal(sidecar.runtime.modelId, 'onnx-community/bge-reranker-v2-m3-ONNX');
  assert.equal(sidecar.runtime.revision, '90213ffc6a8e6f051a6331269a0f5526cdd896f6');
  assert.equal(resolve(sidecar.runtime.modelPath), resolve(repositoryRoot, '.native-pagination-build/multilingual-model-provision'));
  assert.equal(resolve(sidecar.runtime.cacheDirectory), resolve(candidateEvidenceRoot, 'model-cache'));
  assert.equal(resolve(sidecar.provisionManifest.path), resolve(sidecar.runtime.modelPath, 'manifest.json'));
  assert.equal(sidecar.provisionManifest.sha256, '555a0fc9b140681b8abe96727eb154c890a49db86bb41ae398fa39c20d7f8f52');
  assert.equal(hash(await readFile(sidecar.provisionManifest.path)), sidecar.provisionManifest.sha256);
  const manifest = await readJson<{ assets: Array<{ localPath: string; sha256: string; verified: boolean }> }>(sidecar.provisionManifest.path);
  assert.equal(manifest.assets.length, 7);
  assert.equal(Object.keys(sidecar.assets).length, 7);
  assert.equal(sidecar.assets[resolve(sidecar.runtime.modelPath, 'onnx/model_quantized.onnx')], '912fc1215c2dbff6499700534bd8d31253af01573861abbfc43afd1fab6cce5d');
  for (const asset of manifest.assets) {
    assert.equal(asset.verified, true);
    assert.equal(sidecar.assets[resolve(sidecar.runtime.modelPath, asset.localPath)], asset.sha256);
  }
  for (const [path, expected] of Object.entries({ ...sidecar.assets, ...sidecar.native.sourcePins })) assert.equal(await fileHash(path), expected, `Candidate pin changed: ${path}`);
  const baselineAssets = await readJson<{ assets: Array<{ model: string; file: string; sha256: string }> }>(resolve(cacheRoot, 'model-assets.json'));
  for (const asset of baselineAssets.assets) {
    const kind = asset.model === sidecar.embedding.id ? 'embeddings' : 'reranker';
    assert.equal(await fileHash(resolve(cacheRoot, '.cache/agentmemory', kind, asset.model, asset.file)), asset.sha256, 'Frozen baseline model asset changed.');
  }
  const runtime = await runtimeFingerprint();
  const sha256 = hash(bytes);
  if (requireInferenceAcceptance) {
    const acceptance = await readJson<{ nativeSuiteAccepted: boolean; sidecarHarnessAccepted: boolean; candidateSidecarSha256: string; runtimeSha256: string }>(resolve(candidateEvidenceRoot, 'inference-accepted.json'));
    assert.equal(acceptance.nativeSuiteAccepted, true, 'Main must accept the full native suite before candidate loading.');
    assert.equal(acceptance.sidecarHarnessAccepted, true, 'Main must accept the sidecar/harness before candidate loading.');
    assert.equal(acceptance.candidateSidecarSha256, sha256);
    assert.equal(acceptance.runtimeSha256, runtime.sha256);
  }
  return { sidecar, sha256, runtimeSha256: runtime.sha256 };
}
export function configureVerifiedCandidate(sidecar: CandidateSidecar): void {
  configureCandidateReranker(sidecar.runtime);
  configureCandidateSemanticProbes(sidecar.native.probes);
  assert.deepEqual(rerankerConfiguration(), sidecar.runtime);
  assert.equal(configuredSemanticProbes(), sidecar.native.probes);
}
const heldoutEvidenceRoot = resolve(repositoryRoot, '.native-pagination-build/release-heldout-evidence');
const hash = (value: string | Buffer): string => createHash('sha256').update(value).digest('hex');
export function candidateOutputStem(prefix = process.env.AGENTMEMORY_QUALITY_OUTPUT_PREFIX ?? ''): string {
  assert.ok(prefix === '' || prefix === 'queue-fixed', 'Candidate output prefix must be empty or queue-fixed.');
  return prefix ? `${prefix}-` : '';
}
export async function storedVectorHash(
  kv: { get(scope: string, key: string): Promise<unknown | null> },
  documents: ReadonlyArray<{ id: string }>,
): Promise<string> {
  const records: unknown[] = [];
  for (let offset = 0; offset < documents.length; offset += 8) {
    records.push(...await Promise.all(documents.slice(offset, offset + 8)
      .map(document => kv.get(KV.semanticVectors('observations'), document.id))));
  }
  assert.ok(records.every(record => record !== null), 'Stored vectors must cover every calibration document.');
  return hash(JSON.stringify(records));
}
const execFileAsync = promisify(execFile);
const nativeGatePath = resolve(cacheRoot, 'native-accepted.json');
const nativeHostPath = resolve(repositoryRoot, '.native-pagination-build/local-indexed-verification/native-host.ts');
const rssSamplerPath = resolve(repositoryRoot, '.native-pagination-build/semantic-measurement-runtime-repair/rss-sampler.ts');
const rssCollectorPath = resolve(repositoryRoot, '.native-pagination-build/semantic-measurement-runtime-repair/collector.ps1');
const resultsDirectoryFor = (split: Split): string => resolve(split === 'heldout' ? heldoutEvidenceRoot : dataRoot, 'results');
const runtimeDirectoryFor = (split: Split): string => resolve(split === 'heldout' ? heldoutEvidenceRoot : dataRoot, split === 'heldout' ? 'runtime' : '.runtime');
const percentile = (values: number[], quantile: number): number => {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(quantile * sorted.length) - 1)];
};
const distribution = (values: number[]) => ({ count: values.length, p50Ms: percentile(values, 0.5), p95Ms: percentile(values, 0.95), maxMs: Math.max(0, ...values) });
const positiveFinite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value > 0;
const readJson = async <T>(path: string): Promise<T> => JSON.parse(await readFile(path, 'utf8')) as T;
export function parseSplitLines<T>(text: string, split: Split): T[] {
  const selector = new RegExp(`"split"\\s*:\\s*"${split}"`);
  return text.trimEnd().split('\n').filter(line => selector.test(line)).map(line => JSON.parse(line) as T);
}
const readJsonLines = async <T>(path: string, split: Split): Promise<T[]> => parseSplitLines<T>(await readFile(path, 'utf8'), split);
export function parseSplitArray<T>(text: string, split: Split): T[] {
  const selected: T[] = [];
  let depth = 0; let start = -1; let quoted = false; let escaped = false;
  const selector = new RegExp(`"split"\\s*:\\s*"${split}"`);
  for (let index = 0; index < text.length; index++) {
    const character = text[index];
    if (quoted) {
      if (escaped) escaped = false;
      else if (character === '\\') escaped = true;
      else if (character === '"') quoted = false;
      continue;
    }
    if (character === '"') quoted = true;
    else if (character === '{') { if (depth++ === 0) start = index; }
    else if (character === '}' && --depth === 0) {
      const record = text.slice(start, index + 1);
      if (selector.test(record)) selected.push(JSON.parse(record) as T);
    }
  }
  assert.equal(depth, 0, 'Fixture array nesting is incomplete.');
  return selected;
}
export function validateResourceEvidence(snapshots: Array<Record<string, unknown>>, expectedProcesses: number): ResourceEvidence {
  const failures: string[] = [];
  const jointPeaks: number[] = [];
  const samplerPerformanceTargets = snapshots.map(snapshot => snapshot.performanceTargetsPassed);
  if (snapshots.length !== expectedProcesses) failures.push(`Expected ${expectedProcesses} native resource snapshots; received ${snapshots.length}.`);
  if (snapshots.length === 0) failures.push('No native resource snapshots were returned.');

  for (const [index, snapshot] of snapshots.entries()) {
    const label = `Native resource snapshot ${index + 1}`;
    const samples = snapshot.samples;
    if (typeof samples !== 'number' || !Number.isInteger(samples) || samples <= 0) failures.push(`${label} has no positive sample count.`);
    for (const field of ['engineSampledPeakRss', 'engineOsPeakRss', 'nodeSampledPeakRss', 'jointSampledPeakRss']) {
      const value = snapshot[field];
      if (!positiveFinite(value)) failures.push(`${label} is missing a positive finite ${field}.`);
      else if (field === 'jointSampledPeakRss') jointPeaks.push(value);
    }
    if (snapshot.failedSamples !== 0) failures.push(`${label} reports missing or failed samples.`);
    if (snapshot.firstError !== undefined && snapshot.firstError !== null) failures.push(`${label} reports a sampler error.`);
    if (typeof snapshot.maxGapMs !== 'number' || !Number.isFinite(snapshot.maxGapMs) || snapshot.maxGapMs < 0 || snapshot.maxGapMs > 5000) {
      failures.push(`${label} has no valid bounded sample-gap evidence.`);
    }
    if (typeof snapshot.skippedSamples !== 'number' || !Number.isInteger(snapshot.skippedSamples) || snapshot.skippedSamples < 0) {
      failures.push(`${label} omits valid skipped-sample diagnostics.`);
    }
    const phases = snapshot.phases;
    if (!Array.isArray(phases) || !['startup', 'work', 'terminal'].every(name => phases.includes(name))) {
      failures.push(`${label} is missing startup/work/terminal sample coverage.`);
    }
    if (snapshot.pidGone !== true || snapshot.listenerGone !== true || snapshot.collectorGone !== true) {
      failures.push(`${label} did not prove owned process and listener cleanup.`);
    }
    if (!Array.isArray(snapshot.cleanupErrors) || snapshot.cleanupErrors.length > 0) failures.push(`${label} reports incomplete cleanup.`);
  }

  return {
    expectedProcesses,
    completedProcesses: snapshots.length,
    peakJointRssBytes: failures.length === 0 ? Math.max(...jointPeaks) : null,
    samplerPerformanceTargetsPassed: samplerPerformanceTargets.length > 0 && samplerPerformanceTargets.every(value => typeof value === 'boolean')
      ? samplerPerformanceTargets.every(value => value === true)
      : null,
    failures,
  };
}
export function assertCandidateResourceContinuation(snapshots: Array<Record<string, unknown>>): void {
  const evidence = validateResourceEvidence(snapshots, snapshots.length);
  assert.equal(evidence.failures.length, 0, `Candidate resource evidence failed: ${evidence.failures.join(' ')}`);
  assert.ok(evidence.peakJointRssBytes !== null && evidence.peakJointRssBytes <= 2 * 1024 ** 3,
    'Candidate joint RSS must be measured and at most 2 GiB before continuing.');
}

export async function runtimeFingerprint(): Promise<{ sha256: string; files: Record<string, string> }> {
  const sourcePaths: string[] = [];
  const visit = async (directory: string): Promise<void> => {
    const entries = (await readdir(directory, { withFileTypes: true })).sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const path = resolve(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (/\.(?:ts|tsx|js|mjs|cjs)$/.test(entry.name)) sourcePaths.push(path);
    }
  };
  await visit(resolve(repositoryRoot, 'src'));
  sourcePaths.push(fileURLToPath(import.meta.url), nativeHostPath, rssSamplerPath, rssCollectorPath, resolve(repositoryRoot, 'package.json'), resolve(repositoryRoot, 'package-lock.json'));
  const candidateFiles = await readdir(candidateEvidenceRoot).catch((error: unknown) => {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [] as string[];
    throw error;
  });
  for (const name of ['preflight.ts', 'run-bounded.ps1']) {
    const path = resolve(candidateEvidenceRoot, name);
    if (candidateFiles.includes(name)) sourcePaths.push(path);
  }
  sourcePaths.sort((left, right) => left.localeCompare(right));
  const files: Record<string, string> = {};
  const entries: string[] = [];
  for (const path of sourcePaths) {
    const relative = path.slice(repositoryRoot.length + 1).replaceAll('\\', '/');
    const digest = hash(await readFile(path));
    files[relative] = digest;
    entries.push(`${relative}\0${digest}\n`);
  }
  return { sha256: hash(entries.join('')), files };
}

async function verifySealedChild(replicate: number, executable: string): Promise<void> {
  assert.equal(process.env.AGENTMEMORY_QUALITY_REPLICATE, String(replicate), 'Replicate id differs from sealed campaign request.');
  assert.match(process.env.AGENTMEMORY_QUALITY_SEAL ?? '', /^[a-f0-9-]{36}$/i, 'Missing sealed campaign token.');
  const freezeDigest = hash(await readFile(resolve(dataRoot, 'freeze.json')));
  const gateDigest = hash(await readFile(nativeGatePath));
  const executableDigest = hash(await readFile(executable));
  const runtime = await runtimeFingerprint();
  assert.equal(freezeDigest, process.env.AGENTMEMORY_QUALITY_FREEZE_SHA256, 'Frozen fixture changed during the sealed campaign.');
  assert.equal(gateDigest, process.env.AGENTMEMORY_QUALITY_NATIVE_GATE_SHA256, 'Accepted native gate changed during the sealed campaign.');
  assert.equal(executableDigest, process.env.AGENTMEMORY_QUALITY_NATIVE_SHA256, 'Accepted native executable changed during the sealed campaign.');
  assert.equal(runtime.sha256, process.env.AGENTMEMORY_QUALITY_RUNTIME_SHA256, 'Application or harness source changed during the sealed campaign.');
}
const recallAt = (ranked: string[], relevant: Set<string>, k: number): number => {
  if (relevant.size === 0) return 1;
  const found = new Set(ranked.slice(0, k));
  return [...relevant].filter(id => found.has(id)).length / relevant.size;
};
const dcgAt10 = (ranked: string[], grades: Map<string, number>): number => ranked.slice(0, 10).reduce((sum, id, index) => sum + ((2 ** (grades.get(id) ?? 0) - 1) / Math.log2(index + 2)), 0);
const ndcgAt10 = (ranked: string[], grades: Map<string, number>): number => {
  const actual = dcgAt10(ranked, grades);
  const ideal = dcgAt10([...grades.entries()].sort((left, right) => right[1] - left[1]).map(([id]) => id), grades);
  return ideal === 0 ? 0 : actual / ideal;
};
const sortEdges = (edges: GraphEdge[]): GraphEdge[] => [...edges].sort((left, right) => left.id.localeCompare(right.id));

async function verifyFrozenFiles() {
  const freeze = await readJson<Freeze>(resolve(dataRoot, 'freeze.json'));
  for (const [name, expected] of Object.entries(freeze.files)) assert.equal(hash(await readFile(resolve(dataRoot, name))), expected, `Frozen quality file changed: ${name}`);
  assert.equal(hash(await readFile(resolve(cacheRoot, 'model-assets.json'))), freeze.models.assetManifestSha256, 'Cached model manifest differs from preregistration.');
  return freeze;
}

function forbidScopeEnumeration(kv: StateKV): void {
  kv.list = async () => { throw new Error('Full state-scope enumeration is prohibited in the quality campaign.'); };
  kv.pages = async function* () { throw new Error('Paged state-scope enumeration is prohibited in the quality campaign.'); };
}

async function seedCanonicalData(kv: StateKV, documents: Document[], fixture: GraphFixture): Promise<void> {
  for (const document of documents) {
    const observation: CompressedObservation = {
      id: document.id,
      sessionId: document.sessionId,
      timestamp: document.timestamp,
      type: document.type,
      title: document.title,
      subtitle: document.subtitle,
      narrative: document.narrative,
      facts: document.facts,
      concepts: document.concepts,
      files: document.files,
      importance: document.importance,
    };
    await kv.set(KV.observations(document.sessionId), document.id, observation);
  }
  const control = (await kv.getVersioned<GraphControlState>(KV.graphControl, 'current')).value;
  assert.ok(control, 'The native SQLite state adapter did not initialize graph transaction control.');
  const lease = await kv.lease({ action: 'acquire', owner_id: randomUUID(), generation: control.generation, ttl_ms: 120_000 });
  assert.ok(!('released' in lease), 'The native graph transaction lease was unexpectedly released.');
  const now = new Date().toISOString();
  const jobId = `quality-${randomUUID()}`;
  const delta = { id: randomUUID(), ordinal: 0, capturedAt: now, attempt: randomUUID(), phase: 'preparing' as const };
  const storage = new GraphJobStorage(kv, lease, jobId);
  try {
    await storage.restore();
    await storage.stage(KV.graphJobs, jobId, {
      version: 1,
      id: jobId,
      generation: control.generation,
      kind: 'extraction',
      state: 'staging',
      createdAt: now,
      updatedAt: now,
      inputCount: 1,
      captureParts: 1,
      captureDigest: '0'.repeat(64),
      captureComplete: true,
    }, delta);
    const graph = storage.facade(delta);
    for (const node of fixture.nodes) await graph.set(KV.graphNodes, node.id, node);
    for (const edge of fixture.edges) await graph.set(KV.graphEdges, edge.id, edge);
    await graph.set(KV.graphSnapshot, 'current', fixtureSnapshot(fixture, now));
    await storage.freeze(delta, { fixture: 'local-indexed-quality' });
    await storage.apply(delta);
  } finally {
    await kv.lease({ action: 'release', owner_id: lease.owner_id, generation: lease.generation, fence: lease.fence });
  }
}

function fixtureSnapshot(fixture: GraphFixture, updatedAt: string): GraphSnapshot {
  const nodes = fixture.nodes.filter((node) => !node.stale);
  const edges = fixture.edges.filter((edge) => !edge.stale);
  const degrees = new Map<string, number>();
  for (const edge of edges) {
    degrees.set(edge.sourceNodeId, (degrees.get(edge.sourceNodeId) ?? 0) + 1);
    degrees.set(edge.targetNodeId, (degrees.get(edge.targetNodeId) ?? 0) + 1);
  }
  const counts = (values: string[]): Record<string, number> => Object.fromEntries(
    [...new Set(values)].sort().map((value) => [value, values.filter((item) => item === value).length]),
  );
  return {
    version: 1,
    topNodes: [...nodes].sort((left, right) => (degrees.get(right.id) ?? 0) - (degrees.get(left.id) ?? 0) || left.id.localeCompare(right.id)),
    topEdges: [...edges].sort((left, right) => right.weight - left.weight || left.id.localeCompare(right.id)),
    topDegrees: Object.fromEntries(nodes.map((node) => [node.id, degrees.get(node.id) ?? 0])),
    stats: {
      totalNodes: nodes.length,
      totalEdges: edges.length,
      nodesByType: counts(nodes.map((node) => node.type)),
      edgesByType: counts(edges.map((edge) => edge.type)),
    },
    updatedAt,
    dirty: false,
  };
}

async function verifyTemporalGraphParity(kv: StateKV, fixture: GraphFixture): Promise<void> {
  const graph = new GraphRetrieval(kv);
  const project = fixture.nodes.find(node => node.name === fixture.projectName);
  const foreign = fixture.nodes.find(node => node.name === fixture.foreignName);
  const oldEdge = fixture.edges.find(edge => edge.id.endsWith('-old'));
  const currentEdge = fixture.edges.find(edge => edge.id.endsWith('-current'));
  const foreignEdge = fixture.edges.find(edge => edge.id.endsWith('-foreign'));
  assert.ok(project && foreign && oldEdge && currentEdge && foreignEdge);
  for (const edge of fixture.edges) assert.deepEqual(await kv.get(KV.graphEdges, edge.id), edge, `Canonical edge changed: ${edge.id}`);

  const before = await graph.temporalQuery(fixture.projectName, fixture.asOfBefore);
  assert.deepEqual(before.entity, project);
  assert.deepEqual(sortEdges(before.currentState), [oldEdge]);
  assert.deepEqual(sortEdges(before.history), [oldEdge]);
  const after = await graph.temporalQuery(fixture.projectName, fixture.asOfAfter);
  assert.deepEqual(after.entity, project);
  assert.deepEqual(sortEdges(after.currentState), [currentEdge]);
  assert.deepEqual(sortEdges(after.history), [currentEdge]);
  const isolated = await graph.temporalQuery(fixture.foreignName, fixture.asOfAfter);
  assert.deepEqual(isolated.entity, foreign);
  assert.deepEqual(sortEdges(isolated.currentState), [foreignEdge]);
  assert.ok(![...after.history, ...after.currentState].some(edge => edge.id === foreignEdge.id), 'Temporal result crossed into an unrelated entity.');
}

function scoreMap(judgement: Judgement): Map<string, number> {
  return new Map(judgement.relevant.map(item => [item.documentId, item.grade]));
}

async function exhaustiveRerank(query: string, documents: Document[], runtime: Reranker): Promise<string[]> {
  const boundedQuery = query.slice(0, 256);
  const scored: Array<{ id: string; score: number; position: number }> = [];
  for (let position = 0; position < documents.length; position++) {
    const document = documents[position];
    const text = `${document.title}\n${document.narrative}`.slice(0, 8192);
    scored.push({ id: document.id, score: await runtime.score(boundedQuery, text), position });
  }
  return scored.sort((left, right) => right.score - left.score || left.position - right.position).map(item => item.id);
}

async function runCampaign(split: Split, executable: string, replicate?: number, expectedCalibrationSha256?: string, candidateMode = false) {
  const campaignStartedAt = new Date().toISOString();
  const phase = (name: string, details: Record<string, unknown> = {}): void => {
    console.error(JSON.stringify({ qualityCampaign: 'local-indexed', split, phase: name, at: new Date().toISOString(), ...details }));
  };
  phase('bootstrap:start', { applicationProcessId: process.pid });
  const freeze = await verifyFrozenFiles();
  phase('bootstrap:frozen-inputs-verified');
  const nativeExecutable = resolve(repositoryRoot, executable);
  const candidate = candidateMode ? await verifyCandidatePins(nativeExecutable) : null;
  if (candidate) {
    assert.equal(split, 'calibration', 'Candidate evaluation is calibration only until package freeze.');
    const outputStem = candidateOutputStem();
    const preflight = await readJson<{ passed: boolean; candidateSidecarSha256: string; runtimeSha256: string }>(resolve(candidateEvidenceRoot, `${outputStem}preflight-result.json`));
    assert.equal(preflight.passed, true, 'Successful synthetic preflight is required before calibration.');
    assert.equal(preflight.candidateSidecarSha256, candidate.sha256);
    assert.equal(preflight.runtimeSha256, candidate.runtimeSha256);
    const claim = await open(resolve(candidateEvidenceRoot, `${outputStem}calibration.claim`), 'wx');
    await claim.writeFile(JSON.stringify({ startedAt: campaignStartedAt, candidateSidecarSha256: candidate.sha256, runtimeSha256: candidate.runtimeSha256 }));
    await claim.close();
    configureVerifiedCandidate(candidate.sidecar);
  }
  const gate = await readJson<NativeGate>(nativeGatePath);
  const executableSha256 = hash(await readFile(nativeExecutable));
  if (split === 'heldout') {
    assert.equal(gate.accepted, true, 'Main native acceptance is required before held-out inference.');
    assert.equal(resolve(gate.executable), nativeExecutable, 'Executable differs from Main accepted native artifact.');
    assert.equal(executableSha256, gate.sha256, 'Accepted native binary hash changed.');
    assert.ok(replicate === 1 || replicate === 2 || replicate === 3, 'Held-out scoring is allowed only as one of the three sealed campaign processes.');
    await verifySealedChild(replicate, nativeExecutable);
  } else {
    assert.equal(replicate, undefined, 'Calibration does not use sealed replicate mode.');
    if (candidate) {
      assert.equal(executableSha256, candidate.sidecar.native.sha256);
    } else if (expectedCalibrationSha256) {
      assert.match(expectedCalibrationSha256, /^[a-f0-9]{64}$/i, 'Calibration requires a SHA-256 pin when using an unaccepted native executable.');
      assert.equal(executableSha256, expectedCalibrationSha256.toLowerCase(), 'Calibration executable differs from the explicitly pinned offline binary.');
    } else {
      assert.equal(gate.accepted, true, 'Main native acceptance or an explicit calibration binary hash is required before inference.');
      assert.equal(resolve(gate.executable), nativeExecutable, 'Executable differs from Main accepted native artifact.');
      assert.equal(executableSha256, gate.sha256, 'Accepted native binary hash changed.');
    }
  }
  const allDocuments = await readJsonLines<Document>(resolve(dataRoot, 'corpus.jsonl'), split);
  const allQueries = await readJsonLines<Query>(resolve(dataRoot, 'queries.jsonl'), split);
  const allJudgements = await readJsonLines<Judgement>(resolve(dataRoot, 'judgements.jsonl'), split);
  const fixtureList = parseSplitArray<GraphFixture>(await readFile(resolve(dataRoot, 'graph-fixtures.json'), 'utf8'), split);
  const documents = allDocuments.filter(document => document.split === split);
  const queries = allQueries.filter(query => query.split === split);
  const judgements = new Map(allJudgements.filter(item => item.split === split).map(item => [item.queryId, item]));
  const fixture = fixtureList.find(item => item.split === split);
  assert.ok(fixture);
  assert.ok(documents.length > 50, 'Search must use all documents in the selected split.');
  assert.equal(new Set(documents.map(document => document.id)).size, documents.length);
  assert.equal(queries.length, split === 'heldout' ? 108 : 54);
  const localIds = new Set(documents.map(document => document.id));
  const queryLatency: number[] = [];
  const perQuery: Array<Record<string, unknown>> = [];
  const nativeMemory: Array<Record<string, unknown>> = [];
  let preparationMs = 0;
  let embeddingColdProbeMs = 0;
  let rerankerColdLoadMs = 0;
  let graphParityChecks = 0;
  const processColdFirstQueries: number[] = [];
  const nativeProcessStarts: number[] = [];
  let exitCode = 0;
  let failure: string | undefined;
  const runId = `${new Date().toISOString().replace(/[:.]/g, '-')}${replicate ? `-r${replicate}` : ''}`;
  const runRoot = resolve(candidate ? resolve(candidateEvidenceRoot, 'runtime') : runtimeDirectoryFor(split), `${split}-${runId}`);
  await mkdir(runRoot, { recursive: true });
  const database = resolve(runRoot, 'quality.sqlite3');
  process.chdir(cacheRoot);
  const provider = new IndexedLocalEmbedding();
  const phaseTimings = { seedMs: 0, indexedPreparationMs: 0, graphParityMs: 0, rerankerLoadMs: 0 };
  let queriesStarted = 0;
  let scoredPairs = 0;
  let candidateVectorHash: string | null = null;

  try {
    process.env.AGENTMEMORY_RETRIEVAL_MODE = 'indexed';
    phase('native-prepare:start', { documents: documents.length, queries: queries.length, executableSha256 });
    const setup = await startNative(nativeExecutable, database, resolve(runRoot, 'prepare'));
    phase('native-prepare:ready', { nativeProcessId: setup.pid });
    try {
      const kv = new StateKV(setup.worker);
      assert.equal(kv.indexedRetrieval, true);
      forbidScopeEnumeration(kv);
      phase('seed:start', { documents: documents.length });
      const seedStart = performance.now();
      await seedCanonicalData(kv, documents, fixture);
      phaseTimings.seedMs = performance.now() - seedStart;
      phase('seed:complete', { documents: documents.length, elapsedMs: phaseTimings.seedMs });
      phase('model-ready-probe:start');
      const coldEmbedStart = performance.now();
      await provider.embed('local indexed quality campaign model readiness probe');
      embeddingColdProbeMs = performance.now() - coldEmbedStart;
      phase('model-ready-probe:complete', { elapsedMs: embeddingColdProbeMs });
      const vector = getIndexedVector(kv, provider);
      const prepareStart = performance.now();
      phase('indexed-preparation:start', { documents: documents.length });
      const preparedCount = await prepareIndexedCorpus(kv, vector, provider);
      preparationMs = performance.now() - prepareStart;
      phaseTimings.indexedPreparationMs = preparationMs;
      phase('indexed-preparation:complete', { documents: preparedCount, elapsedMs: preparationMs });
      assert.equal(preparedCount, documents.length, 'Prepared count must equal the full split corpus.');
      if (candidate) candidateVectorHash = await storedVectorHash(kv, documents);
      const graphParityStart = performance.now();
      phase('graph-parity:start');
      await verifyTemporalGraphParity(kv, fixture);
      graphParityChecks = 3;
      const status = await kv.retrieval<IndexedStatus>({ action: 'index_status' });
      assert.equal(Number(status.semantic?.find(item => item.index_id === 'observations')?.count), documents.length);
      for (const scope of [KV.graphNodes, KV.graphEdges]) assert.equal(status.graph?.find(item => item.scope === scope)?.status, 'ready');
      phaseTimings.graphParityMs = performance.now() - graphParityStart;
      phase('graph-parity:complete', { checks: graphParityChecks, elapsedMs: phaseTimings.graphParityMs });
      const rerankerLoadStart = performance.now();
      phase('reranker-load:start');
      const reranker = await loadReranker();
      rerankerColdLoadMs = performance.now() - rerankerLoadStart;
      phaseTimings.rerankerLoadMs = rerankerColdLoadMs;
      assert.ok(reranker, 'Cached local reranker is unavailable.');
      phase('reranker-load:complete', { elapsedMs: rerankerColdLoadMs });
    } finally {
      nativeMemory.push(await setup.stop());
      if (candidate) assertCandidateResourceContinuation(nativeMemory);
    }

    for (let offset = 0; offset < queries.length; offset += 12) {
      const chunkStarted = performance.now();
      const batch = queries.slice(offset, offset + 12);
      phase('query-process:start', { first: offset + 1, last: offset + batch.length, total: queries.length });
      const nativeStart = performance.now();
      const chunkSignal = candidate ? AbortSignal.timeout(600_000) : undefined;
      const native = await startNative(nativeExecutable, database, resolve(runRoot, `query-${String(offset / 12).padStart(2, '0')}`), chunkSignal);
      nativeProcessStarts.push(performance.now() - nativeStart);
      phase('query-process:ready', { first: offset + 1, last: offset + batch.length, elapsedMs: nativeProcessStarts.at(-1) });
      try {
        const kv = new StateKV(native.worker);
        assert.equal(kv.indexedRetrieval, true);
        forbidScopeEnumeration(kv);
        const vector = getIndexedVector(kv, provider);
        await vector.ready();
        if (candidate) assert.equal(await storedVectorHash(kv, documents), candidateVectorHash, 'Stored vectors changed before candidate scoring.');
        const hybrid = new HybridSearch(new SearchIndex(), null, provider, kv);
        for (const query of batch) {
          if (candidate) {
            assert.ok(performance.now() - chunkStarted < 600_000, 'Candidate chunk exceeded 10 minutes.');
            assert.ok(Date.now() - Date.parse(campaignStartedAt) < 3_600_000, 'Candidate campaign exceeded 60 minutes.');
          }
          queriesStarted += 1;
          phase('query:start', { index: queriesStarted, total: queries.length });
          const judgement = judgements.get(query.id);
          assert.ok(judgement, `Missing frozen judgement: ${query.id}`);
          const relevant = new Set(judgement.relevant.map(item => item.documentId));
          const embedStart = performance.now();
          const queryEmbedding = await provider.embed(query.query);
          const embeddingMs = performance.now() - embedStart;
          const annStart = performance.now();
          const annIds = (await vector.search(queryEmbedding, 50)).map(item => item.obsId);
          const annMs = performance.now() - annStart;
          assert.ok(annIds.every(id => localIds.has(id)), `ANN crossed split corpus for ${query.id}`);
          const searchStart = performance.now();
          if (candidate) phase('hybrid-search:start', { index: queriesStarted });
          const finalRows: HybridSearchResult[] = await hybrid.search(query.query, 10);
          const endToEndMs = performance.now() - searchStart;
          if (candidate) phase('hybrid-search:complete', { index: queriesStarted, endToEndMs });
          assert.ok(endToEndMs < freeze.thresholds.ordinaryQueryMaxMsExclusive, `Ordinary query exceeded 60 seconds: ${query.id}`);
          assert.ok(finalRows.length > 1, `Fused candidate set was too small to exercise final reranking: ${query.id}`);
          assert.ok(finalRows.every(row => 'rerankPosition' in row && Number.isInteger(row.rerankPosition)), `Final local reranker did not rank every returned candidate: ${query.id}`);
          const finalIds = finalRows.map(item => item.observation.id);
          assert.ok(finalIds.every(id => localIds.has(id)), `Hybrid result crossed split corpus for ${query.id}`);
          if (batch.indexOf(query) === 0) processColdFirstQueries.push(endToEndMs);
          else queryLatency.push(endToEndMs);
          const runtime = await loadReranker();
          assert.ok(runtime, 'Cached local reranker is unavailable.');
          const exhaustiveIds = await exhaustiveRerank(query.query, documents, runtime);
          if (candidate) {
            scoredPairs += documents.length + 50;
            assert.ok(scoredPairs <= candidate.sidecar.limits.maxPairs, 'Candidate pair budget exceeded.');
            assert.ok(performance.now() - chunkStarted < 600_000, 'Candidate chunk exceeded 10 minutes.');
            chunkSignal?.throwIfAborted();
          }
          const grades = scoreMap(judgement);
          const annRecall = recallAt(annIds, relevant, 50);
          const finalRecall = recallAt(finalIds, relevant, 10);
          const finalNdcg = ndcgAt10(finalIds, grades);
          const exhaustiveNdcg = ndcgAt10(exhaustiveIds, grades);
          perQuery.push({
            queryId: query.id, stratum: query.stratum, annCandidateIds: annIds, annRecallAt50: annRecall, queryEmbeddingMs: embeddingMs, annLatencyMs: annMs,
            finalIds, finalRecallAt10: finalRecall, finalNdcgAt10: finalNdcg,
            exhaustiveIdsAt10: exhaustiveIds.slice(0, 10), exhaustiveNdcgAt10: exhaustiveNdcg,
            ndcgDropAt10: exhaustiveNdcg - finalNdcg, endToEndMs,
            hardNegativeHitAt10: judgement.hardNegatives.some(item => finalIds.slice(0, 10).includes(item.documentId)),
          });
          if (candidate) {
            const stratumRows = perQuery.filter(row => row.stratum === query.stratum);
            const maximumFinalRecall = (stratumRows.reduce((sum, row) => sum + Number(row.finalRecallAt10), 0) + 18 - stratumRows.length) / 18;
            const maximumAnnRecall = (stratumRows.reduce((sum, row) => sum + Number(row.annRecallAt50), 0) + 18 - stratumRows.length) / 18;
            assert.ok(maximumFinalRecall >= freeze.thresholds.finalRecallAt10ByStratum, 'Candidate final recall gate is already unreachable; inference budget ended.');
            assert.ok(maximumAnnRecall >= freeze.thresholds.annRecallAt50ByStratum, 'Candidate ANN stratum gate is already unreachable; inference budget ended.');
          }
          phase('query:complete', { index: queriesStarted, total: queries.length, endToEndMs, exhaustiveComparison: 'complete' });
        }
        if (candidate) assert.equal(await storedVectorHash(kv, documents), candidateVectorHash, 'Stored vectors changed after candidate scoring.');
      } finally {
        nativeMemory.push(await native.stop());
        if (candidate) assertCandidateResourceContinuation(nativeMemory);
      }
    }

    const strata = ['English', 'Spanish', 'Code identifier'];
    const metric = (field: string, rows = perQuery): number => rows.reduce((sum, row) => sum + Number(row[field] ?? 0), 0) / Math.max(1, rows.length);
    const annByStratum = Object.fromEntries(strata.map(stratum => [stratum, metric('annRecallAt50', perQuery.filter(row => row.stratum === stratum))]));
    const finalByStratum = Object.fromEntries(strata.map(stratum => [stratum, metric('finalRecallAt10', perQuery.filter(row => row.stratum === stratum))]));
    const nDcgDropByStratum = Object.fromEntries(strata.map(stratum => [stratum, metric('ndcgDropAt10', perQuery.filter(row => row.stratum === stratum))]));
    const resourceEvidence = validateResourceEvidence(nativeMemory, 1 + Math.ceil(queries.length / 12));
    const memoryPeak = resourceEvidence.peakJointRssBytes;
    const result = {
      schemaVersion: 1, split, runId, replicate: replicate ?? null, applicationProcessId: process.pid,
      campaignStartedAt, campaignCompletedAt: new Date().toISOString(), phaseTimings,
      campaignId: process.env.AGENTMEMORY_QUALITY_SEAL ?? null,
      candidateRuntimeSha256: process.env.AGENTMEMORY_QUALITY_RUNTIME_SHA256 ?? null,
      executable: resolve(executable), nativeSha256: executableSha256,
      acceptedNativeProfile: resolve(gate.executable) === nativeExecutable && executableSha256 === gate.sha256 ? (gate as NativeGate & { profile?: string }).profile ?? null : null,
      freezeFiles: freeze.files, modelPins: freeze.models,
      candidateSidecarSha256: candidate?.sha256 ?? null, configuredReranker: rerankerConfiguration(), semanticProbes: configuredSemanticProbes(),
      storedVectorSha256Before: candidateVectorHash, storedVectorSha256After: candidateVectorHash, scoredPairsUpperBound: scoredPairs,
      documents: documents.length, queries: queries.length, documentsPerStratum: Object.fromEntries(strata.map(stratum => [stratum, documents.filter(document => document.stratum === stratum).length])),
      preparationMs, embeddingColdProbeMs, rerankerColdLoadMs,
      latencyMs: { nativeProcessStart: distribution(nativeProcessStarts), firstQueryPerNativeProcess: distribution(processColdFirstQueries), warm: distribution(queryLatency) },
      annRecallAt50: metric('annRecallAt50'), annRecallAt50ByStratum: annByStratum,
      finalRecallAt10ByStratum: finalByStratum, meanNdcgDropAt10: metric('ndcgDropAt10'), meanNdcgDropAt10ByStratum: nDcgDropByStratum,
      temporalGraphParityChecks: graphParityChecks, exactTemporalGraphParity: graphParityChecks === 3 ? 1 : 0, combinedSampledRssPeakBytes: memoryPeak,
      resourceEvidence, resourceSamples: nativeMemory, perQuery, failure,
    };
    const acceptanceFailures: string[] = [...resourceEvidence.failures];
    if (split === 'heldout' || candidate) {
      if (Number(result.annRecallAt50) < freeze.thresholds.annRecallAt50Overall) acceptanceFailures.push('ANN mean recall@50 below overall threshold.');
      for (const stratum of strata) {
        if (annByStratum[stratum] < freeze.thresholds.annRecallAt50ByStratum) acceptanceFailures.push(`ANN recall@50 below threshold for ${stratum}.`);
        if (finalByStratum[stratum] < freeze.thresholds.finalRecallAt10ByStratum) acceptanceFailures.push(`Final recall@10 below threshold for ${stratum}.`);
      }
      if (result.meanNdcgDropAt10 > freeze.thresholds.maxNdcgAt10DropFromSameModelExhaustive) acceptanceFailures.push('Mean nDCG@10 loss exceeds the same-model exhaustive limit.');
    }
    if (graphParityChecks !== 3) acceptanceFailures.push('Exact temporal graph parity did not pass.');
    if (memoryPeak === null) acceptanceFailures.push('Combined sampled RSS is unavailable because resource evidence is invalid.');
    else if (memoryPeak > freeze.thresholds.combinedSampledRssMaxBytes) acceptanceFailures.push('Combined sampled RSS exceeded 2 GiB.');
    (result as typeof result & { acceptanceFailures: string[] }).acceptanceFailures = acceptanceFailures;
    if (acceptanceFailures.length > 0) { exitCode = 1; failure = acceptanceFailures.join(' '); }
    if (candidate) {
      const after = await verifyCandidatePins(nativeExecutable);
      assert.equal(after.sha256, candidate.sha256);
      assert.equal(after.runtimeSha256, candidate.runtimeSha256);
      assert.deepEqual(rerankerConfiguration(), candidate.sidecar.runtime);
      assert.equal(configuredSemanticProbes(), candidate.sidecar.native.probes);
    }
    const resultsDirectory = candidate ? resolve(candidateEvidenceRoot, 'results') : resultsDirectoryFor(split);
    await mkdir(resultsDirectory, { recursive: true });
    await writeFile(resolve(resultsDirectory, `${split}-${runId}.json`), JSON.stringify({ ...result, failure, exitCode }, null, 2));
    console.log(JSON.stringify({ split, queries: queries.length, documents: documents.length, exitCode, resultsFile: `${split}-${runId}.json` }, null, 2));
    if (exitCode !== 0) process.exitCode = exitCode;
  } catch (error) {
    exitCode = 1;
    const originalFailure = error instanceof Error ? error.message : String(error);
    const resourceEvidence = validateResourceEvidence(nativeMemory, 1 + Math.ceil(queries.length / 12));
    const pinFailures: string[] = [];
    if (candidate) {
      try {
        const after = await verifyCandidatePins(nativeExecutable);
        assert.equal(after.sha256, candidate.sha256);
        assert.equal(after.runtimeSha256, candidate.runtimeSha256);
      } catch (pinError) { pinFailures.push(String(pinError)); }
    }
    failure = [originalFailure, ...resourceEvidence.failures, ...pinFailures].join(' ');
    const resultsDirectory = candidate ? resolve(candidateEvidenceRoot, 'results') : resultsDirectoryFor(split);
    await mkdir(resultsDirectory, { recursive: true });
    await writeFile(resolve(resultsDirectory, `${split}-${runId}.json`), JSON.stringify({
      schemaVersion: 1, split, runId, replicate: replicate ?? null, applicationProcessId: process.pid,
      campaignStartedAt, campaignFailedAt: new Date().toISOString(), phaseTimings, queriesStarted,
      campaignId: process.env.AGENTMEMORY_QUALITY_SEAL ?? null,
      candidateRuntimeSha256: process.env.AGENTMEMORY_QUALITY_RUNTIME_SHA256 ?? null,
      executable: nativeExecutable, exitCode, failure, combinedSampledRssPeakBytes: resourceEvidence.peakJointRssBytes, resourceEvidence,
      documents: documents.length, queries: queries.length, preparationMs, embeddingColdProbeMs, rerankerColdLoadMs,
      candidateSidecarSha256: candidate?.sha256 ?? null, runtimeSha256: candidate?.runtimeSha256 ?? null, pinFailures,
      resourceSamples: nativeMemory, perQuery,
    }, null, 2));
    console.error(JSON.stringify({ split, queriesCompleted: perQuery.length, documents: documents.length, exitCode, resultsFile: `${split}-${runId}.json`, failure }, null, 2));
    throw error;
  }
}

async function assertCampaignInputs(expected: { freezeSha256: string; nativeGateSha256: string; nativeSha256: string; modelManifestSha256: string; runtimeSha256: string; executable: string }): Promise<void> {
  assert.equal(hash(await readFile(resolve(dataRoot, 'freeze.json'))), expected.freezeSha256, 'Frozen fixture changed during the held-out campaign.');
  assert.equal(hash(await readFile(nativeGatePath)), expected.nativeGateSha256, 'Accepted native gate changed during the held-out campaign.');
  assert.equal(hash(await readFile(expected.executable)), expected.nativeSha256, 'Accepted native binary changed during the held-out campaign.');
  assert.equal(hash(await readFile(resolve(cacheRoot, 'model-assets.json'))), expected.modelManifestSha256, 'Cached model manifest changed during the held-out campaign.');
  assert.equal((await runtimeFingerprint()).sha256, expected.runtimeSha256, 'Application or campaign source changed during the held-out campaign.');
}

async function runSealedHeldoutCampaign(executable: string): Promise<void> {
  const freeze = await verifyFrozenFiles();
  const gate = await readJson<NativeGate>(nativeGatePath);
  assert.equal(gate.accepted, true, 'Main native acceptance is required before held-out inference.');
  assert.equal(resolve(gate.executable), resolve(executable), 'Executable differs from Main accepted native artifact.');
  const nativeSha256 = hash(await readFile(executable));
  assert.equal(nativeSha256, gate.sha256, 'Accepted native binary hash changed.');
  const freezeSha256 = hash(await readFile(resolve(dataRoot, 'freeze.json')));
  const nativeGateSha256 = hash(await readFile(nativeGatePath));
  const runtime = await runtimeFingerprint();
  const expected = { freezeSha256, nativeGateSha256, nativeSha256, modelManifestSha256: freeze.models.assetManifestSha256, runtimeSha256: runtime.sha256, executable: resolve(executable) };
  const candidateFingerprint = hash(JSON.stringify({ ...expected, node: process.version, platform: process.platform, arch: process.arch, modelManifestSha256: freeze.models.assetManifestSha256 }));
  const campaignId = randomUUID();
  const resultsDirectory = resultsDirectoryFor('heldout');
  await mkdir(resultsDirectory, { recursive: true });
  const campaignPath = resolve(resultsDirectory, `heldout-campaign-${campaignId}.json`);
  const candidateLockPath = resolve(resultsDirectory, `heldout-candidate-${candidateFingerprint}.json`);
  const initial = {
    schemaVersion: 1, status: 'running', campaignId, candidateFingerprint, startedAt: new Date().toISOString(),
    frozenInputs: { ...expected, runtimeFiles: runtime.files, modelManifestSha256: freeze.models.assetManifestSha256, node: process.version, platform: process.platform, arch: process.arch },
    preregisteredReplicates: 3, runs: [] as Array<Record<string, unknown>>,
  };
  const lock = await open(candidateLockPath, 'wx').catch((error: unknown) => {
    if ((error as NodeJS.ErrnoException).code === 'EEXIST') throw new Error('A sealed held-out campaign has already been launched for this exact candidate.');
    throw error;
  });
  await lock.writeFile(JSON.stringify({ ...initial, campaignPath }, null, 2));
  await lock.sync();
  await lock.close();
  await writeFile(campaignPath, JSON.stringify(initial, null, 2));

  const sealEnvironment = {
    ...process.env,
    AGENTMEMORY_QUALITY_SEAL: campaignId,
    AGENTMEMORY_QUALITY_FREEZE_SHA256: freezeSha256,
    AGENTMEMORY_QUALITY_NATIVE_GATE_SHA256: nativeGateSha256,
    AGENTMEMORY_QUALITY_NATIVE_SHA256: nativeSha256,
    AGENTMEMORY_QUALITY_RUNTIME_SHA256: runtime.sha256,
  };
  const completedRuns: Array<Record<string, unknown>> = [];
  let allPassed = true;
  let campaignFailure: string | undefined;
  const scriptPath = fileURLToPath(import.meta.url);
  try {
    for (const replicate of [1, 2, 3]) {
      await assertCampaignInputs(expected);
      let record: Record<string, unknown>;
      try {
        const { stdout, stderr } = await execFileAsync(process.execPath, ['--import', 'tsx', scriptPath, 'heldout', expected.executable, '--sealed-replicate', String(replicate)], {
          cwd: repositoryRoot,
          env: { ...sealEnvironment, AGENTMEMORY_QUALITY_REPLICATE: String(replicate) },
          windowsHide: true,
          maxBuffer: 16 * 1024 * 1024,
        });
        record = { replicate, exitCode: 0, stdout, stderr };
      } catch (error) {
        const childError = error as Error & { code?: string | number; signal?: string; stdout?: string; stderr?: string };
        const exitCode = typeof childError.code === 'number' ? childError.code : childError.code ?? null;
        record = { replicate, exitCode, signal: childError.signal ?? null, stdout: childError.stdout ?? '', stderr: childError.stderr ?? '', failure: childError.message };
        allPassed = false;
        campaignFailure ??= `Held-out replicate ${replicate} exited unsuccessfully.`;
      }
      await assertCampaignInputs(expected);
      completedRuns.push(record);
      const update = { ...initial, status: 'running', runs: completedRuns, failure: campaignFailure ?? null };
      await writeFile(campaignPath, JSON.stringify(update, null, 2));
      await writeFile(candidateLockPath, JSON.stringify({ ...update, campaignPath }, null, 2));
    }
  } catch (error) {
    allPassed = false;
    campaignFailure = error instanceof Error ? error.message : String(error);
  }

  const finalCampaign = {
    ...initial, status: allPassed && completedRuns.length === 3 ? 'completed' : 'failed', completedAt: new Date().toISOString(),
    runs: completedRuns, failure: campaignFailure ?? null,
  };
  await writeFile(campaignPath, JSON.stringify(finalCampaign, null, 2));
  await writeFile(candidateLockPath, JSON.stringify({ ...finalCampaign, campaignPath }, null, 2));
  console.log(JSON.stringify({ split: 'heldout', candidateFingerprint, campaignId, replicateCount: completedRuns.length, exitCode: allPassed && completedRuns.length === 3 ? 0 : 1, resultsFile: campaignPath }, null, 2));
  if (!allPassed || completedRuns.length !== 3) process.exitCode = 1;
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  const [splitArg, executable, mode, replicateArg] = process.argv.slice(2);
  if (!executable) throw new Error('An accepted native executable path is required.');
  if (splitArg === 'calibration' && mode === '--candidate-bge256' && process.argv.length === 5) await runCampaign('calibration', executable, undefined, undefined, true);
  else if (splitArg === 'calibration' && process.argv.length === 4) await runCampaign('calibration', executable);
  else if (splitArg === 'calibration' && process.argv.length === 5) await runCampaign('calibration', executable, undefined, process.argv[4]);
  else if (splitArg === 'heldout' && mode === '--sealed-replicate' && ['1', '2', '3'].includes(replicateArg ?? '')) {
    await runCampaign('heldout', executable, Number(replicateArg));
  } else if (splitArg === 'heldout' && mode === '--authorize-sealed-heldout' && process.argv.length === 5) {
    await runSealedHeldoutCampaign(executable);
  } else throw new Error('Usage: node --import tsx benchmark/local-indexed-quality-campaign.ts calibration ACCEPTED_NATIVE_EXECUTABLE [EXPECTED_BINARY_SHA256] | heldout ACCEPTED_NATIVE_EXECUTABLE --authorize-sealed-heldout');
}
