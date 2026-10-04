import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const hash = value => createHash('sha256').update(value).digest('hex');
const readJson = async name => JSON.parse(await readFile(resolve(root, name), 'utf8'));
const readLines = async name => (await readFile(resolve(root, name), 'utf8')).trimEnd().split('\n').map(line => JSON.parse(line));
const freeze = await readJson('freeze.json');
assert.equal(freeze.schemaVersion, 1);
assert.equal(freeze.judgementPolicy, 'Agent-authored predetermined content intent; no model scores or retrieval output used to assign labels; labels have not been independently human reviewed.');
assert.deepEqual(freeze.thresholds, {
  exactTemporalGraphParity: 1,
  annRecallAt50Overall: 0.95,
  annRecallAt50ByStratum: 0.9,
  finalRecallAt10ByStratum: 0.95,
  maxNdcgAt10DropFromSameModelExhaustive: 0.02,
  ordinaryQueryMaxMsExclusive: 60000,
  combinedSampledRssMaxBytes: 2147483648,
});
assert.equal(freeze.models.embedding.id, 'Xenova/all-MiniLM-L6-v2');
assert.equal(freeze.models.embedding.revision, '751bff37182d3f1213fa05d7196b954e230abad9');
assert.equal(freeze.models.embedding.dtype, 'q8');
assert.equal(freeze.models.reranker.id, 'Xenova/ms-marco-MiniLM-L-6-v2');
assert.equal(freeze.models.reranker.revision, 'a09144355adeed5f58c8ed011d209bf8ee5a1fec');
assert.equal(freeze.models.reranker.dtype, 'q8');
assert.match(freeze.models.assetManifestSha256, /^[a-f0-9]{64}$/);
for (const [name, expected] of Object.entries(freeze.files)) assert.equal(hash(await readFile(resolve(root, name))), expected, `Frozen file hash changed: ${name}`);

const documents = await readLines('corpus.jsonl');
const queries = await readLines('queries.jsonl');
const judgements = await readLines('judgements.jsonl');
const graphs = await readJson('graph-fixtures.json');
assert.equal(documents.length, 162);
assert.equal(queries.length, 162);
assert.equal(judgements.length, 162);
assert.equal(new Set(documents.map(item => item.id)).size, documents.length);
assert.equal(new Set(queries.map(item => item.id)).size, queries.length);
assert.equal(new Set(queries.map(item => item.query.toLocaleLowerCase('en'))).size, queries.length, 'Query text must be unique.');
assert.equal(new Set(queries.map(item => item.intent)).size, queries.length, 'Query intent must be unique.');

const docsById = new Map(documents.map(item => [item.id, item]));
const qById = new Map(queries.map(item => [item.id, item]));
const judgementsById = new Map(judgements.map(item => [item.queryId, item]));
assert.deepEqual(new Set([...qById.keys()]), new Set(judgementsById.keys()));
for (const split of ['calibration', 'heldout']) {
  const splitDocs = documents.filter(item => item.split === split);
  const splitQueries = queries.filter(item => item.split === split);
  assert.equal(splitDocs.length, split === 'heldout' ? 108 : 54);
  assert.equal(splitQueries.length, splitDocs.length);
  assert.equal(freeze.counts.bySplit[split].documents, splitDocs.length);
  assert.equal(freeze.counts.bySplit[split].queries, splitQueries.length);
  assert.ok(splitDocs.length > 50, `${split} retrieval must search the full split corpus, not a tiny judged subset.`);
  for (const stratum of ['English', 'Spanish', 'Code identifier']) {
    assert.equal(splitQueries.filter(item => item.stratum === stratum).length, split === 'heldout' ? 36 : 18);
  }
}

const exactContent = values => new Set(values.map(item => hash(`${item.title}\n${item.narrative}`)));
const calDocHashes = exactContent(documents.filter(item => item.split === 'calibration'));
const heldDocHashes = exactContent(documents.filter(item => item.split === 'heldout'));
assert.equal([...calDocHashes].filter(value => heldDocHashes.has(value)).length, 0, 'Document text crosses split boundary.');
const calQueryHashes = new Set(queries.filter(item => item.split === 'calibration').map(item => hash(item.query)));
const heldQueryHashes = new Set(queries.filter(item => item.split === 'heldout').map(item => hash(item.query)));
assert.equal([...calQueryHashes].filter(value => heldQueryHashes.has(value)).length, 0, 'Query text crosses split boundary.');

let multiRelevant = 0;
for (const judgement of judgements) {
  const query = qById.get(judgement.queryId);
  assert.ok(query, `Unknown query ${judgement.queryId}`);
  assert.equal(judgement.split, query.split);
  assert.equal(judgement.method, 'agent-authored-content-intent');
  assert.ok(judgement.relevant.length >= 1);
  if (judgement.relevant.length > 1) multiRelevant++;
  const relevantIds = new Set(judgement.relevant.map(item => item.documentId));
  assert.equal(relevantIds.size, judgement.relevant.length, `Duplicate positive label for ${query.id}`);
  for (const label of judgement.relevant) {
    assert.equal(label.grade, 3);
    assert.equal(docsById.get(label.documentId)?.split, query.split, `Cross-split positive for ${query.id}`);
  }
  assert.ok(judgement.hardNegatives.length >= 2, `Missing close negatives for ${query.id}`);
  for (const label of judgement.hardNegatives) {
    const document = docsById.get(label.documentId);
    assert.equal(label.grade, 0);
    assert.equal(document?.split, query.split, `Cross-split negative for ${query.id}`);
    assert.equal(document?.stratum, query.stratum, `Cross-stratum hard negative for ${query.id}`);
    assert.equal(document?.group, query.group, `Non-local hard negative for ${query.id}`);
    assert.ok(!relevantIds.has(label.documentId), `Hard negative is also relevant for ${query.id}`);
  }
}
assert.ok(multiRelevant >= 6, `Expected multiple-relevant queries, found ${multiRelevant}.`);

for (const name of ['corpus.jsonl', 'queries.jsonl', 'judgements.jsonl']) {
  const rows = await readLines(name);
  const hashMap = name === 'corpus.jsonl' ? freeze.rows.documents : name === 'queries.jsonl' ? freeze.rows.queries : freeze.rows.judgements;
  for (const [index, row] of rows.entries()) {
    const id = row.id ?? row.queryId;
    assert.equal(hash(JSON.stringify(row)), hashMap[id], `Frozen row changed: ${name}:${id}`);
    assert.equal(hash(JSON.stringify(row)), hashMap[Object.keys(hashMap)[index]], `Frozen row order changed: ${name}:${id}`);
  }
}

assert.equal(graphs.length, 2);
for (const fixture of graphs) {
  const splitIds = new Set(documents.filter(item => item.split === fixture.split).map(item => item.id));
  for (const node of fixture.nodes) for (const observationId of node.sourceObservationIds) assert.ok(splitIds.has(observationId), `Graph fixture crossed ${fixture.split} scope.`);
  for (const edge of fixture.edges) for (const observationId of edge.sourceObservationIds) assert.ok(splitIds.has(observationId), `Graph edge crossed ${fixture.split} scope.`);
}

console.log(JSON.stringify({
  frozenFiles: freeze.files,
  documents: documents.length,
  queries: queries.length,
  uniqueQueryTexts: new Set(queries.map(item => item.query)).size,
  uniqueIntents: new Set(queries.map(item => item.intent)).size,
  multiRelevantQueries: multiRelevant,
  heldoutDocuments: documents.filter(item => item.split === 'heldout').length,
  heldoutQueries: queries.filter(item => item.split === 'heldout').length,
  heldoutCandidatePool: documents.filter(item => item.split === 'heldout').length,
  calibrationDocuments: documents.filter(item => item.split === 'calibration').length,
  calibrationQueries: queries.filter(item => item.split === 'calibration').length,
  byStratum: Object.fromEntries(['English', 'Spanish', 'Code identifier'].map(stratum => [stratum, Object.fromEntries(['calibration', 'heldout'].map(split => [split, queries.filter(item => item.stratum === stratum && item.split === split).length]))])),
  exactTextOverlapAcrossSplits: 0,
}, null, 2));
