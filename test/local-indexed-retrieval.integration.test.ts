import { describe, it, expect } from 'vitest';
import { createHash } from 'node:crypto';
import { runLocalIndexedBenchmark } from '../benchmark/local-indexed-retrieval.js';
import { recall, queries } from '../.native-pagination-build/local-indexed-verification/quality-corpus.js';
import { parseSplitLines, parseSplitArray, assertCandidateResourceContinuation, storedVectorHash } from '../benchmark/local-indexed-quality-campaign.js';
import { KV } from '../src/state/schema.js';

describe('stored vector integrity hash', () => {
  const documents = Array.from({ length: 54 }, (_, index) => ({ id: `vector-${index}` }));
  const records = documents.map((document, index) => ({ id: document.id, vector: [index, index / 54] }));
  it('reads all 54 records in ordered batches of eight within queue capacity 32 with the original hash', async () => {
    let active = 0;
    let peak = 0;
    const requested: string[] = [];
    const completed: string[] = [];
    const scopes: string[] = [];
    const actual = await storedVectorHash({ get: async (scope, id) => {
      requested.push(id);
      scopes.push(scope);
      if (++active > 32) throw new Error('STATE_TX_LIMIT_EXCEEDED');
      peak = Math.max(peak, active);
      const index = documents.findIndex(document => document.id === id);
      await new Promise(resolve => setTimeout(resolve, 8 - index % 8));
      completed.push(id);
      active--;
      return records[index];
    } }, documents);
    expect(peak).toBe(8);
    expect(active).toBe(0);
    expect(requested).toEqual(documents.map(document => document.id));
    expect(completed).not.toEqual(requested);
    expect(scopes).toEqual(documents.map(() => KV.semanticVectors('observations')));
    expect(actual).toBe(createHash('sha256').update(JSON.stringify(records)).digest('hex'));
  });
  it('propagates the original read failure and does not dispatch another batch', async () => {
    const original = new Error('synthetic vector read failure');
    const requested: string[] = [];
    await expect(storedVectorHash({ get: async (_scope, id) => {
      requested.push(id);
      if (id === documents[10].id) throw original;
      return records[documents.findIndex(document => document.id === id)];
    } }, documents)).rejects.toBe(original);
    expect(requested).toEqual(documents.slice(0, 16).map(document => document.id));
  });
  it('retains the missing-record assertion after reading every record', async () => {
    const requested: string[] = [];
    await expect(storedVectorHash({ get: async (_scope, id) => {
      requested.push(id);
      return id === documents[3].id ? null : records[documents.findIndex(document => document.id === id)];
    } }, documents)).rejects.toThrow('Stored vectors must cover every calibration document.');
    expect(requested).toEqual(documents.map(document => document.id));
  });
});

describe('local indexed benchmark evaluation',()=>{
  const resourceSnapshot = () => ({ samples: 3, engineSampledPeakRss: 100, engineOsPeakRss: 100,
    nodeSampledPeakRss: 100, jointSampledPeakRss: 200, failedSamples: 0, firstError: null,
    maxGapMs: 1000, skippedSamples: 0, phases: ['startup', 'work', 'terminal'],
    pidGone: true, listenerGone: true, collectorGone: true, cleanupErrors: [], performanceTargetsPassed: true });
  it.each([
    { failedSamples: 1 }, { maxGapMs: 5001 }, { pidGone: false }, { collectorGone: false },
    { cleanupErrors: ['synthetic cleanup failure'] }, { jointSampledPeakRss: null },
    { jointSampledPeakRss: 2 * 1024 ** 3 + 1 },
  ])('ends the candidate budget before another mocked chunk when resources fail: %j', invalid => {
    const completed: Array<Record<string, unknown>> = [];
    let nextChunks = 0;
    expect(() => {
      completed.push({ ...resourceSnapshot(), ...invalid });
      assertCandidateResourceContinuation(completed);
      nextChunks++;
    }).toThrow();
    expect(completed).toHaveLength(1);
    expect(nextChunks).toBe(0);
  });
  it('permits continuation only while every completed candidate process stays valid and within 2 GiB', () => {
    const completed: Array<Record<string, unknown>> = [resourceSnapshot()];
    expect(() => assertCandidateResourceContinuation(completed)).not.toThrow();
    completed.push({ ...resourceSnapshot(), jointSampledPeakRss: 2 * 1024 ** 3 });
    expect(() => assertCandidateResourceContinuation(completed)).not.toThrow();
    completed.push({ ...resourceSnapshot(), listenerGone: false });
    expect(() => assertCandidateResourceContinuation(completed)).toThrow();
    expect(() => assertCandidateResourceContinuation([])).toThrow();
  });
  it('filters the sealed split before parsing and never parses excluded malformed payloads',()=>{
    expect(parseSplitLines('{"split":"calibration","id":"synthetic"}\n{"split":"heldout", invalid}', 'calibration')).toEqual([{split:'calibration',id:'synthetic'}]);
    expect(parseSplitArray('[{"split":"calibration","nested":{"text":"}"}},{"split":"heldout", invalid}]', 'calibration')).toEqual([{split:'calibration',nested:{text:'}'}}]);
  });
  it('does not count irrelevant documents or duplicate candidates as recall',()=>{
    expect(recall(['a','a','unrelated'],['a','b'])).toBe(0.5);
    expect(recall([],['a'])).toBe(0);
  });
  it('separates Spanish, English and identifier relevance judgments',()=>{
    expect(queries.map(query=>query.language)).toEqual(['English','Spanish','Code']);
    expect(queries.every(query=>query.relevant.length>0)).toBe(true);
  });
  it.runIf(Boolean(process.env.LOCAL_INDEXED_ACCEPTED_ENGINE))('executes the native and real local model integration',async()=>{
    const result=await runLocalIndexedBenchmark(process.env.LOCAL_INDEXED_ACCEPTED_ENGINE!,'quality');
    expect(result.exactGraphParity).toBe(true);
    expect(result.preparedSemanticCount).toBe(10);
    expect(result.quality).toHaveLength(3);
    expect(result.memoryAndLifecycle).toMatchObject({samples:expect.any(Number)});
  },120000);
});
