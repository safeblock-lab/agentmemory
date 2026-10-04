import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { readFile, writeFile, stat, mkdir, appendFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { StateKV } from '../src/state/kv.js';
import { KV } from '../src/state/schema.js';
import { IndexedVector } from '../src/state/indexed-vector.js';
import { IndexedLocalEmbedding } from '../src/state/indexed-embedding.js';
import { prepareIndexedCorpus } from '../src/state/indexed-preparation.js';
import { HybridSearch } from '../src/state/hybrid-search.js';
import { SearchIndex } from '../src/state/search-index.js';
import { rerank, isRerankerAvailable } from '../src/state/reranker.js';
import { startNative } from '../.native-pagination-build/local-indexed-verification/native-host.js';
import { corpus, queries, edges, recall } from '../.native-pagination-build/local-indexed-verification/quality-corpus.js';
import { seedQualityGraph } from '../.native-pagination-build/local-indexed-verification/seed-quality-graph.js';

const evidence = resolve(import.meta.dirname,'../.native-pagination-build/local-indexed-verification');
const largeDatabase = resolve(evidence,'../graph-resource/native-run-diagnostic46-875d6b5d-097c-4355-9213-83f1af9042e1/data/state_store.db');
type Seeds = {items:Array<{id:string;position:string;entity:boolean;observation:boolean}>;generation:string};
type Edges = {items:Array<{key:string;value:unknown}>;generation:string};
export async function runLocalIndexedBenchmark(executable: string, mode: 'quality'|'large') {
  const gate = JSON.parse(await readFile(resolve(evidence,'native-accepted.json'),'utf8')) as {executable:string;accepted:boolean};
  assert.equal(gate.accepted,true,'Main native acceptance is required');
  assert.equal(resolve(gate.executable),resolve(executable),'Executable differs from Main accepted artifact');
  process.chdir(evidence);
  const directory=resolve(evidence,`native-${mode}`);
  await mkdir(directory,{recursive:true});
  const database=mode==='large'?largeDatabase:resolve(directory,'quality.sqlite3');
  if(mode==='large') {
    const manifest=JSON.parse(await readFile(resolve(evidence,'../graph-resource/graph54-evidence/copy-manifest-875d6b5d-097c-4355-9213-83f1af9042e1.json'),'utf8')) as {runDirectory:string;copiedFiles:Array<{clonePath:string;bytes:number}>};
    assert.equal(resolve(manifest.copiedFiles[0].clonePath),database);
    assert.ok((await stat(database)).size>=manifest.copiedFiles[0].bytes,'Existing mutable database is unexpectedly truncated');
  }
  const native=await startNative(executable,database,directory);
  const kv=new StateKV(native.worker);
  kv.list=async()=>{throw new Error('Whole-corpus list is prohibited');};
  kv.pages=async function*(){throw new Error('Graph pages are prohibited in this benchmark');};
  const campaign=new Date().toISOString().replace(/[:.]/g,'-');
  const result: Record<string,unknown>={mode,campaign,node:process.version,database,engineProfile:'Main accepted debug unoptimized',scope:mode==='large'?'existing synthetic large graph, bounded targeted parity; no full semantic corpus claim':'10 curated semantic records with exact small graph parity'};
  const deadline=Date.now()+9*60*1000;
  try {
    if(mode==='quality') {
      for(const record of corpus) await kv.set(KV.observations('quality'),record.id,record);
      await seedQualityGraph(kv);
      const provider=new IndexedLocalEmbedding(); const vector=new IndexedVector(kv,provider);
      const prep=performance.now(); result.preparedSemanticCount=await prepareIndexedCorpus(kv,vector,provider); result.preparationMs=performance.now()-prep;
      const seeds=await kv.retrieval<Seeds>({action:'graph_seeds',entity_names:['agentmemory'],observation_ids:['quality-spanish'],match:'substring'});
      assert.deepEqual(seeds.items.map(item=>item.id),['quality-node-a','quality-node-b']);
      const found=await kv.retrieval<Edges>({action:'graph_edges',node_ids:['quality-node-a','quality-node-b']});
      assert.deepEqual(found.items.map(item=>item.value),edges); result.exactGraphParity=true;
      const search=new HybridSearch(new SearchIndex(),null,provider,kv);
      const coldStart=performance.now(); const coldRows=await search.search(queries[0].query,5);
      result.firstEndToEndMs=performance.now()-coldStart;
      result.firstSearchState='Embedding model warm from preparation; reranker cold; process filesystem cache unspecified';
      result.firstEndToEndIds=coldRows.map(row=>row.observation.id);
      result.firstEndToEndRecallAt5=recall(coldRows.map(row=>row.observation.id),queries[0].relevant);
      result.firstEndToEndRelevantAt1=queries[0].relevant.includes(coldRows[0]?.observation.id);
      assert.equal(isRerankerAvailable(),true,'Cold query used fallback');
      assert.ok((result.firstEndToEndMs as number)<60000,'Cold end-to-end query exceeded one minute');
      const evaluations=[];
      for(const query of queries) {
        const embedding=await provider.embed(query.query); const candidates=await vector.search(embedding,5);
        const candidateResults=candidates.map(item=>({observation:corpus.find(record=>record.id===item.obsId)!,sessionId:item.sessionId,bm25Score:0,graphScore:0,vectorScore:item.score,combinedScore:item.score}));
        const ranked=await rerank(query.query,candidateResults,5); assert.equal(isRerankerAvailable(),true,'Fallback is not a rerank measurement');
        const timings=[]; let last:string[]=[];
        for(let iteration=0;iteration<3;iteration++) { const start=performance.now(); const rows=await search.search(query.query,5); timings.push(performance.now()-start); last=rows.map(row=>row.observation.id); assert.ok(timings.at(-1)!<60000,'Warm query exceeded one minute'); }
        evaluations.push({...query,candidateIds:candidates.map(item=>item.obsId),candidateRecallAt5:recall(candidates.map(item=>item.obsId),query.relevant),rerankedIds:ranked.map(item=>item.observation.id),rerankRelevantAt1:query.relevant.includes(ranked[0]?.observation.id),endToEndIds:last,endToEndRecallAt5:recall(last,query.relevant),endToEndRelevantAt1:query.relevant.includes(last[0]),warmEndToEndMs:timings});
      }
      result.quality=evaluations;
      const fullBatch=Array.from({length:50},(_,index)=>({observation:{...corpus[index%corpus.length],id:`bounded-pair-${index}`,narrative:corpus[index%corpus.length].narrative.repeat(80).slice(0,8192)},sessionId:'quality',bm25Score:0,graphScore:0,vectorScore:0,combinedScore:0}));
      const fullBatchStart=performance.now(); const fullRanked=await rerank(queries[0].query,fullBatch,50);
      assert.equal(fullRanked.length,50); assert.equal(isRerankerAvailable(),true);
      result.maxBoundRerank={pairs:50,maxDocumentCharacters:8192,maxPairTokens:512,elapsedMs:performance.now()-fullBatchStart,syntheticRepeatedDocuments:true};
    } else {
      result.initialIndexStatus=await kv.retrieval({action:'index_status'});
      const start=performance.now(); let calls=0;
      for(const scope of [KV.graphNodes,KV.graphEdges]) {
        let page:{status:string;processed:string};
        do { if(Date.now()>=deadline) {result.finalIndexStatus=await kv.retrieval({action:'index_status'});throw new Error('Preparation stopped at nine-minute work bound to reserve cleanup');} const began=performance.now(); page=await kv.retrieval({action:'index_prepare',scope,max_rows:256,max_bytes:8388608}); calls++; const progress={phase:'graph-preparation',scope,calls,batchRows:256,page,callMs:performance.now()-began,elapsedMs:performance.now()-start}; await appendFile(resolve(evidence,`large-progress-${campaign}.jsonl`),JSON.stringify(progress)+'\n'); result.lastPreparationProgress=progress; if(calls%10===0) console.log(JSON.stringify(progress)); if(existsSync(resolve(evidence,'stop-large'))) {result.finalIndexStatus=await kv.retrieval({action:'index_status'});throw new Error('Main requested stop after committed preparation batch');} } while(page.status==='pending');
        assert.equal(page.status,'ready');
      }
      result.graphPreparationMs=performance.now()-start; result.graphPreparationCalls=calls;
      result.finalIndexStatus=await kv.retrieval({action:'index_status'});
      const timings=[]; const snapshots=[];
      for(const ordinal of [0,1000,56000]) {
        const id=`resource-scale-node-${String(ordinal).padStart(12,'0')}-a`;
        const node=await kv.get<{id:string;name:string}>(KV.graphNodes,id); assert.ok(node,'Pinned synthetic node absent');
        const edgeId=`resource-scale-edge-${String(ordinal).padStart(12,'0')}`;
        const expectedEdge=await kv.get(KV.graphEdges,edgeId); assert.ok(expectedEdge,'Pinned canonical edge absent');
        for(let i=0;i<3;i++) {
          const began=performance.now();
          const seeds:Seeds=await kv.retrieval<Seeds>({action:'graph_seeds',entity_names:[node.name],observation_ids:[],match:'exact',max_items:16384,max_bytes:8388608});
          assert.deepEqual(seeds.items.map(item=>item.id),[node.id]);
          const incident=await kv.retrieval<Edges>({action:'graph_edges',node_ids:[id],max_items:16384,max_bytes:8388608});
          assert.ok(incident.items.length>0,'Targeted edge parity must not be empty');
          assert.deepEqual(incident.items.map(item=>item.key),[edgeId]);
          assert.deepEqual(incident.items[0].value,expectedEdge);
          for(const item of incident.items) assert.deepEqual(item.value,await kv.get(KV.graphEdges,item.key));
          timings.push(performance.now()-began); snapshots.push({id,seedCount:seeds.items.length,edgeCount:incident.items.length});
        }
      }
      assert.ok(timings.every(ms=>ms<60000)); result.targetedGraphMs=timings; result.targetedCanonicalParity=snapshots; result.databaseBytes=(await stat(database)).size;
    }
  } catch(error) { result.failure=error instanceof Error?error.message:String(error); throw error; }
  finally {
    try { result.memoryAndLifecycle=await native.stop(); }
    finally { const encoded=JSON.stringify(result,null,2); await writeFile(resolve(evidence,`${mode}-results-${campaign}.json`),encoded); await writeFile(resolve(evidence,`${mode}-results.json`),encoded); }
  }
  return result;
}
if(process.argv[1] && pathToFileURL(resolve(process.argv[1])).href===import.meta.url) {
  const [executable,mode]=process.argv.slice(2);
  if(!executable || !['quality','large'].includes(mode)) throw new Error('Usage: node --import tsx benchmark/local-indexed-retrieval.ts ACCEPTED_EXE quality|large');
  await runLocalIndexedBenchmark(executable,mode as 'quality'|'large');
}
