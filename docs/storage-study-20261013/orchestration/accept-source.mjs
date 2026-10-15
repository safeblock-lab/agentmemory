import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
const r=JSON.parse(readFileSync('docs/storage-study-20261013/orchestration/registration.json','utf8'));
const unit=r.executionContract.units.find(u=>u.id==='source-mapping');
const observations=[
  'Main inspected current StateKV/indexed-vector/schema/graph-job-storage source matches and native codec patch; saved source-cited mapping with stale graph and dynamic caller limits.',
  'Main read native zlib6/raw codec and stream projection source; saved mapping distinguishes existing compression, lossless sharing, lossy quantization and semantic removal.',
  'Main inspected session/timestamp/raw identity equality and prepared expected-version replay source matches; saved constraints preserve identities, provenance, order, recovery and exports.',
  'Integrated output is a static source analysis with no runtime behavior claim; Main source reads and documentation integration invoked no DB scan, tests, builds, providers or service/configuration operation.'
];
const evidence=unit.validation.map((check,i)=>({check,status:'passed',observedBy:'main',observedResult:observations[i],verificationRef:'D:/agentmemory/docs/storage-study-20261013/source-mapping/storage-contracts.md'}));
console.log(execFileSync(process.execPath,['C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/hooks/frontier-ledger.mjs'],{encoding:'utf8',input:JSON.stringify({action:'accept-completion',sessionId:r.sessionId,taskId:r.taskId,attemptId:r.attemptId,planDigest:r.planDigest,contractDigest:r.contractDigest,acceptedBy:'main',unitId:'source-mapping',evidence})}));
