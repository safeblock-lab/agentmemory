import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
const r=JSON.parse(readFileSync('docs/storage-study-20261013/orchestration/registration.json','utf8'));
const unit=r.executionContract.units.find(u=>u.id==='reduction-proposal');
const observations=[
  'Main integrated physical root reconciliation and temporal perimeters separately from logical candidate representations. Budget469513577 leaves296495357 bytes to the SQLite10x ceiling; overlapping compression, list and vector savings are explicitly excluded from addition.',
  'Main integrated exact blob/list/float32 reconstruction constraints, original content and all index/sidecar perimeter requirements. Coexistence, latest-state rollback, monotonic provider accounting and peak disk formula are specified with unknown capacity explicit.',
  'The saved proposal states whole-database10x is unproved and the retained compressed Git candidate exceeds the extended10x ceiling. TTL eligible bytes remain unknown; no model is presented as a materialized physical result.',
  'Main saved a concrete human-reviewable documentary proposal under the authorized study directory. No implementation, cleanup, migration, VACUUM, service/configuration or provider action was invoked.'
];
const ref='D:/agentmemory/docs/storage-study-20261013/proposal/reduction-proposal.md';
const evidence=unit.validation.map((check,i)=>({check,status:'passed',observedBy:'main',observedResult:observations[i],verificationRef:ref}));
const base={sessionId:r.sessionId,taskId:r.taskId,attemptId:r.attemptId,planDigest:r.planDigest,contractDigest:r.contractDigest,acceptedBy:'main'};
const frontier=JSON.parse(execFileSync(process.execPath,['C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/hooks/frontier-ledger.mjs'],{encoding:'utf8',input:JSON.stringify({action:'accept-completion',...base,unitId:unit.id,evidence})}));
console.log(JSON.stringify({unitId:unit.id,status:frontier.status}));
const report={unitId:r.taskId,status:'complete',claims:[
  {id:'research',text:'Read-only full temporal storage study and source mapping completed with explicit consistency and equality limits.',checkId:'measured'},
  {id:'proposal',text:'Concrete reversible reduction proposal saved; no actual reduction or supported whole-database10x claim.',checkId:'integrated'}
],checks:[
  {id:'measured',command:'Requested bounded temporal measurement and Main aggregate integration',result:'passed',evidence:[{id:'coverage',kind:'observation',text:'Accepted census covers1501092 decoded KV rows,49 families,66 native tables and1158 sidecars. Saved methods and CSV arithmetic distinguish derived state_rows allocation, exhaustive vector byte equality and fingerprint/cache lower bounds.'}]},
  {id:'integrated',command:'Main integration of accepted measurement and source inputs into the authorized proposal artifact',result:'passed',evidence:[{id:'document',kind:'observation',text:'Saved docs/storage-study-20261013/proposal/reduction-proposal.md includes nonoverlapping perimeters, conditional byte budget, fidelity contracts, meaningful-use TTL semantics, reversible coexistence and monotonic accounting. No operational actions or tests were performed;10x and TTL eligibility remain unproved.'}]}
]};
console.log(execFileSync(process.execPath,['C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/hooks/task-ledger.mjs'],{encoding:'utf8',input:JSON.stringify({action:'complete',taskId:r.taskId,sessionId:r.sessionId,turnId:r.attemptId,report})}));
