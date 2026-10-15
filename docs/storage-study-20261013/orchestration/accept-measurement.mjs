import {readFileSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
const r=JSON.parse(readFileSync('docs/storage-study-20261013/orchestration/registration.json','utf8'));
const unit=r.executionContract.units.find(u=>u.id==='measurement');
const observations=[
  'Main integrated local installed configuration identity and the initial/final physical footprint in identity-preliminary.md and measurement-summary.json before accepting measured conclusions.',
  'Main recomputed all 49 family CSV sums, 66 native-table census rows and physical-root/reserved-page reconciliation. Methods retain state_rows temporal residual and non-coherent cut limitations; sidecar census includes streams, queue and snapshots.',
  'Main recomputed provenance model components and vector unique-plus-duplicate totals from returned aggregates. Methods distinguish full temporal coverage, global vector byte equality, fingerprint candidates and bounded-cache byteproof lower bounds; TTL eligibility remains unknown.',
  'Main observed terminal scanner completion and integrated bounded batch/cursor/resource methodology. All readers closed; research outputs only, with no source/database/runtime changes or long pinned snapshot.'
];
const evidence=unit.validation.map((check,i)=>({check,status:'passed',observedBy:'main',observedResult:observations[i],verificationRef:'D:/agentmemory/docs/storage-study-20261013/measurement/methods.md'}));
console.log(execFileSync(process.execPath,['C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/hooks/frontier-ledger.mjs'],{encoding:'utf8',input:JSON.stringify({action:'accept-completion',sessionId:r.sessionId,taskId:r.taskId,attemptId:r.attemptId,planDigest:r.planDigest,contractDigest:r.contractDigest,acceptedBy:'main',unitId:'measurement',evidence})}));
