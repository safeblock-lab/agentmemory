import {execFileSync} from 'node:child_process';
import {readRoutingState} from 'file:///C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/hooks/session-state.mjs';
const identity={sessionId:'01a13723-fc07-7d40-a378-dea223c40aa5',taskId:'d67a1db0-9143-4160-92e9-78bbed3f2332',attemptId:'01a13724-8bca-7cc1-b01b-0972e8d35a48'};
const value=JSON.parse(execFileSync(process.execPath,['docs/storage-study-20261013/orchestration/prepare-contract.mjs','confirm'],{encoding:'utf8'}));
const captured=(await readRoutingState(identity.sessionId)).routeReceipts;
const routeReceipts=value.executionContract.units.map(u=>{const candidates=captured.filter(r=>r.unitId===u.id&&r.agentType===u.route.agentType);if(!candidates.length)throw new Error('missing_exact_receipt');return candidates.at(-1);});
console.log(JSON.stringify({action:'register-plan',...identity,acceptedBy:'main',sessionAuthorization:'approved',...value,routeReceipts}));
