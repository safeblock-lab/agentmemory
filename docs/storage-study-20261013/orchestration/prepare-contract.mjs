import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {resolveExecutorChoice} from 'file:///C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/skills/engineering-execution-quality-v2/scripts/resolve-executor-choice.mjs';
import {routeTupleFor} from 'file:///C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/skills/model-orchestration-v2/scripts/route-catalog.mjs';
import {validateExecutionContract, presentExecutionContract, confirmExecutionContract, assignmentMatrixFor, executionContractDigest, authorizeExecutionUnit} from 'file:///C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/skills/planning-decomposition-v2/scripts/execution-contract.mjs';
import {digestValue} from 'file:///C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/skills/planning-decomposition-v2/scripts/contract-lifecycle.mjs';
const root='docs/storage-study-20261013/orchestration/';
const plan=JSON.parse(readFileSync(root+'pre-route-plan.json','utf8'));
const policyPath='C:/Users/harus/.codex/plugins/cache/safeblock-lab/global-orchestration/0.1.70/skills/coding-guidelines/SKILL.md';
const policy={name:'coding-guidelines',source:policyPath,sha256:createHash('sha256').update(readFileSync(policyPath)).digest('hex'),reason:'Source analysis and proposal quality; no product changes'};
const units=plan.leaves.map((leaf)=>({
  id:leaf.id,task:leaf.objective,paths:leaf.exclusiveWrites,
  executionWave:leaf.dependsOn.length?1:0,executionMode:'delegate',phase:leaf.id==='reduction-proposal'?'planning':'functionalReview',domain:leaf.id==='reduction-proposal'?'architecture':'general',difficulty:'mediumHigh',contextRequirement:'broad',relationshipComplexity:'high',risk:'medium',securityGate:'none',requiresHumanApproval:false,dependsOn:leaf.dependsOn,
  readStateKeys:leaf.id==='measurement'?['live-db-readonly']:leaf.id==='source-mapping'?['source-readonly']:['measurement-result','source-result'],
  writeStateKeys:[],externalStateKeys:[],inputs:['Read-only user research scope',...leaf.readPaths],outputs:['Report returned to Main for saving under owned directory'],validation:leaf.validation,
  qualityGates:['No live/source/data/configuration effects','Explicit coverage and evidence limits'],escalationConditions:['Unknown database identity or read safety','Resource or consistency bounds cannot be met'],status:'ready',owner:'agent',
  route:routeTupleFor('sol-xhigh',leaf.id==='reduction-proposal'?'sol-xhigh-planner-v2':'sol-xhigh-specialist-v2'),
  policyHandoff:{projectRoute:'AGENTS.md',policies:[policy],skills:[]},gates:[],closedGates:[]
}));
let contract={schemaVersion:2,mode:'delegated',objective:plan.objective,acceptanceCriteria:['Full database coverage with clear consistency and attribution','Evidence-supported >=10x alternatives or explicit infeasibility','Reversible migration proposal only'],validation:['Inspect measured reconciliation and proposal evidence'],classificationRef:'classify_plan database-tenfold-study current session',plannerProvenance:'plan_only',synthesisOwner:'main',noDelegationReason:null,authorization:{state:'pending',reasons:['delegated_execution_policy']},lifecycle:{state:'draft',presentedDigest:null,confirmedDigest:null},criticalPath:['measurement','reduction-proposal'],parallelGroups:[['measurement','source-mapping']],approvalGates:[],units};
const valid=validateExecutionContract(contract);if(!valid.ok)throw new Error(JSON.stringify(valid));
contract=presentExecutionContract(contract);
if(process.argv[2]==='present'){console.log(JSON.stringify({contract,matrix:assignmentMatrixFor(contract)}));}
else {
  contract=confirmExecutionContract(contract,contract.lifecycle.presentedDigest);
  const contractDigest=executionContractDigest(contract);
  const executorState=resolveExecutorChoice({planningOwner:'planner',previousExecutorChoice:'delegated'});
  const completedUnits=['measurement','source-mapping'];
  const authorizations=units.map(u=>{const a=authorizeExecutionUnit(contract,u.id,contractDigest,completedUnits,{executorState});return {unitId:u.id,executionAuthorized:a.ok===true&&a.executorState.executionAuthorized===true,reason:a.reason,planDigest:digestValue(plan),contractDigest,proposalDigest:contract.lifecycle.confirmedDigest};});
  console.log(JSON.stringify({plan,executionContract:contract,planDigest:digestValue(plan),contractDigest,unitAuthorizations:authorizations}));
}
