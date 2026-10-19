import { GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION, GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION, type GraphAutomaticRecoveryResponse } from "../state/graph-automatic-recovery.js";
import { StateTransactionError } from "../state/state-transactions.js";

export interface GraphAutomaticRecoveryView {
  version: 1;
  slotId: string;
  rootId: string;
  state: GraphAutomaticRecoveryResponse["state"];
  reason: string | null;
  captured: boolean;
  potentiallySpent: boolean;
  retryAdmitted: boolean;
  adoptionRevision: string;
  unspentChildRevision?: typeof GRAPH_AUTOMATIC_UNSPENT_CHILD_REVISION;
  unspentChildProofRevision?: typeof GRAPH_AUTOMATIC_UNSPENT_CHILD_PROOF_REVISION;
  reservationConsumed: boolean | null;
  retrySpent: boolean | null;
  reservation: GraphAutomaticRecoveryResponse["reservation"];
  deadline: number | null;
  nextCheckAt: number | null;
  replacementJobId: string | null;
  action: "continue" | "wait" | "none";
}

export function graphAutomaticRecoveryView(response: GraphAutomaticRecoveryResponse): GraphAutomaticRecoveryView {
  const replacementJobId = response.new_job_id !== response.anchor.job_id ? response.new_job_id : null;
  return { version: 1, slotId: response.slot_id, rootId: response.root_id, state: response.state,
    reason: response.reason, captured: response.capture !== null, potentiallySpent: response.consumed,
    retryAdmitted: response.retry_admitted, adoptionRevision: response.adoption_revision,
    unspentChildRevision: response.unspent_child_revision,
    unspentChildProofRevision: response.unspent_child_proof_revision,
    reservationConsumed: response.reservation?.consumed ?? null, retrySpent: response.reservation?.consumed ?? null,
    reservation: response.reservation, deadline: response.deadline_ms,
    nextCheckAt: response.retry_after_ms ?? (response.state === "intended" ? response.deadline_ms : null), replacementJobId,
    action: response.reason === "GRAPH_AUTOMATIC_SLOT_EXHAUSTED" || response.reason === "GRAPH_AUTOMATIC_REGENERATION_DEADLINE"
      || ["GRAPH_AUTOMATIC_ADOPTION_UNPROVEN", "GRAPH_AUTOMATIC_ADOPTION_CHILD_PRESENT", "GRAPH_AUTOMATIC_ADOPTION_AUTHORITY_UNPROVEN",
        "GRAPH_AUTOMATIC_UNSPENT_CHILD_UNPROVEN"].includes(response.reason ?? "") ? "none"
      : response.reason === "GRAPH_AUTOMATIC_COOLDOWN" || response.reason === "GRAPH_AUTOMATIC_GLOBAL_LIMIT" ? "wait" : replacementJobId ? "continue"
      : response.state === "terminal" || response.state === "completed" || response.state === "expired" && !response.capture ? "none"
      : response.state === "intended" ? "wait" : "continue" };
}

export class GraphAutomaticRecoveryRequired extends StateTransactionError {
  constructor(readonly recovery: GraphAutomaticRecoveryView, readonly replacement?: { jobId: string; regenerationId: string },
    readonly source?: { systemPrompt: string; userPrompt: string; sourceIds: string[]; completesSource?: boolean; nodesAdded?: number; edgesAdded?: number }) {
    super("STATE_GRAPH_RECOVERY_REQUIRED");
    this.name = "GraphAutomaticRecoveryRequired";
  }
}
