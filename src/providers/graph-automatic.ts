import type { GraphRecoveryAuthorityProfile, GraphRecoveryBinding } from "../state/graph-automatic-recovery.js";
import type { LlmUsage } from "../types.js";

export const GRAPH_AUTOMATIC_CONTRACT: GraphRecoveryBinding["contract"] = Object.freeze({
  id: "graph-extraction-json", version: "1", parser_version: "graph-extraction-provenance-v2",
  schema_json: JSON.stringify({ type: "object", required: ["nodes", "edges"], properties: {
    nodes: { type: "array" }, edges: { type: "array" },
  } }),
});

export interface PreparedGraphAutomaticCall {
  provider: string;
  regenerationSource: { systemPrompt: string; userPrompt: string; sourceIds: string[]; completesSource?: boolean; nodesAdded?: number; edgesAdded?: number };
  binding: GraphRecoveryBinding;
  source: string;
  profile: GraphRecoveryAuthorityProfile;
  dispatch(permit: { binding: Readonly<GraphRecoveryBinding>; attempt_id: string; timeout_ms: number }, capture: (text: string) => Promise<void>): Promise<void>;
  decode(raw: string): string;
  usage(raw: string): { usage: LlmUsage; responseModel: string | null };
  renew(): PreparedGraphAutomaticCall;
}

export interface GraphAutomaticPreparation {
  systemPrompt: string;
  userPrompt: string;
  grouped: boolean;
  sourceIds?: string[];
  completesSource?: boolean;
  nodesAdded?: number;
  edgesAdded?: number;
}

export interface GraphAutomaticProvider {
  prepareGraphAutomaticCall?(request: GraphAutomaticPreparation): PreparedGraphAutomaticCall;
}
