import type { StateKV } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { getTypeSafeConfig, TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD,
  TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD } from "../config.js";
import type { TypeSafeDecisionProvider, TypeSafeQuestion } from "../providers/typesafe.js";
import { selectCompactionCandidates, type CompactionCandidate } from "./typesafe-compaction.js";
import { proceduralFrequency, verifyCapturedSource } from "./consolidation-capture.js";
import { immutablePage, pageKey, saveRoot, type ConsolidationTypeSafeDecision, type PagedConsolidation, type SourceHeader } from "./consolidation-manifest.js";
import { stripPrivateData } from "./privacy.js";

interface TypeSafeGateReceipt {
  identity: { project: string; kind: PagedConsolidation["kind"]; policy: NonNullable<PagedConsolidation["callerPolicy"]>;
    sourceVersions: Array<{ id: string; rowVersion: string; fingerprint: string }> };
  decision: ConsolidationTypeSafeDecision;
}

function receivedDecision(value: unknown): value is TypeSafeGateReceipt {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const receipt = value as TypeSafeGateReceipt;
  return !!receipt.identity && Array.isArray(receipt.identity.sourceVersions)
    && !!receipt.decision && Array.isArray(receipt.decision.selectedSourceIndexes)
    && Array.isArray(receipt.decision.droppedSourceIndexes) && Array.isArray(receipt.decision.compactionFallbackReasons)
    && ["run", "skip", "fail-open", "not-applicable"].includes(receipt.decision.pipelineGate);
}

function pipelineGateState(root: PagedConsolidation, headers: SourceHeader[], selected: number[], frequencies: Map<number, number>): string {
  const previews = selected.slice(0, 3).map(index => {
    const typeSafe = headers[index]!.typesafe!;
    return root.kind === "semantic"
      ? { title: typeSafe.gateState.slice(0, 72), preview: typeSafe.state.slice(0, 96) }
      : { frequency: frequencies.get(index) ?? headers[index]!.frequency, preview: typeSafe.gateState.slice(0, 72) };
  });
  return stripPrivateData(JSON.stringify({ workflow: `${root.kind}-consolidation`, sourceCount: selected.length, previews })).slice(0, 512);
}

function proceduralCompactionState(metadata: NonNullable<SourceHeader["typesafe"]>, frequency: number): string {
  try {
    const captured = JSON.parse(metadata.state) as { preview?: unknown; ageDays?: unknown };
    return stripPrivateData(JSON.stringify({ frequency, preview: captured.preview, ageDays: captured.ageDays })).slice(0, 512);
  } catch {
    return metadata.state;
  }
}

async function decideCompaction(root: PagedConsolidation, headers: SourceHeader[], eligible: number[], frequencies: Map<number, number>, provider?: TypeSafeDecisionProvider) {
  const newestSemantic = root.kind === "semantic"
    ? new Set([...eligible].sort((left, right) => {
      const leftAt = Date.parse(headers[left]!.createdAt ?? "");
      const rightAt = Date.parse(headers[right]!.createdAt ?? "");
      return (Number.isFinite(rightAt) ? rightAt : Number.NEGATIVE_INFINITY)
        - (Number.isFinite(leftAt) ? leftAt : Number.NEGATIVE_INFINITY) || left - right;
    }).slice(0, 6))
    : new Set<number>();
  const candidates: CompactionCandidate<number>[] = eligible.map(index => {
    const metadata = headers[index]!.typesafe;
    if (!metadata) throw new Error("consolidation_typesafe_metadata_missing");
    const protectedItem = metadata.protected || newestSemantic.has(index)
      || root.kind === "procedural" && ((frequencies.get(index) ?? 0) >= 3 || metadata.gateProtected);
    if (protectedItem) return { id: headers[index]!.id, value: index, disposition: "protected" };
    if (metadata.deterministicDrop) return { id: headers[index]!.id, value: index, disposition: "deterministic-drop" };
    return { id: headers[index]!.id, value: index, disposition: "ambiguous",
      state: root.kind === "procedural" ? proceduralCompactionState(metadata, frequencies.get(index) ?? headers[index]!.frequency) : metadata.state };
  });
  if (!root.callerPolicy?.compaction) return { kept: candidates.map(candidate => candidate.value), dropped: [] as number[], fallbackReasons: [] as string[] };
  const selection = await selectCompactionCandidates(candidates, provider ? async batch => {
    const questions: Record<string, TypeSafeQuestion> = {};
    for (const item of batch) questions[item.questionId] = { type: "choice",
      instructions: `Should this source remain in ${root.kind} consolidation?`,
      criteria: { keep: "Keep if this source may add a durable fact or reusable procedure.",
        drop: "Drop only when this source is clearly routine, redundant, and low value." } };
    const answers = await provider.evaluate("compaction", { candidates: batch }, questions);
    if (!answers) return undefined;
    const result: Record<string, { choice: "keep" | "drop"; confidence: number }> = {};
    for (const item of batch) {
      const answer = answers[item.questionId];
      if (answer?.type === "choice" && (answer.choice === "keep" || answer.choice === "drop")) {
        result[item.questionId] = { choice: answer.choice, confidence: answer.confidence };
      }
    }
    return result;
  } : undefined, { maxStateChars: getTypeSafeConfig().maxStateChars });
  return { kept: selection.kept.map(candidate => candidate.value), dropped: selection.dropped.map(candidate => candidate.value),
    fallbackReasons: [...selection.fallbackReasons] };
}

export async function applyPagedConsolidationTypeSafeGate(
  kv: StateKV, root: PagedConsolidation, provider?: TypeSafeDecisionProvider,
): Promise<boolean> {
  const policy = root.callerPolicy;
  if (!policy || root.legacy || root.kind === "memory") return true;
  const receiptKey = pageKey(root.id, "typesafe-gate", 0);
  const headers = await Promise.all(root.refs.map((_, index) => kv.get<SourceHeader>(KV.state, pageKey(root.id, "source", index))));
  if (headers.some(header => !header?.typesafe || !header.rowVersion)) throw new Error("consolidation_typesafe_metadata_missing");
  const sourceVersions = headers.map(header => ({ id: header!.id, rowVersion: header!.rowVersion, fingerprint: header!.fingerprint }));
  const identity: TypeSafeGateReceipt["identity"] = { project: root.project, kind: root.kind, policy, sourceVersions };
  const saved = await kv.get<TypeSafeGateReceipt>(KV.state, receiptKey);
  if (saved) {
    if (!receivedDecision(saved) || JSON.stringify(saved.identity) !== JSON.stringify(identity)) throw new Error("consolidation_typesafe_gate_identity_conflict");
    root.typesafeDecision = saved.decision;
    root.eligibleSources = saved.decision.selectedSourceIndexes;
    if (saved.decision.pipelineGate === "skip") root.typesafeValidation ??= {
      sourceIndexes: [...new Set([...saved.decision.selectedSourceIndexes, ...saved.decision.droppedSourceIndexes])].sort((left, right) => left - right),
      nextSource: 0, complete: false,
    };
    await saveRoot(kv, root);
    return saved.decision.pipelineGate === "skip" ? validateSkipSourceCut(kv, root) : true;
  }
  if (root.typesafeDecision) throw new Error("consolidation_typesafe_gate_receipt_missing");

  const capturedHeaders = headers as SourceHeader[];
  const eligible = [...root.eligibleSources];
  const frequencies = new Map<number, number>();
  if (root.kind === "procedural") {
    for (const index of eligible) frequencies.set(index, await proceduralFrequency(kv, root, index));
  }
  const selection = await decideCompaction(root, capturedHeaders, eligible, frequencies, provider);
  const selected = [...selection.kept].sort((left, right) => left - right);
  const dropped = [...selection.dropped].sort((left, right) => left - right);
  let pipelineGate: ConsolidationTypeSafeDecision["pipelineGate"] = policy.pipelineGate ? "run" : "not-applicable";
  const gateEligible = selected.filter(index => root.kind === "semantic"
    ? !capturedHeaders[index]!.typesafe!.gateProtected
    : (frequencies.get(index) ?? 0) < 3 && !capturedHeaders[index]!.typesafe!.gateProtected);
  const canGate = policy.pipelineGate && (root.kind === "semantic" ? selected.length > 0 && gateEligible.length === selected.length
    : selected.length >= 2 && gateEligible.length === selected.length);
  if (canGate) {
    try {
      const state = pipelineGateState(root, capturedHeaders, selected, frequencies);
      const decision = await provider?.evaluateChoice("pipelineGates", state,
        root.kind === "semantic" ? "Should this scheduled semantic consolidation run?" : "Should this scheduled recurring-pattern extraction run?",
        { run: root.kind === "semantic" ? "Run when selected summaries can add durable facts." : "Run when repeated patterns can form a reusable procedure.",
          skip: "Skip only when the selected inputs are clearly weak or routine and unlikely to add durable memory." });
      const threshold = root.kind === "semantic" ? TYPESAFE_SEMANTIC_GATE_CONFIDENCE_THRESHOLD : TYPESAFE_PROCEDURAL_GATE_CONFIDENCE_THRESHOLD;
      if (!decision || (decision.choice !== "run" && decision.choice !== "skip") || !Number.isFinite(decision.confidence)) pipelineGate = "fail-open";
      else pipelineGate = decision.choice === "skip" && decision.confidence >= threshold ? "skip" : "run";
    } catch {
      pipelineGate = "fail-open";
    }
  }
  const decision: ConsolidationTypeSafeDecision = { selectedSourceIndexes: selected, droppedSourceIndexes: dropped,
    compactionFallbackReasons: selection.fallbackReasons, pipelineGate };
  if (pipelineGate === "skip") root.typesafeValidation = { sourceIndexes: eligible, nextSource: 0, complete: eligible.length === 0 };
  await immutablePage<TypeSafeGateReceipt>(kv, receiptKey, { identity, decision });
  root.typesafeDecision = decision;
  root.eligibleSources = selected;
  await saveRoot(kv, root);
  return pipelineGate === "skip" ? validateSkipSourceCut(kv, root) : true;
}

async function validateSkipSourceCut(kv: StateKV, root: PagedConsolidation): Promise<boolean> {
  const validation = root.typesafeValidation;
  if (!validation || validation.complete) return true;
  const source = validation.sourceIndexes[validation.nextSource];
  if (source === undefined) { validation.complete = true; await saveRoot(kv, root); return true; }
  await verifyCapturedSource(kv, root, source);
  validation.nextSource++;
  validation.complete = validation.nextSource >= validation.sourceIndexes.length;
  await saveRoot(kv, root);
  return validation.complete;
}
