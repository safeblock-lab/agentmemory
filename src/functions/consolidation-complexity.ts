export interface ConsolidationComplexityInput {
  prompt: string;
  maxAuxiliaryInputChars?: number;
}

export interface ConsolidationComplexity {
  complex: boolean;
  reasons: string[];
}

const ASSIGNMENT = /(?:^|\n)\s*([A-Za-z][\w.-]*[_.-][\w.-]*)\s*[:=]\s*([^\n]{1,240})/g;

export function assessConsolidationComplexity(
  input: ConsolidationComplexityInput,
): ConsolidationComplexity {
  const reasons: string[] = [];
  if (
    input.maxAuxiliaryInputChars !== undefined &&
    input.prompt.length > input.maxAuxiliaryInputChars
  ) {
    reasons.push("auxiliary_input_limit");
  }

  const valuesByKey = new Map<string, Set<string>>();
  for (const match of input.prompt.matchAll(ASSIGNMENT)) {
    const key = match[1].toLowerCase();
    const value = match[2].trim().toLowerCase();
    if (!value) continue;
    const values = valuesByKey.get(key) ?? new Set<string>();
    values.add(value);
    valuesByKey.set(key, values);
  }
  if ([...valuesByKey.values()].some((values) => values.size > 1)) {
    reasons.push("conflicting_structured_values");
  }

  if (/\b(?:supersedes|superseded by|replaced by|temporal conflict)\b/i.test(input.prompt)) {
    reasons.push("temporal_conflict_marker");
  }

  return { complex: reasons.length > 0, reasons };
}
