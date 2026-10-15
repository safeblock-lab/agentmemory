import { afterEach, describe, expect, it, vi } from "vitest";
import { createNativeStateDiagnostics } from "../src/cli/native-state-diagnostics.ts";

const sqliteLog = (message: string) => `2026-10-13T10:00:00Z WARN iii::workers::state::adapters::sqlite: ${message}`;
const preparationWarning = (fields: string) => sqliteLog(
  `state SQLite preparation failed; reconcile durable checkpoint before retry ${fields}`,
);
const graphWarning = (fields: string) => sqliteLog(
  `graph preparation refused; durable checkpoint requires reconciliation ${fields}`,
);
const rejectedOperation = (details: string) => sqliteLog(`state operation rejected error=${details}`);

function captureDiagnostics() {
  const reports: string[] = [];
  return { reports, diagnostics: createNativeStateDiagnostics((message) => reports.push(message)) };
}

afterEach(() => vi.useRealTimers());

describe("createNativeStateDiagnostics", () => {
  it("preserves SQLite and codec classifications and recognizes retrieval resource limits", () => {
    const { reports, diagnostics } = captureDiagnostics();
    diagnostics.append(Buffer.from([
      rejectedOperation('TxError("STATE_RETRIEVAL_RESOURCE_LIMIT")'),
      rejectedOperation('SqliteFailure(Error { code: DatabaseBusy, extended_code: 5 }, Some("C:\\\\private\\\\store.db"))'),
      rejectedOperation("SQLite state value length mismatch at C:\\\\private\\\\store.db"),
      rejectedOperation('prompt="private prompt" exception="private exception"'),
    ].join("\n")));

    expect(reports).toEqual([
      "[agentmemory] native state operation rejected: STATE_RETRIEVAL_RESOURCE_LIMIT",
      "[agentmemory] native state operation rejected: sqlite=DatabaseBusy extended_code=5",
      "[agentmemory] native state operation rejected: codec-length",
    ]);
    expect(reports.join("\n")).not.toContain("private");
    diagnostics.finish();
  });

  it("reports allowlisted PreparationBudget stage and failure class only", () => {
    const { reports, diagnostics } = captureDiagnostics();
    diagnostics.append(Buffer.from(`${preparationWarning(
      'operation=63d9c52a-1d21-4d5b-8d7e-9202233a0f00 action="index_prepare C:\\\\private\\\\path" stage="retrieval" failure_class="response_deadline_unknown_outcome" value="private value"',
    )}\n`));

    expect(reports).toEqual([
      "[agentmemory] native state preparation failed: category=preparation_budget stage=retrieval failure_class=response_deadline_unknown_outcome",
    ]);
    expect(reports.join("\n")).not.toMatch(/63d9c52a|private|index_prepare|C:\\/);
    diagnostics.finish();
  });

  it("reports allowlisted graph prerequisite and failure class only", () => {
    const { reports, diagnostics } = captureDiagnostics();
    diagnostics.append(Buffer.from(`${graphWarning(
      'failure_class="prerequisite_resource_limit" prerequisite="node_index" required_bytes=200003 max_bytes=65536',
    )}\n`));

    expect(reports).toEqual([
      "[agentmemory] native graph preparation refused: category=graph_prerequisite prerequisite=node_index failure_class=prerequisite_resource_limit",
    ]);
    diagnostics.finish();
  });

  it("drops forged fields and never echoes identifiers, paths, URLs, prompts, exceptions, or secrets", () => {
    const { reports, diagnostics } = captureDiagnostics();
    const privateFields = 'operation="operation-secret" action="https://secret.example/private?key=secret" value="private-value" prompt="private-prompt" exception="private-exception"';
    const valid = preparationWarning(`${privateFields} stage="ack" failure_class="stage_error"`);
    const forgedPreparation = preparationWarning(
      'stage="C:\\\\private\\\\path" failure_class="stage_error secret-token"',
    );
    const forgedGraph = graphWarning(
      'failure_class="prerequisite_resource_limit secret-token" prerequisite="/private/identifier"',
    );
    diagnostics.append(Buffer.from([valid, forgedPreparation, forgedGraph].join("\n")));
    diagnostics.finish();

    expect(reports).toEqual([
      "[agentmemory] native state preparation failed: category=preparation_budget stage=ack failure_class=stage_error",
    ]);
    expect(reports.join("\n")).not.toMatch(/operation-secret|secret\.example|secret-token|private|https:\/\/|C:\\/);
  });

  it("handles fragmented records and a final unterminated line", () => {
    const { reports, diagnostics } = captureDiagnostics();
    const fragmented = Buffer.from(rejectedOperation('TxError("STATE_RETRIEVAL_RESOURCE_LIMIT")'));
    diagnostics.append(fragmented.subarray(0, 27));
    diagnostics.append(Buffer.concat([fragmented.subarray(27), Buffer.from("\n")]));
    diagnostics.append(Buffer.from(graphWarning(
      'failure_class="invalid_prerequisite" prerequisite="projection"',
    )));
    diagnostics.finish();

    expect(reports).toEqual([
      "[agentmemory] native state operation rejected: STATE_RETRIEVAL_RESOURCE_LIMIT",
      "[agentmemory] native graph preparation refused: category=graph_prerequisite prerequisite=projection failure_class=invalid_prerequisite",
    ]);
  });

  it("discards oversized records and resumes after their newline", () => {
    const { reports, diagnostics } = captureDiagnostics();
    const oversized = preparationWarning(
      'stage="retrieval" failure_class="stage_error"',
    ).padEnd(16 * 1024 + 1, "x");
    const next = rejectedOperation('TxError("STATE_RETRIEVAL_RESOURCE_LIMIT")');
    diagnostics.append(Buffer.from(`${oversized}\n${next}\n`));
    diagnostics.finish();

    expect(reports).toEqual([
      "[agentmemory] native state operation rejected: STATE_RETRIEVAL_RESOURCE_LIMIT",
    ]);
  });

  it("caps reports at ten per minute and resets after the window", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-10-13T10:00:00Z"));
    const { reports, diagnostics } = captureDiagnostics();
    const line = `${rejectedOperation('TxError("STATE_RETRIEVAL_RESOURCE_LIMIT")')}\n`;
    diagnostics.append(Buffer.from(line.repeat(11)));
    expect(reports).toHaveLength(10);

    vi.advanceTimersByTime(60_001);
    diagnostics.append(Buffer.from(line));
    expect(reports).toHaveLength(11);
    diagnostics.finish();
  });
});
