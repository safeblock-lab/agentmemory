const MAX_LINE_BYTES = 16 * 1024;
const WINDOW_MS = 60_000;
const MAX_REPORTS_PER_WINDOW = 10;

const SQLITE_CODES = [
  "InternalMalfunction", "PermissionDenied", "OperationAborted",
  "DatabaseBusy", "DatabaseLocked", "OutOfMemory", "ReadOnly",
  "OperationInterrupted", "SystemIoFailure", "DatabaseCorrupt",
  "NotFound", "DiskFull", "CannotOpen", "FileLockingProtocolFailed",
  "SchemaChanged", "TooBig", "ConstraintViolation", "TypeMismatch",
  "ApiMisuse", "NoLargeFileSupport", "AuthorizationForStatementDenied",
  "ParameterOutOfRange", "NotADatabase", "Unknown",
] as const;

const STATE_CODES = [
  "STATE_TX_FAILED", "STATE_TX_INVALID_REQUEST", "STATE_TX_UNSUPPORTED",
  "STATE_TX_LIMIT_EXCEEDED", "STATE_RECORD_TOO_LARGE",
  "STATE_TX_GENERATION_STALE", "STATE_TX_FENCED", "STATE_TX_LEASE_BUSY",
  "STATE_TX_CHECKPOINT_CONFLICT", "STATE_TX_REPLAY_CONFLICT",
  "STATE_TX_CONFLICT", "STATE_GRAPH_RECOVERY_REQUIRED",
  "STATE_MIGRATION_INCOMPLETE", "STATE_MIGRATION_RECORD_TOO_LARGE",
  "STATE_SCOPE_REVISION_INVALID_REQUEST",
] as const;

function classify(line: string): string | undefined {
  if (!line.includes("state operation rejected")) return;
  if (line.includes("SQLite state value length mismatch")) return "codec-length";
  if (line.includes("SQLite state compressed value is incomplete or has trailing bytes")) {
    return "codec-corrupt";
  }
  if (line.includes("SQLite state value exceeds native format limit")) return "record-limit";

  const sqlite = /SqliteFailure\s*\(\s*Error\s*\{\s*code:\s*([A-Za-z]+),\s*extended_code:\s*(-?\d{1,10})\s*\}/.exec(line);
  if (sqlite) {
    const code = SQLITE_CODES.find((known) => known === sqlite[1]);
    const extended = Number(sqlite[2]);
    if (code && Number.isInteger(extended) && extended >= -2147483648 && extended <= 2147483647) {
      return `sqlite=${code} extended_code=${extended}`;
    }
  }
  return STATE_CODES.find((code) => new RegExp(`\\b${code}\\b`).test(line));
}

export function createNativeStateDiagnostics(
  emit: (message: string) => void,
): { append(chunk: Buffer): void; finish(): void } {
  let fragments: Buffer[] = [];
  let size = 0;
  let discarding = false;
  let windowStarted = Date.now();
  let reports = 0;

  function reportLine(): void {
    if (!discarding && size > 0) {
      const category = classify(Buffer.concat(fragments, size).toString("utf8"));
      if (category) {
        const now = Date.now();
        if (now - windowStarted >= WINDOW_MS) {
          windowStarted = now;
          reports = 0;
        }
        if (reports < MAX_REPORTS_PER_WINDOW) {
          reports += 1;
          emit(`[agentmemory] native state operation rejected: ${category}`);
        }
      }
    }
    fragments = [];
    size = 0;
    discarding = false;
  }

  return {
    append(chunk) {
      let start = 0;
      while (start < chunk.length) {
        const newline = chunk.indexOf(10, start);
        const end = newline === -1 ? chunk.length : newline;
        const length = end - start;
        if (!discarding && size + length <= MAX_LINE_BYTES) {
          if (length > 0) fragments.push(Buffer.from(chunk.subarray(start, end)));
          size += length;
        } else {
          fragments = [];
          size = 0;
          discarding = true;
        }
        if (newline === -1) break;
        reportLine();
        start = newline + 1;
      }
    },
    finish: reportLine,
  };
}
