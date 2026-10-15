import { randomUUID } from "node:crypto";

export function recordContextEmission(
  url: string,
  headers: Record<string, string>,
  sessionId: string,
  source: "session-start" | "pre-tool-use" | "pre-compact",
  text: string,
  candidateTokens?: number,
): void {
  fetch(`${url}/agentmemory/observe`, {
    method: "POST", headers,
    body: JSON.stringify({
      sessionId, project: "context-telemetry", cwd: process.cwd(),
      hookType: "notification", timestamp: new Date().toISOString(),
      data: { event: "context_emitted_v1", eventId: randomUUID(), source, text, candidateTokens },
    }),
    signal: AbortSignal.timeout(1000),
  }).catch(() => {});
  setTimeout(() => process.exit(0), 500).unref();
}
