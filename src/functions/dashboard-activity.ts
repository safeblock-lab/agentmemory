import { subscribeStateWrites } from "../state/kv.js";
import { KV } from "../state/schema.js";
import { stripPrivateData } from "./privacy.js";

export interface DashboardActivityItem {
  id: string;
  kind: "session" | "observation";
  sessionId: string;
  timestamp: string;
  title: string;
  agentId?: string;
}

export interface DashboardActivity {
  source: "runtime-writes";
  availableSince: string;
  partial: true;
  items: DashboardActivityItem[];
}

function safeText(value: unknown, maxLength: number): string | undefined {
  if (typeof value !== "string" || !value || value.length > 4096) return undefined;
  return stripPrivateData(value).replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, maxLength);
}

export function createDashboardActivity() {
  const availableSince = new Date().toISOString();
  const recent = new Map<string, DashboardActivityItem>();
  subscribeStateWrites(({ scope, key, value }) => {
    const session = scope === KV.sessions;
    if (!session && !scope.startsWith("mem:obs:")) return;
    if (key.length > 256 || scope.length > 512 || !value || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    const sessionId = safeText(session ? key : record.sessionId ?? scope.slice("mem:obs:".length), 256);
    const id = safeText(key, 256);
    if (!sessionId || !id) return;
    const agentId = safeText(record.agentId, 128);
    const title = session
      ? `Session ${record.status === "completed" ? "completed" : record.status === "abandoned" ? "abandoned" : "updated"}`
      : safeText(record.title, 160) ?? "Observation captured";
    const dedupKey = `${scope}\u0000${key}`;
    recent.delete(dedupKey);
    recent.set(dedupKey, {
      id, kind: session ? "session" : "observation", sessionId,
      timestamp: new Date().toISOString(), title, ...(agentId ? { agentId } : {}),
    });
    if (recent.size > 50) recent.delete(recent.keys().next().value!);
  });
  return {
    snapshot(agentId?: string): DashboardActivity {
      return {
        source: "runtime-writes", availableSince, partial: true,
        items: [...recent.values()].reverse().filter(item => !agentId || item.agentId === agentId).slice(0, 10).map(item => ({ ...item })),
      };
    },
  };
}
