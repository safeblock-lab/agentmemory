import type { StateKV } from "./kv.js";

export type GraphWorkCategory = "graph_job" | "graph_recovery" | "terminal_recovery" | "layout" | "unknown";
export type GraphWorkFunction = "mem::graph-extract" | "mem::graph-recover" | "mem::graph-rebuild" | "mem::graph-reset" | "mem::storage-maintenance";
export type GraphWorkPhase = "queued" | "granted" | "batch_admission" | "control_read" | "lease_acquire"
  | "restore" | "input_capture" | "handler" | "checkpoint_finalize" | "heartbeat_drain" | "terminal_statistics"
  | "lease_release" | "native_recovery" | "local_apply" | "terminal_proof" | "terminal_ack" | "inspection";
export interface GraphWorkIdentity { category: GraphWorkCategory; functionId: GraphWorkFunction | null }
export interface GraphWorkRelease { (): void; setPhase(phase: GraphWorkPhase): boolean }
export interface GraphWorkQueueSnapshot {
  version: 1;
  observedAt: string;
  draining: boolean;
  current: null | { category: GraphWorkCategory; functionId: GraphWorkFunction | null; phase: GraphWorkPhase;
    requestedAt: string; grantedAt: string | null; phaseAt: string; elapsedMs: number; phaseElapsedMs: number; waitedMs: number };
  queuedCount: number;
  oldestQueuedAt: string | null;
}
const categories = new Set<GraphWorkCategory>(["graph_job", "graph_recovery", "terminal_recovery", "layout", "unknown"]);
const functions = new Set<GraphWorkFunction>(["mem::graph-extract", "mem::graph-recover", "mem::graph-rebuild", "mem::graph-reset", "mem::storage-maintenance"]);
const phases = new Set<GraphWorkPhase>(["queued", "granted", "batch_admission", "control_read", "lease_acquire", "restore", "input_capture",
  "handler", "checkpoint_finalize", "heartbeat_drain", "terminal_statistics", "lease_release", "native_recovery", "local_apply", "terminal_proof", "terminal_ack", "inspection"]);

export interface GraphWorkTurn {
  readonly granted: boolean;
  readonly ready: Promise<void>;
  release(): boolean;
  setPhase(phase: GraphWorkPhase): boolean;
}

interface QueuedTurn {
  state: "queued" | "granted" | "released";
  identity: GraphWorkIdentity;
  requestedAt: number;
  grantedAt: number | null;
  phase: GraphWorkPhase;
  phaseAt: number;
  resolve(): void;
  onGranted?: () => void;
}

export class GraphWorkQueueDrainingError extends Error {
  readonly code = "GRAPH_WORK_QUEUE_DRAINING";
  readonly retryable = false;
  constructor() { super("Graph work admission is draining"); this.name = "GraphWorkQueueDrainingError"; }
}

interface WorkQueue {
  draining: boolean;
  drained?: Promise<void>;
  resolveDrained?: () => void;
  current?: QueuedTurn;
  pending: QueuedTurn[];
}

const queues = new WeakMap<StateKV, WorkQueue>();

function grantNext(queue: WorkQueue): void {
  if (queue.current) return;
  const turn = queue.pending.shift();
  if (!turn) { queue.resolveDrained?.(); return; }
  queue.current = turn;
  turn.state = "granted";
  turn.grantedAt = Date.now();
  turn.phase = "granted";
  turn.phaseAt = turn.grantedAt;
  turn.resolve();
  if (turn.onGranted) queueMicrotask(() => {
    try { turn.onGranted!(); } catch { /* The owner retains its granted ticket for its next normal tick. */ }
  });
}

export function requestGraphWorkTurn(kv: StateKV, onGranted?: () => void, identity?: GraphWorkIdentity): GraphWorkTurn {
  let queue = queues.get(kv);
  if (!queue) { queue = { pending: [], draining: false }; queues.set(kv, queue); }
  if (queue.draining) throw new GraphWorkQueueDrainingError();
  let resolve!: () => void;
  const ready = new Promise<void>(complete => { resolve = complete; });
  const now = Date.now();
  const turn: QueuedTurn = { state: "queued", resolve, onGranted, requestedAt: now, grantedAt: null, phase: "queued", phaseAt: now,
    identity: { category: identity && categories.has(identity.category) ? identity.category : "unknown",
      functionId: identity?.functionId && functions.has(identity.functionId) ? identity.functionId : null } };
  const owner = queue;
  owner.pending.push(turn);
  grantNext(owner);
  return {
    get granted() { return turn.state === "granted"; },
    ready,
    setPhase(phase) {
      if (turn.state !== "granted" || owner.current !== turn || !phases.has(phase)) return false;
      if (turn.phase !== phase) { turn.phase = phase; turn.phaseAt = Date.now(); }
      return true;
    },
    release() {
      if (turn.state !== "granted") return false;
      turn.state = "released";
      owner.current = undefined;
      grantNext(owner);
      return true;
    },
  };
}

export function beginGraphWorkDrain(kv: StateKV): Promise<void> {
  let queue = queues.get(kv);
  if (!queue) { queue = { pending: [], draining: true }; queues.set(kv, queue); }
  queue.draining = true;
  if (!queue.drained) {
    queue.drained = new Promise<void>(resolve => { queue!.resolveDrained = resolve; });
    if (!queue.current && queue.pending.length === 0) queue.resolveDrained!();
  }
  return queue.drained;
}

export async function acquireGraphWorkTurn(kv: StateKV, identity?: GraphWorkIdentity): Promise<GraphWorkRelease> {
  const turn = requestGraphWorkTurn(kv, undefined, identity);
  await turn.ready;
  return Object.assign(() => { turn.release(); }, { setPhase: (phase: GraphWorkPhase) => turn.setPhase(phase) });
}

export function peekGraphWorkQueue(kv: StateKV): GraphWorkQueueSnapshot {
  const now = Date.now(), queue = queues.get(kv), current = queue?.current;
  return { version: 1, observedAt: new Date(now).toISOString(), draining: queue?.draining ?? false,
    current: current ? { category: current.identity.category, functionId: current.identity.functionId, phase: current.phase,
      requestedAt: new Date(current.requestedAt).toISOString(), grantedAt: current.grantedAt === null ? null : new Date(current.grantedAt).toISOString(),
      phaseAt: new Date(current.phaseAt).toISOString(), elapsedMs: Math.max(0, now - (current.grantedAt ?? current.requestedAt)),
      phaseElapsedMs: Math.max(0, now - current.phaseAt), waitedMs: Math.max(0, (current.grantedAt ?? now) - current.requestedAt) } : null,
    queuedCount: queue?.pending.length ?? 0, oldestQueuedAt: queue?.pending[0] ? new Date(queue.pending[0].requestedAt).toISOString() : null };
}
