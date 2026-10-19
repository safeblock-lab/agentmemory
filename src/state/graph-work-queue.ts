import type { StateKV } from "./kv.js";

export interface GraphWorkTurn {
  readonly granted: boolean;
  readonly ready: Promise<void>;
  release(): boolean;
}

interface QueuedTurn {
  state: "queued" | "granted" | "released";
  resolve(): void;
  onGranted?: () => void;
}

interface WorkQueue {
  current?: QueuedTurn;
  pending: QueuedTurn[];
}

const queues = new WeakMap<StateKV, WorkQueue>();

function grantNext(queue: WorkQueue): void {
  if (queue.current) return;
  const turn = queue.pending.shift();
  if (!turn) return;
  queue.current = turn;
  turn.state = "granted";
  turn.resolve();
  if (turn.onGranted) queueMicrotask(() => {
    try { turn.onGranted!(); } catch { /* The owner retains its granted ticket for its next normal tick. */ }
  });
}

export function requestGraphWorkTurn(kv: StateKV, onGranted?: () => void): GraphWorkTurn {
  let queue = queues.get(kv);
  if (!queue) { queue = { pending: [] }; queues.set(kv, queue); }
  let resolve!: () => void;
  const ready = new Promise<void>(complete => { resolve = complete; });
  const turn: QueuedTurn = { state: "queued", resolve, onGranted };
  const owner = queue;
  owner.pending.push(turn);
  grantNext(owner);
  return {
    get granted() { return turn.state === "granted"; },
    ready,
    release() {
      if (turn.state !== "granted") return false;
      turn.state = "released";
      owner.current = undefined;
      grantNext(owner);
      return true;
    },
  };
}

export async function acquireGraphWorkTurn(kv: StateKV): Promise<() => void> {
  const turn = requestGraphWorkTurn(kv);
  await turn.ready;
  return () => { turn.release(); };
}
