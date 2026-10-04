import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";

export interface QwenPaths {
  python: string;
  script: string;
  model: string;
  cpuRuntime: string;
  gpuRuntime: string;
  device: "auto" | "cpu";
}
export class QwenRuntimeError extends Error {
  constructor(public readonly code: string, message: string) { super(message); }
}
interface Pending {
  resolve(value: Record<string, unknown>): void;
  reject(error: Error): void;
  timer: ReturnType<typeof setTimeout>;
}
const MAX_RESPONSE_BYTES = 64 * 1024;

export class QwenProcess {
  private child: ChildProcessWithoutNullStreams;
  private pending: Pending | undefined;
  private buffer = "";
  private sequence = 0;
  private closing = false;
  private closed: Promise<void>;
  private exited = false;
  private exitHook: () => void;
  readonly ready: Promise<Record<string, unknown>>;

  constructor(paths: QwenPaths, device: "gpu" | "cpu", startupTimeoutMs = 120_000) {
    this.child = spawn(paths.python, [
      "-I", "-u", paths.script, "--model", paths.model,
      "--runtime", device === "gpu" ? paths.gpuRuntime : paths.cpuRuntime, "--device", device,
    ], { windowsHide: true, stdio: ["pipe", "pipe", "pipe"] });
    this.ready = this.wait(startupTimeoutMs);
    this.closed = new Promise((resolve) => this.child.once("close", () => {
      this.exited = true;
      this.fail(new QwenRuntimeError("exit", "Qwen process closed."));
      process.removeListener("exit", this.exitHook);
      resolve();
    }));
    this.exitHook = () => { this.child.kill(); };
    process.once("exit", this.exitHook);
    this.child.on("error", () => this.fail(new QwenRuntimeError("runtime", "Cannot start Qwen: configure AGENTMEMORY_QWEN_PYTHON with 64-bit Python.")));
    this.child.stdin.on("error", () => this.fail(new QwenRuntimeError("transport", "Qwen input pipe closed.")));
    this.child.stderr.on("data", () => { /* Drain native diagnostics without exposing retrieved text. */ });
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (chunk: string) => this.receive(chunk));
  }
  get pid(): number | undefined { return this.child.pid; }
  isClosed(): boolean { return this.closing || this.exited; }

  private wait(timeoutMs: number): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.fail(new QwenRuntimeError("timeout", "Qwen startup or inference exceeded its deadline.")), timeoutMs);
      this.pending = { resolve, reject, timer };
    });
  }
  private fail(error: Error): void {
    const pending = this.pending;
    if (pending) { clearTimeout(pending.timer); this.pending = undefined; pending.reject(error); }
    if (!this.closing) { this.closing = true; this.child.kill(); }
  }
  private receive(chunk: string): void {
    this.buffer += chunk;
    if (Buffer.byteLength(this.buffer) > MAX_RESPONSE_BYTES) {
      this.fail(new QwenRuntimeError("protocol", "Qwen response exceeds its byte limit."));
      return;
    }
    let end: number;
    while ((end = this.buffer.indexOf("\n")) >= 0) {
      const line = this.buffer.slice(0, end);
      this.buffer = this.buffer.slice(end + 1);
      try {
        const value: unknown = JSON.parse(line);
        if (!value || typeof value !== "object" || Array.isArray(value) || !this.pending) throw new Error("Unexpected response.");
        const record = value as Record<string, unknown>;
        if (typeof record.error === "string") {
          const codes = new Set(["gpu-unavailable", "gpu-allocation", "integrity", "quality", "runtime"]);
          throw new QwenRuntimeError(typeof record.code === "string" && codes.has(record.code) ? record.code : "inference", record.error);
        }
        const pending = this.pending;
        clearTimeout(pending.timer); this.pending = undefined; pending.resolve(record);
      } catch (error) {
        this.fail(error instanceof QwenRuntimeError ? error : new QwenRuntimeError("protocol", "Qwen returned invalid IPC data."));
      }
    }
  }

  async scores(query: string, documents: string[], signal?: AbortSignal, timeoutMs = 55_000): Promise<number[]> {
    if (this.closing || this.pending) throw new QwenRuntimeError("busy", "Qwen process is closed or already scoring.");
    if (signal?.aborted) throw new QwenRuntimeError("cancelled", "Qwen inference cancelled.");
    const id = ++this.sequence;
    const payload = JSON.stringify({ id, pairs: documents.map((document) => ({ query, document })) }) + "\n";
    if (Buffer.byteLength(payload) > 2_000_000) throw new QwenRuntimeError("input", "Qwen batch exceeds its byte limit.");
    const result = this.wait(timeoutMs);
    const abort = () => this.fail(new QwenRuntimeError("cancelled", "Qwen inference cancelled."));
    signal?.addEventListener("abort", abort, { once: true });
    this.child.stdin.write(payload);
    try {
      const response = await result;
      if (response.id !== id || !Array.isArray(response.scores) || response.scores.length !== documents.length
        || !response.scores.every((score) => typeof score === "number" && Number.isFinite(score) && score >= 0 && score <= 1)) {
        throw new QwenRuntimeError("protocol", "Qwen score count, identity or probability is invalid.");
      }
      return response.scores as number[];
    } catch (error) {
      await this.close();
      throw error;
    } finally { signal?.removeEventListener("abort", abort); }
  }
  async close(): Promise<void> {
    if (this.exited) return;
    this.closing = true;
    if (this.pending) { clearTimeout(this.pending.timer); this.pending.reject(new QwenRuntimeError("closed", "Qwen process closed.")); this.pending = undefined; }
    this.child.stdin.end();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([this.closed, new Promise<void>((resolve) => {
        timer = setTimeout(() => { this.child.kill(); resolve(); }, 2000);
      })]);
      if (!this.exited) {
        await Promise.race([this.closed, new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new QwenRuntimeError("cleanup", "Qwen child did not close after termination.")), 3000);
        })]);
      }
    } finally { if (timer) clearTimeout(timer); }
  }
}
