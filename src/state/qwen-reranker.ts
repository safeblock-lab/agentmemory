import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { loadLocalRerankerConfig } from "../config.js";
import { QwenProcess, QwenRuntimeError, type QwenPaths } from "./qwen-reranker-runtime.js";

export function qwenPaths(): QwenPaths {
  const config = loadLocalRerankerConfig();
  const root = resolve(config.assetRoot || resolve(homedir(), ".agentmemory", "reranker", "qwen"));
  const packaged = fileURLToPath(new URL("./qwen-reranker.py", import.meta.url));
  return {
    python: config.python || "python",
    script: config.script || (existsSync(packaged) ? packaged : fileURLToPath(new URL("../../scripts/qwen-reranker.py", import.meta.url))),
    model: resolve(root, "qwen3-reranker-0.6b-q8_0.gguf"),
    cpuRuntime: resolve(root, "cpu"),
    gpuRuntime: resolve(root, "cuda"),
    device: config.device,
  };
}
export interface QwenRuntime {
  strictBounds: true;
  device: "gpu" | "cpu";
  fallbackReason?: string;
  score(query: string, document: string): Promise<number>;
  scoreBatch(query: string, documents: string[], signal?: AbortSignal): Promise<number[]>;
  close(): Promise<void>;
  isClosed(): boolean;
}
function validateReady(value: Record<string, unknown>, device: "gpu" | "cpu"): void {
  if (value.ready !== true || value.device !== device || value.context !== 4096 || !Array.isArray(value.controls)
    || value.controls.length !== 2 || (device === "gpu" && value.gpuVerified !== true)) throw new QwenRuntimeError("protocol", "Qwen readiness proof is invalid.");
  for (const [index, control] of value.controls.entries()) {
    if (!control || typeof control !== "object") throw new QwenRuntimeError("quality", "Qwen startup control missing.");
    const c = control as Record<string, unknown>;
    if (c.yesTokenId !== 9693 || c.noTokenId !== 2152 || c.truncationApplied !== false
      || typeof c.score !== "number" || !Number.isFinite(c.score) || c.score < 0 || c.score > 1
      || typeof c.deltaLogitYesMinusNo !== "number" || !Number.isFinite(c.deltaLogitYesMinusNo)
      || (index === 0 ? c.deltaLogitYesMinusNo <= 0 : c.deltaLogitYesMinusNo >= 0)) {
      throw new QwenRuntimeError("quality", "Qwen startup scoring controls failed.");
    }
  }
}
export async function createQwenReranker(paths = qwenPaths()): Promise<QwenRuntime> {
  for (const path of [paths.script, paths.model, paths.cpuRuntime]) {
    if (!existsSync(path)) throw new QwenRuntimeError("assets", "Qwen assets missing: provision AGENTMEMORY_QWEN_ASSET_ROOT and packaged scorer before starting.");
  }
  let device: "gpu" | "cpu" = paths.device === "cpu" ? "cpu" : "gpu";
  let fallbackReason: string | undefined;
  let worker = new QwenProcess(paths, device);
  try {
    validateReady(await worker.ready, device);
  } catch (error) {
    await worker.close();
    if (device !== "gpu" || !(error instanceof QwenRuntimeError)
      || !["gpu-unavailable", "gpu-allocation"].includes(error.code)) throw error;
    fallbackReason = error.message;
    device = "cpu";
    console.warn("[agentmemory] Qwen uses CPU: compatible GPU or full-workload VRAM unavailable.");
    worker = new QwenProcess(paths, device);
    try { validateReady(await worker.ready, device); } catch (cpuError) { await worker.close(); throw cpuError; }
  }
  async function scoreBatch(query: string, documents: string[], signal?: AbortSignal): Promise<number[]> {
      if (typeof query !== "string" || query.length > 256 || !documents.length || documents.length > 50
        || documents.some((document) => typeof document !== "string" || document.length > 8192)) {
        throw new QwenRuntimeError("input", "Qwen requires query <=256 characters and 1..50 documents <=8192 characters; truncation is forbidden.");
      }
      return worker.scores(query, documents, signal);
  }
  return {
    strictBounds: true, device, fallbackReason,
    async score(query, document) { return (await scoreBatch(query, [document]))[0]; },
    scoreBatch,
    close: () => worker.close(),
    isClosed: () => worker.isClosed(),
  };
}
