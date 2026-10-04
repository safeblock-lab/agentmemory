import { isAbsolute, resolve } from "node:path";
import { loadLocalRerankerConfig } from "../config.js";
import { createQwenReranker } from "./qwen-reranker.js";

interface RerankerRuntime {
  score(query: string, document: string): Promise<number>;
  strictBounds?: boolean;
  scoreBatch?(query: string, documents: string[], signal?: AbortSignal): Promise<number[]>;
  close?(): Promise<void>;
  isClosed?(): boolean;
}

const MODEL = "Xenova/ms-marco-MiniLM-L-6-v2";
export interface RerankerConfiguration {
  modelId: string;
  revision: string;
  modelPath: string;
  cacheDirectory: string;
}
let candidate: RerankerConfiguration | null = null;
export function configureCandidateReranker(configuration: RerankerConfiguration): void {
  if (runtime || loading || unavailable) throw new Error("Reranker configuration is immutable after loading starts.");
  if (configuration.modelId !== "onnx-community/bge-reranker-v2-m3-ONNX"
    || configuration.revision !== "90213ffc6a8e6f051a6331269a0f5526cdd896f6"
    || !isAbsolute(configuration.modelPath) || !isAbsolute(configuration.cacheDirectory)) {
    throw new Error("Invalid pinned local candidate reranker configuration.");
  }
  candidate = { ...configuration };
}
export function rerankerConfiguration(): RerankerConfiguration {
  return candidate ? { ...candidate } : {
    modelId: MODEL, revision: "a09144355adeed5f58c8ed011d209bf8ee5a1fec",
    modelPath: resolve(process.cwd(), ".cache", "agentmemory", "reranker", ...MODEL.split("/")),
    cacheDirectory: resolve(process.cwd(), ".cache", "agentmemory", "reranker"),
  };
}
let runtime: RerankerRuntime | null = null;
let loading: Promise<RerankerRuntime | null> | null = null;
let unavailable = false;

export function isRerankerLoaded(): boolean {
  return runtime !== null && !runtime.isClosed?.();
}

export async function loadReranker(): Promise<RerankerRuntime | null> {
  if (runtime?.isClosed?.()) {
    const failed = runtime;
    await failed.close?.();
    if (runtime === failed) runtime = null;
  }
  if (runtime || unavailable) return runtime;
  if (loading) return loading;
  loading = (async () => {
    try {
      if (!candidate && loadLocalRerankerConfig().provider === "qwen") {
        runtime = await createQwenReranker();
        return runtime;
      }
      const { AutoTokenizer, AutoModelForSequenceClassification } = await import(
        "@huggingface/transformers"
      );
      const configuration = rerankerConfiguration();
      const modelPath = configuration.modelPath;
      const options = {
        local_files_only: true,
        cache_dir: configuration.cacheDirectory,
      };
      const tokenizer = await AutoTokenizer.from_pretrained(modelPath, options);
      const model = await AutoModelForSequenceClassification.from_pretrained(modelPath, {
        ...options,
        dtype: "q8",
        device: "cpu",
        session_options: { intraOpNumThreads: 1, interOpNumThreads: 1 },
      });
      runtime = {
        async score(query, document) {
          const inputs = tokenizer(query, {
            text_pair: document,
            truncation: true,
            max_length: 512,
            padding: false,
          });
          const output = await model(inputs);
          const logits = output.logits;
          if (logits?.dims?.length !== 2 || logits.dims[0] !== 1 || logits.dims[1] !== 1
            || logits.data?.length !== 1) {
            throw new Error("Expected one cross-encoder relevance logit.");
          }
          const score = logits.data[0];
          if (typeof score !== "number" || !Number.isFinite(score)) {
            throw new Error("Invalid cross-encoder relevance logit.");
          }
          return score;
        },
      };
      return runtime;
    } catch (error) {
      if (!candidate && loadLocalRerankerConfig().provider === "qwen") throw error;
      unavailable = true;
      console.warn("[agentmemory] Local reranker unavailable; provision cached model assets before restarting.");
      return null;
    } finally {
      loading = null;
    }
  })();
  return loading;
}

export async function closeReranker(): Promise<void> {
  const failures: unknown[] = [];
  try { if (loading) await loading; } catch (error) { failures.push(error); }
  const current = runtime;
  try {
    await current?.close?.();
    if (runtime === current) runtime = null;
  } catch (error) { failures.push(error); }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, "Reranker loading and cleanup failed.");
}
