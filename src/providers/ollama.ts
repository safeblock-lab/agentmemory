import type { AuxiliaryLlmConfig, LlmCallOptions, LlmTask, MemoryProvider } from "../types.js";
import { fetchWithTimeout } from "./_fetch.js";
import { startLlmCallTelemetry } from "./_llm-logging.js";

const TASK_OUTPUT_TOKENS: Record<LlmTask, number> = {
  graph_extraction: 512,
  temporal_graph_extraction: 512,
  consolidation: 768,
  compression: 768,
  summary: 768,
  entity_extraction: 384,
  classification: 384,
  reflection: 1024,
  conflict_resolution: 1024,
  skill_extraction: 768,
  query_expansion: 384,
  flow_compression: 768,
};

export class OllamaProvider implements MemoryProvider {
  name = "ollama";
  private readonly endpoint: string;
  private readonly model: string;
  private readonly maxTokens: number;
  private readonly timeoutMs: number;
  private readonly noThink: boolean;
  private readonly keepAlive?: string;

  constructor(config: AuxiliaryLlmConfig) {
    this.endpoint = nativeChatEndpoint(config.baseURL);
    this.model = config.model;
    this.maxTokens = config.maxTokens;
    this.timeoutMs = config.timeoutMs;
    this.noThink = config.noThink;
    this.keepAlive = config.keepAlive;
  }

  async compress(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(systemPrompt, userPrompt, "compress", options?.task);
  }

  async summarize(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(systemPrompt, userPrompt, "summarize", options?.task);
  }

  private async call(
    systemPrompt: string,
    userPrompt: string,
    operation: "compress" | "summarize",
    task?: LlmTask,
  ): Promise<string> {
    const telemetry = startLlmCallTelemetry({
      provider: "ollama",
      model: this.model,
      operation,
      thinkingRequest: this.noThink ? "disabled" : "enabled",
    });
    let response: Response;
    try {
      response = await fetchWithTimeout(this.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: this.model,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt },
          ],
          stream: false,
          think: !this.noThink,
          ...(this.keepAlive ? { keep_alive: this.keepAlive } : {}),
          options: {
            num_predict: Math.min(this.maxTokens, task ? TASK_OUTPUT_TOKENS[task] : this.maxTokens),
            temperature: 0,
          },
        }),
      }, this.timeoutMs);
    } catch (error) {
      const errorKind = error instanceof Error && error.name === "AbortError" ? "timeout" : "network";
      telemetry.failure({ errorKind });
      throw new Error(errorKind === "timeout" ? `Ollama request timed out after ${this.timeoutMs}ms` : "Ollama request failed");
    }
    if (!response.ok) {
      telemetry.failure({ httpStatus: response.status, errorKind: "provider_response" });
      throw new Error(`Ollama API error (${response.status})`);
    }
    let payload: unknown;
    try {
      payload = await response.json();
    } catch {
      telemetry.failure({ errorKind: "invalid_response" });
      throw new Error("Ollama returned invalid JSON");
    }
    const content = readContent(payload);
    if (!content) {
      telemetry.failure({ errorKind: "invalid_response" });
      throw new Error("Ollama response did not contain assistant content");
    }
    telemetry.success({ httpStatus: response.status, responseChars: content.length });
    return content;
  }
}

function nativeChatEndpoint(baseURL: string): string {
  const url = new URL(baseURL);
  if (
    url.protocol !== "http:"
    || url.port !== "11434"
    || !["localhost", "127.0.0.1", "[::1]", "::1"].includes(url.hostname)
  ) {
    throw new Error("Native Ollama requires a local http://localhost:11434 endpoint");
  }
  url.pathname = "/api/chat";
  url.search = "";
  return url.toString();
}

function readContent(payload: unknown): string | undefined {
  if (!payload || typeof payload !== "object") return undefined;
  const message = (payload as Record<string, unknown>)["message"];
  if (!message || typeof message !== "object") return undefined;
  const content = (message as Record<string, unknown>)["content"];
  return typeof content === "string" && content.trim() ? content : undefined;
}
