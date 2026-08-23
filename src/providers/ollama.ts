import type { AuxiliaryLlmConfig, LlmCallOptions, LlmTask, MemoryProvider } from "../types.js";
import { jsonrepair } from "jsonrepair";
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

const NO_THINK_OUTPUT_FORMAT = {
  type: "object",
  properties: { output: { type: "string" } },
  required: ["output"],
  additionalProperties: false,
} as const;

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
      const messages = this.noThink
        ? [
          {
            role: "system",
            content: `${systemPrompt}\n\nReturn exactly one JSON object with one string field named output. Put the complete final response in output. Do not include reasoning or any other field.`,
          },
          { role: "user", content: userPrompt },
        ]
        : [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ];
      response = await fetchWithTimeout(this.endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          model: this.model,
          messages,
          stream: false,
          think: !this.noThink,
          ...(this.noThink ? { format: NO_THINK_OUTPUT_FORMAT } : {}),
          ...(this.keepAlive ? { keep_alive: this.keepAlive === "-1" ? -1 : this.keepAlive } : {}),
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
    const rawContent = readContent(payload);
    const unwrapped = rawContent && this.noThink ? unwrapOutput(rawContent) : rawContent;
    const content = unwrapped && this.noThink ? repairStructuredJson(stripThinking(unwrapped)) : unwrapped;
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

function stripThinking(content: string): string {
  const closedTrace = content.lastIndexOf("</think>");
  if (closedTrace !== -1) return content.slice(closedTrace + "</think>".length).trim();
  return content.replace(/<think>[\s\S]*?<\/think>/g, "").trim();
}

function unwrapOutput(content: string): string {
  const parsed = parseObjectEnvelope(content);
  if (parsed) {
    const output = parsed.output;
    if (typeof output === "string" && output.trim()) return output;
  }
  return content;
}

function parseObjectEnvelope(content: string): Record<string, unknown> | undefined {
  const trimmed = content.trim();
  const fenced = trimmed.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i)?.[1] ?? trimmed;
  for (const candidate of [fenced, extractObjectCandidate(fenced)]) {
    if (!candidate) continue;
    try {
      const parsed: unknown = JSON.parse(candidate);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as Record<string, unknown>;
      }
    } catch {
      try {
        const parsed: unknown = JSON.parse(jsonrepair(candidate));
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          return parsed as Record<string, unknown>;
        }
      } catch {
        // An unrepairable envelope stays unusable.
      }
    }
  }
  return undefined;
}

function repairStructuredJson(content: string): string {
  const trimmed = content.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return content;
  try {
    JSON.parse(trimmed);
    return content;
  } catch {
    try {
      return jsonrepair(trimmed);
    } catch {
      return content;
    }
  }
}

function extractObjectCandidate(content: string): string | undefined {
  const first = content.indexOf("{");
  const last = content.lastIndexOf("}");
  return first >= 0 && last > first ? content.slice(first, last + 1) : undefined;
}
