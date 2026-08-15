import type { MemoryProvider } from "../types.js";
import { fetchWithTimeout } from "./_fetch.js";
import { extractLlmTokenUsage, startLlmCallTelemetry } from "./_llm-logging.js";

export class OpenRouterProvider implements MemoryProvider {
  name: string;
  private apiKey: string;
  private model: string;
  private maxTokens: number;
  private baseUrl: string;

  constructor(
    apiKey: string,
    model: string,
    maxTokens: number,
    baseUrl: string,
  ) {
    this.apiKey = apiKey;
    this.model = model;
    this.maxTokens = maxTokens;
    this.baseUrl = baseUrl;
    this.name = baseUrl.includes("openrouter") ? "openrouter" : "gemini";
  }

  async compress(systemPrompt: string, userPrompt: string): Promise<string> {
    return this.call(systemPrompt, userPrompt, "compress");
  }

  async summarize(systemPrompt: string, userPrompt: string): Promise<string> {
    return this.call(systemPrompt, userPrompt, "summarize");
  }

  private async call(
    systemPrompt: string,
    userPrompt: string,
    operation: "compress" | "summarize",
  ): Promise<string> {
    const telemetry = startLlmCallTelemetry({ provider: this.name, model: this.model, operation });
    let response: Response;
    try {
      response = await fetchWithTimeout(this.baseUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${this.apiKey}`,
        ...(this.baseUrl.includes("openrouter")
          ? { "HTTP-Referer": "https://github.com/rohitg00/agentmemory" }
          : {}),
      },
      body: JSON.stringify({
        model: this.model,
        max_tokens: this.maxTokens,
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
      }),
      });
    } catch (error) {
      telemetry.failure({ errorKind: error instanceof Error && error.name === "AbortError" ? "timeout" : "network" });
      throw error;
    }

    if (!response.ok) {
      telemetry.failure({ httpStatus: response.status, errorKind: "provider_response" });
      const text = await response.text();
      throw new Error(`${this.name} API error (${response.status}): ${text}`);
    }

    let data: Record<string, unknown>;
    try {
      data = (await response.json()) as Record<string, unknown>;
    } catch {
      telemetry.failure({ httpStatus: response.status, errorKind: "invalid_response" });
      throw new Error(`${this.name} returned an invalid JSON response.`);
    }
    const choices = data.choices as
      | Array<{ message: { content: string } }>
      | undefined;
    const content = choices?.[0]?.message?.content;
    if (!content) {
      telemetry.failure({ httpStatus: response.status, errorKind: "invalid_response" });
      throw new Error(
        `${this.name} returned unexpected response: ${JSON.stringify(data).slice(0, 200)}`,
      );
    }
    telemetry.success({ httpStatus: response.status, usage: extractLlmTokenUsage(data["usage"]), responseChars: content.length });
    return content;
  }
}
