import type { LlmCallOptions, MemoryProvider, StructuredLlmRequest, PreparedStructuredLlmCall } from "../types.js";
import { canPrepareStructuredCall, prepareStructuredCall } from "./structured-output-contract.js";

export class FallbackChainProvider implements MemoryProvider {
  name: string;
  prepareGraphAutomaticCall?: MemoryProvider["prepareGraphAutomaticCall"];
  prepareStructuredCall?: (request: StructuredLlmRequest) => PreparedStructuredLlmCall;

  constructor(private providers: MemoryProvider[]) {
    this.name = `fallback(${providers.map((p) => p.name).join(" -> ")})`;
    const first = providers[0];
    this.prepareGraphAutomaticCall = first?.prepareGraphAutomaticCall?.bind(first);
    if (canPrepareStructuredCall(first)) {
      this.prepareStructuredCall = (request) => prepareStructuredCall(first, request);
    }
  }

  async compress(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    if (options?.outputContract || options?.responseFormat) return this.first().compress(systemPrompt, userPrompt, options);
    return this.tryAll((p) => p.compress(systemPrompt, userPrompt, options));
  }

  async summarize(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    if (options?.outputContract || options?.responseFormat) return this.first().summarize(systemPrompt, userPrompt, options);
    return this.tryAll((p) => p.summarize(systemPrompt, userPrompt, options));
  }

  private first(): MemoryProvider {
    const provider = this.providers[0];
    if (!provider) throw new Error("No providers available");
    return provider;
  }

  private async tryAll(
    fn: (p: MemoryProvider) => Promise<string>,
  ): Promise<string> {
    let lastError: Error | null = null;
    for (const provider of this.providers) {
      try {
        return await fn(provider);
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
      }
    }
    throw lastError || new Error("No providers available");
  }
}
