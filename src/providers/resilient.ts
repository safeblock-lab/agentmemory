import type { MemoryProvider, CircuitBreakerState, LlmCallOptions, StructuredLlmRequest, PreparedStructuredLlmCall } from "../types.js";
import { canPrepareStructuredCall, prepareStructuredCall, singleStructuredDispatch } from "./structured-output-contract.js";
import { CircuitBreaker } from "./circuit-breaker.js";

export class ResilientProvider implements MemoryProvider {
  private breaker = new CircuitBreaker();
  name: string;
  prepareGraphAutomaticCall?: MemoryProvider["prepareGraphAutomaticCall"];
  prepareStructuredCall?: (request: StructuredLlmRequest) => PreparedStructuredLlmCall;

  constructor(private inner: MemoryProvider) {
    this.name = `resilient(${inner.name})`;
    this.prepareGraphAutomaticCall = inner.prepareGraphAutomaticCall?.bind(inner);
    if (canPrepareStructuredCall(inner)) {
      this.prepareStructuredCall = (request) => {
        const prepared = prepareStructuredCall(inner, request);
        return singleStructuredDispatch({ ...prepared.binding }, () => this.call(() => prepared.execute()));
      };
    }
  }

  private async call(fn: () => Promise<string>): Promise<string> {
    if (!this.breaker.isAllowed) {
      throw new Error("circuit_breaker_open");
    }
    try {
      const result = await fn();
      this.breaker.recordSuccess();
      return result;
    } catch (err) {
      this.breaker.recordFailure();
      throw err;
    }
  }

  async compress(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(() => this.inner.compress(systemPrompt, userPrompt, options));
  }

  async summarize(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(() => this.inner.summarize(systemPrompt, userPrompt, options));
  }

  get circuitState(): CircuitBreakerState {
    return this.breaker.getState();
  }
}
