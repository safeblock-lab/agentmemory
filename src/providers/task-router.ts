import type { LlmRouteTarget, LlmRoutingConfig, LlmTask, MemoryProvider } from "../types.js";

export type LlmFallbackReason =
  | "auxiliary_unavailable"
  | "auxiliary_error"
  | "auxiliary_empty"
  | "auxiliary_invalid";

export interface LlmProviderTarget {
  provider: MemoryProvider;
  model: string;
}

export interface LlmRoutingEvent {
  task: LlmTask;
  selectedProvider: LlmRouteTarget;
  model: string;
  durationMs: number;
  fallbackUsed: boolean;
  failureReason?: LlmFallbackReason;
}

export interface LlmTaskRouterOptions {
  primary: LlmProviderTarget;
  auxiliary?: LlmProviderTarget;
  routing: LlmRoutingConfig;
  onEvent?: (event: LlmRoutingEvent) => void;
}

export class LlmTaskRouter {
  private readonly primary: LlmProviderTarget;
  private readonly auxiliary?: LlmProviderTarget;
  private readonly routing: LlmRoutingConfig;
  private readonly onEvent?: (event: LlmRoutingEvent) => void;

  constructor(options: LlmTaskRouterOptions) {
    this.primary = options.primary;
    this.auxiliary = options.auxiliary;
    this.routing = options.routing;
    this.onEvent = options.onEvent;
  }

  get hasAuxiliaryProvider(): boolean {
    return this.auxiliary !== undefined;
  }

  hasExplicitRoute(task: LlmTask): boolean {
    return this.routing.explicitRoutes[task] !== undefined;
  }

  async run<T>(
    task: LlmTask,
    operation: (provider: MemoryProvider) => Promise<T>,
    validate: (candidate: T) => boolean,
    routeOverride?: LlmRouteTarget,
  ): Promise<T> {
    const requestedProvider = routeOverride ?? this.routing.routes[task] ?? "primary";
    if (requestedProvider === "primary") {
      return this.runPrimary(task, operation, validate, false);
    }

    if (!this.auxiliary) {
      return this.runPrimary(task, operation, validate, true, "auxiliary_unavailable");
    }

    const startedAt = Date.now();
    try {
      const candidate = await operation(this.auxiliary.provider);
      const failureReason = this.getCandidateFailure(candidate, validate);
      if (!failureReason) {
        this.emit({
          task,
          selectedProvider: "aux",
          model: this.auxiliary.model,
          durationMs: Date.now() - startedAt,
          fallbackUsed: false,
        });
        return candidate;
      }
      return this.runPrimary(task, operation, validate, true, failureReason);
    } catch {
      return this.runPrimary(task, operation, validate, true, "auxiliary_error");
    }
  }

  private async runPrimary<T>(
    task: LlmTask,
    operation: (provider: MemoryProvider) => Promise<T>,
    validate: (candidate: T) => boolean,
    fallbackUsed: boolean,
    failureReason?: LlmFallbackReason,
  ): Promise<T> {
    const startedAt = Date.now();
    const candidate = await operation(this.primary.provider);
    const primaryFailure = this.getCandidateFailure(candidate, validate);
    if (primaryFailure) {
      throw new Error(`LLM ${task} response failed deterministic validation`);
    }
    this.emit({
      task,
      selectedProvider: "primary",
      model: this.primary.model,
      durationMs: Date.now() - startedAt,
      fallbackUsed,
      failureReason,
    });
    return candidate;
  }

  private getCandidateFailure<T>(candidate: T, validate: (candidate: T) => boolean): LlmFallbackReason | undefined {
    if (typeof candidate === "string" && candidate.trim().length === 0) {
      return "auxiliary_empty";
    }
    return validate(candidate) ? undefined : "auxiliary_invalid";
  }

  private emit(event: LlmRoutingEvent): void {
    this.onEvent?.(event);
  }
}
