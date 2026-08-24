import type { LlmRouteTarget, LlmRoutingConfig, LlmTask, LlmUsage, MemoryProvider } from "../types.js";
import { logger } from "../logger.js";

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

export interface LlmUsageEvent extends LlmUsage {
  task: LlmTask;
  provider: LlmRouteTarget;
  model: string;
}

export interface LlmTaskRouterOptions {
  primary: LlmProviderTarget;
  auxiliary?: LlmProviderTarget;
  routing: LlmRoutingConfig;
  onEvent?: (event: LlmRoutingEvent) => void;
  onUsage?: (event: LlmUsageEvent) => Promise<void>;
}

export class LlmTaskRouter {
  private readonly primary: LlmProviderTarget;
  private readonly auxiliary?: LlmProviderTarget;
  private readonly routing: LlmRoutingConfig;
  private readonly onEvent?: (event: LlmRoutingEvent) => void;
  private readonly onUsage?: (event: LlmUsageEvent) => Promise<void>;

  constructor(options: LlmTaskRouterOptions) {
    this.primary = options.primary;
    this.auxiliary = options.auxiliary;
    this.routing = options.routing;
    this.onEvent = options.onEvent;
    this.onUsage = options.onUsage;
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
    const usage: LlmUsage[] = [];
    try {
      const candidate = await operation(this.withTask(this.auxiliary, task, usage));
      await this.emitUsage(task, "aux", this.auxiliary.model, usage);
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
    const usage: LlmUsage[] = [];
    const candidate = await operation(this.withTask(this.primary, task, usage));
    await this.emitUsage(task, "primary", this.primary.model, usage);
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

  private async emitUsage(
    task: LlmTask,
    provider: LlmRouteTarget,
    model: string,
    usage: LlmUsage[],
  ): Promise<void> {
    if (!this.onUsage) return;
    for (const item of usage) {
      try {
        await this.onUsage({ task, provider, model, ...item });
      } catch (error) {
        logger.warn("LLM usage metrics recording failed", {
          task,
          provider,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  private withTask(
    target: LlmProviderTarget,
    task: LlmTask,
    usage: LlmUsage[],
  ): MemoryProvider {
    const thinking = this.routing.thinking?.[task];
    const options = {
      task,
      ...(thinking === undefined ? {} : { thinking }),
      onUsage: (item: LlmUsage) => usage.push(item),
    };
    const provider = target.provider;
    return {
      name: provider.name,
      compress: (systemPrompt, userPrompt) => provider.compress(systemPrompt, userPrompt, options),
      summarize: (systemPrompt, userPrompt) => provider.summarize(systemPrompt, userPrompt, options),
      ...(provider.describeImage
        ? { describeImage: provider.describeImage.bind(provider) }
        : {}),
    };
  }
}
