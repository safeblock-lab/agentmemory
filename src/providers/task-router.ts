import type { LlmRouteTarget, LlmRoutingConfig, LlmTask, LlmUsage, MemoryProvider, StructuredLlmRequest, PreparedStructuredLlmCall } from "../types.js";
import { canPrepareStructuredCall as hasStructuredCallCapability, prepareStructuredCall, singleStructuredDispatch } from "./structured-output-contract.js";
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
  onUsage?: (event: LlmUsageEvent, effectKey?: string) => Promise<void>;
}

export class LlmTaskRouter {
  private readonly primary: LlmProviderTarget;
  private readonly auxiliary?: LlmProviderTarget;
  private readonly routing: LlmRoutingConfig;
  private readonly onEvent?: (event: LlmRoutingEvent) => void;
  private readonly onUsage?: (event: LlmUsageEvent, effectKey?: string) => Promise<void>;

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

  graphAutomaticProvider(): MemoryProvider {
    const route = this.routing.routes.graph_extraction ?? "primary";
    const target = route === "primary" ? this.primary : route === "aux" ? this.auxiliary : undefined;
    if (!target || this.routing.thinking?.graph_extraction !== undefined) throw new Error("GRAPH_AUTOMATIC_ROUTE_UNSUPPORTED");
    return target.provider;
  }
  graphAutomaticRoute(): LlmRouteTarget { return this.routing.routes.graph_extraction ?? "primary"; }
  async recordGraphAutomaticUsage(route: LlmRouteTarget, model: string, usage: LlmUsage, effectKey: string): Promise<void> {
    if (!this.onUsage) throw new Error("GRAPH_USAGE_RECORDER_UNAVAILABLE");
    await this.onUsage({ ...usage, task: "graph_extraction", provider: route, model }, effectKey);
  }

  hasExplicitRoute(task: LlmTask): boolean {
    return this.routing.explicitRoutes[task] !== undefined;
  }

  canPrepareStructuredCall(task: LlmTask, routeOverride?: LlmRouteTarget): boolean {
    const route = this.resolveStructuredRoute(task, routeOverride);
    if (!route) return false;
    const target = this.getStructuredTarget(route);
    return target !== undefined && hasStructuredCallCapability(target.provider);
  }

  prepareStructuredCall(task: LlmTask, request: StructuredLlmRequest, routeOverride?: LlmRouteTarget): PreparedStructuredLlmCall {
    const route = this.resolveStructuredRoute(task, routeOverride);
    if (!route) throw new Error("structured_output_route_invalid");
    const target = this.getStructuredTarget(route);
    if (!target) throw new Error("structured_output_route_unavailable");
    const usage: LlmUsage[] = [];
    const provider = this.withTask(target, task, usage);
    const prepared = prepareStructuredCall(provider, request);
    if (prepared.binding.model !== target.model) throw new Error("structured_output_target_mismatch");
    return singleStructuredDispatch({ ...prepared.binding, route }, async () => {
      const startedAt = Date.now();
      try {
        const result = await prepared.execute();
        this.emit({ task, selectedProvider: route, model: target.model, durationMs: Date.now() - startedAt, fallbackUsed: false });
        return result;
      } finally {
        await this.emitUsage(task, route, target.model, usage);
      }
    });
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
    let contracted = false;
    try {
      let candidate: T;
      try {
        candidate = await operation(this.withTask(this.auxiliary, task, usage, () => { contracted = true; }));
      } finally {
        await this.emitUsage(task, "aux", this.auxiliary.model, usage);
      }
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
      if (contracted) throw new Error("structured_output_validation_failed");
      return this.runPrimary(task, operation, validate, true, failureReason);
    } catch (error) {
      if (contracted) throw error;
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
    let candidate: T;
    try {
      candidate = await operation(this.withTask(this.primary, task, usage));
    } finally {
      await this.emitUsage(task, "primary", this.primary.model, usage);
    }
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
    onContract?: () => void,
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
      ...(hasStructuredCallCapability(provider) ? {
        prepareStructuredCall: (request: StructuredLlmRequest) => {
          onContract?.();
          const onUsage = request.options.onUsage;
          return prepareStructuredCall(provider, {
            ...request,
            options: {
              ...request.options, ...options,
              onUsage: (item: LlmUsage) => { options.onUsage(item); onUsage?.(item); },
            },
          });
        },
      } : {}),
      compress: (systemPrompt, userPrompt, callOptions) => {
        if (callOptions?.responseFormat || callOptions?.outputContract) onContract?.();
        return provider.compress(systemPrompt, userPrompt, { ...callOptions, ...options });
      },
      summarize: (systemPrompt, userPrompt, callOptions) => {
        if (callOptions?.responseFormat || callOptions?.outputContract) onContract?.();
        return provider.summarize(systemPrompt, userPrompt, { ...callOptions, ...options });
      },
      ...(provider.describeImage
        ? { describeImage: provider.describeImage.bind(provider) }
        : {}),
    };
  }

  private resolveStructuredRoute(task: LlmTask, routeOverride?: LlmRouteTarget): LlmRouteTarget | undefined {
    const route: unknown = routeOverride ?? this.routing.routes[task] ?? "primary";
    return route === "primary" || route === "aux" ? route : undefined;
  }

  private getStructuredTarget(route: LlmRouteTarget): LlmProviderTarget | undefined {
    return route === "primary" ? this.primary : this.auxiliary;
  }
}
