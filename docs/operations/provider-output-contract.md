# Provider output contracts

The source API separates a mandatory logical JSON schema from its HTTP transport. Local consumer parsing and deterministic acceptance remain mandatory for both profiles. This change has no runtime verification claim.

## Explicit profiles

`ProviderConfig.httpProfile` and `OpenAIProviderOptions.httpProfile` accept exactly `legacy-native` or `validated-json-text`. `OPENAI_HTTP_PROFILE` configures the primary OpenAI-compatible provider; `AGENTMEMORY_AUX_LLM_HTTP_PROFILE` configures its auxiliary counterpart. Existing merged environment precedence lets process values override the environment file. Invalid values, including an explicit empty string, fail rather than default. An omitted value selects `legacy-native`.

`legacy-native` preserves existing serialization, including native `response_format`. `validated-json-text` emits only `model`, `messages`, `max_tokens` and `stream`; the captured JSON schema remains logical and is deliberately absent from the HTTP body only for a valid bound `outputContract`. Legacy schema calls lacking that marker reject before fetch instead of silently changing transport. It does not change prompts, endpoint, model or credentials. The profile is explicit; hostnames and provider errors never negotiate it.

The text profile rejects explicit task/call thinking (including `false`), provider `noThink`, configured reasoning effort, keep-alive and unknown call options before fetch. These fields must be absent rather than silently removed. Ollama's distinct provider cannot select the OpenAI text profile. No persistent configuration or startup process is changed by this source implementation.

## Bind before persistence

Before selecting a new structured-contract workflow, `canPrepareStructuredCall(provider)` reports whether the provider exposes a callable preparation method without preparing a request or invoking the provider. When task routing applies, use `router.canPrepareStructuredCall(task, routeOverride?)`; it checks the same configured primary or auxiliary target as preparation and returns `false` for an invalid route, a missing auxiliary target, or an unsupported provider. It never falls back to another target. A `false` result leaves consumers free to retain their established legacy workflow without creating a structured binding or intent.

Use `LlmTaskRouter.prepareStructuredCall(task, request, routeOverride?)` when task routing applies; otherwise use `prepareStructuredCall(provider, request)` exported from `src/providers/structured-output-contract.ts`.

`StructuredLlmRequest` contains `operation` (`compress` or `summarize`), `systemPrompt`, `userPrompt`, `contractVersion`, and `options` with a strict `json_schema` response format. Capture the logical schema and consumer parser version in the durable request identity alongside the returned binding before dispatch. JSON text prompts must already describe the required output; the provider does not repair requests.

The returned `PreparedStructuredLlmCall` exposes frozen `binding` metadata and `execute(): Promise<string>`. Binding fields are `provider`, `model`, `endpoint`, `profile`, `operation`, optional `task`, optional router `route`, `contractVersion`, serialized logical `responseFormat`, `requestDigest`, and `timeoutMs`. The request digest covers the exact captured HTTP body without exposing its prompts. The captured schema and bytes cannot be changed by later mutation of the caller's request.

The router resolves its configured target and thinking before preparation. Missing auxiliary targets, unavailable capabilities and model mismatches fail preparation. Persisted resume logic must compare the captured binding with a newly prepared binding before admitting a dispatch; a binding alone never authorizes execution.

`execute()` consumes the prepared call before attempting it. Circuit-breaker rejection, HTTP errors, malformed/empty responses and consumer parser failures do not permit reuse. It returns every string unchanged, including empty or whitespace content, so consumers can capture it before local rejection; non-string content rejects. Resilient and fallback-chain providers preserve the primary capability. Structured calls do not retry or fall back; unconstrained calls retain existing behavior. Usage callbacks, router accounting, telemetry and provider timeouts remain in the dispatch path. Returned text must pass the consumer's strict local parser before publication, cursor advancement or other acceptance effects.

## Fireworks Batch

`FIREWORKS_BATCH_OUTPUT_CAPABILITY` independently declares `transport: fireworks-batch`, `profile: legacy-native`, and `responseFormat: json_schema`. Batch consumers must capture and compare their own batch capability/configuration and schema. They must never copy an HTTP router profile or treat batch support as permission to change targets or retry failed contract calls.

## Integration status

Consumer integration, grouped compilation, installation and startup belong to Main. Tests, typechecking, checksum checks, provider calls and health probes were not run for this source change, following the explicit execution preference.
