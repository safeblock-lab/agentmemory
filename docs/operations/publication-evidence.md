# Publication evidence

`src/functions/publication-evidence.ts` is a durable semantic gate for summary, skill, consolidation and reflection publication. It checks a candidate against complete source snapshots using the existing configured LLM through an injected executor. It does not publish, select providers, grant calls, or replace exact source, project or version checks.

## API and authority

`preparePublicationEvidence(input)` returns `{requestId, systemPrompt, userPrompt}` or an explicit invalid/incomplete result. `evaluatePublicationEvidence(kv, input, execute?)` returns `{requestId, status, reason, replayed}`. Input contains `workflow`, `operationId`, `project`, `candidate`, `sources` and `evidenceComplete`. Each source contains `id`, `scope`, `key`, `version`, `project`, `content` and optional `sessionId`. Versions are strings for the consumer's exact authoritative snapshot identity. Source scope/key pairs and IDs must be unique; every source project must equal the publication project.

Criteria are fixed internally and versioned as `publication-evidence-v3`; input cannot override them. The v3 criteria preserve claim time, actor and scope, distinguish configuration or intention from observed execution and results, and require causal or benefit claims to stay within the evidence. Inferences may be included when clearly qualified and bounded. Identity includes the operation, workflow, candidate, all source content, provenance and versions, project and criteria. Sources are sorted by scope/key to keep request identity deterministic. A changed input creates a different request and cannot reuse an unrelated captured response.

Use the configured provider and keep the originating workflow task route. `publicationCallOptions` preserves supplied call options and adds the publication response format below. The executor receives `(request, captureRaw)` and must await `captureRaw(raw)` immediately after `provider.summarize` returns, **inside** the router operation, before parsing or the router's outer validation. The helper creates no provider/client, tools, dependencies or new configuration.

```typescript
const options = publicationCallOptions({ task: "summary", outputTokens: 1024 });
const result = await evaluatePublicationEvidence(kv, input, (request, captureRaw) =>
  router.run("summary", async provider => {
    const raw = await provider.summarize(request.systemPrompt, request.userPrompt, options);
    await captureRaw(raw);
    return raw;
  }, existingValidator),
);
```

## Structured publication judge request

For publication judges, `publicationCallOptions` sends this strict closed JSON Schema response format through the OpenAI-compatible adapter:

```json
{
  "type": "json_schema",
  "json_schema": {
    "name": "publication_verdict",
    "strict": true,
    "schema": {
      "type": "object",
      "properties": {
        "verdict": { "type": "string", "enum": ["supported", "unsupported", "uncertain"] },
        "reason": { "type": "string", "minLength": 1, "maxLength": 512 }
      },
      "required": ["verdict", "reason"],
      "additionalProperties": false
    }
  }
}
```

This format applies only to publication judge calls. Summary/XML generation keeps its existing options. Other provider adapters continue to rely on host-side closed-response validation; native structured-format support is not claimed for them. The OpenAI-compatible adapter encodes the request, but a custom endpoint can reject or ignore this format. Request encoding does not prove backend acceptance or end-to-end success, and the host parser remains the final gate.

The consumer must recheck current source versions and project membership before applying any effect. No secrets belong in candidate/source content. The system prompt treats source and candidate text as untrusted data and grants no tools or execution authority. An LLM can still misjudge or follow injection; this is a fallible semantic gate, not an infallibility guarantee.

## Decisions and replay

For a v3 request, the model returns exactly one JSON object with `verdict` and `reason`. The host binds that response to the exact request and captured checkpoint. For compatibility, exactly three fields (`requestId`, `verdict`, `reason`) are also accepted only when `requestId` exactly matches the request. Markdown, extra keys, foreign IDs, unknown verdicts and oversized or empty reasons fail closed. There are no lexical/span-matching semantic acceptance heuristics.

Before dispatching a new v3 request, the helper checks the exact v1 and v2 request keys derived from the same complete input. These bounded lookups do not scan or reinterpret other stored requests. A valid legacy-v1 raw response is revalidated against its closed three-field schema; v2 raw responses are revalidated against the v2 schema. Both replay with zero executor calls. An older intention without capture stays `pending`; identity or state mismatches and invalid old responses fail closed and never become a fresh v3 dispatch. Replay always validates the raw response rather than trusting a stored decision. The prior v1/v2 criteria and prepared identities are frozen so existing checkpoints remain addressable.

Only `supported` permits the consumer to consider a **new** effect, after rechecking current authoritative source versions and project membership and applying its existing once-only effect/recovery protocol. Replayed completed publication receipts remain the consumer's responsibility. `unsupported`, `uncertain`, `invalid_response`, `invalid_input`, `incomplete_evidence`, `unavailable` and `pending` do not permit publication or automatic candidate regeneration.

Existing `KV.state` stores one bounded checkpoint per request ID. It records the full exact request/identity, creation timestamp, dispatch intention, raw response, parsed decision and an optional sanitized executor diagnostic. Intention is awaited before execution; raw persistence is awaited before parsing. Captured raw is authoritative even if outer router validation subsequently throws. Executor failures retain only an allowlisted category (`timeout`, `http_error`, `capture_error`, or `executor_error`) and, when present, an integer HTTP status from 100–599; messages, response bodies and secrets are not persisted. Capture, diagnostic, replay and decision updates serialize against the latest checkpoint so a late capture cannot replace a valid response or decision. State storage errors propagate before dispatch or decision acceptance; ambiguous raw-write failure returns pending.

An existing intention without capture returns `pending` (`dispatch_outcome_unknown`), including provider failures, missing executor capture and timeout. A caught executor failure adds the bounded diagnostic to that intention without changing its ambiguous outcome. The helper waits at most 60 seconds; it does not cancel or retry an ambiguous provider request. A late capture can make a subsequent replay usable and retains the diagnostic. Preserve pending records for explicit recovery/accounting; never delete them to force another call. An absent executor returns `unavailable` without creating an intention. No provider error payloads or evidence contents are logged by the helper.

The latest observed real fresh-summary attempt has not demonstrated end-to-end success: no new canonical summary was published, while its publication checkpoint remains `intended` with no raw capture. Treat that as pending/dispatch outcome unknown, not a successful summary or proof that the backend accepted the format. The cause remains unresolved; do not automatically retry it. The explicit summary recovery below is offered only for a retained `summary_publication_invalid_response` failure, not an ambiguous pending intention.

The existing keyed mutex prevents duplicate dispatch within one Node runtime. `get`/`set` are not a cross-process compare-and-swap. Consumers must retain their existing sole-writer/lease ownership across processes, including after a timeout; no new infrastructure is introduced. Pending ownership must not be released into a new dispatcher that can erase or overwrite the intention.

## Bounds and honest incompleteness

Bounds are UTF-8 bytes: candidate 32,768; each source 65,536; serialized evidence input 262,144; response 16,384; reason 2,048; at most 128 sources. Empty evidence, incomplete evidence or excess input returns `incomplete_evidence` before dispatch. No input is truncated. The consumer must gather complete bounded evidence or explicitly retain blocked continuation; evaluating a partial subset as complete is invalid.

For an oversized raw response, the checkpoint records explicit `responseOverflow` byte count/fingerprint and `invalid_response`; it retains no truncated response and cannot accept or retry it. Configure provider response limits through existing options. Mock tests exercise orchestration, injection separation and closed verdict handling; they do not measure actual LLM semantic accuracy. Live provider execution and installation require the separately authorized operator.

## Explicit recovery for an invalid summary publication response

Recovery starts with the ordinary authenticated `POST /agentmemory/summarize` endpoint. Retrieve an eligible continuation by sending the affected session ID:

```json
{"sessionId":"<sessionId>"}
```

The endpoint offers a continuation only for a retained failed manual-origin job whose failure is `summary_publication_invalid_response`, whose project and session still match, and whose source fingerprint still matches the current observations. If any check fails, it does not offer recovery. Token retrieval does not dispatch a provider call.

Use the returned nine-field `continuation` object verbatim; do not construct or edit it:

```json
{
  "operationId":"<summary-recovery fingerprint>",
  "step":0,
  "jobId":"<old failed job id>",
  "project":"<project>",
  "sessionId":"<session id>",
  "sourceFingerprint":"<current source fingerprint>",
  "round":1,
  "unitId":"<old job's final unit id>",
  "deliveryId":"<same recovery fingerprint as operationId>"
}
```

`round` is copied from the old job; `1` is only an example. Submit the exact returned continuation to the same endpoint with the same session ID:

```json
{"sessionId":"<sessionId>","continuation":<exact continuation object returned above>}
```

This human-initiated request revalidates the failed job, session, project, observation count and source fingerprint before creating a separate `full-originals-v1` child. The failed job and its history remain intact. The child verifies the complete originals and their stored versions, hashes and membership, then assembles one fresh summary request from those originals. It does not reuse the failed candidate or old map/reduction outputs.

The UTF-8 byte length of `JSON.stringify([systemPrompt, userPrompt])` is limited to 32,768 bytes. The context budget also reserves the existing 512-token message-envelope estimate, configured safety margin and output tokens. Missing, stale, incomplete or over-budget originals fail before recovery child job/unit state is written or a provider call is dispatched. The ordinary map/reduce path keeps its configured input cap (7,500 bytes by default); this recovery limit does not change that path. A successful recovery cut makes one fresh generation call followed by one publication judge call, at most two provider calls total. Ambiguous intention-without-capture remains pending and is not automatically retried.

Replaying the same nine-field continuation is idempotent, including when JSON object keys arrive in a different order. The failed-job continuation must still be retrieved and submitted explicitly; failure does not trigger an automatic retry. The token shape and retained history are unchanged.

## Reflection result link

A supported reflection publication stores its exact `evidenceRequestId` in the per-unit result ACK at `REFLECT_CONTROL` key `${jobId}:result:${sequence}` before the queue clears the captured response and manifest. Read that ACK and, when `evidenceRequestId` is present, read exactly `KV.state` by that key:

```typescript
const ack = await kv.get(REFLECT_CONTROL, `${jobId}:result:${sequence}`);
const evidence = ack?.evidenceRequestId
  ? await kv.get(KV.state, ack.evidenceRequestId)
  : null;
```

The durable publication request contains the candidate and complete sources alongside the raw judge response and decision. This is a direct key lookup; there is no scan or inference from other state. The field is optional so older ACKs without a link remain readable and replayable; do not re-judge or fetch sources just because an older ACK lacks it.
