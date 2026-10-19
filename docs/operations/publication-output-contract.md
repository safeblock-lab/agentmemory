# Publication output contract

`publication-verdict-v2` uses durable record version `publication-evidence-v4`
and parser `closed-json-verdict-v2`. The grouped application was compiled and
installed on 2026-10-18 (application emission2678, native3063). The primary HTTP
profile was activated for the existing router in the application process only.
Publication V4 itself has not yet been exercised; the historical publication
intent remains held. No tests or typecheck were run.

## Selection and capture

All six publication consumers use `publicationExecutor`: consolidation pipeline,
summary queue, direct summary, skill extraction, reflection callback and reflection
queue. Existing v1, v2 and v3 checkpoints are selected first with their exact
original request identity and parser. They are never dispatched again. An existing
v4 checkpoint also replays its stored contract without consulting current provider
configuration.

For a new request, the effective router target declares capability through
`canPrepareStructuredCall(task)`. A direct provider declares it by exposing
`prepareStructuredCall`. When available, preparation captures the logical schema,
parser version, requested options and provider binding before writing intent.
The binding includes route, provider, model, endpoint, profile, timeout and the
digest of the effective serialized provider request. Execution uses the prepared
closure that produced that binding; it does not reconstruct options from current
configuration. Requested options in the record do not claim to enumerate provider
defaults or router overrides: the effective request is bound by its digest.

The configured target is authoritative. Preparation errors do not select another
version or target. V4 uses one dispatch, with no auxiliary-to-primary fallback or
hidden retry. Router usage reporting and workflow accounting still surround that
dispatch. Invalid response text is captured before local parsing. Overflow retains
the existing bounded fingerprint marker. Time, response, evidence and cost limits
remain in place.

When the selected target declares capability unavailable, selection happens before
intent and uses the original v3 executor path. This preserves existing providers
that do not implement the optional capability. Legacy router fallback behavior is
retained for that path; it does **not** have the v4 single-dispatch guarantee. A
failed v4 preparation or dispatch never switches to this legacy path.

## Closed local acceptance

V4 accepts exactly one JSON object containing exactly `verdict` and `reason`, both
strings. Verdict is `supported`, `unsupported` or `uncertain`. Reason must contain
non-whitespace text, at most 512 Unicode characters and at most 2048 UTF-8 bytes.
The total response bound remains 16384 bytes. Duplicate keys, including escaped
aliases, unknown or missing keys, coercion, decoded control characters, unpaired
surrogates, fences, prose and reasoning blocks fail local acceptance. JSON spaces,
tabs, CR and LF are allowed between tokens. Key order may vary. V4 compares captured
contracts and durable request identities by strict semantic value, independently
of object key order; array order and values remain significant.

V1/v2/v3 keep their original parser behavior. The model verdict establishes only
semantic support; existing source freshness, provenance, quality, permission and
publication gates still govern business effects.

## Activation and historical recovery

Main owns grouped compilation, installation and launch configuration. The explicit
`validated-json-text` HTTP profile is selected only for the configured OpenAI
primary or auxiliary target; native transport remains available. See
[provider output contracts](provider-output-contract.md) for profile activation.

Historical `publication-evidence_c4dc1ffa1b5d6bf4` remains held. This source change
does not delete, supersede, replay, rewrite or advance that record. The existing
reflection disposition binds a same-contract retry and budget; it does not bind a
new logical contract, parser, transport and effective provider request. Prepared
contract executors therefore return `judge_disposition_contract_migration_required`
before that disposition creates any attempt.

A historical migration requires a separate Main-owned disposition contract binding
the exact old checkpoint, new contract and provider binding, budget/window and
authorized effect. No such migration is introduced here. An intended v4 record
with unknown outcome also remains pending; changing configuration cannot resend it.
Rollback of source/configuration cannot make historical intended records safe to
dispatch again.
