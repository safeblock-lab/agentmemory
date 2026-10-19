# Publication judge contract migration

This source path lets Main explicitly replace one retained reflection judge intent with a new judge attempt under the current output contract. Installation and runtime admission remain Main operations; source preparation does not establish runtime success.

## Eligibility and identity

`supersedePublicationJudge` keeps existing same-contract dispositions unchanged. Prepared executors require an explicit `migration` with version `publication-judge-disposition-v2`.

Migration accepts only an exact `publication-evidence-v3` checkpoint in `intended` state, without raw response or overflow, with an `executor_error` diagnostic or HTTP 400. A captured or decided verdict cannot be replaced. Earlier v1/v2 evidence still blocks a new attempt.

The disposition binds the original request identity, a canonical fingerprint of the retained record, its observed checkpoint, and the complete prepared target contract. The target includes the parser, provider, model, endpoint, transport profile, effective serialized-request digest and options. The budget binds the new attempt ID, window and ceiling. A changed record, target contract or budget fails closed.

## Main operation

1. Use the existing iii SDK/StateModule `StateKV` and the exact reflection evidence input reconstructed from the retained job, captured response and manifest. Preserve the existing reflection unit identity and capture fingerprint.
2. Create a fresh `publicationExecutor` using the approved provider/router configuration. Call `preparePublicationJudgeMigration(kv, input, dispositionId, budget, executor)` from `publication-evidence`. This reads the retained checkpoint and prepares the structured request locally; it does not dispatch the provider or write state. Its executor is consumed for preparation; do not reuse that executor for execution.
3. Retain the returned disposition, including its full `migration` object. Pass it with the existing `unit` and `captureFingerprint` to the existing `mem::reflect-unit` `judgeDisposition` control. Use the same approved configuration. Runtime execution creates another fresh executor and compares its full prepared request against the disposition before creating a new intent.
4. Treat the resulting judge status as evidence. Only a captured response accepted by `closed-json-verdict-v2` can produce `supported`, `unsupported` or `uncertain`. A failed or unknown new attempt remains consumed; repeating the same disposition replays its checkpoint without redispatch.

Before provider dispatch, the implementation writes the new v4 intent, budget binding and supersession link through StateKV. The original v3 identity, timestamps, diagnostic and response fields remain historical evidence. A failed link write leaves the new attempt consumed and blocks implicit retry. Do not delete records, rewrite historical responses, or force a successful verdict to recover.

The historical `publication-evidence_c4dc1ffa1b5d6bf4` remains subject to these eligibility and identity checks. This document does not assert its current runtime state or authorize a provider call.
