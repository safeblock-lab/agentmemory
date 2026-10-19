# Consolidation publication recovery

This internal SDK path prepares and admits one new judge attempt for an existing v4 consolidation publication capture that fails the current strict verdict parser. It also permits the same admission to resume its selected intended attempt only when fresh authoritative native budget evidence proves that no authorized dispatch started. It does not repair raw output, relax the reason limit, force publication, or retry an unknown native dispatch.

## Runtime boundary


The source registers `mem::consolidation-publication-prepare` and `mem::consolidation-publication-admit` through the existing consolidation registration. There are no new REST endpoints or MCP tools. These functions require trusted internal SDK access and Main's current operational authorization. This source change does not activate a private legacy grant, install artifacts, start services, provision budget, or authorize a provider request.

Main owns grouped emission and installation, current private legacy grant activation, native budget availability, and authorization for the one provider effect. Future automatic policy integration remains pending graph/native work; this recovery path is not an accepted per-case operator product workflow.

## Preparation

Call `mem::consolidation-publication-prepare` with the closed payload:

```json
{
  "manifestId": "<current manifest ID>",
  "dispositionId": "<unique authorized disposition ID>",
  "budget": { "windowId": "<native authorized window ID>", "ceiling": 1 }
}
```

The SDK transport may supply `_caller_worker_id`; it must be a string of 1–128 characters and is removed before validating the closed application payload. It is transport metadata, not an authorization claim.

Preparation returns the exact closed `admission` payload. It performs no writes, leases, budget claims, cursor changes, or provider calls. It reads the schema-2 root with `kv.getVersioned(pageKey(manifestId, "root", 0))`, avoiding `loadRoot` telemetry, and requires the current publish position with no retained publication receipt. It reconstructs the candidate and publication evidence using the ordinary helpers, validates retained source capture and session project identities, and binds native root, candidate-page, aggregate, source and session versions and values.

The original evidence must be v4, captured or decided, with retained bounded raw output, no overflow or supersession, and an `invalid_response` under the current parser. A decided record must retain its `invalid_response` decision. Valid supported, unsupported and uncertain verdicts are ineligible. Earlier authoritative checkpoints block this path. Preparation binds the original native evidence version and a fingerprint of its identity, request, raw, decision and other retained fields. A fresh executor preparation must produce exactly the original strict current target contract, including provider binding and options.

## Admission and continuation

Submit the returned admission unchanged to `mem::consolidation-publication-admit`. Do not construct missing fields or reuse a preparation after any bound state or contract changes.

Admission takes the ordinary consolidation lock, revalidates the captured root/candidate/source input, and persists a journal before selecting a deterministic new judge attempt. Supersession preserves the original raw response, decision and identity, adds a link, and records the inverse link and budget binding on the new attempt. It rechecks the original native evidence version and fingerprint before linking. The fresh executor must still produce exactly the captured target contract. Immediately before entering the native budget scope, admission reconstructs and revalidates the current consolidation capture again.

The new attempt uses `withProviderAttemptBudget` with its own operation ID, the admitted window, and ceiling 1. A crash after attempt intention but before the link cannot create another attempt: the existing deterministic attempt blocks reselection. A different admission for the same manifest publish position is rejected.

### Same-admission undispatched resume

A cached journal result replays locally, except `pending / dispatch_outcome_unknown` for the exact admitted attempt. That exception revalidates the current capture and follows the existing selected captured-v4 link. Captured or decided attempts still resolve locally. Only an intended attempt with no raw output, overflow, decision or outgoing supersession qualifies. Both link directions, source fingerprint, original checkpoint, historical checkpoint exclusions, request identity and admitted budget binding remain mandatory. A fresh executor preparation must equal the retained request and original target contract; the executor is prepared once per invocation.

`getProviderAttemptBudgetReceipt` must return the exact admitted operation and window, operation and window ceilings of 1, state `available`, operation and window consumption of 0, and operation and window remaining of 1. Missing, changed, expired, started, completed, unknown or exhausted receipts never permit dispatch. Absence of raw output alone grants no permission. The selected executor's provider effects must remain wrapped by `consumeProviderAttempt`; that invariant makes an authoritative available receipt evidence of no authorized dispatch.

Before execution, the original native row version, original value and selected record are reread and compared, and the evaluator rereads the unchanged intended record. Immediately before entering the budget scope, the current consolidation capture is revalidated. The existing `withProviderAttemptBudget` then atomically claims the same operation/window before provider dispatch; racing, duplicate, lost or nonfresh claims cannot dispatch. A status receipt alone never authorizes the effect. No intention, link, grant, operation ID or budget is deleted, replaced or reset. The original invalid raw output and decision remain intact. Replaced journal results remain in `priorResults`; failed native budget admission can retain a bounded sanitized `ProviderAttemptBudgetError.code` on the intended attempt.

The optional internal resume binding applies only to this selected captured-v4 consolidation path. Ordinary publication evaluation and captureless/v3 disposition replay retain their existing behavior. Completed journal results replay locally even after ordinary publication advances the root. A pending journal reconciliation requires the original capture still to match. Unknown or spent native attempts require Main's separate disposition and never receive another dispatch through this path.

For supported or unsupported, the response has `continuationRequired: true` and `completed: false`. Main must separately resume the existing authorized consolidation pipeline. Its normal publication path consumes the selected checkpoint: supported follows ordinary canonical publication checks; unsupported follows ordinary rejection. Neither preparation nor admission publishes, rejects a candidate by itself, deletes source memory, or advances the root cursor. Pending, uncertain, invalid or unavailable results remain explicit and require disposition by Main. A `success` field alone never establishes workflow progress.

## Local verdict parsing

The local parser accepts plain JSON or one exact outer fence with the lowercase json label and LF delimiters, with no surrounding prose or interior fence. It unwraps the body in memory, then applies the existing closed two-field parser. The original raw response remains subject to the 16,384 UTF-8-byte limit before unwrapping. The reason limit remains 512 Unicode code points and 2,048 UTF-8 bytes; existing type, duplicate-key, control-character, and closed-field checks remain unchanged. Retained raw output and historical stored invalid decisions or audit evidence are not rewritten. Ordinary local evaluation may resolve a valid normalized verdict under the current parser; Main still owns any continuation or publication action.

## Evidence and rollback

Retain the preparation/admission, admission journal, original evidence, selected attempt and native budget evidence. The concrete supplied blocker was `publication-evidence_97f7b69050d30a43` for `conmanifest_a6485cd5cd6e6333`: retained fenced JSON with a 624-character reason exceeds the strict 512-character reason contract. The path does not truncate that reason or widen the schema.

Before activation, rollback consists of reverting this source leaf through Main's normal integration process. After admission, do not remove links, overwrite raw evidence, delete attempt intentions, reset budget, or restore an older runtime that cannot read the captured-v4 link. Main must retain a compatible runtime or explicitly design a separate guarded migration. Publication effects require their own established recovery path; source rollback does not undo them.

No tests, typechecking, builds, hashes, artifact checks, health probes, service operations or provider calls were performed by this worker. The described behavior is source implementation, not a verified live result.
