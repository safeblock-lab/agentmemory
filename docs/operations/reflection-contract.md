# Reflection operation contract

Reflection selects bounded indexed evidence, freezes its source versions, generates a candidate, validates its structure, judges it against the originals, and applies idempotent insight effects. State and execution stay behind the existing iii functions and `StateKV` interface.

## Results and coverage

`parseReflectionResult` accepts the existing `title` attribute and nested `<title>` form, including an outer XML Markdown fence. Attribute order is irrelevant. Conflicting titles, unknown structure, malformed nonempty results, more than five insights, and nonfinite or out-of-range confidence fail before judging or applying. Explicit `<insights/>` is a valid empty candidate. Captured raw responses remain unchanged.

`completed: true` means the requested bounded selection completed. `coverage` identifies indexed memory and observation retrieval and explicitly requested fact, lesson, or crystal IDs. Unrequested families are listed as excluded. `globalFamiliesComplete` is always false: finishing this selection does not claim that every family in the corpus was examined. Deferred outcomes remain partial with `completed: false` and a terminal reason.

Malformed generation is `failed`; ambiguous generation or publication dispatch is `blocked`; index and graph waits describe actual retrieval dependencies. Invalid output and unknown dispatch do not trigger automatic regeneration or repeated judging. Consolidation resolves a real child's completed, partial, failed, or blocked state once instead of repeatedly continuing a terminal child.

## Manual continuation and replay

The authenticated REST route `/agentmemory/reflect` accepts a project, an optional request ID, and a valid continuation. It invokes `mem::reflect` with the ordinary manual workflow fields. A continuation belongs to the same request, project, job, checkpoint, and cut; altered or foreign continuations fail. Exact replay returns the saved response without a new provider call or effect.

```typescript
const business = { manual: true, project, requestId, query, maxClusters: 1 };
const first = await sdk.trigger({ function_id: "mem::reflect", payload: business });
const next = { ...business, continuation: first.continuation };
const result = await sdk.trigger({ function_id: "mem::reflect", payload: next });
```

Generation records its intended effect before calling the configured provider and durably captures the raw response before judging or applying it. If execution ends without a capture, the job stays blocked as `reflect_generation_outcome_unknown`; the same unit is not sent again automatically. A late response may fill that capture and the exact continuation can then resume. Publication judging also stores its own durable request and raw response through `publication-evidence.ts`.

Automatic recovery remains paused when configured and does not dispatch manual jobs. The manual workflow has a finite 256-cut operation limit. Individual provider calls have a 45-second timeout. These bounds do not authorize a live request or guarantee that an external provider will return a result.

## Explicit recovery

A manual reflection can request a successor using a fresh `requestId` and the exact `recoveryOfJobId` of an eligible predecessor. Recovery preserves the predecessor's frozen seeds, manifest, source page, cursors, source versions, index identity, graph revision, and any captured response.

Eligibility requires the same project, request selectors, and session identity; fewer than 256 cuts; no applied clusters or insights; and current source identity. A historical operation stopped only by the 256-cut limit must have no captured response. A blocked or failed operation with a captured response must have no publication checkpoint or admitted effect. Any generation intention, progress, result or skip receipt, callback, or conflicting active effect prevents recovery.

A predecessor-keyed journal writes the successor's frozen job, pending intent, and operation identity before marking construction ready. Retrying the same recovery request fills only matching missing rows. Conflicting rows fail without overwrite; a ready successor is never reconstructed from its predecessor. Old jobs, captures, publication records, and acknowledgements remain unchanged. Admitted effects resume through their original receipts and are never copied to a successor.

The keyed locks coordinate one serving iii process; they do not provide cross-process exclusion. Source drift before effect admission fails with the old evidence retained. A canonical effect is applied once through its existing receipt.

## Publication evidence

Reflection judging uses the same durable publication-evidence contract as summaries, skills, and consolidation. The complete original sources and lineage are included in the request. Only a `supported` result permits an insight effect, after current source identity is rechecked. The judge can misread evidence; it is a fallible semantic check, not a guarantee of correctness.

## Validation scope

Structural and mocked-provider tests cover parsing, counts, replay, late capture, source drift, effect acknowledgements, and recovery construction. They do not establish live-provider quality or prove that an installed runtime contains the current source.
