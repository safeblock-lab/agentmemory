# Paged consolidation and recovery

`mem::consolidate-pipeline` uses the shared pager for semantic and procedural
consolidation. `mem::consolidate` uses it for long-term memories. The pager stores
its state through iii-engine `StateKV`; it does not open the database directly.

## Selection and capture

A new job selects at most 12 lightweight references from one exact project.
References retain source ID, session ID, scope and native decimal row version.
Summary references acquire their version during the first capture cut. Selection
is durable before later workflow stages run, and catalog jobs retain their chosen
project until those stages finish.

New semantic jobs read and advance an exact project checkpoint, with a mutex
keyed by that checkpoint. A newer project cannot suppress older evidence in
another project. Legacy cohorts retain their original global watermark. An older
v2 root without checkpoint provenance keeps its capture/effect identity and
historical baseline; capture/quality can safely acquire the project checkpoint.
Later ambiguous states remain explicitly retained rather than adopting a global
watermark as project authority.

Each invocation advances one phase and returns a continuation. Capture reads at
most one source record, with the existing 16 MiB codec ceiling. It checks the
session project, source identity, selected version and source project where the
record carries one. A source outside that ceiling stays pending with an explicit
error; the pager does not truncate it or manufacture a new generation.

The complete source JSON is fragmented, including fields that extraction may not
use. Joining `$json` fragment text reconstructs the captured value, including
Unicode, empty arrays and strings containing quotes or newlines. `start` and
`end` are UTF-16 offsets, with surrogate pairs kept together. Each fragment has
at most 2 KiB of UTF-8 text, reduced further when JSON escaping needs more of the
actual serialized prompt budget. `count: 0` means the streaming capture has not yet
published its final count; the immutable `capture-complete` record supplies that
count and total length. One capture page contains at most 12 fragments.

Source headers, fragment pages, candidate pages, provenance pages, publication
receipts and the root are each limited to 64 KiB. New page keys use the complete
SHA-256 digest. The root contains bounded references, counters and cursors; it
does not contain all fragments, candidate facts or provider responses.

## Extraction, reduction and quality

Before an extraction call, the current original source is checked against its
captured version and fingerprint. Project membership is checked again before
semantic reduction and publication. Original sources are verified individually
after extraction and reduction, before canonical publication starts. A changed
source or project prevents dependent effects and checkpoint advancement.

Every provider request, including its manifest prefix and serialized envelope,
is limited to 8 KiB. At most two new calls or batch enqueues may occur per cut;
the current paged extraction and reduction stages ordinarily issue one. Direct
summarize/compress calls use the configured provider routing and retry policies.
Non-idempotent batch generation submission uses its existing submission
identity and reconciliation. A
captured response is durable before its candidate page or extraction cursor
advances. Repeating a cut or callback reuses that response. Conflicting callbacks
leave the first response intact.

New responses use immutable pages, with a total response ceiling of 64 KiB and
at most eight pages. This also bounds JSON-escaped output that would overflow a
single record when combined with its prompt. The request checkpoint stores the
page count, byte count and SHA-256 digest. Incomplete response capture stays
retained without another provider call; redelivery of the exact original result
can finish it. Legacy inline responses remain readable under their original
request IDs.

Semantic extraction requires real source session IDs from its input fragments.
Reduction requires each input candidate ID exactly once in its output coverage.
Provenance forms a paged DAG: a reduction node references its immediate children,
so a large ancestry is not flattened into a root or prompt. Reduction stops when
a round does not shrink, or at its round bound. Thousands of distinct candidates
can therefore finish as multiple final pages. The pager does not force them into
one lossy final prompt.

Semantic publication requires either two distinct original sessions or confidence
of at least 0.9. Original session IDs are deduplicated; repeated fragments do not
create additional episodes. Procedural extraction requires two original sources
sharing a concept, or an existing pattern with supported session frequency.
Long-term memory extraction requires three original observations sharing a
concept and the requested minimum observation count. Grouping ignores empty or
over-256-byte concept labels, while the complete original labels remain in the
captured JSON.

Procedures require a nonempty name and trigger and at least two nonempty steps.
Memories require an allowed type, nonempty title/content and XML within 4 KiB.
Memory source observation and session IDs come from the actual input fragments;
exact duplicate outputs union that original provenance.

## Distinct outputs with generic labels

Semantic candidates with the same normalized fact union their evidence.
Procedural and memory candidates deduplicate only an exact captured value:
normalized memory XML, or the procedure's name, trigger and steps. Two different
values with the same generic title or name both survive.

The aggregation phase records a small label group before publication starts. If
the group has distinct values, each receives a deterministic label containing a
short original label, a content or trigger excerpt and a 96-bit digest suffix.
Its complete content and steps remain unchanged. The original label stays in
candidate/provenance pages. The existing canonical memory schema has no separate
original-title field. The final disambiguated label drives canonical lookup,
record identity and receipt identity consistently. Reversing extraction order
produces the same labels; replay after a partial application uses the same
captured value and effect key.

## Durable effects and terminal accounting

Publication visits at most 12 candidate positions per cut. Exact duplicates have
one aggregate anchor and one canonical effect. Native canonical writers own
transactional identity and effect receipts. Publication stores the returned
receipt before advancing its cursor. If the native transaction committed but the
page receipt did not, retry probes the same native effect and records a replay.

The local keyed lock only serializes this Node process. It is not a distributed
lock or a replacement for native canonical transactions. Source checks are
bounded observations, not a multi-record snapshot transaction. A direct external
provider call that returns before its response can be durably captured cannot be
claimed exactly once. Already captured responses and native canonical effects
are replay protected.

A direct request durably records its dispatch mode and started attempt before
calling the provider. Reopening that attempt without a captured result returns
`provider_dispatch_uncertain` and makes no new call. This includes transport
loss, timeout, a successful response that cannot be parsed, and a cut before the
first response page is durable. Exact redelivery of the original response can
resolve the retained request. A lost acknowledgement of the intent write is
also conservatively retained, even if dispatch never happened. This mechanism
does not claim exactly-once provider execution.

Only a structured numeric rejection status in the closed set
400/401/403/404/413/422/429 marks the attempt rejected so the caller's existing
bounded retry policy can retry it. An error message resembling an HTTP status is
insufficient. Legacy pending requests without a captured result, work item or
durable dispatch identity remain `legacy_provider_dispatch_unknown`. A queue
intent without its work pointer can retry only the same deterministic enqueue;
it cannot silently switch to direct generation.

An opaque provider that cannot enforce this policy rejects before dispatch with
`PROVIDER_SINGLE_ATTEMPT_UNSUPPORTED` and `dispatched: false`. The request retains
that exact diagnostic and an unsupported attempt state on replay; it does not
automatically retry or report an uncertain remote dispatch for that known
predispatch rejection.

An exact queued work item is checked by ID, correlation, manifest, partition and
prompts. Active work remains queued. An exact completed result can restore a
missing response capture. Terminal, missing or incompatible work returns an
explicit retained failure with request/work IDs; it does not wait forever or
silently submit another request.

The existing explicit batch replacement action supports durable partitions. It
requires the coordinator's replacement guard and retains the original request,
capture and canonical effects. Replacement uses the same prompts and source
fingerprint, plus `replacementOf` for deterministic enqueue deduplication.
Immutable links connect the original work item to replacements, with at most 12
exact link reads and a hard chain bound. Replacement never writes the request
checkpoint, so a callback cannot be changed from captured/completed to pending.
A cut after enqueue but before its link retries the same enqueue identity.
Conflicting or foreign links remain explicit errors. Request/callback locking
shares the exact key inside one worker; this does not claim multi-worker CAS.

`completed: true` requires capture, extraction, source verification and every
final publication page to finish. Results expose coverage and separate examined,
rejected, created, applied and replayed counts. `newFacts` and `newProcedures`
count only receipts that were both applied and created. `createdFacts` and
`createdProcedures` retain historical creation counts on replay. `consolidated`
counts successfully published memory values, including native receipt recovery.
A terminal empty result has an explicit reason rather than fabricated counts.

## Preparation and resource-limited retrieval

`STATE_INDEX_NOT_READY` before capture returns `completed: false`, `queued: true`
and the same selected-input continuation. It performs no generation and does not
claim that the search found no relevant sources.

A usable, nonempty bounded search carries `retrievalCoverage` with at most these
two labels, in this order: `bounded_candidate_budget`, `bounded_query_budget`.
The selected references, capture root and result retain the labels. They describe
the bounded search coverage; they do not claim an exhaustive project search.

An empty interrupted query returns `STATE_RETRIEVAL_RESOURCE_LIMIT`,
`success: false`, `completed: false`, `partial: true`, `queued: false` and
`retryable: true`. Its exact continuation and sanitized selected-input
diagnostics remain available. The caller's existing bounded failure/backoff
policy decides a retry. This outcome is distinct from preparation and from a
valid empty search.

## Legacy recovery

Recovery retains the original manifest, request IDs, source fingerprints,
cohorts, checkpoints, captured responses and native effect keys. It imports one
codec-bounded legacy record per cut into the new pages. Sources captured before
fragment generation are streamed from that original record. Captured legacy
rounds and final responses are reused rather than sent to the provider again.
Historical records are not deleted by the pager.

Already completed legacy semantic manifests retain their terminal result.
Procedural manifests are inspected for ambiguous same-name captured candidates,
including manifests previously marked completed. The old small
batch callback path remains bounded and keeps its original effects. If a source
capture is too large for that older callback path, or its exact project/source
identity is unavailable, recovery reports an explicit retained failure. It does
not invent a replacement generation or advance the original watermark.

Legacy long-term-memory captures retain their original concept/request/effect
identity. If distinct captured partial memories share a generic title, the old
single-winner merge cannot prove their coverage. Such captures remain retained,
including already completed concepts whose necessary partial responses are
missing. They are not retitled or published under fresh identities. Ambiguous
same-name legacy procedural captures are likewise retained before publication.

## Validation and operational limits

The focused suites are `test/consolidation-pipeline.test.ts`,
`test/consolidate-project-scope.test.ts` and `test/consolidation-paging.test.ts`.
They use isolated StateKV and provider fixtures. They check reconstructed JSON,
per-cut bounds, 1,100 distinct facts, original evidence counts, project/version
changes, generic-label collisions, legacy identities, conflicting callbacks and
interruptions around durable pages, responses, cursors and receipts.

These tests do not establish installed-service behavior, actual model output
quality, physical storage savings, a billion-record benchmark or imperceptible
latency. One exceptional record still requires up to 16 MiB of decoding and
hashing per relevant cut. Continuation scheduling, machine load and native
transaction guarantees remain operational dependencies. Legacy fragment counting
and emission make two passes over one codec-bounded source; their binary-search
serialization candidates are bounded by the 8 KiB prompt window rather than the
remaining full source. This is not a universal execution-time guarantee. The pager itself does
not unpause global recovery, restart services or clean up storage.

The call budget is enforced at the injected summarize/enqueue boundary and the
uses the real router, pooled provider and HTTP wrapper with a simulated fetch to
count requests on transport loss, 503, invalid JSON and empty output. It also
checks that reopening an uncertain request performs no additional HTTP request.
This source-level proof does not establish installed-service behavior or an
exactly-once external generation guarantee after a remote acknowledgement loss.
