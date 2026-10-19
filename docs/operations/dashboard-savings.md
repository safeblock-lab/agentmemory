# Dashboard savings methodology

This document explains the dashboard's observed token samples, estimated net
token savings, and API-equivalent price scenarios. It distinguishes identified
values from bounded samples and explicit scenarios.

## Current evidence

The study captured at 2026-10-18 10:04 UTC covers 2026-10-11 10:04 through
2026-10-18 10:04 UTC. Its retained evidence was read from StateModule using
`state::get_versioned`. Scope is available local
Codex/Claude profiles and the configured default agentmemory telemetry key;
there is no project or team attribution.

The expanded same-period extract records 9,666,879,383 input tokens and
30,393,555 output tokens, for
**9,697,272,938 observed tokens** across 64,290 deduplicated Codex responses.
Input already includes cache reads and writes, so cached tokens are not added
again. All 461 discovered Codex files were read (1,809,055,591 bytes); the
collector reports no truncation or skipped oversized lines. Claude remains
unavailable, so this is the available local Codex total, not all-client spend.
The original 158,819,841-token extract was partial and is preserved as evidence;
its percentages are superseded by this expanded denominator.

Context telemetry has 45 hook emissions totaling 64,379 estimated tokens and
29 MCP text replies totaling 34,770 tokens. The combined recorded text is
**99,149 tokens**. Emission proves text was written or returned, not that a
model consumed it. Repeated prompt exposure is not linked to these events.

The processing feed has a 500-event limit, is saturated, and retains 499
events in the study window. Those events report 624,063 input tokens and
148,892 output tokens, or **772,955 observed processing tokens**. The retained
events cover less than one day of the seven-day window. All 499 lack cache
categories and provider/model/request provenance. The observed total is a
partial sample contribution, not a seven-day total.

## Identified net savings

The intended token calculation is:

```text
net tokens = comparable avoided-read tokens
             - context tokens consumed by the agent
             - agentmemory processing tokens

percentage = 100 × net tokens
             / (deduplicated agent input + output
                + disjoint agentmemory processing input + output)
```

The current report leaves net tokens, denominator, percentage, and net USD
unknown. The retained evidence has **zero comparable baseline pairs**, emitted
context has no record of model consumption, processing coverage does not span
the full window, and processing overlap with profile usage is unknown. Missing
coverage and overlap stay unknown; they are not treated as zero. Negative net
values are preserved whenever all required inputs become identifiable.

The processing share of the observed sample is **0.0079702%–0.0079709%**. This uses
772,955 processing tokens over a denominator range of **9,697,272,938–9,698,045,893**
tokens, from full possible overlap to full disjointness. It describes the
retained contribution against the profile's observed seven-day spend; it is not
a complete-period or full-population ratio.

## Explicit sample sensitivity

The dashboard can show a closest-defensible contribution scenario while the
identified estimate remains unknown. These scenarios credit **zero avoided
read tokens as an explicit scenario assumption**, not as a claim that actual
benefit is zero. They apply each recorded context emission and MCP reply either
zero or one time, then subtract the retained processing sample. The same
denominator range accounts for unknown overlap:

| Recorded context exposures | Scenario net tokens | Percentage range |
| ---: | ---: | ---: |
| 0 | -772,955 | -0.0079709% to -0.0079702% |
| 1 | -872,104 | -0.0089933% to -0.0089926% |

These are retained-sample contributions over the observed period spend. They
are not actual net savings, causal attribution, confidence bounds, or a claim
that every emitted token was consumed. Exposure above one, omitted events, and
the full avoided-read baseline are unknown. The positive 7,236-token difference
between candidate and paired emitted context is a **selection scenario** only;
it does not establish an avoided file read and is not credited as savings.

USD net savings remain unknown. Processing records lack the provider, model,
request, and price-category details needed to match their cost to profile
usage. The lifetime LLM metric totals use a different period and do not supply
that missing cost attribution.

The separate fixed seven-day snapshot spans 2026-10-11T15:30:24.833Z through
2026-10-18T15:30:24.833Z. Its retained inputs are:

| Input | Observed value | Interpretation |
| --- | ---: | --- |
| E: emitted context | 123,591 tokens | Local `o200k_base` text count; consumption and repeated exposure unknown |
| P: processing | 3,159,393 tokens | 3,023,180 input + 136,213 output; reported telemetry, not invoice evidence |
| D: profile usage | 9,018,514,251 tokens | Available local profile sample; complete account coverage is not established |

No comparable avoided-read baseline is retained, so the actual causal numerator
and percentage remain unknown. The closest reproducible sensitivity calculation
credits zero proven avoided-read tokens **as a scenario assumption** and assumes
each emitted context token was consumed once:

```text
scenario numerator = 0 - E - P
                   = 0 - 123591 - 3159393
                   = -3282984 tokens
denominator = D + disjoint processing
            in [9018514251, 9018514251 + 3159393]
            = [9018514251, 9021673644]
scenario percentage = 100 * -3282984 / denominator
                    in [-0.0364027146%, -0.0363899663%]
```

The lower denominator assumes full processing overlap with profile usage; the
upper assumes full disjointness. These endpoints bound only that overlap
assumption. They do not bound actual savings, omitted history, repeated context
exposure or avoided work. The result, approximately **-0.0364%**, is a retained
sample cost-contribution scenario, not measured causal savings, statistical
confidence bounds or proof that actual benefit is zero. The scenario subtracts
context from its hypothetical benefit numerator; it does not add context again
to the already observed profile-usage denominator.

This later window must not be combined with the 10:04 UTC study above. Semantic
review found response-level usage deduplicated by response identity, without
reading cumulative usage counters; it justified no numerical change. Coverage,
processing overlap, actual context consumption and causal benefit remain
unknown. Establishing real net savings requires comparable avoided-read pairs,
linked context consumption, complete processing coverage and request identities
that resolve overlap over this same window. Actual net tokens, percentage and
USD remain unknown until that evidence exists.

## Price estimates

Observed profile price estimates use the per-model direct API-equivalent
tariffs in [`usage-pricing.ts`](../../src/functions/usage-pricing.ts), checked
on 2026-10-09 against the [OpenAI API pricing page](https://developers.openai.com/api/docs/pricing)
and [Anthropic pricing documentation](https://platform.claude.com/docs/en/about-claude/pricing).
The source records model, token category, supported service-tier/platform/
geography metadata, and any missing-price reason. Aggregated values are
API-equivalent scenarios, not invoices, subscription refunds, or quota credits.
Unknown models and unsupported metadata remain unpriced; the dashboard does
not substitute a single model tariff for them.

The previous fixed GPT 6.1 Sol valuation and the old processing USD range are
not authoritative for current net savings. The current report keeps
`netSavingsUsd` and the processing sample's USD range null until a comparable
baseline and attributable processing cost exist.

## Data limits and implementation

Each context and processing feed retains at most 500 events per configured
agent key. Current evidence reports its period, retained counts, time span,
invalid/duplicate counts, and limit status. A bounded feed cannot reconstruct
dropped history or prove that an empty feed means zero activity. Context counts
use local `o200k_base` tokenization and include emitted text; server framing and
model-side tokenization may differ.

The report is assembled by [`savings.ts`](../../src/functions/savings.ts) from
[`savings-evidence.ts`](../../src/functions/savings-evidence.ts),
[`agent-usage.ts`](../../src/functions/agent-usage.ts),
[`context-usage.ts`](../../src/functions/context-usage.ts), and
[`processing-usage.ts`](../../src/functions/processing-usage.ts). Its pure
net-token calculation and sample scenarios are in
[`net-savings.ts`](../../src/functions/net-savings.ts). Agent usage is bounded
to available local profiles and deduplicated by client response identity; no
cross-client request identity is retained. The dashboard does not infer
consumption, replacement work, missing events, costs, or repeated exposure.

The cached background profile collector allows 2 GiB, 2,048 files, 4 MiB per
line, one million records and 30 seconds per client. These limits accommodate
the measured local period (about 9.5 seconds) while keeping parsing bounded.
Concurrent requests share one scan and its five-minute cache. Dashboard callers
retain their 1.5-second wait bound; a pending scan continues to populate the cache.
Future profiles exceeding these limits remain explicitly partial.

Current source includes opt-in native `keyset_live` paging, finite COPY seed and
position bounds, legacy progress adoption without reset, coalesced inline queue
work, and unchanged-header no-ops. A single coalesced adaptive scheduler continues
observed progress with backoff and an unknown-outcome hold. Startup uses bounded
paired reads and schedules recovery after policy setup and function registration,
before index initialization. Usage projection no longer gates recovery.

Native status separately reports `reservation: 1`. Adoption of an expired,
unspent `PREPARED` reservation requires full native proof, an unclaimed
reservation and no child slot, and retains the same single extra reservation.
Epoch rotation does not reset spent budget or authorize unknown-outcome retries.
Reevaluation requires fresh native proof and the same allowance and native
budget. Terminal-projection migration is limited to incompatible cursors, and
candidate errors retain safe attribution without raw content. COPY requests
are bounded to 1,024 rows with internal chunks of 128 and a 129-row lookahead;
the 1-MiB byte bound, 2,500-ms work budget and 45-second transport timeout are
unchanged. No speed improvement is inferred from these bounds.

The latest paired application and native update was installed on 2026-10-19 at
09:12:39 UTC, advertising 22 native capabilities. Both prior processes completed
normal CTRL-C shutdown; no SQLite copy was performed and the watchdog was
restored. The earlier native update at 08:00 UTC retained the application.
Installed COPY SQL separates first-page and continuation queries and removes
the mode-selecting `OR`; its native build exited with code 0. This establishes
no measured speed improvement or complete runtime acceptance.

The installed application gates pending graph work before reservation and retires superseded
work only with canonical and native proof bound to the successor fingerprint.
Shutdown draining tracks actual callback promises, FIFO work and maintenance,
gates HTTP/MCP admissions with 503, and holds after 180 seconds without SDK
abort. External direct SDK reentry and detached asynchronous work remain outside
that boundary; universal atomicity is not claimed. Full runtime acceptance
remains pending. No tests were run, and the release remains unpublished.
Further CLI shutdown changes were compiled and installed at 08:41:44 UTC through
a CLI-only update, without restarting the backend. No graceful-stop functional
test was run; installation alone supplies no operational acceptance result.

The maintenance observation at 2026-10-19 08:41:30 UTC showed text
layout in `CUTTING` at ordinal 4,969, with COPY seed, row traversal, GC, final reclaim and
physical-baseline advancement incomplete. A later observation at 08:47 UTC
recorded cursor advancement, without proving completion. Physical reduction is not proven.
At 08:37 UTC the application and engine processes were unexpectedly absent;
their startup was recovered at 08:39:41 UTC after correcting the existing
launcher's file-replacement handling. Recovery required no reinstallation or
SQLite copy. The cause of the earlier process exit remains unknown. Startup
does not establish that maintenance has resumed or completed. The retained
failed empty-XML operation now has a durable `explicitEmptyXML` repair record
with zero nodes and zero edges, and its linked workflow is completed with
10 nodes, 20 edges and five units. This was observed on 2026-10-19 at 05:01:38 UTC;
the completion timestamp predates the latest installation and is not attributed
to it. The previously blocking graph job completed, but the second workflow's
child failed with `STATE_GRAPH_AUTOMATIC_REGENERATION_REQUIRED` while its cache
still reported a child present. The latest installed update adds automatic
recovery of an expired child before execution using native proof bound to the
same child and its retained history. It preserves the same allowance and spent
history, limits recovery to four attempts per hour with a 60-second interval,
renews the 120-second reservation before preparation, and uses typed waiting
outcomes. The affected workflow's outcome under this update remains pending;
installation does not establish its repair. The
watchdog's original enabled state was restored at 08:43 UTC without forcing a
run; the primary task remained disabled. Full runtime acceptance remains pending.
Hook changes cover 18 configurations; no measured
time savings have been established. None of these facts supplies comparable
avoided-read pairs or alters either fixed savings window. Global and causal
savings remain unknown.

The latest compiled source tightens revision and cache reevaluation using the
required native proof and resets the query cursor when selection changes.
Retirement of an old pending operation requires atomic `notApplied` proof under
the newer, stronger lease. A dedicated sparse-session summary prompt is also
included. Installation and operational results for these further changes remain
pending, including the previously unsupported summary, graph recovery, GC and
final reclaim. No token benefit or positive global net savings is inferred
from source completion or compilation.

The attempted further update remains on a shutdown HOLD after its 210-second
wait; no update files have been copied. Additional summary corrective-revision
and bounded index-drain diagnostic changes remain source work. Optional pruning
is deferred until closure. None supplies an operational outcome.
