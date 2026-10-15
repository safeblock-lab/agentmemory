# Summary recovery

Manual recovery resumes through the existing summary API. It preserves the failed attempt, its source evidence, and each cached response; recovery creates or reuses a deterministic child job.

## Start and resume

Start a manual summary with mem::summarize or the summarize endpoint. If the source snapshot has a retained failed manual publication, the response includes the supported full-originals continuation. Pass that continuation unchanged to the same API. The queue derives the child ID from the operation ID and returns the same response when a continuation is replayed.

Do not construct a continuation from stored fields. Do not edit job records, source captures, operation state, or cached request responses. A child with mismatched source identity, generation, or parent is retained as a conflict.

An active historical child blocks recovery while its saved continuation remains valid and its historical deadline has not expired. An explicit current continuation can retire only the exact deterministic direct historical child after its deadline or request limit is reached. Current continuations are never retired for idle time. The transition preserves its job, unit, raw capture, source evidence, and cached responses, and releases only its matching active lease. Replaying the old cut returns its original cached response. If the child already has an applied publication receipt, recovery completes that effect first. A conflicting canonical value preserves the active child, receipt, and canonical value unchanged.

## Active queue coordination

Acquiring, clearing stale, and releasing a session's active-job pointer share the short summary-active keyed lock. A release re-reads the pointer while holding the lock and deletes it only when the job ID still matches. Existing session, unit, job, and canonical locks may acquire the active lock last; code holding the active lock must not acquire those locks. Provider calls remain outside this lock.

This coordination uses the existing single iii Node worker's in-process keyed mutex. It does not provide cross-process compare-and-delete or a durable distributed lease.

## Persistent manual operations

Idle time does not expire a manual summary workflow. New operations and responses have no workflow deadline. A historical deadline remains optional stored metadata; current resumes ignore it. The workflow retains its bounded request limit and provider timeout. Resume a pending child using the exact continuation already returned; do not request another identity.

A historical failed child with manual_summary_limit_exceeded can request an explicit successor through POST /agentmemory/summarize with a session ID and zeroSpendRecoveryOfJobId, or through mem::summarize with the same fields and manual true. The trimmed predecessor ID must contain 1 to 128 characters. Do not combine this request with a continuation. The creation response returns the standard child continuation.

Eligibility is deliberately narrow: the predecessor must be the exact deterministic historical child of an original failed manual summary_publication_invalid_response job. Its frozen source and delivery identity must match, with step zero, the expected request limit, and unit attempts zero. Missing or malformed identity and progress fields fail closed. Any raw record, output, validation, publication effect, completion, or cut dispatch record rejects recovery. Generation persists its raw intent before dispatch, and publication judgment requires captured, parsed output, so an eligible predecessor cannot have issued a publication checkpoint through the supported path. The failed child itself must contain no provider or publication work. A competing active job is a conflict.

The successor IDs derive from the predecessor, original root, project, session, and source fingerprint. Original rows, predecessor rows, failed markers, and old acknowledgements remain unchanged.

## Crash-safe construction

New recovery jobs first persist a creating construction journal in the existing state scope. Construction writes the exact unit, job, and manual operation before acquiring the active pointer, then changes the journal to ready. Dispatch, unit processing, advancement, and manual resume reject incomplete construction. Retrying the same request after a crash fills only missing rows that match the frozen journal, rejects conflicting rows without overwriting them, and never rebuilds or resets a progressed ready successor. Creation acknowledgements remain immutable. Existing historical pending jobs without a journal retain their identity and can resume normally.

Recovery creation locks the manual session, summary session, sorted predecessor units, and predecessor job in that order. The short active lock is acquired last. This assumes one serving iii Node worker; it makes no cross-process claim.

## Provider execution and unknown outcomes

Generation and publication judgment use the application's configured provider and normal routing behavior. The request limit bounds manual continuation work; provider retry and fallback policies remain configured independently.

Never redispatch an intended raw capture: its provider outcome is unknown. A request record without a response is not treated as a cached acknowledgement; when its request limit is exhausted, the queue checks for exact no-call evidence and terminalizes before dispatch when none is available. A captured raw response remains stored and can be parsed only through a continuation when the matching unit and capture are fully identified. Late raw responses are recorded idempotently after terminalization, but parsed output is admitted only while the matching job is pending. An unsupported publication judgment remains terminal and is not sent to the provider again.

## Publication recovery

Before publication, the queue revalidates the job's source evidence. At the canonical write boundary it takes the shared summary-canonical lock, confirms that the session still exists with the same ID and project, then reads the canonical value and publication receipt. New receipts store the exact previous canonical identity before writing the summary under that same lock.

If a receipt exists but the canonical write did not finish, recovery restores the effect only when the canonical value is absent or still equals the receipt's saved preimage. Existing legacy receipts without a preimage can be restored only when canonical state is absent; an already exact canonical effect is accepted. Any different canonical value is a conflict and remains unchanged. Session deletion or project identity change returns summary_source_changed and does not recreate a canonical summary.

The canonical lock serializes summary writes and deletes that use the same key. Evidence verification remains before the canonical lock; the lock does not serialize observation changes or replace source validation.

## Deployment and rollback

Source changes require the normal build/install/start workflow before they affect a running worker. Offline tests do not establish live provider or restart behavior. Source rollback does not delete or reset jobs, journals, raw captures, intents, counters, or acknowledgements. Do not run an older worker against a partially constructed successor; restore the compatible implementation and retry the same request to finish construction.
