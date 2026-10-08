# Graph session source capture

Graph extraction captures one session's compressed observations before graph
application. The capture uses iii-engine state functions and retains native
scope revision checks.

## Consistent source read

`readGraphSessionCapture` reads only `KV.observations(sessionId)`, with pages of
at most 256 records and 1 MiB. It performs no durable graph writes between page
reads. The scope generation and revision must match before and after the entire
scan, including single-page and empty scans.

A stale cursor or changed final revision discards the entire in-memory attempt.
There are at most three restarts, with one 30-second deadline across all attempts.
The scan is bounded to 100,000 scanned records and 64 MiB of serialized source
page items. Exceeding the deadline or capacity fails with
`STATE_TX_LIMIT_EXCEEDED`; it never returns a truncated capture. The serialized
byte limit does not represent a precise JavaScript heap allocation limit.

Native reads cannot be canceled through the current SDK. A read that outlives the
deadline may settle afterward, but cannot publish, continue paging, or modify
the graph. Only one source page request is outstanding per attempt.

## Durable publication and recovery

After a coherent scan, each retained observation is frozen under a fresh attempt
namespace. A complete source reference is persisted only after all rows have
been written. Interrupted publication leaves unreferenced partial rows; replay
uses a fresh namespace rather than mixing old prefixes and a newer session
revision. Terminal job retention owns eventual removal of those remnants.

Complete older captures retain their existing row keys and remain replayable.
An exhausted stale initial capture is invalidated with no graph effects and no
scheduled retry. Later deltas retain their existing recovery behavior. Logs
report only job identity, attempt/page/row/byte counts and elapsed time for a
successful source read, not observation content.

## Provider boundary and observed status

The extraction request has no per-request heuristic-only switch. Its existing
local branch is selected when `GRAPH_EXTRACTION_ENABLED` is not `true`, or when
the provider is a noop provider with no auxiliary provider and no deferred batch
operation. Changing that configuration is not part of this repair. A normal
enabled extraction can proceed to paid provider work after source capture;
`force` bypasses some compaction paths but does not disable providers.

This source repair addresses observed `STATE_PAGE_CURSOR_STALE` failures at
initial delta 1. Source review and installation are not evidence that a real
extraction completed capture, staging, application and finalization. The release
operator must report its actual operation result separately. Continuous source
writes may still exhaust bounded attempts; that outcome requires evidence before
considering a native immutable source snapshot.
