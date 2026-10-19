# Dashboard request bounds

The dashboard requests bounded previews from the existing semantic, procedural and relation GET routes with `preview=true`. Each preview reads one projected page of at most five records and 64 KiB. Preview results are partial and do not declare a global total. Requests without the parameter retain their existing full-list behavior.

The existing consolidation status route also accepts `preview=true`. It reads the last recorded run without scanning five corpus scopes. Its layer counts are `null`, rendered as unavailable; the last recorded outcome remains visible. A preview does not prove current source selection or pipeline completion.

Status dependencies run in parallel. The six-second savings budget includes waiting for the shared function metrics result, so it no longer starts after the five-second dependency budget. Missing evidence remains unavailable. The viewer renders available indicators when recent session history fails, preserving previously loaded data and distinguishing unknown counts from zero.

Application emission includes the repository package asset copy step. The installed viewer HTML and favicon therefore accompany the emitted JavaScript. Normal startup applies nonsecret feature flags from the existing user environment file and records the selected values before launching; it does not replace missing settings with enabled flags.

Recent history GET requests share one in-flight preparation per application state client and explicitly request at most 16 records and 64 KiB. They no longer inherit the native 4,096-record/32-MiB defaults. The native `max_ms` parameter is a safety deadline input, not a successful-batch time slice: native work-based minimum deadlines remain in effect. Cursor, dirty-source and issue state remain authoritative; missing or unindexable history is displayed as unavailable, not empty.

The dashboard can reuse health and function evidence from its status response when the separate health request lacks that component. It does not substitute an unavailable result for an observed successful result or manufacture counts from a failed request.
