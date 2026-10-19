# Normal recovery boundaries

Summary publication verifies every frozen source through its exact row version and content hash. The final session membership check pages only `id` and `title`, because that check compares titled observation identities and counts. The page limits remain 12 rows and 16 KiB; full provider, tool and observation bodies are not copied into membership pages. This does not relax frozen source verification or publication lineage checks.

Ending a session requires an existing session with matching identity and a nonempty project. The REST endpoint rejects missing sessions with `session_not_found` and malformed identities with `session_identity_invalid` before creating a summary intent or updating state. The ended event applies the same guard. End notifications cannot create sessions containing only completion metadata.

Local semantic dirty recovery retains its existing embedding identity, source version, metadata reuse, page, work and time limits. If a concurrent capture creates a dirty source during the final readiness check, recovery continues within the same remaining limits. Identity, graph, lexical or other readiness failures still fail closed. The indexed corpus marker is read from current state during completion; it is never treated as proof of source coverage by itself.

Recovery error logs include the phase, job/session identity and sanitized error codes. They omit observation, provider and session bodies. A failed operation retains its original error; logging does not mark it complete or replay it.
