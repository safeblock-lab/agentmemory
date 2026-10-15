# Bounded viewer data reads

Memory, session and timeline reads return at most 16 candidates in a 64 KiB page. `nextCursor` continues that storage page; the viewer exposes native buttons to browse further. Search, sorting and importance filters apply to the displayed page. They do not search the entire history.

The default shared memory view reads one page of whitelisted display fields directly from state. Latest filtering applies to that same current page. Local history remains visible even when old records have no project or sharing audience; this authenticated view does not use a sharing or synthesis projection. Oversized or stale page failures propagate explicitly. `total` is null when the response cannot establish a complete total, including continuation pages. A partial empty page does not establish an empty memory store.

Project memory reads require `latest=true`: they first select up to 12 exact project sessions through the native project catalog, then select versioned current canonical memory sources for those sessions. Project filtering happens before memory selection and content hydration. The native project projection reports deferred sources as partial. Historical project listing requires an additional native history selector and is rejected explicitly. The ordinary global viewer retains historical listing. Project session cursors are bound to project, catalog epoch and revision. Stale cursors require restarting the read.

Profiles use the same exact project session selector and one observation page per selected session. Their counts describe the bounded sources. The `partial` flag identifies incomplete history, and cached profiles carry the current session catalog revision and epoch. A missing or incomplete catalog is reported explicitly.

The profile view explicitly labels partial profiles and sampled session/observation counts. The graph view preserves query readiness warnings and unknown totals. An incomplete or unready index is shown as pending, rather than an empty history or a disabled extraction feature. Opening the graph view only reads data, even for an empty ready graph. Rebuilding remains an explicit manual action.

Agent-filtered listing requires an authoritative native agent selector. These endpoints currently reject that filter explicitly rather than returning data from another agent or pretending that a filtered page proves absence. Shared mode remains the default. Timeline observations are selected by their exact session scope before paging.

These reads use the iii state and retrieval interfaces. They do not scan the full corpus, change native resource limits, or call an LLM provider.
