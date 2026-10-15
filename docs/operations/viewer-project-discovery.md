# Viewer project discovery

The graph project picker discovers projects through bounded session pages. Its list can be incomplete. Each successful page adds project names and preserves the selected project.

`GET /agentmemory/sessions` returns HTTP 409 with `{ "error": "viewer_cursor_stale", "restartRequired": true }` when session changes invalidate a continuation. An invalid continuation returns HTTP 400 with `{ "error": "invalid_viewer_cursor" }`. Other storage failures remain errors.

The graph picker restarts discovery once after a stale continuation and displays a notice. It retains discovered projects and the current selection. If recovery fails, it clears the unusable continuation and offers an explicit retry. Native pagination consistency checks remain unchanged.

Use **Project name → Open project** to open an exact project that has not appeared in discovered pages. This uses the existing project-scoped graph query and statistics readers. It does not rebuild the graph or invoke a generation provider. Partial graph results continue to display their warning and omit unavailable full totals.

The viewer proxy permits up to 360 seconds for `POST /agentmemory/summarize` and 180 seconds for generation POSTs to `/agentmemory/reflect`, `/agentmemory/consolidate`, `/agentmemory/graph/extract`, `/agentmemory/graph/build`, `/agentmemory/consolidate-pipeline`, and `/agentmemory/slot/reflect`. Other methods and paths retain a 10-second bound. The proxy never retries these requests. These limits do not override an earlier backend timeout; durable operation continuations remain authoritative after an interrupted request.

## Memories, Crystals and Profile

Memories accepts an exact project name before fetching a bounded source page. An empty name selects all projects. Search and type filters apply only to the current page, not the corpus. Refresh returns to the first page; Next replaces the current page without accumulating records. Failed reads retain the previous data with a stale/error notice. Changing scope clears the old scope's data, and late responses cannot replace the newly selected scope.

Crystals uses the same exact-project and page-only search interaction, with at most 16 records requested. Its required backend envelope includes `crystals`, `partial` and opaque `nextCursor` (string or null). A response without pagination metadata is an unavailable page, not an empty or complete collection. The panel does not fetch the full lessons collection to resolve source IDs. Lesson IDs remain visible when their text is not supplied. Installation must include the bounded crystal REST/list backend contract together with this viewer change.

Profile accumulates project names from bounded session pages, retains selection, and offers an exact-name input for undiscovered projects. Discovery failures and preparing states remain visible and offer Refresh. Profile requests are bound to the selected project and a request sequence, so late responses cannot show another project's profile. Partial profile counts remain sample counts.

Timeline retains at most 200 observations from a bounded source window and renders 20 at a time. Local pagination makes every retained row reachable before Next observations fetches another source window. Importance and type filters apply to the retained window; counts are window counts. Type filters are keyboard-operable buttons. Changing the session resets source continuation and filters. Reconnect refreshes the current observation window and refreshes the active Graph, Crystals or Profile surface; it does not establish operation completion.

Function tables describe recorded telemetry across projects. They are not the full registration catalog and do not prove successful output. Tables scroll within their cards on narrow screens.

These source behaviors require installed browser acceptance. Check desktop and mobile, keyboard focus and node inspection, project switches during pending requests, pagination, filters, preparing/partial/error states and reconnect. HTTP status or offline tests alone do not establish rendered acceptance.
