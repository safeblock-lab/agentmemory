# Graph display browsing

The Graph viewer browses the selected project's current graph through
`POST /agentmemory/graph/query` with `display: true`, an exact `project`,
and an optional opaque `cursor`. Page limits range from 1 to 128. Walk,
query, node-type and offset selectors cannot be mixed with display browsing.
Existing full graph query consumers keep their contract.

Display nodes contain ID, type and a Unicode-safe name of at most 256
characters. Edges contain ID, type, endpoints and finite weight. Prepared
reads use compact indexed display fields and project membership in one
consistent native snapshot. They do not decode stored originals or rehydrate
full properties and provenance. Stored originals remain unchanged.

## Preparation and recovery

Entering Graph first reads the selected project. `status: preparing` means
membership is still being prepared; it never means the current graph is empty.
The viewer requests one bounded preparation batch through the same endpoint:

```json
{"display": true, "prepare": true, "project": "agentmemory"}
```

This request has no cursor or other query selectors. Its response contains
`preparation` with `status`, `phase`, `processed`, `revision`, `continue` and
an optional `reason`; it contains no nodes, edges or totals. `processed` is
work in the latest batch, not a project total or completion percentage.
Native preparation persists its phase and cursor so interrupted work resumes.

The viewer yields between batches, with one preparation request in flight.
It continues only while `continue: true`, then reads the first page freshly
when preparation reports `ready`. A stopped or failed batch displays its
reason and Retry. No local batch cap fabricates completion. Cancel preparation,
changing project, or leaving Graph stops continuation and discards late results.
Retry resumes through a fresh read and the same bounded preparation workflow.
Only a current `ready` page with no nodes is displayed as an empty graph.

## Pages and connections

Previous and Next retain opaque cursor history for the selected project.
Changing projects resets history. Cursors bind project and published revision;
malformed or foreign cursors are rejected. `stale_graph_revision` clears history
and triggers a fresh first-page read. If that read is still stale, Retry is
shown rather than repeatedly restarting. A ready membership can be read
immediately after revision refresh without another preparation batch.

The entire serialized native response, including metadata and cursor, fits
within 64 KiB. A projected row that cannot fit produces an explicit error.
Pages contain at most 128 nodes and 256 edges. Edges join endpoints on the
current page (`edgeScope: page`); cross-page edges are `not_loaded`, so the
viewer cannot claim there are no connections across pages. `edgesTruncated`
separately identifies omitted page connections due to response bounds.
All displayed counts describe the current page; historical global counters
are not presented as selected-project totals.

Node selection uses the display projection without fetching full properties
or provenance. Loading, preparation, current empty, stale and error states
have separate recovery behavior. Preparation status is announced to assistive
technology; pagination uses native buttons and wraps within the sidebar.
The sidebar also offers a native node selector for keyboard and touch inspection
of the current page. It exposes the same display fields and page connection
counts as canvas selection. Search and type filters describe the current page.
Canvas resize handlers are replaced when the graph canvas is replaced, retaining
one resize subscription instead of accumulating detached canvases across pages.

Deploying requires rebuilding the native engine with the tracked patch and
the application. Source tests do not establish installed engine support.
Build, installation and live acceptance belong to the sole runtime operator.
