# Project-scoped graph rebuild

The viewer's `POST /agentmemory/graph/build` endpoint requires the selected project:

```json
{ "project": "D:/repo/example" }
```

Each request reads one bounded page from one session selected by the native project-session index, filters that page to compressed observations, verifies the session and observations are still current, and submits at most one `mem::graph-extract` job. It never enumerates all sessions or observation scopes.

When more source pages remain, the response is `partial` and includes an opaque `continuation`. Send the same project and that continuation to process the next page. A `queued` response keeps the same continuation so the exact unit can be checked or replayed safely; it does not mean extraction finished. A queued or extraction-failed continuation is bound to the graph job ID for the captured observation content. If that source content changes before replay, the endpoint returns HTTP 409 `observation_changed` without dispatching another graph job; start a new rebuild to capture the changed source. Failures before extraction keep the cursor for the same source page. The graph job ID is stable for identical project, session, and observation content, so repeating that request reuses the existing job.

Responses use `completed`, `partial`, `queued`, or `failed` and include `sessionsProcessed`, `observationsProcessed`, `nodesAdded`, `edgesAdded`, and `continuation`. Request validation returns HTTP 400, project/session conflicts return HTTP 409, source-page failures return HTTP 503, and graph extraction failures return HTTP 502. Error bodies contain stable codes rather than provider or internal error text.
