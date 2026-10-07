# Export pagination contract

The no-argument export remains available when its complete response fits the engine transport limit. For large stores, page one collection at a time through the REST API or the existing `memory_export` MCP tool.

## Request collection pages

Start with a collection and a bounded limit:

```text
/agentmemory/export?collection=graphNodes&limit=100
```

Continue by passing the returned `pagination.nextCursor` unchanged:

```text
/agentmemory/export?collection=graphNodes&limit=100&cursor=<nextCursor>
```

`collection` accepts `sessions`, `observations`, `memories`, `summaries`, `profiles`, `graphNodes`, `graphEdges`, `semanticMemories`, `proceduralMemories`, `actions`, `actionEdges`, `routines`, `signals`, `checkpoints`, `sentinels`, `sketches`, `crystals`, `facets`, `lessons`, `insights`, and `accessLogs`. `limit` defaults to 100 and must be between 1 and 1000. `offset=0` may be used to start a page, but nonzero offsets are rejected; continuation requires the cursor.

The MCP tool accepts the same collection, cursor, and limit fields. Calling it without arguments still requests a compatible full export. Import collection pages with `merge`; when replacing a destination, use `replace` on the first page and `merge` for every later page so each page does not erase earlier imports.

Each page is an `ExportData` object whose records belong to the requested collection. `pagination.collectionRevision` is an opaque state revision token, not a content fingerprint. The server checks it before and after reading each page. A write to that collection between pages invalidates the cursor with `STATE_EXPORT_COLLECTION_CHANGED`; restart that collection's export after writes stop. This is a consistency guard, not a cross-request snapshot.

`pagination.nextCursor` is present when more records or bounded traversal work remains. `pagination.total` appears only on the final page and counts records actually emitted across the unchanged collection. An observations page can contain no observations while still returning `hasMore: true`: traversal is capped at 16 session scopes per request so many empty sessions cannot hold a request open. Continue until `hasMore` is false. Observation traversal checks both the session list and the observation-scope revision range.

All collection records are enumerated through projected identity pages and fetched individually. No record is skipped to fit a page: if the next record does not fit the remaining byte budget, it stays on the next cursor. A missing or mismatched point-read record fails the page. If one individual record exceeds the response limit, the server returns an explicit oversized error; that record cannot be split.

## Legacy session pages

`maxSessions` and `offset` remain available for existing callers of the legacy full-export path. That path pages sessions but returns other collections as before, so use collection cursors when the complete export exceeds the response limit due to any collection.
