# AgentMemory 0.9.86

This release restores the Dashboard and Health activity panel to meaningful
operations: `mem::compress`, `mem::summarize` and `mem::graph-extract`, with
their outcome and available item counts. The feed retains at most 30 operation
results across service restarts. Routine session and observation writes do not
replace those entries, and observation text and provider error details are not
stored in the feed.

The feed uses the native engine's atomic bounded append operation. The app and
the companion iii-engine were built together so the storage operation is
available to the bundled engine. The engine build completed successfully; no
tests, typechecking or runtime probes were run, and the new native operation was
not exercised in a running service before publication.

Graph snapshot counts are historical summaries refreshed when material graph
changes alter the snapshot. There is no age or graph-size threshold that
automatically rebuilds them. The configured background graph recovery was
paused on the local installation, and its logs contained recovery-required
events after the snapshot timestamp. A full manual rebuild is constrained by a
25,000-node safety ceiling, below the current graph size, so no rebuild or
provider call was performed as part of this release.

This release reuses the published Qwen model and CPU runtime assets. The frozen
release contains the application package and exactly the seven approved public
assets, plus its package candidate and checksum manifest. Tests and typechecking
were not run, following the user's execution preference.
