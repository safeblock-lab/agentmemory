# Consolidation continuation

Normal `mem::consolidate-pipeline` calls perform bounded work and return `completed: false` until every requested stage explicitly completes. Repeat the full top-level `continuation` unchanged. A nested stage result, `queued: false`, or terminal reflection failure does not establish pipeline completion.

For an explicit project invocation with `force: true` and `deferred: false` (or omitted), the normal `all` and `reflect` tiers use foreground manual reflection. Each pipeline call advances at most one manual reflection cut, with at most two provider calls in that cut. The original reflection fields and exact returned checkpoint are retained durably; subsequent pipeline calls reuse them. The underlying manual operation retains its 256-cut and 30-minute limits. Failed, partial, or exhausted terminal reflection stops the pipeline before decay. Calls using automatic project selection, catalog continuation, deferred processing, or without `force: true` retain ordinary queued reflection behavior and do not bypass paused recovery.

Reflection request identity binds the consolidation selection, original summary content and versions, and invocation policy. Completed reflection is reused on identical pipeline repeats; changed source content produces a distinct request.

A normal invocation may reuse a completed manifest originally produced by selected foreground consolidation only when the original manifest identity, project, caller policy, source fingerprints and versions, completed result, and canonical publication acknowledgements remain valid. Replay reads at most 12 final pages with at most 12 candidates per page and returns zero new facts, procedures, or applied effects. It does not relabel the historical selected marker or alter that root or its checkpoint. Inflight selected manifests still require selected foreground execution; mismatched or missing evidence is refused without provider calls.

This behavior does not install software, alter runtime settings, or resume background recovery. Operational approval remains separate for those changes.
