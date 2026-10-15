# Project authority projection

The native project catalog stores current positive authority in `retrieval_project_refs`. Compact exclusions distinguish stale or deleted records from records with an explicit null or empty project. Malformed records remain incomplete. Any existing non-null session takes precedence over the summary fallback. A summary must identify the session in its own storage key.

Ordinary writes, cold preparation and exact repair share the same authority extractor. Cold preparation accepts source documents up to the codec's 16 MiB limit and handles a document larger than the normal cut budget alone. Public page and preparation response limits remain 64 KiB. No original source value, version, position or revision is rewritten.

Projection format 2 performs a one-time, transactional reset of both source catalog epochs. Preparation then resumes through ordered source positions. Existing scoped and graph browse coverage detects the changed epochs and rebuilds through its established continuation protocol. Invalid source records stop preparation truthfully instead of reporting an empty graph or continuing without progress.

Session resolution and membership predicates use compact metadata joined to current source versions and catalog epochs. They do not decode full summary payloads on the read path. Exact repair retains revision, epoch, cursor and source-version conflict fences, including idempotent replay of positive and excluded results.

Individual resolution can establish authority for an exact current row while the catalog is still preparing other rows. Catalog listing, scoped coverage and graph browse retain their completeness checks before claiming a complete result. A missing project field is malformed under the required `Session.project` and `SessionSummary.project` contracts; an explicit null or empty project is an exclusion. Scoped preparation skips that exclusion only when the catalog is complete and current compact exclusion metadata proves the row identity.
