# Graph search budget behavior

Indexed hybrid search treats graph results as optional enrichment when it already has keyword or vector candidates. If graph expansion raises the typed graph resource-budget error, or the exact indexed retrieval resource-limit error, search records one fixed diagnostic, fuses the available candidates without graph scores, and continues through the local reranker.

Cancellation is checked first. Integrity, generation, transport, untyped errors, and budget failures with no base candidates remain fatal. Existing 64 MiB serialized and 256 MiB estimated graph-read limits remain unchanged.
