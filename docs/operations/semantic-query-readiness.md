# Semantic query readiness races

Lexical and semantic reads can race a capture that marks a source not ready after the reader's initial status check. Indexed-vector queries retry only the typed `STATE_SEMANTIC_SOURCE_NOT_READY` response, with at most three query attempts and a complete readiness and identity check before each attempt.

Readiness changes, identity or generation mismatches, resource limits, transport failures, and untyped errors remain fatal. The retry applies only to query reads; pending mutation failures remain sticky and visible.
