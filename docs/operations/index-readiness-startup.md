# Startup with paused indexed recovery

With `AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=1`, startup checks native indexed retrieval readiness without attempting source recovery. Previously, `STATE_INDEX_NOT_READY` from this check terminated the worker, including its capture and graph functions.

The worker now keeps core services running when this specific check reports `STATE_INDEX_NOT_READY` while recovery is paused. It logs degraded indexed retrieval and does not advertise search as active. Other startup errors still propagate; startup with recovery enabled retains its existing recovery and readiness requirements.

No index readiness marker, embedding identity, or stored graph data is changed by this handling. Native lexical and semantic queries retain their `IndexedVector.ready()` guards, so incomplete or incompatible indexes cannot silently produce partial results or fall back to resident corpus search. Status continues to derive readiness from native index metadata. Explicit preparation is still needed before indexed search can become ready; this startup change does not prepare the corpus or call a provider.

The observed failure was `IndexedRetrievalError` with code `STATE_INDEX_NOT_READY` immediately after the paused-recovery startup message. That evidence identifies the startup dependency, but does not distinguish which underlying index readiness condition is incomplete. Runtime restart, source preparation, and verification were outside this source-only correction.
