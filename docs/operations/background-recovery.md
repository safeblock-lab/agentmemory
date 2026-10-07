# Pausing background recovery

Set `AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=1` in the AgentMemory process environment or `~/.agentmemory/.env` before starting AgentMemory to pause summary queue recovery and reconciliation, indexed dirty-source repair, startup vector backfill, and graph job recovery and terminal retention. New completed sessions keep a durable summary intent; the queue does not load their observation snapshot or call a summary provider while paused. Graph jobs and their checkpoints remain untouched while paused.

Live observation capture and embedding requests continue. Existing durable work remains stored and can resume when the setting is removed or changed to `0`, then AgentMemory restarts. The pause applies at startup and at each summary queue operation, so an already running provider call is allowed to finish.
