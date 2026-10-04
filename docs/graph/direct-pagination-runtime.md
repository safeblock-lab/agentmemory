# Direct pagination runtime observation

On October 2, 2026, the packaged modified Windows engine completed a direct
read of the existing large SQLite graph through `iii-sdk` and `state::list_page`.
The database was not copied or indexed. The AgentMemory watchdog and normal
daemon remained disabled. The owned engine was stopped after the read.

The SDK requested up to 256 records and 1,048,576 response bytes per page,
with a 30-second request timeout. Each page was counted and discarded.
The driver enforced a 500,000,000-byte additional joint memory allowance.
It sampled the engine's larger working-set/private-memory measurement plus
the Node driver's RSS increase over its baseline.

| Scope | Records | Serialized value bytes | Elapsed milliseconds |
| --- | ---: | ---: | ---: |
| Nodes | 113,114 | 1,477,834,410 | 1,208,896 |
| Edges | 56,557 | 16,062,188 | 53,595 |
| Name index | 113,114 | 4,072,104 | 29,182 |
| Edge keys | 56,557 | 1,922,938 | 22,265 |
| Node degree | 113,114 | 113,114 | 31,127 |
| Snapshot | 1 | 265,174 | 132 |
| Edge history | 0 | 0 | 1 |
| Total | 452,457 | 1,500,269,928 | 1,347,051 |

The observed peak additional memory was 197,754,880 bytes. The Node driver
exited with code 0 and reported the owned engine stopped. The engine's
shutdown exit code was null following termination; this is not a graceful
shutdown result. OpenTelemetry also reported a closed connection during
shutdown.

The first run exceeded the memory allowance at 502,194,176 bytes and was
stopped. The existing runtime observability configuration retained up to
1,000,000 spans. Changing `memory_max_spans` to 1,000 before the second run
reduced the observed memory peak. The first 100 node pages took 75,610 ms
in the failed run and 6,426 ms in the completed run. Later pages were slower:
the first-page rate must not be presented as the full-graph rate.

This observation establishes successful bounded-memory pagination of the
existing stored values. It does not establish graph-query equivalence,
mutation durability, interrupted extraction recovery, or installer health.
The runtime was the existing `native-run-lLRFOE` SQLite configuration, using
`dist/engine/win32-x64/iii.exe` from this checkout.
