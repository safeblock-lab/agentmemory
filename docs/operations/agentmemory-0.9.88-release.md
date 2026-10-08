# AgentMemory 0.9.88

This release changes graph extraction's session capture to read a consistent
session scope into a bounded buffer before publishing its frozen rows. Capture
is limited to 64 MiB, 100,000 rows and 30 seconds, with up to three fresh
attempts when the native scope revision changes. Incomplete attempts are
discarded; complete captures are published under a unique prefix before the
completion pointer is written. Native cursor revision checks remain enabled.

The change addresses the observed cursor-validation failures caused by
interleaving durable writes with cursor pagination and UUID generation values.
A provider-backed graph extraction must still reach staging and finalization;
package startup alone does not establish that result.

Tests, typechecking and optional health probes were not run, following the
user's execution preference.
