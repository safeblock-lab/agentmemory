# AgentMemory 0.9.89

This release adds bounded native storage diagnostics, incremental five-day
technical retention, a compact 30-operation feed, and exact observation
identity tracking with fail-closed legacy indexing. Startup collects a complete
aggregate baseline and indexes legacy sessions before any retention can be
activated. Cleanup stays paused until the authenticated maintenance endpoint
receives an explicit activation after both steps complete.

Only resolved terminal graph work and age-eligible technical rows are submitted
for cleanup. The protected graph job and unresolved or unindexable source data
remain retained. Raw observations are eligible for stripping only after a real
useful compression carries the matching source digest. Metrics distinguish
logical and stored bytes from reusable SQLite pages and actual database/WAL
sizes; unsupported full-page reclamation is not attempted.

The required package build is recorded by the integration task. Tests,
typechecking and optional probes were not run, following the user's execution
preference.

The emitted engine package manifest retains `deploymentReady: false` and
`verifiedPlatform: null`: this release records native compilation, without
claiming runtime acceptance or platform verification. The package's existing
declaration file is retained unchanged because declaration generation would
run typechecking.
