# Native state diagnostics

The background engine launcher drains engine stdout and reports recognized
native state diagnostics through the launcher's stderr. Existing startup stderr
capture and process ownership handling remain in place.

State operation failures emit only allowlisted SQLite error categories with a
signed 32-bit extended code, known STATE codes, or the existing codec and record
limit categories. The STATE allowlist includes
`STATE_RETRIEVAL_RESOURCE_LIMIT`.

The filter also recognizes two native warning families. Preparation budget
warnings emit `category=preparation_budget` with one known stage (`queue`,
`wal`, `begin`, `retrieval`, `commit`, or `ack`) and one known failure class
(`deadline_or_cancellation`, `budget_cleanup_failed`, `stage_error`,
`reply_delivery_lost_unknown_outcome`, or `response_deadline_unknown_outcome`).
Graph preparation refusals emit `category=graph_prerequisite` with one known
prerequisite (`node_index`, `edge_index`, `authority`, `observation_index`,
`observation_sources`, `observation_sessions`, `project_observations`, or
`projection`) and one known failure class (`deadline_or_cancellation`,
`stored_logical_decode_exhaustion`, `invalid_prerequisite`,
`prerequisite_resource_limit`, or `prerequisite_transaction_failure`).

Raw engine lines, field values outside those allowlists, error messages, state
keys, paths, scopes, operation identifiers, prompts, exception details, and
payloads are never forwarded. Unrecognized or forged warning fields are
discarded.

Each line is limited to 16 KiB. Oversized lines are discarded through their next
newline. The parser retains at most 16 KiB of line content and emits at most ten
diagnostics per minute. A final unterminated line is processed when stdout ends.

These diagnostics expose source warning classifications that otherwise remain
unreported by the launcher. They do not change transaction behavior, initiate
recovery, establish installed log delivery, or correlate a warning to a
historical attempt.
