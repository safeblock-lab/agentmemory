# Native state diagnostics

The background engine launcher drains engine stdout and reports recognized
`state operation rejected` diagnostics through the launcher's stderr. Existing
startup stderr capture and process ownership handling remain in place.

Only static SQLite error categories and a signed 32-bit extended error code,
known STATE error codes, and three codec/record-limit categories can be emitted.
Raw engine lines, error messages, state keys, paths, scopes and payloads are never
forwarded. Unrecognized output is discarded.

Each line is limited to 16 KiB. Oversized lines are discarded through their next
newline. The parser retains at most 16 KiB of line content and emits at most ten
diagnostics per minute. A final unterminated line is processed when stdout ends.

These diagnostics explain native failures that otherwise return a generic
`STATE_TX_FAILED`; they do not change transaction behavior or initiate recovery.
