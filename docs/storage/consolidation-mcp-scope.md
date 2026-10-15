# MCP project scope and consolidation resumes

`memory_consolidate` sends the selected project and bounded continuation values to the existing consolidation pipeline. The MCP handler validates those values, preserves their supplied strings, and builds a field whitelist before calling the pipeline.

## Project selection

Set `project` to consolidate one project. It must be a non-empty string of at most 256 UTF-8 bytes. The MCP handler passes the value unchanged; the pipeline applies its existing project normalization when it starts a selection. A returned `project` should be copied unchanged when resuming a pre-capture selection.

The optional `tier`, `force`, `deferred`, `batchSourceIds`, and `consolidationCatalogProject` fields are accepted for selection continuations. Preserve each returned value when sending the continuation again. `batchSourceIds`, when present, must contain 1 to 12 non-empty IDs, each at most 256 UTF-8 bytes.

## Resuming work

Pass one returned continuation ID at a time:

- `consolidationSelectionId` resumes a pre-capture selection. Also send its returned `tier`, `project`, `force`, `deferred`, `batchSourceIds`, and `consolidationCatalogProject` values when present.
- `consolidationManifestId` resumes captured consolidation work.
- `decayManifestId` resumes decay work.

Continuation IDs are opaque. Each must be a non-empty string of at most 256 UTF-8 bytes; send it exactly as returned. The handler does not trim or reconstruct IDs.

The MCP surface does not accept provider callback payloads such as `batchResponse`, `consolidationPartitionId`, or internal batch effect fields. Extra argument fields are not forwarded to the pipeline.
