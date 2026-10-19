# Data directory storage size

The dashboard's **Data directory size** card uses local file metadata from the running worker. The additive `operationalMetrics.dataDirectoryStorage` field travels through the existing iii `api::status` function; no endpoint or direct SQLite connection is added. The existing `graphStorage` API field remains graph payload metadata for compatibility and is not added to the directory total.

## Directory and byte semantics

The reader requires an absolute `AGENTMEMORY_DATA_DIR`, which the CLI exports after resolving `--data-dir`, environment configuration and the default directory. It does not infer the engine's directory from the worker's current directory or from `config.dataDir` (the agentmemory home). Missing or relative configuration reports unavailable. Custom launchers must supply the engine's actual data directory through this environment variable.

`fileLengthBytes` sums the metadata lengths of all descendant regular files, including database, WAL, SHM, queue and index files present during traversal. GB uses decimal units: one GB is 1,000,000,000 bytes. This is neither graph payload size nor allocated disk space; `allocatedBytes` is deliberately null. Sparse files, filesystem compression and hard links can make allocated usage differ. Hard-linked directory entries contribute their lengths individually. No file contents are read, and no state is read or written outside iii-engine.

Traversal is a point-in-time interval, not an atomic filesystem snapshot: files may grow or change during measurement. `sampleAt` is the end of the most recent complete traversal. `lastAttempt.startedAt` and `finishedAt` describe the latest attempt, which may differ from the retained complete sample.

## Bounds and freshness

The worker shares one reader across status calls and agent scopes. Status reads start a scan without waiting for it and reuse samples for 60 seconds. There is at most one scan in flight, with sequential metadata operations, a two-second traversal budget, 100,000 descendant entries and 64 directory levels. Directory iteration streams entries instead of loading entire directories into memory. A budget expiry publishes incomplete coverage immediately. An outstanding filesystem call cannot be cancelled by Node; it continues to occupy the single flight until it settles, preventing overlapping scans. A later status read triggers the next eligible refresh.

The configured root must be a directory rather than a symbolic link. Descendant symbolic links and junctions are excluded; resolved paths are checked against the canonical root. Exclusions, unsupported entries, missing files, read errors and limits prevent a complete result. A missing directory never becomes a successful zero-byte sample. An empty, successfully traversed directory can report zero.

`state` distinguishes `measuring`, `current`, `stale`, `partial` and `unavailable`. A complete cached sample becomes stale during refresh or after an incomplete attempt. Its timestamp and bytes remain attached to that old sample; `lastAttempt` carries the latest observed subtotal and failure reason. With no complete sample, `fileLengthBytes` stays null and observed partial bytes are only a subtotal. `complete` describes the retained sample's traversal coverage, not freshness. Directory configuration changes discard the previous directory's cached sample.

## Delivery status

This source change has not been compiled or tested. Runtime installation, operational measurement and browser verification remain with Main under the explicit execution preference.
