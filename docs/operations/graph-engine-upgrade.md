# Graph recovery engine upgrade

The source contract requires `state::graph_automatic_recovery_v1` before
AgentMemory can reuse an installed iii-engine. This adds an eighteenth required
CLI capability while keeping the engine version pinned to `0.22.1`.

## Why the capability changes

An older patched `0.22.1` engine can advertise the previous seventeen
capabilities without implementing automatic graph recovery. Version matching
alone cannot distinguish that engine from the new candidate.

The required capability appears in three source inventories:

- [CLI artifact requirements](../../src/cli/engine-artifacts.ts)
- [Engine build requirements](../../scripts/build-iii-engine.mjs)
- The combined native engine's `src/main.rs` inventory for `--capabilities`.

The native recovery implementation owns the corresponding StateModule behavior.
See [native automatic graph recovery](graph-automatic-state-recovery.md).

## Installer behavior

The existing `resolveCompatibleIii` path in [the CLI](../../src/cli.ts) requires
both the pinned version and every required capability. Its capability parser
uses the inventory in `src/cli/engine-artifacts.ts`. With the updated source,
an old engine missing `state::graph_automatic_recovery_v1` cannot satisfy that
selection contract even if its version remains `0.22.1`.

If a compatible private engine is already available, the CLI can select it.
Otherwise the ordinary installer must obtain the bundled patched artifact.
The artifact manifest and packaged executable must be regenerated together
with all eighteen capabilities before this source change is distributed.
The build script requires the native manifest to contain exactly that inventory.
See [patched engine delivery](state-pagination.md).

## Delivery status

This document records the source contract change. It does not establish that a
new executable has been compiled, bundled, installed or started. Release
assembly owns native patch regeneration, manifest metadata, artifact production
and application emission. The source change did not execute tests, typechecking,
capability probes, checksum checks, artifact reviews or runtime installation.
