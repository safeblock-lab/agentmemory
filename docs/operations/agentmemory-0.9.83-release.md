# AgentMemory 0.9.83

Operators can pause startup indexed-source repair and vector backfill, periodic summary recovery, graph recovery, and terminal retention with `AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED=1`. The pause is off by default.

The release adds bounded snapshot and export paging for SQLite V2 graph records, native collection revision cursors, atomic terminal graph-job closure, bounded cleanup of eligible historical staging rows, and a viewer timeline limited to the latest 20 operations. Startup can repair a bounded set of interrupted semantic index writes before enforcing full index readiness.

## Windows native package correction (2026-10-08)

The revision-aware Windows engine was compiled from the pinned iii-engine source and installed in the managed AgentMemory runtime. The package tarball is assembled at `D:\agentmemory\artifacts\private\release-0.9.83\corrected-native-package\agentmemory-agentmemory-0.9.83.tgz` for the publication handoff. Patch and binary SHA-256 metadata are recorded in `patches/iii-engine/manifest.json` and the staged and distribution manifests.

Functional acceptance was not run, so this release note makes no claim that live functionality has been verified.

## Storage maintenance status

One consistent SQLite V2 rollback copy exists at `artifacts/private/release-0.9.83/storage-resolution/v2-rollback`. The frozen V1 copy is retained. Maintenance removed 15,533 eligible historical audit rows. Graph staging cleanup remains in progress after a timeout with acknowledgment uncertain; completion is not claimed. Physical database size was not measured.
