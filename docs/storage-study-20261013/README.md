# Whole-database storage reduction study

Status: read-only study and concrete proposal complete, 2026-10-14. Source mapping and complete temporal database measurement accepted. No physical reduction has been performed; tenfold feasibility remains unproved. See [propuesta de reducción](proposal/reduction-proposal.md).

The subsequent [complete implementation plan](../plans/storage/2026-10-14-memory-improvements.md) incorporates this study, retention and all fourteen TencentDB comparison recommendations. It remains a proposal, with conditional size scenarios after snapshot temporary release.

Objective: explain the entire AgentMemory database footprint, indicated by the user as 7.4 GB, and produce evidence-supported alternatives for a reduction of at least ten times while retaining useful knowledge, fidelity, relationships and behavior. Redesign may be proposed. The initial phase authorizes research and a concrete reversible proposal only.

No deletion, migration, VACUUM, runtime configuration change, service restart, provider call or product source modification is authorized here. Another chat owns function recovery and the sole live operator. All study outputs stay in this directory. Application persistence must remain behind iii-engine/StateModule.

User steering, 2026-10-14: questioned whether so much data must be retained and believes retention is excessive. The proposal must assess necessity and bounded retention alongside lossless redesign: useful durable knowledge/provenance, transient traces and recovery artifacts, derived indexes, and archival copies. This is not authorization to delete anything. Large relationships or old files are not automatically unnecessary; active recovery and rollback dependencies need evidence.

## Evidence and outputs

- `measurement/`: authoritative file identity, physical allocation, logical categories, exact and structural duplication, coverage, temporal consistency and resource bounds.
- `source-mapping/`: source evidence for writers, readers, exports, representation and fidelity constraints.
- `proposal/`: alternatives, non-overlapping byte estimates, target assessment and reversible migration sequence.
- `orchestration/`: route-free scope, execution contract and research authority records. These are control-plane records, not evidence that database measurements succeeded.

## Current ownership

Main owns acceptance and final integration. Measurement and source mapping are complete; proposal work uses their accepted results. The structural code graph generation is 2026-10-03 and flagged metadata changes, so current source fallback was used. No graph refresh was performed.

Ledger task: `d67a1db0-9143-4160-92e9-78bbed3f2332`; session: `01a13723-fc07-7d40-a378-dea223c40aa5`; attempt: `01a13724-8bca-7cc1-b01b-0972e8d35a48`.

Accepted frontier plan digest: `2d8ca2d7c184d5ea3c149a0b90a0a9b197a1f91a583b9263f2e0f48ac1975ea8`; contract digest: `86b2fe4df3e0e8204127c071135c9622164f47afa7a46aa7ea69e878a27c5df3`.

Native assignments:

- `/root/leaf_867bdbb39ab74960a01e3af701d3869b`: measurement completed and accepted; all readers closed.
- `/root/leaf_b34c957f0a9b4d9c99298fb4a422f301`: source mapping completed and accepted; see [storage contracts](source-mapping/storage-contracts.md).
- `/root/leaf_1be6e88f09404655a6c28aa35db65485`: reduction proposal returned; integrated by Main in [proposal](proposal/reduction-proposal.md).

The earlier planner proposal was accepted only for decomposition. No database was opened by either planner. Previous `physicalBytesReclaimed=0` evidence establishes no effective space reduction.
