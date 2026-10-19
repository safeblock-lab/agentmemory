# Storage candidate summary integration — 2026-10-15

This records the isolated application contribution for `summary-final-candidate-integration`. Main acceptance and the native owner's combined delivery receipt are required before its operational use. This leaf performs no installation, restart, migration, live retry, or provider call.

## Source and composition

Candidate: `D:/agentmemory/.codex-task/storage-priority-20261015/native-candidate`.

The application retains the installed `provider-stages-20261015T115401Z` baseline and the existing exact retention/capture/admission composition. Only `app-source/src/functions/summary-queue.ts` changes in application source.

The contribution comes from `D:/agentmemory/src/functions/summary-queue.ts`, explained in [the author's remediation](failure-2026-10-15/remediation.md). The diagnosis directory and its logs are not packaged. The author's source is unchanged.

Included behavior:

- Reconstruct missing or mismatched map lineage from canonical source references with exact source version/hash/ordinal/text checks, scoped to the current unit.
- Discard cached output/raw responses without matching generation input binding; retain validated receipts when their input/output hashes match.
- Capture the request input hash before asynchronous work, persist the output input hash, and reject admission after input changes.
- Resolve canonical legacy reduce path authority using batches of 12, durable checkpoints, and path pages bounded to 16 KiB. Checkpoints bind input, snapshot, root, source revision, and ordered references. Legacy reduce without its canonical snapshot still rejects.

Excluded source differences preserve the candidate baseline: workflow-attempt-accounting import/signature/wrappers, removal of operation-error-evidence, and removal of the normal manual snapshot byte limit. The total legacy reduce limits are removed only from the canonical batched path; the manual path still materializes its bounded snapshot.

Two defects were demonstrated in focused tests and corrected only in the candidate:

1. A singleton path larger than 16 KiB following a nonempty page bypassed the size check. Each singleton is now checked before flushing or appending.
2. A late provider rejection spread a stale unit into persistence, replacing changed input. Generation failure writes now require the current pending job/unit/input identity under the job lock. Late successful responses also skip stale raw persistence.

## Evidence and delivery

Scratch/evidence directory: `.codex-task/storage-priority-20261015/runtime-activation/summary-integration`.

- `summary-queue.before.ts`: candidate beforeimage.
- `summary-queue.authored.ts`: received source snapshot.
- `summary-integrated.patch`: exact beforeimage-to-candidate difference.
- `authored-to-candidate.patch`: exclusions and candidate-only corrections.
- `composition.before.json`: preserved earlier composition receipt; its events remain in the current composition.
- `checks-first.raw.txt`: 11 passing cases and the two demonstrated failures.
- `checks-second.raw.txt`: the same 13 cases pass after the narrow fixes.
- `checks-final.raw.txt`: 18/18 focused cases pass, including checkpoint binding changes and normal completion/replay.
- `app-assets.json`: emitted asset sizes and SHA-256 bindings.
- `delivery-receipt.json`: application delivery and limitations for Main/native owner.

Run the focused checks with:

```powershell
node_modules/.bin/vitest.cmd run --config .codex-task/storage-priority-20261015/runtime-activation/summary-integration/vitest.config.mts
```

The tests import candidate application source and use isolated in-memory persistence and mocked providers/publication decisions. They cover canonical lineage repair/rejection, bound/unbound cache behavior, late success/failure, bounded checkpoint progress and resumption, snapshot/root/input/revision/reference changes, oversized paths, and candidate completion/replay. They do not establish live provider quality or production persistence behavior.

The existing `build-app-isolated.cmd` emitted application bundles successfully (exit 0), using the existing tsdown runtime. The seven runtime/viewer assets match their recorded installed sources byte for byte. `app-dist-isolated` contains 36 recorded assets. `app-dist` remains explicitly excluded from installation. The native `source-delivery.json` is unchanged by this leaf.

Provenance is recorded in `task-deltas/summary-final-contribution.json` and appended to `task-deltas/composition.json`; prior events and receipts are preserved.

## Limits and handoff

No global suite, typecheck, native fixture mutation, live API probe, DB access, engine change, installation, restart, migration, credential change, or provider invocation occurred. No physical savings or live runtime acceptance is claimed. The emitted source and asset hashes bind this application delivery only; the native owner must produce the final combined receipt after Main accepts the composition.

## Main acceptance

Main read the complete integration patch, provenance, focused raw results (18/18), isolated build completion, and delivery manifest. The application contribution was accepted through the frontier ledger for `summary-final-candidate-integration` on 2026-10-15. The native owner was instructed to use only `app-source` and `app-dist-isolated` and bind the application receipt into the combined delivery. This closes the isolated application unit; native runtime acceptance, live installation, migration, physical reclamation, and the rest of the memory improvements plan remain pending.
