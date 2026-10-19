# Physical maintenance recovery

Checkpoint admission persists its operation identity, request digest and next compact generation before physical maintenance begins. A physical-maintenance error is therefore not equivalent to an unapplied transaction. Preserve the original receipt, request, generation and monotonic lease fences. Never delete a pending receipt to retry.

New migration errors persist a `failed` checkpoint outcome while current maintenance ownership remains valid. The record includes the original request, sanitized failure code and observed physical state; it does not claim a completed rebuild. If recording the failure cannot commit, the receipt remains pending.

For a pre-existing pending Rebuild, the operator action `state::storage` / `compact_checkpoint_recover` records `unknown`, preserving the exact original request and recovery audit. It requires the original request digest, current generation equal to original generation plus one, exactly one pending checkpoint, a fresh maintenance lease with a greater fence, explicit exclusive-writer/quiescence acceptance, no active native read views and no pending foreground work. The serialized State writer and an immediate transaction protect the mutation; the operator must establish external writer quiescence. Recovery supports only `auto_vacuum=2`, page size4096 and SQLite user_version4. Recovery does not run VACUUM or fabricate rebuild/GC success.

A new Rebuild uses a new operation identity only after the original pending outcome has been durably reconciled. One-off Rebuild permits up to600000ms with900000ms maintenance ownership. Existing periodic lease defaults remain300000ms. These are ceilings, not duration guarantees.

SQLite VACUUM is atomic physical maintenance and retains no resumable progress offset after interruption. This differs from periodic native batches, which retain durable cursors and exact receipts. A failed or unknown physical outcome is not evidence of reclaimed bytes; measure the same database/WAL/SHM lifecycle after acknowledged completion and checkpoints.
