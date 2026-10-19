# Terminal transaction restoration

Graph terminal commits must publish their checkpoint and exact receipt without admitting a retention sweep. The previous automatic cleanup path checked the seven-day retention window during a newly completed job's transaction, rejected that transaction, and rolled back its completion. Automatic cleanup was already deferred after that check. Removing this obsolete automatic path preserves the manual retention window and protection gates.

Frozen application ranges reserve exact identities and payloads. Failure bookkeeping must not consume an ordinal reserved by a frozen range. A range may be skipped only when its matching exact receipt and compatible committed checkpoint prove application; a staging checkpoint alone is insufficient.

The affected final metadata-only transaction can be reconciled with a separate append-only identity under a current native graph lease. Admission requires the exact interrupted checkpoint, immutable capture lineage, original prepared payload and failure receipt, prior closing receipts, unchanged snapshot, and no foreign recovery barrier. The operator preserves the original body and receipt, restores only its frozen completion metadata, and performs no provider invocation, graph-data publication, generation reset, or spend reset. Any mismatch stops recovery.

Processing usage feeds now request the native supported limit of 500 events. Internal operation diagnostics retain bounded error codes and pipeline phases; public error responses remain unchanged. The old consolidation operation has no persisted detailed cause, so it must not be reported resolved merely because the shared graph completion defect is corrected.
