# Viewer recent preparation caller

`viewerRecentPage` still reads at the viewer's established page bounds: at most 16 rows and 65,536 response bytes. When the native page says `preparing`, it submits at most one `viewer_recent_prepare` call and omits the work-limit fields so the native implementation supplies its central defaults: 4,096 rows, 33,554,432 bytes, and a 60,000 ms safety hint. The finite native maxima are 16,384 rows, 134,217,728 bytes, and 300,000 ms. The safety hint does not stop a useful cut early; the native whole-operation deadline is bounded by:

```text
min(300000, max(max_ms, 30000 + 4*limit + 10*floor(max_bytes/65536))) milliseconds
```

For omitted fields the deadline is 60 seconds. The caller uses the typed `IIIClient.trigger` `timeoutMs` option for the preparation operation only, set to 70,000 ms to leave the proposed 10-second ACK margin. Page reads, status reconciliation, and other retrieval operations retain the normal SDK timeout.

The caller validates the preparation envelope and finite response limits, including `source_bytes`, `decoded_bytes`, `elapsed_ms`, `batch_limits`, cursor/generation, status/readiness, and nullable `required_bytes`. It validates page byte size, row count, cursor, entity scope, item key and version before exposing rows. A successful cut with `continue=false` causes one page refresh when the caller has no cursor; this exposes useful partial rows when issues remain. The refreshed page's own status controls public readiness, so exhausted work does not imply `ready=true`.

A `decode_budget_exceeded` response is returned with its `required_bytes`, whether the cut processed zero or some rows; this call does not loop or issue another mutation. If the preparation invocation times out, the result is treated as slow or unknown. The caller issues exactly one read-only `preparation_status` request. It refreshes the page when durable status confirms the scan is complete and dirty work is empty; the status `has_more` field reflects readiness, so it may remain true for exhausted partial status. Otherwise it returns the original page with reconciliation state. It never infers process death or blindly retries a possibly committed mutation. `partial` remains true for non-ready status, more rows, or cursor-based reads, and partial totals remain unknown.

## Later operator checks

The source owner did not run checks. After the combined candidate is accepted and the existing single compiler/operator is available, run the focused regressions:

```text
npm test -- test/viewer-memory-page.test.ts test/viewer-memory-page-order.test.ts test/state-kv-missing-value.test.ts
```

Then run the combined candidate build once:

```text
npm run build
```

For installed behavior, use the single-operator procedure and evidence matrix in [recent-preparation-performance.md](./recent-preparation-performance.md#installed-runtime-runbook-later-sole-operator). Keep the native tree frozen; reconcile status once after timeout/lost delivery; record identity-bound combined application and engine memory during actual completion and after idle settling. Runtime throughput, installed behavior, and the <=3 GB transient / <1 GB settled memory criteria remain unmeasured by this source change.
