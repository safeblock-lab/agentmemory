# State pagination and patched engine delivery

This document describes bounded state scans through `state::list_page`, the client helpers, and the verified iii-engine artifact required to use them.

## Page contract

`state::list_page` accepts a scope, an optional opaque cursor, an item limit, and a serialized response byte limit. The defaults are 256 items and 1 MiB. The item limit is bounded to 1–1,024 and the byte limit to 1 KiB–1 MiB. The byte count covers the complete UTF-8 JSON response, including the `items` array, `next_cursor`, escaping, commas, and envelope fields.

A response contains `items` and `next_cursor`. A null cursor ends the scan. Otherwise, pass that cursor to the next request without changing scopes. The engine preserves its state order while paging. It rejects malformed, cross-scope, or stale cursors; a state mutation during a scan makes its later cursor stale. An individual record that cannot fit the requested byte limit returns `STATE_RECORD_TOO_LARGE` and remains in state. The engine keeps the connection available after that error.

Use `StateKV.pages(scope)` to consume page results, including each next cursor. `StateKV.values(scope)` yields individual values across pages. `StateKV.list(scope)` collects the complete scope in memory; if a mutation makes a cursor stale, it restarts the scan up to three times after the initial attempt. It does not retry other errors. Prefer `pages` or `values` when a caller can process records incrementally.

The client rejects responses over the requested byte limit, pages larger than the item limit, empty pages with a continuation cursor, and cursors that fail to advance. These checks keep malformed or oversized transport responses from being treated as a completed scan.

Page limits bound each serialized response and let `pages` or `values` avoid collecting the entire scope in client memory. They do not set a total engine memory limit: the native file-based store still loads state into resident memory, and persistence operations may clone state. `StateKV.list` also deliberately collects the complete scope.

## Errors and recovery

- `STATE_PAGE_UNSUPPORTED`: the configured engine does not provide `state::list_page`. Install a package with the patched engine for the host platform. There is no unpaged list fallback.
- `STATE_RECORD_TOO_LARGE`: the current record exceeds `max_bytes`. Retry with a larger limit, up to 1 MiB, or read that record directly by key. The record is not deleted or skipped.
- `STATE_PAGE_CURSOR_STALE`: state changed during traversal. Restart from the first page; `StateKV.list` does this automatically for at most three restarts.
- `STATE_PAGE_CURSOR_INVALID`: the cursor is malformed, belongs to another scope, or did not advance. Start a new scan without the cursor.
- `STATE_PAGE_INVALID_REQUEST` or `STATE_PAGE_INVALID_RESPONSE`: correct the request bounds or investigate an incompatible engine/client pair.
- `STATE_PAGE_FAILED`: the trigger failed for another reason. The client preserves the original error as `cause`; inspect it before deciding whether to retry.

## Engine artifact requirements

The CLI accepts an engine only when its version matches the package pin and its side-effect-free `--capabilities` probe advertises `state::list_page`. Version and capability probes return before engine telemetry or service startup. The installer verifies the bundled manifest, artifact size, SHA-256, version, and capability before atomically placing the executable in the private `~/.agentmemory/bin` directory. It never downloads an upstream engine as a fallback. The upstream v0.22.1 engine and its Docker image do not provide this pagination function.

The native source pin is iii-engine v0.22.1 commit `e7de3820d1e558f3762edf95e4440552444d48d3`. The native handoff records the patch hash and verified executable hash in `patches/iii-engine/manifest.json`. Until that handoff supplies the manifest and binary, do not stage or package a native artifact.

After the pinned native inputs are present, stage a supplied Windows x64 binary with:

```powershell
npm run build:engine -- --binary .native-pagination-build/iii-engine-patched.exe --platform win32 --arch x64
```

The script checks the input against the native manifest, probes its version and capability, and writes verified files under the ignored `.iii-engine-build/artifacts/` directory. Without `--binary`, it fetches the pinned source commit, applies the checked-in patch, and builds the current host target with Cargo. `npm run build` requires a valid staged artifact and copies only validated build output to `dist/engine`; it fails when the artifact is missing or does not match the pin.

The current delivery target is `win32-x64`. Other operating-system and architecture combinations remain unsupported until their patched binaries are built, probed, and listed in the artifact manifest. A missing platform entry produces an explicit unsupported-platform error. Build outputs stay inside the repository, and the build directory can be selected with `AGENTMEMORY_III_BUILD_DIR` only when it resolves inside the workspace.
