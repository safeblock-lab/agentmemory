# AgentMemory local runtime

## Dashboard status sources

`GET /agentmemory/status` uses bounded native `index_status` metadata when native retrieval is enabled. Keyword and vector document counts refer to the active embedding model, dimensions and generation, matched against the configured provider and the `indexed-corpus` marker. Native readiness includes lexical readiness, source preparation, clean source coverage and graph index readiness. `index.native.dirtyCount` counts dirty sources, not a resident vector backfill queue.

The native corpus contains both memories and observations. Native metadata does not expose a separate observation total or session total, so `observationsIndexed`, `sessions` and `missingObservations` are `null` rather than inferred from an empty resident cache. Unavailable native metadata also yields unknown counts. Status does not enumerate the corpus, rebuild indexes, prepare sources or call embedding/LLM providers. Native persistence is engine-owned; resident snapshot persistence diagnostics do not apply.

`activity` reports up to ten sanitized session and observation writes observed by this service process, newest successful write first. Its `source` is `runtime-writes`, `partial` is `true`, and `availableSince` identifies the start of this capture. Capture retains at most fifty deduplicated records in memory and excludes prompts, narratives, facts, paths and raw tool payloads. Agent isolation filters records before the response limit. The list starts empty after restart; it is neither persisted history nor an audit trail. Historical audit records are not used as current activity.

Graph totals have `provenance: "snapshot"` and `countsAreCurrent: false`, with the snapshot's actual `updatedAt` and computed age. A missing snapshot has unknown totals and `provenance: "unavailable"`. `index.native.graphReady` reports live native graph readiness; `index.native.graph` contains each required graph scope's status and revision. These bounded native revisions do not provide live aggregate counts. Status never refreshes the snapshot automatically; rebuilding a large corpus requires a separate explicit operation within its supported limits. Legacy retrieval retains its existing shared observation/session reconciliation scan; native retrieval never invokes it.

Version 0.9.81 is installed on the real host from the frozen local package. Native migration retained the legacy directory and imported 816,398 records in 695 scopes into `C:\Users\harus\.agentmemory\data\state_store.sqlite3`.

Activation is accepted and healthy at version 0.9.81. The existing hidden `AgentMemory` task starts the corrected native engine through the installed CLI. Ports 3111, 3112 and 49134 belong to the single managed engine, and the viewer on 3113 responds. The engine SHA-256 is `bd3126c4549b04c0fb31ce8fd9b298e21dcf20c60b40ee8b0b4105ad7bd5a91d`; the installed package's engine and manifest bind to the same binary. The hidden minute `IgnoreNew` watchdog is enabled and its fresh run returned 0.

Graph preparation completed at node cursor 69,218 and edge cursor 104,472 with zero invalid records, using bounded pages of 256 records and 2 MiB. The accepted activation preserved the original 147,520 semantic embeddings; a later read-only capture listed 147,897 indexed records. No re-embedding or migration was repeated. Evidence is in `preparation-progress.json` and `corrected-task-health-resumed.json` under the cutover directory.

All accepted application corrections, including bounded semantic readiness recovery, were installed through coherent CLI/index entries and referenced chunks. The official service is healthy at version 0.9.81 with supervisor PID 25,612, CLI PID 17,948 and engine PID 51,300. One actual `mem::search` for `agentmemory` returned three existing memories in 13.099 seconds with indexed semantic retrieval and Qwen reranking. Graph control remained at fence 312, generation 1, version 625; the accepted graph resource fallback can omit graph enrichment at its budget. Qwen PID 73,464 uses the pinned CUDA runtime with GPU, reusing the accepted identical full 4096-context/29-layer proof. The existing hidden minute IgnoreNew watchdog is enabled and its fresh run returned 0; healthy runs intentionally add no log entry. Evidence is in `real-search.json`, `qwen-live-backend.json`, `watchdog-live.json`, `application-delta-deployment-readrace.json`, `current-status.json` and `activation-nine-criteria.json`. Earlier failures remain preserved.

The launcher selects cached MiniLM embeddings and Qwen reranking with automatic GPU selection. The accepted restoration receipt `.native-pagination-build/llm-config-restoration-main.json` confirms provider `llm`, compression, consolidation and graph extraction enabled, the watchdog Ready, and local MiniLM plus automatic Qwen selection retained. Existing `.env` provider settings were restored without exposing their values. Historical jobs are retained.

Run these commands in PowerShell:

```powershell
agentmemory --version
Invoke-RestMethod http://127.0.0.1:3111/agentmemory/health
Get-ScheduledTask -TaskName AgentMemory,AgentMemory-Watchdog | Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName AgentMemory-Watchdog | Select-Object LastRunTime,LastTaskResult
Get-Content "$env:USERPROFILE\.agentmemory\agentmemory-service.log" -Tail 40
Get-Content D:\agentmemory\.native-pagination-build\activate-release-scope\cutover\preparation-progress.json
```

Service logs are under `C:\Users\harus\.agentmemory`. The viewer on port 3113 is diagnostic; application health is on port 3111. Activation receipts and initial preparation logs are under `.native-pagination-build/activate-release-scope/cutover/`.



