# Skill retrieval coverage

Skill extraction records the bounded retrieval methods reported by its source selector in the optional `retrievalCoverage` field. The accepted labels are `bounded_candidate_budget` and `bounded_query_budget`. Multiple selected references are combined into the canonical label order, with each label appearing once and at most two labels retained.

The field describes limits on the selected search. It does not claim that extraction inspected every observation in a session or project. Source references remain scoped to the completed session and are checked against their selected row version or timestamp before capture and before a fragmented result can be used.

Small source captures, fragmented source jobs, queued fragmentation results, extraction captures, lifecycle intents, and replayed results preserve the optional field. Replays use the captured selection and provider result without rescanning solely to recover coverage. Older stored captures and jobs without `retrievalCoverage` remain valid and produce results without the field.

If indexed source selection is interrupted before returning references with `STATE_RETRIEVAL_RESOURCE_LIMIT`, extraction returns an unsuccessful partial result with `failureCode` and `reason` set to `source_retrieval_resource_limit`. Its bounded continuation echoes the input session, intent, and snapshot identity for an exact caller retry; it does not mark work queued. The lifecycle records this as an actual failure, applies its existing bounded backoff, and terminalizes on attempt six. After the retrieval error, this path does not run the extraction provider, TypeSafe gate, procedural effect, or effect audit; selector work such as embedding may already have occurred before the retrieval interruption.
