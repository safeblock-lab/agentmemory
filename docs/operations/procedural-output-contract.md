# Procedural output contract

Source preparation introduces `procedural-json-v2` for new normal paged procedural extraction requests whose effective provider declares structured-call preparation. Providers without that capability retain the existing XML prompt, partitioning and parser, selected before dispatch. Existing JSON captures keep their original contract. Provider support has not been tested or established. Runtime emission, installation and live requests remain separate Main-controlled actions.

## Format and validation

The logical format remains the strict named JSON schema `procedural_output_v1`. V2 separates this logical acceptance contract from its effective transport. OpenAI preparation chooses declared `legacy-native` or `validated-json-text`; native mode sends the schema, while text mode omits unsupported wire fields and uses the same strict local parser. The only root field is `procedures`, an array of objects with exactly `name`, `trigger` and `steps`. Every procedure requires nonempty text and at least two ordered steps. Empty evidence produces `{"procedures":[]}`.

The local parser accepts one JSON document up to 65,536 UTF-8 bytes. It rejects duplicate keys (including escaped aliases), extra fields, invalid shapes, nonstrings, empty values, invalid controls, unpaired surrogates, Markdown fences and trailing prose. It bounds depth at 32 and procedure-plus-step nodes at 4,096. It performs no coercion or response repair. Angle brackets and ampersands retain literal meaning. Text is trimmed using the same normalization as procedural XML.

The model supplies no source IDs. Existing captured-source, project and frequency validation remains authoritative. Candidates receive the original group's session provenance and existing candidate identity calculation. Source versions, selection, quality, publication and cost controls remain in their established paths.

## Durable identity and historical requests

The V2 identity includes version, complete logical schema and effective transport alongside manifest ID and exact prompts. Direct transport includes the prepared route, provider, model, endpoint, profile, operation, task, request digest and timeout. Preparation happens before identity and checkpoint creation; the same single-use prepared call executes afterward. A bounded binding record in the existing state scope prevents a changed route or profile from silently generating a second dispatch at the same logical request. Captured V2 responses replay using their saved contract without preparing a current provider.

V2 request identities, logical binding keys and batch fingerprints use recursively sorted object keys, preserving array order and string bytes. StateModule object-key reordering therefore does not change their identities. The captured `binding.responseFormat` string remains exact. XML/V1 identities and legacy batch fingerprints retain their historical serialization.

Historical XML requests retain their original three-element identity and parsing path. `procedural-json-v1` retains its original schema-only identity and options. Existing V1 captures and pending work are read with that contract; a rejected V1 request stays rejected without retry. A new normal V2 request has a distinct identity and does not rewrite the rejected checkpoint. Legacy manifests retain their original XML prompt and repair workflow.

Normal extraction at an unchanged cursor creates a distinct JSON request rather than reusing or rewriting an XML capture. Existing XML requests, captures, repair records and intents are left intact. JSON requests never silently fall back to XML or consume an XML response repair. Candidate persistence and subsequent cursor advancement keep their existing order.

The selected direct provider binds its declared transport through `prepareStructuredCall`. Errors propagate without an auxiliary-to-primary retry for V2; malformed or empty successful responses are captured before parsing rejects them. A definitive provider rejection is retained and blocks automatic redispatch of the same contract request. Existing uncertain-dispatch handling remains unchanged.

New definitive direct-rejection checkpoints retain only an allowlisted HTTP status and safe `code` and `param` identifiers when available. Existing checkpoints are not backfilled.

## Batch integration boundary

Batch preparation resolves the separate `FIREWORKS_BATCH_OUTPUT_CAPABILITY` and configured batch model before the durable request. An HTTP text profile does not imply batch support. The declared batch capability currently uses native JSON schema; unsupported or mismatched capability/model/format is held before submission. Local request/work-item intersections carry the V2 logical/transport contract independently of the optional wire response format. The queue snapshots it into the enqueue journal and work item, includes it in deduplication identity, validates recovery and JSONL serialization, and requires it in exact request/work-item matching. Local contracts are not sent as extra provider JSONL fields.

Replacement validates and snapshots the captured version's own contract and options, preserving historical XML and V1 behavior rather than substituting the current constructor. Requests without format or logical contract retain their historical batch fingerprint and JSONL serialization. Structured callbacks retain raw content and completion intent before invoking the consumer. Callback failures, including logical contracts with no wire format, retain that capture, dead-letter the item and propagate workflow failure; terminal items do not retry the callback. Provider error rows keep the existing dead-letter behavior. No provider compatibility is implied by TypeScript source support.

Existing 8,064-byte partition and 8,192-byte prompt limits, response capture bounds, locks, receipt handling and publication semantics remain unchanged. No new REST/MCP surface, storage scope or table is introduced.
