# Project reflection in the viewer

Session detail offers **Reflect project** when the session has a valid project. The selection covers bounded indexed project memories and observations, not only that session. Fact, lesson and crystal families are excluded from this viewer request. Completion applies to that bounded selection; global family coverage remains incomplete. See [the reflection contract](reflection-contract.md).

Each explicit click sends one foreground cut to the existing `reflect` endpoint. The viewer sends only `project`, a stable `requestId`, `manual: true`, `maxClusters: 1` and, when supplied, the validated server continuation. The application handles provider routing outside the product interface.

Pending, retrieval waits and blocked outcomes retain an explicit **Continue reflection** action when resumable. Terminal partial and failed results remain visibly unfinished; only a completed selection is labeled completed. Counts and available family coverage come from the operation result. No polling or automatic continuation occurs.

An unknown network outcome offers **Resume reflection** with the original payload and identity. It does not create a replacement request. Mismatched job, request, project or cut responses pause the operation. Duplicate clicks are suppressed. Operation identity is retained by project while the viewer page stays open, including panel changes; reloading the page clears this in-memory UI state. Operator recovery should inspect the existing operation before starting another after a reload.

Canonical insights load independently through the existing project-filtered `insights` GET endpoint, limited to 20 records. Text is escaped, foreign project rows are excluded, and loading, empty and error states are explicit. Detached panels, project changes and superseded reads cannot overwrite current insight contents.

Source behavior is covered by mocked viewer VM tests. Installed runtime, rendered browser, responsive layout and assistive technology evidence require the separate authorized installation/browser gate.
