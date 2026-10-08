# AgentMemory 0.9.85

The local updater and PowerShell installer now use the canonical frozen-release package name. The updater pairs it with `SHA256SUMS`, verifies the package checksum and archive identity, and retains support for older releases that expose only the legacy package and checksum names. The installer tries the legacy package URL only when the canonical URL returns HTTP 404.

This release reuses the published Qwen model and CPU runtime assets. It includes the prepared native engine and the application package built from this source revision. Tests and typechecking were not run, following the user's execution preference; the required build and frozen publication workflow are used for delivery.
