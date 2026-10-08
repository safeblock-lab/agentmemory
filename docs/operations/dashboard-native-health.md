# Dashboard native health and updates

This document describes health and release-update behavior in the local AgentMemory viewer.

## Local release updates

The local viewer does not require a separate update password or `AGENTMEMORY_UPDATE_SECRET`. Update requests remain limited to the loopback viewer and trusted `Host` values. Supplied origins must match the exact viewer origin, and supplied Fetch Metadata site values must be `same-origin`. Starting an update requires the exact `Origin` header, no query parameters or body, and a random token from the current server process. The token rotates when an update starts, and the viewer rejects concurrent starts.

These checks protect the local update action from cross-origin browser requests. They do not identify individual Windows users; access to the local viewer remains the operator boundary.

## Supported installation

Updates are available for a native Windows global npm installation when the running worker owns the configured instance, runtime and data directories are absolute, the REST port and native engine metadata agree, the global package matches the running package, and the bundled npm CLI and AgentMemory CLI are present. If supervisor scripts are installed, they must support the updater maintenance protocol.

The updater checks the latest stable GitHub release, verifies its checksum and package contents, then stages installation and restart. The dashboard reports update status. If an update fails, inspect the local update log and status for the recovery result.
