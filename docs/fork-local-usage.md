# Internal fork usage

This repository builds its own GitHub Release archives. It does not publish to npm.

## Create a release

1. Commit and push the changes to `main`.
2. Ensure the Git tag exactly matches the existing `package.json` version. For example, version `0.9.32` requires tag `v0.9.32`.
3. Push the tag:

   ```powershell
   git tag v0.9.32
   git push origin v0.9.32
   ```

GitHub Actions builds and tests the tag, creates the package archive, verifies it, creates a SHA-256 checksum, and attaches all three release assets.

## Install on Windows

1. Open the selected GitHub Release.
2. Download `Install-AgentMemory.ps1`.
3. In PowerShell, run:

   ```powershell
   # Omit -Version to install the latest stable release.
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\Install-AgentMemory.ps1

   # Or pin the installation to a specific release.
   # powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\Install-AgentMemory.ps1 -Version v0.9.32
   ```

Without `-Version`, the installer resolves the latest stable GitHub Release, then downloads its archive and checksum, verifies the checksum, and installs the archive globally with npm. It replaces any existing global `@agentmemory/agentmemory` installation. It does not alter `~/.agentmemory`, its `.env`, MCP configuration, hooks, or running services.

## Update from the local viewer

For a running native Windows instance installed globally with npm, open its local viewer and select **Health → Check for update**. The viewer does not contact GitHub until you select that button. It shows the current and available versions and asks you to confirm **Install and restart**. The updater downloads this fork's latest stable GitHub Release, checks its SHA-256 checksum and the archive's package name, version, and CLI layout, then installs it with npm lifecycle scripts disabled. It restarts the same AgentMemory instance with its existing data directory and REST port. The viewer shows the download, stop, install, start, reconnect, completion, and failure states. Completion requires the iii engine, the expected API version, connected engine health, and an HTTP response from the viewer port advertised by the running API. A skipped or unreachable viewer fails readiness and starts rollback. A slow index rebuild can take up to 15 minutes before readiness; the viewer keeps polling through that period. Failure details stay in the local `update.log`, while the viewer shows a stable error code.

Before using the viewer updater, set `AGENTMEMORY_UPDATE_SECRET` to a random value of at least 32 characters in `~/.agentmemory/.env`, restrict that file to the account running AgentMemory, and restart the service. The updater remains disabled until this secret is configured. The first **Check for update** or **View update status** action opens a masked password dialog. The browser holds the secret only in memory for the current page session and sends it in `X-AgentMemory-Update-Secret` to the local viewer. A page reload requires entering it again. Do not put the secret in a URL or share it with other local accounts.

The button is disabled with a reason when the running instance is not an owned native Windows global npm installation, including WSL, Docker, or a source checkout. An update failure attempts to restore and restart the previous package. If the viewer cannot reconnect, inspect `update.log` in the instance's runtime directory (`AGENTMEMORY_RUNTIME_DIR`, or `~/.agentmemory` by default), then check the service with `agentmemory doctor`. Do not start another update while an update is in progress.

On a machine with `Start-AgentMemory.ps1` and `Invoke-AgentMemoryWatchdog.ps1` in the runtime directory, both scripts must implement **AgentMemory updater protocol v1** before the button is enabled. The updater atomically writes `update-maintenance.json` with `jobId`, its live `pid`, `phase` (`pause` or `resume`), `generation`, and `startedAt`. Before replacing the package, it sets `pause`, stops the old CLI, and waits for the supervisor to atomically write `update-supervisor-ack.json` with the same `jobId` and `generation` after the CLI exits. The supervisor must avoid launching a CLI while a live marker says `pause`; on `resume` it launches the installed CLI normally. The watchdog must skip recovery for either phase while the marker owner is live, including a second check immediately before restarting anything. If the owner process is gone, the scripts ignore the stale marker so normal recovery resumes. Rollback uses a new generation and acknowledgement before replacing the package. The updater removes its marker and acknowledgement when it finishes. A supervisor that cannot acknowledge within 30 seconds leaves the package untouched.

The local viewer limits update routes to loopback Host and Origin values. Every updater route requires the dedicated secret before inspecting the installation or contacting GitHub; starting also requires a one-use update token. A process running as the account that can read `.env` can still obtain the secret, so keep that account and its browser sessions trusted.

For an unsupported installation or manual recovery, download `Install-AgentMemory.ps1` from the intended GitHub Release and use the installer command above, optionally with `-Version`. The installer does not restart a running service; stop the current instance before installing and start `agentmemory` again afterward with the same data directory and port.

## Install the plugin, skills, hooks, and MCP server

Start the server in a separate terminal:

```powershell
agentmemory
```

Then, in Codex or Claude Code, install the plugin from this fork:

```text
/plugin marketplace add safeblock-lab/agentmemory
/plugin install agentmemory
```

The plugin installs the bundled skills and hooks. Its MCP configuration runs the local release command `agentmemory mcp`; it does not run `npx @agentmemory/mcp`.

For a host that does not load the plugin automatically, wire MCP after the global install:

```powershell
agentmemory connect codex
# or: agentmemory connect claude-code
```

Restart the host after installation. The hooks then capture observations automatically, and the skills tell the agent when to use the memory tools.
