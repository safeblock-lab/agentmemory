# Internal fork usage

This repository builds its own GitHub Release archives. It does not publish to npm.

## Create a release

1. Commit and push the changes to `main`.
2. Ensure the Git tag exactly matches the existing `package.json` version. For example, version `0.9.28` requires tag `v0.9.28`.
3. Push the tag:

   ```powershell
   git tag v0.9.28
   git push origin v0.9.28
   ```

GitHub Actions builds and tests the tag, creates the package archive, verifies it, creates a SHA-256 checksum, and attaches all three release assets.

## Install on Windows

1. Open the selected GitHub Release.
2. Download `Install-AgentMemory.ps1`.
3. In PowerShell, run:

   ```powershell
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\Install-AgentMemory.ps1 -Version v0.9.28
   ```

The installer downloads the matching archive and checksum from that release, verifies the checksum, then installs the archive globally with npm. It replaces any existing global `@agentmemory/agentmemory` installation. It does not alter `~/.agentmemory`, its `.env`, MCP configuration, hooks, or running services.

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
