import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const installerPath = fileURLToPath(
  new URL("../scripts/Install-AgentMemory.ps1", import.meta.url),
);

function runInstallerWhatIf(...args: string[]): string {
  return execFileSync(
    "powershell.exe",
    [
      "-NoProfile",
      "-ExecutionPolicy",
      "Bypass",
      "-File",
      installerPath,
      "-WhatIf",
      ...args,
    ],
    { encoding: "utf8" },
  );
}

function runLatestReleaseResolver(responseShape: "windows" | "core" | "missing"): string {
  const script = `
. $env:AGENTMEMORY_INSTALLER_PATH -WhatIf
function Invoke-WebRequest {
  param(
    [string]$Uri,
    [int]$MaximumRedirection,
    [int]$TimeoutSec,
    [switch]$UseBasicParsing
  )
  if (-not $UseBasicParsing) { throw "UseBasicParsing was not set." }
  if ($Uri -ne "https://github.com/safeblock-lab/agentmemory/releases/latest") {
    throw "Unexpected URL: $Uri"
  }
  if ($MaximumRedirection -ne 5 -or $TimeoutSec -ne 60) {
    throw "Unexpected redirect or timeout settings."
  }
  switch ($env:AGENTMEMORY_RESPONSE_SHAPE) {
    "windows" {
      return [pscustomobject]@{
        BaseResponse = [pscustomobject]@{
          ResponseUri = [Uri]"https://github.com/safeblock-lab/agentmemory/releases/tag/v0.9.32"
        }
      }
    }
    "core" {
      return [pscustomobject]@{
        BaseResponse = [pscustomobject]@{
          RequestMessage = [pscustomobject]@{
            RequestUri = [Uri]"https://github.com/safeblock-lab/agentmemory/releases/tag/v0.9.32"
          }
        }
      }
    }
    default { return [pscustomobject]@{ BaseResponse = [pscustomobject]@{} } }
  }
}
Resolve-LatestReleaseTag
`;

  return execFileSync(
    "powershell.exe",
    ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", script],
    {
      encoding: "utf8",
      env: {
        ...process.env,
        AGENTMEMORY_INSTALLER_PATH: installerPath,
        AGENTMEMORY_RESPONSE_SHAPE: responseShape,
      },
    },
  );
}

describe.skipIf(process.platform !== "win32")("Install-AgentMemory.ps1", () => {
  it("accepts an omitted version without downloading or installing", () => {
    expect(runInstallerWhatIf()).toContain("the latest release");
  });

  it("keeps an explicit version install path", () => {
    expect(runInstallerWhatIf("-Version", "v0.9.31")).toContain("v0.9.31");
  });

  it("resolves latest releases from Windows PowerShell responses", () => {
    expect(runLatestReleaseResolver("windows")).toContain("v0.9.32");
  });

  it("resolves latest releases from PowerShell 7 responses", () => {
    expect(runLatestReleaseResolver("core")).toContain("v0.9.32");
  });

  it("rejects responses without a final release URL", () => {
    expect(() => runLatestReleaseResolver("missing")).toThrow(
      /Could not determine the final AgentMemory release URL/,
    );
  });

  it("uses basic parsing for every web request", () => {
    const webRequests = readFileSync(installerPath, "utf8")
      .split(/\r?\n/)
      .filter((line) => /\bInvoke-WebRequest\s+-/.test(line));

    expect(webRequests).toHaveLength(3);
    expect(webRequests.every((line) => line.includes("-UseBasicParsing"))).toBe(true);
  });
});
