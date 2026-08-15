import { execFileSync } from "node:child_process";
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

describe.skipIf(process.platform !== "win32")("Install-AgentMemory.ps1", () => {
  it("accepts an omitted version without downloading or installing", () => {
    expect(runInstallerWhatIf()).toContain("the latest release");
  });

  it("keeps an explicit version install path", () => {
    expect(runInstallerWhatIf("-Version", "v0.9.31")).toContain("v0.9.31");
  });
});
