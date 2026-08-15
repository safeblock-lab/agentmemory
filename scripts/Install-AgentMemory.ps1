[CmdletBinding(SupportsShouldProcess = $true)]
param(
  [Parameter(Mandatory = $false)]
  [ValidatePattern("^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$")]
  [string]$Version
)

$ErrorActionPreference = "Stop"
$Repository = "safeblock-lab/agentmemory"
$ReleaseHost = "github.com"

function Resolve-LatestReleaseTag {
  $latestUrl = "https://$ReleaseHost/$Repository/releases/latest"
  $response = Invoke-WebRequest -Uri $latestUrl -MaximumRedirection 5 -TimeoutSec 60
  # PowerShell returns an HttpResponseMessage whose final redirect target is
  # carried by RequestMessage.RequestUri.
  $resolvedUri = [Uri]$response.BaseResponse.RequestMessage.RequestUri
  $expectedPrefix = "/$Repository/releases/tag/"

  if ($resolvedUri.Host -ne $ReleaseHost -or -not $resolvedUri.AbsolutePath.StartsWith($expectedPrefix, [System.StringComparison]::Ordinal)) {
    throw "Could not resolve the latest AgentMemory release from $latestUrl."
  }

  $tag = $resolvedUri.AbsolutePath.Substring($expectedPrefix.Length)
  if ($tag -notmatch "^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$") {
    throw "Latest release tag is not a supported version: $tag."
  }

  return $tag
}

$requestedRelease = if ($Version) { $Version } else { "the latest release" }
if (-not $PSCmdlet.ShouldProcess("the global npm installation", "Install agentmemory $requestedRelease")) {
  return
}

if (-not $Version) {
  $Version = Resolve-LatestReleaseTag
}

$TarballName = "agentmemory-$Version.tgz"
$ReleaseBase = "https://$ReleaseHost/$Repository/releases/download/$Version"

$node = Get-Command node -ErrorAction SilentlyContinue
$npm = Get-Command npm -ErrorAction SilentlyContinue
if ($null -eq $node -or $null -eq $npm) {
  throw "Node.js 20 or newer (including npm) is required."
}

$nodeVersion = (& $node.Source --version).Trim()
if ($nodeVersion -notmatch "^v(?<major>\d+)\.") {
  throw "Could not determine the installed Node.js version."
}
if ([int]$Matches["major"] -lt 20) {
  throw "Node.js 20 or newer is required; found $nodeVersion."
}

$temporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("agentmemory-install-" + [guid]::NewGuid().ToString("N"))
$tarballPath = Join-Path $temporaryDirectory $TarballName
$checksumsPath = Join-Path $temporaryDirectory "SHA256SUMS.txt"

try {
  New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
  Invoke-WebRequest -Uri "$ReleaseBase/$TarballName" -OutFile $tarballPath -MaximumRedirection 5 -TimeoutSec 60
  Invoke-WebRequest -Uri "$ReleaseBase/SHA256SUMS.txt" -OutFile $checksumsPath -MaximumRedirection 5 -TimeoutSec 60

  $checksumLine = Get-Content -LiteralPath $checksumsPath | Where-Object {
    $_ -match "^(?<hash>[A-Fa-f0-9]{64})\s+\*?$([regex]::Escape($TarballName))$"
  } | Select-Object -First 1
  if ($null -eq $checksumLine) {
    throw "The release checksum file does not contain an entry for $TarballName."
  }
  $expectedHash = ([regex]::Match($checksumLine, "^[A-Fa-f0-9]{64}")).Value.ToLowerInvariant()
  $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $tarballPath).Hash.ToLowerInvariant()
  if ($actualHash -ne $expectedHash) {
    throw "Checksum verification failed. The package was not installed."
  }

  & $npm.Source install --global $tarballPath
  if ($LASTEXITCODE -ne 0) {
    throw "npm global installation failed with exit code $LASTEXITCODE."
  }

  Write-Host "agentmemory $Version installed successfully."
} finally {
  if (Test-Path -LiteralPath $temporaryDirectory) {
    Remove-Item -LiteralPath $temporaryDirectory -Recurse -Force
  }
}
