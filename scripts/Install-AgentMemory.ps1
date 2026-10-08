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
  $response = Invoke-WebRequest -Uri $latestUrl -MaximumRedirection 5 -TimeoutSec 60 -UseBasicParsing
  $resolvedUri = $response.BaseResponse.ResponseUri
  if ($null -eq $resolvedUri) {
    $resolvedUri = $response.BaseResponse.RequestMessage.RequestUri
  }
  if ($null -eq $resolvedUri) {
    throw "Could not determine the final AgentMemory release URL from $latestUrl."
  }
  $resolvedUri = [Uri]$resolvedUri
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

function Invoke-ReleaseAssetDownload([string]$Uri, [string]$Destination) {
  try {
    Invoke-WebRequest -Uri $Uri -OutFile $Destination -MaximumRedirection 5 -TimeoutSec 60 -UseBasicParsing
    return $true
  } catch {
    $response = $_.Exception.Response
    if ($null -ne $response -and $null -ne $response.StatusCode -and [int]$response.StatusCode -eq 404) {
      return $false
    }
    throw
  }
}

$requestedRelease = if ($Version) { $Version } else { "the latest release" }
if (-not $PSCmdlet.ShouldProcess("the global npm installation", "Install agentmemory $requestedRelease")) {
  return
}

if (-not $Version) {
  $Version = Resolve-LatestReleaseTag
}

$ReleaseTag = if ($Version.StartsWith("v", [System.StringComparison]::Ordinal)) { $Version } else { "v$Version" }
$VersionNumber = $ReleaseTag.Substring(1)
$CanonicalTarballName = "agentmemory-agentmemory-$VersionNumber.tgz"
$LegacyTarballName = "agentmemory-$ReleaseTag.tgz"
$TarballName = $CanonicalTarballName
$ReleaseBase = "https://$ReleaseHost/$Repository/releases/download/$ReleaseTag"

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

try {
  New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
  $downloaded = Invoke-ReleaseAssetDownload "$ReleaseBase/$CanonicalTarballName" $tarballPath
  if (-not $downloaded) {
    if (Test-Path -LiteralPath $tarballPath) {
      Remove-Item -LiteralPath $tarballPath -Force
    }
    $TarballName = $LegacyTarballName
    $tarballPath = Join-Path $temporaryDirectory $TarballName
    $downloaded = Invoke-ReleaseAssetDownload "$ReleaseBase/$LegacyTarballName" $tarballPath
    if (-not $downloaded) {
      throw "AgentMemory release $ReleaseTag has no package asset in a supported format."
    }
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
