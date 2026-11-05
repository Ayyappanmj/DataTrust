<#
.SYNOPSIS  Check prerequisites, create .venv and install dependencies (runs on the Windows host).
.PARAMETER Dev  Also install test/lint/dashboard tools (default: yes).
#>
param([switch]$CoreOnly)
. "$PSScriptRoot\_common.ps1"

Write-Host "== Checking prerequisites ==" -ForegroundColor Cyan
$pythonCmd = $null
foreach ($candidate in @("py -3.11", "py -3.12", "python")) {
    try {
        $parts = $candidate.Split(" ")
        $ver = & $parts[0] $parts[1..($parts.Length)] --version 2>$null
        if ($ver -match "Python 3\.(1[0-2])") { $pythonCmd = $candidate; Write-Host "Found $ver via '$candidate'"; break }
    } catch { }
}
if (-not $pythonCmd) { throw "Python 3.10-3.12 is required (3.11 recommended). Install from python.org." }

$java = Get-Command java -ErrorAction SilentlyContinue
if (-not $java) {
    Write-Warning "Java not found on PATH. PySpark needs Java 17 (e.g. Temurin). The Docker route avoids this."
} else { Write-Host (& java -version 2>&1 | Select-Object -First 1) }

if (-not $env:HADOOP_HOME -or -not (Test-Path (Join-Path $env:HADOOP_HOME "bin\winutils.exe"))) {
    Write-Warning ("HADOOP_HOME\bin\winutils.exe not found. Spark on native Windows often needs it to write files. " +
                   "Either install winutils for your Hadoop version or run the pipeline in Docker " +
                   "(see TROUBLESHOOTING.md).")
}
$docker = Get-Command docker -ErrorAction SilentlyContinue
if ($docker) { Write-Host "Docker found (optional)." } else { Write-Host "Docker not found (optional)." }

Write-Host "== Creating virtual environment ==" -ForegroundColor Cyan
if (-not (Test-Path ".venv")) {
    $parts = $pythonCmd.Split(" ")
    & $parts[0] $parts[1..($parts.Length)] -m venv .venv
}
$py = Get-VenvPython
& $py -m pip install --upgrade pip
$req = if ($CoreOnly) { "requirements.txt" } else { "requirements-dev.txt" }
Write-Host "== Installing $req ==" -ForegroundColor Cyan
& $py -m pip install -r $req
if ($LASTEXITCODE -ne 0) { throw "pip install failed" }
if (-not (Test-Path ".env")) { Copy-Item ".env.example" ".env"; Write-Host "Created .env from .env.example (placeholders only)." }
Invoke-DataTrust validate-config
Write-Host "Setup complete." -ForegroundColor Green
