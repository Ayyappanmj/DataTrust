# Shared helpers, dot-sourced by the other scripts. Runs on Windows PowerShell 5.1 or PowerShell 7.
$ErrorActionPreference = "Stop"
$script:ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $script:ProjectRoot

# Automatically configure portable Java 17 and winutils if present
$jdkDir = Join-Path $script:ProjectRoot ".jdk\jdk-17.0.20.1+1"
if (-not $env:JAVA_HOME -and (Test-Path $jdkDir)) {
    $env:JAVA_HOME = $jdkDir
    $env:PATH = "$jdkDir\bin;" + $env:PATH
}
$hadoopDir = Join-Path $script:ProjectRoot ".hadoop"
if (-not $env:HADOOP_HOME -and (Test-Path $hadoopDir)) {
    $env:HADOOP_HOME = $hadoopDir
    $env:PATH = "$hadoopDir\bin;" + $env:PATH
}

function Get-VenvPython {
    $py = Join-Path $script:ProjectRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path $py)) { throw "Virtual environment not found. Run .\scripts\setup.ps1 first." }
    return $py
}

function Invoke-DataTrust {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$CliArgs)
    $py = Get-VenvPython
    & $py -m src.cli @CliArgs
    if ($LASTEXITCODE -ne 0) { throw "datatrust $($CliArgs -join ' ') failed with exit code $LASTEXITCODE" }
}
