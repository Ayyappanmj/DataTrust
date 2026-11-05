<# .SYNOPSIS Run the full batch pipeline (lightweight mode: SQLite metadata, no Airflow). #>
param([string]$InputPath = "data/sample", [switch]$Force, [switch]$IngestOnly, [ValidateSet("duckdb", "dbt")][string]$Engine = "duckdb")
. "$PSScriptRoot\_common.ps1"
$cmd = if ($IngestOnly) { "ingest" } else { "run" }
$extra = @(); if ($Force) { $extra += "--force" }
Invoke-DataTrust $cmd --input $InputPath --engine $Engine @extra
