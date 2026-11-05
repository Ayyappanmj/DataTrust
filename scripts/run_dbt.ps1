<# .SYNOPSIS Run a dbt command (compile, run, test, build) against data\warehouse\datatrust.duckdb. #>
param([ValidateSet("compile", "run", "test", "build", "seed")][string]$Command = "build")
. "$PSScriptRoot\_common.ps1"
Invoke-DataTrust dbt --dbt-command $Command
