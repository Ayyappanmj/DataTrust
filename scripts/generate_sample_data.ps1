<# .SYNOPSIS Generate deterministic synthetic taxi CSVs into data\sample (adds months without changing old files). #>
param([string[]]$Months = @("2023-01", "2023-02"), [int]$Rows = 1000)
. "$PSScriptRoot\_common.ps1"
Invoke-DataTrust generate-sample --out data/sample --months @Months --rows $Rows
