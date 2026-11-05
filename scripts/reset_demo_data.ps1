<# .SYNOPSIS Delete GENERATED data (bronze/silver/gold/quarantine/reports/warehouse/metadata). Asks first. #>
param([switch]$Yes, [switch]$IncludeSample)
. "$PSScriptRoot\_common.ps1"
$extra = @(); if ($Yes) { $extra += "--yes" }; if ($IncludeSample) { $extra += "--include-sample" }
Invoke-DataTrust reset @extra
