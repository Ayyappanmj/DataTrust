<# .SYNOPSIS Rebuild Silver/Gold for a pickup-date range from Bronze. Other partitions are untouched. #>
param([Parameter(Mandatory = $true)][string]$Start, [Parameter(Mandatory = $true)][string]$End)
. "$PSScriptRoot\_common.ps1"
Invoke-DataTrust backfill --start $Start --end $End
