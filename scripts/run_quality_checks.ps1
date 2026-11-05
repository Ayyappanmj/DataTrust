<# .SYNOPSIS Re-run the data quality rules on the warehouse Silver table; reports go to data\reports\quality. #>
. "$PSScriptRoot\_common.ps1"
Invoke-DataTrust quality
