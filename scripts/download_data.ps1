<#
.SYNOPSIS  Optionally download an official NYC TLC monthly Parquet file into data\raw.
.NOTES     The project never needs this at runtime. If the download fails, fetch the file manually from
           https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page and place it in data\raw.
#>
param([string]$Month = "2023-01", [ValidateSet("yellow", "green")][string]$Taxi = "yellow")
. "$PSScriptRoot\_common.ps1"
if ($Month -notmatch '^\d{4}-\d{2}$') { throw "Month must look like 2023-01" }
$file = "${Taxi}_tripdata_$Month.parquet"
$url = "https://d37ci6vzurychx.cloudfront.net/trip-data/$file"
$dest = Join-Path $script:ProjectRoot "data\raw\$file"
if (Test-Path $dest) { Write-Host "$dest already exists; not downloading again."; return }
try {
    Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing
    Write-Host "Saved $dest ($([math]::Round((Get-Item $dest).Length / 1MB, 1)) MB)"
} catch {
    if (Test-Path $dest) { Remove-Item $dest }
    throw "Download failed ($url): $($_.Exception.Message). Download manually from the TLC page into data\raw."
}
Write-Host "Run:  .\scripts\run_pipeline.ps1 -InputPath data\raw"
