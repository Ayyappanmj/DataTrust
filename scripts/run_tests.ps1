<# .SYNOPSIS Lint and run the test suite. Use -Fast to skip Spark/dbt integration tests. #>
param([switch]$Fast)
. "$PSScriptRoot\_common.ps1"
$py = Get-VenvPython
& $py -m ruff check .
if ($LASTEXITCODE -ne 0) { throw "ruff reported problems" }
$paths = if ($Fast) { @("tests/unit", "tests/quality", "tests/incremental") } else { @("tests") }
& $py -m pytest @paths -q
if ($LASTEXITCODE -ne 0) { throw "tests failed" }
