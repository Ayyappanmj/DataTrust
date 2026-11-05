<# .SYNOPSIS Docker helpers. Commands below run INSIDE containers; everything else runs on the Windows host.
   Actions: pipeline | dashboard | postgres | airflow-init | airflow | down #>
param([Parameter(Mandatory = $true)][ValidateSet("pipeline", "dashboard", "postgres", "airflow-init", "airflow", "down")][string]$Action)
. "$PSScriptRoot\_common.ps1"
if (-not (Test-Path ".env")) { Copy-Item ".env.example" ".env"; Write-Warning "Created .env from .env.example - edit the placeholder passwords." }
switch ($Action) {
    "pipeline"     { docker compose run --rm app python -m src.cli generate-sample; docker compose run --rm app python -m src.cli run --input data/sample }
    "dashboard"    { docker compose up dashboard }
    "postgres"     { docker compose --profile postgres up -d postgres }
    "airflow-init" { docker compose -f docker-compose.airflow.yml up airflow-init }
    "airflow"      { docker compose -f docker-compose.airflow.yml up -d }
    "down"         { docker compose --profile postgres --profile tools down; docker compose -f docker-compose.airflow.yml down }
}
