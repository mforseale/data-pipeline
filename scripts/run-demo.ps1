param([string]$SnapshotDate = (Get-Date -Format 'yyyy-MM-dd'))
$ErrorActionPreference = 'Stop'
$projectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
function Invoke-ProjectDocker {
    param([Parameter(ValueFromRemainingArguments=$true)][string[]]$DockerArguments)
    & docker @DockerArguments
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed with exit code $LASTEXITCODE" }
}
Push-Location -LiteralPath $projectRoot
try {
    Invoke-ProjectDocker info --format '{{.ServerVersion}}'
    Invoke-ProjectDocker compose up -d --build --wait --wait-timeout 240
    Invoke-ProjectDocker compose exec -T airflow python -m pipeline.seed
    Invoke-ProjectDocker compose exec -T airflow python scripts/run_unit_tests.py
    Invoke-ProjectDocker compose exec -T airflow python scripts/verify_pipeline.py --snapshot-date $SnapshotDate
    Invoke-ProjectDocker compose exec -T airflow python scripts/render_dag.py
    Invoke-ProjectDocker compose exec -T airflow python scripts/verify_airflow.py --snapshot-date $SnapshotDate
    Write-Output "Done. Airflow: http://localhost:8085"
    Write-Output "Report: $projectRoot\results\$SnapshotDate\report.html"
} finally {
    Pop-Location
}
