# Sets the environment variables needed for dbt to connect to RDS via the
# local SSM tunnel (see scripts/dev-tunnel.ps1).
#
# Usage:
#   .\scripts\dev-env.ps1
#   cd dbt
#   dbt run --target dev

$ErrorActionPreference = "Stop"

Write-Host "Fetching RDS credentials from Secrets Manager..." -ForegroundColor Cyan
$SecretJson = aws secretsmanager get-secret-value `
    --secret-id data-pipeline/rds/admin `
    --query SecretString --output text

$Secret = $SecretJson | ConvertFrom-Json

# Point to the local tunnel, not the real RDS endpoint
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "5433"
$env:POSTGRES_USER = $Secret.username
$env:POSTGRES_PASSWORD = $Secret.password
$env:POSTGRES_DATABASE = "data_pipeline"

Write-Host "Environment variables set for this shell session:" -ForegroundColor Green
Write-Host "  POSTGRES_HOST     = localhost  (tunneled to RDS)"
Write-Host "  POSTGRES_PORT     = 5433"
Write-Host "  POSTGRES_USER     = $env:POSTGRES_USER"
Write-Host "  POSTGRES_PASSWORD = (hidden)"
Write-Host "  POSTGRES_DATABASE = $env:POSTGRES_DATABASE"
Write-Host ""
Write-Host "Make sure scripts/dev-tunnel.ps1 is running in another terminal." -ForegroundColor Yellow
Write-Host "Then you can run dbt from the dbt/ directory:"
Write-Host "  cd dbt"
Write-Host "  dbt deps --profiles-dir ."
Write-Host "  dbt run --profiles-dir . --target dev"
Write-Host "  dbt test --profiles-dir . --target dev"
