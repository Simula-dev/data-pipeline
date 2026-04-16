# Opens an SSM port-forwarding tunnel from localhost:5432 through the bastion
# to the RDS endpoint. Keeps running until you Ctrl-C.
#
# Prerequisites:
#   - AWS CLI v2 installed and configured
#   - Session Manager plugin installed:
#     https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
#
# Usage:
#   .\scripts\dev-tunnel.ps1

$ErrorActionPreference = "Stop"

Write-Host "Looking up bastion instance..." -ForegroundColor Cyan
$BastionId = aws cloudformation describe-stacks `
    --stack-name DataPipeline-Bastion `
    --query 'Stacks[0].Outputs[?OutputKey==`BastionInstanceId`].OutputValue' `
    --output text

if (-not $BastionId) {
    Write-Host "Could not find bastion. Has DataPipeline-Bastion been deployed?" -ForegroundColor Red
    exit 1
}
Write-Host "  bastion: $BastionId"

Write-Host "Looking up RDS endpoint..." -ForegroundColor Cyan
$RdsHost = aws rds describe-db-instances `
    --query 'DBInstances[?DBName==`data_pipeline`].Endpoint.Address | [0]' `
    --output text

if (-not $RdsHost -or $RdsHost -eq "None") {
    Write-Host "Could not find RDS instance with DBName data_pipeline." -ForegroundColor Red
    exit 1
}
Write-Host "  rds: $RdsHost"

Write-Host ""
Write-Host "Starting SSM tunnel: localhost:5433 -> $RdsHost:5432" -ForegroundColor Green
Write-Host "Press Ctrl-C to close the tunnel." -ForegroundColor Yellow
Write-Host ""

# Using local port 5433 so there's no conflict if you have a local Postgres install on 5432.
aws ssm start-session `
    --target $BastionId `
    --document-name AWS-StartPortForwardingSessionToRemoteHost `
    --parameters "{`"host`":[`"$RdsHost`"],`"portNumber`":[`"5432`"],`"localPortNumber`":[`"5433`"]}"
