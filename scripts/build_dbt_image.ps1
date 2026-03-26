# Manual dbt image build + push to the CDK-managed ECR repo (Windows PowerShell).
#
# You DO NOT need to run this before `cdk deploy` — CDK's DockerImageAsset
# builds and pushes the image automatically during deploy. This script is
# here for quick iteration (e.g. rebuilding after a Dockerfile tweak without
# running a full cdk synth).

$ErrorActionPreference = "Stop"

$Region   = if ($env:AWS_REGION) { $env:AWS_REGION } else { "us-east-1" }
$Account  = (aws sts get-caller-identity --query Account --output text).Trim()
$Repo     = "cdk-hnb659fds-container-assets-$Account-$Region"
$Registry = "$Account.dkr.ecr.$Region.amazonaws.com"

Write-Host "Logging into ECR: $Registry"
aws ecr get-login-password --region $Region | docker login --username AWS --password-stdin $Registry

$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

Write-Host "Building image from ./dbt ..."
docker build --platform linux/amd64 -t "data-pipeline-dbt:local" ./dbt

$Tag = "$Registry/${Repo}:dbt-$([int][double]::Parse((Get-Date -UFormat %s)))"
docker tag "data-pipeline-dbt:local" $Tag
docker push $Tag

Write-Host ""
Write-Host "Pushed: $Tag"
Write-Host "Note: CDK manages image refs via asset hash. Run 'cdk deploy DataPipeline-Compute' to point the task definition at a rebuilt image."
