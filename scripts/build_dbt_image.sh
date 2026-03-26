#!/usr/bin/env bash
# Manual dbt image build + push to the CDK-managed ECR repo.
#
# You DO NOT need to run this before `cdk deploy` \u2014 CDK's DockerImageAsset
# builds and pushes the image automatically during deploy. This script is
# here for quick iteration (e.g. rebuilding after a Dockerfile tweak without
# running a full cdk synth).

set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

# CDK DockerImageAsset pushes to a repo named "cdk-hnb659fds-container-assets-<account>-<region>"
REPO="cdk-hnb659fds-container-assets-${ACCOUNT}-${REGION}"
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"

echo "Logging into ECR: ${REGISTRY}"
aws ecr get-login-password --region "${REGION}" \
    | docker login --username AWS --password-stdin "${REGISTRY}"

cd "$(dirname "$0")/.."
echo "Building image from ./dbt ..."
docker build --platform linux/amd64 -t data-pipeline-dbt:local ./dbt

TAG="${REGISTRY}/${REPO}:dbt-$(date +%s)"
docker tag data-pipeline-dbt:local "${TAG}"
docker push "${TAG}"

echo ""
echo "Pushed: ${TAG}"
echo "Note: CDK manages image refs via asset hash. Run \`cdk deploy DataPipeline-Compute\` to point the task definition at a rebuilt image."
