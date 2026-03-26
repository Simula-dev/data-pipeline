# dbt container \u2014 runs on ECS Fargate, invoked by Step Functions.
# Built automatically by CDK DockerImageAsset during `cdk deploy`.
#
# Manual build (for testing):
#   docker build -t data-pipeline-dbt:local dbt/
#   docker run --rm \
#     -e SNOWFLAKE_ACCOUNT=... \
#     -e SNOWFLAKE_USER=... \
#     -e SNOWFLAKE_PASSWORD=... \
#     data-pipeline-dbt:local dbt run

FROM python:3.12-slim

WORKDIR /app/dbt

# System deps: git is required by dbt for package installs from GitHub
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt Snowflake adapter (pulls dbt-core transitively)
RUN pip install --no-cache-dir \
    dbt-core==1.8.* \
    dbt-snowflake==1.8.*

# Copy the dbt project (the Dockerfile lives inside the dbt project, so
# the build context = project root)
COPY . /app/dbt/

# Install dbt packages declared in packages.yml (dbt_utils, etc.)
RUN dbt deps --profiles-dir /app/dbt

ENV DBT_PROFILES_DIR=/app/dbt \
    PYTHONUNBUFFERED=1

# Default command \u2014 Step Functions overrides this per-step so the same
# container image can run `dbt run`, `dbt test`, `dbt source freshness`, etc.
CMD ["dbt", "build", "--profiles-dir", "/app/dbt"]
