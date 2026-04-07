# dbt container \u2014 runs on ECS Fargate, invoked by Step Functions.
# Built automatically by CDK DockerImageAsset during `cdk deploy`.

FROM python:3.12-slim

WORKDIR /app/dbt

# System deps: git for dbt package installs, gcc/libpq-dev for dbt-redshift
# (psycopg2-binary doesn't exist for all platforms so we build from source)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.8.* \
    dbt-redshift==1.8.*

COPY . /app/dbt/

RUN dbt deps --profiles-dir /app/dbt

ENV DBT_PROFILES_DIR=/app/dbt \
    PYTHONUNBUFFERED=1

CMD ["dbt", "build", "--profiles-dir", "/app/dbt"]
