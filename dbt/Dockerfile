# dbt container — runs on ECS Fargate, invoked by Step Functions.
# Build and push to ECR before first pipeline run.
#
# Build:  docker build -t data-pipeline-dbt .
# Push:   docker tag data-pipeline-dbt:latest <account>.dkr.ecr.<region>.amazonaws.com/data-pipeline-dbt:latest
#         docker push <account>.dkr.ecr.<region>.amazonaws.com/data-pipeline-dbt:latest

FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir dbt-snowflake==1.8.*

COPY . /app/dbt

# Credentials injected at runtime via ECS environment variables from SSM
ENV DBT_PROFILES_DIR=/app/dbt

ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "/app/dbt"]
