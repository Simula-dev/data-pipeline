# LinkedIn Post — Draft

## Post text (under 1300 characters)

---

Most data pipeline tutorials stop at "read CSV, load to database."

I wanted to know what a production pipeline actually looks like — so I built one from scratch.

The result: a fully orchestrated ETL/ELT pipeline on AWS that ingests from any REST API, loads into PostgreSQL, transforms via dbt, runs optional ML inference, validates data quality, and notifies on success or failure.

Everything is infrastructure-as-code. One command deploys 8 CloudFormation stacks. One command tears it all down.

Tech stack:
- AWS CDK (Python) for all infrastructure
- Step Functions for orchestration
- Lambda + ECS Fargate for compute
- RDS PostgreSQL as the warehouse
- dbt Core for transformations (staging / intermediate / marts)
- SageMaker for batch ML inference
- CloudWatch EMF metrics + alarms + dashboard
- GitHub Actions CI/CD (test + synth + deploy)

What I learned building it:
- CDK cross-stack references have real gotchas (circular deps, export locks)
- dbt's power is in the layered transformation model, not just SQL templating
- pg8000 (pure Python Postgres driver) eliminates Docker bundling entirely
- Step Functions + Lambda is genuinely simpler than Airflow for this scale

Full source code (open source): github.com/simula-dev/data-pipeline
Architecture diagrams in the repo.
Live demo available — happy to walk through the running pipeline on a call.

#DataEngineering #AWS #dbt #Python #CDK #PostgreSQL #ETL #DataPipeline

---
