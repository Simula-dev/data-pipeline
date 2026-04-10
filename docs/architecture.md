# Architecture

## Pipeline flow

```mermaid
flowchart TD
    subgraph INGEST["Step 1: Ingest"]
        A[REST API Source] -->|HTTP with pagination,<br/>rate limiting, auth| B[Lambda: Ingest]
        B -->|NDJSON| C[(S3 Raw Bucket)]
    end

    subgraph LOAD["Step 2: Load"]
        C -->|Read NDJSON| D[Lambda: Load]
        D -->|Batch INSERT jsonb| E[(PostgreSQL<br/>raw.landing)]
    end

    subgraph TRANSFORM["Step 3: dbt Transform"]
        E -->|SELECT jsonb| F[ECS Fargate:<br/>dbt build]
        F -->|staging views| G[staging schema]
        F -->|ephemeral CTEs| H[intermediate]
        F -->|materialized tables| I[marts schema]
    end

    subgraph ML["Step 4: ML Inference (optional)"]
        direction LR
        J{ML Enabled?}
        J -->|Yes| K[Lambda: Export<br/>marts to S3 CSV]
        K --> L[SageMaker<br/>Batch Transform]
        L --> M[Lambda: Load<br/>predictions back]
        J -->|No| N[Skip]
    end

    subgraph QUALITY["Step 5: Quality Gate"]
        O[Lambda: Quality Gate]
        O -->|row counts, freshness,<br/>null rates, uniqueness,<br/>custom SQL| P{Checks Pass?}
    end

    subgraph NOTIFY["Step 6: Notify"]
        P -->|Yes| Q[Lambda: Notify<br/>SUCCESS]
        P -->|No| R[Lambda: Notify<br/>FAILURE]
        Q --> S[SNS Email +<br/>CloudWatch Metrics]
        R --> S
    end

    INGEST --> LOAD --> TRANSFORM --> ML --> QUALITY

    style INGEST fill:#e8f5e9,stroke:#2e7d32
    style LOAD fill:#e3f2fd,stroke:#1565c0
    style TRANSFORM fill:#fff3e0,stroke:#ef6c00
    style ML fill:#f3e5f5,stroke:#7b1fa2
    style QUALITY fill:#fce4ec,stroke:#c62828
    style NOTIFY fill:#eceff1,stroke:#37474f
```

## AWS services map

```mermaid
flowchart LR
    subgraph Orchestration
        SF[AWS Step Functions]
    end

    subgraph Compute
        L1[AWS Lambda x6]
        ECS[ECS Fargate]
    end

    subgraph Storage
        S3[(Amazon S3)]
        RDS[(RDS PostgreSQL)]
    end

    subgraph ML
        SM[SageMaker<br/>Batch Transform]
    end

    subgraph Monitoring
        CW[CloudWatch<br/>Metrics + Alarms]
        SNS[SNS<br/>Notifications]
    end

    subgraph Security
        SEC[Secrets Manager]
        SSM[SSM Parameters]
        IAM[IAM Roles]
    end

    subgraph IaC
        CDK[AWS CDK<br/>8 Python stacks]
        GHA[GitHub Actions<br/>CI/CD]
    end

    SF --> L1
    SF --> ECS
    L1 --> S3
    L1 --> RDS
    ECS --> RDS
    L1 --> SM
    L1 --> SNS
    L1 --> CW
    CDK --> SF
    CDK --> L1
    CDK --> ECS
    CDK --> S3
    CDK --> RDS
    GHA --> CDK
```

## CDK stack dependency graph

```mermaid
flowchart TD
    NET[Network Stack<br/>VPC, 3 AZs, NAT] --> RDS_S[RDS Stack<br/>PostgreSQL, Secrets,<br/>Security Groups]
    NET --> ING[Ingestion Stack<br/>S3 Bucket, 6 Lambdas]
    RDS_S --> ING
    RDS_S --> COMP[Compute Stack<br/>ECS Cluster, dbt Image,<br/>Task Definition]
    NET --> COMP
    ING --> DS[DataSync Stack<br/>Bulk S3 transfers]
    ING --> SM[SageMaker Stack<br/>ML Execution Role,<br/>Model Registry]
    MON[Monitoring Stack<br/>SNS, Notify Lambda,<br/>Alarms, Dashboard]
    ING --> ORCH[Orchestration Stack<br/>Step Functions<br/>State Machine]
    COMP --> ORCH
    MON --> ORCH

    style NET fill:#e0e0e0
    style RDS_S fill:#e3f2fd
    style ING fill:#e8f5e9
    style COMP fill:#fff3e0
    style ORCH fill:#f3e5f5
    style DS fill:#eceff1
    style SM fill:#fce4ec
    style MON fill:#eceff1
```

## Tech stack summary

| Layer | Technology |
|---|---|
| **Infrastructure as Code** | AWS CDK (Python), 8 stacks |
| **Orchestration** | AWS Step Functions |
| **Compute** | AWS Lambda (Python 3.12) + ECS Fargate |
| **Database** | Amazon RDS PostgreSQL 16 (free tier) |
| **Object Storage** | Amazon S3 |
| **Transformation** | dbt Core (dbt-postgres) |
| **ML** | AWS SageMaker (Batch Transform, scikit-learn) |
| **Monitoring** | CloudWatch (EMF metrics, alarms, dashboard) |
| **Notifications** | Amazon SNS |
| **CI/CD** | GitHub Actions (test + synth + deploy) |
| **Secrets** | AWS Secrets Manager + SSM Parameter Store |
